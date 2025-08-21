import uvicorn
import sys
import os

# Add the project root to the Python path. This is required for the API server
# to find the `src` module when running from the `api/` directory.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import subprocess
import threading
import logging
from collections import deque
from datetime import datetime
from src import google_sheets_helpers
from config.config import settings
import base64
from fastapi.security import APIKeyHeader

# --- Security ---
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key: str = Depends(api_key_header)):
    """Dependency to validate the API key."""
    if not api_key or api_key != settings.WEB_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials"
        )
    return api_key

# --- Setup ---
app = FastAPI()

# Adjust paths for Vercel deployment
# The templates directory is now one level up from the 'api' directory
templates_dir = os.path.join(os.path.dirname(__file__), '..', 'templates')
app.mount("/static", StaticFiles(directory=templates_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)

log_buffer = deque(maxlen=300) # Store the last 300 log lines

# --- Logging ---
# A custom handler to route logs to our in-memory buffer
class DequeLogHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        log_buffer.append(log_entry)

# Configure the root logger to use our handler
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Add our custom handler
logging.getLogger().addHandler(DequeLogHandler())


# --- State Management ---
# Track the status of each script: idle, running, success, error
process_status = {
    "build_prospect_list": {"status": "idle", "pid": None},
    "run_daily_sending": {"status": "idle", "pid": None},
    "run_follow_ups": {"status": "idle", "pid": None},
    "process_bounces": {"status": "idle", "pid": None},
    "deduplicate_sheet": {"status": "idle", "pid": None}
}

# --- Helper Functions ---
def run_script_in_thread(script_name: str, args: list = []):
    """
    Target function for threads. Runs a script and captures its output.
    """
    logger = logging.getLogger(script_name)
    process_status[script_name]["status"] = "running"
    
    try:
        command = ["python3", f"{script_name}.py"] + args
        logger.info(f"Starting script with command: {' '.join(command)}")
        
        process = subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True, 
            bufsize=1, 
            universal_newlines=True
        )
        process_status[script_name]["pid"] = process.pid

        # Stream output to the logger in real-time
        for line in iter(process.stdout.readline, ''):
            logger.info(line.strip())
        
        process.stdout.close()
        return_code = process.wait()
        
        if return_code == 0:
            process_status[script_name]["status"] = "success"
            logger.info(f"Script finished successfully.")
        else:
            process_status[script_name]["status"] = "error"
            logger.error(f"Script failed with return code {return_code}.")
            
    except Exception as e:
        process_status[script_name]["status"] = "error"
        logger.error(f"An exception occurred while running the script: {e}", exc_info=True)
    finally:
        process_status[script_name]["pid"] = None


# --- FastAPI Routes ---
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serves the main control panel HTML page."""
    return templates.TemplateResponse("index.html", {"request": request, "api_key": settings.WEB_API_KEY})

@app.post("/run-script/{script_name}")
async def run_script_endpoint(script_name: str, 
                              query: str = Form(None), 
                              max_leads: int = Form(None),
                              max_emails: int = Form(None),
                              limit: int = Form(None),
                              api_key: str = Depends(get_api_key)):
    """
    Endpoint to trigger a script. Runs it in a background thread to avoid blocking.
    """
    if script_name not in process_status:
        return JSONResponse(status_code=404, content={"message": "Script not found"})
        
    if process_status[script_name]["status"] == "running":
        return JSONResponse(status_code=409, content={"message": "Process is already running"})

    args = []
    if script_name == "build_prospect_list":
        if not query or max_leads is None:
            return JSONResponse(status_code=400, content={"message": "Missing required parameters for build_prospect_list"})
        args.extend([query, f"--max_leads={max_leads}"])
    elif script_name == "run_daily_sending":
        if max_emails is None:
            return JSONResponse(status_code=400, content={"message": "Missing required parameters for run_daily_sending"})
        args.append(f"--max_emails={max_emails}")
    elif script_name == "run_follow_ups":
        if limit is None:
            return JSONResponse(status_code=400, content={"message": "Missing required parameters for run_follow_ups"})
        args.append(f"--limit={limit}")

    # Reset status before starting
    process_status[script_name]["status"] = "idle"

    thread = threading.Thread(target=run_script_in_thread, args=(script_name, args))
    thread.daemon = True # Allows main thread to exit even if background threads are running
    thread.start()
    
    return JSONResponse(status_code=202, content={"message": f"Script '{script_name}' started."})

@app.get("/status")
async def get_status():
    """Endpoint to fetch the current status of all scripts."""
    return JSONResponse(content=process_status)

@app.get("/logs")
async def get_logs():
    """Endpoint to fetch the latest logs for the frontend."""
    return JSONResponse(content={"logs": list(log_buffer)})

if __name__ == "__main__":
    print("Starting web server for local development at http://127.0.0.1:8000")
    uvicorn.run("index:app", host="127.0.0.1", port=8000, reload=True)
