import os
import logging
from collections import deque
import threading
import subprocess
import time
from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
import numpy as np

from config.config import settings

# --- Logging Setup ---
# A deque is a thread-safe, memory-efficient list-like object
log_buffer = deque(maxlen=300) 

class DequeHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        DequeHandler(),
        logging.StreamHandler() # Also print to console
    ]
)

# --- State Management ---
# Track the status of each script: idle, running, success, error
process_status = {
    "build_prospect_list": {"status": "idle"},
    "run_daily_sending": {"status": "idle"},
    "run_follow_ups": {"status": "idle"},
    "process_bounces": {"status": "idle"}
}

# --- Dependencies ---
templates = Jinja2Templates(directory="templates")
security = HTTPBasic()

# --- Security ---
def check_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """Checks for correct username and password."""
    correct_username = secrets.compare_digest(credentials.username, settings.ADMIN_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, settings.ADMIN_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# --- Script Execution Logic ---
def get_project_root():
    """Helper function to get the project's root directory."""
    return os.path.abspath(os.path.dirname(__file__))

def run_script_in_thread(script_name: str, args: list):
    """
    Runs a script in a background thread using subprocess, streaming its output to the log buffer.
    """
    process_status[script_name]["status"] = "running"
    logging.info(f"Starting script with command: python3 -u {script_name}.py {' '.join(args)}")
    
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    command = ["python3", "-u", os.path.join(project_root, f"{script_name}.py")] + args

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            cwd=project_root
        )

        # Stream stdout line by line
        for line in iter(process.stdout.readline, ''):
            logging.info(line.strip())
        
        process.stdout.close()
        return_code = process.wait() # Wait for the process to complete

        if return_code == 0:
            process_status[script_name]["status"] = "success"
            logging.info(f"Script '{script_name}' finished successfully.")
        else:
            process_status[script_name]["status"] = "error"
            logging.error(f"Script '{script_name}' failed with return code {return_code}.")

    except Exception as e:
        process_status[script_name]["status"] = "error"
        logging.error(f"An exception occurred while running script '{script_name}': {e}", exc_info=True)


# --- Setup ---
app = FastAPI()

app.mount("/static", StaticFiles(directory="templates"), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request, username: str = Depends(check_auth)):
    """Serves the main control panel HTML page, protected by basic auth."""
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/run-script/{script_name}")
async def run_script_endpoint(script_name: str, 
                              query: str = Form(None), 
                              max_leads: int = Form(None),
                              max_emails: int = Form(None),
                              limit: int = Form(None),
                              username: str = Depends(check_auth)):
    """
    Endpoint to trigger a script. Runs it in a background thread to avoid blocking.
    Protected by Basic Authentication.
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
    thread.daemon = True
    thread.start()
    
    return JSONResponse(status_code=202, content={"message": f"Script '{script_name}' started."})

@app.get("/status")
async def get_status(username: str = Depends(check_auth)):
    """Endpoint to fetch the current status of all scripts. Protected."""
    return JSONResponse(content=process_status)

@app.get("/logs")
async def get_logs(username: str = Depends(check_auth)):
    """Endpoint to fetch the latest logs for the frontend. Protected."""
    return JSONResponse(content={"logs": list(log_buffer)})

@app.get("/dashboard-data")
async def get_dashboard_data(username: str = Depends(check_auth)):
    """
    Endpoint to get summary statistics for the dashboard. Protected.
    """
    from src import google_sheets_helpers
    from config.config import settings
    
    service = google_sheets_helpers.get_google_sheets_service()
    if not service:
        return JSONResponse(status_code=500, content={"error": "Could not connect to Google Sheets."})

    dashboard_data = google_sheets_helpers.get_sheet_summary_stats(
        service,
        settings.SPREADSHEET_ID,
        settings.GOOGLE_SHEET_NAME
    )
    
    if dashboard_data.get("error"):
        logging.error(f"Error fetching dashboard data: {dashboard_data['error']}")
        return JSONResponse(status_code=500, content=dashboard_data)

    # Convert numpy int64 types to standard Python int for JSON serialization
    for key, value in dashboard_data.items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                if isinstance(inner_value, np.int64):
                    value[inner_key] = int(inner_value)
        if isinstance(value, np.int64):
            dashboard_data[key] = int(value)
            
    return JSONResponse(content=dashboard_data)

# To run locally: uvicorn api.index:app --reload
