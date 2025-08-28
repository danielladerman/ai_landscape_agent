import time
import json
import os
import subprocess
import logging
from config.config import settings

# --- Setup ---
QUEUE_FILE = os.path.join(settings.BASE_DIR, "job_queue.json")
LOG_FILE = os.path.join(settings.BASE_DIR, "logs", "worker.log")

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_project_root():
    """Returns the absolute path of the project root."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def run_job(job):
    """Executes a job from the queue."""
    script_name = job.get("script_name")
    args = job.get("args", [])
    
    if not script_name:
        logging.error("Job is missing 'script_name'. Skipping.")
        return

    logging.info(f"--- Starting job: {script_name} with args: {args} ---")
    
    project_root = get_project_root()
    script_path = os.path.join(project_root, f"{script_name}.py")
    command = ["python3", "-u", script_path] + args

    try:
        process = subprocess.Popen(
            command,
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        # Stream the output
        for line in iter(process.stdout.readline, ''):
            logging.info(f"[{script_name}] {line.strip()}")
        
        process.stdout.close()
        return_code = process.wait()
        
        if return_code == 0:
            logging.info(f"--- Job '{script_name}' completed successfully. ---")
        else:
            logging.error(f"--- Job '{script_name}' failed with return code {return_code}. ---")

    except Exception as e:
        logging.error(f"--- An exception occurred while running job '{script_name}': {e} ---", exc_info=True)

def process_queue_once():
    """Checks the queue file and processes any jobs found, then exits."""
    logging.info("Worker started. Checking for jobs in queue...")
    jobs_to_run = []
    
    # --- Read and clear the queue atomically ---
    try:
        if os.path.exists(QUEUE_FILE) and os.path.getsize(QUEUE_FILE) > 0:
            with open(QUEUE_FILE, 'r+') as f:
                try:
                    # Lock the file if possible (on Unix-like systems)
                    import fcntl
                    fcntl.flock(f, fcntl.LOCK_EX)
                    
                    jobs_to_run = json.load(f)
                    
                    # Clear the queue after reading
                    f.seek(0)
                    f.truncate()
                except (json.JSONDecodeError, ImportError):
                    # Fallback for non-locking environments or corrupted file
                    jobs_to_run = []
                finally:
                    # Always release the lock
                    if 'fcntl' in locals():
                        fcntl.flock(f, fcntl.LOCK_UN)
    except Exception as e:
        logging.error(f"Error reading or clearing queue file: {e}")

    # --- Run the jobs ---
    if jobs_to_run:
        logging.info(f"Found {len(jobs_to_run)} job(s) in the queue. Executing...")
        for job in jobs_to_run:
            run_job(job)
        logging.info("Finished processing all jobs in the queue.")
    else:
        logging.info("No jobs found in the queue.")

if __name__ == "__main__":
    process_queue_once()
