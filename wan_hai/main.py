import subprocess
import sys
import os
from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI(title="Wan Hai Tracker API")

# Path to worker script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_PATH = os.path.join(BASE_DIR, "tracker_worker.py")
PYTHON_EXE = sys.executable 
# Use the venv python if available, else system python
VENV_PYTHON = os.path.join(os.path.dirname(BASE_DIR), "venv", "Scripts", "python.exe")
if os.path.exists(VENV_PYTHON):
    PYTHON_EXE = VENV_PYTHON

@app.get("/wan", summary="Track Wan Hai Container")
async def track_container(
    container_no: str = Query(..., description="Container Number")
):
    """
    Launches the Wan Hai tracker worker for the given container number.
    The worker will open the browser and wait for further instructions.
    """
    try:
        # Launch worker as a subprocess
        # We use Popen so it runs in background and doesn't block API
        # But we need to capture output or let it run attached?
        # User said "open and wait", so likely they want to see it running.
        # If we run it headless=False, it will pop up on server (if GUI exists) or fail.
        # Assuming local dev environment for now.
        
        cmd = [PYTHON_EXE, WORKER_PATH, container_no]
        print(f"Launching worker: {' '.join(cmd)}")
        
        # Run and capture output
        # Using subprocess.run to wait for completion
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
             return {"status": "error", "message": "Worker failed", "details": result.stderr}
             
        # Parse stdout (which should contain the JSON)
        # We need to filter out any non-JSON lines if present (though we print to stderr mostly)
        output = result.stdout
        try:
            # Find the start of JSON? Or assume mostly JSON.
            # Worker prints logs to stderr, only data to stdout (hopefully).
            # But earlier prints in worker used `file=sys.stderr`? 
            # I checked tracker_worker.py, most prints are to stderr.
            # The data print is `print(json.dumps(data, indent=2))` (to stdout).
            import json
            data = json.loads(output)
            return data
        except json.JSONDecodeError:
            return {"status": "error", "message": "Invalid JSON from worker", "raw_output": output, "logs": result.stderr}

        
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=1017)
