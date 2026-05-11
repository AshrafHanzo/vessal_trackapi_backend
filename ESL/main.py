from fastapi import FastAPI, HTTPException
import subprocess
import json
import os
import sys

app = FastAPI(title="ESL Tracker API")

@app.get("/track")
def track_get(container_no: str):
    return run_worker(container_no)

@app.post("/track")
def track_post(payload: dict):
    container_no = payload.get("container_no") or payload.get("container_number")
    if not container_no:
        raise HTTPException(status_code=400, detail="Missing container_no")
    return run_worker(container_no)

def run_worker(container_no: str):
    worker_path = os.path.join(os.path.dirname(__file__), "tracker_worker.py")
    
    # Use the venv python explicitly if it exists to fix ModuleNotFoundError
    venv_python = os.path.join(os.path.dirname(os.path.dirname(__file__)), "venv", "Scripts", "python.exe")
    if os.path.exists(venv_python):
        python_exe = venv_python
    else:
        python_exe = sys.executable

    try:
        # Run the Playwright/Selenium worker
        result = subprocess.run(
            [python_exe, worker_path, container_no],
            capture_output=True,
            text=True,
            timeout=180 # Giving more time because EasyOCR + Playwright takes some time, + retries
        )
        
        # Parse JSON output from stdout
        # The worker should print raw logs to stderr and the final JSON to stdout
        try:
            # Extract last line or attempt to parse whole thing
            output = result.stdout.strip()
            # If there are multiple lines, try to find the JSON block inside
            if not output.startswith("{"):
                 # maybe it's at the end
                 lines = output.splitlines()
                 for line in reversed(lines):
                     if line.startswith("{"):
                         return json.loads(line)
                 raise json.JSONDecodeError("Could not find start of JSON block", output, 0)
                 
            return json.loads(output)
        except json.JSONDecodeError:
            return {
                "status": "error",
                "message": "Worker failed to return valid JSON",
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
    except subprocess.TimeoutExpired:
         return {"status": "error", "message": "Tracking request timed out after 180s"}
    except Exception as e:
         return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1014)
