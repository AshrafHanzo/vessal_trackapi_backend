"""
RCL Container Tracking API
FastAPI service to track containers on the RCL eService website.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
import json
import os
import sys

# Use the same python executable as the server
PYTHON_EXE = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_PATH = os.path.join(BASE_DIR, "tracker_worker.py")

# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="RCL Container Tracker API",
    description="API to fetch ETA (arrival) and departure details for containers from RCL eService.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class TrackingResponse(BaseModel):
    container_no: str
    departed_value: str
    departed_date: str
    eta_date: str
    eta_value: str


class ErrorResponse(BaseModel):
    error: str
    container_no: str | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/track/{container_number}", response_model=TrackingResponse)
def track_get(container_number: str):
    """
    Track a container by providing the container number in the URL.

    - **container_number**: e.g. `RCIU1234567`

    Returns POL as departed_value, ETA as eta_date, and POD as eta_value.
    """
    
    try:
        cmd = [PYTHON_EXE, WORKER_PATH, container_number]
        print(f"Launching RCL worker: {' '.join(cmd)}")
        
        import tempfile
        import os
        
        # Use a temporary file to capture output instead of subprocess.PIPE (capture_output=True)
        # Because pipelined STDOUT alerts undetected_chromedriver/Chrome to headless-like behavior
        # Which triggers Turnstile immediately on Windows systems!
        fd, tmp_path = tempfile.mkstemp(suffix=".txt", text=True)
        os.close(fd)
        
        try:
            with open(tmp_path, "w", encoding="utf-8", errors="replace") as out:
                subprocess.run(cmd, cwd=BASE_DIR, stdout=out, stderr=subprocess.STDOUT)
            
            with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
                output = f.read()
        finally:
            os.remove(tmp_path)
        
        tracking_data = None
        if "--- TRACKING RESULT ---" in output:
            try:
                json_str = output.split("--- TRACKING RESULT ---")[1].strip()
                # Clean up any trailing traceback data caused by Windows OSError during driver.quit()
                end_idx = json_str.rfind('}')
                if end_idx != -1:
                    json_str = json_str[:end_idx+1]
                tracking_data = json.loads(json_str)
            except Exception as e:
                print(f"JSON parsing error: {e}. Raw JSON portion: {json_str[:100]}...")
                pass
                
        if tracking_data and "error" not in tracking_data:
            return TrackingResponse(**tracking_data)
        elif tracking_data and "error" in tracking_data:
            raise HTTPException(status_code=404, detail=tracking_data["error"])
        else:
            # For debugging, we return the last part of the output to see what happened
            last_msg = output[-500:] if len(output) > 500 else output
            print(f"Worker output mismatch. Last 500 chars:\n{last_msg}")
            raise HTTPException(
                status_code=500, 
                detail=f"Tracking data not found. Possible bot blocking or timeout. Last output: {last_msg}"
            )
            
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Server Internal Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Run with: uvicorn main:app --reload
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1016)
