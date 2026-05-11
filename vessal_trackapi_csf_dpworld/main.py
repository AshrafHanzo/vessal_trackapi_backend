import os
import sys
import subprocess
import json
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse

# Define script directory and paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# DPW Tracker Script Path
DPW_SCRIPT = os.path.join(SCRIPT_DIR, "dpw_tracker.py")

# Use the same python that's running this script (from venv)
PYTHON_EXE = sys.executable

app = FastAPI(
    title="DP World CFS Tracker API",
    description="API for tracking containers on DP World Chennai Container Terminal.",
    version="1.0.0"
)

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

@app.get("/dpw", summary="DP World CFS Search")
def dpw_search(
    container_no: str = Query(..., description="Container number to search")
):
    """
    Get CFS details from DP World CCT.
    Returns: CFS Name, In-Time, Out-Time, Scan Mark status.
    """
    if not container_no:
        raise HTTPException(status_code=400, detail="Container number is required")
        
    try:
        # Run the DPW tracker script as a subprocess
        result = subprocess.run(
            [PYTHON_EXE, DPW_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=120, # Increased timeout for browser launch
            cwd=SCRIPT_DIR
        )
        
        if result.returncode != 0:
             # Try to parse stderr if it's JSON (custom error) or just text
             error_msg = result.stderr.strip()
             raise HTTPException(status_code=500, detail=f"Tracker error: {error_msg}")
        
        output = result.stdout.strip()
        json_start = output.find('{')
        if json_start == -1:
            raise HTTPException(status_code=500, detail=f"No JSON output. Output: {output}")
        
        json_str = output[json_start:]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return {"status": "error", "message": "Failed to parse JSON", "raw_output": output}
            
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    print("=" * 50)
    print("DP WORLD CFS TRACKER API")
    print("=" * 50)
    
    # Platform specific settings
    if sys.platform == "win32":
        print("Detected Windows OS. Starting Server (Single Worker for Compatibility)...")
        uvicorn.run(app, host="0.0.0.0", port=8016, workers=1)
    else:
        print("Detected Linux/Mac OS. Starting Server...")
        uvicorn.run(app, host="0.0.0.0", port=8016)
    