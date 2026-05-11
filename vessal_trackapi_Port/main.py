"""
Vessel Tracking API - Port Search Only
Uses subprocess to avoid thread issues with Playwright
Run with: python main.py
"""

import subprocess
import json
from fastapi import FastAPI, HTTPException, Query
import uvicorn
import os
import sys

app = FastAPI(title="Vessel Tracking API - Port Search", version="2.0.0")

# Path to the tracking script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LDB_SCRIPT = os.path.join(SCRIPT_DIR, "ldb_tracker.py")
# Use the same python that's running this script (from venv)
PYTHON_EXE = sys.executable


@app.get("/search", summary="Port Search - In, Out, Name")
def search_container(
    container_no: str = Query(..., description="Container number to search")
):
    """Get Port Name, Port In (Date & Time), Port Out (Date & Time) for a container"""
    if not container_no:
        raise HTTPException(status_code=400, detail="Container number is required")
        
    try:
        # Run the LDB tracker script as a subprocess
        result = subprocess.run(
            [PYTHON_EXE, LDB_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=SCRIPT_DIR
        )
        
        if result.returncode != 0:
             raise HTTPException(status_code=500, detail=f"LDB tracker error: {result.stderr}")
        
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
    print("VESSEL TRACKING API - PORT SEARCH")
    print("=" * 50)
    print("Server running at: http://localhost:8015")
    print("API docs: http://localhost:8015/docs")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8015)
