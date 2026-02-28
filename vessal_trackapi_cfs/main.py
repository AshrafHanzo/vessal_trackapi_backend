"""
CITPL Container Tracking API
Uses subprocess to avoid thread issues with Playwright
Run with: python main.py
"""

import subprocess
import json
from fastapi import FastAPI, HTTPException, Query
import uvicorn
import os
import sys

app = FastAPI(title="CITPL Tracking API", version="1.0.0")

# Path to the tracking script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CITPL_SCRIPT = os.path.join(SCRIPT_DIR, "citpl_tracker.py")

# Use the same python that's running this script (from venv)
PYTHON_EXE = sys.executable

@app.get("/citpl", summary="CITPL CFS Search - In, Out, Name")
def citpl_search(
    container_no: str = Query(..., description="Container number to search")
):
    """Get CITPL CFS Name, Entry (Date & Time), Exit (Date & Time) for a container"""
    if not container_no:
        raise HTTPException(status_code=400, detail="Container number is required")
        
    try:
        # Run the CITPL tracker script as a subprocess
        result = subprocess.run(
            [PYTHON_EXE, CITPL_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=SCRIPT_DIR
        )
        
        if result.returncode != 0:
             raise HTTPException(status_code=500, detail=f"CITPL tracker error: {result.stderr}")
        
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
    print("CITPL TRACKING API")
    print("=" * 50)
    print("Server running at: http://localhost:8014")
    print("API docs: http://localhost:8014/docs")
    print("=" * 50)
    
    import platform
    os_name = platform.system()
    
    if os_name == "Windows":
        print(f"Detected Windows OS. Starting Server...")
        uvicorn.run(app, host="0.0.0.0", port=8014)
    else:
        print(f"Detected Linux/Mac OS. Starting Server...")
        uvicorn.run(app, host="0.0.0.0", port=8014)
