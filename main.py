"""
Vessel Tracking API
Uses subprocess to avoid thread issues with Playwright
Run with: python main.py
"""

import subprocess
import json
import re
from fastapi import FastAPI, HTTPException, Query
import uvicorn
from typing import Dict, Any, List
import os
import sys

app = FastAPI(title="Vessel Tracking API", version="1.0.0")


# Path to the tracking script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "tracker.py")
ICEGATE_SCRIPT = os.path.join(SCRIPT_DIR, "icegate_tracker.py")
LDB_SCRIPT = os.path.join(SCRIPT_DIR, "ldb_tracker.py")
# Use the same python that's running this script (from venv)
PYTHON_EXE = sys.executable


@app.get("/track")
def track(container_number: str = Query(..., description="Container number to track")):
    """Track a container by its number"""
    if not container_number:
        raise HTTPException(status_code=400, detail="Container number is required")
    
    try:
        # Run the tracker script as a subprocess using venv python
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_number],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=SCRIPT_DIR
        )
        
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Tracker error: {result.stderr}")
        
        # Parse the JSON output
        output = result.stdout.strip()
        
        # Find JSON in output (skip any print statements before it)
        json_start = output.find('{')
        if json_start == -1:
            raise HTTPException(status_code=500, detail="No JSON output from tracker")
        
        json_str = output[json_start:]
        data = json.loads(json_str)
        
        return data
        
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Tracking request timed out")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse tracker output: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/icegate")
def icegate_track(
    mbl_no: str = Query(..., description="Master Bill of Lading number"),
    bl_no: str = Query(..., description="Bill of Lading number"),
    port: str = Query(None, description="Port code (optional)")
):
    """Track shipment from ICEGATE using MBL, Port and BL number"""
    try:
        # Prepare arguments
        port_arg = port if port else "ALL_PORTS"
        
        # Run the ICEGATE tracker script as a subprocess
        result = subprocess.run(
            [PYTHON_EXE, ICEGATE_SCRIPT, mbl_no, port_arg, bl_no],
            capture_output=True,
            text=True,
            timeout=300,  # Longer timeout for multiple ports
            cwd=SCRIPT_DIR
        )
        
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=f"ICEGATE tracker error: {result.stderr}")
        
        # Parse the JSON output
        output = result.stdout.strip()
        
        # Find JSON in output
        json_start = output.find('{')
        if json_start == -1:
            raise HTTPException(status_code=500, detail="No JSON output from ICEGATE tracker")
        
        json_str = output[json_start:]
        data = json.loads(json_str)
        
        return data
        
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="ICEGATE request timed out")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse ICEGATE output: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
def search_container(
    container_no: str = Query(..., description="Container number to search")
):
    """Search for a container on LDB website"""
    if not container_no:
        raise HTTPException(status_code=400, detail="Container number is required")
        
    try:
        # Run the LDB tracker script as a subprocess
        result = subprocess.run(
            [PYTHON_EXE, LDB_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=300,  # Allow time for manual inspection
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
    print("VESSEL TRACKING API")
    print("=" * 50)
    print("Server running at: http://localhost:8011")
    print("API docs: http://localhost:8011/docs")
    print("=" * 50) # This line was missing in the provided snippet, adding if __name__ == "__main__":
    import platform
    import multiprocessing
    
    os_name = platform.system()
    
    if os_name == "Windows":
        # Windows: Use standard uvicorn.run without manual workers argument 
        # or properly protect the entry point. 
        # The simplest reliable way on Windows for dev is single worker or 
        # careful string-based loading.
        print(f"🚀 Detected Windows OS. Starting Server (Single Worker for Compatibility)...")
        # Multi-worker on Windows often causes socket sharing issues in simple scripts.
        # We will revert to single worker for Windows dev to ensure stability.
        # If parallel processing is needed, we can run multiple instances on different ports,
        # but for this UAT testing, single worker async server should handle 5 connections fine.
        uvicorn.run(app, host="0.0.0.0", port=8011)
    else:
        # Linux/Mac: Single process (compatible with external gunicorn commands)
        print(f"🐧 Detected Linux/Mac OS. Starting Server...")
        uvicorn.run(app, host="0.0.0.0", port=8011)
