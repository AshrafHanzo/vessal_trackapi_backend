from fastapi import FastAPI, HTTPException, Query
import subprocess
import os
import sys
import json
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Adani Ports (Ennore) Container Tracker")

# Constants
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADANI_SCRIPT = os.path.join(SCRIPT_DIR, "adani_tracker.py")

# Determine Python Executable
if os.path.exists(os.path.join(SCRIPT_DIR, "venv", "Scripts", "python.exe")):
    PYTHON_EXE = os.path.join(SCRIPT_DIR, "venv", "Scripts", "python.exe")
elif os.path.exists(os.path.join(SCRIPT_DIR, "venv", "bin", "python")):
    PYTHON_EXE = os.path.join(SCRIPT_DIR, "venv", "bin", "python")
else:
    PYTHON_EXE = sys.executable



@app.get("/ennore", summary="Adani Ports (Ennore) Search")
def ennore_search(
    container_no: str = Query(..., description="Container number to search")
):
    """
    Search for a container on Adani Ports (Ennore).
    Returns Destination Code, Entry Time, Exit Time, and Scan Status.
    """
    if not container_no:
        raise HTTPException(status_code=400, detail="Container number is required")

    logger.info(f"Received request for container: {container_no}")

    try:
        # Run the tracker script
        result = subprocess.run(
            [PYTHON_EXE, ADANI_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=120, # Playwright can be slow
            cwd=SCRIPT_DIR,
            check=False
        )

        if result.returncode != 0:
            logger.error(f"Tracker script failed: {result.stderr}")
            # Try to parse stdout even if failed, sometimes it prints JSON error
            try:
                data = json.loads(result.stdout)
                return data
            except:
                raise HTTPException(status_code=500, detail=f"Tracker script error: {result.stderr}")

        # Parse output
        output = result.stdout.strip()
        logger.info(f"Tracker output: {output}")

        try:
            data = json.loads(output)
            return data
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON output: {output}")
            raise HTTPException(status_code=500, detail="Invalid response from tracker script")

    except subprocess.TimeoutExpired:
        logger.error("Tracker script timed out")
        raise HTTPException(status_code=504, detail="Tracking request timed out")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Host on 0.0.0.0 to be accessible, port 8017 as before
    uvicorn.run(app, host="0.0.0.0", port=8017)
