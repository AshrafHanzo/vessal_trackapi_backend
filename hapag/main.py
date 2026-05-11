from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
import os
import sys

app = FastAPI()

class ContainerRequest(BaseModel):
    container_no: str

# Use the same python executable as the server
PYTHON_EXE = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_PATH = os.path.join(BASE_DIR, "tracker_worker.py")

@app.get("/hapag")
def track_hapag(container_no: str):
    try:
        if not container_no:
            raise HTTPException(status_code=400, detail="Container number is required")

        cmd = [PYTHON_EXE, WORKER_PATH, container_no]
        print(f"Launching worker: {' '.join(cmd)}")
        
        import json
        
        # Run and capture output synchronously
        result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)
        
        output = result.stdout
        
        # Extract the JSON payload after our known delimiter
        tracking_data = None
        if "--- TRACKING RESULT ---" in output:
            try:
                json_str = output.split("--- TRACKING RESULT ---")[1].strip()
                tracking_data = json.loads(json_str)
            except Exception as e:
                pass
                
        if tracking_data:
            return {
                "status": "success",
                "container_no": container_no,
                "data": tracking_data
            }
        else:
            return {
                "status": "error",
                "message": "Failed to retrieve tracking data. Container may be unauthorized or bot blocked.",
                "worker_stderr": result.stderr
            }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8016)
