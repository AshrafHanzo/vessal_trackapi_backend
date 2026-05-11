from fastapi import FastAPI, HTTPException, Query
import uvicorn
import sys
import os
import json
import subprocess

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

app = FastAPI()

@app.get("/cosco")
def track(container_no: str = Query(..., description="Container Number")):
    try:
        python_executable = sys.executable 
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "tracker_worker.py"))
        
        print(f"Running worker: {python_executable} {script_path} {container_no}")
        
        # To match the structure of other projects, we call the worker via subprocess
        result = subprocess.run(
            [python_executable, script_path, container_no],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        
        if result.returncode != 0:
            print(f"Worker failed: {result.stderr}")
            return {"status": "error", "message": "Tracker script failed", "details": result.stderr}
            
        try:
            output = result.stdout.strip()
            start = output.find('{')
            end = output.rfind('}') + 1
            if start != -1 and end != -1:
                json_str = output[start:end]
                data = json.loads(json_str)
                return data
            else:
                 return {"status": "error", "message": "No JSON found in worker output", "raw_output": output}

        except json.JSONDecodeError as e:
            return {"status": "error", "message": "Invalid JSON from worker", "details": str(e), "raw_output": result.stdout}

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8030)
