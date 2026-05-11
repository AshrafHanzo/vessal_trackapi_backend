from fastapi import FastAPI, HTTPException, Query
import uvicorn
import sys
import os
import json
import subprocess

# Add current directory to sys.path to allow imports if needed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

app = FastAPI()

@app.get("/interasia")
def track(container_no: str = Query(..., description="Container Number")):
    try:
        # Run the tracker worker as a subprocess
        # script_path = os.path.join(os.path.dirname(__file__), "tracker_worker.py")
        # Use python from the same venv
        python_executable = sys.executable 
        
        # Determine script path
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "tracker_worker.py"))
        
        print(f"Running worker: {python_executable} {script_path} {container_no}")
        
        result = subprocess.run(
            [python_executable, script_path, container_no],
            capture_output=True,
            text=True,
            encoding='utf-8' # Force UTF-8
        )
        
        if result.returncode != 0:
            print(f"Worker failed: {result.stderr}")
            return {"status": "error", "message": "Tracker script failed", "details": result.stderr}
            
        # Parse the JSON output from the worker
        try:
            # The worker should print ONLY the JSON to stdout
            # But sometimes logs get mixed in. We need to find the JSON.
            output = result.stdout.strip()
            # print(f"Raw Output: {output}")
            
            # Simple heuristic: find the last occurrence of '{' and '}'
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
    uvicorn.run(app, host="0.0.0.0", port=1010)
