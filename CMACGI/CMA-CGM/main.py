import subprocess
import sys
import json
from fastapi import FastAPI, HTTPException, Query
import uvicorn

app = FastAPI(title="CMA-CGM Tracking API")

@app.get("/")
async def root():
    return {"message": "CMA-CGM Tracking API is running. Use /track?container=YOUR_NUMBER"}

@app.get("/track")
async def track(
    container: str = Query(..., description="The container number to track"),
    api_key: str = Query(None, description="Optional API key if required")
):
    """
    Tracks a container and returns departure and ETA details.
    """
    if not container:
        raise HTTPException(status_code=400, detail="Container number is required")
    
    print(f"Tracking container: {container}")
    
    try:
        process = subprocess.run(
            [sys.executable, "tracker_worker.py", container],
            capture_output=True,
            text=True,
            timeout=180
        )
        
        # The worker might have crashed on exit but still printed JSON before crashing
        stdout = process.stdout
        lines = [line.strip() for line in stdout.split('\n') if line.strip()]
        
        result = None
        for line in reversed(lines):
            try:
                result = json.loads(line)
                break
            except json.JSONDecodeError:
                continue
                
        if result is None:
             raise HTTPException(status_code=500, detail=f"Failed to parse output. Stdout: {stdout[:200]}, Stderr: {process.stderr[:200]}")
             
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Request to CMA-CGM timed out.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")

    if "error" in result:
        if "Timeout" in result["error"]:
             raise HTTPException(status_code=504, detail="Data not found or timed out waiting for elements.")
        raise HTTPException(status_code=500, detail=result["error"])
    
    if result.get("departure_date") == "Not Found" and result.get("eta_date") == "Not Found":
        return {
            "status": "No data found",
            "container": container,
            "details": result
        }

    return {
        "status": "success",
        "container": container,
        "data": result
    }

if __name__ == "__main__":
    print("Starting CMA-CGM Tracking API...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
