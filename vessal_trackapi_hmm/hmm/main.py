from fastapi import FastAPI, HTTPException
from tracker_worker import get_tracking_data
import uvicorn

app = FastAPI(title="HMM Tracker API")

@app.get("/track")
def track(container_number: str):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = get_tracking_data(container_number)
            if "error" in result:
                # If it's the last attempt, raise the error.
                if attempt == max_retries - 1:
                    raise HTTPException(status_code=400, detail=result["error"])
                continue # Retry on error 
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                raise HTTPException(status_code=500, detail=str(e))
            continue

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=1011)
