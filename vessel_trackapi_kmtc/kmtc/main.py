"""
KMTC Container Tracking API
FastAPI service to track containers on the eKMTC website.
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from tracker_worker import track_container


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="KMTC Container Tracker API",
    description="API to fetch ETA (arrival) and departure details for containers from eKMTC.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class TrackingRequest(BaseModel):
    container_number: str


class TrackingResponse(BaseModel):
    container_no: str
    departure_value: str
    departure_date: str
    eta_date: str
    eta_value: str


class ErrorResponse(BaseModel):
    error: str
    container_no: str | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/track/{container_number}", response_model=TrackingResponse)
def track_get(container_number: str):
    """
    Track a container by providing the container number in the URL.

    - **container_number**: e.g. `BEAU2857767`
    """
    result = track_container(container_number, headless=True)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return TrackingResponse(**result)


# ---------------------------------------------------------------------------
# Run with: uvicorn main:app --reload
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1012)
