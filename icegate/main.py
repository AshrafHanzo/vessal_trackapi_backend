from fastapi import FastAPI, Query
import uvicorn

app = FastAPI(
    title="Icegate Sea IGM Tracker",
    description="Tracks Sea IGM document status on Icegate portal",
    version="1.0.0"
)


@app.get("/icegate", summary="Track Sea IGM Document Status")
async def track_document(
    mbl_no: str = Query(..., description="Master Bill of Lading number"),
    bl_no: str = Query(..., description="Bill of Lading number"),
    port: str = Query(None, description="Port code (optional, default: ALL_PORTS)"),
):
    """
    Provide the following details to track the Sea IGM document status on Icegate:
    - **mbl_no**: Master Bill of Lading number
    - **bl_no**: Bill of Lading number
    """
    from tracker import run_tracker
    result = await run_tracker(
        port=port,
        mbl_no=mbl_no,
        bl_no=bl_no
    )
    return result


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8013, reload=True)
