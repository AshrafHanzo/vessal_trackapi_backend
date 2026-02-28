"""
tracker.py — Called by FastAPI main.py.
Launches tracker_worker.py as a completely separate subprocess using
subprocess.run (blocking) inside a ThreadPoolExecutor.
This avoids ALL asyncio event loop conflicts on Windows.
"""
import asyncio
import json
import sys
import os
import subprocess
import concurrent.futures


def _launch_worker(port: str, mbl_no: str, bl_no: str) -> dict:
    """Runs in a thread — calls tracker_worker.py as a blocking subprocess."""
    python_exe = sys.executable
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracker_worker.py")

    print(f"[Tracker] Launching worker: {worker_path}", flush=True)

    try:
        proc = subprocess.run(
            [python_exe, worker_path, port, mbl_no, bl_no],
            capture_output=True,
            text=True
        )

        # Print worker stderr logs to our console
        if proc.stderr:
            for line in proc.stderr.splitlines():
                print(f"  {line}", flush=True)

        # Parse JSON result from stdout (last line)
        output = proc.stdout.strip()
        if output:
            last_line = output.splitlines()[-1]
            try:
                return json.loads(last_line)
            except json.JSONDecodeError:
                return {
                    "port_name": port,
                    "mbl_number": mbl_no,
                    "bl_number": bl_no,
                    "status": "error",
                    "message": f"Worker output not valid JSON: {last_line}"
                }
        else:
            return {
                "port_name": port,
                "mbl_number": mbl_no,
                "bl_number": bl_no,
                "status": "error",
                "message": f"Worker produced no output. Return code: {proc.returncode}"
            }

    except Exception as e:
        import traceback
        print(f"[Tracker] Failed to launch worker:\n{traceback.format_exc()}", flush=True)
        return {
            "port_name": port,
            "mbl_number": mbl_no,
            "bl_number": bl_no,
            "status": "error",
            "message": f"Failed to launch worker: {str(e)}"
        }


async def run_tracker(port: str, mbl_no: str, bl_no: str) -> dict:
    """
    Runs the blocking subprocess launcher in a thread pool,
    so FastAPI's async endpoint doesn't block.
    """
    # Multi-port mapping: each name maps to ALL related port codes in the region
    # The worker will try each code until it finds results
    PORT_MAPPING = {
        "chennai":    ["INMAA", "INKAT", "INENR"],
        "kattupalli": ["INKAT", "INMAA", "INENR"],
        "ennore":     ["INENR", "INMAA", "INKAT"],
    }
    
    if port:
        normalized_port = port.strip().lower()
        # Get the list of port codes, or use the raw input as a single-element list
        port_codes = PORT_MAPPING.get(normalized_port, [port.upper()])
        # Pass as comma-separated string to the worker
        base_port = ",".join(port_codes)
    else:
        # If port is None, use ALL unique port codes from the mapping
        all_codes = set()
        for codes in PORT_MAPPING.values():
            all_codes.update(codes)
        # Sort to ensure deterministic order (optional, but good for debugging)
        # Default order: INMAA first if possible, but set is unordered so we need a list
        # Creating a reasonable default list: INMAA, INKAT, INENR
        # Or just sorted list of unique codes
        base_port = ",".join(sorted(list(all_codes)))

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        result = await loop.run_in_executor(
            pool,
            _launch_worker,
            base_port,
            mbl_no,
            bl_no
        )
    return result
