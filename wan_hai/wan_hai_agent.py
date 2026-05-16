"""
Wan Hai Agent Worker — Redis Queue Consumer with Auto-Scaling

Features:
  - Supervisor process monitors queue depth and auto-scales workers
  - Workers pull jobs from tc:queue:wan_hai via BRPOP
  - Runs wan_hai/tracker_worker.py as subprocess (Playwright)
  - POSTs Departed and ETA events to Portal API
  - Auto-retry with exponential backoff on failures
"""

import redis
import json
import subprocess
import requests
import time
import sys
import os
import traceback
import multiprocessing
import math
from datetime import datetime

# Add parent directory to path to import shared_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import shared_utils

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "tracker_worker.py")

# Detect Python: prefer local venv, fallback to current
VENV_PYTHON_WIN = os.path.join(os.path.dirname(SCRIPT_DIR), "venv", "Scripts", "python.exe")
VENV_PYTHON_LINUX = os.path.join(os.path.dirname(SCRIPT_DIR), "venv", "bin", "python")

if os.path.exists(VENV_PYTHON_WIN):
    PYTHON_EXE = VENV_PYTHON_WIN
elif os.path.exists(VENV_PYTHON_LINUX):
    PYTHON_EXE = VENV_PYTHON_LINUX
else:
    PYTHON_EXE = sys.executable

# Redis Keys
QUEUE_KEY = "tc:queue:wan_hai"
PROCESSING_KEY = "tc:processing:wan_hai"
FAILED_KEY = "tc:failed:wan_hai"
COMPLETED_KEY = "tc:completed:wan_hai"
RETRY_KEY = "tc:retries:wan_hai"
WORKERS_KEY = "tc:workers:wan_hai"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 300      # 5 minutes for Wan Hai (complex navigation)
SLEEP_BETWEEN_JOBS = 5

# Retry Config
RETRY_DELAY_CAP = 600
RETRY_DELAYS = [30, 60, 120, 300, 600]
MAX_RETRIES = 5  # Stop retrying after 5 failures to prevent death spirals

# Auto-Scaling Config
CPU_CORES = os.cpu_count() or 4
MIN_WORKERS = 1
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 5)) # Increased to speed up backlog
CONTAINERS_PER_WORKER = 1
SCALE_CHECK_INTERVAL = 30

# Status Hierarchy
STATUS_RANK = {
    "Created": 0, "Empty Return": 0, "Departed": 1, "ETA": 2,
    "IGM": 3, "Inward": 4, "Arrived at POD": 5, "Port In": 6,
    "Port Out": 7, "CFS In": 8, "CFS Out": 9, "Completed": 10
}


def get_rank(status):
    return STATUS_RANK.get(status, -1)


def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)




def process_job(job_data):
    """
    Process a single Wan Hai tracking job.
    """
    container_no = job_data.get("container_no", "UNKNOWN")
    api_status = job_data.get("status", "")

    print(f"    [WAN HAI] Tracking {container_no}...")
    try:
        # Run the tracker script as a subprocess
        # We allow it to print directly to sys.stderr so it shows up in journalctl logs in real-time
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_no],
            capture_output=True, # We still capture for JSON parsing, but let's print stderr live
            text=True,
            timeout=SCRAPER_TIMEOUT,
            cwd=SCRIPT_DIR
        )
        # Print the stderr (Navigating..., etc.) so we can see progress in journalctl
        if result.stderr:
            print(result.stderr, file=sys.stderr)
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Wan Hai tracker timed out"}

    if result.returncode != 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": f"Tracker failed: {result.stderr[:500]}"}

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": "No JSON output from tracker"}

    try:
        raw_data = json.loads(output[json_start:])
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}"}

    if raw_data.get("status") == "error":
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": raw_data.get("error")}

    data = raw_data.get("data", {})

    # --- STEP 2: Post Departed event ---
    events_posted = 0
    status_details = job_data.get("status_details", {})
    departed_value = data.get("departed_value", "")

    if departed_value and get_rank(api_status) <= get_rank("Departed"):
        existing_val = status_details.get("Departed", {}).get("value")
        data_changed = str(departed_value) != str(existing_val)
        # Force status update when portal status is behind
        is_changed = True if get_rank(api_status) < get_rank("Departed") else data_changed
        
        if shared_utils.post_event(container_no, "Departed", "", departed_value, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [Departed] {departed_value} (Changed: {data_changed}, StatusForce: {is_changed})", flush=True)

    # --- STEP 3: Post ETA event ---
    eta_date = data.get("eta_date", "")
    eta_value = data.get("eta_value", "")

    if eta_date and get_rank(api_status) < get_rank("Inward"):
        existing_eta = status_details.get("ETA", {}).get("date")
        existing_val = status_details.get("ETA", {}).get("value")
        data_changed = str(eta_date) != str(existing_eta) or str(eta_value) != str(existing_val)
        # Force status update when portal status is behind
        is_changed = True if get_rank(api_status) < get_rank("ETA") else data_changed
        
        if shared_utils.post_event(container_no, "ETA", eta_date, eta_value, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [ETA] {eta_date} — {eta_value} (Changed: {data_changed}, StatusForce: {is_changed})")

    if events_posted == 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)

    return {"success": True, "events_posted": events_posted}


def worker_process(worker_num):
    worker_id = f"worker-{worker_num}-pid{os.getpid()}"
    r = get_redis()

    r.hset(WORKERS_KEY, worker_id, json.dumps({
        "started_at": datetime.now().isoformat(),
        "status": "idle"
    }))

    print(f"  [{worker_id}] Started", flush=True)

    try:
        while True:
            try:
                job_raw = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)

                if not job_raw:
                    continue

                _, payload = job_raw
                job_data = json.loads(payload)
                container_no = job_data.get("container_no", "UNKNOWN")

                r.hset(WORKERS_KEY, worker_id, json.dumps({
                    "status": "processing",
                    "container": container_no,
                    "since": datetime.now().isoformat()
                }))

                r.hset(PROCESSING_KEY, container_no, json.dumps({
                    **job_data,
                    "worker": worker_id,
                    "started_at": datetime.now().isoformat()
                }))

                try:
                    result = process_job(job_data)

                    if result.get("success"):
                        r.hdel(PROCESSING_KEY, container_no)
                        r.hdel(RETRY_KEY, container_no)
                        r.incr(COMPLETED_KEY)
                        print(f"  [{worker_id}] [✓] {container_no} done (events={result.get('events_posted', 0)})", flush=True)
                    else:
                        r.hdel(PROCESSING_KEY, container_no)
                        retry_count = int(r.hget(RETRY_KEY, container_no) or 0) + 1
                        r.hset(RETRY_KEY, container_no, retry_count)
                        
                        if retry_count >= MAX_RETRIES:
                            print(f"  [{worker_id}] [✗] {container_no} DROPPED after {retry_count} retries: {result.get('error')}", flush=True)
                            r.hdel(RETRY_KEY, container_no)
                            # Still update last_check_date so orchestrator knows we tried
                            shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
                        else:
                            delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                            print(f"  [{worker_id}] [↻] {container_no} failed ({retry_count}/{MAX_RETRIES}): {result.get('error')}. Postponing {delay}s", flush=True)
                            time.sleep(delay)
                            r.lpush(QUEUE_KEY, json.dumps(job_data))

                except Exception as e:
                    r.hdel(PROCESSING_KEY, container_no)
                    retry_count = int(r.hget(RETRY_KEY, container_no) or 0) + 1
                    r.hset(RETRY_KEY, container_no, retry_count)
                    if retry_count >= MAX_RETRIES:
                        print(f"  [{worker_id}] [✗] {container_no} DROPPED after {retry_count} crashes: {e}", flush=True)
                        r.hdel(RETRY_KEY, container_no)
                    else:
                        print(f"  [{worker_id}] [Crashed] {container_no} ({retry_count}/{MAX_RETRIES}): {e}", flush=True)
                        time.sleep(10)
                        r.lpush(QUEUE_KEY, json.dumps(job_data))

                time.sleep(SLEEP_BETWEEN_JOBS)

            except Exception as e:
                print(f"  [{worker_id}] Worker loop error: {e}", flush=True)
                time.sleep(10)

    finally:
        try:
            r.hdel(WORKERS_KEY, worker_id)
        except: pass


def supervisor():
    r = get_redis()
    print(f"\n{'='*60}")
    print(f"WAN HAI AGENT SUPERVISOR STARTED")
    print(f"{'='*60}")
    
    r.delete(WORKERS_KEY)
    workers = []
    
    for i in range(MAX_WORKERS):
        p = multiprocessing.Process(target=worker_process, args=(i+1,), daemon=True)
        p.start()
        workers.append(p)
        
    try:
        while True:
            time.sleep(SCALE_CHECK_INTERVAL)
            # Basic health check
            for i, p in enumerate(workers):
                if not p.is_alive():
                    print(f"  [SUPERVISOR] Worker {i+1} died, restarting...")
                    new_p = multiprocessing.Process(target=worker_process, args=(i+1,), daemon=True)
                    new_p.start()
                    workers[i] = new_p
    except KeyboardInterrupt:
        for p in workers: p.terminate()

if __name__ == "__main__":
    if "--single" in sys.argv:
        worker_process(1)
    else:
        supervisor()
