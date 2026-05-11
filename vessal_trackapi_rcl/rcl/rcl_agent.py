"""
RCL Agent Worker — Redis Queue Consumer
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

# Add parent dirs to path for shared modules
# Works whether at rcl/rcl_agent.py or vessal_trackapi_rcl/rcl/rcl_agent.py
_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_this_dir))                    # rcl/ -> root
sys.path.insert(0, os.path.dirname(os.path.dirname(_this_dir)))   # vessal_trackapi_rcl/rcl/ -> root
import shared_utils

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = shared_utils.REDIS_HOST
REDIS_PORT = shared_utils.REDIS_PORT
REDIS_PASSWORD = shared_utils.REDIS_PASSWORD

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "tracker_worker.py")

# Detect Python
PYTHON_EXE = sys.executable

# Redis Keys
QUEUE_KEY = "tc:queue:rcl"
PROCESSING_KEY = "tc:processing:rcl"
COMPLETED_KEY = "tc:completed:rcl"
RETRY_KEY = "tc:retries:rcl"
WORKERS_KEY = "tc:workers:rcl"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 300 
SLEEP_BETWEEN_JOBS = 5

RETRY_DELAYS = [30, 60, 120, 300, 600]

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)


def process_job(job_data):
    container_no = job_data.get("container_no", "UNKNOWN")
    api_status = job_data.get("status", "")

    try:
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_no],
            capture_output=True, text=True,
            timeout=SCRAPER_TIMEOUT, cwd=SCRIPT_DIR
        )
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "RCL tracker timed out"}

    if result.returncode != 0:
        return {"success": False, "error": f"Tracker failed: {result.stderr[:500]}"}

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        return {"success": False, "error": f"No JSON output. Raw: {output[:200]}"}

    # Extract JSON using brace-depth matching (handles trailing Chrome/Selenium garbage)
    depth = 0
    json_end = json_start
    for i in range(json_start, len(output)):
        if output[i] == '{':
            depth += 1
        elif output[i] == '}':
            depth -= 1
            if depth == 0:
                json_end = i + 1
                break

    try:
        data = json.loads(output[json_start:json_end])
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}"}
    
    # RCL Format:
    # {
    #     "container_no": "...",
    #     "departed_value": "...",
    #     "departed_date": "...",
    #     "eta_date": "...",
    #     "eta_value": "..."
    # }
    
    dep_val = data.get("departed_value")
    dep_date = data.get("departed_date")
    eta_val = data.get("eta_value")
    eta_date = data.get("eta_date")

    events_posted = 0
    status_details = job_data.get("status_details", {})

    # Post Departed event
    if dep_date and dep_date != "N/A" and shared_utils.get_rank(api_status) < shared_utils.get_rank("Departed"):
        existing_dep = status_details.get("Departed", {}).get("date")
        existing_val = status_details.get("Departed", {}).get("value")
        data_changed = str(dep_date) != str(existing_dep) or str(dep_val) != str(existing_val)
        is_changed = True if shared_utils.get_rank(api_status) < shared_utils.get_rank("Departed") else data_changed
        
        if shared_utils.post_event(container_no, "Departed", dep_date, dep_val, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"    [MATCH] Departed = {dep_date} at {dep_val} (Changed: {data_changed}, StatusForce: {is_changed})")

    # Post ETA event
    if eta_date and eta_date != "N/A" and shared_utils.get_rank(api_status) < shared_utils.get_rank("Inward"):
        existing_eta = status_details.get("ETA", {}).get("date")
        existing_val = status_details.get("ETA", {}).get("value")
        data_changed = str(eta_date) != str(existing_eta) or str(eta_val) != str(existing_val)
        is_changed = True if shared_utils.get_rank(api_status) < shared_utils.get_rank("ETA") else data_changed
        
        if shared_utils.post_event(container_no, "ETA", eta_date, eta_val, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"    [MATCH] ETA = {eta_date} (Changed: {data_changed}, StatusForce: {is_changed})")

    # Update last check date anyway
    if events_posted == 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)

    return {"success": True, "events_posted": events_posted}

def worker_process(worker_num):
    worker_id = f"rcl-worker-{worker_num}-pid{os.getpid()}"
    r = get_redis()
    r.hset(WORKERS_KEY, worker_id, json.dumps({"started_at": datetime.now().isoformat(), "status": "idle"}))
    print(f"  [{worker_id}] Started")

    try:
        while True:
            try:
                job_raw = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)
                if not job_raw:
                    r.hset(WORKERS_KEY, worker_id, json.dumps({"started_at": datetime.now().isoformat(), "status": "idle"}))
                    continue

                _, payload = job_raw
                job_data = json.loads(payload)
                container_no = job_data.get("container_no", "UNKNOWN")

                r.hset(WORKERS_KEY, worker_id, json.dumps({"status": "processing", "container": container_no, "since": datetime.now().isoformat()}))
                r.hset(PROCESSING_KEY, container_no, json.dumps({**job_data, "worker": worker_id, "started_at": datetime.now().isoformat()}))

                try:
                    result = process_job(job_data)
                    if result.get("success"):
                        r.hdel(PROCESSING_KEY, container_no)
                        r.hdel(RETRY_KEY, container_no)
                        r.incr(COMPLETED_KEY)
                        print(f"  [{worker_id}] [OK] {container_no} done (events={result.get('events_posted', 0)})")
                    else:
                        r.hdel(PROCESSING_KEY, container_no)
                        retry_count = int(r.hget(RETRY_KEY, container_no) or 0) + 1
                        delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                        r.hset(RETRY_KEY, container_no, retry_count)
                        print(f"  [{worker_id}] [RETRY] {container_no} failed (attempt {retry_count}): {result.get('error')}")
                        time.sleep(delay)
                        r.lpush(QUEUE_KEY, json.dumps(job_data))
                except Exception as e:
                    r.hdel(PROCESSING_KEY, container_no)
                    print(f"  [{worker_id}] [!] Crash: {e}")
                    r.lpush(QUEUE_KEY, json.dumps(job_data))

                time.sleep(SLEEP_BETWEEN_JOBS)

            except redis.ConnectionError:
                time.sleep(10); r = get_redis()
            except Exception as e:
                print(f"  [{worker_id}] Error: {e}")
                time.sleep(5)
    finally:
        try: r.hdel(WORKERS_KEY, worker_id)
        except: pass

if __name__ == "__main__":
    print(f"RCL AGENT STARTING (Redis: {REDIS_HOST}:{REDIS_PORT})")
    worker_process(1)
