"""
Cosco Agent Worker — Redis Queue Consumer
"""

import redis
import json
import subprocess
import requests
import time
import sys
import os
import traceback
from datetime import datetime

# Add parent dir to path for shared modules
# Structure: cosco/cosco_agent.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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

PYTHON_EXE = sys.executable

QUEUE_KEY = "tc:queue:cosco"
PROCESSING_KEY = "tc:processing:cosco"
COMPLETED_KEY = "tc:completed:cosco"
RETRY_KEY = "tc:retries:cosco"
WORKERS_KEY = "tc:workers:cosco"

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
        return {"success": False, "error": "Cosco tracker timed out"}

    if result.returncode != 0:
        return {"success": False, "error": f"Tracker failed: {result.stderr[:500]}"}

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        return {"success": False, "error": f"No JSON output. Raw: {output[:200]}"}

    try:
        data = json.loads(output[json_start:])
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}"}
    
    # Cosco Format:
    # {
    #   "Departed_value": "...",
    #   "Departed date": "...",
    #   "Eta_value": "...",
    #   "Eta_date": "..."
    # }
    
    dep_val = data.get("Departed_value")
    dep_date = data.get("Departed date")
    eta_val = data.get("Eta_value")
    eta_date = data.get("Eta_date")

    events_posted = 0
    status_details = job_data.get("status_details", {})

    if dep_date and shared_utils.get_rank(api_status) < shared_utils.get_rank("Departed"):
        existing_dep = status_details.get("Departed", {}).get("date")
        data_changed = str(dep_date) != str(existing_dep)
        is_changed = True if shared_utils.get_rank(api_status) < shared_utils.get_rank("Departed") else data_changed
        
        if shared_utils.post_event(container_no, "Departed", dep_date, dep_val, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"    [MATCH] Departed = {dep_date} (Changed: {data_changed}, StatusForce: {is_changed})")

    if eta_date and shared_utils.get_rank(api_status) < shared_utils.get_rank("Inward"):
        existing_eta = status_details.get("ETA", {}).get("date")
        data_changed = str(eta_date) != str(existing_eta)
        is_changed = True if shared_utils.get_rank(api_status) < shared_utils.get_rank("ETA") else data_changed
        
        if shared_utils.post_event(container_no, "ETA", eta_date, eta_val, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"    [MATCH] ETA = {eta_date} (Changed: {data_changed}, StatusForce: {is_changed})")

    if events_posted == 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)

    return {"success": True, "events_posted": events_posted}

def worker_process(worker_num):
    worker_id = f"cosco-worker-{worker_num}-pid{os.getpid()}"
    r = get_redis()
    print(f"  [{worker_id}] Started")

    while True:
        try:
            job_raw = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)
            if not job_raw: continue

            _, payload = job_raw
            job_data = json.loads(payload)
            container_no = job_data.get("container_no", "UNKNOWN")

            try:
                result = process_job(job_data)
                if result.get("success"):
                    print(f"  [{worker_id}] [OK] {container_no} done")
                else:
                    print(f"  [{worker_id}] [RETRY] {container_no} failed: {result.get('error')}")
                    r.lpush(QUEUE_KEY, json.dumps(job_data))
            except Exception as e:
                print(f"  [{worker_id}] [!] Crash: {e}")
                r.lpush(QUEUE_KEY, json.dumps(job_data))

            time.sleep(SLEEP_BETWEEN_JOBS)
        except redis.ConnectionError:
            time.sleep(10); r = get_redis()
        except Exception as e:
            time.sleep(5)

if __name__ == "__main__":
    print(f"COSCO AGENT STARTING (Redis: {REDIS_HOST}:{REDIS_PORT})")
    worker_process(1)
