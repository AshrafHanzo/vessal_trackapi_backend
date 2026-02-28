"""
DP World CFS Agent Worker — Redis Queue Consumer with Auto-Scaling

Features:
  - Supervisor process monitors queue depth and auto-scales workers
  - Workers pull jobs from tc:queue:dpw via BRPOP
  - Runs vessal_trackapi_csf_dpworld/dpw_tracker.py as subprocess (Playwright)
  - POSTs CFS In and CFS Out events to Portal API
  - Auto-retry with exponential backoff on failures

Run:  python dpw_agent.py
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

# Add parent dir to path for shared modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cfs_lookup import resolve_cfs_name


# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "dpw_tracker.py")

# Detect Python: prefer local venv, fallback to current
VENV_PYTHON_WIN = os.path.join(SCRIPT_DIR, "venv", "Scripts", "python.exe")
VENV_PYTHON_LINUX = os.path.join(SCRIPT_DIR, "venv", "bin", "python")
if os.path.exists(VENV_PYTHON_WIN):
    PYTHON_EXE = VENV_PYTHON_WIN
elif os.path.exists(VENV_PYTHON_LINUX):
    PYTHON_EXE = VENV_PYTHON_LINUX
else:
    PYTHON_EXE = sys.executable

# Redis Keys
QUEUE_KEY = "tc:queue:dpw"
PROCESSING_KEY = "tc:processing:dpw"
FAILED_KEY = "tc:failed:dpw"
COMPLETED_KEY = "tc:completed:dpw"
RETRY_KEY = "tc:retries:dpw"
WORKERS_KEY = "tc:workers:dpw"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 180
SLEEP_BETWEEN_JOBS = 2

# Retry Config — infinite retries, websites can be down for days
RETRY_DELAY_CAP = 600  # Max 10 min between retries
RETRY_DELAYS = [30, 60, 120, 300, 600]

# Auto-Scaling Config
CPU_CORES = os.cpu_count() or 4
MIN_WORKERS = 1
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", CPU_CORES * 2))
CONTAINERS_PER_WORKER = 2
SCALE_CHECK_INTERVAL = 10

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


def post_event(container_no, status, date, value, cfs_code=None, cfs_name=None, is_status_changed=True):
    payload = {
        "container_no": container_no,
        "status": status if status else "",
        "date": date if date else "",
        "value": value or "",
        "cfs_code": cfs_code or "",
        "cfs_name": cfs_name or "",
        "last_check_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "is_status_changed": is_status_changed
    }
    try:
        response = requests.post(f"{API_BASE_URL}/shipment-timeline", json=payload, timeout=30)
        return response.status_code in [200, 201]
    except Exception as e:
        print(f"      Post error: {e}")
        return False


def sync_cfs_details(container_no, cfs_code, cfs_name):
    """Sync CFS code and name to job-level fields via sync-job-details API."""
    if not cfs_code and not cfs_name:
        return False
    payload = {
        "container_no": container_no.strip(),
        "cfs_code": cfs_code or "",
        "cfs_name": cfs_name or ""
    }
    try:
        response = requests.post(f"{API_BASE_URL}/sync-job-details", json=payload, timeout=30)
        if response.status_code in [200, 201]:
            print(f"      [SYNC] CFS details synced: code={cfs_code}, name={cfs_name}")
            return True
        else:
            print(f"      [SYNC] Failed ({response.status_code}): {response.text[:200]}")
            return False
    except Exception as e:
        print(f"      [SYNC] Error: {e}")
        return False



def process_job(job_data):
    """
    Process a DP World CFS tracking job:
    1. Run dpw_tracker.py subprocess
    2. Extract CFS In (cfs_in_time) and CFS Out (cfs_out_time) dates
    3. Post events to Portal API
    """
    container_no = job_data.get("container_no", "UNKNOWN")
    api_status = job_data.get("status", "")

    try:
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_no],
            capture_output=True, text=True,
            timeout=SCRAPER_TIMEOUT, cwd=SCRIPT_DIR
        )
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "DPW tracker timed out"}

    if result.returncode != 0:
        return {"success": False, "error": f"Tracker failed: {result.stderr[:500]}"}

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        return {"success": False, "error": "No JSON output"}

    try:
        data = json.loads(output[json_start:])
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}"}

    result_data = data.get("data", data)
    cfs_in = result_data.get("cfs_in_time")
    cfs_out = result_data.get("cfs_out_time")
    scraped_code = result_data.get("cfs_code")  # "CFS Name" from DPWorld (used as code)

    # Resolve CFS code to name via fuzzy lookup
    matched_code, matched_name = resolve_cfs_name(scraped_code)
    cfs_display = "DPW"  # Website source name for Port In/Port Out value
    print(f"    [CFS LOOKUP] scraped={scraped_code} -> code={matched_code}, name={matched_name}")

    events_posted = 0

    # Post Port In / Port Out events (At Port section in UI)
    if cfs_in and get_rank(api_status) < get_rank("Port In"):
        if post_event(container_no, "Port In", cfs_in, cfs_display, cfs_code=matched_code, cfs_name=matched_name):
            events_posted += 1
            print(f"    [MATCH] Port In = {cfs_in}")

    if cfs_out and get_rank(api_status) < get_rank("Port Out"):
        if post_event(container_no, "Port Out", cfs_out, cfs_display, cfs_code=matched_code, cfs_name=matched_name):
            events_posted += 1
            print(f"    [MATCH] Port Out = {cfs_out}")

    # Post CFS In / CFS Out events (Customs section in UI — CFS Code & CFS Name)
    if matched_code and get_rank(api_status) < get_rank("CFS In"):
        if post_event(container_no, "CFS In", cfs_in or cfs_out or "", matched_code):
            events_posted += 1
            print(f"    [MATCH] CFS In (CFS Code) = {matched_code}")

    if matched_name and get_rank(api_status) < get_rank("CFS Out"):
        if post_event(container_no, "CFS Out", cfs_in or cfs_out or "", matched_name):
            events_posted += 1
            print(f"    [MATCH] CFS Out (CFS Name) = {matched_name}")

    # If nothing new found, still post no-change to update last_check_date
    if events_posted == 0:
        post_event(container_no, "", "", "", is_status_changed=False)

    return {"success": True, "events_posted": events_posted}



# ==========================================
# WORKER PROCESS
# ==========================================
def worker_process(worker_num):
    worker_id = f"worker-{worker_num}-pid{os.getpid()}"
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
                        print(f"  [{worker_id}] [✓] {container_no} done (events={result.get('events_posted', 0)})")
                    else:
                        r.hdel(PROCESSING_KEY, container_no)
                        retry_count = int(r.hget(RETRY_KEY, container_no) or 0) + 1
                        delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                        r.hset(RETRY_KEY, container_no, retry_count)
                        print(f"  [{worker_id}] [↻] {container_no} failed (attempt {retry_count}): {result.get('error')}")
                        time.sleep(delay)
                        r.lpush(QUEUE_KEY, json.dumps(job_data))
                except Exception as e:
                    r.hdel(PROCESSING_KEY, container_no)
                    retry_count = int(r.hget(RETRY_KEY, container_no) or 0) + 1
                    delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                    r.hset(RETRY_KEY, container_no, retry_count)
                    print(f"  [{worker_id}] [↻] {container_no} crashed (attempt {retry_count}): {e}")
                    time.sleep(delay)
                    r.lpush(QUEUE_KEY, json.dumps(job_data))

                time.sleep(SLEEP_BETWEEN_JOBS)

            except redis.ConnectionError as e:
                print(f"  [{worker_id}] Redis connection lost: {e}")
                time.sleep(10); r = get_redis()
            except Exception as e:
                print(f"  [{worker_id}] Unexpected error: {e}")
                traceback.print_exc(); time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        try: r.hdel(WORKERS_KEY, worker_id)
        except: pass
        print(f"  [{worker_id}] Stopped")


# ==========================================
# SUPERVISOR (AUTO-SCALING)
# ==========================================
def calculate_target_workers(queue_length):
    if queue_length == 0: return MIN_WORKERS
    return max(MIN_WORKERS, min(math.ceil(queue_length / CONTAINERS_PER_WORKER), MAX_WORKERS))


def supervisor():
    r = get_redis()
    print(f"\n{'='*60}")
    print(f"DP WORLD CFS AGENT SUPERVISOR STARTED (Auto-Scaling)")
    print(f"{'='*60}")
    print(f"  Redis: {REDIS_HOST}:{REDIS_PORT} | Queue: {QUEUE_KEY}")
    print(f"  Tracker: {TRACKER_SCRIPT} | Python: {PYTHON_EXE}")
    print(f"  Workers: {MIN_WORKERS}-{MAX_WORKERS} | Per worker: {CONTAINERS_PER_WORKER}")
    print(f"  Time: {datetime.now()}")
    print(f"{'='*60}")

    r.delete(WORKERS_KEY)
    workers = []
    next_worker_num = 1

    def spawn_worker():
        nonlocal next_worker_num
        p = multiprocessing.Process(target=worker_process, args=(next_worker_num,), daemon=True)
        p.start()
        workers.append((p, next_worker_num))
        print(f"  [SUPERVISOR] Spawned worker-{next_worker_num} (pid={p.pid})")
        next_worker_num += 1

    def remove_dead_workers():
        alive = []
        for p, num in workers:
            if p.is_alive(): alive.append((p, num))
            else: print(f"  [SUPERVISOR] Worker-{num} exited, cleaning up")
        return alive

    for _ in range(MIN_WORKERS): spawn_worker()

    try:
        while True:
            time.sleep(SCALE_CHECK_INTERVAL)
            workers[:] = remove_dead_workers()
            try:
                queue_len = r.llen(QUEUE_KEY)
                processing_count = r.hlen(PROCESSING_KEY)
                completed = r.get(COMPLETED_KEY) or "0"
                failed_count = r.llen(FAILED_KEY)
            except redis.ConnectionError:
                time.sleep(5); r = get_redis(); continue

            current_workers = len(workers)
            target = calculate_target_workers(queue_len)
            print(f"  [SUPERVISOR] Queue={queue_len} | Processing={processing_count} | Workers={current_workers}→{target} | Completed={completed} | Failed={failed_count}")

            if target > current_workers:
                for _ in range(target - current_workers): spawn_worker()
            elif target < current_workers and queue_len == 0:
                for _ in range(current_workers - target):
                    if len(workers) > MIN_WORKERS:
                        p, num = workers.pop(); p.terminate(); p.join(timeout=5)
                        print(f"  [SUPERVISOR] Terminated worker-{num}")
    except KeyboardInterrupt:
        print(f"\n  [SUPERVISOR] Shutting down...")
        for p, num in workers: p.terminate()
        for p, num in workers: p.join(timeout=10)
        try: r.delete(WORKERS_KEY)
        except: pass
        print(f"  [SUPERVISOR] All workers stopped. Goodbye!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--single": worker_process(1)
    else: supervisor()
