"""
InterAsia Agent Worker — Redis Queue Consumer with Auto-Scaling

Features:
  - Supervisor process monitors queue depth and auto-scales workers
  - Workers pull jobs from tc:queue:interasia via BRPOP
  - Runs interasia/tracker_worker.py as subprocess (Playwright)
  - POSTs Departed and ETA events to Portal API
  - Auto-retry with exponential backoff on failures

Run:  python interasia_agent.py
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
VENV_PYTHON_WIN = os.path.join(SCRIPT_DIR, "venv", "Scripts", "python.exe")
VENV_PYTHON_LINUX = os.path.join(SCRIPT_DIR, "venv", "bin", "python")
if os.path.exists(VENV_PYTHON_WIN):
    PYTHON_EXE = VENV_PYTHON_WIN
elif os.path.exists(VENV_PYTHON_LINUX):
    PYTHON_EXE = VENV_PYTHON_LINUX
else:
    PYTHON_EXE = sys.executable

# Redis Keys
QUEUE_KEY = "tc:queue:interasia"
PROCESSING_KEY = "tc:processing:interasia"
FAILED_KEY = "tc:failed:interasia"
COMPLETED_KEY = "tc:completed:interasia"
RETRY_KEY = "tc:retries:interasia"
WORKERS_KEY = "tc:workers:interasia"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 180      # 3 minutes max for Playwright
SLEEP_BETWEEN_JOBS = 2

# Retry Config
RETRY_DELAYS = [30, 60, 120, 300, 600]

# Auto-Scaling Config
CPU_CORES = os.cpu_count() or 4
MIN_WORKERS = 1
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", CPU_CORES * 2))
CONTAINERS_PER_WORKER = 2
SCALE_CHECK_INTERVAL = 10

# Status Hierarchy (same as main_orchestrator)
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
    Process a single InterAsia tracking job:
    1. Run tracker_worker.py subprocess (Playwright)
    2. Extract Departed_value → Departed event (no date)
    3. Extract Eta_date/Eta_value → ETA event
    """
    container_no = job_data.get("container_no", "UNKNOWN")
    api_status = job_data.get("status", "")

    # --- STEP 1: Run InterAsia tracker subprocess ---
    print(f"    [INTERASIA] Tracking {container_no}...", flush=True)
    try:
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=SCRAPER_TIMEOUT,
            cwd=SCRIPT_DIR
        )
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "InterAsia tracker timed out"}

    if result.returncode != 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": f"Tracker failed: {result.stderr[:500]}"}

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": "No JSON output from tracker"}

    # Extract JSON object using brace-depth matching
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

    json_str = output[json_start:json_end]

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}"}

    # Check for tracker errors
    if data.get("status") == "error" or data.get("status") == "not_found":
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": data.get("message", "Tracker returned error")}

    # --- STEP 2: Post Departed event (no date) ---
    events_posted = 0
    status_details = job_data.get("status_details", {})
    departure_value = data.get("Departed_value", "")

    if departure_value and get_rank(api_status) < get_rank("Departed"):
        existing_val = status_details.get("Departed", {}).get("value")
        data_changed = str(departure_value) != str(existing_val)
        is_changed = True if get_rank(api_status) < get_rank("Departed") else data_changed
        
        if shared_utils.post_event(container_no, "Departed", "", departure_value, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [Departed] {departure_value} (Changed: {data_changed}, StatusForce: {is_changed})", flush=True)

    # --- STEP 3: Post ETA event ---
    eta_date = data.get("Eta_date", "")
    eta_value = data.get("Eta_value", "")

    if eta_date and get_rank(api_status) < get_rank("Inward"):
        existing_eta = status_details.get("ETA", {}).get("date")
        existing_val = status_details.get("ETA", {}).get("value")
        data_changed = str(eta_date) != str(existing_eta) or str(eta_value) != str(existing_val)
        is_changed = True if get_rank(api_status) < get_rank("ETA") else data_changed
        
        if shared_utils.post_event(container_no, "ETA", eta_date, eta_value, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [ETA] {eta_date} — {eta_value} (Changed: {data_changed}, StatusForce: {is_changed})", flush=True)

    # If nothing was posted, still update last_check_date
    if events_posted == 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)

    return {"success": True, "events_posted": events_posted}


# ==========================================
# WORKER PROCESS
# ==========================================
def worker_process(worker_num):
    """Single worker process — pulls jobs from Redis queue and processes them."""
    worker_id = f"worker-{worker_num}-pid{os.getpid()}"

    print(f"  [{worker_id}] Initializing...", flush=True)

    try:
        r = get_redis()
        r.ping()
        print(f"  [{worker_id}] Redis connected OK", flush=True)
    except Exception as e:
        print(f"  [{worker_id}] FATAL: Cannot connect to Redis: {e}", flush=True)
        return

    try:
        r.hset(WORKERS_KEY, worker_id, json.dumps({
            "started_at": datetime.now().isoformat(),
            "status": "idle"
        }))
    except Exception as e:
        print(f"  [{worker_id}] FATAL: Cannot write to Redis: {e}", flush=True)
        return

    print(f"  [{worker_id}] Started — waiting for jobs on {QUEUE_KEY}", flush=True)

    try:
        while True:
            try:
                job_raw = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)

                if not job_raw:
                    r.hset(WORKERS_KEY, worker_id, json.dumps({
                        "started_at": datetime.now().isoformat(),
                        "status": "idle"
                    }))
                    continue

                _, payload = job_raw
                job_data = json.loads(payload)
                container_no = job_data.get("container_no", "UNKNOWN")
                print(f"  [{worker_id}] Picked up job: {container_no}", flush=True)

                r.hset(WORKERS_KEY, worker_id, json.dumps({
                    "status": "processing",
                    "container": container_no,
                    "since": datetime.now().isoformat()
                }))

                processing_entry = json.dumps({
                    **job_data,
                    "worker": worker_id,
                    "started_at": datetime.now().isoformat()
                })
                r.hset(PROCESSING_KEY, container_no, processing_entry)

                try:
                    result = process_job(job_data)

                    if result.get("success"):
                        r.hdel(PROCESSING_KEY, container_no)
                        r.hdel(RETRY_KEY, container_no)
                        r.incr(COMPLETED_KEY)
                        print(f"  [{worker_id}] [✓] {container_no} done (events={result.get('events_posted', 0)})", flush=True)
                    else:
                        r.hdel(PROCESSING_KEY, container_no)
                        retry_count = int(r.hget(RETRY_KEY, container_no) or 0)
                        retry_count += 1

                        delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                        r.hset(RETRY_KEY, container_no, retry_count)
                        print(f"  [{worker_id}] [↻] {container_no} failed (attempt {retry_count}): {result.get('error')}", flush=True)
                        time.sleep(delay)
                        r.lpush(QUEUE_KEY, json.dumps(job_data))

                except Exception as e:
                    r.hdel(PROCESSING_KEY, container_no)
                    retry_count = int(r.hget(RETRY_KEY, container_no) or 0)
                    retry_count += 1

                    delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                    r.hset(RETRY_KEY, container_no, retry_count)
                    print(f"  [{worker_id}] [↻] {container_no} crashed (attempt {retry_count}): {e}", flush=True)
                    time.sleep(delay)
                    r.lpush(QUEUE_KEY, json.dumps(job_data))

                time.sleep(SLEEP_BETWEEN_JOBS)

            except redis.ConnectionError as e:
                print(f"  [{worker_id}] Redis connection lost: {e}", flush=True)
                time.sleep(10)
                r = get_redis()

            except Exception as e:
                print(f"  [{worker_id}] Unexpected error: {e}", flush=True)
                traceback.print_exc()
                sys.stdout.flush()
                sys.stderr.flush()
                time.sleep(5)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            r.hdel(WORKERS_KEY, worker_id)
        except Exception:
            pass
        print(f"  [{worker_id}] Stopped", flush=True)


# ==========================================
# SUPERVISOR (AUTO-SCALING)
# ==========================================
def calculate_target_workers(queue_length):
    if queue_length == 0:
        return MIN_WORKERS
    target = math.ceil(queue_length / CONTAINERS_PER_WORKER)
    return max(MIN_WORKERS, min(target, MAX_WORKERS))


def supervisor():
    """Supervisor process — monitors queue depth and auto-scales workers."""
    r = get_redis()

    print(f"\n{'='*60}")
    print(f"INTERASIA AGENT SUPERVISOR STARTED (Auto-Scaling)")
    print(f"{'='*60}")
    print(f"  Redis         : {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Queue         : {QUEUE_KEY}")
    print(f"  API           : {API_BASE_URL}")
    print(f"  Tracker       : {TRACKER_SCRIPT}")
    print(f"  Python        : {PYTHON_EXE}")
    print(f"  Min workers   : {MIN_WORKERS}")
    print(f"  Max workers   : {MAX_WORKERS}")
    print(f"  Per worker    : {CONTAINERS_PER_WORKER} containers")
    print(f"  Check every   : {SCALE_CHECK_INTERVAL}s")
    print(f"  Time          : {datetime.now()}")
    print(f"{'='*60}")

    r.delete(WORKERS_KEY)

    workers = []
    next_worker_num = 1

    def spawn_worker():
        nonlocal next_worker_num
        p = multiprocessing.Process(
            target=worker_process,
            args=(next_worker_num,),
            daemon=True
        )
        p.start()
        workers.append((p, next_worker_num))
        print(f"  [SUPERVISOR] Spawned worker-{next_worker_num} (pid={p.pid})")
        next_worker_num += 1

    def remove_dead_workers():
        alive = []
        for p, num in workers:
            if p.is_alive():
                alive.append((p, num))
            else:
                print(f"  [SUPERVISOR] Worker-{num} (pid={p.pid}) exited, cleaning up")
        return alive

    for _ in range(MIN_WORKERS):
        spawn_worker()

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
                print("  [SUPERVISOR] Redis connection lost, reconnecting...")
                time.sleep(5)
                r = get_redis()
                continue

            current_workers = len(workers)
            target = calculate_target_workers(queue_len)

            print(f"  [SUPERVISOR] Queue={queue_len} | Processing={processing_count} | "
                  f"Workers={current_workers}→{target} | "
                  f"Completed={completed} | Failed={failed_count}")

            if target > current_workers:
                to_add = target - current_workers
                print(f"  [SUPERVISOR] ⬆ Scaling UP: adding {to_add} worker(s)")
                for _ in range(to_add):
                    spawn_worker()

            elif target < current_workers and queue_len == 0:
                to_remove = current_workers - target
                print(f"  [SUPERVISOR] ⬇ Scaling DOWN: removing {to_remove} worker(s)")
                for _ in range(to_remove):
                    if len(workers) > MIN_WORKERS:
                        p, num = workers.pop()
                        p.terminate()
                        p.join(timeout=5)
                        print(f"  [SUPERVISOR] Terminated worker-{num}")

    except KeyboardInterrupt:
        print(f"\n  [SUPERVISOR] Shutting down all workers...")
        for p, num in workers:
            p.terminate()
        for p, num in workers:
            p.join(timeout=10)
        try:
            r.delete(WORKERS_KEY)
        except Exception:
            pass
        print(f"  [SUPERVISOR] All workers stopped. Goodbye!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--single":
        worker_process(1)
    else:
        supervisor()
