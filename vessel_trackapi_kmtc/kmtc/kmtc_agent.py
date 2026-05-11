"""
KMTC Agent Worker — Redis Queue Consumer with Auto-Scaling

Features:
  - Supervisor process monitors queue depth and auto-scales workers
  - Workers pull jobs from tc:queue:kmtc via BRPOP
  - Runs vessel_trackapi_kmtc/kmtc/tracker_worker.py as subprocess (Selenium)
  - POSTs Departed and ETA events to Portal API
  - If ETA is missing from KMTC, falls back to Chennai API (port 1015)
  - Auto-retry with exponential backoff on failures

Run:  python kmtc_agent.py
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
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import shared_utils

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "tracker_worker.py")

# Chennai ETA fallback API (vessel schedule lookup)
CHENNAI_API_URL = os.environ.get("CHENNAI_API_URL", "http://localhost:1015/chennai")

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
QUEUE_KEY = "tc:queue:kmtc"
PROCESSING_KEY = "tc:processing:kmtc"
FAILED_KEY = "tc:failed:kmtc"
COMPLETED_KEY = "tc:completed:kmtc"
RETRY_KEY = "tc:retries:kmtc"
WORKERS_KEY = "tc:workers:kmtc"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 180      # 3 minutes max for Selenium
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




def fetch_eta_from_chennai(vessel_name):
    """
    Fallback: fetch ETA from Chennai Port schedule API.
    The Chennai API accepts vessel_name and returns ETA from port PDFs.
    """
    if not vessel_name:
        return None

    print(f"      [Chennai Fallback] Looking up ETA for vessel: {vessel_name}")
    try:
        response = requests.get(
            CHENNAI_API_URL,
            params={"vessel_name": vessel_name},
            timeout=60
        )
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                # Prefer final_eta > revised_eta_date > eta_date
                eta = data.get("final_eta") or data.get("revised_eta_date") or data.get("eta_date")
                if eta:
                    print(f"      [Chennai Fallback] Found ETA: {eta} (source: {data.get('source', 'Chennai')})")
                    return eta
                else:
                    print(f"      [Chennai Fallback] Success response but no ETA date found")
            else:
                print(f"      [Chennai Fallback] Not found: {data.get('message', 'Unknown')}")
        else:
            print(f"      [Chennai Fallback] HTTP {response.status_code}")
    except Exception as e:
        print(f"      [Chennai Fallback] Error: {e}")

    return None


def process_job(job_data):
    """
    Process a single KMTC tracking job:
    1. Run tracker_worker.py subprocess (Selenium) to scrape eKMTC
    2. Extract departure_value → Departed event
    3. Extract eta_date → ETA event
    4. If ETA missing from KMTC, fallback to Chennai API using vessel_name
    """
    container_no = job_data.get("container_no", "UNKNOWN")
    api_status = job_data.get("status", "")
    vessel_name = job_data.get("vessel_name", "")

    # --- STEP 1: Run KMTC tracker subprocess ---
    print(f"    [KMTC] Tracking {container_no}...")
    try:
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=SCRAPER_TIMEOUT,
            cwd=SCRIPT_DIR
        )
    except subprocess.TimeoutExpired:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": "KMTC tracker timed out"}

    if result.returncode != 0:
        # Still update last_check_date even on failure
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": f"Tracker failed: {result.stderr[:500]}"}

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": "No JSON output from tracker"}

    # Extract only the JSON object (tracker prints debug lines + banners around it)
    # Find the matching closing brace by counting nesting depth
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
    if data.get("error"):
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": True, "events_posted": 0, "error": data["error"]}

    # --- STEP 2: Post Departed event ---
    events_posted = 0
    status_details = job_data.get("status_details", {})
    departure_value = data.get("departure_value", "")
    departure_date = data.get("departure_date", "")

    if departure_value and get_rank(api_status) < get_rank("Departed"):
        existing_dep = status_details.get("Departed", {}).get("date")
        existing_val = status_details.get("Departed", {}).get("value")
        data_changed = str(departure_date) != str(existing_dep) or str(departure_value) != str(existing_val)
        # ALWAYS force status change when portal status is behind — fixes "Created" badge stuck
        is_changed = True if get_rank(api_status) < get_rank("Departed") else data_changed
        
        if shared_utils.post_event(container_no, "Departed", departure_date, departure_value, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [Departed] {departure_date} - {departure_value} (Changed: {data_changed}, StatusForce: {is_changed})", flush=True)

    # --- STEP 3: Post ETA event ---
    eta_date = data.get("eta_date", "")
    eta_value = data.get("eta_value", "")

    # If KMTC didn't return ETA, try Chennai API fallback
    if not eta_date and vessel_name:
        print(f"      [KMTC] No ETA from eKMTC — trying Chennai fallback...")
        chennai_eta = fetch_eta_from_chennai(vessel_name)
        if chennai_eta:
            eta_date = chennai_eta
            eta_value = eta_value or "Chennai Port Schedule"

    if eta_date and shared_utils.get_rank(api_status) < shared_utils.get_rank("Inward"):
        existing_eta = status_details.get("ETA", {}).get("date")
        existing_val = status_details.get("ETA", {}).get("value")
        data_changed = str(eta_date) != str(existing_eta) or str(eta_value) != str(existing_val)
        # ALWAYS force status change when portal status is behind — fixes "Created" badge stuck
        is_changed = True if shared_utils.get_rank(api_status) < shared_utils.get_rank("ETA") else data_changed
        
        if shared_utils.post_event(container_no, "ETA", eta_date, eta_value, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [ETA] {eta_date} — {eta_value} (Changed: {data_changed}, StatusForce: {is_changed})")

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
                print(f"  [{worker_id}] BRPOP waiting...", flush=True)
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
    print(f"KMTC AGENT SUPERVISOR STARTED (Auto-Scaling)")
    print(f"{'='*60}")
    print(f"  Redis         : {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Queue         : {QUEUE_KEY}")
    print(f"  API           : {API_BASE_URL}")
    print(f"  Tracker       : {TRACKER_SCRIPT}")
    print(f"  Python        : {PYTHON_EXE}")
    print(f"  Chennai API   : {CHENNAI_API_URL}")
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
