"""
Sealion Agent Worker — Redis Queue Consumer with Auto-Scaling

Features:
  - Supervisor process monitors queue depth and auto-scales workers
  - Workers pull jobs from tc:queue:sealion via BRPOP
  - Runs Sealion/tracker.py as subprocess (Playwright)
  - Processes output through GPT for structured parsing
  - POSTs Departed, ETA, Arrived at POD events to Portal API
  - Syncs vessel/voyage details via sync-job-details API
  - Auto-retry with exponential backoff on failures

Run:  python sealion_agent.py
"""

import redis
import json
import subprocess
import requests
import time
import sys
import os
import signal
import traceback
import multiprocessing
import re
import math
from datetime import datetime
from rapidfuzz import fuzz

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
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "tracker.py")

# Detect Python: prefer local venv, fallback to current
VENV_PYTHON_WIN = os.path.join(SCRIPT_DIR, "venv", "Scripts", "python.exe")
VENV_PYTHON_LINUX = os.path.join(SCRIPT_DIR, "venv", "bin", "python")
if os.path.exists(VENV_PYTHON_WIN):
    PYTHON_EXE = VENV_PYTHON_WIN
elif os.path.exists(VENV_PYTHON_LINUX):
    PYTHON_EXE = VENV_PYTHON_LINUX
else:
    PYTHON_EXE = sys.executable

# OpenAI logic has been completely removed in favor of fuzzy matching.

# Redis Keys
QUEUE_KEY = "tc:queue:sealion"
PROCESSING_KEY = "tc:processing:sealion"
FAILED_KEY = "tc:failed:sealion"
COMPLETED_KEY = "tc:completed:sealion"
RETRY_KEY = "tc:retries:sealion"
WORKERS_KEY = "tc:workers:sealion"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 180      # 3 minutes max for Playwright + GPT
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


def normalize_date(date_str):
    """Normalize date for comparison."""
    if not date_str:
        return None
    try:
        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S",
                     "%d %b %Y", "%d %B %Y", "%Y-%m-%d %H:%M:%S",
                     "%d %b %Y %H:%M", "%d %B %Y %H:%M"]:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return str(date_str).strip()




def sync_job_details(container_no, sealion_data, original_shipping_line):
    """Sync vessel/voyage/POL/POD details to the Portal API."""
    vessel_details = sealion_data.get("vessel_details") or {}
    c_type = sealion_data.get("container_type") or ""
    size = c_type[:50]

    payload = {
        "container_no": container_no.strip(),
        "size": size,
        "vessel_name": vessel_details.get("vessel", ""),
        "voyage_no": vessel_details.get("voyage", ""),
        "shipping_line": original_shipping_line,
        "pol": vessel_details.get("loading", ""),
        "pod": vessel_details.get("discharge", "")
    }

    try:
        response = requests.post(
            f"{API_BASE_URL}/sync-job-details",
            json=payload,
            timeout=30
        )
        if response.status_code in [200, 201]:
            print(f"      Sync Success: vessel={vessel_details.get('vessel')}, pod={vessel_details.get('discharge')}")
            return True
        else:
            print(f"      Sync Failed ({response.status_code}): {response.text[:200]}")
            return False
    except Exception as e:
        print(f"      Sync Error: {e}")
        return False


def is_fuzzy_match(text, keyword, threshold=80):
    if not text: return False
    return fuzz.partial_ratio(keyword.lower(), text.lower()) >= threshold

def extract_events_fuzzy(raw_data):
    """Parse raw tracking events into structured data using fuzzy matching."""
    events = raw_data.get("events", [])
    
    extracted = {
        "Departed Origin": None,
        "Departed Date": None,
        "Current Status": "ETA",
        "Status Date": None,
        "Arrived Location": None
    }
    
    now = datetime.now()
    
    for ev in events:
        ev_date = ev.get("date")
        ev_loc = ev.get("location")
        ev_desc = ev.get("event", "")
        
        # Check Departed (overwrites to ensure latest event like 'Vessel Departure' trumps 'Gate in')
        if (is_fuzzy_match(ev_desc, "depart") or is_fuzzy_match(ev_desc, "load") or is_fuzzy_match(ev_desc, "gate in") or is_fuzzy_match(ev_desc, "vessel departure")):
            extracted["Departed Date"] = ev_date
            extracted["Departed Origin"] = ev_loc
            
        # Check Arrival
        if is_fuzzy_match(ev_desc, "arriv") or is_fuzzy_match(ev_desc, "discharge") or is_fuzzy_match(ev_desc, "vessel arrived"):
            extracted["Status Date"] = ev_date
            extracted["Arrived Location"] = ev_loc
            extracted["Current Status"] = "ATA"
            
        # Check explicit ETA
        if is_fuzzy_match(ev_desc, "eta"):
            extracted["Status Date"] = ev_date
            extracted["Current Status"] = "ETA"
            
    # Downgrade ATA to ETA if the date is in the future
    if extracted["Current Status"] == "ATA" and extracted["Status Date"]:
        try:
            norm_date = normalize_date(extracted["Status Date"])
            if norm_date:
                dt = datetime.strptime(norm_date, "%Y-%m-%d")
                if dt > now:
                    extracted["Current Status"] = "ETA"
        except Exception:
            pass

    # Fallback to vessel details for ETA if not found in events
    if not extracted["Status Date"]:
        vessel_details = raw_data.get("vessel_details", {})
        vessels = raw_data.get("vessels", [])
        
        # Try to find ETA from vessels array first
        for v in vessels:
            if v.get("eta"):
                extracted["Status Date"] = v.get("eta")
                break
        
        if not extracted["Status Date"] and vessel_details.get("eta"):
            extracted["Status Date"] = vessel_details.get("eta")
            
    return extracted


def process_job(job_data):
    """
    Process a single Sealion tracking job:
    1. Run tracker.py subprocess to scrape data
    2. Process through GPT for structured parsing
    3. Match vessel details for correct destination
    4. Post events (Departed, ETA, Arrived) to Portal API
    5. Sync job details (vessel, voyage, POL, POD)
    """
    container_no = job_data.get("container_no", "UNKNOWN")
    api_status = job_data.get("status", "")
    status_details = job_data.get("status_details", {})

    # --- STEP 1: Run Sealion tracker subprocess ---
    try:
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_no],
            capture_output=True,
            text=True,
            timeout=SCRAPER_TIMEOUT,
            cwd=SCRIPT_DIR
        )
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Sealion tracker timed out"}

    if result.returncode != 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": f"Tracker failed: {result.stderr[:500]}"}

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": f"No JSON output from tracker"}

    try:
        raw_data = json.loads(output[json_start:])
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}"}

    # Check for known errors from tracker
    if raw_data.get("error"):
        error_msg = raw_data["error"]
        if error_msg in ["Shipping Line Unknown", "Incorrect Tracking Number"]:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            shared_utils.post_event(container_no, "Departed", now_str, "unable to track this container")
            return {"success": True, "events_posted": 1, "note": f"Error: {error_msg}"}
        return {"success": False, "error": error_msg}

    # --- STEP 2: Process through Fuzzy Matching ---
    analyzed_data = extract_events_fuzzy(raw_data)

    # --- STEP 3: Intelligent Vessel Selection ---
    arrived_loc = analyzed_data.get("Arrived Location")
    vessels_list = raw_data.get("vessels", [])
    matched_vessel = None

    if arrived_loc and vessels_list:
        target_city = arrived_loc.lower().split(',')[0].strip()
        for v in vessels_list:
            v_discharge = v.get("discharge")
            if v_discharge:
                v_city = v_discharge.lower().split(',')[0].strip()
                if target_city in v_city or v_city in target_city:
                    matched_vessel = v
                    break

    if matched_vessel:
        analyzed_data["vessel_details"] = {
            "vessel": matched_vessel.get("vessel"),
            "voyage": matched_vessel.get("voyage"),
            "loading": matched_vessel.get("loading"),
            "discharge": matched_vessel.get("discharge")
        }
    elif "vessel_details" in raw_data:
        analyzed_data["vessel_details"] = raw_data["vessel_details"]

    if "container_type" in raw_data:
        analyzed_data["container_type"] = raw_data["container_type"]

    # --- STEP 4: Post Events ---
    events_posted = 0

    # Departed event
    dep_origin = analyzed_data.get("Departed Origin")
    dep_date = analyzed_data.get("Departed Date")
    existing_dep = status_details.get("Departed", {}).get("date")
    existing_val = status_details.get("Departed", {}).get("value")

    if (get_rank(api_status) < get_rank("Departed") or not existing_dep) and dep_origin and dep_date:
        data_changed = str(dep_date) != str(existing_dep) or str(dep_origin) != str(existing_val)
        # Force status update when portal status is behind
        is_changed = True if get_rank(api_status) < get_rank("Departed") else data_changed
        
        if shared_utils.post_event(container_no, "Departed", dep_date, dep_origin, is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [Departed] {dep_date} - {dep_origin} (Changed: {data_changed}, StatusForce: {is_changed})")

    # ETA event
    current_status = analyzed_data.get("Current Status")
    status_date = analyzed_data.get("Status Date")
    existing_eta = status_details.get("ETA", {}).get("date")

    if current_status == "ETA" and status_date and (get_rank(api_status) < get_rank("Inward") or not existing_eta):
        data_changed = str(status_date) != str(existing_eta)
        # Force status update when portal status is behind
        is_changed = True if get_rank(api_status) < get_rank("ETA") else data_changed
        
        if shared_utils.post_event(container_no, "ETA", status_date, "", is_status_changed=is_changed):
            if data_changed: events_posted += 1
            print(f"      [ETA] {status_date} (Changed: {data_changed}, StatusForce: {is_changed})")

    # Arrived at POD event
    arrived_loc = analyzed_data.get("Arrived Location")
    existing_arr = status_details.get("Arrived at POD", {}).get("date")
    existing_arr_val = status_details.get("Arrived at POD", {}).get("value")

    if (current_status in ["ATA", "Arrived"]) and status_date and arrived_loc:
        if (get_rank(api_status) < get_rank("Arrived at POD") or not existing_arr):
            data_changed = str(status_date) != str(existing_arr) or str(arrived_loc) != str(existing_arr_val)
            # Force status update when portal status is behind
            is_changed = True if get_rank(api_status) < get_rank("Arrived at POD") else data_changed
            
            if shared_utils.post_event(container_no, "Arrived at POD", status_date, arrived_loc, is_status_changed=is_changed):
                if data_changed: events_posted += 1
                print(f"      [Arrived at POD] {status_date} - {arrived_loc} (Changed: {data_changed}, StatusForce: {is_changed})")
            
            # Also update ETA to match actual arrival
            shared_utils.post_event(container_no, "ETA", status_date, "", is_status_changed=False)

    # --- STEP 5: Sync Job Details ---
    shipping_line = job_data.get("shipping_line", "")
    if analyzed_data.get("vessel_details"):
        sync_job_details(container_no, analyzed_data, shipping_line)

    # Update last check date anyway
    if events_posted == 0:
        shared_utils.post_event(container_no, "", "", "", is_status_changed=False)

    return {"success": True, "events_posted": events_posted}


# ==========================================
# WORKER PROCESS
# ==========================================
def worker_process(worker_num):
    """Single worker process — pulls jobs from Redis queue and processes them."""
    worker_id = f"worker-{worker_num}-pid{os.getpid()}"
    r = get_redis()

    r.hset(WORKERS_KEY, worker_id, json.dumps({
        "started_at": datetime.now().isoformat(),
        "status": "idle"
    }))

    print(f"  [{worker_id}] Started")

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
                        print(f"  [{worker_id}] [✓] {container_no} done (events={result.get('events_posted', 0)})")
                    else:
                        r.hdel(PROCESSING_KEY, container_no)
                        retry_count = int(r.hget(RETRY_KEY, container_no) or 0)
                        retry_count += 1

                        delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                        r.hset(RETRY_KEY, container_no, retry_count)
                        print(f"  [{worker_id}] [↻] {container_no} failed (attempt {retry_count}): {result.get('error')}")
                        time.sleep(delay)
                        r.lpush(QUEUE_KEY, json.dumps(job_data))

                except Exception as e:
                    r.hdel(PROCESSING_KEY, container_no)
                    retry_count = int(r.hget(RETRY_KEY, container_no) or 0)
                    retry_count += 1

                    delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                    r.hset(RETRY_KEY, container_no, retry_count)
                    print(f"  [{worker_id}] [↻] {container_no} crashed (attempt {retry_count}): {e}")
                    time.sleep(delay)
                    r.lpush(QUEUE_KEY, json.dumps(job_data))

                time.sleep(SLEEP_BETWEEN_JOBS)

            except redis.ConnectionError as e:
                print(f"  [{worker_id}] Redis connection lost: {e}")
                time.sleep(10)
                r = get_redis()

            except Exception as e:
                print(f"  [{worker_id}] Unexpected error: {e}")
                traceback.print_exc()
                time.sleep(5)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            r.hdel(WORKERS_KEY, worker_id)
        except Exception:
            pass
        print(f"  [{worker_id}] Stopped")


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
    print(f"SEALION AGENT SUPERVISOR STARTED (Auto-Scaling)")
    print(f"{'='*60}")
    print(f"  Redis         : {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Queue         : {QUEUE_KEY}")
    print(f"  API           : {API_BASE_URL}")
    print(f"  Tracker       : {TRACKER_SCRIPT}")
    print(f"  Python        : {PYTHON_EXE}")
    print(f"  Fuzzy Search  : rapidfuzz")
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
