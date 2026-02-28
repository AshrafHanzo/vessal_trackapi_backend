"""
Icegate Agent Worker — Redis Queue Consumer with Auto-Scaling

Features:
  - Supervisor process monitors queue depth and auto-scales workers
  - Workers pull jobs from tc:queue:icegate via BRPOP
  - Runs icegate/icegate_tracker.py as subprocess (Playwright + Captcha)
  - POSTs IGM and Inward events to Portal API
  - Auto-retry with exponential backoff on failures
  - Multi-port probing: Chennai cluster ports (INMAA1, INENR1, INKAT1)

Run:  python icegate_agent.py
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
import math
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "icegate_tracker.py")

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
QUEUE_KEY = "tc:queue:icegate"
PROCESSING_KEY = "tc:processing:icegate"
FAILED_KEY = "tc:failed:icegate"
COMPLETED_KEY = "tc:completed:icegate"
RETRY_KEY = "tc:retries:icegate"
WORKERS_KEY = "tc:workers:icegate"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 120      # 2 min — no captcha, just navigation + table scraping
SLEEP_BETWEEN_JOBS = 2

# Retry Config — infinite retries, websites can be down for days
RETRY_DELAY_CAP = 600  # Max 10 min between retries
RETRY_DELAYS = [30, 60, 120, 300, 600]

# Auto-Scaling Config
CPU_CORES = os.cpu_count() or 4
MIN_WORKERS = 1
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 8))  # No captcha — can run more workers
CONTAINERS_PER_WORKER = 2
SCALE_CHECK_INTERVAL = 10

# Chennai port cluster for multi-port probing (use port CODES, not full names)
CHENNAI_CLUSTER_PORTS = ["INMAA1", "INENR1", "INKAT1"]
CLUSTER_TRIGGERS = ["chennai", "ennore", "kattupalli", "kamarajar", "inmaa", "inenr", "inkat"]

INVALID_DATES = ["", "n.a.", "n/a", "-", "null", "none", "n.a"]


def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def post_event(container_no, status, date, value, is_status_changed=True):
    """Post a shipment timeline event to the Portal API."""
    payload = {
        "container_no": container_no,
        "status": status if status else "",
        "date": date if date else "",
        "value": value or "",
        "last_check_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "is_status_changed": is_status_changed
    }
    try:
        response = requests.post(
            f"{API_BASE_URL}/shipment-timeline",
            json=payload,
            timeout=30
        )
        if response.status_code in [200, 201]:
            return True
        else:
            print(f"      Post failed ({response.status_code}): {response.text[:200]}")
            return False
    except Exception as e:
        print(f"      Post error: {e}")
        return False


def run_icegate_tracker(mbl_no, port, bl_no):
    """Run the icegate tracker subprocess for a specific port."""
    port_arg = port if port else "ALL_PORTS"
    cmd = [PYTHON_EXE, TRACKER_SCRIPT, port_arg, mbl_no, bl_no]  # tracker expects: port, mbl, bl

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SCRAPER_TIMEOUT,
            cwd=SCRIPT_DIR
        )
    except subprocess.TimeoutExpired:
        return None, "Icegate tracker timed out"

    if result.returncode != 0:
        return None, f"Tracker error: {result.stderr[:500]}"

    output = result.stdout.strip()
    json_start = output.find('{')
    if json_start == -1:
        return None, "No JSON output from tracker"

    try:
        data = json.loads(output[json_start:])
        return data, None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"


def process_job(job_data):
    """
    Process a single Icegate tracking job:
    1. Determine target ports (Chennai cluster or single port)
    2. Run tracker for each port until IGM + Inward found
    3. Post IGM and Inward events to Portal API
    4. If found_port is returned, update port_of_discharge via sync API
    """
    container_no = job_data.get("container_no", "UNKNOWN")
    api_status = job_data.get("status", "")
    mbl_no = job_data.get("master_bl_no")
    bl_no = job_data.get("bl_no")
    port = job_data.get("port_of_discharge", "")

    # Validate inputs
    if not (mbl_no and bl_no):
        return {"success": True, "events_posted": 0, "note": f"Missing MBL/BL (MBL: {mbl_no}, BL: {bl_no})"}

    # Determine target ports — extract port CODE from full name if needed
    import re
    raw_port = (port or "").strip()
    
    # Extract port code from parentheses: "CHENNAI SEA (INMAA1)" -> "INMAA1"
    paren_match = re.search(r'\(([^)]+)\)', raw_port)
    if paren_match:
        city = paren_match.group(1).strip()
    elif raw_port and "," in raw_port:
        city = raw_port.split(",")[0].strip()
    else:
        city = raw_port

    if city.lower() in CLUSTER_TRIGGERS:
        target_ports = CHENNAI_CLUSTER_PORTS
    else:
        target_ports = [city] if city else ["ALL_PORTS"]

    # Probe each port
    found_igm = False
    found_inward = False
    events_posted = 0

    for check_port in target_ports:
        if found_igm and found_inward:
            break

        print(f"      Probing {check_port}...")
        data, error = run_icegate_tracker(mbl_no, check_port, bl_no)

        if error:
            print(f"      Error ({check_port}): {error}")
            continue

        if not data:
            continue

        # Handle both response formats:
        # New flat format: {"igm_no": ..., "igm_date": ..., "found_port": ...}
        # Old nested format: {"data": {"igm_no": ..., "igm_date": ...}}
        result_data = data.get("data", None)
        if result_data is None or (isinstance(result_data, dict) and not result_data):
            # No "data" wrapper — fields are at top level (new format)
            result_data = data

        if not result_data:
            continue

        # If found_port is returned and not null, update port_of_discharge
        found_port = result_data.get("found_port")
        if found_port and str(found_port).lower() not in ["null", "none", "", "n.a.", "n/a"]:
            print(f"      [PORT] Found port: {found_port} — syncing as port_of_discharge...")
            try:
                sync_payload = {
                    "container_no": container_no.strip(),
                    "pod": found_port
                }
                sync_resp = requests.post(
                    f"{API_BASE_URL}/sync-job-details",
                    json=sync_payload,
                    timeout=30
                )
                if sync_resp.status_code in [200, 201]:
                    print(f"      [PORT] Sync success: pod={found_port}")
                else:
                    print(f"      [PORT] Sync failed ({sync_resp.status_code}): {sync_resp.text[:200]}")
            except Exception as e:
                print(f"      [PORT] Sync error: {e}")

        # Check IGM
        igm_no = result_data.get("igm_no")
        igm_date = result_data.get("igm_date")
        if igm_date and str(igm_date).lower() not in INVALID_DATES and not found_igm:
            found_igm = True
            val = igm_no if igm_no else ""
            if post_event(container_no, "IGM", igm_date, val):
                events_posted += 1
                print(f"      [MATCH] Found IGM at {check_port}")

        # Check Inward
        inw_date = result_data.get("inw_date")
        if inw_date and str(inw_date).lower() not in INVALID_DATES and not found_inward:
            found_inward = True
            if post_event(container_no, "Inward", inw_date, ""):
                events_posted += 1
                print(f"      [MATCH] Found Inward at {check_port}")

        if found_igm and found_inward:
            break

    # If nothing new found, still post no-change to update last_check_date
    if events_posted == 0:
        post_event(container_no, "", "", "", is_status_changed=False)

    return {"success": True, "events_posted": events_posted, "igm": found_igm, "inward": found_inward}


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
    print(f"ICEGATE AGENT SUPERVISOR STARTED (Auto-Scaling)")
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
