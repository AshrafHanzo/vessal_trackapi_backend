"""
LDB Agent Worker — Redis Queue Consumer with Auto-Scaling

Features:
  - Supervisor process monitors queue depth and auto-scales workers
  - Workers pull jobs from tc:queue:ldb via BRPOP
  - Runs ldb_tracker.py as subprocess (Playwright)
  - POSTs results to Portal API shipment-timeline endpoint
  - Auto-retry with exponential backoff on failures
  - Dynamic scaling: 1 worker per 3 containers, min=1, max=8

Run:  python ldb_agent.py
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
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LDB_SCRIPT = os.path.join(SCRIPT_DIR, "ldb_tracker.py")
PYTHON_EXE = sys.executable  # Uses whatever Python is running this script

# Redis Keys
QUEUE_KEY = "tc:queue:ldb"
PROCESSING_KEY = "tc:processing:ldb"
FAILED_KEY = "tc:failed:ldb"
COMPLETED_KEY = "tc:completed:ldb"
RETRY_KEY = "tc:retries:ldb"      # Hash: container_no -> retry count
WORKERS_KEY = "tc:workers:ldb"    # Hash: worker_id -> status (for monitoring)

# Timeouts
BRPOP_TIMEOUT = 30        # seconds to wait for a job before re-checking
SCRAPER_TIMEOUT = 300     # 5 minutes max for Playwright scrape
SLEEP_BETWEEN_JOBS = 2    # seconds between jobs to be gentle on resources

# Retry Config — infinite retries, websites can be down for days
RETRY_DELAY_CAP = 600  # Max 10 min between retries
RETRY_DELAYS = [30, 60, 120, 300, 600]  # backoff: 30s, 1m, 2m, 5m, 10m

# Auto-Scaling Config
CPU_CORES = os.cpu_count() or 4
MIN_WORKERS = 1                                                    # always keep at least 1 worker alive
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", CPU_CORES * 2))   # default: 2x CPU cores (no fixed cap)
CONTAINERS_PER_WORKER = 2  # 1 worker per N containers in queue (aggressive for SaaS speed)
SCALE_CHECK_INTERVAL = 10  # seconds between scaling decisions (faster checks for SaaS)


def get_redis():
    """Create and return a Redis connection."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def post_event(container_no, status, date, value, is_status_changed=True):
    """Post a single timeline event to the Portal API."""
    payload = {
        "container_no": container_no,
        "status": status if status else "",
        "date": date if date else "",
        "value": value if value else "",
        "last_check_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "is_status_changed": is_status_changed
    }

    print(f"    [POST] {status} -> date={date}, value={value}, changed={is_status_changed}")
    try:
        response = requests.post(
            f"{API_BASE_URL}/shipment-timeline",
            json=payload,
            timeout=30
        )
        if response.status_code in [200, 201]:
            print(f"      -> Success ({response.status_code})")
            return True
        else:
            print(f"      -> Failed ({response.status_code}): {response.text[:200]}")
            return False
    except Exception as e:
        print(f"      -> Error: {e}")
        return False


def run_ldb_scraper(container_no, mode="port"):
    """
    Run ldb_tracker.py as a subprocess and return parsed result.
    Reuses the existing Playwright scraping logic without modification.
    """
    print(f"  [SCRAPE] Running ldb_tracker.py for {container_no} (mode={mode})...")

    try:
        result = subprocess.run(
            [PYTHON_EXE, LDB_SCRIPT, container_no, mode],
            capture_output=True,
            text=True,
            timeout=SCRAPER_TIMEOUT,
            cwd=SCRIPT_DIR
        )

        if result.returncode != 0:
            print(f"  [SCRAPE] Subprocess error (exit={result.returncode})")
            print(f"    stderr: {result.stderr[:500]}")
            return None

        output = result.stdout.strip()

        # Find JSON in output (skip any debug prints before it)
        json_start = output.find('{')
        if json_start == -1:
            print(f"  [SCRAPE] No JSON found in output")
            print(f"    stdout: {output[:500]}")
            return None

        json_str = output[json_start:]
        data = json.loads(json_str)
        return data

    except subprocess.TimeoutExpired:
        print(f"  [SCRAPE] TIMEOUT after {SCRAPER_TIMEOUT}s")
        return None
    except json.JSONDecodeError as e:
        print(f"  [SCRAPE] JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"  [SCRAPE] Unexpected error: {e}")
        return None


def process_job(job_data):
    """
    Process a single LDB job:
    1. Run the scraper
    2. Parse results
    3. POST events to Portal API
    """
    container_no = job_data["container_no"]
    mode = job_data.get("mode", "port")

    print(f"\n  {'='*40}")
    print(f"  PROCESSING: {container_no} (mode={mode})")
    print(f"  {'='*40}")

    # Run the scraper
    result = run_ldb_scraper(container_no, mode)

    if not result:
        # Post no-change event to update last_check_date
        post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": "Scraper returned no data"}

    if result.get("status") != "success":
        error_msg = result.get("error", result.get("message", "Unknown error"))
        print(f"  [RESULT] Scraper returned non-success: {error_msg}")
        # Post no-change event to update last_check_date
        post_event(container_no, "", "", "", is_status_changed=False)
        return {"success": False, "error": error_msg}

    data = result.get("data", {})

    # POST events based on mode
    events_posted = 0

    if mode == "port":
        port_name = data.get("port_name", "")
        port_in = data.get("port_in")
        port_out = data.get("port_out")

        print(f"  [RESULT] Port: {port_name}, In: {port_in}, Out: {port_out}")

        if port_in:
            if post_event(container_no, "Port In", port_in, port_name):
                events_posted += 1

        if port_out:
            if post_event(container_no, "Port Out", port_out, port_name):
                events_posted += 1

    elif mode == "cfs":
        cfs_name = data.get("cfs_name", "")
        cfs_in = data.get("cfs_in")
        cfs_out = data.get("cfs_out")

        print(f"  [RESULT] CFS: {cfs_name}, In: {cfs_in}, Out: {cfs_out}")

        if cfs_in:
            if post_event(container_no, "CFS In", cfs_in, cfs_name):
                events_posted += 1

        if cfs_out:
            if post_event(container_no, "CFS Out", cfs_out, cfs_name):
                events_posted += 1

    print(f"  [DONE] {events_posted} events posted for {container_no}")
    return {"success": True, "events_posted": events_posted}


def worker_process(worker_num):
    """
    Single worker process — runs in its own process via multiprocessing.
    Continuously pulls jobs from Redis queue and processes them.
    """
    worker_id = f"worker-{worker_num}-pid{os.getpid()}"
    r = get_redis()

    # Register this worker in Redis for monitoring
    r.hset(WORKERS_KEY, worker_id, json.dumps({
        "started_at": datetime.now().isoformat(),
        "status": "idle"
    }))

    print(f"  [{worker_id}] Started")

    try:
        while True:
            try:
                # Wait for a job (blocking pop with timeout)
                job_raw = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)

                if not job_raw:
                    # No job available, update status and loop back
                    r.hset(WORKERS_KEY, worker_id, json.dumps({
                        "started_at": datetime.now().isoformat(),
                        "status": "idle"
                    }))
                    continue

                _, payload = job_raw
                job_data = json.loads(payload)
                container_no = job_data.get("container_no", "UNKNOWN")

                # Update worker status to processing
                r.hset(WORKERS_KEY, worker_id, json.dumps({
                    "status": "processing",
                    "container": container_no,
                    "since": datetime.now().isoformat()
                }))

                # Move to processing set
                processing_entry = json.dumps({
                    **job_data,
                    "worker": worker_id,
                    "started_at": datetime.now().isoformat()
                })
                r.hset(PROCESSING_KEY, container_no, processing_entry)

                # Process the job with retry logic
                try:
                    result = process_job(job_data)

                    if result.get("success"):
                        # Success
                        r.hdel(PROCESSING_KEY, container_no)
                        r.hdel(RETRY_KEY, container_no)
                        r.incr(COMPLETED_KEY)
                        print(f"  [{worker_id}] [✓] {container_no} completed successfully")
                    else:
                        # Failed: always re-queue with backoff
                        r.hdel(PROCESSING_KEY, container_no)
                        retry_count = int(r.hget(RETRY_KEY, container_no) or 0)
                        retry_count += 1

                        delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                        r.hset(RETRY_KEY, container_no, retry_count)
                        print(f"  [{worker_id}] [↻] {container_no} failed (attempt {retry_count})")
                        print(f"      Error: {result.get('error', 'Unknown')}")
                        print(f"      Retrying in {delay}s...")
                        time.sleep(delay)
                        r.lpush(QUEUE_KEY, json.dumps(job_data))
                        print(f"      Re-queued {container_no}")

                except Exception as e:
                    r.hdel(PROCESSING_KEY, container_no)
                    retry_count = int(r.hget(RETRY_KEY, container_no) or 0)
                    retry_count += 1

                    delay = RETRY_DELAYS[min(retry_count - 1, len(RETRY_DELAYS) - 1)]
                    r.hset(RETRY_KEY, container_no, retry_count)
                    print(f"  [{worker_id}] [↻] {container_no} crashed (attempt {retry_count}): {e}")
                    time.sleep(delay)
                    r.lpush(QUEUE_KEY, json.dumps(job_data))

                # Pause between jobs
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
        # Cleanup: remove from workers list
        try:
            r.hdel(WORKERS_KEY, worker_id)
        except Exception:
            pass
        print(f"  [{worker_id}] Stopped")


def calculate_target_workers(queue_length):
    """Calculate how many workers we need based on queue depth."""
    import math
    if queue_length == 0:
        return MIN_WORKERS  # Keep at least MIN alive for quick pickup
    target = math.ceil(queue_length / CONTAINERS_PER_WORKER)
    return max(MIN_WORKERS, min(target, MAX_WORKERS))


def supervisor():
    """
    Supervisor process — monitors queue depth and auto-scales workers.

    Scaling logic:
      - Checks queue length every SCALE_CHECK_INTERVAL seconds
      - target_workers = ceil(queue_length / CONTAINERS_PER_WORKER)
      - Clamps between MIN_WORKERS and MAX_WORKERS
      - Spawns new workers if needed, terminates excess workers if idle
    """
    r = get_redis()

    print(f"\n{'='*60}")
    print(f"LDB AGENT SUPERVISOR STARTED (Auto-Scaling)")
    print(f"{'='*60}")
    print(f"  Redis         : {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Queue         : {QUEUE_KEY}")
    print(f"  API           : {API_BASE_URL}")
    print(f"  Tracker       : {LDB_SCRIPT}")
    print(f"  Python        : {PYTHON_EXE}")
    print(f"  Min workers   : {MIN_WORKERS}")
    print(f"  Max workers   : {MAX_WORKERS}")
    print(f"  Per worker    : {CONTAINERS_PER_WORKER} containers")
    print(f"  Check every   : {SCALE_CHECK_INTERVAL}s")
    print(f"  Time          : {datetime.now()}")
    print(f"{'='*60}")

    # Clear stale worker entries from previous runs
    r.delete(WORKERS_KEY)

    workers = []  # List of (process, worker_num)
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
        """Clean up workers that have crashed or exited."""
        alive = []
        for p, num in workers:
            if p.is_alive():
                alive.append((p, num))
            else:
                print(f"  [SUPERVISOR] Worker-{num} (pid={p.pid}) exited, cleaning up")
        return alive

    # Start initial workers
    for _ in range(MIN_WORKERS):
        spawn_worker()

    try:
        while True:
            time.sleep(SCALE_CHECK_INTERVAL)

            # Clean up dead workers first
            workers[:] = remove_dead_workers()

            # Check queue depth
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

            # Log status
            print(f"  [SUPERVISOR] Queue={queue_len} | Processing={processing_count} | "
                  f"Workers={current_workers}→{target} | "
                  f"Completed={completed} | Failed={failed_count}")

            # Scale UP: need more workers
            if target > current_workers:
                to_add = target - current_workers
                print(f"  [SUPERVISOR] ⬆ Scaling UP: adding {to_add} worker(s)")
                for _ in range(to_add):
                    spawn_worker()

            # Scale DOWN: too many workers (only kill idle ones)
            elif target < current_workers and queue_len == 0:
                to_remove = current_workers - target
                print(f"  [SUPERVISOR] ⬇ Scaling DOWN: removing {to_remove} worker(s)")
                # Remove from the end (newest workers first)
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
        # Cleanup Redis
        try:
            r.delete(WORKERS_KEY)
        except Exception:
            pass
        print(f"  [SUPERVISOR] All workers stopped. Goodbye!")


if __name__ == "__main__":
    # Allow running a single worker for debugging: python ldb_agent.py --single
    if len(sys.argv) > 1 and sys.argv[1] == "--single":
        worker_process(1)
    else:
        supervisor()

