import time
import json
import redis
import requests
import subprocess
import os
import sys
from datetime import datetime
import multiprocessing

# Add parent directory to path for shared_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import shared_utils
from shared_utils import post_event, get_rank

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Use the NEW tracker worker that works with Angular/Playwright
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
QUEUE_KEY = "tc:queue:icegate"
PROCESSING_KEY = "tc:processing:icegate"
FAILED_KEY = "tc:failed:icegate"
COMPLETED_KEY = "tc:completed:icegate"
RETRY_KEY = "tc:retries:icegate"
WORKERS_KEY = "tc:workers:icegate"

# Timeouts
BRPOP_TIMEOUT = 30
SCRAPER_TIMEOUT = 300      # 5 min — scraping multiple ports can take time
SLEEP_BETWEEN_JOBS = 2

# Chennai port cluster for multi-port probing
CHENNAI_CLUSTER_PORTS = ["INMAA", "INENR", "INKAT"] 
INVALID_DATES = ["", "n.a.", "n/a", "-", "null", "none", "n.a"]

# Scaling Config
MIN_WORKERS = 1
MAX_WORKERS = 8
CONTAINERS_PER_WORKER = 2
SCALE_CHECK_INTERVAL = 10

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def format_date(date_str):
    """Convert ICEGATE date '20-Apr-2026 12:04:00' to ISO '2026-04-20'"""
    if not date_str or str(date_str).lower() in INVALID_DATES:
        return None
    try:
        # Try DD-Mon-YYYY
        parts = str(date_str).split()
        if not parts: return None
        dt = datetime.strptime(parts[0], "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except:
        return date_str

def run_icegate_tracker(mbl_no, port, bl_no):
    """Run the NEW tracker_worker.py subprocess."""
    port_arg = port if port else "INMAA,INKAT,INENR"
    cmd = [PYTHON_EXE, TRACKER_SCRIPT, port_arg, mbl_no, bl_no]

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
        return None, f"Tracker error (code {result.returncode}): {result.stderr[:500]}"

    output = result.stdout.strip()
    lines = output.splitlines()
    if not lines:
        return None, "No output from tracker"
        
    last_line = lines[-1]
    json_start = last_line.find('{')
    if json_start == -1:
        json_start = output.find('{')
        if json_start == -1:
            return None, f"No JSON output from tracker."
        last_line = output[json_start:]

    try:
        data = json.loads(last_line)
        return data, None
    except:
        return None, "JSON parse error"

def process_job(job_data):
    """Process a single tracking job."""
    container_no = job_data.get("container_no", "").strip().upper()
    status = job_data.get("status", "")
    mbl_no = job_data.get("master_bl_no", "").strip()
    bl_no = job_data.get("bl_no", "").strip()
    port = job_data.get("port_of_discharge", "").strip()
    shipment_id = job_data.get("id") or job_data.get("shipment_id")

    if not container_no or not mbl_no:
        return False

    # 1. Determine ports to probe
    # If the port is Chennai, Ennore, or Kattupalli, we always try the cluster
    is_chennai_area = False
    if port:
        p_lower = port.lower()
        if "chennai" in p_lower or "ennore" in p_lower or "kattupalli" in p_lower or "katupalli" in p_lower:
            is_chennai_area = True

    if not port or port.upper() == "NONE" or is_chennai_area:
        probe_ports = CHENNAI_CLUSTER_PORTS
    else:
        probe_ports = [port]
    
    found_any = False
    for check_port in probe_ports:
        print(f"    [PROBE] {container_no} at {check_port}...", flush=True)
        
        # SMART FIX: If HBL is missing, use Container Number to find the row!
        search_bl = bl_no if bl_no and bl_no.upper() not in ["", "NONE", "-", "N.A."] else container_no
        
        result_data, error = run_icegate_tracker(mbl_no, check_port, search_bl)
        
        if error:
            print(f"      Error ({check_port}): {error}", flush=True)
            continue

        if not result_data or result_data.get("status") != "success":
            continue

        # Extract and Format
        igm_no = result_data.get("igm_no")
        igm_date = format_date(result_data.get("igm_date"))
        inw_date = format_date(result_data.get("inw_date"))
        found_port = result_data.get("found_port")

        # Sync Port
        if found_port:
            print(f"      [SYNC] Found port: {found_port}", flush=True)
            try:
                requests.post(f"{API_BASE_URL}/sync-job-details", 
                             json={"container_no": container_no, "pod": found_port}, timeout=10)
            except: pass

        # Post Events
        current_rank = get_rank(status)
        is_changed = current_rank < 3 # 3 is IGM rank

        posted = False
        if igm_date:
            val = f"IGM No: {igm_no}" if igm_no else "IGM Updated"
            if post_event(container_no, "IGM", igm_date, val, is_status_changed=is_changed, shipment_id=shipment_id):
                print(f"      [MATCH] Posted IGM: {igm_date} (Shipment: {shipment_id})", flush=True)
                posted = True

        if inw_date:
            if post_event(container_no, "Inward", inw_date, "Inward Entry Found", is_status_changed=False, shipment_id=shipment_id):
                print(f"      [MATCH] Posted Inward: {inw_date}", flush=True)
                posted = True

        if posted:
            found_any = True
            break

    return found_any

def worker_process(worker_id, stop_event):
    """Worker process that pulls jobs from Redis."""
    print(f"  [Worker-{worker_id}] Started.", flush=True)
    r = get_redis()
    
    while not stop_event.is_set():
        try:
            # Pull job from queue
            job_tuple = r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)
            if not job_tuple:
                continue
            
            job_json = job_tuple[1]
            job_data = json.loads(job_json)
            container_no = job_data.get("container_no", "UNKNOWN")
            
            print(f"  [Worker-{worker_id}] Processing: {container_no}", flush=True)
            r.hset(PROCESSING_KEY, container_no, json.dumps({
                "worker": worker_id,
                "started_at": datetime.now().isoformat(),
                "data": job_data
            }))

            # Execute job
            success = process_job(job_data)

            # Cleanup
            r.hdel(PROCESSING_KEY, container_no)
            if success:
                r.incr(COMPLETED_KEY)
                print(f"  [Worker-{worker_id}] [✓] {container_no} success", flush=True)
            else:
                print(f"  [Worker-{worker_id}] [x] {container_no} no data found", flush=True)
            
            time.sleep(SLEEP_BETWEEN_JOBS)

        except Exception as e:
            print(f"  [Worker-{worker_id}] [ERROR] {e}", flush=True)
            time.sleep(5)

def main():
    """Supervisor process."""
    print("="*60, flush=True)
    print("ICEGATE AGENT SUPERVISOR STARTED (Auto-Scaling)", flush=True)
    print("="*60, flush=True)
    
    r = get_redis()
    stop_event = multiprocessing.Event()
    workers = []

    try:
        while True:
            # Auto-scaling logic
            queue_len = r.llen(QUEUE_KEY)
            active_workers = len([w for w in workers if w.is_alive()])
            
            # Simple scaling
            target_workers = max(MIN_WORKERS, min(MAX_WORKERS, (queue_len // CONTAINERS_PER_WORKER) + 1))
            
            if active_workers < target_workers:
                for i in range(target_workers - active_workers):
                    p = multiprocessing.Process(target=worker_process, args=(len(workers)+1, stop_event))
                    p.start()
                    workers.append(p)
                print(f"  [SUPERVISOR] Scaled up to {len(workers)} workers", flush=True)

            # Print status
            completed = r.get(COMPLETED_KEY) or 0
            print(f"  [SUPERVISOR] Queue={queue_len} | Active={active_workers} | Total Completed={completed}", flush=True)
            
            time.sleep(SCALE_CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("  [SUPERVISOR] Shutting down...", flush=True)
        for p in workers:
            p.terminate()

if __name__ == "__main__":
    main()
