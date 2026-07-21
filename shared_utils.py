import os
import requests
import redis
import json
from datetime import datetime
import urllib3

# Suppress SSL warnings since we are using verify=False to bypass Nginx EOF bugs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")

# Redis Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "api.trackcontainer.in")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 30093))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)

import sys
IS_WINDOWS = sys.platform == 'win32'
WINDOWS_MANAGED_SERVICES = ["hapag", "cosco", "rcl", "hmm"]

DEDUP_TTL = 1800  # 30 minutes
LAST_CHECK_TTL = 14400  # 4 hours in seconds — cooldown between checks for same container

# Redis Queue Keys
QUEUE_KEYS = {
    "kmtc": "tc:queue:kmtc",
    "one_line": "tc:queue:one_line",
    "interasia": "tc:queue:interasia",
    "esl": "tc:queue:esl",
    "hmm": "tc:queue:hmm",
    "icegate": "tc:queue:icegate",
    "ldb": "tc:queue:ldb",
    "cfs": "tc:queue:cfs",
    "dpw": "tc:queue:dpw",
    "adani_katu": "tc:queue:adani_katu",
    "adani_ennore": "tc:queue:adani_ennore",
    "wan_hai": "tc:queue:wan_hai",
    "hapag": "tc:queue:hapag",
    "cosco": "tc:queue:cosco",
    "rcl": "tc:queue:rcl",
    "sealion": "tc:queue:sealion",
}
DEDUP_KEYS = {k: f"tc:queued:{k}" for k in QUEUE_KEYS}

# Status Hierarchy
STATUS_RANK = {
    "Created": 0, "Empty Return": 0, "Departed": 1, "ETA": 2,
    "IGM": 3, "Inward": 4, "Arrived at POD": 5, "Port In": 6,
    "Port Out": 7, "CFS In": 8, "CFS Out": 9, "Completed": 10
}

def get_rank(status):
    return STATUS_RANK.get(status, -1)

def is_valid_container(container_no):
    """
    Validates shipping container format.
    Standard: 4 letters followed by 6 or 7 digits.
    Example: MAEU1234567
    """
    import re
    if not container_no or not isinstance(container_no, str):
        return False
    # Standard: Usually 4 letters + 7 digits. 
    # But some lines use long prefixes like KMTCSHAP or FSZCNN.
    pattern = r'^[A-Z]{3,10}[0-9]{6,10}$'
    return bool(re.match(pattern, str(container_no).strip().upper()))

# ==========================================
# UTILITIES
# ==========================================

def get_redis_client():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        print(f"  [ERROR] Redis connection failed: {e}")
        return None

def was_recently_checked(r, container_no):
    """
    Check if a container was processed within the last 4 hours.
    Uses Redis key tc:last_checked:{container_no} with TTL-based auto-expiry.
    Returns True if recently checked (should SKIP), False if due for re-check.
    """
    if not r or not container_no:
        return False
    try:
        return r.exists(f"tc:last_checked:{container_no}") > 0
    except Exception:
        return False

def mark_checked(r, container_no):
    """
    Mark a container as recently checked. Sets Redis key with 4-hour TTL.
    After 4 hours, the key auto-expires and the container becomes eligible again.
    """
    if not r or not container_no:
        return
    try:
        r.setex(f"tc:last_checked:{container_no}", LAST_CHECK_TTL, datetime.now().isoformat())
    except Exception as e:
        print(f"  [WARN] Could not mark {container_no} as checked: {e}")

def fetch_active_containers():
    """Fetch list of all active containers from the API."""
    r = None
    try:
        r = get_redis_client()
        if r:
            cached = r.get("tc:cache:active_containers")
            if cached:
                print("  -> Returning cached active containers from Redis.")
                return json.loads(cached)
    except Exception as e:
        print(f"  [WARN] Redis cache read failed: {e}")

    print("Fetching active containers from API...")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{API_BASE_URL}/containers/active", verify=False, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            candidates = []
            if data.get("status") == "success":
                hierarchy = [
                    "Completed", "CFS Out", "CFS In", "Port Out", "Port In",
                    "Arrived at POD", "Inward", "IGM", "ETA", "Departed", "Created"
                ]
                for container in data.get("data", []):
                    # Auto-Heal the master status using the deepest valid date tree
                    details = container.get("status_details", {})
                    if details:
                        for state in hierarchy:
                            if details.get(state, {}).get("date"):
                                container["status"] = state
                                break
                    if not container.get("status"):
                        js = container.get("job_status", "Created")
                        container["status"] = js.title() if js else "Created"
                        
                    candidates.append(container)
            
            print(f"  -> Found {len(candidates)} active containers from API.")
            
            # Cache the result in Redis for 120 seconds (2 minutes)
            try:
                if r:
                    r.setex("tc:cache:active_containers", 120, json.dumps(candidates))
            except Exception as ce:
                print(f"  [WARN] Failed to write cache to Redis: {ce}")
                
            return candidates
        except Exception as e:
            print(f"  [RETRY {attempt+1}/{max_retries}] Error fetching candidates: {e}")
            import time
            time.sleep(2)

    print("  [ERROR] Could not fetch active containers after retries.")
    return []

def fetch_job_details_map():
    r = None
    try:
        r = get_redis_client()
        if r:
            cached = r.get("tc:cache:job_details_map")
            if cached:
                print("  -> Returning cached job details map from Redis.")
                return json.loads(cached)
    except Exception as e:
        print(f"  [WARN] Redis cache read failed: {e}")

    print("Fetching enriched job details map from API...")
    job_map = {}
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Use verify=False to bypass Nginx SSL handshake bugs
            response = requests.get(f"{API_BASE_URL}/get-job-details", verify=False, timeout=60)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "success":
                for item in data.get("data", []):
                    cnt_no = item.get("container_no")
                    if cnt_no:
                        job_map[cnt_no] = item
            print(f"  -> Loaded {len(job_map)} job detail records from API.")
            
            # Cache the result in Redis for 120 seconds (2 minutes)
            try:
                if r:
                    r.setex("tc:cache:job_details_map", 120, json.dumps(job_map))
            except Exception as ce:
                print(f"  [WARN] Failed to write cache to Redis: {ce}")
                
            return job_map
        except Exception as e:
            print(f"  [RETRY {attempt+1}/{max_retries}] Error fetching job details: {e}")
            import time
            time.sleep(2)
            
    print("  [ERROR] Could not fetch job details after retries.")
    return job_map

def push_to_queue(r, service_name, job_payload):
    """
    Push a job to a Redis queue with dedup.
    Returns True if pushed, False if already queued.
    """
    if not r or not service_name:
        return False
        
    container_no = job_payload.get("container_no", "UNKNOWN")
    queue_key = QUEUE_KEYS.get(service_name)
    dedup_key = DEDUP_KEYS.get(service_name)
    
    if not queue_key or not dedup_key:
        return False

    # Check dedup: don't push if already queued in this cycle
    if r.sismember(dedup_key, container_no):
        return False

    r.lpush(queue_key, json.dumps(job_payload))
    r.sadd(dedup_key, container_no)
    r.expire(dedup_key, DEDUP_TTL)
    return True

def get_shipping_line_service(shipping_line_str):
    """Maps shipping line string to Redis queue service name."""
    sl = (shipping_line_str or "").strip().upper()
    if "KMTC" in sl: return "kmtc"
    if any(x in sl for x in ["ONE LINE", "OCEAN NETWORK EXPRESS", "ONE"]): return "one_line"
    if "INTERASIA" in sl or "INTER ASIA" in sl: return "sealion"  # Routed to Sealion (InterAsia agent disabled)
    if "EMIRATES" in sl or "ESL" == sl: return "esl"
    if "HMM" in sl or "HYUNDAI" in sl: return "hmm"
    if "WAN HAI" in sl or "WANHAI" in sl: return "wan_hai"
    if "HAPAG" in sl or "HAPAG-LLOYD" in sl or "HLCU" in sl: return "hapag"
    if "COSCO" in sl: return "cosco"
    if "RCL" in sl or "REGIONAL CONTAINER" in sl: return "rcl"
    # These shipping lines are explicitly routed to Sealion for tracking
    if any(x in sl for x in ["CMA", "CMA CGM", "CMACGM"]): return "sealion"
    if any(x in sl for x in ["OOCL", "ORIENT OVERSEAS"]): return "sealion"
    if any(x in sl for x in ["MSC", "MEDITERRANEAN"]): return "sealion"
    if any(x in sl for x in ["YML", "YANG MING"]): return "sealion"
    if any(x in sl for x in ["MAERSK", "MSK"]): return "sealion"
    return "sealion" # Return sealion as fallback for unmapped or "other" lines


def post_event(container_no, status, date, value, is_status_changed=True, shipment_id=None):
    """
    Centralized function to post a shipment timeline event to the Portal API.
    Uses an unambiguous date format (%d %b %Y) to prevent month/day swapping in the portal.
    Also marks the container as recently checked in Redis (4-hour cooldown).
    """
    payload = {
        "container_no": container_no,
        "status": status if status else "",
        "date": date if date else "",
        "value": value or "",
        # Dynamic unambiguous format: e.g. "12 Apr 2026 23:15"
        "last_check_date": datetime.now().strftime("%d %b %Y %H:%M"),
        "is_status_changed": is_status_changed,
        "shipment_id": shipment_id
    }
    try:
        response = requests.post(
            f"{API_BASE_URL}/shipment-timeline",
            json=payload,
            timeout=30,
            verify=False
        )
        if response.status_code in [200, 201]:
            # Mark as checked in Redis so orchestrator won't re-queue for 4 hours
            try:
                r = get_redis_client()
                if r:
                    mark_checked(r, container_no)
            except Exception:
                pass
            return True
        else:
            print(f"      [Portal] Post failed ({response.status_code}): {response.text[:200]}")
            return False
    except Exception as e:
        print(f"      [Portal] Post error: {e}")
        return False


def normalize_date(date_str):
    """Normalize date to YYYY-MM-DD for accurate comparison."""
    if not date_str:
        return None
    try:
        # Strip string and remove any trailing time or whitespace
        date_clean = str(date_str).strip()
        
        # If it has a space, split and filter out any part containing ':' (time part)
        if " " in date_clean:
            parts = date_clean.split(" ")
            clean_parts = []
            for p in parts:
                if ":" in p:
                    break
                clean_parts.append(p)
            date_clean = " ".join(clean_parts).strip()
            
        if "T" in date_clean:
            date_clean = date_clean.split("T")[0]
            
        # Try various common formats
        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d %b %Y", "%d %B %Y"]:
            try:
                return datetime.strptime(date_clean, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return str(date_str).strip()


def are_dates_equal(date1, date2):
    """Compare two date strings in a normalized format."""
    if not date1 and not date2:
        return True
    if not date1 or not date2:
        return False
    return normalize_date(date1) == normalize_date(date2)


def run_agent_subprocess(python_exe, script_path, container_no, timeout_seconds, cwd):
    """
    Runs an agent tracker worker script as a subprocess.
    Avoids the Windows pipe deadlock bug by redirecting stdout/stderr to files.
    If a timeout occurs, it recursively kills the process tree using taskkill (on Windows)
    to ensure no zombie Chrome/Chromedriver processes are left.
    """
    import tempfile
    import sys
    import subprocess
    import uuid

    # Use a local temp logs directory instead of the system TEMP directory to bypass Windows Server Temp virtualization issues.
    local_temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_logs")
    if not os.path.exists(local_temp_dir):
        try:
            os.makedirs(local_temp_dir, exist_ok=True)
        except Exception:
            pass

    # Fallback to tempfile if local directory creation fails
    if os.path.exists(local_temp_dir):
        unique_dir = os.path.join(local_temp_dir, f"tmp_{uuid.uuid4().hex}")
        try:
            os.makedirs(unique_dir, exist_ok=True)
            tmpdir = unique_dir
        except Exception:
            tmpdir = tempfile.mkdtemp()
    else:
        tmpdir = tempfile.mkdtemp()

    stdout_path = os.path.join(tmpdir, "stdout.log")
    stderr_path = os.path.join(tmpdir, "stderr.log")
    
    try:
        with open(stdout_path, "w", encoding="utf-8", errors="ignore") as f_out, \
             open(stderr_path, "w", encoding="utf-8", errors="ignore") as f_err:
            
            proc = subprocess.Popen(
                [python_exe, script_path, container_no],
                stdout=f_out,
                stderr=f_err,
                cwd=cwd
            )
            
            try:
                proc.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                print(f"  [SUBPROCESS] Timeout reached ({timeout_seconds}s) for PID {proc.pid}. Terminating process tree...", flush=True)
                
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/F", "/PID", str(proc.pid), "/T"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                else:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                
                try:
                    proc.wait(timeout=5)
                except Exception:
                    pass
                
                raise subprocess.TimeoutExpired(proc.args, timeout_seconds)
                
        stdout_val = ""
        stderr_val = ""
        if os.path.exists(stdout_path):
            with open(stdout_path, "r", encoding="utf-8", errors="ignore") as f:
                stdout_val = f.read()
        if os.path.exists(stderr_path):
            with open(stderr_path, "r", encoding="utf-8", errors="ignore") as f:
                stderr_val = f.read()
                
        class CompletedProcessShim:
            def __init__(self, returncode, stdout, stderr):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr
                
        return CompletedProcessShim(proc.returncode, stdout_val, stderr_val)

    finally:
        # Clean up files and directory
        try:
            if os.path.exists(stdout_path):
                os.remove(stdout_path)
        except Exception:
            pass
        try:
            if os.path.exists(stderr_path):
                os.remove(stderr_path)
        except Exception:
            pass
        try:
            if os.path.exists(tmpdir):
                os.rmdir(tmpdir)
        except Exception:
            pass




