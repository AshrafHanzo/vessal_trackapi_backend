import os
import sys
import json
import time
import requests
import re
import redis
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = "https://trackcontainer.in/api/external"
SEALION_API_URL = "http://localhost:8012/sealion"
ICEGATE_API_URL = "http://localhost:8013/icegate"
CFS_API_URL = "http://localhost:8014/citpl"   # CITPL CFS
PORT_API_URL = "http://localhost:8015/search" # Port Terminal
DPW_API_URL = "http://localhost:8016/dpw"     # DP World CFS
ADANI_KATU_API_URL = "http://localhost:8018/adani" # Adani Kattupalli
ADANI_ENNORE_API_URL = "http://localhost:8017/ennore" # Adani Ennore

# Redis Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
DEDUP_TTL = 1800  # 30 minutes: matches orchestrator cron cycle

# Redis Queue Keys
QUEUE_KEYS = {
    "sealion": "tc:queue:sealion",
    "icegate": "tc:queue:icegate",
    "ldb": "tc:queue:ldb",
    "cfs": "tc:queue:cfs",
    "dpw": "tc:queue:dpw",
    "adani_katu": "tc:queue:adani_katu",
    "adani_ennore": "tc:queue:adani_ennore",
}
DEDUP_KEYS = {k: f"tc:queued:{k}" for k in QUEUE_KEYS}

CFS_PROVIDER_MAP_FILE = "cfs_provider_map.json"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Services are now managed externally (PM2/Systemd). 
# This script only consumes their APIs on the defined ports.

# ==========================================
# STATUS HIERARCHY
# ==========================================
STATUS_RANK = {
    "Created": 0,
    "Empty Return": 0,
    "Departed": 1,
    "ETA": 2,
    "IGM": 3,
    "Inward": 4,
    "Arrived at POD": 5,
    "Port In": 6,
    "Port Out": 7,
    "CFS In": 8,
    "CFS Out": 9,
    "Completed": 10
}

def get_rank(status):
    return STATUS_RANK.get(status, -1)

# ==========================================
# SHARED UTILS (STATELESS)
# ==========================================

def normalize_date(date_str):
    if not date_str: return None
    try: return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except: pass
    try: return datetime.strptime(date_str, "%d %b %Y %H:%M")
    except: pass
    try: return datetime.strptime(date_str, "%d %b %Y")
    except: pass
    try: return datetime.strptime(date_str, "%d-%m-%Y %I:%M %p") # Adani format
    except: pass
    return None

def post_event(container_no, status, date, value):
    payload = {
        "container_no": container_no,
        "status": status,
        "date": date if date else "",
        "value": value if value else ""
    }
    
    print(f"  [POST] Pushing {status}...")
    try:
        response = requests.post(f"{API_BASE_URL}/shipment-timeline", json=payload)
        if response.status_code in [200, 201]:
            print("    -> Success")
            return True
        else:
            print(f"    -> Failed: {response.text}")
            return False
    except Exception as e:
        print(f"    -> Error: {e}")
        return False

def fetch_active_containers():
    print("Fetching active containers...")
    try:
        response = requests.get(f"{API_BASE_URL}/containers/active")
        response.raise_for_status()
        data = response.json()
        
        candidates = []
        if data.get("status") == "success":
            for container in data.get("data", []):
                curr_status = container.get("status")
                if curr_status != "Completed":
                    candidates.append(container)
        
        print(f"  -> Found {len(candidates)} active containers.")
        return candidates
    except Exception as e:
        print(f"Error fetching candidates: {e}")
        return []

def fetch_job_details_map():
    print("Fetching enriched job details map...")
    try:
        response = requests.get(f"{API_BASE_URL}/get-job-details")
        response.raise_for_status()
        data = response.json()
        job_map = {}
        if data.get("status") == "success":
            for item in data.get("data", []):
                cnt_no = item.get("container_no")
                if cnt_no:
                    # Map by container_no for quick lookup
                    job_map[cnt_no] = item
        print(f"  -> Loaded {len(job_map)} job detail records.")
        return job_map
    except Exception as e:
        print(f"Error fetching job details: {e}")
        return {}

# ==========================================
# SERVICE MANAGEMENT
# ==========================================
# Service Management features removed. 
# Orchestrator assumes services are running via PM2/Systemd.


# ==========================================
# PROVIDER MAP UTILS
# ==========================================
def load_provider_map():
    if os.path.exists(CFS_PROVIDER_MAP_FILE):
        try:
            with open(CFS_PROVIDER_MAP_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_provider_map(data):
    try:
        with open(CFS_PROVIDER_MAP_FILE, "w") as f: json.dump(data, f, indent=4)
    except: pass

def update_provider_map(container_no, provider, clear=False):
    pmap = load_provider_map()
    if clear:
        if container_no in pmap:
            del pmap[container_no]
            save_provider_map(pmap)
            print(f"  [MAP] Job Complete at {provider}. Unlocked.")
    else:
        if pmap.get(container_no) != provider:
            pmap[container_no] = provider
            save_provider_map(pmap)
            print(f"  [MAP] Locked to provider: {provider}")

# ==========================================
# MODULES
# ==========================================

def sync_job_details(container_no, sealion_data):
    print(f"  [SYNC] Syncing Job Details...")
    vessel_details = sealion_data.get("vessel_details") or {}
    c_type = sealion_data.get("container_type") or ""
    
    # Truncate to 50 chars to fit UPDATED DB schema (VARCHAR 50)
    size = c_type[:50]
    
    payload = {
        "container_no": container_no.strip(),
        "size": size,
        "vessel_name": vessel_details.get("vessel", ""),
        "voyage_no": vessel_details.get("voyage", ""),
        "shipping_line": "Sealion",
        "pol": vessel_details.get("loading", ""),
        "pod": vessel_details.get("discharge", "")
    }
    
    print(f"    [Sync Payload]: {json.dumps(payload)}")
    try:
        response = requests.post(f"{API_BASE_URL}/sync-job-details", json=payload)
        
        if response.status_code in [200, 201]:
            print(f"    -> Sync Success ({response.status_code}): {response.text}")
        else:
            print(f"    -> Sync Failed ({response.status_code}): {response.text}")
            
    except Exception as e:
        print(f"    -> Sync Error: {e}")

def run_sealion_logic(container_obj):
    container_no = container_obj.get("container_no")
    api_status = container_obj.get("status", "")
    api_eta = container_obj.get("eta_date")
    
    # Smart Sync Check: Robustly detect existing data keys (API uses mixed keys)
    def get_api_val(keys):
        for k in keys:
            val = container_obj.get(k)
            if val and str(val).lower() not in ["null", "none", ""]: return val
        return None

    api_vessel = get_api_val(["vessel_name", "vessel"])
    api_voyage = get_api_val(["voyage_no", "voyage"])
    api_pol = get_api_val(["pol", "port_of_loading"])
    api_pod = get_api_val(["pod", "port_of_discharge"])

    # Efficiency Guard: Skip Sealion if already Arrived AND all core vessel info is present
    # We also check if the data is "real" (not placeholders)
    invalid_placeholders = ["", "Vessels are not available at the moment.", "null", None]
    
    is_vessel_valid = api_vessel not in invalid_placeholders
    is_voyage_valid = api_voyage not in invalid_placeholders
    is_pol_valid = api_pol not in invalid_placeholders
    is_pod_valid = api_pod not in invalid_placeholders
    
    has_full_data = all([is_vessel_valid, is_voyage_valid, is_pol_valid, is_pod_valid])

    if get_rank(api_status) >= get_rank("Arrived at POD") and has_full_data:
        print(f"  [Sealion] Skipping... Status is {api_status} and Vessel Data is complete and valid.")
        return None

    print(f"  [Sealion] Checking Status...")
    try:
        response = requests.get(SEALION_API_URL, params={"container_number": container_no})
        
        data = None
        if response.status_code == 200:
            data = response.json()

        if not data: return None
        
        # Handle known errors from Sealion
        error_msg = data.get("error")
        if error_msg in ["Shipping Line Unknown", "Incorrect Tracking Number"]:
            print(f"  [Sealion] Error for {container_no}: {error_msg}")
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if post_event(container_no, "Departed", now_str, "unable to track this container"):
                container_obj["status"] = "Departed"
            return None

        # ---------------------------------------------------------
        # Smart Sync: Update Job Details if platform has mission info
        # ---------------------------------------------------------
        vessel_details = data.get("vessel_details") or {}
        fresh_vessel = vessel_details.get("vessel")
        fresh_voyage = vessel_details.get("voyage")
        fresh_pol = vessel_details.get("loading")
        fresh_pod = vessel_details.get("discharge")

        needs_sync = not (api_vessel and api_voyage and api_pol and api_pod)
        if needs_sync and (fresh_vessel or fresh_voyage):
            sync_job_details(container_no, data)
        
        # Update local container object for downstream logic
        if vessel_details:
             container_obj["vessel_name"] = fresh_vessel
             container_obj["voyage_no"] = fresh_voyage
             container_obj["port_of_discharge"] = fresh_pod
             print(f"  [Update] Local Info Updated: POD={fresh_pod}")

        # ---------------------------------------------------------
        # Event Posting (Immediate)
        # ---------------------------------------------------------
        dep_origin = data.get("Departed Origin")
        dep_date = data.get("Departed Date")
        if get_rank(api_status) < get_rank("Departed") and dep_origin and dep_date:
            if post_event(container_no, "Departed", dep_date, dep_origin):
                container_obj["status"] = "Departed"

        current_status = data.get("Current Status") 
        status_date = data.get("Status Date")
        arrived_loc = data.get("Arrived Location")
        
        # ETA Transition
        if current_status == "ETA" and status_date and get_rank(api_status) < get_rank("Arrived at POD"):
            if normalize_date(status_date) != normalize_date(api_eta):
                post_event(container_no, "ETA", status_date, "")
        
        # Arrived at POD Transition (Immediate - No more Icegate Lock)
        if (current_status == "ATA" or current_status == "Arrived") and status_date and arrived_loc:
            if get_rank(api_status) < get_rank("Arrived at POD"):
                if post_event(container_no, "Arrived at POD", status_date, arrived_loc):
                    container_obj["status"] = "Arrived at POD"
                    # Also push to ETA to mark arrival if needed
                    post_event(container_no, "ETA", status_date, "")

        return data 
    except Exception as e:
        print(f"    Sealion Error: {e}")
        return None

def run_icegate_logic(container_obj):
    container_no = container_obj.get("container_no")
    api_status = container_obj.get("status")
    
    if api_status in ["Completed"]:
        return { "igm": False, "inward": False }

    # Backfilling check: Only run if we are missing critical Icegate info
    has_igm = container_obj.get("igm_no") and container_obj.get("igm_date")
    has_inward = container_obj.get("inward_date")
    
    if has_igm and has_inward:
        print(f"  [Icegate] Skipping... Data already complete.")
        return { "igm": True, "inward": True }

    mbl_no = container_obj.get("master_bl_no")
    bl_no = container_obj.get("bl_no")
    port = container_obj.get("port_of_discharge") 
    
    if not (mbl_no and bl_no and port):
        print(f"  [Icegate] Skipping... Missing Info (MBL: {mbl_no}, BL: {bl_no}, Port: {port})")
        return { "igm": False, "inward": False }

    # ... remaining port checking logic ...
    if "," in port: city = port.split(",")[0].strip()
    else: city = port.strip()
    
    cluster_triggers = ["chennai", "ennore", "kattupalli", "kamarajar"]
    CHENNAI_CLUSTER_PORTS = ["CHENNAI SEA (INMAA1)", "KAMARAJAR (INENR1)", "KATTUPALLI (INKAT1)"]
    
    target_ports = CHENNAI_CLUSTER_PORTS if city.lower() in cluster_triggers else [city]

    found_igm = bool(has_igm)
    found_inward = bool(has_inward)
    
    for check_port in target_ports:
        if found_igm and found_inward: break 
        
        print(f"    -> Probing {check_port}...")
        try:
            params = {"mbl_no": mbl_no, "bl_no": bl_no, "port": check_port}
            response = requests.get(ICEGATE_API_URL, params=params)
            
            if response.status_code == 200:
                result = response.json()
                data = result.get("data", {})
                if data:
                    invalid_dates = ["", "n.a.", "n/a", "-", "null", "none", "n.a"]
                    
                    igm_no = data.get("igm_no")
                    igm_date = data.get("igm_date")
                    if igm_date and str(igm_date).lower() not in invalid_dates and not has_igm:
                        found_igm = True
                        val = igm_no if igm_no else ""
                        if post_event(container_no, "IGM", igm_date, val):
                            print(f"       [MATCH] Found IGM at {check_port}")
                    
                    inw_date = data.get("inw_date")
                    if inw_date and str(inw_date).lower() not in invalid_dates and not has_inward:
                        found_inward = True
                        if post_event(container_no, "Inward", inw_date, ""):
                            print(f"       [MATCH] Found Inward at {check_port}")

                    if found_igm and found_inward: break 

        except Exception as e:
            print(f"    Icegate Error ({check_port}): {e}")
        
    return { "igm": found_igm, "inward": found_inward }

# Arrived logic merged into Sealion Tracker

def check_port_terminal(container_obj):
    container_no = container_obj.get("container_no")
    print(f"  [Port] Checking Terminal Status...")
    try:
        response = requests.get(PORT_API_URL, params={"container_no": container_no})
        if response.status_code == 200:
            data = response.json().get("data", {})
            port_name = data.get("port_name")
            port_in = data.get("port_in")
            port_out = data.get("port_out")
            
            p_in = False
            p_out = False
            
            if port_in and port_name: 
                if get_rank(container_obj.get("status")) < get_rank("Port In"):
                    if post_event(container_no, "Port In", port_in, port_name):
                        container_obj["status"] = "Port In"
                p_in = True
            if port_out and port_name: 
                if get_rank(container_obj.get("status")) < get_rank("Port Out"):
                    if post_event(container_no, "Port Out", port_out, port_name):
                        container_obj["status"] = "Port Out"
                p_out = True
            return p_in, p_out
    except Exception as e:
        print(f"    Port Error: {e}")
    return False, False

def check_dpw(container_obj): return check_generic_cfs(container_obj, DPW_API_URL, "DPW", "cfs_in_time", "cfs_out_time")
def check_citpl(container_obj): return check_generic_cfs(container_obj, CFS_API_URL, "CITPL", "cfs_in", "cfs_out")

def check_adani_katu(container_obj):
    container_no = container_obj.get("container_no")
    print(f"  [Adani Katu] Checking...")
    try:
        response = requests.get(ADANI_KATU_API_URL, params={"container_no": container_no})
        if response.status_code == 200:
            data = response.json().get("data", {})
            if not data: data = response.json()
            return process_cfs_data(container_obj, data.get("entry_time"), data.get("exit_time"), "Adani Kattupalli")
    except Exception as e:
        print(f"    Adani Kattu Error: {e}")
    return False, False

def check_adani_ennore(container_obj):
    container_no = container_obj.get("container_no")
    print(f"  [Adani Ennore] Checking...")
    try:
        response = requests.get(ADANI_ENNORE_API_URL, params={"container_no": container_no})
        if response.status_code == 200:
            data = response.json().get("data", {})
            if not data: data = response.json()
            return process_cfs_data(container_obj, data.get("entry_time"), data.get("exit_time"), "Adani Ennore")
    except Exception as e:
        print(f"    Adani Ennore Error: {e}")
    return False, False

def check_generic_cfs(container_obj, url, name, key_in, key_out):
    container_no = container_obj.get("container_no")
    print(f"  [{name}] Checking...")
    try:
        response = requests.get(url, params={"container_no": container_no})
        if response.status_code == 200:
            data = response.json().get("data", {})
            return process_cfs_data(container_obj, data.get(key_in), data.get(key_out), data.get("cfs_name", name))
    except Exception as e:
        print(f"    {name} Error: {e}")
    return False, False

def process_cfs_data(container_obj, cfs_in, cfs_out, val):
    container_no = container_obj.get("container_no")
    p_in = False
    p_out = False
    
    if cfs_in:
        if get_rank(container_obj.get("status")) < get_rank("CFS In"):
            if post_event(container_no, "CFS In", cfs_in, val):
                container_obj["status"] = "CFS In"
        p_in = True
        
    if cfs_out:
        if get_rank(container_obj.get("status")) < get_rank("CFS Out"):
            if post_event(container_no, "CFS Out", cfs_out, val):
                container_obj["status"] = "CFS Out"
        p_out = True
        
    return p_in, p_out

def run_smart_cfs_logic(container_obj):
    curr_rank = get_rank(container_obj.get("status"))
    if curr_rank < get_rank("Arrived at POD"): return # Not ready for Port

    container_no = container_obj.get("container_no")
    
    # 1. Port Check (Skip if already Port Out)
    if curr_rank < get_rank("Port Out"):
        has_port_in, has_port_out = check_port_terminal(container_obj)
        curr_rank = get_rank(container_obj.get("status"))
        
        if not has_port_out:
            print("  [Smart CFS] Waiting for Port Out...")
            return 
    else:
        print("  [Skip] Port Check (Already Port Out/CFS).")

    # 2. Smart CFS Routing
    if curr_rank >= get_rank("CFS Out"):
        print("  [Skip] CFS Check (Already CFS Out/Completed).")
        return

    pmap = load_provider_map()
    sticky_provider = pmap.get(container_no)
    
    cfs_in = False
    cfs_out = False
    provider_used = None
    
    if sticky_provider:
        print(f"  [Smart CFS] Using Sticky Provider: {sticky_provider}")
        if sticky_provider == "DPW": cfs_in, cfs_out = check_dpw(container_obj)
        elif sticky_provider == "CITPL": cfs_in, cfs_out = check_citpl(container_obj)
        elif sticky_provider == "Adani_Katu": cfs_in, cfs_out = check_adani_katu(container_obj)
        elif sticky_provider == "Adani_Ennore": cfs_in, cfs_out = check_adani_ennore(container_obj)
        provider_used = sticky_provider
    else:
        print(f"  [Smart CFS] Discovery Mode (Waterfall)...")
        # Waterfall: DPW -> CITPL -> Katu -> Ennore
        cfs_in, cfs_out = check_dpw(container_obj)
        if cfs_in or cfs_out:
            provider_used = "DPW"
        else:
            cfs_in, cfs_out = check_citpl(container_obj)
            if cfs_in or cfs_out:
                provider_used = "CITPL"
            else:
                cfs_in, cfs_out = check_adani_katu(container_obj)
                if cfs_in or cfs_out:
                    provider_used = "Adani_Katu"
                else:
                    cfs_in, cfs_out = check_adani_ennore(container_obj)
                    if cfs_in or cfs_out:
                        provider_used = "Adani_Ennore"
    
    # 3. Handle Transient Lock
    if provider_used:
        if cfs_in and not cfs_out:
            update_provider_map(container_no, provider_used)
        elif cfs_out:
            update_provider_map(container_no, provider_used, clear=True)

# ==========================================
# REDIS QUEUE HELPERS
# ==========================================
def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def push_to_queue(r, service_name, job_payload):
    """
    Push a job to a Redis queue with dedup.
    Returns True if pushed, False if already queued.
    """
    container_no = job_payload.get("container_no", "UNKNOWN")
    queue_key = QUEUE_KEYS[service_name]
    dedup_key = DEDUP_KEYS[service_name]

    # Check dedup: don't push if already queued in this cycle
    if r.sismember(dedup_key, container_no):
        return False

    r.lpush(queue_key, json.dumps(job_payload))
    r.sadd(dedup_key, container_no)
    r.expire(dedup_key, DEDUP_TTL)
    return True


# ==========================================
# MAIN (REDIS QUEUE MODE)
# ==========================================
def main():
    print("=" * 60)
    print("MAIN ORCHESTRATOR STARTED (Redis Queue Mode)")
    print("=" * 60)

    try:
        r = get_redis_client()
        r.ping()
        print(f"  Redis connected: {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"  [FATAL] Cannot connect to Redis: {e}")
        print(f"  Falling back to legacy HTTP mode...")
        main_legacy()
        return

    try:
        # 1. Fetch data
        containers = fetch_active_containers()
        job_map = fetch_job_details_map()

        # Stats
        stats = {k: 0 for k in QUEUE_KEYS}
        skipped = 0

        # 2. Push containers to appropriate queues based on status
        for container_obj in containers:
            try:
                container_no = container_obj.get("container_no")

                # Enrich with job_map data
                extra_info = job_map.get(container_no, {})
                for k, v in extra_info.items():
                    if not container_obj.get(k) and v:
                        container_obj[k] = v

                status = container_obj.get("status", "Created")
                rank = get_rank(status)

                print(f"\n>>> {container_no} [Status: {status}]")

                # Build the base job payload
                job_payload = {
                    "container_no": container_no,
                    "status": status,
                    "eta_date": container_obj.get("eta_date"),
                    "master_bl_no": container_obj.get("master_bl_no"),
                    "bl_no": container_obj.get("bl_no"),
                    "port_of_discharge": container_obj.get("port_of_discharge"),
                    "vessel_name": container_obj.get("vessel_name"),
                    "voyage_no": container_obj.get("voyage_no"),
                    "igm_no": container_obj.get("igm_no"),
                    "igm_date": container_obj.get("igm_date"),
                    "inward_date": container_obj.get("inward_date"),
                    "queued_at": datetime.now().isoformat()
                }

                pushed_any = False
                skipped_queues = []

                # --- SMART QUEUE SKIPPING ---
                # Check status_details from API to skip queues where
                # data is already filled. Only push to queues with missing data.
                sd = container_obj.get("status_details", {})

                def has_event(event_name):
                    """Check if a status_details event has a date (= data exists)"""
                    evt = sd.get(event_name, {})
                    return bool(evt and evt.get("date"))

                # SEALION — DISABLED (not running, 0 workers)
                skipped_queues.append("sealion")
                print(f"    ⏭ sealion — SKIP (disabled)")

                # ICEGATE — skip only when BOTH IGM + Inward are filled
                # Keep checking until both are found
                if has_event("IGM") and has_event("Inward"):
                    skipped_queues.append("icegate")
                    print(f"    ⏭ icegate — SKIP (IGM+Inward both filled)")
                else:
                    if push_to_queue(r, "icegate", job_payload):
                        stats["icegate"] += 1
                        pushed_any = True

                # LDB — DISABLED
                skipped_queues.append("ldb")
                print(f"    ⏭ ldb — SKIP (disabled)")

                # CFS — skip ALL 4 CFS queues only if ALL data is filled:
                # Port In + Port Out (At Port section) AND CFS In + CFS Out (Customs section)
                if has_event("Port In") and has_event("Port Out") and has_event("CFS In") and has_event("CFS Out"):
                    skipped_queues.extend(["cfs", "dpw", "adani_katu", "adani_ennore"])
                    print(f"    ⏭ cfs (all 4) — SKIP (Port+CFS all filled)")
                else:
                    # Push to sticky provider or discovery mode
                    pmap = load_provider_map()
                    sticky = pmap.get(container_no)

                    if sticky:
                        provider_map = {
                            "DPW": "dpw", "CITPL": "cfs",
                            "Adani_Katu": "adani_katu", "Adani_Ennore": "adani_ennore"
                        }
                        queue_name = provider_map.get(sticky)
                        if queue_name and push_to_queue(r, queue_name, job_payload):
                            stats[queue_name] += 1
                            pushed_any = True
                            print(f"    → CFS: {sticky} (sticky)")
                    else:
                        for cfs_queue in ["dpw", "cfs", "adani_katu", "adani_ennore"]:
                            if push_to_queue(r, cfs_queue, job_payload):
                                stats[cfs_queue] += 1
                                pushed_any = True
                        print(f"    → CFS: all queues (discovery)")

                # Summary for this container
                if pushed_any:
                    pushed_list = [q for q in QUEUE_KEYS if q not in skipped_queues]
                    print(f"    ✓ Pushed to: {', '.join(pushed_list)}")
                elif skipped_queues:
                    skipped += 1
                    print(f"    ✓ ALL data filled — nothing to push")
                else:
                    skipped += 1
                    print(f"    (dedup blocked all queues)")

            except Exception as e:
                print(f"Error processing {container_obj.get('container_no')}: {e}")

        # 3. Summary
        print(f"\n{'='*60}")
        print(f"ORCHESTRATOR SUMMARY")
        print(f"{'='*60}")
        print(f"  Total containers : {len(containers)}")
        print(f"  Skipped          : {skipped}")
        for svc, count in stats.items():
            q_len = r.llen(QUEUE_KEYS[svc])
            print(f"  {svc:15s}  : +{count} pushed (queue depth: {q_len})")
        print(f"{'='*60}")

    finally:
        print("=== MAIN ORCHESTRATOR FINISHED ===")


def main_legacy():
    """Legacy HTTP-based processing (fallback if Redis is unavailable)."""
    print("=== LEGACY MODE (HTTP) ===")
    try:
        containers = fetch_active_containers()
        job_map = fetch_job_details_map()

        for container_obj in containers:
            try:
                container_no = container_obj.get("container_no")
                extra_info = job_map.get(container_no, {})
                for k, v in extra_info.items():
                    if not container_obj.get(k) and v:
                        container_obj[k] = v

                status = container_obj.get("status")
                print(f"\n>>> Processing {container_no} [Status: {status}]")
                rank = get_rank(status)

                if rank < get_rank("Completed"):
                    run_sealion_logic(container_obj)
                    run_icegate_logic(container_obj)

                rank = get_rank(container_obj.get("status"))
                if rank >= get_rank("Arrived at POD"):
                    run_smart_cfs_logic(container_obj)

                time.sleep(1)
            except Exception as e:
                print(f"Error processing container {container_no}: {e}")
    finally:
        print("=== LEGACY ORCHESTRATOR FINISHED ===")


if __name__ == "__main__":
    main()
