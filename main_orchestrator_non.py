
import os
import sys
import json
import time
import os
import sys
import json
import time
import requests
import re
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = "https://uat.trackcontainer.in/api/external"
SEALION_API_URL = "http://localhost:8012/sealion"
ICEGATE_API_URL = "http://localhost:8013/icegate"
CFS_API_URL = "http://localhost:8014/citpl"   # CITPL CFS
PORT_API_URL = "http://localhost:8015/search" # Port Terminal
DPW_API_URL = "http://localhost:8016/dpw"     # DP World CFS
ADANI_KATU_API_URL = "http://localhost:8018/adani" # Adani Kattupalli
ADANI_ENNORE_API_URL = "http://localhost:8017/ennore" # Adani Ennore

CFS_PROVIDER_MAP_FILE = "cfs_provider_map.json"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

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
        
        print(f"Found {len(candidates)} active containers.")
        return candidates
    except Exception as e:
        print(f"Error fetching candidates: {e}")
        return []

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
    
    size = "40HC"
    if "40" in c_type and "High Cube" in c_type: size = "40HC"
    elif "40" in c_type: size = "40ft"
    elif "20" in c_type: size = "20ft"
    
    payload = {
        "container_no": container_no,
        "size": size,
        "vessel_name": vessel_details.get("vessel", ""),
        "voyage_no": vessel_details.get("voyage", ""),
        "shipping_line": "Sealion",
        "pol": vessel_details.get("loading", ""),
        "pod": vessel_details.get("discharge", "")
    }
    
    try:
        requests.post(f"{API_BASE_URL}/sync-job-details", json=payload)
        print("    -> Sync Success")
    except Exception as e:
        print(f"    -> Sync Error: {e}")

def run_sealion_logic(container_obj):
    container_no = container_obj.get("container_no")
    api_status = container_obj.get("status", "")
    api_eta = container_obj.get("eta_date")

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
            # Post "Departed" with value "unable to track this container" as requested
            # Using current time as calling date is unavailable
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if post_event(container_no, "Departed", now_str, "unable to track this container"):
                # Also consider setting status to Departed so we don't keep retrying excessively?
                # or keep it as is. User said "push status as Departed".
                container_obj["status"] = "Departed"
            return None

        sync_job_details(container_no, data)
        
        # Update local container object with found details so subsequent logic (Icegate) can use them immediately
        vessel_details = data.get("vessel_details") or {}
        if vessel_details:
             container_obj["vessel"] = vessel_details.get("vessel")
             container_obj["voyage"] = vessel_details.get("voyage")
             container_obj["port_of_discharge"] = vessel_details.get("discharge")
             print(f"  [Update] Local Container Updated: POD={container_obj.get('port_of_discharge')}")

        dep_origin = data.get("Departed Origin")
        dep_date = data.get("Departed Date")
        if api_status in ["Created", "Empty Return"] and dep_origin and dep_date:
            if post_event(container_no, "Departed", dep_date, dep_origin):
                container_obj["status"] = "Departed"

        current_status = data.get("Current Status") 
        status_date = data.get("Status Date")
        
        # Existing ETA Logic
        if current_status == "ETA" and status_date and get_rank(api_status) < get_rank("Arrived at POD"):
            if normalize_date(status_date) != normalize_date(api_eta):
                post_event(container_no, "ETA", status_date, "")
        
        # New: If Arrived/ATA, allow pushing that date to ETA to mark it completed
        if (current_status == "ATA" or current_status == "Arrived") and status_date:
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

    mbl_no = container_obj.get("master_bl_no")
    bl_no = container_obj.get("bl_no")
    port = container_obj.get("port_of_discharge") 
    
    if not (mbl_no and bl_no and port):
        print(f"  [Icegate] Skipping... Missing Info (MBL: {mbl_no}, BL: {bl_no}, Port: {port})")
        return { "igm": False, "inward": False }

    print(f"  [Icegate] Checking (Port: {port})...")
    found_igm = False
    found_inward = False
    
    try:
        params = {"mbl_no": mbl_no, "bl_no": bl_no, "port": port}
        response = requests.get(ICEGATE_API_URL, params=params)
        
        if response.status_code == 200:
            result = response.json()
            data = result.get("data", {})
            if data:
                igm_no = data.get("igm_no")
                igm_date = data.get("igm_date")
                if igm_date:
                    found_igm = True
                    # Check against both old and new status strings if needed, 
                    # but mostly we care about rank. get_rank handles the lookup.
                    if get_rank(api_status) < get_rank("IGM"):
                        val = igm_no if igm_no else ""
                        if post_event(container_no, "IGM", igm_date, val):
                            container_obj["status"] = "IGM"
                
                inw_date = data.get("inw_date")
                if inw_date:
                    found_inward = True
                    if get_rank(container_obj.get("status")) < get_rank("Inward"):
                        if post_event(container_no, "Inward", inw_date, ""):
                            container_obj["status"] = "Inward"
    except Exception as e:
        print(f"    Icegate Error: {e}")
        
    return { "igm": found_igm, "inward": found_inward }

def run_arrived_logic(container_obj, sealion_data, icegate_result):
    if not sealion_data: return
    container_no = container_obj.get("container_no")
    api_status = container_obj.get("status")

    if get_rank(api_status) >= get_rank("Arrived at POD"): return 

    current_status = sealion_data.get("Current Status")
    status_date = sealion_data.get("Status Date")
    arrived_loc = sealion_data.get("Arrived Location")
    
    if (current_status == "ATA" or current_status == "Arrived") and status_date and arrived_loc:
        is_igm_done = icegate_result["igm"] or get_rank(api_status) >= get_rank("IGM")
        if is_igm_done:
            if post_event(container_no, "Arrived at POD", status_date, arrived_loc):
                container_obj["status"] = "Arrived at POD"
        else:
            print(f"  [HOLD] 'Arrived at POD' found but waiting for Icegate IGM.")

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

def check_dpw(container_no): return check_generic_cfs(container_no, DPW_API_URL, "DPW", "cfs_in_time", "cfs_out_time")
def check_citpl(container_no): return check_generic_cfs(container_no, CFS_API_URL, "CITPL", "cfs_in", "cfs_out")

def check_adani_katu(container_no):
    print(f"  [Adani Katu] Checking...")
    try:
        response = requests.get(ADANI_KATU_API_URL, params={"container_no": container_no})
        if response.status_code == 200:
            data = response.json().get("data", {})
            if not data: data = response.json()
            # Kattupalli returns entry_time/exit_time
            return process_cfs_data(container_no, data.get("entry_time"), data.get("exit_time"), "Adani Kattupalli")
    except Exception as e:
        print(f"    Adani Kattu Error: {e}")
    return False, False

def check_adani_ennore(container_no):
    print(f"  [Adani Ennore] Checking...")
    try:
        response = requests.get(ADANI_ENNORE_API_URL, params={"container_no": container_no})
        if response.status_code == 200:
            data = response.json().get("data", {})
            if not data: data = response.json()
            # Ennore returns entry_time/exit_time
            return process_cfs_data(container_no, data.get("entry_time"), data.get("exit_time"), "Adani Ennore")
    except Exception as e:
        print(f"    Adani Ennore Error: {e}")
    return False, False

def check_generic_cfs(container_no, url, name, key_in, key_out):
    print(f"  [{name}] Checking...")
    try:
        response = requests.get(url, params={"container_no": container_no})
        if response.status_code == 200:
            data = response.json().get("data", {})
            return process_cfs_data(container_no, data.get(key_in), data.get(key_out), data.get("cfs_name", ""))
    except Exception as e:
        print(f"    {name} Error: {e}")
    return False, False

def process_cfs_data(container_no, cfs_in, cfs_out, val):
    if cfs_in: post_event(container_no, "CFS In", cfs_in, val)
    if cfs_out: post_event(container_no, "CFS Out", cfs_out, val)
    return bool(cfs_in), bool(cfs_out)

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
        if sticky_provider == "DPW": cfs_in, cfs_out = check_dpw(container_no)
        elif sticky_provider == "CITPL": cfs_in, cfs_out = check_citpl(container_no)
        elif sticky_provider == "Adani_Katu": cfs_in, cfs_out = check_adani_katu(container_no)
        elif sticky_provider == "Adani_Ennore": cfs_in, cfs_out = check_adani_ennore(container_no)
        provider_used = sticky_provider
    else:
        print(f"  [Smart CFS] Discovery Mode (Waterfall)...")
        # Waterfall: DPW -> CITPL -> Katu -> Ennore
        cfs_in, cfs_out = check_dpw(container_no)
        if cfs_in or cfs_out:
            provider_used = "DPW"
        else:
            cfs_in, cfs_out = check_citpl(container_no)
            if cfs_in or cfs_out:
                provider_used = "CITPL"
            else:
                cfs_in, cfs_out = check_adani_katu(container_no)
                if cfs_in or cfs_out:
                    provider_used = "Adani_Katu"
                else:
                    cfs_in, cfs_out = check_adani_ennore(container_no)
                    if cfs_in or cfs_out:
                        provider_used = "Adani_Ennore"
    
    # 3. Handle Transient Lock
    if provider_used:
        if cfs_in and not cfs_out:
            update_provider_map(container_no, provider_used)
        elif cfs_out:
            update_provider_map(container_no, provider_used, clear=True)

# ==========================================
# MAIN
# ==========================================
def main():
    print("=== MAIN ORCHESTRATOR STARTED ===")
    # Services are managed externally
    
    try:
        containers = fetch_active_containers()
        
        # Prioritize CAAU2633856 for Ennore verification
        containers.sort(key=lambda x: x.get("container_no") != "CAAU2633856")

        for container_obj in containers:
            try:
                container_no = container_obj.get("container_no")
                status = container_obj.get("status")
                print(f"\n>>> Processing {container_no} [Status: {status}]")
                
                # STAGE GATING
                rank = get_rank(status)
                
                sealion_data = None
                icegate_result = {"igm": False, "inward": False}

                if rank < get_rank("Arrived at POD"):
                    sealion_data = run_sealion_logic(container_obj)
                    icegate_result = run_icegate_logic(container_obj)
                    run_arrived_logic(container_obj, sealion_data, icegate_result)
                else:
                    print(f"  [Skip] Sealion/Icegate (Status >= Arrived at POD)")

                run_smart_cfs_logic(container_obj)
                
                time.sleep(1)
            except Exception as e:
                print(f"Error processing container {container_no}: {e}")
                
    finally:
        print("=== MAIN ORCHESTRATOR FINISHED ===")

if __name__ == "__main__":
    main()
