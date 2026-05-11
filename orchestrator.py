
import os
import sys
import json
import time
import subprocess
import requests
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = "https://uat.trackcontainer.in/api/external"
SEALION_API_URL = "http://localhost:8012/sealion"
HISTORY_FILE = "posted_history.json" # Kept for variable compat, but unused logic

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SEALION_DIR = os.path.join(SCRIPT_DIR, "Sealion")
SEALION_MAIN = os.path.join(SEALION_DIR, "main.py")
SEALION_VENV_PYTHON = os.path.join(SEALION_DIR, "venv", "Scripts", "python.exe")

# ==========================================
# SHARED UTILS
# ==========================================

def normalize_date(date_str):
    """Normalize date strings for comparison."""
    if not date_str: return None
    # Try API format (%Y-%m-%d %H:%M:%S)
    try: return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except: pass
    # Try Sealion format (%d %b %Y %H:%M or %d %b %Y)
    try: return datetime.strptime(date_str, "%d %b %Y %H:%M")
    except: pass
    try: return datetime.strptime(date_str, "%d %b %Y")
    except: pass
    return None

def post_event(container_no, status, date, value):
    """Post event to timeline. Stateless."""
    payload = {
        "container_no": container_no,
        "status": status,
        "date": date if date else "",
        "value": value if value else ""
    }
    
    print(f"  [POST] Pushing {status}...")
    try:
        response = requests.post(f"{API_BASE_URL}/shipment-timeline", json=payload)
        # response.raise_for_status() 
        
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

def start_sealion_server():
    print("Starting Sealion API server...")
    os.system('taskkill /F /IM python.exe /FI "WINDOWTITLE eq Sealion*" >nul 2>&1')
    process = subprocess.Popen(
        [SEALION_VENV_PYTHON, SEALION_MAIN],
        cwd=SEALION_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    print("Waiting for server to initialize...")
    time.sleep(15)
    return process

def get_sealion_data(container_no):
    print(f"Tracking {container_no} via Sealion API...")
    try:
        cmd = [
            "curl", 
            "-s", 
            "--max-time", "180", 
            f"{SEALION_API_URL}?container_number={container_no}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            print(f"Error fetching Sealion data: {result.stderr}")
            return None
    except Exception as e:
        print(f"Exception tracking container: {e}")
        return None

def sync_job_details(container_no, sealion_data):
    print(f"  [SYNC] Syncing Job Details to External API...")
    
    vessel_details = sealion_data.get("vessel_details", {})
    
    c_type = sealion_data.get("container_type")
    if c_type is None: c_type = ""

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
        response = requests.post(f"{API_BASE_URL}/sync-job-details", json=payload)
        if response.status_code in [200, 201]:
            print("    -> Sync Success")
        else:
            print(f"    -> Sync Failed: {response.text}")
    except Exception as e:
        print(f"    -> Sync Error: {e}")

def process_container(container_obj):
    container_no = container_obj.get("container_no")
    api_status = container_obj.get("status", "")
    api_eta = container_obj.get("eta_date")

    data = get_sealion_data(container_no)
    if not data:
        print("  No data returned from Sealion.")
        return

    # 1. ALWAYS SYNC HEADER
    sync_job_details(container_no, data)

    # 2. DEPARTED ORIGIN (Only if Created)
    dep_origin = data.get("Departed Origin")
    dep_date = data.get("Departed Date")
    
    if api_status in ["Created", "Empty Return"] and dep_origin and dep_date:
        post_event(container_no, "Departed Origin", dep_date, dep_origin)

    # 3. SMART ETA
    current_status = data.get("Current Status") 
    status_date = data.get("Status Date")
    arrived_loc = data.get("Arrived Location")
    
    if current_status == "ETA" and status_date and api_status != "Arrived at POD":
        if normalize_date(status_date) != normalize_date(api_eta):
            post_event(container_no, "ETA", status_date, "")

    # 4. ARRIVED AT POD (Logic Gate)
    elif (current_status == "ATA" or current_status == "Arrived") and status_date and arrived_loc:
        # Check if Icegate is done based on API Status
        # If orchestrator.py is standalone/Sealion-only, it relies on Main to push Icegate, 
        # OR it relies on the status eventually updating.
        if api_status in ["IGM Filed", "Inward Entry"]:
            post_event(container_no, "Arrived at POD", status_date, arrived_loc)
        else:
            print(f"  [HOLD] 'Arrived at POD' found but API Status '{api_status}' implies Icegate Pending.")

def main():
    print("=== Orchestrator Started (Stateless) ===")
    
    candidates = fetch_active_containers()
    
    if not candidates:
        print("No candidates found. Exiting.")
        return

    server_process = start_sealion_server()
    
    try:
        for container_obj in candidates:
            print(f"\nProcessing {container_obj.get('container_no')}...")
            process_container(container_obj)
            print(f"Finished.")
            
    finally:
        print("\nStopping Sealion Server...")
        server_process.terminate()
        os.system('taskkill /F /IM python.exe /FI "WINDOWTITLE eq Sealion*" >nul 2>&1')
        print("=== Orchestrator Finished ===")

if __name__ == "__main__":
    main()
