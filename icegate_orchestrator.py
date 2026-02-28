
import os
import json
import requests
import time
from datetime import datetime

# Configuration
API_BASE_URL = "https://uat.trackcontainer.in/api/external"
ICEGATE_API_URL = "http://localhost:8013/icegate"
HISTORY_FILE = "posted_history.json"

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []

def get_port_from_history(container_no, history):
    # Search history for "Container|port_of_discharge|Value"
    prefix = f"{container_no}|port_of_discharge|"
    for entry in history:
        if entry.startswith(prefix):
            return entry.split("|")[2] # Return the value part
    return None

def fetch_active_containers():
    print("Fetching active containers...")
    try:
        response = requests.get(f"{API_BASE_URL}/containers/active")
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "success":
            return data.get("data", [])
    except Exception as e:
        print(f"Error fetching containers: {e}")
    return []

def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=4)

def post_event(container_no, status, date, value, history):
    # Create a unique key for this event to verify against history
    # Key format: Container|Status|Value (or Date if Value is empty)
    check_val = value if value else date
    history_key = f"{container_no}|{status}|{check_val}"
    
    if history_key in history:
        print(f"  [SKIP] Event already pushed: {status} -> {check_val}")
        return False
    
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
            history.append(history_key)
            save_history(history)
            return True
        else:
            print(f"    -> Failed: {response.text}")
            return False
    except Exception as e:
        print(f"    -> Error: {e}")
        return False

def track_icegate(container_no, mbl_no, bl_no, port, history):
    print(f"Tracking Icegate for {container_no} (MBL: {mbl_no}, BL: {bl_no}, Port: {port})...")
    try:
        params = {
            "mbl_no": mbl_no,
            "bl_no": bl_no,
            "port": port
        }
        response = requests.get(ICEGATE_API_URL, params=params, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            print(f"  -> Success: {result}")
            
            # Save for analysis
            with open("icegate_last_result.json", "w") as f:
                json.dump(result, f, indent=4)
                
            # Process Data
            data = result.get("data", {})
            if not data:
                return 

            # 1. IGM Filed
            igm_no = data.get("igm_no")
            igm_date = data.get("igm_date")
            if igm_date:
                # Use IGM No as value if available, else empty
                val = igm_no if igm_no else ""
                post_event(container_no, "IGM Filed", igm_date, val, history)
            
            # 2. Inward Entry
            inw_date = data.get("inw_date")
            if inw_date:
                post_event(container_no, "Inward Entry", inw_date, "", history)
                
            return result
        else:
            print(f"  -> Failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"  -> Error: {e}")
        return None

def main():
    print("DEBUG: Script Launching...", flush=True)
    print("=== Icegate Orchestrator Started ===", flush=True)
    history = load_history()
    print(f"DEBUG: Loaded history entries: {len(history)}", flush=True)
    containers = fetch_active_containers()
    
    print(f"Found {len(containers)} active containers.")
    
    for c in containers:
        container_no = c.get("container_no")
        mbl_no = c.get("master_bl_no")
        bl_no = c.get("bl_no")
        api_port = c.get("port_of_discharge")
        
        # Resolve Port
        port = api_port if api_port else get_port_from_history(container_no, history)
        
        # Validation checks
        if not mbl_no:
            print(f"Skipping {container_no}: Missing Master BL")
            continue
        if not bl_no:
            print(f"Skipping {container_no}: Missing BL")
            continue
        if not port:
            print(f"Skipping {container_no}: Missing Port of Discharge (Not in API or History)")
            continue
            
        # If all valid, process
        track_icegate(container_no, mbl_no, bl_no, port, history)

    print("=== Icegate Orchestrator Finished ===")

if __name__ == "__main__":
    main()
