
import os
import json
import requests
import time
from datetime import datetime

# Configuration
API_BASE_URL = "https://uat.trackcontainer.in/api/external"
DPW_API_URL = "http://localhost:8016/dpw"
HISTORY_FILE = "posted_history.json"

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []

def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=4)

def post_event(container_no, status, date, value, history):
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

def get_port_out_containers(history):
    candidates = set()
    # We look for containers that have "Port Out"
    prefix = "|Port Out|"
    for entry in history:
        parts = entry.split("|")
        # Entry format: "Container|Status|Value"
        if len(parts) >= 2 and parts[1] == "Port Out":
            candidates.add(parts[0])
    return list(candidates)

def track_dpw(container_no, history):
    print(f"Tracking DP World CFS for {container_no}...")
    try:
        response = requests.get(DPW_API_URL, params={"container_no": container_no}, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                data = result.get("data", {})
                cfs_name = data.get("cfs_name")
                cfs_in_time = data.get("cfs_in_time")
                cfs_out_time = data.get("cfs_out_time")
                
                print(f"  -> Found: Name={cfs_name}, In={cfs_in_time}, Out={cfs_out_time}")
                
                # Use cfs_name as value if available
                val = cfs_name if cfs_name else ""

                # 1. CFS In
                if cfs_in_time:
                    post_event(container_no, "CFS In", cfs_in_time, val, history)
                
                # 2. CFS Out
                if cfs_out_time:
                    post_event(container_no, "CFS Out", cfs_out_time, val, history)
                    
            else:
                print(f"  -> API return status: {result.get('status')}")
        else:
            print(f"  -> Failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"  -> Error: {e}")

def main():
    print("=== DP World Orchestrator Started ===")
    history = load_history()
    
    # 1. Find containers that have "Port Out"
    active_containers = get_port_out_containers(history)
    print(f"Found {len(active_containers)} containers with 'Port Out': {active_containers}")
    
    # 2. Track them
    for container_no in active_containers:
        track_dpw(container_no, history)

    print("=== DP World Orchestrator Finished ===")

if __name__ == "__main__":
    main()
