
import os
import json
import requests
import time
from datetime import datetime

# Configuration
API_BASE_URL = "https://uat.trackcontainer.in/api/external"
CFS_API_URL = "http://localhost:8014/citpl"
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
    prefix = "|Port Out|"
    for entry in history:
        # Check if "Port Out" is in the entry string
        # Entry format: "Container|Status|Value"
        parts = entry.split("|")
        if len(parts) >= 2 and parts[1] == "Port Out":
            candidates.add(parts[0])
    return list(candidates)

def track_cfs(container_no, history):
    print(f"Tracking CFS details for {container_no}...")
    try:
        response = requests.get(CFS_API_URL, params={"container_no": container_no}, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                data = result.get("data", {})
                cfs_in = data.get("cfs_in")
                cfs_out = data.get("cfs_out")
                scan = data.get("scan")
                
                print(f"  -> Found: In={cfs_in}, Out={cfs_out}, Scan={scan}")
                
                # 1. CFS In
                if cfs_in:
                    post_event(container_no, "CFS In", cfs_in, "", history)
                
                # 2. CFS Out
                if cfs_out:
                    post_event(container_no, "CFS Out", cfs_out, "", history)
                    
            else:
                print(f"  -> API returned status: {result.get('status')}")
        else:
            print(f"  -> Failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"  -> Error: {e}")

def main():
    print("=== CFS Orchestrator Started ===")
    history = load_history()
    
    # 1. Find containers that have "Port Out"
    active_containers = get_port_out_containers(history)
    print(f"Found {len(active_containers)} containers with 'Port Out': {active_containers}")
    
    # 2. Track them
    for container_no in active_containers:
        track_cfs(container_no, history)

    print("=== CFS Orchestrator Finished ===")

if __name__ == "__main__":
    main()
