
import os
import json
import requests
import time
from datetime import datetime

# Configuration
API_BASE_URL = "https://uat.trackcontainer.in/api/external"
ADANI_API_URL = "http://localhost:8018/adani"
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

def fetch_active_containers():
    print("Fetching active containers from API...")
    try:
        response = requests.get(f"{API_BASE_URL}/containers/active")
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return data.get("data", [])
    except Exception as e:
        print(f"Error fetching active containers: {e}")
    return []

def track_adani(container_no, history):
    print(f"Tracking Adani Kattupalli for {container_no}...")
    try:
        response = requests.get(ADANI_API_URL, params={"container_no": container_no}, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get("status") == "success":
                data = result.get("data", {})
                dest_code = data.get("destination_code")
                entry_time = data.get("entry_time")
                exit_time = data.get("exit_time")

                print(f"  -> Found: Entry={entry_time}, Exit={exit_time}")
                
                val = dest_code if dest_code else "Adani Kattupalli"

                # 1. CFS In (from Entry Time)
                if entry_time:
                    post_event(container_no, "CFS In", entry_time, val, history)
                
                # 2. CFS Out (from Exit Time)
                if exit_time:
                    post_event(container_no, "CFS Out", exit_time, val, history)
                    
            else:
                print(f"  -> API return status: {result.get('status')}")
        else:
            print(f"  -> Failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"  -> Error: {e}")

def main():
    print("=== Adani Orchestrator Started ===")
    
    # We load history only to prevent DUPLICATE pushes, NOT for flow control.
    # Flow control is done via API status check as requested.
    history = load_history()
    
    containers = fetch_active_containers()
    print(f"Found {len(containers)} active containers.")
    
    count = 0
    for container in containers:
        c_no = container.get("container_no")
        status = container.get("status")
        
        # TRIGGER CHECK: Status must be "Port Out"
        if status == "Port Out":
            print(f"\n[MATCH] {c_no} has status '{status}'. Running Adani Tracker...")
            track_adani(c_no, history)
            count += 1
        else:
            # Optional: Print skipped ones or too noisy?
            # print(f"[SKIP] {c_no} status is '{status}'")
            pass

    print(f"\n=== Adani Orchestrator Finished. Processed {count} matching containers. ===")

if __name__ == "__main__":
    main()
