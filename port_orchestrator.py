
import os
import json
import requests
import time
from datetime import datetime

# Configuration
API_BASE_URL = "https://uat.trackcontainer.in/api/external"
PORT_API_URL = "http://localhost:8015/search"
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
    # For Port events, value is port_name, date is timestamp
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

def get_arrived_containers(history):
    arrived = set()
    prefix = "|Arrived at POD|"
    for entry in history:
        parts = entry.split("|")
        if len(parts) >= 2 and parts[1] == "Arrived at POD":
            arrived.add(parts[0])
    return list(arrived)

def track_port(container_no, history):
    print(f"Tracking Port details for {container_no}...")
    try:
        response = requests.get(PORT_API_URL, params={"container_no": container_no}, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                data = result.get("data", {})
                port_name = data.get("port_name")
                port_in = data.get("port_in")
                port_out = data.get("port_out")
                
                print(f"  -> Found: In={port_in}, Out={port_out}, Port={port_name}")
                
                # 1. Port In
                if port_in and port_name:
                    post_event(container_no, "Port In", port_in, port_name, history)
                
                # 2. Port Out
                if port_out and port_name:
                    post_event(container_no, "Port Out", port_out, port_name, history)
            else:
                print(f"  -> API returned status: {result.get('status')}")
        else:
            print(f"  -> Failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"  -> Error: {e}")

def main():
    print("=== Port Orchestrator Started ===")
    history = load_history()
    
    # 1. Find containers that have "Arrived at POD"
    arrived_containers = get_arrived_containers(history)
    print(f"Found {len(arrived_containers)} arrived containers: {arrived_containers}")
    
    # 2. Track them
    for container_no in arrived_containers:
        track_port(container_no, history)

    print("=== Port Orchestrator Finished ===")

if __name__ == "__main__":
    main()
