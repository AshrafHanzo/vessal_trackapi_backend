import requests
import json
import time
import os
from datetime import datetime
from dateutil import parser

# Configuration
PORTAL_ACTIVE_URL = "https://uat.trackcontainer.in/api/containers/active"
PORTAL_POST_URL = "https://uat.trackcontainer.in/api/shipment-timeline"
LOCAL_API_BASE = "http://localhost:8011"
HISTORY_FILE = "posted_history.json"

# Load persistent history (ContainerNo|Status|Date)
def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def save_history(history):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(list(history), f)
    except Exception as e:
        print(f"Error saving history: {e}")

# Global history set
posted_history = load_history()

def format_to_iso(date_str):
    """Converts various date formats to YYYY-MM-DD."""
    if not date_str or str(date_str).upper() in ["N.A.", "NULL", "NONE"]:
        return date_str
    try:
        dt = parser.parse(str(date_str))
        return dt.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"  [DEBUG] Date parse failed for '{date_str}': {e}")
        return date_str

def fetch_active_containers():
    try:
        response = requests.get(PORTAL_ACTIVE_URL, timeout=30)
        if response.status_code == 200:
            return response.json().get("data", [])
        return []
    except Exception as e:
        print(f"Error fetching active containers: {e}")
        return []

def post_timeline_event(container_no, status, date, value):
    # Unique key for this event to avoid duplicates
    history_key = f"{container_no}|{status}|{date}"
    
    if history_key in posted_history:
        # Already posted this exact event
        return True

    payload = {
        "container_no": container_no,
        "status": status,
        "date": date,
        "value": value
    }
    try:
        print(f"  [POSTING] {status} for {container_no} ({date})...")
        response = requests.post(PORTAL_POST_URL, json=payload, timeout=30)
        
        if response.status_code in [200, 201]:
            # Add to history and save
            posted_history.add(history_key)
            save_history(posted_history)
            time.sleep(1) # Sequentiality pause
            return True
        else:
            print(f"  [ERROR] Portal returned {response.status_code} for {status}")
            return False
    except Exception as e:
        print(f"  [ERROR] Posting {status}: {e}")
        return False

def sync_sealion(container_no):
    try:
        url = f"{LOCAL_API_BASE}/track?container_number={container_no}"
        resp = requests.get(url, timeout=180).json()
        events = resp.get("events", [])
        for ev in events:
            ev_type = ev.get("event", "").lower()
            if "departure" in ev_type:
                post_timeline_event(container_no, "Departed Origin", format_to_iso(ev["date"]), ev["location"])
            elif "arrival" in ev_type:
                post_timeline_event(container_no, "Arrived at POD", format_to_iso(ev["date"]), ev["location"])
    except Exception as e:
        print(f"  [ERROR] Sealion sync: {e}")

def sync_icegate(mbl, port, bl, container_no):
    if not (mbl and port and bl): return
    try:
        url = f"{LOCAL_API_BASE}/icegate?mbl_no={mbl}&port={port}&bl_no={bl}"
        resp = requests.get(url, timeout=300).json()
        if resp.get("status") == "success":
            data = resp.get("data", {})
            if data.get("igm_no"):
                post_timeline_event(container_no, "IGM", format_to_iso(data["igm_date"]), data["igm_no"])
            if data.get("inw_date"):
                post_timeline_event(container_no, "Inward", format_to_iso(data["inw_date"]), data.get("igm_no", "IGM"))
    except Exception as e:
        print(f"  [ERROR] Icegate sync: {e}")

def sync_ldb(container_no):
    try:
        print(f"    - Running LDB Tracker...")
        url = f"{LOCAL_API_BASE}/search?container_no={container_no}"
        resp = requests.get(url, timeout=300).json()
        all_events = resp.get("data", {}).get("all_events_sorted", [])
        for ev in all_events:
            status_text = ev.get("status", "").upper()
            if "PORT IN" in status_text:
                post_timeline_event(container_no, "Discharged", ev["date"], ev["location"])
            elif "PORT OUT" in status_text:
                post_timeline_event(container_no, "Gate Out", ev["date"], ev["location"])
            elif "CFS IN" in status_text:
                post_timeline_event(container_no, "CFS", ev["date"], ev["location"])
    except Exception as e:
        print(f"    [ERROR] LDB sync: {e}")

def run_sync():
    print(f"\n[{datetime.now()}] Starting Sequential Sync Batch...")
    containers = fetch_active_containers()
    print(f"Retrieved {len(containers)} active containers.")

    processed_cnos = set()

    for item in containers:
        raw_cno = item.get("container_no", "")
        bl = item.get("bl_no")
        mbl = item.get("master_bl_no")
        port = item.get("port")
        
        # Strict validation: All 4 fields must be present and non-null
        if not (raw_cno and bl and mbl and port):
            continue
        
        # Split container numbers if they contain a slash
        cnos = [c.strip() for c in raw_cno.split("/") if c.strip()]
        for cno in cnos:
            if cno in processed_cnos:
                continue
            
            print(f"\n" + "="*40)
            print(f"PROCESSING CONTAINER: {cno}")
            print(f"="*40)
            
            # Run trackers sequentially
            sync_sealion(cno)
            sync_ldb(cno)
            sync_icegate(mbl, port, bl, cno)
            
            processed_cnos.add(cno)
            print(f"\nCOMPLETED FULL CYCLE FOR: {cno}")
            print("="*40)
            time.sleep(2) # Finish pause

    print(f"\n[{datetime.now()}] Sequential batch completed.")

if __name__ == "__main__":
    while True:
        try:
            run_sync()
        except KeyboardInterrupt:
            print("\nSync Manager shut down.")
            break
        except Exception as e:
            print(f"Critical Loop Error: {e}")
        
        print("\nWaiting 30 minutes for next sync cycle...")
        time.sleep(1800)
