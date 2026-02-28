import requests
import json
import time
import os
from datetime import datetime
from rapidfuzz import fuzz
from dateutil import parser

# Configuration
PORTAL_ACTIVE_URL = "https://uat.trackcontainer.in/api/containers/active"
PORTAL_POST_URL = "https://uat.trackcontainer.in/api/shipment-timeline"
LOCAL_API_BASE = "http://localhost:8011"
HISTORY_FILE = "selective_posted_history.json"

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

def safe_requests_get(url, timeout=300, max_retries=3, retry_delay=5):
    """Wait for API response and handle connection errors with retries."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"  [DEBUG] API returned status {resp.status_code}. Retrying in {retry_delay}s...")
        except requests.exceptions.ConnectionError:
            print(f"  [DEBUG] Connection refused (API starting up?). Retrying in {retry_delay}s... ({attempt+1}/{max_retries})")
        except Exception as e:
            print(f"  [DEBUG] Unexpected error: {e}. Retrying...")
        
        time.sleep(retry_delay)
    
    print(f"  [ERROR] Failed to get data from {url} after {max_retries} attempts.")
    return {}

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
    history_key = f"{container_no}|{status}|{date}"
    if history_key in posted_history:
        return True

    payload = {
        "container_no": container_no,
        "status": status,
        "date": date,
        "value": value
    }
    try:
        print(f"  [POSTING] Sending to Portal for {container_no}:")
        print(json.dumps(payload, indent=4))
        response = requests.post(PORTAL_POST_URL, json=payload, timeout=30)
        if response.status_code in [200, 201]:
            posted_history.add(history_key)
            save_history(posted_history)
            time.sleep(1) # Small delay between posts
            return True
        else:
            print(f"  [ERROR] Portal returned {response.status_code} for {status}")
            return False
    except Exception as e:
        print(f"  [ERROR] Posting {status}: {e}")
        return False

def is_fuzzy_match(text, keyword, threshold=80):
    if not text: return False
    score = fuzz.partial_ratio(keyword.lower(), text.lower())
    return score >= threshold

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

def get_sealion_events(container_no, pol=None, pod=None):
    to_post = []
    try:
        # Use query parameter as defined in main.py
        url = f"{LOCAL_API_BASE}/track?container_number={container_no}"
        resp = safe_requests_get(url, timeout=300)
        events = resp.get("events", [])
        for ev in events:
            ev_type = ev.get("event", "")
            ev_loc = ev.get("location", "")

            # Fuzzy match for Departed Origin (Must match port_of_loading if provided)
            if is_fuzzy_match(ev_type, "Departure"):
                if pol:
                    if is_fuzzy_match(ev_loc, pol):
                        to_post.append({"status": "Departed Origin", "date": format_to_iso(ev["date"]), "value": ev["location"]})
                else:
                    # Fallback if no POL provided
                    to_post.append({"status": "Departed Origin", "date": format_to_iso(ev["date"]), "value": ev["location"]})
            
            # Fuzzy match for Arrived at POD (Map Discharge -> Arrived at POD, must match port_of_discharge)
            elif is_fuzzy_match(ev_type, "Discharge"):
                if pod:
                    if is_fuzzy_match(ev_loc, pod):
                        to_post.append({"status": "Arrived at POD", "date": format_to_iso(ev["date"]), "value": ev["location"]})
                else:
                    # Fallback if no POD provided
                    to_post.append({"status": "Arrived at POD", "date": format_to_iso(ev["date"]), "value": ev["location"]})

            # Fuzzy match for ETA (New)
            elif is_fuzzy_match(ev_type, "ETA"):
                to_post.append({"status": "ETA", "date": format_to_iso(ev["date"]), "value": ev["location"]})
    except Exception as e:
        print(f"  [ERROR] Sealion fetch: {e}")
    return to_post

def get_port_events(container_no):
    to_post = []
    try:
        url = f"{LOCAL_API_BASE}/search?container_no={container_no}"
        resp = safe_requests_get(url, timeout=300)
        inland_transit = resp.get("data", {}).get("inland_transit", [])
        for ev in inland_transit:
            status_text = ev.get("status", "")
            if is_fuzzy_match(status_text, "Port In"):
                to_post.append({
                    "status": "Port In", 
                    "date": format_to_iso(ev.get("timestamp") or ev.get("date")), 
                    "value": ev.get("location")
                })
            elif is_fuzzy_match(status_text, "Port Out"):
                to_post.append({
                    "status": "Port Out", 
                    "date": format_to_iso(ev.get("timestamp") or ev.get("date")), 
                    "value": ev.get("location")
                })
            elif is_fuzzy_match(status_text, "CFS In"):
                to_post.append({
                    "status": "CFS In", 
                    "date": format_to_iso(ev.get("timestamp") or ev.get("date")), 
                    "value": ev.get("location")
                })
            elif is_fuzzy_match(status_text, "CFS Out"):
                to_post.append({
                    "status": "CFS Out", 
                    "date": format_to_iso(ev.get("timestamp") or ev.get("date")), 
                    "value": ev.get("location")
                })
    except Exception as e:
        print(f"  [ERROR] Port search fetch (LDB): {e}")
    return to_post

def get_icegate_events(mbl, port, bl, container_no):
    to_post = []
    if not (mbl and port and bl): return to_post
    try:
        url = f"{LOCAL_API_BASE}/icegate?mbl_no={mbl}&port={port}&bl_no={bl}"
        resp = safe_requests_get(url, timeout=300)
        if resp.get("status") == "success":
            data = resp.get("data", {})
            # IGM - Value is igm_no, Date is igm_date
            if data.get("igm_no") and data.get("igm_no") != "N.A.":
                to_post.append({"status": "IGM", "date": format_to_iso(data["igm_date"]), "value": data["igm_no"]})
            
            # Inward - Value is igm_no, Date is inw_date. ONLY IF NOT N.A.
            inw = data.get("inw_date")
            if inw and str(inw).upper() not in ["N.A.", "NULL", "NONE"]:
                to_post.append({"status": "Inward", "date": format_to_iso(inw), "value": data.get("igm_no", "IGM")})
    except Exception as e:
        print(f"  [ERROR] Icegate fetch: {e}")
    return to_post

def run_sync():
    print(f"\n[{datetime.now()}] Starting Sequential Sync Batch (Selective Order)...")
    containers = fetch_active_containers()
    print(f"Retrieved {len(containers)} active containers.")

    processed_cnos = set()
    
    # NEW Strict Priority order for posting
    POST_ORDER = ["Departed Origin", "ETA", "IGM", "Inward", "Arrived at POD", "Port In", "Port Out", "CFS In", "CFS Out"]

    for item in containers:
        raw_cno = item.get("container_no", "")
        bl = item.get("bl_no")
        mbl = item.get("master_bl_no")
        port = item.get("port")
        pol = item.get("port_of_loading")
        pod = item.get("port_of_discharge")
        
        if not (raw_cno and bl and mbl and port): continue
        
        cnos = [c.strip() for c in raw_cno.split("/") if c.strip()]
        for cno in cnos:
            if cno in processed_cnos: continue
            
            print(f"\n" + "="*40)
            print(f"PROCESSING CONTAINER: {cno}")
            print(f"="*40)
            
            # 1. Gather all data first
            all_collected = []
            
            print(f"  Searching Sealion (Departure/Discharge/ETA)...")
            all_collected.extend(get_sealion_events(cno, pol=pol, pod=pod))
            
            print(f"  Searching Port Search (Port In/Out)...")
            all_collected.extend(get_port_events(cno))

            print(f"  Searching Icegate (IGM/Inward)...")
            all_collected.extend(get_icegate_events(mbl, port, bl, cno))
            
            # 2. Strict Sequential Posting
            print(f"  Sequential Posting Start...")
            
            # Check if Arrived at POD is already in history for this container
            is_arrived = any(f"{cno}|Arrived at POD|" in key for key in posted_history)

            for status_target in POST_ORDER:
                # SPECIAL LOGIC FOR ETA: Update every run until Arrived at POD
                if status_target == "ETA":
                    if is_arrived:
                        print(f"    - [SKIP] ETA: Container already 'Arrived at POD'. Stopping ETA updates.")
                        continue
                        
                    matches = [ev for ev in all_collected if ev["status"] == "ETA"]
                    if not matches:
                        print(f"    - [DEBUG] No ETA data found in current tracker response.")
                        # ETA is NOT a blocker for subsequent steps (IGM, etc.), it's optional but updating
                        continue

                    for match in matches:
                        h_key = f"{cno}|ETA|{match['date']}"
                        if h_key in posted_history:
                            print(f"    - [SKIP] ETA {match['date']}: No change (already in history).")
                        else:
                            print(f"    - [UPDATE] ETA changed to {match['date']}. Posting...")
                            post_timeline_event(cno, "ETA", match["date"], match["value"])
                    continue

                # NORMAL LOGIC FOR OTHER STATUSES
                is_in_history = any(f"{cno}|{status_target}|" in key for key in posted_history)
                matches = [ev for ev in all_collected if ev["status"] == status_target]
                
                if not is_in_history and not matches:
                    # BLOCKING CONDITION: Status is missing and not already done
                    print(f"    [STOP] Status '{status_target}' is missing. Skipping remaining sequence for {cno}.")
                    break
                
                if is_in_history:
                    print(f"    - [SKIP] {status_target}: Already posted (in history).")
                    continue

                for match in matches:
                    success = post_timeline_event(cno, match["status"], match["date"], match["value"])
                    if success:
                        print(f"    - Success: {match['status']} moved to next.")
                        time.sleep(1)
                    else:
                        print(f"    - [STOP] Failed to post '{match['status']}'. Breaking sequence.")
                        break
                else:
                    continue
                break
            
            processed_cnos.add(cno)
            print(f"\nCOMPLETED FULL CYCLE FOR: {cno}")
            print("="*40)
            time.sleep(2) # Pause between containers

    print(f"\n[{datetime.now()}] Sequential batch completed.")

if __name__ == "__main__":
    while True:
        try:
            run_sync()
        except KeyboardInterrupt:
            print("\nSelective Sync Manager shut down.")
            break
        except Exception as e:
            print(f"Critical Loop Error: {e}")
        
        print("\nWaiting 30 minutes for next cycle...")
        time.sleep(1800)
