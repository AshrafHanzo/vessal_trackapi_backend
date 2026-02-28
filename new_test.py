import requests
import json
import time
import os
from datetime import datetime
from rapidfuzz import fuzz
from dateutil import parser
from openai import OpenAI

# Configuration for UAT (Company ID 5)
PORTAL_ACTIVE_URL = "https://uat.trackcontainer.in/api/company/active-containers?company_id=5"
PORTAL_POST_URL = "https://uat.trackcontainer.in/api/company/shipment-timeline"
LOCAL_API_BASE = "http://localhost:8011"
HISTORY_FILE = "uat_test_history.json"
COMPANY_ID = 5

# OpenAI Configuration
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)

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
        print(f"Fetching active containers from: {PORTAL_ACTIVE_URL}")
        response = requests.get(PORTAL_ACTIVE_URL, timeout=30)
        if response.status_code == 200:
            return response.json().get("data", [])
        print(f"Error: API returned status {response.status_code}")
        return []
    except Exception as e:
        print(f"Error fetching active containers: {e}")
        return []

def post_timeline_event(container_no, status, date, value):
    history_key = f"{container_no}|{status}|{date}"
    if history_key in posted_history:
        return True

    payload = {
        "company_id": COMPANY_ID,
        "container_no": container_no,
        "status": status,
        "date": date,
        "value": value
    }
    try:
        print(f"  [POSTING UAT] Sending to Portal for {container_no}:")
        print(json.dumps(payload, indent=4))
        response = requests.post(PORTAL_POST_URL, json=payload, timeout=30)
        if response.status_code in [200, 201]:
            posted_history.add(history_key)
            save_history(posted_history)
            time.sleep(1) # Small delay between posts
            return True
        else:
            print(f"  [ERROR] Portal returned {response.status_code} for {status}")
            print(f"  Response: {response.text}")
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


def extract_milestones_with_chatgpt(snapshot, pol=None, pod=None):
    """
    Send the raw snapshot to ChatGPT and ask it to extract shipment milestones.
    Returns a list of milestone dictionaries.
    """
    print(f"  [LLM] Sending snapshot to ChatGPT for intelligent extraction...")
    
    prompt = f"""You are a shipment tracking data parser. I will provide you with raw JSON data from multiple tracking sources (Sealion, Port Search, and Icegate). 

Your task is to extract the following shipment milestones from this data:
1. **Departed Origin**: The departure event from the port of loading. Look for keywords like "Departure", "Sailed", "Left", etc.
2. **ETA**: Estimated Time of Arrival at the destination port.
3. **Arrived at POD**: The discharge/arrival event at the port of discharge. Look for keywords like "Discharge", "Arrived", "Berthed", etc.
4. **Port In**: Container entered the port area. Look for "Port In", "Gate In", "Received at Port", etc.
5. **Port Out**: Container left the port area. Look for "Port Out", "Gate Out", "Released from Port", etc.
6. **CFS In**: Container entered the Container Freight Station. Look for "CFS In", "CFS Gate In", etc.
7. **CFS Out**: Container left the Container Freight Station. Look for "CFS Out", "CFS Gate Out", etc.
8. **IGM**: Import General Manifest number and date.
9. **Inward**: Inward date from customs.

**Important matching rules:**
- For "Departed Origin": If port_of_loading is provided ({pol}), only match departure events from that location.
- For "Arrived at POD": If port_of_discharge is provided ({pod}), only match arrival/discharge events at that location.
- Use fuzzy/semantic matching - don't require exact keyword matches.
- Extract the most specific timestamp available (prefer "timestamp" over "date" fields).
- Return ONLY valid milestones found in the data.

Return your response as a JSON array of milestone objects with this exact structure:
[
  {{"status": "Departed Origin", "date": "YYYY-MM-DD", "value": "Location"}},
  {{"status": "ETA", "date": "YYYY-MM-DD", "value": "Location"}},
  ...
]

If a milestone is not found, do not include it in the array.

Here is the raw tracking data:
{json.dumps(snapshot, indent=2)}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise shipment data extraction assistant. Always return valid JSON arrays."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=2000
        )
        
        llm_response = response.choices[0].message.content.strip()
        
        # Extract JSON from markdown code blocks if present
        if "```json" in llm_response:
            llm_response = llm_response.split("```json")[1].split("```")[0].strip()
        elif "```" in llm_response:
            llm_response = llm_response.split("```")[1].split("```")[0].strip()
        
        milestones = json.loads(llm_response)
        print(f"  [LLM] Successfully extracted {len(milestones)} milestones.")
        return milestones
        
    except Exception as e:
        print(f"  [ERROR] ChatGPT extraction failed: {e}")
        print(f"  [FALLBACK] Using empty milestone list.")
        return []

import concurrent.futures
import threading

# Global lock for thread-safe history updates
history_lock = threading.Lock()

def process_single_container(cno, item):
    """
    Process a single container: Snapshot -> Extract -> Post.
    This function will be run in parallel threads.
    """
    bl = item.get("bl_no")
    mbl = item.get("master_bl_no")
    port = item.get("port")
    pol = item.get("port_of_loading")
    pod = item.get("port_of_discharge")
    
    print(f"\n" + "="*40)
    print(f"PROCESSING UAT SNAPSHOT: {cno}")
    print(f"="*40)
    
    # Persistent file to store ALL raw data
    json_dir = "json.data"
    save_file = os.path.join(json_dir, f"{cno}.json")
    
    # Step 1: Collect RAW data from all sources
    print(f"  [{cno}] Fetching raw data from all trackers...")
    
    sealion_raw = safe_requests_get(f"{LOCAL_API_BASE}/track?container_number={cno}")
    port_raw = safe_requests_get(f"{LOCAL_API_BASE}/search?container_no={cno}")
    icegate_raw = {}
    if bl and mbl and port:
        icegate_raw = safe_requests_get(f"{LOCAL_API_BASE}/icegate?mbl_no={mbl}&port={port}&bl_no={bl}")
    else:
        print(f"  [{cno}] [SKIP] Icegate raw fetch (Missing BL/MBL/Port data).")

    snapshot = {
        "container_no": cno,
        "metadata": {
            "pol": pol,
            "pod": pod,
            "bl": bl,
            "mbl": mbl,
            "port": port,
            "fetched_at": str(datetime.now())
        },
        "sources": {
            "sealion": sealion_raw,
            "port_search": port_raw,
            "icegate": icegate_raw
        }
    }
    
    # Step 2: Use ChatGPT to extract milestones
    all_collected = extract_milestones_with_chatgpt(snapshot, pol=pol, pod=pod)
    
    # Step 3: Save the snapshot WITH extracted milestones
    snapshot["extracted_milestones"] = all_collected
    
    try:
        print(f"  [{cno}] [STORAGE] Saving complete snapshot...")
        with open(save_file, "w") as f:
            json.dump(snapshot, f, indent=4)
    except Exception as e:
        print(f"  [{cno}] [ERROR] Failed to save snapshot: {e}")

    # Step 4: Posting logic
    print(f"  [{cno}] Sequential Posting Start...")
    
    # Check history safely with lock NOT strictly required for reading if we rely on set, 
    # but posting needs lock to update file correctly.
    # Actually, post_timeline_event handles the lock/save logic, so we just check the set.
    # But for thread safety, let's look at the set.
    
    is_arrived = any(f"{cno}|Arrived at POD|" in key for key in posted_history)
    POST_ORDER = ["Departed Origin", "ETA", "IGM", "Inward", "Arrived at POD", "Port In", "Port Out", "CFS In", "CFS Out"]

    for status_target in POST_ORDER:
        if status_target == "ETA":
            if is_arrived:
                print(f"    [{cno}] - [SKIP] ETA: Container already 'Arrived at POD'.")
                continue
                
            matches = [ev for ev in all_collected if ev["status"] == "ETA"]
            if not matches:
                continue

            for match in matches:
                h_key = f"{cno}|ETA|{match['date']}"
                if h_key in posted_history:
                    print(f"    [{cno}] - [SKIP] ETA {match['date']}: No change.")
                else:
                    print(f"    [{cno}] - [UPDATE] ETA changed to {match['date']}. Posting...")
                    post_timeline_event(cno, "ETA", match["date"], match["value"])
            continue

        is_in_history = any(f"{cno}|{status_target}|" in key for key in posted_history)
        matches = [ev for ev in all_collected if ev["status"] == status_target]
        
        if not is_in_history and not matches:
            print(f"    [{cno}] [STOP] Status '{status_target}' is missing. Breaking sequence.")
            break
        
        if is_in_history:
            print(f"    [{cno}] - [SKIP] {status_target}: Already posted.")
            continue

        for match in matches:
            success = post_timeline_event(cno, match["status"], match["date"], match["value"])
            if success:
                print(f"    [{cno}] - Success: {match['status']} posted.")
                time.sleep(2) 
            else:
                print(f"    [{cno}] - [STOP] Failed to post '{match['status']}'.")
                break
        else:
            continue
        break
    
    print(f"[{cno}] DONE.")
    return cno

def run_sync():
    print(f"\n[{datetime.now()}] Starting UAT PARALLEL Sync Batch (Company {COMPANY_ID})...")
    containers = fetch_active_containers()
    if not containers:
        print(f"No active containers found for Company {COMPANY_ID}.")
        return
        
    print(f"Retrieved {len(containers)} active containers.")

    processed_cnos = set()

    # Ensure json.data directory exists
    json_dir = "json.data"
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)
        print(f"Created directory: {json_dir}")

    # Prepare list of tasks
    tasks = []
    
    for item in containers:
        raw_cno = item.get("container_no", "")
        if not raw_cno: continue
        
        cnos = [c.strip() for c in raw_cno.split("/") if c.strip()]
        for cno in cnos:
            if cno in processed_cnos: continue
            
            tasks.append((cno, item))
            processed_cnos.add(cno)

    # Run in parallel with ThreadPoolExecutor
    max_threads = 5 # Limit concurrency to 5
    print(f"Starting execution with {max_threads} concurrent threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(process_single_container, cno, item): cno for cno, item in tasks}
        
        for future in concurrent.futures.as_completed(futures):
            cno = futures[future]
            try:
                future.result()
                print(f"✅ Finished processing: {cno}")
            except Exception as e:
                print(f"❌ Error processing {cno}: {e}")

    print(f"\n[{datetime.now()}] UAT batch completed.")

if __name__ == "__main__":
    try:
        run_sync()
    except KeyboardInterrupt:
        print("\nUAT Sync interrupted.")
    except Exception as e:
        print(f"Critical Error: {e}")
