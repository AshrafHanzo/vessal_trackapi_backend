import requests
import json
import os
from rapidfuzz import fuzz
from dateutil import parser

# Configuration
LOCAL_API_BASE = "http://localhost:8011"
OUTPUT_FILE = "HALU5671316_iso_payloads.json"

# Specific Container Info
CONTAINER_NO = "HALU5671316"
MBL_NO = "SNKO03C260102172"
BL_NO = "1071770857"
PORT = "Chennai"

def is_fuzzy_match(text, keyword, threshold=80):
    if not text: return False
    score = fuzz.partial_ratio(keyword.lower(), text.lower())
    return score >= threshold

def format_to_iso(date_str):
    if not date_str or str(date_str).upper() in ["N.A.", "NULL", "NONE"]:
        return date_str
    try:
        dt = parser.parse(str(date_str))
        return dt.strftime("%Y-%m-%d")
    except:
        return date_str

def get_sealion_events(container_no):
    to_post = []
    try:
        url = f"{LOCAL_API_BASE}/track?container_number={container_no}"
        resp = requests.get(url, timeout=180).json()
        print(f"Sealion Response: {resp}")
        events = resp.get("events", [])
        for ev in events:
            ev_type = ev.get("event", "")
            print(f"Checking event: {ev_type}")
            if is_fuzzy_match(ev_type, "Departure"):
                to_post.append({"container_no": container_no, "status": "Departed Origin", "date": format_to_iso(ev["date"]), "value": ev["location"]})
            elif is_fuzzy_match(ev_type, "Arrival"):
                to_post.append({"container_no": container_no, "status": "Arrived at POD", "date": format_to_iso(ev["date"]), "value": ev["location"]})
            elif is_fuzzy_match(ev_type, "ETA"):
                to_post.append({"container_no": container_no, "status": "ETA", "date": format_to_iso(ev["date"]), "value": ev["location"]})
    except Exception as e:
        print(f"Error fetching Sealion: {e}")
    return to_post

def get_icegate_events(mbl, port, bl, container_no):
    to_post = []
    try:
        url = f"{LOCAL_API_BASE}/icegate?mbl_no={mbl}&port={port}&bl_no={bl}"
        resp = requests.get(url, timeout=300).json()
        if resp.get("status") == "success":
            data = resp.get("data", {})
            if data.get("igm_no") and data.get("igm_no") != "N.A.":
                to_post.append({"container_no": container_no, "status": "IGM", "date": format_to_iso(data["igm_date"]), "value": data["igm_no"]})
            
            inw = data.get("inw_date")
            if inw and str(inw).upper() not in ["N.A.", "NULL", "NONE"]:
                to_post.append({"container_no": container_no, "status": "Inward", "date": format_to_iso(inw), "value": data.get("igm_no", "IGM")})
    except Exception as e:
        print(f"Error fetching Icegate: {e}")
    return to_post

def run_dry_run():
    print(f"Starting ISO JSON Payload Dry Run for {CONTAINER_NO}...")
    all_collected = []
    all_collected.extend(get_sealion_events(CONTAINER_NO))
    all_collected.extend(get_icegate_events(MBL_NO, PORT, BL_NO, CONTAINER_NO))
    
    POST_ORDER = ["Departed Origin", "IGM", "Inward", "Arrived at POD", "ETA"]
    final_payloads = []
    inward_ready = any(ev["status"] == "Inward" for ev in all_collected)

    for status_target in POST_ORDER:
        matches = [ev for ev in all_collected if ev["status"] == status_target]
        if status_target == "Arrived at POD" and not inward_ready: continue
        for match in matches:
            final_payloads.append(match)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_payloads, f, indent=2)

    print(f"Done! ISO JSON payloads written to {OUTPUT_FILE}")

if __name__ == "__main__":
    run_dry_run()
