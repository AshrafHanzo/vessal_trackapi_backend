import sys
import json
import time
import re
from playwright.sync_api import sync_playwright

def track_ldb(container_no: str, mode: str = "port"):
    """
    Track container using LDB website
    
    Args:
        container_no: Container number
    """
    result = {
        "container_no": container_no,
        "status": "pending",
        "message": "Browser opened for manual inspection"
    }
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        
        try:
            page = browser.new_page()
            
            # 3. Persistent Input Strategy with Retry and 5s Deadline
            found_input = False
            for attempt in range(1, 4):
                sys.stderr.write(f"DEBUG: Input attempt {attempt}...\n")
                
                try:
                    # 1. Faster Page Load Strategy
                    sys.stderr.write(f"DEBUG: Loading page (domcontentloaded) for attempt {attempt}...\n")
                    # Use domcontentloaded as networkidle can take forever on LDB
                    page.goto("https://www.ldb.co.in/ldb/containersearch", wait_until="domcontentloaded", timeout=60000)
                    
                    # Start the 5s timer IMMEDIATELY after navigation starts returning
                    load_start_time = time.time()
                    
                    # 2. Aggressive Interaction/Verification Loop
                    sys.stderr.write(f"DEBUG: Entering 5s aggressive interaction loop...\n")
                    while time.time() - load_start_time < 5:
                        try:
                            # 2.1 Handle Cookie Banner (Accept) - don't wait, just try
                            page.evaluate("""
                                (function() {
                                    let acceptBtn = Array.from(document.querySelectorAll('button')).find(b => b.innerText.includes('Accept'));
                                    if (acceptBtn) acceptBtn.click();
                                })()
                            """)

                            # 2.2 Ensure we are on Container/Single tab if not already
                            # LDB 2.0 defaults to these, but good to be sure via JS
                            page.evaluate("""
                                (function() {
                                    let containerTab = Array.from(document.querySelectorAll('.nav-link, button')).find(t => t.innerText === 'Container');
                                    if (containerTab && !containerTab.classList.contains('active')) containerTab.click();
                                    
                                    let singleTab = Array.from(document.querySelectorAll('.nav-link, button')).find(t => t.innerText === 'Single');
                                    if (singleTab && !singleTab.classList.contains('active')) singleTab.click();
                                })()
                            """)

                            # 2.3 Attempt to fill the input via JS (jspath style)
                            page.evaluate(f"""
                                (function() {{
                                    let input = document.querySelector('#cntrNo') || 
                                                document.querySelector('input[placeholder*="Enter Container No."]') ||
                                                Array.from(document.querySelectorAll('input')).find(i => i.placeholder?.includes('Container'));
                                    if (input) {{
                                        if (input.value !== '{container_no}') {{
                                            input.focus();
                                            input.value = '{container_no}';
                                            input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                            input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                                        }}
                                        return true;
                                    }}
                                    return false;
                                }})()
                            """)
                            
                            # 2.4 VERIFY value
                            current_val = page.evaluate("""
                                (function() {
                                    let input = document.querySelector('#cntrNo') || 
                                                document.querySelector('input[placeholder*="Enter Container No."]') ||
                                                Array.from(document.querySelectorAll('input')).find(i => i.placeholder?.includes('Container'));
                                    return input ? input.value : null;
                                })()
                            """)
                            
                            if current_val == container_no:
                                sys.stderr.write(f"DEBUG: Input VERIFIED via jspath in {round(time.time() - load_start_time, 2)}s.\n")
                                found_input = True
                                break
                        except:
                            pass
                        time.sleep(0.3) # Fast polling

                    if found_input:
                        break
                        
                except Exception as e:
                    sys.stderr.write(f"DEBUG: Attempt {attempt} error: {e}\n")
                
                sys.stderr.write(f"DEBUG: Attempt {attempt} failed within 5s or timed out. Refreshing...\n")
            
            if not found_input:
                sys.stderr.write("ERROR: Failed to input container number after 3 attempts with 5s deadlines.\n")
            
            # 4. Click Search Button
            sys.stderr.write("DEBUG: Clicking Search Button...\n")
            # The search button is the magnifying glass icon next to the input
            # In screenshot it looks like it might be a button with an <i> or <img> inside
            try:
                # Try finding the magnifying glass icon inside a button
                search_btn = page.locator("button:has(.fa-search), button:has(img[src*='search']), .search-btn").first
                if not search_btn.is_visible():
                    # Fallback to the one provided by user
                    search_btn = page.locator("#myHeader > div > button").first
                
                if search_btn.is_visible(timeout=3000):
                    search_btn.click()
                    sys.stderr.write("DEBUG: Clicked search button.\n")
                else:
                    # Last resort: find button near input
                    page.keyboard.press("Enter")
                    sys.stderr.write("DEBUG: Search button not found, pressed Enter.\n")
            except Exception as e:
                sys.stderr.write(f"DEBUG: Search click failed: {e}\n")
                page.keyboard.press("Enter")
                
            sys.stderr.write("DEBUG: Action performed. Waiting for initial results...\n")
            page.wait_for_timeout(5000) # Give results time to appear
            
            # Expand "Import Voyage Information" BEFORE scraping as requested by user
            # This ensures both Transit and Voyage data are loaded/visible in the DOM
            sys.stderr.write("DEBUG: expanding 'Import Voyage Information' before scraping...\n")
            try:
                # Use a specific selector for the Voyage header to be sure
                # On LDB 2.0 it's often a button or div with this text
                voyage_btn = page.get_by_text("Import Voyage Information").first
                if voyage_btn.is_visible(timeout=5000):
                    voyage_btn.click()
                    sys.stderr.write("DEBUG: Clicked 'Import Voyage Information' header.\n")
                    page.wait_for_timeout(3000) # Wait for section to expand and load data
                else:
                    # Alternative: click by selector if text click fails
                    page.click("text=Import Voyage Information")
                    page.wait_for_timeout(3000)
            except Exception as e:
                sys.stderr.write(f"DEBUG: Could not click 'Import Voyage Information' (might be already expanded or not found): {e}\n")
            
            # Now that everything is expanded, extract data using BeautifulSoup
            sys.stderr.write("DEBUG: Starting web scrape (BeautifulSoup)...\n")
            from bs4 import BeautifulSoup
            html = page.content()
            soup = BeautifulSoup(html, 'lxml')
            
            extracted_data = {}
            
            # Remove redundant generic table extraction to focus on timeline
            # (If you need basic tables back, we can add them later)
            extracted_data["tables"] = []
            
            # 2. Advanced Stream Extraction with Strict Boundaries
            # To prevent section leakage, we get ALL strings and slice them by header indices
            all_raw_text = [t.strip() for t in soup.stripped_strings if t.strip()]
            
            # Find boundaries in the global text stream
            transit_idx = -1
            voyage_idx = -1
            for i, t in enumerate(all_raw_text):
                if "Inland Transit Information" in t: transit_idx = i
                if "Import Voyage Information" in t: voyage_idx = i
            
            sys.stderr.write(f"DEBUG: Section Indices - Transit: {transit_idx}, Voyage: {voyage_idx}\n")
            
            # Helper to parse events from a slice of text
            def parse_text_slice(text_slice, is_voyage=False):
                events = []
                date_pattern = re.compile(r'^\d{2}-\d{2}-\d{4}$')
                current = None
                
                # Keywords that define a new "status" line
                status_keywords = ["PORT IN", "PORT OUT", "CFS IN", "CFS OUT", "TEU", "LADEN", "EMPTY", "VESSEL", "VOYAGE", "DISCHARGE"]
                
                type_tag = "Voyage" if is_voyage else ""
                
                for text in text_slice:
                    is_date = bool(date_pattern.match(text))
                    is_timestamp = "IST" in text or re.search(r'\d{2}:\d{2}:\d{2}', text)
                    is_status = any(k in text.upper() for k in status_keywords)
                    
                    if is_date:
                        if current: events.append(current)
                        current = {"date": text, "location": "", "status": "", "timestamp": "", "details": []}
                        if type_tag: current["type"] = type_tag
                    elif current:
                        # If we find a SECOND timestamp OR a new Status keyword AFTER the previous timestamp was set,
                        # it means we are starting a sub-event in the same card/date block.
                        if (is_timestamp or is_status) and current["timestamp"]:
                            events.append(current)
                            current = {
                                "date": current["date"], # Inherit block date
                                "location": current["location"], # Inherit location
                                "status": "",
                                "timestamp": "",
                                "details": []
                            }
                            if type_tag: current["type"] = type_tag
                        
                        if is_timestamp:
                            current["timestamp"] = text
                            # Extract specific date from timestamp if present (PRIORITY over block date)
                            inner_date = re.search(r'\d{2}-\d{2}-\d{4}', text)
                            if inner_date: current["date"] = inner_date.group()
                        elif not current["location"] and not is_status:
                            current["location"] = text
                        else:
                            current["details"].append(text)
                            if is_status:
                                if not current["status"]: current["status"] = text
                                else: current["status"] += " " + text
                
                if current and (current["status"] or current["timestamp"]):
                    events.append(current)
                return events

            # Extract Transit Slice (between transit and voyage headers)
            timeline_data = []
            voyage_data = []

            if transit_idx != -1:
                end_slice = voyage_idx if voyage_idx > transit_idx else len(all_raw_text)
                transit_slice = all_raw_text[transit_idx + 1:end_slice]
                timeline_data = parse_text_slice(transit_slice, is_voyage=False)
                extracted_data["inland_transit"] = timeline_data
            
            # Extract Voyage Slice (after voyage header)
            if voyage_idx != -1:
                # Stop Voyage slice if we hit common footer markers
                voyage_slice = []
                for t in all_raw_text[voyage_idx + 1:]:
                    if any(x in t for x in ["Disclaimer", "Copyright", "Site Map"]): break
                    voyage_slice.append(t)
                voyage_data = parse_text_slice(voyage_slice, is_voyage=True)
                extracted_data["import_voyage"] = voyage_data
            
            # 4. Global Sorting & Deduplication
            all_events = timeline_data + voyage_data
            
            # Use a dict to deduplicate by key (Date + Location + rounded Time if possible)
            unique_events = {}
            from datetime import datetime
            
            def parse_date(d_str):
                try: return datetime.strptime(d_str, "%d-%m-%Y")
                except: return datetime.min

            for ev in all_events:
                # Key: Date + Location + first 5 words of details to be safe
                # Details can be a list, join them
                det_str = " ".join(ev["details"][:2])
                key = (ev["date"], ev["location"], ev["timestamp"], det_str)
                if key not in unique_events:
                    unique_events[key] = ev
            
            all_events_sorted = list(unique_events.values())
            all_events_sorted.sort(key=lambda x: parse_date(x['date']))
            
            extracted_data["all_events_sorted"] = all_events_sorted
            sys.stderr.write(f"DEBUG: Extracted & Deduplicated {len(all_events_sorted)} total unique events.\n")

            # Convert IST 24-hour timestamp to 12-hour AM/PM format
            def to_ampm(ts):
                if not ts:
                    return ts
                try:
                    # Remove IST and extra spaces: "06-02-2026 16:26:52    IST" -> "06-02-2026 16:26:52"
                    clean = re.sub(r'\s*IST\s*', '', ts).strip()
                    dt = datetime.strptime(clean, "%d-%m-%Y %H:%M:%S")
                    return dt.strftime("%d-%m-%Y %I:%M:%S %p")
                except Exception:
                    try:
                        clean = re.sub(r'\s*IST\s*', '', ts).strip()
                        dt = datetime.strptime(clean, "%d-%m-%Y %H:%M")
                        return dt.strftime("%d-%m-%Y %I:%M %p")
                    except Exception:
                        return ts

            # Determine search keywords based on mode
            if mode == "cfs":
                in_keyword = "CFS IN"
                out_keyword = "CFS OUT"
            else:
                in_keyword = "PORT IN"
                out_keyword = "PORT OUT"

            # Build simplified response
            location_name = None
            in_datetime = None
            out_datetime = None

            for ev in all_events_sorted:
                ev_status = ev.get("status", "").upper()
                ev_details = " ".join(ev.get("details", [])).upper()
                ev_location = ev.get("location", "")
                ev_timestamp = ev.get("timestamp", "").strip()
                ev_date = ev.get("date", "")

                # Use timestamp if available, otherwise use date
                datetime_str = ev_timestamp if ev_timestamp else ev_date

                if in_keyword in ev_status or in_keyword in ev_details:
                    if not in_datetime:
                        in_datetime = datetime_str
                        if ev_location and not location_name:
                            location_name = ev_location

                if out_keyword in ev_status or out_keyword in ev_details:
                    if not out_datetime:
                        out_datetime = datetime_str
                        if ev_location and not location_name:
                            location_name = ev_location

            # If location not found from target events, try to get from any event
            if not location_name:
                for ev in all_events_sorted:
                    loc = ev.get("location", "")
                    if loc:
                        location_name = loc
                        break

            if mode == "cfs":
                simplified_result = {
                    "container_no": container_no,
                    "status": "success",
                    "data": {
                        "cfs_name": location_name,
                        "cfs_in": to_ampm(in_datetime),
                        "cfs_out": to_ampm(out_datetime)
                    }
                }
            else:
                simplified_result = {
                    "container_no": container_no,
                    "status": "success",
                    "data": {
                        "port_name": location_name,
                        "port_in": to_ampm(in_datetime),
                        "port_out": to_ampm(out_datetime)
                    }
                }

            page.screenshot(path="ldb_debug_final.png")
            print(json.dumps(simplified_result))
            
            # Remove input() so it closes automatically as requested
            # input("Press Enter to close browser...")
            
        except Exception as e:
            sys.stderr.write(f"ERROR: Top level error: {str(e)}\n")
            result["status"] = "error"
            result["error"] = str(e)
            print(json.dumps(result))
            
        finally:
            sys.stderr.write("DEBUG: Closing browser...\n")
            browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Missing arguments"}))
        sys.exit(1)
        
    container_no = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "port"
    
    track_ldb(container_no, mode)
