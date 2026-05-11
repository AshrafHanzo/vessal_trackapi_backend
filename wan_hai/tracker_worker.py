import sys
import time
import json
import re
import random
from datetime import datetime
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth


def fmt_am_pm(date_str):
    """Format a date string to YYYY-MM-DD for API compatibility."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Remove any stray unicode symbols
    date_str = re.sub(r'[^\x00-\x7F]+', '', date_str).strip()

    formats = [
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y %m %d %H:%M",
        "%Y/%m/%d",
        "%Y-%m-%d"
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def extract_date_from_text(text):
    """Extract ONLY the date/time substring from text using regex."""
    if not text:
        return None
    # Remove unicode symbols
    text = re.sub(r'[^\x00-\x7F]+', '', text).strip()
    # Match YYYY/MM/DD or YYYY-MM-DD with optional time
    match = re.search(r'(\d{4}[/-]\d{2}[/-]\d{2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)', text)
    if match:
        return match.group(1).strip()
    return None


def extract_location_from_text(text):
    """Extract clean location from text, removing dates and extra details."""
    if not text:
        return None
    # Remove date patterns (YYYY/MM/DD or YYYY-MM-DD) and following text
    text = re.sub(r'\d{4}[/-]\d{2}[/-]\d{2}.*', '', text)
    return text.strip()

def run_wan_hai(container_no):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            channel="chrome",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ]
        )

        # Retry page load up to 2 times with fresh context
        page = None
        context = None
        for attempt in range(2):
            try:
                if context:
                    try: context.close()
                    except: pass
                
                context = browser.new_context(
                    viewport={'width': 1280, 'height': 720},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                Stealth().apply_stealth_sync(page)

                # Random delay to reduce bot fingerprint
                time.sleep(random.uniform(1.0, 3.0))

                print(f"[Worker] Navigating to Wan Hai (attempt {attempt+1})...", file=sys.stderr)
                page.goto("https://www.wanhai.com/views/cargo_track_v2/tracking_query.xhtml", timeout=60000, wait_until="domcontentloaded")
                page.wait_for_selector("#cargoType", timeout=45000)
                print(f"[Worker] Page loaded OK on attempt {attempt+1}", file=sys.stderr)
                break  # Success
            except Exception as e:
                print(f"[Worker] Attempt {attempt+1} failed: {e}", file=sys.stderr)
                if attempt == 1:  # Last attempt
                    try: page.screenshot(path="wanhai_debug_headless.png")
                    except: pass
                    print(json.dumps({
                        "container_no": container_no,
                        "status": "error",
                        "data": {},
                        "error": f"Page load failed after 2 attempts: {str(e)[:200]}"
                    }))
                    browser.close()
                    return
                time.sleep(random.uniform(3.0, 6.0))  # Wait before retry
        
        # 1. Click #cargoType
        print("[Worker] Selecting Cargo Type...", file=sys.stderr)
        page.locator("#cargoType").click()
        
        # 2. Type "Ctnr No."
        # Assuming it's a searchable dropdown or input
        page.locator("#cargoType").type("Ctnr No.")
        page.keyboard.press("Enter")
        
        # 3. Click #q_ref_no1 and enter container number
        print(f"[Worker] Entering Container No: {container_no}", file=sys.stderr)
        page.locator("#q_ref_no1").click()
        page.locator("#q_ref_no1").fill(container_no)
        
        # 4. Click #Query
        # 4. Click #Query and wait for new tab/window
        print("[Worker] Clicking Query (expects new tab/window)...", file=sys.stderr)
        
        # Get current pages
        current_pages = context.pages
        
        # Use Force JS Click for Query
        print("[Worker] Executing JS click on #Query...", file=sys.stderr)
        page.evaluate("document.querySelector('#Query').click()")
        
        # Wait for a new page to appear
        print("[Worker] Waiting for new page (up to 30s)...", file=sys.stderr)
        new_page = None
        for i in range(60): # Try for 30 seconds
            if len(context.pages) > len(current_pages):
                new_page = context.pages[-1]
                break
            
            # If 10s passed and no page, try clicking again?
            if i == 20: 
                 print("[Worker] No new page yet (10s). Retrying Standard Click...", file=sys.stderr)
                 try: page.locator("#Query").click(force=True, timeout=2000)
                 except: pass

            time.sleep(0.5)
            
        if not new_page:
             print(f"[Worker] Error: No new page detected! Pages: {len(context.pages)}", file=sys.stderr)
             print(json.dumps({
                 "container_no": container_no,
                 "status": "error",
                 "data": {},
                 "error": "No new page detected after Query click"
             }))
             browser.close()
             return

        print(f"[Worker] New page detected (Title: {new_page.title()}). Waiting for load...", file=sys.stderr)
        new_page.wait_for_load_state()

        # 5. Extract Parameters, Click Unlock Button, and Open in New Tab
        # The user requested to capture the file_num, top_file_num, and parent_id from the current URL,
        # explicitly click the B/L link first to "unlock" the session data natively,
        # and then open the constructed URL in a new tab natively to parse the data. 
        current_url = new_page.url
        print(f"[Worker] Current list URL: {current_url}", file=sys.stderr)
        
        detail_url = None
        if "?" in current_url:
             # Preserve the domain structure (e.g. th.wanhai.com instead of www.wanhai.com)
             from urllib.parse import urlparse
             parsed = urlparse(current_url)
             base_url = f"{parsed.scheme}://{parsed.netloc}"
             query_string = parsed.query
             
             detail_url = f"{base_url}/views/cargo_track_v2/tracking_data_page_by_bl.xhtml?{query_string}"
             print(f"[Worker] Constructed detail URL from parameters: {detail_url}", file=sys.stderr)
        else:
             print("[Worker] Could not find query parameters in the list URL.", file=sys.stderr)
             
        if detail_url:
             max_retries = 3
             success = False
             
             for attempt in range(max_retries):
                 print(f"[Worker] Attempt {attempt+1}/{max_retries}: Unlocking data and opening details natively...", file=sys.stderr)
                 try:
                     # FIRST: Explicitly click the "B/L Data" link to unlock the data exactly as requested by the user
                     print("[Worker] Modifying DOM to prevent native new tab and clicking B/L Data...", file=sys.stderr)
                     new_page.screenshot(path="wan_hai_new_page_debug.png", full_page=True)
                     new_page.evaluate('''() => {
                         let links = document.querySelectorAll('a');
                         links.forEach(l => {
                             if (l.innerText && l.innerText.includes('B/L Data')) {
                                 l.removeAttribute('target');
                             }
                         });
                     }''')
                     
                     print("[Worker] Sending click to B/L Data link...", file=sys.stderr)
                     bl_link = new_page.locator("text='B/L Data'").first
                     bl_link.click()
                     print("[Worker] Waiting 8 seconds for server to unlock session...", file=sys.stderr)
                     time.sleep(8) # Wait 5-10s before opening new tab per user request
                     
                     # SECOND: Open the URL constructed statically in the new Playwright tab to get around WAF blocks on same-tab 
                     print(f"[Worker] Fetching unlocked payload natively in new tab...", file=sys.stderr)
                     detail_page = context.new_page()
                     Stealth().apply_stealth_sync(detail_page)
                     detail_page.goto(detail_url, timeout=45000)
                     
                     print("[Worker] Waiting for data table to appear...", file=sys.stderr)
                     # Wait for 'Vessel Name' as proof the page actually loaded tracking data
                     detail_page.wait_for_selector("text='Vessel Name'", timeout=10000)
                     print("[Worker] Tracking data verified visible.", file=sys.stderr)
                     
                     new_page = detail_page    
                     success = True
                     break
                 except Exception as e:
                     print(f"[Worker] Page load failed or 'Vessel Name' not found: {e}", file=sys.stderr)
                     try:
                         detail_page.close()
                     except: pass
                     
                     print(f"[Worker] Details failed to load. Closing tab and retrying in 3s...", file=sys.stderr)
                     time.sleep(3)
                     
             if not success:
                 print("[Worker] Failed to load tracking data after all retries.", file=sys.stderr)
                 print(json.dumps({
                     "container_no": container_no,
                     "status": "error",
                     "data": {},
                     "error": "Failed to load B/L tracking data after retries"
                 }))
                 browser.close()
                 return

        # 6. Extract Data
        print("[Worker] Extracting data using BeautifulSoup...", file=sys.stderr)
        data = {}
        
        try:
            from bs4 import BeautifulSoup
            html_content = new_page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Based on the user screenshot, the data is laid out in a table format:
            # [Label] [Location] [Vessel / Voyage] [Event Label] [Event Date]
            # e.g., "Place of Receipt" "SHANGHAI (CN)" "KMTC JEBEL ALI / 2601W" "Estimated Departure Date" "2026/02/12"
            
            tables = soup.find_all('table')
            vessel_voyage_found = False
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if not cells: continue
                    
                    cell_texts = [c.get_text(strip=True) for c in cells]
                    if len(cell_texts) < 2: continue
                    
                    label = cell_texts[0].lower()
                    
                    if "place of receipt" in label and len(cell_texts) >= 5:
                        data["place_of_receipt"] = cell_texts[1]
                        data["etd"] = cell_texts[4]
                        if not vessel_voyage_found:
                             parts = cell_texts[2].split('/')
                             if len(parts) >= 2:
                                 data["vessel_name"] = parts[0].strip()
                                 data["voyage"] = parts[1].strip()
                                 vessel_voyage_found = True
                    elif "port of loading" in label and len(cell_texts) >= 5:
                        data["port_of_loading"] = cell_texts[1]
                        data["atd"] = cell_texts[4]
                        if not vessel_voyage_found:
                             parts = cell_texts[2].split('/')
                             if len(parts) >= 2:
                                 data["vessel_name"] = parts[0].strip()
                                 data["voyage"] = parts[1].strip()
                                 vessel_voyage_found = True
                    elif "port of discharging" in label and len(cell_texts) >= 5:
                        data["port_of_discharge"] = cell_texts[1]
                        data["eta"] = cell_texts[4]
                    elif "place of delivery" in label and len(cell_texts) >= 5:
                        data["place_of_delivery"] = cell_texts[1]
                        data["ata"] = cell_texts[4]
            
            # Build final JSON matching the standard schema
            final_data = {
                "container_no": container_no,
                "status": "success",
                "data": {
                    "departed_value": extract_location_from_text(data.get("port_of_loading", "")),
                    "eta_value": extract_location_from_text(data.get("port_of_discharge", "")),
                    "eta_date": fmt_am_pm(extract_date_from_text(data.get("eta", "")))
                },
                "error": None
            }
            
            print(json.dumps(final_data, indent=2))
            
        except Exception as e:
            print(f"[Worker] Extraction Error: {e}", file=sys.stderr)
            print(json.dumps({
                "container_no": container_no,
                "status": "error",
                "data": {},
                "error": str(e)
            }, indent=2))

        # print("[Worker] Waiting for user instructions... (Process will keep running)", file=sys.stderr)
        
        # Clear cache/cookies/storage as requested
        print("[Worker] Clearing cache and cookies...", file=sys.stderr)
        try:
            context.clear_cookies()
            if new_page:
                new_page.evaluate("try { window.localStorage.clear(); window.sessionStorage.clear(); } catch(e) {}")
            page.evaluate("try { window.localStorage.clear(); window.sessionStorage.clear(); } catch(e) {}")
        except: pass

        browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tracker_worker.py <container_no>")
        sys.exit(1)
    
    container_no = sys.argv[1]
    run_wan_hai(container_no)
