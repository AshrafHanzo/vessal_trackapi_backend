import sys
import json
import time
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime

def track_adani(container_no: str):
    result = {
        "container_no": container_no,
        "status": "pending",
        "data": {
            "cfs_code": None,
            "entry_time": None,
            "exit_time": None,
            "scan_status": None
        },
        "error": None
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            
            # Go to HOME page
            url = "https://www.adaniports.com/"
            sys.stderr.write(f"DEBUG: Navigating to {url}...\n")
            page.goto(url, timeout=60000)
            
            # Click tracking button
            page.wait_for_selector("a[href='#cTrackingModal'], .btn-tracking", timeout=30000)
            page.click("a[href='#cTrackingModal'], .btn-tracking")
            
            # Modal interaction
            modal_selector = "#cTrackingModal"
            page.wait_for_selector(modal_selector, state="visible", timeout=10000)
            modal = page.locator(modal_selector)
            
            # Select Kattupalli
            if modal.locator("select").count() > 0:
                modal.locator("select").first.select_option(label="Kattupalli")
            else:
                modal.locator("text=Select Terminal").click()
                page.click("text=Kattupalli")
            
            # Input Container
            inp = modal.locator("input[placeholder*='Container'], input[type='text']")
            if inp.count() > 0:
                inp.first.fill(container_no)
            else:
                raise Exception("Input in modal not found")
            
            # Search
            btn = modal.locator("button:has-text('Search'), input[type='submit']")
            if btn.count() > 0:
                btn.first.click()
            else:
                page.keyboard.press("Enter")
            
            # Wait for Results
            sys.stderr.write("DEBUG: Waiting for results...\n")
            try:
                page.wait_for_selector("text=Container No", timeout=30000)
            except:
                pass
            
            # Parse
            html = page.content()
            soup = BeautifulSoup(html, 'lxml')
            
            def find_kv(label):
                # Search for string containing label
                tags = soup.find_all(string=lambda t: t and label.lower() in t.strip().lower())
                for t in tags:
                    # Traverse up to find the containing cell (td or th)
                    parent = t.parent
                    while parent and parent.name not in ['td', 'th', 'body', 'html']:
                        parent = parent.parent
                    
                    if parent and parent.name in ['td', 'th']:
                        # Found the cell containing label. Value should be in next cell.
                        next_cell = parent.find_next_sibling(['td', 'th'])
                        if next_cell:
                            return next_cell.get_text(strip=True)
                return None

            # Get Destination Code from website to fill JSON cfs_code
            dest_code_web = find_kv("Destination Code")
            entry_dttm = find_kv("Entry DTTM") or find_kv("Entry Date") or find_kv("Gate In")
            exit_dttm = find_kv("Exit DTTM") or find_kv("Exit Date") or find_kv("Gate Out") 
            scan_status = find_kv("Scan Status")
            
            def fmt_am_pm(date_str):
                if not date_str: return None
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    return dt.strftime("%d-%m-%Y %I:%M %p")
                except:
                    return date_str

            # As per user request: delete destination_code field, map cfs_code to "Destination Code" field
            result["data"]["cfs_code"] = dest_code_web if dest_code_web else None
            result["data"]["entry_time"] = fmt_am_pm(entry_dttm)
            result["data"]["exit_time"] = fmt_am_pm(exit_dttm)
            result["data"]["scan_status"] = scan_status if scan_status else None
            result["status"] = "success" if entry_dttm or exit_dttm else "not_found"
            
        except Exception as e:
            sys.stderr.write(f"ERROR: {e}\n")
            result["status"] = "error"
            result["error"] = str(e)
            try:
                 with open("adani_debug_latest.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
            except: pass
        finally:
            browser.close()
            print(json.dumps(result, indent=4))

if __name__ == "__main__":
    track_adani(sys.argv[1] if len(sys.argv) > 1 else "CAAU2633856")
