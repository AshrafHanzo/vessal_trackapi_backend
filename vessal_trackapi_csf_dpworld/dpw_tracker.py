import sys
import json
import re
import time
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime

def format_date_ampm(date_str):
    """
    Converts 'DD-MM-YYYY HH:MM' (24h) to 'DD-MM-YYYY HH:MM AM/PM'
    """
    if not date_str or date_str == "":
        return None
    
    try:
        # Remove any extra chars
        clean_date = date_str.strip()
        # Parse
        dt = datetime.strptime(clean_date, "%d-%m-%Y %H:%M")
        return dt.strftime("%d-%m-%Y %I:%M %p")
    except ValueError:
        return date_str

def get_text_for_label(soup, label_text):
    """
    Finds the value associated with a label.
    """
    try:
        # Escape the label text for regex to handle special chars like '?' or '-' safely if needed
        # But here we use it to find the string node first.
        label_elem = soup.find(string=re.compile(re.escape(label_text), re.IGNORECASE))
        if label_elem:
            # We need to find the containg ROW (which holds both label and value cols)
            # Structure is: .row > .col (Label) + .col (Value)
            row = label_elem.find_parent(class_="row")
            if not row:
                # Fallback for tables or other structures
                row = label_elem.find_parent("tr")
            
            if row:
                # Get text of the whole row with a separator
                all_text = row.get_text(separator="|", strip=True) 
                
                # Split by the label text to get the value part
                # specific split to handle case insensitivity if needed, but simple split usually works
                # We use regex split to be robust against whitespace diffs if necessary, 
                # but let's try simple string processing first as get_text(strip=True) normalizes well.
                
                # We need to be careful if label_text appears multiple times or is a substring.
                # Let's try to locate the label part in the all_text string.
                
                # Case-insensitive split
                parts = re.split(re.escape(label_text), all_text, flags=re.IGNORECASE)
                
                # If we found the label, the value should be in the subsequent part
                if len(parts) > 1:
                    # Join the rest in case the value itself had the label text (unlikely)
                    value_part = parts[1]
                    return value_part.strip().lstrip(":|").strip()
                    
    except Exception:
        pass
    return None

def track_dpw(container_no):
    result = {
        "container_no": container_no,
        "status": "not_found",
        "data": {}
    }
    
    with sync_playwright() as p:
        # Launch browser visible to user
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()
        
        try:
            # Navigate to the calculator/index page
            url = "https://122.252.230.102/DPWCCTTracking/Index.php" 
            page.goto(url, timeout=60000)
            
            # Wait for input
            page.wait_for_selector("input[name='containerNo']", state="visible", timeout=10000)
            
            # Fill Input
            page.fill("input[name='containerNo']", container_no)
            
            # Click Submit
            page.click("input[type='submit'], button[type='submit']")
            
            # Wait for results
            try:
                page.wait_for_selector("text=Container Details", timeout=10000)
            except:
                content = page.content()
                if "No Record Found" in content:
                    result["status"] = "not_found"
                    result["message"] = "No Record Found"
                    return result
            
            # Extract HTML
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            result["status"] = "success"
            
            # Extract Fields
            dest_code_raw = get_text_for_label(soup, "Destination Code")
            in_time_raw = get_text_for_label(soup, "In- Time")
            if not in_time_raw: in_time_raw = get_text_for_label(soup, "In-Time")
            out_time_raw = get_text_for_label(soup, "Out Time")
            scan_mark_raw = get_text_for_label(soup, "Scan Mark")
            
            # Process Fields
            scan_mark_val = None
            if scan_mark_raw and len(scan_mark_raw.strip()) > 0:
                scan_mark_val = scan_mark_raw.strip()
            
            data = {
                "cfs_code": dest_code_raw if dest_code_raw else None,
                "cfs_in_time": format_date_ampm(in_time_raw),
                "cfs_out_time": format_date_ampm(out_time_raw),
                "scan_mark": scan_mark_val
            }
            
            result["data"] = data
            
            # Keep browser open for a few seconds so user can see it
            time.sleep(3)
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            
        finally:
            browser.close()
            
    return result

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No container number provided"}))
    else:
        cno = sys.argv[1]
        print(json.dumps(track_dpw(cno), indent=4))
