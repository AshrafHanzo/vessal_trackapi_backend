import sys
import json
import re
import os
import time
from playwright.sync_api import sync_playwright
import cv2
import easyocr
from PIL import Image
import numpy as np
from bs4 import BeautifulSoup

def track_icegate(mbl_no: str, port_arg: str, bl_no: str):
    """
    Track shipment details from ICEGATE
    
    Args:
        mbl_no: Master Bill of Lading number
        port_arg: Port code or "ALL_PORTS"
        bl_no: Bill of Lading number
    """
    result = {
        "mbl_no": mbl_no,
        "port": port_arg,
        "bl_no": bl_no,
        "status": "pending",
        "data": None,
        "error": None
    }
    
    # Define ports to try
    default_ports = ["CHENNAI SEA (INMAA1)", "KAMARAJAR (INENR1)", "KATTUPALLI (INKAT1)"]
    ports_to_try = []
    
    # Logic:
    # 1. If user provides a port: Try it FIRST.
    # 2. Then try default ports (as fallback if first one fails or has CAPTCHA issues).
    
    if port_arg and port_arg != "ALL_PORTS":
        # Add provided port first
        ports_to_try.append(port_arg)
    
    # Add defaults (avoiding duplicates)
    for dp in default_ports:
        # Check if dp is already covered by the user's port (loose match)
        is_duplicate = False
        for p in ports_to_try:
            if p.lower() in dp.lower() or dp.lower() in p.lower():
                is_duplicate = True
                break
        
        if not is_duplicate:
            ports_to_try.append(dp)
        
    sys.stderr.write(f"DEBUG: Ports to try path: {ports_to_try}\n")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        
        try:
            page = browser.new_page()
            
            data_found = False
            
            for current_port in ports_to_try:
                sys.stderr.write(f"DEBUG: --------------------------------------------------\n")
                sys.stderr.write(f"DEBUG: Trying port: {current_port}\n")
                sys.stderr.write(f"DEBUG: --------------------------------------------------\n")
                
                try:
                    # Navigate to ICEGATE (Always reload for fresh state)
                    sys.stderr.write("DEBUG: Opening ICEGATE website...\n")
                    page.goto("https://enquiry.icegate.gov.in/enquiryatices/seaIgmEntry")
                    page.wait_for_load_state("networkidle")
                    
                    # Wait for the location dropdown to be visible
                    page.wait_for_selector("#location", state="visible", timeout=10000)
                    
                    # Get all options to find the matching one
                    location_select = page.locator("#location")
                    options = location_select.evaluate("select => Array.from(select.options).map(o => ({text: o.text, value: o.value}))")
                    
                    # Find matching option
                    selected_value = None
                    for opt in options:
                        # Flexible matching: check if target port string is part of option text or value
                        # Split current_port by space to check for keywords like "Chennai"
                        keywords = current_port.lower().split()
                        match = False
                        
                        # Special handling for "Chennai Port" -> match "Chennai"
                        # If any keyword (len > 3) matches
                        for kw in keywords:
                            if len(kw) > 3 and kw in opt['text'].lower():
                                match = True
                                break
                        
                        # Also check direct substring
                        if current_port.lower() in opt['text'].lower() or current_port.lower() in opt['value'].lower():
                            match = True
                            
                        if match:
                            selected_value = opt['value']
                            sys.stderr.write(f"DEBUG: Found matching location: {opt['text']} ({opt['value']})\n")
                            break
                    
                    if selected_value:
                        page.select_option("#location", value=selected_value)
                    else:
                        sys.stderr.write(f"DEBUG: Warning - Could not find exact match for port '{current_port}', trying to select by index 1 or skip...\n")
                        # Skip this port if we can't select it? or try partial match?
                        # Let's try to proceed strictly only if match found to avoid false negatives?
                        # Or user might have given exact code.
                        # For now, if not matched, log and continue to next port
                        sys.stderr.write("DEBUG: Skipping this port due to no match in dropdown.\n")
                        continue

                    # Step 2: Fill MBL Number
                    sys.stderr.write(f"DEBUG: Filling MBL Number: {mbl_no}\n")
                    page.fill("#MAWB_NO", mbl_no)
                    page.wait_for_timeout(500)
                    
                    # Step 3: Solve CAPTCHA using EasyOCR (with retry loop)
                    # Initialize EasyOCR reader once
                    reader = easyocr.Reader(['en'], gpu=False)
                    
                    # Initial CAPTCHA attempt
                    retry_captcha = True
                    captcha_attempts = 0
                    
                    # Infinite loop until definitive result (Success or No Record)
                    while True:
                        captcha_attempts += 1
                        sys.stderr.write(f"DEBUG: CAPTCHA Attempt {captcha_attempts} (Infinite Loop)...\n")
                        
                        captcha_img = page.query_selector("#capimg")
                        if captcha_img:
                            # 1. Screenshot
                            captcha_img.screenshot(path="captcha_temp.png")
                            
                            # 2. Image Processing
                            img = cv2.imread("captcha_temp.png")
                            scale = 3
                            img_resized = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
                            
                            # Save for inspection
                            cv2.imwrite("captcha_original_scaled.png", img_resized)
                            
                            # 3. OCR (Try original scaled first)
                            results = reader.readtext(img_resized, allowlist='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')
                            
                            captcha_text = ""
                            for (bbox, text, prob) in results:
                                captcha_text += text
                            captcha_text = re.sub(r'[^A-Za-z0-9]', '', captcha_text)
                            
                            sys.stderr.write(f"DEBUG: OCR result: {captcha_text}\n")
                            
                            # 4. Fill and Submit
                            page.fill("#captchaResp", "")
                            page.fill("#captchaResp", captcha_text)
                            
                            sys.stderr.write("DEBUG: Clicking Submit...\n")
                            page.click("#SubB")
                            # Wait longer to ensure table loads
                            page.wait_for_timeout(5000)
                            
                            # 5. Check Result
                            # Detect table by specific class or just general table
                            # ICEGATE tables often have class 'sb-table-hover' or similar
                            results_table = page.query_selector("table")
                            captcha_field = page.query_selector("#captchaResp")
                            
                            # Check for "No Record Found" message
                            page_content = page.content().lower()
                            no_record = "no record found" in page_content or "no data found" in page_content
                            
                            sys.stderr.write(f"DEBUG: Check - Table: {bool(results_table)}, CaptchaVisible: {captcha_field.is_visible() if captcha_field else None}, NoRecord: {no_record}\n")
                            
                            # Success condition:
                            # 1. Table detected
                            # 2. Captcha field NOT visible (meaning we moved past form)
                            # 3. No "No Record Found" message
                            if results_table and (not captcha_field or not captcha_field.is_visible()) and not no_record:
                                # Success! Form submitted and table appeared
                                sys.stderr.write("DEBUG: Form submitted successfully! Table detected.\n")
                                retry_captcha = False
                                break # Break CAPTCHA loop
                                
                            elif no_record:
                                sys.stderr.write("DEBUG: 'No Record Found' detected.\n")
                                retry_captcha = False
                                break
                            
                            else:
                                # Failed (probably wrong CAPTCHA)
                                sys.stderr.write("DEBUG: Submit failed (No table & No 'No Record'). Retrying...\n")
                                page.wait_for_timeout(1000)
                        else:
                            sys.stderr.write("DEBUG: CAPTCHA image not found!\n")
                            break
                    
                    # Check if we have data or not
                    page_content = page.content().lower()
                    if "no record found" in page_content or "no data found" in page_content:
                        sys.stderr.write(f"DEBUG: No data found for port {current_port}. Trying next...\n")
                        continue # Continue to next port in outer loop
                        
                    # If we are here, we have results (or at least didn't get "No data found")
                    # We should STOP trying other ports because we found the valid port
                    sys.stderr.write(f"DEBUG: Data table detected at {current_port}. Stopping port search.\n")
                    data_found = True # Mark as found (even if extraction might fail later)
                    
                    # Step 5: Find matching BL row and click More>>
                    sys.stderr.write(f"DEBUG: Looking for BL number: {bl_no}\n")
                    matching_button = None
                    
                    # Search logic...
                    rows = page.query_selector_all("table tr")
                    for row in rows:
                        row_text = row.inner_text()
                        if bl_no.upper() in row_text.upper():
                            sys.stderr.write(f"DEBUG: Found matching row with BL: {bl_no}\n")
                            more_button = row.query_selector("a, button, input[type='button'], input[type='submit']")
                            if more_button:
                                matching_button = more_button
                                break
                    
                    if not matching_button:
                        # Fallback search
                        more_links = page.query_selector_all("a")
                        for link in more_links:
                            link_text = link.inner_text()
                            if "more" in link_text.lower():
                                parent_row = link.evaluate("el => el.closest('tr')")
                                if parent_row:
                                    row_html = page.evaluate("(row) => row.innerHTML", parent_row)
                                    if bl_no.upper() in row_html.upper():
                                        matching_button = link
                                        break
                                        
                    if matching_button:
                        sys.stderr.write("DEBUG: Clicking More>> button...\n")
                        matching_button.click()
                        page.wait_for_timeout(2000)
                        
                        # Step 6: Extract Data
                        sys.stderr.write("DEBUG: Extracting IGM data...\n")
                        page_html = page.content()
                        soup = BeautifulSoup(page_html, 'lxml')
                        
                        extracted_data = {"igm_no": None, "igm_date": None, "inw_date": None}
                        
                        tables = soup.find_all('table')
                        for table in tables:
                            header_row = table.find('tr')
                            if not header_row: continue
                            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
                            
                            if not any('igm' in h for h in headers): continue
                            
                            # flexible indices
                            igm_no_idx = next((i for i, h in enumerate(headers) if 'igmno' in h.replace(' ','')), None)
                            igm_date_idx = next((i for i, h in enumerate(headers) if 'igmdate' in h.replace(' ','')), None)
                            inw_date_idx = next((i for i, h in enumerate(headers) if 'inwdate' in h.replace(' ','')), None)
                            
                            rows = table.find_all('tr')
                            for row in rows[1:]:
                                cells = row.find_all(['td', 'th'])
                                if len(cells) < 3: continue
                                
                                if igm_no_idx is not None and igm_no_idx < len(cells):
                                    extracted_data["igm_no"] = cells[igm_no_idx].get_text(strip=True)
                                if igm_date_idx is not None and igm_date_idx < len(cells):
                                    extracted_data["igm_date"] = cells[igm_date_idx].get_text(strip=True)
                                if inw_date_idx is not None and inw_date_idx < len(cells):
                                    extracted_data["inw_date"] = cells[inw_date_idx].get_text(strip=True)
                                
                                if any(extracted_data.values()): break
                            if any(extracted_data.values()): break
                        
                        # Check if we got data
                        if any(extracted_data.values()):
                            result["status"] = "success"
                            result["data"] = extracted_data
                            result["port_used"] = current_port
                            sys.stderr.write(f"DEBUG: SUCCESS! Data found at {current_port}: {extracted_data}\n")
                            # We found the data, so stop trying other ports
                            break
                        else:
                             sys.stderr.write("DEBUG: Table found but extraction failed (empty data).\n")
                    else:
                        sys.stderr.write(f"DEBUG: BL {bl_no} not found in results table for port {current_port}.\n")
                        # BL not found in this port's results.
                        # Do NOT break here - allow loop to check next port (if any)
                        pass
                        
                except Exception as e:
                    sys.stderr.write(f"DEBUG: Error trying port {current_port}: {str(e)}\n")
                    # Continue to next port
                    continue
            
            if not data_found:
                sys.stderr.write("DEBUG: No data found in any processed port.\n")
                result["status"] = "not_found"
                result["message"] = "Data not found in any of the attempted ports."
            
            # Print JSON result for main.py to capture
            print(json.dumps(result))
            
        except Exception as e:
            sys.stderr.write(f"ERROR: Top level error: {str(e)}\n")
            result["status"] = "error"
            result["error"] = str(e)
            print(json.dumps(result))
            
        finally:
            browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(json.dumps({"error": "Missing arguments"}))
        sys.exit(1)
        
    mbl_no = sys.argv[1]
    port_arg = sys.argv[2]
    bl_no = sys.argv[3]
    
    track_icegate(mbl_no, port_arg, bl_no)
