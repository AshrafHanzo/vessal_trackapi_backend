import sys
import time
import json
import re
import os
import platform
import cv2
import numpy as np
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import easyocr

def clean_captcha_text(text):
    text = text.replace(" ", "").replace("\n", "").strip().lower()
    
    # Visual similarity mapping for common OCR misreads
    # Based on the user's specific feedback, 'Z' is often misread as 'I'.
    # Because this captcha is alphanumeric (contains both letters and numbers),
    # aggressively mapping all 'o' to '0' or 's' to '5' will break valid letter captchas.
    # So we only apply the highly confident corrections for known hallucination patterns.
    similarity_map = str.maketrans({
        'i': 'z',
        'l': 'z',
        '|': 'z',
        '!': 'z'
    })
    
    text = text.translate(similarity_map)
    return text

def preprocess_image(image_path):
    img = cv2.imread(image_path)
    if img is None:
        return image_path
        
    # Convert to HSV color space to isolate the blue text
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    
    # Define range for the blue text color
    lower_blue = np.array([100, 50, 50])
    upper_blue = np.array([130, 255, 255])
    
    # Threshold the HSV image
    mask = cv2.inRange(hsv, lower_blue, upper_blue)
    
    # Use Connected Components to filter out isolated noise
    # This completely prevents smudging because we don't alter the surviving pixels at all
    num_labels, labels, stats, centroids = cv2.connectedComponentsWithStats(mask, connectivity=8)
    
    filtered_mask = np.zeros_like(mask)
    
    for i in range(1, num_labels):
        area = stats[i, cv2.CC_STAT_AREA]
        w = stats[i, cv2.CC_STAT_WIDTH]
        h = stats[i, cv2.CC_STAT_HEIGHT]
        
        # 1. Remove dots and small fragments
        if area < 35:
            continue
            
        # 2. Remove isolated thin lines 
        extent = area / float(w * h) if w * h > 0 else 0
        aspect_ratio = float(w) / float(h) if h > 0 else 0
        
        # If it's very thin (low extent) and very wide or tall (extreme aspect ratio)
        if extent < 0.35 and (aspect_ratio > 2.5 or aspect_ratio < 0.4):
            continue
            
        # Keep everything else exactly as it is to prevent warping/smudging
        filtered_mask[labels == i] = 255
        
    # Invert so text is black and background is white
    final = cv2.bitwise_not(filtered_mask)
    
    # Upscale the image by 2x to help EasyOCR read the raw unsmudged shapes better
    final = cv2.resize(final, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    
    processed_path = "processed_captcha.png"
    cv2.imwrite(processed_path, final)
    return processed_path

def parse_esl_results(html_content, container_no):
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {
        "container_no": container_no,
        "Departed_value": None,
        "Current Position": None,
        "Loaded Vessel and Voyage": None,
        "Eta_value": None,
        "Eta_date": None,
        "history": []
    }
    
    # Extract headers based on screenshot
    labels = {
        "Actual Departure from Place of Receipt:": "Departed_value",
        "Current Position:": "Current Position",
        "Loaded Vessel and Voyage:": "Loaded Vessel and Voyage",
        "Expected Arrival at Place of Delivery:": "Eta_value"
    }

    # Text blocks for key-value pairs
    text_blocks = [t.strip() for t in soup.stripped_strings if t.strip()]
    for i, t in enumerate(text_blocks):
        for label_key, data_key in labels.items():
            if t.startswith(label_key):
                 val = t[len(label_key):].strip()
                 if not val and i+1 < len(text_blocks):
                     val = text_blocks[i+1]
                 data[data_key] = val

    # Extract tables
    tables = soup.find_all('table')
    for table in tables:
        headers = []
        thead = table.find('thead')
        if thead:
            headers = [th.text.strip() for th in thead.find_all(['th', 'td'])]
        # sometimes headers are just th in the first tr
        if not headers:
            first_row = table.find('tr')
            if first_row:
                 headers = [th.text.strip() for th in first_row.find_all('th')]

        tbody = table.find('tbody')
        if not tbody:
            tbody = table # Fallback to table itself if no tbody

        rows = tbody.find_all('tr')
        for row in rows:
            cols = [td.text.strip() for td in row.find_all('td')]
            if len(cols) == 7: # Expecting Date, Service, Vessel, Voyage, Bound, Movement, Location
                movement_str = cols[5]
                date_str = cols[0]
                
                data["history"].append({
                    "Date": date_str,
                    "Service": cols[1],
                    "Vessel": cols[2],
                    "Voyage": cols[3],
                    "Bound": cols[4],
                    "Movement": movement_str,
                    "Location": cols[6]
                })
                
                # Check for Eta_date
                if movement_str.upper() == "IMPORT LADEN DISCHARGED FROM VESSEL":
                    if not data["Eta_date"]:
                        data["Eta_date"] = date_str
    
    return data

def run_esl_tracker(container_no):
    print(f"[Worker] Starting ESL Tracker for {container_no}", file=sys.stderr)
    
    print("[Worker] Initializing EasyOCR...", file=sys.stderr)
    # Redirect stdout to avoid easyocr printing unwanted stuff
    old_stdout = sys.stdout
    sys.stdout = sys.stderr
    reader = easyocr.Reader(['en'], gpu=False, verbose=False)
    sys.stdout = old_stdout
    
    os_name = platform.system()
    headless_mode = True
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless_mode,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        url = f"https://www.emiratesline.com/cargo-tracking/?url={container_no}"
        
        max_retries = 30
        success = False
        final_data = {}
        
        try:
            print(f"[Worker] Navigating to {url}", file=sys.stderr)
            page.goto(url, timeout=60000)
            
            for attempt in range(max_retries):
                print(f"[Worker] Captcha Attempt {attempt+1}/{max_retries}...", file=sys.stderr)
                time.sleep(3) # Let page load
                
                try:
                    table_locator = page.locator("table")
                    input_locator = page.locator("#securityCode")
                    
                    if table_locator.count() > 0 and input_locator.count() == 0:
                        print("[Worker] Results table found directly!", file=sys.stderr)
                        success = True
                        break
                        
                    if input_locator.count() == 0:
                         print("[Worker] Form not found. Retrying navigation...", file=sys.stderr)
                         page.goto(url, timeout=60000)
                         continue
                except Exception as e:
                     print(f"[Worker] Error checking element states: {e}", file=sys.stderr)

                # Solve Captcha
                try:
                    # Captcha image usually near the input or inside .captcha-validator
                    captcha_img = page.locator(".captcha-validator img").first
                    if captcha_img.count() == 0:
                         captcha_img = page.locator("img[src*='captcha']").first
                    if captcha_img.count() == 0:
                         # Just use the first image on the page
                         captcha_img = page.locator("img").first
                    
                    captcha_img.screenshot(path="captcha.png")
                except Exception as e:
                    print(f"[Worker] Error taking screenshot: {e}", file=sys.stderr)
                    time.sleep(2)
                    continue
                
                try:
                    # Preprocess to remove lines and dots
                    processed_captcha = preprocess_image("captcha.png")
                    
                    allow_list = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                    ocr_results = reader.readtext(processed_captcha, detail=0, allowlist=allow_list)
                    raw_text = "".join(ocr_results)
                    captcha_text = clean_captcha_text(raw_text)
                    print(f"[Worker] OCR Result: '{raw_text}' -> Cleaned: '{captcha_text}'", file=sys.stderr)
                except Exception as e:
                    print(f"[Worker] Error running OCR: {e}", file=sys.stderr)
                    captcha_text = ""
                
                if not captcha_text or len(captcha_text) < 4:
                     print("[Worker] OCR empty or too short, clicking refresh...", file=sys.stderr)
                     try:
                         page.get_by_text("Click Here").first.click(force=True)
                     except:
                         pass
                     continue

                # Fill (#securityCode)
                page.locator("#securityCode").fill(captcha_text)
                
                # Submit
                submit_btn = page.locator("body > div.container.captcha-validator > div > div > div > form > div:nth-child(3) > input")
                if submit_btn.count() == 0:
                     submit_btn = page.locator("input[value='Submit']").first
                     
                if submit_btn.count() > 0:
                    submit_btn.click(force=True)
                    print("[Worker] Clicked Submit, waiting for response...", file=sys.stderr)
                    time.sleep(5)
                else:
                    print("[Worker] Could not find submit button.", file=sys.stderr)
                    break
                
                # Check outcome
                if page.locator("table").count() > 0:
                     print("[Worker] Results table found! Captcha solved successfully.", file=sys.stderr)
                     success = True
                     break
                else:
                     print("[Worker] Captcha failed.", file=sys.stderr)
                
            if success:
                print("[Worker] Parsing Data...", file=sys.stderr)
                html_source = page.content()
                final_data = parse_esl_results(html_source, container_no)
                final_data["status"] = "success"
                
                # output final JSON to stdout
                print(json.dumps(final_data))
            else:
                print(json.dumps({"status": "error", "message": "Failed to bypass captcha after multiple retries"}))

        except Exception as e:
            print(f"[Worker] Error: {e}", file=sys.stderr)
            print(json.dumps({"status": "error", "message": str(e)}))
        finally:
            browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "Missing container number"}))
        sys.exit(1)
        
    container_no = sys.argv[1]
    run_esl_tracker(container_no)
