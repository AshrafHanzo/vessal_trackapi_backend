"""
RCL Container Tracker Worker
Automates rclgroup.com/CargoTracking to fetch container tracking details.
"""

import time
import os
import platform
import random
import re
import ssl
import json
import sys
from bs4 import BeautifulSoup

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

def get_chrome_major_version():
    """Find the installed Chrome major version using Registry, then path, then fallback."""
    import platform, subprocess, re, os
    if platform.system() == "Windows":
        # Strategy 1: Registry check (Most reliable on Windows)
        try:
            import winreg
            keys = [
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Update\Clients\{8A69D345-D564-463c-AFF1-A69D9E530F96}"),
                (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Google\Update\Clients\{8A69D345-D564-463c-AFF1-A69D9E530F96}"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\Google\Update\Clients\{8A69D345-D564-463c-AFF1-A69D9E530F96}")
            ]
            for hkey, path in keys:
                try:
                    with winreg.OpenKey(hkey, path) as key:
                        version, _ = winreg.QueryValueEx(key, "version")
                        m = re.search(r'^(\d+)\.', version)
                        if m: return int(m.group(1))
                except: pass
        except: pass

        # Strategy 2: File Path check
        paths = [
            os.path.join(os.environ.get("ProgramFiles", "C:\\Program Files"), "Google\\Chrome\\Application\\chrome.exe"),
            os.path.join(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)"), "Google\\Chrome\\Application\\chrome.exe")
        ]
        for p in paths:
            if os.path.exists(p):
                try:
                    cmd = f'powershell -NoProfile -Command "(Get-Item \'{p}\').VersionInfo.ProductVersion"'
                    out = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL)
                    m = re.search(r'^(\d+)\.', out.strip())
                    if m: return int(m.group(1))
                except: pass
    
    print("[Worker] Warning: Auto-detection returned fallback 146.")
    return 146

ssl._create_default_https_context = ssl._create_unverified_context
TARGET_URL = "https://eservice.rclgroup.com/CargoTracking/"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def wait_for_cloudflare(driver, timeout: int = 75) -> bool:
    """Highly reliable Cloudflare solver. Returns when search form is ready."""
    print("[Worker] Waiting for Cloudflare challenge...", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            # Immediate check: if search form is visible
            ready = driver.execute_script("return document.querySelector('#statusBkgNo') !== null && document.querySelector('#statusBkgNo').offsetHeight > 0;")
            if ready:
                print("[Worker] Cloudflare cleared! Search form detected.", flush=True)
                return True
                
            driver.switch_to.default_content()
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            cf_iframe = None
            for f in iframes:
                try:
                    src = str(f.get_attribute("src") or "")
                    title = str(f.get_attribute("title") or "")
                    if "challenges.cloudflare.com" in src or "Cloudflare" in title:
                        cf_iframe = f
                        break
                except: pass
            
            if cf_iframe:
                print(f"[Worker] Found Turnstile iframe. Attempting bypass...", flush=True)
                loc = cf_iframe.location
                try:
                    driver.switch_to.frame(cf_iframe)
                    try:
                        # Priority 1: Direct checkbox click
                        cb = driver.find_element(By.CSS_SELECTOR, "input[type='checkbox'], .mark, .cb-lb")
                        if cb.is_displayed():
                            ActionChains(driver).move_to_element_with_offset(cb, 5, 5).click().perform()
                            print("[Worker] Cloudflare checkbox clicked (ActionChains).", flush=True)
                        else: raise Exception("Not visible")
                    except:
                        # Priority 2: Multi-point grid click via CDP (Hardware-level)
                        # Try several common spots for the Turnstile interaction
                        driver.switch_to.default_content()
                        points = [(30, 30), (50, 50), (100, 30), (30, 100), (80, 80)]
                        for (dx, dy) in points:
                            try:
                                abs_x = loc['x'] + dx
                                abs_y = loc['y'] + dy
                                driver.execute_cdp_cmd('Input.dispatchMouseEvent', {
                                    'type': 'mousePressed', 'x': abs_x, 'y': abs_y, 'button': 'left', 'clickCount': 1
                                })
                                driver.execute_cdp_cmd('Input.dispatchMouseEvent', {
                                    'type': 'mouseReleased', 'x': abs_x, 'y': abs_y, 'button': 'left', 'clickCount': 1
                                })
                                print(f"[Worker] CDP grid click at ({abs_x}, {abs_y}).", flush=True)
                            except: pass
                    
                    time.sleep(8) # Verification cooldown
                except: pass
                finally: driver.switch_to.default_content()
        except Exception as e:
            print(f"[Worker] CF loop error: {e}", flush=True)
        time.sleep(4)
    return False

def _get_options(headless: bool):
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    if headless: options.add_argument("--headless")
    return options

def track_container(container_number: str, headless: bool = False) -> dict:
    container_number = container_number.strip().upper()
    if not container_number: return {"error": "Container number empty."}

    version = get_chrome_major_version()
    print(f"[Worker] Using Chrome version {version}")
    
    driver = None
    try:
        # Strategy: attempt with local driver first
        local_driver = os.path.join(BASE_DIR, "chromedriver_extracted", "chromedriver-win64", "chromedriver.exe")
        if os.path.exists(local_driver):
            print(f"[Worker] Using local pre-downloaded driver: {local_driver}")
            try:
                driver = uc.Chrome(options=_get_options(headless), driver_executable_path=local_driver)
            except Exception as e_local:
                print(f"[Worker] uc.Chrome with local driver failed: {e_local}")
        
        if not driver:
            # Fallback 1: specific version
            try:
                print(f"[Worker] Attempting uc.Chrome with version_main={version}...")
                driver = uc.Chrome(options=_get_options(headless), version_main=version)
            except Exception as e1:
                print(f"[Worker] uc.Chrome(version_main={version}) failed: {e1}")
                # Fallback 2: automatic
                try:
                    print("[Worker] Retrying uc.Chrome with automatic version detection...")
                    driver = uc.Chrome(options=_get_options(headless))
                except Exception as e2:
                    print(f"[Worker] All uc.Chrome initialization attempts failed: {e2}")
                    return {"error": f"Chrome failed to start: {e2}"}
        
        print(f"[1/5] Opening {TARGET_URL}", flush=True)
        driver.get(TARGET_URL)
        time.sleep(5)
        
        if not wait_for_cloudflare(driver):
            print("[Worker] WARNING: Cloudflare not cleared, but attempting search...")

        print(f"[2/5] Entering container number: {container_number}")
        input_el = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#statusBkgNo")))
        input_el.click()
        input_el.clear()
        input_el.send_keys(container_number)
        time.sleep(1)
        
        print("[4/5] Clicking Search...")
        driver.execute_script('document.querySelector("#submitBlNo").click();')
        
        print("[Worker] Waiting for results to load...", flush=True)
        # Wait up to 60s for AJAX to populate the #pol DOM element with real text
        # Do NOT check raw HTML for '${day}' - it always exists in embedded JS source
        deadline = time.time() + 60
        loaded = False
        while time.time() < deadline:
            try:
                if "No record found" in driver.execute_script("return document.body.innerText || '';"):
                    print("[Worker] No record found on page.", flush=True)
                    break
                # Check DOM innerText of #pol - empty until AJAX populates it
                pol_val = driver.execute_script("""
                    var el = document.querySelector('#pol');
                    return el ? el.innerText.trim() : '';
                """)
                print(f"[Worker] #pol value: '{pol_val}'", flush=True)
                if pol_val and not pol_val.startswith("${") and len(pol_val) > 1:
                    loaded = True
                    break
            except Exception as loop_e:
                print(f"[Worker] Wait loop error: {loop_e}", flush=True)
            time.sleep(3)
        
        if not loaded:
            print("[Worker] WARNING: Results did not load in time. Parsing anyway...", flush=True)

        # [5/5] Expand timeline row
        print("[5/5] Expanding container row to show timeline...")
        try:
            # Try multiple selectors for the expand button/icon
            driver.execute_script("""
                var icon = document.querySelector("#cargoTrackingDetails i.fa-plus-circle") || 
                           document.querySelector("td.dt-control i") || 
                           document.querySelector("td.dt-control");
                if (icon) {
                    icon.scrollIntoView({block:'center'});
                    icon.click();
                }
            """)
            print("[Worker] Timeline expansion icon clicked.")
            time.sleep(4)
        except Exception as e:
            print(f"[Worker] Timeline expansion failed: {e}")

        return _parse_tracking_results(driver.page_source, container_number)

    except Exception as e:
        return {"error": f"Automation failed: {str(e)}"}
    finally:
        if driver:
            try: driver.quit()
            except: pass

def _parse_tracking_results(html: str, container_number: str) -> dict:
    """Parse results using BeautifulSoup, focusing on POL/POD and Timeline milestones."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Save debug HTML
    with open("debug_results.html", "w", encoding="utf-8") as f:
        f.write(html)

    # 1. Basic check for results
    if "No record found" in html:
        return {"error": f"No data found for container {container_number}."}
    
    # 2. Extract POL as departed_value and POD as eta_value
    pol_el = soup.select_one("#pol")
    pod_el = soup.select_one("#pod")
    
    departed_value = pol_el.get_text(strip=True) if pol_el else "N/A"
    eta_value = pod_el.get_text(strip=True) if pod_el else "N/A"

    # Verify that the extracted header data is real and doesn't contain templates like ${pol}
    if "${" in departed_value or "${" in eta_value:
        print("[Parser] Extracted values still contain template placeholders.")
        return {"error": "Page data is still in a loading state (template strings present)."}

    # 3. Extract Timeline milestone dates (Loaded / Discharged)
    # The timeline structure has dates/times positioned above milestone names
    # e.g., <div class="date">07-Mar-2026 07:20</div> <div class="status">Loaded</div>
    loaded_date = "N/A"
    discharged_date = "N/A"
    
    # Brute-force search in soup for milestone boxes
    # Often milestones are in a list or div series
    milestones = soup.find_all(True, string=re.compile(r"Loaded|Discharged", re.IGNORECASE))
    for m_el in milestones:
        text = m_el.get_text(strip=True).lower()
        # Find the date, which is usually in the sibling or parent block above the status text
        parent = m_el.parent
        # Try to find a date pattern (DD-MMM-YYYY HH:mm) in nearby elements
        nearby_text = parent.get_text(" ", strip=True) if parent else ""
        date_match = re.search(r"(\d{2}-[a-zA-Z]{3}-\d{4}\s\d{2}:\d{2})", nearby_text)
        
        if "loaded" in text and loaded_date == "N/A":
            if date_match: loaded_date = date_match.group(1)
        elif "discharged" in text and discharged_date == "N/A":
            if date_match: discharged_date = date_match.group(1)

    # Fallback to ETD/ETA if timeline parsing failed
    if loaded_date == "N/A":
        etd_el = soup.select_one("#etd")
        if etd_el: loaded_date = etd_el.get_text(strip=True).replace("ETD:", "").strip()
    
    if discharged_date == "N/A":
        eta_el = soup.select_one("#eta")
        if eta_el: discharged_date = eta_el.get_text(strip=True).replace("ETA:", "").strip()

    data = {
        "container_no": container_number,
        "departed_value": departed_value,
        "departed_date": loaded_date,
        "eta_value": eta_value,
        "eta_date": discharged_date if discharged_date not in ["N/A", "No"] else None,
    }

    print(f"[Parser] Success: {json.dumps(data)}")
    return data

if __name__ == "__main__":
    if len(sys.argv) > 1:
        res = track_container(sys.argv[1])
        print("\n--- TRACKING RESULT ---")
        print(json.dumps(res, indent=2))
        sys.stdout.flush()
        sys.stderr.flush()
        sys.exit(0)
    else:
        test_container = input("Enter container number: ").strip()
        res = track_container(test_container)
        print("\n--- Result ---")
        print(json.dumps(res, indent=2))
        sys.exit(0)
