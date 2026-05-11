import sys
import time
import random
import math
import json
import os
import platform
import ssl
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

def get_chrome_major_version():
    """Dynamically find the installed Chrome major version to prevent driver mismatch."""
    import platform, subprocess, re
    try:
        if platform.system() == "Windows":
            # Check registry first
            try:
                out = subprocess.check_output(r'reg query "HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon" /v version', shell=True, text=True, stderr=subprocess.DEVNULL)
                m = re.search(r'version\s+REG_SZ\s+(\d+)\.', out)
                if m: return int(m.group(1))
            except: pass
            
            # Fallback for Windows
            out = subprocess.check_output(r'powershell -command "(Get-Item \"C:\Program Files\Google\Chrome\Application\chrome.exe\").VersionInfo.ProductVersion"', shell=True, text=True, stderr=subprocess.DEVNULL)
            m = re.search(r'^(\d+)\.', out.strip())
            if m: return int(m.group(1))
            
        elif platform.system() == "Linux":
            out = subprocess.check_output(['google-chrome', '--product-version'], text=True, stderr=subprocess.DEVNULL)
            m = re.search(r'^(\d+)\.', out.strip())
            if m: return int(m.group(1))
    except Exception:
        pass
    return None

# SSL Certificate verification workaround for Windows and Linux servers 
# with missing certificate stores. This allows undetected_chromedriver 
# to download its driver patches successfully.
ssl._create_default_https_context = ssl._create_unverified_context

def bezier_point(t, p0, p1, p2, p3):
    """Cubic Bezier curve point at t"""
    u = 1 - t
    tt = t * t
    uu = u * u
    u3 = u * u * u
    t3 = t * t * t
    
    pixel = [0, 0]
    pixel[0] = u3 * p0[0] + 3 * uu * t * p1[0] + 3 * u * tt * p2[0] + t3 * p3[0]
    pixel[1] = u3 * p0[1] + 3 * uu * t * p1[1] + 3 * u * tt * p2[1] + t3 * p3[1]
    return pixel

def human_mouse_move(driver, start_x, start_y, end_x, end_y, steps=60):
    """
    Simulates human-like mouse movement with excessive jiggle using Selenium ActionChains.
    """
    # Random control points for the curve with EXTRA chaos
    offset = random.randint(-200, 200) # Increased offset
    cp1_x = start_x + (end_x - start_x) * 0.3 + offset
    cp1_y = start_y + (end_y - start_y) * 0.3 + random.randint(-100, 100)
    
    # Control point 2
    cp2_x = start_x + (end_x - start_x) * 0.7 + random.randint(-200, 200)
    cp2_y = start_y + (end_y - start_y) * 0.7 + random.randint(-100, 100)
    
    p0 = [start_x, start_y]
    p1 = [cp1_x, cp1_y]
    p2 = [cp2_x, cp2_y]
    p3 = [end_x, end_y]

    actions = ActionChains(driver)

    for i in range(steps):
        t = i / steps
        point = bezier_point(t, p0, p1, p2, p3)
        
        # Add JIGGLE
        jitter_x = random.uniform(-5, 5)
        jitter_y = random.uniform(-5, 5)
        
        # ActionChains move_by_offset is relative to current mouse position.
        # So we need to compute delta. Or use move_to_element_with_offset.
        # But we are doing raw coordinates. In Selenium, raw coordinate moves are tricky.
        pass # Placeholder

    # Because absolute positioning is difficult in standard ActionChains without a reference element,
    # we will rely entirely on ActionChains moving RELATIVE to the element we want to click.
    pass

def solve_cloudflare_with_actionchains(driver, iframe_element):
    """Solves Turnstile by moving to the iframe, then clicking with slight jitter."""
    actions = ActionChains(driver)
    
    # 1. Broadly move to the element
    actions.move_to_element(iframe_element)
    
    # 2. Add some jiggle (relative to current position)
    for _ in range(5):
        try:
            x_jig = random.randint(-20, 20)
            y_jig = random.randint(-20, 20)
            actions.move_by_offset(x_jig, y_jig)
            actions.pause(random.uniform(0.01, 0.05))
        except Exception:
            pass # ignore out of bounds
            
    # 3. Move back relative to the center of the iframe (to hit the checkbox)
    # The checkbox is usually on the left side of the widget
    # Instead of clicking blindly, we can just click the element.
    actions.move_to_element_with_offset(iframe_element, -50, 0) # Adjust leftward toward the checkbox
    actions.pause(random.uniform(0.2, 0.5))
    actions.click()
    
    actions.perform()


def run_hapag(container_no):
    print(f"[Worker] Launching Chrome (Undetected)...", file=sys.stderr)
    
    # -------------------------------------------------------------------
    # Virtual Display (Linux only)
    # On Linux servers without a physical display, we use Xvfb through
    # pyvirtualdisplay. This lets Chrome run visibly (headless=False)
    # inside a virtual framebuffer — bypassing Cloudflare's headless checks.
    # On Windows/macOS this is skipped entirely.
    # -------------------------------------------------------------------
    virtual_display = None
    if platform.system() == "Linux":
        try:
            from pyvirtualdisplay import Display
            virtual_display = Display(visible=0, size=(1280, 720))
            virtual_display.start()
            print("[Worker] Virtual display (Xvfb) started.", file=sys.stderr)
        except Exception as e:
            print(f"[Worker] Could not start virtual display: {e}", file=sys.stderr)
    
    # -------------------------------------------------------------------
    # Persistent User Profile (Stealth Enhancement)
    # Using a user_data_dir lets Chrome store cookies and fingerprints,
    # which makes Cloudflare much more likely to trust the browser.
    # -------------------------------------------------------------------
    user_data_path = os.path.join(os.getcwd(), "hapag_profile")
    if not os.path.exists(user_data_path):
        os.makedirs(user_data_path)

    options = uc.ChromeOptions()
    # NOTE: Do NOT add --headless. Chrome must run visibly
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(f'--user-data-dir={user_data_path}')
    options.add_argument('--profile-directory=Default')
    
    # Extra Stealth arguments
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-infobars')
    
    chrome_version = get_chrome_major_version()
    try:
        if chrome_version:
            print(f"[Worker] Auto-detected Chrome version: {chrome_version}", file=sys.stderr)
            driver = uc.Chrome(options=options, version_main=chrome_version, use_subprocess=True)
        else:
            driver = uc.Chrome(options=options, use_subprocess=True)
    except Exception as e:
        print(f"[Worker] uc.Chrome init failed: {e}. Retrying without version...", file=sys.stderr)
        driver = uc.Chrome(options=options, use_subprocess=True)

    try:
        driver.set_window_size(1280, 720)
        print(f"[Worker] Navigating to Hapag-Lloyd tracking page...", file=sys.stderr)
        driver.get("https://www.hapag-lloyd.com/en/online-business/track/track-by-container-solution.html")
        
        time.sleep(5) # Initial buffer

        # Accept Cookies Modal (Privacy Preference Center)
        print("[Worker] Checking for cookies modal...", file=sys.stderr)
        try:
            # Wait up to 10 seconds for the cookie button
            wait = WebDriverWait(driver, 10)
            cookie_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Select All' or @id='accept-recommended-btn-handler']")))
            
            print("[Worker] Found cookie modal. Clicking 'Select All'...", file=sys.stderr)
            actions = ActionChains(driver)
            actions.move_to_element(cookie_btn).pause(0.5).click().perform()
            time.sleep(2)
        except Exception:
            print("[Worker] No cookie modal detected after waiting.", file=sys.stderr)

        # Cloudflare Bypass Logic with Retry Loop
        print("[Worker] Entering Security Check Loop...", file=sys.stderr)
        
        max_duration = 120 # 2 minutes max
        start_time = time.time()
        
        while time.time() - start_time < max_duration:
            # Check if input is present (Success condition)
            try:
                input_el = driver.find_element(By.CSS_SELECTOR, "input[id*='tracing_by_container_f:hl12']")
                if input_el.is_displayed():
                    print("[Worker] Container input detected! Security check passed.", file=sys.stderr)
                    break
            except Exception:
                pass # Not found yet
            
            # If not, look for challenge iframe
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            cf_frame = None
            for frame in frames:
                try:
                    src = frame.get_attribute('src')
                    if src and ("cloudflare" in src or "turnstile" in src):
                        cf_frame = frame
                        break
                except:
                    pass
            
            # If challenge frame found
            if cf_frame:
                print("[Worker] Cloudflare frame detected. Attempting to solve...", file=sys.stderr)
                try:
                    # Switch to the iframe to interact with the actual checkbox
                    driver.switch_to.frame(cf_frame)
                    
                    # Wait for the checkbox to become clickable
                    wait = WebDriverWait(driver, 5)
                    checkbox = wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='challenge-stage'] | //*[@class='ctp-checkbox-label'] | //input[@type='checkbox']")))
                    
                    # Add human-like delay
                    time.sleep(random.uniform(1.0, 2.5))
                    
                    # Move to it and click
                    actions = ActionChains(driver)
                    actions.move_to_element(checkbox).pause(random.uniform(0.1, 0.5)).click().perform()
                    
                    print("[Worker] Clicked challenge inside iframe. Waiting 8s...", file=sys.stderr)
                    driver.switch_to.default_content()
                    time.sleep(8)
                    continue
                except Exception as e:
                    driver.switch_to.default_content()
                    print(f"[Worker] Error interacting with frame: {e}", file=sys.stderr)

            # Fallback Text Check (Verify you are human)
            try:
                # Need to check inside iframes too, but let's check top level first
                verify_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Verify you are human')]")
                for el in verify_elements:
                    if el.is_displayed():
                        print("[Worker] Found 'Verify you are human' text. Clicking...", file=sys.stderr)
                        actions = ActionChains(driver)
                        actions.move_to_element(el).pause(0.5).click().perform()
                        time.sleep(5)
                        break
            except Exception:
                pass


            print("[Worker] waiting...", file=sys.stderr)
            time.sleep(2)
        
        # Attempt to fill form
        print("[Worker] Proceeding to input...", file=sys.stderr)
        
        try:
            wait = WebDriverWait(driver, 10)
            input_el = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[id*='tracing_by_container_f:hl12']")))
            
            # Clear existing text first
            input_el.clear()
            
            # Use action chains to type
            actions = ActionChains(driver)
            actions.move_to_element(input_el).click().pause(0.5).send_keys(container_no).perform()
            
            print("[Worker] Clicking Search button...", file=sys.stderr)
            btn_el = driver.find_element(By.CSS_SELECTOR, "[id*='tracing_by_container_f:hl25']")
            try:
                actions = ActionChains(driver)
                actions.move_to_element(btn_el).pause(0.2).click().perform()
            except Exception:
                pass
            
            # Fallback to direct JS click if ActionChains is intercepted
            try:
                driver.execute_script("arguments[0].click();", btn_el)
            except Exception:
                pass
            
        except Exception as e:
            print(f"[Worker] Form interaction failed (might be stuck on Security Check): {e}", file=sys.stderr)

        # --------------------------------------------------------------------------
        # Data Extraction via BeautifulSoup
        # --------------------------------------------------------------------------
        try:
            print("[Worker] Waiting for tracking results table to load...", file=sys.stderr)
            time.sleep(10) # Simple sleep to let Angular/React populate 
            
            # Scroll down to ensure lazy-loaded elements render
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            from bs4 import BeautifulSoup
            htmlSource = driver.page_source
            soup = BeautifulSoup(htmlSource, 'html.parser')
            
            departed_value = None
            eta_value = None
            eta_date = None
            departed_date = None
            
            # Find rows
            trs = soup.find_all('tr')
            for row in trs:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 4: # Table in screenshot has Status, Place, Date, Time, Transport, Voyage
                    status_text = cells[0].get_text(strip=True).lower()
                    place_text = cells[1].get_text(strip=True)
                    date_text = cells[2].get_text(strip=True)
                    time_text = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    
                    if "vessel departed" in status_text:
                        departed_value = place_text
                        departed_date = f"{date_text} {time_text}".strip()
                    elif "vessel arrival" in status_text:
                        eta_value = place_text
                        eta_date = date_text
                        
            result_data = {
                "Departed_value": departed_value,
                "Eta_value": eta_value,
                "Eta_date": eta_date,
                "departed_date": departed_date
            }
            
            print("\n--- TRACKING RESULT ---")
            print(json.dumps(result_data, indent=2))
            sys.stdout.flush() # Ensure the API caller reads the output immediately
            
        except Exception as e:
            print(f"[Worker] Failed to extract data using BeautifulSoup: {e}", file=sys.stderr)

    finally:
         try:
             browser_pid = driver.browser_pid
         except:
             browser_pid = None

         try:
             driver.quit()
         except Exception:
             pass
             
         # 100% guarantee no memory leaks: violently kill this specific Chrome instance
         if browser_pid:
             os.system(f"taskkill /F /PID {browser_pid} /T >nul 2>&1")
             
         # Stop virtual display on Linux
         if virtual_display is not None:
             try:
                 virtual_display.stop()
                 print("[Worker] Virtual display stopped.", file=sys.stderr)
             except Exception:
                 pass
         
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tracker_worker.py <container_no>")
        sys.exit(1)
    
    container_no = sys.argv[1]
    run_hapag(container_no)
    
    # Force exit to prevent undetected_chromedriver __del__ from throwing WinError 6 on Windows
    import os
    os._exit(0)
