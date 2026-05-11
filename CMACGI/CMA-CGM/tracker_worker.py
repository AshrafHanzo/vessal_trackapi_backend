import time
import re
import os
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup

def clean_date(date_str):
    """Removes the day name from the date string."""
    if not date_str:
        return ""
    if ',' in date_str:
        date_str = date_str.split(',', 1)[1].strip()
    return re.sub(r'(\d{4})(\d{2}:)', r'\1 \2', date_str.strip())

def clean_location(loc_str):
    if "Accessible text" in loc_str:
        return loc_str.replace("Accessible text", "").strip()
    return loc_str.strip()

def log_debug(msg):
    with open("debug.log", "a") as f:
        f.write(msg + "\n")

def get_chrome_major_version():
    """Auto-detect the installed Chrome major version using multiple methods."""
    import subprocess
    import winreg

    # Method 1: Check Registry
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon")
        version, _ = winreg.QueryValueEx(key, "version")
        return int(version.split('.')[0])
    except Exception:
        pass

    try:
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\Google Chrome")
        version, _ = winreg.QueryValueEx(key, "DisplayVersion")
        return int(version.split('.')[0])
    except Exception:
        pass

    # Method 2: Check common file paths
    paths = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.join(os.environ.get('LOCALAPPDATA', ''), r"Google\Chrome\Application\chrome.exe"),
    ]
    for path in paths:
        if os.path.exists(path):
            try:
                # Use powershell for cleaner version extraction
                cmd = f'(Get-Item "{path}").VersionInfo.ProductVersion'
                output = subprocess.check_output(['powershell', '-Command', cmd], stderr=subprocess.STDOUT).decode().strip()
                if output:
                    return int(output.split('.')[0])
            except Exception:
                pass
    return None

def human_like_drag(driver, source, target):
    """
    Simulate realistic human mouse drag with acceleration, deceleration,
    and slight jitter — bypasses DataDome slider detection.
    """
    action = ActionChains(driver)
    src_loc = source.location
    tgt_loc = target.location
    src_size = source.size
    tgt_size = target.size

    # Start at center of source
    start_x = src_loc['x'] + src_size['width'] // 2
    start_y = src_loc['y'] + src_size['height'] // 2

    # End at center of target
    end_x = tgt_loc['x'] + tgt_size['width'] // 2
    end_y = tgt_loc['y'] + tgt_size['height'] // 2

    total_x = end_x - start_x
    steps = random.randint(30, 50)

    action.move_to_element(source)
    action.click_and_hold()
    action.pause(random.uniform(0.1, 0.3))

    for i in range(steps):
        progress = i / steps
        # Ease-in-out curve
        if progress < 0.5:
            eased = 2 * progress * progress
        else:
            eased = -1 + (4 - 2 * progress) * progress

        move_x = int(eased * total_x / steps) + random.randint(-2, 2)
        move_y = random.randint(-2, 2)
        action.move_by_offset(move_x, move_y)
        action.pause(random.uniform(0.005, 0.02))

    action.release()
    action.perform()

def track_container(container_number):
    log_debug("Starting track_container")
    options = uc.ChromeOptions()

    # --- Bare Essentials Anti-detection options ---
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")

    chrome_version = get_chrome_major_version()
    log_debug(f"Detected Chrome version: {chrome_version}")

    # Realistic dynamic user agent
    ua_version = f"{chrome_version}.0.0.0" if chrome_version else "146.0.0.0"
    options.add_argument(
        f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{ua_version} Safari/537.36"
    )

    try:
        log_debug("Initializing Chrome")
        if chrome_version:
            driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
        else:
            driver = uc.Chrome(options=options, use_subprocess=True)
        driver.__class__.__del__ = lambda self: None
        log_debug("Chrome initialized")
    except Exception as e:
        log_debug(f"Failed to init Chrome: {e}")
        return {"error": f"Chrome init failed: {e}"}

    try:
        # UC internal patches usually handle navigator.webdriver
        # Additional patches might actually trigger DataDome if not done perfectly

        # Visit a neutral site first
        log_debug("Visiting google.com first")
        driver.get("https://www.google.com")
        time.sleep(random.uniform(2, 4))

        url = "https://www.cma-cgm.com/ebusiness/tracking"
        log_debug(f"Visiting {url}")
        driver.get(url)
        wait = WebDriverWait(driver, 30)

        # Random human-like pause before doing anything
        time.sleep(random.uniform(5, 8))

        log_debug("Saving debug screenshot")
        driver.save_screenshot("page_load_debug.png")
        page_source = driver.page_source
        with open("page_source.html", "w", encoding="utf-8") as f:
            f.write(page_source)

        # Check for Challenge/Block
        is_blocked = False
        if "temporarily restricted" in page_source.lower():
            is_blocked = True
        
        # Check for CAPTCHA slider iframe
        try:
            log_debug("Checking for CAPTCHA iframe")
            iframe = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//iframe[contains(@src, 'captcha-delivery.com')]"))
            )
            log_debug("CAPTCHA iframe found")
            
            # If the text was inside the iframe, we might have missed it in global page_source
            driver.switch_to.frame(iframe)
            log_debug("Switched to iframe")
            
            iframe_source = driver.page_source
            with open("iframe_source.html", "w", encoding="utf-8") as f:
                f.write(iframe_source)
                
            if "temporarily restricted" in iframe_source.lower():
                log_debug("Hard block detected inside iframe")
                return {"error": "Access temporarily restricted by DataDome (IP block)"}

            with open("iframe_source.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)

            log_debug("Waiting for slider element")
            slider_wait = WebDriverWait(driver, 10)
            slider_source = slider_wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div > div.slider"))
            )
            slider_target = driver.find_element(
                By.CSS_SELECTOR, "#captcha__frame__bottom > div.sliderContainer > div.sliderTarget"
            )

            log_debug("Performing human-like drag")
            human_like_drag(driver, slider_source, slider_target)
            time.sleep(random.uniform(3, 6))
            driver.save_screenshot("after_captcha.png")

            log_debug("Switching back to default content")
            driver.switch_to.default_content()

            # Wait for page to load after CAPTCHA
            time.sleep(random.uniform(3, 5))

        except Exception as e:
            log_debug(f"No CAPTCHA or CAPTCHA error: {e}")
            try:
                driver.switch_to.default_content()
            except Exception:
                pass

        # Simulate a small mouse movement before interacting (human-like)
        try:
            body = driver.find_element(By.TAG_NAME, "body")
            action = ActionChains(driver)
            action.move_to_element_with_offset(body, random.randint(100, 400), random.randint(100, 300))
            action.perform()
            time.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass

        log_debug("Waiting for reference input")
        reference_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#Reference")))

        log_debug("Typing container number")
        reference_input.click()
        time.sleep(random.uniform(0.3, 0.8))
        reference_input.clear()
        # Type like a human — character by character with small delays
        for char in container_number:
            reference_input.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))

        time.sleep(random.uniform(0.5, 1.2))

        log_debug("Clicking tracking button")
        tracking_button = driver.find_element(By.CSS_SELECTOR, "#btnTracking")
        tracking_button.click()

        log_debug("Waiting for results")
        time.sleep(random.uniform(8, 12))

        log_debug("Parsing output")
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        rows = soup.find_all('tr')
        tracking_records = []
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 3:
                text_content = [td.get_text(strip=True) for td in tds]
                if any(re.search(r'\d{2}-[A-Z]{3}-\d{4}', t) for t in text_content):
                    tracking_records.append(text_content)

        data = {
            "departure_date": "Not Found",
            "departure_value": "Not Found",
            "eta_date": "Not Found",
            "eta_value": "Not Found"
        }

        if len(tracking_records) >= 1:
            first_row = tracking_records[0]
            for cell in first_row:
                if re.search(r'\d{2}-[A-Z]{3}-\d{4}', cell):
                    data["departure_date"] = clean_date(cell)
                    break
            if len(first_row) >= 3:
                data["departure_value"] = clean_location(first_row[-2])

        if len(tracking_records) >= 2:
            second_row = tracking_records[1]
            for cell in second_row:
                if re.search(r'\d{2}-[A-Z]{3}-\d{4}', cell):
                    data["eta_date"] = clean_date(cell)
                    break
            if len(second_row) >= 3:
                data["eta_value"] = clean_location(second_row[-2])

        log_debug("Returning data")
        return data

    except Exception as e:
        log_debug(f"Scraping error: {e}")
        return {"error": str(e)}
    finally:
        try:
            if 'driver' in locals():
                driver.quit()
        except OSError:
            pass
        except Exception:
            pass

if __name__ == "__main__":
    import sys
    import json
    if os.path.exists("debug.log"):
        os.remove("debug.log")
    container = sys.argv[1] if len(sys.argv) > 1 else "ECMU5627855"
    result = track_container(container)
    with open("result.json", "w") as f:
        json.dump(result, f)
    print(json.dumps(result))