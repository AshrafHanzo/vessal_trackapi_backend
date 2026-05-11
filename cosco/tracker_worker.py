import sys
import time
import json
import re
import ssl
from bs4 import BeautifulSoup

# SSL Certificate verification workaround for Windows and Linux servers 
# with missing certificate stores.
ssl._create_default_https_context = ssl._create_unverified_context
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support import expected_conditions as EC

def run_cosco(container_no):

    options = Options()
    # headless=False so browser is visible as user requested
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 30)

    try:
        # 1. Navigate to Cosco tracking page
        print("[Worker] Navigating to Cosco tracking page...", file=sys.stderr)
        driver.get("https://elines.coscoshipping.com/ebusiness/cargotracking")
        time.sleep(6)  # Wait for cookie banner to appear

        # 2. Dismiss cookie banner using known CSS class 'btnBlue'
        print("[Worker] Checking for cookie banner...", file=sys.stderr)
        try:
            allow_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btnBlue")))
            driver.execute_script("arguments[0].click();", allow_btn)
            print("[Worker] Cookie banner dismissed via Allow All.", file=sys.stderr)
            time.sleep(5)  # Wait for page to fully settle
        except Exception:
            print("[Worker] No cookie banner found.", file=sys.stderr)

        # 3. Switch into the tracking iframe
        # The actual tracking form lives inside iframe id='scctCargoTracking'
        print("[Worker] Switching into tracking iframe...", file=sys.stderr)
        tracking_frame = wait.until(
            EC.presence_of_element_located((By.ID, "scctCargoTracking"))
        )
        driver.switch_to.frame(tracking_frame)
        print("[Worker] Inside tracking iframe.", file=sys.stderr)
        time.sleep(2)

        # 4. Click the Ant Design dropdown to open it (shows "Booking No." by default)
        print("[Worker] Clicking dropdown to select 'Container No.'...", file=sys.stderr)
        
        # We must use ActionChains for Ant Design dropdowns to trigger the native click event properly
        from selenium.webdriver.common.action_chains import ActionChains
        dropdown_selector = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".ant-select-selection-item"))
        )
        ActionChains(driver).move_to_element(dropdown_selector).click().perform()
        time.sleep(1.5)

        # 5. Select "Container No." from the Ant Design dropdown options
        print("[Worker] Selecting 'Container No.' option...", file=sys.stderr)
        container_option = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[contains(@class,'ant-select-item-option-content') and text()='Container No.']")
            )
        )
        ActionChains(driver).move_to_element(container_option).click().perform()
        print("[Worker] 'Container No.' selected.", file=sys.stderr)
        time.sleep(1)

        # 6. Enter container number into the Ant Design input
        print(f"[Worker] Entering container number: {container_no}...", file=sys.stderr)
        search_input = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input.ant-input"))
        )
        search_input.clear()
        search_input.send_keys(container_no)
        print("[Worker] Container number entered.", file=sys.stderr)
        time.sleep(1)

        # 7. Click the Search button inside the iframe
        print("[Worker] Clicking Search button...", file=sys.stderr)
        search_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[.//span[text()='Search'] or text()='Search']")
            )
        )
        search_btn.click()
        print("[Worker] Search clicked. Waiting for results...", file=sys.stderr)
        time.sleep(15)  # Wait for results to load - increased from 8s 

        # 8. Dump Data (Development mode)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        eta_date = ""
        # 1. Extract Last Pod Eta OR Last Pod Ata
        # The text might be "Last Pod Eta" or "Last Pod Ata" and split by HTML elements, 
        # so we scan layout for a matching inner text.
        pod_element = soup.find(lambda t: t.name and t.text and ("Pod Eta" in t.text or "Pod Ata" in t.text) and len(t.text) < 40)
        
        if pod_element:
            # We get the parent container block text and use a regular expression to cleanly extract the date
            # This avoids issues where the container number or "Size Type 40HQ" is returned instead of the date.
            parent = pod_element.find_parent("div")
            if parent:
                parent_text = " ".join(parent.stripped_strings)
                match = re.search(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', parent_text)
                if match:
                    eta_date = match.group(0)

        departed_value = ""
        departed_date = ""
        
        # 2. Extract Event Location and Event Time from the tracking table
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            is_tracking_table = any("event location" in h or "event time" in h for h in headers)
            
            if is_tracking_table:
                # Determine column indices from headers, with defaults matching screenshot
                time_idx = 1
                loc_idx = 2
                for i, h in enumerate(headers):
                    if "time" in h: time_idx = i
                    if "location" in h: loc_idx = i
                
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if cells and len(cells) > max(time_idx, loc_idx):
                        departed_date = cells[time_idx].get_text(strip=True)
                        departed_value = cells[loc_idx].get_text(strip=True)
                        break # Only need the first/latest row's data
                
                if departed_value: 
                    break

        # 3. Build the final output format exactly as requested
        final_data = {
            "Departed_value": departed_value,
            "Departed date": departed_date,
            "Eta_value": "Chennai(IN)",
            "Eta_date": eta_date
        }

        print(json.dumps(final_data, indent=2))

    except Exception as e:
        print(f"[Worker] Error: {e}", file=sys.stderr)
        print(json.dumps({"status": "error", "message": str(e)}))

    finally:
         try:
             browser_pid = driver.browser_pid
         except:
             browser_pid = None
             
         print("[Worker] Closing browser...", file=sys.stderr)
         try:
             driver.quit()
         except Exception:
             pass
             
         if browser_pid:
             os.system(f"taskkill /F /PID {browser_pid} /T >nul 2>&1")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "Missing container number"}))
        sys.exit(1)

    container_no = sys.argv[1]
    run_cosco(container_no)
