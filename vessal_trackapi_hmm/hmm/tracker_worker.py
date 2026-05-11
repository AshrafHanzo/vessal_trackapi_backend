import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def get_tracking_data(container_number: str):
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    options.add_argument('--incognito')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        driver.get("https://www.hmm21.com/company.do")
        wait = WebDriverWait(driver, 20)
        
        # 1. Wait for initial page load and add human-like scrolling
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, 300);")
        time.sleep(2)
        
        # Try to close chatbot popup if it exists
        try:
            close_btns = driver.find_elements(By.XPATH, "//button[contains(@class, 'close') or contains(@class, 'btn-close')] | //i[contains(@class, 'close')]")
            for btn in close_btns:
                if btn.is_displayed():
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(1)
        except:
            pass
            
        # 2. Click the specific Track & Trace tab element from original request
        menu_selector = "#overlay-eservice-gate-wrap > div > div.background_white > div.background_navy > div > ul > li:nth-child(2) > span"
        menu_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, menu_selector)))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", menu_btn)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", menu_btn)
        time.sleep(2)
        
        # 3. Click the Container No radio label
        radio_selector = "#overlay-eservice-gate-wrap > div > div.eservice-form > form > div.tracktace > div:nth-child(1) > div > label:nth-child(4)"
        radio_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, radio_selector)))
        driver.execute_script("arguments[0].click();", radio_btn)
        time.sleep(1)
        
        # 4. Enter Container Number
        try:
            input_elem = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#selectTnt")))
        except:
            input_elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#selectTnt")))
            
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", input_elem)
        time.sleep(1)
        driver.execute_script("arguments[0].value = '';", input_elem)
        
        try:
            input_elem.send_keys(container_number)
        except Exception as e:
            # Fallback to pure javascript injection if the element is obscured
            print(f"Warning: Standard send_keys failed ({e}). Falling back to JS injection.")
            driver.execute_script(f"arguments[0].value = '{container_number}';", input_elem)
            
        time.sleep(1)
        
        # 5. Click Search
        search_selector = "#overlay-eservice-gate-wrap > div > div.eservice-form > form > div.tracktace > button"
        search_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, search_selector)))
        driver.execute_script("arguments[0].click();", search_btn)
        
        # Wait for tracking results
        time.sleep(15)
        
        # Switch to the new tab where results open
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            time.sleep(5)
            
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        
        if container_number not in page_source:
             return {"error": "Entered container number not found in the results table."}
             
        departure_value = None
        eta_value = None
        eta_date = None
        dest_departure = None
        
        tables = soup.find_all('table')
        for table in tables:
            headers = [th.text.strip() for th in table.find_all('th')]
            
            if 'Origin' in headers and 'Destination' in headers:
                rows = table.find_all('tr')
                for row in rows:
                    cols = [c.text.strip() for c in row.find_all(['th', 'td'])]
                    if not cols or len(cols) < 5:
                        continue
                        
                    row_header = cols[0]
                    if row_header == 'Location':
                        departure_value = cols[1]  # Origin
                        eta_value = cols[4]        # Destination
                    elif row_header == 'Arrival(ETB)' or row_header == 'Arrival':
                        eta_date = cols[4]
                    elif row_header == 'Departure':
                        dest_departure = cols[4]
                        
            elif 'Loading Port' in headers and 'Discharging Port' in headers:
                 rows = table.find_all('tr')
                 for row in rows:
                    cols = [c.text.strip() for c in row.find_all(['th', 'td'])]
                    if not cols or len(cols) < 5:
                        continue
                    row_header = cols[0]
                    if row_header == 'Arrival(ETB)' or row_header == 'Arrival':
                        eta_date = cols[4] if not eta_date else eta_date
        
        if dest_departure and dest_departure != '' and eta_date and eta_date != dest_departure:
            eta_date = dest_departure
            
        if not departure_value and not eta_value:
             return {"error": "Could not parse tracking tables for the container."}
             
        response = {
            "container_number": container_number,
            "departure_value": departure_value,
            "ETA_value": eta_value,
            "ETA_date": eta_date
        }
        
        return {"status": "success", "data": response}
        
    except Exception as e:
        return {"error": f"Exception occurred: {str(e)}"}
    finally:
        try:
            # Clear cookies and local storage explicitly
            driver.delete_all_cookies()
            driver.execute_script("window.localStorage.clear();")
            driver.execute_script("window.sessionStorage.clear();")
        except:
            pass
        driver.quit()

if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) > 1:
        container = sys.argv[1].strip()
    else:
        container = "KOCU4829423"  # Default test container

    result = get_tracking_data(container)
    print(json.dumps(result))

