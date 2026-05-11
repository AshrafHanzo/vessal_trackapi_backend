import undetected_chromedriver as uc
import time
import sys

def test_uc():
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")

    try:
        print("Initializing Chrome...")
        driver = uc.Chrome(options=options, version_main=145)
        
        print("Visiting URL...")
        driver.get("https://www.cma-cgm.com/ebusiness/tracking")
        
        print("Waiting 15 seconds to let DataDome process...")
        time.sleep(15)
        
        print("Taking screenshot...")
        driver.save_screenshot("test_uc_screenshot.png")
        
        print("Dumping HTML...")
        with open("test_uc_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
            
        print("Success! Checking for Reference input...")
        from selenium.webdriver.common.by import By
        inputs = driver.find_elements(By.CSS_SELECTOR, "#Reference")
        print(f"Inputs found: {len(inputs)}")

    except Exception as e:
        print(f"FAILED: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    test_uc()
