from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
import time

def test_stealth():
    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--headless")

    try:
        print("Initializing ChromeDriver via webdriver-manager...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )

        print("Visiting URL...")
        driver.get("https://www.google.com")
        print(f"Page title: {driver.title}")
        
        print("Visiting CMA-CGM...")
        driver.get("https://www.cma-cgm.com/ebusiness/tracking")
        time.sleep(10)
        
        driver.save_screenshot("stealth_test_screenshot.png")
        print("Screenshot saved.")

    except Exception as e:
        print(f"FAILED: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    test_stealth()
