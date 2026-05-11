import undetected_chromedriver as uc
import time
import os

def check_access():
    options = uc.ChromeOptions()
    # options.add_argument("--headless") # Don't use headless for this test
    options.add_argument("--window-size=1920,1080")
    
    # Try to match the actual version
    # Chrome/146.0.7680.80
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")

    print("Initializing Chrome...")
    try:
        driver = uc.Chrome(options=options)
        print("Visiting CMA CGM...")
        driver.get("https://www.cma-cgm.com/ebusiness/tracking")
        
        time.sleep(10)
        
        print("Saving screenshot to 'access_test.png'...")
        driver.save_screenshot("access_test.png")
        
        source = driver.page_source.lower()
        if "temporarily restricted" in source:
            print("ACCESS RESTRICTRICTED")
        elif "reference" in source:
            print("SUCCESS: Input field found")
        else:
            print("Unknown state. Check access_test.png")
            
        with open("access_test_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    check_access()
