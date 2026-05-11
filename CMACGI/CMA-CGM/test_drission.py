from DrissionPage import ChromiumPage, ChromiumOptions
import time

def test_drission():
    print("Initializing DrissionPage...")
    co = ChromiumOptions()
    # co.incognito() # Sometimes help
    # co.headless() # Headless can be detected more easily
    
    try:
        page = ChromiumPage(co)
        url = "https://www.cma-cgm.com/ebusiness/tracking"
        print(f"Visiting {url}...")
        page.get(url)
        
        print("Waiting for page load and anti-bot check...")
        time.sleep(10)
        
        # Check for CAPTCHA or Verification
        if "Verification Required" in page.html or "captcha" in page.html.lower():
            print("CAPTCHA detected. DrissionPage often handles this by mimicking real user moves.")
            # For DataDome, sometimes just a small scroll or mouse move helps
            page.scroll.down(200)
            time.sleep(5)
            
        print("Searching for Reference input...")
        # DrissionPage uses a simplified locator syntax
        input_ref = page.ele('#Reference', timeout=10)
        
        if input_ref:
            print("SUCCESS: Search input found!")
            page.get_screenshot("drission_success.png")
            
            print("Typing container number...")
            input_ref.input("ECMU5627855")
            
            btn = page.ele('#btnTracking')
            if btn:
                print("Clicking track button...")
                btn.click()
                time.sleep(10)
                page.get_screenshot("drission_results.png")
        else:
            print("Still blocked. Saving debug artifacts...")
            page.get_screenshot("drission_blocked.png")
            with open("drission_source.html", "w", encoding="utf-8") as f:
                f.write(page.html)

    except Exception as e:
        print(f"FAILED: {e}")
    finally:
        try:
            page.quit()
        except:
            pass

if __name__ == "__main__":
    test_drission()
