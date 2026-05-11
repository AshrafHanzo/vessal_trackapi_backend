from seleniumbase import Driver
import time

def test_sb_uc():
    print("Initializing Driver with uc=True...")
    try:
        driver = Driver(uc=True, headless=False)
        
        url = "https://www.cma-cgm.com/ebusiness/tracking"
        print(f"Visiting {url}...")
        driver.uc_open_with_reconnect(url, reconnect_time=3)
        
        print("Waiting for DataDome to settle...")
        time.sleep(10)
        
        # Check if we are still on the verification page
        if "Verification Required" in driver.page_source or "captcha" in driver.page_source.lower():
            print("CAPTCHA detected. Attempting to click through...")
            # Try to find the iframe and switch to it, or use SB's automated clicker
            try:
                # This is a specific SB method for clicking through DataDome/Cloudflare
                driver.uc_gui_click_captcha()
                print("Click attempted. Waiting for reload...")
                time.sleep(10)
            except Exception as e:
                print(f"Bypass click failed: {e}")

        print("Checking for Reference input...")
        if driver.is_element_visible("#Reference"):
            print("SUCCESS: Search input found!")
            driver.save_screenshot("sb_success.png")
            # Try to actually track a sample container
            driver.type("#Reference", "ECMU5627855")
            driver.click("#btnTracking")
            time.sleep(10)
            driver.save_screenshot("sb_results.png")
        else:
            print("Still blocked. Saving debug artifacts...")
            driver.save_screenshot("sb_final_debug.png")
            with open("sb_final_source.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)

    except Exception as e:
        print(f"FAILED: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    test_sb_uc()
