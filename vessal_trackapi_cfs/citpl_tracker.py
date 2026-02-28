import sys
import json
import re
from playwright.sync_api import sync_playwright

def track_citpl(container_number: str) -> dict:
    """Track a container on CITPL and return the result as dict"""
    
    result = {
        "container_number": container_number,
        "status": "pending",
        "data": {
            "cfs_code": None,
            "cfs_in": None,
            "cfs_out": None,
            "scan": False
        }

    }
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            # 1. Navigate to CITPL Tracking Page
            sys.stderr.write("DEBUG: Navigating to CITPL...\n")
            page.goto("https://cp.citpl.co.in/enquiry/ctrHist", timeout=60000)
            
            # 2. Wait for Input Field
            sys.stderr.write("DEBUG: Waiting for input field...\n")
            input_selector = "#containerNoContainerHistoryFormId-inputEl"
            page.wait_for_selector(input_selector, state="visible", timeout=30000)
            
            # 3. Enter Container Number
            sys.stderr.write(f"DEBUG: Entering container no: {container_number}\n")
            page.fill(input_selector, container_number)
            
            # Press Tab to trigger validation/enable button
            sys.stderr.write("DEBUG: Pressing Tab to verify container number...\n")
            page.press(input_selector, "Tab")
            page.wait_for_timeout(1000) # Wait for validation
            
            # 4. Click Refresh/Search Button
            sys.stderr.write("DEBUG: Clicking Refresh button...\n")
            refresh_btn_selector = "#containerHistoryRefreshButtonlId" 
            page.click(refresh_btn_selector)
            
            # 5. Wait for Results (Look for Terminal Code or Status field population)
            sys.stderr.write("DEBUG: Waiting for results...\n")
            terminal_selector = "#terminalCdContainerHistoryFormId-inputEl"
            page.wait_for_selector(terminal_selector, state="visible", timeout=30000)
            page.wait_for_timeout(2000)

            # 7. Extract Entry Date & Time using Labels
            # sys.stderr.write("DEBUG: Clicking 'Entry' tab...\n")
            page.click("#button-1030") # Entry Button
            page.wait_for_timeout(2000) 
            
            # JS to find input ID by iterating labels with specific text
            js_find_input_value_by_label = """
                (labelText) => {
                    const labels = Array.from(document.querySelectorAll('label'));
                    // Find label containing text (case insensitive)
                    const targetLabel = labels.find(l => l.innerText.trim().toLowerCase().includes(labelText.toLowerCase()));
                    
                    if (targetLabel && targetLabel.htmlFor) {
                        const input = document.getElementById(targetLabel.htmlFor);
                        return input ? input.value : null;
                    }
                    return null;
                }
            """
            
            entry_val = page.evaluate(js_find_input_value_by_label, "Entry DTTM")
            if entry_val:
                result["data"]["cfs_in"] = entry_val.strip()
            
            fpd_entry = page.evaluate(js_find_input_value_by_label, "FPD")
            if fpd_entry:
                result["data"]["cfs_code"] = fpd_entry.strip()

            
            # 8. Extract Exit Date & Time
            # sys.stderr.write("DEBUG: Clicking 'Exit' tab...\n")
            page.click("#button-1031") # Exit Button
            page.wait_for_timeout(2000)
            
            exit_val = page.evaluate(js_find_input_value_by_label, "Exit DTTM")
            if exit_val:
                result["data"]["cfs_out"] = exit_val.strip()
            
            fpd_exit = page.evaluate(js_find_input_value_by_label, "FPD")
            if fpd_exit:
                result["data"]["cfs_code"] = fpd_exit.strip()


            # 9. Extract Scan checkbox state
            scan_checked = page.evaluate("""
                () => {
                    const cb = document.getElementById('cmccheckboxfield-1067-inputEl');
                    if (!cb) return false;
                    // ExtJS marks checked checkboxes with x-form-cb-checked class on wrapper
                    const wrapper = cb.closest('.x-form-cb-wrap-inner') || cb.parentElement;
                    if (wrapper && wrapper.classList.contains('x-form-cb-checked')) return true;
                    // Alternative: check the input element itself for checked class
                    return cb.classList.contains('x-form-cb-checked');
                }
            """)
            result["data"]["scan"] = bool(scan_checked)

            result["status"] = "success"
            print(json.dumps(result))
            
        except Exception as e:
            sys.stderr.write(f"ERROR: {e}\n")
            result["status"] = "error"
            result["error"] = str(e)
            print(json.dumps(result))
            
        finally:
            browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No container number provided"}))
    else:
        track_citpl(sys.argv[1])
