import sys
import time
import json
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def run_interasia(container_no):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            channel="chrome", # Use installed Chrome for robustness
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            # 1. Navigate
            print(f"[Worker] Navigating to Interasia...", file=sys.stderr)
            page.goto("https://www.interasia.cc/Service/Form?servicetype=0", timeout=60000)
            
            # 2. Input Container Number
            input_selector = "#wrapper > main > section:nth-child(2) > div > div:nth-child(1) > div > form > div.main-group > div > input[type=text]"
            print(f"[Worker] Typing container number {container_no}...", file=sys.stderr)
            page.locator(input_selector).fill(container_no)
            
            # 3. Click Submit
            submit_selector = "#containerSumbit"
            print(f"[Worker] Clicking Submit...", file=sys.stderr)
            page.locator(submit_selector).click()
            time.sleep(1)
            page.screenshot(path="debug_interasia_input.png")
            print("[Worker] Waiting for results...", file=sys.stderr)
            time.sleep(5) # Wait for initial load/search
            page.screenshot(path="debug_interasia_results.png")
            
            # Debug: Print the list group HTML to see what's there
            print("[Worker] Dumping list group HTML...", file=sys.stderr)
            try:
                list_group = page.locator(".m-list-group")
                if list_group.count() > 0:
                    print(list_group.inner_html(), file=sys.stderr)
                else:
                    print("[Worker] .m-list-group not found!", file=sys.stderr)
            except: pass

            # Fallback strategy: Click ANY detail link found
            fallback_selector = "p.detail > a"
            
            # Wait for detail link to appear
            print(f"[Worker] Waiting for Detail Link...", file=sys.stderr)
            # Correct Strategy: Click "Show History"
            print(f"[Worker] Searching for '(Show History)' link...", file=sys.stderr)
            try:
                # Try exact text match first
                if page.get_by_text("(Show History)").count() > 0:
                     print("[Worker] Found '(Show History)' text.", file=sys.stderr)
                     page.get_by_text("(Show History)").first.scroll_into_view_if_needed()
                     page.get_by_text("(Show History)").first.click(force=True)
                elif page.get_by_text("Show History").count() > 0:
                     print("[Worker] Found 'Show History' text.", file=sys.stderr)
                     page.get_by_text("Show History").first.click(force=True)
                elif page.locator(detail_link_selector).count() > 0:
                     print("[Worker] Found specific detail link (fallback).", file=sys.stderr)
                     page.locator(detail_link_selector).first.click(force=True)
                else:
                     raise Exception("No 'Show History' link found")

                print(f"[Worker] Clicked Detail Link. Waiting for page load...", file=sys.stderr)
                time.sleep(5) # Wait for detail page
            except Exception as e:
                print(f"[Worker] Detail link not found or click failed: {e}", file=sys.stderr)
                page.screenshot(path="debug_interasia_fail.png")
                print(json.dumps({"status": "not_found", "message": "Container not found or detail link missing"}))
                return

            # 5. Scrape Data with BeautifulSoup
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract basic info
            data = {}
            data["container_no"] = container_no
            
            # Function to extract tables
            # Interasia usually has tables with class "m-table" or similar structure
            # Let's extract all tables and try to make sense of them
            tables = soup.find_all('table')
            
            extracted_tables = []
            for i, table in enumerate(tables):
                headers = []
                rows = []
                
                # Get headers
                thead = table.find('thead')
                if thead:
                    th_cols = thead.find_all('th')
                    headers = [th.text.strip() for th in th_cols]
                
                # Get rows from body
                tbody = table.find('tbody')
                if tbody:
                    tr_rows = tbody.find_all('tr')
                    for tr in tr_rows:
                        cols = tr.find_all('td')
                        row_data = [col.text.strip() for col in cols]
                        if row_data:
                            # Map headers if available
                            if headers and len(headers) == len(row_data):
                                row_dict = dict(zip(headers, row_data))
                                
                                # Apply User's Transformation to "Port"
                                if "Port" in row_dict:
                                    raw_port = row_dict["Port"].replace("\n", "").replace(" ", "").strip()
                                    if len(raw_port) > 5:
                                        row_dict["Port"] = raw_port[:5] + "," + raw_port[5:]
                                    else:
                                         row_dict["Port"] = raw_port
                                         
                                rows.append(row_dict)
                            else:
                                rows.append(row_data)
                
                extracted_tables.append({"index": i, "headers": headers, "rows": rows})
            
            data["tables"] = extracted_tables
            
            # --- Event Parsing Logic ---
            # User Input: 
            # "EMPTY CONTAINER" -> Departed Origin
            # "DISCHARGED" -> Arrived at POD
            # Strategy: Scan Chronologically (Oldest -> Newest) to find the FIRST matching events.
            
            data["Departed_value"] = None
            data["Eta_value"] = None
            data["Eta_date"] = None
            
            if extracted_tables and "rows" in extracted_tables[0]:
                raw_rows = extracted_tables[0]["rows"]
                # Create a chronological list (assuming table is Newest -> Oldest)
                chronological_rows = raw_rows[::-1]
                
                for row in chronological_rows:
                    desc = row.get("Event Description", "").upper()
                    
                    # 1. Departed Origin (First "EMPTY CONTAINER")
                    if not data["Departed_value"] and "EMPTY CONTAINER" in desc:
                        data["Departed_value"] = row.get("Port")
                        
                    # 2. Arrived at POD (First "DISCHARGED")
                    if not data["Eta_value"] and "DISCHARGED" in desc:
                        data["Eta_value"] = row.get("Port")
                        data["Eta_date"] = row.get("Event Date")
            
            # Create the final response payload with only the requested keys
            final_response = {
                "Departed_value": data.get("Departed_value"),
                "Eta_value": data.get("Eta_value"),
                "Eta_date": data.get("Eta_date")
            }
            
            # Print the structured result for the API to consume
            print(json.dumps(final_response, indent=2))
            
            # Save full extracted data to store_data.json for user analysis
            with open('store_data.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print("[Worker] Full data dump saved to store_data.json", file=sys.stderr)

        except Exception as e:
            print(f"[Worker] Error: {e}", file=sys.stderr)
            print(json.dumps({"status": "error", "message": str(e)}))
        finally:
            browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "Missing container number"}))
        sys.exit(1)
    
    container_no = sys.argv[1]
    run_interasia(container_no)
