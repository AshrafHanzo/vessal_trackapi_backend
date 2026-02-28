"""
tracker_worker.py — Runs as a standalone subprocess.
Called by tracker.py via subprocess.run().
Receives: port_name, mbl_number, bl_number as CLI args.
Outputs: JSON result to stdout.
"""
import sys
import json
import traceback

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

ICEGATE_URL = "https://foservices.icegate.gov.in/#/public-enquiries/document-status/sea-igm"


def run(port_name: str, mbl_number: str, bl_number: str) -> dict:
    result = {
        "port_name": port_name,
        "mbl_number": mbl_number,
        "bl_number": bl_number,
        "status": "pending",
        "message": "",
        "found_port": None
    }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            # Step 1: Navigate
            print("[Worker] Navigating to Icegate...", file=sys.stderr, flush=True)
            page.goto(ICEGATE_URL, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(2000)

            # Debug: print all input fields visible on page
            inputs = page.query_selector_all("input")
            print(f"[Worker] Total inputs on page: {len(inputs)}", file=sys.stderr, flush=True)
            for i, inp in enumerate(inputs):
                try:
                    ph = inp.get_attribute("placeholder") or ""
                    tp = inp.get_attribute("type") or ""
                    ac = inp.get_attribute("aria-autocomplete") or ""
                    print(f"  input[{i}] type={tp} placeholder={ph!r} aria-autocomplete={ac!r}", file=sys.stderr, flush=True)
                except Exception:
                    pass

            # Parse port codes (comma-separated from tracker.py)
            port_codes = port_name.split(",")
            print(f"[Worker] Port codes to try: {port_codes}", file=sys.stderr, flush=True)

            port_js = (
                'document.querySelector("#filter-section > div.col-lg-3.col-md-4.ds-sea-igm-style-0 > div > '
                'div.search-box > ng-select > div > div > div.ng-input > input[type=text]")'
            )
            mbl_js = (
                'document.querySelector("#filter-section > div.col-lg-3.col-md-4.search-filter > '
                'div > div.search-box > input")'
            )
            btn_css = "#filter-section > div.col-md-2.p-4 > button"

            # Wait for port input element to be present
            page.wait_for_function(f"() => {port_js} !== null", timeout=30000)
            
            found_success = False  # Flag to break out of all loops

            for port_code in port_codes:
                if found_success:
                    break
                    
                print(f"\n[Worker] ===== Trying port code: {port_code} =====", file=sys.stderr, flush=True)
                
                # Step 2: Port Name discovery with scrolling
                print(f"[Worker] Activating port selection via JS path...", file=sys.stderr, flush=True)

                # Explicitly click and focus
                page.evaluate(f"{port_js}.click()")
                page.evaluate(f"{port_js}.focus()")
                page.wait_for_timeout(500)

                # Clear previous value
                page.evaluate(f"{port_js}.value = ''")
                page.evaluate(f"{port_js}.dispatchEvent(new Event('input', {{bubbles: true}}))")
                page.wait_for_timeout(300)

                # Type via keyboard to trigger dropdown
                print(f"[Worker] Typing port prefix: {port_code}", file=sys.stderr, flush=True)
                for char in port_code:
                    page.keyboard.type(char)
                    page.wait_for_timeout(80)
                page.wait_for_timeout(1000)

                # Exhaustive Discovery: Scroll through the dropdown to find all options
                print("[Worker] Discovering all terminals via scrolling...", file=sys.stderr, flush=True)
                all_terminals = set()
                
                dropdown_panel_selector = ".ng-dropdown-panel-items, .ng-dropdown-panel"
                try:
                    page.wait_for_selector(dropdown_panel_selector, timeout=5000)
                    
                    last_count = -1
                    for _ in range(10):
                        option_els = page.query_selector_all(".ng-option")
                        for opt in option_els:
                            txt = opt.inner_text().strip()
                            if txt.startswith(port_code):
                                all_terminals.add(txt)
                        
                        if len(all_terminals) == last_count:
                            break
                        last_count = len(all_terminals)
                        
                        page.evaluate(f"""
                            var panel = document.querySelector("{dropdown_panel_selector}");
                            if(panel) panel.scrollTop += 300;
                        """)
                        page.wait_for_timeout(500)
                except Exception as e:
                    print(f"[Worker] Dropdown scrolling failed or not needed: {e}", file=sys.stderr, flush=True)
                
                terminal_list = sorted(list(all_terminals))
                if not terminal_list:
                    terminal_list = [port_code]
                
                print(f"[Worker] Terminals for {port_code}: {terminal_list}", file=sys.stderr, flush=True)


                for terminal in terminal_list:
                  try:
                    print(f"[Worker] >>> Trying terminal: {terminal}", file=sys.stderr, flush=True)
                    
                    # Re-select terminal with explicit click/focus on JS path
                    page.evaluate(f"{port_js}.click()")
                    page.evaluate(f"{port_js}.focus()")
                    page.wait_for_timeout(500)
                    
                    # Clear content before typing terminal
                    page.evaluate(f"{port_js}.value = ''")
                    page.evaluate(f"{port_js}.dispatchEvent(new Event('input', {{bubbles: true}}))")
                    
                    for char in terminal:
                        page.keyboard.type(char)
                        page.wait_for_timeout(50)
                    page.wait_for_timeout(500)
                    
                    # Click the matching option
                    target_option = page.query_selector(f".ng-option:has-text('{terminal}')")
                    if target_option:
                        target_option.click()
                    else:
                        page.keyboard.press("Enter")
                    page.wait_for_timeout(800)

                    # Step 3: MBL Number
                    print(f"[Worker] Entering MBL: {mbl_number}", file=sys.stderr, flush=True)
                    page.evaluate(f"""
                        var el = {mbl_js};
                        el.value = '{mbl_number}';
                        el.dispatchEvent(new Event('input', {{bubbles: true}}));
                        el.dispatchEvent(new Event('change', {{bubbles: true}}));
                    """)
                    page.wait_for_timeout(500)

                    # Step 4: Click Search
                    print("[Worker] Clicking Search button...", file=sys.stderr, flush=True)
                    try:
                        page.locator(btn_css).click(force=True, timeout=5000)
                    except:
                        try:
                            page.evaluate(f'document.querySelector("{btn_css}").click()')
                        except:
                            print("[Worker] ⚠️ Search button click failed entirely!", file=sys.stderr, flush=True)
                            continue
                    
                    print("[Worker] Search clicked. Detecting response...", file=sys.stderr, flush=True)

                    # Step 5: Fast polling loop — check every 500ms for popup/results
                    # DO NOT use 'table' or 'mat-table' — they exist on the page before search
                    user_paginator_js = 'document.querySelector("#tablerecords > mat-paginator > div > div > div.mat-paginator-page-size.ng-star-inserted > mat-form-field > div > div.mat-form-field-flex")'
                    
                    found_result = False
                    found_popup = False
                    # Infinite polling as requested
                    print(f"[Worker] Entering infinite polling for {terminal}...", file=sys.stderr, flush=True)
                    poll = 0
                    while True:
                        poll += 1
                        page.wait_for_timeout(500)
                        
                        # Check for popup FIRST (fastest response)
                        # Use evaluate for potentially faster text retrieval than locator.inner_text()
                        popup_text = page.evaluate("document.body.innerText")
                        if "No records found" in popup_text or "No data found" in popup_text or "Record Not Found" in popup_text or "No details found" in popup_text:
                            print(f"[Worker] No records for {terminal} (detected in {(poll)*0.5}s). Dismissing...", file=sys.stderr, flush=True)
                            try:
                                page.locator("button:has-text('OK'), button:has-text('Close'), .mat-button").first.click(timeout=2000)
                            except: pass
                            page.wait_for_timeout(500)
                            found_popup = True
                            break
                        
                        # Check for results
                        paginator_found = page.evaluate(f"{user_paginator_js} !== null")
                        tablerecords_found = page.evaluate('document.querySelector("#tablerecords") !== null')
                        
                        if paginator_found or tablerecords_found:
                            print(f"[Worker] Results detected in {(poll)*0.5}s! paginator={paginator_found}, table={tablerecords_found}", file=sys.stderr, flush=True)
                            found_result = True
                            break

                        # Retry Click every 20s (40 polls)
                        if poll % 40 == 0:
                             print(f"[Worker] ⚠️ Still waiting for {terminal} ({poll*0.5}s). Clicking Search again...", file=sys.stderr, flush=True)
                             try:
                                 v_text = page.evaluate("document.body.innerText")
                                 print(f"[Worker] DOM SAMPLE (500 chars):\n{v_text[:500]}", file=sys.stderr, flush=True)
                                 page.locator(btn_css).click(force=True)
                             except: pass
                    
                    if found_popup:
                        continue  # Next terminal
                    
                    # Since loop is infinite, we only reach here if found_result=True (via break)
                    # No need for failure check block anymore

                    # Step 6: Paginator — set to 100
                    print(f"[Worker] ✅ Results found for {terminal}! Setting page size to 100...", file=sys.stderr, flush=True)
                    try:
                        page.evaluate(f"{user_paginator_js}.click()")
                        page.wait_for_timeout(1000)
                        
                        # Select '100' option
                        option_100 = page.locator("mat-option:has-text('100')")
                        if option_100.count() > 0:
                            option_100.first.click()
                            print("[Worker] Page size set to 100.", file=sys.stderr, flush=True)
                            page.wait_for_timeout(1500)
                        else:
                            print("[Worker] '100' option not found in paginator dropdown.", file=sys.stderr, flush=True)
                    except Exception as e:
                        print(f"[Worker] Paginator interaction failed: {e}", file=sys.stderr, flush=True)

                    # Step 7: Search for BL number in rows
                    print(f"[Worker] Searching rows for BL number: {bl_number}", file=sys.stderr, flush=True)
                    page.wait_for_timeout(1000)
                    
                    rows = page.query_selector_all("#tablerecords mat-row, #tablerecords tr")
                    print(f"[Worker] Found {len(rows)} rows in table.", file=sys.stderr, flush=True)
                    view_clicked = False
                    for row in rows:
                        row_text = row.inner_text() or ""
                        if bl_number in row_text:
                            print(f"[Worker] Found BL {bl_number} in row!", file=sys.stderr, flush=True)
                            view_btn = row.query_selector("button, a, .mat-button")
                            if view_btn:
                                print(f"[Worker] Clicking View button...", file=sys.stderr, flush=True)
                                view_btn.click()
                                view_clicked = True
                                page.wait_for_timeout(3000)
                            break

                    if not view_clicked:
                        print(f"[Worker] BL {bl_number} not found in {terminal}. Trying next...", file=sys.stderr, flush=True)
                        continue

                    # Step 8: Extract IGM details
                    print("[Worker] Extracting IGM details from table...", file=sys.stderr, flush=True)
                    try:
                        page.wait_for_selector("mat-row, tr.mat-row, .mat-row, .mat-table, table", timeout=20000)
                        page.wait_for_timeout(1500)
                        
                        table_containers = page.query_selector_all("mat-table, table, .mat-table")
                        correct_table = None
                        headers = []
                        col_indices = {}

                        for container in table_containers:
                            header_els = container.query_selector_all("mat-header-cell, .mat-header-cell, th")
                            current_headers = [h.inner_text().strip().lower().replace("\n", " ") for h in header_els]
                            
                            igm_idx = next((idx for idx, h in enumerate(current_headers) if "igm" in h and "no" in h), None)
                            if igm_idx is not None:
                                correct_table = container
                                headers = current_headers
                                col_indices['igm_no'] = igm_idx
                                col_indices['igm_date'] = next((idx for idx, h in enumerate(current_headers) if "igm" in h and "date" in h), None)
                                col_indices['inw_date'] = next((idx for idx, h in enumerate(current_headers) if "inw" in h), None)
                                break
                        
                        if correct_table:
                            rows = correct_table.query_selector_all("mat-row, .mat-row, tr")
                            rows = [r for r in rows if r.query_selector("mat-cell, .mat-cell, td")]
                            if rows:
                                cells = rows[0].query_selector_all("mat-cell, .mat-cell, td")
                                cell_texts = [c.inner_text().strip() or c.text_content().strip() for c in cells]
                                
                                result["igm_no"]   = cell_texts[col_indices['igm_no']] if col_indices.get('igm_no') is not None else ""
                                result["igm_date"] = cell_texts[col_indices['igm_date']] if col_indices.get('igm_date') is not None else ""
                                result["inw_date"] = cell_texts[col_indices['inw_date']] if col_indices.get('inw_date') is not None else ""
                                
                                if any([result["igm_no"], result["igm_date"], result["inw_date"]]):
                                    result["status"] = "success"
                                    result["message"] = "Extracted details successfully."
                                    result["found_port"] = terminal # Store the successful terminal/port
                                    print(f"[Worker] ✅ Result found in {terminal}", file=sys.stderr, flush=True)
                                    found_success = True
                                    break # Exit the terminal loop
                    except Exception as e:
                        print(f"[Worker] Extraction failed for {terminal}: {e}", file=sys.stderr, flush=True)

                  except Exception as terminal_err:
                    print(f"[Worker] ⚠️ Error processing terminal {terminal}: {terminal_err}", file=sys.stderr, flush=True)
                    import traceback as tb
                    tb.print_exc(file=sys.stderr)
                    continue


            if not found_success:
                 print("[Worker] 📸 Saving debug screenshot to 'debug_failed.png'...", file=sys.stderr, flush=True)
                 try:
                     page.screenshot(path="debug_failed.png", full_page=True)
                 except: pass

            print("[Worker] Done. Closing browser.", file=sys.stderr, flush=True)
            browser.close()


    except PlaywrightTimeoutError as e:
        result["status"] = "error"
        result["message"] = f"Timeout: {str(e)}"
        print(f"[Worker] Timeout: {e}", file=sys.stderr, flush=True)

    except Exception as e:
        result["status"] = "error"
        result["message"] = f"Error: {str(e)}"
        print(f"[Worker] Exception:\n{traceback.format_exc()}", file=sys.stderr, flush=True)

    # If status is still pending after all loops, it means we didn't find anything
    if result["status"] == "pending":
        result["status"] = "not_found"
        result["message"] = "No records found in any mapped terminal after exhaustive search."

    return result


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(json.dumps({"status": "error", "message": "Usage: tracker_worker.py <port_name> <mbl_number> <bl_number>"}))
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2], sys.argv[3])
    print(json.dumps(result))
