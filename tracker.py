"""
Tracker Script - Run by main.py as subprocess
Opens browser, tracks container, outputs JSON result
"""

import sys
import json
import re
from playwright.sync_api import sync_playwright


def track_container(container_number: str) -> dict:
    """Track a container and return the result as dict"""
    
    result = {
        "container_number": container_number,
        "tracking_data": []
    }
    
    with sync_playwright() as p:
        # Launch browser (headless=False to see it)
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            # Go to tracking page and wait for it to load
            page.goto("https://www.sealioncargo.com/track.html")
            
            # Use explicit polling to find the input element (handling Shadow DOM)
            sys.stderr.write("DEBUG: Waiting for input field...\\n")
            input_found = False
            for _ in range(30):  # Try for 30 seconds
                try:
                    js_path = 'document.querySelector("#tracking_system_root").shadowRoot.querySelector("#app-root > div > div.container-tracking-VvPpX6 > div > div.container-tracking-r8H33s > div > div > input[type=text]")'
                    element_exists = page.evaluate(f'!!({js_path})')
                    if element_exists:
                        input_found = True
                        break
                except Exception:
                    pass
                page.wait_for_timeout(1000)
            
            if not input_found:
                raise Exception("Input field not found after 30 seconds")

            # Additional small wait to be safe
            page.wait_for_timeout(1000)

            # Enter container number in shadow DOM input
            input_eval_js = '''
                const input = document.querySelector("#tracking_system_root")
                    .shadowRoot.querySelector("#app-root > div > div.container-tracking-VvPpX6 > div > div.container-tracking-r8H33s > div > div > input[type=text]");
                input.focus();
                input.value = '';
            '''
            page.evaluate(input_eval_js)
            
            # Type the container number character by character
            page.keyboard.type(container_number, delay=100)
            
            # Trigger input event just in case
            trigger_js = '''
                const input = document.querySelector("#tracking_system_root")
                    .shadowRoot.querySelector("#app-root > div > div.container-tracking-VvPpX6 > div > div.container-tracking-r8H33s > div > div > input[type=text]");
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
            '''
            page.evaluate(trigger_js)
            
            # Click search button
            sys.stderr.write("DEBUG: Looking for search button...\\n")
            button_found = False
            for _ in range(10): # Try for 10 seconds
                 try:
                    btn_path = 'document.querySelector("#tracking_system_root").shadowRoot.querySelector("#app-root > div > div.container-tracking-VvPpX6 > div > div.container-tracking-r8H33s > div > button")'
                    exists = page.evaluate(f'!!({btn_path})')
                    if exists:
                        button_found = True
                        break
                 except Exception:
                     pass
                 page.wait_for_timeout(1000)
            
            if not button_found:
                 raise Exception("Search button not found")

            # Click search button
            click_js = '''
                document.querySelector("#tracking_system_root")
                    .shadowRoot.querySelector("#app-root > div > div.container-tracking-VvPpX6 > div > div.container-tracking-r8H33s > div > button")
                    .click()
            '''
            page.evaluate(click_js)
            
            # Wait for results to load - Polling for "Route" header
            sys.stderr.write("DEBUG: Waiting for 'Route' header...\\n")
            results_found = False
            for _ in range(60): # Wait up to 60 seconds
                is_loaded = page.evaluate('''
                    (() => {
                        const root = document.querySelector("#tracking_system_root");
                        if (!root || !root.shadowRoot) return false;
                        const text = root.shadowRoot.textContent;
                        return text.includes("Route") && text.includes("Vessel");
                    })()
                ''')
                
                if is_loaded:
                    results_found = True
                    sys.stderr.write("DEBUG: Route header found! Waiting for details...\\n")
                    break
                page.wait_for_timeout(1000)
            
            if not results_found:
                 sys.stderr.write("WARNING: Timed out waiting for 'Route' text. Attempting extraction anyway...\\n")
            
            # Explicit wait for details to render (10 seconds)
            # The list items render dynamically after the main header
            page.wait_for_timeout(10000)
            
            # Click on "Details" tab to reveal the timeline events
            sys.stderr.write("DEBUG: Looking for 'Details' tab...\\n")
            try:
                click_details_js = '''
                    (() => {
                        const shadowRoot = document.querySelector("#tracking_system_root").shadowRoot;
                        const allElements = shadowRoot.querySelectorAll("*");
                        for (const el of allElements) {
                            if (el.textContent.trim() === "Details") {
                                el.click();
                                return true;
                            }
                        }
                        return false;
                    })()
                '''
                clicked = page.evaluate(click_details_js)
                if clicked:
                    sys.stderr.write("DEBUG: Clicked Details tab, waiting for content...\\n")
                    page.wait_for_timeout(3000)
                else:
                    sys.stderr.write("DEBUG: Could not find Details tab\\n")
            except Exception as e:
                sys.stderr.write(f"DEBUG: Error clicking Details: {e}\\n")
            
            # Extract innerHTML from the Shadow DOM for HTML-based parsing
            extract_html_js = '''
                (() => {
                    const shadowRoot = document.querySelector("#tracking_system_root").shadowRoot;
                    const container = shadowRoot.querySelector("#app-root");
                    if (!container) return "";
                    return container.innerHTML;
                })()
            '''
            html_content = page.evaluate(extract_html_js)
            
            if html_content:
                sys.stderr.write(f"DEBUG: Extracted HTML content ({len(html_content)} chars)\\n")
                
                # Save HTML for debugging
                with open("debug_html.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                
                # Parse with BeautifulSoup
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html_content, 'lxml')
                
                # Use stripped_strings to get individual text pieces in order
                lines = []
                for text in soup.stripped_strings:
                    # Split by internal newlines to get granular lines
                    for subline in text.split('\n'):
                        subline = subline.strip()
                        if not subline:
                            continue
                        # Skip junk
                        if any(x in subline.lower() for x in ['leaflet', 'svg']):
                            continue
                        if re.match(r'^[0-9.\s,MLCZmlcz]+$', subline):
                            continue
                        if len(subline) > 1:
                            lines.append(subline)
                
                sys.stderr.write(f"DEBUG: Extracted {len(lines)} granular text lines from HTML\n")
                
                # Save lines for debugging
                with open("debug_lines.txt", "w", encoding="utf-8") as f:
                    f.write("\n".join(lines))
                
                # Group lines into blocks by location
                blocks = []
                current_block = {"location": "Unknown", "items": []}
                
                for line in lines:
                    if line in ["Route", "Vessel", "Back", "In transit", "Download", "Map", "Details", "Basic", "Grey", "Leaflet", "CT", "Containers"]:
                        continue
                    if re.match(r'^[A-Z]{4}\d+$', line) or line in ["ATD", "ATA", "ETA", "ETD"]:
                        continue
                        
                    # Location check
                    is_location = False
                    if "," in line and len(line) < 60:
                        parts = line.split(",")
                        if len(parts) == 2 and len(parts[1].strip()) == 2 and parts[1].strip().isupper():
                            is_location = True
                    
                    if is_location:
                        if current_block["items"]:
                            blocks.append(current_block)
                        current_block = {"location": line, "items": []}
                    else:
                        current_block["items"].append(line)
                
                if current_block["items"]:
                    blocks.append(current_block)
                
                # Pair descriptions and dates in each block
                events_list = []
                for b in blocks:
                    loc = b["location"]
                    items = b["items"]
                    
                    # Split items into descriptions and dates, while handling combined lines
                    descriptions = []
                    dates = []
                    
                    for item in items:
                        # Granular date search: "6 Jan 2026 17:14" or "16 Jan 2026"
                        date_match = re.search(r'(\d{1,2}\s+[A-Za-z]{3}\s+\d{4}(?:\s+\d{2}:\d{2})?)', item)
                        if date_match:
                            date_str = date_match.group(1)
                            # Check for text before/after date in same line
                            pre_text = item[:date_match.start()].strip()
                            post_text = item[date_match.end():].strip()
                            event_text = (pre_text + " " + post_text).strip()
                            
                            if event_text:
                                # This is a combined line (e.g., "ATD 19 Jan 2026")
                                # Add it directly
                                events_list.append({"location": loc, "date": date_str, "event": event_text})
                            else:
                                # It's just a date line
                                dates.append(date_str)
                        else:
                            # It's a description line
                            descriptions.append(item)
                    
                    # Now pair the remaining separate blocks
                    # If counts match, pair sequentially
                    if len(descriptions) == len(dates):
                        for d, t in zip(descriptions, dates):
                            events_list.append({"location": loc, "date": t, "event": d})
                    elif len(dates) > 0:
                        # Fallback: pair as many as possible
                        for i in range(min(len(descriptions), len(dates))):
                            events_list.append({"location": loc, "date": dates[i], "event": descriptions[i]})
                        # Append orphaned dates
                        if len(dates) > len(descriptions):
                             for i in range(len(descriptions), len(dates)):
                                 events_list.append({"location": loc, "date": dates[i], "event": "Status Update"})
                
                final_output = {
                    "container_number": container_number,
                    "events": events_list
                }
                
                print(json.dumps(final_output, indent=4))
                sys.stderr.write(f"DEBUG: Successfully extracted {len(events_list)} total events\n")
            else:
                sys.stderr.write("DEBUG: No HTML content extracted!\n")
            
        finally:
            try:
                browser.close()
            except:
                pass


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No container number provided"}))
    else:
        track_container(sys.argv[1])
