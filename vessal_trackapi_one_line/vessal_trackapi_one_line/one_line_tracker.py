import sys
import json
import time
import re
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import datetime


def fmt_am_pm(date_str):
    """Format a date string to YYYY-MM-DD for API compatibility."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Remove any stray unicode symbols (info icons, etc.)
    date_str = re.sub(r'[^\x00-\x7F]+', '', date_str).strip()

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y %m %d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str  # Return as-is if no format matched


def extract_date_from_text(text):
    """Extract ONLY the date/time substring from text using regex."""
    if not text:
        return None
    # Remove unicode symbols
    text = re.sub(r'[^\x00-\x7F]+', '', text).strip()
    # Match YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM
    match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}(?::\d{2})?)', text)
    if match:
        return match.group(1).strip()
    # Match just YYYY-MM-DD
    match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
    if match:
        return match.group(1).strip()
    return None


def extract_location_from_text(text):
    """Extract clean location from text, removing dates and extra details."""
    if not text:
        return None
    
    # Remove date patterns (YYYY-MM-DD or DD-MM-YYYY) and following text
    text = re.sub(r'\d{4}-\d{2}-\d{2}.*', '', text)
    text = re.sub(r'\d{2}-\d{2}-\d{4}.*', '', text)
    
    # Remove "SIPG" and following text (specific noise for this site)
    if "SIPG" in text:
        text = text.split("SIPG")[0]
        
    return text.strip()


def track_one_line(container_no: str):
    result = {
        "container_no": container_no,
        "status": "pending",
        "data": {
            "departed_value": None,
            "eta_date": None,
            "eta_value": None
        },
        "error": None
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()

            # Navigate directly with URL params (auto-selects Container No. and pre-fills)
            url = (
                f"https://ecomm.one-line.com/one-ecom/manage-shipment/cargo-tracking"
                f"?trakNoParam={container_no}&trakNoTpCdParam=C"
            )
            sys.stderr.write(f"DEBUG: Navigating to {url}...\n")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            sys.stderr.write("DEBUG: DOM loaded, waiting for SPA to render...\n")

            # Close any popups (Customize Columns / promo / tour)
            for _ in range(3):
                try:
                    skip_btn = page.locator("button:has-text('Skip')")
                    if skip_btn.count() > 0:
                        skip_btn.first.click(timeout=2000)
                        sys.stderr.write("DEBUG: Closed a popup\n")
                        time.sleep(0.5)
                    else:
                        break
                except Exception:
                    break

            # Wait for results table data rows to appear
            sys.stderr.write("DEBUG: Waiting for results table...\n")
            page.wait_for_selector("div[role='row']", state="attached", timeout=30000)
            sys.stderr.write("DEBUG: Table rows found!\n")

            # Small wait for rendering to finish
            time.sleep(1)
            
            # Slightly scroll down to reveal timeline (as per user advice)
            sys.stderr.write("DEBUG: Scrolling down slightly...\n")
            page.mouse.wheel(0, 400)
            time.sleep(2) # Give it a moment to render timeline

            # Grab full HTML and parse with BeautifulSoup
            html = page.content()
            soup = BeautifulSoup(html, 'lxml')

            # --- Find the results table (div based) ---
            # Prioritize the main table with id="table-wrap"
            target_table = soup.find('div', id='table-wrap')
            
            if not target_table:
                # Fallback: find any table with role='table'
                tables = soup.find_all('div', role='table')
                if not tables:
                    result["status"] = "not_found"
                    result["error"] = "No results table found on page"
                    return

                # Find the table that has headers and looks like the result table
                for tbl in tables:
                    # Skip the fake header table if identified by id
                    if tbl.get('id') == 'table-wrap-fake':
                        continue
                        
                    header_cells = tbl.find_all('div', role='columnheader')
                    if header_cells:
                        current_headers = [cell.get_text(strip=True).lower() for cell in header_cells]
                        if 'container no' in str(current_headers) or 'latest place' in str(current_headers):
                            target_table = tbl
                            break
            
            if not target_table:
                # If still found nothing, check if we matched the fake table by accident or just failed
                result["status"] = "not_found"
                result["error"] = "No valid results table found"
                return

            # Extract headers from the target table
            headers = []
            header_cells = target_table.find_all('div', role='columnheader')
            if header_cells:
                headers = [cell.get_text(strip=True).lower() for cell in header_cells]


            sys.stderr.write(f"DEBUG: Table headers: {headers}\n")

            # Map column names to indices
            col_map = {}
            for i, h in enumerate(headers):
                if 'latest place' in h:
                    col_map['latest_place'] = i
                elif 'latest event' in h or 'status/time' in h or 'status/ time' in h:
                    col_map['latest_event'] = i
                elif 'pod' in h or 'vessel arrival' in h:
                    col_map['pod_arrival'] = i

            sys.stderr.write(f"DEBUG: Column map: {col_map}\n")

            # --- Extract data from the first result row ---
            # Rows are usually in a div with role='rowgroup'
            rowgroup = target_table.find('div', role='rowgroup')
            rows = []
            if rowgroup:
                rows = rowgroup.find_all('div', role='row')
            else:
                # Fallback: look for rows directly in table if no rowgroup
                rows = target_table.find_all('div', role='row')
                # Filter out header row if it is marked as role=row but inside the header area
                # (Logic depends on structure, usually header is separate or in thead equivalent)
                # For now assuming first row found in rowgroup is data

            if not rows:
                result["status"] = "not_found"
                result["error"] = "No data rows found"
                return

            # Used the first row
            first_row = rows[0]
            cells = first_row.find_all('div', role='cell')
            sys.stderr.write(f"DEBUG: Found {len(cells)} cells in first row\n")

            # Extract raw text from each target column
            latest_place_text = ""
            latest_event_text = ""
            pod_arrival_text = ""

            if 'latest_place' in col_map and col_map['latest_place'] < len(cells):
                cell = cells[col_map['latest_place']]
                # debug HTML shows specific structure: location name + yard item
                # we want all text separated by spaces/newlines
                latest_place_text = cell.get_text(separator=' ', strip=True)

            if 'latest_event' in col_map and col_map['latest_event'] < len(cells):
                cell = cells[col_map['latest_event']]
                latest_event_text = cell.get_text(separator=' ', strip=True)

            if 'pod_arrival' in col_map and col_map['pod_arrival'] < len(cells):
                cell = cells[col_map['pod_arrival']]
                pod_arrival_text = cell.get_text(separator=' ', strip=True)

            sys.stderr.write(f"DEBUG: Latest Place: {latest_place_text}\n")
            sys.stderr.write(f"DEBUG: Latest Event: {latest_event_text}\n")
            sys.stderr.write(f"DEBUG: POD Arrival: {pod_arrival_text}\n")

            # --- Build "Departed" ---
            # Prioritize "Place of Receipt" from the timeline as the true origin
            departed_value = None
            try:
                sys.stderr.write("DEBUG: Attempting to find Place of Receipt in timeline...\n")
                # Wait for the text to appear in the timeline area
                receipt_selector = "text='Place of Receipt'"
                page.wait_for_selector(receipt_selector, state="visible", timeout=10000)
                
                receipt_loc = page.locator(receipt_selector)
                if receipt_loc.count() > 0:
                    # The value is usually a sibling or inside a parent container
                    # We'll try to get the parent's text and extract the value
                    parent = receipt_loc.first.locator("xpath=..")
                    full_text = parent.inner_text()
                    sys.stderr.write(f"DEBUG: Found Receipt Parent Text: {full_text}\n")
                    
                    # Remove "Place", "of", "Receipt" (case insensitive) and grab the remainder
                    # Handle both single line and multiline cases
                    clean_text = re.sub(r'Place|of|Receipt', '', full_text, flags=re.I).strip()
                    # Remove multiple spaces/newlines
                    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                    
                    if clean_text:
                        departed_value = clean_text
                        sys.stderr.write(f"DEBUG: Extracted Receipt: {departed_value}\n")
            except Exception as e:
                sys.stderr.write(f"DEBUG: Place of Receipt timeline extraction failed: {e}\n")

            if not departed_value:
                # Second attempt via BeautifulSoup search in case Playwright failed
                receipt_label = soup.find(string=re.compile(r'Place of Receipt', re.I))
                if receipt_label:
                    potential_container = receipt_label.find_parent(['div', 'p', 'span'])
                    if potential_container:
                        text_content = potential_container.get_text(separator=' ', strip=True)
                        clean_text = re.sub(r'Place of Receipt', '', text_content, flags=re.I).strip()
                        if clean_text:
                            departed_value = clean_text

            if not departed_value:
                departed_value = extract_location_from_text(latest_place_text)

            # --- Build "ETA" (POD/Vessel Arrival) ---
            # POD text: "CHENNAI, INDIA 2026-03-02 08:30"
            eta_value = extract_location_from_text(pod_arrival_text)
            eta_date_raw = extract_date_from_text(pod_arrival_text)
            eta_date = fmt_am_pm(eta_date_raw)

            eta_parts = []
            if eta_value:
                eta_parts.append(eta_value)
            if eta_date:
                eta_parts.append(eta_date)
            eta_combined = " | ".join(eta_parts) if eta_parts else None

            # Populate result
            result["data"]["departed_value"] = departed_value
            result["data"]["eta_date"] = eta_date
            result["data"]["eta_value"] = eta_value
            result["status"] = "success" if (departed_value or eta_value) else "not_found"

        except Exception as e:
            sys.stderr.write(f"ERROR: {e}\n")
            result["status"] = "error"
            result["error"] = str(e)
            try:
                with open("one_line_debug_latest.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                sys.stderr.write("DEBUG: Saved debug HTML to one_line_debug_latest.html\n")
            except Exception:
                pass
        finally:
            browser.close()
            print(json.dumps(result, indent=4))


if __name__ == "__main__":
    track_one_line(sys.argv[1] if len(sys.argv) > 1 else "CAAU3011733")
