"""
KMTC Container Tracker Worker
Automates the eKMTC website to fetch container departure and arrival details.
Uses Selenium for browser automation and BeautifulSoup for HTML parsing.
"""

import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def track_container(container_number: str, headless: bool = True) -> dict:
    """
    Track a container using Selenium browser automation on the eKMTC website.
    Uses BeautifulSoup to parse the results table.

    Process:
        1. Open https://www.ekmtc.com/index.html#/main
        2. Click on the Cargo Tracking tab
        3. Enter the container number
        4. Click Search
        5. Parse results with BeautifulSoup

    Args:
        container_number: The container number to track (e.g. 'BEAU2857767')
        headless: Run browser in headless mode (default True)

    Returns:
        dict with keys: container_no, departure, arrival, vessel, bl_no, booking_no
    """
    container_number = container_number.strip().upper()

    if not container_number:
        return {"error": "Container number cannot be empty."}

    # Setup Chrome options
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(10)

        # ---- Step 1: Navigate to the KMTC main page ----
        print("[1/5] Opening eKMTC website...")
        driver.get("https://www.ekmtc.com/index.html#/main")
        time.sleep(5)  # Wait for SPA to fully load

        # ---- Step 2: Click Cargo Tracking tab ----
        print("[2/5] Clicking Cargo Tracking tab...")
        cargo_tab = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "#frm_main > div > div:nth-child(2)")
            )
        )
        cargo_tab.click()
        time.sleep(2)

        # ---- Step 3: Enter container number ----
        print(f"[3/5] Entering container number: {container_number}")
        input_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#cargoKeyword"))
        )
        input_field.clear()
        input_field.send_keys(container_number)
        time.sleep(1)

        # ---- Step 4: Click Search button ----
        print("[4/5] Clicking Search...")
        search_btn = driver.find_element(
            By.CSS_SELECTOR,
            "#frm_main > div > div:nth-child(2) > div > div > a"
        )
        search_btn.click()
        time.sleep(6)  # Wait for results to load

        # ---- Step 5: Parse results with BeautifulSoup ----
        print("[5/5] Parsing results...")
        page_source = driver.page_source
        result = _parse_tracking_results(page_source, container_number)
        return result

    except Exception as e:
        return {"error": f"Automation failed: {str(e)}"}

    finally:
        if driver:
            driver.quit()


def _convert_to_ampm(text: str) -> str:
    """
    Convert 24-hour time in a string to 12-hour AM/PM format.
    E.g. 'SHANGHAI 2026.02.21 00:30' -> 'SHANGHAI 2026.02.21 12:30 AM'
         'CHENNAI 2026.03.08 15:00'  -> 'CHENNAI 2026.03.08 3:00 PM'
    """
    import re
    def replace_time(match):
        h, m = int(match.group(1)), match.group(2)
        period = "AM" if h < 12 else "PM"
        h = h % 12 or 12
        return f"{h}:{m} {period}"
    return re.sub(r'\b(\d{2}):(\d{2})\b', replace_time, text)


def _split_location_date(text: str) -> tuple[str, str]:
    """Splits a string like 'CHENNAI(...) 2026.03.08 12:30 AM' into location and date/time."""
    import re
    # Patterns look for YYYY.MM.DD
    match = re.search(r'\d{4}\.\d{2}\.\d{2}', text)
    if match:
        idx = match.start()
        location_part = text[:idx].strip()
        date_time_part = text[idx:].strip()
        return location_part, date_time_part
    return text.strip(), ""


def _parse_tracking_results(html: str, container_number: str) -> dict:
    """
    Parse the eKMTC cargo tracking page HTML with BeautifulSoup.
    Extracts departure and arrival from the results table.

    Table columns (as seen on the website):
        B/L No. (Status) | Booking No. | Container No. | Size/Type | CGO | Departure | Arrival | VSL / VOY | Reserve/Confirm
        Index:  0            1              2              3          4       5           6         7            8
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find all tables on the page
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 7:
                continue

            # Get text content of all cells
            cell_texts = [cell.get_text(separator=" ", strip=True) for cell in cells]

            # Check if this row contains our container number (column index 2)
            row_container = cell_texts[2] if len(cell_texts) > 2 else ""

            if container_number.upper() in row_container.upper():
                # Found the matching row — extract departure (col 5) and arrival (col 6)
                departure = cell_texts[5] if len(cell_texts) > 5 else "N/A"
                arrival   = cell_texts[6] if len(cell_texts) > 6 else "N/A"

                departure_raw = _convert_to_ampm(departure)
                arrival_raw = _convert_to_ampm(arrival)

                dep_loc, dep_date = _split_location_date(departure_raw)
                arr_loc, arr_date = _split_location_date(arrival_raw)

                return {
                    "container_no": container_number,
                    "departure_value": dep_loc,
                    "departure_date": dep_date,
                    "eta_date": arr_date,
                    "eta_value": arr_loc,
                }

    return {
        "error": (
            "Could not find container data in the results page. "
            "The container number may be invalid, or no results were found."
        ),
        "container_no": container_number,
        "departure": "N/A",
        "arrival": "N/A",
    }


# ---------------------------------------------------------------------------
# Standalone / Agent entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    # Accept container number from command line (used by kmtc_agent.py)
    if len(sys.argv) > 1:
        container = sys.argv[1].strip().upper()
    else:
        container = "BEAU2857767"  # Default test container

    result = track_container(container, headless=True)
    print(json.dumps(result))
