import sys
import json
import re
import requests
import tempfile
import os
import pdfplumber
from rapidfuzz import fuzz
try:
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "beautifulsoup4"])
    from bs4 import BeautifulSoup

def strip_clean(text):
    return text.strip().replace('\n', ' ')

def scrape_global_psa_chennai(vessel_name):
    print(f"[Worker] Scraping Global PSA Chennai for '{vessel_name}'...", file=sys.stderr)
    try:
        # Step 1: Use requests + BeautifulSoup to find the PDF URL (no Playwright needed)
        print(f"[Worker] Fetching PSA page with BeautifulSoup...", file=sys.stderr)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }
        page_resp = requests.get("https://india.globalpsa.com/vessel-schedule/vessel-schedule-chennai/", headers=headers, verify=False, timeout=30)
        page_resp.raise_for_status()
        
        soup = BeautifulSoup(page_resp.text, 'html.parser')
        
        # Find the embedded PDF URL
        pdf_url = None
        embed_tag = soup.find('embed', attrs={'type': 'application/pdf'})
        if embed_tag and embed_tag.get('src'):
            pdf_url = embed_tag['src']
        
        if not pdf_url:
            # Also check for iframe or object tags
            iframe_tag = soup.find('iframe')
            if iframe_tag and iframe_tag.get('src') and '.pdf' in iframe_tag['src']:
                pdf_url = iframe_tag['src']
        
        if not pdf_url:
            # Fallback: search for any PDF link in the page
            import re as re_mod
            pdf_links = re_mod.findall(r'https?://[^\s"\']+\.pdf', page_resp.text)
            if pdf_links:
                pdf_url = pdf_links[0]
        
        if not pdf_url:
            return {"status": "error", "source": "Global PSA Chennai (Fallback)", "message": "No PDF URL found on PSA page."}
            
        print(f"[Worker] Found PSA PDF URL: {pdf_url}. Downloading...", file=sys.stderr)
        
        # Step 2: Download the PSA PDF
        response = requests.get(pdf_url, verify=False, timeout=30)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(response.content)
            tmp_psa = tmp.name
        
        # Step 3: Extract table data using pdfplumber
        vessels_data = []
        try:
            with pdfplumber.open(tmp_psa) as pdf:
                for page_obj in pdf.pages:
                    tables = page_obj.extract_tables()
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        # Find the header row containing "VESSEL NAME"
                        header_row_idx = -1
                        for ri, row in enumerate(table):
                            if row and any(cell and "VESSEL NAME" in str(cell).upper() for cell in row):
                                header_row_idx = ri
                                break
                        
                        if header_row_idx == -1:
                            continue
                        
                        headers_list = [str(c).strip() if c else "" for c in table[header_row_idx]]
                        
                        # Find column indices
                        vessel_idx = -1
                        eta_idx = -1
                        revised_eta_idx = -1
                        for ci, h in enumerate(headers_list):
                            hu = h.upper()
                            if "VESSEL NAME" in hu:
                                vessel_idx = ci
                            elif "REVISED ETA" in hu:
                                revised_eta_idx = ci
                            elif hu == "ETA":
                                eta_idx = ci
                        
                        if vessel_idx == -1:
                            continue
                        
                        # Extract data rows (skip header + any sub-header)
                        for ri in range(header_row_idx + 1, len(table)):
                            row = table[ri]
                            if not row or len(row) <= vessel_idx:
                                continue
                            v_name = str(row[vessel_idx]).strip() if row[vessel_idx] else ""
                            if not v_name or v_name.upper() == "VESSEL NAME" or v_name.upper() == "CLEARANCE)" or len(v_name) < 2:
                                continue
                            
                            v_eta = str(row[eta_idx]).strip() if eta_idx != -1 and eta_idx < len(row) and row[eta_idx] else None
                            v_revised = str(row[revised_eta_idx]).strip() if revised_eta_idx != -1 and revised_eta_idx < len(row) and row[revised_eta_idx] else None
                            
                            vessels_data.append({
                                "vessel_name": v_name,
                                "eta": v_eta,
                                "revised_eta": v_revised
                            })
        except Exception as e:
            os.remove(tmp_psa)
            return {"status": "error", "source": "Global PSA Chennai (Fallback)", "message": f"Failed to parse PSA PDF: {e}"}
            
        os.remove(tmp_psa)
        
        print(f"[Worker] Found {len(vessels_data)} vessels in PSA table.", file=sys.stderr)
        
        # Step 4: Fuzzy match against VESSEL NAME column only
        best_match = None
        highest_score = 0
        
        for info in vessels_data:
            score = fuzz.ratio(vessel_name.upper(), info["vessel_name"].upper())
            partial_score = fuzz.partial_ratio(vessel_name.upper(), info["vessel_name"].upper())
            final_score = max(score, partial_score)
            
            if final_score > highest_score:
                highest_score = final_score
                best_match = info
                
        if best_match and highest_score >= 70:
            # Prefer REVISED ETA if available
            eta = best_match["eta"]
            revised_eta = best_match["revised_eta"]
            final_eta = revised_eta if revised_eta else eta
            
            if revised_eta:
                print(f"[Worker] ETA: {eta}, REVISED ETA: {revised_eta}. Using REVISED ETA.", file=sys.stderr)
            else:
                print(f"[Worker] ETA: {eta}, no REVISED ETA.", file=sys.stderr)
            
            return {
                "status": "success",
                "source": "Global PSA Chennai (Fallback)",
                "search_vessel_name": vessel_name,
                "matched_vessel_name": best_match["vessel_name"],
                "eta_date": eta,
                "revised_eta_date": revised_eta,
                "final_eta": final_eta,
                "match_score": round(highest_score, 2)
            }
        else:
            print(f"[Worker] Global PSA match failed / score too low ({round(highest_score, 2) if best_match else 0}). Initiating Adani Ports Mundra Fallback...", file=sys.stderr)
            return scrape_adani_ports(vessel_name)
            
    except Exception as e:
        print(f"[Worker] Global PSA Scraper failed: {e}. Initiating Adani Ports Mundra Fallback...", file=sys.stderr)
        return scrape_adani_ports(vessel_name)

def scrape_adani_ports(vessel_name):
    print(f"[Worker] Scraping Adani Ports Mundra for '{vessel_name}'...", file=sys.stderr)
    pdf_url = "https://www.adaniports.com/-/media/Project/Ports/PortsAndTerminals/Mundra-Documents/Berthing-Report/Latest-Berthing-Report-Mundra.pdf"
    
    try:
        response = requests.get(pdf_url, verify=False, timeout=30)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(response.content)
            tmp_adani = tmp.name
            
        vessels_data = []
        try:
            with pdfplumber.open(tmp_adani) as pdf:
                for page_obj in pdf.pages:
                    tables = page_obj.extract_tables()
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                            
                        # Look for the header row that indicates an "EXPECTED" vessels table
                        # The user specified to skip tide/berth tables and look for "Sr No"
                        header_row_idx = -1
                        has_sr_no = False
                        
                        for ri, row in enumerate(table):
                            if not row: continue
                            row_upper = [str(c).upper() for c in row if c]
                            
                            if any("SR NO" in cell for cell in row_upper):
                                has_sr_no = True
                                header_row_idx = ri
                                break
                                
                        if not has_sr_no or header_row_idx == -1:
                            continue
                            
                        headers_list = [str(c).strip().upper() for c in table[header_row_idx] if c]
                        
                        # Find indices for Vessel Name and ATA/ETA Date
                        vessel_idx = -1
                        eta_idx = -1
                        
                        # Since pdfplumber might not align columns perfectly if they are blank,
                        # we iterate over the actual cells in the header row
                        raw_header_row = table[header_row_idx]
                        for ci, h in enumerate(raw_header_row):
                            if not h: continue
                            hu = str(h).upper()
                            if "VESSEL NAME" in hu:
                                vessel_idx = ci
                            elif "ATA" in hu or "ETA" in hu:
                                eta_idx = ci
                                
                        if vessel_idx == -1 or eta_idx == -1:
                            continue
                            
                        for ri in range(header_row_idx + 1, len(table)):
                            row = table[ri]
                            if not row or len(row) <= max(vessel_idx, eta_idx): continue
                            
                            v_name = str(row[vessel_idx]).strip() if row[vessel_idx] else ""
                            eta_date = str(row[eta_idx]).strip() if row[eta_idx] else ""
                            
                            if not v_name or len(v_name) < 3 or v_name.upper() == "VESSEL NAME":
                                continue
                                
                            # the ATA/ETA column is split across two sub-columns visually (Date, Day)
                            # pdfplumber extracts them as adjacent indices.
                            eta_day = ""
                            if eta_idx + 1 < len(row) and row[eta_idx + 1]:
                                eta_day = str(row[eta_idx + 1]).strip()
                                
                            vessels_data.append({
                                "vessel_name": v_name,
                                "eta_date": eta_date,
                                "eta_day": eta_day
                            })
                            
        except Exception as e:
            os.remove(tmp_adani)
            return {"status": "error", "source": "Adani Ports Mundra (Fallback 2)", "message": f"Failed to parse Adani PDF: {e}"}
            
        os.remove(tmp_adani)
        print(f"[Worker] Found {len(vessels_data)} vessels in Adani Ports tables.", file=sys.stderr)
        
        best_match = None
        highest_score = 0
        
        for info in vessels_data:
            score = fuzz.ratio(vessel_name.upper(), info["vessel_name"].upper())
            partial_score = fuzz.partial_ratio(vessel_name.upper(), info["vessel_name"].upper())
            final_score = max(score, partial_score)
            
            if final_score > highest_score:
                highest_score = final_score
                best_match = info
                
        if best_match and highest_score >= 70:
            return {
                "status": "success",
                "source": "Adani Ports Mundra (Fallback 2)",
                "search_vessel_name": vessel_name,
                "matched_vessel_name": best_match["vessel_name"],
                "eta_date": best_match["eta_date"],
                "eta_day": best_match["eta_day"],
                "match_score": round(highest_score, 2)
            }
        else:
            return {
                "status": "not_found",
                "source": "All Sources Failed (Chennai, PSA, Adani)",
                "message": f"No vessel found matching '{vessel_name}' with score >= 70 across all 3 sources.",
                "best_match_found": best_match["vessel_name"] if best_match else None,
                "best_score": round(highest_score, 2) if best_match else 0
            }
            
    except Exception as e:
        return {"status": "error", "source": "Adani Ports Mundra", "message": f"Adani Ports Scraper failed: {e}"}

def run_chennai_tracker(vessel_name):
    # PDF URL for ETA
    pdf_url = "https://www.chennaiport.gov.in/api/static/default/vessel_report/eta.pdf"
    
    # Download the PDF
    print(f"[Worker] Downloading PDF from {pdf_url}...", file=sys.stderr)
    try:
        # Disable SSL verification just in case gov.in has cert issues
        response = requests.get(pdf_url, verify=False, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(json.dumps({"status": "error", "message": f"Failed to download PDF: {e}"}))
        return

    # Parse the PDF
    print(f"[Worker] Extracting text from PDF...", file=sys.stderr)
    vessels_info = []
    current_date = None
    
    # Date regex pattern: dd/mm/yyyy or dd-mm-yyyy or dd.mm.yyyy
    date_pattern = re.compile(r'\b\d{2}[/.-]\d{2}[/.-]\d{4}\b')
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name
        
    try:
        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                
                # Split text by lines
                lines = text.split('\n')
                for line in lines:
                    line = strip_clean(line)
                    if not line:
                        continue
                        
                    # Check if line contains a date
                    date_match = date_pattern.search(line)
                    if date_match:
                        # Sometimes lines might be "02/03/2026" itself, or have data alongside
                        # Based on screenshot, date is usually a standalone line above vessels
                        # Update the running current_date var to the matched date
                        current_date = date_match.group(0)
                        
                        # Process rest of line if it has a vessel name attached?
                        # In the screenshot, the date line ONLY contains the date.
                        # We will skip adding this exact line as a vessel name if it's just a date.
                        cleaned_line = date_pattern.sub('', line).strip()
                        if len(cleaned_line) < 3: 
                            continue

                    # If it's a vessel name (and we have seen a date recently), store it
                    # Exclude header lines
                    if current_date and "Vessel Name" not in line and len(line) > 2:
                        vessels_info.append({
                            "vessel_name_in_pdf": line,
                            "date": current_date
                        })
                        
    except Exception as e:
        print(json.dumps({"status": "error", "message": f"Failed to parse PDF: {e}"}))
        os.remove(tmp_path)
        return

    # Clean up temp file
    os.remove(tmp_path)
    
    print(f"[Worker] Extracted {len(vessels_info)} vessel entries. Searching for '{vessel_name}'...", file=sys.stderr)
    
    # Fuzzy Matching
    best_match = None
    highest_score = 0
    
    for info in vessels_info:
        # Simple ratio
        score = fuzz.ratio(vessel_name.upper(), info["vessel_name_in_pdf"].upper())
        # Partial ratio to handle situations where line contains extra columns (like agent names)
        partial_score = fuzz.partial_ratio(vessel_name.upper(), info["vessel_name_in_pdf"].upper())
        
        # Take the maximum of standard ratio or partial ratio
        final_score = max(score, partial_score)
        
        if final_score > highest_score:
            highest_score = final_score
            best_match = info
    
    # User requested match threshold is 70%
    if best_match and highest_score >= 70:
        result = {
            "status": "success",
            "source": "Chennai Port PDF",
            "search_vessel_name": vessel_name,
            "matched_vessel_name": best_match["vessel_name_in_pdf"],
            "eta_date": best_match["date"],
            "match_score": round(highest_score, 2)
        }
    else:
        print(f"[Worker] Primary PDF match failed / score too low ({round(highest_score, 2) if best_match else 0}). Initiating Global PSA Fallback...", file=sys.stderr)
        result = scrape_global_psa_chennai(vessel_name)
        
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "Missing vessel name argument"}))
        sys.exit(1)
        
    vessel_name_arg = sys.argv[1]
    
    # Disable requests warnings for unverified HTTPS just in case
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    run_chennai_tracker(vessel_name_arg)
