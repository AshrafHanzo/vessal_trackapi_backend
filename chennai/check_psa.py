import requests, pdfplumber, urllib3
urllib3.disable_warnings()

# Download the primary Chennai port PDF
r = requests.get("https://www.chennaiport.gov.in/api/static/default/vessel_report/eta.pdf", verify=False, timeout=30)
open("primary.pdf", "wb").write(r.content)

with pdfplumber.open("primary.pdf") as pdf:
    for i, page in enumerate(pdf.pages):
        text = page.extract_text() or ""
        # Search for TCI in any form
        for line in text.split("\n"):
            if "TCI" in line.upper():
                print(f"Page {i+1} TEXT: {line}")
        
        # Also check tables
        tables = page.extract_tables()
        for ti, table in enumerate(tables):
            for ri, row in enumerate(table):
                if row:
                    row_text = " ".join([str(c) for c in row if c])
                    if "TCI" in row_text.upper():
                        print(f"Page {i+1} TABLE {ti} ROW {ri}: {row}")

print("\n--- All vessel names from text (first 60) ---")
with pdfplumber.open("primary.pdf") as pdf:
    count = 0
    for page in pdf.pages:
        text = page.extract_text() or ""
        for line in text.split("\n"):
            line = line.strip()
            if len(line) > 3 and "Vessel" not in line:
                print(f"  {line[:80]}")
                count += 1
                if count > 60:
                    break
