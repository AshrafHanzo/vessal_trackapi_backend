import os
from fastapi import FastAPI, Query, HTTPException
import uvicorn
from openai import OpenAI

app = FastAPI(title="Special Import Document Finder")

# OpenAI Configuration
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """You are a customs-compliance assistant expert for India.

Your ONLY task is:

When the user gives an HS code (typically 8, 10, or 11 digits), analyze the core digits (first 4, 6, or 8 digits) to identify India-specific SPECIAL regulatory requirements. 

STRICT RULES:

1. Identify and list ALL applicable special approvals. If an HS code (like in Chapters 72 or 73) requires BOTH SIMS and BIS, you MUST list both.
2. Respond ONLY with the names of the SPECIAL regulatory systems, approvals, registrations, or mandatory certifications.
3. DO NOT mention normal shipping or customs documents such as:
   IEC, Bill of Entry, Invoice, Packing List, Bill of Lading, Insurance, Certificate of Origin, Mill Test Certificate.
4. Output format:
   - Just the special document/system names.
   - One per line if multiple apply.
   - If none apply, output exactly: "No special regulatory document required."
5. Do not add extra commentary, advice, or explanations.
6. Assume India import rules.
7. If the code is 10 or 11 digits, treat it as the 8-digit ITC-HS code it starts with.

Special documents include (but are not limited to):
- SIMS (Steel Import Monitoring System)
- BIS / ISI / QCO (Mandatory Technical Certification)
- PIMS (Paper Import Monitoring System)
- NFMIMS (Non-Ferrous Metal Import Monitoring System)
- WPC / ETA (Wireless)
- TEC / MTCTE (Telecom)
- CDSCO (Medical/Cosmetics)
- FSSAI (Food)
- PQ / Plant Quarantine
- Drug Controller
- Textile Committee / QCO
- Battery/E-Waste/Environment Registration
- Wildlife / CITES / PESO
- Legal Metrology
- Any DGFT import license requirement (Restricted/Prohibited)

Example:
Input: 72104900
Output:
SIMS
BIS (QCO – Steel)
"""

@app.get("/find-special-docs")
async def find_special_docs(hs_code: str = Query(..., description="The HS Code (typically 8, 10, or 11 digits)")):
    try:
        response = client.chat.completions.create(
            model="gpt-4o", # Using gpt-4o as it's the latest flagship model
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": hs_code}
            ],
            temperature=0, # Keep it strictly deterministic
        )
        
        result = response.choices[0].message.content.strip()
        return {"hs_code": hs_code, "special_documents": result}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("Starting Special Import Document Finder API on port 5000...")
    uvicorn.run(app, host="0.0.0.0", port=30016)
