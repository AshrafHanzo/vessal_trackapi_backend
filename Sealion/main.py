"""
Sealion Tracking API
Uses subprocess to avoid thread issues with Playwright
Run with: python main.py
"""

import subprocess
import json
import re
from fastapi import FastAPI, HTTPException, Query
import uvicorn
from typing import Dict, Any, List
import os
import sys
import requests

# OpenAI API Key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

def get_gpt_analysis(data: dict) -> dict:
    """
    Send extracted tracking data to ChatGPT for structured parsing.
    Extracts: Departed Origin, Departed Date, ETA/ATA Status, Status Date, Arrived Location, Arrived Date.
    """
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    
    from datetime import datetime
    current_date = datetime.now().strftime("%d %b %Y")
    
    prompt = f"""
    You are a logistics data analyst. I will provide you with raw container tracking events in JSON format.
    Your task is to analyze the events and extract the following specific details into a JSON object:
    
    Current Reference Date: {current_date}
    
    1. "Departed Origin": The location where the shipment journey BEGAN.
       - Logic: Look for the FIRST location in the chronological history.
       - Keywords: "Empty Container Released", "Gate In", "Received", "Loaded".
       - Ignore intermediate transshipment ports.
    2. "Departed Date": The date AND TIME of the actual departure from that Origin.
       - Logic: Find the "Vessel departure", "Loaded", or "Shipped" event AT the Origin location.
    3. "Current Status": Determine if the shipment has arrived at final destination (ATA) or is still in transit (ETA).
       - CHECK THE LABELS: If the text explicitly says "ETA" or "Estimated", it is ETA. If it says "ATA" or "Actual", it is ATA.
       - CHECK THE DATE: If the latest event date is AFTER {current_date}, it MUST be "ETA" (unless explicitly marked as ATA).
       - Status is "ATA" ONLY if the shipment has "Discharged" or "Arrived" at the final destination AND the date is in the past/today.
       - Otherwise, Status is "ETA".
    4. "Status Date": The date AND TIME of the latest status (the ETA date if in transit, or ATA date if arrived).
    5. "Arrived Location": The final destination port.
       - Logic: This is the LAST location in the planned journey.
    6. "Arrived Date": 
       - STRICT RULE: If Current Status is "ETA", this MUST BE NULL.
       - If Current Status is "ATA", this is the date AND TIME of arrival/discharge at destination.

    CRITICAL RULES:
    - Return ONLY valid JSON. No markdown formatting.
    - If a field cannot be determined, set it to null.
    - "Departed Date" must go with "Departed Origin".
    - TRUST EXPLICIT LABELS ("ETA"/"ATA") over your own assumptions.
    
    Input Data:
    {json.dumps(data, indent=2)}
    """
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that parses logistics data into strict JSON."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.0
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        content = result['choices'][0]['message']['content']
        
        # Clean potential markdown code blocks
        if "```json" in content:
            content = content.replace("```json", "").replace("```", "")
        
        return json.loads(content.strip())
    except Exception as e:
        print(f"GPT Error: {e}")
        return {"error": "Failed to process with GPT", "raw_data": data}


app = FastAPI(title="Sealion Tracking API", version="1.0.0")


# Path to the tracking script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_SCRIPT = os.path.join(SCRIPT_DIR, "tracker.py")

# Detect local venv from current directory
VENV_PYTHON = os.path.join(SCRIPT_DIR, "venv", "Scripts", "python.exe")

if os.path.exists(VENV_PYTHON):
    PYTHON_EXE = VENV_PYTHON
    print(f"Using isolated environment: {PYTHON_EXE}")
else:
    PYTHON_EXE = sys.executable
    print(f"Using current environment: {PYTHON_EXE}")


@app.get("/sealion")
def sealion_track(container_number: str = Query(..., description="Container number to track")):
    """Track a container by its number using Sealion Scraper"""
    if not container_number:
        raise HTTPException(status_code=400, detail="Container number is required")
    
    try:
        # Run the tracker script as a subprocess using venv python
        result = subprocess.run(
            [PYTHON_EXE, TRACKER_SCRIPT, container_number],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=SCRIPT_DIR
        )
        
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Tracker error: {result.stderr}")
        
        # Parse the JSON output
        output = result.stdout.strip()
        
        # Find JSON in output (skip any print statements before it)
        json_start = output.find('{')
        if json_start == -1:
            raise HTTPException(status_code=500, detail="No JSON output from tracker")
        
        json_str = output[json_start:]
        
        # Fix duplicated line in original file
        data = json.loads(json_str)
        
        # Check for known errors from tracker (e.g. Shipping Line Unknown)
        if data.get("error"):
            return data

        # Process with ChatGPT
        analyzed_data = get_gpt_analysis(data)

        # ---------------------------------------------------------
        # Intelligent Vessel Selection
        # ---------------------------------------------------------
        arrived_loc = analyzed_data.get("Arrived Location")
        vessels_list = data.get("vessels", [])
        
        matched_vessel = None
        if arrived_loc and vessels_list:
            # Clean arrived_loc for comparison (e.g., "Ennore, IN" -> "ennore")
            target_city = arrived_loc.lower().split(',')[0].strip()
            
            for v in vessels_list:
                v_discharge = v.get("discharge")
                if v_discharge:
                    v_city = v_discharge.lower().split(',')[0].strip()
                    # Look for a match in either direction
                    if target_city in v_city or v_city in target_city:
                        matched_vessel = v
                        break
        
        if matched_vessel:
            print(f"DEBUG: Matched vessel for {arrived_loc} -> {matched_vessel.get('vessel')}")
            analyzed_data["vessel_details"] = {
                "vessel": matched_vessel.get("vessel"),
                "voyage": matched_vessel.get("voyage"),
                "loading": matched_vessel.get("loading"),
                "discharge": matched_vessel.get("discharge")
            }
        elif "vessel_details" in data:
            # Fallback to the default (last leg) if no destination match found
            analyzed_data["vessel_details"] = data["vessel_details"]
            
        # Merge container type if available
        if "container_type" in data:
            analyzed_data["container_type"] = data["container_type"]
            
        return analyzed_data
        
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Tracking request timed out")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse tracker output: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





if __name__ == "__main__":
    print("=" * 50)
    print("SEALION TRACKING API")
    print("=" * 50)
    print("Server running at: http://localhost:8012")
    print("API docs: http://localhost:8012/docs")
    print("=" * 50)
    import platform
    
    os_name = platform.system()
    
    if os_name == "Windows":
        print(f"Detected Windows OS. Starting Server...")
        uvicorn.run(app, host="0.0.0.0", port=8012)
    else:
        print(f"🐧 Detected Linux/Mac OS. Starting Server...")
        uvicorn.run(app, host="0.0.0.0", port=8012)
