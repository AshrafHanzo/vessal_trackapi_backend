
import sys
import json
import requests
import re

def track_http(container_number):
    try:
        # Step 1: Initialize session
        session = requests.Session()
        
        # Mimic browser headers captured from network logs
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.sealioncargo.com/track.html",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.sealioncargo.com"
        }
        
        # Step 2: Hit the 'check' endpoint to initialize (if needed)
        # seach_url = "https://www.searates.com/tracking-system/check?platform_id=3025"
        # session.get(seach_url, headers=headers)
        
        # Step 3: Get tracking data
        # Note: scac is often needed or automatically detected by the server
        # We try without SCAC first, or try to detect it
        api_url = f"https://www.searates.com/tracking-system/reverse/tracking?route=true&last_successful=false&number={container_number}"
        
        sys.stderr.write(f"DEBUG: Fetching data from {api_url}...\n")
        response = session.get(api_url, headers=headers)
        
        if response.status_code != 200:
            return {"error": f"API error: {response.status_code}", "body": response.text[:200]}
            
        data = response.json()
        
        if data.get("status") == "error":
             # Sometimes it returns error but has partial data or needs SCAC
             return {"error": data.get("message"), "data": data.get("data")}
             
        # Process JSON into the "separate separate" format
        # This part depends on the exact JSON structure returned when 'status' is 'success'
        # Based on my research, the structure usually has 'data' -> 'route' -> 'points' and 'containers'
        
        return data

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No container number provided"}))
    else:
        cn = sys.argv[1]
        result = track_http(cn)
        print(json.dumps(result, indent=4))
