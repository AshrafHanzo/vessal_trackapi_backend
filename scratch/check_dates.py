import shared_utils
import json
import requests

def check():
    url = f"{shared_utils.API_BASE_URL}/containers/active"
    response = requests.get(url)
    data = response.json()
    
    if data.get("status") == "success":
        for c in data.get("data", [])[:10]:
            print(f"Container: {c.get('container_no')}")
            print(f"  Status: {c.get('job_status')}")
            print(f"  Last Update (DB): {c.get('updated_at')}")
            print(f"  Last Checked (DB): {c.get('last_check_date')}")
            print(f"  Last Updated At (API-Raw): {c.get('last_updated_at')}")
            print(f"  Last Checked At (API-Raw): {c.get('last_checked_at')}")
            print("-" * 20)

if __name__ == "__main__":
    check()
