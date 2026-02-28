
import requests

API_BASE_URL = "https://uat.trackcontainer.in/api/external"

def fetch_active_containers():
    print("Fetching active containers...")
    try:
        response = requests.get(f"{API_BASE_URL}/containers/active")
        response.raise_for_status()
        data = response.json()
        
        candidates = []
        if data.get("status") == "success":
            for container in data.get("data", []):
                curr_status = container.get("status")
                if curr_status != "Completed":
                    candidates.append(container.get("container_no"))
        
        print(f"Found {len(candidates)} active containers:")
        for c in candidates:
            print(f" - {c}")
            
    except Exception as e:
        print(f"Error fetching candidates: {e}")

if __name__ == "__main__":
    fetch_active_containers()
