
import os
import sys
import json
import time
import subprocess
import requests

# CONFIGURATION
ENNORE_API_URL = "http://localhost:8017/ennore"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_DIR = os.path.join(SCRIPT_DIR, "vessal_trackapi_adaniports_ennore")
SERVICE_SCRIPT = "main.py"
PORT = 8017

def start_service():
    print(f"Starting Adani Ennore Service on port {PORT}...")
    venv_python = os.path.join(SERVICE_DIR, "venv", "Scripts", "python.exe")
    if not os.path.exists(venv_python):
        print(f"Error: Venv not found at {venv_python}")
        return None

    try:
        proc = subprocess.Popen(
            [venv_python, SERVICE_SCRIPT],
            cwd=SERVICE_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        print("Service process launched. Waiting 10s for initialization...")
        time.sleep(10)
        return proc
    except Exception as e:
        print(f"Failed to start service: {e}")
        return None

def stop_service():
    print(f"Stopping service on port {PORT}...")
    try:
        # Find PID using netstat
        result = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
        lines = result.stdout.splitlines()
        pid = None
        for line in lines:
            if f":{PORT}" in line and "LISTENING" in line:
                parts = line.split()
                pid = parts[-1]
                break
        
        if pid:
            os.system(f"taskkill /F /PID {pid} >nul 2>&1")
            print(f"Service (PID {pid}) stopped.")
        else:
            print("No active service found on port.")
    except Exception as e:
        print(f"Error checking/stopping service: {e}")

def check_ennore(container_no):
    print(f"\nChecking Ennore for {container_no}...")
    try:
        response = requests.get(ENNORE_API_URL, params={"container_no": container_no})
        if response.status_code == 200:
            print("Response Received:")
            print(json.dumps(response.json(), indent=4))
        else:
            print(f"API Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Connection Error: {e}")

def main():
    print("=== ADANI ENNORE ORCHESTRATOR TEST ===")
    
    # 1. Cleanup old instances
    stop_service()
    
    # 2. Start Service
    proc = start_service()
    if not proc: return

    try:
        # 3. Test Container
        test_container = "CAAU2633856" # User provided example
        check_ennore(test_container)
        
    finally:
        # 4. Cleanup
        input("\nPress Enter to stop service and exit...")
        stop_service()
        print("Test Complete.")

if __name__ == "__main__":
    main()
