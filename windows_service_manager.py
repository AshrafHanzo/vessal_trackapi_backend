"""
Windows Service Manager
A simple script to start and monitor all Windows-based tracking components.
This ensures the 'Handshake' is always active.
"""

import subprocess
import sys
import os
import time
import signal

# Paths to the components
COMPONENTS = [
    {"name": "Windows Orchestrator", "path": "windows_orchestrator_runner.py"},
    {"name": "Cosco Agent", "path": "cosco/cosco_agent.py"},
    {"name": "Hapag Agent", "path": "hapag/hapag_agent.py"},
    {"name": "RCL Agent",   "path": "rcl/rcl_agent.py"},
    {"name": "HMM Agent",   "path": "vessal_trackapi_hmm/hmm/hmm_agent.py"},
]

# FORCE: Use the local virtual environment for all background processes
PYTHON_EXE = os.path.join(os.getcwd(), "venv", "Scripts", "python.exe")
if not os.path.exists(PYTHON_EXE):
    # Fallback to system python if venv not found (e.g. on dev machines)
    PYTHON_EXE = sys.executable
processes = {}

def start_component(comp):
    name = comp["name"]
    path = comp["path"]
    print(f"[MANAGER] Starting {name}...", flush=True)
    try:
        # Use -u for unbuffered child output
        proc = subprocess.Popen([PYTHON_EXE, "-u", path], cwd=os.getcwd())
        processes[name] = proc
    except Exception as e:
        print(f"[MANAGER] Failed to start {name}: {e}", flush=True)

def monitor():
    print("=" * 60, flush=True)
    print("WINDOWS TRACKING SERVICE MANAGER", flush=True)
    print("=" * 60, flush=True)
    
    # 🚨 EMERGENCY ZOMBIE CLEANUP ON STARTUP 🚨
    print("[MANAGER] Performing startup cleanup of old Chrome zombies...", flush=True)
    os.system("taskkill /F /IM chrome.exe /T >nul 2>&1")
    os.system("taskkill /F /IM chromedriver.exe /T >nul 2>&1")
    time.sleep(2)
    
    # Start all
    for comp in COMPONENTS:
        start_component(comp)
        time.sleep(2)

    print("\n[MANAGER] All components started. Monitoring...", flush=True)
    
    try:
        while True:
            for name, proc in list(processes.items()):
                if proc.poll() is not None:
                    print(f"\n[MANAGER] WARNING: {name} stopped. Restarting...", flush=True)
                    # Find original config
                    comp = next(c for c in COMPONENTS if c["name"] == name)
                    start_component(comp)
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n[MANAGER] Stopping all services...")
        for name, proc in processes.items():
            proc.terminate()
        print("[MANAGER] Goodbye.")

if __name__ == "__main__":
    monitor()
