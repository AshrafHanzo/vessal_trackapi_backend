import subprocess
import json
import os

def check(terminal, container):
    script_dir = f"vessal_trackapi_adaniports_{terminal}"
    py_exe = os.path.join(script_dir, "venv", "Scripts", "python.exe")
    tracker = os.path.join(script_dir, "adani_tracker.py")
    
    print(f"\n--- Checking {terminal.upper()} for {container} ---")
    res = subprocess.run([py_exe, tracker, container], capture_output=True, text=True)
    
    output = res.stdout.strip()
    json_start = output.find('{')
    if json_start != -1:
        data = json.loads(output[json_start:])
        print(json.dumps(data, indent=4))
    else:
        print("Raw Output:", output)
        print("Error:", res.stderr)

check("katu", "WHSU2479563")
check("ennore", "AMFU4244478")
