"""
Container Status Report — Shows which data is filled vs missing for all active containers.

Run: python check_container_status.py
"""

import requests
import sys
import os

# Force UTF-8 output on Windows
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

API_BASE_URL = "https://trackcontainer.in/api/external"

# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

def check(val):
    """Return Y or - based on whether value exists."""
    if val and str(val).strip() not in ["", "null", "None", "N/A", "n.a."]:
        return f"{GREEN}Y{RESET}"
    return f"{RED}-{RESET}"

def main():
    print(f"\n{BOLD}{'='*90}{RESET}")
    print(f"{BOLD}  CONTAINER STATUS REPORT{RESET}")
    print(f"{BOLD}{'='*90}{RESET}\n")

    # Fetch active containers with status details
    try:
        resp = requests.get(f"{API_BASE_URL}/containers/active", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        containers = data.get("data", [])
    except Exception as e:
        print(f"{RED}Error fetching containers: {e}{RESET}")
        sys.exit(1)

    if not containers:
        print(f"{YELLOW}No active containers found.{RESET}")
        return

    # Track stats
    total = len(containers)
    stats = {
        "Departed": 0, "ETA": 0, "Arrived at POD": 0,
        "IGM": 0, "Inward": 0,
        "Port In": 0, "Port Out": 0,
        "CFS In": 0, "CFS Out": 0
    }

    events_to_check = ["Departed", "ETA", "Arrived at POD", "IGM", "Inward", "Port In", "Port Out", "CFS In", "CFS Out"]

    # Print header
    print(f"  {'Container':<16} {'Status':<15} {'Departed':^8} {'ETA':^5} {'Arrived':^8} {'IGM':^5} {'Inward':^7} {'PortIn':^7} {'PortOut':^8} {'CFSCode':^8} {'CFSName':^8}")
    print(f"  {'-'*16} {'-'*15} {'-'*8} {'-'*5} {'-'*8} {'-'*5} {'-'*7} {'-'*7} {'-'*8} {'-'*8} {'-'*8}")

    for c in containers:
        container_no = c.get("container_no", "?")
        status = c.get("status", "?")
        sd = c.get("status_details", {})

        def has_event(name):
            evt = sd.get(name, {})
            return bool(evt and evt.get("date"))

        row_checks = []
        for evt_name in events_to_check:
            filled = has_event(evt_name)
            if filled:
                stats[evt_name] += 1
            row_checks.append(check(filled if filled else None))

        # Truncate status for display
        status_display = status[:14] if status else "?"

        print(f"  {container_no:<16} {status_display:<15} {'  '.join(row_checks)}")

    # Summary
    print(f"\n{BOLD}{'='*90}{RESET}")
    print(f"{BOLD}  SUMMARY ({total} containers){RESET}")
    print(f"{BOLD}{'='*90}{RESET}")

    for evt_name in events_to_check:
        filled = stats[evt_name]
        missing = total - filled
        pct = (filled / total * 100) if total > 0 else 0
        bar_len = int(pct / 2)
        bar = f"{'█' * bar_len}{'░' * (50 - bar_len)}"

        label_map = {"CFS In": "CFS Code", "CFS Out": "CFS Name"}
        display_name = label_map.get(evt_name, evt_name)

        color = GREEN if pct >= 80 else YELLOW if pct >= 40 else RED
        print(f"  {display_name:<16} {color}{bar} {filled:>3}/{total} ({pct:.0f}%){RESET}")

    print()

if __name__ == "__main__":
    main()
