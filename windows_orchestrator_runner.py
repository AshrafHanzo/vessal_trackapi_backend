"""
Windows Orchestrator Runner
This script runs on Windows and pulls candidate jobs from the Linux DB/API,
then pushes them to the Centralized Redis.
"""

import shared_utils
from datetime import datetime, timedelta
import json
import time
import sys
import os

def run_windows_orchestration():
    print("=" * 60)
    print("WINDOWS ORCHESTRATOR RUNNER")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Connect to Central Redis
    r = shared_utils.get_redis_client()
    if not r:
        print("Central Redis not available. Exiting.")
        return

    # 2. Fetch Active Containers & Job Details from Linux API
    # (Using the shared_utils which already point to trackcontainer.in)
    containers = shared_utils.fetch_active_containers()
    job_map = shared_utils.fetch_job_details_map()

    stats = {"pushed_windows": 0, "skipped": 0, "errors": 0}
    
    # These are handled by this Windows runner
    WINDOWS_MANAGED_SERVICES = ["hapag", "cosco", "rcl", "hmm"]

    # 3. Process Containers
    for container in containers:
        try:
            container_no = container.get("container_no")
            status = container.get("status", "")

            # RULE: Validation — Skip invalid container numbers (like 23467)
            if not shared_utils.is_valid_container(container_no):
                # print(f"  [SKIP] {container_no}: Invalid container format.")
                continue
            
            # Eligibility check
            if shared_utils.get_rank(status) >= shared_utils.get_rank("Inward"):
                continue

            extra_info = job_map.get(container_no, {})
            shipping_line = extra_info.get("shipping_line", "")
            
            service = shared_utils.get_shipping_line_service(shipping_line)
            if service not in WINDOWS_MANAGED_SERVICES:
                continue

            # MANDATORY RULE: Do not check if checked in the last 4 hours
            last_checked = container.get("last_check_date")
            if last_checked:
                try:
                    # Sync API uses "%Y-%m-%d %H:%M"
                    last_dt = datetime.strptime(last_checked, "%Y-%m-%d %H:%M")
                    # FIX: Handle Windows Server clock drift (AM vs PM discrepancy)
                    diff = (datetime.now() - last_dt).total_seconds()
                    if diff > 0 and diff < 14400: # 14400 seconds = 4 hours
                        continue
                except:
                    pass # Parse error? Allow tracking to be safe

            print(f"  [PROCESS-WINDOWS] {container_no} (Line: {shipping_line})")
            
            job_payload = {
                "container_no": container_no,
                "status": status,
                "shipping_line": shipping_line,
                "queued_at": datetime.now().isoformat()
            }
            
            if shared_utils.push_to_queue(r, service, job_payload):
                print(f"    -> Pushed to {service}")
                stats["pushed_windows"] += 1
            else:
                print(f"    -> Already in queue")

        except Exception as e:
            print(f"  [ERROR] {container.get('container_no')}: {e}")
            stats["errors"] += 1

    print("-" * 60)
    print(f"Summary: {stats}")
    print("WINDOWS ORCHESTRATOR FINISHED")
    print("=" * 60)

if __name__ == "__main__":
    import time
    print("Starting Persistent Windows Orchestrator...")
    while True:
        try:
            run_windows_orchestration()
        except Exception as e:
            print(f"Loop error: {e}")
        
        print("\nSleeping for 10 minutes before next handshake...")
        time.sleep(600) 
