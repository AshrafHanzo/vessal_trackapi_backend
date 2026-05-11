import shared_utils
from datetime import datetime, timedelta
import json
import time

def run():
    print("=" * 60)
    print("ORCHESTRATOR: CREATED")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Connect to Redis
    r = shared_utils.get_redis_client()
    if not r:
        print("Redis not available. Exiting.")
        return

    # 2. Fetch Active Containers & Job Details
    containers = shared_utils.fetch_active_containers()
    job_map = shared_utils.fetch_job_details_map()

    stats = {"pushed": 0, "skipped_status": 0, "skipped_delay": 0, "errors": 0}
    
    # 3. Process Containers
    for container in containers:
        try:
            container_no = container.get("container_no")
            status = container.get("status", "Created")
            
            # Enrich with job details
            extra_info = job_map.get(container_no, {})
            shipping_line = extra_info.get("shipping_line", "")

            # RULE: Eligibility check for 'Created' status 
            # EXCEPTION: Allow KMTC and Wan Hai to re-run even if they moved to ETA, to force status reconciliation
            is_kmtc_or_wanhai = any(x in (shipping_line or "").upper() for x in ["KMTC", "WAN HAI", "WANHAI"])
            if status != "Created" and not is_kmtc_or_wanhai:
                stats["skipped_status"] += 1
                continue
            
            # RULE: Validation — Skip invalid container numbers (like 23467)
            if not shared_utils.is_valid_container(container_no):
                continue
            
            # RULE: Last checked check (Delay of 4 hours)
            # Revert to 'last_updated' which is the correct field in the API
            last_checked = container.get("last_updated")
            if last_checked:
                try:
                    # Parse ISO format from API
                    last_dt = datetime.fromisoformat(last_checked.replace('Z', '+00:00'))
                    diff = datetime.now(last_dt.tzinfo) - last_dt
                    # SKIP only if checked between 0 and 4 hours ago
                    # If diff is negative (Future date), we DO NOT skip, so we can fix it.
                    if timedelta(0) <= diff < timedelta(hours=4):
                        print(f"  [SKIP] {container_no}: Last checked less than 4 hours ago.")
                        stats["skipped_delay"] += 1
                        continue
                except Exception as parse_err:
                    pass

            # BIFURCATE: Split workload between Linux and Windows
            IS_WINDOWS = shared_utils.IS_WINDOWS
            WINDOWS_MANAGED = shared_utils.WINDOWS_MANAGED_SERVICES
            service = shared_utils.get_shipping_line_service(shipping_line)
            
            is_windows_line = service in WINDOWS_MANAGED
            
            if IS_WINDOWS:
                if not is_windows_line:
                    continue
            else:
                if is_windows_line:
                    continue

            print(f"  [PROCESS-{'WINDOWS' if IS_WINDOWS else 'LINUX'}] {container_no} (Line: {shipping_line})")
            
            # Build payload
            job_payload = {
                "container_no": container_no,
                "status": status,
                "shipping_line": shipping_line,
                "status_details": container.get("status_details", {}),
                "queued_at": datetime.now().isoformat()
            }
            
            # Determine Service
            service = shared_utils.get_shipping_line_service(shipping_line)
            
            if not service:
                print(f"    -> Skipping: No active tracker for '{shipping_line}'")
                continue
                
            # Push to queue
            if shared_utils.push_to_queue(r, service, job_payload):
                print(f"    -> Pushed to {service}")
                stats["pushed"] += 1
            else:
                print(f"    -> Already in queue (dedup)")

        except Exception as e:
            print(f"  [ERROR] Processing {container.get('container_no')}: {e}")
            stats["errors"] += 1

    # 4. Summary
    print("-" * 60)
    print(f"Summary: {stats}")
    print("ORCHESTRATOR: CREATED FINISHED")
    print("=" * 60)

if __name__ == "__main__":
    run()
