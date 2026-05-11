import shared_utils
from datetime import datetime, timedelta
import json
import os

CFS_PROVIDER_MAP_FILE = "cfs_provider_map.json"

def load_provider_map():
    if os.path.exists(CFS_PROVIDER_MAP_FILE):
        try:
            with open(CFS_PROVIDER_MAP_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def run():
    print("=" * 60)
    print("ORCHESTRATOR: PORT OUT")
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
    pmap = load_provider_map()

    stats = {"pushed": 0, "skipped_status": 0, "errors": 0}
    
    # 3. Process Containers
    for container in containers:
        try:
            container_no = container.get("container_no")
            status = container.get("status", "")
            
            # RULE: Eligibility check for 'Port In' status
            if status != "Port In":
                stats["skipped_status"] += 1
                continue

            # RULE: Validation — Skip invalid container numbers (like 23467)
            if not shared_utils.is_valid_container(container_no):
                # print(f"  [SKIP] {container_no}: Invalid container format.")
                continue

            # RULE: Last checked check (Delay of 4 hours to avoid spamming trackers)
            last_checked = container.get("last_updated")
            if last_checked:
                try:
                    # Parse ISO format from API
                    last_dt = datetime.fromisoformat(last_checked.replace('Z', '+00:00'))
                    diff = datetime.now(last_dt.tzinfo) - last_dt
                    # SKIP only if checked between 0 and 4 hours ago
                    # If diff is negative (Future date bug), we DO NOT skip, so we can fix it.
                    if timedelta(0) <= diff < timedelta(hours=4):
                        # stats["skipped_up_to_date"] += 1
                        continue
                except Exception as parse_err:
                    pass

            print(f"  [PROCESS] {container_no} (Status: {status})")
            
            # Build payload
            job_payload = {
                "container_no": container_no,
                "status": status,
                "port_of_discharge": container.get("port_of_discharge") or job_map.get(container_no, {}).get("port_of_discharge"),
                "status_details": container.get("status_details", {}),
                "queued_at": datetime.now().isoformat()
            }
            
            # Action: Push to CFS discovery queues or sticky provider
            # Same logic as Port In - we need to monitor for the "Port Out" event.
            sticky = pmap.get(container_no)
            pushed_any = False

            if sticky:
                provider_map = {
                    "DPW": "dpw", "CITPL": "cfs",
                    "Adani_Katu": "adani_katu", "Adani_Ennore": "adani_ennore"
                }
                queue_name = provider_map.get(sticky)
                if queue_name and shared_utils.push_to_queue(r, queue_name, job_payload):
                    print(f"    -> Pushed to sticky: {sticky}")
                    pushed_any = True
            else:
                for cfs_queue in ["dpw", "cfs", "adani_katu", "adani_ennore"]:
                    if shared_utils.push_to_queue(r, cfs_queue, job_payload):
                        print(f"    -> Pushed to discovery: {cfs_queue}")
                        pushed_any = True
            
            if pushed_any:
                stats["pushed"] += 1
            else:
                print(f"    -> Already in queue (dedup)")

        except Exception as e:
            print(f"  [ERROR] Processing {container.get('container_no')}: {e}")
            stats["errors"] += 1

    # 4. Summary
    print("-" * 60)
    print(f"Summary: {stats}")
    print("ORCHESTRATOR: PORT OUT FINISHED")
    print("=" * 60)

if __name__ == "__main__":
    run()
