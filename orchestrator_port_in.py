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
    print("ORCHESTRATOR: PORT IN")
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
            
            # RULE: Eligibility check for 'Inward' status ONLY
            # (Based on USER requirement: port agents only run AFTER inward is complete)
            if status != "Inward":
                stats["skipped_status"] += 1
                continue

            # RULE: Validation — Skip invalid container numbers (like 23467)
            if not shared_utils.is_valid_container(container_no):
                # print(f"  [SKIP] {container_no}: Invalid container format.")
                continue

            # GATE CHECK: If status is "Arrived at POD" but IGM/Inward are NOT done,
            # redirect to ICEGATE queue instead of Port queues.
            # This prevents containers from skipping the IGM/Inward stage.
            status_details = container.get("status_details", {})
            igm_done = bool(status_details.get("IGM", {}).get("date"))
            inward_done = bool(status_details.get("Inward", {}).get("date"))

            if status == "Arrived at POD" and not igm_done:
                # IGM not done yet — try to push to ICEGATE queue first
                extra_info = job_map.get(container_no, {})
                mbl_no = container.get("master_bl_no") or extra_info.get("master_bl_no")
                bl_no = container.get("bl_no") or extra_info.get("bl_no")
                port = container.get("port_of_discharge") or extra_info.get("port_of_discharge")
                
                # FALLBACK: Use MBL as BL if HBL missing
                if mbl_no and not bl_no:
                    bl_no = mbl_no

                # FALLBACK: Derive port from city if missing
                if not port:
                    city = (container.get("city_of_discharge") or extra_info.get("city_of_discharge") or "").strip().lower()
                    CITY_TO_PORTS = {
                        "chennai": "INMAA,INENR,INKAT",
                        "ennore": "INENR,INMAA,INKAT",
                        "kattupalli": "INKAT,INMAA,INENR",
                        "kochi": "INCOK",
                        "cochin": "INCOK",
                    }
                    port = CITY_TO_PORTS.get(city, "")

                if mbl_no and bl_no:
                    icegate_payload = {
                        "container_no": container_no,
                        "status": status,
                        "master_bl_no": mbl_no,
                        "bl_no": bl_no,
                        "port_of_discharge": port or "",
                        "status_details": status_details,
                        "queued_at": datetime.now().isoformat()
                    }
                    if shared_utils.push_to_queue(r, "icegate", icegate_payload):
                        print(f"  [REDIRECT] {container_no}: Arrived at POD but IGM not done → pushed to icegate queue")
                    else:
                        print(f"  [REDIRECT] {container_no}: Arrived at POD but IGM not done → already in icegate queue")
                else:
                    print(f"  [SKIP] {container_no}: Arrived at POD, IGM not done, but no MBL/BL available")
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
    print("ORCHESTRATOR: PORT IN FINISHED")
    print("=" * 60)

if __name__ == "__main__":
    run()
