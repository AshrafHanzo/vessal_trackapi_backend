import shared_utils
from datetime import datetime, date, timedelta
import json

def parse_date(date_str):
    if not date_str:
        return None
    try:
        # Standard format from API: %Y-%m-%d %H:%M:%S or %Y-%m-%d
        if ' ' in date_str and '-' in date_str:
            return datetime.strptime(date_str.split(' ')[0], "%Y-%m-%d").date()
        if '-' in date_str:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        
        # Try Portal format: %d %b (e.g. 11 Apr)
        this_year = datetime.now().year
        return datetime.strptime(f"{date_str} {this_year}", "%d %b %Y").date()
    except Exception as e:
        return None

def run():
    print("=" * 60)
    print("ORCHESTRATOR: IGM (ICEGATE)")
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

    stats = {"pushed": 0, "skipped_status": 0, "skipped_date_range": 0, "errors": 0}
    
    today = date.today()
    upper_bound = today + timedelta(days=5)
    print(f"Tracking Window: {today} to {upper_bound}")

    # 3. Process Containers
    for container in containers:
        try:
            container_no = container.get("container_no")
            status = container.get("status", "")
            
            # RULE: Validation — Skip invalid container numbers (like 23467)
            if not shared_utils.is_valid_container(container_no):
                # print(f"  [SKIP] {container_no}: Invalid container format.")
                continue

            # RULE: Eligibility check — Allow any status that might still need IGM/Inward updates
            # (Based on USER requirement: Always find IGM/Inward even for late jobs)
            eligible_statuses = ["ETA", "IGM", "Inward", "Arrived at POD", "Port In", "Port Out", "Customs"]
            if status not in eligible_statuses:
                stats["skipped_status"] += 1
                continue

            # Enrich with job details
            extra_info = job_map.get(container_no, {})
            eta_str = container.get("eta_date") or extra_info.get("eta_date")
            
            # RULE: ETA Date window (Only skip if ETA is known and MORE than 5 days in the future)
            # This ensures we KEEP checking old/overdue containers and MISSING ETA containers.
            eta_date = parse_date(eta_str)
            if eta_date and eta_date > upper_bound:
                print(f"  [SKIP] {container_no}: ETA '{eta_str}' is too far in the future (> 5 days).")
                stats["skipped_date_range"] += 1
                continue

            # Check for MBL/BL - required for ICEGATE
            mbl_no = container.get("master_bl_no") or extra_info.get("master_bl_no")
            bl_no = container.get("bl_no") or extra_info.get("bl_no")
            port = container.get("port_of_discharge") or extra_info.get("port_of_discharge")

            # FALLBACK: If HBL is missing, use MBL as HBL (works for clients without HBL)
            if mbl_no and not bl_no:
                bl_no = mbl_no
                print(f"  [INFO] {container_no}: No HBL found, using MBL '{mbl_no}' as HBL.")

            # FALLBACK: If port is missing, derive from city_of_discharge
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
                if port:
                    print(f"  [INFO] {container_no}: Port missing, derived '{port}' from city '{city}'")

            # Only MBL and BL are strictly required — ICEGATE agent handles missing port
            # with its own Chennai cluster fallback (CHENNAI_CLUSTER_PORTS)
            if not (mbl_no and bl_no):
                print(f"  [SKIP] {container_no}: Missing MBL/BL for ICEGATE (MBL: {mbl_no}, BL: {bl_no}).")
                continue

            # RULE: Last checked check (Delay of 4 hours to avoid spamming trackers)
            # Revert to 'last_updated' which is the correct field in the API
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

            print(f"  [PROCESS] {container_no} (ETA: {eta_str})")
            
            # Build payload
            job_payload = {
                "container_no": container_no,
                "status": status,
                "master_bl_no": mbl_no,
                "bl_no": bl_no,
                "port_of_discharge": port,
                "status_details": container.get("status_details", {}),
                "queued_at": datetime.now().isoformat()
            }
            
            # Push to icegate queue
            if shared_utils.push_to_queue(r, "icegate", job_payload):
                print(f"    -> Pushed to icegate")
                stats["pushed"] += 1
            else:
                print(f"    -> Already in queue (dedup)")

        except Exception as e:
            print(f"  [ERROR] Processing {container.get('container_no')}: {e}")
            stats["errors"] += 1

    # 4. Summary
    print("-" * 60)
    print(f"Summary: {stats}")
    print("ORCHESTRATOR: IGM FINISHED")
    print("=" * 60)

if __name__ == "__main__":
    run()
