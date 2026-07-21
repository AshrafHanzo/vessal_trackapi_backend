import sys
import os
import re
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from shared_utils import fetch_active_containers, post_event

def heal_corrupted_etas():
    print("Fetching active containers to find corrupted ETA dates...")
    containers = fetch_active_containers()
    healed_count = 0
    
    current_year = str(datetime.now().year)

    for c in containers:
        details = c.get("status_details", {})
        eta_obj = details.get("ETA", {})
        eta_date = eta_obj.get("date", "")
        
        if eta_date:
            # Look for 3 or 4 digit years that are obviously wrong (e.g., 900, 1660, 928, 481, 888)
            # Anything less than 2000 is corrupted
            match = re.search(r'\b(4\d{2}|8\d{2}|9\d{2}|1\d{3})\b', str(eta_date))
            
            if match:
                container_no = c.get("container_no")
                bad_year_or_time = match.group(1)
                
                # Reconstruct the date properly. 
                # e.g., "12 Jul 900" -> "12 Jul 2026"
                # Strip the bad part out
                clean_date = str(eta_date).replace(bad_year_or_time, "").strip()
                
                # Append current year if it's missing
                if current_year not in clean_date:
                    fixed_date = f"{clean_date} {current_year}"
                else:
                    fixed_date = clean_date
                
                # Make sure multiple spaces are collapsed
                fixed_date = re.sub(r'\s+', ' ', fixed_date).strip()
                
                desc = eta_obj.get("description", "Auto-Healed Corrupted Date")
                
                print(f"Healing {container_no}: '{eta_date}' -> '{fixed_date}'")
                
                # Force the API to overwrite the old date
                success = post_event(
                    container_no=container_no, 
                    status="ETA", 
                    date=fixed_date, 
                    value=desc, 
                    is_status_changed=True
                )
                
                if success:
                    healed_count += 1
                else:
                    print(f"  [ERROR] Failed to update {container_no} in API.")

    print(f"\nSuccessfully healed {healed_count} containers.")

if __name__ == "__main__":
    heal_corrupted_etas()
