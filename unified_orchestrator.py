import time
import sys
import traceback
from datetime import datetime, timedelta

# Import all individual state-based orchestrators
try:
    from orchestrator_created import run as run_created
    from orchestrator_eta import run as run_eta
    from orchestrator_igm import run as run_igm
    from orchestrator_inward import run as run_inward
    from orchestrator_port_in import run as run_port_in
    from orchestrator_port_out import run as run_port_out
    from orchestrator_customs import run as run_customs
except ImportError as e:
    print(f"Error importing orchestrators: {e}")
    sys.exit(1)

# Memory Database for Intervals (Exactly mirroring the Postgres Table)
INTERVAL_MINUTES = {
    "Created": 10,
    "ETA": 10,
    "IGM": 30,
    "Inward": 30,
    "Port In": 30,
    "Port Out": 30,
    "Customs": 30
}

LAST_RUN = {
    "Created": None,
    "ETA": None,
    "IGM": None,
    "Inward": None,
    "Port In": None,
    "Port Out": None,
    "Customs": None
}

ORCHESTRATORS = {
    "Created": run_created,
    "ETA": run_eta,
    "IGM": run_igm,
    "Inward": run_inward,
    "Port In": run_port_in,
    "Port Out": run_port_out,
    "Customs": run_customs
}

def should_run(status_name):
    last_run = LAST_RUN[status_name]
    if last_run is None:
        return True
    
    interval_minutes = INTERVAL_MINUTES[status_name]
    elapsed = datetime.now() - last_run
    return elapsed >= timedelta(minutes=interval_minutes)

def main():
    while True:
        print("\n" + "=" * 60)
        print("UNIFIED ORCHESTRATOR MANAGER (API-DRIVEN MEMORY SCHEDULE)")
        print(f"Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        for status_name, run_func in ORCHESTRATORS.items():
            print(f"\n[CHECK] Stage: {status_name}")
            print(f"  -> Rule Interval: {INTERVAL_MINUTES[status_name]} minutes")
            print(f"  -> Last Run: {LAST_RUN[status_name]}")

            if should_run(status_name):
                print(f"  -> [ACTION] Interval reached! Executing orchestrator...")
                try:
                    run_func()
                    LAST_RUN[status_name] = datetime.now()
                    print(f"  -> [SUCCESS] {status_name} finished. Timestamp updated.")
                except Exception as e:
                    print(f"  -> [ERROR] Failed to execute {status_name}: {e}")
                    traceback.print_exc()
            else:
                next_in = (LAST_RUN[status_name] + timedelta(minutes=INTERVAL_MINUTES[status_name])) - datetime.now()
                print(f"  -> [SKIP] Next run in {next_in.total_seconds() / 60:.1f} minutes.")

        print("\n" + "=" * 60)
        print("MANAGER CYCLE FINISHED - Sleeping for 60s")
        print("=" * 60)
        time.sleep(60)

if __name__ == "__main__":
    main()
