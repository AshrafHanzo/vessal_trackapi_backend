"""
LDB Orchestrator — Redis Queue Publisher
Fetches active containers from Portal API, filters eligible ones,
and pushes jobs to Redis queue tc:queue:ldb for LDB agents to process.

Run: python orchestrator_ldb.py
Scheduled via PM2 cron or systemd timer every 10 minutes.
"""

import redis
import requests
import json
import os
import sys
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
API_BASE_URL = os.environ.get("API_BASE_URL", "https://trackcontainer.in/api/external")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Redis Keys
QUEUE_KEY = "tc:queue:ldb"
QUEUED_SET_KEY = "tc:queued:ldb"  # SET to track what's already in the queue

# Status Hierarchy (must match main_orchestrator.py)
STATUS_RANK = {
    "Created": 0,
    "Empty Return": 0,
    "Departed": 1,
    "ETA": 2,
    "IGM": 3,
    "Inward": 4,
    "Arrived at POD": 5,
    "Port In": 6,
    "Port Out": 7,
    "CFS In": 8,
    "CFS Out": 9,
    "Completed": 10
}

def get_rank(status):
    return STATUS_RANK.get(status, -1)


def fetch_active_containers():
    """Fetch active containers from Portal API."""
    print(f"[{datetime.now()}] Fetching active containers from {API_BASE_URL}...")
    try:
        response = requests.get(f"{API_BASE_URL}/containers/active", timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "success":
            containers = data.get("data", [])
            print(f"  -> Retrieved {len(containers)} total containers.")
            return containers
        else:
            print(f"  -> API returned non-success status: {data.get('status')}")
            return []
    except Exception as e:
        print(f"  -> Error: {e}")
        return []


def push_ldb_jobs(containers):
    """
    Filter containers eligible for LDB tracking and push to Redis queue.
    
    Eligibility:
      - Status >= 'Arrived at POD' (rank 5)
      - Status < 'Port Out' (rank 7) — means Port In/Out not yet tracked
      
    For re-checking Port Out containers that might need CFS data,
    we also push containers with status == 'Port Out' in 'cfs' mode.
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()  # Verify connection
    except redis.ConnectionError as e:
        print(f"[ERROR] Cannot connect to Redis at {REDIS_HOST}:{REDIS_PORT} — {e}")
        sys.exit(1)

    pushed = 0
    skipped_rank = 0
    skipped_duplicate = 0

    for c in containers:
        container_no = c.get("container_no", "").strip()
        if not container_no:
            continue

        status = c.get("status", "")
        rank = get_rank(status)

        # ---- PORT MODE: Arrived at POD but not yet Port Out ----
        if rank >= get_rank("Arrived at POD") and rank < get_rank("Port Out"):
            # Check if already in queue (idempotency)
            queue_key = f"{container_no}:port"
            if r.sismember(QUEUED_SET_KEY, queue_key):
                skipped_duplicate += 1
                continue

            job = {
                "container_no": container_no,
                "container_id": c.get("id"),
                "source": "ldb",
                "mode": "port",
                "status": status,
                "queued_at": datetime.now().isoformat()
            }
            r.lpush(QUEUE_KEY, json.dumps(job))
            r.sadd(QUEUED_SET_KEY, queue_key)
            pushed += 1

        else:
            skipped_rank += 1

    # Auto-expire the queued set after 30 minutes (next cycle will re-evaluate)
    r.expire(QUEUED_SET_KEY, 1800)

    print(f"\n{'='*50}")
    print(f"LDB ORCHESTRATOR SUMMARY")
    print(f"{'='*50}")
    print(f"  Total containers    : {len(containers)}")
    print(f"  Pushed to queue     : {pushed}")
    print(f"  Skipped (wrong rank): {skipped_rank}")
    print(f"  Skipped (duplicate) : {skipped_duplicate}")
    print(f"  Queue length now    : {r.llen(QUEUE_KEY)}")
    print(f"{'='*50}")


def main():
    print(f"\n{'='*50}")
    print(f"LDB ORCHESTRATOR STARTED")
    print(f"Time: {datetime.now()}")
    print(f"{'='*50}")

    containers = fetch_active_containers()
    if not containers:
        print("No active containers found. Exiting.")
        return

    push_ldb_jobs(containers)

    print(f"\nLDB ORCHESTRATOR FINISHED at {datetime.now()}")


if __name__ == "__main__":
    main()
