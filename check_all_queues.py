"""
Queue Monitor — Check status of all Redis queues for Track Container

Usage:
    python check_all_queues.py              # Quick overview
    python check_all_queues.py --detail     # Show detailed per-service breakdown
    python check_all_queues.py --watch      # Live dashboard (refreshes every 5s)
    python check_all_queues.py --flush      # Clear all queues (with confirmation)

Run from project root:  python check_all_queues.py
"""

import redis
import json
import sys
import os
import time
from datetime import datetime

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# All services and their Redis key prefixes
SERVICES = [
    {"name": "Sealion",         "key": "sealion",       "folder": "Sealion"},
    {"name": "Icegate",         "key": "icegate",       "folder": "icegate"},
    {"name": "LDB Port",       "key": "ldb",           "folder": "vessal_trackapi_Port"},
    {"name": "CFS CITPL",      "key": "cfs",           "folder": "vessal_trackapi_cfs"},
    {"name": "CFS DPWorld",    "key": "dpw",           "folder": "vessal_trackapi_csf_dpworld"},
    {"name": "Adani Kattupalli", "key": "adani_katu",  "folder": "vessal_trackapi_adaniports_katu"},
    {"name": "Adani Ennore",   "key": "adani_ennore",  "folder": "vessal_trackapi_adaniports_ennore"},
]


def get_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        return r
    except redis.ConnectionError as e:
        print(f"[ERROR] Cannot connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        print(f"        {e}")
        sys.exit(1)


def get_service_stats(r, svc):
    key = svc["key"]
    return {
        "queue":      r.llen(f"tc:queue:{key}"),
        "processing": r.hlen(f"tc:processing:{key}"),
        "completed":  int(r.get(f"tc:completed:{key}") or 0),
        "failed":     r.llen(f"tc:failed:{key}"),
        "workers":    r.hlen(f"tc:workers:{key}"),
        "retrying":   r.hlen(f"tc:retries:{key}"),
    }


def print_overview(r):
    print(f"\n{'='*80}")
    print(f"  TRACK CONTAINER — QUEUE STATUS       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"  {'Service':<20} {'Queue':>6} {'Active':>7} {'Done':>7} {'Failed':>7} {'Workers':>8} {'Retry':>6}")
    print(f"  {'-'*20} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*8} {'-'*6}")

    totals = {"queue": 0, "processing": 0, "completed": 0, "failed": 0, "workers": 0, "retrying": 0}

    for svc in SERVICES:
        stats = get_service_stats(r, svc)
        for k in totals:
            totals[k] += stats[k]

        # Color indicators
        q_indicator = "🔴" if stats["queue"] > 10 else ("🟡" if stats["queue"] > 0 else "🟢")

        print(f"  {q_indicator} {svc['name']:<18} {stats['queue']:>5} {stats['processing']:>7} "
              f"{stats['completed']:>7} {stats['failed']:>7} {stats['workers']:>8} {stats['retrying']:>6}")

    print(f"  {'-'*20} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*8} {'-'*6}")
    print(f"  {'TOTAL':<20} {totals['queue']:>6} {totals['processing']:>7} "
          f"{totals['completed']:>7} {totals['failed']:>7} {totals['workers']:>8} {totals['retrying']:>6}")
    print(f"{'='*80}")


def print_detail(r):
    print_overview(r)

    for svc in SERVICES:
        key = svc["key"]
        stats = get_service_stats(r, svc)

        if stats["queue"] == 0 and stats["processing"] == 0 and stats["workers"] == 0:
            continue

        print(f"\n  --- {svc['name']} (tc:*:{key}) ---")

        # Show workers
        workers = r.hgetall(f"tc:workers:{key}")
        if workers:
            print(f"  Workers ({len(workers)}):")
            for wid, wdata in workers.items():
                try:
                    w = json.loads(wdata)
                    status = w.get("status", "unknown")
                    container = w.get("container", "")
                    since = w.get("since", w.get("started_at", ""))
                    print(f"    {wid}: {status} {f'→ {container}' if container else ''} (since {since})")
                except:
                    print(f"    {wid}: {wdata}")

        # Show processing
        processing = r.hgetall(f"tc:processing:{key}")
        if processing:
            print(f"  Processing ({len(processing)}):")
            for cno, pdata in list(processing.items())[:5]:
                try:
                    p = json.loads(pdata)
                    print(f"    {cno} → {p.get('worker', '?')} (started: {p.get('started_at', '?')})")
                except:
                    print(f"    {cno}")
            if len(processing) > 5:
                print(f"    ... and {len(processing) - 5} more")

        # Show failed (last 3)
        if stats["failed"] > 0:
            print(f"  Failed ({stats['failed']}):")
            for i in range(min(3, stats["failed"])):
                raw = r.lindex(f"tc:failed:{key}", i)
                if raw:
                    try:
                        f = json.loads(raw)
                        print(f"    {f.get('container_no', '?')}: {f.get('error', '?')[:80]} "
                              f"(attempts: {f.get('total_attempts', '?')})")
                    except:
                        print(f"    {raw[:80]}")
            if stats["failed"] > 3:
                print(f"    ... and {stats['failed'] - 3} more")

        # Show queue peek (first 3)
        if stats["queue"] > 0:
            print(f"  Queue ({stats['queue']} pending):")
            for i in range(min(3, stats["queue"])):
                raw = r.lindex(f"tc:queue:{key}", -(i+1))
                if raw:
                    try:
                        j = json.loads(raw)
                        print(f"    {j.get('container_no', '?')} [{j.get('status', '?')}]")
                    except:
                        print(f"    {raw[:60]}")
            if stats["queue"] > 3:
                print(f"    ... and {stats['queue'] - 3} more")


def watch_mode(r):
    print("Starting live dashboard (Ctrl+C to stop)...\n")
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            print_overview(r)
            print(f"\n  Refreshing every 5s... Press Ctrl+C to stop")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n  Dashboard stopped.")


def flush_queues(r):
    print("\n⚠️  This will CLEAR all queues, processing, failed, retry, and completed counters!")
    confirm = input("  Type 'YES' to confirm: ").strip()
    if confirm != "YES":
        print("  Aborted.")
        return

    for svc in SERVICES:
        key = svc["key"]
        r.delete(f"tc:queue:{key}")
        r.delete(f"tc:processing:{key}")
        r.delete(f"tc:failed:{key}")
        r.delete(f"tc:completed:{key}")
        r.delete(f"tc:retries:{key}")
        r.delete(f"tc:workers:{key}")
        r.delete(f"tc:queued:{key}")
        print(f"  Cleared: {svc['name']}")

    print("\n  All queues flushed!")


def main():
    r = get_redis()

    if "--watch" in sys.argv:
        watch_mode(r)
    elif "--flush" in sys.argv:
        flush_queues(r)
    elif "--detail" in sys.argv:
        print_detail(r)
    else:
        print_overview(r)

    # Also show folder structure
    if "--detail" not in sys.argv and "--watch" not in sys.argv and "--flush" not in sys.argv:
        print(f"\n  📁 Agent Locations:")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for svc in SERVICES:
            agent_path = os.path.join(script_dir, svc["folder"])
            agents = [f for f in os.listdir(agent_path) if f.endswith("_agent.py")] if os.path.isdir(agent_path) else []
            status = "✅" if agents else "❌"
            agent_name = agents[0] if agents else "missing"
            print(f"  {status} {svc['folder']}/{agent_name}")


if __name__ == "__main__":
    main()
