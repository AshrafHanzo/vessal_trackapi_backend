"""Quick test: inspect Redis queue contents."""
import redis
import json

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

print("=" * 50)
print("REDIS QUEUE INSPECTION")
print("=" * 50)

# Queue stats
queue_len = r.llen("tc:queue:ldb")
queued_set = r.scard("tc:queued:ldb")
completed = r.get("tc:completed:ldb") or "0"
failed_len = r.llen("tc:failed:ldb")
processing = r.hlen("tc:processing:ldb")

print(f"  tc:queue:ldb      : {queue_len} jobs pending")
print(f"  tc:queued:ldb     : {queued_set} in dedup set")
print(f"  tc:processing:ldb : {processing} in flight")
print(f"  tc:completed:ldb  : {completed} completed")
print(f"  tc:failed:ldb     : {failed_len} failed")
print()

# Show queue contents
if queue_len > 0:
    print("QUEUE CONTENTS:")
    items = r.lrange("tc:queue:ldb", 0, -1)
    for i, item in enumerate(items):
        data = json.loads(item)
        print(f"  [{i+1}] {data['container_no']} | mode={data.get('mode')} | status={data.get('status')}")
    print()

# Show failed items
if failed_len > 0:
    print("FAILED ITEMS:")
    failed = r.lrange("tc:failed:ldb", 0, -1)
    for item in failed:
        data = json.loads(item)
        print(f"  {data['container_no']} | error={data.get('error', 'N/A')}")
    print()

# Show processing items
if processing > 0:
    print("IN-FLIGHT ITEMS:")
    all_processing = r.hgetall("tc:processing:ldb")
    for k, v in all_processing.items():
        data = json.loads(v)
        print(f"  {k} | worker={data.get('worker')} | started={data.get('started_at')}")

print("=" * 50)
