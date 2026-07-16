"""
scripts/purge_stale_detail_payloads.py
---------------------------------------
One-time cleanup: remove stale Workday detail payloads from Redis queues.

Stale payloads were produced by _build_detail_payload() before commit c76d345
(2026-05-26), which only forwarded _external_path for Workday jobs — NOT
_slug, _wd, or _path.  detail_worker drops these with a WARNING every time
a scheduler restart re-queues them from inflight recovery, triggering auto-heal.

What this script removes:
  Any Workday payload where _slug, _wd, or _path is absent/empty.

Queues cleaned:
  • queue:detail:fullscan          (main low-priority queue)
  • queue:detail:adaptive          (main high-priority queue)
  • queue:detail:fullscan:inflight:*   (dead-worker inflight keys only)
  • queue:detail:adaptive:inflight:*   (dead-worker inflight keys only)

Run once on the VM.  Safe to run while the pipeline is live — each queue is
cleaned atomically via a Lua script so no running worker can race between the
read and the write.

Usage:
    python scripts/purge_stale_detail_payloads.py [--dry-run]
"""

import argparse
import json
import sys
from pathlib import Path

# allow importing from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from workers.redis_client import get_redis
from config import (
    REDIS_DETAIL_ADAPTIVE,
    REDIS_DETAIL_FULLSCAN,
)

# Keys that a valid Workday payload must have as non-empty strings.
# Matches _REQUIRED_DETAIL_KEYS["workday"] in detail_worker.py.
_WORKDAY_REQUIRED = {"_slug", "_wd", "_path"}


def _is_stale_workday(raw: str) -> bool:
    """Return True if this JSON entry is a Workday payload with missing keys."""
    try:
        job = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return False
    if job.get("ats_platform") != "workday":
        return False
    return any(not job.get(k) for k in _WORKDAY_REQUIRED)


# Lua script: atomically filter a LIST key.
# Returns the count of removed entries.
# Using KEYS[1] = list key.
_FILTER_LUA = """
local items = redis.call('LRANGE', KEYS[1], 0, -1)
local good = {}
local removed = 0
for _, item in ipairs(items) do
    local ok, parsed = pcall(cjson.decode, item)
    local stale = false
    if ok and type(parsed) == 'table' and parsed['ats_platform'] == 'workday' then
        if not parsed['_slug'] or parsed['_slug'] == ''
        or not parsed['_wd']   or parsed['_wd']   == ''
        or not parsed['_path'] or parsed['_path'] == '' then
            stale = true
        end
    end
    if stale then
        removed = removed + 1
    else
        table.insert(good, item)
    end
end
if removed > 0 then
    redis.call('DEL', KEYS[1])
    for _, item in ipairs(good) do
        redis.call('RPUSH', KEYS[1], item)
    end
end
return removed
"""


def _purge_list(r, key: str, dry_run: bool) -> tuple[int, int]:
    """
    Purge stale Workday entries from a single Redis LIST key.
    Returns (total_items, removed_items).
    """
    raw_items = r.lrange(key, 0, -1)
    total = len(raw_items)
    if total == 0:
        return 0, 0

    stale_count = sum(
        1 for item in raw_items
        if _is_stale_workday(item.decode() if isinstance(item, bytes) else item)
    )

    if stale_count == 0:
        return total, 0

    if dry_run:
        return total, stale_count

    removed = r.eval(_FILTER_LUA, 1, key)
    return total, int(removed)


def _find_dead_inflight_keys(r, queue_key: str) -> list[str]:
    """
    Return inflight keys for dead workers (no live heartbeat).
    Only dead-worker keys are safe to modify — live workers own their inflight lists.
    """
    prefix = f"{queue_key}:inflight:"
    dead_keys = []
    cursor = 0
    while True:
        cursor, raw_keys = r.scan(cursor, match=f"{prefix}*", count=100)
        for raw_key in raw_keys:
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            peer_token = key[len(prefix):]
            # Strip epoch suffix (hostname:pid:epoch → hostname:pid) for HB lookup
            parts = peer_token.split(":")
            hb_token = ":".join(parts[:2]) if len(parts) >= 3 else peer_token
            hb_key = f"worker:alive:detail_worker:{hb_token}"
            if not r.exists(hb_key):
                dead_keys.append(key)
        if cursor == 0:
            break
    return dead_keys


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Count stale entries without removing them")
    args = parser.parse_args()

    dry = args.dry_run
    prefix = "[DRY RUN] " if dry else ""

    r = get_redis()
    print(f"{prefix}Connected to Redis.")

    total_removed = 0

    # ── Main queues ────────────────────────────────────────────────────────────
    for queue_key in (REDIS_DETAIL_FULLSCAN, REDIS_DETAIL_ADAPTIVE):
        total, removed = _purge_list(r, queue_key, dry_run=dry)
        print(f"{prefix}{queue_key}: {total} total, {removed} stale Workday payloads {'would be ' if dry else ''}removed")
        total_removed += removed

    # ── Dead-worker inflight queues ────────────────────────────────────────────
    for queue_key in (REDIS_DETAIL_FULLSCAN, REDIS_DETAIL_ADAPTIVE):
        dead_keys = _find_dead_inflight_keys(r, queue_key)
        if not dead_keys:
            print(f"No dead-worker inflight keys found for {queue_key}")
            continue
        for inflight_key in dead_keys:
            total, removed = _purge_list(r, inflight_key, dry_run=dry)
            print(f"{prefix}{inflight_key}: {total} total, {removed} stale Workday payloads {'would be ' if dry else ''}removed")
            total_removed += removed

    print(f"\n{prefix}Done. Total stale payloads {'to remove' if dry else 'removed'}: {total_removed}")
    if dry and total_removed > 0:
        print("Re-run without --dry-run to apply.")


if __name__ == "__main__":
    main()
