#!/usr/bin/env python3
"""
scripts/check_thundering_herd.py — Thundering herd detector

Reads poll:adaptive and poll:fullscan from Redis and shows whether
company poll times are evenly distributed (healthy) or clustered at
the same timestamps (thundering herd).

Usage:
    python scripts/check_thundering_herd.py
    python scripts/check_thundering_herd.py --queue adaptive
    python scripts/check_thundering_herd.py --queue fullscan
    python scripts/check_thundering_herd.py --bucket 30   # 30-minute buckets
"""

import sys
import os
import argparse
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv()

import redis as redis_lib
from config import REDIS_URL

BOLD  = "\033[1m"
RED   = "\033[91m"
YELL  = "\033[93m"
GREEN = "\033[92m"
DIM   = "\033[2m"
RESET = "\033[0m"

BAR_WIDTH = 40   # max bar characters


def analyse(queue_name: str, r, bucket_minutes: int):
    key = f"poll:{queue_name}"
    entries = r.zrange(key, 0, -1, withscores=True)

    if not entries:
        print(f"{DIM}  [{key}] — empty{RESET}")
        return

    now = datetime.now(timezone.utc).timestamp()
    scores = [score for _, score in entries]
    total  = len(scores)

    # ── Bucket by time ────────────────────────────────────────────────────────
    bucket_s    = bucket_minutes * 60
    buckets     = defaultdict(list)
    overdue     = []

    for name, score in entries:
        if score < now:
            overdue.append(name.decode() if isinstance(name, bytes) else name)
        else:
            slot = int((score - now) // bucket_s)
            buckets[slot].append(name.decode() if isinstance(name, bytes) else name)

    max_bucket_count = max((len(v) for v in buckets.values()), default=0)
    if overdue:
        max_bucket_count = max(max_bucket_count, len(overdue))

    # ── Header ────────────────────────────────────────────────────────────────
    print(f"\n{BOLD}  [{key}]  {total} companies{RESET}")
    print(f"  Bucket size: {bucket_minutes} min  |  Now: {datetime.now().strftime('%H:%M:%S')}")
    print()

    # ── Overdue row ───────────────────────────────────────────────────────────
    if overdue:
        bar_len = int(len(overdue) / max_bucket_count * BAR_WIDTH) if max_bucket_count else 0
        color   = RED if len(overdue) > total * 0.2 else YELL
        print(f"  {color}{'OVERDUE':>12}  {len(overdue):>4}  {'█' * bar_len}{RESET}")

    # ── Future buckets ────────────────────────────────────────────────────────
    if buckets:
        for slot in sorted(buckets.keys()):
            count    = len(buckets[slot])
            lo_min   = slot * bucket_minutes
            hi_min   = lo_min + bucket_minutes
            label    = f"+{lo_min:>3}–{hi_min}m"
            bar_len  = int(count / max_bucket_count * BAR_WIDTH) if max_bucket_count else 0
            pct      = count / total * 100

            # Flag as thundering herd if >30% of companies land in one bucket
            if pct > 30:
                color = RED
                flag  = f"  ← {BOLD}THUNDERING HERD{RESET}{RED} ({pct:.0f}% in one bucket){RESET}"
            elif pct > 15:
                color = YELL
                flag  = f"  ← {YELL}spike ({pct:.0f}%){RESET}"
            else:
                color = GREEN
                flag  = ""

            print(f"  {color}{label:>12}  {count:>4}  {'█' * bar_len}{flag}{RESET}")
    else:
        print(f"  {GREEN}  (no future polls scheduled){RESET}")

    # ── Summary ───────────────────────────────────────────────────────────────
    non_empty      = [len(v) for v in buckets.values() if v]
    max_pct        = max(non_empty) / total * 100 if non_empty else 0
    # Ideal = total companies spread evenly across ALL slots in a full 24 h
    # cycle — not just the occupied slots.  Dividing by occupied buckets only
    # would inflate the "ideal" as companies cluster (fewer occupied buckets →
    # higher ideal_per_bk), masking the thundering herd.
    slots_in_cycle = 86400 // (bucket_minutes * 60)
    ideal_per_bk   = total / max(slots_in_cycle, 1)
    evenness_ok    = max_pct < 20

    print()
    if evenness_ok:
        print(f"  {GREEN}✓  Distribution looks healthy — max spike = {max_pct:.1f}% per bucket "
              f"(ideal ≈ {ideal_per_bk:.1f} per {bucket_minutes}-min slot){RESET}")
    else:
        print(f"  {RED}✗  Thundering herd detected — {max_pct:.1f}% of companies in one bucket "
              f"(ideal ≈ {ideal_per_bk:.1f} per {bucket_minutes}-min slot across 24 h){RESET}")

    # Show top-5 most clustered companies (same minute)
    minute_buckets = defaultdict(list)
    for name, score in entries:
        mn = int(score // 60)
        minute_buckets[mn].append(name.decode() if isinstance(name, bytes) else name)

    worst = sorted(minute_buckets.items(), key=lambda x: -len(x[1]))[:3]
    if worst and worst[0][1].__len__() > 3:
        print(f"\n  {BOLD}Top clusters (same minute):{RESET}")
        for ts_min, names in worst:
            t = datetime.fromtimestamp(ts_min * 60).strftime("%H:%M")
            sample = ", ".join(names[:5]) + (f" …+{len(names)-5}" if len(names) > 5 else "")
            print(f"    {t}  ({len(names)} companies)  {DIM}{sample}{RESET}")


def main():
    parser = argparse.ArgumentParser(description="Thundering herd detector for Redis poll queues")
    parser.add_argument("--queue",  choices=["adaptive", "fullscan", "both"], default="both")
    parser.add_argument("--bucket", type=int, default=60,
                        help="Bucket size in minutes (default: 60)")
    args = parser.parse_args()

    r = redis_lib.from_url(REDIS_URL, decode_responses=False)

    print(f"\n{'═'*60}")
    print(f"{BOLD}  Thundering Herd Check — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{'═'*60}")

    queues = ["adaptive", "fullscan"] if args.queue == "both" else [args.queue]
    for q in queues:
        analyse(q, r, args.bucket)

    print(f"\n{'═'*60}\n")
    print(f"{DIM}Tip: use --bucket 30 for finer resolution, --bucket 120 for coarser{RESET}\n")


if __name__ == "__main__":
    main()
