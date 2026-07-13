# Adaptive Job Monitoring Architecture

## Table of Contents

1. [The Problem We Are Solving](#1-the-problem-we-are-solving)
2. [Current State vs Future State](#2-current-state-vs-future-state)
3. [Core Concept: Continuous Polling vs Daily Batch](#3-core-concept-continuous-polling-vs-daily-batch)
4. [System Components Overview](#4-system-components-overview)
5. [The Priority Queue Scheduler](#5-the-priority-queue-scheduler)
6. [Adaptive Interval Engine](#6-adaptive-interval-engine)
7. [Tier 1 and Tier 2 — What They Mean](#7-tier-1-and-tier-2--what-they-mean)
8. [Adaptive MAX_INTERVAL Cap](#8-adaptive-maxinterval-cap)
9. [Full Scan Architecture](#9-full-scan-architecture)
10. [Dormant Company Reactivation](#10-dormant-company-reactivation)
11. [Smart Early Exit — Pagination Without Limits](#11-smart-early-exit--pagination-without-limits)
12. [Incremental Filtering — The Core Efficiency Gain](#12-incremental-filtering--the-core-efficiency-gain)
13. [Deduplication Strategy](#13-deduplication-strategy)
14. [The first_published vs updated_at Problem](#14-the-first_published-vs-updated_at-problem)
15. [Redis — Why and What For](#15-redis--why-and-what-for)
16. [PostgreSQL Schema](#16-postgresql-schema)
17. [End-to-End Data Flow](#17-end-to-end-data-flow)
18. [Resilience and Failure Handling](#18-resilience-and-failure-handling)
19. [Per-ATS Dynamic Concurrency](#19-per-ats-dynamic-concurrency)
20. [Error Type Differentiation](#20-error-type-differentiation)
21. [Scaling Properties](#21-scaling-properties)
22. [Observability — Measuring Whether the System is Working](#22-observability--measuring-whether-the-system-is-working)
23. [Implementation Roadmap](#23-implementation-roadmap)
24. [Thundering Herd Prevention — Hash-Based Slot Distribution](#24-thundering-herd-prevention--hash-based-slot-distribution)
25. [Glossary](#25-glossary)

---

## 1. The Problem We Are Solving

### What the current system does

Every day at 7 AM, the pipeline runs a full scan of all monitored companies one by one (with some parallelism). For each company it:

1. Fetches every single job currently posted at that company
2. Compares against what it already knows
3. Saves any genuinely new ones to the database
4. Sends a digest email with the day's new jobs

### The numbers that reveal the problem

With only 139 companies monitored today, a single daily run:

- **Fetches ~146,497 jobs** from all companies combined
- **Finds ~157 genuinely new jobs** (the ones we care about)
- **Discards 146,340 jobs** that were already seen

That is a **930:1 ratio** of wasted work to useful work. For every 1 new job found, 930 already-seen jobs are fetched, processed, and thrown away.

### Why this becomes a crisis at scale

| Companies | Jobs fetched/day | New jobs/day | Wasted fetches |
|-----------|-----------------|--------------|----------------|
| 139 (today) | 146,497 | 157 | 99.9% |
| 500 | ~527,000 | ~565 | 99.9% |
| 5,000 | ~5,270,000 | ~5,650 | 99.9% |

At 139 companies, one full run takes **45–90 minutes**. At 5,000 companies, a single run would take **25–55 hours** — longer than a day. The system would never finish before the next run starts.

### The deeper problem: a 24-hour blind spot

Because the system runs once per day, there is always up to a **24-hour gap** between when a job is posted and when you see it. If a company posts a job at 8 AM and you receive the digest at 7 AM the next day, that's a 23-hour window where:

- Other candidates may have already applied
- The recruiter may have already started reviewing applications
- Early applicants get their materials seen first

The goal of this pipeline is to be among the **first applicants** — a 24-hour gap defeats that purpose entirely.

### What we need instead

A system that:
- Checks **active companies frequently** (every 30–60 minutes)
- Checks **quiet companies infrequently** (every 6–24 hours)
- Only fetches the **minimum data needed** to detect new jobs
- Automatically adjusts its own pace based on what it observes
- Can grow to **5,000 companies** without requiring any manual configuration
- Never misses a job even when a company suddenly reactivates after months of silence

---

## 2. Current State vs Future State

### Current state

```
Every day at 7 AM:
  For each company (in sequence, with some parallelism):
    Fetch ALL jobs (full listing + detail for some ATS)
    Compare everything against DB
    Save new ones

Problems:
  ✗ 24-hour detection delay
  ✗ Fetches 146k jobs to find 157 new ones (99.9% waste)
  ✗ Accenture/Starbucks (20k jobs) slow the whole run
  ✗ One batch run — if it crashes halfway, companies scanned first
    get 24hr delay, companies not yet scanned get 48hr delay
  ✗ Cannot scale beyond ~200 companies without the run
    taking longer than 24 hours
```

### Future state

```
Continuously, 24 hours a day:
  Two independent scheduling systems:

  ADAPTIVE POLLING (Tier 1):
    Priority queue: companies ordered by next poll time
    When a company is due:
      Fetch ONLY listing metadata (IDs + titles + dates)
      Diff against seen_ids (already in DB) → new IDs only
      Only fetch full detail for NEW job IDs
      Update company's poll score
      Schedule next poll based on score

  FULL SCAN (Tier 2):
    Separate queue: companies ordered score-ascending (dormant first)
    When a company is due:
      Exhaustively scan ALL pages
      Use Bloom filter to skip already-seen jobs
      Catch anything Tier 1 missed
      Self-reschedule on completion

Benefits:
  ✓ Active company detected within 30–60 minutes of posting
  ✓ Dormant company polled once every 6–24 hours (still guaranteed)
  ✓ 99% reduction in data fetched (only new jobs get detail fetches)
  ✓ Full scan safety net catches anything adaptive missed
  ✓ Crash-safe: each company is independently scheduled
  ✓ Scales to 5,000+ companies with no architectural changes
  ✓ Self-tuning: no manual configuration needed as you add companies
```

---

## 3. Core Concept: Continuous Polling vs Daily Batch

### The analogy

Think of the difference between two email clients:

**Daily batch (current system):** Like checking your email once every morning. You receive everything at once, but if someone sent you an urgent message at 9 PM, you won't see it until tomorrow morning.

**Continuous polling (new system):** Like having email push notifications. Important senders (companies that post frequently) have notifications turned on. Less important senders (companies that rarely post) have notifications checked every few hours. You still get everything — but urgent items arrive within minutes.

### What "polling" means

Polling means: "go look at this company's job board and see if anything is new."

The question this architecture answers is: **how often should we poll each company?**

The answer is: **it depends on that company's posting behavior**, and we learn it automatically from observation.

- Starbucks posts 50+ new jobs every week → poll every 30 minutes
- A small fintech startup posts 0-1 jobs per month → poll every 12 hours
- A company that went silent for 3 months → poll every 24 hours
- That same company suddenly posts 10 new jobs → immediately increase polling to every 30 minutes

This adaptive behavior is entirely automatic — no one configures it by hand.

---

## 4. System Components Overview

```
┌─────────────────────────────────────────────────────────────────┐
│               ADAPTIVE DISPATCHER (Tier 1 — scheduling layer)    │
│  Runs every second                                               │
│  ZRANGEBYSCORE poll:adaptive -inf {now} → due companies          │
│  Ceiling check: PEL size of stream:adaptive:{dc_key}            │
│  XADD stream:adaptive:{dc_key}  then  ZREM poll:adaptive        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│          stream:adaptive:{dc_key}  (Redis Stream per DC key)     │
│  Crash-safe delivery: PEL tracks in-flight work                  │
│  XAUTOCLAIM recovers from worker crashes (10 min idle timeout)  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  XREADGROUP
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LISTING SCAN WORKERS                          │
│  For each company when its scheduled time arrives:              │
│    1. Fetch job listing (IDs + titles + dates only)             │
│    2. Smart early exit (stop when mostly-seen jobs appear)      │
│    3. Diff new IDs against job_postings (DB dedup)              │
│    4. Push only NEW job IDs to queue:detail:adaptive            │
│    5. Update this company's poll statistics                     │
│    6. on_adaptive_complete() → ZADD poll:adaptive (reschedule)  │
│    7. XACK stream:adaptive:{dc_key}  ← only after step 6       │
└──────────────────────────┬──────────────────────────────────────┘
                           │ (new job IDs only)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                DETAIL FETCH WORKERS (Two-Tier)                   │
│  queue:detail:adaptive  ← high priority (from listing scan)     │
│  queue:detail:fullscan  ← low priority  (from full scan)        │
│  Workers drain adaptive queue first, then fullscan queue        │
│  Each worker:                                                   │
│    1. BRPOP from priority queues                                │
│    2. Fetch full job detail (location, date, description)       │
│    3. Save to PostgreSQL (status: pending_detail → new)         │
│    4. Mark as seen in DB                                        │
│    5. Trigger notification if job matches filters               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│               FULL SCAN DISPATCHER (Tier 2 — scheduling layer)   │
│  ZRANGEBYSCORE poll:fullscan -inf {now} → due companies          │
│  XADD stream:fullscan  then  ZREM poll:fullscan        │
│  Rule 4 pre-check: adaptive run today? (else defer 15 min)      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│          stream:fullscan  (single shared Redis Stream)           │
│  Crash-safe delivery: PEL tracks in-flight work                  │
│  XAUTOCLAIM recovers from worker crashes (20 min idle timeout)  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  XREADGROUP
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FULL SCAN WORKERS                             │
│  Exhaustively scans ALL pages for each company:                 │
│    1. Check per-company Bloom filter (skip already-seen jobs)   │
│    2. SORTED platforms: 2-page early exit at >80% overlap       │
│    3. NON-SORTED platforms: scan all pages, no cutoff           │
│    4. Push new job IDs to queue:detail:fullscan                 │
│    5. Build new Bloom filter for next cycle (36h TTL)           │
│    6. Write checkpoint on pause signal                          │
│    7. XACK stream:fullscan                                      │
└─────────────────────────────────────────────────────────────────┘

Supporting infrastructure:
┌─────────────────────┐  ┌─────────────────────┐
│  Redis              │  │  PostgreSQL          │
│  • poll:adaptive    │  │  • job_postings      │
│  • poll:fullscan    │  │  • company_poll_stats│
│  • stream:adaptive  │  │  • company_config    │
│    (shared stream)  │  │  • adaptive_poll_    │
│  • stream:fullscan  │  │    metrics           │
│    (shared stream)  │  │  • api_health        │
│  • Bloom filters    │  │  • custom_ats_diag.  │
│  • Detail queues    │  │                      │
│  • Rate limiters    │  │                      │
│  • Pub/Sub channels │  │                      │
│  • Sliding windows  │  │                      │
└─────────────────────┘  └─────────────────────┘

Two co-scheduled worker pools (see Section 9 for full design):
  scan_workers:   calculated at 7 AM from company count + avg listing duration
  detail_workers: calculated at 7 AM from expected new jobs + avg detail duration
  Both pools managed by scheduler as multiprocessing.Process children
  Liveness check every tick (5s) — dead workers replaced immediately
  Fast error check every 5 min — error-triggered worker reduction
  Slow throughput check every 30 min — queue-depth-driven scaling
  DB pool split proportionally between the two pools (combined ≤ DB_POOL_MAXCONN - 3)
```

**Key insight:** Adaptive listing scans and full scans are completely decoupled. A full scan for Starbucks (20k jobs) runs independently of an adaptive poll for a small startup. Neither blocks the other.

---

## 5. The Priority Queue Scheduler

### What a priority queue is (plain English)

Imagine a to-do list where each item has a scheduled time stamped on it. The item with the earliest scheduled time always rises to the top. You pick the top item, do it, then it reschedules itself for later and drops back into the list.

That's exactly how the scheduler works. Every company has a "next poll time" timestamp. The company with the earliest timestamp is always processed first.

### Two separate queues — one per scan type

Each company has **two independent entries** in Redis:

```
poll:adaptive  (Redis ZSET)
  Score (next adaptive poll time)  →  Company
  ─────────────────────────────────────────────
  1745000100  →  "Stripe"        ← due in 2 seconds
  1745000350  →  "Snowflake"     ← due in 252 seconds
  1745086400  →  "Dormant Inc"   ← due in 24 hours

poll:fullscan  (Redis ZSET)
  Score (next full scan time)  →  Company
  ─────────────────────────────────────────────
  1745001000  →  "Dormant Inc"   ← dormant first (score-ascending)
  1745010000  →  "Snowflake"     ← moderate activity
  1745080000  →  "Stripe"        ← active companies last
```

The two queues are driven independently. A company's position in `poll:fullscan` is determined by its activity score — dormant companies are scanned first (they're least likely to have new jobs), active companies last (adaptive polling is already catching their jobs throughout the day).

### How the scheduler loop works

Both Tier 1 and Tier 2 use the same **two-layer pattern**: a Redis ZSET as the scheduling ledger (when is each company due?) and a Redis Stream per DC key as the crash-safe delivery queue (in-flight work tracked by PEL).

```text
At 7 AM cycle start:
  1. Store cycle_start timestamp → redis.set("cycle:start", now)
  2. Dynamic worker count recalculated for this cycle
     (dawn_patrol removed — hash-based slot distribution eliminates clustering)

Adaptive dispatcher (runs every second):
  1. ZRANGEBYSCORE poll:adaptive -inf {now} LIMIT 0 50
     → list of due companies (non-destructive read)
  2. For each due company:
       dc_key  = get_dc_key(company)
       ceiling = get_ceiling(redis, dc_key)          ← worker:ceil:learned:{dc_key}
       inflight = pel_size(stream:adaptive:{dc_key}) ← PEL replaces inflight ZSET
       if inflight >= ceiling: skip (hold, retry next tick)
       XADD stream:adaptive:{dc_key} MAXLEN ~ 1000 * {company, due_at}
       ZREM poll:adaptive {company}   ← only after successful XADD

Listing scan workers (one pool per dc_key):
  consumer = f"worker-{hostname}-{pid}"   ← unique per process, prevents PEL theft

  Startup (once per worker):
       XGROUP CREATE stream:adaptive:{dc_key} scan-workers $ MKSTREAM
       ↑ id=$ → only new messages; BUSYGROUP error = group exists → ignore

  Main loop (500ms block, not 5000ms — must react to pipeline:pause within ~1s):
  1. if pause_event.is_set(): wait_for_resume(); continue
  2. XREADGROUP GROUP scan-workers {consumer} COUNT 1 BLOCK 500
          STREAMS stream:adaptive:{dc_key} >
  3. Run listing scan for company
  4. on_adaptive_complete(company):
       a. Update stats (recent_poll_counts, score, interval)
       b. ZADD poll:adaptive {company: now + new_interval}  ← reschedule
       c. Rule 3 / Rule 5: ZADD poll:fullscan if full scan due
  5. XACK stream:adaptive:{dc_key} {msg_id}  ← only after step 4 completes

  Crash recovery (run when step 2 returns empty — no new messages):
       p95_ms = get_p95_listing_scan_ms(redis, dc_key)   ← from api_health, 5-min cache
       idle_ms = max(p95_ms * 3, 60_000)                 ← at least 1 min
       XAUTOCLAIM stream:adaptive:{dc_key} scan-workers {consumer}
                  MIN-IDLE-TIME {idle_ms}
       → re-delivers messages idle longer than 3× p95 from crashed workers
       → check delivery_count before running; dead-letter after MAX_REDELIVERIES (5)

Full scan dispatcher (runs every 5 seconds):
  1. ZRANGEBYSCORE poll:fullscan -inf {now} LIMIT 0 50
  2. For each due company:
       Rule 4 pre-check: did adaptive run today?
         If not: ZADD poll:adaptive {company: now}
                 ZADD poll:fullscan  {company: now + 900}
                 continue  ← come back after adaptive runs
       dc_key = get_dc_key(company)
       Ceiling check (Lua — atomic): if worker:ceil:learned:{dc_key} exists:
         ZREMRANGEBYSCORE inflight:scans:{dc_key} 0 (now-10min)     ← prune stale adaptive entries
         ZREMRANGEBYSCORE inflight:fullscans:{dc_key} 0 (now-2h)    ← prune stale fullscan entries
         total = ZCARD inflight:scans:{dc_key} + ZCARD inflight:fullscans:{dc_key}
         if total >= ceiling:
           ZADD poll:fullscan {company: now+30}
           continue  ← re-queue and skip
         ZADD inflight:fullscans:{dc_key} {now} {company}  ← claim slot atomically
       XADD stream:fullscan (single shared stream) * {company, dc_key, triggered_at}
         On XADD failure: ZREM inflight:fullscans:{dc_key} {company}  ← release claimed slot
           and ZADD poll:fullscan {company: now+60}  ← reschedule; prevents slot leak
       ZREM poll:fullscan {company}

Full scan workers (one pool per dc_key):
  consumer = f"worker-{hostname}-{pid}"

  Startup:
       XGROUP CREATE stream:fullscan fullscan-workers $ MKSTREAM
       ↑ BUSYGROUP error = group exists → ignore

  Main loop:
  1. if pause_event.is_set(): wait_for_resume(); continue
  2. XREADGROUP GROUP fullscan-workers {consumer} COUNT 1 BLOCK 500
          STREAMS stream:fullscan >
  3. Run full scan (all pages, Bloom filter, checkpoint support)
  4. XACK stream:fullscan {msg_id}
     Full scan does NOT self-reschedule — on_adaptive_complete() handles
     re-entry into poll:fullscan via Rules 3 and 5

  Crash recovery (run when step 2 returns empty):
       p95_ms = get_p95_full_scan_ms(redis, dc_key)   ← separate from listing scan p95
       idle_ms = max(p95_ms * 3, 300_000)             ← at least 5 min
       XAUTOCLAIM stream:fullscan fullscan-workers {consumer}
                  MIN-IDLE-TIME {idle_ms}
       → check delivery_count; dead-letter after MAX_REDELIVERIES (5)
```

**Why BLOCK 500 (not BLOCK 5000) and why pause_event matters:**
`XREADGROUP BLOCK 5000` would keep a worker blocked for up to 5 seconds waiting for new messages. During that block, the `pipeline:pause` Pub/Sub signal arrives on a *separate* Redis connection (Pub/Sub requires its own connection) and sets a `multiprocessing.Event`. The worker won't see the event until the block returns. With a 5-second block, the worker could start a new 30-minute scan immediately after `pipeline:pause` — only detecting the signal at the next page boundary.

Using `BLOCK 500` (half a second) means the main loop re-checks `pause_event.is_set()` at most 500ms after the signal arrives — fast enough to defer any new scan start cleanly.

The Pub/Sub listener runs in a **daemon thread** spawned at worker startup:
```python
def _pubsub_listener(pause_event: threading.Event, resume_event: threading.Event):
    sub = redis.pubsub()
    sub.subscribe("pipeline:pause", "pipeline:resume")
    for msg in sub.listen():
        if msg["channel"] == b"pipeline:pause":
            pause_event.set()
            resume_event.clear()
        elif msg["channel"] == b"pipeline:resume":
            resume_event.set()
            pause_event.clear()

def wait_for_resume(pause_event, resume_event):
    logger.info("worker paused — waiting for pipeline:resume")
    resume_event.wait()   # blocks until resume signal
    logger.info("worker resumed")
```

This pattern is identical for both adaptive scan workers and full scan workers.

**Why ZRANGEBYSCORE + ZREM instead of ZPOPMIN:**
`ZPOPMIN` is destructive — if the process crashes between the pop and the `XADD`, the company exists in neither the ZSET nor the Stream. With `ZRANGEBYSCORE` (read) → `XADD` → `ZREM` (delete only after successful write), a crash between `XADD` and `ZREM` leaves the company in both structures. On restart the dispatcher finds it in the ZSET again and tries another `XADD`. The duplicate in the stream is harmless: the scan worker runs twice in quick succession; the second scan finds all IDs already in `adaptive_seen:{company}` and exits immediately.

**Why there is no watchdog process:**
Watchdog existed to detect expired `heartbeat:{company}` keys and re-queue orphaned companies. With Streams, the PEL is the watchdog — messages not ACK'd within the idle timeout are automatically re-delivered by `XAUTOCLAIM`. No separate process, no TTL tuning, no race between ZPOPMIN and heartbeat:set.

### Why Redis instead of Python's heapq?

Python's `heapq` is in-memory and lives in a single process. When we scale to multiple worker processes, they would each have their own separate heap — leading to the same company being polled multiple times at once.

Redis operations are atomic — only one worker can pop a given company at a time. This is the critical property that makes multi-worker polling safe.

### Scheduling uses Redis TIME, not system clock

All score calculations use the Redis server's clock (`TIME` command), not `time.time()` on the application server. This prevents clock skew issues if the application server's NTP sync causes a sudden time jump.

The 7 AM daily cycle start is computed using `America/New_York` timezone (handles DST automatically) and stored as a Unix timestamp in Redis at cycle start:

```python
import pytz
eastern = pytz.timezone("America/New_York")
cycle_start = eastern.localize(
    datetime.now().replace(hour=7, minute=0, second=0)
).timestamp()
redis.set("cycle:start", cycle_start)
```

---

## 6. Adaptive Interval Engine

### The core question

After each poll of a company, we must decide: **when should we poll this company again?**

The answer should be:
- **Soon** if the company is actively posting new jobs
- **Later** if the company rarely posts anything
- **Much later** if the company has been silent for a long time

### The signal: 5-poll weighted queue

For each company, we maintain a rolling window of the last 5 poll results as a JSON array in `company_poll_stats.recent_poll_counts`.

```
recent_poll_counts = [0, 0, 2, 3, 1]
                      ↑           ↑
                   oldest       newest
```

On each poll: append new result, pop oldest.

The score is a **weighted average** with recency bias:

```python
WEIGHTS = [0.10, 0.15, 0.20, 0.25, 0.30]   # oldest → newest

def compute_score(recent_poll_counts):
    if len(recent_poll_counts) < 3:
        return None   # not enough history yet — use default interval
    
    # Pad with zeros if fewer than 5 polls
    counts = recent_poll_counts[-5:]
    weights = WEIGHTS[-len(counts):]
    weight_sum = sum(weights)
    
    return sum(c * w for c, w in zip(counts, weights)) / weight_sum
```

**Why recency-weighted instead of simple average:**

```
Queue: [5, 0, 0, 0, 0]  →  simple avg = 1.0  (burst 5 polls ago still inflating)
Queue: [0, 0, 0, 0, 5]  →  simple avg = 1.0  (burst just happened — same score, wrong!)

With weights:
Queue: [5, 0, 0, 0, 0]  →  weighted = 0.50  (fading burst, correctly low)
Queue: [0, 0, 0, 0, 5]  →  weighted = 1.50  (fresh burst, correctly higher)
```

**Cap per-poll contribution at 10** to prevent a single mass-hiring event from locking a company into maximum frequency for the next 5 polls.

### Converting score to interval

`band_lookup` maps a score to a poll interval using **dynamic thresholds** calibrated daily from the actual portfolio distribution (see [Band Calibration](#band-calibration) below). Scores are compared against three stored threshold values (`low`, `moderate`, `active`) rather than hardcoded numbers:

```python
def band_lookup(score, thresholds):
    if score is None or score == 0.0:
        return 12 * 3600    # 12h — no history yet, or no new jobs in window

    if score < thresholds["low"]:
        return  9 * 3600    # 9h  — below-median activity
    elif score < thresholds["moderate"]:
        return  6 * 3600    # 6h  — moderate activity
    elif score < thresholds["active"]:
        return  4 * 3600    # 4h  — active
    else:
        return  3 * 3600    # 3h  — very active (MIN_INTERVAL)
```

`score = None` (fewer than 3 polls of history) and `score = 0.0` (no new jobs across the entire window) both return 12h. These companies are **excluded from ranking** — they always get the default interval regardless of how other companies are performing.

### Band Calibration

**The problem with hardcoded thresholds:** If your portfolio of 50 companies mostly posts 0–1 new jobs per poll (realistic for most markets), scores cluster between 0 and 1.0. With thresholds like `score < 1.5 → 9h` and `score < 3.5 → 6h`, 90% of companies permanently sit in the 12h or 9h band — the 6h, 4h, and 3h bands go unused even when some companies are meaningfully more active than others.

**The fix: rank-based calibration.** Instead of asking "is your score above an arbitrary absolute number?", we ask "are you in the top 10% of your peers?" Thresholds are computed daily from the actual distribution of scores across the live portfolio.

#### Algorithm

Once per day (at cycle start) and at scheduler startup:

1. **Query** `company_poll_stats.adaptive_score` for all companies with `score > 0` polled within the last 30 days. These are the only companies that participate in ranking — dormant companies (score=0) always get 12h and are excluded.

2. **Winsorize** the top 5% of scores (replace with the 95th percentile value). This prevents one mass-hiring outlier from shifting the thresholds upward and penalizing every other company in the portfolio.

3. **Sort descending** and compute the score at each rank boundary:

```
Rank cut   Target     Band    Meaning
top 10%  → "active"  → 3h    Exceptional, consistent new postings
next 15% → "moderate"→ 4h    Clearly above-average hiring activity
next 25% → "low"     → 6h    Moderate — worth checking twice a day
bottom 50%            → 9h    Quiet relative to peers, baseline service
```

4. **Tie-promotion rule:** If multiple companies share the exact score sitting at a band boundary, all of them are promoted to the better (faster) band. Example: if scores at the 10% boundary are `[8.0, 1.8, 1.8, ...]`, both 1.8s join the 3h band.

5. **Store** the three threshold values in Redis (`adaptive:band_thresholds` hash). Workers read from a 5-minute in-process cache backed by this key.

6. **Fallback:** If fewer than 5 active companies exist (cold start or very small portfolio), use `DEFAULT_THRESHOLDS = {low: 1.5, moderate: 3.5, active: 6.0}` — the original hardcoded values. Calibration kicks in automatically once real data accumulates.

#### Concrete example (10 active companies)

```
Raw scores (ascending):   [0.2, 0.3, 0.4, 0.5, 0.7, 0.9, 1.2, 1.4, 1.8, 8.0]

Step 1 — Winsorize top 5% (1 company): replace 8.0 → 1.8
Winsorized:               [0.2, 0.3, 0.4, 0.5, 0.7, 0.9, 1.2, 1.4, 1.8, 1.8]

Step 2 — Rank boundaries (descending sort):
  top 10%  = rank 1       → boundary score = 8.0  → active  threshold
  next 15% = ranks 2–3    → boundary score = 1.4  → moderate threshold
  next 25% = ranks 4–6    → boundary score = 0.7  → low     threshold

Stored:  active=8.0, moderate=1.4, low=0.7

Result:
  Company  Score  Band
  Z        8.0    3h   (>= active=8.0)
  9        1.8    4h   (>= moderate=1.4, < active=8.0)
  8        1.4    4h   (= moderate threshold, tie-promoted up)
  7        1.2    6h   (>= low=0.7, < moderate=1.4)
  6        0.9    6h
  5        0.7    6h   (= low threshold, tie-promoted up)
  4–1      0.5    9h   (> 0, < low=0.7)
  ...      0.0    12h  (not ranked)
```

#### Spike reactivity — why there is no 24-hour lag

A common concern: "if thresholds only update daily, won't a hiring spike take 24 hours to reflect?"

No — because **thresholds and scores are independent**. Thresholds are fixed within a day; scores update on every single poll. When Company A's score jumps from 0.4 to 5.0 mid-day:

```
8 AM calibration stored thresholds:  active=8.0, moderate=1.4, low=0.7

10 AM — Company A posts 10 new jobs:
  Rolling window update: [0, 0, 0, 0, 10] → score = 3.0
  band_lookup(3.0, thresholds):  3.0 >= moderate(1.4) → 4h  ← immediate
```

The company moves from 9h to 4h **on the very next poll**, with no recalibration needed. The spike is detected the moment the score crosses a threshold — not at the next calibration window.

The only 24-hour effect is **threshold drift**: if a spike is so extreme it would shift P92 significantly, today's thresholds won't reflect that until tomorrow. But because we Winsorize the top 5%, even an extreme outlier barely moves the stored thresholds. Other companies are not penalized.

### Asymmetric smoothing — preventing oscillation without dampening reactivation

The computed interval from `band_lookup` is not applied directly. It is smoothed — but **asymmetrically**:

```python
def compute_next_interval(computed, current_interval):
    if computed < current_interval:
        # Company getting MORE active → react immediately, no smoothing
        return computed
    else:
        # Company going DORMANT → change gradually
        return 0.7 * current_interval + 0.3 * computed
```

| Direction | Behavior | Why |
|---|---|---|
| Active → dormant | Gradual decay over ~5 polls | Don't stop polling after one empty day |
| Dormant → active | Immediate drop | Don't miss a hiring burst |

**Why not symmetric EMA (0.7 × old + 0.3 × new) for both directions:**

Symmetric EMA is too slow on reactivation. A dormant company (24h interval) suddenly posting 5 jobs:
- Symmetric EMA: `0.7 × 24h + 0.3 × 6h = 18.8h` — barely moved on first poll
- Asymmetric: interval drops immediately to 6h on first new job found

### Full calculation flow

```python
def update_poll_interval(company, new_jobs_found, thresholds):
    # Step 1: Update queue
    counts = company.recent_poll_counts or []
    counts.append(min(new_jobs_found, 10))   # cap at 10
    if len(counts) > 5:
        counts.pop(0)
    
    # Step 2: Compute score
    score = compute_score(counts)
    
    # Step 3: Band lookup — uses calibrated thresholds (or DEFAULT_THRESHOLDS)
    computed = band_lookup(score, thresholds)
    
    # Step 4: Asymmetric smoothing
    new_interval = compute_next_interval(computed, company.current_interval_s)
    
    # Step 5: Apply MAX_INTERVAL cap (see Section 8)
    max_interval = get_max_interval(score, thresholds)
    final_interval = min(new_interval, max_interval)
    
    # Step 6: Save
    company.recent_poll_counts = counts
    company.current_interval_s = final_interval
    company.adaptive_score = score or 0.0
    company.next_poll_at = now() + final_interval

# thresholds loaded once per on_adaptive_complete call:
#   thresholds = get_band_thresholds(r)   ← 5-min in-process cache, Redis-backed
```

---

## 7. Tier 1 and Tier 2 — What They Mean

Tier 1 and Tier 2 are **two separate systems** running independently. They are not two categories of companies — every company goes through both.

### Tier 1: Adaptive Polling

*Continuous, score-driven, variable interval.*

The listing scan worker runs for a company whenever its scheduled time arrives. The interval is determined by the adaptive engine (Section 6) — frequently for active companies, infrequently for quiet ones.

**Tier 1 catches new jobs quickly.** An active company posting a new job will have it detected within 3–6 hours at most, often within 30–60 minutes.

Jobs found by Tier 1 are tagged: `found_by = 'tier1_adaptive'`

### Tier 2: Full Scan

*Exhaustive, safety-net, own schedule.*

The full scan worker runs on a completely separate schedule. It scans **every page** of every company's job listing. It is the guarantee that nothing slips through — even if Tier 1's adaptive interval was slightly too long, or the company uses a non-sorted ATS where the early exit can't be used.

**Tier 2 catches what Tier 1 missed.** Most of the time it finds nothing new (because Tier 1 already caught it). When it does find something, it means the adaptive engine had an interval gap.

Jobs found by Tier 2 are tagged: `found_by = 'tier2_full_scan'`

### The `found_by` field is the health monitor

```
miss_rate = tier2_new_jobs / total_new_jobs
```

- miss_rate < 5%  → Tier 1 is working well
- miss_rate > 15% → Tier 1 intervals need tuning

This single field tells you whether the adaptive engine is doing its job.

### The adaptive-first rule — four enforcement layers

Full scan must always run **after at least one adaptive poll has completed today**. This ensures `found_by` attribution is accurate and miss_rate reflects genuine adaptive failures, not scheduling races.

Four rules enforce this together:

**Rule 1 — 12h MAX_INTERVAL floor (primary prevention)**
No company polls less frequently than every 12 hours. Worst case: a dormant company last polled at 1 AM gets its adaptive poll at 1 PM — full scan follows at 1:05 PM. Covered before end of day.

**Rule 2 — Dawn patrol (RETIRED)**
Dawn patrol redistributed late adaptive polls into the 7 AM–9 AM window at cycle start. This is no longer needed. Hash-based slot assignment (see Section 24) distributes adaptive polls evenly across the 24-hour day from the very first cycle — there are no clustered late polls to redistribute. Dawn patrol is removed.

**Rule 3 — Adaptive triggers full scan directly (structural enforcement)**
When adaptive completes, it checks whether a full scan needs to be scheduled or
rescheduled, and queues one if needed. Full scan can enter `poll:fullscan` via three paths:

- **Path 1** — `fullscan.py` post-completion rescheduling (normal cadence)
- **Path 2** — `_maybe_reschedule_full_scan()` triggered by adaptive completion (this rule)
- **Path 3** — `_queue_fullscans_for_missed()` in `job_monitor.py` (safety net for companies
  whose fullscan was missed entirely — NULL or stale `next_full_scan_at`). This path bypasses
  the adaptive-first rule (Rule 4) since the company has already been seen by the listing
  fallback; it schedules a fullscan directly.

```python
def on_adaptive_complete(company):
    update_stats(company)
    reschedule_adaptive(company)

    if success:
        _maybe_reschedule_full_scan(company, row)

def _maybe_reschedule_full_scan(company, row):
    # Suppressed during active ATS scan backoff
    if r.exists(f"retry:backoff:scan:{company}"):
        return

    avg_duration = float(row["avg_fullscan_duration_s"] or 1800.0)
    next_scan    = row["next_full_scan_at"]

    if next_scan is not None:
        next_ts  = next_scan.timestamp()
        deadline = _next_digest_deadline(now)
        if next_ts > now and (next_ts + avg_duration) < deadline:
            return   # already scheduled and will finish before 7 AM digest

    # Use _atomic_schedule (gap-filling) rather than raw zadd
    _atomic_schedule(
        r, REDIS_POLL_FULLSCAN, company,
        now + SCHEDULER_FULL_SCAN_BUFFER_S,
        SCHEDULER_FULL_SCAN_INTERVAL_S, 0.20,
        deadline_ts    = _next_digest_deadline(now),
        avg_duration_s = avg_duration,
    )
```

Three cases trigger (re)scheduling: `next_full_scan_at` is NULL (never scheduled),
`next_full_scan_at` is in the past (overdue / ZSET entry lost), or the scheduled time
plus average scan duration would miss the 7 AM digest deadline.

Because fullscan requires a prior adaptive completion (Path 2 only fires inside
`on_adaptive_complete`), the adaptive-first rule is enforced by design for the normal
scan cycle. **Exception — Path 3 safety net:** `_queue_fullscans_for_missed()` in
`job_monitor.py` (Path 3) can schedule a fullscan after a listing-scan fallback even
when no adaptive completion has occurred, to catch companies whose fullscan window
would otherwise be missed entirely.

**Rule 5 — Pre-7 AM full scan trigger (crossing-day boundary)**
When `on_adaptive_complete()` reschedules the next adaptive poll to *tomorrow* (crossing the 7 AM boundary), the full scan for the current cycle may not have run yet. If sufficient time remains tonight, trigger the full scan now rather than leaving it undone until tomorrow's cycle.

```python
def on_adaptive_complete(company):
    update_stats(company)
    reschedule_adaptive(company)   # e.g. 8:30 PM → next_poll_at = 8:30 AM tomorrow

    # Rule 3 + Rule 5: _maybe_reschedule_full_scan handles both the standard
    # 24h trigger and the crossing-7AM case via _atomic_schedule (gap-filling).
    _maybe_reschedule_full_scan(company, row)


def _maybe_trigger_pre7am_fullscan(company):
    now = time.time()
    eastern = pytz.timezone("America/New_York")
    today_7am = eastern.localize(
        datetime.now(eastern).replace(hour=7, minute=0, second=0, microsecond=0)
    ).timestamp()

    # Only fires when next poll crosses to tomorrow morning
    if company.next_poll_at.timestamp() <= today_7am:
        return

    # Full scan already done this cycle — nothing to do
    if company.last_full_scan_at and \
       company.last_full_scan_at.timestamp() >= today_7am - 86400:
        return

    # Time gate: full scan p95 × 3 + 2h safety buffer must fit before 7 AM.
    # Must use full scan p95 (not listing scan p95 — full scans take far longer).
    p95_fullscan_s  = get_p95_full_scan_s(redis, company.dc_key)   # full scan history only
    buffer_s        = 2 * 3600
    time_until_7am  = today_7am + 86400 - now
    if time_until_7am > p95_fullscan_s * 3 + buffer_s:
        redis.zadd("poll:fullscan", {company.name: now + 300})
        logger.info(
            "rule5: %s → full scan triggered pre-7am "
            "(%.0fs until 7am, p95_fullscan=%.0fs)",
            company.name, time_until_7am, p95_fullscan_s,
        )
```

| Rule 5 scenario | Outcome |
|---|---|
| next_poll_at = tomorrow 8:30 AM, full scan not done today, time_until_7am > p95_fullscan × 3 + 2h | Full scan triggered tonight |
| next_poll_at = today 3 PM (not crossing 7 AM boundary) | Rule 5 skipped |
| next_poll_at = tomorrow 8:30 AM, full scan already done today | Rule 5 skipped |
| next_poll_at = tomorrow 8:30 AM, only 90 min until 7 AM (< p95_fullscan × 3 + 2h) | Rule 5 skipped — insufficient time |

**Rule 4 — Full scan worker pre-check (safety net for restarts and edge cases)**
As a fallback for system restarts, first-run companies, or repeated adaptive errors:

```python
def run_full_scan(company):
    cycle_start = float(redis.get("cycle:start"))
    last_poll   = company.last_poll_at

    if last_poll is None or last_poll.timestamp() < cycle_start:
        # Adaptive hasn't run today — trigger it immediately
        redis.zadd("poll:adaptive", {company.name: time.time()})
        # Requeue full scan for 15 min from now
        redis.zadd("poll:fullscan", {company.name: time.time() + 900})
        return   # come back after adaptive runs

    proceed_with_full_scan(company)
```

| Scenario | Rule that handles it |
|---|---|
| Dormant company, last polled 11 PM | Rule 1 → 12h cap guarantees adaptive by 11 AM at latest |
| Many companies with clustered adaptive intervals | Section 24 hash distribution → never cluster in the first place |
| New company, never polled | Rule 4 → triggers adaptive before allowing full scan |
| Adaptive keeps erroring | Rule 4 → keeps retrying before allowing full scan |
| Active company, 4h interval | Rule 3 → full scan triggered once per day naturally |
| System restart mid-day | Rule 4 → pre-check catches any companies not yet polled |
| next_poll_at crosses to tomorrow, full scan not done today | Rule 5 → triggers full scan tonight if time allows |

---

## 8. Adaptive MAX_INTERVAL Cap

The adaptive engine can compute very long intervals for dormant companies. The MAX_INTERVAL cap ensures no company goes unpolled for too long — this is what guarantees reactivation detection within a bounded window.

The cap is **score-tiered** — active companies get a tighter cap than dormant ones. The tier boundary uses the calibrated `moderate` threshold (the P75 of the portfolio), so the cap automatically adjusts as the portfolio's activity level changes:

```python
def get_max_interval(score, thresholds):
    # Companies at or above the moderate threshold (top 25% of active portfolio)
    # get the tighter 6h cap. All others get 12h.
    if score is not None and score > 0.0 and score >= thresholds["moderate"]:
        return  6 * 3600   # 6 hours — moderate+ companies
    return 12 * 3600       # 12 hours — dormant / low activity
```

| Activity level | Threshold position | Natural band | MAX_INTERVAL |
|---|---|---|---|
| **Very active** | score >= thresholds["active"] (top 10%) | 3h | 6h |
| **Active** | thresholds["moderate"] <= score < thresholds["active"] (next 15%) | 4h | 6h |
| **Moderate** | thresholds["low"] <= score < thresholds["moderate"] (next 25%) | 6h | 6h |
| **Low activity** | 0 < score < thresholds["low"] (bottom 50%) | 9h | 12h |
| **Dormant** | score = 0 or None | 12h | 12h |

**24h MAX_INTERVAL is removed entirely.** 12h is the new floor for all companies.

**Why this works:**

The 12h floor for dormant companies serves two purposes simultaneously:
1. Keeps the adaptive-first rule achievable — any company polled up to 1 AM will get its next adaptive poll by 1 PM, leaving the full afternoon for the full scan
2. Catches reactivations within half a day instead of a full day

For active companies, the 6h MAX_INTERVAL prevents EMA smoothing from drifting the interval too high during a temporarily slow week while still allowing the 3h natural band for peak activity.

---

## 9. Full Scan Architecture

### Purpose

Full scan is the **bulletproof safety net**. It exhaustively scans every page of every company's job board on a rolling daily cycle. It guarantees that no job is permanently missed, regardless of how the adaptive engine behaves.

### Scheduling strategy — two-layer design

Full scan uses a **two-layer design**: a ZSET for scheduling (when is each company due?) and a Redis Stream per DC key for crash-safe delivery to workers.

```text
poll:fullscan  (Redis ZSET — scheduling ledger)
  Score = next_full_scan_at timestamp
  Written by: on_adaptive_complete() — Rules 3 and 5
  Read by: full scan dispatcher loop

stream:fullscan  (Redis Stream — single shared delivery queue)
  One stream shared by all DC keys; dc_key is a field in each message, not part of the key.
  Written by: dispatcher — XADD when score ≤ now
  Read by: full scan workers — XREADGROUP (consumer groups, PEL, XCLAIM)
```

**Why Streams instead of BLPOP LIST?**
Redis Streams provide at-least-once delivery via the Pending Entries List (PEL). If a worker crashes mid-scan, the entry stays in the PEL and `XCLAIM` re-delivers it to another worker after a timeout. No job is silently dropped on worker crash — a property BLPOP/LPUSH cannot provide.

**Why one shared stream (not one stream per DC key)?**
A single `stream:fullscan` simplifies consumer group management — one `XGROUP CREATE` on startup covers all DC keys. Workers filter by the `dc_key` field in each message; per-DC ceiling enforcement is handled in the worker via the `inflight:fullscans:{dc_key}` ZSET and the atomic Lua script, not by stream routing.

**Dispatcher loop (runs every 5 seconds):**

```python
def fullscan_dispatcher_loop():
    while True:
        due = redis.zrangebyscore("poll:fullscan", "-inf", time.time(), start=0, num=50)
        for company in due:
            dc_key = get_dc_key(company)   # e.g. workday_wd1, greenhouse
            redis.xadd("stream:fullscan", {
                "company": company,
                "dc_key": dc_key,
                "triggered_at": str(time.time()),
            })
            redis.zrem("poll:fullscan", company)
        time.sleep(5)
```

**Workers:**

```python
def fullscan_worker(dc_key: str, group: str, consumer: str):
    stream_key = "stream:fullscan"
    redis.xgroup_create(stream_key, group, id="$", mkstream=True)  # id="$": new messages only; BUSYGROUP ignored
    while True:
        msgs = redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1, block=500)
        if not msgs:
            continue
        _, entries = msgs[0]
        msg_id, fields = entries[0]
        run_full_scan(fields["company"])
        redis.xack(stream_key, group, msg_id)
        # Stale PEL entries (crashed workers) are recovered by claim_stale_work()
        # via XAUTOCLAIM — idle threshold = max(p95_full_scan_ms × 3, 300 000 ms)
```

**Crash recovery and dead-letter handling:**

```python
MAX_STREAM_REDELIVERIES = 5   # after this many claims, give up and backoff

def claim_stale_work(stream_key: str, group: str, consumer: str,
                     p95_ms: int, run_fn, op_type: str):
    """
    Re-claim messages idle longer than 3× p95 scan time.
    Dead-letters after MAX_STREAM_REDELIVERIES to prevent infinite loops
    on persistently broken companies.
    """
    idle_ms = max(p95_ms * 3, 300_000)   # at least 5 min; scales with actual scan times
    stale = redis.xautoclaim(stream_key, group, consumer, min_idle_time=idle_ms)

    for msg_id, fields in stale[1]:
        # Redis 7+ returns (None, None) for PEL entries whose backing stream
        # message was trimmed/deleted by MAXLEN. Skip rather than XACK with None
        # (which would raise a Redis error). These are logged separately as deleted_ids.
        if msg_id is None:
            continue
        company = fields["company"]

        # Check how many times this message has been delivered already
        pending = redis.xpending_range(stream_key, group, msg_id, msg_id, count=1)
        delivery_count = pending[0]["times_delivered"] if pending else 0

        if delivery_count >= MAX_STREAM_REDELIVERIES:
            # Dead-letter: move out of stream into poll:* with exponential backoff
            delay = _get_backoff_delay(redis, company, op_type=op_type)
            if op_type == "fullscan":
                redis.zadd("poll:fullscan", {company: time.time() + delay})
            else:
                redis.zadd("poll:adaptive", {company: time.time() + delay})
            redis.xack(stream_key, group, msg_id)
            logger.warning(
                "stream dead-letter: %s after %d redeliveries → backoff +%ds",
                company, delivery_count, delay,
            )
            continue

        run_fn(company)
        redis.xack(stream_key, group, msg_id)


# Called from full scan worker main loop when XREADGROUP returns empty:
p95_ms = get_p95_full_scan_ms(redis, dc_key)   # from api_health — full scan duration only
claim_stale_work(
    "stream:fullscan", "fullscan-workers", consumer,
    p95_ms=p95_ms, run_fn=run_full_scan, op_type="fullscan",
)

# Called from listing scan worker main loop when XREADGROUP returns empty:
p95_ms = get_p95_listing_scan_ms(redis, dc_key)   # listing scan duration only
claim_stale_work(
    f"stream:adaptive:{dc_key}", "scan-workers", consumer,
    p95_ms=p95_ms, run_fn=run_listing_scan, op_type="scan",
)
```

**Why p95 × 3 and not a hardcoded timeout:**
A hardcoded 20 min reclaims messages from legitimately slow workers processing large companies (Starbucks full scans can take 30–45 min). Using `p95_full_scan_ms × 3` means the timeout automatically accommodates whatever scan durations the ATS + company size combination actually produces, and self-adjusts as the portfolio grows.

**Overdue companies at startup (restart / crash):**
Companies with `next_full_scan_at` in the past are added directly to the stream (bypassing the ZSET) so workers begin processing immediately at ceiling capacity. FIFO order means the most-overdue companies (added first) are processed first.

Full scan entries live in `poll:fullscan` Redis ZSET, ordered **score-ascending** (dormant companies first, active companies last):

```
Dormant companies (score ≈ 0):
  → Full scan early in the day (few hours after 7 AM)
  → They rarely have new jobs, so full scan is fast (hits Bloom filter quickly)
  → No risk of missing them — adaptive already polls at 24h

Active companies (score > 1.5):
  → Full scan late in the day (toward end of daily cycle)
  → Adaptive polling is already catching their jobs throughout the day
  → Full scan is a formality for active companies — rarely finds anything new
```

**Why dormant first?** If workers can't finish all full scans before the next 7 AM cycle, the companies most likely to have new jobs (active, high-score) were already covered by adaptive polling. Missing full scan for a dormant company is more harmful than missing it for an active company.

### When full scan is scheduled

Full scan is NOT scheduled at 7 AM or on any independent timer. It is triggered **directly by adaptive completion** via `on_adaptive_complete()`. When the adaptive poll finishes and `full_scan_interval_s` (24h) has elapsed since the last full scan, adaptive queues the full scan automatically — 5 minutes later:

```python
def on_adaptive_complete(company):
    update_stats(company)          # update score, interval, recent_poll_counts
    reschedule_adaptive(company)   # ZADD poll:adaptive with new next_poll_at

    if success:
        _maybe_reschedule_full_scan(company, row)
```

`_maybe_reschedule_full_scan` is a no-op when the company already has a future fullscan
scheduled that will finish before the 7 AM digest (checked via `next_full_scan_at` from
`company_poll_stats` + `avg_fullscan_duration_s` EMA). It only writes to `poll:fullscan`
when scheduling is genuinely missing or stale, and uses `_atomic_schedule` (gap-filling)
rather than a raw `zadd` to prevent clustering with other companies.

This makes the adaptive-first rule structural: full scan **cannot** enter `poll:fullscan`
without an adaptive poll having just completed — **except** via Path 3
(`_queue_fullscans_for_missed()` in `job_monitor.py`), which schedules a fullscan directly
after a listing-scan fallback when `next_full_scan_at` is NULL or stale. The
`SCHEDULER_FULL_SCAN_BUFFER_S` offset (default 5 min) gives detail workers a head start
on any new jobs adaptive just found before the full scan begins.

**Full scan frequency** is controlled entirely by `full_scan_interval_s` (default 24h) in `company_poll_stats`. Even if adaptive runs twice in a day (which it will for active companies at 3–6h intervals), only the first call after the 24h window has elapsed triggers a new full scan:

```
Active company — 4h adaptive interval:
  7:00 AM  → adaptive runs → 24h elapsed → full scan queued for 7:05 AM ✓
  11:00 AM → adaptive runs → only 4h elapsed → full scan NOT triggered
  3:00 PM  → adaptive runs → only 8h elapsed → full scan NOT triggered
  7:00 PM  → adaptive runs → only 12h elapsed → full scan NOT triggered

Next day:
  7:00 AM  → adaptive runs → 24h elapsed → full scan queued again ✓
```

Full scan runs exactly once per 24h per company, regardless of how often adaptive polls.

### Missed-company fullscan queuing — job_monitor safety net

When the 7 AM job_monitor runs, `_get_worker_missed_companies()` identifies companies
that were not covered by scan workers (worker death, thundering herd, throughput ceiling).
The monitor performs a listing fallback scan for each missed company (all pages via
`fetch_jobs()`), which catches new jobs for the digest. However, this listing fallback
does **not** update `last_full_scan_at` and does **not** queue a fullscan — the Bloom
filter stays stale and the company may not get a proper fullscan until late in the day.

`_queue_fullscans_for_missed(missed)` runs after the listing fallback:

```python
def _queue_fullscans_for_missed(missed):
    # For each missed company where next_full_scan_at is NULL or in the past
    for company in missed:
        next_scan = stats_row["next_full_scan_at"]
        if next_scan is not None and next_scan.timestamp() > now:
            continue   # already has a future fullscan — leave it alone
        _atomic_schedule(
            r, REDIS_POLL_FULLSCAN, company,
            now + SCHEDULER_FULL_SCAN_BUFFER_S,
            SCHEDULER_FULL_SCAN_INTERVAL_S, 0.20,
            deadline_ts    = _next_digest_deadline(now),
            avg_duration_s = avg_duration,
        )
```

**Why this matters:** The listing fallback in job_monitor serves as the digest-day
recovery for missed companies (all pages, same coverage as a fullscan), but the fullscan
worker is the only path that updates `last_full_scan_at` and rebuilds the Bloom filter.
Without `_queue_fullscans_for_missed`, a company that was missed today would: (a) not have
an updated Bloom filter for tomorrow's adaptive early-exit checks, and (b) have a stale or
NULL `next_full_scan_at` which might trigger Path 2 rescheduling during the *next* adaptive
completion — possibly at the wrong time. Queuing the fullscan explicitly ensures the company
gets proper coverage within the same day.

Redis or DB failures in `_queue_fullscans_for_missed` are non-fatal: the listing fallback
result is already saved regardless, and the warning is logged.

### Full scan behavior by ATS platform type

#### SORTED platforms (Greenhouse, Lever, Ashby, Eightfold)

Jobs returned newest-first. Full scan uses the 2-consecutive-page early exit:

```python
OVERLAP_THRESHOLD = 0.80   # 80% of page already seen
CONFIRM_PAGES     = 2      # 2 consecutive high-overlap pages to stop
```

Even though this is full scan, the early exit is still valid here — if 80%+ of two consecutive pages are already seen, all new jobs have been found.

#### NON-SORTED platforms (Workday, iCIMS, custom ATS)

Jobs not in reliable order. **No early exit. No time-based cutoff.** Full scan goes all pages unconditionally:

```python
def full_scan_unsorted(company):
    page = 0
    while True:
        jobs = fetch_listing_page(company, page)
        if not jobs:
            break   # genuine end of results only
        
        process_page(jobs, company)   # Bloom filter + DB upsert
        page += 1
        
        # Check for pause signal between pages
        if redis.pubsub_check("pipeline:pause"):
            write_checkpoint(company, page)
            requeue_with_score(company, time.time() - 1)
            return
```

This is the "bulletproof" guarantee — non-sorted platforms cannot use smart early exit, so full scan must go the distance.

### Bloom filter per company

Each company has its own Bloom filter in Redis representing the **complete state of the job board as of the last full scan cycle**:

```text
Key:  bloom:fullscan:{company}
TTL:  36 hours (24h cycle + 12h buffer for timing drift)
Size: ~0.5MB per company at 0.1% error rate
```

The Bloom filter serves two purposes:
1. **Full scan** — skip DB checks for already-known jobs (performance speedup)
2. **Adaptive scan** — page-level early exit signal (see Section 11)

Full scan flow with Bloom filter:

```text
1. Check if OLD bloom:fullscan:{company} exists
   NO  → cold start, no reference — process everything, build NEW_BLOOM from scratch
   YES → use OLD bloom as "already seen" reference for DB check optimisation

2. Initialise NEW_BLOOM = empty (built fresh this cycle)
   DEL adaptive_seen:{company}     ← wipe today's adaptive scan history

3. For each page:
   For each job ID on the page:
     Already in NEW_BLOOM?   YES → pagination overlap → skip entirely
     In OLD bloom?           YES → known job, skip DB check (speedup)
                                   ADD to NEW_BLOOM
                             NO  → DB check → not in DB? queue for detail fetch
                                   ADD to NEW_BLOOM
   (ALL currently fetched jobs go into NEW_BLOOM, regardless of old/new status)

4. Full scan completes:
   SET bloom:fullscan:{company} = NEW_BLOOM
   EXPIRE bloom:fullscan:{company} 36h
```

**Why ALL jobs go into NEW_BLOOM (not just new ones):**
The naive approach (only add jobs not in old bloom to NEW_BLOOM) causes oscillation: cycle 2's bloom only contains jobs that were new in cycle 1, so cycle 3 re-processes all old jobs as if they were new. Building NEW_BLOOM from every currently active job means closed or filled positions fall out naturally — they are no longer fetched, so they never enter NEW_BLOOM, and are absent from the next cycle's reference.

**False positives (0.1% error rate):** 1 in 1000 new jobs incorrectly skipped by the old bloom. Since adaptive polling runs throughout the day, the skipped job was almost certainly already detected by Tier 1. Worst case: one full-scan cycle delay on 1 in 1000 new jobs — acceptable.

**Fallback if RedisBloom unavailable:** Use Redis SET at key `bloom:fallback:{company}`. Less memory efficient (~100MB vs ~4.5MB at scale) but functionally identical.

### Graceful termination during full scan

Full scan workers listen to Redis Pub/Sub for pause signals:

```
pipeline:pause  → worker finishes current page, commits, writes checkpoint, requeues
pipeline:resume → worker picks up from checkpoint page
```

Checkpoint columns on `company_poll_stats`:
- `full_scan_interrupted` — was the last full scan paused mid-way?
- `interrupted_at_page` — which page to resume from
- `interrupted_at` — when the pause happened

On requeue after pause:
```python
redis.zadd("poll:fullscan", {company.name: time.time() - 1})
# score = past timestamp → immediately due when workers resume
```

### Dynamic worker scaling

The system manages two independent but co-scheduled worker pools as child processes of the scheduler (`multiprocessing.Process`). Using real processes (not threads) gives full fault isolation and true parallelism — a crashed worker cannot affect the scheduler or other workers. The process manager (systemd) only needs to keep the scheduler alive; the scheduler manages everything else.

`MONITOR_MAX_WORKERS` in `config.py` is a **cold-start fallback ceiling only** — used on day 1 before `api_health` has any historical data. Once averages are available it is never consulted.

#### Startup — calculated at cycle start, not minimal

At `record_cycle_start()` (7 AM, after digest is sent), both pools start at their data-driven counts immediately. Starting at a floor and scaling up wastes the first 30–60 minutes doing catch-up work that was entirely predictable.

```python
window_s = 23 * 3600   # 7 AM to 6 AM next day

# Scan worker pool — based on total polling workload today
scan_polls_today      = sum(window_s / company.interval_s for company in active_companies)
avg_listing_scan_s    = get_30day_avg_listing_duration()   # from api_health
scan_workers_needed   = ceil((scan_polls_today * avg_listing_scan_s) / window_s)

# Detail worker pool — based on expected new-job volume today
expected_new_jobs     = get_30day_avg_daily_new_jobs()     # from adaptive_poll_metrics
avg_detail_fetch_s    = get_30day_avg_detail_duration()    # from api_health
detail_workers_needed = ceil((expected_new_jobs * avg_detail_fetch_s) / window_s)

# Hard ceiling: both pools share the DB connection pool
hard_ceil_combined = DB_POOL_MAXCONN - 3     # 3 reserved for scheduler + maintenance
scan_ceil   = floor(hard_ceil_combined * 0.6)
detail_ceil = floor(hard_ceil_combined * 0.4)

scan_workers_start   = min(scan_workers_needed,   scan_ceil,   MONITOR_MAX_WORKERS)
detail_workers_start = min(detail_workers_needed, detail_ceil, MONITOR_MAX_WORKERS)
```

Fallback when <7 days of `api_health` data: use `MONITOR_MAX_WORKERS` as the startup count for both pools.

#### DB connection pool — combined ceiling, not independent

Scan workers and detail workers share the same PostgreSQL connection pool. The ceiling must apply to their **combined** total. The 60/40 split is a starting point; the slow check recalculates the ratio based on observed queue drain rates each cycle.

#### Three monitoring layers

**Layer 1 — Liveness check (every scheduler tick, ~5 seconds)**

```python
for proc in all_worker_processes:
    if not proc.is_alive():
        spawn_replacement(proc.worker_type)   # immediate, no delay
```

Dead workers are replaced on the next tick — not at the next 5-minute or 30-minute check.

For dead fullscan workers, the liveness check also releases the inflight slot the worker was
holding so future dispatches are not incorrectly throttled:

```python
# After spawning replacement, for each dead fullscan worker:
raw = r.get(f"worker:current_job:fullscan:{old_pid}")
if raw:
    company, dc_key = raw.split("|", 1)
    r.zrem(f"inflight:fullscans:{dc_key}", company)   # release slot
r.delete(f"worker:current_job:fullscan:{old_pid}")    # clean up
```

This runs event-driven within ~5s of the crash. If the worker died before it wrote the
`worker:current_job:fullscan:{pid}` key, the slot remains until the 2-h stale prune on
the next fullscan dispatch for that DC key.

**Layer 2 — Fast error check (every 5 minutes)**

Reacts to error spikes that the semaphore alone cannot resolve (semaphore already at floor):

```python
for platform in active_platforms:
    error_rate   = get_error_rate(r, platform)          # errwin sliding window
    semaphore_at_floor = get_limit(r, platform) <= CONCURRENCY_FLOOR[platform]

    if error_rate > CONCURRENCY_ERROR_RATE_REDUCE and semaphore_at_floor:
        # Semaphore can't go lower — workers themselves are the variable
        reduce_scan_workers(by=1)
        deprioritise_platform_in_queue(platform)        # push its companies back
        # Do NOT reduce below floor=2 or remaining_work_minimum, whichever is higher
```

Worker reduction is **platform-aware**: a Workday spike deprioritises Workday companies in `poll:adaptive` (pushes their scores forward by 300s) rather than killing workers that Greenhouse/Lever/Ashby companies still need.

**Layer 3 — Slow throughput check (every 30 minutes)**

```python
# Recalculate ceiling from current state (handles mid-cycle company additions)
scan_ceil   = floor((DB_POOL_MAXCONN - 3 - current_detail_workers) * 0.6)
detail_ceil = floor((DB_POOL_MAXCONN - 3 - current_scan_workers)   * 0.4)

# Scan worker adjustment
if scan_queue_growing for 2 consecutive checks:
    if detail_queue_depth < DETAIL_QUEUE_HIGH_WATERMARK:
        add_scan_worker()       # production can increase safely
    else:
        add_detail_worker()     # drain backlog first, don't add more production

if scan_queue_empty for 2 consecutive checks:
    remove_scan_worker(floor=max(2, remaining_work_minimum))

# Detail worker adjustment
if detail_queue_growing for 2 consecutive checks:
    add_detail_worker()

if detail_queue_empty for 2 consecutive checks:
    remove_detail_worker(floor=2)

# Cascade: detail ceiling reached AND queue still growing → slow production
if detail_workers == detail_ceil and detail_queue_still_growing for 2 checks:
    remove_scan_worker()        # reduce production to match consumption capacity
```

The **2-consecutive-checks hysteresis** on every add/remove decision prevents thrashing — a single noisy measurement never triggers an action.

`remaining_work_minimum` is recalculated on every slow check:
```python
remaining_companies = companies not yet polled this cycle
remaining_window_s  = seconds until 6 AM
remaining_work_minimum = ceil((remaining_companies * avg_listing_scan_s) / remaining_window_s)
```
The floor shrinks naturally as the day progresses and work completes.

#### Pace mismatch — scan workers producing faster than detail workers consume

One listing scan (~3s) can produce N new jobs for the detail queue in the time a detail worker processes one job (~2s). Left uncoordinated, the detail queue grows permanently.

The full cascade:
```
detail queue growing             → add detail worker (first response)
detail workers at ceil           → do not add scan workers (stop increasing production)
detail at ceil + queue growing × 2 checks → remove scan worker (slow production)
errors subside, queues balanced  → both pools recover via slow check
```

The hard backpressure threshold (`DETAIL_QUEUE_MAX_ADAPTIVE = 5000`) remains as the **emergency brake** — listing scans are delayed 30s when the queue exceeds this depth. Normal operation never reaches it because the cascade above intervenes earlier.

#### Graceful shutdown and mid-cycle worker removal

**Scheduler shutdown (SIGTERM)**

Each worker process receives a `multiprocessing.Event` at spawn. On full scheduler shutdown, all events are set simultaneously. Workers complete their current scan/detail fetch, flush `api_health`, and exit cleanly. Forced SIGKILL after `WORKER_SHUTDOWN_TIMEOUT_S` (30s) for stragglers.

**Error-triggered worker removal (mid-cycle)**

When `_fast_error_check_loop` removes a worker due to platform errors, that worker may be mid-scan when the shutdown event fires. With the Stream delivery model, the company's message remains in the PEL — it will be re-delivered by `XAUTOCLAIM` to another worker after the idle timeout. No explicit re-queuing logic is needed for the "removed cleanly before scan starts" case.

However, for the worker that is **already mid-scan** when shutdown fires: the worker should re-queue with exponential backoff (not rely on `XAUTOCLAIM`) so the delay is proportional to the ATS error state rather than a fixed idle timeout.

The worker checks the shutdown event at each **page boundary** inside `_run_listing_scan()`. If set: partial results are discarded, the company is explicitly re-queued in `poll:adaptive` with backoff, and the message is `XACK`'d (so `XAUTOCLAIM` does not re-deliver the message after recovery — the ZADD to `poll:adaptive` already handles rescheduling):

```python
# Inside _run_listing_scan(), at each page boundary:
if shutdown_event.is_set():
    delay = _get_backoff_delay(r, company, op_type="scan")
    r.zadd(REDIS_POLL_ADAPTIVE, {company: time.time() + delay})
    r.xack(stream_key, group, msg_id)   # ACK so XAUTOCLAIM doesn't double-deliver
    logger.info("scan_worker: shutdown re-queue %r with +%ds backoff", company, delay)
    return
```

Partial results are never committed — a full clean re-scan on recovery is safer than a partial write to the DB.

See Section 18 (Exponential Backoff) for the backoff formula and Redis key structure.

---

#### Per-DC dispatch throttling — platform-isolated load control

Worker removal affects all platforms equally since workers are not platform-specific. Reducing total worker count to protect Workday also slows Greenhouse/Lever dispatching — unnecessary when those platforms are healthy.

The correct isolation: track **in-flight scans per platform/DC** and throttle at dispatch time, not at worker-count level.

**In-flight tracking — two ZSET keys (one per scan type)**

Adaptive and fullscan workers share the same learned ceiling per DC key but use **separate ZSET keys** to track their inflight slots. This is necessary because the two scan types have very different durations — a 10-minute stale window that prunes abandoned adaptive slots would prune active fullscan slots mid-scan (full scans take 20–30 min).

```text
inflight:scans:{dc_key}      — adaptive scan inflight  (stale window: INFLIGHT_STALE_WINDOW_S = 10 min)
inflight:fullscans:{dc_key}  — fullscan inflight        (stale window: INFLIGHT_FULLSCAN_STALE_S  = 2 h)
```

Both counts are summed before comparing to the ceiling. Neither scan type can proceed if their combined total hits the limit.

**Adaptive dispatch throttle check**

```python
ceiling_raw = r.get(f"worker:ceil:learned:{dc_key}")
if ceiling_raw:
    inflight_key = f"inflight:scans:{dc_key}"
    r.zremrangebyscore(inflight_key, 0, now - INFLIGHT_STALE_WINDOW_S)  # prune stale adaptive
    # inflight:fullscans:{dc_key} is NOT pruned here: fullscan slots have a
    # 2 h stale window (INFLIGHT_FULLSCAN_STALE_S) so pruning with the 10 min
    # adaptive window would remove active fullscan slots mid-scan.  Stale
    # fullscan entries are pruned atomically inside the Lua dispatch script.
    pending_count = (
        r.zcard(inflight_key)
        + r.zcard(f"inflight:fullscans:{dc_key}")   # fullscan slots count toward ceiling too
    )
    if pending_count >= int(ceiling_raw):
        r.zadd(REDIS_POLL_ADAPTIVE, {company: now + 30})
        continue
```

**Fullscan dispatch throttle check (atomic Lua)**

The fullscan dispatcher uses a Lua script so the check + ZADD is atomic — no two dispatches can both read "slot available" and both proceed in the same loop iteration.

```lua
-- KEYS[1] = inflight:scans:{dc_key}      KEYS[2] = inflight:fullscans:{dc_key}
-- ARGV[1] = ceiling   ARGV[2] = adaptive stale cutoff   ARGV[3] = fullscan stale cutoff
-- ARGV[4] = company   ARGV[5] = now
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[2])   -- prune stale adaptive entries
redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, ARGV[3])   -- prune stale fullscan entries
local total = redis.call('ZCARD', KEYS[1]) + redis.call('ZCARD', KEYS[2])
if total >= tonumber(ARGV[1]) then
    return 0   -- throttled
end
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[4])   -- claim slot
return 1   -- ok
```

If `_claimed == 0`: company re-queued to `poll:fullscan` at `+30s` and skipped. The slot claimed here is the authoritative reservation — `_run_fullscan()` does not ZADD again; it only writes `worker:current_job:fullscan:{pid}` for crash-recovery bookkeeping and releases the slot in its finally block.

**Why not use the Stream PEL for inflight tracking (historical evaluation):**

The Stream PEL was evaluated and rejected in favour of the ZSET approach. ZSET with stale-window pruning (`ZREMRANGEBYSCORE`) is the authoritative inflight count; `_get_stream_pending_count()` queries `XPENDING` for monitoring only, not for ceiling checks.

| Property | Stream PEL | ZSET with stale window (current approach) |
|---|---|---|
| Fullscan + adaptive combined count | Two separate streams → can't combine easily | Two ZSETs → simple ZCARD sum |
| Atomic check + claim | Requires Lua anyway; PEL written by XADD not by the check | Lua can ZREMRANGEBYSCORE + ZCARD + ZADD atomically |
| Stale window per type | PEL uses XAUTOCLAIM timeout (same for all messages) | Independent per-type stale windows |

A Workday DC12 ceiling has **zero effect** on Greenhouse or Lever dispatching. Each DC key has its own set of inflight ZSETs — fully independent.

**Learned ceiling — empirical discovery**

The safe in-flight ceiling for each DC is discovered by observing what caused errors and stored permanently in Redis:

```
worker:ceil:learned:{dc_key}      # max safe concurrent scans (int, no TTL)
worker:ceil:last_error:{dc_key}   # Unix ts of last error-triggered event
```

Set when `_fast_error_check_loop` triggers a reduction for that platform. The in-flight count at the moment errors peaked = ceiling + 1, so:

```python
ceiling = max(WORKER_FLOOR, current_inflight - 1)
r.set(f"worker:ceil:learned:{dc_key}", ceiling)
r.set(f"worker:ceil:last_error:{dc_key}", now)
```

**If no ceiling key exists for a platform**: that platform has never triggered error-based throttling. The only limit is server capacity (`DB_POOL_MAXCONN - 3`). Ceilings are never pre-configured — only discovered.

**Decay — gradual capacity re-testing**

Learned ceilings never expire (ATS capacity changes slowly) but inch upward after sustained clean operation. Checked in `_slow_throughput_check_loop` every 30 minutes:

```python
last_error = float(r.get(f"worker:ceil:last_error:{dc_key}") or 0)
if now - last_error > 86400:   # 24h without a new error event
    new_ceil = min(current_ceil + 1, MONITOR_MAX_WORKERS)
    r.set(f"worker:ceil:learned:{dc_key}", new_ceil)
    logger.info("ceil:learned relaxed: %r → %d", dc_key, new_ceil)
```

The ceiling probes upward +1 per 24h clean window. If the higher count triggers errors, it drops back immediately and the decay clock resets. The system converges on the true safe maximum without manual tuning.

The ceiling is clamped: `max(WORKER_FLOOR, ceiling)` — never stored below the redundancy minimum.

**Scaling lock — preventing fast/slow loop conflicts**

`_fast_error_check_loop` (5 min) and `_slow_throughput_check_loop` (30 min) run on independent timers and can conflict: fast check reduces workers due to errors while slow check simultaneously sees a backlog and adds them back.

After any error-triggered action, the fast check sets:
```
worker:scaling_lock:{platform}    TTL = WORKER_SLOW_CHECK_INTERVAL_S (30 min)
```
The slow throughput check skips scale-up for any platform with an active scaling lock, preventing it from immediately undoing a deliberate error-driven reduction.

---

#### ATS outage detection

Worker reduction resolves concurrency-induced errors. But if the ATS is experiencing an outage, no amount of reduction helps — errors persist regardless of request volume. Continuing to reduce workers starves other healthy platforms unnecessarily.

**Detection: consecutive ineffective reductions**

Before every worker reduction, snapshot the current state:
```
worker:reduction:before_rate:{platform}    # error_rate at time of reduction
worker:reduction:ts:{platform}             # timestamp of reduction
```

At the next fast check (5 min later): if `current_error_rate < CONCURRENCY_ERROR_RATE_REDUCE`, the reduction worked — record the learned ceiling, reset the counter. If still high, increment:
```
worker:consec_reductions:{platform}    TTL = 3600 (auto-clears after 1h)
```

At `consec_reductions >= 3`: three reductions, errors still not improving. This is an ATS-level outage.

**Outage mode — platform pause, workers untouched**

```python
r.set(f"worker:outage:{platform}", "1", ex=3600)   # 60-min dispatch pause
```

In `adaptive_loop`: companies for a platform in outage mode are skipped and pushed forward by the remaining outage TTL. Workers are **not** reduced further — they continue serving all other healthy platforms at full capacity.

**Canary probe — early recovery detection**

At 30 minutes into the outage window, one canary request is sent for that platform:
- **Success** → exit outage mode immediately (delete the flag), resume normal dispatch
- **Failure** → reset the TTL to 3600s (full 60-min extension), increase outage backoff

This prevents a full 60-min blackout when the ATS actually recovered after 20 minutes.

**Scheduler restart during outage**

`worker:outage:{platform}` is a TTL key — a restart mid-outage inherits the remaining window from Redis automatically. `calculate_worker_counts()` excludes companies from platforms in outage mode when estimating today's workload, so over-spawning idle workers is avoided.

---

## 10. Dormant Company Reactivation

### The problem

A company that has been silent for 3 months (score ≈ 0, interval = 24 hours) suddenly starts posting jobs. How quickly does the system respond?

### How reactivation works with asymmetric smoothing

Because we use asymmetric smoothing (no dampening when interval is dropping), reactivation is **immediate** rather than gradual.

> **Note:** The walkthrough below uses `DEFAULT_THRESHOLDS` (`low=1.5, moderate=3.5, active=6.0`) for illustration. In production, `band_lookup` uses the daily-calibrated thresholds from the portfolio distribution (Section 6). The exact interval values will differ, but the asymmetric smoothing behaviour — immediate drop on reactivation — is identical regardless of which thresholds are in use.

```
State: dormant company, 3 months silent
  recent_poll_counts = [0, 0, 0, 0, 0]
  weighted_avg       = 0.0
  current_interval   = 24h (at MAX_INTERVAL cap)

Company posts 5 new jobs.

POLL 1 (after reactivation):
  new_jobs_found = 5  (capped at 10 per poll)
  Queue update: [0, 0, 0, 0, 5]
  weighted_avg = 0×0.10 + 0×0.15 + 0×0.20 + 0×0.25 + 5×0.30 = 1.50
  computed_interval = band_lookup(1.50) = 12h

  Asymmetric check: 12h < 24h (interval dropping) → react immediately
  new_interval = 12h   ← no smoothing applied
  MAX_INTERVAL for score=1.5 → 12h cap
  final_interval = 12h

  Next poll in 12 hours.

POLL 2 (12 hours later — 3 more new jobs):
  Queue: [0, 0, 0, 5, 3]
  weighted_avg = 0 + 0 + 0 + 5×0.25 + 3×0.30 = 2.15
  computed = 6h
  Asymmetric: 6h < 12h → immediate
  final_interval = 6h

POLL 3 (6 hours later — 2 more new jobs):
  Queue: [0, 0, 5, 3, 2]
  weighted_avg = 0 + 0 + 5×0.20 + 3×0.25 + 2×0.30 = 2.35
  computed = 6h
  MAX_INTERVAL for score=2.35 → 4h
  final_interval = 4h

POLL 4 (4 hours later — 2 more new jobs):
  Queue: [0, 5, 3, 2, 2]
  weighted_avg = 0 + 5×0.15 + 3×0.20 + 2×0.25 + 2×0.30 = 2.65
  final_interval = 4h (stable at active tier)
```

**Total time from first detection to 4-hour polling cycle: ~22 hours.**

Compare to symmetric EMA which takes 1–2 weeks. Asymmetric smoothing reacts in one day.

### The dormancy decay (going quiet after active)

```
Company that was active [3, 4, 2, 5, 4] → goes quiet

Poll: 0 new jobs → queue [4, 2, 5, 4, 0] → weighted 2.55 → 6h (smoothed: stays 4h→6h)
Poll: 0 new jobs → queue [2, 5, 4, 0, 0] → weighted 1.55 → 6h→12h (interval rising → smoothed)
Poll: 0 new jobs → queue [5, 4, 0, 0, 0] → weighted 0.95 → 12h (smoothed gradually)
Poll: 0 new jobs → queue [4, 0, 0, 0, 0] → weighted 0.40 → 24h
Poll: 0 new jobs → queue [0, 0, 0, 0, 0] → weighted 0.0  → 24h (MAX cap)
```

Graceful decay over ~5 polls. Never abrupt.

---

## 11. Smart Early Exit — Pagination Without Limits

### The problem with static page limits

For large companies like Starbucks or Amazon (20,000+ job listings), we cannot fetch all 200+ pages every adaptive poll. But we also cannot arbitrarily cap at, say, "50 pages" — what if all the new jobs are on pages 51–60?

**Hardcoded limits are wrong because:**
- Too low → miss new jobs
- Too high → waste time fetching old jobs

The right answer is: **stop when you've clearly passed the zone of new jobs.**

### How most ATS platforms sort their results

| Platform | Default sort | Can rely on? |
|----------|-------------|--------------|
| Greenhouse | newest first | ✓ Yes |
| Lever | newest first | ✓ Yes |
| Ashby | newest first | ✓ Yes |
| Eightfold (Amazon, Starbucks) | newest first | ✓ Yes |
| SmartRecruiters | newest first | ✓ Yes |
| Workday | tenant-dependent | ✗ Unreliable |
| iCIMS | no guaranteed order | ✗ No |
| SuccessFactors | single XML dump | N/A (no pages) |
| ADP | single dump | N/A (no pages) |

For platforms with reliable newest-first ordering, we can detect the "old job frontier" and stop paginating as soon as we pass it.

### The algorithm (SORTED platforms only)

The early exit check compares page job IDs against `bloom:fullscan:{company}` — the Bloom filter built during the last full scan. This represents the complete state of the board at that point. If most jobs on a page are already in the Bloom filter, we are past the zone of new postings.

```python
def smart_paginate(company, ats):
    page              = 0
    overlap_pages     = 0
    OVERLAP_THRESHOLD = 0.80   # 80% of a page is in the Bloom filter
    CONFIRM_PAGES     = 2      # need 2 consecutive high-overlap pages to stop

    while True:
        jobs = fetch_listing_page(company, ats, page)
        if not jobs:
            break

        bloom_hits    = sum(1 for j in jobs if bf_exists("bloom:fullscan:{company}", j.job_id))
        overlap_ratio = bloom_hits / len(jobs)

        if overlap_ratio >= OVERLAP_THRESHOLD:
            overlap_pages += 1
            if overlap_pages >= CONFIRM_PAGES:
                break   # 2 consecutive pages 80%+ in Bloom filter — past the new-job frontier
        else:
            overlap_pages = 0   # found new jobs → reset, keep going

        page += 1
```

**Why the Bloom filter instead of `seen:{company}` SET:**
The old `seen:{company}` SET only contained jobs that passed keyword/location filters — typically 5–10% of all jobs on the board. An 80% overlap threshold against a 5–10% populated set never triggered, so early exit was effectively broken. The Bloom filter contains **all** jobs from the last full scan (100% of the board), making the threshold meaningful.

**Between full scan cycles:** New jobs found by adaptive scan are not written to the Bloom filter — it only reflects the state at the last full scan. This means the overlap ratio slightly understates how much is already known, and early exit may trigger one page later than strictly necessary. This is acceptable and conservative — it is better to fetch one extra page than to stop too early.

### NON-SORTED platforms — no early exit in adaptive polling

For Workday, iCIMS, and custom ATS with no reliable ordering:

- **Adaptive polling**: scan pages until a time-based cutoff (e.g., skip jobs with `postedOn` older than 72 hours). Bloom filter early exit is NOT used — jobs can appear in any order so overlap on one page does not indicate we have passed the new-job frontier.
- **Full scan**: scan ALL pages unconditionally — no cutoff, no early exit.

This is why full scan exists. Adaptive polling's time-based cutoff on non-sorted platforms means it might miss a new job posted at an unusual position in the listing. Full scan eliminates that risk entirely.

### Why 80% threshold and 2 pages?

**80%:** At the exact frontier, pages are mixed (some new, some old). We want to be clearly past the frontier before stopping — 80% threshold provides a safety buffer.

**2 pages:** Companies occasionally "re-bump" (update) old job postings, making them appear near the top even though they're already in our database. Requiring 2 consecutive high-overlap pages prevents false exits from bumping events.

---

## 12. Incremental Filtering — The Core Efficiency Gain

### What it means

"Incremental" means: only process the **difference** between what you saw last time and what you see now.

Today the pipeline fetches 146,497 jobs and processes all of them to find 157 new ones. In the new architecture, it fetches listing metadata, compares against known job IDs, and only processes the ~157 genuinely new ones.

### How adaptive polling identifies new jobs

For each job ID on a fetched page, adaptive polling checks two layers in order:

```text
1. adaptive_seen:{company} SET (Redis)
   → Job ID present? Already processed in an earlier adaptive scan today → skip entirely
   → Not present?    Proceed to layer 2, then add to SET regardless of outcome

2. PostgreSQL job_postings (source of truth)
   → Job ID found in DB?     Already stored → skip detail fetch
   → Job ID not in DB?       Genuinely new → queue for detail fetch
```

**Why `adaptive_seen` instead of a Redis `seen:{company}` SET:**
The old design maintained a `seen:{company}` SET rebuilt from `job_postings` on every Redis restart. This was large (~100MB at scale), required a full table scan to rebuild, and grew unbounded. `adaptive_seen:{company}` is scoped to the current day only (24h TTL, DEL'd at full scan start), keeping it small and self-cleaning. The DB remains the authoritative source; `adaptive_seen` is purely a same-day lookup cache to avoid redundant DB round-trips.

**`adaptive_seen` lifecycle:**
- Written by adaptive scan — ALL fetched job IDs added regardless of filter or DB outcome
- TTL: 24 hours (fixed)
- DEL'd explicitly at the start of each full scan
- Never written by full scan

**Source of truth:** `job_postings` table (`job_id` + `company` columns). PostgreSQL is always consulted for any job ID not already in `adaptive_seen`. No separate seen-IDs table is needed.

The `job_postings` table has a `last_polled` column updated every time a job appears in a listing fetch. This tracks which jobs are still actively on the ATS.

### The updated_at delta

Simply checking "is this ID already in seen_ids?" handles new jobs, but it doesn't handle **updated jobs** — jobs that were first posted 3 weeks ago but have since had their requirements or salary updated.

When we see a job we've already processed, we check:

```python
if job.updated_at > seen_ids[job_id].last_updated + REFRESH_WINDOW:
    enqueue_detail_fetch(job)   # re-fetch the detail page
```

| Platform | REFRESH_WINDOW |
|----------|---------------|
| Greenhouse | 72 hours |
| Lever | 72 hours |
| Ashby | 48 hours |
| Workday | 24 hours |
| iCIMS | N/A |

---

## 13. Deduplication Strategy

### The four layers

Deduplication operates at four independent layers, each catching what the layer above it missed:

```text
Layer 1: In-cycle Python set (per scan run)
  → Catches duplicate job IDs within a single page sequence (pagination overlap)
  → Plain Python set(), lives only for the duration of one scan function call
  → cycle_seen = set(); if job_id in cycle_seen: skip

Layer 2a: Bloom filter — full scan cross-cycle dedup
  → bloom:fullscan:{company}, 36h TTL, 0.1% false positive rate
  → Full scan checks: if job_id IN old bloom → skip DB check (speedup)
  → Bloom filter is rebuilt from scratch each cycle (ALL active jobs added)
  → Closed/filled jobs fall out naturally — not fetched → not in NEW_BLOOM

Layer 2b: adaptive_seen:{company} SET — same-day adaptive dedup
  → Covers job IDs already processed by an earlier adaptive scan today
  → If job_id IN adaptive_seen → skip entirely (no DB check)
  → 24h TTL, DEL'd at full scan start, written only by adaptive scan

Layer 3: Database unique constraint
  → UNIQUE(company, job_id) on job_postings
  → Upsert pattern — any duplicate that reaches this layer is a safe no-op
  → Final correctness guarantee — cannot be bypassed
```

### Bloom filter role — full scan speedup AND adaptive early exit

The Bloom filter (`bloom:fullscan:{company}`) has two distinct uses at different points in the cycle:

**During full scan (speedup):** For each job ID fetched, if the ID is already in the old bloom (the previous cycle's complete board state), skip the DB check. Only job IDs absent from the old bloom trigger a DB lookup. This eliminates ~99.9% of DB round-trips during full scan.

**During adaptive scan (early exit):** If 80%+ of job IDs on two consecutive pages are found in the Bloom filter, stop paginating. The board is mostly unchanged since the last full scan. See Section 11 for the full algorithm.

The Bloom filter is **never used to decide whether a job needs detail fetching** — that decision always comes from the DB (layer 3). The Bloom filter only decides: skip DB check (full scan speedup) or stop paginating (adaptive early exit).

### Memory comparison

| Approach | Memory at 2.5M job IDs |
|---|---|
| PostgreSQL job_postings (existing table) | no extra cost — already present |
| Redis SET seen:{company} (old design, removed) | ~100MB RAM |
| Redis Bloom filter bloom:fullscan:{company} | ~4.5MB RAM |
| adaptive_seen:{company} SET (per-day, ~hundreds of IDs) | negligible |

### Bloom filter false positives

0.1% error rate = 1 in 1000 new jobs whose DB check is incorrectly skipped during full scan (the old bloom says "seen" when the job is actually new).

This is acceptable because:
1. Adaptive polling runs throughout the day — the job was very likely already detected by Tier 1
2. The next full scan cycle rebuilds the bloom from scratch — the false positive is absent from the new bloom and the job is processed correctly
3. Maximum delay: one full-scan cycle (≤24 hours) on 1 in 1000 new jobs

### Adaptive-first guarantee

Full scan only runs after at least one adaptive poll has completed for that company (see Section 7). This ensures that when a false positive occurs, adaptive polling has already had the opportunity to detect the same job.

---

## 14. The first_published vs updated_at Problem

### Why this matters for freshness

Consider how Greenhouse works: when a company edits a job posting (updates the salary range, changes the required skills, fixes a typo), the `updated_at` field changes to the current time. But the job was originally posted months ago.

If we naively use `updated_at` to determine "is this job recent?", every edited job would appear as if it were just posted today — flooding your digest with months-old jobs every time a recruiter makes a minor edit.

### The updated_at signal is still valuable — just differently

Even though `updated_at` is unreliable for "was this job just posted?", it is useful for detecting active hiring activity. A job with `updated_at` within the last 72 hours is being actively maintained — the role is not yet filled and the hiring team is engaged.

### How we handle each platform

| Platform | Date field for freshness | Meaning |
|----------|-------------------------|---------|
| Greenhouse | `first_published` | Original post date — never changes |
| Lever | `createdAt` | Original creation date — never changes |
| Ashby | `publishedAt` | Original publish date — never changes |
| Workday | `postedOn` | Posting date string — reliable |
| Oracle HCM | `PostedDate` | Original posting date — reliable |
| iCIMS | parsed from page text | "Posted Date 3 days ago (05/03/2026)" |
| SuccessFactors | `Posted-Date` | Original posting date in XML |
| ADP | `postingDate` | When available — often None |

### The dual-timestamp approach

```sql
first_published  DATE       -- When the job was originally posted (never changes)
last_updated     TIMESTAMP  -- When we last saw a change to this job's data
```

`first_published` determines **freshness** — whether to show it in the digest as "new."
`last_updated` determines **activity** — whether the role is still being actively managed.

---

## 15. Redis — Why and What For

### Why Redis

The architecture has:
- A scheduler needing sub-millisecond queue operations
- Multiple workers checking seen_ids simultaneously
- Rate limiters enforced atomically across all workers
- Detail queues multiple workers consume concurrently

PostgreSQL can handle this but introduces latency and contention. Redis is optimized for exactly this: **fast, atomic, in-memory operations shared across multiple processes.**

### Redis data structures

#### 1. Adaptive Poll Queue (Sorted Set)

```
Key: "poll:adaptive"
Type: Sorted Set (ZSET)
Score: Unix timestamp of next adaptive poll time

ZADD poll:adaptive {timestamp} {company}               — schedule adaptive poll
ZRANGEBYSCORE poll:adaptive -inf {now} LIMIT 0 50      — dispatcher reads due companies
ZREM poll:adaptive {company}                           — dispatcher removes after XADD
```

#### 1b. Adaptive Delivery Stream (Redis Stream per DC key)

```text
Key: "stream:adaptive:{dc_key}"   e.g. stream:adaptive:workday_wd1
Type: Stream (consumer groups, PEL)

XADD stream:adaptive:{dc_key} MAXLEN ~ 1000 * company {name} due_at {ts}  — dispatcher enqueues (capped)
XGROUP CREATE stream:adaptive:{dc_key} scan-workers $ MKSTREAM             — worker startup; id=$ = new only
  (BUSYGROUP error → group exists → ignore; do NOT use id=0 → re-delivers all history)
consumer = f"worker-{hostname}-{pid}"                                       — unique per process
XREADGROUP GROUP scan-workers {consumer} COUNT 1 BLOCK 500                 — 500ms block; reacts to pause signal
  STREAMS stream:adaptive:{dc_key} >
XACK stream:adaptive:{dc_key} scan-workers {msg_id}                        — after on_adaptive_complete
XAUTOCLAIM stream:adaptive:{dc_key} scan-workers {consumer}
  MIN-IDLE-TIME {p95_listing_scan_ms * 3}                                  — self-calibrating, not hardcoded
XPENDING stream:adaptive:{dc_key} scan-workers                             — inflight count (ceiling check)
```

#### 2. Full Scan Queue (Sorted Set + Stream)

```text
Key: "poll:fullscan"
Type: Sorted Set (ZSET)
Score: Unix timestamp of next full scan time
       (score-ascending = dormant companies get lower scores = polled first)

ZADD poll:fullscan {timestamp} {company}               — schedule full scan
ZRANGEBYSCORE poll:fullscan -inf {now} LIMIT 0 50      — dispatcher reads due companies
ZREM poll:fullscan {company}                           — dispatcher removes after XADD

Key: "stream:fullscan"  (single shared stream; dc_key is a message field, not part of the key)
Type: Stream (consumer groups, PEL)

XADD stream:fullscan MAXLEN ~ 500 * company {name} dc_key {dc_key}   — dispatcher enqueues (capped)
XGROUP CREATE stream:fullscan fullscan-workers $ MKSTREAM  — id=$ = new only; BUSYGROUP → ignore
consumer = f"worker-{hostname}-{pid}"                               — unique per process
XREADGROUP GROUP fullscan-workers {consumer} COUNT 1 BLOCK 500      — 500ms block; reacts to pause signal
  STREAMS stream:fullscan >
XACK stream:fullscan fullscan-workers {msg_id}
XAUTOCLAIM stream:fullscan fullscan-workers {consumer}
  MIN-IDLE-TIME {p95_full_scan_ms * 3}                              — self-calibrating; separate from listing scan p95

Key: "inflight:fullscans:{dc_key}"   e.g. inflight:fullscans:workday_wd1
Type: Sorted Set (ZSET)
Score: Unix timestamp when slot was claimed (dispatch time)
Member: company name

Written by fullscan_loop's Lua script at dispatch time (atomically with ceiling check).
ZREM'd by _run_fullscan() finally block on all exit paths.
ZREMRANGEBYSCORE prunes entries older than INFLIGHT_FULLSCAN_STALE_S (2 h) — backstop for
hard crashes where the finally block and _replace_dead_workers() both failed.
Stale window is 2h (not 10min like adaptive) because full scans run 20–30 min.

Key: "worker:current_job:fullscan:{pid}"   e.g. worker:current_job:fullscan:12345
Type: String
Value: "{company}|{dc_key}"   e.g. "Accenture|workday_wd1"
TTL: 3600s (1 h — safety backstop only)

Written by _run_fullscan() at scan start (after acquiring the lock).
DEL'd by _run_fullscan() finally block on all exit paths.
Read by _replace_dead_workers() when a fullscan worker is found dead — extracts company+dc_key,
ZREMs the inflight:fullscans:{dc_key} slot, then DELs this key.
If the worker died before _run_fullscan() reached the SET (e.g. died during XREADGROUP),
this key is never written and the slot waits for the 2-h stale prune.
```

#### 3. Detail Fetch Queues (Two-Tier Lists)

```text
Keys: "queue:detail:adaptive"  (high priority — from listing scan)
      "queue:detail:fullscan"  (low priority  — from full scan)
Type: List

LPUSH queue:detail:adaptive {job_json}    — push from adaptive listing scan
LPUSH queue:detail:fullscan {job_json}    — push from full scan
BRPOP queue:detail:adaptive 0.1           — workers drain adaptive first
BRPOP queue:detail:fullscan 5             — then fullscan queue
```

Backpressure: listing scan checks queue depth before pushing. If depth > MAX_QUEUE_DEPTH (5,000 for adaptive, 2,000 for fullscan), listing scan pauses until workers catch up.

#### 4. Per-Company Bloom Filters

```text
Key: "bloom:fullscan:{company}"
Type: RedisBloom (BF)
TTL: 36 hours (24h cycle + 12h timing buffer)
Error rate: 0.1%

BF.EXISTS bloom:fullscan:{company} {job_id}   — check (full scan speedup + adaptive early exit)
BF.ADD    bloom:fullscan:{company} {job_id}   — written by full scan only
```

Built fresh on every full scan cycle. Contains ALL job IDs currently active on the board (not just new ones). Closed/filled jobs fall out naturally — not fetched → not added to new filter.

Two uses:
- Full scan: skip DB check for jobs already in old bloom (~99.9% of jobs per cycle)
- Adaptive scan: page-level early exit when 80%+ of two consecutive pages are in the bloom

Fallback if RedisBloom unavailable: Redis SET at key `bloom:fallback:{company}`.

#### 5. Adaptive Seen Cache (Sets, one per company)

```text
Key: "adaptive_seen:{company}"
Type: Set
TTL: 24 hours (fixed)

SISMEMBER adaptive_seen:{company} {job_id}   — check if processed today (O(1))
SADD      adaptive_seen:{company} {job_id}   — mark as processed (written by adaptive scan only)
DEL       adaptive_seen:{company}            — cleared at start of each full scan
```

Same-day lookup cache for adaptive scans. Prevents redundant DB round-trips when the same job ID appears across multiple adaptive scans within a single day. Job IDs are added regardless of whether the job passed filters or was already in the DB — the only question answered is "did we touch this job ID today?"

#### 6. Rate Limiter (Strings with expiry)

```
Key: "rate:{domain}:{minute_bucket}"
Type: String (integer counter)

INCR rate:{domain}:{bucket}
EXPIRE rate:{domain}:{bucket} 120
```

#### 7. Per-ATS Sliding Window (429 + 404 tracking)

```
Key: "errwin:{platform}:{10min_bucket}"
Type: Hash with request counts and error counts
TTL: 20 minutes (two 10-min windows)

Used for dynamic concurrency adjustment (see Section 19)
```

#### 8. Worker Heartbeats (retired for scan workers)

Scan worker heartbeat keys (`heartbeat:{company}`) are retired. Crash recovery for adaptive and full scan workers is handled by the Stream PEL + `XAUTOCLAIM` — no separate heartbeat or watchdog process required.

Heartbeat keys are still used by **detail fetch workers** (which use BRPOP, not Streams) to detect stuck workers.

#### 9. Worker Progress (for hung worker detection)

```
Key: "progress:{company}"
Type: String with TTL

SET progress:{company} "fetching_page_5" EX 120   — updated after each meaningful step
If key expires while heartbeat still alive → worker frozen between requests → kill + requeue
```

#### 10. Pub/Sub Channels (Graceful Termination)

```
pipeline:pause   — nightly cron broadcasts before maintenance window
pipeline:resume  — nightly cron broadcasts after maintenance completes
cronchain:alive  — cron chain heartbeat, refreshed every step, TTL=300s
db:maintenance   — set when DB maintenance starts, cleared when complete (no auto-TTL)
```

#### 11. Full Scan Exclusive Lock (retired)

`fullscan:lock:{company}` NX key is retired. Double-dispatch is prevented by the Stream delivery model: a company in the `stream:fullscan` PEL (delivered but not ACK'd) is not re-added by the dispatcher because `ZREM poll:fullscan` already removed it from the scheduling ledger. `XAUTOCLAIM` re-delivers to a *different* worker on crash, not a duplicate second dispatch.

#### 12. Company Stats Cache (Hash)

```
Key: "stats:{company}"
Type: Hash
Fields: adaptive_score, current_interval_s, recent_poll_counts,
        consecutive_empty, last_job_found_at

Read after every poll for interval computation (~0.1ms vs ~5ms DB query)
Written through to PostgreSQL periodically for persistence
```

### Redis is not the source of truth

If Redis loses all its data, nothing is permanently lost. Every structure can be rebuilt from PostgreSQL:

| Redis structure | Rebuild source |
|---|---|
| poll:adaptive | `company_poll_stats.next_poll_at` |
| poll:fullscan | `company_poll_stats.next_full_scan_at` |
| queue:detail:* | `job_postings WHERE status='pending_detail'` |
| adaptive_seen:{company} | not rebuilt — 24h TTL, next adaptive scan repopulates naturally |
| bloom:fullscan:* | not rebuilt — next full scan is a cold start (correct but expensive) |
| stats:{company} | `company_poll_stats` table |

Rebuild takes ~30 seconds on startup. Redis is a **performance layer**, not a storage layer.

---

## 16. PostgreSQL Schema

### Tables

#### `company_poll_stats` — Adaptive engine state

```sql
CREATE TABLE company_poll_stats (
    id                      BIGSERIAL PRIMARY KEY,
    company                 TEXT NOT NULL UNIQUE,
    ats_platform            TEXT,

    -- Adaptive interval state
    current_interval_s      INTEGER NOT NULL DEFAULT 86400,
    next_poll_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    last_poll_at            TIMESTAMP,
    adaptive_score          REAL DEFAULT 0.0,
    recent_poll_counts      TEXT DEFAULT '[]',      -- JSON array of last 5 poll results
    consecutive_empty       INTEGER NOT NULL DEFAULT 0,
    total_polls             BIGINT NOT NULL DEFAULT 0,
    total_new_jobs          BIGINT NOT NULL DEFAULT 0,

    -- Full scan state
    next_full_scan_at       TIMESTAMP,
    last_full_scan_at       TIMESTAMP,
    full_scan_interval_s    INTEGER DEFAULT 86400,
    full_scan_deferred      BOOLEAN DEFAULT FALSE,
    full_scan_interrupted   BOOLEAN DEFAULT FALSE,
    interrupted_at_page     INTEGER,
    interrupted_at          TIMESTAMP,

    -- Health tracking
    consecutive_errors      INTEGER NOT NULL DEFAULT 0,
    last_error_at           TIMESTAMP,
    last_success_at         TIMESTAMP,

    -- Lifecycle phase (Section 24: NEW → WARMING → STABLE)
    warming_polls_remaining SMALLINT DEFAULT NULL,
    -- NULL  = STABLE (adaptive engine fully in control)
    -- 3,2,1 = WARMING (fixed 2h interval; decremented by on_adaptive_complete)
    -- 0     = transition tick (this poll completes WARMING; next is STABLE)
    -- Set to 3 by on_fullscan_complete() when first full scan finishes.

    initial_slot_offset_s   INTEGER DEFAULT NULL,
    -- slot_offset(batch_position) stored at registration time.
    -- Used for the slot during NEW + WARMING phases.
    -- Becomes irrelevant once STABLE (slot_offset(company_id) takes over).
    -- Survives restarts — no need to know batch_position again.

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_company_poll_stats_next     ON company_poll_stats (next_poll_at);
CREATE INDEX idx_company_poll_stats_fullscan ON company_poll_stats (next_full_scan_at);
```

#### `company_config` — Per-company overrides

```sql
CREATE TABLE company_config (
    id                  BIGSERIAL PRIMARY KEY,
    company             TEXT NOT NULL UNIQUE,

    -- Fetch behavior overrides
    sorted_by_recency   BOOLEAN,
    refresh_window_hr   INTEGER,

    -- Interval overrides
    min_interval        INTEGER,
    max_interval        INTEGER,
    force_interval      INTEGER,   -- NULL = adaptive

    -- Manual flags
    is_pinned           BOOLEAN DEFAULT FALSE,   -- never go dormant
    is_suspended        BOOLEAN DEFAULT FALSE,   -- skip in scheduler

    notes               TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
```

#### `job_postings` — Full job records (extended)

New columns added:

```sql
ALTER TABLE job_postings
    ADD COLUMN IF NOT EXISTS job_id          TEXT,
    ADD COLUMN IF NOT EXISTS ats_platform    TEXT,
    ADD COLUMN IF NOT EXISTS first_published DATE,
    ADD COLUMN IF NOT EXISTS last_updated    TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_polled     TIMESTAMP,
    -- Updated every time the job appears in a listing fetch.
    -- Source of truth for job existence checks (adaptive and full scan both query this).
    ADD COLUMN IF NOT EXISTS found_by        TEXT,
    -- 'tier1_adaptive'  → found by adaptive polling
    -- 'tier2_full_scan' → found only by full scan
    ADD COLUMN IF NOT EXISTS _country_code   CHAR(2);

-- Fast lookup: "have we seen this job ID for this company?"
-- This index is what makes the incremental diff fast at the DB layer.
CREATE INDEX IF NOT EXISTS idx_job_postings_company_jobid
    ON job_postings (company, job_id);
```

Status flow:
```
'pending_detail' → job ID pushed to detail queue, detail not yet fetched
'new'            → detail fetched, not yet included in digest
'digested'       → included in digest email
```

#### `api_health` — Request tracking with error sub-types and request context

```sql
-- Replace single requests_error column with four sub-types (Fix 1)
ALTER TABLE api_health
    ADD COLUMN IF NOT EXISTS requests_timeout   INTEGER DEFAULT 0,
    -- timeout → likely concurrency-induced (too many parallel requests)
    ADD COLUMN IF NOT EXISTS requests_conn_err  INTEGER DEFAULT 0,
    -- connection refused → network-level issue
    ADD COLUMN IF NOT EXISTS requests_5xx       INTEGER DEFAULT 0,
    -- server errors → soft rate limit or platform instability
    ADD COLUMN IF NOT EXISTS requests_other_err INTEGER DEFAULT 0;
    -- parse failures, unexpected responses
```

**Phase 10 — `context` column (baseline distortion prevention)**

When a company is in exponential backoff or a platform is in outage/canary mode, the requests made during that period are unusual by design — they are either test probes or heavily throttled retries, not representative of normal ATS behaviour. Including them in the 30-day baseline would drag the "normal" error rate upward, making the spike_factor insensitive to genuine concurrency spikes.

Fix: tag every request with its operational context at write time.

```sql
ALTER TABLE api_health
    ADD COLUMN IF NOT EXISTS context TEXT NOT NULL DEFAULT 'normal';
    -- 'normal'  → standard polling (counts toward baseline)
    -- 'backoff' → request made while company/platform is in exponential backoff
    -- 'canary'  → single test request sent during an outage window
```

`record_request()` in `db/api_health.py` accepts an optional `context` parameter (default `'normal'`). Callers:

| Call site | context value |
|---|---|
| Normal adaptive poll / detail fetch | `'normal'` (default — no change needed) |
| Request made while `retry:backoff:{op}:{company}` is set | `'backoff'` |
| Canary probe during `worker:outage:{platform}` | `'canary'` |

`query_30day_avg_error_rate()` and `query_30day_avg_response_ms()` both filter `WHERE context = 'normal'` so baselines are computed only from representative traffic. All contexts are still stored — the raw totals remain intact for full observability.

**Response time columns — already tracked**

`total_ms`, `max_response_ms`, and `avg_response_ms` are already present in the `api_health` schema and updated on every request (including errors and timeouts). `max_response_ms` uses `GREATEST()` to track the worst-case response across the day. `avg_response_ms` is recomputed as `total_ms / requests_made` after each write.

Note: timeouts contribute their full elapsed duration (e.g. 30 000 ms) to `total_ms` and `max_response_ms`. This is intentional for the worker-count estimator in `calculate_worker_counts()` — a timed-out worker was genuinely occupied for that duration. A separate `total_ms_ok` column (successful requests only) was considered for a clean ATS-speed baseline, but deferred until real data is available to confirm whether the distortion is material in practice.

---

#### `worker_scaling_events` — Worker scaling decision audit log

```sql
CREATE TABLE worker_scaling_events (
    id                    BIGSERIAL   PRIMARY KEY,
    event_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- What happened
    event_type            TEXT        NOT NULL,
    -- worker_add        : a worker process was spawned
    -- worker_remove     : a worker process was removed (error-triggered or excess)
    -- ceiling_learned   : learned ceiling updated for a DC key
    -- outage_start      : 3 consecutive ineffective reductions → outage declared
    -- canary_probe      : single test request sent during outage window
    -- outage_end        : error rate recovered; outage mode cleared

    -- Why it happened
    trigger_layer         TEXT,
    -- fast_error        : _fast_error_check_loop (5-min cycle)
    -- slow_throughput   : _slow_throughput_check_loop (30-min cycle)
    -- cascade           : detail queue full → scan worker removed
    -- liveness          : dead worker replaced

    -- Scope
    platform              TEXT,         -- e.g. "workday", "greenhouse"; NULL = multi-platform
    dc_key                TEXT,         -- e.g. "workday_wd12"; NULL when not DC-specific
    worker_type           TEXT,         -- "scan" | "detail"; NULL for outage/canary events

    -- Worker counts at the moment of the event
    scan_workers_before   INTEGER,
    scan_workers_after    INTEGER,
    detail_workers_before INTEGER,
    detail_workers_after  INTEGER,

    -- Health signals that triggered the decision
    error_rate            REAL,         -- current sliding-window rate (0.0–1.0)
    baseline_error_rate   REAL,         -- 30-day baseline from api_health
    spike_factor          REAL,         -- error_rate / (baseline + 0.001); NULL if no baseline
    scan_queue_depth      INTEGER,      -- LLEN SCAN_QUEUE at decision time
    detail_queue_depth    INTEGER,      -- LLEN DETAIL_QUEUE at decision time
    inflight_count        INTEGER,      -- XPENDING stream:adaptive:{dc_key} scan-workers (PEL size)
    learned_ceiling       INTEGER,      -- value stored/read from worker:ceil:learned:{dc_key}
    consec_reductions     INTEGER,      -- value of worker:consec_reductions:{platform}

    notes                 TEXT          -- free-form debug annotation
);

CREATE INDEX wse_platform_ts ON worker_scaling_events (platform, event_ts DESC);
CREATE INDEX wse_type_ts     ON worker_scaling_events (event_type,  event_ts DESC);
```

The table is **append-only** — no row is ever updated after insert. Write volume is very low (at most one event per 5-minute monitoring cycle per platform), so it uses a lightweight synchronous writer rather than the background queue used by `api_health`. See Section 22 for effectiveness query patterns.

#### `adaptive_poll_metrics` — Daily per-company observability

```sql
CREATE TABLE IF NOT EXISTS adaptive_poll_metrics (
    id                    BIGSERIAL PRIMARY KEY,
    date                  DATE NOT NULL,
    company               TEXT NOT NULL,
    ats_platform          TEXT,

    total_polls           INTEGER DEFAULT 0,
    total_new_jobs        INTEGER DEFAULT 0,
    tier1_new_jobs        INTEGER DEFAULT 0,
    tier2_new_jobs        INTEGER DEFAULT 0,

    found_within_1hr      INTEGER DEFAULT 0,
    found_within_4hr      INTEGER DEFAULT 0,
    found_within_24hr     INTEGER DEFAULT 0,
    found_after_24hr      INTEGER DEFAULT 0,
    avg_detection_hrs     FLOAT,

    reactivation_lag_hr   FLOAT,

    wasted_polls          INTEGER DEFAULT 0,
    http_requests_made    INTEGER DEFAULT 0,
    cost_per_new_job      FLOAT,

    early_exit_triggered  INTEGER DEFAULT 0,
    early_exit_missed     INTEGER DEFAULT 0,

    avg_poll_interval_s   INTEGER,
    score_oscillation     FLOAT,

    error_streak          INTEGER DEFAULT 0,
    tier_crossed          BOOLEAN DEFAULT FALSE,

    created_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE (date, company)
);
```

---

## 17. End-to-End Data Flow

### Daily cycle start (7 AM)

```
1. --monitor-jobs runs (cron at 7:00 AM)

   Smart hybrid mode:
   a. Query company_poll_stats for each company.
      "Covered"  = last_full_scan_at >= (now - 24h)   ← fullscan completed overnight
      "Missed"   = last_full_scan_at <  (now - 24h)   ← fullscan worker fell behind
      Field: last_full_scan_at (written by on_fullscan_complete — exhaustive all-pages).
             NOT last_poll_at (adaptive scan — uses smart early exit, may miss pages).
      Note: the coverage window is 24h rolling, NOT fixed at "today 7:00 AM".
            Using today_cycle.timestamp() would classify every overnight fullscan as
            "missed" because last_full_scan_at (e.g. 4 AM) < cycle_start (7:00:00 AM).

   b. Fallback re-fetch — only for missed companies
      Normal day (workers healthy): 0 re-fetches → digest at ~7:02 AM
      Workers partially down:       re-fetch only the missed subset → still fast
      Workers completely down:      re-fetch all companies → digest at ~7:30 AM

   c. PDF digest built from ALL status='new' rows in DB
      (covers both worker-found and fallback-found jobs)

2. Digest email sent with all status='new' jobs
3. UPDATE job_postings SET status='digested' WHERE status='new'
   (send FIRST, mark digested AFTER — prevents silent loss if send fails)
4. cycle_start Unix timestamp written to Redis: SET cycle:start {timestamp}
5. Dynamic worker count calculated and applied
6. Adaptive polling workers already running — they continue uninterrupted
7. Full scan scheduler begins dispatching based on score-ascending order
   (after each company's adaptive poll completes for the day)
```

### Adaptive polling — ongoing

```
Adaptive poll for company Stripe:

1. Dispatcher: ZRANGEBYSCORE poll:adaptive -inf {now} → "Stripe" due
   PEL check: XPENDING stream:adaptive:greenhouse scan-workers < ceiling
   XADD stream:adaptive:greenhouse * company Stripe due_at {now}
   ZREM poll:adaptive "Stripe"

2. Scan worker: XREADGROUP → receives Stripe message (msg_id = X)
   Listing scan:
     Fetch Stripe listing pages with smart early exit
     SET progress:Stripe "fetching_page_1" EX 120  (updated each page)
     new_ids = fetched_ids - known_ids[Stripe] from job_postings

3. For each new ID:
   UPDATE job_postings SET status='pending_detail' (INSERT if first time)
   LPUSH queue:detail:adaptive {stripe_job_json}

4. on_adaptive_complete():
   Update recent_poll_counts queue
   Compute new interval (asymmetric smoothing)
   Update company_poll_stats
   ZADD poll:adaptive {now + interval} "Stripe"   ← reschedule for next poll

5. XACK stream:adaptive:greenhouse scan-workers X  ← only after step 4 complete

Meanwhile, detail workers:
   BRPOP queue:detail:adaptive → job
   Fetch full detail
   UPDATE job_postings SET status='new', found_by='tier1_adaptive', ...
```

### Full scan flow

```text
Full scan for company Workday_Corp:

1. Full scan dispatcher: ZRANGEBYSCORE poll:fullscan -inf {now} → "Workday_Corp" due
   Pre-check: has adaptive polled today?
   last_poll_at >= cycle_start → YES → proceed
   XADD stream:fullscan * company Workday_Corp dc_key workday_wd1
   ZREM poll:fullscan "Workday_Corp"

2. Full scan worker: XREADGROUP → receives Workday_Corp message

3. Check resume state:
   full_scan_interrupted = TRUE → resume from interrupted_at_page
   full_scan_interrupted = FALSE → start from page 0

4. For each page:
   SET progress:Workday_Corp "fullscan_page_5" EX 120

   For each job on page:
     BF.EXISTS bloom:fullscan:Workday_Corp {job_id}
     → "seen": skip
     → "not seen": upsert to DB
       if genuinely new: LPUSH queue:detail:fullscan {job_json}
       Add to NEW_BLOOM

   Check pipeline:pause signal:
     If received:
       Commit current page to DB
       UPDATE company_poll_stats SET
         full_scan_interrupted=TRUE,
         interrupted_at_page=current_page,
         interrupted_at=NOW()
       ZADD poll:fullscan {time.time()-1} "Workday_Corp"  ← immediate re-pickup on resume
       XACK stream:fullscan fullscan-workers {msg_id}
       Return — wait for resume

4. Full scan completes:
   BF.RESERVE / SET bloom:fullscan:Workday_Corp = NEW_BLOOM, EXPIRE 36h
   UPDATE company_poll_stats SET
     full_scan_interrupted=FALSE,
     last_full_scan_at=NOW(),
     next_full_scan_at=NOW() + full_scan_interval_s
   (on_adaptive_complete() will ZADD poll:fullscan next time — not self-scheduled)
   XACK stream:fullscan fullscan-workers {msg_id}
```

### Nightly cron chain

```
1. PUBLISH pipeline:pause ""
   SET cronchain:alive 1 EX 300  (heartbeat, refreshed each cron step)
   SET db:maintenance 1          (explicit maintenance flag, no TTL)

   Workers: finish current page → commit → checkpoint → wait

2. DB backup
   Refresh cronchain:alive heartbeat

3. Cleanup jobs (old metrics pruning, vacuum, stale seen:{company} keys)
   Refresh cronchain:alive heartbeat

4. Other maintenance tasks
   Refresh cronchain:alive heartbeat

5. DEL db:maintenance
   DEL cronchain:alive
   PUBLISH pipeline:resume ""

   Workers: resume from checkpoints
```

---

## 18. Resilience and Failure Handling

Every component is treated as potentially fragile. The following policies ensure the system degrades gracefully and recovers automatically.

### Redis failures

**Restart / data loss:**
All Redis structures are rebuilt from PostgreSQL on startup. Rebuild order:
1. `poll:adaptive` ← from `company_poll_stats.next_poll_at`
2. `poll:fullscan` ← from `company_poll_stats.next_full_scan_at`
3. `queue:detail:*` ← from `job_postings WHERE status='pending_detail'`
4. `stats:{company}` ← from `company_poll_stats`
5. `adaptive_seen:{company}` ← not rebuilt; 24h TTL means next adaptive scan repopulates naturally
6. `bloom:fullscan:*` ← not rebuilt; next full scan treats company as cold start (correct but one expensive cycle)

**Memory full (OOM):**
Redis eviction policy must be set to `noeviction`. Redis refuses new writes instead of silently evicting critical keys. Workers handle write failures gracefully (retry, alert) rather than losing data unknowingly.

**Connection loss:**
Workers reconnect with exponential backoff. If Redis unreachable for > 2 minutes: pause workers, alert, resume when connection restores.

### Worker failures

**Crashed workers — Stream PEL recovery (replaces watchdog):**
Scan worker crashes no longer require a watchdog process. The Stream PEL tracks every message delivered but not yet ACK'd. `XAUTOCLAIM` re-delivers unACK'd messages after the idle timeout to another available worker:

```text
stream:adaptive:{dc_key}  → XAUTOCLAIM after 10 min idle (adaptive scans)
stream:fullscan  → XAUTOCLAIM after 20 min idle (full scans, longer timeout)
```

Re-delivery is automatic — no separate process polls for expired heartbeats. Each scan worker calls `XAUTOCLAIM` on its own stream at the top of its loop whenever `XREADGROUP` returns empty.

**Error-aware re-queue for mid-scan crashes:**
When a scan is already in progress (past the XREADGROUP checkpoint) and the worker is terminated, the shutdown handler explicitly re-queues the company in `poll:adaptive` with exponential backoff and `XACK`s the message (so `XAUTOCLAIM` does not also re-deliver it). See Section 9 — Graceful shutdown and mid-cycle worker removal for details.

**Hung workers (stuck HTTP request or frozen between requests):**
Two-layer detection:
1. Per-HTTP-request timeout: `connect_timeout=10s, read_timeout=30s`
   Kills truly dead connections without disrupting legitimate large reads.
2. Progress heartbeat TTL (120 seconds):
   Updated after every meaningful step (each page processed, each DB write).
   If progress key expires while heartbeat is still alive → worker is frozen
   between requests → scheduler liveness check kills the process; Stream PEL
   entry remains unACK'd and is recovered by `XAUTOCLAIM` after idle timeout.

This correctly distinguishes a SAP XML dump taking 3 minutes (many progress updates)
from a worker frozen waiting on a dead DB connection (no progress updates).

**Worker process killed by OS (OOM killer, SIGKILL):**
Supervisor (systemd/supervisord) restarts worker processes automatically.
Orphaned company detection catches any companies left in limbo.

### Exponential backoff — per-company, per-operation type

Re-queuing a failed company with `score=now` hammers the same endpoint again. Re-queuing with a flat 300s delay still repeats the failure at the same pace for persistent outages. Exponential backoff adapts the retry pace to the severity and duration of the problem.

**Three independent counters** — a listing scan failure and a detail fetch failure are unrelated events (different endpoints, possibly different root causes):

```
retry:backoff:scan:{company}      TTL = 86400 (auto-expires overnight)
retry:backoff:detail:{company}    TTL = 86400
retry:backoff:fullscan:{company}  TTL = 86400
```

Each stores the **retry count** (int, 0-based). Delay formula:

```python
delay = min(WORKER_DEPRIORITISE_SECS * (2 ** retry_count), 3600)
```

| Retry | Delay |
|-------|-------|
| 0 (1st failure) | 300s  (5 min) |
| 1               | 600s  (10 min) |
| 2               | 1200s (20 min) |
| 3               | 2400s (40 min) |
| 4               | 3600s (1h — cap) |
| 5+ (persistent) | 86400s (24h — give up for today) |

The counter is **reset to 0** on any successful operation of that type (`r.delete(f"retry:backoff:{op_type}:{company}")`). The 86400s TTL means all counters expire overnight automatically — every company starts fresh at the next cycle without any explicit reset at `record_cycle_start()`.

**Applies to all three operation types:**
- **Scan**: listing scan failure or error-triggered worker removal → re-queue to `poll:adaptive`
- **Detail**: detail fetch failure → re-queue to `queue:detail:adaptive`
- **Full scan**: full scan failure → re-queue to `poll:fullscan`

**Full scan suppressed during active scan backoff**

`_maybe_reschedule_full_scan()` checks `r.exists(f"retry:backoff:scan:{company}")` at entry. If the scan backoff key exists, fullscan scheduling is suppressed entirely. A full scan makes significantly more requests than a listing scan — triggering one while the ATS is already struggling accelerates the problem.

**Backoff and outage mode — take the maximum, not the sum**

When a platform enters outage mode, companies are pushed forward by the outage TTL. Individual company backoff delays also exist from prior failures. The two are combined by taking the **maximum**:

```python
actual_delay = max(backoff_delay, outage_remaining_s)
```

This prevents double-stacking that would push companies unnecessarily into the next day's window.

---

### PostgreSQL failures

**Connection pool exhausted:**
```
Rule: max_workers ≤ DB_POOL_MAXCONN - 3
```
Workers that can't acquire a connection within 10 seconds back off and retry —
they do not crash or silently discard work.

**Deadlock:**
Retry on PostgreSQL error code `40P01`. Three retries with 100ms exponential backoff.
If still failing: log, skip this poll cycle, reschedule normally.

**Database down:**
Workers pause (same mechanism as nightly maintenance). Alert immediately.
Don't discard fetched data — hold in memory up to a limit, then stop fetching.

**Disk full:**
Monitor disk space as part of nightly health check. Alert at 80% capacity.
Log rotation prevents log files from filling disk.

### Workers paused — never forever

Workers paused by `pipeline:pause` resume under two conditions:

1. Explicit `pipeline:resume` signal (normal case — nightly cron completes)
2. Cron chain heartbeat expired (`cronchain:alive` TTL elapsed without refresh)
   **AND** `db:maintenance` flag is absent (DB is not in maintenance mode)

```python
def should_resume():
    if received_resume_signal:
        return True
    
    cron_alive = redis.exists("cronchain:alive")
    db_maintenance = redis.exists("db:maintenance")
    
    if not cron_alive and not db_maintenance:
        alert("Cron chain may have crashed — auto-resuming workers")
        return True
    
    return False   # stay paused
```

The `db:maintenance` flag has **no TTL** — it must be explicitly cleared. This is intentional: if both the cron chain AND DB maintenance crash simultaneously, workers stay paused until an engineer manually confirms the DB is healthy.

### ATS / HTTP failures

**Single platform down:**
Apply per-platform backoff. Continue polling all other platforms.
After 7 consecutive errors: alert, consider suspending company temporarily.

**All platforms failing simultaneously:**
Indicates a network outage on our end, not an ATS issue:
```
If error_rate > 80% across ALL platforms in last 10 minutes:
    → pause all polling, alert "possible network outage"
    → resume when cross-platform error rate drops below 20%
```
Without this check, a network outage triggers hundreds of simultaneous error alerts —
noise that obscures the real problem.

**Unexpected empty response (0 jobs from a company that normally has 500):**
Flag to `custom_ats_diagnostics` with `pattern_hint='sudden_empty_response'`.
Do NOT delete or modify any `job_postings` rows.
Verify on next 2 consecutive polls before taking any action.

**Rate limiting (429):**
Respect `Retry-After` header if present. Apply per-ATS sliding window backoff.
See Section 19 for dynamic concurrency details.

### Full scan double-dispatch protection

`fullscan:lock:{company}` NX key is retired. Double-dispatch is prevented structurally by the two-layer delivery model: once the dispatcher `XADD`s a company to `stream:fullscan` and `ZREM`s it from `poll:fullscan`, it no longer exists in the scheduling ledger. A crashed worker leaves the message in the PEL; `XAUTOCLAIM` re-delivers it to exactly one other worker — not as a second concurrent dispatch.

### Digest email safety

Send email **before** marking jobs as digested:
```
1. SELECT jobs WHERE status='new'
2. Generate digest
3. SEND email
4. Only on success: UPDATE status='digested'
```
If system crashes between steps 3 and 4, next digest re-sends the same jobs.
Use a `digest_id` to detect and suppress re-sends gracefully.

### Clock safety

All scheduling uses Unix timestamps derived from Redis `TIME` command.
The 7 AM cycle start uses `America/New_York` pytz (handles DST automatically):
```python
eastern = pytz.timezone("America/New_York")
cycle_start = eastern.localize(
    datetime.now().replace(hour=7, minute=0, second=0, microsecond=0)
).timestamp()
```

---

## 19. Per-ATS Dynamic Concurrency

### The problem

Hardcoded worker counts per ATS platform caused 32 `requests_error` spikes on Workday. The system was hammering Workday's servers with more concurrent requests than they could handle, producing 404s and connection errors that were indistinguishable from genuine API failures.

A per-process `threading.Semaphore` only limits concurrency within a single process. With N `detail_worker` or `scan_worker` processes each honouring a local limit of L, the total actual concurrency is N×L — the limit is meaningless when workers scale. The fix requires a **cross-process distributed semaphore backed by Redis** so all workers share one global counter.

### Central enforcement — `workers/http_client.py`

All HTTP calls across every ATS module go through a single wrapper:

```python
# workers/http_client.py
def ats_get(url, platform, dc_key=None, **kwargs) -> requests.Response:
    """
    Drop-in replacement for requests.get() used by all ATS modules.

    1. Acquire distributed semaphore (Redis counter)
    2. Make HTTP call with standard timeouts
    3. Classify error type
    4. Update errwin sliding window in Redis (per-request)
    5. Record to api_health (PostgreSQL, background writer)
    6. Run feedback loop to adjust limit
    7. Release distributed semaphore
    """
```

The `dc_key` parameter allows Workday callers to pass a DC-level key instead of the generic platform key, achieving per-DC rate limiting with no special-casing in the semaphore logic.

### Distributed semaphore (Redis counter pair)

For each concurrency key (platform or Workday DC key):

```
concurrency:active:{key}   ← current in-flight count  (INCR / DECR atomically)
concurrency:limit:{key}    ← current allowed max       (written by feedback loop)
```

Before each HTTP call:
```python
active = r.incr(f"concurrency:active:{key}")
limit  = int(r.get(f"concurrency:limit:{key}") or DEFAULT_LIMIT)
if active > limit:
    r.decr(f"concurrency:active:{key}")
    # back off with jitter, retry up to MAX_RETRIES
```

After each HTTP call (success or failure):
```python
r.decr(f"concurrency:active:{key}")
```

This enforces the limit globally across all worker processes and machines. The Redis round-trip (~0.1 ms) is negligible against HTTP calls of 500–3000 ms.

### Sliding window rate tracking

A 10-minute sliding window in Redis is updated on **every single HTTP call**:

```
Key:    errwin:{key}:{bucket}      bucket = int(time.time() // 600)
Fields: total_requests, errors_429, errors_404, errors_timeout, errors_5xx
TTL:    1200s  (covers 2 buckets — current + previous)
```

429 (explicit rate limit), 404 (Workday overload), timeout (concurrency queue overflow), and 5xx (server-side instability) are all tracked because each is an early signal of a different failure mode. See Section 20 for the diagnostic table.

To read the current 10-min error rate, sum both buckets across all four error signals:
```python
def get_error_rate(r, key) -> float:
    now    = int(time.time() // 600)
    totals = {
        "total_requests": 0,
        "errors_429":     0,
        "errors_404":     0,
        "errors_timeout": 0,
        "errors_5xx":     0,
    }
    for bucket in (now, now - 1):
        raw = r.hgetall(f"errwin:{key}:{bucket}")
        for field in totals:
            totals[field] += int(raw.get(field, 0))
    if totals["total_requests"] == 0:
        return 0.0
    errors = (totals["errors_429"] + totals["errors_404"]
              + totals["errors_timeout"] + totals["errors_5xx"])
    return errors / totals["total_requests"]
```

### Feedback loop

Runs inside `ats_get()` after every HTTP call — no batch delay, no lag:

```python
def adjust_concurrency(r, key, error_rate):
    current = int(r.get(f"concurrency:limit:{key}") or DEFAULT_LIMIT)
    
    if error_rate > 0.10:    # >10% errors → reduce concurrency
        new_limit = max(current - 1, CONCURRENCY_FLOOR[key])
    elif error_rate < 0.02:  # <2% errors → cautiously increase
        new_limit = min(current + 1, CONCURRENCY_CEIL[key])
    else:
        new_limit = current  # stable range, no change
    
    if new_limit != current:
        r.set(f"concurrency:limit:{key}", new_limit)
```

`CONCURRENCY_FLOOR` and `CONCURRENCY_CEIL` are per-platform constants in `config.py`. The floor ensures the system never reaches zero concurrency (minimum 1).

### Workday DC-level semaphores

Workday uses regional data centers. Requests to different companies may hit different DCs, each with its own rate limits.

DC keys are discovered from the `ats_slug` JSON field in `prospective_companies` — NOT from a `career_page_url` column (which does not exist). The `wd` field in the slug JSON contains the DC identifier directly:

```json
{"slug": "salesforce", "wd": "wd12", "path": "External_Career_Site"}
```

```python
def _extract_workday_dc_key(ats_slug: dict) -> str:
    """
    Extract DC identifier from the parsed ats_slug JSON for a Workday company.

    The 'wd' field contains the data-center suffix already parsed at
    ATS detection time.

    Examples:
      {"wd": "wd1",  ...}  →  "workday_wd1"
      {"wd": "wd12", ...}  →  "workday_wd12"
      {}                   →  "workday_default"
    """
    wd = ats_slug.get("wd")
    if wd:
        return f"workday_{wd}"
    return "workday_default"
```

At startup, DC keys are discovered by querying the DB:
```python
def discover_workday_dc_keys() -> set:
    rows = db.execute("""
        SELECT DISTINCT ats_slug FROM prospective_companies
        WHERE ats_platform IN ('workday', 'workdaysites')
          AND ats_slug IS NOT NULL
    """).fetchall()

    dc_keys = set()
    for row in rows:
        try:
            slug = json.loads(row["ats_slug"])
            dc_keys.add(_extract_workday_dc_key(slug))
        except (json.JSONDecodeError, TypeError):
            dc_keys.add("workday_default")

    return dc_keys   # e.g. {"workday_wd1", "workday_wd5", "workday_wd12"}
```

Each discovered DC key gets its own Redis semaphore pair, starting at a conservative default (`CONCURRENCY_WORKDAY_DEFAULT = 3`) and adjusting dynamically via the feedback loop.

For scan_worker and detail_worker, Workday calls pass `dc_key` from `ats_slug["wd"]` to `ats_get()`. Non-Workday platforms pass only `platform`.

### Scope

Both `scan_worker` (listing fetches) and `detail_worker` (detail fetches) go through `ats_get()`. All ATS modules replace direct `requests.get()` calls with `ats_get()`. This ensures the limit is enforced regardless of which worker type is making the request.

---

## 20. Error Type Differentiation

### The problem

A spike in `requests_error` could mean two very different things:
1. **Concurrency-induced**: too many parallel requests overwhelmed the platform (our fault — fix by reducing concurrency)
2. **Genuine API failure**: platform down, changed API, authentication issue (their fault — fix by investigating platform)

Without distinguishing these, the on-call response is wrong: investigating "why is Workday API broken?" when the real problem is "we're sending 32 concurrent requests."

### Fix 1 — Error sub-types in schema

Replace the single `requests_error` column with four sub-types:

```sql
requests_timeout    INTEGER DEFAULT 0   -- TCP/HTTP read timeout → concurrency signal
requests_conn_err   INTEGER DEFAULT 0   -- Connection refused → network issue
requests_5xx        INTEGER DEFAULT 0   -- Server errors → soft rate limit / instability
requests_other_err  INTEGER DEFAULT 0   -- Parse failures, unexpected responses
```

Classification at the call site:

```python
def classify_error(exception):
    if isinstance(exception, requests.Timeout):
        return "timeout"
    elif isinstance(exception, requests.ConnectionError):
        return "conn_err"
    elif hasattr(exception, 'response') and exception.response.status_code >= 500:
        return "5xx"
    else:
        return "other_err"
```

**Why timeout is a concurrency signal:** When too many parallel requests are sent, the server queues them. Queued requests exceed their timeout. Timeouts are the first symptom of concurrency overload — before 429s, before 5xxs.

### Fix 2 — Baseline deviation detection (real-time, Redis-cached)

The errwin sliding window tracks **four error signals**, not just 429 and 404:

```
Key:    errwin:{key}:{bucket}
Fields: total_requests, errors_429, errors_404, errors_timeout, errors_5xx
```

Timeouts are included because they are the **earliest** concurrency signal — they appear before 429s or 5xxs when the server is overloaded. The aggregate error rate in the feedback loop becomes:

```python
error_rate = (errors_429 + errors_404 + errors_timeout + errors_5xx) / total_requests
```

The 30-day historical baseline is computed from `api_health` and **cached in Redis at `baseline:error_rate:{platform}`** with a 1-hour TTL. This makes it available for real-time feedback loop decisions without a PostgreSQL query on every request.

```python
def get_baseline_error_rate(r, platform) -> float:
    """
    Return 30-day avg error rate for a platform.
    Read from Redis cache (1h TTL); refresh from api_health on miss.
    Returns 0.0 if fewer than 7 days of history exist (cold start).
    """
    cached = r.get(f"baseline:error_rate:{platform}")
    if cached is not None:
        return float(cached)
    # Cache miss — query PostgreSQL and re-cache
    rate = query_30day_avg_error_rate(platform)    # from api_health
    r.set(f"baseline:error_rate:{platform}", rate, ex=3600)
    return rate
```

**Safe fallback:** if fewer than 7 days of `api_health` data exist for a platform, `get_baseline_error_rate()` returns 0.0 and the feedback loop skips the spike_factor check entirely — falling back to the raw error_rate threshold only. This prevents a cold-start platform from triggering aggressive reduction on its first day of errors.

**Baseline purity — `context` column filtering**

Managed-error requests (backoff retries, canary probes) have artificially high error rates by design. Including them in the 30-day baseline would make normal variance appear larger, raising the threshold at which a real spike is detected.

`query_30day_avg_error_rate()` therefore filters `WHERE context = 'normal'` — only requests made under normal operating conditions count toward the baseline. Backoff and canary requests are still stored (all contexts persisted for observability) but excluded from the calculation:

```sql
SELECT
    SUM(requests_timeout + requests_5xx + requests_429 + requests_404) AS total_errors,
    SUM(requests_made)                                                  AS total_requests
FROM api_health
WHERE platform = ?
  AND date >= ?
  AND context = 'normal'       -- exclude backoff and canary requests
  AND requests_made > 0
```

The feedback loop in `ats_get()` uses both signals together:

```python
error_rate   = get_error_rate(r, key)        # errwin: last 10 minutes
baseline     = get_baseline_error_rate(r, platform)
spike_factor = error_rate / (baseline + 0.001)

if error_rate > CONCURRENCY_ERROR_RATE_REDUCE:
    if spike_factor > 5:
        # Anomalous spike above historical baseline → concurrency-induced
        # Aggressive: drop by 2 (or to floor if only 1 step away)
        new_limit = max(current - 2, floor)
    else:
        # High error rate but within historical norms → cautious reduction
        new_limit = max(current - 1, floor)
elif error_rate < CONCURRENCY_ERROR_RATE_INCREASE:
    new_limit = min(current + 1, ceil)
```

**Why spike_factor matters:** a platform with a 5% historical error rate hitting 8% today is normal variance — no action. A platform with a 0.2% historical rate hitting 8% today is a 40× spike — almost certainly concurrency-induced. The raw threshold treats both identically; the spike_factor distinguishes them.

Used together for diagnosis:

| Error sub-type | Spike factor | Diagnosis | Action |
|---|---|---|---|
| `errors_timeout` | > 5× | Concurrency overload — server queueing requests | Reduce semaphore + reduce workers (if at floor) |
| `errors_5xx` | > 5× | Soft rate limit or server instability | Reduce semaphore + add delay |
| `errors_conn_err` | > 5× | Network issue — not concurrency | Check connectivity, not concurrency |
| `errors_other` | > 5× | API structure changed | Check `custom_ats_diagnostics` |
| Any type | ≤ 5× baseline | Normal noise | No action needed |

This is what makes the 32-error Workday spike diagnosable: all 32 were `errors_timeout`, spike_factor was ~32× the baseline → concurrency-induced → fix was reducing parallel Workday workers, not investigating the API.

---

## 21. Scaling Properties

### How the numbers change

| Scale | Companies | Jobs fetched/poll | Detail fetches/day | Redis memory |
|-------|-----------|------------------|-------------------|--------------|
| Today | 139 | 146,497 (full scan) | 146,497 | N/A |
| After migration | 139 | ~1,390 (listings only) | ~157 (new only) | ~50MB |
| 500 companies | 500 | ~5,000 | ~565 | ~150MB |
| 5,000 companies | 5,000 | ~50,000 | ~5,650 | ~400MB |

Bloom filters add ~4.5MB total across all 5,000 companies (trivial).

### Dynamic worker scaling

See Section 9 for the full two-pool design (scan workers + detail workers, co-scheduled, multiprocessing.Process, three monitoring layers, cascade backpressure, pace mismatch handling).

**Graceful degradation:** Full scan queue is ordered score-ascending (dormant first). If workers can't finish all full scans before next 7 AM, active companies are the last to be skipped — and adaptive polling has already been covering them throughout the day.

### Why 5,000 companies is achievable

At 5,000 companies with 24-hour Tier 2 guarantee:
- 10 workers × 12 listing scans/minute = 120 companies/minute = 172,800/day capacity
- 5,000 companies × avg 10 adaptive polls/day = 50,000 adaptive polls/day needed
- Full scans: 5,000/day, staggered across 23 hours = ~3.6 seconds between full scans
- Well within capacity with 10+ workers

---

## 22. Observability — Measuring Whether the System is Working

### The `found_by` field — foundation of all effectiveness metrics

```sql
found_by TEXT
-- 'tier1_adaptive'  → found during a normally-scheduled adaptive poll
-- 'tier2_full_scan' → found only because full scan caught it (Tier 1 missed it)
```

### The 8 metrics that matter

#### Metric 1 — Miss Rate
```
miss_rate = tier2_new_jobs / total_new_jobs
```
| Value | Meaning | Action |
|-------|---------|--------|
| < 5% | ✓ Adaptive capturing almost everything | No action |
| 5–15% | ⚠ Some intervals too long | Review per-company miss rate |
| > 15% | ✗ Adaptive underperforming | Score model or MAX_INTERVAL needs tuning |

#### Metric 2 — Detection Age Distribution
```
% found within  1 hour  → target > 50% for active companies
% found within  4 hours → target > 80%
% found within 24 hours → target > 99%
% found after  24 hours → target = 0%
```

#### Metric 3 — Tier 1 Capture Rate
```
tier1_capture_rate = tier1_new_jobs / total_new_jobs   (target > 95%)
```

#### Metric 4 — Cost Per New Job Found
```
cost_per_new_job = http_requests_made / total_new_jobs
```
Baseline today: 930. Target after migration: 5–50.

#### Metric 5 — Wasted Poll Rate
```
wasted_poll_rate = wasted_polls / total_polls   (target < 80%)
```
Some wasted polls are healthy — 100% efficiency means the interval is dangerously long.

#### Metric 6 — Reactivation Detection Time
```
reactivation_lag = first_new_job_detected_at - first_new_job_posted_at
```
Target: < 24hr (guaranteed by MAX_INTERVAL cap). < 6hr is good.

#### Metric 7 — Early Exit Miss Rate
```
early_exit_miss_rate = early_exit_missed / total_new_jobs_on_early_exit_polls
```
Should be exactly 0%. Any nonzero value means the 80%/2-page threshold stopped too early.

#### Metric 8 — Score Oscillation
```
score_oscillation = stddev(daily_scores, last_14_days) per company
```
Target: < 0.15. > 0.20 means interval is thrashing — increase smoothing factor.

### Error type health metrics

Per-platform daily aggregates from `api_health`:
```sql
SELECT
    platform,
    SUM(requests_timeout)   AS timeouts,
    SUM(requests_conn_err)  AS conn_errors,
    SUM(requests_5xx)       AS server_errors,
    SUM(requests_other_err) AS other_errors,
    -- Compare to 30-day baseline to detect spikes
    AVG(requests_timeout) OVER (
        PARTITION BY platform
        ORDER BY date
        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS baseline_timeout_rate
FROM api_health
GROUP BY platform, date;
```

### Response time metrics

`avg_response_ms`, `max_response_ms`, and `total_ms` are tracked per platform per day in `api_health` and updated on every request — including errors and timeouts. `max_response_ms` uses `GREATEST()` (updated on each write). `avg_response_ms` is recomputed as `total_ms / requests_made` after each write.

Timeouts contribute their full elapsed wall-clock duration to both columns (e.g. a 30 s timeout → 30 000 ms). This is intentional for `calculate_worker_counts()`, which uses `avg_response_ms` as a proxy for how long a worker is actually occupied. A separate `total_ms_ok` column tracking only successful-request time was considered but deferred — the distortion from timeout inflation may be acceptable given that timed-out workers are genuinely busy for that duration, and splitting the column adds complexity with no confirmed benefit until real data is available.

---

### Worker scaling health — `worker_scaling_events` table

Every decision made by the three monitoring layers is recorded in `worker_scaling_events` (see Section 16 schema). Because it is append-only, effectiveness is derived by querying adjacent events rather than updating rows.

**Did a worker removal actually fix the problem?**

```sql
WITH reductions AS (
    SELECT id, event_ts, platform, error_rate AS rate_before
    FROM worker_scaling_events
    WHERE event_type = 'worker_remove'
),
next_check AS (
    SELECT r.id, MIN(e2.error_rate) AS rate_after
    FROM reductions r
    JOIN worker_scaling_events e2
      ON e2.platform = r.platform
     AND e2.event_ts BETWEEN r.event_ts + INTERVAL '4 min'
                         AND r.event_ts + INTERVAL '11 min'
    GROUP BY r.id
)
SELECT
    r.platform,
    r.event_ts,
    ROUND(r.rate_before * 100, 1)  AS error_pct_before,
    ROUND(n.rate_after  * 100, 1)  AS error_pct_after,
    CASE WHEN n.rate_after < r.rate_before * 0.7
         THEN 'effective' ELSE 'ineffective' END AS outcome
FROM reductions r
LEFT JOIN next_check n USING (id)
ORDER BY r.event_ts DESC;
```

**Weekly summary — reductions per platform, escalation rate**

```sql
SELECT
    platform,
    DATE_TRUNC('week', event_ts)                                      AS week,
    COUNT(*) FILTER (WHERE event_type = 'worker_remove')              AS reductions,
    COUNT(*) FILTER (WHERE event_type = 'outage_start')               AS outages,
    ROUND(AVG(error_rate) FILTER (
        WHERE event_type = 'worker_remove') * 100, 1)                 AS avg_error_pct_at_reduction
FROM worker_scaling_events
GROUP BY platform, week
ORDER BY week DESC, platform;
```

**Average outage duration**

```sql
SELECT
    s.platform,
    ROUND(AVG(EXTRACT(EPOCH FROM (e.event_ts - s.event_ts)) / 60)) AS avg_outage_min
FROM worker_scaling_events s
JOIN LATERAL (
    SELECT event_ts FROM worker_scaling_events
    WHERE platform  = s.platform
      AND event_type = 'outage_end'
      AND event_ts   > s.event_ts
    ORDER BY event_ts LIMIT 1
) e ON true
WHERE s.event_type = 'outage_start'
GROUP BY s.platform;
```

These queries feed directly into the weekly adaptive health report below.

---

### Weekly adaptive health report

Included in Monday digest:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ADAPTIVE POLLING HEALTH — Week of Apr 21
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CORE EFFECTIVENESS
  Tier 1 capture rate:    97.3%     ✓  (target >95%)
  Miss rate:               2.7%     ✓  (target <5%)
  Reactivation detection: avg 3.8hr ✓  (target <24hr)

DETECTION SPEED
  Within  1 hour:          61%      ✓
  Within  4 hours:         89%      ✓
  Within 24 hours:         99.1%    ✓
  After  24 hours:          0.9%    ⚠  → 3 jobs (Honeywell ×2, Siemens ×1)

EFFICIENCY
  Cost per new job:        8.3 req  ✓  (baseline was 930)
  Wasted poll rate:         44%     ✓

ERROR HEALTH
  Workday timeouts:         2       ✓  (baseline 1.8, ratio 1.1x — normal)
  iCIMS 5xx errors:         0       ✓
  Greenhouse conn errors:   0       ✓

WORKER SCALING (from worker_scaling_events)
  Total reductions:         3       workday×2, greenhouse×1
  Effective reductions:     3/3     ✓  (error rate dropped >30% within 10 min)
  Outages triggered:        0       ✓
  Avg response time:      412 ms    workday (baseline 380 ms — within normal range)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 23. Implementation Roadmap

### Phase 1 — PostgreSQL Migration

**What:** Replace SQLite with PostgreSQL. Create `company_poll_stats`, `company_config` tables. Add new columns to `job_postings` (`job_id`, `ats_platform`, `first_published`, `last_updated`, `last_polled`, `found_by`, `_country_code`) and `api_health` (error sub-type columns).

**Deliverable:** All existing functionality works on PostgreSQL. New tables exist but empty.

**Estimated effort:** 2–3 days

---

### Phase 2 — Incremental Filter

**What:** Implement `pipeline/incremental.py`. Diff fetched IDs against `job_postings` (existing table, `job_id` column). Only detail-fetch new IDs.

**Deliverable:** Daily run completes in ~5 minutes instead of 45–90 minutes. Same digest output.

**Estimated effort:** 2–3 days

---

### Phase 3 — Redis Integration

**What:** Set up Redis. Implement `bloom:fullscan:{company}` Bloom filters and `adaptive_seen:{company}` SETs. Rate limiter. Two-tier detail fetch queue (adaptive + fullscan). Backpressure on listing scan. `pending_detail` status + queue rebuild on restart.

**Deliverable:** Adaptive scan early exit functional. Full scan DB check optimisation active. Detail fetches parallel with backpressure.

**Estimated effort:** 2–3 days

---

### Phase 4 — Priority Queue Scheduler

**What:** `pipeline/scheduler.py`. Replace cron batch with continuous two-layer scheduler. Both Tier 1 and Tier 2 use ZSET (scheduling ledger) + Redis Stream per DC key (crash-safe delivery):
- `poll:adaptive` ZSET → `stream:adaptive:{dc_key}` — adaptive dispatcher, scan workers via XREADGROUP
- `poll:fullscan` ZSET → `stream:fullscan` — full scan dispatcher, full scan workers via XREADGROUP
- `inflight:scans:{dc_key}` ZSET active — used with `inflight:fullscans:{dc_key}` for per-DC ceiling enforcement in the adaptive dispatch Lua script; Stream PEL (`XPENDING`) is monitoring-only
- Watchdog process retired — `XAUTOCLAIM` handles crash recovery (10 min for adaptive, 20 min for full scan)
- `ZPOPMIN` replaced by `ZRANGEBYSCORE` → `XADD` → `ZREM` pattern (crash between XADD and ZREM is idempotent)

Hash-based slot assignment (Section 24): `now + slot_offset(batch_position)` for NEW → WARMING bootstrap; `now + adaptive_interval` for STABLE recurring polls — using `hashlib.md5`, not Python's `hash()`. No midnight anchoring — `now + offset` is always in the future regardless of time-of-day and distributes load evenly throughout the day. WARMING state persisted in `warming_polls_remaining` + `initial_slot_offset_s` columns (survives restarts). `on_fullscan_complete()` defined — bootstraps NEW → WARMING. `on_adaptive_complete()` Rule 5 (pre-7 AM full scan trigger) uses `p95_full_scan_s`, not listing scan p95. Dawn patrol removed.

Stream correctness: `XADD MAXLEN ~ N` on every enqueue (bounded stream size). Consumer names = `worker-{hostname}-{pid}` (unique; no PEL theft). `XGROUP CREATE id=$` on startup (`BUSYGROUP` → skip). `XREADGROUP BLOCK 500` (not 5000) + Pub/Sub daemon thread for `pipeline:pause` reaction within 1s. `XAUTOCLAIM` idle timeout = `p95_scan_ms × 3` (self-calibrating; full scan uses full scan p95 separately). Dead-letter after 5 re-deliveries → exponential backoff in `poll:*`.

**Deliverable:** Continuous monitoring. Companies on individual schedules. Adaptive polls permanently distributed across the day — no thundering herd. Both scan types delivered via crash-safe Redis Streams with automatic recovery. No watchdog process required.

**Estimated effort:** 3–4 days

---

### Phase 5 — Adaptive Interval Engine

**What:** `pipeline/adaptive.py`. 5-queue weighted average. Asymmetric smoothing. Score-tiered MAX_INTERVAL cap. `recent_poll_counts` column. Reactivation handles immediately.

**Deliverable:** Each company's frequency auto-adjusts based on observed behavior.

**Estimated effort:** 2–3 days

---

### Phase 6 — Full Scan

**What:** `pipeline/fullscan.py`. Score-ascending scheduling. Adaptive-first prerequisite check. Bloom filter per company (36h TTL, 0.1% error, RedisBloom + SET fallback). SORTED vs NON-SORTED behavior. Graceful termination (Pub/Sub + checkpoint). Dynamic worker scaling.

**Deliverable:** Exhaustive safety net running daily. miss_rate metric meaningful.

**Estimated effort:** 3–4 days

---

### Phase 7 — Smart Early Exit Pagination

**What:** `pipeline/paginator.py`. 80%/2-page overlap exit for SORTED platforms. Time-based cutoff for NON-SORTED adaptive polling. Full scan of NON-SORTED always goes all pages.

**Deliverable:** Large companies don't paginate all 200 pages on every poll.

**Estimated effort:** 1–2 days

---

### Phase 8 — Per-ATS Dynamic Concurrency

**What:** `workers/http_client.py` — central HTTP wrapper used by all ATS modules replacing direct `requests.get()` calls. Redis distributed semaphore (counter pair `concurrency:active:{key}` / `concurrency:limit:{key}`) enforced cross-process. Per-request 10-minute sliding window (`errwin:{key}:{bucket}`) for 429 + 404 tracking. Feedback loop adjusting limit after every call. Workday DC-level semaphores keyed from `ats_slug["wd"]` (not career URL). `discover_workday_dc_keys()` queries `prospective_companies.ats_slug` at startup.

**Deliverable:** No more concurrency-induced error spikes. Workday per-DC rate limiting. Limit enforced globally across all worker processes.

**Estimated effort:** 2 days

---

### Phase 9 — Error Type Differentiation + Dynamic Worker Scaling

**What (error differentiation):** Fix 1 already complete (4 sub-type columns, `classify_error()`, wired through `ats_get()`). Fix 2: extend `errwin` sliding window to track `errors_timeout` and `errors_5xx` in addition to 429 + 404. Baseline error rate cached in Redis (`baseline:error_rate:{platform}`, 1h TTL) from 30-day `api_health` average. Feedback loop in `ats_get()` uses spike_factor (anomalous vs normal variance) to choose aggressive vs cautious concurrency reduction. Safe fallback when <7 days history: skip spike_factor, use raw error_rate only.

**What (dynamic worker scaling):** Replace fixed thread pools with `multiprocessing.Process` worker pools managed by the scheduler. Both scan and detail worker pools calculated at 7 AM from historical averages — not starting at minimum. Three monitoring layers: (1) liveness check every tick (dead worker replaced immediately), (2) fast error check every 5 min (error-triggered worker reduction, platform-aware deprioritisation), (3) slow throughput check every 30 min (queue-depth-driven scaling with 2-consecutive-checks hysteresis). Cascade backpressure: detail queue growing → add detail workers first; detail at ceiling → stop adding scan workers; detail at ceiling + queue still growing → reduce scan workers. DB pool split proportionally between pools (combined ≤ DB_POOL_MAXCONN - 3). Graceful shutdown via `multiprocessing.Event`.

**Deliverable:** Concurrency-induced errors distinguishable from genuine API failures. Worker count self-adjusts from a data-driven starting point and heals under load, including detail/scan pace mismatch.

**Estimated effort:** 3–4 days

---

### Phase 10 — Adaptive ATS Protection + Resilience Hardening

**What (ATS protection — Section 9):**
Exponential backoff for all operation types (scan, detail, fullscan): 300s → 600s → 1200s → 2400s → 3600s → 86400s. Error-triggered worker removal re-queues in-progress company to `poll:adaptive` with backoff delay; message `XACK`'d so `XAUTOCLAIM` does not also re-deliver. Shutdown event checked at each page boundary in `_run_listing_scan()`. Per-DC in-flight tracking via `inflight:scans:{dc_key}` ZSET (adaptive) and `inflight:fullscans:{dc_key}` ZSET (fullscan), enforced atomically in dispatch Lua scripts; Stream PEL (`XPENDING`) is monitoring-only. Platform-isolated dispatch throttling in adaptive dispatcher — Workday DC ceiling has zero effect on Greenhouse/Lever. Per-DC learned ceiling (`worker:ceil:learned:{dc_key}`) discovered empirically, stored in Redis indefinitely, decays +1 per 24h of clean operation. ATS outage detection: 3 consecutive ineffective worker reductions → 60-min dispatch pause via `worker:outage:{platform}`. Canary probe at 30-min mark for early recovery. Scaling lock (`worker:scaling_lock:{platform}`) prevents slow throughput check from undoing fast error check interventions. Full scan suppressed while scan backoff is active. Watchdog process retired — `XAUTOCLAIM` handles scan worker crash recovery automatically.

**What (observability — Sections 16 and 22):**
`context` column added to `api_health` (`normal` | `backoff` | `canary`). Baseline queries (`query_30day_avg_error_rate`, `query_30day_avg_response_ms`) filter `WHERE context = 'normal'` so managed-error periods do not inflate the historical baseline. `record_request()` accepts an optional `context` parameter; all call sites default to `'normal'` with no change needed unless in backoff or canary path. New `worker_scaling_events` table records every scaling decision (worker add/remove, outage start/end, canary probe, ceiling learned) with full health-signal context. Used for weekly health report and post-incident effectiveness analysis.

**What (resilience hardening):**
Redis `noeviction` policy. Progress heartbeat for hung worker detection. `cronchain:alive` auto-resume on expiry. All-ATS failure correlation check. Bloom filter corruption detection.

**Deliverable:** System never re-hits a known ATS rate limit from scratch. Failed companies back off exponentially and recover proportionally. ATS outages are isolated — healthy platforms continue at full throughput while the affected platform pauses. Every scaling decision is queryable: did reducing workers actually improve error rate, how quickly, how often does it escalate to an outage?

**Estimated effort:** 3–4 days

---

### Phase 11 — Monitoring and Alerting

**What:** Weekly adaptive health report in Monday digest. Per-ATS miss rate dashboard. Error streak alerting. Redis memory monitoring. Detail queue depth alerting.

**Estimated effort:** 1–2 days

---

## 24. Thundering Herd Prevention — Hash-Based Slot Distribution

### The problem

When multiple companies are onboarded at the same time (or the system restarts after a crash), all of them have the same `last_poll_at` timestamp. After one full poll cycle, they all receive identical intervals from the adaptive engine — which means they all become due simultaneously on every subsequent cycle. The result: all their adaptive polls cluster at the same moment → all their full scans trigger simultaneously → workers drain serially → the last full scan finishes 30–40 minutes after the 7 AM digest deadline.

This is not a worker-count problem. Adding more workers does not help because the ATS concurrency ceiling (Section 19 learned ceiling) bounds throughput regardless of how many workers are waiting for the same semaphore slot. The root fix must eliminate the cluster, not process it faster.

### The fix: permanent hash-based slot assignment

Each company is assigned a fixed, deterministic offset derived from a hash of a stable identifier. The offset distributes companies evenly across any 24-hour window, making clustering structurally impossible regardless of how many companies joined together.

```text
first_poll_at = time.time() + slot_offset(identifier)
```

Using `now + slot_offset` (not `midnight + slot_offset`) means:
- The first poll is **always in the future** — no timezone math, no "push to tomorrow" edge case.
- Workers are loaded **evenly throughout the day** rather than hitting a burst at midnight.
- The deterministic hash ordering is preserved regardless of when the formula runs.

**Critical: must use a proper hash function, not Python's built-in `hash()`.**
`hash(n) == n` for small integers in CPython — `hash(1) % 86400 = 1`, `hash(2) % 86400 = 2`, etc. All new companies would cluster in the first few seconds of the window. Use MD5 truncated to an integer instead:

```python
import hashlib

def slot_offset(identifier: int) -> int:
    """Deterministic, well-distributed offset in [0, 86400)."""
    digest = hashlib.md5(str(identifier).encode()).hexdigest()
    return int(digest, 16) % 86400
```

The **identifier** depends on the company's lifecycle phase:

#### Phase 1: First scan (batch_position — not company_id)

When a batch of new companies is registered, each is assigned a position within that batch (1, 2, 3 … N). This position is hashed, not the global company ID or global registration count.

```python
import hashlib

def slot_offset(identifier: int) -> int:
    digest = hashlib.md5(str(identifier).encode()).hexdigest()
    return int(digest, 16) % 86400

# 5 new companies added to a system with 133 already running.
# batch_position = ordinal within THIS registration event (resets to 1 each batch).

slot_offsets = [
    slot_offset(1),   # → 27,291 s  (07:34 AM)   Company A
    slot_offset(2),   # → 68,104 s  (18:55 PM)   Company B
    slot_offset(3),   # → 41,840 s  (11:37 AM)   Company C
    slot_offset(4),   # → 14,523 s  (04:02 AM)   Company D
    slot_offset(5),   # → 55,217 s  (15:20 PM)   Company E
]
# Evenly distributed across 24h — no clustering.
```

**Why batch_position and not company_id?**
Sequential company IDs (e.g. 134–138) are close integers. Even with a proper hash, a run of 5 sequential IDs could land within similar ranges by chance. Batch position resets to 1 for every registration event — tomorrow's 3-company batch also uses positions 1, 2, 3 — giving maximum independence between batches. `initial_slot_offset_s` is stored in `company_poll_stats` at registration so the slot survives restarts (see PostgreSQL schema, Section 16).

#### Phase 2: Recurring scans (STABLE — adaptive engine takes over)

Once WARMING completes (`warming_polls_remaining` reaches 0), the adaptive engine takes full control. Rescheduling is simply:

```python
next_adaptive_at = time.time() + adaptive_interval   # computed by on_adaptive_complete()
```

`slot_offset(company_id)` is no longer used for scheduling — companies naturally spread out over time because their adaptive intervals diverge based on observed posting activity. The switch happens once: when `warming_polls_remaining` is set to NULL in `on_adaptive_complete()`. From that point forward, the interval is fully dynamic for the company's lifetime.

### New company lifecycle: NEW → WARMING → STABLE

```text
NEW (last_poll_at IS NULL AND last_full_scan_at IS NULL)
  ─────────────────────────────────────────────────────────────────
  At registration:
    • Compute initial_slot_offset_s = slot_offset(batch_position)
      and persist to company_poll_stats immediately.
    • XADD stream:fullscan — full scan first, spread by
      startup window formula.
    • Do NOT add to poll:adaptive yet.

  Why full scan before adaptive?
  Full scan marks all existing jobs as pre_existing — prevents the
  digest from flooding with hundreds of jobs that were already posted
  before monitoring started. Adaptive would queue all of them for
  detail fetch if it ran first.

    ↓  (first full scan completes → on_fullscan_complete() fires)

WARMING (warming_polls_remaining = 3, 2, 1)
  ─────────────────────────────────────────────────────────────────
  on_fullscan_complete():
    • Sets warming_polls_remaining = 3 in company_poll_stats.
    • Schedules first adaptive poll:
        now + initial_slot_offset_s
        (always in the future — no midnight anchoring, no daily wave)
    • ZADDs into poll:adaptive with that timestamp.

  Each on_adaptive_complete() during WARMING:
    • Decrements warming_polls_remaining.
    • Ignores adaptive engine output — always schedules next poll
      at current_slot + 2h.
    • warming_polls_remaining = 0 → transition tick:
        SET warming_polls_remaining = NULL  (→ STABLE)
        Adaptive engine computes next interval normally from this point.

  State survives restarts: both warming_polls_remaining and
  initial_slot_offset_s are persisted in company_poll_stats.

    ↓  (warming_polls_remaining set to NULL)

STABLE (adaptive engine takes full control)
  ─────────────────────────────────────────────────────────────────
  • Slot = slot_offset(company_id) — fixed for company lifetime.
  • Interval set by adaptive engine (Section 6).
  • Full scan triggered by on_adaptive_complete() Rule 3 / Rule 5.
```

**`on_fullscan_complete()` — bootstraps NEW → WARMING:**

```python
def on_fullscan_complete(company):
    """Called after a company's first-ever full scan finishes."""
    if company.last_poll_at is not None:
        return   # not a NEW company — standard rules apply

    # Spread first adaptive poll across the next 24 h from now.
    # Using now + offset means the poll is always in the future regardless
    # of what time of day this runs — no timezone math, no midnight anchoring,
    # no "push to tomorrow" edge case.  Workers stay evenly loaded throughout
    # the day rather than being idle all day and overwhelmed at midnight.
    first_poll_at = time.time() + company.initial_slot_offset_s

    # Persist WARMING state before touching Redis
    db.execute("""
        UPDATE company_poll_stats
           SET warming_polls_remaining = 3,
               next_poll_at = %s
         WHERE company = %s
    """, (datetime.fromtimestamp(first_poll_at, tz=timezone.utc), company.name))

    redis.zadd("poll:adaptive", {company.name: first_poll_at})
    logger.info(
        "on_fullscan_complete: %s → WARMING (first poll in %.1fh, offset=%ds)",
        company.name,
        company.initial_slot_offset_s / 3600,
        company.initial_slot_offset_s,
    )
```

### Startup and restart: three-way categorisation

The monitoring day runs from **7 AM → 7 AM** (`CYCLE_START_HOUR = 7`), not midnight → midnight. `rebuild_redis()` uses this boundary to categorise companies:

| Category | Condition | Adaptive action | Full scan action |
|---|---|---|---|
| **NEW** | `last_poll_at IS NULL AND last_full_scan_at IS NULL` | NOT added — `on_fullscan_complete()` bootstraps after first full scan with `now + slot_offset` | ZADD `poll:fullscan` spread across startup window |
| **STALE** | has history; `next_poll_at` is NULL or before cycle start (last 7 AM) | ZADD `poll:adaptive` spread across recovery window (most-overdue first) — schedule is from a previous day, may be clustered | Full scan preserved if future; bumped to `now+300` if also stale |
| **CURRENT** | has history; `next_poll_at ≥ cycle start` | ZADD `poll:adaptive` with **stored DB timestamp** — no spread applied. If slightly past: immediately due. If future: scheduled as-is. | ZADD `poll:fullscan` with stored `next_full_scan_at` |

**Key design principle — DB is the source of truth for within-cycle companies.** The recovery spread is only applied when work is genuinely stale (from a previous monitoring day and potentially clustered). When companies were evenly distributed before the restart, their stored `next_poll_at` values already represent that distribution — restoring them directly preserves it.

The cycle boundary is resolved in this order:
1. `cycle:start` Redis key — written by `record_cycle_start()` after each `--monitor-jobs` digest. Most accurate.
2. Computed fallback — most recent `CYCLE_START_HOUR` in local time. Used on first startup before any cycle has completed.

### Spread window formula

When multiple companies need to be added to a queue simultaneously (startup, batch registration), they are spread across a dynamic window instead of all being queued at `now`:

```python
def spread_window_s(n: int, n_workers: int, avg_scan_s: float) -> float:
    """
    ceil(N / W) × avg_scan_s seconds.

    With N companies and W workers each taking avg_scan_s, it takes
    ceil(N/W) batches to process all of them once. Spreading companies
    evenly across this window means one company becomes due roughly
    every time a worker finishes its current scan — no thundering herd,
    no idle workers.

    Examples (2 workers, 30s avg):
        10  companies →  5 batches × 30s =  150s  ( 2.5 min)
        50  companies → 25 batches × 30s =  750s  (12.5 min)
       150  companies → 75 batches × 30s = 2250s  (37.5 min)
    """
    if n <= 0 or n_workers <= 0:
        return 0.0
    return math.ceil(n / n_workers) * avg_scan_s
```

### What this eliminates

| Old behaviour | New behaviour |
|---|---|
| All companies onboarded together → same slot forever | batch_position hash → spread from day 1 |
| Restart → all companies "overdue" → thundering herd | Three-way triage: STALE (before last 7 AM) → recovery spread; CURRENT (within today's cycle) → DB timestamps restored directly |
| Dawn patrol redistributing late polls each morning | Not needed — slots never cluster |
| Midnight anchoring creating daily wave (idle all day, burst at midnight) | `now + offset` — load distributed continuously throughout the day |
| Full scans all trigger in the same 5-minute window | Full scans trigger throughout the day as adaptive polls complete at different times |
| Last full scan finishes at 7:35 AM | Last full scan finishes well before 7 AM |

### reschedule_on_deploy.py — manual escape hatch only

> **Normal deployments do not need this script.** `rebuild_redis()` runs
> automatically at scheduler startup and handles all restart scenarios via
> the 7 AM cycle boundary — fresh deploys, long outages, and brief rolling
> restarts alike. See the startup rebuild section above.

`scripts/reschedule_on_deploy.py` exists for one exceptional case: Redis ZSET
scores have become **corrupted or severely clustered while workers are already
running** and you cannot afford a full restart to let `rebuild_redis()` fix
them. It rewrites scores in-place without stopping anything.

```bash
# Dry run — shows what would change, touches nothing
python scripts/reschedule_on_deploy.py --dry-run

# Live run — rewrites all ZSET scores
python scripts/reschedule_on_deploy.py

# Surgical: reset only one queue
python scripts/reschedule_on_deploy.py --adaptive-only
python scripts/reschedule_on_deploy.py --fullscan-only
```

**Algorithm:**
- `poll:adaptive` — each company's new score = `now + slot_offset(company)` — spreads companies deterministically across the next 24 h from now.
- `poll:fullscan` — each company's new score = `now + (slot_offset(company) / 86400 × full_scan_interval)` — maps the same [0, 86400) hash range into one full-scan window.

**Safety properties:**
- Idempotent — safe to run multiple times; companies always get the same relative ordering (scores shift with wall-clock but ordering holds).
- Does NOT clear the ZSETs — only updates existing member scores.
- Does NOT touch `company_poll_stats` in the DB — the DB is the historical record; Redis is the scheduling surface.
- Supports `--adaptive-only` and `--fullscan-only` flags if only one queue needs redistribution.
- Exits non-zero on any Redis connection failure so CI/CD can gate on it.

**When to reach for it (rare):**
- A bug or manual Redis edit has left scores clustered while workers are live and you cannot restart.
- You need to reset one queue surgically without touching the other.
- Always run `--dry-run` first to verify the scope before committing.

---

## 25. Glossary

**Adaptive interval:** A polling frequency that automatically adjusts based on observed company behavior. Computed from a 5-poll weighted queue with asymmetric smoothing.

**Asymmetric smoothing:** Interval change behavior where dropping (reactivation) is immediate and rising (going dormant) is gradual. Prevents missing hiring bursts while avoiding thrashing.

**ATS (Applicant Tracking System):** Software companies use to manage job postings. Examples: Greenhouse, Lever, Workday, iCIMS.

**Bloom filter:** A probabilistic data structure that efficiently checks "have I seen this before?" with a small false positive rate (0.1%). Used per-company for full scan dedup. Never produces false negatives.

**Cold start:** The first time a company is polled. All existing jobs marked `pre_existing` to prevent digest flooding.

**consecutive_empty:** Counter incremented when a poll finds 0 new jobs. Feeds into the 5-queue to drive the interval toward MAX.

**DC-level semaphore:** A concurrency limit applied at the Workday data center level (e.g., `myworkdayjobs_wd5`) rather than globally. Discovered dynamically from the `prospective_companies` DB.

**Detail fetch:** Fetching the full content of a single job. Only done for genuinely new jobs.

**EMA (Exponential Moving Average):** A smoothing technique. In this architecture, replaced by asymmetric smoothing applied to the 5-queue output.

**found_by:** Field on `job_postings` tracking whether a job was caught by `tier1_adaptive` or `tier2_full_scan`. Foundation of the miss_rate health metric.

**Heartbeat:** A short-TTL Redis key refreshed by a running worker. Still used by detail fetch workers (BRPOP-based) to detect stuck workers. Retired for adaptive and full scan workers — Stream PEL + `XAUTOCLAIM` handles their crash recovery without a separate heartbeat or watchdog.

**Incremental filtering:** Only processing jobs that are genuinely new since the last poll. Core efficiency mechanism.

**Listing fetch:** Fetching just job IDs and metadata. Much faster than detail fetches.

**MAX_INTERVAL / Tier 2 cap:** Score-tiered ceiling on adaptive intervals. 6h for companies at or above the calibrated `moderate` threshold (top 25% of active portfolio); 12h for all others. The cap boundary adjusts automatically with daily band calibration.

**miss_rate:** `tier2_new_jobs / total_new_jobs`. The headline metric for adaptive polling effectiveness.

**pending_detail:** Job status indicating it was found in a listing scan but detail has not yet been fetched. Persisted to DB so Redis queue can be rebuilt on restart.

**Poll:** One round of fetching the current job listing for a company.

**Priority queue:** A list where items are ordered by scheduled time. Implemented as Redis Sorted Sets.

**Progress heartbeat:** Short-TTL Redis key updated after each meaningful step in a scan. Distinguishes hung workers (no progress updates) from legitimately slow large reads.

**Redis:** Fast in-memory database for the poll queues, seen_ids, rate limiters, and detail queues. Rebuilt from PostgreSQL on restart — not the source of truth.

**Score:** The recency-weighted average of `recent_poll_counts` for a company (0.0 to 10+). Higher = posts more jobs = polls more frequently. A score of 0.0 means either insufficient poll history (< 3 polls) or no new jobs found across the entire rolling window — both result in the default 12h interval. Companies with score > 0 are ranked against each other via daily band calibration.

**Band calibration:** The daily process that computes `low`, `moderate`, and `active` score thresholds from the real distribution of `adaptive_score` values across the live portfolio. Run at cycle start and scheduler startup. Replaces hardcoded absolute thresholds with rank-based boundaries that reflect actual portfolio activity levels. Stored in Redis under `adaptive:band_thresholds`.

**DEFAULT_THRESHOLDS:** The fallback band thresholds used before the first band calibration runs (cold start or portfolio smaller than 5 active companies). Values: `low=1.5, moderate=3.5, active=6.0` — matches the original hardcoded design so behaviour is unchanged until real data is available.

**Winsorization:** Statistical technique used during band calibration. The top 5% of scores are replaced with the 95th-percentile value before computing rank boundaries. Prevents one extreme outlier (a company on a mass-hiring spike) from shifting thresholds upward and unintentionally demoting all other companies to slower polling intervals.

**Score-ascending order:** Full scan scheduling where dormant companies (low score) are processed first. Ensures active companies (already covered by adaptive) are last if workers run out of time.

**adaptive_seen:** Per-company Redis SET (`adaptive_seen:{company}`) tracking all job IDs touched by adaptive scan within the current day. 24h TTL. DEL'd at full scan start. Prevents redundant DB lookups when the same job ID appears across multiple adaptive scans within a single day. Never rebuilt on Redis restart — the next adaptive scan repopulates it naturally.

**bloom:fullscan:** Per-company RedisBloom filter (`bloom:fullscan:{company}`) representing the complete state of the job board as of the last full scan. 36h TTL. Built fresh each full scan cycle from ALL currently active job IDs. Used by full scan to skip DB checks and by adaptive scan for page-level early exit. Not rebuilt on Redis restart — next full scan runs as a cold start.

**Sliding window:** 10-minute Redis window tracking per-platform 429 + 404 errors for dynamic concurrency adjustment.

**Tier 1:** Adaptive polling. Score-driven variable intervals. Catches new jobs quickly for active companies.

**Tier 2:** Full scan. Exhaustive, own schedule, separate Redis ZSET. Safety net for what Tier 1 missed.

**Two-tier detail queue:** `queue:detail:adaptive` (high priority) and `queue:detail:fullscan` (low priority). Workers drain adaptive first.

**Pace mismatch:** The condition where scan workers produce new jobs into `queue:detail:adaptive` faster than detail workers can drain it. Resolved by the slow throughput check cascade: add detail workers first; if detail pool is at ceiling, stop adding scan workers; if detail queue still growing, reduce scan workers.

**Spike_factor:** `error_rate / (baseline_rate + 0.001)`. Measures how anomalous today's error rate is relative to the 30-day historical average. > 5× = concurrency-induced (aggressive reduction). ≤ 5× = normal variance (no action or cautious reduction).

**Liveness check:** Per-tick (5s) `process.is_alive()` check on all managed worker processes. Dead workers are replaced immediately — not at the next 5-minute or 30-minute monitoring interval.

**Hysteresis:** Requiring 2 consecutive check intervals showing the same signal before acting on it. Prevents thrashing (add worker → errors spike → remove worker → queue grows → add worker → loop).

**Weighted queue:** The 5-element `recent_poll_counts` array with recency-biased weights `[0.10, 0.15, 0.20, 0.25, 0.30]`. Replaces the composite score formula from earlier designs.

**Watchdog:** Background process checking for expired worker heartbeats. Re-queues orphaned companies using an error-rate-aware delay (backoff if the platform is still struggling; `score=now` if healthy).

**Exponential backoff (per-company):** Per-operation retry delay that doubles on each consecutive failure: 300s → 600s → 1200s → 2400s → 3600s → 86400s. Three independent counters per company (`retry:backoff:scan`, `retry:backoff:detail`, `retry:backoff:fullscan`), each with a 24h TTL so they auto-reset overnight.

**Learned ceiling:** The empirically discovered maximum safe in-flight scan count for a platform/DC key. Stored in Redis as `worker:ceil:learned:{dc_key}`. Set when error-triggered worker reduction fires; decays +1 per 24h of clean operation. Never pre-configured — only discovered through observed behaviour.

**In-flight ZSET (`inflight:scans:{dc_key}`):** Active sorted set tracking adaptive scan slots per DC key. Scored by dispatch timestamp; stale entries pruned by `ZREMRANGEBYSCORE` with `INFLIGHT_STALE_WINDOW_S`. Used together with `inflight:fullscans:{dc_key}` in the adaptive dispatch Lua script to enforce the per-DC ceiling atomically. Stream PEL (`XPENDING`) is monitoring-only — not used for ceiling enforcement.

**ATS outage mode:** State entered when 3 consecutive worker reductions fail to improve a platform's error rate. All dispatches for that platform are paused for 60 minutes (`worker:outage:{platform}` TTL key). Workers continue serving all other healthy platforms. A canary probe fires at 30 minutes for early recovery detection.

**Canary probe:** A single test request sent to a platform at the halfway point of an outage window. Success → outage mode cleared early. Failure → TTL reset for another full window.

**Scaling lock:** Short-TTL Redis key (`worker:scaling_lock:{platform}`, TTL = 30 min) set by `_fast_error_check_loop` after any error-triggered action. Prevents `_slow_throughput_check_loop` from immediately adding workers back and undoing a deliberate reduction.

**Consecutive reductions counter:** `worker:consec_reductions:{platform}` (TTL = 1h). Incremented each time a worker reduction fires for a platform without improving its error rate. Reaching 3 triggers outage mode detection.

**context (api_health column):** Operational context at the time a request was recorded. Values: `normal` (standard polling — used in baseline calculations), `backoff` (request made while company/platform is in exponential backoff), `canary` (single test probe during an outage window). Backoff and canary requests are stored but excluded from `query_30day_avg_error_rate()` and `query_30day_avg_response_ms()` to prevent managed-error periods from distorting the historical baseline.

**worker_scaling_events:** Append-only PostgreSQL table recording every worker scaling decision with full health context (error rate, baseline, spike factor, queue depths, worker counts before/after). Used for post-incident analysis and weekly health reporting. Effectiveness is derived by joining adjacent events within a time window — no row is ever updated after insert.

**batch_position:** A company's ordinal position within a single registration event (1, 2, 3 … N). Used exclusively to compute the first adaptive poll slot via `slot_offset(batch_position)` (see Section 24 for the canonical definition). Resets to 1 for every new registration batch. Replaced by `slot_offset(company_id)` after the WARMING phase completes. Not stored in the DB — computed transiently at registration time.

**Hash-based slot assignment:** The mechanism that distributes adaptive poll times evenly across any 24-hour window. Formula: `now + slot_offset(identifier)` where `slot_offset` is defined in Section 24. For new companies during NEW → WARMING bootstrap: identifier = batch_position, giving `first_poll_at = now + slot_offset(batch_position)`. For STABLE companies: the adaptive engine computes `now + interval` directly. Using `now + offset` (not `midnight + offset`) guarantees the result is always in the future regardless of time-of-day, avoids timezone math, and distributes load evenly throughout the day rather than creating a daily burst at midnight. Eliminates thundering herds permanently without requiring dawn patrol or any periodic redistribution.

**Thundering herd:** The condition where many companies become due simultaneously, overloading workers and the ATS concurrency ceiling. Root cause: companies onboarded together drift to the same time-of-day slot. Fixed by hash-based slot assignment (Section 24).

**stream:adaptive:{dc_key}:** Redis Stream used as the crash-safe delivery queue for adaptive listing scans. One stream per DC key (e.g. `stream:adaptive:workday_wd1`, `stream:adaptive:greenhouse`). Companies are XADD'd (with `MAXLEN ~ 1000`) by the adaptive dispatcher when their `poll:adaptive` score is due and the DC ceiling allows. Workers consume via XREADGROUP with a 500ms block timeout (short enough to react to `pipeline:pause` within ~1s). PEL + XAUTOCLAIM handle crash recovery — idle timeout = `p95_listing_scan_ms × 3`, self-calibrating. Consumer names are `worker-{hostname}-{pid}` to prevent PEL theft between workers. Consumer group created with `id=$` (new messages only); `BUSYGROUP` error on re-create is ignored. Dead-letter path after `MAX_STREAM_REDELIVERIES` (5) moves company to `poll:adaptive` with exponential backoff. Per-DC ceiling enforcement uses the `inflight:scans:{dc_key}` ZSET (ZADD on dispatch, ZREM on completion); PEL size (`XPENDING`) is available for monitoring but is not the primary ceiling gate.

**stream:fullscan:** Redis Stream used as the crash-safe delivery queue for full scan work. Single shared stream for all DC keys; `dc_key` is a field in each message, not part of the key. Companies are XADD'd (with `MAXLEN ~ 500`) by the full scan dispatcher when their `poll:fullscan` score becomes due. Workers consume via XREADGROUP with a 500ms block timeout. PEL + XAUTOCLAIM recover work from crashed workers — idle timeout = `p95_full_scan_ms × 3` (uses full scan duration from api_health, not listing scan duration). Consumer names are `worker-{hostname}-{pid}`. Consumer group created with `id=$`; `BUSYGROUP` ignored. Dead-letter after 5 redeliveries. Replaces the previous BLPOP/LIST approach.

**NEW → WARMING → STABLE:** The three lifecycle phases for a newly added company. NEW: full scan first (pre_existing mark), no adaptive yet. WARMING: 3 adaptive polls at fixed 2h interval using `initial_slot_offset_s` (stored in DB). STABLE: adaptive engine takes control, slot switches to `slot_offset(company_id)`, recurring Rules 3/5 govern full scans. Phase tracked by `warming_polls_remaining` column — NULL = STABLE, 1–3 = WARMING.

**slot_offset(identifier):** `int(hashlib.md5(str(identifier).encode()).hexdigest(), 16) % 86400`. Returns a deterministic, well-distributed number of seconds in [0, 86400). Must use MD5 (or equivalent proper hash) — Python's built-in `hash()` is an identity function for small integers and produces no distribution.

**warming_polls_remaining:** Column in `company_poll_stats` tracking WARMING phase progress. NULL = STABLE. 3, 2, 1 = WARMING (decremented on each `on_adaptive_complete()`). 0 = transition tick (this call completes WARMING; next slot uses `slot_offset(company_id)`). Persisted so WARMING state survives scheduler restarts.

**initial_slot_offset_s:** Column in `company_poll_stats` storing `slot_offset(batch_position)` computed at registration. Used as the adaptive poll slot during NEW and WARMING phases. Persisted so restart doesn't require knowing the original batch_position. Irrelevant once STABLE (`slot_offset(company_id)` is recomputed on the fly from the immutable company_id).

**on_fullscan_complete():** Callback fired when a company's first-ever full scan finishes. Transitions the company from NEW → WARMING: sets `warming_polls_remaining = 3`, schedules first adaptive poll at `now + initial_slot_offset_s` (always in the future — no midnight anchoring), ZADDs to `poll:adaptive`. Not called for subsequent full scans (only for the one that sets `last_full_scan_at` for the first time).

**Dead-letter (stream):** When `XAUTOCLAIM` re-delivers a stream message more than `MAX_STREAM_REDELIVERIES` (5) times, the message is considered un-processable in-stream. The company is moved to `poll:adaptive` (or `poll:fullscan`) with exponential backoff, and the stream message is `XACK`'d. Prevents broken companies from looping in the PEL indefinitely.

**p95_full_scan_ms vs p95_listing_scan_ms:** Two distinct p95 latency values from `api_health`, keyed by `(dc_key, scan_type)`. Used separately: listing scan p95 governs adaptive stream idle timeout and dispatcher ceiling check; full scan p95 governs fullscan stream idle timeout and Rule 5 time gate. Using the wrong value (listing scan p95 for full scan timeouts) would reclaim legitimate 30-min scans after 10 minutes.

**Rule 5 (pre-7 AM full scan trigger):** An addition to `on_adaptive_complete()` that triggers a full scan tonight when the next adaptive poll crosses to tomorrow morning AND the full scan for the current cycle has not yet run AND sufficient time remains before 7 AM. Prevents a full scan cycle from being silently skipped for companies with long adaptive intervals.

**Inflight expiry timestamp:** The score stored in `inflight:scans:{dc_key}` — the Unix timestamp when the adaptive slot was claimed (dispatch time). Used by `ZREMRANGEBYSCORE` to prune stale entries that were never cleaned up (e.g. worker crash before ZREM). Also stored in `inflight:fullscans:{dc_key}` for fullscan slots with a separate longer stale window (`INFLIGHT_FULLSCAN_STALE_S`).

**total_ms_ok (deferred):** A proposed `api_health` column tracking cumulative response time for successful (HTTP 200) requests only, separate from `total_ms` which includes all requests including timeouts. Deferred pending real data — the distortion from timeout inflation may be acceptable given that timed-out workers are genuinely occupied for that duration. To be revisited once production data confirms whether the difference is material for baseline accuracy.
