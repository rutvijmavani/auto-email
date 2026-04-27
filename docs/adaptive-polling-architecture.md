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
24. [Glossary](#24-glossary)

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
│                     ADAPTIVE SCHEDULER (Tier 1)                  │
│  Runs continuously (24/7)                                        │
│  Reads Redis ZSET poll:adaptive to know which company is due     │
│  Kicks off a listing scan worker for each due company            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LISTING SCAN WORKERS                          │
│  For each company when its scheduled time arrives:              │
│    1. Fetch job listing (IDs + titles + dates only)             │
│    2. Smart early exit (stop when mostly-seen jobs appear)      │
│    3. Diff new IDs against job_postings (DB dedup)              │
│    4. Push only NEW job IDs to queue:detail:adaptive            │
│    5. Update this company's poll statistics                     │
│    6. Compute next poll time and reschedule                     │
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
│                    FULL SCAN SCHEDULER (Tier 2)                  │
│  Separate Redis ZSET poll:fullscan                              │
│  Score-ascending order: dormant companies first, active last    │
│  Prerequisite: at least 1 adaptive poll completed today         │
│  Kicks off a full scan worker for each due company              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
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
│    7. Self-reschedule on completion                             │
└─────────────────────────────────────────────────────────────────┘

Supporting infrastructure:
┌─────────────────────┐  ┌─────────────────────┐
│  Redis              │  │  PostgreSQL          │
│  • poll:adaptive    │  │  • job_postings      │
│  • poll:fullscan    │  │  • company_poll_stats│
│  • Bloom filters    │  │  • company_config    │
│  • Detail queues    │  │  • adaptive_poll_    │
│  • Rate limiters    │  │    metrics           │
│  • Pub/Sub channels │  │  • api_health        │
│  • Sliding windows  │  │  • custom_ats_diag.  │
│  • Worker heartbeats│  │                      │
└─────────────────────┘  └─────────────────────┘

Dynamic worker pool:
  Calculated at 7 AM: workers = (total_scans × avg_duration) / window_seconds
  Throughput monitor runs every 30 min, adjusts count up/down
  Worker count always ≤ DB maxconn - 3 (reserve connections for maintenance)
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

```
At 7 AM cycle start:
  1. Store cycle_start timestamp → redis.set("cycle:start", now)
  2. Run dawn_patrol() — redistribute any adaptive polls due after 11 AM
     into the 7 AM–9 AM window (Rule 2)
  3. Dynamic worker count recalculated for this cycle

Every second (adaptive scheduler):
  1. Check poll:adaptive — which companies have next_poll_time <= now?
  2. ZPOPMIN (atomic) — pop due companies
  3. SET heartbeat:{company} EX 300 (worker heartbeat)
  4. Start a listing scan worker for each one
  5. After worker completes: on_adaptive_complete(company)
       a. Update company stats (recent_poll_counts, score, interval)
       b. Reschedule company back into poll:adaptive (never orphaned)
       c. If full scan is due (elapsed >= full_scan_interval_s):
            ZADD poll:fullscan {company: now + 300}  ← Rule 3: 5-min buffer

Full scan dispatcher (runs separately):
  1. Check poll:fullscan — which companies have score <= now?
  2. ZPOPMIN (atomic) — pop due companies
  3. Rule 4 pre-check: did adaptive run today?
       If not: ZADD poll:adaptive {company: now}
               ZADD poll:fullscan {company: now + 900}
               return  ← come back after adaptive runs
  4. SET fullscan:lock:{company} NX EX 3600 (prevent double-dispatch)
  5. Start a full scan worker
  6. After completion: DEL fullscan:lock:{company}
     Full scan does NOT self-reschedule — next reschedule comes from
     the next on_adaptive_complete() call (Rule 3)
```

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

```python
def band_lookup(score):
    if score is None:
        return 12 * 3600    # default before 3 polls of history
    elif score < 0.5:
        return 12 * 3600    # 12h — dormant
    elif score < 1.5:
        return  9 * 3600    # 9h  — low activity
    elif score < 3.5:
        return  6 * 3600    # 6h  — moderate
    elif score < 6.0:
        return  4 * 3600    # 4h  — active
    else:
        return  3 * 3600    # 3h  — very active (MIN_INTERVAL)
```

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
def update_poll_interval(company, new_jobs_found):
    # Step 1: Update queue
    counts = company.recent_poll_counts or []
    counts.append(min(new_jobs_found, 10))   # cap at 10
    if len(counts) > 5:
        counts.pop(0)
    
    # Step 2: Compute score
    score = compute_score(counts)
    
    # Step 3: Band lookup
    computed = band_lookup(score)
    
    # Step 4: Asymmetric smoothing
    new_interval = compute_next_interval(computed, company.current_interval_s)
    
    # Step 5: Apply MAX_INTERVAL cap (see Section 8)
    max_interval = get_max_interval(score)
    final_interval = min(new_interval, max_interval)
    
    # Step 6: Save
    company.recent_poll_counts = counts
    company.current_interval_s = final_interval
    company.adaptive_score = score or 0.0
    company.next_poll_at = now() + final_interval
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

**Rule 2 — Dawn patrol at cycle start (redistribute late polls)**
At 7 AM, any adaptive poll scheduled after 11 AM is redistributed to run within the first 2 hours of the cycle. Prevents dormant companies from clustering their adaptive polls in the afternoon.

```python
def dawn_patrol():
    cycle_start = float(redis.get("cycle:start"))
    threshold   = cycle_start + (4 * 3600)   # anything due after 11 AM
    late        = redis.zrangebyscore("poll:adaptive", threshold, "+inf")
    if not late:
        return
    spread = 2 * 3600   # redistribute across 7 AM → 9 AM
    step   = spread / max(len(late), 1)
    for i, company in enumerate(late):
        new_time = cycle_start + (i * step) + random.uniform(0, step)
        redis.zadd("poll:adaptive", {company: new_time})
```

**Rule 3 — Adaptive triggers full scan directly (structural enforcement)**
When adaptive completes, it checks whether full scan is due and queues it automatically. Full scan can only enter `poll:fullscan` via this path — never independently.

```python
def on_adaptive_complete(company):
    update_stats(company)
    reschedule_adaptive(company)

    if should_trigger_full_scan(company):
        redis.zadd("poll:fullscan", {company.name: time.time() + 300})

def should_trigger_full_scan(company):
    if company.last_full_scan_at is None:
        return True   # never been full-scanned
    elapsed = time.time() - company.last_full_scan_at.timestamp()
    return elapsed >= company.full_scan_interval_s   # 24h
```

Because full scan is triggered 5 minutes after adaptive, the adaptive-first rule is enforced by design — full scan physically cannot run before adaptive.

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
| Dormant company, last polled 11 PM | Rule 1 + Rule 2 → adaptive by 9 AM at latest |
| Many dormant companies clustered overnight | Rule 2 → spread evenly across 7–9 AM |
| New company, never polled | Rule 4 → triggers adaptive before allowing full scan |
| Adaptive keeps erroring | Rule 4 → keeps retrying before allowing full scan |
| Active company, 4h interval | Rule 3 → full scan triggered once per day naturally |
| System restart mid-day | Rule 4 → pre-check catches any companies not yet polled |

---

## 8. Adaptive MAX_INTERVAL Cap

The adaptive engine can compute very long intervals for dormant companies. The MAX_INTERVAL cap ensures no company goes unpolled for too long — this is what guarantees reactivation detection within a bounded window.

The cap is **score-tiered** — active companies get a tighter cap than dormant ones:

```python
def get_max_interval(score):
    if score is None or score < 0.5:
        return 12 * 3600   # 12 hours — dormant/slow company
    elif score < 1.5:
        return 12 * 3600   # 12 hours — low activity
    else:
        return  6 * 3600   # 6 hours  — active/high-velocity company
```

| Activity Level | Score Range | Natural band | MAX_INTERVAL |
|---|---|---|---|
| **Very active** | score ≥ 3.5 | 3–4h | 6h |
| **Moderate** | 1.5 ≤ score < 3.5 | 6h | 6h |
| **Low activity** | 0.5 ≤ score < 1.5 | 9h | 12h |
| **Dormant** | score < 0.5 | 12h | 12h |

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

### Scheduling strategy

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

    if should_trigger_full_scan(company):
        redis.zadd("poll:fullscan", {company.name: time.time() + 300})

def should_trigger_full_scan(company):
    if company.last_full_scan_at is None:
        return True   # never been full-scanned → trigger immediately
    elapsed = time.time() - company.last_full_scan_at.timestamp()
    return elapsed >= company.full_scan_interval_s   # 86400s (24h)
```

This makes the adaptive-first rule structural: full scan **cannot** enter `poll:fullscan` without an adaptive poll having just completed. The 5-minute offset (`time.time() + 300`) gives detail workers a head start on any new jobs adaptive just found before the full scan begins.

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

Each company has its own Bloom filter in Redis built from the previous full scan:

```
Key:  bloom:fullscan:{company}
TTL:  36 hours (24h cycle + 12h buffer for timing drift)
Size: ~0.5MB per company at 0.1% error rate
```

Full scan flow with Bloom filter:

```
1. Check: does bloom:fullscan:{company} exist?
   NO  → cold start, process everything (first scan)
   YES → use as "already seen" reference

2. For each job on each page:
   if job_id IN bloom:fullscan:{company}:
       skip   ← 99.9% accurate "seen before"
   else:
       upsert into DB
       tag found_by='tier2_full_scan' only if genuinely new
       add to NEW_BLOOM (building for next cycle)

3. Full scan completes:
   SET bloom:fullscan:{company} = NEW_BLOOM
   EXPIRE bloom:fullscan:{company} 36h
   Self-reschedule in poll:fullscan
```

**False positives (0.1% error rate):** 1 in 1000 new jobs incorrectly skipped. Since adaptive polling runs first and throughout the day, the skipped job was almost certainly already detected by Tier 1. Worst case: 24-hour delay on 1 in 1000 new jobs — acceptable.

**Fallback if RedisBloom unavailable:** Use Redis SET for dedup. Less memory efficient (~100MB vs ~4.5MB at scale) but functionally correct.

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

Worker count is calculated at 7 AM cycle start:

```python
def calculate_required_workers():
    total_companies = count_active_companies()
    avg_scan_duration = get_avg_full_scan_duration_seconds()
    window_seconds = 23 * 3600   # 7 AM to 6 AM next day
    
    required = math.ceil(
        (total_companies * avg_scan_duration) / window_seconds
    )
    
    max_allowed = DB_POOL_MAXCONN - 3   # reserve 3 for maintenance
    return min(required, max_allowed)
```

A throughput monitor runs every 30 minutes:
- Queue depth growing → add workers (up to max)
- Queue consistently empty → reduce workers (down to min of 2)

---

## 10. Dormant Company Reactivation

### The problem

A company that has been silent for 3 months (score ≈ 0, interval = 24 hours) suddenly starts posting jobs. How quickly does the system respond?

### How reactivation works with asymmetric smoothing

Because we use asymmetric smoothing (no dampening when interval is dropping), reactivation is **immediate** rather than gradual:

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

```python
def smart_paginate(company, ats, seen_ids):
    page              = 0
    overlap_pages     = 0
    OVERLAP_THRESHOLD = 0.80   # 80% of a page is already seen
    CONFIRM_PAGES     = 2      # need 2 consecutive high-overlap pages to stop

    while True:
        jobs = fetch_listing_page(company, ats, page)
        if not jobs:
            break

        seen_on_page  = sum(1 for j in jobs if j.job_id in seen_ids)
        overlap_ratio = seen_on_page / len(jobs)

        if overlap_ratio >= OVERLAP_THRESHOLD:
            overlap_pages += 1
            if overlap_pages >= CONFIRM_PAGES:
                break   # 2 consecutive pages 80%+ seen — past the new-job frontier
        else:
            overlap_pages = 0   # found new jobs → reset, keep going

        page += 1
```

### NON-SORTED platforms — no early exit in adaptive polling

For Workday, iCIMS, and custom ATS with no reliable ordering:

- **Adaptive polling**: scan pages until a time-based cutoff (e.g., skip jobs with `postedOn` older than 72 hours)
- **Full scan**: scan ALL pages unconditionally — no cutoff, no early exit

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

For each company, Redis holds a SET of all known job IDs:

```
seen:{company}  →  {"123456", "123457", "123458", ...}
```

When we fetch a new listing page and get 50 job IDs back:

```python
new_ids = fetched_ids - redis_seen_set["Stripe"]
```

Only the jobs in `new_ids` proceed to the detail fetch stage. This set subtraction is O(N) in Redis and takes microseconds even for 20,000 job IDs.

**Source of truth for the Redis SET:** `job_postings` table (using the `job_id` + `company` columns). On Redis restart, the SET is rebuilt from `job_postings` in ~30 seconds. No separate `seen_job_ids` table is needed — `job_postings` already stores everything we need.

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

### The three layers

Deduplication operates at three independent layers:

```
Layer 1: In-cycle set (per poll)
  → Catches duplicates within a single scan (pagination overlap)
  → Python set, lives for milliseconds, tiny memory
  → cycle_seen = set(); if job_id in cycle_seen: skip

Layer 2: Bloom filter (full scan only)
  → Cross-cycle dedup for full scan
  → Per-company Redis Bloom filter, 36h TTL, 0.1% false positive
  → ~0.5MB per company vs ~20MB for Redis SET at same scale

Layer 3: Database unique constraint
  → Final correctness guarantee for both adaptive and full scan
  → UNIQUE(company, job_id) on job_postings
  → Upsert pattern — duplicate inserts are safe no-ops
```

### Why Bloom filter only for full scan, not adaptive polling

Adaptive polling already uses the incremental diff (new_ids = fetched - known). It never tries to insert a known job — so no Bloom filter is needed. The DB check is the right layer for adaptive polling.

Full scan processes every job on every page. Without Bloom filter, it would DB-check thousands of already-known jobs. Bloom filter eliminates ~99.9% of those checks.

### Memory comparison

| Approach | Memory at 2.5M job IDs |
|---|---|
| PostgreSQL job_postings (existing table) | no extra cost — already present |
| Redis SET seen:{company} (adaptive dedup) | ~100MB RAM |
| Redis Bloom filter (0.1% error, full scan) | ~4.5MB RAM |

### Bloom filter false positives

0.1% error rate = 1 in 1000 new jobs incorrectly skipped during full scan.

This is acceptable because:
1. Adaptive polling ran first — likely already caught the job
2. Next day's full scan picks it up (fresh Bloom filter, job absent from it)
3. Maximum delay: 24 hours on 1 in 1000 new jobs

### Adaptive-first guarantee

Full scan only runs after at least one adaptive poll has completed for that company today (see Section 7). This ensures that false positives — when they do occur — are on jobs already detected by adaptive polling.

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

ZADD poll:adaptive {timestamp} {company}    — schedule adaptive poll
ZPOPMIN poll:adaptive                        — get next due company (atomic)
```

#### 2. Full Scan Queue (Sorted Set)

```
Key: "poll:fullscan"
Type: Sorted Set (ZSET)
Score: Unix timestamp of next full scan time
       (score-ascending = dormant companies get lower scores = polled first)

ZADD poll:fullscan {timestamp} {company}    — schedule full scan
ZPOPMIN poll:fullscan                        — get next due company
```

#### 3. Detail Fetch Queues (Two-Tier Lists)

```
Keys: "queue:detail:adaptive"  (high priority — from listing scan)
      "queue:detail:fullscan"  (low priority  — from full scan)
Type: List

LPUSH queue:detail:adaptive {job_json}    — push from adaptive listing scan
LPUSH queue:detail:fullscan {job_json}    — push from full scan
BRPOP queue:detail:adaptive 0.1           — workers drain adaptive first
BRPOP queue:detail:fullscan 5             — then fullscan queue
```

Backpressure: listing scan checks queue depth before pushing. If depth > MAX_QUEUE_DEPTH (5,000 for adaptive, 2,000 for fullscan), listing scan pauses until workers catch up.

#### 4. Seen Job IDs (Sets, one per company)

```
Key: "seen:{company}"
Type: Set

SADD seen:{company} {job_id}             — mark job as seen
SISMEMBER seen:{company} {job_id}        — check if known (O(1))
SDIFF {fetched_set} seen:{company}       — find new IDs (O(N))
```

#### 5. Per-Company Bloom Filters (Full Scan)

```
Key: "bloom:fullscan:{company}"
Type: RedisBloom (BF)
TTL: 36 hours
Error rate: 0.1%

BF.ADD bloom:fullscan:{company} {job_id}
BF.EXISTS bloom:fullscan:{company} {job_id}
```

Fallback if RedisBloom unavailable: Redis SET at key `bloom:fallback:{company}`.

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

#### 8. Worker Heartbeats

```
Key: "heartbeat:{company}"
Type: String with TTL

SET heartbeat:{company} "processing" EX 300   — set on worker start
EXPIRE heartbeat:{company} 300                 — refresh every 30 seconds
DEL heartbeat:{company}                        — clear on completion

Watchdog: if key expires while company not in queue → company orphaned → requeue
```

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

#### 11. Full Scan Exclusive Lock

```
Key: "fullscan:lock:{company}"
Type: String with TTL

SET fullscan:lock:{company} "1" NX EX 3600   — only if not exists, 1-hour TTL
Prevents double-dispatch if scheduler restarts mid-cycle
```

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
| seen:{company} | `job_postings WHERE company=X` (job_id column) |
| bloom:fullscan:* | Next full scan is a cold start (expensive but correct) |
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
    -- Used to rebuild Redis seen:{company} SET on restart.
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

#### `api_health` — Request tracking with error sub-types

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
1. Digest email sent with all status='new' jobs
2. UPDATE job_postings SET status='digested' WHERE status='new'
   (send FIRST, mark digested AFTER — prevents silent loss if send fails)
3. cycle_start Unix timestamp written to Redis: SET cycle:start {timestamp}
4. Dynamic worker count calculated and applied
5. Adaptive polling workers already running — they continue uninterrupted
6. Full scan scheduler begins dispatching based on score-ascending order
   (after each company's adaptive poll completes for the day)
```

### Adaptive polling — ongoing

```
Adaptive poll for company Stripe:

1. Scheduler: ZPOPMIN poll:adaptive → "Stripe"
   SET heartbeat:Stripe EX 300

2. Listing scan:
   Fetch Stripe listing pages with smart early exit
   SET progress:Stripe "fetching_page_1" EX 120  (updated each page)
   new_ids = fetched_ids - known_ids[Stripe] from job_postings

3. For each new ID:
   UPDATE job_postings SET status='pending_detail' (INSERT if first time)
   LPUSH queue:detail:adaptive {stripe_job_json}

4. Stats update:
   Update recent_poll_counts queue
   Compute new interval (asymmetric smoothing)
   Update company_poll_stats

5. Reschedule: ZADD poll:adaptive {now + interval} "Stripe"
   DEL heartbeat:Stripe

Meanwhile, detail workers:
   BRPOP queue:detail:adaptive → job
   Fetch full detail
   UPDATE job_postings SET status='new', found_by='tier1_adaptive', ...
   SADD seen:Stripe {job_id}   ← Redis SET for fast in-process dedup
```

### Full scan flow

```
Full scan for company Workday_Corp:

1. Pre-check: has adaptive polled today?
   last_poll_at >= cycle_start → YES → proceed

2. Exclusive lock: SET fullscan:lock:Workday_Corp 1 NX EX 3600
   If fails (lock exists) → skip, requeue

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
       ZADD poll:fullscan {time.time()-1} "Workday_Corp"  ← immediate re-pickup
       DEL fullscan:lock:Workday_Corp
       Return — wait for resume

5. Full scan completes:
   BF.RESERVE / SET bloom:fullscan:Workday_Corp = NEW_BLOOM, EXPIRE 36h
   UPDATE company_poll_stats SET
     full_scan_interrupted=FALSE,
     last_full_scan_at=NOW(),
     next_full_scan_at=NOW() + full_scan_interval_s
   ZADD poll:fullscan {next_full_scan_at} "Workday_Corp"
   DEL fullscan:lock:Workday_Corp
   DEL heartbeat:Workday_Corp
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
4. `seen:{company}` ← from `job_postings` (company + job_id columns)
5. Stats cache ← from `company_poll_stats`
6. Bloom filters ← cold start on next full scan (correct but expensive)

**Memory full (OOM):**
Redis eviction policy must be set to `noeviction`. Redis refuses new writes instead of silently evicting critical keys. Workers handle write failures gracefully (retry, alert) rather than losing data unknowingly.

**Connection loss:**
Workers reconnect with exponential backoff. If Redis unreachable for > 2 minutes: pause workers, alert, resume when connection restores.

### Worker failures

**Orphaned companies (worker crashes after ZPOPMIN):**
Worker heartbeat with 5-minute TTL. Watchdog process runs every 60 seconds:
```
If heartbeat:{company} expired AND company not in poll queue:
    → company orphaned → requeue with score=now
```
Applies to both adaptive workers and full scan workers.

**Hung workers (stuck HTTP request or frozen between requests):**
Two-layer detection:
1. Per-HTTP-request timeout: `connect_timeout=10s, read_timeout=30s`
   Kills truly dead connections without disrupting legitimate large reads.
2. Progress heartbeat TTL (120 seconds):
   Updated after every meaningful step (each page processed, each DB write).
   If progress key expires while heartbeat is still alive → worker is frozen
   between requests → watchdog kills and requeues.

This correctly distinguishes a SAP XML dump taking 3 minutes (many progress updates)
from a worker frozen waiting on a dead DB connection (no progress updates).

**Worker process killed by OS (OOM killer, SIGKILL):**
Supervisor (systemd/supervisord) restarts worker processes automatically.
Orphaned company detection catches any companies left in limbo.

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

Before starting any full scan:
```python
acquired = redis.set(f"fullscan:lock:{company}", "1", nx=True, ex=3600)
if not acquired:
    return   # another worker is already scanning this company
```
If worker crashes mid-scan, lock expires after 1 hour → automatically released.

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

### Sliding window rate tracking

A 10-minute sliding window in Redis tracks 429s AND 404s per platform:

```
Key: "errwin:{platform}:{10min_bucket}"
Fields: total_requests, errors_429, errors_404

error_rate = (errors_429 + errors_404) / total_requests (last 10 min)
```

Both 429 (explicit rate limit) and 404 (Workday-specific overload response) are tracked because Workday returns 404 under concurrency overload rather than a standard 429.

### Feedback loop

After each request batch, the concurrency controller adjusts:

```python
def adjust_concurrency(platform, error_rate):
    current = get_semaphore_count(platform)
    
    if error_rate > 0.10:    # >10% errors → reduce concurrency
        new_count = max(current - 1, platform_min[platform])
    elif error_rate < 0.02:  # <2% errors → cautiously increase
        new_count = min(current + 1, platform_max[platform])
    else:
        new_count = current  # stable range, no change
    
    set_semaphore_count(platform, new_count)
```

### Workday DC-level semaphores

Workday uses regional data centers. Requests to different companies may hit different DCs, each with its own rate limits.

DC key is discovered dynamically from `prospective_companies` table — NOT hardcoded:

```python
def _extract_workday_dc_key(career_url):
    """
    Extract DC identifier from Workday URL.
    Handles both myworkdayjobs.com and workdaysites.com variants.
    
    Examples:
      https://amazon.myworkdayjobs.com/en-US/...   → "myworkdayjobs_wd5"
      https://nike.wd1.myworkdayjobs.com/...        → "myworkdayjobs_wd1"
      https://company.workdaysites.com/...          → "workdaysites_wd3"
    """
    import re
    
    # Pattern: optional wd{N} subdomain in myworkdayjobs URLs
    m = re.search(r'(wd\d+)\.myworkdayjobs\.com', career_url)
    if m:
        return f"myworkdayjobs_{m.group(1)}"
    
    if 'myworkdayjobs.com' in career_url:
        return "myworkdayjobs_default"
    
    m = re.search(r'(wd\d+)\.workdaysites\.com', career_url)
    if m:
        return f"workdaysites_{m.group(1)}"
    
    if 'workdaysites.com' in career_url:
        return "workdaysites_default"
    
    return None
```

At startup, DC keys are discovered by querying the DB:
```python
def discover_workday_dc_keys():
    rows = db.execute("""
        SELECT DISTINCT career_page_url FROM prospective_companies
        WHERE ats_platform IN ('workday', 'workdaysites')
    """).fetchall()
    
    dc_keys = set()
    for row in rows:
        key = _extract_workday_dc_key(row["career_page_url"])
        if key:
            dc_keys.add(key)
    
    return dc_keys   # e.g. {"myworkdayjobs_wd1", "myworkdayjobs_wd5", "workdaysites_wd3"}
```

Each discovered DC key gets its own semaphore, starting at a conservative default and adjusting dynamically via the feedback loop.

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

### Fix 2 — Baseline deviation detection

Compare today's error rate against the 30-day historical baseline:

```python
def is_concurrency_induced_error(platform):
    today_rate    = get_todays_error_rate(platform)
    baseline_rate = get_30day_avg_error_rate(platform)
    spike_factor  = today_rate / (baseline_rate + 0.001)
    return spike_factor > 5   # 5x above 30-day baseline = concurrency problem
```

Used together:

| Error sub-type | Spike factor | Diagnosis | Action |
|---|---|---|---|
| `requests_timeout` | > 5x | Concurrency overload | Reduce semaphore count |
| `requests_5xx` | > 5x | Soft rate limit | Reduce concurrency + add delay |
| `requests_conn_err` | > 5x | Network issue | Check connectivity, not concurrency |
| `requests_other_err` | > 5x | API structure changed | Check `custom_ats_diagnostics` |
| Any type | ≤ 5x baseline | Normal noise | No action needed |

This is what makes the 32-error Workday spike diagnosable: all 32 were `requests_timeout`, spike factor was ~32x the baseline → concurrency-induced → fix was reducing parallel Workday workers, not investigating the API.

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

Workers are not hardcoded. The required count is calculated at each 7 AM cycle:

```python
required_workers = ceil(
    (total_companies × avg_scan_duration_s) / (23 × 3600)
)
max_workers = DB_POOL_MAXCONN - 3
actual_workers = min(required_workers, max_workers)
```

Throughput monitor adjusts every 30 minutes based on actual queue drain rate.

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

**What:** Set up Redis. Implement `seen:{company}` SETs. Rate limiter. Two-tier detail fetch queue (adaptive + fullscan). Backpressure on listing scan. `pending_detail` status + queue rebuild on restart.

**Deliverable:** Seen_ids lookups sub-millisecond. Detail fetches parallel with backpressure.

**Estimated effort:** 2–3 days

---

### Phase 4 — Priority Queue Scheduler

**What:** `pipeline/scheduler.py`. Replace cron batch with continuous Redis ZSET loop. Two queues: `poll:adaptive` and `poll:fullscan`. Worker heartbeats + watchdog. Full scan exclusive lock. Clock using Redis TIME + `America/New_York` pytz.

**Deliverable:** Continuous monitoring. Companies on individual schedules. Orphan detection.

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

**What:** 10-minute sliding window for 429 + 404 per platform. Feedback loop adjusting semaphores. Workday DC-level semaphores discovered dynamically from DB. `_extract_workday_dc_key()` handling both myworkdayjobs and workdaysites variants.

**Deliverable:** No more concurrency-induced error spikes. Workday per-DC rate limiting.

**Estimated effort:** 2 days

---

### Phase 9 — Error Type Differentiation

**What:** Replace `requests_error` with four sub-type columns. Baseline deviation function (30-day average). Per-platform error classification at call site. Weekly health report updated.

**Deliverable:** Concurrency-induced errors distinguishable from genuine API failures.

**Estimated effort:** 1–2 days

---

### Phase 10 — Resilience Hardening

**What:** Redis `noeviction` policy. Watchdog for orphaned companies. Progress heartbeat for hung worker detection. Cron chain heartbeat + `db:maintenance` flag. `cronchain:alive` auto-resume on expiry. All-ATS failure correlation check. Bloom filter corruption detection.

**Deliverable:** System degrades gracefully under all identified failure modes.

**Estimated effort:** 2–3 days

---

### Phase 11 — Monitoring and Alerting

**What:** Weekly adaptive health report in Monday digest. Per-ATS miss rate dashboard. Error streak alerting. Redis memory monitoring. Detail queue depth alerting.

**Estimated effort:** 1–2 days

---

## 24. Glossary

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

**Heartbeat:** A short-TTL Redis key refreshed by a running worker. Expiry signals worker crash to the watchdog.

**Incremental filtering:** Only processing jobs that are genuinely new since the last poll. Core efficiency mechanism.

**Listing fetch:** Fetching just job IDs and metadata. Much faster than detail fetches.

**MAX_INTERVAL / Tier 2 cap:** Score-tiered ceiling on adaptive intervals. 4h for active, 12h for moderate, 24h for dormant companies.

**miss_rate:** `tier2_new_jobs / total_new_jobs`. The headline metric for adaptive polling effectiveness.

**pending_detail:** Job status indicating it was found in a listing scan but detail has not yet been fetched. Persisted to DB so Redis queue can be rebuilt on restart.

**Poll:** One round of fetching the current job listing for a company.

**Priority queue:** A list where items are ordered by scheduled time. Implemented as Redis Sorted Sets.

**Progress heartbeat:** Short-TTL Redis key updated after each meaningful step in a scan. Distinguishes hung workers (no progress updates) from legitimately slow large reads.

**Redis:** Fast in-memory database for the poll queues, seen_ids, rate limiters, and detail queues. Rebuilt from PostgreSQL on restart — not the source of truth.

**Score:** The weighted average of `recent_poll_counts` for a company (0 to 10+). Higher = post more jobs = poll more frequently.

**Score-ascending order:** Full scan scheduling where dormant companies (low score) are processed first. Ensures active companies (already covered by adaptive) are last if workers run out of time.

**seen_ids:** Set of job IDs already processed for a company. Redis SET for fast lookups (`seen:{company}`). Rebuilt from `job_postings.job_id` on Redis restart — no separate table needed.

**Sliding window:** 10-minute Redis window tracking per-platform 429 + 404 errors for dynamic concurrency adjustment.

**Tier 1:** Adaptive polling. Score-driven variable intervals. Catches new jobs quickly for active companies.

**Tier 2:** Full scan. Exhaustive, own schedule, separate Redis ZSET. Safety net for what Tier 1 missed.

**Two-tier detail queue:** `queue:detail:adaptive` (high priority) and `queue:detail:fullscan` (low priority). Workers drain adaptive first.

**Weighted queue:** The 5-element `recent_poll_counts` array with recency-biased weights `[0.10, 0.15, 0.20, 0.25, 0.30]`. Replaces the composite score formula from earlier designs.

**Watchdog:** Background process checking for expired worker heartbeats. Requeues any orphaned companies.
