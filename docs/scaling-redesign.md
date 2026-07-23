# Pipeline Scaling Redesign — Decision Record

> **Status**: Manager architecture implemented. `workers/manager.py` and the `manager:cmds` channel are live; remaining scaffolding (config constants, watchdog extensions) is in progress.  
> **Purpose**: Capture every agreed architectural decision. `adaptive-polling-architecture.md` Section 26 is the companion implementation reference.  
> **Date**: 2026-07-20

---

## 1. The Problem with the Current Design

The current system uses five absolute depth thresholds spread across two files with no shared formula:

| Threshold | Value | File | Purpose |
|---|---|---|---|
| `DETAIL_QUEUE_WARN` | 100 | `watchdog.py` (hardcoded) | Warn level |
| `DETAIL_QUEUE_ALERT` | 500 | `watchdog.py` (hardcoded) | CRITICAL trigger → restart scheduler |
| `DETAIL_QUEUE_HIGH_WATERMARK` | 1000 | `config.py` → `scheduler.py` | Layer 3 add-worker trigger |
| `DETAIL_QUEUE_MAX_FULLSCAN` | 2000 | `config.py` → `fullscan.py` | Fullscan backpressure (actually pauses) |
| `DETAIL_QUEUE_MAX_ADAPTIVE` | 5000 | `config.py` → `scan_worker.py` | Adaptive backpressure (LOGS ONLY — does not pause) |

**Why this fails:**

1. **Inverted escalation ladder.** Watchdog fires a destructive action (restart scheduler → rebuild detail queue) at depth=500. Scheduler adds workers (constructive action) at depth=1000. The nuclear option fires before the proportional one.

2. **Burst vs stall are indistinguishable.** Accenture pushes 585 jobs in one minute. Depth spikes past 500. Watchdog calls it a CRITICAL stall. It is not — it is a healthy burst. Queue delay would be 0 seconds because workers are actively consuming. Depth alone cannot tell the difference.

3. **Numbers are divorced from fleet size.** 500 jobs with 1 detail worker is catastrophic. 500 jobs with 10 detail workers is 50 jobs per worker — fine. The threshold means different things depending on how many workers are alive, but the number never changes.

4. **Watchdog and scheduler are independent.** They read the same queues but with different thresholds and make conflicting decisions without coordinating. One is trying to destroy and restart; the other is trying to add capacity.

5. **No cross-queue awareness.** Layer 3 only monitors `REDIS_DETAIL_ADAPTIVE`, ignoring `REDIS_DETAIL_FULLSCAN`. Worker removal picks by pool type, not by which queue has the healthiest delay.

---

## 2. Core Principle Shift: Queue Delay as the Master Metric

**Old metric**: `queue_depth` (how many jobs are waiting)  
**New metric**: `queue_delay` (how long the oldest waiting job has been waiting)

### Why queue delay is the right signal

A queue with 5,000 jobs and 0-second delay is healthy — workers are keeping up.  
A queue with 100 jobs and 20-minute delay is in trouble — jobs are stuck.

Queue delay encodes both queue size AND worker throughput in one number. It is the user-visible consequence: delay is what causes jobs to appear stale in the DB, what makes a full scan take longer than a day, what makes the pipeline miss postings.

### How to calculate it

`enqueued_at` is already written into every job payload at push time:
- `scan_worker.py:721` → `"enqueued_at": datetime.now(timezone.utc).isoformat()`
- `fullscan.py:693` → same

Redis LIST semantics: `LPUSH` adds to left (index 0 = newest); `BRPOP` consumes from right (index -1 = oldest). So:

```
oldest_job_raw  = redis.LINDEX("queue:detail:adaptive", -1)
oldest_job      = json.loads(oldest_job_raw)
queue_delay_s   = (now_utc - parse_iso(oldest_job["enqueued_at"])).total_seconds()
```

This `LINDEX -1` delay calculation applies to the **two detail queues only**:
- `queue:detail:adaptive`
- `queue:detail:fullscan`

Scan work does not use Redis LISTs. Scan delay is measured differently: the score stored in the `poll:adaptive` and `poll:fullscan` ZSETs is the scheduled time, so `delay = now - min(score ≤ now)` via `ZRANGEBYSCORE`. Manager uses this ZSET approach for scan and fullscan pools; LINDEX is only for detail queues.

---

## 3. Queue Length as a Secondary Predictor

Queue delay is the primary trigger. Queue length is used as a **predictor** — it estimates how much delay will grow if no action is taken.

```
effective_throughput_per_s  = n_workers × (1 / est_fetch_s)
projected_delay_s           = queue_length / effective_throughput_per_s
```

Where `est_fetch_s` is the P75 of job durations from the last 30 days, cached in Redis (see Section 13.2). P75 gives a conservative estimate that absorbs outliers without being dominated by them — better than mean for drain-rate planning.

**How length and delay work together:**

| Actual Delay | Projected Delay | Action |
|---|---|---|
| Low | Low | Healthy — no action |
| Low | High | Burst incoming — pre-scale (add 1 worker, watch next cycle) |
| High | (any) | Genuine stall — scale up aggressively |
| Low | Low, trending down | Possibly remove worker if above floor |

**Queue length still gates backpressure** (pausing ingestion when the queue is too long to catch up before hitting the delay SLA), but the threshold is fully derived — no hardcoded constant:

```
est_fetch_s        = scaling_params["detail"]["fetch_p75"]   (P75 of detail fetch durations from cache)
throughput         = n_detail_workers / est_fetch_s
time_left          = max(DELAY_WARN_S_detail - actual_delay_s, 0)
backpressure_depth = throughput × time_left

if queue_depth > backpressure_depth → pause ingestion
```

This replaces both `DETAIL_QUEUE_MAX_ADAPTIVE` and `DETAIL_QUEUE_MAX_FULLSCAN` with one derived threshold that scales automatically with fleet size and queue delay. As actual_delay rises, time_left shrinks, backpressure_depth shrinks — the threshold tightens before the SLA is breached.

---

## 4. The Universal Scaling Formula

Replace all five hardcoded thresholds with one formula. Only two parameters differ per worker type: `DELAY_WARN_S` and `est_fetch_s`. Everything else is identical.

### 4.1 — Per worker type inputs

```
                  est_fetch_s source                           DELAY_WARN_S  queue_depth                         actual_delay
                  ─────────────────                           ────────────  ───────────                         ────────────
detail workers    P75 of api_health durations, last 30 days    60s           LLEN(queue:detail:adaptive)         now − enqueued_at of LINDEX −1
                  (PERCENTILE_CONT 0.75 across all requests,               + LLEN(queue:detail:fullscan)
                  all platforms). Cached in Redis, 25h TTL.
scan workers      P75 of company_poll_stats.avg_scan_          1800s         ZCOUNT(poll:adaptive, −inf, now)    now − score of most-overdue company
                  duration_s (PERCENTILE_CONT 0.75 across                   ← overdue companies only
                  all companies). Cached in Redis, 25h TTL.
fullscan workers  P75 of company_poll_stats.avg_fullscan_      7200s         ZCOUNT(poll:fullscan, −inf, now)    now − score of most-overdue company
                  duration_s (PERCENTILE_CONT 0.75 across
                  all companies). Cached in Redis, 25h TTL.
```

**Edge case — single ATS-ceiling-blocked company inflates actual_delay (known limitation):**
If one company is persistently skipped (CeilingExceeded on every cycle — e.g., Eightfold
concurrency limit always full), it stays in the ZSET with a fixed score and its delay grows
indefinitely. actual_delay reports the max delay across ALL overdue companies, so one stuck
company can trigger urgent even when the other 154 are being scanned on time. The system
response (add workers, throttle dispatch) will not help — the root cause is a per-platform
ceiling, not fleet size. Mitigation: when urgent fires but is immediately released because
workers_target ≤ n_workers (already right-sized), log a warning to check platform ceiling
flags. A proper fix (P90 of company delays instead of max) is left for a future iteration.

**est_fetch_s — baseline values confirmed (2026-07-20 from production DB):**
- Detail: P75 of all `api_health` request durations over last 30 days ≈ **~3–5s**. Higher than Workday mean (~1s) because P75 captures the slower platforms (SuccessFactors, Eightfold) that make up the tail. Conservative estimate with built-in margin — see Section 13.2.
- Scan: P75 of `company_poll_stats.avg_scan_duration_s` distribution — new column added 2026-07-20; EMA accumulates from first scan cycle. Estimated P75 from api_health: **~40s** (most companies scan in 20–40s; Eightfold tail at ~343s is absorbed by P75 without being ignored)
- Fullscan: P75 of `company_poll_stats.avg_fullscan_duration_s` distribution — real EMA data. Mean across 155 companies ≈ 69s but P75 ≈ **~90–100s** (Eightfold outlier at 831s pulls the tail; P75 is more representative of a typical hard fullscan than mean)

**DELAY_WARN_S — corrected from production poll interval data:**
- Detail: **60s** — confirmed reasonable. Jobs should never wait more than a minute.
- Scan: **1800s (30 min)** — poll intervals range 4.8h–12h. Being 30 min late = 4–10% of interval. 120s was <1% and too sensitive.
- Fullscan: **7200s (2h)** — fullscan interval is 24h. Being 2h late = 8% of interval. 300s was 0.3% and meaningless.

**Key capacity insight (2026-07-20):**
With real est_fetch_s values (P75), 2 scan workers can serve ~3 scans/min vs 0.32 scans/min needed (155 companies ÷ 8h avg interval). The current 10 scan workers are the startup default from `calculate_worker_counts()` — the manager's scale-down logic will right-size to 2–3 workers once deployed.

### 4.2 — What every worker publishes each cycle

```python
# Incremented at job completion: busy_ms += time.monotonic_ns() - job_start_ns
redis.set(f"worker:{type}:busy_ms:{pid}", busy_ms, ex=cycle_s * 2)
# Reset to 0 at start of each new cycle window
```

### 4.3 — Formula (same code for all three worker types)

```python
# ── est_fetch_s — P75 from cache, same lookup for all three pools ──────────
# P75 of job durations (api_health for detail; company_poll_stats for scan/fullscan).
# Cached in manager:scaling_params (25h TTL). Read once at startup into memory.
# P75 absorbs slow-platform outliers without being dominated by high-volume fast
# platforms (Workday). We need an estimate with a margin, not sub-second precision.
est_fetch_s = scaling_params[pool]["fetch_p75"]

# ── Utilisation ────────────────────────────────────────────────────────────
pool_busy_ms      = sum(redis.get(f"worker:{wtype}:busy_ms:{pid}") for pid in pool)
pool_capacity_ms  = n_workers * cycle_s * 1000
pool_utilization  = pool_busy_ms / pool_capacity_ms       # 0.0 – 1.0

# ── Drain rate ─────────────────────────────────────────────────────────────
time_left         = DELAY_WARN_S - actual_delay_s          # seconds left before WARN
drain_rate        = queue_depth / max(time_left, 1)        # jobs/s to clear queue in time

workers_target    = math.ceil(drain_rate * est_fetch_s)
# NOTE: amplification below is vestigial since urgent fires at WARN×0.75.
# When delay > WARN, time_left=1 already drives drain_rate to queue_depth and
# workers_target to a huge number that clamps to ceil regardless of the multiplier.
# Kept for safety in edge cases where time_left somehow stays > 1 past WARN.
if actual_delay_s > DELAY_WARN_S:                          # already past WARN → amplify
    workers_target = math.ceil(workers_target * (actual_delay_s / DELAY_WARN_S))
workers_target    = clamp(workers_target, WORKER_FLOOR, worker_ceil)

# ── Urgent (1 cycle) — fires on either condition, only when understaffed ───
# Demand trigger requires BOTH: approaching ceiling AND exceeding historical norm.
# Effective demand threshold = max(ceil×0.75, peak_Nd+1) — whichever is tighter wins.
#
# The `n_workers < min(workers_target, worker_ceil)` guard is critical:
# without it, the `if` re-fires every cycle while delay stays >= WARN×0.75
# (which is true for all 14+ recovery cycles in a scan/fullscan burst),
# preventing the `elif` release check from ever running and re-applying
# throttle every cycle even when workers are already at the right count.
needs_urgent_spawn = (
    actual_delay_s >= DELAY_WARN_S * 0.75
    or (workers_target >= worker_ceil * 0.75 and workers_target > peak_Nd)
) and n_workers < min(workers_target, worker_ceil)

if needs_urgent_spawn:
    # Step 1: emergency Layer 1 recalculation — midnight estimate may be stale
    new_ceil = _emergency_layer1_recalc(pool)   # same formula as midnight, runs now
    if new_ceil > worker_ceil:
        worker_ceil = new_ceil                  # correct stale ceiling immediately
    # Step 2: spawn exactly what the formula says (not always the ceiling)
    spawn_to(min(workers_target, worker_ceil))
    # Step 3: throttle dispatch to 75% of new capacity while workers come online
    set_dispatch_throttle(pool, 0.75)
    scale_up_cycles = 0                         # prevent stale count from triggering add_one
    urgent_active[pool] = True

# ── Urgent release (next cycle, once workers confirmed online) ─────────────
elif urgent_active[pool]:
    # workers are up; recalculate with real observed data
    if workers_target <= n_workers:             # confirmed right-sized or over
        set_dispatch_throttle(pool, 1.0)        # release immediately — delay drops after
        urgent_active[pool] = False
        if workers_target < n_workers - 1:
            scale_down_cycles += 1              # over-provisioned → begin scale-down
    else:                                       # still need more (at ceiling → Layer 2)
        pass                                    # throttle holds; Layer 2 takes over

# ── Scale-up (2 consecutive cycles) ───────────────────────────────────────
elif pool_utilization > 0.80 \
     and actual_delay_s > DELAY_WARN_S * 0.5 \
     and workers_target > n_workers:
    scale_up_cycles += 1
    if scale_up_cycles >= 2:
        add_one_worker()
        scale_up_cycles = 0
else:
    scale_up_cycles = 0

# ── Scale-down (5 consecutive cycles) ─────────────────────────────────────
if pool_utilization < 0.50 \
   and actual_delay_s < DELAY_WARN_S * 0.25 \
   and workers_target <= n_workers - 1:
    scale_down_cycles += 1
    if scale_down_cycles >= 5:
        remove_one_worker()
        scale_down_cycles = 0
else:
    scale_down_cycles = 0
```

**Decision table — three modes:**

```
                  SCALE-DOWN            SCALE-UP (normal)     SCALE-UP (urgent)
                  ──────────            ─────────────────     ─────────────────
utilization       < 50%                 > 80%                 —
delay             < WARN × 0.25         > WARN × 0.5          >= WARN × 0.75
target            ≤ current − 1         > current             >= ceil×0.75 AND > peak_Nd
spawn             remove 1              add 1                 min(workers_target, ceil)
dispatch          normal                normal                throttle to 75% capacity
cycles            5                     2                     1 (immediate)
release           —                     —                     recalc confirms count
                                                              (1 cycle after spawn)
```

Urgent fires when **either** trigger condition is met AND `n_workers < min(workers_target, ceil)` (pool is understaffed). When workers are already at the right count, the `elif urgent_active` release check runs instead — this prevents throttle being re-applied every cycle during multi-cycle recovery when delay is still elevated.

The two trigger conditions catch different shapes of burst:
- `delay >= WARN × 0.75` — time-based: delay is approaching the line, act before it's breached
- `workers_target >= ceil × 0.75 AND workers_target > peak_Nd` — demand-based: formula predicts near-ceiling load AND demand exceeds historical norms

The demand trigger requires **both** sub-conditions simultaneously:
- `>= ceil × 0.75` alone would fire for trivially small numbers when peak_Nd is low (e.g. peak_Nd=1, ceil=8: don't treat "need 2 workers" as urgent)
- `> peak_Nd` alone would fire routinely at everyday peak load (e.g. peak_Nd=6, ceil=8: fires every time the normal daily peak hits)
- Together, the effective threshold is `max(ceil × 0.75, peak_Nd + 1)` — the tighter constraint wins:

```
peak_Nd=6, ceil=8:   ceil×0.75=6,  peak_Nd+1=7  → fires at workers_target ≥ 7
peak_Nd=1, ceil=8:   ceil×0.75=6,  peak_Nd+1=2  → fires at workers_target ≥ 6
peak_Nd=7, ceil=8:   ceil×0.75=6,  peak_Nd+1=8  → fires at workers_target ≥ 8
peak_Nd=3, ceil=10:  ceil×0.75=8,  peak_Nd+1=4  → fires at workers_target ≥ 8
```

**Spawn target is `min(workers_target, ceil)`, not always `ceil`**: in most urgent scenarios `workers_target` clamps to `ceil` anyway (small `time_left` → large `drain_rate`). The difference matters for demand-triggered urgent where delay is still moderate — spawning 7 when formula says 7 and ceil=10 is more precise than slamming to 10.

**Emergency Layer 1 recalculation**: runs immediately when urgent fires — same formula as midnight (`peak_Nd + max(growth_buffer, volatility_buffer)`), using today's observed peak. If the nightly estimate was too low, the ceiling is corrected on the spot before spawning. If it was correct, nothing changes.

**Throttle release fires on worker confirmation, not on delay dropping**: the throttle exists only to protect newly-spawned workers during their ramp-up window. Once the next cycle confirms workers are online and the recalculation validates the count, the throttle lifts immediately. Delay drops *after* the release — waiting for delay to drop first is backwards.

**Stable band**: 50%–80% utilization with delay below WARN × 0.5 → no action either way.
Below 50% for 5 cycles → scale down. Above 80% for 2 cycles with delay building → scale up.

### 4.4 — Worked examples

---

#### Detail Workers

```
est_fetch_s ≈ 3–5s (P75 of all api_health durations)  |  DELAY_WARN_S = 60s  |  ceil = 6  |  floor = 2  |  cycle = 60s
est_fetch_s source: PERCENTILE_CONT(0.75) across all platforms, last 30 days. Cached in Redis (25h TTL).
Examples below use 1s for illustration (Workday-heavy baseline); real est_fetch_s includes slow-platform tail.
```

**Scale-up — normal (2 cycles):**
```
3 workers, pressure building

Cycle 1:
  pool_busy_ms     = 145,000ms
  pool_capacity_ms = 3 × 60,000 = 180,000ms
  pool_utilization = 145,000 / 180,000 = 80.6%

  queue_depth  = 80          actual_delay = 35s
  time_left    = 60 − 35 = 25s
  drain_rate   = 80 / 25 = 3.2 jobs/s
  workers_target = ceil(3.2 × 1) = 4 → clamp(4, 2, 6) = 4

  utilization(80.6%) > 80%  ✓
  delay(35s) > 60×0.5=30s   ✓
  workers_target(4) > current(3)  ✓
  → scale_up_cycles = 1

Cycle 2: same conditions → scale_up_cycles = 2 → ADD 1 WORKER → 3 becomes 4
```

**Scale-up — urgent override (Accenture burst, 1 cycle):**
```
actual_delay = 50s ≥ DELAY_WARN_S × 0.75(45s) → urgent fires 10s before breach

  time_left      = max(60−50, 1) = 10s
  drain_rate     = 400 / 10 = 40 jobs/s
  workers_target = clamp(⌈40 × 1⌉, 2, 6) = clamp(40, 2, 6) = 6   ← capped at ceil

  Step 1: Emergency Layer 1 recalculation → new_ceil confirmed at 6
  Step 2: Spawn to min(workers_target=6, worker_ceil=6) = 6   (hits ceil here due to large queue)
  Step 3: Throttle dispatch to 75%  →  release after 1 recalculation cycle
```

**Scale-down (5 cycles):**
```
3 workers, 2 would suffice

Cycle 1:
  pool_busy_ms     = 72,000ms
  pool_capacity_ms = 180,000ms
  pool_utilization = 72,000 / 180,000 = 40%

  queue_depth = 15     actual_delay = 8s
  time_left   = 60 − 8 = 52s
  drain_rate  = 15 / 52 = 0.29 jobs/s
  workers_target = ceil(0.29 × 1) = 1 → clamp(1, 2, 6) = 2

  utilization(40%) < 50%      ✓
  delay(8s) < 60×0.25=15s     ✓
  workers_target(2) ≤ 3−1=2   ✓
  → scale_down_cycles = 1

After 5 cycles → REMOVE 1 → 3 becomes 2

Verify with 2 workers:
  pool_utilization = 72,000 / 120,000 = 60% > 50% → stops here ✓
```

---

**End-to-end burst scenario — scale-up + backpressure + recovery (3 cycles):**

```
Config: est_fetch_s = 4s (P75 from cache)  |  WORKER_FLOOR = 2  |  worker_ceil = 5  |  DELAY_WARN_S = 60s  |  cycle_s = 60s

─── Cycle 1 — Pressure building ────────────────────────────────────────────────────────
Inputs: n_workers=3  pool_busy_ms=155,000  queue_depth=140  actual_delay=35s

  pool_capacity_ms = 3 × 60,000 = 180,000ms
  pool_utilization = 155,000 / 180,000 = 86.1%

  time_left        = 60 − 35 = 25s
  drain_rate       = 140 / 25 = 5.6 jobs/s
  workers_target   = clamp(⌈5.6 × 4⌉, 2, 5) = clamp(23, 2, 5) = 5   ← capped at CEIL

  Urgent check: actual_delay(35s) > DELAY_WARN_S(60s)?   NO → normal path

  Scale-up:    util(86.1%) > 80% ✓  delay(35s) > 30s ✓  target(5) > current(3) ✓
               → scale_up_cycles = 1  (need 2 to act, no worker change yet)

  Backpressure: throughput = 3/4 = 0.75 jobs/s
                backpressure_depth = 0.75 × 25 = 18.75 jobs
                queue_depth(140) > 18.75  → PAUSE scan/fullscan inflow to detail queue

─── Cycle 2 — Scale-up fires ───────────────────────────────────────────────────────────
Inputs: n_workers=3  pool_busy_ms=162,000  queue_depth=95  actual_delay=50s
        (45 jobs drained at 0.75/s; no new inflow due to backpressure)

  pool_utilization = 162,000 / 180,000 = 90%

  time_left        = 60 − 50 = 10s
  drain_rate       = 95 / 10 = 9.5 jobs/s
  workers_target   = clamp(⌈9.5 × 4⌉, 2, 5) = clamp(38, 2, 5) = 5   ← still capped

  Urgent check: actual_delay(50s) > DELAY_WARN_S(60s)?   NO → 10s of margin remaining

  Scale-up:    util(90%) > 80% ✓  delay(50s) > 30s ✓  target(5) > current(3) ✓
               → scale_up_cycles = 2  → ADD 1 WORKER → n_workers = 4

  Backpressure: backpressure_depth = 0.75 × 10 = 7.5 jobs
                queue_depth(95) > 7.5  → PAUSE continues

─── Cycle 3 — Recovery, backpressure lifts ─────────────────────────────────────────────
Inputs: n_workers=4  pool_busy_ms=210,000  queue_depth=35  actual_delay=20s
        (60 jobs drained at 1 job/s with 4 workers)

  pool_capacity_ms = 4 × 60,000 = 240,000ms
  pool_utilization = 210,000 / 240,000 = 87.5%

  time_left        = 60 − 20 = 40s
  drain_rate       = 35 / 40 = 0.875 jobs/s
  workers_target   = clamp(⌈0.875 × 4⌉, 2, 5) = clamp(4, 2, 5) = 4   ← matches current

  Urgent check: actual_delay(20s) > DELAY_WARN_S(60s)?   NO

  Scale-up:    delay(20s) > 30s?   NO  → scale_up_cycles = 0
  Scale-down:  util(87.5%) < 50%?  NO  → scale_down_cycles = 0

  Backpressure: throughput = 4/4 = 1.0 job/s
                backpressure_depth = 1.0 × 40 = 40 jobs
                queue_depth(35) > 40?   NO → BACKPRESSURE LIFTS ✓

  Re-introduction begins: push 1 job every est_fetch_s/n_workers = 4/4 = 1s
  (rate-limited to prevent secondary spike)

Outcome: system drains to healthy state without ever breaching DELAY_WARN_S.
         Lever 1 (urgent override) never fired — proactive backpressure + 2-cycle
         scale-up absorbed the burst entirely.
```

---

#### Scan Workers

```
est_fetch_s ≈ 40s (P75 of avg_scan_duration_s distribution)  |  DELAY_WARN_S = 1800s  |  ceil = 10  |  floor = 2  |  cycle = 60s
est_fetch_s source: PERCENTILE_CONT(0.75) of company_poll_stats.avg_scan_duration_s. Cached in Redis (25h TTL).
queue_depth = ZCOUNT(poll:adaptive, −inf, now)  ← overdue companies only
actual_delay = now − score of most-overdue company
```

**Scale-up — normal (2 cycles):**
```
3 scan workers, companies piling up

Cycle 1:
  pool_busy_ms     = 3 × 58,000 = 174,000ms
  pool_capacity_ms = 3 × 60,000 = 180,000ms
  pool_utilization = 174,000 / 180,000 = 96.7%

  queue_depth  = 60 overdue companies    actual_delay = 1200s
  time_left    = 1800 − 1200 = 600s
  drain_rate   = 60 / 600 = 0.1 companies/s
  workers_target = ceil(0.1 × 40) = 4 → clamp(4, 2, 10) = 4

  utilization(96.7%) > 80%         ✓
  delay(1200s) > 1800×0.5=900s     ✓
  workers_target(4) > current(3)   ✓
  → scale_up_cycles = 1

Cycle 2: same conditions → scale_up_cycles = 2 → ADD 1 → 3 becomes 4
```

**Scale-up — urgent override (1 cycle):**
```
actual_delay = 1500s ≥ DELAY_WARN_S × 0.75(1350s) → urgent fires 300s before breach

  time_left      = 1800 − 1500 = 300s
  drain_rate     = 50 / 300 = 0.167 companies/s
  workers_target = clamp(⌈0.167 × 40⌉, 2, 10) = clamp(7, 2, 10) = 7

  Step 1: Emergency Layer 1 recalculation → new_ceil confirmed at 10
  Step 2: Spawn to min(workers_target=7, worker_ceil=10) = 7   ← formula-driven, not max
          (queue=50 doesn't fully saturate demand; ceiling reserved for worse bursts)
  Step 3: Throttle dispatch to 75%  →  release after 1 recalculation cycle
```

**Scale-down (5 cycles):**
```
3 scan workers, quiet period — 2 would suffice

Cycle 1:
  pool_busy_ms     = 80,000ms
  pool_capacity_ms = 3 × 60,000 = 180,000ms
  pool_utilization = 80,000 / 180,000 = 44.4%

  queue_depth  = 2 overdue    actual_delay = 200s
  time_left    = 1800 − 200 = 1600s
  drain_rate   = 2 / 1600 = 0.00125 companies/s
  workers_target = ceil(0.00125 × 40) = ceil(0.05) = 1 → clamp(1, 2, 10) = 2

  utilization(44.4%) < 50%        ✓
  delay(200s) < 1800×0.25=450s    ✓
  workers_target(2) ≤ 3−1=2       ✓
  → scale_down_cycles = 1

After 5 cycles → REMOVE 1 → 3 becomes 2

Verify with 2 workers:
  pool_utilization = 80,000 / 120,000 = 66.7% > 50% → stops here ✓
```

**End-to-end burst scenario — urgent override + throttled recovery (3 cycles):**

```
Config: est_fetch_s = 40s  |  WORKER_FLOOR = 2  |  worker_ceil = 10  |  DELAY_WARN_S = 1800s
        WARN×0.75 = 1350s  |  cycle_s = 60s

─── Cycle 1 — Urgent fires proactively ────────────────────────────────────────────────────
Inputs: n_workers=4  pool_busy_ms=228,000  queue_depth=80  actual_delay=1500s
        (delay already past WARN×0.75 when cycle begins)

  pool_capacity_ms = 4 × 60,000 = 240,000ms
  pool_utilization = 228,000 / 240,000 = 95%

  time_left        = 1800 − 1500 = 300s
  drain_rate       = 80 / 300 = 0.267 companies/s
  workers_target   = clamp(⌈0.267 × 40⌉, 2, 10) = clamp(11, 2, 10) = 10   ← capped at ceil

  Urgent check: actual_delay(1500s) ≥ DELAY_WARN_S × 0.75(1350s)?   YES — 300s before breach

  Step 1: Emergency Layer 1 recalculation (same midnight formula, runs now with today's data)
          → new_ceil = 10  (midnight estimate confirmed; no update needed)
  Step 2: Spawn to min(workers_target=10, worker_ceil=10) = 10  → 6 workers spawned (pending)
          (hits ceil because large queue + tight time_left drives workers_target past ceiling)
  Step 3: Throttle dispatch to 75% capacity (protects newly-spawned workers during ramp-up)

  4 existing workers drain: 4 × (60/40) ≈ 6 companies this cycle
  → queue = 74,  delay = 1560s  (new workers come online by end of this cycle)

─── Cycle 2 — Workers online, recalculate ─────────────────────────────────────────────────
Inputs: n_workers=10  pool_busy_ms=450,000  queue_depth=74  actual_delay=1560s
        (10 workers at 75% throttle)

  pool_capacity_ms = 10 × 60,000 = 600,000ms
  pool_utilization = 450,000 / 600,000 = 75%

  time_left        = 1800 − 1560 = 240s
  drain_rate       = 74 / 240 = 0.308 companies/s
  workers_target   = clamp(⌈0.308 × 40⌉, 2, 10) = clamp(13, 2, 10) = 10

  urgent_active release check: workers_target(10) ≤ n_workers(10) → confirmed right-sized
  → RELEASE THROTTLE (full dispatch from next cycle)

  Throttled drain this cycle: 10 × 1.5 × 0.75 ≈ 11 companies
  → queue = 63,  delay = 1620s

─── Cycle 3 — Full capacity, queue draining ────────────────────────────────────────────────
Inputs: n_workers=10  pool_busy_ms=600,000  queue_depth=63  actual_delay=1620s

  pool_capacity_ms = 600,000ms
  pool_utilization = 100%

  time_left        = 180s
  drain_rate       = 63 / 180 = 0.35 companies/s
  workers_target   = clamp(⌈0.35 × 40⌉, 2, 10) = clamp(14, 2, 10) = 10

  Drain: 10 × (60/40) = 15 companies/cycle
  → queue = 48,  delay = 1680s

What happens next: 15 companies/cycle, queue clears in ~3 more cycles.
Delay peaks near WARN(1800s) as the last companies drain; urgent re-checks each cycle but
n_workers already equals min(workers_target, ceil)=10 so no additional spawning occurs.

Key advantage of WARN×0.75 trigger: urgent fires with 300s remaining (5 extra drain
cycles), so workforce reaches ceiling before time_left collapses. Old design fires only
after delay > 1800s, when time_left = 1 and workers_target would be capped the same way
but the queue has grown larger and the recovery window has closed.
```

---

#### Fullscan Workers

```
est_fetch_s ≈ 90–100s (P75 of avg_fullscan_duration_s distribution)  |  DELAY_WARN_S = 7200s  |  ceil = 5  |  floor = 2  |  cycle = 60s
est_fetch_s source: PERCENTILE_CONT(0.75) of company_poll_stats.avg_fullscan_duration_s. Cached in Redis (25h TTL).
Mean ≈ 69s but Eightfold outlier (831s) pulls P75 to ~90–100s — more conservative, prevents underprovisioning.
queue_depth = ZCOUNT(poll:fullscan, −inf, now)  ← overdue companies only
actual_delay = now − score of most-overdue company
```

**Scale-up — normal (2 cycles):**
```
3 fullscan workers, large companies backing up

Cycle 1:
  pool_busy_ms     = 3 × 60,000 = 180,000ms  (all fully occupied)
  pool_capacity_ms = 180,000ms
  pool_utilization = 100%

  queue_depth  = 150 overdue    actual_delay = 4000s
  time_left    = 7200 − 4000 = 3200s
  drain_rate   = 150 / 3200 = 0.047 companies/s
  workers_target = ceil(0.047 × 95) = ceil(4.47) = 5 → clamp(5, 2, 5) = 5

  utilization(100%) > 80%          ✓
  delay(4000s) > 7200×0.5=3600s    ✓
  workers_target(5) > current(3)    ✓
  → scale_up_cycles = 1

Cycle 2: → scale_up_cycles = 2 → ADD 1 → 3 becomes 4
```

**Scale-down (5 cycles):**
```
4 fullscan workers, relaxed schedule — 3 would suffice

Cycle 1:
  pool_busy_ms     = 110,000ms
  pool_capacity_ms = 4 × 60,000 = 240,000ms
  pool_utilization = 110,000 / 240,000 = 45.8%

  queue_depth  = 3 overdue    actual_delay = 600s
  time_left    = 7200 − 600 = 6600s
  drain_rate   = 3 / 6600 = 0.00045 companies/s
  workers_target = ceil(0.00045 × 95) = ceil(0.043) = 1 → clamp(1, 2, 5) = 2

  utilization(45.8%) < 50%          ✓
  delay(600s) < 7200×0.25=1800s     ✓
  workers_target(2) ≤ 4−1=3         ✓
  → scale_down_cycles = 1

After 5 cycles → REMOVE 1 → 4 becomes 3

Verify with 3 workers:
  pool_utilization = 110,000 / 180,000 = 61.1% > 50% → stops here ✓
```

**End-to-end burst scenario — urgent override + throttled recovery (3 cycles):**

```
Config: est_fetch_s = 95s  |  WORKER_FLOOR = 2  |  worker_ceil = 5  |  DELAY_WARN_S = 7200s
        WARN×0.75 = 5400s  |  cycle_s = 60s
Note: fullscan jobs span ~1.58 cycles (95s > 60s); workers are always fully occupied
      mid-job. "companies drained" = completed jobs that handed off to new dispatch.

─── Cycle 1 — Urgent fires proactively ─────────────────────────────────────────────────────
Inputs: n_workers=2  pool_busy_ms=120,000  queue_depth=45  actual_delay=6000s
        (delay already past WARN×0.75 when cycle begins)

  pool_capacity_ms = 2 × 60,000 = 120,000ms
  pool_utilization = 120,000 / 120,000 = 100%  (always mid-job)

  time_left        = 7200 − 6000 = 1200s
  drain_rate       = 45 / 1200 = 0.0375 companies/s
  workers_target   = clamp(⌈0.0375 × 95⌉, 2, 5) = clamp(⌈3.56⌉, 2, 5) = clamp(4, 2, 5) = 4

  Urgent check: actual_delay(6000s) ≥ DELAY_WARN_S × 0.75(5400s)?   YES — 1200s before breach

  Step 1: Emergency Layer 1 recalculation → new_ceil = 5  (confirmed, no correction needed)
  Step 2: Spawn to min(workers_target=4, worker_ceil=5) = 4   ← formula says 4, not max
          (1200s of headroom means fewer workers needed; 5th slot held in reserve)
          → 2 workers spawned (pending)
  Step 3: Throttle dispatch to 75%

  2 existing workers drain: 2 × (60/95) ≈ 1 company this cycle
  → queue = 44,  delay = 6060s  (new workers come online by end of this cycle)

─── Cycle 2 — Workers online, recalculate ──────────────────────────────────────────────────
Inputs: n_workers=4  pool_busy_ms=180,000  queue_depth=44  actual_delay=6060s
        (4 workers at 75% dispatch throttle)

  pool_capacity_ms = 4 × 60,000 = 240,000ms
  pool_utilization = 180,000 / 240,000 = 75%

  time_left        = 7200 − 6060 = 1140s
  drain_rate       = 44 / 1140 = 0.0386 companies/s
  workers_target   = clamp(⌈0.0386 × 95⌉, 2, 5) = clamp(⌈3.67⌉, 2, 5) = clamp(4, 2, 5) = 4

  urgent_active release check: workers_target(4) ≤ n_workers(4) → confirmed right-sized
  → RELEASE THROTTLE

  Throttled drain this cycle: 4 × (60/95) × 0.75 ≈ 1.9 → ~2 companies
  → queue = 42,  delay = 6120s

─── Cycle 3 — Full capacity, queue draining ────────────────────────────────────────────────
Inputs: n_workers=4  pool_busy_ms=240,000  queue_depth=42  actual_delay=6120s

  pool_capacity_ms = 240,000ms
  pool_utilization = 100%

  time_left        = 1080s
  drain_rate       = 42 / 1080 = 0.039 companies/s
  workers_target   = clamp(⌈0.039 × 95⌉, 2, 5) = clamp(⌈3.7⌉, 2, 5) = clamp(4, 2, 5) = 4

  Drain: 4 × (60/95) ≈ 2.53 → ~3 companies/cycle
  → queue = 39,  delay = 6180s

What happens next: ~3 companies/cycle with 42 remaining (after throttled cycle 2).
Queue clears in ~14 more cycles; delay at that point ≈ 6120 + 14×60 = 6960s — below WARN(7200s).
5th worker slot never consumed; urgent resolved with 240s of headroom to spare.

Key advantage of WARN×0.75 trigger + formula-driven spawn: urgent fires with 1200s
remaining and the formula correctly determines 4 workers suffice (not 5). Queue clears
BEFORE DELAY_WARN_S is breached. Old design fires only after delay ≥ 7200s, then
workers_target = clamp(⌈(queue/1) × 95⌉, 2, 5) = 5 (ceiling) and recovery window is zero.
```

---

### 4.5 — Formula summary

```
              DELAY_WARN_S   est_fetch_s (P75)    est_fetch_s source                                    scale-up   scale-down
              ────────────   ─────────────────    ──────────────────                                    ─────────  ──────────
detail        60s            ~3–5s                PERCENTILE_CONT(0.75) across all api_health           2 cycles   5 cycles
                                                  requests, all platforms, last 30 days.
                                                  Cached in Redis (25h TTL).
scan          1800s          ~40s                 PERCENTILE_CONT(0.75) of avg_scan_duration_s          2 cycles   5 cycles
                                                  across all companies. Cached 25h in Redis.
fullscan      7200s          ~90–100s             PERCENTILE_CONT(0.75) of avg_fullscan_duration_s      2 cycles   5 cycles
                                                  across all companies. Cached 25h in Redis.

Same formula. Same thresholds (80% / 50%). Same cycle counts (2 / 5).
Only DELAY_WARN_S and est_fetch_s differ per worker type.
est_fetch_s is never a simple average — using mean is wrong because it is dominated by high-volume
fast platforms (detail) or underweights outlier companies (scan/fullscan). P75 is the correct metric.
```

---

## 5. One Manager for All Queues

Replace the current two-decision-maker setup (watchdog + scheduler Layer 3) with one coordinating manager that observes everything and is the sole decision-maker for scaling.

### What the manager observes (every cycle)

Three factors drive every scaling decision:

**Factor 1 — Queue delay** (primary trigger: how urgent is the backlog?)
```
for each queue in [detail:adaptive, detail:fullscan, scan:adaptive, scan:fullscan]:
    delay_s     = time_since_oldest_job(queue)           ← LINDEX -1, parse enqueued_at
    depth       = redis.LLEN(queue)                      ← predictor input
    projected_s = depth / effective_throughput(queue)    ← early warning: is pressure incoming?
```

**Factor 2 — Pool utilisation + drain rate** (see Section 4 for the complete formula)

Two signals combined:

```
# Utilisation: what fraction of the pool was actually busy this cycle?
pool_busy_ms      = sum(worker:{type}:busy_ms:{pid} for pid in pool)
pool_capacity_ms  = n_workers × cycle_s × 1000
pool_utilization  = pool_busy_ms / pool_capacity_ms    # 0.0 – 1.0

# Drain rate: jobs/s needed to clear existing backlog before delay hits WARN
time_left         = DELAY_WARN_S - actual_delay_s
drain_rate        = queue_depth / max(time_left, 1)
workers_target    = clamp(ceil(drain_rate × est_fetch_s), WORKER_FLOOR, worker_ceil)
```

**How the three scenarios play out:**

| Scenario | utilization | depth | delay | workers_target | Decision |
|---|---|---|---|---|---|
| Burst just landed | 95% | 500 | 0s | ceil(500/60×est_fetch_s)→ceil | scale_up_cycles resets (workers_target>current only applies if util>80%) |
| Genuine idle | 40% | 15 | 8s | 2 (floor) | scale_down_cycles increments if ≤ current−1 |
| Stall | 30% | 100 | 400s | ceil(100/1×2)×6.7→ceil | Urgent override fires at 1 cycle |
| Healthy balance | 65% | 10 | 5s | 2 (floor) | 50%–80% stable band → no action |

`pool_utilization` works for both short jobs (detail, ~2s — many jobs per cycle) and long jobs (scan/fullscan, 3–10min per job — worker reports 60,000ms busy_ms every cycle it's working). Point-in-time snapshot would fail for long jobs; cumulative busy_ms does not.

**Factor 3 — Head-of-queue movement** (stall confirmation: are workers stuck?)
```
for each queue:
    head_job_id        = json.loads(redis.LINDEX(queue, -1)).get("job_id")
    head_unchanged     = (head_job_id == prev_head_job_id[queue])
    stall_cycles[queue] += 1 if head_unchanged else 0 (reset on movement)
```

**All three together:**
```
worker_counts  = read from scheduler:health    ← already published by scheduler
```

**What each combination means:**

| Queue delay | Worker idle rate | Head moving? | Meaning | Action |
|---|---|---|---|---|
| Low | High (many idle) | Yes | Over-provisioned | Scale down toward floor |
| Low | Low (all busy) | Yes | Healthy balance | No action |
| High | Low (all busy) | Yes | Burst — need capacity | Add workers |
| High | Low (all busy) | No | Genuine stall | Investigate → restart as last resort |
| High | High (many idle) | No | Workers stuck/dead | Restart workers, not add |
| Low | High (many idle) | No | Queue empty, workers waiting | Scale down (idle contraction) |

### Manager cycle interval

The manager runs every **30–60 seconds** — not 30 minutes. The 30-minute interval in the current `_slow_throughput_check_loop` exists because queue depth is noisy and you need a long window to confirm a trend. Queue delay is not noisy: a genuine stall grows monotonically every cycle; a burst has delay ≈ 0 from the moment of push. A clean signal needs a short window, not a long one.

The 30-minute slow throughput loop is **eliminated**. The manager replaces it entirely.

### What the manager decides (every 30–60 seconds)

See Section 4.3 for the full formula. In terms of the manager loop:

```
for each pool in [detail, scan, fullscan]:

    compute pool_utilization, workers_target  (Section 4.3)

    # --- Urgent / normal / scale-down: see Section 4.3 for the authoritative formula ---
    # High-level flow (Section 4.3 is the implementation source of truth):
    #
    #   if needs_urgent_spawn:          # delay >= WARN×0.75 (or demand trigger) AND understaffed
    #       Layer 1 recalc → spawn to min(workers_target, ceil) → throttle 75%
    #       scale_up_cycles = 0
    #       urgent_active[pool] = True
    #
    #   elif urgent_active[pool]:       # workers came online → check if right-sized
    #       if workers_target <= n_workers: release throttle, urgent_active = False
    #       else: throttle holds (Layer 2 takes over at DELAY_WARN_S)
    #
    #   elif util > 80% and delay > WARN×0.5 and workers_target > current:
    #       scale_up_cycles += 1 → add 1 worker after 2 consecutive cycles
    #
    #   if util < 50% and delay < WARN×0.25 and workers_target <= current−1:
    #       scale_down_cycles += 1 → remove 1 worker after 5 consecutive cycles

    # --- Stall confirmation: DETAIL workers only ---
    # Scan/fullscan workers use XAUTOCLAIM for dead-worker recovery.
    # Head-of-queue tracks consumption for detail (LIST: head changes on pickup)
    # but not for scan/fullscan (ZSET score only changes on COMPLETION, not pickup).
    # A fullscan lasting 831s would false-positive at 3 cycles (3 min) — wrong signal.
    if pool == "detail" and actual_delay_s > DELAY_WARN_S and head_unchanged_cycles["detail"] >= 3:
        if n_workers >= worker_ceil:
            restart_pool_workers("detail")    # at ceiling AND stalled → restart detail_worker
```

**Hysteresis is asymmetric by design**:
- Add workers after **2 consecutive cycles** (~2 minutes) — fast response to real pressure
- Remove workers after **5 consecutive cycles** (~5 minutes) — conservative, prevents thrashing after a burst clears

**Why burst resets scale-down automatically**: when a burst lands, `queue_depth` spikes → `drain_rate` spikes → `workers_target` exceeds `n_workers` → the scale-down `else` branch fires → `scale_down_cycles` resets to 0. No explicit "reset on burst" needed.

**Restart mechanism differs per worker type — do not generalise:**

| Worker type | Queue type | Stall signal | Why | Threshold | Who decides |
|---|---|---|---|---|---|
| detail | Redis LIST | `LINDEX -1` job_id unchanged | Head changes on job *pickup*, so unchanged = nobody consuming | 3 cycles (180s) | manager |
| scan | Redis ZSET | worker progress not updating | ZSET score only changes on scan *completion*, not pickup; a 343s Eightfold scan unchanged for 3 cycles is normal | `check_hung_workers()` in watchdog | watchdog |
| fullscan | Redis ZSET | worker progress not updating | Same as scan; max observed fullscan = 831s = 13.8 cycles; 3-cycle trigger would false-positive constantly | `check_hung_workers()` in watchdog | watchdog |

Manager owns detail worker restarts. Scan and fullscan restarts stay in watchdog's `check_hung_workers()` — the signal there is worker progress (set_progress() call frequency), not queue-head movement. That signal is correct for any job duration.

**Key rule**: restarts are the last resort, not the first response. Required confirmation for detail:
- Head of queue unchanged for 3+ cycles (workers alive but not consuming this queue)
- `n_workers >= worker_ceil` (we have already added all capacity we can)
- Workers still heartbeating (dead-worker detection stays in watchdog, not manager)

### What moves to the manager

Current responsibilities that belong to the manager:
- Layer 3 slow throughput check (currently in `scheduler.py _slow_throughput_check_loop`) — **eliminated as a 30-min loop; replaced by manager's 30–60s cycle**
- Detail queue CRITICAL/WARN detection (currently in `watchdog.py`)
- Worker add/remove decisions for both scan and detail pools

### What watchdog gains

- Manager heartbeat check: if `worker:alive:manager` expires → `sudo systemctl restart recruiter-manager`
  (same pattern as existing scheduler liveness heal action)

What stays in watchdog:
- Manager heartbeat liveness (is manager.py alive? → restart via systemd if dead)
- Scheduler heartbeat liveness (is the scheduler process alive?)
- PEL health (stuck stream messages)
- DLQ depth
- Bloom filter health
- Coverage miss checks
- All non-scaling alerts

The watchdog becomes a health reporter only. It never triggers a scheduler restart based on queue depth. Scheduler restart stays as the heal action for "scheduler process is dead" (heartbeat missing) — not for "queue is big".

---

## 6. Manager as a Standalone Systemd Process (Resolved — 2026-07-20)

Manager runs as a dedicated standalone process under systemd — not as an elected role inside a worker. This removes all election complexity (no SET NX, no follower logic in workers, no split-brain handling) and reuses the same watchdog → systemd restart pattern already in place for the scheduler.

### Process hierarchy

```
systemd  (OS-level supervisor — always alive while host is up)
  ├── recruiter-watchdog   [Restart=always]  ← systemd restarts if watchdog crashes
  │     └── watchdog.py monitors:
  │           ├── manager heartbeat  → systemctl restart recruiter-manager  (if dead)
  │           └── scheduler heartbeat → systemctl restart recruiter-scheduler (if dead)
  │
  ├── recruiter-manager    [Restart=always]  ← systemd restarts if manager crashes
  │     └── manager.py: scaling decisions every 60s
  │
  └── recruiter-scheduler  [Restart=always]  ← systemd restarts if scheduler crashes
        └── scheduler.py: job dispatch + parent of all workers
              ├── scan_worker.py × N   (child processes via multiprocessing)
              ├── detail_worker.py × N
              └── fullscan.py × N
```

### Failure coverage

| Failure mode | Who catches it | Recovery |
|---|---|---|
| manager.py crashes / exits | systemd | restarts recruiter-manager within RestartSec |
| manager.py alive but frozen (heartbeat timeout) | watchdog | SIGTERM → systemd restarts |
| watchdog.py crashes / exits | systemd | restarts recruiter-watchdog within RestartSec |
| watchdog.py alive but frozen | nothing (narrow edge case) | — |
| scheduler.py crashes | systemd + watchdog | both detect; systemd is faster |
| host machine dies | nothing | accepted — single-host deployment |

### Manager state survives restarts

All manager state lives in Redis: `scale_up_cycles`, `scale_down_cycles`, head-of-queue snapshots, scaling params cache, learned ceilings. A restarted manager reads Redis at startup and resumes with no state lost. Restart gap = `RestartSec` (suggest 10s) + one missed scaling cycle at most.

### Implementation note — unit files

`deploy/systemd/` contains all three unit files. Before deploying manager:

1. ~~Verify both existing unit files have `Restart=always`~~ **Done** — `recruiter-watchdog` has `Restart=always RestartSec=10s`, `recruiter-scheduler` has `Restart=always RestartSec=30s`
2. ~~Create `recruiter-manager.service`~~ **Done** — `deploy/systemd/recruiter-manager.service` created
3. Add `recruiter-manager` to watchdog's heal map: heartbeat miss → `sudo systemctl restart recruiter-manager`
4. Add `opc ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart recruiter-manager` to sudoers

### One-time server setup (run once when deploying manager for the first time)

Unit files in `deploy/systemd/` are the source of truth but systemd only reads from `/etc/systemd/system/`. Use symlinks so future `git pull` changes take effect after just a `daemon-reload` — no manual copy needed.

```bash
# 1. Symlink all three unit files (watchdog + scheduler were previously copied,
#    replace with symlinks so they stay in sync with the repo going forward)
sudo ln -sf /home/opc/mail/deploy/systemd/recruiter-watchdog.service  /etc/systemd/system/recruiter-watchdog.service
sudo ln -sf /home/opc/mail/deploy/systemd/recruiter-scheduler.service /etc/systemd/system/recruiter-scheduler.service
sudo ln -sf /home/opc/mail/deploy/systemd/recruiter-manager.service   /etc/systemd/system/recruiter-manager.service

# 2. Reload systemd so it picks up the new/changed unit files
sudo systemctl daemon-reload

# 3. Enable manager to start on reboot
sudo systemctl enable recruiter-manager

# 4. Add sudoers entry so watchdog can restart manager
echo "opc ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart recruiter-manager" \
  | sudo tee /etc/sudoers.d/recruiter-manager

# 5. Start manager
sudo systemctl start recruiter-manager

# 6. Verify all three are running
sudo systemctl status recruiter-watchdog recruiter-scheduler recruiter-manager
```

### After every git pull that changes a unit file

```bash
sudo systemctl daemon-reload
# Then restart whichever unit changed:
sudo systemctl restart recruiter-manager   # or watchdog / scheduler
```

No election protocol needed. No changes to worker processes.

---

## 7. Cross-Pool Worker Management — Three-Layer Architecture

Workers are not permanently fixed to one pool. Manager coordinates across all three pools using three independent layers operating at different timescales:

- **Layer 0 — Normal autoscaler** (every 60s): scales each pool dynamically within [WORKER_FLOOR, worker_ceil] using the utilization + drain rate formula from Section 4. This is the primary scaling mechanism during normal operation.
- **Layer 1 — Proactive capacity planner** (daily, midnight): computes each pool's historical peak demand + buffer and sets `worker_ceil` for the next 24 hours. Layer 0 scales within that ceiling. Layer 1 does NOT maintain a fixed running count.
- **Layer 2 — Reactive emergency** (event-driven): fires when the autoscaler hits its ceiling and delay is still rising — Lever 1 backpressure, worker borrowing, and deadlock resolution.

The system operates in Layer 0 almost all the time. Layer 1 makes one capacity decision per day. Layer 2 should rarely fire if Layer 1 calibrated ceilings correctly.

**Priority order** (highest to lowest): detail > scan > fullscan

---

### Layer 0 — Normal autoscaler (every 60s)

Section 4 formula runs every manager cycle, independently per pool:

```
pool_utilization = pool_busy_ms / (n_workers × cycle_s × 1000)
drain_rate       = queue_depth / max(DELAY_WARN_S − actual_delay_s, 1)
workers_target   = clamp(ceil(drain_rate × est_fetch_s), WORKER_FLOOR, worker_ceil)

Scale up:   util > 0.80 AND delay > DELAY_WARN_S×0.5 AND workers_target > current → 2 cycles
Scale down: util < 0.50 AND delay < DELAY_WARN_S×0.25 → 5 cycles
Urgent:     (delay >= DELAY_WARN_S×0.75 OR demand trigger) AND understaffed
            → Layer 1 recalc + spawn to min(workers_target, ceil) + 75% throttle in 1 cycle
            (see Section 4.3 for full needs_urgent_spawn condition)
```

`worker_ceil` = `capacity(pool)` set by Layer 1 each day. Between recomputes it is stable.

**Per-cycle maintenance (runs alongside the autoscaler every cycle):**

- **Replace crashed workers**: if `current_workers < WORKER_FLOOR` → spawn immediately, without waiting for the next autoscaler signal.
- **Enforce ceiling**: if `current_workers > worker_ceil` → remove **1 per maintenance cycle** (not all at once — same gradual pacing as autoscaler scale-down; workers mid-job complete before exiting). This handles stale over-allocation when a midnight recompute lowers capacity.

**effective_target (gap fix):** When scan lends 2 workers to fullscan during a deadlock, scan's `current_workers` drops from 8 to 6. Without `effective_target`, the per-cycle maintenance sees a deficit=2 and immediately spawns 2 scan workers — undoing the borrow every cycle, thrashing against the reactive layer for the entire recovery period.

```
effective_target uses the already-persisted manager:borrow:* keys — no new Redis state:

borrowed_out(pool) = sum of all manager:borrow:{pool}:* values
borrowed_in(pool)  = sum of all manager:borrow:*:{pool} values

effective_target(pool) = workers_target − borrowed_out(pool) + borrowed_in(pool)

scan lent 2 to fullscan:
  effective_target(scan)     = 8 − 2 + 0 = 6  →  current=6, deficit=0 → don't spawn ✓
  effective_target(fullscan) = 5 − 0 + 2 = 7  →  current=7, deficit=0 → don't spawn ✓
```

**pending_spawns tracking (gap fix):** Worker startup takes 10–30s. Without tracking, the manager sees the deficit unchanged on the next cycle and spawns again — double-spawning every cycle until workers finish starting up, then immediately over-provisioned.

```
On spawn:   INCR manager:pool:{type}:pending_spawns
            SET  manager:pool:{type}:pending_spawns TTL=90s   (auto-expires if worker fails to start)
On startup: worker signals ready → manager DECRs pending_spawns
```

deficit = target − current_workers − pending_spawns ensures each missing slot is only spawned once.

**Distributed lock (gap fix):** If a manager cycle takes > 60s (slow Redis, large decision tree), a second instance could start and both spawn the same workers.

```
At cycle start: SET manager:lock NX EX 90    (90s TTL — auto-expires if manager crashes mid-cycle)
If lock fails → skip this cycle, try next
```

**Lendable** from another pool = what it has above its own target, never below `WORKER_FLOOR`:

```
lendable(pool) = max(0, current_workers − max(target(pool), WORKER_FLOOR))
```

`target(pool)` here is `workers_target` — the autoscaler's computed demand for the source pool this cycle (Section 4 formula output). Lendable workers are those the source pool has above what it currently needs to serve its own queue. A pool lends what it doesn't need right now, not what it has above its proactive ceiling.

The `WORKER_FLOOR=2` guard ensures no pool is ever stripped below its minimum regardless of how low its target has fallen. Without it, a pool with target=1 and current=3 would show lendable=2 but going to 1 worker leaves it dangerously understaffed if a job arrives mid-borrow.

Example — fullscan has deficit=2, scan has lendable=2:

```
scan:     current=8, target=6, WORKER_FLOOR=2  → lendable = max(0, 8 − max(6,2)) = 8−6 = 2
fullscan: current=3, target=5                  → deficit  = 5−3 = 2

→ reassign 2 slots: scan 8→6, fullscan 3→5
→ fullscan now at its target ✓

Edge case — pool at floor:
scan:     current=3, target=1, WORKER_FLOOR=2  → lendable = max(0, 3 − max(1,2)) = 3−2 = 1
          floor prevents lending the 3rd worker even though target says it's spare
```

---

### Layer 1 — Proactive capacity planner (daily, midnight)

Runs once per day. Computes each pool's ceiling (`worker_ceil`) for the next 24 hours and hands it to Layer 0. Layer 1 does **not** maintain a fixed running count — it sets a soft upper bound that the autoscaler scales within.

#### Step 1 — Measure daily_peak per pool

At end of each day (midnight), record that day's peak demand:

```
daily_peak = max(n_workers at cycles where pool_utilization ≥ 0.50)
```

**Intraday tracking — maintained in Redis so manager restarts don't lose it:**

```
Each cycle, if pool_utilization ≥ 0.50:
    current = GET manager:pool:{type}:daily_peak:running
    if n_workers > current (or key missing):
        SET manager:pool:{type}:daily_peak:running  {n_workers}

At midnight:
    value = GET manager:pool:{type}:daily_peak:running
    daily_peak = max(int(value or 0), WORKER_FLOOR)   ← 0 treated same as missing (quiet day)
    SET manager:pool:{type}:daily_peak:{YYYY-MM-DD}  {daily_peak}
    SET manager:pool:{type}:daily_peak:running  0    ← reset for new day
```

If the key is missing OR zero at midnight (no cycle had util ≥ 0.50 all day), daily_peak = WORKER_FLOOR.
Note: the key is SET to 0 at midnight (not deleted), so "missing" and "zero" must both be handled
as the quiet-day case — use `max(value, WORKER_FLOOR)` rather than checking `if key is None`.
This prevents recording daily_peak=0 which would set capacity below WORKER_FLOOR via the buffer formula.

**Why ≥ 0.50 utilization?** The Layer 0 scale-down boundary is `util < 0.50`. Workers surviving that boundary had genuine work to do. Workers removed below it were idle overhead. Using the same threshold means daily_peak reflects real demand, not startup bloat.

**Startup bloat example:**

```
detail pool starts day with worker_ceil = 6 (yesterday's proactive ceiling)
Layer 0 may spawn up to 6 workers if demand signals warrant.

Cycles 1–9:   n_workers=6, pool_utilization < 0.50  → Layer 0 scales down
Cycle 10:     n_workers=4, pool_utilization ≥ 0.50  → real demand
Cycles 10+:   n_workers=4, demand holds

daily_peak(detail) = 4   ← not 6 (startup bloat excluded)
```

Without the utilization filter, daily_peak would record 6 every day (the starting ceiling count), and the proactive ceiling would never reflect the true sweet spot.

**Cold start / no history:** On day 1 there are no daily_peak records yet. Rather than
computing a ceiling from missing data (which produces 0 → manager kills all workers),
start with the known steady-state ceilings and let the system self-calibrate.

```
Bootstrap rule (fewer than 28 days of daily_peak records):
  Skip the peak_Nd + buffer formula entirely.
  Use fixed starting ceilings derived from Phase 4 steady-state allocation:

    worker_ceil(detail)   = 6
    worker_ceil(scan)     = 10
    worker_ceil(fullscan) = 5

  These ceilings are permission, not a worker count. Layer 0 scales each pool
  down to its true right-size naturally:
    - Workers with pool_utilization < 0.50 are scaled down each cycle
    - daily_peak:running only records workers with util ≥ 0.50
    - Real demand accumulates in daily_peak:running organically, without
      startup-default counts (e.g. 10 idle scan workers) inflating the record

  After 28 days of daily_peak records, Phase 4 formula takes over automatically.
  No mode flag, no switchover logic needed — the formula simply has data to work with.

  Log a warning each midnight recompute while in bootstrap mode (< 28 days of records).
```

**Why this is simpler and safer than a conservative bootstrap formula:**
Starting low (WORKER_FLOOR-based capacity) forces urgent fires on any real demand spike,
requires emergency Layer 1 recalcs, and risks multiple pools racing to claim spare
connections intraday. Starting at known-good ceilings costs nothing — idle workers are
scaled down within 5 cycles (5 min) by Layer 0, and the utilization filter ensures
daily_peak:running reflects real demand from the very first day.

#### Step 2 — Compute peak_Nd (rolling max, N=28 days)

```
peak_Nd = max(daily_peak over last 28 days)
```

**Why N=28?** 28 days matches the growth_buffer outer window (peak_28d) so both formulas draw from the same history. std_dev over 28 days gives a more stable volatility estimate than a shorter window. A spike stays in the ceiling for one calendar month — appropriate for a weekly-pattern business — then decays naturally.

**Natural decay:** peak_Nd automatically decreases as old high-demand days age out of the rolling 28-day window. No separate mechanism needed.

Example:

```
Day 1:  daily_peak(scan) = 8  (Accenture spike — mass scrape day)
Day 7:  daily_peak(scan) = 5  (normal day)
...
Day N+1: Day 1's record falls out of the window
          if no other day reached 8 → peak_Nd drops 8 → 5 ✓
```

After N days of quiet operation, the proactive ceiling follows actual recent usage downward. A spike inflates the ceiling for exactly N days, then stops — no manual reset needed.

Store per pool in Redis:
```
manager:pool:{type}:daily_peak:{YYYY-MM-DD} = {daily_peak}
peak_Nd = max over all dates in the rolling N-day window
```

#### Step 3 — Compute growth_buffer

**What it measures:** how fast is this pool's peak demand growing week over week? This captures new companies being added.

```
peak_7d  = max(daily_peak over last 7 days)     ← same daily_peak records as Step 2
peak_28d = peak_Nd  (= max over last 28 days)   ← already computed in Step 2; reuse it

weekly_growth_rate = (peak_7d − peak_28d) / peak_28d / 3
```

**Bootstrap fallback:** if fewer than 7 days of history exist, `growth_buffer = 0` — can't compute a growth rate from a single data point. The `buffer = 2` cold-start value from Step 1 covers this window anyway.

**Shrinking demand:** if `peak_7d < peak_28d`, `weekly_growth_rate` is negative → `growth_buffer = ceil(negative) = 0`. peak_Nd itself is already declining naturally; growth_buffer contributes nothing when demand is falling.

Breaking this down with a real example — scan: `peak_7d=8, peak_28d=6`:

```
raw growth over the period  = 8 − 6 = 2 workers
divide by peak_28d (=6)     → 2/6 = 33% total growth since a month ago
                               (dividing by 6 converts workers into a percentage
                                of where we started — same as "prices went from
                                £6 to £8, that is a 33% increase")
divide by 3 weeks           → 33% / 3 = 11% growth per week
                               (peak_28d to peak_7d spans 3 weekly intervals,
                                not 4 — we measure from week 1 to week 4)

weekly_growth_rate = (8 − 6) / 6 / 3 = 0.11 = 11% per week
```

```
growth_buffer = ceil(peak_Nd × weekly_growth_rate)
              = ceil(8 × 0.11) = ceil(0.88) = 1
```

Plain English: *"Scan demand is growing 11% per week — probably new companies being added. Pre-assign 1 extra worker now so next week's load is already covered."*

#### Step 4 — Compute volatility_buffer

**What it measures:** how wildly does daily worker demand swing? This captures unpredictable bursts (Accenture pattern).

Standard deviation of daily peak worker counts over the last N days:

```
scan daily peaks over 7 days: [4, 7, 5, 8, 6, 9, 7]

mean   = (4+7+5+8+6+9+7) / 7 = 6.6

daily swings from mean:  [−2.6, +0.4, −1.6, +1.4, −0.6, +2.4, +0.4]
square each swing:        [6.76, 0.16, 2.56, 1.96, 0.36, 5.76, 0.16]
average of squares:       17.72 / 7 = 2.53
square root (std_dev):    √2.53 ≈ 1.5
```

`std_dev = 1.5` means: *"On any given day, scan worker demand swings roughly ±1.5 workers from its average."*

```
volatility_buffer = ceil(std_dev) = ceil(1.5) = 2
```

Plain English: *"Demand swings by up to 1.5 workers day to day — keep 2 spare to absorb the worst case without hitting reactive steps."*

#### Step 5 — Combine into capacity and set worker_ceil

Growth and volatility are two separate risks covered by the same spare workers — take whichever is larger:

```
buffer         = max(growth_buffer, volatility_buffer)
               = max(1, 2) = 2

capacity(pool) = min(peak_Nd + buffer, pool_ceiling)    ← becomes worker_ceil in Layer 0
               = min(8 + 2, 10) = 10
```

Full picture across all pools:

```
                peak_Nd  growth_buf  volatility_buf  buffer  ceiling  capacity (worker_ceil)
                ───────  ──────────  ──────────────  ──────  ───────  ──────────────────────
detail            4          0             1           1        6        5
scan              6          1             2           2       10       10
fullscan          5          0             1           1        5        5  ← ceiling blocks ideal target of 6
```

Layer 1 sets these ceilings at midnight and hands them to Layer 0 as `worker_ceil`. Layer 0 then dynamically scales each pool between WORKER_FLOOR and worker_ceil throughout the day. Layer 1 does nothing else until the next midnight recompute.

#### When ceiling blocks the ideal target — structural borrowing

Ceilings are soft per-pool fractions of `DB_POOL_MAXCONN`, not hard walls. If one pool's ceiling is not fully used, those slots are available to any other pool that needs more than its own ceiling allows. Phase 4 runs once daily as part of Layer 1 recompute, immediately after Step 5, to set the final `worker_ceil` values:

```
Phase 1 — Compute uncapped ideal targets (no ceiling applied):
  ideal(pool) = peak_Nd + buffer

Phase 2 — Clip to ceiling (soft first-pass allocation):
  actual(pool) = min(ideal(pool), ceiling(pool))

Phase 3 — Count spare slots (total available minus what pools actually claimed):
  total_available = DB_POOL_MAXCONN − 3          (3 reserved: watchdog + scheduler + API;
                                                  manager uses a raw psycopg2.connect() for
                                                  its once-per-25h P75 refresh — no pool,
                                                  zero idle Postgres backends between reads)
  used  = sum(actual(pool) for each pool)
  spare = total_available − used

Phase 4 — Fill structural shortages from spare, priority order (detail → scan → fullscan):
  for pool in [detail, scan, fullscan]:
      shortage = max(0, ideal(pool) − actual(pool))
      grant    = min(shortage, spare)
      actual(pool) += grant
      spare        -= grant

  if any pool still has shortage > 0 after spare is exhausted:
      → capacity overflow path (raise DB_POOL_MAXCONN toward PROBE ceiling,
        or alert if already at PROBE)
```

**Key insight**: a pool is allowed to exceed its own ceiling if spare slots exist. The ceiling only prevents one pool from starving others during normal operation — it is not a hard cap when there is genuine total headroom.

After Phase 4, the result of `actual(pool)` becomes the final `worker_ceil` handed to Layer 0 for the next 24 hours.

---

**Permutation table** (DB_POOL_MAXCONN=25, overhead=3 [watchdog+scheduler+API], total_available=22; ceilings: detail=6, scan=10, fullscan=5):

```
Scenario                               ideal           actual (clipped)    spare   after Phase 4
─────────────────────────────────────  ──────────────  ──────────────────  ─────   ──────────────────────────
User's example:
  fullscan structurally short          d=5  s=8  f=7   d=5  s=8  f=5       4      d=5  s=8  f=7  ✓
  (spare absorbs fullscan shortage=2)

Detail structurally short:
  detail=8 (heavy hiring season)       d=8  s=8  f=5   d=6  s=8  f=5       3      d=8  s=8  f=5  ✓
  (spare covers detail shortage=2)

Scan structurally short:
  scan=13 (mass hiring wave)           d=5  s=13 f=5   d=5  s=10 f=5       2      d=5  s=12 f=5  ✓*
  (* spare=2 → scan gets 2 more → 12; still 1 short → capacity overflow for that 1)

Fullscan + detail both short:
  d=8, f=7                             d=8  s=8  f=7   d=6  s=8  f=5       3      d=8  s=8  f=6  (f still 1 short → overflow)
  priority fills detail first (2),
  1 spare left → fullscan gets 1

Fullscan + scan both short:
  s=12, f=7                            d=5  s=12 f=7   d=5  s=10 f=5       2      d=5  s=12 f=7  ✓
  scan gets priority (2 from spare),
  fullscan gets 0 → overflow for f=2 short

Detail + scan both short:
  d=8, s=12                            d=8  s=12 f=4   d=6  s=10 f=4       2      d=8  s=12 f=4  ✓
  detail priority (2), scan gets 0
  → overflow for scan's remaining 2

All three short (sfd trigger):
  d=8  s=13  f=7                       d=8  s=13 f=7   d=6  s=10 f=5       1      d=7  s=10 f=5  (d gets 1 from spare)
  → detail+scan+fullscan all still                                                  all still short → overflow
    short after spare → capacity overflow
```

After Phase 4, if any pool is still short, run the capacity overflow path from the reactive section. The alert email now includes which pools were shorted and by how much:

```
→ alert email:
  Subject: [Pipeline] Pool structural shortage — action needed
  Body:    One or more pools cannot reach their buffer target even after
           redistributing spare slots.

           detail:   target=X  assigned=Y  short=Z
           scan:     target=X  assigned=Y  short=Z
           fullscan: target=X  assigned=Y  short=Z

           DB_POOL_MAXCONN: 25  |  PROBE ceiling: 60
           Manager will attempt autonomous raise on next cycle.
           If already at PROBE, operator action required.
```

---

### Layer 2 — Reactive: emergency when autoscaler hits ceiling

Fires as a sequential escalation ladder — each step gives the previous one time to work before escalating. Priority across all decisions: detail > scan > fullscan.

**Layer 0 urgent fires BEFORE Layer 2 Lever 1**: Layer 0 urgent triggers at delay ≥ DELAY_WARN_S×0.75 (spawning workers and throttling dispatch), while Lever 1 backpressure only fires when delay first crosses DELAY_WARN_S. If Layer 0 resolves the burst in time, Lever 1 never fires. If delay still reaches DELAY_WARN_S despite urgent workers, Layer 0 and Lever 1 then act together in that cycle — Layer 0 at ceiling, Lever 1 halting inflow. These are complementary: Layer 0 adds capacity, Lever 1 reduces demand. Deadlock resolution (worker borrowing) waits for 3 cycles of confirmed rising delay after Lever 1 fires.

```
Delay rising
     │
     ├── delay > DELAY_WARN_S           ──→  Lever 1 fires (backpressure)
     │   (crossed the line)                    halt inflow, give workers a chance
     │                                         to drain what's already there
     │                                         snapshot D and R immediately (see learning loop)
     │
     └── delay > DELAY_WARN_S           ──→  Deadlock resolution (worker borrowing)
         AND current_workers                   only when Lever 1 hasn't resolved it
             + pending_spawns ≥ worker_ceil   after 3 cycles — this is expensive
         AND trending up over 3 cycles
```

**Why not simultaneous?** A brief 90-second spike might self-resolve once inflow stops. Firing worker borrowing immediately causes workers to start moving between pools before they even help — thrashing. Sequential gives each step time to work first.

**"Rising for 3 cycles" definition (gap fix):** "Rising" does not mean strictly monotonic — a single dip resets the counter and deadlock resolution is delayed indefinitely on oscillating load. Use a directional definition instead:

```
Condition: delay > DELAY_WARN_S
           AND current_delay > delay_3_cycles_ago    ← directional, not strictly monotonic
           AND 3 of the last 4 cycles had delay > DELAY_WARN_S

This tolerates one brief dip without resetting the counter, while still requiring
a genuine upward trend rather than a one-off spike.
```

**Lever 2 (heavy hitter deprioritisation) is removed.** At 1s/job, a 500-job detail spike drains in ~83 seconds — the mechanism wouldn't help before it self-resolves. For scan and fullscan there is no one-company-many-jobs pattern. No meaningful use case across any pool.

---

#### Lever 1 — Backpressure: halt inflow (fires at delay > DELAY_WARN_S)

The only genuinely useful demand-reduction lever. Halting inflow gives existing workers time to drain the current backlog without new work arriving. This works because it stops new work at the source — it is not deferring, it is controlling the flow rate.

**Slow dispatch and interval multipliers are NOT used.** They only affect future scheduling — companies already sitting overdue in the ZSET are completely unaffected. They defer the problem, not fix it.

*Detail:*
```
Both scan_worker AND fullscan_worker pause before pushing to detail queues:
  scan_worker  → pauses push to queue:detail:adaptive
  fullscan     → pauses push to queue:detail:fullscan
Resume: delay < DELAY_WARN_S_detail × 0.5 for 3 consecutive cycles
        → lift backpressure at rate-limited push (see priority inversion note below)
        → return to unrestricted push when: actual_delay < DELAY_WARN_S × RECOVERY_STABILITY_RATIO for 3 cycles
          (RECOVERY_STABILITY_RATIO = 0.25 → threshold = 15s for detail; same constant used in
           two-pool ordering, three-pool ordering, and worker-return trigger — see config.py S11)
```

**In-flight lag (edge case):** Lever 1 halts scan/fullscan workers from pushing to detail queues. But workers that are already mid-job at the moment Lever 1 fires continue executing and WILL push their results when they finish — for up to `est_fetch_s` seconds (scan P75 ≈ 40s). The detail queue may continue growing briefly after Lever 1 fires. This is acceptable: the backpressure formula already accounts for it (`time_left = DELAY_WARN_S − actual_delay`; as delay continues rising, `backpressure_depth` tightens automatically, and workers already at DELAY_WARN_S can drain the lag without a secondary response. No additional handling is needed for this transient.

**Priority inversion when lifting detail backpressure (gap fix):** During backpressure, scan and
fullscan workers kept completing jobs. Each completed scan normally pushes N jobs to the detail
queue — those pushes were blocked. The moment backpressure lifts, all held pushes flush
simultaneously, potentially re-deadlocking detail immediately after recovery.

```
Fix: when lifting detail backpressure, rate-limit the flush:
  Resume scan + fullscan pushes at:
    max_push_rate = n_detail_workers / est_fetch_s    (jobs/s detail can absorb; P75 from cache)

  Rate-limiting applies per individual job, not per batch:
    inter_job_interval = 1 / max_push_rate            (seconds between each individual job push)
    total_batch_time   = batch_size / max_push_rate   (total time to flush a full batch at this rate)

  scan_worker:  enforce inter_job_interval between each job pushed from a batch
  fullscan:     same rate

  Return to unrestricted push only after detail delay < DELAY_WARN_S × RECOVERY_STABILITY_RATIO for 3 cycles.
  This is the same systematic re-introduction logic applied to inflow resumption.
```

*Scan:*
```
Scheduler halts all new dispatch to poll:adaptive (stop adding companies to scan queue)
Existing scan workers drain the current backlog uninterrupted
Resume: systematic re-introduction — 1 company every (est_fetch_s / n_workers) seconds
        applies even if deadlock resolution never fired (see formula below)
        stop when: ZCOUNT poll:adaptive -inf (now − DELAY_WARN_S) == 0 for 2 cycles
```

*Fullscan:*
```
Scheduler halts all new dispatch to poll:fullscan
Existing fullscan workers drain the current backlog uninterrupted
Resume: systematic re-introduction — 1 company every (est_fetch_s / n_workers) seconds
        applies even if deadlock resolution never fired (see formula below)
        stop when: ZCOUNT poll:fullscan -inf (now − DELAY_WARN_S) == 0 for 2 cycles
```

---

#### Deadlock resolution — worker borrowing (fires when Lever 1 hasn't resolved after 3 cycles)

**Systematic re-introduction formula (used after Lever 1 is lifted — whether or not deadlock resolution fired):**

Whenever Lever 1 is lifted, don't resume full dispatch at once — the overdue ZSET has been accumulating the whole time backpressure was active, and dumping it at once creates a secondary spike identical to the original. Rate-limit re-introduction whether the recovery happened in 1 cycle or 10.

**Re-introduction completion condition** — stop rate-limiting when the overdue backlog is truly gone:

```
scan / fullscan:  ZCOUNT poll:{type} -inf (now − DELAY_WARN_S) == 0
                  for 2 consecutive cycles     ← no overdue companies remaining
detail:           actual_delay < DELAY_WARN_S × 0.10 for 2 consecutive cycles
                  (detail queue empties fast; delay signal is more reliable than LLEN == 0
                   which can flicker between pushes)
```

Push overdue companies at exactly the rate workers can handle:

```
throughput = n_workers / est_fetch_s          (companies or jobs per second; P75 from cache)

all pools:  push 1 item every (est_fetch_s / n_workers) seconds
            est_fetch_s = scaling_params[pool]["fetch_p75"]

Example — 10 scan workers, est_fetch_s = 40s:
  throughput = 10 / 40 = 0.25 companies/s → 1 company every 4 seconds
  scheduler pushes overdue companies at this rate until all are dispatched
```

**est_fetch_s uses P75 everywhere, not mean:** A single outlier (e.g. Eightfold fullscan at 831s
alongside 20 normal fullscans at 40s) inflates the mean unpredictably. A single unusually fast day
deflates it — making re-introduction too aggressive and workers_target too low.

```
Use P75 of the rolling duration distribution from company_poll_stats.
P75 absorbs outliers without ignoring them entirely.
Store in Redis: manager:pool:{type}:fetch_p75

Use P75 everywhere: re-introduction rate, workers_target, backpressure_depth, learning loop.
P75 > mean → higher est_fetch_s → more workers_target → more conservative in every formula.
The old note "use mean for the learning loop because it is more conservative" was wrong:
higher F in true_required_workers = ceil((D×F/W) + (R×F)) gives MORE workers — P75 is
the conservative choice there too.
```

**Re-introduction rate is dynamic, not locked at start (gap fix):** As borrowed workers are
returned mid-re-introduction, n_workers changes. Locking the rate at the start of re-introduction
means pushing slightly above capacity as workers are taken away.

```
Fix: recalculate throughput = n_workers / est_fetch_s before every push step.
     n_workers = live count from Redis, not the count at re-introduction start.
     est_fetch_s = scaling_params[pool]["fetch_p75"] — same cached value used everywhere.
     Rate adjusts automatically as the pool shrinks or grows during recovery.
```

**Learning loop — derive true required workers (snapshot taken at Lever 1 trigger, cycle 1):**

Every variable is measured from Redis — nothing is hardcoded or assumed. **Critical: snapshot must
be taken at cycle 1 (when delay first crosses DELAY_WARN_S), not at cycle 3 (when deadlock
resolution fires).** By cycle 3, Lever 1 has been running for 3 full cycles — dispatch is halted,
inflow is paused, the queue is already draining. R at cycle 3 ≈ 0 or negative, which severely
understates the true required workers and teaches the proactive layer the wrong peak.

```
At cycle 1 — the moment delay first crosses DELAY_WARN_S (before Lever 1 acts):
  D = queue_depth                                          ← Redis ZCARD / LLEN right now
  W = DELAY_WARN_S                                         ← config constant
  F = est_fetch_s                                          ← P75 from company_poll_stats (see Section 13.2)
  R = inflow_rate = (depth_this_cycle − depth_last_cycle) / cycle_s
                  ← how fast the queue is growing, observed from two consecutive snapshots

Store immediately in Redis (Lever 1 fires right after this snapshot):
  SET manager:snapshot:{pool}:D  {queue_depth}   EX 3600
  SET manager:snapshot:{pool}:R  {inflow_rate}   EX 3600

At cycle 3 (deadlock resolution), read from the stored snapshot — NOT live values.

**Edge case — snapshot D underestimates when urgent fired earlier (known limitation):**
Layer 0 urgent fires at WARN×0.75 and spawns workers immediately. By the time delay
crosses DELAY_WARN_S, those workers have been draining the queue for (0.25×WARN/cycle_s)
cycles — scan: ~7.5 cycles, fullscan: ~30 cycles. D at the snapshot is already lower than
the true burst peak. `true_required_workers` will be slightly understated, causing the
proactive ceiling to be set a touch below ideal. This is acceptable: the system still learns
directionally (ceiling rises), and the next burst will be closer to covered. The alternative
— snapshotting at WARN×0.75 — would capture R before Lever 1 backpressure is available as
a tool, which overstates the inflow problem.

true_required_workers = ceil((D × F / W) + (R × F))
                              ─────────────   ──────
                              drain backlog   keep up with inflow
                              within W secs
```

**What each term means:**

`D × F / W` — workers needed to drain the existing backlog within the deadline:
```
200 companies in queue, each takes 40s, deadline = 300s
→ 200 × 40 / 300 = 26.7 → 27 workers
(27 workers × 300s = 8100 worker-seconds ÷ 40s/company = 202 companies drained ✓)
```

`R × F` — workers needed to keep pace with new arrivals arriving *while* you drain:
```
R is not assumed — it is measured from the queue depth delta:
  depth 60s ago = 140 companies
  depth now     = 260 companies
  cycle_s       = 60s

  R = (260 − 140) / 60 = 2 companies/s

R × F = 2 × 40 = 80 workers permanently occupied just absorbing inflow

Why 80? At 2 companies/s arriving and 40s per company:
  80 workers × 40s = 3200 worker-seconds per 40s window
  = 80 completions per 40s = 2 completions/s = exactly matches inflow ✓

Without these 80 workers, the queue grows by 2 companies every second
no matter how hard the drain workers work — you can never catch up.
```

Combined:
```
true_required_workers = ceil(26.7 + 80) = 107

This says: at the moment the deadlock triggered, truly draining it requires 107 workers.
107 > DB_POOL_MAXCONN capacity → inflow rate is unsustainably high → capacity overflow alert fires.
In practice R is much smaller on normal days (e.g. 0.05 companies/s → R×F = 2 workers).
The formula gives extreme values only when the inflow rate itself is extreme — which is exactly
when you need to know about it.
```

```
After resolution: write true_required_workers into TODAY's daily peak record (not the watermark):
  manager:pool:{type}:daily_peak:{date} = max(existing_daily_peak, true_required_workers)

The rolling window recompute (daily, midnight) then picks it up automatically:
  peak_Nd = max(daily_peak over last N days)

→ target rises for the next N days → proactive layer pre-arms correctly
→ after N days, if demand normalised, peak_Nd decays naturally as the spike day ages out
→ same spike → already covered → reactive steps never fire again
```

**Why write to daily record, not directly to peak_Nd watermark (gap fix):** Writing directly to
the watermark (`peak_Nd = max(peak_Nd, true_required)`) means it never decays. A single spike
on day 1 permanently inflates peak_Nd forever, even when normal demand is much lower. Writing to
the daily record lets the rolling window handle decay naturally — the spike contributes for N days,
then falls out of the window.

Snapshot was taken at Lever 1 trigger (cycle 1, before levers acted) so R reflects real inflow,
not the suppressed inflow after backpressure.

---

#### Capacity overflow: when required workers exceed total available

Scenario: `true_required_workers(pool) + targets(other pools) > workers derivable from DB_POOL_MAXCONN`.

**Manager has two limits, not one:**

```
DB_POOL_MAXCONN        = 25     ← current operating limit (what workers use now)
DB_POOL_MAXCONN_PROBE  = 60     ← server's confirmed safe ceiling (set by operator once)

Manager can autonomously raise DB_POOL_MAXCONN up to PROBE ceiling.
Above PROBE → alert only, operator action required.
```

Raising `DB_POOL_MAXCONN` is not free — it opens more connections to Postgres. Raising blindly past the server's `max_connections` kills the database for everything (API, workers, all of it). `DB_POOL_MAXCONN_PROBE` is the operator-confirmed ceiling beyond which no autonomous action is taken.

**Overflow resolution steps:**

```
Step 1: Is DB_POOL_MAXCONN < DB_POOL_MAXCONN_PROBE?
        YES → raise DB_POOL_MAXCONN in increments of 5
              recompute ceilings, re-check if requirement is now met
              repeat until met or PROBE ceiling reached
        NO  → requirement cannot be met autonomously
              → alert: "need X workers total, server ceiling is Y"
              → operator decides: upgrade server, add read replica, or accept shortage

Step 2: If at PROBE ceiling and still short (not all pools can be satisfied):
        Allocate in priority order — higher-priority pool gets its full requirement first:

        available = DB_POOL_MAXCONN_PROBE − 3    (reserve 3 for watchdog/scheduler/API)

        detail_allocation  = min(true_required(detail),  detail_ceil_at_probe)
        scan_allocation    = min(true_required(scan),    min(scan_ceil_at_probe,   available − detail_allocation))
        fullscan_allocation = max(WORKER_FLOOR, available − detail_allocation − scan_allocation)

        → fullscan accepts being shorted (lowest priority, not time-critical)
        → alert which pool was shorted and by how much
```

This means detail is never shorted if there is any capacity at all. Scan is shorted before detail. Fullscan absorbs whatever the other two leave.

---

#### Borrowing mechanics: all at once vs one by one

Not all borrowed workers carry the same risk. The source determines how fast to move:

```
Source                          Speed           Reason
──────────────────────────────  ──────────────  ──────────────────────────────────────────
Lendable workers                ALL AT ONCE     Already above the lending pool's own target.
(pool above its own target)                     Taking them all immediately carries zero
                                                risk — the lending pool still meets its
                                                target after the transfer.

Unused capacity workers         ONE BY ONE      Eating into another pool's safety buffer.
(override another pool's        + check after   Take 1, wait 1 manager cycle (~60s), check
buffer)                         each move       if the deadlock resolved. Stop the moment
                                                it does. Prevents cascading: stealing too
                                                many workers from scan to fix fullscan
                                                could deadlock scan next cycle.
```

**Detail's buffer is protected by default.** Never take from detail's unused capacity for scan or fullscan unless detail's inflow is already halted (backpressure active). Detail inflow halted = detail queue won't grow while you borrow from it.

**Concrete example — fullscan needs 10 workers, has 5:**

```
scan lendable = 2  →  take both immediately          fullscan: 5 → 7    check: still deadlocked
detail lendable = 1 → take immediately               fullscan: 7 → 8    check: still deadlocked
need 2 more; must use unused capacity now:
  take 1 from scan unused capacity                   fullscan: 8 → 9    wait 1 cycle, check
  still deadlocked → take 1 more from scan unused    fullscan: 9 → 10   check
if still deadlocked at 10 → problem is backlog shape, not worker count
  → halt dispatch + drain + systematic re-introduction
```

---

#### Single-pool resolution

**Detail deadlock (d):**
```
Lever 1:  backpressure — halt scan + fullscan from pushing to detail queues
Borrow:   ALL lendable from scan + fullscan immediately (all at once — they're spare)
Escalate: unused capacity from scan + fullscan 1-by-1 with check after each
          (detail is highest priority — can override any other pool's buffer)
Drain → systematic re-introduction of detail inflow
After:    true_required_workers → update peak_Nd permanently
          if true_required > available → capacity overflow steps above
```

**Scan deadlock (s):**
```
Lever 1:  halt scheduler dispatch to poll:adaptive
          scan queue is now bounded — no new companies enter; existing backlog will drain
Borrow:   ALL lendable from fullscan immediately (all at once)
Escalate: unused capacity from fullscan 1-by-1 with check after each
          detail is FULLY PROTECTED — never borrow from detail during scan deadlock.
          Reason: scan workers keep completing jobs and pushing results to detail queues.
          Taking detail workers would cannibalize a pool that is actively receiving that work.
          With inflow halted, scan will drain on its own — no need to touch detail.
Drain → systematic re-introduction: 1 company every (est_fetch_s / n_workers) seconds
After:    true_required_workers → update peak_Nd permanently
          if true_required > available → capacity overflow steps above
```

**Fullscan deadlock (f):**
```
Lever 1:  halt scheduler dispatch to poll:fullscan
Borrow:   ALL lendable from scan + detail immediately (all at once)
Escalate: unused capacity from scan 1-by-1 with check after each (detail protected)
Drain → systematic re-introduction: 1 company every (est_fetch_s / n_workers) seconds
After:    true_required_workers → update peak_Nd permanently
          if true_required > available → capacity overflow steps above
```

---

#### Two-pool deadlocks

Same pattern — halt inflow to both struggling pools simultaneously, borrow from the healthy pool, drain both, re-introduce sequentially by priority (never simultaneously — would cause a combined secondary spike).

**df** (detail + fullscan both struggling):
```
Halt:     backpressure on detail + halt fullscan dispatch
Borrow:   ALL scan lendable immediately → detail first (fill to target), remainder → fullscan
Escalate: scan unused capacity 1-by-1 → detail first until detail resolves, then fullscan
Drain both backlogs
Re-introduce: detail first → stable (delay < DELAY_WARN_S × RECOVERY_STABILITY_RATIO for 3 cycles)
              → fullscan systematic re-introduction
After:    update peak_Nd for both pools; check capacity overflow if either was shorted
```

**ds** (detail + scan both struggling):
```
Halt:     backpressure on detail + halt scan dispatch
Borrow:   ALL fullscan lendable immediately → detail first, remainder → scan
Escalate: fullscan unused capacity 1-by-1 → detail first until resolved, then scan
Drain both backlogs
Re-introduce: detail first → stable → scan systematic re-introduction
After:    update peak_Nd for both pools; check capacity overflow if either was shorted
```

**fs** (fullscan + scan both struggling):
```
Halt:     halt fullscan dispatch + halt scan dispatch
Borrow:   ALL detail lendable immediately → scan first (scan > fullscan priority)
Escalate: detail unused capacity 1-by-1 → scan first until scan resolves, then fullscan
          (detail inflow NOT halted in fs — detail queue could still grow;
           cap detail unused capacity taken to max(0, detail_current − detail_target − 1))
Drain both backlogs
Re-introduce: scan systematic re-introduction first → stable → fullscan re-introduction
After:    update peak_Nd for both pools; check capacity overflow if either was shorted
```

**Key rule for two-pool deadlocks**: re-introduce in priority order, never simultaneously. Higher-priority pool resumes first, must be stable (delay < DELAY_WARN_S × RECOVERY_STABILITY_RATIO for 3 cycles) before lower-priority pool begins its re-introduction.

**Borrowed worker handoff during partial resolution:**

When the higher-priority pool resolves first, its borrowed workers must NOT be returned to their source yet — the lower-priority pool is still recovering and still needs them. Instead, reassign those workers to the next struggling pool.

```
df example:
  detail resolves while fullscan is still draining
  → don't return detail's borrowed scan workers to scan
  → reassign them to fullscan (fullscan still needs them for drain + re-introduction)
  → only return to scan after fullscan is also fully stable

ds example:
  detail resolves while scan is still in systematic re-introduction
  → don't return detail's borrowed fullscan workers to fullscan
  → reassign to scan (scan's re-introduction might spike without them)
  → only return to fullscan after scan is stable

fs example:
  scan resolves while fullscan is still draining
  → don't return scan's borrowed detail workers to detail
  → reassign to fullscan
  → only return to detail after fullscan is stable
```

Worker return trigger: ALL pools in the deadlock group reach `delay < DELAY_WARN_S × RECOVERY_STABILITY_RATIO for 3 consecutive cycles`. Then return borrowed workers to source pools and let the proactive Phase 4 algorithm normalize the final distribution.

**All recovery state must survive manager restart (gap fix):** Manager crashes and systemd restarts it
in 10s. Without persisted state, the restarted manager has no idea a recovery was in progress.

```
When borrow starts:
  SET manager:borrow:{source}:{target} {count}    (no TTL — persists until explicitly cleared)
  Example: SET manager:borrow:scan:fullscan 2

When Lever 1 fires:
  SET manager:lever1:{pool}:active 1              (no TTL — persists until backpressure lifts)

When re-introduction starts:
  SET manager:reintro:{pool}:active 1             (no TTL — persists until re-intro completes)
  SET manager:reintro:{pool}:rate {rate_per_s}    (updated dynamically at every push step)

On manager startup — read these keys before making ANY scaling decisions:
  1. Read manager:borrow:*       → skip removal of borrowed workers; resume borrow tracking
  2. Read manager:lever1:*       → resume backpressure (continue halting dispatch for active pools)
  3. Read manager:reintro:*      → resume rate-limited push at stored rate for active pools
  If none present: clean state — normal autoscaler operation

When borrow is fully returned:
  DEL manager:borrow:{source}:{target}
When Lever 1 lifts:
  DEL manager:lever1:{pool}:active
When re-introduction completes:
  DEL manager:reintro:{pool}:active
  DEL manager:reintro:{pool}:rate
```

Example: manager crashes at 10:45 while scan is in re-introduction (rate=0.025 companies/s). On restart at 10:45:10, manager reads `manager:reintro:scan:active` and `manager:reintro:scan:rate` → immediately resumes rate-limited dispatch at 0.025 companies/s. Without these keys, manager would resume full dispatch → secondary spike.

---

#### Three-pool deadlock (sfd) — all pools struggling simultaneously

```
Step 1: halt all inflows simultaneously
        backpressure on detail, halt scan dispatch, halt fullscan dispatch
Step 2: check and take ALL lendable from every pool
        lendable(pool) = max(0, current_workers − max(target(pool), WORKER_FLOOR))
        A pool can be struggling (delay > DELAY_WARN_S) AND still have lendable workers
        if its target was recently lowered, or if borrowed workers from a prior deadlock
        haven't been returned yet (current_workers > target despite the deadlock).
        Do not assume lendable = 0 in sfd — always check.
        Distribute lendable in priority order: detail first, scan second, fullscan last.
Step 3: if still not resolved after M cycles → capacity overflow path:
        raise DB_POOL_MAXCONN toward DB_POOL_MAXCONN_PROBE in increments of 5
        recompute ceilings (at PROBE=60: scan_ceil ~33, detail ~22, fullscan ~12)
        if already at PROBE → alert, operator action only
Step 4: drain all three backlogs
Step 5: re-introduce in order: detail → scan → fullscan (each must stabilise before next)
        borrowed worker handoff applies here too:
          when detail stabilises → reassign detail's workers to scan (scan still recovering)
          when scan stabilises  → reassign to fullscan (fullscan still recovering)
          when fullscan stable  → all workers return to source, proactive Phase 4 normalises
Step 6: update peak_Nd for all three pools
→ Alert immediately on entering sfd regardless of resolution outcome
→ This state means proactive learning broke down — check why true_required_workers
  was not captured and applied after the last emergency
```

---

## 8. Worker Removal — Two Distinct Scenarios

Removal happens in two different contexts with different selection logic.

**Ordered worker PID list (required for both cases):**

Manager must maintain a per-pool ordered list of worker PIDs in insertion order:

```
Key: manager:pool:{type}:worker_pids   (Redis LIST, permanent)
On spawn:   RPUSH manager:pool:{type}:worker_pids {pid}
On removal: LREM manager:pool:{type}:worker_pids 1 {pid}
```

This gives O(1) access to the most recently added worker (`LINDEX -1`) for LIFO removal (Case B) and the least-recently added worker (`LINDEX 0`) for scale-down removal (Case A, FIFO). Add to Q11 TTL table: permanent by design, rebuilt from live PIDs on manager restart.

### Case A: Scale-down (all queues healthy)

When the manager decides overall capacity is too high and all delays are below threshold, remove the worker at `LINDEX 0` of `manager:pool:{type}:worker_pids` (first-in, first-out — least recently spawned). No queue is under pressure, so the choice has no meaningful impact; LINDEX 0 is fine.

### Case B: Error-spike removal (platform probe caused errors)

This is the dangerous case. The ceiling probe adds 1 worker above the current safe ceiling. If that causes error spikes on a specific platform (e.g., Workday WDX), we need to remove a worker that is actually serving Workday WDX — not a random pop that might kill a Greenhouse worker.

```
# We already know which platform caused the spike (from manager Q9 logic)
target_platform = "workday_wdx"

# Among all workers currently serving that platform:
candidates = workers whose worker:current_job:{pid} shows target_platform

# Among those candidates, remove from the sub-queue with the least delay
# (least impact on throughput — that queue is healthiest and can absorb the loss)
target_worker = candidate whose sub-queue has minimum delay_s

signal_graceful_exit(target_worker)
```

**Why minimum delay within the platform**: if Workday WDX has workers on both the adaptive detail queue and the fullscan detail queue, and the adaptive delay is 10s while fullscan delay is 400s — remove the adaptive one. The adaptive queue is healthier and loses less by giving up a worker.

**Fallback if `worker:current_job:{pid}` lacks platform identity**: `worker:current_job:{pid}` keys exist in Redis but may not include platform identity. Verify during implementation. If they don't, fall back to LIFO: remove `LINDEX -1` of `manager:pool:detail:worker_pids` — the probe worker is always the most recently spawned, so LIFO is accurate for the spike case. This also means the Q9 code never needs to look up `manager:probe:{platform}:worker_pid` — the ordered PID list already gives the right answer.

---

## 9. Head-of-Queue Tracking for Detail Queues

The watchdog already tracks `adp_head_c` and `adp_head_s` (head company + score) for poll queues to detect stalls vs genuine processing. This same pattern should be applied to detail queues.

**Current gap**: for detail queues, the watchdog only looks at depth and depth delta. It has no concept of "is the head of the queue moving?".

**New tracking**: on each manager cycle, record the job_id (or url hash) of the oldest job (LINDEX -1). On the next cycle, if the oldest job is the same → queue has not made forward progress → genuine stall signal. If the oldest job changed → queue is draining even if depth grew (burst scenario).

```
prev_detail_adp_head  = redis.GET("manager:snapshot:detail_adp_head")
curr_detail_adp_head  = json.loads(redis.LINDEX("queue:detail:adaptive", -1) or "{}").get("job_id")

if curr_detail_adp_head == prev_detail_adp_head and curr_detail_adp_head is not None:
    detail_adp_stall_cycles += 1   # head hasn't moved
else:
    detail_adp_stall_cycles = 0    # head moved — draining even if depth grew
```

Combined with queue delay, this gives a two-signal stall confirmation:
- `queue_delay > CRITICAL_S` **AND** `head unchanged for N cycles` → genuine stall → escalate
- `queue_delay > CRITICAL_S` **BUT** `head is moving` → burst → add workers, do NOT restart

---

## 10. What We're Keeping from the Current Design

These are correct and should be preserved exactly:

| Component | Why It Stays |
|---|---|
| BRPOP priority routing (adaptive before fullscan within detail workers) | Correct priority — adaptive is more time-sensitive |
| Band calibration (`recalibrate_band_thresholds`) | Correct approach to adaptive scoring |
| Bloom filter early exit for pagination | Smart early-stop logic, working correctly |
| `slot_offset()` thundering herd prevention | Correct — prevents synchronized scanning bursts |
| `adaptive_seen:{company}` TTL per company | Correct — prevents redundant DB lookups |
| PEL health tracking (stuck stream messages) | Correct — separate concern from queue depth |
| DLQ pattern | Correct |
| `errwin` sliding window for concurrency feedback | Correct — platform-level backoff working well |
| `detail_payload` JSONB persistence on restart | Essential — the Accenture fix |
| `_REBUILD_SKIP_PLATFORMS` | Essential — prevents corrupt rebuild payloads |
| WARMING phase (3 fixed polls for new companies) | Correct |
| Learned ceiling per DC per platform | Correct — protects per-platform ATS limits |
| `context` column on api_health (normal/canary/backoff) | Correct — prevents bad periods from distorting baseline |
| `worker_scaling_events` audit log | Correct — keep writing every scaling decision |
| `DETAIL_QUEUE_ALERT_CYCLES = 3` for Layer 3 | The stall-cycle guard logic is correct; it's in the wrong place (scheduler). Move it to the manager. |
| Scaling lock (`worker:scaling_lock:{platform}`) | Correct — prevents Layer 3 from undoing Layer 2. Keep concept; wire it into the manager. |

---

## 11. What Changes in the Implementation

### Files that need changes

**`workers/watchdog.py`**
- Remove `DETAIL_QUEUE_WARN = 100` and `DETAIL_QUEUE_ALERT = 500` (move to `config.py` as delay-based constants)
- Remove the CRITICAL heal action for `queue_detail_adaptive` and `queue_detail_fullscan` — manager owns those decisions
- Watchdog keeps: scheduler liveness, PEL, DLQ, bloom, coverage miss
- Watchdog does NOT trigger restarts based on queue depth

**`workers/scheduler.py`**
- Remove `_slow_throughput_check_loop` — manager replaces it
- Remove `_fast_error_check_loop` — manager replaces it (see Q9: error spike detection, probe removal, outage declaration, learned ceiling, scan worker removal, scaling lock all move to manager)
- Scheduler still owns: worker spawn/kill commands, band calibration, fullscan scheduling, startup queue rebuild
- Scheduler still publishes `scheduler:health` — manager reads it

**`config.py`**
- Add `DETAIL_DELAY_WARN_S`, `DETAIL_DELAY_ALERT_S`, `DETAIL_DELAY_CRITICAL_S`
- Add `SCAN_DELAY_WARN_S`, `SCAN_DELAY_ALERT_S`, `SCAN_DELAY_CRITICAL_S`
- Add `MANAGER_HEARTBEAT_TTL`, `MANAGER_INTERVAL_S`
- Add fallback P75 defaults: `MANAGER_FETCH_P75_DETAIL`, `MANAGER_FETCH_P75_SCAN`, `MANAGER_FETCH_P75_FULLSCAN` (cold-start only)
- Add `RECOVERY_STABILITY_RATIO = 0.25` — fraction of DELAY_WARN_S a pool must sustain below to be considered "stable" during recovery re-introduction (used in 4 places: detail Lever 1 lift, two-pool re-introduce ordering, three-pool re-introduce ordering, worker return trigger)
- Remove or deprecate `DETAIL_QUEUE_WARN` (currently only in watchdog), `DETAIL_QUEUE_ALERT` (same), `DETAIL_QUEUE_HIGH_WATERMARK`, `DETAIL_QUEUE_MAX_FULLSCAN`, `DETAIL_QUEUE_MAX_ADAPTIVE`

**`workers/scan_worker.py`**
- Fix backpressure to actually pause (not just log) — currently `DETAIL_QUEUE_MAX_ADAPTIVE = 5000` logs only
- New threshold is fully derived: `backpressure_depth = (n_detail_workers / est_fetch_s) × time_left` — no hardcoded constant
- Add blocking poll loop matching what `fullscan.py` already does correctly

**`workers/fullscan.py`**
- Replace hardcoded `DETAIL_QUEUE_MAX_FULLSCAN = 2000` check with derived threshold
- Blocking poll loop already implemented here — keep the structure, update the threshold

**`workers/detail_worker.py`**
- Add ceiling-check-and-skip path: when distributed Redis semaphore for a platform/DC key returns False after all retries, requeue the job atomically using the existing `_ATOMIC_REQUEUE_LUA` script (RPUSH to tail = high priority), then immediately try the next item in the queue
- Remove the per-process `threading.Semaphore` (`_PLATFORM_SEMAPHORES`) — it is vestigial for single-threaded workers and does nothing for cross-worker ceiling enforcement; the distributed Redis semaphore in `ats_get()` is the correct enforcement point
- NOTE: `ats_get()` currently proceeds anyway after semaphore retry exhaustion (line 552–558). For skip-to-next to work, `ats_get()` must signal ceiling-exceeded back to the caller so the detail_worker can requeue. Needs a return value or exception type to distinguish "ceiling full, skip" from "network error, retry".

**New file: `workers/manager.py`**
- Queue delay calculation for all four queues
- Head-of-queue tracking for all four queues
- Cross-pool reassignment logic
- Smart worker removal (minimum-delay queue)
- Per-cycle distributed lock (`manager:lock NX EX 90`) — prevents double-cycling on restart, not an election
- Absorbs slow throughput logic from `scheduler.py`
- Absorbs detail queue scaling decisions from `watchdog.py`

---

## 12. Platform Ceiling Behavior — What's In Place vs What's Missing

Two scenarios were discussed. Results of code review:

### Scenario A: Probe worker caused Workday WDX errors → redirect to other platforms

**Already works implicitly. No new code needed.**

When `adjust_concurrency()` reduces `concurrency:limit:workday_wdx` and `_deprioritise_platform("workday")` pushes Workday companies far forward in the poll:adaptive queue, the probe worker naturally picks up non-Workday jobs next. There is no need to explicitly reassign the worker process — the queue ordering and semaphore limit do it automatically. The worker stays alive and serves Stripe, Greenhouse, etc.

### Scenario B: Worker gets free, next job is Workday WDX at ceiling → skip to Stripe/Greenhouse

**NOT currently implemented. Gap confirmed in code.**

> **BUG — fix before or alongside manager implementation.**
> `http_client.py` lines 552–559: when all 4 semaphore-acquire retries are exhausted,
> `ats_get()` logs a warning and **proceeds anyway** — the ceiling is never actually
> enforced. Fix: raise `CeilingExceeded` instead of proceeding. Callers:
> - `detail_worker`: catch → RPUSH job to tail (high-priority requeue) → pick next item
> - `scan_worker`: let it propagate through the ATS scraper → catch at dispatch loop → skip company this cycle
> Also remove the per-process `threading.Semaphore` (`_PLATFORM_SEMAPHORES`) in
> `detail_worker.py` — it is vestigial for single-threaded workers and gives a false
> impression of cross-worker enforcement.

Current behavior in `ats_get()` (line 538–558): when the distributed semaphore for Workday WDX is at ceiling, it retries up to 4 times with exponential backoff (~7.5s total), then **proceeds anyway** with a warning log. The ceiling is never truly enforced — the call goes through regardless.

Current behavior in `detail_worker.py` (line 612): the per-process `threading.Semaphore` blocks with `with sem:`. For a single-threaded detail worker (one job at a time per process), this semaphore always grants immediately. It provides no cross-worker ceiling enforcement.

**What needs to be built:**

1. `ats_get()` needs a way to signal "ceiling full, skip this job" distinct from a network error. Options:
   - Raise a new exception class `CeilingExceeded` when retries exhausted
   - Return a sentinel `Response` with a synthetic status code
   - Add a `strict=False` parameter: `strict=True` raises `CeilingExceeded` instead of proceeding

2. `detail_worker` main loop: catch `CeilingExceeded`, atomically requeue the job using the existing `_ATOMIC_REQUEUE_LUA` (RPUSH to tail = high priority, picked up again once ceiling frees), then immediately `LMOVE` the next item

3. Remove `_PLATFORM_SEMAPHORES` (threading.Semaphore) from `detail_worker.py` — it is redundant and misleading for single-threaded workers

This skip-and-requeue behavior means a worker blocked by WDX ceiling never stalls — it processes Stripe, Greenhouse, and returns to the WDX job once another worker finishes and frees the semaphore.

---

## 13. Dynamic Parameter Computation (Resolved — 2026-07-20)

Both `est_fetch_s` (P75 of job durations, one value per pool) and `delay_warn_s` are computed from real production data daily and cached in Redis. Neither is hardcoded. Config.py holds safe fallback defaults used only on cold start (no DB data yet).

### 13.1 — Redis cache layout

```
Key:  manager:scaling_params
TTL:  25 hours  (survives a full day + buffer; recomputed on expiry or manager restart)
Value: JSON

{
  "computed_at": "2026-07-20T03:00:00Z",
  "detail":   { "fetch_p75": 4.0,   "delay_warn_s": 60 },
  "scan":     { "fetch_p75": 40.0,  "delay_warn_s": 1728 },
  "fullscan": { "fetch_p75": 95.0,  "delay_warn_s": 8640 }
}

Same structure for all three pools. est_fetch_s = scaling_params[pool]["fetch_p75"] everywhere.
```

Manager reads this once at startup into a local dict. No Redis or DB hit per scaling cycle.

### 13.2 — est_fetch_s: why P75, not mean

**The problem with mean:**

For detail, `SUM(total_ms) / SUM(requests_made)` is dominated by Workday (~90% of requests at ~1s). Result: ~1–1.5s — always close to Workday speed regardless of what's in the queue. Slow platforms (Eightfold ~14s, SuccessFactors ~9.5s) barely move the needle.

For scan/fullscan, the mean is dragged down by fast companies and up by slow outliers (Eightfold fullscan at 831s). Neither direction is reliable.

**Why P75:**

P75 = "75% of jobs complete within this time." It absorbs the slow-platform tail without being dominated by it. We don't need sub-second precision — we need an estimate with a sensible margin that won't wildly undercount workers on a hard day.

```
detail mean  ≈ 1.5s   → severely underestimates Eightfold/SF jobs
detail P75   ≈ 3–5s   → reasonable margin covering typical hard-day mix

scan mean    ≈ 40s    → may undercount if heavy companies pile up
scan P75     ≈ 40–50s → slightly more conservative, stable

fullscan mean  ≈ 69s  → Eightfold (831s) inflates unpredictably
fullscan P75   ≈ 95s  → Eightfold absorbed without dominating
```

**Same lookup for all three pools:**

```python
est_fetch_s = scaling_params[pool]["fetch_p75"]   # read once at startup, zero per-cycle cost
```

No queue sampling, no per-cycle computation, no per-platform counters. Just one cached number per pool, refreshed every 25h.

### 13.3 — est_fetch_s SQL per pool

```sql
-- detail: P75 across all requests, all platforms, last 30 days
SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_ms / NULLIF(requests_made, 0)) / 1000.0
FROM api_health
WHERE date > NOW() - INTERVAL '30 days' AND context = 'normal';
-- Result stored as scaling_params["detail"]["fetch_p75"]

-- scan: P75 of per-company EMA values (written by upsert_poll_stats on every scan)
SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_scan_duration_s)
FROM company_poll_stats
WHERE avg_scan_duration_s IS NOT NULL;
-- Result stored as scaling_params["scan"]["fetch_p75"]

-- fullscan: P75 of per-company EMA values (written by _complete_fullscan_db)
SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_fullscan_duration_s)
FROM company_poll_stats
WHERE avg_fullscan_duration_s IS NOT NULL;
-- Result stored as scaling_params["fullscan"]["fetch_p75"]
```

Same structure, same lookup pattern for all three. See Section 13.2 for why P75 not mean.

### 13.4 — delay_warn_s: how each value is derived

**Detail (60s — policy constant, not data-derived):**
Jobs should never sit in the detail queue longer than a minute regardless of fleet size. This is a product SLA, not an infrastructure metric. Stays in config.py.

**Scan (`p10 of current_interval_s × 0.10`):**

Each company in `poll:adaptive` has a scheduled scan time. `actual_delay` = how many seconds past that time the most-overdue company is.

```sql
SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY current_interval_s) * 0.10
FROM company_poll_stats;
-- p10 ≈ 17,280s (4.8h, avature) × 0.10 = 1,728s ≈ 30 min
```

Reading: *"if the fastest-cycling companies are 30 minutes past their scheduled scan time, we don't have enough workers."*

Why p10 not avg? The average-interval company (7–9h) can tolerate a 30-min delay easily. We want to protect the companies with the tightest schedules — they're the ones that notice a shortage first.

Why 10%? It's "missed one meaningful time slice." 1% late is startup jitter. 10% late means the backlog is real and compounding.

Self-adjusting: add more companies with short intervals → p10 drops → `delay_warn_s` tightens automatically.

**Fullscan (`AVG(full_scan_interval_s) × 0.10`):**

Same logic, longer timescale.

```sql
SELECT AVG(full_scan_interval_s) * 0.10
FROM company_poll_stats;
-- ≈ 86,400s (24h) × 0.10 = 8,640s ≈ 2h
```

Reading: *"if fullscans are running more than 2 hours behind schedule, we need more workers."*

Why avg not p10? Fullscan intervals have very little variance — almost every company uses the same ~24h default. Avg and p10 are nearly identical; avg is simpler.

Self-adjusting: shift to weekly fullscans → avg interval rises → `delay_warn_s` loosens automatically.

**Note — dynamically computed values differ from config fallbacks used in examples:**

The Section 4 worked examples use the config fallback values (scan=1800s, fullscan=7200s) for
round-number clarity. The production system uses dynamically computed values from the SQL above:

```
                config fallback     computed (production)     urgent threshold (WARN×0.75)
scan            1800s               ≈ 1728s                   1296s (not 1350s as in examples)
fullscan        7200s               ≈ 8640s                   6480s (not 5400s as in examples)
```

The fullscan discrepancy is significant: examples show urgent at 5400s, but production uses
8640s → urgent fires at 6480s. Both reflect 75% of the actual WARN threshold; the examples
are just calibrated to the cold-start fallback. On day 1 (no DB data), config fallbacks apply.
Once company_poll_stats accumulates any data, the SQL value takes over automatically.

### 13.5 — Config.py fallback defaults (cold-start only)

```python
# Used only when DB has no data yet (fresh install / first day)
MANAGER_AVG_FETCH_S_DETAIL    = 1.5
MANAGER_AVG_FETCH_S_SCAN      = 120.0
MANAGER_AVG_FETCH_S_FULLSCAN  = 300.0
MANAGER_DELAY_WARN_S_DETAIL   = 60
MANAGER_DELAY_WARN_S_SCAN     = 1800
MANAGER_DELAY_WARN_S_FULLSCAN = 7200
```

Once any real data exists in `api_health` or `company_poll_stats`, Redis wins and these are never read again.

### 13.6 — Manager load function

```python
def _load_scaling_params(r: Redis) -> dict:
    raw = r.get("manager:scaling_params")
    if raw:
        return json.loads(raw)
    params = _compute_scaling_params()          # single DB round-trip
    r.set("manager:scaling_params", json.dumps(params), ex=25 * 3600)
    return params
```

Called once at startup. If the key expired mid-day (edge case: manager restarted after 25h), recomputes transparently. Per-cycle scaling loop uses the in-memory dict — zero overhead.

---

## 14. Open Questions (Decide Before Implementing)

> All items below must be resolved before manager.py implementation begins.
> Items 1–7 identified during formula design. Items 8–10 raised 2026-07-20. Items 11–12 raised 2026-07-21.

---

### Manager cycle computation cost (resolved — 2026-07-20)

Before deciding cycle interval, confirm the computation is not a constraint.

**What the manager does each cycle (per pool × 3 pools):**
```
1. Read queue depth         → LLEN or ZCOUNT              (1 op per pool)
2. Read oldest job          → LINDEX -1 or ZRANGE 0 0     (1 op per pool)
3. Read busy_ms per worker  → GET worker:{type}:busy_ms:{pid} × n_workers
4. Read head snapshot       → GET manager:snapshot:{queue} (1 op per pool)
5. Python math              → pure arithmetic, microseconds
6. Write head snapshot      → SET manager:snapshot:{queue} (1 op per pool)
7. Write scaling decision   → INCR/DECR pool counter      (0–1 op, only if scaling)
8. Renew election key       → EXPIRE pipeline:manager:live (1 op total)
```

At max workers (10 scan + 6 detail + 5 fullscan = 21): **~36 Redis ops total**

**Timing:**
```
Without pipelining:  36 ops × 0.3ms RTT = ~11ms per cycle
With pipelining:     all reads batched → 1 round trip → ~2–5ms per cycle
```

**No DB hit per cycle.** Scaling params are cached in Redis (25h TTL), read into memory at startup. The only DB call is `_compute_scaling_params()` once every 25h.

**Implementation: use Redis pipeline for all reads:**
```python
with r.pipeline() as pipe:
    pipe.llen("queue:detail:adaptive")
    pipe.lindex("queue:detail:adaptive", -1)
    pipe.zcount("poll:adaptive", "-inf", now)
    pipe.zrange("poll:adaptive", 0, 0, withscores=True)
    pipe.zcount("poll:fullscan", "-inf", now)
    pipe.zrange("poll:fullscan", 0, 0, withscores=True)
    for wtype, pid in all_worker_pids:
        pipe.get(f"worker:{wtype}:busy_ms:{pid}")
    results = pipe.execute()   # single network round trip
```

**Conclusion:** computation cost is not a constraint on cycle interval. At 60s cycles the manager spends ~0.01% of its time computing. Even at 5s cycles it would be ~0.1%. Cycle interval is a policy choice (how reactive do we want scaling), not a performance limit.

---

### Pre-existing questions

**1. Manager cycle interval (resolved — 2026-07-21)**

Single 60s cycle for everything. Scale-up confirmation = 2 cycles = 2 min. Scale-down = 5 cycles = 5 min. Aligned with worker `cycle_s`. A 5-min outer gate adds latency without safety — the hysteresis cycle counts already provide the required confirmation window.

**2. Worker add/remove mechanism (resolved — 2026-07-21)**

Manager pushes commands to a Redis LIST; scheduler's `_manager_cmds_loop()` thread reads them via BLPOP.

- Manager writes commands via `RPUSH manager:cmds <command>` each cycle (FIFO — commands consumed in insertion order)
- Scheduler daemon thread reads via `BLPOP manager:cmds 0` and applies commands:
  - `{pool}:target:{n}` — spawn or terminate workers until pool size equals N
  - `platform:deprioritize:{platform}` — push platform companies forward 300s in `poll:adaptive`
  - `platform:outage:{platform}:set` — enter 60-min dispatch pause for that platform
  - `platform:outage:{platform}:clear` — exit outage mode early
- Lever 1 halt/resume uses `manager:lever1:{pool}:active` key as before (direct Redis key, no command needed)

Rationale: RPUSH/BLPOP decouples manager from scheduler's dispatch loop and preserves FIFO ordering (BLPOP pops from the head; RPUSH appends to the tail, so the oldest command is always consumed first). Commands survive a manager restart (LIST persists in Redis); BLPOP in the scheduler daemon gives sub-second latency without polling. Option A (pub/sub) loses messages during scheduler restart. Option B (embed manager in scheduler) couples failure domains.

**3. Backpressure threshold (resolved — 2026-07-21)**

No `CATCHUP_WINDOW_S` constant needed. The threshold is fully derived from existing per-cycle values:

```
est_fetch_s        = scaling_params["detail"]["fetch_p75"]   (detail pool's P75 — this formula always measures detail queue capacity)
throughput         = n_detail_workers / est_fetch_s
time_left          = max(DELAY_WARN_S_detail - actual_delay_s, 0)
backpressure_depth = throughput × time_left

if queue_depth > backpressure_depth → scan/fullscan pause pushing to detail
```

Self-adjusting: as actual_delay rises, time_left shrinks → backpressure_depth shrinks → threshold tightens automatically. At actual_delay = DELAY_WARN_S, backpressure_depth = 0 → any queue depth triggers pause. No separate constant per pool needed — the formula is already pool-aware via est_fetch_s and n_detail_workers.

**4. Stall cycle count before restart (resolved — 2026-07-20)**

3 cycles is right for detail workers; scan/fullscan do not use this mechanism at all.

- **Detail**: head-of-queue unchanged for 3 cycles (180s). LIST head changes on job *pickup* — unchanged means no worker is consuming. 3 min confirmation before restart is appropriate.
- **Scan**: ZSET score changes on scan *completion*, not pickup. avg_scan_duration_s ≈ 40s means one scan spans 1 cycle; Eightfold avg ≈ 343s = 5–6 cycles. A 3-cycle check would false-positive on normal long scans. Restart owned by watchdog's `check_hung_workers()` (progress-based, not queue-head-based).
- **Fullscan**: same reasoning, longer durations. Max observed fullscan = 831s = 13+ cycles. `check_hung_workers()` in watchdog is the correct owner.

See Section 5 for the updated manager stall-confirmation block.

**5. `calculate_worker_counts()` at startup (resolved — 2026-07-21)**

Keep `calculate_worker_counts()` as the startup seed; manager takes over from cycle 1 onward.

Rationale: cold-starting at `WORKER_FLOOR=2` under an existing backlog causes a 2+ minute gap before the manager scales up to the correct level. The existing function uses 30-day `api_health` history and produces a good initial estimate at zero extra complexity. Manager cycle 1 immediately re-evaluates and adjusts from there.

**6. Deploy order (resolved — 2026-07-21)**

Two independent phases — no cross-dependency:

- **Phase 1 (now):** Accenture fix (`detail_payload` + `_REBUILD_SKIP_PLATFORMS`) + `avg_scan_duration_s` migration — both blocked on a single `init_db()` run. Deploy together.
- **Phase 2 (after implementation):** Full manager. No Phase 1 dependency; can go out on any deploy after manager is built and tested.

**7. `CeilingExceeded` in `ats_get()` (resolved — 2026-07-21)**

Raise `CeilingExceeded` exception (not a sentinel `Response`). Rationale: type-safe, easy to catch specifically, no ambiguity about whether a Response was real.

Call chain:
- **detail_worker:** catches `CeilingExceeded` → skip-and-requeue via `_ATOMIC_REQUEUE_LUA` (RPUSH to tail so the job is retried with high priority as soon as a slot frees)
- **scan_worker:** does not call `ats_get()` directly — goes through ATS scraper library which calls it. `CeilingExceeded` propagates naturally through the scraper and is caught at the scan_worker dispatch loop → log + skip company (company remains in `poll:adaptive` ZSET and will be re-dispatched normally next cycle).

No sentinel needed. The exception path is the only path that must signal skip-and-requeue.

---

### New questions (2026-07-20)

**8. Manager heartbeat + watchdog monitoring (resolved — 2026-07-20)**

Manager runs as a standalone systemd process (`recruiter-manager`). No election protocol.

- Manager publishes `worker:alive:manager` heartbeat each cycle
- Watchdog monitors it like any other process: heartbeat miss → `sudo systemctl restart recruiter-manager`
- systemd restarts manager on crash (`Restart=always`)
- All manager state is in Redis — restarts are stateless from manager's perspective

See Section 6 for the full process hierarchy and failure coverage table.

**9. Error spike detection — 5-minute loop replacement (resolved — 2026-07-21)**

Workers publish a Redis flag when errwin crosses threshold; manager reads it each 60s cycle.

**Mechanism:**
```python
# In adjust_concurrency() / http_client.py — existing per-request callback:
if error_rate(platform) > ERRWIN_SPIKE_THRESHOLD:
    r.set(f"manager:platform:{platform_key}:error_spike", 1, ex=300)
    # existing rate-limit logic continues unchanged

# In manager — each 60s cycle, after computing pool state:
# Use scan_iter, not r.keys() — KEYS blocks the Redis event loop on large keyspaces
spike_keys = list(r.scan_iter("manager:platform:*:error_spike"))
for key in spike_keys:
    platform_key = key.decode().split(":")[2]
    if r.get(f"manager:probe:{platform_key}:active"):
        # probe is active → this spike was caused by the probe; revert by removing a probe worker
        # manager doesn't know the exact PID (scheduler spawned it); use Case B LIFO:
        # pick the most recently added detail worker from manager:pool:detail:worker_pids (see Section 8)
        candidate_pid = r.lindex("manager:pool:detail:worker_pids", -1)
        if candidate_pid:
            _remove_worker("detail", int(candidate_pid))
        r.delete(key)
        r.delete(f"manager:probe:{platform_key}:active")
```

**What changes — full scope:**
- `fast_error_check_loop()` in `scheduler.py` is removed entirely. All its behaviors move to the manager:
  - **Probe-worker removal on error spike** → manager each 60s cycle (above)
  - **Effectiveness tracking + learned ceiling from inflight** → manager records `inflight_at_spike` at spike time and writes `pipeline:platform:{platform_key}:learned_ceil` if the removal proved effective (error_rate drops on next cycle)
  - **Consecutive-ineffective-reduction tracking** → manager increments a per-platform counter; after `WORKER_CONSEC_REDUCTIONS_THRESHOLD` consecutive ineffective reductions → write `worker:outage:{platform} EX 3600` + alert
  - **Scan worker removal + platform deprioritization** → when outage declared, manager sends a scan worker removal directive via `manager:cmds` and sends `platform:deprioritize:{platform}` (no scaling lock written — manager owns both error detection and pool sizing in the same 60s cycle, so no cross-loop race exists)
- `adjust_concurrency()` rate-limiting stays exactly as-is (fires per-request, no changes)
- Detection latency drops from up to 5 min → at most 60s (one manager cycle)
- Manager never reads errwin directly — only reads the flag, keeping the errwin abstraction inside `http_client.py`

**Key: `manager:platform:{name}:error_spike`** — TTL=300s (auto-expires if manager is down; cleared explicitly by manager when handled)

**10. Ceiling probe + race condition prevention (resolved — 2026-07-21)**

*Sub-problem A — when to probe beyond current ceiling:*

`CEILING_PROBE_CYCLES = 10` (10 min at ceiling with 0 error spikes AND demand present before probing).
`PROBE_CONFIRM_CYCLES = 5` (5 demand-verified clean cycles = 5 min to confirm new ceiling is stable).

Workers set a flag whenever they INCR the concurrency counter to the probed limit (demand signal):
```python
count = r.evalsha(ACQUIRE_SLOT_SHA, 1, f"concurrency:current:{platform_key}", 300)
if count == probed_limit:
    r.set(f"manager:probe:{platform_key}:ceiling_reached", 1, ex=90)
```

```
Per manager cycle, per platform:

  PRE-PROBE (accumulate CEILING_PROBE_CYCLES):
    if ceiling_reached flag set AND no error_spike → clean_cycles_at_ceil++
    if ceiling_reached NOT set → neutral (no demand, don't count, don't reset)
    if error_spike → clean_cycles_at_ceil = 0
    if clean_cycles_at_ceil == 10:
        INCR concurrency:limit:{platform_key}           ← probe: try +1
        SET  manager:probe:{platform_key}:active 1
        confirm_cycles = 0

  CONFIRM WINDOW (accumulate PROBE_CONFIRM_CYCLES):
    if error_spike → DECR concurrency:limit:{platform_key}   ← revert
                     DEL  manager:probe:{platform_key}:active
    if ceiling_reached flag set AND no error_spike → confirm_cycles++
    if ceiling_reached NOT set → neutral (pause clock, no demand = no evidence)
    if confirm_cycles == 5:
        SET  pipeline:platform:{platform_key}:learned_ceil {new_limit}  (no TTL)
        DEL  manager:probe:{platform_key}:active
```

Three cycle states — error spike (revert/reset), demand + no spike (count it), no demand (neutral, pause clock). This prevents falsely confirming a ceiling that was never actually tested because the platform had no jobs during the confirm window.

Rationale: 10 min of real demand at ceiling eliminates transient bursts; 5 demand-verified confirm cycles weeds out coincident spikes that weren't caused by the probe. Both constants live in config.py.

*Sub-problem B — race condition when two managers briefly coexist (TOCTOU):*

Atomic INCR-first pattern (already documented in Section 13):

```python
# Adding a worker (pool-level ceiling):
new_count = INCR pipeline:pool:{type}:worker_count
if new_count > ceiling:
    DECR pipeline:pool:{type}:worker_count   # roll back — ceiling already hit
    return

# Removing a worker:
new_count = DECR pipeline:pool:{type}:worker_count
if new_count < WORKER_FLOOR:
    INCR pipeline:pool:{type}:worker_count   # roll back — floor already hit
    return
```

INCR/DECR are atomic. No two callers get the same return value. Race-free even during the brief window where two manager instances may coexist after a crash.

`pipeline:pool:{type}:worker_count` — permanent (no TTL). On manager restart: re-derive from live worker PIDs and reset the counter to the actual count before entering the main loop.

---

### New questions (2026-07-21)

**11. Redis key retention / TTL audit (resolved — 2026-07-21)**

Resolved TTL policy for every manager-introduced key:

```
Key pattern                                    TTL / retention        Decision
─────────────────────────────────────────────  ─────────────────────  ────────
manager:lock                                   90s                    Auto-expires ✓
manager:pool:{type}:pending_spawns             90s                    Auto-expires ✓
manager:scaling_params                         25h                    Auto-expires ✓
manager:snapshot:{pool}:D                      3600s                  Auto-expires ✓
manager:snapshot:{pool}:R                      3600s                  Auto-expires ✓
worker:{type}:busy_ms:{pid}                    cycle_s × 2            Auto-expires ✓
manager:platform:{name}:error_spike            300s                   Auto-expires ✓ (Q9)
manager:probe:{platform}:ceiling_reached       90s                    Auto-expires ✓ (Q10)

manager:probe:{platform}:active                permanent by design    DEL on confirm or revert ✓ (Q10)
manager:pool:{type}:worker_pids                permanent by design    LREM on removal; rebuilt on restart (S8)
worker:current_job:{pid}                       est_fetch_s × 3 TTL   SET at job start, renewed mid-job,
                                                                      auto-expires on crash (no explicit DEL
                                                                      needed); used by S8 platform removal

manager:lever1:{pool}:active                   permanent by design    DEL on Lever 1 lift ✓
manager:reintro:{pool}:active                  permanent by design    DEL on re-intro complete ✓
manager:reintro:{pool}:rate                    permanent by design    DEL on re-intro complete ✓
manager:borrow:{source}:{target}               permanent by design    DEL on borrow return ✓

manager:pool:{type}:daily_peak:{YYYY-MM-DD}    29-day TTL at write    SET … EX 2505600 ← FIXED
manager:pool:{type}:daily_peak:running         permanent by design    RESET at midnight (not deleted)
pipeline:pool:{type}:learned_ceil              permanent              intentional — config value ✓
pipeline:pool:{type}:worker_count              permanent              intentional — ground truth ✓
```

**daily_peak keys:** set `EX 2505600` (29 days = 2,505,600 s) at write time. Bounded at 3 pools × 29 keys = 87 keys maximum at any time. No cleanup loop needed.

**lever1/reintro/borrow crash safety:** Manager startup sweep — on boot, read all `manager:lever1:*`, `manager:reintro:*`, and `manager:borrow:*` keys and re-evaluate against current queue state. Any key whose condition is no longer true → DEL. This covers crash-before-DEL with no manual intervention.

**learned_ceil and worker_count:** Intentionally permanent — they are configuration/ground-truth values, not transient state. `worker_count` is re-derived from live PIDs on manager restart and reset before entering the main loop.

**12. Stale reference in Section 11 — Manager election (SETNX) (resolved — 2026-07-21)**

Section 11's new file list for `workers/manager.py` includes "Manager election (Redis SETNX)". This was superseded by Q8's resolution: manager is a standalone systemd process with no election protocol. The distributed lock (`manager:lock NX EX 90`) is a per-cycle execution guard, not an election — it prevents a restarting manager from double-cycling, not two competing managers from running simultaneously. The SETNX election reference has been removed from the implementation checklist.

---

**13. Per-platform concurrency ceiling enforcement (resolved — 2026-07-21)**

Three tightly related sub-problems about how platform-level concurrency ceilings (e.g. `concurrency:limit:workday_wdx = 3`) are enforced, protected from races, and safely probed upward. This is distinct from the pool-level worker ceiling (Q10).

**Sub-problem A — ceiling full: RPUSH to tail (high priority requeue)**

```
Worker dequeues WDX job
→ INCR concurrency:current:workday_wdx → result > limit
→ DECR immediately (release the slot)
→ RPUSH job back to queue tail  ← high priority: BRPOP consumes from right, so this job is next
→ LMOVE next item from right and try again
```

RPUSH-to-tail is correct. WDX slots free within ~1s (single api call), so the requeued job will be picked up almost immediately when any WDX worker finishes. LPUSH-to-head would put it at lowest priority and delay it behind all waiting jobs — wrong for a ceiling-retry.

**Sub-problem B — race at ceiling−1 (TOCTOU): Lua acquire-with-TTL**

Wrong (has race):
```python
count = GET concurrency:current:workday_wdx   # workers A and B both read 2
if count < limit:                              # both pass
    INCR ...                                   # both increment → 4 total, limit violated
```

Correct (atomic INCR-first + crash-safe TTL via Lua):
```lua
-- _ACQUIRE_SLOT_LUA
local count = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])   -- e.g. 300s; renewed on each acquire
return count
```

```python
count = r.evalsha(ACQUIRE_SLOT_SHA, 1, f"concurrency:current:{platform_key}", 300)
if count > limit:
    r.decr(f"concurrency:current:{platform_key}")
    raise CeilingExceeded
# proceed ✓
```

INCR is atomic — A gets 3, B gets 4, no tie. `EXPIRE` renewed on each acquire prevents the counter from sticking permanently if a worker crashes mid-job without decrementing. Per-pid slot keys are an alternative but proliferate Redis keys unnecessarily — Lua-with-TTL achieves the same crash safety with a single counter key.

**Sub-problem C — two distinct upward-probing mechanisms (do not conflate)**

`adjust_concurrency()` in `http_client.py` handles **both directions** within the current ceiling per-request:
- error_rate > 10% → decrease limit by 1–2 (immediate, spike-weighted)
- error_rate < 2% → increase limit by 1 (immediate, capped at `CONCURRENCY_CEIL`)

This per-request within-ceiling probing stays exactly as-is. **No changes to `adjust_concurrency()`.**

**Above-ceiling probing** (going past `CONCURRENCY_CEIL` itself) belongs to manager — workers cannot distinguish a transient blip from a genuine platform limit, and only the manager has the global view needed to validate a new ceiling safely:

See Q10 Sub-problem A for the full logic. Key points:
- Workers set `manager:probe:{platform_key}:ceiling_reached EX 90` when INCR reaches the probed limit
- Confirm cycles only count when `ceiling_reached` flag is set — no demand = neutral cycle (clock paused)
- Spike during confirm → immediate revert; 5 demand-verified clean cycles → ceiling permanent

CEILING_PROBE_CYCLES = 10 (~10 min of real demand at ceiling before probing).
PROBE_CONFIRM_CYCLES = 5 (~5 min of demand-verified confirmation).
Both in config.py. Workers never probe independently.

---

## 15. Testing — Comprehensive Test Suite (TODO — Write Alongside Implementation)

The autoscaling design is complete. Before or in parallel with writing `workers/manager.py`,
a comprehensive test suite must be written covering every layer, every state machine transition,
every failure mode, and every edge case documented in this spec.

Tests are the only way to verify that the design works correctly under all scenarios —
including failure, recovery, and the subtle interactions between Layer 0 urgent mode,
Layer 1 midnight recompute, Layer 2 backpressure, and the existing scheduler/watchdog.

**Scope:** Layer 0 state machine, Layer 1 formula + Phase 4 redistribution, Layer 2
backpressure + deadlock resolution, failure injection (worker crash, manager restart,
Redis reconnect, DB down), multi-cycle burst simulations, bootstrap behavior, and
capacity overflow path. Every doc example with specific numbers becomes a test case.

**Structure:**
```
tests/
  unit/        ← pure formula math (drain_rate, workers_target, peak_Nd, Phase 4)
  integration/ ← full cycle loop with real/fake Redis (state machine, midnight recompute)
  scenarios/   ← multi-cycle simulations matching doc burst examples exactly
  failure/     ← crash injection, restart recovery, external service down
```

**Prerequisite:** complete the gap analysis in Section 16 first — responsibility boundaries
between manager, scheduler, and watchdog must be settled before tests can correctly assign
ownership of each behavior.

---

## 16. Gap Analysis — New Design vs Actual Implementation

**Status:** Complete. Based on reading `scheduler.py` (3171 lines), `watchdog.py`,
`http_client.py`, `config.py`, and cross-referencing with `adaptive-polling-architecture.md`
and `ats-fetch-strategy.md`. Analysis covers every behavior in the current code and assigns
ownership in the new world.

---

### 16.1 Naming mismatch — old layers vs new layers

The old design doc and `scheduler.py` use "Layer 1/2/3". The new design uses "Layer 0/1/2".
These are **completely different things**. Do not confuse them.

| Old name | What it does | New equivalent |
|---|---|---|
| Old Layer 1 (liveness, 5s) | Dead worker → immediate replacement | Watchdog (unchanged) |
| Old Layer 2 (fast error, 5min) | Error rate spike → reduce scan workers + deprioritize platform | Manager Layer 0 (partially) + Scheduler dispatch (partially) |
| Old Layer 3 (slow throughput, 30min) | Queue depth → add/remove workers | Manager Layer 0 (replaced entirely) |
| *(new)* Manager Layer 0 | 60s cycle: queue delay → workers_target formula | replaces old Layer 2+3 |
| *(new)* Manager Layer 1 | Midnight: peak_Nd recompute + Phase 4 redistribution | replaces `calculate_worker_counts()` |
| *(new)* Manager Layer 2 | Reactive backpressure at DELAY_WARN_S | replaces DETAIL_QUEUE_MAX_FULLSCAN |

---

### 16.2 Decision Table — Who Owns What After Transplant

`MOVE` = code moves from current owner to new owner and is REMOVED from old owner.
`STAYS` = no change in ownership; behavior preserved as-is.
`REPLACE` = old behavior deleted; new behavior in new owner covers the same need differently.
`NEW` = behavior doesn't exist today; must be written from scratch.
`SPLIT` = behavior is divided: decision logic moves to manager, execution stays in scheduler.

| Behavior | Currently in | New owner | Action | Notes |
|---|---|---|---|---|
| Worker count decision on startup | `scheduler.py:calculate_worker_counts()` | manager.py | REPLACE | Old: 30d avg × load formula at boot. New: Layer 1 midnight recompute + bootstrap ceilings. `calculate_worker_counts()` is deleted from scheduler. |
| Worker spawn / kill execution | `scheduler.py` (multiprocessing.Process) | scheduler.py | STAYS | Manager sends `worker:cmd:{pool}:{count}` to Redis channel; scheduler reads and adjusts its process pool. Manager **never** calls subprocess directly. |
| Liveness check (dead process replacement) | `scheduler.py` (proc.is_alive() loop) | scheduler.py | STAYS | Process-level liveness stays in scheduler. Watchdog's Redis heartbeat check is a parallel signal for alerts, not a replacement. |
| Scaling decision (60s cycle) | `scheduler.py:_slow_throughput_check_loop()` | manager.py Layer 0 | REPLACE | Old: depth-based (DETAIL_QUEUE_HIGH_WATERMARK), 30min interval. New: delay-based, 60s interval, drain_rate formula. Old loop is deleted from scheduler. |
| Error-rate worker reduction | `scheduler.py:_fast_error_check_loop()` | manager.py Layer 0 | SPLIT | Error rate reads stay in manager (reads same Redis keys http_client.py writes). Worker reduction decision moves to manager. Execution still via scheduler Redis channel. |
| Platform deprioritization on high error | `scheduler.py:_deprioritise_platform()` | scheduler.py | STAYS | Manager sends "deprioritize" command on same Redis channel as worker counts (add new command type: `platform:deprioritize:{platform}`). Scheduler pushes ZSET scores forward. This behavior MUST be preserved — it prevents a broken platform from starving healthy queues. |
| Outage mode declaration | `scheduler.py:_fast_error_check_loop()` | scheduler.py | STAYS | After `WORKER_CONSEC_REDUCTIONS_THRESHOLD` ineffective reductions, manager sets `worker:outage:{platform}` key. Scheduler dispatch loop reads this key and skips companies on that platform. Key management (set/expire/clear) moves to manager; key reading stays in scheduler. |
| Canary dispatch during outage | `scheduler.py` adaptive_loop | scheduler.py | STAYS | When `worker:outage:{platform}` is set, scheduler allows one company through per N minutes as canary to detect recovery. No change needed here. |
| Per-DC learned ceiling (HTTP concurrency) | `scheduler.py` + `http_client.py` | scheduler.py + http_client.py | STAYS | `worker:ceil:learned:{dc_key}` is per-DC HTTP concurrency — completely different from manager's pool-level worker ceiling. These two systems coexist: one limits concurrent HTTP requests, the other limits worker process counts. No conflict. |
| Learned ceiling update on error | `_fast_error_check_loop()` | scheduler.py | STAYS | When manager detects high error rate AND sends a worker-reduction command, it also sends a signal to update the learned ceiling. Alternatively: manager sets a Redis flag, scheduler's existing ceiling logic reads it. Simpler: leave ceiling update in fast_error_check logic in scheduler; manager only sends the reduce-worker command. |
| Learned ceiling decay (24h clean → +1) | `_slow_throughput_check_loop()` | scheduler.py | STAYS | This is dispatch-level ceiling management. Keep in scheduler (or move to a separate daemon thread). Manager does not need to know about it. |
| Scaling lock (`worker:scaling_lock:{platform}`) | `scheduler.py` | manager.py | MOVE | Old: fast error check sets lock to prevent slow check from adding workers back. New: manager owns both the error check and the add decision, so there's no race between two separate loops. Lock concept replaced by manager's own cycle-to-cycle state tracking. Delete from scheduler. |
| 60/40 scan/detail DB pool split | `scheduler.py:calculate_worker_counts()` | manager.py | REPLACE | Old: fixed fraction split at startup. New: Phase 4 redistribution at midnight based on demand. The fractions (`WORKER_POOL_SCAN_FRACTION`, `WORKER_POOL_DETAIL_FRACTION`) are deleted from config.py. |
| Fullscan pool in startup formula | `scheduler.py:calculate_worker_counts()` | manager.py | REPLACE | Old: fullscan gets `int(db_budget × WORKER_POOL_FULLSCAN_FRACTION)`. New: bootstrap ceiling=5, Layer 1 recomputes from demand. `WORKER_POOL_FULLSCAN_FRACTION` deleted. |
| Per-DC inflight dispatch slots | `scheduler.py` (`inflight:scans:{dc_key}`, Lua) | scheduler.py | STAYS | These are dispatcher-level throttles (how many HTTP requests in-flight to one datacenter). Manager deals with worker counts, not per-DC in-flight slots. No change. |
| Redis memory alert (Phase 11) | `_slow_throughput_check_loop()` | watchdog.py | MOVE | Old: buried in slow throughput loop. New: watchdog already runs infra checks (Redis health, Postgres health). Redis memory alert belongs there — it's an infrastructure alert, not a scaling decision. |
| `dynamic_floor` calculation | `scheduler.py:_remaining_work_minimum()` | manager.py | MOVE | `_remaining_work_minimum()` calculates a work-driven floor — min workers to finish today's queue. New: manager.py implements this as part of workers_target clamping. Value: never drop below what remaining work needs. Delete from scheduler. |
| `record_scaling_event()` caller | scheduler.py loops | manager.py | MOVE | All worker add/remove decisions move to manager. Manager calls `record_scaling_event()` directly. Scheduler calls it only for liveness replacements (proc.is_alive failures). |
| Band thresholds (adaptive poll intervals) | `scheduler.py:recalibrate_band_thresholds()` | scheduler.py | STAYS | Unrelated to worker scaling. Determines how often each company is polled. No change. |
| `record_cycle_start()` | scheduler.py | scheduler.py | STAYS | Still needed — manager reads `cycle:start` to calculate remaining window. No change. |
| Bloom filter dedup | scan_worker.py, fullscan.py | workers (unchanged) | STAYS | No interaction with manager. |
| DLQ cleanup (7-day retention) | watchdog.py:`_check_dlq_health()` | watchdog.py | STAYS | No change. Watchdog still owns DLQ health for all queues. |
| Hung worker detection | watchdog.py:`check_hung_workers()` | watchdog.py | STAYS | No change. Watchdog reads `worker:progress:{company}` keys. |
| Worker heartbeat keys | workers via `heartbeat.py` | workers + watchdog | STAYS | No change. |
| DETAIL_QUEUE_MAX_FULLSCAN=2000 | config.py → fullscan.py | Deleted | REPLACE | Old: fullscan checks depth > 2000 and pauses. New: manager's Layer 2 backpressure (derived from `backpressure_depth` formula) halts dispatch before fullscan needs to self-limit. Delete constant; remove self-check from fullscan.py. **Do not delete until manager.py Layer 2 is live.** |
| DETAIL_QUEUE_MAX_ADAPTIVE=5000 | config.py → scan_worker.py | Deleted | REPLACE | Old: scan_worker logs a warning at depth > 5000 (does NOT pause). New: manager's backpressure supersedes this. Delete constant; remove check from scan_worker.py. **Do not delete until manager.py Layer 2 is live.** |
| DETAIL_QUEUE_HIGH_WATERMARK=1000 | config.py → scheduler.py slow check | Deleted | REPLACE | Old: slow check add-worker trigger. New: delay-based trigger in manager. Delete after manager Layer 0 is live. |
| DETAIL_QUEUE_ALERT/WARN (watchdog) | watchdog.py (hardcoded 100/500) | watchdog.py → reads from manager | REPLACE | Old: watchdog fires CRITICAL at depth=500 (before scheduler adds workers at depth=1000 — documented inverted ladder bug). New: watchdog reads `manager:backpressure:threshold:{pool}` from Redis (manager writes this on every Layer 0 cycle). If manager is down, watchdog falls back to raw delay check via LINDEX. |
| `est_fetch_s` P75 from DB | *(not implemented)* | manager.py | NEW | Once per 25h via raw `psycopg2.connect()`. Stored in `manager:scaling_params`. |
| Queue delay as primary metric | *(not implemented)* | manager.py | NEW | `LINDEX queue:detail:adaptive -1` → `enqueued_at` → delay_s. Replaces depth as the trigger. |
| `daily_peak:running:{pool}` tracking | *(not implemented)* | manager.py | NEW | Written each 60s cycle for workers with util ≥ 0.50. |
| `peak_Nd` / `worker_ceil` per pool | *(not implemented)* | manager.py | NEW | Layer 1 midnight recompute; stored in Redis. |
| Phase 4 redistribution | *(not implemented)* | manager.py | NEW | Spare slot lending at midnight. |
| Bootstrap mode (< 28 days) | *(not implemented)* | manager.py | NEW | Fixed ceilings: detail=6, scan=10, fullscan=5. |
| Manager→Scheduler command channel | *(not implemented)* | manager.py → scheduler.py | NEW | Redis channel `manager:cmds`. Command types: `{pool}:target:{n}`, `platform:deprioritize:{platform}`, `platform:outage:{platform}`. Scheduler daemon thread reads and executes. |

---

### 16.3 Behaviors in Old Code NOT in New Design (Must Preserve)

These five behaviors exist in the current code, have no equivalent in this design doc, and
must not be lost in the transplant:

**1. Platform deprioritization (`_deprioritise_platform`)**
When error rate is high AND concurrency is at floor, the old code pushes all companies on
that platform forward in `poll:adaptive` by N seconds. This prevents one broken platform from
occupying all scan workers indefinitely. The new design only talks about reducing worker count
— it doesn't address what happens to the queued companies. Add to manager's error-reduction
flow: after sending the reduce-worker command, also send `platform:deprioritize:{platform}`.

**2. Outage mode (`worker:outage:{platform}`)**
After `WORKER_CONSEC_REDUCTIONS_THRESHOLD` consecutive ineffective reductions, the platform
is declared in outage — dispatch is paused for that platform entirely. The new design's Layer 0
would keep firing and keep reducing workers without ever reaching a floor where dispatch is
suspended. Outage mode is the escape hatch. Preserve it exactly: manager declares it, scheduler
enforces it in dispatch, canary dispatch still occurs.

**3. Canary dispatch during outage**
While `worker:outage:{platform}` is set, scheduler allows one company through every N minutes
as a canary to detect recovery. If the canary succeeds (error rate drops), outage mode clears.
This is currently in `adaptive_loop` in scheduler.py. No change needed — keep in scheduler.

**4. Learned ceiling decay (24h clean → ceiling +1)**
When no errors have occurred for 24 hours for a DC key, the per-DC HTTP concurrency ceiling
is incremented by 1. This probes upward toward the true safe maximum. This loop is in the
old `_slow_throughput_check_loop()`. When that loop is removed, this decay logic must be
moved to a separate thread in scheduler.py or to watchdog. It cannot be lost — otherwise
a temporarily lowered ceiling never recovers.

**5. `dynamic_floor` (`_remaining_work_minimum`)**
The old code calculates a work-driven floor: min workers needed to finish today's remaining
queue. This is better than a fixed `WORKER_FLOOR` constant because it prevents removing
workers when there's still a large backlog, even if queue delay is currently low. Manager.py
should implement this as part of `clamp(workers_target, max(WORKER_FLOOR, dynamic_floor), ceil)`.
The exact formula from scheduler.py must be ported, not reimplemented from scratch.

---

### 16.4 Scheduler Changes (Blast Radius Map)

When `workers/manager.py` is introduced, these are the ONLY changes needed in `scheduler.py`:

**Delete (once manager is live):**
- `calculate_worker_counts()` function
- `_fast_error_check_loop()` function  
- `_slow_throughput_check_loop()` function
- Thread that starts those loops in `run_scheduler()`
- `WORKER_POOL_SCAN_FRACTION`, `WORKER_POOL_DETAIL_FRACTION`, `WORKER_POOL_FULLSCAN_FRACTION` from config
- `DETAIL_QUEUE_HIGH_WATERMARK` from config (once manager Layer 0 is live)
- `worker:scaling_lock:{platform}` set/read logic

**Add:**
- Daemon thread reading `manager:cmds` Redis channel
- Handler for `{pool}:target:{n}` — adjust worker pool to target count
- Handler for `platform:deprioritize:{platform}` — call existing `_deprioritise_platform()`
- Handler for `platform:outage:{platform}` — set/clear `worker:outage:{platform}` key

**Move (from slow throughput loop to standalone thread):**
- Learned ceiling decay (24h clean → +1) — extract into `_ceiling_decay_loop()` daemon thread

**Unchanged (touch nothing):**
- All dispatch loops (adaptive_loop, fullscan_loop, their inflight ZSETs)
- `record_cycle_start()`, `get_cycle_start()`
- `recalibrate_band_thresholds()`, `get_band_thresholds()`
- `set_heartbeat()`, `clear_heartbeat()`, `set_progress()`
- `on_adaptive_complete()`, `_atomic_schedule()`
- `rebuild_redis()`, `_redis_watchdog_loop()`
- Canary dispatch logic
- Outage mode key reading in dispatch

---

### 16.5 Config Constants — What Changes

| Constant | Currently used by | After transplant |
|---|---|---|
| `WORKER_POOL_SCAN_FRACTION` | scheduler.py | Delete — replaced by Phase 4 demand-driven allocation |
| `WORKER_POOL_DETAIL_FRACTION` | scheduler.py | Delete — same |
| `WORKER_POOL_FULLSCAN_FRACTION` | scheduler.py | Delete — same |
| `WORKER_FAST_CHECK_INTERVAL_S` | scheduler.py | Delete — manager.py uses 60s fixed interval |
| `WORKER_SLOW_CHECK_INTERVAL_S` | scheduler.py | Delete — same |
| `DETAIL_QUEUE_HIGH_WATERMARK` | scheduler.py | Delete — replaced by delay-based trigger |
| `DETAIL_QUEUE_MAX_FULLSCAN` | fullscan.py | Delete **after** manager Layer 2 is live |
| `DETAIL_QUEUE_MAX_ADAPTIVE` | scan_worker.py | Delete **after** manager Layer 2 is live |
| `WORKER_FLOOR` | scheduler.py | Move to manager.py (manager is the floor enforcer now) |
| `MONITOR_MAX_WORKERS` | scheduler.py | Move to manager.py (ceiling probe uses it) |
| `WORKER_CONSEC_REDUCTIONS_THRESHOLD` | scheduler.py fast_error | Move to manager.py |
| `WORKER_CONSEC_REDUCTIONS_TTL` | scheduler.py | Move to manager.py |
| `CONCURRENCY_ERROR_RATE_REDUCE` | http_client.py + scheduler.py | Split: http_client keeps it; scheduler's use moves to manager |
| `WORKER_SCALING_LOCK_TTL` | scheduler.py | Delete — lock concept replaced by manager state |
| `WORKER_OUTAGE_TTL_S` | scheduler.py | Move to manager.py (manager declares outage) |
| `DB_POOL_MAXCONN` | connection.py + scheduler.py | Stays in connection.py; manager reads it via import |
| `DB_POOL_MAXCONN_PROBE` | *(not yet in config)* | Add to config.py — operator-set ceiling for autonomous raise |
| `WORKER_FAST_CHECK_INTERVAL_S` | scheduler.py | Delete |
| `RECOVERY_STABILITY_RATIO` | *(not yet in config)* | Add to config.py: `0.25` |
| All constants for delay/backpressure | *(not yet in config)* | Add to config.py as part of manager implementation |

---

### 16.6 Redis Key Contract

New keys manager introduces (must not collide with existing keys):

| Key | Type | Writer | Reader(s) | Purpose |
|---|---|---|---|---|
| `manager:scaling_params` | String (JSON) | manager (every 25h) | manager | est_fetch_s + DELAY_WARN_S per pool |
| `manager:worker_ceil:{pool}` | STRING | manager (Layer 1) | manager, scheduler | Pool ceiling from latest recompute |
| `daily_peak:running:{pool}:{date}` | STRING | manager (Layer 0) | manager (Layer 1) | Peak util≥0.50 worker count for peak_Nd formula |
| `manager:cmds` | List | manager | scheduler daemon thread | Worker add/remove/deprioritize/outage commands (RPUSH by manager for FIFO, BLPOP by scheduler) |
| `manager:backpressure:threshold:{pool}` | STRING | manager (Layer 0) | watchdog | Computed backpressure_depth for this pool |
| `manager:borrow:{source}:{target}` | STRING | manager | manager | Recovery borrow count (survives restart) |
| `manager:lever1:{pool}:active` | STRING | manager | scheduler dispatch | Backpressure active flag |
| `manager:reintro:{pool}:active` | STRING | manager | manager | Re-introduction in progress flag |
| `manager:reintro:{pool}:rate` | STRING | manager | manager | Current re-introduction rate |
| `manager:bootstrap` | STRING | manager | manager | Set while < 28 days of daily_peak records |

Existing keys manager READS but does NOT write:

| Key | Written by | Manager reads for |
|---|---|---|
| `queue:detail:adaptive` / `queue:detail:fullscan` | detail_worker | Queue delay (LINDEX -1) |
| `poll:adaptive` / `poll:fullscan` | scheduler dispatch | Overdue job count (ZCOUNT −inf now) + delay = now − min(score ≤ now) via ZRANGEBYSCORE |
| `worker:ceil:learned:{dc_key}` | scheduler / fast_error | Awareness only (HTTP concurrency, separate from pool ceiling) |
| `worker:outage:{platform}` | manager (new owner) | Manager sets; scheduler reads |
| `worker:consec_reductions:{platform}` | fast_error (moving to manager) | Part of outage-mode state machine |
| `cycle:start` | `record_cycle_start()` in scheduler | Remaining window calculation |

---

### 16.7 Integration Seams — Race Conditions to Watch

**1. Manager command vs liveness replacement race**
Manager sends `scan:target:4` (reduce from 5 to 4). Simultaneously, scheduler's liveness
check sees a dead scan worker and spawns a replacement (now 6). Manager's next cycle sees
n_workers=6 and sends `scan:target:4` again. This self-corrects — no bug, just latency.
Acceptable because the max overshoot is 1 worker for 1 cycle (60s).

**2. Outage mode and manager Layer 0**
Manager declares outage. Manager's Layer 0 next cycle sees low n_workers (because outage
mode removed workers), low queue depth (because dispatch is halted), and might conclude
"healthy — no action." This is correct behavior: the pool naturally drains with no new inflow.
Manager must check `worker:outage:{platform}` before making scale-up decisions for a pool
that touches that platform.

**3. Phase 4 at midnight vs in-flight backpressure**
If Layer 2 backpressure is active at midnight, Phase 4 redistribution should be skipped or
run after backpressure lifts. A redistribution that takes workers away from the struggling
pool would worsen the deadlock. Manager checks `manager:lever1:{pool}:active` before running
Phase 4 — if any pool has active backpressure, defer Phase 4 to the next 60s cycle when the
situation may have resolved.

**4. Bootstrap ceiling vs existing workers**
On day 0 of manager.py deployment, the system already has 10 scan workers (current production).
Manager's bootstrap ceiling is 10 for scan. Manager's first Layer 0 cycle: sees 10 workers,
ceiling is 10, util may be < 0.50. Over the next few cycles, manager scales down naturally
(util filter). This is correct. No special migration needed.

**5. Scheduler's `calculate_worker_counts()` vs manager's startup**
Both must not run simultaneously on the first deployment day. Rollout order:
1. Deploy manager.py (starts up, sets Redis ceilings)
2. Remove `calculate_worker_counts()` call from scheduler.py (or gate it: skip if manager
   ceilings exist in Redis)
The gate is cleaner: `if r.exists("manager:worker_ceil:scan"): skip calculate_worker_counts()`.
Remove the gate and the old function together after manager has been running for 7 days.

---

### 16.8 Implementation Order (Based on Gap Analysis)

1. **Add manager:cmds channel handler in scheduler.py** (daemon thread, command router)
   This is zero-risk — just adds a new code path. Nothing breaks if manager isn't running yet.

2. **Extract `_ceiling_decay_loop()` from `_slow_throughput_check_loop()` in scheduler.py**
   Move the learned ceiling decay block to its own daemon thread. Removes coupling before
   the old loop is deleted.

3. **Write workers/manager.py** — Layer 0 only, no Layer 1, no Phase 4, no Layer 2.
   This already improves on the old slow-check (60s vs 30min, delay vs depth).

4. **Validate Layer 0 against thundering herd data** — run in shadow mode (log decisions,
   don't send commands yet). Confirm scaling_params values look right.

5. **Wire Layer 0 commands to scheduler** — manager sends, scheduler executes. Watch for
   the race conditions in §16.7 items 1 and 2.

6. **Delete old loops from scheduler.py** — `_fast_error_check_loop`, `_slow_throughput_check_loop`,
   `calculate_worker_counts`. Gate the last one with the Redis exists check first.

7. **Add Layer 1 (midnight recompute + Phase 4)** — once Layer 0 has accumulated 7 days of
   `daily_peak:running` data.

8. **Add Layer 2 (backpressure)** — once Layer 0+1 are stable. Then delete `DETAIL_QUEUE_MAX_*`
   constants and watchdog depth checks, replacing with `manager:backpressure:threshold:{pool}`.

9. **Update watchdog** — replace hardcoded depth thresholds with manager-derived delay thresholds.

This order means at no point is the system ever without scaling protection. Old protection
stays active until manager's equivalent is proven live and correct.
