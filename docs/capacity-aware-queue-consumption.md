# Capacity-Aware Queue Consumption — Gap Analysis

**Status: NOT YET IMPLEMENTED — identified 2026-07-22**

---

## 0. Relationship to Existing Design

**The basic fix was already identified and designed in `docs/scaling-redesign.md` §12 Scenario B and Q7 (resolved 2026-07-21).** This document was created during a session where we were trying to find the solution — and discovered the solution already existed in the docs.

### What scaling-redesign.md §12 already covers

The core behavior: **when a worker hits the platform ceiling, put the job back in the queue and move to the next item.** Same intent for all three worker types, different mechanics per queue structure:

| Worker | Queue type | CeilingExceeded response | Move on via |
|---|---|---|---|
| Detail | LIST (LMOVE RIGHT, tail = high priority) | RPUSH to tail | `_pop_with_inflight()` |
| Adaptive scan | XREADGROUP stream | XACK + ZADD `poll:adaptive` score=now+30s | `XREADGROUP` next message |
| Fullscan | Checkpoint-based | `_write_checkpoint()` + requeue | Next company dispatch |

Single code change unlocks all three: `ats_get()` line 552-558 must raise `CeilingExceeded` instead of proceeding after retry exhaustion. Each worker already has infrastructure to handle the catch — just needs the catch added.

**Ship this first. Small blast radius. Solves the confirmed bug.**

### What THIS document covers — the structural optimisation

The basic fix handles the immediate problem but has a starvation-at-scale failure mode:

- 500 Accenture/Workday jobs at the front of the queue, all ceiling-blocked
- Worker puts first job back → moves to next → still Accenture → puts back → next → still Accenture
- Cycles through all 500 before reaching 150 processable greenhouse jobs
- At peak (20k jobs, 20 detail workers), this becomes a significant Redis scan problem

The structural fix described in this document (§3 onwards) solves that. **Only implement if starvation at scale is actually observed in production after the basic fix ships.** The basic fix is the 80% solution with 5% of the effort. The structural changes are the remaining 20%.

### Correct implementation order

1. **Ship `CeilingExceeded` fix** — `ats_get()` + catch in each worker. Already designed in scaling-redesign.md.
2. **Observe in production** — does the 500-job pile-up starvation actually manifest?
3. **If yes, ship structural changes** from §3 of this document.

---

## 1. Background

All worker types (scan, detail, fullscan) share the same distributed Redis semaphore per platform:

```
concurrency:active:{dc_key}   ← INCR on acquire, DECR on release
concurrency:limit:{dc_key}    ← current allowed ceiling (floats between floor and ceil)
```

`_acquire()` in `workers/http_client.py` is the atomic gate: it INCRs `active`, compares against `limit`, and DECRs + rejects if over. This is correct and stays. The gap is in **what happens after rejection**.

---

## 2. The Problem — Queue Starvation

### Current failure paths when `_acquire()` fails

| Worker | Queue type | What happens today |
|---|---|---|
| Detail | `queue:detail:adaptive` LIST (BRPOP) | Item already popped. Must RPUSH back to end. Worker loops through same saturated-platform jobs. |
| Scan | `stream:adaptive` XREADGROUP | Message stays in PEL. XAUTOCLAIM reclaims after timeout. Retried by same or another worker. Same platform. |
| Fullscan | similar stream | Same as scan. |

All three paths share the same flaw: **no mechanism to skip a saturated platform and find work from a platform that has available capacity.**

### Concrete starvation scenario

```
queue:detail:adaptive (flat LIST):
  [accenture, accenture, accenture ... × 500, greenhouse × 100, lever × 50]

State:
  concurrency:active:workday = 2
  concurrency:limit:workday  = 2   ← at ceil (held by 2 fullscan workers)
```

What happens:
1. Detail worker BRPOP → gets Accenture/Workday job
2. `_acquire()` → active=3 > limit=2 → rejected → RPUSH Accenture job to end of list
3. Repeat for next Accenture job
4. Worker processes 500 Accenture requeues before reaching greenhouse/lever entries
5. By the time workers reach greenhouse/lever, the 2 fullscan workers may have already finished and released Workday capacity
6. Greenhouse and lever jobs starved for minutes for no reason

The requeue approach makes this worse, not better — it is O(500 requeues) to reach 150 processable jobs, and by then the original bottleneck is gone.

---

## 3. The Fix — `blocked_platforms` SET + Per-Platform ZSET Queues

### Two new Redis keys

```
queue:detail:platforms    SET    {workday, greenhouse, lever, talentbrew, ...}
blocked_platforms         SET    {workday}
```

- `queue:detail:platforms` — which platforms currently have jobs waiting. Updated at enqueue/drain.
- `blocked_platforms` — platforms currently at their concurrency ceiling. Updated inside `_acquire()` / `_release()`.

### Queue structure change

Replace `queue:detail:adaptive` (flat LIST) with per-platform ZSETs scored by enqueue timestamp:

```
queue:detail:workday      ZSET   {job_payload → enqueue_ts, ...}   (500 Accenture entries)
queue:detail:greenhouse   ZSET   {job_payload → enqueue_ts, ...}   (100 entries)
queue:detail:lever        ZSET   {job_payload → enqueue_ts, ...}   (50 entries)
```

Score = `time.time()` at enqueue. ZPOPMIN always returns the oldest waiting job for a platform (highest wait time = highest priority). Workers naturally process oldest jobs first within each platform.

### Worker dequeue — 2 Redis calls, O(1)

```python
# Step 1: which platforms have jobs AND have capacity?
available = r.sdiff("queue:detail:platforms", "blocked_platforms")
# → {greenhouse, lever}   (workday excluded — it's in blocked_platforms)

# Step 2: pick a platform and take the oldest job
platform = pick_platform(available)        # round-robin or shortest-queue
job = r.zpopmin(f"queue:detail:{platform}")

# If queue is now empty, clean up the index
if r.zcard(f"queue:detail:{platform}") == 0:
    r.srem("queue:detail:platforms", platform)
```

Zero scanning of Accenture entries. Zero requeuing. Two Redis round trips.

### `blocked_platforms` maintenance in `_acquire()` / `_release()`

```python
# workers/http_client.py

def _acquire(self, key):
    active = r.incr(f"concurrency:active:{key}")
    limit = int(r.get(f"concurrency:limit:{key}") or CONCURRENCY_CEIL.get(key, CONCURRENCY_CEIL_DEFAULT))
    if active > limit:
        r.decr(f"concurrency:active:{key}")
        r.sadd("blocked_platforms", key)   # ← mark as blocked
        return False                        # CapacityRejected — caller moves to next platform
    # under limit — ensure not in blocked set (recovery path)
    if active < limit:
        r.srem("blocked_platforms", key)
    return True

def _release(self, key):
    active = r.decr(f"concurrency:active:{key}")
    limit = int(r.get(f"concurrency:limit:{key}") or CONCURRENCY_CEIL.get(key, CONCURRENCY_CEIL_DEFAULT))
    if active < limit:
        r.srem("blocked_platforms", key)   # ← unblock when capacity frees
```

`_acquire()` still returns True/False — `CapacityRejected` is not raised because with the new queue design, the worker never committed to a specific job until ZPOPMIN succeeded. If SDIFF already excluded the platform, `_acquire()` is called on a platform that has capacity — rejection is rare (only in the race window between SDIFF and INCR).

### Enqueue change (scan_worker.py)

```python
# Before:
r.rpush("queue:detail:adaptive", json.dumps(job))

# After:
platform = dc_key  # e.g. "greenhouse", "workday", "lever"
r.zadd(f"queue:detail:{platform}", {json.dumps(job): time.time()})
r.sadd("queue:detail:platforms", platform)
```

Cost: one extra SADD per job enqueued. Trivial.

---

## 4. The Accenture/Workday Scenario — With Fix

```
blocked_platforms         = {workday}
queue:detail:platforms    = {workday, greenhouse, lever}

Worker dequeue:
  SDIFF → available = {greenhouse, lever}    ← workday excluded, O(1)
  pick greenhouse
  ZPOPMIN queue:detail:greenhouse → oldest greenhouse job
  _acquire("greenhouse") → active=2 < limit=8 → acquired
  HTTP request proceeds immediately

500 Accenture entries: untouched, sitting in queue:detail:workday

When fullscan workers finish:
  _release("workday") → active drops to 1 < limit=2
  SREM blocked_platforms workday
  Next worker SDIFF → available = {workday, greenhouse, lever}
  Workers start pulling from queue:detail:workday → Accenture jobs processed
```

---

## 5. Universal Applicability — All Worker Types

The same pattern applies to scan and fullscan workers. The only difference is queue type.

| Worker | Current queue | New queue structure | Worker selection |
|---|---|---|---|
| Detail | `queue:detail:adaptive` LIST | `queue:detail:{platform}` ZSET per platform | SDIFF + ZPOPMIN |
| Scan | `stream:adaptive` single stream | `stream:adaptive:{platform}` per platform | SDIFF + XREADGROUP |
| Fullscan | similar stream | `stream:fullscan:{platform}` per platform | SDIFF + XREADGROUP |

For scan and fullscan streams, workers use the same `blocked_platforms` SET to decide which stream to XREADGROUP from:

```python
available_streams = r.sdiff("stream:adaptive:platforms", "blocked_platforms")
platform = pick_platform(available_streams)
msg = r.xreadgroup("scan_workers", worker_id, {f"stream:adaptive:{platform}": ">"}, count=1, block=500)
```

`stream:adaptive:platforms` is maintained by the scheduler at dispatch time (SADD when pushing to a stream, SREM when stream drains).

---

## 6. Race Condition — SDIFF → ZPOPMIN Window

Between the SDIFF read and the ZPOPMIN + `_acquire()`, another worker may have saturated the platform:

```
Worker A: SDIFF → {greenhouse, lever}
Worker B: SDIFF → {greenhouse, lever}
Worker A: ZPOPMIN queue:detail:greenhouse → job1
Worker B: ZPOPMIN queue:detail:greenhouse → job2
Both:     _acquire("greenhouse") → active=2 + 2 = 4, limit=8 → both pass (fine)

OR with a smaller limit:
Worker A: _acquire("talentbrew") → active=3, limit=2 → rejected
```

When `_acquire()` rejects in this race window:
1. ZPOPMIN already consumed the job — it must be re-inserted
2. `r.zadd(f"queue:detail:{platform}", {job: original_score})` — put back with original timestamp, not current time (preserves priority)
3. Worker loops back to SDIFF — this time `blocked_platforms` includes the platform, so it gets excluded

This is O(1) and rare — only fires in the narrow race window, not on every capacity-full situation.

---

## 7. What Changes Per File

### `workers/http_client.py`
- `_acquire()`: add `r.sadd("blocked_platforms", key)` on rejection, `r.srem("blocked_platforms", key)` when under limit
- `_release()`: add `r.srem("blocked_platforms", key)` when active drops below limit

### `workers/scan_worker.py`
- Enqueue path: `r.zadd(f"queue:detail:{platform}", ...)` + `r.sadd("queue:detail:platforms", platform)` instead of `r.rpush("queue:detail:adaptive", ...)`

### `workers/detail_worker.py`
- Main loop: SDIFF + ZPOPMIN instead of BRPOP
- On `_acquire()` rejection (race window): re-insert with original score, loop back

### `workers/scheduler.py`
- Scan dispatch: `XADD stream:adaptive:{platform}` instead of `stream:adaptive`
- Add `SADD stream:adaptive:platforms {platform}` at dispatch
- XAUTOCLAIM: check per-platform streams for stale PEL messages

### New Redis key inventory

| Key | Type | TTL | Owner |
|---|---|---|---|
| `blocked_platforms` | SET | none (maintained by _acquire/_release) | http_client.py |
| `queue:detail:platforms` | SET | none (maintained at enqueue/drain) | scan_worker.py |
| `queue:detail:{platform}` | ZSET | none | scan_worker.py / detail_worker.py |
| `stream:adaptive:platforms` | SET | none | scheduler.py |
| `stream:adaptive:{platform}` | Stream | none | scheduler.py / scan_worker.py |

---

## 8. Connection to Other Gaps

### Utilization-gated probing (`docs/adaptive-polling-architecture.md` §19)
The blind-climb in `adjust_concurrency()` — limit increases even when workers weren't actually using the current limit. Ship alongside this fix: both touch `_acquire()`/`_release()` in `http_client.py`.

### inflight=0 edge case (`docs/adaptive-polling-architecture.md` §19)
When `ZCARD inflight:scans:{dc_key}` = 0 at fast_error escalation time, learned ceiling is set with no real observation. Skip setting learned ceiling when inflight=0. Independent of this fix.

### manager.py transplant (`docs/scaling-redesign.md` §16)
manager.py's Layer 1 and Layer 2 decisions depend on accurate `concurrency:active` utilization signals. With the current starvation problem, workers blocked on saturated platforms inflate idle time and distort utilization metrics. This fix should land before or alongside manager.py to ensure Layer 1 sees clean data.

### Dynamic CONCURRENCY_CEIL probing (task chip `task_9e7ec585`)
When system is at configured ceil with clean error rate, probe beyond config max. Independent enhancement, lower priority.

---

## 9. Implementation Order

1. Add `blocked_platforms` maintenance to `_acquire()` / `_release()` in `http_client.py`
2. Change scan_worker.py enqueue path to per-platform ZSET + `queue:detail:platforms` SET
3. Change detail_worker.py dequeue to SDIFF + ZPOPMIN with race-window re-insert
4. Change scheduler.py dispatch to per-platform streams + `stream:adaptive:platforms` SET
5. Change scan_worker.py XREADGROUP to SDIFF-selected platform stream
6. Migrate existing `queue:detail:adaptive` entries to per-platform ZSETs on deploy (one-time script)
7. Drop `queue:detail:adaptive` LIST after confirming drain

Step 6 migration script: `LRANGE queue:detail:adaptive 0 -1` → parse platform from each entry → `ZADD queue:detail:{platform}`.
