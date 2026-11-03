# Detail pool ceiling, delay threshold and Lever 1 trigger (TH) — design

Status: DESIGN AGREED 2026-11-02 (user: "entire design discussed"); nothing implemented. Blast radius and implementation steps are in section 10. Written 2026-10-31 from the TH discussion.
Format per bug: 1 bug detail, 2 how it behaves today (evidence), 3 proposed solution.
Related: `docs/scaling-redesign.md`, `docs/scan-fullscan-sizing-plan.md`.
Memory: `project_fullscan_requeue_finding_2026_10_30.md`, `project_bug_triage_2026_10_28.md`.

## 0. Order of work

1. Fix the Accenture fullscan re-queue (D4 dedup, section 7) BEFORE deriving any detail ceiling. It is 62% of measured arrivals and is waste. User decision 2026-10-31: D4 is not a blocker for this design; finish the TH design first, D4 is fixed anyway (no bug stays unfixed).
2. Start collecting clean detail data (section 6) and re-measure arrivals after the fix.
3. Then implement TH following section 10 (steps, tests, deploy order, rollback). Nothing here is coded until the user says to start.

## 1. Bug detail (TH)

The detail pool has no data-derived sizing inputs:

- `delay_warn_s` for detail is a fixed 300 s (`_FALLBACK_PARAMS`), not derived from anything.
- The nightly ceiling is `peak_28d + max(growth, volatility buffer)` (`manager.py` `_pool_ideal` 867-897). The peak is polluted by dedup bursts, and the buffers are guesses.
- Lever 1 (`_fire_lever1`) has no queue bound for detail (`LEVER1_MAX_HOLD_S["detail"] = None`) and the backpressure limits are literals (`DETAIL_QUEUE_MAX_ADAPTIVE` 5000, `DETAIL_QUEUE_MAX_FULLSCAN` 2000).
- Related open codes this design covers: TH, BPL-style literals, BI/BP23, L1NOMAX.

## 2. How it behaves today (evidence)

### 2.1 Real arrival data (VM `scheduler.log*`, 16 days, 30-min buckets)

Source: scan_worker "done" lines (`new=M`, counted before the detail filter) plus fullscan "done" lines. Logs on the VM keep only 14-15 days (`scheduler.log.2026-10-21` is missing).

| metric | value |
|---|---|
| total new jobs | 204,347 |
| non-empty buckets | 595 of 768 |
| min / p50 / p90 / p95 / p99 / max | 0 / 35 / 337 / 1266 / 7147 / 8047 |
| top 18 buckets (6000-8047) | 62% of volume, all Accenture Workday fullscans |

Steady load is about 1 worker (0.15 jobs/s x 7 s). `s` = 7 s per job comes from an old trace and is unverified.

### 2.2 Why the bursts are bogus (PROVEN in outline)

- Accenture/workday fullscans (fetched=32739) report new about 7000 repeatedly: 43,555 inserts in 16 days, more than the board size.
- Only 8 old rows survive; 3155 pending_detail from 10-29 are queued.
- pg_stat since 2026-07-08: ins 856,734, del 828,175, live 57,695.
- Cause: `detail_worker` deletes filtered rows (`delete_pending_detail`, `db/job_monitor.py:959-975`); `fullscan.py:1271-1311` falls through to `save_pending_detail` (INSERT ... ON CONFLICT (job_url) DO NOTHING, `db/job_monitor.py:803-855`); the deleted row is re-inserted, queued and counted new.
- Same job ids recurring is not yet proven (only counts).

### 2.3 Current control chain (verified in `workers/manager.py`)

- `_plan_ceilings` (900-957): bootstrap early return (917-927, no soft-ceiling clamp) until 28 daily peak records exist (`BOOTSTRAP_DAYS_REQUIRED` = 28); afterwards actual = min(ideal, `_pool_soft_ceiling`), spare fill in `_POOL_PRIORITY = ("detail","scan","fullscan")` (765), then raise total toward `_seat_cap`.
- `_pool_soft_ceiling` = max(`WORKER_FLOOR`, total // `POOL_CEIL_DIVISOR[pool]`), divisors scan 2, detail 3, fullscan 5. Not used during bootstrap.
- Seats are WORKERS (`WORKER_SEAT_BUDGET` = 22), about 2 DB connections each. `_seat_cap` (`workers/db_capacity.py`) = (`max_connections` - `superuser_reserved` - `DB_CAPACITY_MARGIN`(10) - non-worker connections) // per_worker. Non-worker = manager, watchdog, scheduler, API; measured live. Raise step = `DB_POOL_RAISE_STEP` = 5 connections (about 2 seats).
- Runtime chain in `_check_layer2` (1684-1750). Starts when ALL hold:
  1. delay > `delay_warn_s`;
  2. workers + pending spawns >= worker ceiling;
  3. `_is_deadlock_rising`: at least 3 of the last 4 manager cycles (about 60 s) above warn, and current delay > the oldest of the 4.
  Then in order: `_emergency_recalc` -> `_attempt_borrow` -> `_raise_db_total`. Lever 1 (`_fire_lever1`) fires only if none gained capacity, it is not active, and `organic_rate x service_s > n_workers` (or lambda/s unknown). Lift needs `LEVER1_STABLE_REQUIRED` = 3 stable cycles or the max hold.
- Arrival counter: `signal:arrivals:{queue}:organic` (`workers/queue_stamp.py:29-40`), monotone INCR, never reset; the manager takes deltas (`_organic_inflow_rate`). `job_postings.created_at` undercounts because filtered rows are deleted.

## 3. Proposed solution — detail ceiling from arrival slices

### 3.1 Data: 30-minute arrival slices

- Slice = counter delta over 30 min (not a reset of the counter). Window 28 days x 48 = 1344 slices per queue.
- Store in a Postgres table (not Redis): survives Redis restart or flush. Redis AOF is on (`deploy/configure-redis.sh`), no FLUSH in the repo, but the counter can still reset after a restart; skip a slice whose delta is negative and persist the last sample across manager restarts.
- Row layout (1-minute resolution): `(minute, arrivals)`, ONE combined series for the detail pool (28 d x 1440 = 40,320 rows total, not per queue). `minute` = full timestamp truncated to the minute, e.g. `2026-10-31 14:07`; `arrivals` = new jobs discovered by scans AND fullscans in that minute. Decided 2026-10-31: no per-queue split. Verified: one detail worker drains both queues, taking the queue whose tail has waited longer (`detail_worker.py:365-401`; adaptive first only when a stamp is unreadable or the queue is empty; the "(priority)" banner at `:1092-1093` is stale text) and the manager already sums both counters into one inflow (`manager.py:1360-1363`), and the ceiling C is for that one pool. The manager writes one row per minute from the delta of the discovery counter. The window length T is not stored: sliding sums over the last T minutes are computed from the rows.
- WHERE ARRIVALS ARE COUNTED (decided 2026-10-31, avoids circularity): at DISCOVERY, i.e. when `save_pending_detail` returns True, before any hold, bucket or push. Today `count_arrival` (`workers/queue_stamp.py:29`) runs after the `LPUSH` (`scan_worker.py:582`, `:739`, `fullscan.py:1310`) and the Lever 1 hold branches skip it (`scan_worker.py:576-578`). With a token bucket a count at push time would measure the admitted rate, capped at our own capacity, so it could never show that capacity is too small. The count must move to the save point.
- The rebuild path keeps its separate source (`count_arrival(..., "rebuild")`, `rebuild.py:422`) so recovery pushes are never counted as arrivals. To confirm: a rebuild must not push held `pending_detail` rows past the bucket (let the drainer release them).
- Redis restart or wipe: the Postgres rows are untouched. The Redis counter restarts at 0, so the first delta is negative and that one minute is skipped (and the last sample is persisted across manager restarts). Arrivals whose INCR fails while Redis is down are lost from the count (`count_arrival` swallows errors), an undercount for that period only.
- NO seeding (DECIDED 2026-11-02, section 9.0): the VM-log seed idea is dropped. Bootstrap defaults run until 24 h of rows exist, then the table is used.

### 3.2 Formula

Symbols: A = jobs arriving in one slice, s = seconds per job, u = target utilisation (0.80), p = coverage level (q = 1 - p).

- Per-slice load: `L = A x s / (1800 x u)` workers.
- Detail ceiling: `C = ceil(P_p(L))` over the 28-day slices. With p95 and the measured data: 1266 x 7 / 1800 = 4.92; / 0.8 = 6.15; ceil = **7**.
- This replaces `peak_28d + buffer` for detail. Buffers (growth, volatility) are dropped for detail. Scan and fullscan keep their own sizing.
- Runtime sizing stays `(depth/W + lambda) x s`, W = `delay_warn_s` = 300 s, clamped to the ceiling.

### 3.3 Derived D (no hardcoded delay)

- Queue bound: `Q_max = C x D / s`.
- D is derived, not chosen: `D = A_p x s / C`. This gives `Q_max = A_p`: the queue never needs to hold more than one slice at the covered level.
- p95: C=7, D about 1266 s (21 min). p90: A=337, C=2, D about 1180 s (20 min).
- A 7000-job burst with C=7 takes about 1.9 h to drain. Lever 1 does not speed that up; it bounds queue length and wait.

### 3.4 Detail is allocated LAST

1. Size scan and fullscan first (their own formulas).
2. `C_avail` = total seats - scan seats - fullscan seats.
3. Detail gets `min(ceil(P_95), C_avail)`.
4. If `C_avail` < `ceil(P_95)`, FIRST raise the seat total toward the live seat cap (all long-running processes already subtracted by the probe), THEN reduce the covered level.

### 3.5 Reducing the level (p ladder) and D bound

- Ladder (changed by the user 2026-11-01): start at p99 and step down by 1 (p99 -> p98 -> ... -> p75), stopping at the first p whose `ceil(P_p)` fits `C_avail`; restarts at p99 every night. The old 5-point steps and "step only when ceil drops strictly" are dropped (the stop-at-first-fit rule covers it). Alert thresholds below (p80 / p75) were written for the old ladder and are to be redone, ideally on the per-job on-time share.
- If p95 and p90 are close (flat distribution), stepping gives no relief and a missing worker uncovers most slices at once. Then do not step: raise the capacity alert immediately.
- Continuous equivalent (preferred): `C = min(ceil(P_95), C_avail)`; report the effective p (highest p with `ceil(P_p) <= C`) and `D = A_95 x s / C`.
- D can reach hours when capacity is the limit (A=1266, s=7): C=3 -> 49 min, C=2 -> 74 min, C=1 -> 2.5 h. Two different delays: the design delay at the covered level `D_p = A_p x s / C` (stays about 20 min as p steps down because C shrinks with A_p) and the cost of degrading, the delay a normal p95 slice now suffers `A_95 x s / C` (C=2 -> 74 min). A hardcoded `D_max` was rejected by the user (2026-10-31); the token bucket in 3.6 bounds the queue wait by construction instead.
- Alerts (DECIDED 2026-11-02, replaces the old p80/p75 text): notify by EMAIL at every 5-point step down (effective p = 95, 90, 85, 80), CRITICAL below p75 or when the chain is exhausted (held backlog growing, delay rising, at ceiling, nothing to borrow; this replaces detail Lever 1). The user does not read logs daily, so email must be built (alerts are log-only today); DEFERRED 2026-11-02 by the user: email is an add-on, designed AFTER all bugs are fixed; until then these alerts are log-only. Findings for that later design: manager alerts go to `pipeline_alerts` and are only emailed by `run_pipeline_alert_report` after `--find-only`/`--monitor-jobs` (pipeline.py:273,1022,1075), dedup is per type for 24 h (config.py:379), and the only immediate sender is the watchdog's `_send_email` (watchdog.py:414, Redis NX dedup, Gmail SMTP). A shared sender, a per-rung dedup key, a critical repeat interval and a recovery mail were proposed, not agreed.

### 3.6 Admission by token bucket (user proposal 2026-10-31, replaces the Lever 1 depth bound)

Why not a depth bound: `Q_max = C x D / s = A_p` is a whole slice. A slice-sized burst fills the queue to Q_max within minutes while the oldest job has barely waited and the tail wait equals D, which is the load the pool was sized for. A depth trigger would fire on in-design loads, and p95 means about 2.4 slices per day (48 x 5%) exceed it. Also the first chain step at a fixed 300 s can borrow and return seats repeatedly (borrow returns after 3 calm cycles, `_maybe_return_borrows`, `manager.py:1639-1663`).

Design:
- Bucket capacity `Q_max = C x D / s` (equals the covered-level slice `A_p`); refill rate `Q_max / D = C / s` jobs per second, i.e. the pool's service rate. Only Q_max jobs are admitted per D; the rest wait upstream. Wait, not drop (the article's token bucket drops).
- At a degraded level (C covers only p85) capacity is the p85 slice and the design delay is D85, as the user stated.
- Parameters are recomputed each manager cycle: capacity `n_bucket x D / s`, refill `n_bucket / s`, where `n_bucket` is the base ceiling plus the spare borrowed workers (the corrected rule in 3.7; the live worker count and plain borrows do NOT widen the bucket).
- The queue then never exceeds about Q_max, so the queue wait is bounded by D by construction. No `D_max` and no depth literals (5000 / 2000).
- Excess jobs wait in `pending_detail` (DB rows, cheap) and a drainer pushes them as tokens refill. Total wait for a burst bigger than Q_max is still long (7000 jobs at C=7 about 1.9 h); it is now explicit and measurable instead of sitting in Redis.
- Capacity-shortfall signal: the held backlog is non-empty and growing while the pool is at its ceiling. That replaces "delay > 300 s" as the chain trigger (emergency recalc -> borrow -> seat raise); no fixed delay number.

Verified in code (2026-10-31):
- `_check_layer2` (`manager.py:1684-1726`) already has n_workers, delay_s, depth, arrival rate lambda and service time s in the same cycle, so the bucket parameters are available at the trigger point.
- Today's admission: Lever 1 on/off flag read in `scan_worker.py:516-587` and `fullscan.py:1289-1312`, plus literal depth gates `scheduler.py:1450`, `fullscan.py:1179`. No token bucket exists in the repo (graph + grep); the idea comes from the rate-limiting article fetched 2026-10-28.
- A held job is saved (`save_pending_detail`, `db/job_monitor.py:803`), not marked adaptive_seen, and on rediscovery `save_pending_detail` returns False (row exists), so it is not pushed again. The drainer must therefore push held rows itself; this also closes HO/RI2/F-HOLDORPHAN.

Decided 2026-10-31: NO extra "admit only while queue depth < Q_max" condition. If workers run slower than planned the queue may pass Q_max; that is intended, so the emergency recalc, buffer workers and borrowing still get to act. The bucket only shapes arrivals into the queue.
To confirm: with that condition dropped, slow workers show up only as queue delay, not as held backlog, so the shortfall signal must cover both (held backlog growing, and queue delay rising at the ceiling).

Shortfall signal (agreed 2026-10-31): the chain (emergency recalc, borrow, seat raise) starts on EITHER the held backlog growing at the ceiling (arrivals exceed the pool) OR queue delay rising at the ceiling (workers slower than planned).
Trigger values (proposed 2026-11-01, no literals): held backlog = rows waiting at the bucket in `pending_detail` (how they are marked and counted is to be designed). "Growing" is a trend, not a level: the backlog rose in at least `DEADLOCK_RISING_MIN` of the last `DEADLOCK_HISTORY_CYCLES` cycles and is above the oldest sample (same shape as `_is_deadlock_rising`), while workers plus pending spawns are at the ceiling. The queue-delay leg replaces the fixed `delay_warn_s` = 300 s with the derived design delay D (delay above D means the queue waits longer than designed).
Recommended: ONE bucket for the whole detail pool (workers already take the older tail across both queues, one C, one D, one refill n/s); the drainer releases held rows oldest-discovery first. Separate buckets would need an arbitrary split of n.

Confirmed since: one shared bucket; refill n/s; own-process drainer (3.7); scan/fullscan keep their Lever 1, detail's is removed (9).

### 3.7 Held-job marker (AGREED 2026-11-02; section 9 is the consolidated version)

1. Bug detail: a job held at the bucket and a job already in the Redis queue are both `job_postings.status = 'pending_detail'`; nothing says which. So the held backlog cannot be counted, the drainer cannot find what to release, and `rebuild_detail_queue` (`workers/rebuild.py:366-371`) pushes every `pending_detail` row, bypassing the bucket.
2. Today: `save_pending_detail` (`db/job_monitor.py:803`) inserts the row with no push state; the hold branches (`scan_worker.py:569-587`, `fullscan.py:1289-1312`) save then skip the push; a rediscovery returns False so the row is never pushed (HO/RI2/F-HOLDORPHAN). `job_postings` has `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP` (Postgres timestamps keep microseconds; `db/schema.py:598`; the live column type was not queried) and `id BIGSERIAL PRIMARY KEY` (`:585`); `first_seen` is only a DATE. Each `save_pending_detail` commits on its own, so each row gets its own `created_at`, but two producers can still tie on a microsecond.
3. Proposed:
   - New nullable column `job_postings.pushed_at`: NULL = held (not yet in the Redis queue), set = pushed. Held backlog = `COUNT(*) WHERE status='pending_detail' AND pushed_at IS NULL`, with a partial index on that predicate.
   - Producers (scan_worker, fullscan) only save the row (held) and count the arrival at that point. One drainer releases held rows in order of `(created_at, id)` as tokens allow (`id` breaks microsecond ties, so the order is total even with hundreds of jobs per second): it sets `pushed_at` FIRST, then pushes (a crash in between leaves a row that rebuild re-pushes, never a lost job).
   - Rebuild pushes only rows with `pushed_at` set; held rows stay for the drainer.
   - Cost without a fast path: every job waits up to one drainer period before it is queued. Resolved by syncing the drainer to the token refill (below). A duplicate queue entry is harmless (verified: `complete_pending_detail` only matches `status='pending_detail'`).
   - AGREED 2026-11-02, drainer: its own process (+2 DB connections, about 1 worker seat; `db_capacity.probe` counts it as non-worker automatically, `db/connection.py:81-98`, `db_capacity.py:82`). The manager only writes n, s, D to Redis each cycle; the drainer reads them each wake.
   - AGREED 2026-11-02, token clock: token tick = `s / n` seconds. Tokens are built from elapsed time (capped at capacity `n x D / s`) and kept in a Redis key written by the drainer, so a restart does not reset the bucket. Only the drainer takes tokens, so no atomic script is needed and a burst after an idle spell is still released at once. Producers never push.
   - AGREED 2026-11-02, refill is `n / s` (not `u x n / s`): the bucket matches the pool's full service rate; the 0.8 is already inside the ceiling sizing. The bucket fills while idle up to capacity `n x D / s` and stops there; a burst spends the stored tokens. CORRECTED by user 2026-11-02: (a) the bucket is sized by the CEILING (peak capacity), not by the current worker count, so a pool that has scaled down still releases at its full ceiling rate and the queue can grow and show the shortfall (queue delay rises, scale-up and the chain fire) instead of the bucket hiding it. Tokens are never cut when the ceiling falls (a lower ceiling only limits refill). (b) Borrowed workers do NOT widen the bucket while the pool is still late (a borrow means it is late, admitting more would worsen it). They widen it only as the manager's target falls (AGREED 2026-11-02, "option A"): `n_bucket = base ceiling + min(borrowed, max(0, available - target))`, where available = base ceiling + borrowed. Example: ceiling 7, borrowed 3 (available 10): target 10 -> 7; target 8 -> 9; target <= 7 -> 10. Capacity and refill use `n_bucket`. The target counts the Redis queue backlog, so the extra opens up as the queue drains, with no separate "queue empty" check. NOTE (found in the 2026-11-02 review): held rows are NOT in the Redis queue, so the target does not see them; the held count is deliberately NOT added to the backlog term (decided 2026-11-02, section 10.6 H1). The borrow is returned when the borrower is calm for 3 cycles or the lender reclaims (section 8); the extra is then 0 again. Base ceiling (not the live worker count n) (`_get_base_ceil`, `manager.py:722`) includes emergency-recalc and seat-raise growth (`:1627`) but not `manager:borrow:*` seats. Borrowed workers still drain the queue faster. (c) Missing token key (first start, Redis wipe, unknown bucket state): start with `max(0, capacity - jobs already in the detail queues (+ inflight))` tokens, never a full bucket, so a restart cannot admit a second full burst on top of a nearly full queue. This is an initial value only; it is not a standing depth condition (the rejected 2026-10-31 rule).
   - AGREED 2026-11-02, wake period: wake every `k x tick` where `k = ceil(DRAIN_MIN_WAKE_S / tick)` (config.py env var, proposed default 1 s), so each wake releases a whole number k of tokens. If the tick is already >= the minimum, k = 1. Recomputed on every wake from the live n and s. A job on an idle pool therefore waits less than `DRAIN_MIN_WAKE_S + tick`. Example: s=7 s, n=10 gives tick 0.7 s, k=2, wake 1.4 s, 2 tokens; n=40 gives tick 0.175 s, k=6, wake 1.05 s, 6 tokens.

Two delays for detail (user, 2026-11-01): hold delay = time from `created_at` until `pushed_at` (for jobs still held: age of the oldest row with `pushed_at IS NULL`); queue delay = time in the Redis queue (`queued_at` payload stamp, `workers/queue_stamp.py`; the drainer stamps it at push, so it is the same moment as `pushed_at`). End-to-end delay is their sum and is what the digest cares about.
Drainer placement (DECIDED 2026-11-02: its own process): the manager main loop is one sequential loop with a 60 s sleep and no threads (`manager.py:2207-2216`), which is why the drainer is not a manager thread. The manager only writes the bucket parameters to Redis and the drainer reads them. Verified duplicates: `complete_pending_detail` matches only `status='pending_detail'`, so a second copy of a finished job updates nothing; `detail_worker.py:889-897` logs a WARNING, and `:899-905` still reports outcome "new" and logs "NEW job" again (misleading), plus one wasted detail fetch. Whether the "new" outcome feeds any metric was not checked.

## 4. Soft ceiling

Not needed for detail in the new design (detail takes the remainder). It stays only for the other pools after bootstrap.

## 5. Open decisions

1. DECIDED 2026-10-31: when the pools' targets exceed the seats, the order is scan, then fullscan, then detail (detail last). Replaces the current `_POOL_PRIORITY = ("detail","scan","fullscan")` at `manager.py:765`. Reason: scan finds new jobs fastest, fullscan is the slower re-check, and detail only consumes what they feed it.
2. DECIDED 2026-11-02: T = 30 minutes (config.py env var). Window length T (bucket size). D is about u x T (21 min vs 24 min at T=30 min; ceil rounding makes it slightly smaller), so T is the delay the system accepts, a policy input, not a data-resolution choice. Store arrival deltas at 1-minute resolution (28 d x 1440 = 40,320 rows per queue) and evaluate sliding windows of length T (fixed 30-min buckets split a burst across a boundary and understate A_p). `D_max` dropped. Looked up 2026-10-31: the 7 AM digest takes every `status='new'` row (`db/job_monitor.py:135`), so a job whose detail ends after 7:00 rides the next day's digest; the frontend shows new/digested as soon as detail completes; discovery lag is 3-12 h poll intervals (`config.py:630-641`). Agreed in principle: express the policy as the share of jobs allowed to slip one digest (about D / 24 h if arrivals were even: 21 min about 1.5%, 30 min about 2%, 2 h about 8%), as a config.py setting. The number itself is NOT decided; measure the real share from the per-minute table.
3. SUPERSEDED by section 9.0 (rolling window of 5 valid cycles with busy-second carry; no last-good s). Original text kept for the evidence: `s` = 7 s is only an example number I assumed, not from code. Verified 2026-10-31: the runtime `s` is measured live, `_get_pool_service_time` (`manager.py:377-426`) = sum(busy_ms delta) / sum(processed delta) over the heartbeats and the last `SERVICE_TIME_WINDOW_CYCLES` cycles; it is used at `:1199` and `:2245-2247`; `fetch_p75` (30-day p75 of `api_health` ms per request, `:185-196`, cold fallback 1.5 s at `:114`) is only the fallback while `s` is unknown. For the nightly ceiling to use a measured `s` too, the per-minute row would need busy seconds and jobs done next to arrivals (to decide). `u` = 0.80 is still a design input. User preference 2026-11-01: use the live measured `s` for the nightly ceiling instead of storing busy/done per minute (less exact, simpler, same measure as the runtime sizing). Caveat verified in code: `_get_pool_service_time` returns None when no job finished inside its window (`manager.py:424-425`), which can happen at the nightly recompute while idle, so the ceiling needs a retained last-known-good `s` (not the cold 1.5 s fallback).
4a. SUPERSEDED by 9.2 (W_max is a typed number, 120 min). Kept as background only. Policy measured per job: share of jobs on time = sum(min(A, A_p)) / sum(A) over all windows (A = jobs in one window, A_p = covered window size at level p; only the part of a window above A_p waits longer than D). Example, 10 windows 50,50,60,60,70,80,90,100,200,1000: p90 covers 9 of 10 windows (A_p = 200) but only 960 / 1760 = 55% of the jobs. The min(A, A_p) formula ignores backlog carried into the next window; the middle way between "size for p95" and "size for the top bucket" is to simulate the history with a FIFO backlog for each candidate C (B_t = max(0, B_(t-1) + A_t - K) with K = jobs one pool of C workers finishes per window) and tabulate C against on-time share, average utilisation and drain time of the biggest burst; the ceiling is the smallest C that meets the policy target, capped at C_avail (proposed 2026-11-01, not agreed). The replay on the current data is not trusted (polluted by the D4 re-queues); recompute on clean data.
4. DROPPED (no seeding, 9.0): the replay check on polluted log data. The nightly drain simulation (9.2) runs on the clean per-minute table instead.
5. Commit question for the pending uncommitted work (config.py, deploy.yml, manager.py, tests, docs) — still unanswered, see section 10.7.

## 6. Data collection (before TH is trusted)

The detail stage has no per-job outcome record (filtered / non-US / stale outcomes are DEBUG only: `detail_worker.py` 776, 796, 850, 866; NEW is INFO at 902). AGREED 2026-11-02: one INFO line per detail job with outcome and seconds spent, so `s` and the true arrival split are measurable. Exact format and placement are decided when it is written. Note: DB access stays raw psycopg2 (SQLAlchemy was judged not a fit).

## 7. D4 — Accenture re-queue fix (design pending, not agreed)

Options to evaluate in the 1-2-3 format before any code:

- Keep filtered rows with a `filtered` status so `ON CONFLICT (job_url) DO NOTHING` blocks re-insertion.
- Reuse `seen_job_ids` (`db/schema.py:1096`).
- Check the `adaptive_seen` SREM calls at `detail_worker.py:606` and `:753`.
- Prove first that the same job ids recur (counts only so far).
- After the fix re-measure arrivals; the TH ceiling is derived from the post-fix data.

## 8. Capacity chain order and borrow reclaim (AGREED 2026-11-01, not built)

**1. Bug.** A lender cannot take back seats it lent. `_maybe_return_borrows` (`manager.py:1657-1663`) returns a borrow only when BOTH pools are calm for 3 cycles, and a starved lender is never calm. The borrow key has no TTL. One pool's slowdown spreads to a second pool, worst for detail (detail lends only while its Lever 1 is on, and `LEVER1_MAX_HOLD_S["detail"]` is None, L1NOMAX).

**2. Today.** Chain in `_check_layer2` (`:1731-1733`): emergency recalc → borrow → `_raise_db_total` → Lever 1. The emergency recalc already raises the total through `_plan_ceilings`, so recalc and the raise overlap. `_give_seats` (`:1544-1549`) cuts the lender at once; nothing cuts the borrower back. The scheduler applies `{pool}:target:{n}` by calling `_remove_one_worker` (`scheduler.py:2809-2811`).

**3. Agreed.**
- Chain order: emergency recalc (includes the seat raise) → seat raise → **reclaim own lent seats** → borrow from others (last resort) → Lever 1.
- Return rule: a borrow returns once the BORROWER has been calm for 3 cycles, whatever the lender's state. The handoff to a third struggling pool stays.
- Reclaim, when the lender is at its ceiling with delay above warn and rising: idle seats (borrower running fewer workers than its ceiling) go back at once; seats in use go back ONE per cycle, the borrower shrinking by one worker. Never more than the lender lent.
- No clock value: a borrow lasts only while the lender is not short. A short lender has no free seats (`_borrow_seats` `:1531-1532`), so the borrower cannot borrow them straight back.
- Removal is graceful (`_remove_one_worker` sets the shutdown event, pops the entry from the pool list at once so the lender can spawn in the same cycle). In-flight work on shutdown: detail checks the event only between jobs (`detail_worker.py:1169,1189`), a popped but unstarted job is put back atomically, a started job finishes; scan discards a finished fetch and re-queues the company with backoff (`scan_worker.py:421-432`) or leaves the stream message in the PEL; fullscan writes a checkpoint and resumes (`fullscan.py:1188-1219`). Nothing is lost; scan wastes one fetch. While the old worker finishes its job both workers run, so DB connections briefly exceed the seats (absorbed by DB_CAPACITY_MARGIN).
- STANDING RULE (user, 2026-11-01), bug RMIDLE: when a pool is shrunk (reclaim, scale-down, any `{pool}:target:{n}` below the current size), `_remove_one_worker` (`scheduler.py:2315-2364`) removes a stalled worker first, then an IDLE one, and a worker with a job in progress only when no idle one is left. Today it takes the last entry unless one is stalled. Idle is readable from the heartbeat key `worker:alive:{type}_worker:{host}:{pid}`: `busy_since` is null when no job is running (`heartbeat.py:50-84,192`; written every 10 s, fullscan every 60 s, so the reading can be stale and then costs no more than today). Among busy workers: the one whose job started most recently (least work thrown away) — AGREED 2026-11-02. SEATCAP-DROP (the seat total is only ever raised, never lowered when the live cap falls) is a mechanical fix.

## 9. Held-job marker and drainer (DESIGN FOR APPROVAL 2026-11-02, not built)

**1. Bug.** A job that is "held" is lost until the next scheduler restart (F-HOLDORPHAN / HO / RI2), and a held job is not counted or visible as held.

**2. Today (code-verified).**
- Three places save the row and then decide whether to push: `scan_worker.py:569-587` (adaptive), `scan_worker.py:733-740` (first scan), `fullscan.py:1289-1312`. A hold happens when Lever 1 is active (`:571`, `fullscan.py:1295`) or the re-intro trickle limit is reached (`:574`, `:1300`). The row is already `pending_detail` in Postgres; only the `lpush` is skipped.
- The comments say "re-discovered next scan", but that is wrong. The next scan calls `save_pending_detail`, whose `INSERT ... ON CONFLICT (job_url) DO NOTHING` (`job_monitor.py:837`) returns False for the existing row (`:853`), so the job is not pushed again. Scan adaptive also skips `adaptive_seen`, fullscan adds the job to its new bloom (`:1316`).
- The only thing that pushes such a row is `rebuild_detail_queue` (`rebuild.py:366-371`, every `pending_detail` row, ordered by `first_seen`, a DATE), which runs at a scheduler restart. So held jobs wait for a restart, then arrive as one burst, with the same-day order broken by `enqueued_at` in the payload.
- `job_postings` has `created_at` (`schema.py:598`) but no column that says "pushed". `idx_job_postings_status_seen` is on `(status, first_seen)` (`:613`).

**3. Proposed (items marked AGREED were settled in this discussion).**
- AGREED: producers (the three places above) only save the row and call `count_arrival` (when `save_pending_detail` returns True). They no longer push and no longer hold. The Lever 1 / re-intro hold branches in them are removed.
- New nullable column `job_postings.pushed_at TIMESTAMP`. NULL on a `pending_detail` row means held. Partial index `ON job_postings (created_at, id) WHERE status='pending_detail' AND pushed_at IS NULL`. Added in `init_db` like the other `ALTER ... ADD COLUMN IF NOT EXISTS` steps.
- Drainer = its own process (AGREED), reads n_bucket, s, D from Redis (written by the manager each cycle, section 3.7 option A), keeps the token count in a Redis key (so a restart does not reset it), wakes every `k x s/n` with `k = ceil(DRAIN_MIN_WAKE_S / tick)`.
- Each wake: `release = min(tokens built, held rows)`. Select that many held rows in `(created_at, id)` order (oldest first), `UPDATE ... SET pushed_at = now()` FIRST, commit, then `lpush` with `stamp_queued_at`. Which stream (adaptive or fullscan) comes from `found_by`.
- Crash between the UPDATE and the push loses a job. Mitigation (proposed): `rebuild_detail_queue` pushes rows with `pushed_at` set (so it re-pushes lost ones), and never pushes `pushed_at IS NULL` rows (the drainer owns those). A duplicate push is harmless: `complete_pending_detail` only matches `status='pending_detail'`.
- First start / Redis wipe: bucket starts with `max(0, capacity - queue length)` tokens (section 3.7 (c)).
- Two delays are tracked: hold delay (age of the oldest `pushed_at IS NULL` row) and queue delay (the existing `queued_at` stamp). Both are signals for the chain (section 8).
- Existing rows at deploy: the backfill of 9.3 option C (AGREED): mark only the jobs really in `queue:detail:*` or in flight; the rest stay NULL and drain at the bucket rate. (The earlier draft `pushed_at = created_at` for all rows is option B, rejected.)
- Closes: HO, RI2, F-HOLDORPHAN. L1SKIP / F-L1SPLIT / L1NOMAX for detail are replaced by the bucket (Lever 1 for detail stops being a push gate); scan/fullscan Lever 1 stays.
- AGREED 2026-11-02, wake period: `DRAIN_MIN_WAKE_S` (config.py env var, default 1). Tick >= 1 s: wake every tick, release 1 token. Tick < 1 s: wake every `k x tick`, `k = ceil(DRAIN_MIN_WAKE_S / tick)`, release k whole tokens (tick 0.7 s -> 1.4 s, 2 tokens; tick 0.175 s -> 1.05 s, 6 tokens).
- AGREED 2026-11-02, detail Lever 1 is REMOVED (scan and fullscan keep theirs). The bucket and drainer are the permanent admission gate. Its "ran out of options" role becomes an ALERT, raised when held jobs are still growing AND queue delay is still rising AND the pool is at its ceiling AND nothing is left to borrow. Knock-on, to design next: the detail lender gate at `manager.py:1528` (detail lends only while its Lever 1 is active) goes away and is replaced by "detail lends only when its delay and held jobs are under control".
- DECIDED 2026-11-02: the backfill is 9.3 option C; the first-scan path (`scan_worker.py:733`) also goes through the drainer (9.0).

### 9.1 Detail lender gate (AGREED 2026-11-02, not built)

**1. Bug.** `_borrow_seats` (`manager.py:1528`) lets detail lend only while its own Lever 1 is active. With detail Lever 1 removed that test is always false, so detail would never lend. Even today the test is odd: Lever 1 fires only after the chain found nothing (`:1745`), so detail lends only after it was itself overloaded.

**2. Today.** After the gate, the amount is: `free = ceiling - max(target, floor)` (`:1530-1532`); toward fullscan at most `running - target - 1` (`:1533-1534`); `lendable` = workers running above target, `buffer` = seats above peak_Nd (`:1537-1539`). Borrowers try sources fullscan, scan, detail (`_BORROW_SOURCE_ORDER` `:1513`). The target counts queue depth, and (after section 9) held rows are NOT in the queue, so a detail pool with a large held backlog can show a low target and look idle.

**3. Proposed.**
- Replace the Lever 1 test by a calm test: detail lends only when (a) its queue delay has been below `delay_warn_s x RECOVERY_STABILITY_RATIO` for `LEVER1_STABLE_REQUIRED` cycles in a row (the same test a borrow uses to return, `:1657-1663`) AND (b) its hold delay (age of the oldest held job) is below the same threshold. No new number.
- The amount lent is unchanged (`free`, `lendable`, `buffer` formulas stay).
- (b) needs the held figure in Redis: the drainer already selects held rows each wake, so it writes `held_count` and `held_oldest_age_s` to Redis; the manager reads them without touching Postgres.
- If detail then becomes short, the agreed reclaim rule (section 8) takes the seats back one per cycle, and a short lender has no free seats, so there is no ping-pong.
- Same change removes the detail branch of `_fire_lever1` in `_check_layer2` (`:1745-1750`): for detail, an exhausted chain raises the alert from section 9 instead.

### 9.2 Longest acceptable wait W_max (DECIDED 2026-11-02 by user: a typed number, 120 minutes; not built)

DECISION (supersedes the share-based proposal below): `W_max` is a plain config.py env var, `DETAIL_MAX_WAIT_MIN`, default 120. Reason (user): anything derived from history gives no guarantee for today, and a share is not intuitive. The share/minute conversion below is kept only as the rough scale of what 120 min costs (about 8% of jobs if arrivals were even). Uses (a)-(c) below are unchanged.

**1. Bug.** The nightly check compares the simulated drain time with a "longest acceptable wait", but that number does not exist yet, so the check cannot alert and the p-ladder floor (p75) has no stated meaning.

**2. Today.** Wait has no cost in the code. The digest query takes every `status='new'` row (`db/job_monitor.py:135`) and runs from the `0 7 * * *` cron entry (`deploy/update_crontab.sh:5,86`; cron timezone not checked). A job whose detail finishes after the digest run rides the next day's digest; discovery lag is already 3-12 h (`config.py:630-641`, per section 5 item 2).

**3. Proposed.**
- The policy is a SHARE, not minutes: `DETAIL_MAX_SLIP_SHARE` (config.py env var), the largest share of jobs allowed to miss a digest because of detail wait.
- W_max is derived from the history, not typed: W_max = the largest W such that (arrivals in the W minutes before the digest time, averaged over the 28 days) / (daily arrivals) <= the share. It uses the per-minute arrivals table already planned (section 6), so it needs clean data (D4 re-queue fix first).
- Rough scale if arrivals were even over 24 h: 2% about 29 min, 5% about 72 min, 8% about 115 min.
- Uses: (a) nightly, simulated drain time of the biggest backlog on the chosen rung > W_max gives an alert (not a change of C); (b) the design delay D must be <= W_max, else the nightly run alerts that the sizing itself breaks the policy; (c) the live shortfall chain keeps using the derived D, not W_max.
- Recommended default 0.05 (judgement, not code-verified): a 1 h detail wait is small next to the 3-12 h discovery lag. The user decides the number.

### 9.0 Decisions of 2026-11-02 (agreed, not built)

- ALL pending detail fetches go through the drainer, irrespective of where they came from: scan adaptive (`scan_worker.py:569`), first scan (`scan_worker.py:733`), fullscan (`fullscan.py:1289`), rebuild, and any future producer. No code path pushes a detail job to Redis directly; the drainer is the only writer of `queue:detail:*` payloads.
- The literal detail depth gates are removed (`DETAIL_QUEUE_MAX_ADAPTIVE` 5000, `DETAIL_QUEUE_MAX_FULLSCAN` 2000, `scheduler.py:1450`, `fullscan.py:1179`): extra jobs wait as held rows in the database. Closes BI, BP23, BPL, F-BPCHUNK.
- T = 30 min. Alerts: email at every 5-point step down, critical below p75 (see 3.5); email to be designed.
- No seeding of the arrivals history. The bootstrap defaults run until 24 h of data exist, then the database is used (with fewer than 28 days, the proposal is to use the days available; to confirm).
- Service time `s` (AGREED 2026-11-02, replaces the "last-good s" idea): the rolling window holds the last `SERVICE_TIME_WINDOW_CYCLES` (5) VALID cycles, each stored as (busy seconds, jobs finished). A cycle with 0 jobs finished is not valid: it is skipped, takes no slot, and the window is unchanged. Its busy seconds are carried forward and added to the next valid cycle (otherwise a long job spanning cycles makes `s` read low). `s` = sum(busy) / sum(finished) over the window. The window and the carried busy seconds are saved in Redis so a manager restart does not empty them. Before 5 valid cycles exist, use what exists; with none, fall back to the cold `fetch_p75`. Change point: `_get_pool_service_time` (manager.py:377-426), which today appends every cycle and returns None when the window has no finished job.
- RMIDLE tie-break among busy workers agreed: remove the one whose job started most recently.

### 9.3 `pushed_at` backfill at deploy (AGREED 2026-11-02: option C; not built)

**1. Bug.** The new column starts NULL on every existing row, and NULL means "held". At deploy there are `pending_detail` rows in three states, and the column cannot tell them apart: (a) already in a Redis detail queue, (b) being processed, (c) held and orphaned by the old hold branches (never queued).

**2. Today (code-verified).** `rebuild_redis` runs at scheduler startup (`rebuild.py:479-509`) and `rebuild_detail_queue` pushes EVERY `pending_detail` row (`:366-371`, `:421`), with no check of what is already in Redis. So today a restart re-queues the orphans (as one burst) and duplicates whatever is already queued (RBDUP, existing bug). New columns are added through the `(col, defn)` list in `init_db` (`schema.py:1183-1193`, `ADD COLUMN IF NOT EXISTS`).

**3. Options.**
- A. Leave NULL for all. The drainer releases everything at the bucket rate; the restart rebuild pushes nothing (NULL rows). Cost: rows already in Redis are queued a second time (at most the queue length at deploy); duplicates are harmless but each costs one wasted detail fetch.
- B. `pushed_at = created_at` for all existing `pending_detail` rows (my first draft). Rebuild at the deploy restart pushes them all at once, which is today's burst; the orphans get no rate limit.
- C. (recommended) One-time step at deploy, after the column exists and before the drainer starts: read the jobs currently in `queue:detail:*` and the in-flight set, `UPDATE pushed_at = now()` for exactly those `job_url`s, leave the rest NULL. No duplicates, no burst; orphans drain at the bucket rate. Costs one small script and one pass over the queues (thousands of entries).
- Column add is nullable with no default, so it is instant in Postgres; the partial index is on a small set.
- In all options `rebuild_detail_queue` is changed to push only rows with `pushed_at` set (it does not fix RBDUP).

## 10. Blast radius and implementation steps (reviewed 2026-11-02, nothing built)

### 10.1 Blast radius (what each agreed change touches; code-verified by reading)

| Change | Files and places | What else breaks or must follow |
|---|---|---|
| New column `pushed_at` + partial index | `db/schema.py` `(col, defn)` list (1183-1193) | `rebuild_detail_queue` (`rebuild.py:353-432`) must filter on it; watchdog stuck check (`watchdog.py:1624-1652`) and `scripts/health_check.py` (332, 722, 759) count `pending_detail` older than 1 h and would raise false alarms on held rows; `scripts/purge_stale_detail_payloads.py` and `outreach/report_templates/monitor_report.py` read `pending_detail`, check they tolerate NULL |
| Producers stop pushing/holding | `scan_worker.py` 515-527 (lever/reintro reads), 569-587 (hold + `lpush` :580, `count_arrival` :582, `sadd adaptive_seen` :587), 733-740 (first scan); `fullscan.py` 1169-1171, 1177-1204 (backpressure, `_backpressure_wait_s` reused at :1431 for the EMA), 1289-1312, `bloom.new_add` :1316, import `DETAIL_QUEUE_MAX_FULLSCAN` :126 | `adaptive_seen` is added only after a push today; it must be marked at save time (the job is safe in the DB). `_backpressure_wait_s` EMA code must be removed with the block. `count_arrival` moves to the `save_pending_detail == True` point; counter keys stay `signal:arrivals:{queue}:organic`, so `_organic_inflow_rate` (`manager.py:1345-1374`) is unchanged |
| Depth gates removed | `scheduler.py` 1449-1456, 1666-1674, imports :63 and :70; `config.py` `DETAIL_QUEUE_MAX_ADAPTIVE/FULLSCAN` | tests that set those constants |
| Detail Lever 1 + reintro removed | `manager.py`: `_fire_lever1` 1377 and its call 1745-1750, lift/reintro block 1753-1800, `_get_lever1_active` 1340, `LEVER1_MAX_HOLD_S["detail"]` :145, `REINTRO_*` :132-133, `_reintro_stable_cycles` :165, `_set_reintro_active` :1106, startup re-evaluation 2144-2178 (iterates all pools: must skip detail) | scan and fullscan keep their lever and reintro (scheduler reads at 1429, 1572, 1647, 1754 stay); `deploy.yml:205-206` still DELs detail lever keys (harmless); tests `test_layer2.py` (561, 716, 730), `test_deadlocks.py` (439-440) |
| Detail lender gate | `manager.py:1528` | `test_layer2.py` 384-395, `test_deadlocks.py:267` assert "detail lends only when lever1 active" |
| Derived D replaces 300 s | `manager.py` `_FALLBACK_PARAMS`, `_check_layer2` delay leg, `_check_health_alerts` 512-575, published `delay_warn_s` | `watchdog.py` reads the manager-published detail `delay_warn_s` via `_backpressure_threshold` (:909, :1401, :1535), so watchdog thresholds change with D; `tests/test_manager.py` (32 refs), `tests/test_watchdog.py` |
| `s` rolling window | `manager.py` `_get_pool_service_time` 377-426, callers :1186, :2245 | Redis keys for window and carry; scan/fullscan share the function, so their `s` also changes (intended: same rule) |
| Nightly ceiling, ladder, C_avail, seat order | `manager.py` `_pool_ideal` 867-897, `_plan_ceilings` 900-957, `_POOL_PRIORITY` :765; 13+2 references in manager/scheduler, 2 in `tests/test_manager.py` | detail no longer uses the soft ceiling; bootstrap path (917-927) stays until 24 h of rows |
| Drainer process | new `workers/detail_drainer.py`; shared payload builder factored out of `rebuild_detail_queue` (payload reconstruction for old rows without `detail_payload`, `_REBUILD_SKIP_PLATFORMS`) | rebuild pushes everything to the ADAPTIVE queue; the drainer picks the stream from `found_by`, so rebuild's behaviour changes for held-then-pushed rows only |
| Systemd and deploy | `deploy/systemd/recruiter-detail-drainer.service` (auto-installed by `install-systemd.sh`); `deploy.yml` unit lists at lines 111, 124, 209, 218, 258 (restart section, `_rollback_to_sha`, status loop) | `startup_failure_alert.py` registration only for oneshot units: not needed |
| Watchdog heal for drainer | `watchdog.py` `_get_heal_action` (:190), unit constants (:134 area) | a dead drainer silently stops ALL detail admission (single point of failure) |
| Held figures | drainer writes `held_count`, `held_oldest_age_s` to Redis; manager reads them (lender gate, shortfall alert) | queue metrics at `manager.py:635-650` use only `llen` and the tail stamp, so held rows are invisible to them (see H1) |
| RMIDLE, SEATCAP-DROP | `scheduler._remove_one_worker` 2315-2364; seat-total code in manager | heartbeat `busy_since` / job start time must be readable for the tie-break (`heartbeat.py` BusyMeter) |
| Per-job INFO line | `detail_worker.py` outcome sites (776, 796, 850, 866, 902) | log volume: one line per job; format decided when written |

Unaffected (checked): `detail_worker.py` pops with `lmove`/`blmove` (:399, :410) and its only `lpush` is to the DLQ (:453), so it needs no change for admission; `complete_pending_detail` (:899) and `delete_pending_detail` (:959) match `status='pending_detail'`, so duplicate queue entries stay harmless; `db_capacity.probe` counts the drainer as non-worker automatically. `scripts/test_e2e.py` (puts items back with `lpush` :123, :127) is a test helper, review only.

### 10.2 Implementation steps (ordered; each step is one deploy-safe unit, existing tests run after each)

Rule: steps 1-6 are additive (nothing changes behaviour until step 9). Step 9 is the cut-over.

1. **Config** (`config.py`) — BUILT 2026-11-02, uncommitted: `DETAIL_BUCKET_WINDOW_MIN` (T, 30), `DETAIL_ARRIVALS_HISTORY_DAYS` (28), `DETAIL_ARRIVALS_MIN_HOURS` (24), `DETAIL_COVERAGE_START_PCT` (99), `DETAIL_COVERAGE_FLOOR_PCT` (75), `DETAIL_MAX_WAIT_MIN` (120), `DRAIN_MIN_WAKE_S` (1). Utilisation u reuses the existing `MANAGER_SCALE_UP_UTIL` (0.80, "one value for gate and sizing"), so no new `DETAIL_UTILISATION`. `SERVICE_TIME_WINDOW_CYCLES` stays (5). Bootstrap defaults for the detail ceiling and `s` are added in step 6 where they are used. Remove `DETAIL_QUEUE_MAX_ADAPTIVE/FULLSCAN` only in step 9. Nothing reads the new vars yet.
2. **Schema** (`db/schema.py`) — BUILT 2026-11-02, uncommitted, applied on the next `init_db`: `pushed_at TIMESTAMP` in the `(col, defn)` list; partial index `idx_job_postings_held ON (created_at, id) WHERE status='pending_detail' AND pushed_at IS NULL`; table `detail_arrivals (minute TIMESTAMP PRIMARY KEY, arrivals INTEGER NOT NULL)`. Nothing reads or writes them yet. Existing rows get NULL (= held) until the step 9 backfill, so do NOT deploy this step alone together with the drainer.
3. **Shared payload builder** — BUILT 2026-11-02, uncommitted (`workers/detail_payload.py`, `rebuild_detail_queue` switched to it, `tests/test_detail_payload.py`; also fixed an old bug: rebuild's SELECT lacked `first_seen`, which its sort used): factor the payload logic out of `rebuild_detail_queue` into one function used by rebuild and the drainer, including the queue choice from `found_by` (H5) and keeping the real `found_by` in the payload; unit-test it against old rows without `detail_payload`, the skip-platform set and each `found_by` value.
4. **`s` rolling window** — BUILT 2026-11-02, uncommitted (window + carry in Redis key `manager:svc_window:<pool>`, TTL 1 h; the old in-memory `_svc_window` is gone; tests in `tests/test_layer2.py`) (`manager.py` `_get_pool_service_time`): 5 valid cycles, zero-finished cycles skipped with busy-second carry, state in Redis, cold `fetch_p75` fallback.
5. **Arrivals table writer** — BUILT 2026-11-02, uncommitted (`manager.py` `_record_detail_arrivals`, called once per cycle after `_check_health_alerts`; delta of the two `:organic` counters upserted into the current DB minute, a quiet cycle writes a 0 row; last sample in Redis `manager:arrivals:last`; first sample and negative delta write nothing; DB failure is logged, never stops the cycle; the counters still count at push time until step 9 moves the count to discovery; tests in `tests/test_layer2.py`) (manager): one row per minute from the counter delta (negative delta skipped, last sample persisted); the counters themselves stay by queue.
6. **Nightly ceiling** (manager) — BUILT 2026-11-02, uncommitted: p-ladder p99 to p75 with first fit under `C_avail`, seat order scan, fullscan, detail; D = A_p x s / C; checks D <= W_max and simulated drain time <= W_max raise log alerts; bootstrap until 24 h of rows. Pure sizing in `workers/detail_ceiling.py`; `manager._plan_detail_nightly` (called at the end of `_midnight_recompute`) reads `detail_arrivals`, takes `s` from the step-4 window (cold: `fetch_p75`), sizes scan/fullscan via `_plan_ceilings`, and stores the plan in Redis `manager:detail_plan` (30 h TTL). It **computes and stores only; nothing applies the plan yet** (applied in step 8). Tests: `tests/test_detail_ceiling.py`. Caveats: manager-downtime minutes count as zero arrivals; the drain simulation assumes the full service rate 60 x C / s; the plan's `total` may exceed the current seat total until step 8 applies it.
7. **Drainer process** — BUILT 2026-11-02, uncommitted: `workers/detail_drainer.py` (token clock, wake k, UPDATE+commit before push, held figures, heartbeat), `deploy/systemd/recruiter-detail-drainer.service`, unit added to the 5 lists in `deploy.yml` and the restart/rollback/status lists in `deploy/deploy.sh`, watchdog unit constant + heal entries (systemd and subprocess) + systemd-state check + `check_detail_drainer_heartbeat` (H2). Contract with step 8: the manager must write `manager:detail_bucket` = JSON `{"n","s","d"}` each cycle; until then the drainer releases nothing and only publishes `drainer:detail:held`. New config `DRAIN_HEARTBEAT_TTL_S` (30). Tests `tests/test_detail_drainer.py`. Original spec: unit file + `deploy.yml` (5 places) + watchdog heal entry. Written to run idle: it releases only rows with `pushed_at IS NULL`, and it does nothing harmful while producers still push directly (no held rows exist yet).
7a. **Watchdog queue head for detail (F-NOHEAD) + derived-D threshold** — BUILT: head tracking per detail queue and for the oldest held row; stalled = same head, overloaded = head changes (report only, no restart/rebuild). Must be in place before step 9. As built: `_detail_queue_head` (digest of the tail payload) and `_held_state` (drainer's `drainer:detail:held`, ignored when stale) feed `detail_adp_head`, `detail_fs_head`, `held_head` in the snapshot; section 8 treats a changed head as draining and falls back to the depth delta when a previous head is missing; a "detail held" WARNING fires when the oldest held id is unchanged between cycles. The watchdog threshold already reads `manager:backpressure:threshold:detail`, which step 8 writes as the derived D. H3 (stuck checks ignoring held rows) stays in step 9g.
8. **Manager bucket parameters** — BUILT 2026-11-02, uncommitted: each cycle write `n_bucket` (option A), `s`, `D` to Redis; read `held_count` / `held_oldest_age_s`; shortfall trigger on held growth OR delay above D at the ceiling; reclaim own lent seats; borrow return on borrower calm; detail lender gate (9.1). As built (`workers/manager.py`):
   - **Plan applied** in `_plan_detail_nightly`: `_store_plan(first)` for scan and fullscan, `manager:worker_ceil:detail` = plan ceil, `manager:db_pool_maxconn` raised when the plan's total is higher. Bootstrap (no plan) leaves the old ceilings.
   - **Derived D**: `_detail_design_delay` reads `d_s` from `manager:detail_plan`; `_cycle_params` returns `scaling_params` with detail's `delay_warn_s` = D, used by the pool cycle, Layer 2, the borrow return and the published watchdog threshold (H4). No plan: detail keeps its 300 s.
   - **Bucket**: `_publish_detail_bucket` writes `manager:detail_bucket` `{"n","s","d"}` (TTL 3 cycles) after the pool loop. `n` = `_detail_n_bucket` (base ceiling + min(borrowed, available - target), target = `_needed_workers["detail"]`); `s` = service window else `fetch_p75`; `d` = plan D else `DETAIL_BUCKET_WINDOW_MIN` x 60 x `MANAGER_SCALE_UP_UTIL`.
   - **Held**: `_read_held` (stale or missing = no signal), `_push_held_history` (Redis `manager:layer2:detail:held_history`, 5 samples), `_is_held_growing` (3 rises of 4 and above the oldest). `_check_layer2(held_growing=)` starts the chain for detail on held growth OR delay above D and rising, at the ceiling.
   - **Chain order**: emergency recalc, seat raise (`_raise_db_total`), `_reclaim_lent_seats`, `_attempt_borrow`, Lever 1 (the detail Lever 1 removal is step 9e). Reclaim: seats the borrower has not filled return at once; seats in use return one per cycle with the borrower shrinking by one worker; one borrower per cycle; never more than lent.
   - **Borrow return**: `_maybe_return_borrows` judges the borrower only; the handoff to a third struggling pool stays.
   - **Lender gate (9.1)**: `_borrow_seats` lets detail lend only when `_detail_calm["cycles"]` >= `LEVER1_STABLE_REQUIRED` (queue delay and oldest held age under D x `RECOVERY_STABILITY_RATIO`); the counter is in memory, so a restart waits 3 cycles.
   - Tests: `tests/test_detail_step8.py` (new); `tests/test_layer2.py`, `tests/test_deadlocks.py` (gate tests now use the calm counter; borrow scenarios patch the seat raise off), `tests/test_detail_ceiling.py` (plan application).
9. **Cut-over deploy** (the one risky step): in this order. (a) deploy code with the column; (b) run the backfill of 9.3 option C BEFORE the drainer starts; (c) start the drainer; (d) producers switch to save-only (scan_worker three sites, fullscan one site, `adaptive_seen` at save time, `count_arrival` at the save point); (e) remove depth gates, backpressure block and its EMA, detail Lever 1 and reintro, startup re-evaluation skip; (f) `rebuild_detail_queue` pushes only rows with `pushed_at`; (g) watchdog and `health_check.py` stuck checks become `pushed_at`-aware (use hold age for held rows).
   **Step 9 BUILT (uncommitted, not deployed).** As built:
   - **Producers save-only**: `scan_worker.py` (adaptive and first-scan sites) and `fullscan.py` call `save_pending_detail`, then `count_arrival`, and nothing pushes to Redis. `adaptive_seen` is marked at save time. Lever 1 / reintro reads and the fullscan backpressure block (with `_backpressure_wait_s` in the EMA) are gone.
   - **Gates removed**: scheduler depth gates (adaptive, fullscan) and `DETAIL_QUEUE_MAX_ADAPTIVE` / `DETAIL_QUEUE_MAX_FULLSCAN` from `config.py`.
   - **Manager**: no detail Lever 1, reintro or `LEVER1_MAX_HOLD_S["detail"]`; `REINTRO_DETAIL_BATCH_MAX` removed. An exhausted chain for detail logs `ALERT manager [detail]: capacity chain exhausted ...` (log only). A leftover detail lever flag is ignored, and startup deletes `manager:lever1:detail:*` and `manager:reintro:detail:active` and skips detail in the re-evaluation. Scan and fullscan are unchanged.
   - **Backfill**: `scripts/backfill_detail_pushed_at.py [--dry-run]` stamps `pushed_at = NOW()` for `pending_detail` rows whose `job_url` is in the detail queues, `:inflight:*` lists, `:retry_delay` ZSETs or the DLQ; other rows stay held. Run after the column exists and BEFORE the drainer starts.
   - **Rebuild**: `rebuild_detail_queue` selects only `pushed_at IS NOT NULL`.
   - **Stuck checks**: `watchdog.py` coverage block and `scripts/health_check.py` count only released rows older than 1 hour by `pushed_at`; held-backlog age is already reported by the drainer (`drainer:detail:held`) and the manager.
   - Tests: `tests/test_detail_backfill.py` (new); `tests/test_layer2.py` (detail alert, no Lever 1, leftover flag ignored); `tests/test_deadlocks.py` (recovery path runs on scan).
10. **Mechanical fixes with it**: RMIDLE (stalled, idle, busy, latest start first), SEATCAP-DROP, per-job INFO line.
   **Step 10 BUILT (uncommitted).** As built: `scheduler._remove_one_worker` picks via `_pick_removal_index` (stalled, then idle by heartbeat `busy_since` null, then the busy worker with the latest `busy_since`; a pid with no heartbeat reading counts as busy with the oldest start; `_busy_since_by_pid` reads `worker:alive:{type}_worker:*`). `manager._drop_db_total` runs each cycle: when the live seat cap is below the raised total it lowers `manager:db_pool_maxconn` to the cap (deleted when the cap is at or under `WORKER_SEAT_BUDGET`); a failed probe changes nothing; pool ceilings are re-fitted by the next plan. The per-job INFO line already existed in the `detail_worker` main loop (icon, tier, company, job_id, ms, fetch_ms, db_ms); it now carries `outcome=` and a `[dup]` icon for duplicates. Tests: `tests/test_detail_step10.py` (new).
11. **Tests**: update the listed files (10.4), run the related existing tests only (standing rule); new tests only for new modules (drainer, `s` window, ladder).
    - **BUILT 2026-11-02 (as built)**: every file in 10.4 already covered by steps 3-10 except one fix found here: `tests/test_scheduler_streams.py` mock row `_make_poll_stats_row` lacked `avg_scan_duration_s` (read at `scheduler.py:837` since the sizing commit), 6 `TestWarmingLifecycleOnAdaptiveComplete` tests failed with KeyError; key added (None). Related set run (layer2, deadlocks, manager, watchdog, detail_worker_inflight, queue_stamp, scheduler*, scan_worker*, fullscan*, all detail_* new tests): 600 passed.
12. **Docs and memory**: `docs/scaling-redesign.md`, the codebase map, `project_bug_triage_2026_10_28.md`; run `graphify update .`.

### 10.3 Rollback

- Before step 9 everything is additive: revert the commit; the extra column and table are harmless.
- After step 9: stop the drainer, run `UPDATE job_postings SET pushed_at = now() WHERE status='pending_detail' AND pushed_at IS NULL`, then a scheduler restart rebuilds the queue from DB (old behaviour: one burst). The old producer code returns with the revert. `deploy.yml` already has `_rollback_to_sha`, which must list the drainer unit (step 7).

### 10.4 Tests to update

`tests/test_layer2.py` (384-395, 561, 716, 730), `tests/test_deadlocks.py` (267, 439-440), `tests/test_manager.py` (ceiling / `delay_warn_s` / `s` window), `tests/test_watchdog.py` (stuck check, thresholds), `tests/test_detail_worker_inflight.py`, `tests/test_queue_stamp.py` (count point), scan_worker and fullscan tests that cover the hold branches. New: drainer (token clock, wake k, backfill-first order, crash after UPDATE before push), `s` window, ladder.

### 10.5 Deploy order summary

column and table, then backfill, then drainer running, then producers save-only. If producers switch before the drainer runs, jobs are held with nobody releasing them. If the drainer starts before the backfill, jobs already queued are queued again (harmless but wasteful).

### 10.6 Gaps found in this review (to settle before or during the build)

- H1. RESOLVED 2026-11-02 (user): the backlog term B for detail stays the Redis queue only; `held_count` is NOT added. Reason: the bucket releases at the ceiling rate whatever the live worker count, so the queue refills and the target rises and spawns workers (only a cycle or two of lag, inside D); widening the bucket while held jobs wait and the queue is small is desired; held count is already used by the lender gate, the shortfall trigger and the alert. Keeping the two signals (held backlog, queue delay) separate was the intent.
- H2. Drainer is a single point of failure: needs a watchdog heal action and a stale-heartbeat alert (step 7).
- H3. Watchdog and `health_check.py` stuck-`pending_detail` checks must ignore held rows (step 9g).
- H4. RESOLVED 2026-11-02 (user): the watchdog's detail threshold follows the derived D (about 20-30 min, not 300 s). ADDED by user: queue-head tracking for detail (F-NOHEAD, "documented, not built") must be implemented BEFORE the watchdog's stalled-queue decision, so a stalled queue (same head job across cycles, nobody consuming) is told apart from an overloaded one (workers at full speed, inflow higher; head keeps changing). Today `watchdog.py:1542-1580` judges detail only by depth delta and oldest-job delay, so a burst looks like a stall and ends in `queue_detail_*` alert, scheduler restart and rebuild (same chain as BASELINE-NODRAIN). Design (AGREED 2026-11-02; N = the existing poll-queue stall rule, no new number): record `job_url` + `queued_at` of the oldest entry per detail queue each cycle (as the poll queues do at `watchdog.py:960-994`); same head for the existing stall-cycle rule = stalled, may heal; head changing = overloaded, report only, the manager chain owns it. Same idea on the oldest held row to detect a stuck drainer (H2). Added as step 7a in 10.2.
- H5. RESOLVED 2026-11-02 (user: rebuild losing the source is wrong): `rebuild_detail_queue` (`rebuild.py:366-371`, `:421`) does not select `found_by` and pushes every row to `queue:detail:adaptive`, so fullscan rows land in the adaptive queue after a restart (skews per-queue depth, delay, counters and watchdog checks). Fix: the shared payload builder (step 3) also owns the queue choice, used by rebuild and the drainer: `tier2_fullscan` -> fullscan queue; `tier1_adaptive` and `first_scan_fresh` -> adaptive queue (matches the live push sites `fullscan.py:1308`, `scan_worker.py:580`, `:737`). Rebuild keeps the row's real `found_by` in the payload instead of `"rebuild"`; the `"rebuild"` label stays only for the arrival counter so recovery pushes are not counted as arrivals.
- H6. FIXED 2026-11-03: when `complete_pending_detail` updates no row, outcome is now `noop` (icon `[skip]`, no `NEW job` line); it only fed the per-job log line (no metric reads it). Original: the duplicate "new" outcome at `detail_worker.py:889-905` (reports "new" for an already-done job); whether it feeds a metric is still unchecked.
- H7. DECIDED 2026-11-02 (user): D4 is fixed AFTER TH is built, and nothing goes live until the majority of bugs are fixed, so the polluted-arrivals concern does not apply to a live deploy (TH and D4 reach the VM together). Original concern: D4 (Accenture re-queue) is still unfixed; until it is, the nightly ceiling is derived from polluted arrivals. With no seeding the table fills from deploy day, so the pollution enters unless D4 lands first.
- H8. Alert email deferred; alerts are log-only (3.5).
- H9. Scan and fullscan Lever 1 keep their own reintro and hold; no change here.

### 10.7 Not part of this build

Commit/push of the uncommitted earlier work (config.py, deploy.yml, manager.py, tests, scaling-redesign.md) and this doc: the user decides. Leave `q57_compact.txt` and `deploy/cf-probe-worker/.wrangler/` alone.
