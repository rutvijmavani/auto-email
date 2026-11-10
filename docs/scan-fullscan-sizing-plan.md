# Scan and fullscan pool sizing: plan and blast radius

Status: design AGREED with the user 2026-10-29.  BUILT 2026-10-29 (steps 1-6, 8): config, `workers/company_load.py`,
writers in `scheduler.on_adaptive_complete` and `fullscan.py`, manager metrics + formula + nightly rebuild, tests.
Step 7 DONE: the Redis-memory and detail-queue-depth alerts moved into the manager (`_check_health_alerts`,
tests/test_health_alerts.py); dead `_slow_throughput_check_loop` and `_remaining_work_minimum` deleted.
OPEN: step 9 docs (scaling-redesign.md), step 10 VM verification; NOT deployed or committed.  Authority doc for the rest of the
scaling machinery stays `docs/scaling-redesign.md`; this file records the change to how the
scan and fullscan pools compute `needed`, and the steps to build it.

## 1. Problem

`_run_pool_cycle` (workers/manager.py:1036-1045) sizes every pool with
`needed = ceil((depth / time_left + arrival_rate) x s)` and `time_left = max(warn - delay, 1)`
(historical: this describes the formula before this change; the shrinking `time_left` is gone).
For scan and fullscan:

* `depth` is a COUNT of late companies and `s` is one number (fetch_p75, about 35 s) for every
  company.  Accenture (about 7000 s per scan) and a company that takes 8 s count the same.
* After a breach `time_left` is a hardcoded 1 s, so 30 late companies ask for 30 / 1 x 35 = 1050
  workers; the ceiling clamp hides it, but the 1050 is stored as `needed` and feeds the peak.
* Before the breach, 2 big + 10 small late companies with 1200 s left give 0.4 workers -> floor 2
  -> scale-up and urgent never fire (target not above n) -> small companies wait about 7000 s.
* `_remaining_work_minimum` (scheduler.py:1897), meant to be a dynamic floor, is only called by the
  dead `_slow_throughput_check_loop`.

## 2. Agreed design

Detail keeps its formula.  Scan and fullscan use work-seconds from per-company data.

    needed = ceil( backlog + steady )

    backlog = sum over late companies c of  min( d_c / H , 1 )
    steady  = ( sum over ALL companies c of  d_c / (d_c + T_c) ) / target_util

* d_c = that company's averaged duration, the EMA the DB already keeps
  (`avg_scan_duration_s`, `avg_fullscan_duration_s`).  Never the raw last duration.
* T_c = its interval (`current_interval_s` for scan; `full_scan_interval_s` for fullscan).
  The next run is scheduled at COMPLETION + interval for both pools
  (scheduler.py `_reschedule_adaptive`, fullscan.py:1442-1447), so a company's cycle is d + T and
  its steady load is d / (d + T).
* H = the pool's existing `delay_warn_s` (scan 1683 s, fullscan 8640 s on 2026-10-29), derived in
  `_compute_scaling_params` (manager.py:213-233).  A FIXED positive horizon replaces the shrinking
  `time_left`, so a near-zero time_left can never inflate the count.  `min(., 1)` because one job
  can use at most one worker.
* target_util = the utilization at which the scale-up gate fires (today the literal 0.80 at
  manager.py:1087); steady is sized so the steady load runs at that utilization.
* A late company with no table entry (new, NULL average) uses fetch_p75: unknown is never zero.
* The urgent gate, scale-up/scale-down gates, floor/ceiling clamp, `_needed_workers` pre-clamp
  record, Layer 1 and Layer 2 are UNCHANGED. (The `delay / warn` multiplier no longer exists in the
  code: lateness is the urgent branch's job, and a multiplier would count the same backlog twice,
  `workers/manager.py` `_run_pool_cycle`.)
  Recorded peak stays `max(n, min(needed, seat_cap))`; outage spikes stay in the peak (user decision).
* No cap on the late-company list: all overdue ZSET members and all undelivered stream entries are
  read.  Cost is proportional to the late count (unmeasured; log it).

Worked check (scan, H = 1683): 300 late companies at 40 s = 300 x 0.0238 = 7.13; steady 1.18;
needed = ceil(8.31) = 9.  30 late companies at 5 s = 0.09, not 30.  Two 7000 s + 30 small:
2 + 0.09 + 1.18 = 3.27 -> 4, one worker always free for the small ones.

## 3. The company table (event-driven, rebuilt nightly)

Per pool (`scan`, `fullscan`), in Redis (the manager and the writers are different processes):

* hash `company -> "d,T"`, plus a running total `sum d/(d+T)`.
* Updated after every scan by one atomic Lua script: read the company's old load, compute the new,
  add the difference to the total, store `d,T`.
* Rebuilt from `company_poll_stats` at the nightly 23:00 ET recompute: build under temporary keys,
  RENAME over the old ones, set a TTL of recompute period + 1 h (about 25 h).
* If the table or total is missing (TTL, Redis restart, first start), the manager REBUILDS it from
  the DB immediately with the same function.  p75 is only for a single company with no row/NULL.
  If the DB read itself fails the manager keeps its last good values, logs an error, never uses 0.
  On the FIRST cycle there is no last good value to keep: a pool that has never had a work-based
  need falls back to the depth-based formula (`depth / delay_warn_s + arrival_rate`) until the
  table can be read (`_last_work_need` in `_run_pool_cycle`).

## 4. Blast radius (graphify query + grep, 2026-10-29)

graphify (`python -m graphify query`) returned heuristic, test-heavy results; exact callers below
come from grep.

Changed:

| File | What |
|---|---|
| `workers/company_load.py` (NEW) | Lua writer, rebuild-from-DB, read steady total, read d for named companies |
| `workers/manager.py` | `_get_scan_pool_metrics` (445) returns late names + work; `_get_queue_metrics` (481-534); `_run_pool_cycle` (996-1045) new inputs/formula for scan+fullscan; main loop (2060-2110); `_midnight_recompute` (848) rebuilds tables; `_compute_scaling_params` (213-233) floors 300/600 -> config; gate literals 0.80/0.5/0.25 -> config; per-cycle INFO line |
| `workers/scheduler.py` | `on_adaptive_complete` (746 SELECT add `avg_scan_duration_s`; hook after the DB commit at 830, before reschedule at 851); delete `_remaining_work_minimum` (1897) and the dead `_slow_throughput_check_loop` (2955-), its only caller |
| `workers/fullscan.py` | hook after `_complete_fullscan_db` (1452-1458) with `updated_avg_duration` and `fs_state` interval |
| `config.py` | warn floors, gate utilizations, table TTL, target_util |
| `docs/scaling-redesign.md` | scan/fullscan formula, warn derivation, hung-worker ownership (842-845), detail restart claim |
| tests | `tests/test_manager.py` (31 refs), `tests/test_layer2.py` (7); `test_phase11_alerts.py` and test_manager only mention the dead loop in docstrings |
| memory | codebase map, design memory |

Read-only (no change): `db/job_monitor.py upsert_poll_stats` (already persists the EMA; the table
is fed from the scheduler hook AFTER it ran, since scan_worker.py:611 writes the average before the
completion event reaches `on_adaptive_complete`); `workers/rebuild.py` (ZSET rebuild, no d/T change);
`workers/stream_backlog.py` (reads only); detail formula; Layer 1/2 math; dispatch loops (D4).

## 5. Implementation steps

1. config.py: add env vars for the warn floors (300, 600), the gate utilizations, the table TTL
   (derived from the recompute period), and target_util (default = the scale-up gate value, ONE
   config value shared by gate and sizing).
2. workers/company_load.py: Lua update script; `rebuild(r, pool)` from `company_poll_stats`
   (every row of the table, not only active companies), atomic RENAME + TTL; `steady(r, pool)` rebuilding on a missing table;
   `loads(r, pool, names)` via HMGET with p75 for missing names.
3. Writers: scheduler `on_adaptive_complete` hook; fullscan completion hook.  Failure path of a
   scan changes neither d nor T, so no write.  WARMING override interval is the T written.
4. Manager metrics: `_get_scan_pool_metrics` also returns the names of every overdue ZSET member
   and every undelivered stream entry (no cap), and an unreadable read is reported as UNKNOWN, not
   as a zero (today `_get_queue_metrics` falls back to depth 0 / delay 0 at line 534).
5. `_run_pool_cycle`: for scan and fullscan compute `needed` from backlog + steady; detail path
   untouched; keep delay multiplier, `_needed_workers`, clamp.
6. Nightly: `_midnight_recompute` rebuilds both tables before the ceiling plan.
7. Delete `_remaining_work_minimum` and `_slow_throughput_check_loop` after confirming the Redis
   memory alert it contains (scheduler.py 2994-) lives elsewhere (scheduler.py:2898 says it was
   extracted).
8. Tests: update test_manager / test_layer2 for the new inputs; new tests for company_load and for
   the formula cases above (long job cap, 30 tiny late companies, head-of-line, missing table
   rebuild, unknown company uses p75).
9. Docs and memory: scaling-redesign.md, this file, codebase map, design memory.
10. Verify on the VM: INFO line shows backlog and steady; table size equals company count; nightly
    rebuild equals the event-built total within rounding.

## 6. Risks

* The table is a second copy of DB values; drift is bounded by the nightly rebuild and
  rebuild-on-missing.
* The late-name read costs O(late); measure with a log field before assuming it is small.
* The formula clears a backlog in about H, it does not promise a deadline per company;
  the delay multiplier and urgent gate remain the escalation.
* Open (separate): 90 of 92 companies with under 10 s average duration may be empty or failed
  boards (silent-failure angle).
