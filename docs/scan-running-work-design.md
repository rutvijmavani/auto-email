# Scan / fullscan sizing: count the scans that are running (BUILT 2026-11-12, uncommitted, VM verify pending)

Status: built 2026-11-12 (steps 1-9 of "Implementation steps"; step 10 is VM verification).
Code: `company_load.entries/steady_excluding`, `manager._get_running_companies`,
`_get_scan_pool_metrics` (`running_names`), `_scan_pool_work` (`running`), target line and
`running=` in the manager log.  Tests: `tests/test_company_load.py` (TestRunningTerm,
TestGetRunningCompanies).  Extends
`docs/scan-fullscan-sizing-plan.md` (backlog + steady).

## 1. Bug

The scan and fullscan pools are sized `ceil(backlog + steady)`.

* backlog = sum over LATE companies of min(d/H, 1)   (late = due, not yet taken by a worker)
* steady  = (sum over all companies of d/(d+T)) / MANAGER_SCALE_UP_UTIL

A scan that is RUNNING is not late, so backlog never sees it.  Steady gives it only its
long-run average share (accenture d=3045 s, T=10800 s -> 0.22 of a worker) although while
it runs it holds a whole worker for about 51 minutes.  When every worker is inside a long
scan, small companies wait behind them and the target does not move.
Utilization sees the busy workers but is only a gate (manager.py:1475, :1497); it never
raises the target.

## 2. How it behaves today (evidence)

PROVEN, manager log 2026-11-10 10:00-10:50 (win_1000_1105.txt; memory
project_watchdog_restart_2026_11_10.md, round 14):

* ca:27 (since 09:59:45) and accenture (since 10:29:45) occupied both scan workers.
* Every cycle: `n=2 needed=2 target=2 ceil=20 ... backlog=0.02 steady=1.03 -> hold`
  while depth went 3 -> 7 and delay 857 -> 1997 s.
* Code: target = ceil(0.02 + 1.03) = 2 (manager.py:1419-1420); urgent needs n < target
  (:1441-1444); scale-up needs effective_target > n (:1475-1477).
* After the 10:54 restart MathWorks finished in 224 ms and the queue drained in ~20 s:
  one free worker was all that was missing.

Table values (operator's Redis output, `manager:company_load:scan`): Bank of America
d=8549/T=43196, accenture 3045/10800, ca:27 3496/21600, Starbucks 678/32294.  These four
are the only companies with d > 600 s and carry about 0.68 of the logged steady.

## 3. Solution

One universal formula, no gates:

```
target = ceil( backlog_late + running + steady_rest )

backlog_late = sum over late companies                    min(d/H, 1)      (unchanged)
running      = sum over running scans                      min(d/H, 1)      (new)
steady_rest  = (sum over companies NOT running of d/(d+T)) / MANAGER_SCALE_UP_UTIL
```

* A long running scan adds 1 (so target >= number of pinned workers); a short one adds
  d/H, about 0.
* The running company is removed from steady so its share is not counted twice.
* A company that is both running and late (redelivery) is counted once, as running.
* Where steady <= WORKER_FLOOR it changes nothing, as before.

### Worked examples (H = 300 s, the floor; small-company d = 0.2 s illustrative)

| Situation | Sum | Old | New |
|---|---|---|---|
| n=2, ca:27 + accenture running, small late (0.02) | 2 + 0.02 + 0.581 = 2.60 | 2 | 3 |
| same, nothing late | 2 + 0 + 0.581 = 2.58 | 2 | 3 |
| n=2, only accenture running, small late | 1 + 0.02 + 0.755 = 1.78 | 2 | 2 |
| n=3, BoA + accenture + ca:27 running, small late | 3 + 0.02 + 0.375 = 3.40 | 2 | 4 |
| 200 small due, 2 small running | 0.13 + 0.0013 + 1.03 = 1.16 | 2 | 2 |
| 200 small due while ca:27 + accenture run | 0.13 + 2 + 0.581 = 2.71 | 2 | 3 |

steady_rest = (0.824 - removed shares) / 0.8, with raw total 1.03 x 0.8 = 0.824.

### Where the running companies come from (no worker change)

Heartbeats carry only pid/ts/processed/busy_ms/busy_since (heartbeat.py:186-192) and
`worker:cur:{consumer}` only msg_id/alive_until (progress.py:73), so the company is read
from the stream, exactly as the scheduler already does (scheduler.py:366, :388):

1. `xpending_range(stream, STREAM_CONSUMER_GROUP, min="-", max="+", count=N)` -> message_id, consumer
   (`worker-{host}-{pid}`).  redis-py is pinned 5.2.1.
2. `xrange(stream, min=id, max=id, count=1)` -> `company`.
3. durations from `company_load.durations(r, pool, names, fetch_p75)` (fetch_p75 when no entry).

Only entries whose owner is actually running are counted (heartbeat busy_since set for
that pid); entries of stalled/dead owners are the reaper's.  At most one lookup per worker
per cycle.

### Code changes

* workers/manager.py `_get_scan_pool_metrics`: also return `running_names` (None when the
  PEL is unreadable; the pool then keeps its last need, never a zero, as for late_names).
* workers/manager.py `_scan_pool_work`: compute running term and steady_rest; log
  `running=` next to `backlog=`/`steady=`.
* workers/company_load.py: `steady_load_excluding(...)` = total minus sum of load_of for the names.
* Tests: the six table rows as unit tests of `_scan_pool_work`; PEL/owner handling with a fake Redis.
* Docs: scan-fullscan-sizing-plan.md, codebase map.  Applies to scan and fullscan pools.

### Interaction with the existing gates (verified)

* Urgent fires on target alone only when `target >= 0.75 x ceil and target > peak_nd`
  (manager.py:1441-1443).  Scan ceil was 20 on 11-10 (win_1000_1105.txt line 3: `ceil=20`),
  so target must reach 15: a target of 3-4 never fires on target alone.  (peak_nd value
  was not in the logs; irrelevant while target < 15.)
* So the new target becomes a scale-out through the existing delay gates: urgent at
  delay >= 0.75 x H (>= 225 s at the 300 s floor), normal scale-up at util > 0.80 and
  delay > 0.5 x H for 2 cycles.  The 10:50 case would have reacted about 4 minutes after
  the first small company went late instead of never.
* Scale-in: scale-down already needs util < 0.50 (manager.py:1497), which a pinned worker
  prevents; the new term also keeps one worker free beyond the pinned ones.  Accepted cost:
  one extra worker for the length of the long scans (Bank of America about 2.4 h).
* Scheduler removal order (RMIDLE) is unchanged: stalled, idle, then busy.

## Corrections found while assessing the blast radius

* `company_load.durations()` returns only d.  steady_rest needs d AND T of each running
  company, so a new reader `entries()` returning (d, T) is required.
* `steady_load` is the pool total kept in Redis (`manager:company_load:{pool}:total`,
  company_load.py:155-170).  Subtract the share of a running company ONLY if it has an entry
  in the table; a company missing from the table is not in the total, subtracting it would
  undercount.  Clamp the result at 0.
* Keep the `_run_pool_cycle` contract: add a new `work["running"]` key and read it with
  `work.get("running", 0.0)` at manager.py:1420.  Existing callers/tests that pass
  `{"backlog","steady","late"}` (tests/test_company_load.py:91) keep working.

## Blast radius (verified by grep/read, 2026-11-12)

Direct code
* `work` dict is built only in `_scan_pool_work` (manager.py:685) and consumed only at
  manager.py:1419-1421 (target) and 2668-2671 (log line).  No other reader.
* `late_names` is produced only in `_get_scan_pool_metrics` (:505-551) and consumed only in
  `_scan_pool_work` (:696).  New `running_names` has the same single producer/consumer.
* `_get_scan_pool_metrics` is called only from `_get_queue_metrics` (:766), which is also the
  startup call (:2547).  tests/test_manager.py exercises `_get_queue_metrics` with a MagicMock
  Redis (:558-683): the new PEL read must sit inside try/except so a MagicMock/unreadable
  stream gives `running_names=None` (work unknown, last need kept), never an exception.
* Log line: nothing in the repo parses `backlog=`/`steady=` (grep over py/md/sh found only the
  manager line itself), so adding `running=` is safe.
* company_load table writers (scheduler.py:879, fullscan.py:1372, manager.py:1218 rebuild)
  are untouched; only a read function is added.

Downstream of `workers_target` (it rises by up to the number of long running scans)
* `_needed_workers` -> `_update_daily_peak_running` (manager.py:2678-2681) -> `daily_peak:running`
  -> nightly ceiling input (peak_Nd + buffer, :998-1028).  Effect: the recorded scan peak can
  be higher by the long scans that overlapped a late queue, so the learned ceiling can drift
  up.  Scan ceil was 20 on 11-10, so +1..+3 is small; watch it after deploy.
* `all_workers_targets` -> `_borrow_seats` (:1894-1920): `free = adj_ceil - max(src_tgt, floor)`;
  a scan target higher by 1..3 lets scan lend 1..3 fewer seats to other pools.  At ceil 20 this
  is no effect unless seats are tight.  `_attempt_borrow` itself runs only when the pool is
  already at its ceiling (:2110-2117), which the new term does not change.
* `{pool}:target:N` command (urgent :1457, scale-up :1489) makes the scheduler spawn scan
  workers: each extra worker is a process with about 2 DB connections (memory
  project_db_connection_budget_2026_10_29) inside the seat budget.  Bounded by the ceiling.
* Scale-in: scale-down needs util < 0.50 AND `effective_target <= n-1` (:1497-1499); both now
  keep the extra worker while long scans run.  RMIDLE removal order unchanged.

Behavioural
* Timing is unchanged: the new target becomes a scale-out only through urgent (delay >=
  0.75 x H, or target >= 0.75 x ceil and > peak_nd; ceil 20 means target >= 15, not reachable)
  or scale-up (util > 0.80, delay > 0.5 x H, 2 cycles).  Pools with no long scans: running
  term is about 0, formula equals today's (case 5 of the table).
* More concurrent long scans can raise load on one Workday tenant (07:20-07:59 429s on 11-10).
  Capped by ceiling; not mitigated by this change.
* Detail pool, watchdog, drainer, log_monitor: not touched (detail does not use `work`).

Failure modes
* PEL/stream unreadable, NOGROUP, or xrange error -> `running_names=None` -> `_scan_pool_work`
  returns None -> `_last_work_need` kept (manager.py:1422-1423), same as an unreadable late list.
* Company missing from the table -> `fetch_p75` default d (as `durations` does); not removed
  from steady.
* A PEL entry of a stalled/dead owner -> skipped (owner has no live heartbeat with busy_since);
  the reaper handles it.

Rollback: revert the commit; no schema, no Redis key format and no worker change, so nothing
to migrate.  Optionally gate behind an env flag (default on) for a quick off switch.

## Implementation steps

1. `workers/company_load.py`: add `entries(r, pool, names, default_d)` -> list of (d, T, in_table)
   and `steady_excluding(total, entries)` (subtract `load_of(d,T)` for in-table entries, clamp >= 0).
2. `workers/manager.py` `_get_running_companies(r, stream_key, group)`: `xpending_range`
   (bounded count) -> keep entries whose consumer pid has a live heartbeat with `busy_since`
   (reuse the `worker:alive:{pool}_worker:*` scan used by `_get_pool_busy_ms`, :348-361) ->
   `xrange` each msg_id -> company names.  Raise on Redis error (caller catches).
3. `_get_scan_pool_metrics`: add `running_names` (None when unreadable); remove running names
   from `late_names` so a redelivered company counts once.
4. `_scan_pool_work`: running = `backlog_term(running d, H)`; steady = steady_excluding(...)
   / MANAGER_SCALE_UP_UTIL; return `running` too.
5. `_run_pool_cycle` (:1420): `ceil(work["backlog"] + work.get("running", 0.0) + work["steady"])`.
6. Log line (:2670): add `running=%.2f`.
7. Tests (tests/test_company_load.py + new cases): the six table rows through `_scan_pool_work`
   with patched reads; unreadable PEL -> None; running company not in table; running+late dedupe;
   `_get_running_companies` with a fake Redis (stalled owner skipped); `_get_queue_metrics`
   still passes with a MagicMock Redis.
8. Docs: scan-fullscan-sizing-plan.md formula, this doc status, codebase map and memory.
9. Run tests/test_company_load.py, tests/test_manager.py, tests/test_layer2.py, tests/test_stream_backlog*
   (existing manager suites) before anything else.
10. VM verify after deploy: manager log shows `running=`; with a long scan running and late
    companies, `target` exceeds `n` and urgent fires at delay >= 0.75 x H; scan daily_peak and
    DB connection count stay inside the budget.

## Open items

* Actual H (delay_warn_s) of the scan pool at the time: floor 300 s, runtime value not seen.
* Whether to relax the delay gate for the running term (separate decision; not in this design).
* How to test owner-state reuse from the manager (heartbeat busy_since by pid vs scheduler._owner_state).
* Extra concurrent scans raise load on the same Workday tenants (11-10 07:20-07:59 429s);
  the ceiling and the DB seat budget still cap it.
