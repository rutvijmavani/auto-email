# Adaptive health report: one data source (DESIGN, 2026-11-07, BUILT)

Status: BUILT 2026-11-07 (see Section 9); not yet deployed and not VM-verified.

## 0. Why this exists

The Monday "Adaptive Polling Health" block of the daily digest reads `adaptive_poll_metrics` for five
of its blocks (miss rate, detection age, wasted polls, score stability, early-exit). That table is
empty on the VM (query 2026-11-06 returned 0 rows) and no non-test code writes to it (repo-wide search
for INSERT/UPDATE, 2026-11-06). So all five blocks have never shown data. The user chose ONE data
source: `adaptive_poll_metrics`, filled by a nightly snapshot job.

Related finding: `query_score_oscillation` selects `poll_score`, a column that does not exist in the
table (`db/schema.py:1140-1166`; the name appears only in `db/adaptive_health.py:376-384`). That
query would fail even with rows.

## 1. Decision A: `wasted_polls` (poll that completed and found nothing new)

**1. Bug.** The report needs "polls that returned no new jobs" per day. Nothing records it, and it
cannot be derived from what is stored. `company_poll_stats` keeps only running totals: `total_polls`
(+1 per adaptive scan), `total_new_jobs` (sum of jobs, and the fullscan also adds to it,
`workers/fullscan.py:659,685`), and `consecutive_empty` (resets to 0 on a hit). A daily difference of
`total_new_jobs` gives jobs, not empty polls.

**2. Today (evidence).**
- `upsert_poll_stats` (`db/job_monitor.py:1046-1056`) does `total_polls + 1` and updates
  `consecutive_empty`; `scan_worker.py:594` calls it once per scan.
- `on_adaptive_complete` (`workers/scheduler.py:808-815`) updates `consecutive_empty` with the same
  CASE; `scan_worker.py:922` calls it once per scan too.
- So `consecutive_empty` advances by 2 per empty poll (PROVEN by reading the code; not measured on
  the VM). A failed scan also reaches `on_adaptive_complete` with `new_jobs=0` and advances it.
  This is a separate finding (not fixed here). The new counter below does not depend on it.

**3. Proposed.**
- Add `company_poll_stats.empty_polls BIGINT DEFAULT 0`, incremented in `upsert_poll_stats` in the
  same statement that increments `total_polls`, when `new_jobs = 0`. `upsert_poll_stats` is step 7 of
  a completed scan, so a failed scan does not count as wasted. (Verify the failure path never reaches
  it, implementation step 2.) Exception, PROVEN in code: an Avature SearchJobs pagination failure
  that still returns partial stubs (`scan_worker.py:407-416`, `_fetch_complete = False`) continues to
  `upsert_poll_stats` with `success=True`, so an incomplete fetch whose partial results hold no new
  jobs DOES increment `empty_polls`. Such a poll is counted as wasted although the board was not fully
  read; accepted (rare, and the partial scan is a real poll), not gated.
- `adaptive_poll_metrics.wasted_polls` for day D = `empty_polls` now minus `empty_polls` at the
  previous snapshot. `total_polls` the same way.
- The snapshot needs the previous cumulative value per company. Add `polls_cum BIGINT` and
  `empty_polls_cum BIGINT` to `adaptive_poll_metrics`; the next night's delta is now minus the last
  row's `*_cum`. If a company has no earlier row, store the cumulative values and write a delta of 0
  for that first night (no spike).

## 2. Decision B: the daily score and what "stability" means

**1. Bug.** Two problems. (a) There is no score history: `company_poll_stats.adaptive_score` holds
only the current value (overwritten by `on_adaptive_complete`, `scheduler.py:808`). (b) The query
takes `STDDEV_POP(poll_score)` across ALL rows of a platform for 14 days
(`db/adaptive_health.py:372-386`). That mixes differences between companies with day-to-day
movement, so a platform with one busy company and many quiet ones shows high "oscillation" even if
every company's score is constant.

**2. Today (evidence).** Column name `poll_score` does not exist in the table; `score_oscillation`
exists but is a derived value, not a daily score. `adaptive_score` is the real column in
`company_poll_stats` (`db/schema.py:1127`).

**3. Proposed.**
- Add `adaptive_score REAL` to `adaptive_poll_metrics`; the snapshot copies
  `company_poll_stats.adaptive_score` at snapshot time (one value per company per day).
- Rewrite the query: per company, `STDDEV_POP(adaptive_score)` over the 14 days; per platform, the
  average and maximum of those company values. Require at least 3 days per company. That measures
  movement over time, which is what the block's description says.
- `score_oscillation` stays in the table unused (dropping columns is out of scope).

## 3. Decision C: detection age and miss rate (from `job_postings`)

**1. Bug.** `found_within_1hr/4hr/24hr`, `found_after_24hr`, `tier1_new_jobs`, `tier2_new_jobs` are
never filled. The source data does exist: `job_postings.found_by` (`tier1_adaptive` /
`tier2_fullscan`, written at `scan_worker.py:550` and `fullscan.py:739`), `posted_at` (TIMESTAMP,
`db/schema.py:591`), `created_at`.

**2. Today (evidence).** `scripts/early_exit_miss_rate.py` already derives miss rate from these
columns on demand, with cold start (24h after a company's first row) excluded. Nothing runs it on a
schedule. Two limits: (a) `posted_at` is a real timestamp only on some platforms (the script says so
at its docstring); (b) `created_at` is set when the SCAN inserts the `pending_detail` row
(`save_pending_detail` does not list it, so the column default applies; `complete_pending_detail` at
`db/job_monitor.py:913` does not update it). So `created_at - posted_at` is posting-to-scan-insert
time and does NOT include detail-queue delay. If that delay is ever wanted, a detail-completion
timestamp must be recorded first. OPEN: which of the 23 feeders in `jobs/ats/`
fill `posted_at` with a real time (113 mentions across 23 files, not yet read per platform).

**3. Proposed.**
- Snapshot per company and day, from `job_postings` rows with `created_at` in that day, excluding
  `status = 'pre_existing'` and the 24h cold start:
  `tier1_new_jobs`, `tier2_new_jobs`, `total_new_jobs = tier1 + tier2`.
- Detection age buckets use `created_at - posted_at` and count only platforms on an allow-list of
  real-timestamp feeders (config list, built in implementation step 1 from the feeder code and a VM
  query). Other platforms contribute to counts but not to age buckets. Age is aggregated across the
  allow-listed platforms (the report shows no per-platform "n/a"), and the section is omitted when
  there are no dated jobs. The block is labelled "posting time to scan-insert time" (it does not include detail-queue delay).
- Evidence query for step 1 (run on the VM):
  `SELECT ats_platform, COUNT(*) n, COUNT(posted_at) dated, COUNT(*) FILTER (WHERE posted_at::time <> '00:00') timed FROM job_postings WHERE created_at > NOW() - INTERVAL '14 days' GROUP BY 1 ORDER BY n DESC;`

## 4. Decision D: early-exit block

**1. Bug.** `early_exit_missed` and `early_exit_triggered` are never filled. "Missed" cannot be
measured directly: a skipped job is, by definition, not seen. `early_exit` exists only as a log
field (`scan_worker.py:578`).

**2. Today (evidence).** `early_exit_miss_rate.py` gives "fullscan inserted it, posted more than 36h
earlier" (`likely_miss`) plus undated. Not all of those are early-exit misses (long poll interval,
failed scan, filter error), and misses recovered by a fullscan within 36h are not counted, so it is
an indicator, not a bound in either direction. It also cannot see jobs that appeared and disappeared
between two fullscans.

**3. Proposed.**
- `early_exit_triggered`: add `company_poll_stats.early_exit_polls BIGINT DEFAULT 0`, incremented in
  `upsert_poll_stats` when the scan's stop reason is early-exit (pass it from `scan_worker.py:594`,
  where `early_exited` is already known, `scan_worker.py:496`). Snapshot delta as in Decision A.
- `early_exit_missed`: fill with the script's `likely_miss` rule, per company per day. This is a
  scoped indicator, not a bound on all misses: it counts only fullscan inserts of jobs posted more
  than `ADAPTIVE_MISS_LAG_HOURS` earlier (config, default 36; the report label shows the configured
  value) on `LISTING_DATE_RELIABLE` platforms. Misses that a fullscan recovers within that lag,
  jobs without a posting date, and platforms outside that list are not counted, and not every counted
  job was caused by early exit (long poll interval, failed scan, filter error). The report label says
  so.

## 5. Snapshot job

- Entry point as built: `db/adaptive_metrics.py::run_nightly(today)` (originally planned as a single
  `snapshot_day(day)`; it is split into `snapshot_counters` and `snapshot_jobs`). Idempotent
  (`INSERT ... ON CONFLICT (date, company) DO UPDATE`), one row per company per day.
- Where it runs: the manager's nightly recompute at 23:00 local time (not after midnight). It
  records the CURRENT cumulative counters against today's date, refreshes the job metrics for today
  (partial: the run is at 23:00) and yesterday (re-read at its end, NOT guaranteed final: a
  `pending_detail` row can later change status or posting date, and deleting a company's last
  qualifying row leaves that company's earlier stored count unchanged because the snapshot only
  upserts rows it finds; changed or deleted rows are not reconciled), and backfills job metrics only for days
  inside `ADAPTIVE_SNAPSHOT_BACKFILL_DAYS` that have NO rows at all. A day that is only partially
  populated is not recovered; there is no after-midnight run.
- Backfill of history is not possible for polls and scores (never stored). Tier counts and
  detection age CAN be backfilled from `job_postings` for days with no rows.

## 6. Blast radius

| Change | Touches | Risk |
|---|---|---|
| New columns on `company_poll_stats` (`empty_polls`, `early_exit_polls`) | `db/schema.py` ALTER ... IF NOT EXISTS | Low. Applied by init_db on deploy; table is in the hot path of every scan, but ADD COLUMN with a default is metadata-only on Postgres 11+. |
| `upsert_poll_stats` signature (new `early_exited` arg) and SQL | `db/job_monitor.py:1010`, `db/db.py:227` (re-export), callers `scan_worker.py:594`, `scripts/test_e2e.py:329,697` | Medium. Executed on every adaptive scan. Default the new arg so existing callers keep working. A bug here stops poll stats updating for all companies. |
| New columns on `adaptive_poll_metrics` (`polls_cum`, `empty_polls_cum`, `adaptive_score`) | `db/schema.py` | Low (table is empty). |
| New `db/adaptive_metrics.py` + scheduling | new file, cron or manager | Low if it only reads `company_poll_stats` and `job_postings`. Run during low load; reads are large on `job_postings` (index on `(status, first_seen)` exists; `created_at` filter needs checking). |
| `db/adaptive_health.py` queries (score, wasted, detection) | read side only | Low. The digest already swallows query errors (`build_weekly_health_data`). |
| Report text/labels | `outreach/report_templates/monitor_report.py` | Low. |
| Tests | `tests/test_adaptive_health.py` (its fixtures create their own table with the old columns; will need `adaptive_score`) | Medium. This file passes today because the fixtures hide the bad column name. |

Not touched: `consecutive_empty` behaviour, scheduling of polls, queues, scaling, worker code beyond
passing one flag.

## 7. Implementation steps

1. Read each feeder's `posted_at` handling and run the evidence query; write the real-timestamp
   allow-list into `config.py`.
2. Verify the failure path of `scan_worker` never reaches `upsert_poll_stats`; if it does, gate the
   `empty_polls` increment on success.
3. Schema: add the columns (company_poll_stats: `empty_polls`, `early_exit_polls`;
   adaptive_poll_metrics: `polls_cum`, `empty_polls_cum`, `early_exit_cum`, `adaptive_score`).
4. `upsert_poll_stats`: increment the two new counters; thread `early_exited` from `scan_worker`.
5. Write `db/adaptive_metrics.py` (`run_nightly`, built from `snapshot_counters` + `snapshot_jobs`);
   it runs in the manager's 23:00 recompute.
6. Fix the `adaptive_health` queries (column name, per-company stddev, label changes).
7. Update `tests/test_adaptive_health.py` fixtures; add tests for the snapshot delta (first night = 0,
   counter reset/negative delta clamps to 0) and for the new upsert counters.
8. Update the Monday section header text and the codebase map; correct
   `docs/ats-fetch-strategy.md` (company_config claim) in the same pass.
9. VM: deploy, run `db.adaptive_metrics.run_nightly(<date in MANAGER_RECOMPUTE_TZ>)` once by hand: derive the date
   the way the manager's `_recompute_date()` does (`datetime.now(ZoneInfo(MANAGER_RECOMPUTE_TZ)).date()`),
   not the host-local `date.today()`, check rows, read the next Monday digest.

## 8. Retention (decided 2026-11-07: config-driven, default 90 days)

Today: none. No code deletes from `adaptive_poll_metrics` (the table was never written either).
Existing pattern: each table has `_cleanup_<table>(c)` in `db/schema.py` (e.g. `_cleanup_api_health`
:194, `_cleanup_scan_health` :205) using a `RETENTION_*` value from `config.py`, all called from the
cleanup pass in `init_db` (:2212-2235).
Growth: one row per company per day, so rows/year = monitored companies x 365.
Proposed: `RETENTION_ADAPTIVE_POLL_METRICS_DAYS = int(os.getenv(..., "90"))` in `config.py`,
`_cleanup_adaptive_poll_metrics` deleting `date < cutoff`, added to the init_db cleanup pass ONLY (BUILT).
init_db runs at every process start of manager/scheduler/rebuild/pipeline.py/CLI scripts, including the
daily 07:00 `pipeline.py --monitor-jobs` cron (`setup_cron.sh:293-308`), so cleanup runs daily without
the manager. Proof it works: `api_health` oldest row sits exactly at its 60-day cutoff on the VM.
The manager step does NOT run cleanup. 90 covers the 14-day
score window, the 7-day Monday report and the 7-day backfill with margin. The snapshot's `*_cum`
baseline is read from the latest row. `_cleanup_adaptive_poll_metrics` deletes by date only and does
NOT protect a company's latest row: if snapshots stop for longer than the retention window, the
company's baseline is deleted and the next night writes cumulative values with a delta of 0 (a
one-night gap in that company's counts, no spike). With 90 days of retention against a nightly job
this needs a three-month outage; it is accepted rather than guarded.

## 9. Open points

- RESOLVED: snapshot runs in the manager's 11 pm recalculation (`_midnight_recompute`,
  `workers/manager.py:1180`, once per local day via `manager:last_recompute_date`). Step 5 must make
  it a separate try/except so a snapshot failure cannot block the ceiling recompute.
- Which feeders have real `posted_at` (step 1). Eightfold confirmed by user; rest unread.
- RESOLVED/FIXED 2026-11-07: `consecutive_empty` double increment. Only readers: `workers/rebuild.py:555`
  copies it to a Redis hash field that nothing reads (grep of all .py/.lua). So it was wrong but harmless.
  `on_adaptive_complete` (`scheduler.py`) no longer touches it; `upsert_poll_stats` (completed
  scans only) is the single owner. Failed scans no longer advance it.
- BUILT 2026-11-07 (uncommitted, not deployed, not VM-verified): config constants, schema columns
  (`company_poll_stats.empty_polls/early_exit_polls`, `adaptive_poll_metrics.*_cum/adaptive_score`),
  `upsert_poll_stats(..., early_exited)`, `db/adaptive_metrics.py` (`snapshot_counters`, `snapshot_jobs`,
  `run_nightly`), manager hook `_start_adaptive_snapshot` (daemon thread, one at a time; started by
  `_adaptive_snapshot_due` once `manager:last_recompute_date` is today and `manager:adaptive_snapshot_date`
  is not, so a snapshot failure cannot touch the ceiling recompute; a run with errors or a manager restart
  mid-run is retried every cycle until the local date rolls over, after which that day's counter delta is
  merged into the next snapshot's delta), `db/adaptive_health.py` fixes
  (`query_score_oscillation` per-company stddev of `adaptive_score`, was selecting non-existent
  `poll_score`; detection-age percentages over dated jobs), report labels, `tests/test_adaptive_metrics.py`.
- KEY MISMATCH (from code, `scan_worker.py` ca:NN note): `upsert_poll_stats` uses the routing key
  (`ca:42`) while `job_postings.company` holds the display name. Counter rows and job rows for such
  companies therefore land on different `company` values in `adaptive_poll_metrics`. Platform-level sums
  (all the weekly report uses) are unaffected; per-company score oscillation uses counter rows only.
- A negative delta (counter reset after a row delete in `company_poll_stats`, see
  `scheduler.py:2953`) is clamped to 0 and logged.
