# ATS Re-detection Design

**Status:** Designed 2026-08-16 — pending implementation  
**Related docs:** `enrichment_discovery_design.md`, `job-monitoring.md`, `prospective-companies.md`

---

## Problem

When a monitored company switches ATS platforms (e.g. Nomura's Taleo path goes silent while their Workday path stays active), the current pipeline has no mechanism to detect the change. The old row stays `is_monitored=TRUE` indefinitely, the job monitor keeps scraping a dead endpoint, and the new ATS is never discovered.

The current code path in `_upsert_company_ats` makes this worse: if an old `(domain, platform)` row is `is_monitored=TRUE`, new detections for that FEIN/domain are silently dropped with "already monitored — skipping."

---

## Design Goals

- Detect ATS changes per-path, not per-company (Nomura's Workday path must not be disturbed when its Taleo path goes silent)
- Multi-ATS companies (UNIQUE(domain, platform)) work correctly throughout
- No wasted compute on `unknown`/`unsupported` platforms — those are never re-detected
- Reuse existing workers; no new worker process
- Human review required before any new detection goes live

---

## Trigger

Both `company_ats` and `prospective_companies` feed the same trigger condition:

```
consecutive_empty_days >= JOB_MONITOR_REDETECT_DAYS (14)
AND is_monitored = TRUE
AND platform NOT IN ('unknown', 'unsupported')   -- prospective_companies only
```

`staleness_checker.py` gains a **3rd pass** (after enrichment staleness + discovery staleness) that queries both tables for silent rows and pushes them to `REDETECT_QUEUE`.

No career page liveness check — `consecutive_empty_days` alone is the trigger. The job monitor already knows no jobs are coming from that ATS/slug.

---

## New Redis Queue

```
REDETECT_QUEUE = "redetect_queue"   # Redis ZSET, score = petition_count
```

Payload:
```json
{"fein": "12-3456789", "source": "company_ats"}
{"fein": "12-3456789", "source": "prospective"}
```

`source` travels through the entire pipeline so `discover_h1b_ats_worker` knows which table to flag stale after re-detection completes.

---

## Pipeline Flow

```
staleness_checker.py (3rd pass)
    company_ats rows: consecutive_empty_days >= 14, is_monitored=TRUE
    prospective_companies rows: same condition, platform NOT IN ('unknown','unsupported')
        ↓  ZADD REDETECT_QUEUE score=petition_count {"fein": ..., "source": ...}

domain_enrichment_worker.py
    Polls REDETECT_QUEUE FIRST (priority), then DOMAIN_ENRICHMENT_QUEUE (fallback)
    Re-runs full domain enrichment for the FEIN:
        → resolves fresh public_domain + careers_url
        → writes back to fein_domain_map
        → pushes to DISCOVERY_QUEUE with source field preserved in payload

discover_h1b_ats_worker.py
    Reads DISCOVERY_QUEUE as before (no queue change)
    Runs ATS detection from fresh domain/careers_url
    
    If source = "redetect":
        For each newly detected (domain, platform):
            INSERT new company_ats row (is_monitored=FALSE, pending human review)
        After inserts complete:
            SET stale_since = NOW() on old rows for this FEIN
            WHERE consecutive_empty_days >= JOB_MONITOR_REDETECT_DAYS
            AND stale_since IS NULL
        
        For prospective_companies (source = "prospective"):
            UPDATE prospective_companies SET ats_platform=new, ats_slug=new, is_monitored=FALSE
            WHERE company = ... AND ats_platform != new_platform
```

---

## Schema Changes

### `company_ats` — 1 new column

```sql
ALTER TABLE company_ats ADD COLUMN IF NOT EXISTS stale_since TIMESTAMPTZ;
```

| Column | Type | Default | Purpose |
|---|---|---|---|
| `stale_since` | `TIMESTAMPTZ` | `NULL` | Set by discover worker after re-detection finds a different ATS on this path. NULL = active. Purged after `ATS_STALE_TTL_DAYS`. |

**Deliberately no `previous_platform` or `previous_slug`.** Once a company has migrated ATS, the old detection data is irrelevant — the old career URL may not even exist anymore. Tracking it adds noise without value.

### No changes to `prospective_companies`

Existing `is_monitored` equivalent + `ats_platform`/`ats_slug` columns are sufficient. The discover worker updates the row in-place.

---

## New Config Constants

```python
# config.py
REDETECT_QUEUE      = "redetect_queue"   # Redis ZSET — silent monitored companies awaiting re-detection
ATS_STALE_TTL_DAYS  = 30                 # days before stale company_ats rows are purged
```

---

## Stale Row Cleanup

A 4th pass in `staleness_checker.py` (or dedicated cron):

```sql
DELETE FROM company_ats
WHERE stale_since IS NOT NULL
  AND stale_since < NOW() - INTERVAL '30 days';
```

30-day TTL gives the human reviewer enough time to approve/reject the new `is_monitored=FALSE` rows before the old stale rows disappear.

---

## Human Review Flow

After re-detection, the Discover page shows new `company_ats` rows with `is_monitored=FALSE`. These are indistinguishable from any other pending detection — the reviewer sees company, domain, platform, slug, and approves by flipping `is_monitored=TRUE`. No special UI needed.

The old stale row (platform that went silent) is no longer scraped immediately (job monitor only picks up `is_monitored=TRUE AND stale_since IS NULL`). It auto-purges after 30 days.

---

## What Stays the Same

- `discover_h1b_ats_worker.py` queue consumption loop unchanged — still reads `DISCOVERY_QUEUE`
- `domain_enrichment_worker.py` re-detection logic is identical to normal enrichment — just priority-ordered differently
- `_upsert_company_ats` dedup guard (`is_monitored=TRUE` early return) stays — but does NOT fire during redetect path because old row gets `stale_since` set first, and job monitor query excludes `stale_since IS NOT NULL` rows
- Multi-ATS companies: only the silent path is affected; other `(domain, platform)` rows for the same FEIN are untouched

---

## Manager-Based Worker Lifecycle (locked design)

### Problem with current approach

`domain_enrichment_worker` and `discover_h1b_ats_worker` are oneshot queue consumers — they start when `staleness_checker.py` pushes work, drain the ZSET, and exit. Two gaps result:

1. **`api.py` blind spot:** `_trigger_enrichment` pushes a FEIN to `DOMAIN_ENRICHMENT_QUEUE` but no worker is guaranteed to be running. If no worker is alive, the job sits until the next `staleness_checker` cron fires — up to 90 days later.
2. **`REDETECT_QUEUE` same problem:** pushing to a new queue doesn't help if no worker is listening.

`startup_failure_alert.py` (the `OnFailure=` handler) only sends an email — it does **not** restart the worker. It grabs the last 30 journal lines, composes an HTML email ("Manual intervention required"), and exits. A human must SSH in and restart.

### Why watchdog doesn't apply here

The watchdog monitors **persistent stream consumers** (scan_workers, detail_workers, fullscan_workers) that run 24/7. It cannot distinguish between a normal exit (queue empty — correct) and a crash exit (queue still has items — problem). Domain enrichment and discovery workers exit in both cases, so the watchdog would incorrectly restart them after every normal drain.

### Locked solution: manager autoscaling

The manager (`workers/manager.py`) already reads `DOMAIN_ENRICHMENT_QUEUE` and `DISCOVERY_QUEUE` depths but marks them `# informational — not autoscaled`. This gap gets closed as part of this implementation.

**Behaviour per worker pool (enrichment + discovery + redetect):**

| Condition | Manager action |
|---|---|
| Queue depth > 0, no worker alive | Start worker @1 |
| Queue depth > `ATS_SCALE_UP_THRESHOLD` | Start worker @2 |
| Queue empty for N consecutive cycles | Stop all workers for this pool |
| Worker crashes while queue non-empty | Manager sees depth > 0, no alive worker → restarts; `OnFailure=` alert fires only after `StartLimitBurst` exhausted |

**Consequence for `staleness_checker.py`:** it stops calling `start_workers()` after ZADD. Its only job is to populate queues. The manager handles worker lifecycle.

**Consequence for `api.py`:** `_trigger_enrichment` just does ZADD — no `start_workers()` call needed. Manager sees the queue depth immediately on next poll cycle and starts a worker.

**Consequence for `OnFailure=` / `%p-%i`:** the `%p-%i` fix already implemented stays correct. The alert still fires on genuine repeated-crash scenarios (`StartLimitBurst=5` in 5 minutes). But the manager's restart loop means a single crash no longer leaves the queue stranded — the manager restarts the worker before alerting is even needed.

### New config constants for manager scaling

```python
# config.py
ATS_MANAGER_SCALE_UP_THRESHOLD  = 50   # queue depth → start 2nd enrichment/discovery worker
ATS_MANAGER_IDLE_CYCLES         = 3    # consecutive empty poll cycles → stop workers
```

---

## Files to Change (implementation order)

1. `db/schema.py` — add `stale_since` column to `company_ats`
2. `config.py` — add `REDETECT_QUEUE`, `ATS_STALE_TTL_DAYS`, `ATS_MANAGER_SCALE_UP_THRESHOLD`, `ATS_MANAGER_IDLE_CYCLES`
3. `scripts/staleness_checker.py` — 3rd pass (redetect) + 4th pass (stale purge); remove `start_workers()` calls after ZADD
4. `workers/domain_enrichment_worker.py` — poll `REDETECT_QUEUE` first, carry `source` through to DISCOVERY_QUEUE payload
5. `workers/discover_h1b_ats_worker.py` — handle `source=redetect`: set `stale_since` on old rows, update `prospective_companies` for `source=prospective`
6. `db/job_monitor.py` — exclude `stale_since IS NOT NULL` rows from monitored company queries
7. `workers/manager.py` — promote enrichment + discovery + redetect pools from informational to autoscaled; remove `start_workers()` from `api.py` `_trigger_enrichment`
