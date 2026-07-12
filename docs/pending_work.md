# Pending Work — Job Pipeline

Last updated: 2026-06-23

Everything that needs to be implemented, in priority order.
Items marked ✅ are done locally but not yet deployed.

---

## PHASE 1 — Next Deploy (ship together)

These are already coded or trivially small. Hold until scheduler.py cleanup
is done so we don't ship a half-broken file.

### 1.1 Coverage metric fix ✅ DONE (monitor_report.py)
**File:** `outreach/report_templates/monitor_report.py`  
**Problem:** Email/PDF showed "Coverage: 28/139 (20%)" instead of "111/139 (80%)".
`companies_with_results` was only counting fallback-fetched missed companies,
not the 111 already covered by scan workers.  
**Fix applied:** `total_covered = covered_by_workers + fallback_scanned + in_flight`.
Email now shows breakdown: "111 by workers + 9 by job monitor (6 with jobs, 3 empty) + 2 in-flight".

### 1.2 Remove premature _least_loaded_slot() from scheduler.py ✅ DONE
**File:** `workers/scheduler.py`  
`_least_loaded_slot()` removed. Replaced by `_pick_schedule_time()` (Phase 2).
Both call sites (`_reschedule_adaptive()` and fullscan scheduling) updated.

---

## PHASE 2 — Thundering Herd Prevention ✅ DONE

Full design in `docs/architecture.md`.

### 2.1 Core algorithm — _pick_schedule_time() ✅ DONE
**File:** `workers/scheduler.py`  
**Replaces:** `_least_loaded_slot()` (20-slot min-heap, now removed)  
**Algorithm:** Gap-detection / largest-gap midpoint.

```python
window_s = interval_s * tolerance_pct      # e.g. 20% of 86400 = 17280 s = 4.8 h
lo       = target_ts - window_s / 2
hi       = target_ts + window_s / 2
existing = r.zrangebyscore(queue_key, lo, hi, withscores=True)   # one Redis call

points = sorted([lo] + [s for _, s in existing] + [hi])
gaps   = [(points[i+1]-points[i], points[i], points[i+1]) for i in range(len(points)-1)]
gaps.sort(key=lambda g: (-g[0], abs((g[1]+g[2])/2 - target_ts)))

for gap_size, gap_lo, gap_hi in gaps:
    midpoint = (gap_lo + gap_hi) / 2
    if deadline_ts and midpoint + avg_duration_s >= deadline_ts:
        continue     # would miss 7 AM digest — try next gap
    return midpoint
return target_ts     # fallback (all gaps violate deadline)
```

**Key improvements over _least_loaded_slot():**
- One `ZRANGEBYSCORE` call (was 20 pipelined `ZCOUNT` calls)
- Works at arbitrary resolution — no slot-boundary clustering
- Converges from full cluster to even distribution in 2–3 cycles
- Integrated deadline guard (fullscan only) — replaces danger-zone jitter

### 2.2 _reschedule_adaptive() wired ✅ DONE
**File:** `workers/scheduler.py`  
`tolerance_pct=0.20`, no deadline check (adaptive scans are seconds to minutes).

### 2.3 fullscan scheduling wired ✅ DONE
**File:** `workers/fullscan.py` — `_run_fullscan()` completion path  
Replaced 40-line danger-zone/jitter/clamping block with `_pick_schedule_time()` call.
`tolerance_pct=0.20`, `deadline_ts=_next_digest_deadline(now)`, `avg_duration_s` from EMA.

Also updated `on_fullscan_complete()` in `workers/scheduler.py` (legacy path).

### 2.4 inflight:fullscan ZSET tracking ✅ DONE
**Files:** `workers/fullscan.py`, `config.py`  
- `REDIS_INFLIGHT_FULLSCAN = "inflight:fullscan"` added to `config.py`
- `ZADD inflight:fullscan {company: start_ts}` after lock acquisition in `_run_fullscan()`
- `ZREM inflight:fullscan company` in `finally` block (runs on crash, clean exit, error)

### 2.5 Duration EMA ✅ DONE
**Files:** `workers/fullscan.py`, `db/schema.py`  
- `_complete_fullscan_db()` now accepts `duration_s` + `prev_avg_duration_s`
- EMA written to DB after every successful scan (α=0.3)
- `_get_fullscan_state()` fetches `avg_fullscan_duration_s` so it's available at scheduling time
- DB migration added to `init_db()` in `db/schema.py` (idempotent `ADD COLUMN IF NOT EXISTS`):
  - `last_fullscan_duration_s  INTEGER`
  - `avg_fullscan_duration_s   DOUBLE PRECISION DEFAULT 1800.0`

### 2.6 job_monitor inflight exclusion ✅ DONE
**File:** `jobs/job_monitor.py` — `_get_worker_missed_companies()`  
Reads `inflight:fullscan` ZSET from Redis and excludes in-progress companies from the
fallback re-fetch list. Redis unavailable → proceeds without exclusion (conservative).

### 2.7 rebuild.py comment updated ✅ DONE
**File:** `workers/rebuild.py`  
Stale reference to `_least_loaded_slot()` replaced with `_pick_schedule_time()`.

---

## PHASE 3 — Reliability Layer (implement after Phase 2)

### 3.1 systemd unit files ✅ DONE
**Files created:**
- `deploy/systemd/recruiter-scheduler.service` — runs `pipeline.py --scheduler`, spawns ALL workers
- `deploy/systemd/recruiter-watchdog.service` — runs `workers/watchdog.py` continuously
- `deploy/systemd/recruiter-pipeline-alert@.service` — OnFailure= template, calls `startup_failure_alert.py`
- `deploy/install-systemd.sh` — one-time server setup (run once: `sudo bash deploy/install-systemd.sh`)
- `deploy/deploy.sh` — code deploy script: `git pull → pip install → systemctl restart → health_check`
- `scripts/startup_failure_alert.py` — email alert with last 30 journal lines when StartLimitBurst hit

**How it works:**
- `recruiter-scheduler.service` → `Restart=always RestartSec=30s` → scheduler restarts within 30s on any crash
- `recruiter-watchdog.service` → `Restart=always RestartSec=10s` → watchdog restarts within 10s
- `StartLimitBurst=5` in 300s → if service dies 5× in 5 min, systemd stops retrying and fires `OnFailure=`
- `OnFailure=recruiter-pipeline-alert@%p.service` → `startup_failure_alert.py` sends email with journal logs

**Who manages what (current state with systemd):**

| Component | Spawned by | Managed by | Restarts within |
|---|---|---|---|
| scheduler | systemd | systemd (`recruiter-scheduler.service`) | 30s |
| scan_worker pool | scheduler | scheduler (pool) | ~35s (30s restart + spawn) |
| detail_worker pool | scheduler | scheduler (pool) | ~35s |
| fullscan_worker | scheduler | scheduler (pool) | ~35s |
| watchdog | systemd | systemd (`recruiter-watchdog.service`) | 10s |

**Watchdog ↔ systemd integration (wired):**
- `_SYSTEMD_AVAILABLE` detected at import via `shutil.which("systemctl")` + `/run/systemd/system`
- `_SYSTEMCTL` = resolved full path (e.g. `/usr/bin/systemctl`) — matches sudoers rule exactly
- Heal for any dead worker → `sudo systemctl reset-failed recruiter-scheduler && sudo systemctl restart recruiter-scheduler`
- Heal for failed systemd service → same (caught by new `check_systemd_services()` check)
- `check_systemd_services()` directly queries `systemctl is-active` for both units every cycle
- sudoers rule in `/etc/sudoers.d/mail-pipeline` grants NOPASSWD for reset-failed + restart + is-active

**Deployment commands:**
```bash
# One-time server setup:
sudo bash deploy/install-systemd.sh

# Every code deploy (no sudo needed — sudoers covers it):
bash deploy/deploy.sh

# Day-to-day operations:
sudo systemctl status recruiter-scheduler       # status + last 10 log lines
sudo systemctl status recruiter-watchdog
journalctl -u recruiter-scheduler -f            # live scheduler logs
journalctl -u recruiter-watchdog -f             # live watchdog logs
python scripts/health_check.py             # instant full status
```

### 3.2 Worker heartbeat in Redis ✅ DONE
**Files:** `workers/scan_worker.py`, `workers/fullscan.py`, `workers/detail_worker.py`,
`workers/scheduler.py`  
**Key:** `worker:alive:{type}:{hostname}:{pid}` with appropriate TTL per worker  
**Payload:** `{"pid": os.getpid(), "processed": count, "ts": time.time()}`  
**TTLs:** scheduler=30s (per-loop key), scan_worker=30s, detail_worker=30s, fullscan_worker=180s

### 3.3 workers/watchdog.py ✅ DONE (complete rewrite + systemd wired)
**Purpose:** Runs continuously under systemd. Checks all components every 5 min,
sends email alerts, auto-heals via systemctl, escalates after 3 failed attempts.

**Checks implemented:**
- `check_systemd_services()` — direct `systemctl is-active` query for both units (NEW — catches failed/inactive state before heartbeat TTL expires)
- Worker heartbeats (scheduler, scan_worker, detail_worker, fullscan_worker)
- Queue health: poll:adaptive, poll:fullscan, detail queues, stream PEL ages
- Bloom filter presence (detects Redis wipe)
- Coverage: companies not scanned in 26h
- Hung workers: heartbeat alive but no per-company progress update
- Redis persistence: last RDB save age

**Alert format:** HTML email, per-type dedup, suggested fix command in body.  
**scripts/health_check.py** ✅ DONE — instant color-coded CLI status tool.  

**Self-healing state machine:**
```text
NEW issue detected
  → healable? → attempt_heal() → email "⚠ Auto-heal attempted (1/3)"
  → not healable? → email "⚠ Issue detected — manual fix needed"

NEXT CYCLE (5 min later):
  → issue resolved → email "✅ Auto-healed"
  → issue persists + under attempt limit → attempt_heal() again
  → issue persists + 3 failed attempts → email "🆘 ESCALATION — manual required"
                                        → stop retrying for 24h
```

**Heal actions (systemd mode — production):**
- `worker:scan_worker` → `systemctl reset-failed recruiter-scheduler && systemctl restart recruiter-scheduler`
- `worker:detail_worker` → same (workers are children of scheduler)
- `worker:fullscan_worker` → same
- `worker:scheduler` → same
- `systemd:recruiter-scheduler failed/inactive` → same (detected by check_systemd_services)
- `queue:poll:adaptive empty` → `python pipeline.py --rebuild` (foreground)
- `queue:poll:fullscan empty` → `python pipeline.py --rebuild` (foreground)

**Heal actions (subprocess mode — dev/cron, no systemd):**
- `worker:*` → spawn detached background process (`start_new_session=True`)

**Usage:**
```bash
python scripts/health_check.py              # instant status, no email, no healing
python -m workers.watchdog                  # run forever (managed by systemd in prod)
python -m workers.watchdog --once           # single check (for cron / debugging)
python -m workers.watchdog --status         # print status then exit (no email)
python -m workers.watchdog --no-heal        # alerts only, no auto-restart
```

### 3.4 Startup validation in each worker ✅ DONE
**Files:** `workers/startup.py` (new), `workers/scan_worker.py`, `workers/fullscan.py`, `workers/detail_worker.py`  
**Checks:** Redis PING + write test + version check (≥6.2), PostgreSQL SELECT 1 (connectivity only), required `.env` keys present.  
Each worker calls `validate_startup(worker_name)` at the top of `run_worker()` before the main loop.
Exits with `sys.exit(1)` + clear error message on any failure — surfaced immediately in `journalctl`.

### 3.5 Queue depth alert in daily digest email ✅ DONE
**File:** `outreach/report_templates/monitor_report.py` — new `_build_queue_health_section()`  
Embedded in every daily digest email above the API warnings section.  
Shows depth of `detail:adaptive`, `detail:fullscan`, `poll:adaptive`, `poll:fullscan`.  
Color-coded: green OK / amber >100 / red >500. Highlights overdue companies.

### 3.6 Redis AOF persistence ✅ DONE
**Script:** `deploy/configure-redis.sh` (one-time, run with sudo)  
Applies `appendonly yes` + `appendfsync everysec` via `CONFIG SET` (live, no restart) and
patches `redis.conf` for persistence across Redis restarts.  
Data-loss window: ~5 min (RDB) → ~1 second (AOF).  
Also sets `auto-aof-rewrite-percentage 100` + `auto-aof-rewrite-min-size 64mb`.

### 3.7 At-least-once delivery for detail queue ✅ DONE
**File:** `workers/detail_worker.py`  
Replaced `BRPOP` (destructive) with `LMOVE → per-PID inflight list` pattern:
- `_pop_with_inflight()` atomically moves item from source → `queue:detail:*:inflight:{pid}`
- Item is `LREM`'d from inflight ONLY after successful DB write (or left in inflight if `retryable=True`)
- `_recover_stuck_jobs(r, own_pid)` called on startup: scans for all `:inflight:*` keys, checks each
  peer's heartbeat (`worker:alive:detail_worker:{pid}`), and drains only confirmed-dead peers' items —
  never touching live workers' active jobs
- Shutdown requeue is atomic via Lua script (LREM + LPUSH in a single `eval`) — no crash window
- Priority ordering preserved: adaptive checked first (non-blocking), fullscan second

---

## PHASE 4 — Known Issues / Ongoing Monitoring

### 4.1 Accenture India jobs — RESOLVED
**Status:** Root cause fully traced and fixed.  
- **Before Jul 7 deploy:** `ats_slug` path keys were wrong → detail fetch guard fired
  silently → Workday `_external_path`/`_slug`/`_wd` missing from payload → `_enriched=False`
  (location/cc/description unchanged after "fetch") → URL-city fallback text matched Italian
  cities (Torino/Milano/Roma) which are not in `_NON_US_CITY_OVERRIDES` → Signal 8 default
  `True` → jobs leaked into digest.  
- **Jul 7 deploy:** correct `{"slug":"accenture","wd":"wd103","path":"AccentureCareers"}`
  in DB → Workday detail fetch makes real HTTP → `_country_code='IN'` for India jobs
  → filtered before save.  
- **All saved Accenture jobs** (1284) are now `status='digested'`. 0 `status='new'`.  
**Action needed:** None — Jul 8+ logs confirm 0 new Accenture India jobs saved.

### 4.2 General Motors [SKIP] in fullscan
**Status:** One-off stale lock from crashed session (lock TTL=3600s, auto-expired).
DB config is valid: `{"slug": "generalmotors", "wd": "wd5", "path": "Careers_GM"}`.  
**Action needed:** None — will scan normally on next cycle. Monitor for recurrence.

### 4.3 28 companies missed fullscan before 7 AM
**Root cause:** fullscan worker started May 23, scan_worker and detail_worker
both died at some point. Only 1 fullscan worker running, all 139 companies to
scan before 7 AM — throughput ceiling reached.  
**Fix:** Phase 2 thundering herd prevention ensures companies are spread evenly
across the monitoring window so all complete before 7 AM.  
**Also:** Phase 3 systemd + watchdog prevents workers from staying dead.

### 4.4 check_thundering_herd.py ideal_per_bk formula
**Status:** ✅ Fixed. Was dividing by occupied buckets only; now divides by
all slots in a full 24h cycle (`slots_in_cycle = math.ceil(86400 / (bucket_minutes * 60))`).

---

## PHASE 5 — Concurrency Hardening ✅ DONE

### 5.1 PEL consumer dead-detection fix ✅ DONE
**File:** `workers/watchdog.py` — `_consumer_pid_alive()`  
**Problem:** Scheduler consumers use the name format `scheduler-{hostname}-{pid}`, but
`_consumer_pid_alive()` only handled `worker-{hostname}-{pid}` format.  All scheduler
consumers were incorrectly reported as DEAD in the PEL health check.  
**Root cause:** The heartbeat key lookup was being done with the wrong key name; the function
also needed to parse PID from the consumer name and match it against the heartbeat JSON payload.  
**Fix:** Added a `scheduler-` prefix branch that extracts the PID suffix from the consumer
name and checks whether either `worker:alive:scheduler:adaptive` or
`worker:alive:scheduler:fullscan` contains a matching `"pid"` field.  
**Tests added:** 3 new test methods in `TestCheckPelHealthPID`
(`test_scheduler_consumer_alive_same_pid_is_ok`,
`test_scheduler_consumer_dead_different_pid_is_error`,
`test_scheduler_consumer_no_heartbeat_is_error`).

### 5.2 Fullscan worker auto-scaling ✅ DONE
**Files:** `workers/scheduler.py`, `config.py`  
**Problem:** Fullscan worker pool was fixed at `WORKER_FLOOR=2` and never scaled.
`_slow_throughput_check_loop` only handled scan and detail workers.  
**Fix:**
- Added `WORKER_POOL_FULLSCAN_FRACTION = 0.25` to `config.py`
- Added `"fullscan_add"` / `"fullscan_remove"` hysteresis counters to `_hysteresis` dict
- Extended `_add_one_worker()` / `_remove_one_worker()` to handle `"fullscan"` type
- Added `_get_fullscan_pool_size()` helper
- Added fullscan scaling block in `_slow_throughput_check_loop`:
  - Scale up when `stream:fullscan` has more messages than current pool size (2-check hysteresis)
  - Scale down when stream is idle and pool > `WORKER_FLOOR` (2-check hysteresis)
  - Ceiling: `max(WORKER_FLOOR, int((db_budget - n_scan - n_detail) * 0.25))`

### 5.3 Per-DC ceiling enforcement for fullscan dispatch ✅ DONE
**Files:** `workers/scheduler.py`, `workers/fullscan.py`, `config.py`  
**Problem:** Adaptive scan workers respect a per-DC learned ceiling (`worker:ceil:learned:{dc_key}`)
via `inflight:scans:{dc_key}` ZSET.  Fullscan workers bypassed this ceiling entirely — a running
fullscan still occupied a DB connection toward the DC's limit but was invisible to the adaptive
ceiling check.  A TOCTOU race also existed: if multiple companies were dispatched in the same loop
iteration before any worker registered its slot, all could simultaneously read "slot available" and
proceed.  
**Fix — three parts:**

**Part A — Atomic slot claim in `fullscan_loop` (scheduler.py):**
- Added `_FULLSCAN_INFLIGHT_CLAIM_LUA` Lua script: atomically prunes stale entries from
  both `inflight:scans:{dc_key}` (adaptive, 10-min stale window) and
  `inflight:fullscans:{dc_key}` (fullscan, 2-h stale window), counts them together, and
  ZADDs the company to `inflight:fullscans:{dc_key}` only if `total < ceiling`.  Returns 1 if
  claimed, 0 if throttled.  Fullscan and adaptive workers share the same learned ceiling.
- Dispatched after `_get_dc_key_for_company()`, before `XADD` to `stream:fullscan`.
- If throttled: re-queue company in `poll:fullscan` at `+30s` and `continue`.

**Part B — Current-job key in `_run_fullscan` (fullscan.py):**
- Added `dc_key: str = ""` parameter to `_run_fullscan()`.
- At scan start: `SET worker:current_job:fullscan:{pid} "{company}|{dc_key}" EX 3600`.
  This key lets `_replace_dead_workers()` identify which inflight slot to release on crash.
- In `finally`: `ZREM inflight:fullscans:{dc_key} {company}` + `DEL worker:current_job:fullscan:{pid}`.
- `run_worker()` now extracts `dc_key = fields.get("dc_key", "")` from the stream message and
  passes it to `_run_fullscan()`.

**Part C — Dead-worker inflight cleanup in `_replace_dead_workers` (scheduler.py):**
- After the existing `record_scaling_event` loop, if any dead fullscan workers were replaced:
  - `GET worker:current_job:fullscan:{old_pid}` → parse `company|dc_key`
  - `ZREM inflight:fullscans:{dead_dc_key} company` → release the per-DC slot
  - `DEL worker:current_job:fullscan:{old_pid}` → clean up
- Runs event-driven (~5s after crash) — no polling required.
- If the worker died before `_run_fullscan()` SET the current_job key (e.g., died between
  XREADGROUP and scan start), the slot remains until the 2-h stale window prunes it on
  the next dispatch — acceptable backstop.

**New config constants:**
- `REDIS_INFLIGHT_FULLSCAN_DC_PREFIX = "inflight:fullscans"` — per-DC ZSET key prefix
- `WORKER_CURRENT_JOB_FULLSCAN_PREFIX = "worker:current_job:fullscan"` — per-PID key prefix
- `WORKER_CURRENT_JOB_FULLSCAN_TTL = 3600` — 1-h safety TTL on current-job key

**Adaptive ceiling also updated:** `adaptive_loop` ceiling check now counts both
`inflight:scans:{dc_key}` and `inflight:fullscans:{dc_key}` so a running fullscan is
visible to adaptive throttling as well.

**Why separate ZSET keys (not shared `inflight:scans:{dc_key}`):**
Adaptive entries in `inflight:scans:{dc_key}` are pruned after 10 minutes (`INFLIGHT_STALE_WINDOW_S`).
Fullscans take 20–30 min — sharing the key would cause the adaptive stale cleanup to prune active
fullscan slots mid-scan, silently undercounting concurrent load.

---

## PHASE 6 — Correctness Fixes ✅ DONE (locally, awaiting deploy)

Traced from the India Accenture leak investigation (Jun–Jul 2026). All fixes are
implemented in this PR and awaiting deployment. Ship together — they address the same root-cause chain.

### 6.1 scan_worker: empty-string detail keys treated as absent ✅ DONE
**File:** `workers/scan_worker.py` — `_build_detail_payload()`  
**Problem:** `if job.get(key) is not None:` forwarded empty strings (`""`) into the
detail payload. `fetch_job_detail()` guard clauses use `not all([...])` which treats
`""` as falsy — so the guard fires, the HTTP request is skipped, and the job returns
unenriched. Downstream code can't distinguish this from a successful detail fetch.  
**Fix:** Changed to `if job.get(key):` — empty strings are now treated as absent
and not forwarded into the detail payload.  
**Impact:** When `has_detail=True` and `listing_filter=="title_only"`, the absent
key causes `should_fetch_detail()` in `detail_worker` to return `False`. The
pre-fetch eligibility check (`not detail_attempted and has_detail and
listing_filter=="title_only"`) then fires before any HTTP request or internal
`fetch_job_detail()` guard is reached, returning `error/retryable` instead of
silently entering the filter pipeline with incomplete data. Other filter modes
(`full`, etc.) are unaffected; equivalent protection requires separate handling.

### 6.2 job_monitor + registry: Workday `_should_fetch_detail` requires all 4 keys ✅ DONE
**Files:** `jobs/job_monitor.py` — `_should_fetch_detail()`, `jobs/ats/registry.py` — `should_fetch_detail()`  
**Problem:** Workday detail fetch was attempted when only `_external_path`, `_slug`, and
`_wd` were present, even if `_path` was missing. `fetch_job_detail()` needs all four to
construct the correct Workday URL — missing `_path` causes the guard to fire and the job
returns unenriched. The same gap existed in both the local `_should_fetch_detail()` in
`job_monitor.py` and the registry-level `should_fetch_detail()` in `registry.py`.  
**Fix:**
```python
if platform == "workday":
    return bool(
        job.get("_external_path")
        and job.get("_slug")
        and job.get("_wd")
        and job.get("_path")
    )
```
All four must be truthy before the detail fetch is attempted. Applied identically in both
`job_monitor.py` and `registry.py`.

### 6.3 detail_worker Bug 1: missing detail keys + title_only → retryable ✅ DONE
**File:** `workers/detail_worker.py`  
**Problem:** If `should_fetch_detail()` returns `False` (required Workday keys absent in
payload) but the company config has `has_detail=True` and `listing_filter="title_only"`,
the detail fetch is silently skipped and the job proceeds through the filter pipeline with
no location data. `is_us_location("")` returns `True` by default → non-US jobs leak into
the digest.  
**Fix:** After `detail_attempted = should_fetch_detail(...)`:
```python
if (
    not detail_attempted
    and config.get("has_detail")
    and config.get("listing_filter") == "title_only"
):
    result["outcome"]   = "error"
    result["retryable"] = True
    return result   # retry: can't safely filter without location data
```
The job is retried (up to `_MAX_DETAIL_RETRIES`), giving scan_worker time to push a
payload with the required keys. If retries are exhausted, it dead-letters with a full
warning log rather than silently leaking.

### 6.4 detail_worker Bug 2: `_enriched=False` after Workday fetch → retryable ✅ DONE
**File:** `workers/detail_worker.py`  
**Problem:** Even when `detail_attempted=True` and `fetch_job_detail()` was called, the
function can silently return the original job unchanged if its internal guard fires
(all required keys present at call time but the HTTP request returned nothing useful, or
the guard fired for a different reason). Previously the job fell through to the filter
pipeline with no location data.  
**Fix:** Post-fetch enrichment audit compares location/cc/description against pre-fetch
snapshots. If none changed (`_enriched=False`):
```python
result["outcome"]   = "error"
result["retryable"] = True
return result   # retry: guard fired or API returned empty data
```
A WARNING is logged so the pattern is visible in `scheduler_{date}.log`.

### 6.5 scheduler: fullscan Path 2 thundering herd fix ✅ DONE
**File:** `workers/scheduler.py` — `on_adaptive_complete()`  
**Problem:** After each adaptive scan completion, `_should_trigger_full_scan()` used
`last_full_scan_at` elapsed ≥ 24h to decide whether to queue a fullscan, then
`_schedule_full_scan()` did a raw `zadd(now + 5min)` — clustering all overdue companies
at the same timestamp and silently overwriting gap-filled entries from Path 1
(`fullscan.py` post-completion rescheduling via `_atomic_schedule`).  
**Fix:** Replaced both with `_maybe_reschedule_full_scan(company, row)`:

```python
def _maybe_reschedule_full_scan(company: str, row) -> None:
    # Suppressed if ATS has active scan backoff
    if r.exists(f"{REDIS_BACKOFF_PREFIX}:scan:{company}"):
        return

    avg_duration = float(row["avg_fullscan_duration_s"] or 1800.0)
    next_scan    = row["next_full_scan_at"]

    if next_scan is not None:
        next_ts  = next_scan.timestamp()
        deadline = _next_digest_deadline(now)
        if next_ts > now and (next_ts + avg_duration) < deadline:
            return   # already scheduled and will finish before 7 AM digest

    # Only reschedule when needed — uses _atomic_schedule (gap-filling)
    _atomic_schedule(
        r, REDIS_POLL_FULLSCAN, company,
        now + SCHEDULER_FULL_SCAN_BUFFER_S,
        SCHEDULER_FULL_SCAN_INTERVAL_S, 0.20,
        deadline_ts    = _next_digest_deadline(now),
        avg_duration_s = avg_duration,
    )
```

**Key differences from old path:**
- Checks `next_full_scan_at` (DB truth) before touching `poll:fullscan` — never overwrites
  a valid future schedule from Path 1
- Checks deadline: if scheduled time + avg_duration would miss 7 AM, reschedule now
- Uses `_atomic_schedule` with gap-filling — never clusters with other companies
- Suppressed during ATS backoff (same guard as old `_should_trigger_full_scan`)

**Also:** Added `next_full_scan_at` and `avg_fullscan_duration_s` to the `company_poll_stats`
SELECT in `on_adaptive_complete` so these fields are available without an extra query.

### 6.6 job_monitor: queue fullscans for missed companies ✅ DONE
**File:** `jobs/job_monitor.py`  
**Problem:** When the 7 AM job_monitor runs, it calls `_get_worker_missed_companies()` and
performs a listing fallback scan for each missed company (all pages via `fetch_jobs()`).
This catches new jobs but does NOT update `last_full_scan_at` and does NOT queue a fullscan.
If `next_full_scan_at` is NULL or past, the company won't get a fullscan until the next
adaptive completion triggers Path 2 — which may not happen until late in the day.  
**Fix:** After the listing fallback scan, call `_queue_fullscans_for_missed(missed)`:

```python
def _queue_fullscans_for_missed(missed: list) -> None:
    # For each missed company where next_full_scan_at is NULL or in the past:
    # queue a fullscan via _atomic_schedule so the Bloom filter and
    # last_full_scan_at get updated properly in the background.
    for company in missed:
        stats = get_stats(company)
        if stats["next_full_scan_at"] is not None:
            if stats["next_full_scan_at"].timestamp() > now:
                continue   # already has a future fullscan scheduled
        _atomic_schedule(
            r, REDIS_POLL_FULLSCAN, company,
            now + SCHEDULER_FULL_SCAN_BUFFER_S,
            SCHEDULER_FULL_SCAN_INTERVAL_S, 0.20,
            deadline_ts    = _next_digest_deadline(now),
            avg_duration_s = avg_duration,
        )
```

Redis or DB failures are non-fatal (warning logged, fullscan skipped for that company).
The listing fallback result is still saved regardless.

---

## Deployment Order

```text
Phase 1  →  Phase 2  →  Phase 3  →  Phase 4 monitoring  →  Phase 5  →  Phase 6
(fix + cleanup)  (thundering herd)  (reliability)  (observe)  (concurrency)  (correctness)

All Phase 1 + Phase 2 changes ship in one commit.
Phase 3 can ship incrementally (systemd first, then watchdog, then Redis).
Phase 5 ships together (5.2 + 5.3 are interdependent; 5.1 is independent).
Phase 6 ships together — all correctness fixes address the same root-cause chain.
```
