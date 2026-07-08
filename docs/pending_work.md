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

### 4.1 Accenture India jobs (393 already saved)
**Status:** Future scans fixed — workers now run correct code, all four Workday
keys (`_slug/_wd/_path/_external_path`) forwarded in detail payload.
ATCI-* jobs correctly get `_country_code='IN'` → filtered before save.  
**The 393 already-saved jobs:** In DB as `status='new'`, already included in
a digest and marked digested. Will not reappear.  
**Action needed:** None — monitor next digest to confirm no new Accenture India leakage.

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

## Deployment Order

```text
Phase 1  →  Phase 2  →  Phase 3  →  Phase 4 monitoring
(fix + cleanup)  (thundering herd)  (reliability)  (observe)

All Phase 1 + Phase 2 changes ship in one commit.
Phase 3 can ship incrementally (systemd first, then watchdog, then Redis).
```
