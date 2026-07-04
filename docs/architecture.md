# Architecture

## Related Documentation

| Document | What it covers |
|---|---|
| [adaptive-polling-architecture.md](./adaptive-polling-architecture.md) | Complete design of the new continuous polling system — priority queue, adaptive intervals, Tier 1/2 guarantees, Redis integration, PostgreSQL schema, scaling properties. Start here for the new architecture. |
| [ats-fetch-strategy.md](./ats-fetch-strategy.md) | Per-platform fetch strategy — which data is available at listing level vs detail level, how smart early exit works per ATS, the canonical `ATS_TIER1_CONFIG` reference. |
| [job-monitoring.md](./job-monitoring.md) | Current job monitoring pipeline — ATS detection, job filtering, freshness detection, digest generation. |

---

## Overview

The Recruiter Outreach Pipeline is an automated system that finds recruiter contacts for companies you apply to and sends personalized cold outreach emails. It consists of three independently runnable stages that work together as a daily workflow.

## Components

### Pipeline Stages

**`--sync-forms`** — Job Ingestion
Pulls new job applications from a Google Form, inserts them into the database, and immediately attempts to scrape the job description from the provided URL. Fast portals (Greenhouse, Lever) succeed here. JS-heavy portals (Ashby, Workday) are retried during `--find-only`.

**`--find-only`** — Contact Discovery
The core intelligence stage. Runs overnight and does four things:
1. Verifies existing recruiters using the tiered verification system
2. Scrapes CareerShift for new recruiter contacts using quota-aware distribution
3. Generates personalized AI email content via Gemini for all applications
4. Tops up under-stocked companies using leftover quota

**`--import-prospects`** — Prospective Company Import
Bulk imports a list of target companies to pre-scrape during quiet quota periods.
Recruiters found in advance so outreach begins immediately when you apply.

**`--monitor-jobs`** — Job Monitoring
Queries public ATS APIs (Greenhouse, Lever, Ashby, SmartRecruiters, Workday,
Oracle HCM, iCIMS, Avature, Phenom, TalentBrew, SuccessFactors, Jobvite,
and fully custom career pages) to find newly posted jobs matching your profile.
Companies are processed in parallel using `ThreadPoolExecutor(max_workers=20)`
with per-ATS concurrency limits (`MONITOR_PLATFORM_CONCURRENCY`) to avoid
rate-limiting shared API domains (e.g. Greenhouse capped at 5 concurrent).
Uses a 4-phase ATS detection system (DB lookup → API probe → HTML redirect → Serper)
to identify which platform each company uses. Slug database built monthly via
AWS Athena queries against Common Crawl index (ats_discovery.db). Sends a daily
digest email with ranked results. Also tracks URL presence per company — each day
a tracked job URL is missing from the API response, its `consecutive_missing_days`
counter increments.

> **Planned evolution:** The current once-daily batch model is being replaced
> with a continuous adaptive polling architecture that checks active companies
> every 5–30 minutes and dormant companies every 6–24 hours. This reduces
> detection delay from 24 hours to under 1 hour for active companies, and
> reduces wasted fetches by ~99% (today: 146,497 fetches to find 157 new jobs).
> See [adaptive-polling-architecture.md](./adaptive-polling-architecture.md)
> for the full design.

**`--verify-filled`** — Filled Position Cleanup
Runs nightly after `--find-only` as part of the nightly chain. Picks up job postings that have been absent from the API for `VERIFY_FILLED_MISSING_DAYS` (3) or more consecutive days and verifies them via direct HTTP request:
- **404/410 response** → position confirmed filled → `status='filled'`, description cleared
- **200 response** → job still live (was a false positive) → `consecutive_missing_days` reset to 0
- **Timeout/error** → inconclusive → retried the next nightly run

After `VERIFY_FILLED_RETENTION` (7) days as `filled`, the row is deleted entirely to keep the database clean. If a filled job reappears within that 7-day window, it is automatically reactivated to `pre_existing`. Saves detailed run stats to `verify_filled_stats` table including a breakdown of why verifications were inconclusive (timeout vs connection error vs unexpected status code vs exception).

**`--verify-only`** — Recruiter Freshness Check
Keeps recruiter data up to date independent of job search activity.
1. Runs full tiered verification for all active recruiters (free — cached profiles)
2. Detects and reports companies that became under-stocked after verification
3. Under-stocked companies are automatically picked up by next --find-only run

**`--outreach-only`** — Email Sending
Sends scheduled outreach emails within the configured morning send window (9 AM - 11 AM). Handles the full email sequence (initial, follow-up 1, follow-up 2) and detects hard bounces automatically.

---

## Recommended Schedule

```
Daily (9 AM — automated)
  └── python pipeline.py --outreach-only
        → schedules initial emails for new recruiter+application pairs
        → sends due emails within send window
        → schedules next follow-up after each send
        → detects hard bounces → marks recruiter inactive

Weekly (Monday — automated)
  └── python pipeline.py --verify-only
        → tiered verification of ALL active recruiters (free)
        → marks departed recruiters as inactive
        → cancels their pending outreach
        → reports under-stocked companies

Nightly (1 AM — automated chain):
  Tue-Sun: sync → backup → find-only → verify-filled
  Monday:  sync → backup → verify-only → find-only → verify-filled
  1st:     sync → backup → find-only → build-slugs → enrich → VACUUM → verify-filled

As needed (when applying to new jobs)
  └── python pipeline.py --add
        → interactive: company, job URL, title, date
        → extracts expected_domain from job URL
        → scrapes and caches job description

  └── python pipeline.py --find-only
        → tiered verification of existing recruiters (free)
        → tops up companies flagged as under-stocked by --verify-only
        → scrapes CareerShift for new companies (quota-aware)
        → generates AI email content via Gemini
        → uses leftover Gemini quota for missing cache
        → runs quota health check → alert if needed

  OR (via Google Form)
  └── python pipeline.py --sync-forms
        → pulls Google Form responses into DB
        → scrapes job descriptions automatically
  └── python pipeline.py --find-only
```

---

## Data Flow

### Outreach pipeline (batch)

```
Google Form
    ↓
applications table
    ↓
jobs table (job descriptions)
    ↓
recruiters table (CareerShift scraping)
    ↓
application_recruiters table (many-to-many links)
    ↓
ai_cache table (Gemini generated content)
    ↓
outreach table (email sequence)
    ↓
Email sent to recruiter

prospective_companies table
    ↓ --monitor-jobs (daily 7 AM)
job_postings table
    ↓ URL presence tracked per company per day
    ↓ --verify-filled (nightly 1 AM, after find-only)
    ↓ 404 confirmed → status='filled' → deleted after 7 days
verify_filled_stats table (daily run metrics)
```

### Continuous adaptive polling flow

```text
PostgreSQL (company list)
    ↓ rebuild_redis() at startup
poll:adaptive ZSET (scored by next_poll_at timestamp)
poll:fullscan ZSET
    ↓ scheduler loop (continuous)
    ↓ companies due for a check are dispatched
detail:adaptive queue          detail:fullscan queue
    ↓                              ↓
detail_worker pool             fullscan_worker
    ↓                              ↓
ATS API fetch                  ATS API full scan
    ↓                              ↓
job_postings table (upsert)    job_postings table (upsert)
    ↓
Watchdog (every 5 minutes)
    ↓ checks heartbeats, queue depths, stuck jobs, service states
    ↓ auto-heals problems; escalates to email if 3 attempts fail
    ↓
Alert email (only if intervention needed)
```

---

## Component Responsibilities

| Component | Responsibility |
|---|---|
| `careershift/auth.py` | Login to CareerShift via Symplicity portal, save session |
| `careershift/find_emails.py` | Scrape recruiters, tiered verification, quota management |
| `outreach/outreach_engine.py` | Schedule and send emails, bounce detection |
| `outreach/template_engine.py` | Build email body and subject from AI cache (uses job_title from applications table) |
| `outreach/ai_full_personalizer.py` | Generate email content via Gemini AI (enforces RPM + daily limits) |
| `outreach/email_sender.py` | SMTP sending with resume attachment |
| `jobs/job_fetcher.py` | Fetch and cache job descriptions |
| `jobs/job_scraper.py` | Scrape JD from various ATS portals |
| `jobs/form_sync.py` | Pull Google Form responses into DB |
| `jobs/ats_sitemap.py` | Phase 1: ats_discovery.db lookup (Athena-built slug DB) |
| `jobs/ats_verifier.py` | Phase 2: ATS API name probe + verification |
| `jobs/career_page.py` | Phase 3a: HTML redirect + fingerprint scan |
| `jobs/serper.py` | Phase 3b: Serper API for Workday + Oracle |
| `jobs/ats_detector.py` | ATS detection orchestrator (4 phases) |
| `jobs/ats/patterns.py` | ATS URL pattern matching including Greenhouse embed format |
| `jobs/ats/greenhouse.py` | Greenhouse API client (uses first_published date) |
| `jobs/ats/lever.py` | Lever API client |
| `jobs/ats/ashby.py` | Ashby API client |
| `jobs/ats/smartrecruiters.py` | SmartRecruiters API client |
| `jobs/ats/workday.py` | Workday undocumented API (human-readable date parsing, pagination) |
| `jobs/ats/oracle_hcm.py` | Oracle HCM API client (uses Id field, pagination fixed) |
| `jobs/ats/icims.py` | iCIMS HTML scraping client |
| `jobs/ats/avature.py` | Avature sitemap scraper |
| `jobs/ats/phenom.py` | Phenom People career page scraper |
| `jobs/ats/talentbrew.py` | Radancy/TalentBrew career page scraper |
| `jobs/ats/successfactors.py` | SAP SuccessFactors career page scraper |
| `jobs/ats/jobvite.py` | Jobvite API client |
| `jobs/ats/custom_career.py` | Universal custom career page scraper — handles any company that doesn't fit a standard ATS (Amazon, Microsoft, Apple, Tesla, Meta, Wayfair, Siemens, etc.) via auto-detected session strategies (cookie_only, csrf_token, bearer_token, graphql, url_session) |
| `jobs/job_filter.py` | Filter + score jobs, content hash generation (includes job_id) |
| `jobs/job_monitor.py` | Daily job monitoring + digest email + URL presence tracking |
| `jobs/fill_verifier.py` | Nightly filled position verification via HTTP |
| `db/db.py` | Single source of truth — all database operations |
| `db/quota.py` | Gemini daily + RPM quota tracking (in-memory sliding window) |
| `db/quota_manager.py` | Thin wrapper for Gemini quota functions |
| `db/job_cache.py` | Thin wrapper for job cache functions |
| `db/job_monitor.py` | DB ops for job_postings, monitor_stats, verify_filled_stats |
| `pipeline.py` | Orchestrator with CLI flags |
| `config.py` | All configuration in one place |
| `workers/watchdog.py` | Pipeline health monitor and auto-healer. Runs continuously. Checks worker heartbeats, queue depths, systemd service states, stuck jobs, bloom filter presence, and Redis persistence every 5 minutes. Sends alert emails and restarts services automatically before escalating to a human. |
| `workers/startup.py` | Startup validator run by every worker before its main loop begins. Checks that Redis is reachable, PostgreSQL is reachable, and all required `.env` keys are present. Exits immediately with a clear error message if anything is missing — prevents silent failures. |
| `scripts/health_check.py` | Instant CLI status tool. Prints a color-coded table of all components (services, workers, queues, Redis). Exit code 0 = healthy or warnings-only; exit code 1 = at least one ERROR or CRITICAL issue found. Run any time for a quick system snapshot. |
| `scripts/startup_failure_alert.py` | Email alert triggered automatically by systemd when a service crashes too many times in a short window (5 crashes in 5 minutes). Embeds the last 30 journal log lines in the email so you can diagnose without SSH. |
| `deploy/deploy.sh` | Code deployment script. Run after every `git push`: pulls latest code, installs new dependencies, restarts both services, waits for heartbeats, and runs `health_check.py` to confirm everything is healthy. |
| `deploy/install-systemd.sh` | One-time server setup script (run with `sudo`). Installs and enables the systemd unit files, removes old cron/nohup processes, secures `.env` permissions, and adds the sudoers rule for watchdog self-healing. |
| `deploy/configure-redis.sh` | One-time Redis AOF persistence setup (run with `sudo`). Switches Redis from saving every 5 minutes to saving every 1 second, reducing the data loss window from ~5 minutes to ~1 second. |

---

## Thundering Herd Prevention

Phase 2 solves a structural scheduling problem: when many companies are onboarded at the same time (or after a long outage), they all get the same `next_full_scan_at` timestamp. Every fullscan fires at once, saturating the worker, and most companies miss the 7 AM digest window. The fix is a two-layer approach — one layer at startup, one layer on every reschedule.

---

### The root cause

Imagine 139 companies all imported on a Monday. Without spreading, `rebuild_redis()` gives them all `next_poll_at = now`, and they all fire at midnight on Tuesday. The fullscan worker processes them sequentially — one Workday scan takes 20–30 minutes, so 139 companies would take 46–70 hours. All 139 miss the 7 AM digest.

Even with hash-based slot offsets at import time, companies drift back toward clusters over time if every reschedule simply uses `now + interval` — two companies that complete scans 10 seconds apart land 10 seconds apart forever.

---

### Layer 1 — `rebuild_redis()` at startup

Every time the scheduler starts, `rebuild_redis()` classifies each company by comparing `next_poll_at` against the last 7 AM cycle boundary:

| Company state | What happens |
|---|---|
| **CURRENT** — `next_poll_at` is within today's cycle (≥ last 7 AM) | DB timestamp restored directly. Even distribution is preserved. |
| **STALE** — `next_poll_at` is before last 7 AM (long outage or fresh deploy) | Spread evenly across a recovery window proportional to average scan time × company count. Thundering herd broken on first restart. |
| **NEW** — `next_poll_at` is NULL | Full scan first; then `slot_offset(company_id)` schedules first adaptive poll at a deterministic daily slot. |

This prevents clustering on restarts. But it only runs once — clusters can re-form during continuous operation if the rescheduling algorithm itself doesn't enforce spread.

---

### Layer 2 — `_pick_schedule_time()` on every reschedule

Every time a company is rescheduled — after an adaptive scan (`_reschedule_adaptive()`) or after a full scan (`_run_fullscan()`) — the new time is chosen by the gap-detection algorithm instead of a fixed `now + interval` or random jitter.

**Algorithm:**

```text
window_s = interval_s × tolerance_pct     # e.g. 20% of 86400 s = 17,280 s = 4.8 h
lo       = target_ts − window_s / 2
hi       = target_ts + window_s / 2

existing = ZRANGEBYSCORE queue lo hi       # all companies already scheduled in window

points = sorted([lo] + existing_scores + [hi])
gaps   = [(size, gap_lo, gap_hi) for each consecutive pair of points]

sort gaps by size descending (tiebreaker: midpoint closest to target_ts)
return midpoint of the largest gap
```

**Why gaps instead of slots:**

The old `_least_loaded_slot()` divided the window into 20 fixed sub-windows and counted companies per sub-window. Two companies could still land at the same slot centre. Slot boundaries created periodic clustering at multiples of `window_s / 20`.

The gap algorithm works at arbitrary resolution. If 50 companies are already scheduled in the window, it finds the actual largest empty interval between them — not "the least-loaded bucket". It guarantees the maximum possible distance from the nearest neighbour regardless of fleet size.

**Self-correction from full clustering:**

```text
Day 1: 139 companies all at T  →  all gaps = window_s / 139 ≈ 125 s
       Each gets a different midpoint → 139 evenly spaced times
Day 2: window now has 139 points spread evenly → gaps are equal → any midpoint is fine
Day 3+: stable, maximum-spread distribution maintained automatically
```

**Tolerances:**

| Queue | `tolerance_pct` | Total window | Deadline check |
|---|---|---|---|
| `poll:adaptive` | 0.20 | 20% of interval (e.g. 2.4 h on a 12 h poll) | None — adaptive scans are fast |
| `poll:fullscan` | 0.20 | 20% of 24 h = 4.8 h | Yes — see below |

---

### Digest deadline guard — `avg_fullscan_duration_s` EMA

A Workday scan can take 20–30 minutes. If a company is scheduled at 6:45 AM ET, it will not finish before the 7 AM digest fires. The gap algorithm is extended with a deadline constraint for fullscan scheduling:

> Skip any gap midpoint where `midpoint + avg_fullscan_duration_s ≥ next_7am_deadline`.

`_next_digest_deadline(now)` computes the next 7 AM ET timestamp. Gaps that would cause the scan to run past that boundary are skipped; the algorithm moves to the next-largest gap.

`avg_fullscan_duration_s` is a per-company EMA stored in `company_poll_stats`:

```text
new_avg = 0.3 × last_duration_s + 0.7 × prev_avg
```

Initial value: 30.0 s (conservative — unknown companies get a safe default until their first scan completes). Updated in `_complete_fullscan_db()` after every successful scan.

**Important:** `_pick_schedule_time()` receives the **updated** EMA (computed inline after the scan's `duration_s` is measured, using the same α=0.3 formula) rather than the stale pre-scan value loaded from `fs_state`. This ensures the deadline guard reflects the scan that just completed — if a scan unexpectedly took 4 h, the next scheduling decision uses a 4 h-weighted EMA rather than the old 30 s average, preventing a digest collision on the next cycle.

**DB columns added (idempotent `ADD COLUMN IF NOT EXISTS` in `init_db()`):**

```sql
last_fullscan_duration_s  INTEGER                       -- seconds of last scan
avg_fullscan_duration_s   DOUBLE PRECISION DEFAULT 30.0 -- EMA of all past scans
```

---

### `inflight:fullscan` ZSET — mid-scan protection for `--monitor-jobs`

When `--monitor-jobs` runs at 7 AM, it calls `_get_worker_missed_companies()` to identify companies the fullscan worker did not cover. Without inflight tracking, a company 20 minutes into a 30-minute Workday scan would appear "missed" — its `last_full_scan_at` is from the previous day. `--monitor-jobs` would launch a fallback HTTP re-fetch for it, doing redundant work and potentially pushing duplicate jobs to the detail queue.

The `inflight:fullscan` ZSET (score = scan start timestamp) tracks every company currently being scanned:

- Written: `ZADD inflight:fullscan {company: start_ts}` at scan start, after lock acquisition
- Removed: `ZREM inflight:fullscan company` in the `finally` block (runs on clean exit or Python-raised error; SIGKILL bypasses `finally` — stale entries are handled by the 2-hour staleness window below)
- Read: `_get_worker_missed_companies()` reads the ZSET with a **2-hour staleness window** — only entries with `score ≥ now − 7200` are considered. Entries older than 2 h come from workers that were killed without cleanup and must not permanently exclude companies from the missed-jobs check.

If Redis is unavailable when `_get_worker_missed_companies()` runs, the inflight exclusion is skipped (the function proceeds without it — conservative: may do extra HTTP work, but no data is lost).

---

### Summary — what prevents clusters at each stage

| When | Mechanism | What it prevents |
|---|---|---|
| Server startup / restart | `rebuild_redis()` — STALE spread | All companies firing at once after long outage |
| New company onboarded | `slot_offset(company_id)` — deterministic hash | Batch imports landing at the same time |
| Every adaptive reschedule | `_pick_schedule_time()` — largest gap | Drift back toward clusters over time |
| Every fullscan reschedule | `_pick_schedule_time()` + deadline guard | Clusters AND late-scheduled scans missing 7 AM digest |
| `--monitor-jobs` fallback | `inflight:fullscan` ZSET exclusion | Redundant HTTP fetches for in-progress scans |

---

## Reliability & Observability Layer

Phase 3 adds a self-managing safety net so the pipeline can recover from failures automatically and alert a human only when it cannot fix something on its own.

### The two systemd services

The OS service manager (systemd) keeps two processes running at all times, even across server reboots:

**`recruiter-scheduler.service`** manages the scheduler process. Because the scheduler spawns all scan, detail, and fullscan workers as child processes, a single restart (taking about 30 seconds) brings the entire worker pool back up.

**`recruiter-watchdog.service`** manages the watchdog monitor. If the watchdog itself crashes, systemd restarts it within 10 seconds.

A third unit, **`recruiter-pipeline-alert@.service`**, is a one-shot template triggered by systemd's `OnFailure=` mechanism. It fires when a service crashes 5 times within 5 minutes (indicating a persistent startup problem that systemd can no longer auto-recover), and sends an email containing the last 30 journal log lines to help diagnose without SSH.

### The watchdog — continuous health monitoring and self-healing

Think of the watchdog as a dedicated operations person who checks the entire system every 5 minutes, tries to fix problems immediately, and only calls you when they cannot fix it themselves.

**What it checks every 5 minutes — complete walkthrough:**

---

## 1 · systemd service states

**Detection:**

The watchdog calls `systemctl is-active recruiter-scheduler` (and `recruiter-watchdog`) as a subprocess and reads the single-word output. The possible states are:

- `active` → healthy, nothing to do
- `activating` → starting up, skip this cycle
- `failed` → the service crashed 5 times within 5 minutes. systemd's `StartLimitBurst` protection kicked in and stopped retrying. The service is now frozen — **it will not restart on its own**
- `inactive` / anything else → the service is stopped or was never started

**Why this check exists separately from heartbeats:**

A worker heartbeat key in Redis has a TTL of 15–45 seconds. If the scheduler just crashed, that key is still alive in Redis for up to 45 more seconds — during that window the heartbeat check says "healthy" while the process is actually dead. `systemctl is-active` reads the true OS-level state immediately, before the heartbeat key has had a chance to expire. It is a faster and more authoritative signal.

**Why `failed` is different from `inactive`:**

`failed` is a terminal state. systemd does not attempt any more restarts. And critically, `systemctl restart` is **silently blocked** when a service is in `failed` state — it does nothing without an error message. You must first call `systemctl reset-failed` to clear the failure counter before a restart is possible.

**Resolution:**

```bash
sudo systemctl reset-failed recruiter-scheduler
sudo systemctl restart recruiter-scheduler
```

The watchdog runs both commands atomically in one `bash -c` call. `reset-failed` is always safe to run — if the service was not in `failed` state it is a no-op — so the watchdog doesn't need to branch on `failed` vs `inactive`. One command handles both cases.

**The watchdog's own blind spot:**

The watchdog cannot restart itself via this mechanism. If `recruiter-watchdog.service` enters `failed` state, there is no running watchdog to detect it. This is handled by a separate `OnFailure=` hook in the unit file:

```ini
OnFailure=recruiter-pipeline-alert@%n.service
```

When the watchdog crashes too many times, systemd fires `recruiter-pipeline-alert@.service` — a one-shot unit that runs `startup_failure_alert.py` and emails the last 30 journal lines so you can diagnose without SSH.

**If 3 heals fail:** Escalation email sent; auto-heal paused for 24 hours.

---

## 2 · Worker heartbeats

**Multi-worker per-PID key architecture:**

The scheduler runs **multiple workers per type** (e.g. 3 scan workers, 4 detail workers, 3 fullscan workers — counts calculated dynamically from 30-day API health history). Because multiple workers of the same type run simultaneously, a single shared key per type would collapse all of them into one — the last writer would overwrite everyone else's heartbeat, making it impossible to track individual workers.

Each worker writes its own key including its PID:

```python
r.set(f"worker:alive:{worker_type}:{_HOSTNAME}:{os.getpid()}", json.dumps({
    "pid": os.getpid(), "ts": time.time(), "processed": count
}), ex=30)   # TTL = 30 seconds (3× the 10s write interval)
```

Write intervals, TTLs, and dead thresholds per worker type:

| Worker | Write interval | TTL | Dead after |
|---|---|---|---|
| `scheduler` | ~1 s (every loop tick) | 15 s | 20 s |
| `scan_worker` | 10 s | 30 s | 45 s |
| `detail_worker` | 10 s | 30 s | 45 s |
| `fullscan_worker` | 60 s | 180 s | 1,900 s (≈31 min) |

TTL = 3× the write interval, so two consecutive missed writes are tolerated (Redis blip, GIL stall) before the key disappears.

> **fullscan_worker note:** TTL (180 s) and Dead after (1,900 s) are intentionally different. The heartbeat daemon thread writes every 60 s so the key stays alive as long as the process runs. The 1,900 s "Dead after" threshold checks the embedded `ts` field — if the timestamp is more than 31 minutes stale while the key is still present, the process is likely hung (e.g. stuck waiting on a network response). This is separate from the TTL expiry that fires ~3 minutes after the process actually dies.

**Why a daemon thread — not a loop-top write:**

Earlier versions wrote the heartbeat at the top of the main loop. A single Workday full scan taking 60–90 seconds meant the key (TTL=30 s) expired mid-scan — the watchdog falsely declared the worker dead. Moving the write into a daemon thread fixes this: the thread writes continuously regardless of how long the current job takes.

Daemon threads are hard-tied to their process. When the process exits for any reason — clean shutdown, crash, SIGKILL — the OS terminates all daemon threads immediately. The key's TTL then runs out naturally, and the watchdog correctly detects the dead worker. No ghost heartbeats from a dead process are possible.

**Two-layer watchdog detection:**

Because workers are child processes of the scheduler, the watchdog uses a two-layer approach rather than monitoring individual per-PID keys:

**Layer 1 — `worker:alive:scheduler`** (fast, TTL=15s):
The scheduler writes this key every ~1 second. If missing or stale (age > 20s), the watchdog fires an ERROR immediately and returns early — all workers are presumed dead along with their parent. No point reading pool state when the scheduler is gone.

**Layer 2 — `scheduler:health`** (rich pool state, TTL=10min):
The scheduler publishes this JSON key on every pool event (worker death, respawn, scale up/down). It contains per-type alive counts and `consecutive_deaths` counters:

```json
{
  "ts": 1700000000.0,
  "pool": {
    "scan":     {"alive": 3, "consecutive_deaths": 1, "total_replacements": 4},
    "detail":   {"alive": 4, "consecutive_deaths": 0, "total_replacements": 1},
    "fullscan": {"alive": 3, "consecutive_deaths": 0, "total_replacements": 2}
  }
}
```

`consecutive_deaths` increments when a replacement worker dies within 60 seconds of being spawned (indicating a startup crash loop). It resets to 0 when a replacement survives ≥ 60 seconds (stable). The watchdog thresholds:

- `consecutive_deaths ≥ 3` → **WARNING** — scheduler struggling to keep workers up
- `consecutive_deaths ≥ 5` → **ERROR** — pool cannot stabilize; startup crash loop

The per-PID keys (`worker:alive:{type}:{hostname}:{pid}`) serve three purposes:
1. **Display** — `health_check.py` scans them to show which PIDs are currently alive.
2. **PEL consumer-liveness** — the watchdog's `_consumer_pid_alive()` uses them to determine whether a stream PEL entry belongs to a still-running consumer (decides orphan vs. in-progress).
3. **Stuck-job recovery** — `detail_worker._recover_stuck_jobs()` checks them to decide whether a peer's inflight list should be drained.

No pool-level alerting decisions (crash loops, worker counts) are made from individual per-PID keys — those come from `scheduler:health`.

**Responsibility split:**
- **Scheduler** — owns worker lifecycle (spawn / replace / scale). Publishes `scheduler:health` so the watchdog can see inside.
- **Watchdog** — monitors the scheduler and escalates when it cannot recover. Never tries to manage individual worker processes directly.

**Resolution:**

All workers (scan, detail, fullscan) are child processes spawned by the scheduler. They are not independent systemd units. The heal action for any dead worker — including the scheduler itself — is always the same:

```bash
sudo systemctl reset-failed recruiter-scheduler
sudo systemctl restart recruiter-scheduler
```

Restarting the scheduler recreates the entire managed worker pool. Spawning an individual worker directly while the scheduler is alive would create an unmanaged orphan — the scheduler would not know about it and would spawn a duplicate on its next liveness check. The watchdog handles this correctly: if both the scheduler and a worker are dead in the same cycle, it heals the scheduler and skips the individual worker heal (the revived scheduler spawns its own pool).

---

## 3 · Queue depths

The scheduler uses two types of queues:

- **Poll queues** (`poll:adaptive`, `poll:fullscan`) — Redis ZSETs where each company's score is the Unix timestamp of its next scheduled scan
- **Detail queues** (`queue:detail:adaptive`, `queue:detail:fullscan`) — Redis LISTs of job IDs waiting for a detail-page fetch

**Why not a simple overdue count or ratio?**

An absolute count ("more than 10 overdue") doesn't scale — 10 out of 139 companies is 7% (a problem); 10 out of 1,000 is 1% (normal noise). A percentage ratio scales with fleet size but still cannot answer the real question: **is the queue actually moving?**

A queue can have 50 companies overdue and be perfectly healthy (workers are processing flat-out, just have more work than one cycle can drain). Or it can have 3 companies overdue and be completely broken (workers are dead, nothing is being picked up). A snapshot — no matter what threshold — cannot tell these apart.

**How it detects — velocity tracking across watchdog cycles:**

The watchdog writes a state snapshot to Redis (`watchdog:queue_snapshot`, TTL = 10 min) at the end of every cycle. On the next cycle it compares current state to the snapshot using three independent signals:

| Signal | How it's read | Stalling when… |
|---|---|---|
| **Overdue count delta** | `ZCOUNT poll:X -inf now` vs previous cycle | Count not shrinking (stable or growing) and overdue > 0 |
| **Queue head** | `ZRANGE poll:X 0 0 WITHSCORES` — front-of-queue company + score | Same company AND same score as last cycle → nothing was picked up |
| **Worker processed count** | Sum of `processed` fields across all `worker:alive:{type}:{pid}` per-PID keys | Unchanged since last cycle → worker pool completed nothing |

**Stall verdict:**

| Signals stalling | out of valid | Verdict |
|---|---|---|
| 3 | 3 | **ERROR** — all signals agree: nothing moved → auto-restart scheduler |
| 2 | ≥2 | **WARNING** — likely stalling, watch next cycle |
| 0–1 | any | **OK** — queue is making progress, even if running behind |

Requiring multiple signals prevents false alarms from natural variance — e.g. a brief Redis blip that stalls one signal while the others show movement.

**Fullscan exoneration — the lock signal:**

A single Workday full scan can legitimately run for 20–30 minutes. Between two 5-minute watchdog cycles, the fullscan_worker's processed count won't change and the queue head won't move — yet the worker is perfectly healthy. Without an additional signal, this looks like a stall.

The `fullscan:lock:{company}` key is written at the start of every full scan and cleared when it completes (or on crash). The watchdog checks for any `fullscan:lock:*` key via `SCAN`. If a lock is active, all stall signals for `poll:fullscan` are suppressed for that cycle — the worker is provably mid-scan.

**Empty queue — handled separately:**

`ZCARD = 0` means no companies are scheduled at all — Redis was wiped or the scheduler never ran. This fires an ERROR and triggers `--rebuild` immediately, **except** for `poll:fullscan`: an empty fullscan queue is expected right after a rebuild (companies haven't been rescheduled yet) so it starts as WARNING and only escalates to ERROR if the queue remains empty beyond the expected rebuild window.

**Detail queues — different metric, same principle:**

Detail queues use `LLEN` delta (depth growing, shrinking, or stalled at a level) combined with the detail worker's processed count delta. The absolute depth still matters as a severity indicator (>100 → WARNING, >500 → ERROR) because detail queue depth is a **throughput metric**, not a fleet-size metric — 500 backed-up jobs represents the same processing lag regardless of how many companies are registered. But whether to alert depends on the direction of change, not just the number.

| Queue | Empty | Stall / not draining | Auto-heal |
|---|---|---|---|
| `poll:adaptive` | ERROR → `--rebuild` | ERROR → restart scheduler | ✅ Yes |
| `poll:fullscan` | WARNING (normal right after rebuild) | ERROR → restart scheduler | ✅ Yes |
| `queue:detail:*` | OK (idle) | ERROR if depth >500 and not draining | ❌ Alert only |

**Snapshot TTL:**

The snapshot expires after 2× the watchdog interval (10 minutes). If the watchdog was stopped and restarted, or skipped a cycle, the snapshot is gone. The first run after the gap is treated as a baseline cycle — current state is recorded, no alarms fire. This prevents stale comparisons producing false positives after a restart.

---

## 4 · Stuck jobs (Stream PEL age)

**What the PEL is and which workers use it:**

The pipeline uses Redis Streams for crash-safe job delivery to `scan_worker` and `fullscan_worker`. When a worker reads a job via `XREADGROUP`, Redis does two things simultaneously: delivers the message to the worker, and moves it into the **PEL** (Pending Entry List) — a per-consumer ledger of "claimed but not yet acknowledged" messages. The job stays in the PEL until the worker calls `XACK` after completing it.

If a worker crashes between `XREADGROUP` and `XACK`, the job is left orphaned in the PEL indefinitely — no other worker picks it up automatically because Redis considers it "in progress."

> **`detail_worker` is not covered here.** It uses Redis LISTs (`queue:detail:adaptive`, `queue:detail:fullscan`) with a separate per-PID at-least-once mechanism: `LMOVE` atomically transfers a job into this worker's own inflight list (`queue:detail:*:inflight:{pid}`) before processing, and `LREM` removes it only after the DB write succeeds. On startup, `_recover_stuck_jobs()` drains inflight lists from dead workers (confirmed via heartbeat absence) — never touching live peers' lists. No PEL involved.

**Detection — consumer liveness, not just time:**

The watchdog calls:

```python
r.xpending(stream_key, STREAM_CONSUMER_GROUP)           # total pending count
r.xpending_range(stream_key, group, "-", "+", count=1)  # oldest entry details
```

`xpending_range` returns the consumer name for each entry — e.g. `worker-myhost-18432`. This name embeds the worker's PID at launch time. The watchdog checks the **specific** per-PID heartbeat key `worker:alive:{type}:18432` directly via `EXISTS`, rather than a shared single-type key. This gives an unambiguous answer even when multiple workers of the same type are running:

```text
Consumer name:     worker-myhost-18432
EXISTS worker:alive:scan_worker:18432  → 1 → worker is alive, job is in progress → OK
EXISTS worker:alive:scan_worker:18432  → 0 → worker 18432 is dead → entry is orphaned
```

With the old shared single-type key, if worker PID 19001 was running and had overwritten the `worker:alive:scan_worker` key, the watchdog would have incorrectly concluded PID 18432 was alive because a *different* worker of the same type was alive. Per-PID keys eliminate this cross-worker false negative entirely.

**Why not just use time thresholds?**

A fullscan legitimately runs for 20–30 minutes. A time-only threshold (the old approach: >10 min = WARNING, >30 min = ERROR) fires constantly on a healthy `fullscan_worker` mid-scan. The PID comparison is the precise signal: if the owning worker is alive, the entry is not stuck — it is actively being worked on, regardless of how long it has been running.

**Time thresholds still apply — but only for orphaned entries:**

Once the consumer is confirmed dead, time becomes meaningful:

- Orphaned entry **<10 min** → OK — `XAUTOCLAIM` will reclaim it shortly (runs every ~1 second)
- Orphaned entry **>10 min** → WARNING — XAUTOCLAIM should have caught this by now
- Orphaned entry **>30 min** → ERROR — XAUTOCLAIM itself may be stuck

**Recovery:**

The scheduler's `claim_stale_work()` function calls `XAUTOCLAIM` on every tick. It finds PEL entries idle longer than `max(p95_scan_ms × 3, 300_000ms)` and transfers them to the next available worker automatically. No manual action is needed unless the ERROR threshold is hit — which means `XAUTOCLAIM` is stuck, almost always because the scheduler itself is dead. Restarting the scheduler (which the heartbeat check will already be triggering) resolves both problems simultaneously.

---

## 5 · Bloom filter presence

**What Bloom filters do:**

Every completed full scan builds a Redis Bloom filter (`bloom:fullscan:{company}`) containing all job IDs seen on that company's board. On the next full scan, each fetched job ID is checked against the filter before hitting PostgreSQL — if it is already in the filter, the DB check is skipped entirely. Without these filters, every full scan would need to compare tens of thousands of job IDs against the database on every cycle.

Three keys per company:
- `bloom:fullscan:{company}` — the authoritative filter from the last *completed* scan (read-only during the current scan)
- `bloom:fullscan:new:{company}` — being built during the current scan; promoted to the authoritative key on completion
- `bloom:fallback:{company}` — Redis SET fallback when RedisBloom module is unavailable (exact match, more memory)

**Detection:**

```python
cursor, keys = r.scan(cursor, match="bloom:fullscan:*", count=200)
bloom += len(keys)
# Also scans bloom:fallback:*
```

If the total count across both patterns is zero, all Bloom filter state is gone.

**What this means:**

Redis was wiped — either `FLUSHALL` was called, or Redis restarted with persistence disabled (no AOF, no RDB snapshot). All deduplication state is lost.

**Resolution:**

No auto-heal. This fires as a WARNING, not an ERROR — nothing is immediately broken. The next full scan per company runs as a cold start: it fetches all jobs and checks each against PostgreSQL. No duplicate rows are created because the DB has a `UNIQUE` constraint on job ID. The Bloom filters rebuild automatically as each scan completes. The cost is extra DB traffic for one full scan cycle per company.

The preventive fix is running `sudo bash deploy/configure-redis.sh` once, which enables AOF persistence (saves every ~1 second). After that, a Redis restart loses at most 1 second of state rather than losing everything.

---

## 6 · Company scan coverage

**Detection:**

Two queries run against PostgreSQL:

```sql
-- Companies that missed a full scan in the last 26 hours
SELECT COUNT(*) FROM company_poll_stats
WHERE last_full_scan_at IS NULL
   OR last_full_scan_at < NOW() - INTERVAL '26 hours';

-- Total registered companies
SELECT COUNT(*) FROM company_poll_stats;
```

If `missed / total > 25%` → ERROR.

The window is **26 hours** rather than 24 to give a 2-hour buffer for companies that legitimately finish their scan just before the 7 AM digest boundary. The threshold is **25%** rather than 0% because some minor misses are expected during ramp-up, brief ATS platform outages, or when a new company batch was just imported and hasn't had its first scan yet.

A second check looks for detail jobs that got stuck before being processed:

```sql
SELECT COUNT(*) FROM job_postings
WHERE status = 'pending_detail'
  AND created_at < NOW() - INTERVAL '1 hour';
```

More than 10 stuck rows → WARNING. These are jobs whose detail page was never fetched — scan_worker queued them but detail_worker never picked them up.

**Resolution:**

No auto-heal. This is an observational check, not a directly fixable state. The root causes and their own fixes:

- **Fullscan worker is dead** → the heartbeat check fires simultaneously and heals it. Coverage alert is a corroborating signal.
- **Worker is alive but throughput is too low** → `_pick_schedule_time()` (Phase 2, implemented) spreads companies automatically across the 24-hour window on every reschedule — a cluster resolves in 2–3 cycles without manual intervention.
- **One ATS platform is down** → the scheduler's platform outage detection handles this automatically (pauses dispatches for that platform).
- **Stuck `pending_detail` rows** → check that detail_worker heartbeat is alive. If dead, restarting the scheduler fixes it.

---

## 7 · Redis persistence

**Why this check exists:**

Redis is an in-memory database. Everything — poll queues, heartbeat keys, Bloom filters, stream entries, the watchdog snapshot — lives in RAM. If the Redis process crashes or the server loses power, RAM is gone. Without persistence, you lose everything.

**Detection:**

```python
info = r.info("persistence")
last_save = info.get("rdb_last_save_time", 0)  # Unix epoch of last successful RDB snapshot
age_minutes = (time.time() - last_save) / 60
```

> **Note:** `rdb_last_bgsave_time_sec` is the *duration* of the last bgsave in seconds (e.g. 3), not an epoch timestamp. Using it as an epoch would compute `time.time() - 3 ≈ 28 million minutes`, causing a permanent false WARNING. The correct field is `rdb_last_save_time`.

If `age_minutes > 30` → WARNING.

By default Redis takes a full RDB snapshot every 5 minutes. The watchdog fires at 30 minutes to catch a broken snapshot process (disk full, permissions issue) before the gap becomes catastrophic.

**Resolution:**

No auto-heal. Run this once:

```bash
sudo bash deploy/configure-redis.sh
```

This switches Redis from RDB snapshots to **AOF (Append-Only File)** mode:

- `appendonly yes` — every Redis write is appended to a log file on disk
- `appendfsync everysec` — the log is flushed to disk every 1 second
- Crash data-loss window shrinks from ~5 minutes (RDB) to ~1 second (AOF)

**AOF file size — automatic compaction:**

The AOF file grows as every operation is appended. Without compaction it would grow indefinitely — the heartbeat daemon alone writes ~1,440 entries per hour across 4 workers. Redis handles this via automatic background rewriting (`BGREWRITEAOF`):

Redis forks a background process that looks at the current in-memory state and writes the *minimal* set of commands needed to recreate it, discarding all intermediate history. Example: 1,440 heartbeat writes collapse to 4 lines (one current value per worker).

Two control knobs set by `configure-redis.sh`:

```text
auto-aof-rewrite-percentage 100   # rewrite when AOF doubles vs post-rewrite baseline
auto-aof-rewrite-min-size   64mb  # but not until the file is at least this large
```

Both conditions must be true simultaneously. The file oscillates between the post-rewrite baseline size and roughly 2× that size — it never grows unbounded. After running `configure-redis.sh`, the 30-minute RDB check is permanently green because AOF writes continuously.

---

**Escalation path:**

```text
Problem detected
  → Attempt auto-fix (if auto-healable)
  → Email: "⚠ Pipeline Issue — Auto-heal Attempted (1/3)"
  → Wait 5 minutes

  → Fixed? Email: "✅ Auto-healed"
  → Resolved on its own? Email: "✅ Resolved"
  → Still broken, attempt 2/3 → try again
  → Still broken, attempt 3/3 → try again
  → Still broken after 3 attempts?
    → Email: "🆘 ESCALATION — manual intervention required"
    → Auto-heal paused for 24 hours
```

Deduplication prevents repeated emails for the same issue: the same alert type is suppressed for 1 hour so you do not receive a flood of identical messages between healing attempts.

The watchdog can call `sudo systemctl restart` without a password because `install-systemd.sh` writes a narrowly-scoped sudoers rule that grants exactly the following commands — nothing else: `systemctl reset-failed recruiter-scheduler`, `systemctl restart recruiter-scheduler`, `systemctl restart recruiter-watchdog`, `systemctl is-active recruiter-scheduler`, `systemctl is-active recruiter-watchdog`, `systemctl daemon-reload`, and `sudo tee` writes to `/etc/systemd/system/recruiter-*.service` for deploy-time unit sync.

### Startup validation

Every worker runs `workers/startup.py` before entering its main loop. It checks:
- Redis is reachable and responding
- PostgreSQL is reachable
- All required `.env` configuration keys are present

If any check fails, the worker exits immediately with a clear, human-readable error message rather than silently hanging or producing cryptic errors minutes later.

### At-least-once detail queue

The detail queue (which holds job IDs waiting for full detail fetches) uses a "processing list" pattern rather than a simple pop.

**Per-PID inflight keys:** Each `detail_worker` process uses its own inflight list keyed by PID:
```text
queue:detail:adaptive:inflight:{pid}
queue:detail:fullscan:inflight:{pid}
```
`LMOVE` atomically transfers a job from the source queue into this worker's inflight list before any processing begins. The job is removed from the inflight list (`LREM`) only after the DB write succeeds.

**Crash-safe recovery:** On startup, `_recover_stuck_jobs()` scans for all `:inflight:*` keys, checks each peer's heartbeat (`worker:alive:detail_worker:{pid}`), and drains only the keys whose owner has no active heartbeat (confirmed dead). Keys belonging to live peers are never touched — this prevents a restarting worker from stealing an active peer's in-progress job.

**Atomic shutdown requeue:** When the scheduler sends a shutdown event mid-job, a single Lua script atomically removes the job from the inflight list and pushes it back to the front of the source queue (`LREM` + `LPUSH` in one Redis eval). No crash window exists between the two operations.

**Transient error handling:** `_process_detail()` returns a `retryable` flag. Unexpected errors (outer `except Exception`) set `retryable=True` — the item is left in the inflight list and recovered on next startup. Known-permanent outcomes (filtered, fetch failure where DB row was already cleaned up) set `retryable=False` and the item is acknowledged immediately.

In plain terms: if a worker crashes mid-processing, the job is not lost. On the next worker startup, orphaned inflight items from confirmed-dead workers are requeued automatically. A job may be processed more than once in a crash scenario (hence "at-least-once") but it will never be silently dropped.

### Redis AOF persistence

By default, Redis saves its in-memory data to disk every 5 minutes (RDB snapshots). With AOF (Append-Only File) mode enabled by `deploy/configure-redis.sh`, Redis writes every operation to disk within 1 second.

In plain terms: if Redis crashes, the maximum data you can lose shrinks from "up to 5 minutes of scan results and queue state" to "at most 1 second". This is especially important for the bloom filter and queue depths, which are expensive to rebuild.

---

## Error Handling and Fallbacks

**Job description scraping fails:**
```
JD scraped successfully    → generate_all_content(company, title, job_text)
JD scraping fails          → generate_all_content_without_jd(company, title)
AI quota exhausted         → skip, retry tomorrow
```

**CareerShift search fails:**
```
Pass 1: HR title + RequireEmail + exclude senior titles  → ideal
Pass 2: HR title + RequireEmail + include senior titles  → fallback
Pass 3: No filters + exclude senior titles               → last resort
```

**Email sending fails:**
```
Hard bounce (SMTPRecipientsRefused) → mark recruiter inactive, cancel outreach
Other failure                       → mark outreach failed, retry not automatic
Outside send window                 → reschedule for tomorrow
AI cache missing                    → warn and skip, run --find-only first
```

**Verify-filled HTTP request fails:**
```
404/410           → confirmed filled → mark status='filled', clear description
200               → still live → reset consecutive_missing_days to 0
Timeout           → inconclusive → logged as inconclusive_timeout, retry tomorrow
Connection error  → inconclusive → logged as inconclusive_conn_error, retry tomorrow
403/500           → inconclusive → logged as inconclusive_other_status, retry tomorrow
Exception         → inconclusive → logged as inconclusive_exception, check logs
```

**Gemini RPM limit hit:**
```
calls_in_last_60s >= RPM_LIMITS[model]
  → wait 60 seconds
  → retry same model
  → if still over limit → try next model
  → never silently drops content generation
```

---

## Key Design Decisions

**Single SQLite database** — All data lives in `data/recruiter_pipeline.db` with WAL mode enabled for concurrent reads/writes. Three separate databases (`job_cache.db`, `quota.db`, `recruiter_pipeline.db`) were consolidated into one.

**Company-level recruiters** — Recruiter contacts are stored at the company level, not per application. The `application_recruiters` join table handles the many-to-many relationship. This prevents duplicate recruiter rows when applying to multiple roles at the same company.

**Option B outreach scheduling** — Follow-up emails are scheduled only after the previous stage is sent with no reply. This prevents scheduling follow-ups for recruiters who already responded.

**Cached profiles are free** — CareerShift's daily limit (50 new contacts) only applies to first-time profile views. Re-visiting cached profiles is free, which is why tiered recruiter verification costs zero quota.

**AI content pre-generated** — Email content is generated during `--find-only` (night) not during `--outreach-only` (morning). This ensures the morning send window is purely a sending step with no external API dependencies.

**job_title passed from applications table to template engine** — `outreach_engine.py` reads `job_title` from the applications table row and passes it explicitly to `template_engine.py`. This ensures the correct AI cache key is built and avoids falling back to the generic default "Software Engineer" title which would cause a cache miss.

**Content hash includes job_id** — The deduplication hash is now `SHA256(company|title|location|job_id)` where `job_id` comes directly from the ATS API. This prevents false duplicate suppression when the same job title exists in multiple locations at the same company (e.g. Workday multi-location postings). Legacy hash (without job_id) is checked alongside the new hash during the rollout period.

**Digest accumulates until email confirmed sent** — `get_new_postings_for_digest()` returns all `status='new'` rows with no date filter. Only after the email is confirmed sent does `mark_postings_digested()` flip those rows to `status='digested'`. If the email fails to send, the jobs remain `new` and are automatically included in the next run — nothing is silently lost.

**verify-filled uses raw_jobs for URL tracking** — URL presence tracking during `--monitor-jobs` is built from `raw_jobs` (the full unfiltered API response) not `matched` (jobs that passed title/location filters). This prevents jobs that don't match your profile from being incorrectly counted as "missing" just because they were filtered out of your digest.

**Gemini RPM enforced via in-memory sliding window** — A 60-second sliding window in `db/quota.py` tracks call timestamps per model. When the RPM limit is hit, the pipeline waits 60 seconds and retries rather than silently switching to a lower-quality model. This makes the pipeline self-throttling without sacrificing email quality.

**Greenhouse now uses first_published** — Greenhouse's `updated_at` field changes whenever a job is edited (even minor updates), making it unreliable for freshness detection. The pipeline now uses `first_published` instead — the date the job was originally posted, which never changes.

**Greenhouse embed URL format handled** — Companies like Databricks use the embed URL format (`job-boards.greenhouse.io/embed/job_board?for=Databricks`) on their careers page instead of the standard format. The ATS URL pattern matcher now extracts the slug from the `?for=` query parameter, so detection works correctly for these companies too.