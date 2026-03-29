# Database

## Overview

All data lives in a single SQLite database at `data/recruiter_pipeline.db` with WAL (Write-Ahead Logging) mode enabled. WAL mode allows simultaneous reads and writes without blocking, which is important since you can add new applications while the pipeline is running.

---

## Tables

### `applications`
Jobs you applied to. This is the entry point for the entire pipeline.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `company` | TEXT | Company name |
| `job_url` | TEXT UNIQUE | Job posting URL |
| `job_title` | TEXT | Role title |
| `applied_date` | DATE | Date you applied |
| `status` | TEXT | `active` / `closed` / `exhausted` |
| `created_at` | TIMESTAMP | When added to DB |

**Retention:** Auto-closed after `APPLICATION_AUTO_CLOSE_DAYS` (60 days) from `applied_date`. Applications are never deleted — only their status changes to `closed`. This assumes no response within 60 days means the application is no longer active. Configurable in `config.py`.

---

### `recruiters`
Company-level recruiter contacts. One row per person, shared across all applications at the same company.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `company` | TEXT | Company name |
| `name` | TEXT | Recruiter full name |
| `position` | TEXT | Job title |
| `email` | TEXT UNIQUE | Email address |
| `confidence` | TEXT | `auto` / `manual_review` |
| `recruiter_status` | TEXT | `active` / `inactive` |
| `last_scraped_at` | TIMESTAMP | Last CareerShift scrape |
| `used_search_terms` | TEXT | JSON array of tried HR terms |
| `verified_at` | TIMESTAMP | Last verification timestamp |
| `created_at` | TIMESTAMP | When added to DB |

**Retention:** Permanent — never auto-deleted. Inactive recruiters are soft-deleted (`recruiter_status = inactive`) to preserve history and prevent re-scraping.

**Confidence levels:**
- `auto` — matched strong HR keywords (Recruiter, Talent Acquisition, HR Manager, etc.)
- `manual_review` — matched loose keywords or found via fallback search

---

### `application_recruiters`
Many-to-many join table linking recruiters to applications. Allows the same recruiter to be linked to multiple applications at the same company without duplicating recruiter data.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `application_id` | INTEGER FK | References `applications.id` |
| `recruiter_id` | INTEGER FK | References `recruiters.id` |
| `created_at` | TIMESTAMP | When linked |

**Cap:** At most `MAX_RECRUITERS_PER_APPLICATION` (3) recruiters can be linked per application. Enforced at DB level inside `link_recruiter_to_application()` — applies universally regardless of entry point (scraping, manual import, prospective conversion, sync form). Configurable in `config.py`.

**Retention:** Auto-deleted when the linked application is closed. Runs after `_cleanup_auto_close_applications` in `init_db()` so newly auto-closed applications are cleaned up in the same run.

---

### `outreach`
Email sequences per recruiter+application pair. Each stage gets its own row, building a complete audit trail.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `recruiter_id` | INTEGER FK | References `recruiters.id` |
| `application_id` | INTEGER FK | References `applications.id` |
| `stage` | TEXT | `initial` / `followup1` / `followup2` |
| `status` | TEXT | `pending` / `sent` / `failed` / `bounced` / `cancelled` |
| `replied` | INTEGER | `0` = no reply, `1` = replied |
| `scheduled_for` | DATE | When to send |
| `sent_at` | TIMESTAMP | When actually sent |
| `created_at` | TIMESTAMP | When row created |

**Retention:** Auto-deleted based on status (see Retention Policies below).

**Status lifecycle:**
```text
pending → sent       (email delivered successfully)
pending → failed     (send attempt failed)
pending → cancelled  (recruiter marked inactive before sending)
sent    → bounced    (hard bounce detected on delivery)
```

**Outreach sequence per recruiter+application:**
```text
Day 0:  initial   scheduled → sent → followup1 scheduled
Day 7:  followup1 scheduled → sent → followup2 scheduled
Day 14: followup2 scheduled → sent → sequence complete
```

---

### `ai_cache`
Generated email content per company+job title combination. Keyed by SHA256 hash of company+job_title+job_text (or company+job_title for fallback generation).

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `cache_key` | TEXT UNIQUE | SHA256 hash |
| `company` | TEXT | Company name |
| `job_title` | TEXT | Role title |
| `subject_initial` | TEXT | Initial email subject |
| `subject_followup1` | TEXT | Follow-up 1 subject |
| `subject_followup2` | TEXT | Follow-up 2 subject |
| `intro` | TEXT | Initial email body |
| `followup1` | TEXT | Follow-up 1 body |
| `followup2` | TEXT | Follow-up 2 body |
| `created_at` | TIMESTAMP | When generated |
| `expires_at` | TIMESTAMP | When to expire |

**Retention:** Auto-deleted when `expires_at <= now`. TTL = 21 days (covers full outreach cycle of 3 emails × 7 days).

---

### `jobs`
Cached job descriptions scraped from job posting URLs. Content stored compressed with zlib.

| Column | Type | Description |
|---|---|---|
| `url_hash` | TEXT PK | SHA256 hash of URL |
| `job_url` | TEXT | Original URL |
| `content` | BLOB | zlib-compressed job description |
| `created_at` | INTEGER | Unix timestamp |

**Retention:** Auto-deleted after 21 days from `created_at`.

---

### `model_usage`
Tracks daily Gemini API call counts per model. Used to enforce daily limits locally before hitting the API. Works in combination with an in-memory RPM (requests-per-minute) sliding window to enforce both daily and per-minute limits simultaneously.

| Column | Type | Description |
|---|---|---|
| `model` | TEXT | Model name |
| `date` | TEXT | YYYY-MM-DD |
| `count` | INTEGER | Calls made today |

**Primary key:** `(model, date)`

**Retention:** Auto-deleted after 21 days.

**How daily + RPM limits work together:**
Before every Gemini API call, the pipeline checks two things:
1. **Daily limit** — reads `count` from this table. If today's count >= `DAILY_LIMITS[model]`, the model is skipped.
2. **RPM limit** — checks an in-memory sliding window (last 60 seconds). If the number of calls in the last 60 seconds >= `RPM_LIMITS[model]`, the pipeline waits 60 seconds before retrying rather than silently skipping to a worse model.

Both checks must pass before any API call is made. Current limits enforced:
```text
gemini-2.5-flash-lite:  20 calls/day,  10 calls/minute
gemini-2.5-flash:       20 calls/day,   5 calls/minute
```

The RPM window is in-memory and resets on process restart — acceptable since RPM windows are 60 seconds, much shorter than any nightly run.

---

### `careershift_quota`
Tracks daily CareerShift profile view usage. Synced with real value from CareerShift account page at start of each `--find-only` run.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `date` | DATE UNIQUE | YYYY-MM-DD |
| `total_limit` | INTEGER | Daily limit (50) |
| `used` | INTEGER | Profile views used |
| `remaining` | INTEGER | Profile views left |

**Retention:** Auto-deleted after 30 days.

---

### `quota_alerts`
Records quota health alerts sent by email. Prevents duplicate alerts from being sent repeatedly.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `alert_type` | TEXT | `underutilized` / `exhausted` |
| `quota_type` | TEXT | `careershift` / `gemini` |
| `start_date` | DATE | First day of streak |
| `end_date` | DATE | Third day (trigger date) |
| `avg_used` | REAL | Average daily usage over streak |
| `avg_remaining` | REAL | Average daily remaining over streak |
| `suggested_cap` | INTEGER | Auto-calculated suggested MAX_CONTACTS_HARD_CAP |
| `notified` | INTEGER | `0` = pending, `1` = email sent |
| `created_at` | TIMESTAMP | When alert was created |

**Retention:** Auto-deleted after 30 days.

---

### `prospective_companies`
Target companies to monitor for job postings and pre-scrape recruiters for. Populated via `--import-prospects prospects.txt`.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `company` | TEXT UNIQUE | Company name |
| `domain` | TEXT | Company domain (e.g. `capitalone.com`) used for domain validation during scraping and Phase 3a HTML redirect scan |
| `priority` | INTEGER | Higher = scraped first (default 0) |
| `status` | TEXT | `pending` / `scraped` / `converted` / `exhausted` |
| `ats_platform` | TEXT | Detected ATS: `greenhouse` / `lever` / `ashby` / `smartrecruiters` / `workday` / `oracle_hcm` / `custom` / `unknown` / `unsupported` |
| `ats_slug` | TEXT | ATS slug or JSON (e.g. `{"slug":"capitalone","wd":"wd12","path":"Capital_One"}`) |
| `ats_detected_at` | TIMESTAMP | When ATS was last detected |
| `first_scanned_at` | TIMESTAMP | When first `--monitor-jobs` scan completed |
| `last_checked_at` | TIMESTAMP | When last checked by `--monitor-jobs` |
| `consecutive_empty_days` | INTEGER | Days with 0 jobs returned (triggers re-detection at 14) |
| `scraped_at` | TIMESTAMP | When CareerShift scrape completed |
| `converted_at` | TIMESTAMP | When converted to active application |
| `created_at` | TIMESTAMP | When added to DB |

**Status lifecycle:**
```text
pending   → scraped    (recruiters found via CareerShift)
pending   → exhausted  (CareerShift found no recruiters)
scraped   → converted  (--add command used for this company)
```

**ATS detection phases:**
```text
Phase 1: slug probe → top 3 slug variants against ATS APIs (fast, free)
Phase 2: api      → boards-api.greenhouse.io/v1/boards/{slug} (free)
Phase 3a: html    → company.com/careers redirect scan (free)
Phase 3b: serper  → Google search via Serper API (2 credits)
manual   → --override flag (never auto-changed)
custom   → KNOWN_CUSTOM_ATS list (Amazon/Apple/Google etc.)
```

**Retention:** Permanent — never auto-deleted.

---

### `job_postings`
Job postings discovered by `--monitor-jobs`. Only new postings appear in daily PDF digest.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `company` | TEXT | Company name |
| `title` | TEXT | Job title |
| `job_url` | TEXT UNIQUE | Job posting URL |
| `content_hash` | TEXT | SHA256 of company+title+location+job_id (dedup key) |
| `location` | TEXT | Job location |
| `posted_at` | TIMESTAMP | Original posting date (from ATS API) |
| `description` | TEXT | Job description (cleared on expiry or when filled) |
| `skill_score` | INTEGER | Relevance score (0-100+) |
| `status` | TEXT | `new` / `pre_existing` / `digested` / `expired` / `dismissed` / `applied` / `filled` |
| `first_seen` | DATE NOT NULL | Date first detected by pipeline |
| `consecutive_missing_days` | INTEGER | Days URL has been absent from API scan |
| `stale_since` | DATE | Date when job first went missing from API scan |
| `created_at` | TIMESTAMP | When inserted |

**Status lifecycle:**
```text
new          → digested    (auto: after digest email sent successfully)
new          → expired     (auto: after 7 days, description cleared)
new          → dismissed   (manual: user dismissed from digest)
new          → applied     (auto: when added via --add)
digested     → expired     (auto: after 7 days from first_seen, description cleared)
pre_existing → (stays)     (first scan or stale date — not shown in digest)
any active   → filled      (auto: URL confirmed 404/gone via --verify-filled)
filled       → pre_existing (auto: URL reappears in API scan within 7-day window — reactivated)
filled       → DELETE       (auto: after VERIFY_FILLED_RETENTION days from stale_since)
```

**Understanding `digested` vs `new`:**
The pipeline accumulates all `status='new'` rows into the digest email. Only after the email is confirmed sent does it flip those rows to `status='digested'`. This means if the email send fails (e.g. SMTP error), the jobs stay `new` and will automatically be included in the next run's digest — nothing is lost. Once `digested`, rows age out the same as `new` (7 days → expired).

**Understanding `filled` and reactivation:**
When `--verify-filled` confirms a job URL returns 404, the row is marked `filled` and its description is cleared. The URL is kept for 7 days (`VERIFY_FILLED_RETENTION`) in case the job reappears — if it does within that window, the row is automatically reactivated to `pre_existing` with all counters reset. After 7 days as `filled` the row is deleted entirely, and if the same URL ever reappears after that it will be treated as a brand new posting and shown in the digest.

**Content hash format (updated):**
The content hash now includes the ATS job ID to prevent false duplicate matches. For example, Workday sometimes lists the same job title in multiple locations with identical `locationsText` but different internal job IDs — the old hash (company+title+location only) would incorrectly suppress the second posting. The new hash is:
```text
SHA256(company | title | location | job_id)
```
Where `job_id` comes directly from the ATS API response:
- Greenhouse, Lever, Ashby, SmartRecruiters: `job.get("id")`
- Oracle HCM: `job.get("Id")`
- Workday: extracted from URL suffix (`_R164560` or `_JR-0104946`)
- iCIMS: extracted from HTML href

For backwards compatibility during rollout, `job_hash_exists()` checks both the new hash AND the old legacy hash (without job_id) so existing DB rows are still matched correctly and no duplicates are created.

**`posted_at` reliability by platform (updated):**
```text
Greenhouse:      first_published  — RELIABLE ✓ (previously None — now fixed)
Lever:           createdAt        — RELIABLE ✓ (Unix ms timestamp)
Ashby:           publishedAt      — RELIABLE ✓
SmartRecruiters: releasedDate     — RELIABLE ✓
Workday:         postedOn         — RELIABLE ✓ (parsed from human-readable strings)
Oracle HCM:      PostedDate       — RELIABLE ✓
iCIMS:           HTML / JSON-LD   — RELIABLE ✓ (populated after fetch_job_detail call)
```

**Workday `postedOn` human-readable parsing:**
Workday returns date strings in plain English rather than ISO format. The parser handles all known formats. Critically, human-readable strings are checked BEFORE the ISO format check — this is important because the capital "T" in "Posted Today" would incorrectly trigger the ISO datetime parser if checked in the wrong order:
```text
"Posted Today"         → today's date
"Posted Yesterday"     → yesterday's date
"Posted 3 Days Ago"    → 3 days ago
"Posted 30+ Days Ago"  → 30 days ago (conservative estimate)
"MM/DD/YYYY"           → parsed as date
ISO format string      → parsed as datetime (checked last)
```

**Indexes:**
```text
UNIQUE idx_job_postings_hash      ON content_hash (WHERE NOT NULL)
       idx_job_postings_status_seen ON (status, first_seen)
```

**Retention:**
```text
new/digested  → expired after 7 days (description cleared, URL kept forever)
filled        → deleted after VERIFY_FILLED_RETENTION (7) days from stale_since
dismissed     → deleted after 30 days
applied       → deleted immediately (already in applications table)
expired       → kept forever (prevents re-showing same jobs)
```

---

### `monitor_stats`
Daily performance metrics for `--monitor-jobs` runs. Used to track pipeline health and detect issues.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `date` | DATE UNIQUE | YYYY-MM-DD |
| `companies_monitored` | INTEGER | Total companies scanned |
| `companies_with_results` | INTEGER | Companies returning ≥1 job |
| `companies_unknown_ats` | INTEGER | Companies with unknown ATS (skipped) |
| `api_failures` | INTEGER | Companies with API errors |
| `total_jobs_fetched` | INTEGER | Raw jobs fetched before filtering |
| `new_jobs_found` | INTEGER | Genuinely new jobs added to digest |
| `jobs_matched_filters` | INTEGER | Jobs passing title+location filters |
| `run_duration_seconds` | INTEGER | Total run time |
| `pdf_generated` | INTEGER | `1` = PDF sent, `0` = skipped |
| `email_sent` | INTEGER | `1` = email sent, `0` = failed |
| `created_at` | TIMESTAMP | When row created |

**Retention:** Auto-deleted after `RETENTION_MONITOR_STATS` (60 days). Configurable in `config.py`.

**Key metrics derived from this table:**
```text
Coverage rate:        companies_with_results / companies_monitored
Filter match rate:    jobs_matched_filters / total_jobs_fetched
Pipeline reliability: runs with pdf_generated=1 / total_runs (7 days)
```

---

### `verify_filled_stats`
Daily performance metrics for `--verify-filled` runs. Tracks how many stale job URLs were verified and what happened to each one. Useful for diagnosing whether filled position cleanup is working correctly and whether any ATS platforms are blocking verification requests.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `date` | DATE NOT NULL UNIQUE | YYYY-MM-DD |
| `verified` | INTEGER | Total jobs verified this run |
| `filled` | INTEGER | Confirmed gone (404 or terminal redirect) |
| `active` | INTEGER | Still live — counter reset (false positive) |
| `inconclusive` | INTEGER | Total inconclusive (all reasons combined) |
| `inconclusive_timeout` | INTEGER | HTTP request timed out |
| `inconclusive_conn_error` | INTEGER | Connection error (DNS failure, server down) |
| `inconclusive_other_status` | INTEGER | Unexpected HTTP status (e.g. 403, 500) |
| `inconclusive_exception` | INTEGER | Unexpected exception during request |
| `remaining` | INTEGER | Stale jobs not processed this run (backlog) |
| `run_duration_secs` | INTEGER | Total run time in seconds |
| `created_at` | TIMESTAMP | When row created |

**Retention:** Auto-deleted after `RETENTION_VERIFY_FILLED_STATS` (60 days). Configurable in `config.py`.

**Reading the inconclusive breakdown:**
- High `inconclusive_timeout` → ATS servers are slow or blocking direct requests with timeouts
- High `inconclusive_conn_error` → network issues on the VM, or ATS blocks by IP
- High `inconclusive_other_status` → common if ATS returns 403 (bot detection) or 500 (server error)
- High `inconclusive_exception` → unexpected code error, check logs for details
- High `remaining` → batch size (200) is too small for current stale job volume; increase `VERIFY_FILLED_BATCH_SIZE` in config.py

---

### `coverage_stats`
Daily performance metrics for the `--find-only` and `--outreach-only` pipelines. Tracks how effectively the recruiter sourcing pipeline is working — are we finding recruiters for the companies we apply to, and are those recruiters ready to be emailed?

Think of this as the "health dashboard" for the recruiter pipeline specifically, complementing `monitor_stats` which tracks the job monitoring pipeline.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `date` | DATE NOT NULL UNIQUE | YYYY-MM-DD |
| `total_applications` | INTEGER | Total active applications that day |
| `companies_attempted` | INTEGER | Companies where scraping was attempted (excludes already-stocked) |
| `auto_found` | INTEGER | Companies where recruiters were found with `auto` confidence |
| `rejected_count` | INTEGER | Companies where buffer was discarded (domain mismatch, low confidence) |
| `exhausted_count` | INTEGER | Companies marked `exhausted` (CareerShift has no data) |
| `metric1` | REAL | Find-only performance % — see below |
| `metric2` | REAL | Outreach coverage % — see below |
| `created_at` | TIMESTAMP | When row created |

**Metric 1 — Find-Only Pipeline Performance:**
```text
Formula: (auto_found / companies_attempted) * 100

Example:
  10 applications total → 4 already have recruiters → 6 attempted
  4 found with auto confidence
  Metric 1 = (4/6) * 100 = 66.7%

Thresholds:
  Green:  >= 70%   → healthy
  Yellow: 50-70%   → degrading, monitor
  Red:    < 50%    → alert fires after 3 consecutive days
```

**Metric 2 — Outreach Coverage Performance:**
```text
Formula: (companies_with_sendable_recruiters / total_applications) * 100

Example:
  10 total applications
  4 already had recruiters + 4 newly found = 8 ready for outreach
  Metric 2 = (8/10) * 100 = 80%

Thresholds:
  Green:  >= 75%   → healthy
  Yellow: 60-75%   → degrading, monitor
  Red:    < 60%    → alert fires after 3 consecutive days
```

**Alert behavior:**
When Metric 1 < 50% OR Metric 2 < 60% for 3 consecutive days, a `pipeline_alerts` row is created and an email alert is sent. Crucially, before exhausting an application when metrics are below threshold, the pipeline skips the exhaust and fires an alert instead — human intervention required. See `validation-and-metric.md` for full exhaust vs skip logic.

**Implementation status:** Schema created and deployed. Rows are persisted by `careershift/find_emails.py` during the `--find-only` run. The writer populates `metric1` and `metric2` at the end of each `--find-only` run.

**Retention:** Add `RETENTION_COVERAGE_STATS` to `config.py` and implement `_cleanup_coverage_stats()` function. Suggested value: 60 days (same as `monitor_stats`).

---

### `api_health`
Per-platform ATS API reliability metrics recorded during each `--monitor-jobs` run. Tracks request counts, success/failure rates, response times, and rate limiting behavior per platform per day. Designed to power Metric 6 (API Failure Rate) and surface degrading ATS APIs before they silently cause job postings to be missed.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `date` | DATE NOT NULL | YYYY-MM-DD |
| `platform` | TEXT NOT NULL | ATS platform name (`greenhouse` / `lever` / `ashby` etc.) |
| `requests_made` | INTEGER | Total API requests attempted |
| `requests_ok` | INTEGER | Responses with 200 status |
| `requests_429` | INTEGER | Rate limit responses received |
| `requests_404` | INTEGER | Not found responses (usually ATS detection misses) |
| `requests_error` | INTEGER | All other errors (timeout, connection error, malformed JSON) |
| `avg_response_ms` | INTEGER | Average response time in milliseconds |
| `max_response_ms` | INTEGER | Slowest response time in milliseconds |
| `total_ms` | INTEGER | Total time spent waiting on this platform |
| `first_429_at` | TIMESTAMP | When the first rate limit response occurred |
| `backoff_total_s` | INTEGER | Total seconds spent in backoff/retry waits |
| `created_at` | TIMESTAMP | When row created |

**UNIQUE constraint:** `(date, platform)` — one row per platform per day.

**Why this matters:**
```text
Without api_health, a platform degrading looks like this:
  Greenhouse: 40 companies → 0 new jobs found
  Cause: Greenhouse API returning 429 all morning
  What you see: empty digest
  What you think: no new jobs posted today
  Reality: you missed 40 companies worth of jobs

With api_health, the same event looks like this:
  Greenhouse: requests_made=40, requests_ok=0,
              requests_429=40, backoff_total_s=2400
  → Immediately visible in PDF digest health section
  → pipeline_alerts row created → email alert sent
```

**Derived metrics:**
```text
Success rate:      requests_ok / requests_made
Rate limit rate:   requests_429 / requests_made   (target < 5%)
Error rate:        requests_error / requests_made  (target < 10%)
Avg backoff/req:   backoff_total_s / requests_made
```

**Implementation status:** Schema created and ready. Writer not yet implemented — all columns are currently 0. Will be populated inside `jobs/job_monitor.py` as each ATS platform's API calls are instrumented.

**Retention:** No cleanup function yet — rows accumulate indefinitely. Add `RETENTION_API_HEALTH` to `config.py` and a `_cleanup_api_health()` function when the writer is implemented. Suggested retention: 60 days (same as `monitor_stats`).

---

### `pipeline_alerts`
Unified alert table for all pipeline-level threshold breaches. A more flexible replacement for the recruiter-specific `quota_alerts` table — covers job monitoring failures, ATS API degradation, recruiter pipeline performance drops, and any other configurable threshold.

Unlike `quota_alerts` which is specific to CareerShift/Gemini quota, `pipeline_alerts` is designed to be the single place where any automated alert is recorded before being emailed.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `alert_type` | TEXT NOT NULL | e.g. `metric1_low` / `metric2_low` / `api_failure_rate` / `coverage_drop` |
| `severity` | TEXT NOT NULL | `warning` / `critical` |
| `platform` | TEXT | ATS platform name if alert is platform-specific (NULL for pipeline-wide alerts) |
| `value` | REAL | The actual metric value that triggered the alert |
| `threshold` | REAL | The threshold that was breached |
| `message` | TEXT | Human-readable description of what triggered the alert |
| `notified` | INTEGER | `0` = alert created but email not yet sent, `1` = email sent |
| `notified_at` | TIMESTAMP | When email was sent |
| `created_at` | TIMESTAMP | When alert was created |

**Alert types planned:**
```text
metric1_low      → find-only performance < METRIC1_ALERT_THRESHOLD (50%) for 3 days
metric2_low      → outreach coverage < METRIC2_ALERT_THRESHOLD (60%) for 3 days
api_failure_rate → platform requests_error / requests_made > 10% for 3 days
api_rate_limited → platform requests_429 > 0 causing backoff > threshold
coverage_drop    → monitor_stats companies_with_results / companies_monitored < 70%
```

**Implementation status:** Schema created and deployed. Rows are created and read by `db/pipeline_alerts.py` and `pipeline.py`. Alerts are triggered by `--find-only` and `--monitor-jobs` when performance thresholds are breached.

**Retention:** Add `RETENTION_PIPELINE_ALERTS` to `config.py` and implement cleanup. Suggested retention: 30 days (same as `quota_alerts`).

---

### `serper_quota`
Tracks total Serper API credit usage. One row (id=1), never deleted.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Always 1 (single row) |
| `credits_used` | INTEGER | Total credits consumed so far |
| `credits_limit` | INTEGER | Total credits (default 2500) |
| `low_credit_alert_sent` | INTEGER | `0` = not sent, `1` = alert sent |
| `last_updated` | TIMESTAMP | When last incremented |

**Retention:** Permanent — single row, never deleted.

**Alert behavior:** When `credits_used` causes remaining to drop below
`SERPER_LOW_CREDIT_THRESHOLD` (50), a one-time email alert is sent and
`low_credit_alert_sent` is set to 1. Reset via `reset_low_credit_alert()`
after purchasing more credits.

---

## `ats_discovery.db` — ATS Slug Discovery Database

Separate SQLite database at `data/ats_discovery.db`. Completely independent from `recruiter_pipeline.db`. Safe to delete and rebuild at any time by re-running `build_ats_slug_list.py`.

---

### `ats_companies`
Master list of known ATS company slugs. Populated monthly via AWS Athena queries against the Common Crawl columnar index. Self-populates from successful pipeline detections.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incremented |
| `platform` | TEXT | ATS platform: `greenhouse` / `lever` / `ashby` / `workday` / `oracle_hcm` / `icims` |
| `slug` | TEXT | Company slug or JSON (Workday/Oracle) e.g. `stripe` or `{"slug":"capitalone","wd":"wd12","path":"Capital_One"}` |
| `company_name` | TEXT | Human-readable company name (from enrichment) |
| `website` | TEXT | Company website (from enrichment) |
| `job_count` | INTEGER | Open jobs at last enrichment |
| `crawl_source` | TEXT | First crawl that found this slug e.g. `CC-MAIN-2026-08` |
| `first_seen` | TIMESTAMP | When first inserted |
| `last_verified` | TIMESTAMP | When ATS API was last called for this slug |
| `last_seen_crawl` | TEXT | Most recent Common Crawl crawl containing this slug |
| `is_active` | INTEGER | `1` = active, `0` = inactive (404 on verify) |
| `is_enriched` | INTEGER | `0` = company_name not fetched yet, `1` = enriched |
| `source` | TEXT | `crawl` / `detection` / `manual` / `backfill` |
| `created_at` | TIMESTAMP | When row created |

**UNIQUE constraint:** `(platform, slug)`

**Source types:**
- `crawl` — discovered via Athena/Common Crawl. Subject to sliding window cleanup.
- `detection` — found by pipeline Phase 2/3 detection. Never auto-deleted.
- `manual` — manually added. Never auto-deleted.
- `backfill` — one-time historical import (e.g. Lever pre-2025-47). Never auto-deleted.

**Freshness strategy:**
- Monthly Athena query adds new slugs (INSERT OR IGNORE)
- Sliding window cleanup: slugs not seen in last 3 crawls → archived then deleted (source=crawl only)
- Before deletion: rows archived to `data/ats_archive.csv.gz`
- Phase 2 API probe catches stale slugs on cache hit (404 → `delete_company()`)

**Retention:** Sliding window for `source='crawl'`. Permanent for detection/manual/backfill.

---

### `scanned_crawls`
Tracks which Common Crawl crawls have already been processed by Athena. Prevents re-querying the same crawl on repeat runs. Normal monthly run = 1 new crawl = 1 Athena query.

| Column | Type | Description |
|---|---|---|
| `crawl_id` | TEXT PK | e.g. `CC-MAIN-2026-08` or `backfill-CC-MAIN-2025-43` |
| `scanned_at` | TIMESTAMP | When Athena query ran |
| `slugs_found` | INTEGER | Total slugs returned by query |
| `slugs_new` | INTEGER | Net new slugs inserted into DB |
| `query_type` | TEXT | `athena` / `backfill` |

**Retention:** Permanent — used to avoid re-querying Athena.

---

## `ats_discovery.db` Storage Estimates

```text
Table             Rows        Size
──────────────────────────────────────
ats_companies     ~43,000     ~8 MB
scanned_crawls    ~12/year    ~0.01 MB
──────────────────────────────────────
Total DB size     ~8 MB

data/ats_archive.csv.gz   ~800 KB/year (historical slugs)
data/athena_*.csv         ~10 MB (deleted after 2 days)
data/bing_quota.json      ~0.1 KB
data/collinfo_cache.json  ~5 KB (cached crawl list)
data/cdx_page_counts.json ~10 KB (deprecated — Athena replaces CDX)
```

---

## ATS Discovery Workflow

```text
Monthly (1st of month):
  python build_ats_slug_list.py
    → get_recent_crawls() → [CC-MAIN-2026-08, ...]
    → get_unscanned_crawls() → [CC-MAIN-2026-08]  ← only new
    → Athena query for new crawl (~$0.00024)
    → CSV saved locally → S3 result deleted immediately
    → slugs inserted into ats_companies
    → crawl marked in scanned_crawls
    → stale slugs archived → deleted from DB
    → Bing queries for Lever + Oracle fallback

  python enrich_ats_companies.py
    → ATS API call per unenriched slug
    → 200 → company_name, website, job_count saved
    → 404 → delete_company() (row deleted permanently)

Cost: ~$0.00024/month Athena + ~$0 S3 = ~$0.003/year

Recovery (if script crashed mid-insert):
  python build_ats_slug_list.py --from-csv data/athena_2026-03-09.csv
```

---

## Retention Policies

All retention values are configured in `config.py` and enforced at startup via `init_db()`.

| Table | Retention | Condition |
|---|---|---|
| `applications` | Never deleted | Status auto-set to `closed` after 60 days (`APPLICATION_AUTO_CLOSE_DAYS`) |
| `recruiters` | Permanent | Never deleted (soft delete only) |
| `application_recruiters` | Deleted when application closes | Cascades from `_cleanup_auto_close_applications` in same `init_db()` run |
| `prospective_companies` | Permanent | Never deleted |
| `monitor_stats` | 60 days | `date < now - 60 days` (`RETENTION_MONITOR_STATS`) |
| `verify_filled_stats` | 60 days | `date < now - 60 days` (`RETENTION_VERIFY_FILLED_STATS`) |
| `coverage_stats` | Not yet implemented | Will use `RETENTION_COVERAGE_STATS` when writer added |
| `api_health` | Not yet implemented | Will use `RETENTION_API_HEALTH` when writer added |
| `pipeline_alerts` | Not yet implemented | Will use `RETENTION_PIPELINE_ALERTS` when writer added |
| `job_postings` (expired URLs) | Permanent | URL kept to prevent re-showing |
| `outreach` (sent) | 30 days | `sent_at < now - 30 days` (`RETENTION_OUTREACH_SENT`) |
| `outreach` (pending) | 30 days | `scheduled_for < now - 30 days` |
| `outreach` (failed/bounced/cancelled) | 30 days | `created_at < now - 30 days` |
| `job_postings` (new/digested→expired) | 7 days | `first_seen < now - 7 days` (description cleared) |
| `job_postings` (filled→deleted) | 7 days | `stale_since < now - VERIFY_FILLED_RETENTION days` (row deleted entirely) |
| `job_postings` (dismissed) | 30 days | `first_seen < now - 30 days` |
| `ai_cache` | 21 days | `expires_at <= now` |
| `jobs` | 21 days | `created_at < now - 21 days` |
| `model_usage` | 21 days | `date < now - 21 days` |
| `careershift_quota` | 30 days | `date < now - 30 days` |
| `quota_alerts` | 30 days | `created_at < now - 30 days` |
| `serper_quota` | Permanent | Single row, never deleted |

**Cleanup execution order in `init_db()`:**
```text
1. _cleanup_auto_close_applications       ← mark applications closed first
2. _cleanup_closed_application_recruiters ← then clean up their recruiter links
3. _cleanup_monitor_stats                 ← independent, order doesn't matter
4. _cleanup_verify_filled_stats           ← independent, order doesn't matter
5. _cleanup_job_postings                  ← handles new→expired, digested→expired, filled→delete, dismissed→delete
6. (all other existing cleanup functions)
```

---

## Relationships

```text
applications (1)
    ↓
application_recruiters (many) — capped at MAX_RECRUITERS_PER_APPLICATION (3)
    ↓
recruiters (1) ←→ outreach (many)

applications (1) ←→ outreach (many)
applications (1) ←→ ai_cache (1)
applications (1) ←→ jobs (1)

prospective_companies (1)
    → job monitoring (daily --monitor-jobs)
    → recruiter scraping (--find-only leftover quota)
    → recruiters stored at company level (recruiters table only)
    → converted to applications (--add) → top recruiters linked then

job_postings (many) ← --monitor-jobs
    → PDF digest (daily 7 AM email) → mark_postings_digested()
    → applied: moves to applications table
    → filled: confirmed gone via --verify-filled → deleted after 7 days

monitor_stats (1 per day) ← --monitor-jobs
    → pipeline health metrics
    → 7-day reliability score

verify_filled_stats (1 per day) ← --verify-filled
    → filled position cleanup metrics
    → inconclusive breakdown for diagnosing ATS blocks

coverage_stats (1 per day) ← --find-only
    → recruiter pipeline performance (metric1 + metric2)
    → alert trigger when thresholds breached for 3 consecutive days
    → persisted by careershift/find_emails.py

api_health (1 per platform per day) ← --monitor-jobs [writer pending]
    → per-platform ATS API reliability
    → surfaces rate limiting and degrading APIs

pipeline_alerts ← all pipelines
    → unified alert log for all threshold breaches
    → created/read by db/pipeline_alerts.py and pipeline.py
```

---

## Storage Estimates

```text
Table                  Rows (6 months)    Size
─────────────────────────────────────────────────
applications           ~200               ~0.1 MB
recruiters             ~1,000             ~0.5 MB
application_recruiters ~600 (rolling)     ~0.05 MB
outreach               ~5,000 (rolling)   ~1.5 MB
ai_cache               ~200 (rolling)     ~1 MB
jobs                   ~200 (rolling)     ~1 MB
careershift_quota      ~30 (rolling)      ~0.01 MB
quota_alerts           ~10 (rolling)      ~0.01 MB
prospective_companies  ~137               ~0.1 MB
job_postings (active)  ~2,000             ~6 MB
job_postings (expired) ~50,000            ~12 MB
monitor_stats          ~60 (rolling)      ~0.05 MB
verify_filled_stats    ~60 (rolling)      ~0.01 MB
coverage_stats         ~60 (rolling)      ~0.01 MB
api_health             ~60×6 (rolling)    ~0.05 MB  [writer pending — 1 row/platform/day]
pipeline_alerts        ~10 (rolling)      ~0.01 MB
─────────────────────────────────────────────────
Total DB size          ~22 MB (6 months)

Well within SQLite comfort zone.
Monthly VACUUM + ANALYZE keeps DB lean.
See deployment.md for maintenance schedule.
```