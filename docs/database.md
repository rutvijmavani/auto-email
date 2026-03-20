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
Tracks daily Gemini API call counts per model. Used to enforce daily limits locally before hitting the API.

| Column | Type | Description |
|---|---|---|
| `model` | TEXT | Model name |
| `date` | TEXT | YYYY-MM-DD |
| `count` | INTEGER | Calls made today |

**Primary key:** `(model, date)`

**Retention:** Auto-deleted after 21 days.

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
| `content_hash` | TEXT | SHA256 of company+title+location (dedup key) |
| `location` | TEXT | Job location |
| `posted_at` | TIMESTAMP | Original posting date (from ATS API) |
| `description` | TEXT | Job description (cleared on expiry) |
| `skill_score` | INTEGER | Relevance score (0-100+) |
| `status` | TEXT | `new` / `pre_existing` / `expired` / `dismissed` / `applied` |
| `first_seen` | DATE | Date first detected by pipeline |
| `created_at` | TIMESTAMP | When inserted |

**Status lifecycle:**
```text
new          → expired     (auto: after 7 days, description cleared)
new          → digested    (auto: after digest email sent successfully)
new          → dismissed   (manual: user dismissed from digest)
new          → applied     (auto: when added via --add)
pre_existing → (stays)     (first scan — not shown in digest)
```

**Indexes:**
```text
UNIQUE idx_job_postings_hash      ON content_hash (WHERE NOT NULL)
       idx_job_postings_status_seen ON (status, first_seen)
```

**Retention:**
```text
new       → expired after 7 days (description cleared, URL kept forever)
dismissed → deleted after 30 days
applied   → deleted immediately (already in applications table)
expired   → kept forever (prevents re-showing same jobs)
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
| `job_postings` (expired URLs) | Permanent | URL kept to prevent re-showing |
| `outreach` (sent) | 30 days | `sent_at < now - 30 days` (`RETENTION_OUTREACH_SENT`) |
| `outreach` (pending) | 30 days | `scheduled_for < now - 30 days` |
| `outreach` (failed/bounced/cancelled) | 30 days | `created_at < now - 30 days` |
| `job_postings` (new→expired) | 7 days | `first_seen < now - 7 days` (description cleared) |
| `job_postings` (dismissed) | 30 days | `first_seen < now - 30 days` |
| `ai_cache` | 21 days | `expires_at <= now` |
| `jobs` | 21 days | `created_at < now - 21 days` |
| `model_usage` | 21 days | `date < now - 21 days` |
| `careershift_quota` | 30 days | `date < now - 30 days` |
| `quota_alerts` | 30 days | `created_at < now - 30 days` |
| `serper_quota` | Permanent | Single row, never deleted |

**Cleanup execution order in `init_db()`:**
```text
1. _cleanup_auto_close_applications     ← mark applications closed first
2. _cleanup_closed_application_recruiters ← then clean up their recruiter links
3. _cleanup_monitor_stats               ← independent, order doesn't matter
4. (all other existing cleanup functions)
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
    → PDF digest (daily 8 AM email)
    → applied: moves to applications table

monitor_stats (1 per day) ← --monitor-jobs
    → pipeline health metrics
    → 7-day reliability score
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
─────────────────────────────────────────────────
Total DB size          ~22 MB (6 months)

Well within SQLite comfort zone.
Monthly VACUUM + ANALYZE keeps DB lean.
See deployment.md for maintenance schedule.
```