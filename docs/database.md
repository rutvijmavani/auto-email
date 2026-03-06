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
| `status` | TEXT | `active` / `closed` |
| `created_at` | TIMESTAMP | When added to DB |

**Retention:** Permanent — never auto-deleted.

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

**Retention:** Permanent — never auto-deleted. Deleting rows here would cause the pipeline to re-scrape companies unnecessarily.

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
```
pending → sent       (email delivered successfully)
pending → failed     (send attempt failed)
pending → cancelled  (recruiter marked inactive before sending)
sent    → bounced    (hard bounce detected on delivery)
```

**Outreach sequence per recruiter+application:**
```
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
| `priority` | INTEGER | Higher = scraped first (default 0) |
| `status` | TEXT | `pending` / `scraped` / `converted` / `exhausted` |
| `ats_platform` | TEXT | Detected ATS: `greenhouse` / `lever` / `ashby` / `smartrecruiters` / `workday` / `oracle_hcm` / `unknown` |
| `ats_slug` | TEXT | ATS slug or JSON (e.g. `{"slug":"capitalone","wd":"wd12","path":"Capital_One"}`) |
| `ats_detected_at` | TIMESTAMP | When ATS was last detected |
| `first_scanned_at` | TIMESTAMP | When first `--monitor-jobs` scan completed |
| `last_checked_at` | TIMESTAMP | When last checked by `--monitor-jobs` |
| `consecutive_empty_days` | INTEGER | Days with 0 jobs returned (triggers re-detection at 14) |
| `scraped_at` | TIMESTAMP | When CareerShift scrape completed |
| `converted_at` | TIMESTAMP | When converted to active application |
| `created_at` | TIMESTAMP | When added to DB |

**Status lifecycle:**
```
pending   → scraped    (recruiters found via CareerShift)
pending   → exhausted  (CareerShift found no recruiters)
scraped   → converted  (--add command used for this company)
```

**ATS detection methods:**
```
google  → detected via Google site: search (most reliable)
api     → detected via direct API buffer scan
manual  → manually set via --override flag (never auto-changed)
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
```
new          → expired     (auto: after 7 days, description cleared)
new          → dismissed   (manual: user dismissed from digest)
new          → applied     (auto: when added via --add)
pre_existing → (stays)     (first scan — not shown in digest)
```

**Indexes:**
```
UNIQUE idx_job_postings_hash  ON content_hash (WHERE NOT NULL)
       idx_job_postings_status_seen ON (status, first_seen)
```

**Retention:**
```
new     → expired after 7 days (description cleared, URL kept forever)
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

**Retention:** Permanent — used for long-term reliability tracking.

**Key metrics derived from this table:**
```
Coverage rate:     companies_with_results / companies_monitored
Filter match rate: jobs_matched_filters / total_jobs_fetched
Pipeline reliability: runs with pdf_generated=1 / total_runs (7 days)
```


---

## Retention Policies

All retention values are configured in `config.py` and enforced at startup via `init_db()`.

| Table | Retention | Condition |
|---|---|---|
| `applications` | Permanent | Never deleted |
| `recruiters` | Permanent | Never deleted (soft delete only) |
| `application_recruiters` | Permanent | Never deleted |
| `prospective_companies` | Permanent | Never deleted |
| `monitor_stats` | Permanent | Never deleted |
| `job_postings` (expired URLs) | Permanent | URL kept to prevent re-showing |
| `outreach` (sent) | 90 days | `sent_at < now - 90 days` |
| `outreach` (pending) | 30 days | `scheduled_for < now - 30 days` |
| `outreach` (failed/bounced/cancelled) | 30 days | `created_at < now - 30 days` |
| `job_postings` (new→expired) | 7 days | `first_seen < now - 7 days` (description cleared) |
| `job_postings` (dismissed) | 30 days | `first_seen < now - 30 days` |
| `ai_cache` | 21 days | `expires_at <= now` |
| `jobs` | 21 days | `created_at < now - 21 days` |
| `model_usage` | 21 days | `date < now - 21 days` |
| `careershift_quota` | 30 days | `date < now - 30 days` |
| `quota_alerts` | 30 days | `created_at < now - 30 days` |

---

## Relationships

```
applications (1)
    ↓
application_recruiters (many)
    ↓
recruiters (1) ←→ outreach (many)
    
applications (1) ←→ outreach (many)
applications (1) ←→ ai_cache (1)
applications (1) ←→ jobs (1)

prospective_companies (1)
    → job monitoring (daily --monitor-jobs)
    → recruiter scraping (--find-only leftover quota)
    → converted to applications (--add)

job_postings (many) ← --monitor-jobs
    → PDF digest (daily 8 AM email)
    → applied: moves to applications table

monitor_stats (1 per day) ← --monitor-jobs
    → pipeline health metrics
    → 7-day reliability score
```

---

## Storage Estimates

```
Table                  Rows (6 months)    Size
─────────────────────────────────────────────────
applications           ~200               ~0.1 MB
recruiters             ~1,000             ~0.5 MB
application_recruiters ~3,000             ~0.2 MB
outreach               ~15,000            ~5 MB
ai_cache               ~200 (rolling)     ~1 MB
jobs                   ~200 (rolling)     ~1 MB
careershift_quota      ~30 (rolling)      ~0.01 MB
quota_alerts           ~10 (rolling)      ~0.01 MB
prospective_companies  ~137               ~0.1 MB
job_postings (active)  ~2,000             ~6 MB
job_postings (expired) ~50,000            ~12 MB
monitor_stats          ~180               ~0.1 MB
─────────────────────────────────────────────────
Total DB size          ~27 MB (6 months)

Well within SQLite comfort zone.
Monthly VACUUM + ANALYZE keeps DB lean.
See deployment.md for maintenance schedule.
```