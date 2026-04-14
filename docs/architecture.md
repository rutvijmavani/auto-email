# Architecture

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