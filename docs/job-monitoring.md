# Job Monitoring Pipeline

## Overview

The job monitoring pipeline automatically scans career pages of all companies
in your target list daily, finds newly posted jobs matching your profile, and
delivers a PDF digest to your inbox every morning at 8 AM. The goal is to
apply to positions within hours of posting — before competition builds up and
while the recruiter is actively reviewing applications.

---

## Why Early Application Matters

```
Job posted Day 0:
  → Recruiter actively hiring NOW
  → ATS queue is empty or near-empty
  → You are one of the first applicants
  → Higher chance of being noticed
  → Position unlikely to be filled yet

Job applied Day 7+:
  → 100+ applicants already reviewed
  → Recruiter may have found strong candidates
  → Position may be in interview stage
  → Your application buried in queue
```

This pipeline is the entry point of the entire system.
If it fails to detect new jobs early, every downstream
pipeline (recruiter finding, outreach, response) loses
effectiveness. Maximum reliability is required.

---

## Architecture

### Data Flow

```
prospects.txt (137 companies)
         ↓
--import-prospects
         ↓
prospective_companies table (DB source of truth)
         ↓
--monitor-jobs (daily 8 AM)
         ↓
ATS Detection (one-time per company, cached in DB)
         ↓
API calls (Greenhouse/Lever/Ashby/SmartRecruiters/Workday)
         ↓
Client-side filtering (title → seniority → skills → USA)
         ↓
Freshness check (URL + content hash)
         ↓
job_postings table
         ↓
PDF generation (grouped by company)
         ↓
Email with PDF attachment (8 AM daily)
```

---

## ATS Platform Support

### Free Public APIs (no authentication required)

#### Greenhouse
```
Endpoint:   GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true
Auth:       None
Cost:       Free forever
Rate limit: None documented
Date field: updated_at — UNRELIABLE (changes on every edit)
Response:   id, title, updated_at, location, absolute_url, content
Companies:  Stripe, Doordash, Coinbase, Robinhood, Databricks,
            Snowflake, Pinterest, Okta, Palo Alto Networks,
            Nutanix, DocuSign, Pure Storage, Twilio, Block,
            Figma, Notion, Dropbox, Instacart and more

Freshness:  first_seen + content_hash approach
            (updated_at not used for freshness — unreliable)
```

#### Lever
```
Endpoint:   GET https://api.lever.co/v0/postings/{slug}?mode=json
Auth:       None
Cost:       Free forever
Rate limit: None documented
Date field: createdAt — RELIABLE (Unix timestamp, never changes)
Response:   id, text, createdAt, categories, descriptionPlain, hostedUrl
Companies:  Netflix, Waymo, Cruise, Lyft, Spotify, Airtable and more

Freshness:  createdAt used directly for date comparison ✓
```

#### Ashby
```
Endpoint:   GET https://api.ashbyhq.com/posting-api/job-board/{slug}
Auth:       None
Cost:       Free forever
Rate limit: None documented
Date field: publishedAt — RELIABLE (original publish date)
Response:   id, title, publishedAt, location, descriptionHtml, jobUrl
Companies:  Linear, Vercel, Loom, Retool, Ramp, Brex,
            Modern Treasury and more

Freshness:  publishedAt used directly for date comparison ✓
```

#### SmartRecruiters
```
Endpoint:   GET https://api.smartrecruiters.com/v1/companies/{slug}/postings
Auth:       None for public jobs
Cost:       Free
Date field: releasedDate — RELIABLE
Response:   id, title, location, releasedDate, department, description
Companies:  Starbucks, Visa, Bosch and more

Freshness:  releasedDate used directly for date comparison ✓
```

#### Workday (undocumented public API)
```
Endpoint:   GET https://{company}.wd5.myworkdayjobs.com/wday/cxs/
                {company}/careers/jobs
Auth:       None required (used by Workday career page frontend)
Cost:       Free
Risk:       Undocumented — could change without notice
Date field: postedOn — RELIABLE (original posting date)
Response:   title, postedOn, locationsText, externalUrl, bulletFields
Companies:  JPMorgan, Goldman Sachs, Citibank, BlackRock, Mastercard,
            American Express, Morgan Stanley, Visa, Wells Fargo,
            Bank of America, Charles Schwab, State Street and more
Variants:   Try wd1, wd2, wd3, wd5 during auto-detection

Freshness:  postedOn used directly for date comparison ✓
```

### Coverage Summary
```
Greenhouse:      ~40 companies
Lever:           ~20 companies
Ashby:           ~15 companies
SmartRecruiters: ~5 companies
Workday:         ~12 companies
─────────────────────────────
Via APIs:        ~92 companies (67%)

Remaining ~45: Big tech custom pages
  (Google, Apple, Microsoft, Amazon, Meta etc.)
  → Skipped for now — bot detection issues,
    unreliable scraping, high maintenance
  → Add custom scrapers as future enhancement
```

---

## ATS Detection (Option C — Hybrid)

### How it works
```
First time a company is monitored:
  1. Try Greenhouse: boards-api.greenhouse.io/v1/boards/{slug}/jobs
  2. Try Lever:      api.lever.co/v0/postings/{slug}
  3. Try Ashby:      api.ashbyhq.com/posting-api/job-board/{slug}
  4. Try SmartRecruiters
  5. Try Workday variants (wd1, wd2, wd3, wd5)
  6. None matched → mark 'unknown'

Slug variants tried per attempt:
  "Stripe"          → "stripe"
  "JPMorgan Chase"  → "jpmorganchase", "jpmorgan-chase", "jpmorgan"
  "Palo Alto Networks" → "paloaltonetworks", "palo-alto-networks",
                         "paloalto"

Result stored in prospective_companies table:
  ats_platform    = 'greenhouse'
  ats_slug        = 'stripe'
  ats_detected_at = 2026-03-04

Subsequent runs: read from DB — no re-detection
```

### Re-detection triggers
```
Automatic re-detection when:
  → consecutive_empty_days >= JOB_MONITOR_REDETECT_DAYS (14)
    (company may have switched ATS)
  → ats_platform = 'unknown' or NULL
  → ats_slug = NULL or empty

Manual re-detection:
  → python pipeline.py --detect-ats "Stripe"
  → Forces fresh detection for specific company
```

### Empty day counter reset (critical behavior)
```
After EVERY successful ATS detection:
  → consecutive_empty_days reset to 0
  → Regardless of how many jobs were returned

Why this matters:
  Two very different scenarios both result in 0 jobs:

  Scenario A — Company switched ATS:
    → Ashby returns 0 jobs (wrong ATS now)
    → 14 days accumulate → re-detect → Lever found
    → consecutive_empty_days reset to 0
    → Next 14 days accumulate before re-detect again
    → Correct: re-detection fires when needed ✓

  Scenario B — Hiring freeze (same ATS, 0 openings):
    → Ashby returns 0 jobs (correct ATS, no openings)
    → 14 days accumulate → re-detect → Ashby confirmed again
    → consecutive_empty_days reset to 0
    → Next 14 days accumulate before re-detect again
    → Correct: no infinite re-detection loop ✓

  Without reset:
    → Hiring freeze company re-detected EVERY day
    → Wastes API calls on all 5 ATS platforms daily
    → 137 companies × 5 APIs × daily = massive waste

Maximum job posting gap when ATS switches:
  → Switch happens Day 0
  → Re-detection triggers Day 14
  → Configurable: JOB_MONITOR_REDETECT_DAYS = 14
  → Lower = faster detection, more API calls
  → Higher = slower detection, fewer API calls
```

### Detection validation
```
When API returns jobs during detection:
  → Verify: at least 1 job returned
    (empty response = wrong slug or hiring freeze)
  → Verify: response company name fuzzy-matches
    expected company name
    (prevents "apple" matching "Apple Leisure Group")
  → If validation fails → try next slug variant
```

---

## Job Filtering

### Filter priority
```
All filtering is client-side (APIs return ALL jobs)

Priority 1 — Title match (broad keyword):
  Keywords: engineer, developer, programmer,
            "software", "swe", "mts",
            "member of technical staff",
            "software development engineer"
  Match: case-insensitive, partial match
  Hard reject: jobs with 0 keyword matches

Priority 2 — Seniority (soft score):
  TARGET_SENIORITY = ["Senior", "Staff",
                      "Principal", "Lead"]
  Match: +5 score if matched
  No hard reject — junior roles included
  but ranked lower

Priority 3 — Skills (soft score):
  TARGET_SKILLS = ["Python", "JavaScript",
                   "TypeScript", "React",
                   "Node.js", "AWS", "Go",
                   "Java", "Kubernetes", "Docker"]
  +2 score per skill matched in description

Priority 4 — Location (USA only):
  Accept: "united states", "usa", "u.s.",
          "remote", any US city/state,
          2-letter state abbreviations (NY, CA etc.)
  Reject: "canada", "uk", "india", "germany",
          "australia", "singapore" etc.
  Default: include if location unclear
```

### Relevance scoring
```python
score = 10                          # base (title matched)
score += 5  if seniority matched    # seniority bonus
score += skill_count * 2            # 2pts per skill match
score += 5  if posted today         # freshness bonus
score += 3  if posted yesterday
score += 1  if posted 2-3 days ago

# Jobs sorted by score DESC within each company group
```

---

## Freshness Detection

### The Greenhouse problem
```
Greenhouse updated_at changes on every edit:
  → Job posted 6 months ago, edited yesterday
  → updated_at = yesterday
  → Looks fresh but is actually old

Solution: Do NOT use updated_at for freshness
  → Use first_seen + content_hash instead
```

### Two-layer deduplication
```
Layer 1 — URL check:
  Job URL already in job_postings table
  (any status including 'expired') → SKIP
  → Handles: same job seen before

Layer 2 — Content hash check:
  hash = SHA256(company + normalized_title + location)
  Hash already in job_postings table → SKIP
  → Handles: same job reposted with new URL
  → Handles: Greenhouse URL changes on edit

Both layers must pass for job to be "new"
```

### Date-based freshness (when reliable date available)
```
Lever, Ashby, SmartRecruiters, Workday:
  posted_at available and reliable
  → If (today - posted_at) > JOB_MONITOR_DAYS_FRESH
    → Store as 'pre_existing', don't show in digest

Greenhouse:
  updated_at unreliable
  → Rely on first_seen only
  → No date-based freshness check
```

### First run per company
```
CRITICAL edge case:
  First time we scan a company →
  All 50-200 existing jobs appear as "new"
  → Would flood digest with stale jobs

Solution:
  → On first scan per company:
    Fetch all jobs → mark ALL as 'pre_existing'
    → Do NOT include in digest
  → Only jobs seen AFTER first scan = truly new
  → Track: prospective_companies.first_scanned_at
  → If first_scanned_at IS NULL → first run
```

---

## Edge Cases

### ATS Detection
```
E1: Wrong slug returns wrong company
    → Validate: company name in response fuzzy-matches expected
    → If mismatch → try next variant

E2: Company switches ATS (e.g. Ashby → Lever)
    → Trigger: consecutive_empty_days >= 14
    → Auto re-detect across all ATS platforms
    → New platform + slug stored in DB
    → consecutive_empty_days reset to 0
    → Maximum gap: 14 days of missed postings

E3: Detection during hiring freeze (0 jobs, correct ATS)
    → Company has 0 openings but is on correct ATS
    → 14 empty days → re-detect → same ATS confirmed
    → consecutive_empty_days reset to 0 after confirmation
    → No re-detection again for another 14 days
    → Prevents infinite re-detection loop ✓
    → Note: previously documented as 'detected_empty' status
      — this approach was replaced with counter reset

E4: Workday URL variant (wd1/wd2/wd3/wd5)
    → Try all 4 variants during detection
    → Store exact URL that worked
```

### API Calls
```
E5: API pagination
    → Greenhouse/Lever may paginate for large companies
    → Always fetch all pages before filtering
    → Critical: missing page 2+ = missing jobs

E6: API rate limiting (429)
    → Exponential backoff: wait 60s, retry once
    → If still fails → skip company today
    → Log: company skipped due to rate limit
    → Do NOT mark as ATS failure

E7: API timeout
    → 10 second timeout per request
    → If timeout → skip company today
    → Do NOT mark as ATS failure

E8: Malformed JSON response
    → Log parsing error
    → Skip company today
    → If 3 consecutive parse failures
      → mark ats_platform = 'needs_redetection'

E9: Partial run failure (VM crash mid-run)
    → Track last_checked_at per company
    → On restart → only process companies
      where last_checked_at < today
    → Prevents duplicate processing
```

### Job Data
```
E10: Empty job title
     → Skip — can't filter without title

E11: Empty job description
     → Title filter still works
     → Skill score = 0
     → Include with lower score

E12: Job posted and removed same day
     → We detect and show → user applies
     → Position may already be cancelled
     → Can't prevent → document as known limitation

E13: Duplicate job across ATS platforms
     → content_hash deduplication handles this ✓

E14: Non-English job title
     → Normalize: lowercase + strip accents
       before keyword matching

E15: Location field variations
     → "Remote, USA" / "United States" / "Anywhere"
     → All normalized to US location ✓

E16: Special characters in title/description
     → Sanitize before PDF rendering
     → Replace problematic Unicode chars
```

### PDF Generation
```
E17: 0 jobs match filters
     → Don't generate PDF
     → Send brief email: "No matching jobs today"

E18: PDF generation fails (reportlab error)
     → Catch exception
     → Send plain text email with job list
     → Log error

E19: Disk full during PDF write
     → Catch IOError
     → Log error + send text email
     → Check disk space before generation

E20: Very large PDF (200+ jobs)
     → 200 jobs ≈ 20 pages ≈ 2-3 MB
     → Gmail limit 25 MB → no concern ✓
```

---

## Performance Metrics

### Why metrics are critical
```
This is the entry pipeline. If it fails silently:
  → You never see relevant job postings
  → You apply late → lower response rate
  → Every downstream pipeline loses effectiveness

Metrics catch silent failures before they compound.
```

### Metrics tracked per run

#### Metric 1 — Detection Coverage
```
Formula: companies_with_results / total_monitored
Target:  ≥ 70%
Alert:   < 70% for 3 consecutive days
Meaning: How many companies returned at least 1 job?
         Low value = ATS detection issues or
         widespread API failures
```

#### Metric 2 — ATS Known Rate
```
Formula: companies_with_known_ats / total_companies
Target:  ≥ 80%
Alert:   < 80%
Meaning: What % of companies have confirmed ATS?
         Low value = many companies unmonitored
```

#### Metric 3 — Filter Match Rate
```
Formula: jobs_matched_filters / total_jobs_fetched
Target:  5% - 40%
Alert:   < 5% (filters too strict)
         > 60% (filters too loose)
Meaning: Are our title/skill filters calibrated?
```

#### Metric 4 — New Job Rate
```
Formula: new_jobs_found / total_jobs_fetched
Target:  varies by season
Meaning: Hiring activity indicator
         Low in Nov-Dec (off-season) → normal
         Low in Jan-Mar (peak) → pipeline issue
```

#### Metric 5 — Pipeline Reliability
```
Formula: successful_runs / total_runs (last 7 days)
Target:  ≥ 90%
Alert:   < 90%
Meaning: How often does --monitor-jobs complete?
         Tracks VM stability and API availability
```

#### Metric 6 — API Failure Rate
```
Formula: api_failures / total_api_calls
Target:  < 10%
Alert:   > 10% for 3 consecutive days
Meaning: Are APIs degrading or rate limiting us?
```

#### Metric 7 — Application Conversion Rate (future)
```
Formula: jobs_applied / jobs_shown_in_digest
Target:  TBD after data collected
Meaning: Quality of job recommendations
         Requires tracking which digest jobs
         you actually applied to
```

#### Metric 8 — Time to Apply (future)
```
Formula: avg(applied_date - posted_date) in hours
Target:  < 24 hours
Meaning: Are you applying fast enough?
         Measures effectiveness of early
         application strategy
```

### DB schema for metrics
```sql
CREATE TABLE IF NOT EXISTS monitor_stats (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    date                   DATE NOT NULL UNIQUE,
    companies_monitored    INTEGER DEFAULT 0,
    companies_with_results INTEGER DEFAULT 0,
    companies_unknown_ats  INTEGER DEFAULT 0,
    api_failures           INTEGER DEFAULT 0,
    total_jobs_fetched     INTEGER DEFAULT 0,
    new_jobs_found         INTEGER DEFAULT 0,
    jobs_matched_filters   INTEGER DEFAULT 0,
    run_duration_seconds   INTEGER DEFAULT 0,
    pdf_generated          INTEGER DEFAULT 0,
    email_sent             INTEGER DEFAULT 0,
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Alert delivery
```
In PDF report header (every run):
  → Coverage rate
  → ATS known rate
  → API failures list
  → Any threshold breaches

Weekly summary email (Mondays):
  → 7-day trend for all metrics
  → Filter calibration suggestion if needed
  → Pipeline reliability score
```

---

## DB Schema

### job_postings table
```sql
CREATE TABLE IF NOT EXISTS job_postings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    company      TEXT NOT NULL,
    title        TEXT NOT NULL,
    job_url      TEXT NOT NULL UNIQUE,
    content_hash TEXT,
    location     TEXT,
    posted_at    TIMESTAMP,
    description  TEXT,
    skill_score  INTEGER DEFAULT 0,
    status       TEXT DEFAULT 'new',
    first_seen   DATE NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_postings_hash
ON job_postings(content_hash)
WHERE content_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_job_postings_status_seen
ON job_postings(status, first_seen);
```

### prospective_companies table (updated)
```sql
-- New columns added to existing table
ALTER TABLE prospective_companies
  ADD COLUMN ats_platform     TEXT DEFAULT 'unknown';
  ADD COLUMN ats_slug         TEXT;
  ADD COLUMN ats_detected_at  TIMESTAMP;
  ADD COLUMN first_scanned_at TIMESTAMP;
  ADD COLUMN last_checked_at  TIMESTAMP;
  ADD COLUMN consecutive_empty_days INTEGER DEFAULT 0;
```

### Retention policy
```
job_postings cleanup (runs in init_db()):

  Archive expired new postings:
    UPDATE job_postings
    SET status = 'expired', description = NULL
    WHERE status = 'new'
    AND first_seen < DATE('now', '-7 days')

  Delete old dismissed:
    DELETE FROM job_postings
    WHERE status = 'dismissed'
    AND first_seen < DATE('now', '-30 days')

  Keep expired URLs forever:
    → Prevents re-showing old jobs
    → ~12 MB after 6 months → negligible

PDF digests:
  Keep last 30 days only
  Cron: find data/digests/ -name "*.pdf" -mtime +30 -delete
```

---

## Configuration

```python
# config.py

# ─────────────────────────────────────────
# JOB MONITORING
# ─────────────────────────────────────────

# Job title keywords (broad match — any of these)
TARGET_JOB_TITLES = [
    "software engineer",
    "software developer",
    "software development engineer",
    "backend engineer",
    "frontend engineer",
    "full stack engineer",
    "full stack developer",
    "web developer",
    "platform engineer",
    "application engineer",
    "member of technical staff",
    "swe",
]

# Seniority keywords (soft score only — no hard reject)
TARGET_SENIORITY = [
    "senior", "staff", "principal", "lead",
]

# Skills (soft score — job description match)
TARGET_SKILLS = [
    "python", "javascript", "typescript",
    "react", "node.js", "aws", "go",
    "java", "kubernetes", "docker",
]

# Location — USA only
USA_LOCATION_KEYWORDS = [
    "united states", "usa", "u.s.", "remote",
    "new york", "san francisco", "seattle",
    "austin", "boston", "chicago", "denver",
    "los angeles", "atlanta", "miami", "dallas",
]
EXCLUDE_LOCATIONS = [
    "canada", "toronto", "uk", "london",
    "india", "bangalore", "germany", "berlin",
    "australia", "singapore", "ireland", "dublin",
    "poland", "netherlands", "france", "paris",
]

# Freshness
JOB_MONITOR_DAYS_FRESH        = 3    # days to consider fresh
JOB_MONITOR_REDETECT_DAYS     = 14   # re-detect ATS after X empty days
JOB_MONITOR_PDF_RETENTION     = 30   # days to keep PDF files

# No job cap — show ALL matching jobs in PDF
JOB_MONITOR_MAX_JOBS          = 0    # 0 = unlimited

# Alert thresholds
MONITOR_COVERAGE_ALERT        = 0.70  # alert if < 70% coverage
MONITOR_ATS_UNKNOWN_ALERT     = 0.20  # alert if > 20% unknown ATS
MONITOR_RELIABILITY_ALERT     = 0.90  # alert if < 90% runs succeed
MONITOR_MATCH_RATE_LOW_ALERT  = 0.05  # alert if < 5% jobs match
MONITOR_MATCH_RATE_HIGH_ALERT = 0.60  # alert if > 60% jobs match
```

---

## PDF Digest Format

```
data/digests/jobs_digest_2026-03-04.pdf

Page 1 — Summary + Pipeline Health
  ┌────────────────────────────────────────┐
  │  Job Digest · March 4, 2026           │
  │  Companies monitored: 137             │
  │  New jobs found: 47                   │
  │  Matching your profile: 23            │
  ├────────────────────────────────────────┤
  │  PIPELINE HEALTH                      │
  │  Coverage:    129/137 (94%) ✓         │
  │  ATS Known:   119/137 (87%) ✓         │
  │  API Failures: 3  ⚠ (Stripe, Linear) │
  │  Match Rate:   18% ✓                  │
  ├────────────────────────────────────────┤
  │  TOP MATCHES TODAY                    │
  │  1. Sr SWE — Google (Remote)          │
  │  2. Sr Backend — Stripe (NYC)         │
  │  3. Staff SWE — Meta (Remote)         │
  └────────────────────────────────────────┘

Page 2+ — Grouped by company (score sorted within group)
  ┌────────────────────────────────────────┐
  │  GOOGLE  ·  3 new jobs                │
  ├────────────────────────────────────────┤
  │  ●●●●●  Senior SWE — Search           │
  │  Remote · Posted: today               │
  │  Skills: Python, Go, Kubernetes       │
  │  https://careers.google.com/...       │
  ├────────────────────────────────────────┤
  │  ●●●●○  Staff SWE — Infrastructure   │
  │  Seattle, WA · Posted: yesterday     │
  │  Skills: Python, AWS                  │
  │  https://careers.google.com/...       │
  └────────────────────────────────────────┘
```

---

## CLI Commands

```bash
# Run job monitoring + send PDF digest
python pipeline.py --monitor-jobs

# Detect ATS for all undetected companies
python pipeline.py --detect-ats

# Force re-detect ATS for specific company
python pipeline.py --detect-ats "Stripe"

# Check monitoring status
python pipeline.py --monitor-status

# Remove company from monitoring (future feature)
# python pipeline.py --remove-prospect "CompanyName"
```

---

## File Structure

```
jobs/
  ats/
    __init__.py
    base.py              → shared ATS client logic
    greenhouse.py        → Greenhouse public API client
    lever.py             → Lever public API client
    ashby.py             → Ashby public API client
    smartrecruiters.py   → SmartRecruiters client
    workday.py           → Workday undocumented API client
  ats_detector.py        → auto-detect ATS per company
  job_filter.py          → filter + score jobs
  job_monitor.py         → orchestrator run() entry point
  job_fetcher.py         → existing (unchanged)
  form_sync.py           → existing (unchanged)

db/
  job_monitor.py         → DB ops for job_postings +
                           monitor_stats tables

outreach/
  report_templates/
    monitor_report.py    → PDF digest generation +
                           email with attachment

data/
  digests/               → saved PDF digests (30 day retention)

tests/
  test_job_monitor.py    → comprehensive tests
```

---

## Cron Schedule (updated)

```bash
# Daily 8 AM — monitor jobs + send PDF digest
0 8 * * * cd /home/ubuntu/mail && \
  source venv/bin/activate && \
  python pipeline.py --monitor-jobs \
  >> logs/monitor_$(date +\%Y-\%m-\%d).log 2>&1
```

---

## Implementation Plan

```
Phase 1 — DB + Config:
  db/schema.py        → add job_postings + monitor_stats tables
                      → add columns to prospective_companies
  config.py           → add all monitoring config constants

Phase 2 — ATS clients:
  jobs/ats/base.py        → shared HTTP logic, retry, timeout
  jobs/ats/greenhouse.py  → Greenhouse API client
  jobs/ats/lever.py       → Lever API client
  jobs/ats/ashby.py       → Ashby API client
  jobs/ats/smartrecruiters.py
  jobs/ats/workday.py     → Workday undocumented API

Phase 3 — ATS detection:
  jobs/ats_detector.py    → try slugs, validate, store result
                          → re-detection logic

Phase 4 — Job filtering:
  jobs/job_filter.py      → title/seniority/skills/location
                          → relevance scoring
                          → content hash generation
                          → US location detection

Phase 5 — DB operations:
  db/job_monitor.py       → save_job_posting()
                          → get_new_postings()
                          → mark_pre_existing()
                          → cleanup_expired_postings()
                          → save_monitor_stats()
                          → get_monitor_stats()

Phase 6 — Orchestrator:
  jobs/job_monitor.py     → run() entry point
                          → coordinate all above
                          → track metrics
                          → handle all edge cases

Phase 7 — PDF + Email:
  outreach/report_templates/monitor_report.py
                          → PDF generation (reportlab)
                          → grouped by company
                          → pipeline health section
                          → email with attachment

Phase 8 — Pipeline integration:
  pipeline.py             → --monitor-jobs flag
                          → --detect-ats flag
                          → --monitor-status flag

Phase 9 — Tests:
  tests/test_job_monitor.py
                          → ATS client tests (mocked HTTP)
                          → ATS detection logic
                          → Job filtering + scoring
                          → Freshness detection
                          → Content hash deduplication
                          → First run pre_existing logic
                          → DB retention cleanup
                          → Metrics calculation
                          → Edge case coverage
```

---

## Known Limitations

```
1. Big tech custom pages not monitored
   (Google, Apple, Microsoft, Amazon, Meta)
   → Add custom scrapers as future enhancement

2. API cache delay (2-4 hours)
   Some APIs cache responses
   → Job posted at 8 AM may not appear until 10 AM
   → We see it next day at 8 AM (24 hour delay max)

3. Job posted and immediately removed
   → We show it, you apply, position cancelled
   → Can't prevent — rare occurrence

4. Workday API undocumented
   → Could change without notice
   → Monitor for failures → fall back to HTML scrape

5. No duplicate detection across ATS platforms
   → Same role posted on multiple platforms
   → Content hash handles same-platform duplicates
   → Cross-platform handled by company+title+location hash

6. Application conversion tracking
   → Metrics 7 and 8 require future implementation
   → Need to track which digest jobs you applied to
```