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

## ATS Detection — Confidence Scoring Buffer

### Philosophy
```
Never trust the first API response that returns jobs.
A short slug like "capital" on Lever returns jobs for
"Capital Group" — not "Capital One". First-match-wins
produces wrong company data in your digest.

Instead: try ALL platforms × ALL slug variants, score
every response, pick the best one with enough confidence.

Same buffer approach as recruiter domain validation.
```

### How it works
```
For each company (e.g. "Capital One"):

Step 1 — Generate slug variants:
  "Capital One" → ["capitalone", "capital-one", "capital"]

Step 2 — Try ALL platforms × ALL slug variants:
  Greenhouse × capitalone → 404   → skip
  Greenhouse × capital    → jobs  → score it
  Lever      × capitalone → jobs  → score it
  Lever      × capital    → jobs  → score it
  Ashby      × capitalone → 404   → skip
  Workday    × capitalone × wd5 → jobs → score it
  ... (every combination)

Step 3 — Score each response:
  confidence% = jobs where ALL company keywords appear
                ─────────────────────────────────────
                total sampled jobs (max 20)

  final_score = confidence% × log10(job_count + 1)

  Example:
    Lever "capital"    → 50 jobs, "capital" in URL
                       → "one" missing → 0/20 match
                       → confidence = 0%
                       → final_score = 0

    Lever "capitalone" → 61 jobs, "capital one" in URL+title
                       → 19/20 match
                       → confidence = 95%
                       → final_score = 95 × log10(62) = 170

Step 4 — Classify:
  Clear winner:  top score > threshold AND gap > 10%
                 → auto-accept silently
  Close call:    top score > threshold AND gap ≤ 10%
                 → auto-select best + send email to verify
  Unknown:       top score < threshold
                 → mark unknown + send email to review

Step 5 — Tie-break by date reliability:
  When two platforms score within 10% of each other:
    ashby > lever > workday > smartrecruiters > greenhouse
  Reason: prefer most reliable posted_at date field
```

### Scoring formula
```
confidence% × log10(job_count + 1)

Why log10()?
  Prevents huge job counts from dominating
  Same as recruiter pipeline domain scoring

  100 jobs: log10(101) = 2.00  → 95 × 2.00 = 190
  500 jobs: log10(501) = 2.70  → 95 × 2.70 = 257
  10  jobs: log10(11)  = 1.04  → 95 × 1.04 = 99

Empty jobs (hiring freeze):
  confidence = 50 (neutral — ATS structure confirmed)
  final_score = 50 × log10(1) = 0
  Only accepted if no other viable match exists
```

### Keyword extraction
```
Company name → significant keywords (ALL must match)

"Capital One"        → ["capital", "one"]
"JPMorgan Chase"     → ["jpmorgan", "chase"]
"AT&T"               → ["at"]  (& removed, t too short)
"Palo Alto Networks" → ["palo", "alto"]  (networks = stop word)
"Stripe"             → ["stripe"]

Stop words filtered:
  inc, corp, llc, ltd, co, the, and, jobs, careers,
  group, technologies, tech, systems, solutions,
  services, america, usa, global, international

ALL keywords must appear in job title+URL for a match.
This prevents partial matches (Capital Group ≠ Capital One).
```

### Detection status values
```
detected    → high confidence, auto-accepted silently
close_call  → auto-selected by tie-break, email sent to verify
unknown     → low confidence, email sent for manual review
manual      → manually overridden by user, never auto-re-detected
```

### Re-detection triggers
```
Automatic re-detection when:
  → ats_platform = 'unknown' or NULL
  → ats_slug = NULL or empty
  → ats_detected_at is NULL (never detected)
  → consecutive_empty_days >= JOB_MONITOR_REDETECT_DAYS (14)

Never re-detected:
  → ats_platform = 'manual' (user override is permanent)

Manual re-detection:
  → python pipeline.py --detect-ats "Stripe"
  → Forces fresh detection for specific company

Manual override:
  → python pipeline.py --detect-ats "Capital One" --override workday capitalone
  → Permanently sets platform + slug, never auto-changed
```

### Empty day counter reset (critical behavior)
```
After EVERY detection attempt:
  → consecutive_empty_days reset to 0
  → Prevents infinite re-detection loops

Two scenarios both produce 0 jobs:

  Scenario A — Company switched ATS:
    → Ashby returns 0 jobs (wrong ATS)
    → 14 days accumulate → re-detect → Lever found
    → consecutive_empty_days reset to 0 ✓

  Scenario B — Hiring freeze (correct ATS, 0 openings):
    → Ashby returns 0 jobs (correct ATS, no roles)
    → 14 days accumulate → re-detect → Ashby confirmed
    → consecutive_empty_days reset to 0
    → No re-detection again for 14 more days ✓

Maximum job posting gap when ATS switches:
  → 14 days (configurable via JOB_MONITOR_REDETECT_DAYS)
```

### Detection summary email
```
ONE email sent after --detect-ats completes.
Only sent when there are close calls or unknowns.
Silently skipped when everything detected cleanly.

Subject: 🔍 ATS Detection Complete · March 4, 2026
         ✅ 131 auto-detected | ⚠ 2 close calls | ❌ 1 unknown

CLOSE CALLS section:
  Company | Selected ATS | Confidence | Verify Link
  Linear  | Ashby (94%)  | 201 jobs   | jobs.ashbyhq.com/linear
  Runner-up: Lever (91%, 198 jobs)
  Override: python pipeline.py --detect-ats "Linear" --override ashby linear

NEEDS MANUAL REVIEW section:
  Company      | Best Attempt              | Override Command
  Obscure Corp | smartrecruiters/obscure   | --override <ats> <slug>
               | (40% conf, 3 jobs)
```

### Monitorable companies
```
--monitor-jobs only processes companies with confirmed ATS:
  ats_platform NOT NULL
  ats_platform != 'unknown'
  ats_slug NOT NULL

Unknown companies are SKIPPED in daily monitoring.
"X companies with unknown ATS — run --detect-ats" shown in output.
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
E1: Wrong slug matches wrong company (e.g. "capital" → Capital Group)
    → Confidence scoring catches this:
      "capital" on Lever → 0% confidence for "Capital One"
      (keyword "one" missing from all job URLs/titles)
    → Score too low → rejected automatically
    → "capitalone" slug → 95% confidence → accepted ✓
    → No manual intervention needed

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

# Detect ATS for all undetected/stale companies
python pipeline.py --detect-ats

# Re-detect ATS for specific company
python pipeline.py --detect-ats "Stripe"

# Manually override ATS for a company
# Use after reviewing close call / unknown email
python pipeline.py --detect-ats "Capital One" --override workday capitalone
python pipeline.py --detect-ats "Linear" --override ashby linear

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
  jobs/ats_detector.py    → confidence scoring buffer
                          → try ALL platforms x ALL slugs
                          → score: confidence% x log10(jobs+1)
                          → classify: detected/close_call/unknown
                          → tie-break by date reliability
                          → re-detection logic
                          → manual override support

  outreach/report_templates/detection_report.py
                          → detection summary email
                          → close calls + unknowns batched
                          → verify URLs + override commands

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