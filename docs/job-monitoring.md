# Job Monitoring Pipeline

## Overview

The job monitoring pipeline automatically tracks career pages of prospective
companies and sends a daily digest of newly posted jobs matching your profile.
This closes the job search loop — instead of manually checking company websites,
the pipeline finds relevant opportunities and you simply decide which to apply to.

---

## The Complete Automated Loop

```
8:00 AM: --monitor-jobs
  → Queries ATS APIs + scrapes career pages
  → Finds jobs posted in last 3 days
  → Filters by title → seniority → skills → location
  → Sends digest email with ranked results

You review digest over morning coffee:
  → See 5-15 relevant new postings
  → Pick the ones you want to apply to
  → Fill Google Form

3:00 PM: --sync-forms (automated)
  → Picks up your form submissions
  → Scrapes job descriptions

2:00 AM: --find-only (automated)
  → Prospective recruiters already pre-scraped ✓
  → AI email content generated
  → Outreach scheduled for 9 AM

9:00 AM: --outreach-only (automated)
  → Emails sent within send window
```

---

## ATS Public APIs

Three major ATS platforms offer completely free public APIs
requiring zero authentication for reading job postings.

### Greenhouse
```
Endpoint:  GET https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true
Auth:      None required
Cost:      Free forever
Rate limit: None
Date field: updated_at (last updated — not exact post date)
Returns:   id, title, location, updated_at, absolute_url, description

Example companies:
  Stripe, Doordash, Coinbase, Robinhood, Databricks,
  Snowflake, Pinterest, Okta, Palo Alto Networks,
  Nutanix, DocuSign, Pure Storage, Twilio, Zendesk,
  Figma, Notion, Dropbox, Instacart, Block
```

### Lever
```
Endpoint:  GET https://api.lever.co/v0/postings/{company}?mode=json
Auth:      None required
Cost:      Free forever
Rate limit: None
Date field: createdAt (exact creation timestamp ✓)
Returns:   id, title, location, createdAt, description,
           tags, workplaceType, hostedUrl

Example companies:
  Netflix, Waymo, Cruise, Lyft, Spotify,
  Airtable, Figma (some roles)
```

### Ashby
```
Endpoint:  GET https://api.ashbyhq.com/posting-api/job-board/{company}
Auth:      None required
Cost:      Free forever
Rate limit: None
Date field: publishedAt (exact publish timestamp ✓)
Returns:   title, location, publishedAt, description,
           jobUrl, compensationTier, department

Example companies:
  Linear, Vercel, Loom, Retool, Ramp,
  Notion (some), Modern Treasury, Brex
```

### SmartRecruiters
```
Endpoint:  GET https://api.smartrecruiters.com/v1/companies/{company}/postings
Auth:      None required for public jobs
Cost:      Free
Date field: releasedDate (exact ✓)
Returns:   id, title, location, releasedDate, department, description

Example companies:
  Starbucks, McDonald's, Visa, IKEA, Bosch
```

### Workday
```
Public API: NO — requires authentication token
Approach:   Scrape career page directly with Playwright
Date field: varies by company configuration
            (sometimes shows, sometimes doesn't)

Example companies:
  Google, Apple, Microsoft, Amazon, Meta,
  Tesla, Nvidia, Intel, AMD, Walmart,
  JPMorgan, Goldman Sachs, Salesforce
```

---

## Coverage of 137 Prospective Companies

```
Tier 1 — Free API (daily, ~2 minutes total):
  Greenhouse users: ~40 companies
  Lever users:      ~20 companies
  Ashby users:      ~15 companies
  SmartRecruiters:  ~5 companies
  Subtotal:         ~80 companies (58%)

Tier 2 — Playwright scrape (staggered, ~20 min/day):
  Workday + custom pages: ~57 companies (42%)
  ~11-12 companies/day across 5 days (Mon-Fri)

Total coverage: 100% within 5 days
Fresh postings: caught within 1-5 days
```

---

## ATS Detection

Before querying APIs, the pipeline detects which ATS
each company uses. This is stored in `prospective_companies`
table and refreshed monthly.

```python
Detection order:
  1. Try Greenhouse API → returns jobs? → Greenhouse
  2. Try Lever API      → returns jobs? → Lever
  3. Try Ashby API      → returns jobs? → Ashby
  4. Try SmartRecruiters→ returns jobs? → SmartRecruiters
  5. None matched       → Workday/custom → scrape

Stored in: prospective_companies.ats_platform
Values:    'greenhouse', 'lever', 'ashby',
           'smartrecruiters', 'workday', 'custom', 'unknown'
```

---

## Job Freshness Detection

### The challenge
```
posted_at date not always available (especially Greenhouse, Workday)
Can't reliably determine when a job was originally posted
```

### Solution — first_seen approach
```
Every job URL seen for the first time = "new" = fresh
Job URL seen before = "old" = skip

This works because:
  → New URL in our DB = we haven't seen it before
  → Could have been posted today or 3 days ago
  → Still fresh enough to act on
  → When exact date available (Lever, Ashby) → use it
```

### Preventing re-showing old jobs after cleanup
```
Key design decision:
  Never fully delete job URLs — only archive them
  
  Status flow:
    'new'      → freshly discovered, shown in digest
    'applied'  → you applied → moved to applications table
    'dismissed'→ you chose not to apply
    'expired'  → past freshness window → archived

  On cleanup:
    UPDATE job_postings SET status = 'expired', description = NULL
    WHERE status = 'new' AND first_seen < DATE('now', '-7 days')
    
    DELETE FROM job_postings
    WHERE status = 'applied'
    OR (status = 'dismissed' AND first_seen < DATE('now', '-30 days'))

  On re-detection:
    URL found in DB (any status including 'expired') → SKIP
    URL not in DB → genuinely new → show in digest
    
    Result: same job never shown twice ✓
```

---

## Job Filtering

### Filter configuration (config.py)
```python
# ─────────────────────────────────────────
# JOB MONITORING SETTINGS
# ─────────────────────────────────────────

# Priority 1 — Job titles (required match)
TARGET_JOB_TITLES = [
    "Software Engineer",
    "Software Developer",
    "Backend Engineer",
    "Frontend Engineer",
    "Full Stack Engineer",
    "Full Stack Developer",
    "Web Developer",
    "Platform Engineer",
    "Application Engineer",
]

# Priority 2 — Seniority (required if configured)
# Leave empty [] to include all levels
TARGET_SENIORITY = [
    "Senior",
    "Staff",
    "Principal",
    "Lead",
]

# Priority 3 — Skills (soft filter — affects score)
# Job description should mention at least 1
TARGET_SKILLS = [
    "Python",
    "JavaScript",
    "TypeScript",
    "React",
    "Node.js",
    "AWS",
    "Go",
    "Java",
    "Kubernetes",
    "Docker",
]

# Priority 4 — Location
TARGET_LOCATIONS = [
    "Remote",
    "New York",
    "San Francisco",
    "Seattle",
    "Austin",
    "Boston",
    "Chicago",
]
# True = accept any US location even if not in list above
ACCEPT_ANY_US_LOCATION = True

# Freshness window (days)
JOB_MONITOR_DAYS_FRESH = 3
```

### Matching logic
```python
def matches_job(job):
    """
    Only title is a hard filter.
    Seniority, skills, location affect score only.
    Goal: return up to 70 best-scored jobs for PDF digest.
    """
    title = job["title"].lower()
    description = (job["description"] or "").lower()
    location = (job["location"] or "").lower()

    # Priority 1 — Title (HARD filter — only strict requirement)
    title_match = any(t.lower() in title for t in TARGET_JOB_TITLES)
    if not title_match:
        return False, 0

    score = 10  # base score for title match

    # Priority 2 — Seniority (SOFT — score only)
    if TARGET_SENIORITY:
        if any(s.lower() in title for s in TARGET_SENIORITY):
            score += 5
        # No match = included but ranked lower (not rejected)

    # Priority 3 — Skills (SOFT — score only)
    skill_score = sum(1 for s in TARGET_SKILLS if s.lower() in description)
    score += skill_score * 2

    # Priority 4 — Location (SOFT — score only)
    if any(loc.lower() in location for loc in TARGET_LOCATIONS):
        score += 3
    elif not ACCEPT_ANY_US_LOCATION:
        # Hard reject only if ACCEPT_ANY_US_LOCATION = False
        return False, 0

    return True, score
```

### Relevance scoring (for PDF ordering)
```python
score = 10                                    # base (title matched)
score += skill_score * 2                      # 2 pts per skill match
score += 5 if seniority matched               # seniority bonus
score += 3 if location in TARGET_LOCATIONS    # preferred location bonus
score += 5 if posted/seen today               # freshness bonus
score += 3 if posted/seen yesterday
score += 1 if posted/seen 2-3 days ago

# All matching jobs sorted by score descending
# Top 70 included in PDF digest
# You review full list and decide where to apply
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
    location     TEXT,
    posted_at    TIMESTAMP,    -- from API if available, else NULL
    description  TEXT,         -- cleared on expiry to save space
    skill_score  INTEGER DEFAULT 0,
    status       TEXT DEFAULT 'new',
    first_seen   DATE NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast freshness queries
CREATE INDEX IF NOT EXISTS idx_job_postings_status_seen
ON job_postings(status, first_seen);
```

### prospective_companies table (updated)
```sql
CREATE TABLE IF NOT EXISTS prospective_companies (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    company      TEXT NOT NULL UNIQUE,
    ats_platform TEXT DEFAULT 'unknown',
    -- 'greenhouse', 'lever', 'ashby', 'smartrecruiters',
    -- 'workday', 'custom', 'unknown'
    ats_slug     TEXT,
    -- company slug used in ATS API
    -- e.g. "stripe" for boards-api.greenhouse.io/v1/boards/stripe
    priority     INTEGER DEFAULT 0,
    status       TEXT DEFAULT 'pending',
    scraped_at   TIMESTAMP,
    converted_at TIMESTAMP,
    ats_detected_at TIMESTAMP,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Retention Policy

```
job_postings cleanup (runs in init_db()):

  Archive expired new postings (keep URL, clear description):
    UPDATE job_postings
    SET status = 'expired', description = NULL
    WHERE status = 'new'
    AND first_seen < DATE('now', '-7 days')

  Delete applied (moved to applications table):
    DELETE FROM job_postings WHERE status = 'applied'

  Delete old dismissed:
    DELETE FROM job_postings
    WHERE status = 'dismissed'
    AND first_seen < DATE('now', '-30 days')

Steady state DB size:
  Active (new):     ~500-800 rows with description (~400KB)
  Archive (expired):~18,000 rows after 6 months (~3.6MB)
  Total:            ~4MB for entire job search period
  → Completely negligible for SQLite
```

---

## Daily Digest — PDF Email Attachment

### Format
```
Delivery:  Email with PDF attachment
Subject:   🆕 Job Digest — 68 matches — March 4, 2026
Filename:  jobs_digest_2026-03-04.pdf
Volume:    Up to 70 jobs per digest (sorted by relevance score)
```

### Filter strategy
```
Hard filter (must match):
  → Job title only
    Any job not matching TARGET_JOB_TITLES is excluded

Soft filters (affect score/ranking only):
  → Seniority  → matched = +5 score, not matched = included but ranked lower
  → Skills     → each match = +2 score
  → Location   → preferred location = +3 score
                 any US location included if ACCEPT_ANY_US_LOCATION = True

Result:
  → Wider net than strict filtering
  → Up to 70 best-scored jobs included in PDF
  → You review and decide where to apply
```

### PDF structure
```
Page 1 — Summary
  ┌─────────────────────────────────────────┐
  │  Job Digest — March 4, 2026             │
  │  Companies monitored: 137               │
  │  Total matches found: 68                │
  │  New since yesterday: 12                │
  ├─────────────────────────────────────────┤
  │  TOP 5 TODAY                            │
  │  1. Senior SWE — Google (Remote)        │
  │  2. Senior Backend — Stripe (NYC)       │
  │  3. Staff SWE — Meta (Remote)           │
  │  4. Senior SWE — Netflix (Remote)       │
  │  5. Platform Engineer — Airbnb (SF)     │
  └─────────────────────────────────────────┘

Page 2+ — Full ranked job list (~8-10 jobs/page)
  ┌─────────────────────────────────────────┐
  │  #1  ●●●●●  Score: 28                   │
  │  Senior Software Engineer — Infrastructure│
  │  Google · Remote · Posted: today        │
  │  Skills: Python, Go, Kubernetes, AWS    │
  │  https://careers.google.com/jobs/...    │
  ├─────────────────────────────────────────┤
  │  #2  ●●●●○  Score: 24                   │
  │  Senior Backend Engineer                │
  │  Stripe · New York, NY · Posted: 1d ago │
  │  Skills: Python, AWS, Docker            │
  │  https://stripe.com/jobs/...            │
  ├─────────────────────────────────────────┤
  │  ...                                    │
  └─────────────────────────────────────────┘

Total pages: ~8-9 (cover + 7-8 job pages)
```

### Email body (brief)
```
Subject: 🆕 Job Digest — 68 matches — March 4, 2026

Hi,

Your daily job digest is attached.

  68 jobs matched your profile today
  12 are new since yesterday
  Top match: Senior SWE at Google (Remote)

Review the PDF and fill the Google Form
for any roles you want to apply to.

[jobs_digest_2026-03-04.pdf attached]
```

---

## CLI Commands

```bash
# Run job monitoring + send digest email
python pipeline.py --monitor-jobs

# View today's digest without sending email (terminal output)
python pipeline.py --jobs-digest

# Detect and store ATS platform for all prospective companies
python pipeline.py --detect-ats

# Show monitoring status
python pipeline.py --monitor-status
```

---

## Updated Cron Schedule

```bash
# Daily 8 AM — monitor jobs + send digest
# BEFORE outreach (9 AM) so you see new jobs first thing
0 8 * * * cd /home/ubuntu/mail && \
  source venv/bin/activate && \
  python pipeline.py --monitor-jobs \
  >> logs/monitor_$(date +\%Y-\%m-\%d).log 2>&1
```

---

## Implementation Plan

```
Phase 1 — Core infrastructure:
  db/schema.py      → add job_postings table
                    → add ats_platform to prospective_companies
  db/job_monitor.py → save_job_posting()
                    → get_new_postings()
                    → mark_posting_applied()
                    → cleanup_expired_postings()

Phase 2 — ATS API clients:
  jobs/ats/
    __init__.py
    greenhouse.py   → query Greenhouse public API
    lever.py        → query Lever public API
    ashby.py        → query Ashby public API
    smartrecruiters.py → query SmartRecruiters public API
    workday.py      → Playwright scrape for Workday

Phase 3 — ATS detection:
  jobs/ats_detector.py → auto-detect which ATS each company uses
                        → store in prospective_companies.ats_platform

Phase 4 — Job filtering:
  jobs/job_filter.py   → matches_job() → title/seniority/skills/location
                        → score_job()  → relevance scoring

Phase 5 — Orchestrator:
  jobs/job_monitor.py  → run() → main entry point
                        → query all APIs
                        → filter and score results
                        → save new postings to DB

Phase 6 — Pipeline integration:
  pipeline.py          → add --monitor-jobs flag
                        → add --jobs-digest flag
                        → add --detect-ats flag

Phase 7 — PDF digest generation:
  outreach/report_templates/monitor_report.py
    → Generate PDF using existing pdf skill
    → Page 1: summary (date, counts, top 5)
    → Page 2+: ranked job listings (~8-10 per page)
    → Email PDF as attachment with brief body
    → Save copy to data/digests/jobs_digest_YYYY-MM-DD.pdf

Phase 8 — Tests:
  tests/test_job_monitor.py
    → ATS API response parsing
    → Job filtering logic
    → Freshness detection
    → Duplicate prevention
    → DB retention cleanup
```

---

## File Structure

```
jobs/
  ats/
    __init__.py
    greenhouse.py       → Greenhouse public API client
    lever.py            → Lever public API client
    ashby.py            → Ashby public API client
    smartrecruiters.py  → SmartRecruiters public API client
    workday.py          → Playwright scraper for Workday
  ats_detector.py       → detect ATS platform for each company
  job_filter.py         → filter and score job postings
  job_monitor.py        → orchestrator — run() entry point
  job_fetcher.py        → existing (unchanged)
  form_sync.py          → existing (unchanged)

db/
  job_monitor.py        → DB operations for job_postings table

outreach/
  report_templates/
    monitor_report.py   → PDF digest generation + email with attachment

data/
  digests/              → saved PDF digests (one per day)

tests/
  test_job_monitor.py   → comprehensive tests
```

---

## Key Design Decisions

**Free APIs first, scraping as fallback:**
```
Greenhouse/Lever/Ashby/SmartRecruiters → free REST APIs
→ No Playwright, no quota, instant responses
→ ~80 companies covered in ~2 minutes

Workday + custom pages → Playwright scraping
→ Staggered across week (Mon-Fri, ~11 companies/day)
→ ~20 minutes/day
→ Zero CareerShift quota used
```

**first_seen as universal freshness signal:**
```
Works for ALL companies regardless of ATS
Never resets (URL archived not deleted)
Prevents same job appearing twice ever
Simple and 100% reliable
```

**Archive not delete:**
```
Expired URLs kept as 'expired' status (no description)
~4MB total for 6-month job search
Guarantees no re-showing of old jobs
Clean slate available after job search ends
```

**Score-based digest ordering:**
```
Not alphabetical or random
Most relevant jobs appear first
Based on: title match + seniority + skill count + location + freshness
You see the best matches immediately
```