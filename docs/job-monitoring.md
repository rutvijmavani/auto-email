# Job Monitoring Pipeline

## Overview

The job monitoring pipeline automatically scans career pages of all companies
in your target list daily, finds newly posted jobs matching your profile, and
delivers a PDF digest to your inbox every morning at 8 AM. The goal is to
apply to positions within hours of posting — before competition builds up and
while the recruiter is actively reviewing applications.

---

## Why Early Application Matters

```text
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
```text

This pipeline is the entry point of the entire system.
If it fails to detect new jobs early, every downstream
pipeline (recruiter finding, outreach, response) loses
effectiveness. Maximum reliability is required.

---

## Architecture

### Data Flow

```text
prospects.txt (137 companies)
         ↓
--import-prospects
         ↓
prospective_companies table (DB source of truth)
         ↓
--monitor-jobs (daily 8 AM)
         ↓
ATS Detection (one-time per company, cached in DB)
  Phase 1: ATS sitemap lookup (free, instant)
  Phase 2: ATS API name probe (free, ~50ms)
  Phase 3a: HTML + redirect scan (free, ~100ms)
  Phase 3b: Serper API (Workday + Oracle only, 2500 free credits)
         ↓
API calls (Greenhouse/Lever/Ashby/SmartRecruiters/Workday/OracleHCM)
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
```text

---

## ATS Detection Architecture

The pipeline uses a 4-phase detection approach to identify which ATS
each company uses. Detection runs once per company and results are
cached in the database indefinitely.

### Phase 1 — Sitemap Lookup (Free, Instant)

Greenhouse, Lever, and Ashby expose public sitemaps listing every
company on their platform. We download once and cache for 7 days.

```text
https://boards.greenhouse.io/sitemap.xml
https://jobs.lever.co/sitemap.xml
https://jobs.ashbyhq.com/sitemap.xml
```

The sitemap is standard XML listing all job board URLs:

```xml
<urlset>
  <url>
    <loc>https://boards.greenhouse.io/stripe/jobs/123</loc>
  </url>
  <url>
    <loc>https://boards.greenhouse.io/airbnb/jobs/456</loc>
  </url>
</urlset>
```

We extract slugs (`stripe`, `airbnb`) and match against company names
using `validate_slug_for_company()` with word boundary rules.

Expected coverage: ~60% of companies.

### Phase 2 — ATS API Name Probe (Free, ~50ms)

For companies not found in sitemap, probe ATS APIs directly with
slug variants and verify the returned company name.

```text
GET boards-api.greenhouse.io/v1/boards/{slug}
→ {"name": "Stripe", "jobs": [...]}
→ All company keywords present → ACCEPT ✓

GET boards-api.greenhouse.io/v1/boards/charles
→ {"name": "Charles River Analytics"}
→ Missing keyword "schwab" → REJECT ✓
```

404 = definitively not on this platform.
200 + name match = confirmed detection.

No fuzzy matching — uses deterministic keyword presence check:
all significant company keywords must appear in the API name.

Expected coverage: additional ~15%.

### Phase 3a — HTML + Redirect Scan (Free, ~100ms)

Fetches the company career page and checks two signals:

```text
Signal 1 — Redirect URL:
  GET capitalone.com/careers
  → redirects to capitalone.wd12.myworkdayjobs.com/Capital_One
  → Extract platform + slug from final URL ✓

Signal 2 — HTML fingerprint:
  Scan page source for ATS domain patterns:
  "myworkdayjobs.com", "greenhouse.io", etc.
  → Extract slug from embed URL ✓
```

Works for non-JavaScript-rendered pages (~30% of remaining companies).

Expected coverage: additional ~10%.

### Phase 3b — Serper API (2500 Free Credits)

Only runs when Phases 1-3a all fail AND company is not in
`KNOWN_CUSTOM_ATS`. Searches Google via Serper for Workday and
Oracle HCM tenants — the two platforms that cannot be discovered
through public APIs or sitemaps.

```text
Query 1: "capital one site:myworkdayjobs.com"
→ capitalone.wd12.myworkdayjobs.com/Capital_One ✓

Query 2: "jpmorgan site:fa.oraclecloud.com"
→ jpmc.fa.oraclecloud.com/hcmUI/.../CX_1001 ✓
```

Cost: 2 queries per company. 2500 free credits on signup covers
1250 companies. Email alert sent when fewer than 50 credits remain.

Expected coverage: additional ~10%.

### Phase 4 — Unknown / Custom ATS

Companies known to use fully custom ATS platforms are stored
immediately without consuming Serper credits:

```text
KNOWN_CUSTOM_ATS = {
    Amazon, Apple, Google, Meta,
    Microsoft, Netflix, Uber, Lyft
}
```

Any remaining undetected companies stored as `unknown` and
retried after 14 consecutive empty days.

### Detection Accuracy

| Approach | Coverage | Accuracy |
|---|---|---|
| Sitemap | ~60% | ~99% (ground truth) |
| API probe | ~15% | ~97% (name verification) |
| HTML redirect | ~10% | ~95% (URL pattern) |
| Serper | ~10% | ~95% (slug validation) |
| Unknown/Custom | ~5% | N/A |

### Re-detection Triggers

| Trigger | Action |
|---|---|
| `consecutive_empty_days >= 14` | Re-run all 4 phases |
| `ats_platform = unknown` | Always re-run |
| `ats_platform = custom` | Always re-run |
| Manual override (`--override`) | Never re-detect |
| `ats_platform = unsupported` | Never re-detect (iCIMS etc.) |

---

## ATS Platform Support

### Free Public APIs (no authentication required)

#### Greenhouse
```text
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
```text

#### Lever
```text
Endpoint:   GET https://api.lever.co/v0/postings/{slug}?mode=json
Auth:       None
Cost:       Free forever
Rate limit: None documented
Date field: createdAt — RELIABLE (Unix timestamp, never changes)
Response:   id, text, createdAt, categories, descriptionPlain, hostedUrl
Companies:  Netflix, Waymo, Cruise, Lyft, Spotify, Airtable and more

Freshness:  createdAt used directly for date comparison ✓
```text

#### Ashby
```text
Endpoint:   GET https://api.ashbyhq.com/posting-api/job-board/{slug}
Auth:       None
Cost:       Free forever
Rate limit: None documented
Date field: publishedAt — RELIABLE (original publish date)
Response:   id, title, publishedAt, location, descriptionHtml, jobUrl
Companies:  Linear, Vercel, Loom, Retool, Ramp, Brex,
            Modern Treasury and more

Freshness:  publishedAt used directly for date comparison ✓
```text

#### SmartRecruiters
```text
Endpoint:   GET https://api.smartrecruiters.com/v1/companies/{slug}/postings
Auth:       None for public jobs
Cost:       Free
Date field: releasedDate — RELIABLE
Response:   id, title, location, releasedDate, department, description
Companies:  Starbucks, Visa, Bosch and more

Freshness:  releasedDate used directly for date comparison ✓
```text

#### Workday (undocumented public API)
```text
Endpoint:   GET https://{company}.{wd}.myworkdayjobs.com/wday/cxs/
                {company}/{path}/jobs
Auth:       None required (used by Workday career page frontend)
Cost:       Free
Risk:       Undocumented — could change without notice
Date field: postedOn — RELIABLE (original posting date)
Response:   title, postedOn, locationsText, externalUrl, bulletFields
Companies:  Capital One (wd12), Goldman Sachs, Citibank, BlackRock,
            Mastercard, American Express, Morgan Stanley, Visa,
            Wells Fargo, Bank of America, Charles Schwab, State Street
Variants:   wd1-wd8, wd10, wd12 tried during detection
Path:       Varies per company — discovered via Google search
            e.g. Capital One → /Capital_One (not /careers)

Freshness:  postedOn used directly for date comparison ✓
```text

#### Oracle HCM Cloud
```text
Endpoint:   GET https://{slug}.fa.oraclecloud.com/hcmRestApi/resources/
                latest/recruitingCEJobRequisitions?
                finder=CandidateExperience&
                CandidateExperienceId={site_id}
Auth:       None required (public job postings API)
Cost:       Free
Date field: PostedDate — RELIABLE (original posting date) ✓✓✓
Response:   Title, PostedDate, PrimaryLocation, ExternalJobId
Companies:  JPMorgan Chase (jpmc/CX_1001), Goldman Sachs,
            and other enterprise/financial companies
Discovery:  slug + site_id extracted from Google search result
            Cannot be guessed — requires Google detection

Freshness:  PostedDate used directly for date comparison ✓
```text

### Coverage Summary
```text
Greenhouse:      ~40 companies  (100% confidence via Google ✓)
Lever:           ~20 companies  (100% confidence via Google ✓)
Ashby:           ~15 companies  (100% confidence via Google ✓)
SmartRecruiters: ~30 companies  (verified via Google ✓)
Workday:         ~20 companies  (slug+path via Google ✓)
Oracle HCM:      ~10 companies  (slug+site_id via Google ✓)
─────────────────────────────────────────────────────────
Via Google+API:  ~135 companies (99%)

Unknown (~2%):
  → Custom career pages (Meta, Google, Apple, Amazon)
  → Email notification → manual --override
```text

---

## ATS Detection — Google Search + API Verification

### Philosophy
```text
Google already knows the correct ATS URL for every company.
"capital one site:myworkdayjobs.com" returns:
  capitalone.wd12.myworkdayjobs.com/Capital_One/jobs
  → slug=capitalone, wd=wd12, path=Capital_One

No guessing needed. No slug variants. No false positives.
API-only approach proved unreliable — SmartRecruiters
accepts any slug and returns empty responses, causing
112/134 companies to falsely detect as SmartRecruiters.
```text

### How it works
```text
For each company (e.g. "Capital One"):

Phase 1 — Google search (PRIMARY):
  For each ATS platform, search:
    "Capital One site:myworkdayjobs.com"
    "Capital One site:boards.greenhouse.io"
    "Capital One site:jobs.lever.co"
    ... (7 platforms total)

  Extract URLs from results.
  Match against ATS URL patterns.

  CRITICAL: validate company name in URL slug:
    capitalone.wd12... contains "capital" ✓ → accept
    openfx.greenhouse... no "capital" ✗   → reject

  First valid match → stop, move to Phase 2.

Phase 2 — Validate and store:
  Take Google result: workday, capitalone, wd12, Capital_One
  → Check platform is in ATS_REGISTRY (supported)
  → If supported → _store_detection(company, platform, slug)
  → If unsupported (iCIMS, SuccessFactors etc.) → store as "custom"

Phase 3 — API buffer fallback:
  Only runs if Google finds nothing
  Tries all slugs × all platforms via direct API
  Scores responses with confidence formula
  classify: detected / close_call / unknown

Phase 4 — Unknown:
  Email notification with best attempt
  User provides --override
```text

### Google search queries
```text
Platform        Query format
──────────────────────────────────────────────────
Greenhouse:     "{company} site:boards.greenhouse.io"
Greenhouse:     "{company} site:job-boards.greenhouse.io"
Lever:          "{company} site:jobs.lever.co"
Ashby:          "{company} site:jobs.ashbyhq.com"
SmartRec:       "{company} site:jobs.smartrecruiters.com"
Workday:        "{company} site:myworkdayjobs.com"
Oracle HCM:     "{company} site:oraclecloud.com"
iCIMS:          "{company} site:icims.com"     → stored as "custom" (no fetch module)
SuccessFactors: "{company} site:successfactors.com" → stored as "custom"

Platforms not listed (e.g. Taleo, Jobvite, Workable):
  → If found by Google → stored as "custom" (out of scope for now)
  → Use --override to manually set if company is critical

Autocorrect disabled: &nfpr=1
```text

### URL slug validation (prevents false positives)
```text
After Google returns URLs, validate slug belongs to company:

  "jp morgan site:boards.greenhouse.io"
  → returns: boards.greenhouse.io/usenourish (Nourish)
  → slug "usenourish" checked against keywords ["jp","morgan"]
  → "jp" not in "usenourish" → REJECTED ✓

  "capital one site:myworkdayjobs.com"
  → returns: capitalone.wd12.myworkdayjobs.com/Capital_One
  → slug "capitalone"+"Capital_One" checked against ["capital","one"]
  → "capital" in "capitaloneCapital_One" → ACCEPTED ✓

Known aliases handled:
  Meta     → also check "facebook" slug
  Block    → also check "squareup", "square"
  JPMorgan → also check "jpmc", "jpmorganchase"
  X        → also check "twitter"
```text

### Rate limiting and CAPTCHA handling
```text
Playwright browser with realistic user agent.
3-5 second human-like delay between searches.
Browser restarted every 50 companies (memory cleanup).
CAPTCHA detected → wait 120s → retry once → skip.
Progress saved per company → auto-resume if interrupted.
```text

### Scoring formula
```text
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
```text

### Keyword extraction
```text
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
```text

### Detection status values
```text
detected    → high confidence, auto-accepted silently
close_call  → auto-selected by tie-break, email sent to verify
unknown     → low confidence, email sent for manual review
manual      → manually overridden by user, never auto-re-detected
```text

### Re-detection triggers
```text
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
```text

### Empty day counter reset (critical behavior)
```text
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
```text

### Detection summary email
```text
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
```text

### Monitorable companies
```text
--monitor-jobs only processes companies with confirmed ATS:
  ats_platform NOT NULL
  ats_platform != 'unknown'
  ats_slug NOT NULL

Unknown companies are SKIPPED in daily monitoring.
"X companies with unknown ATS — run --detect-ats" shown in output.
```text

---

## Job Filtering

### Filter priority
```text
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
```text

### Relevance scoring
```python
score = 10                          # base (title matched)
score += 5  if seniority matched    # seniority bonus
score += skill_count * 2            # 2pts per skill match
score += 5  if posted today         # freshness bonus
score += 3  if posted yesterday
score += 1  if posted 2-3 days ago

# Jobs sorted by score DESC within each company group
```text

---

## Freshness Detection

### The Greenhouse problem
```text
Greenhouse updated_at changes on every edit:
  → Job posted 6 months ago, edited yesterday
  → updated_at = yesterday
  → Looks fresh but is actually old

Solution: Do NOT use updated_at for freshness
  → Use first_seen + content_hash instead
```text

### Two-layer deduplication
```text
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
```text

### Date-based freshness (when reliable date available)
```text
Lever, Ashby, SmartRecruiters, Workday:
  posted_at available and reliable
  → If (today - posted_at) > JOB_MONITOR_DAYS_FRESH
    → Store as 'pre_existing', don't show in digest

Greenhouse:
  updated_at unreliable
  → Rely on first_seen only
  → No date-based freshness check
```text

### First run per company
```text
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
```text

---

## Edge Cases

### ATS Detection
```text
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
```text

### API Calls
```text
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
```text

### Job Data
```text
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
```text

### PDF Generation
```text
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
```text

---

## Performance Metrics

### Why metrics are critical
```text
This is the entry pipeline. If it fails silently:
  → You never see relevant job postings
  → You apply late → lower response rate
  → Every downstream pipeline loses effectiveness

Metrics catch silent failures before they compound.
```text

### Metrics tracked per run

#### Metric 1 — Detection Coverage
```text
Formula: companies_with_results / total_monitored
Target:  ≥ 70%
Alert:   < 70% for 3 consecutive days
Meaning: How many companies returned at least 1 job?
         Low value = ATS detection issues or
         widespread API failures
```text

#### Metric 2 — ATS Known Rate
```text
Formula: companies_with_known_ats / total_companies
Target:  ≥ 80%
Alert:   < 80%
Meaning: What % of companies have confirmed ATS?
         Low value = many companies unmonitored
```text

#### Metric 3 — Filter Match Rate
```text
Formula: jobs_matched_filters / total_jobs_fetched
Target:  5% - 40%
Alert:   < 5% (filters too strict)
         > 60% (filters too loose)
Meaning: Are our title/skill filters calibrated?
```text

#### Metric 4 — New Job Rate
```text
Formula: new_jobs_found / total_jobs_fetched
Target:  varies by season
Meaning: Hiring activity indicator
         Low in Nov-Dec (off-season) → normal
         Low in Jan-Mar (peak) → pipeline issue
```text

#### Metric 5 — Pipeline Reliability
```text
Formula: successful_runs / total_runs (last 7 days)
Target:  ≥ 90%
Alert:   < 90%
Meaning: How often does --monitor-jobs complete?
         Tracks VM stability and API availability
```text

#### Metric 6 — API Failure Rate
```text
Formula: api_failures / total_api_calls
Target:  < 10%
Alert:   > 10% for 3 consecutive days
Meaning: Are APIs degrading or rate limiting us?
```text

#### Metric 7 — Application Conversion Rate (future)
```text
Formula: jobs_applied / jobs_shown_in_digest
Target:  TBD after data collected
Meaning: Quality of job recommendations
         Requires tracking which digest jobs
         you actually applied to
```text

#### Metric 8 — Time to Apply (future)
```text
Formula: avg(applied_date - posted_date) in hours
Target:  < 24 hours
Meaning: Are you applying fast enough?
         Measures effectiveness of early
         application strategy
```text

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
```text

### Alert delivery
```text
In PDF report header (every run):
  → Coverage rate
  → ATS known rate
  → API failures list
  → Any threshold breaches

Weekly summary email (Mondays):
  → 7-day trend for all metrics
  → Filter calibration suggestion if needed
  → Pipeline reliability score
```text

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
```text

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
```text

### Retention policy
```text
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
```text

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
```text

---

## PDF Digest Format

```text
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
```text

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
```text

---

## File Structure

```text
jobs/
  ats/
    __init__.py
    base.py              → shared HTTP logic, retry, timeout
    patterns.py          → ATS URL patterns + slug validation
    greenhouse.py        → Greenhouse API client
    lever.py             → Lever API client
    ashby.py             → Ashby API client
    smartrecruiters.py   → SmartRecruiters API client
    workday.py           → Workday API client
    oracle_hcm.py        → Oracle HCM API client (JPMorgan etc.)
  google_detector.py     → Google search + URL extraction
  ats_detector.py        → detection orchestrator (Google+API)
  job_filter.py          → filter + score jobs
  job_monitor.py         → run() entry point
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
```text

---

## Cron Schedule (updated)

```bash
# Daily 8 AM — monitor jobs + send PDF digest
0 8 * * * cd /home/ubuntu/mail && \
  source venv/bin/activate && \
  python pipeline.py --monitor-jobs \
  >> logs/monitor_$(date +\%Y-\%m-\%d).log 2>&1
```text

---

## Implementation Plan

```text
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
```text

---

## Known Limitations

```text
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
```text