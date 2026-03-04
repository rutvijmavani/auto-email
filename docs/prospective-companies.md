# Prospective Companies

## Overview

Prospective companies are target companies you plan to apply to but haven't
yet. The pipeline pre-scrapes their recruiter data during quiet quota periods
so that when you do apply, outreach can begin immediately with zero additional
CareerShift quota cost.

---

## The Problem It Solves

### Heavy application days run out of quota

```
Daily CareerShift quota: 50 profiles
Max recruiters per company: 3

Heavy day (30 applications):
  Need: 30 × 3 = 90 profiles
  Have: 50 profiles
  Gap:  40 profiles → 13+ companies never get recruiters
        → Outreach pipeline starved for those companies
        → Missed opportunities
```

### Quiet days waste quota

```
Quiet day (10 applications):
  Need: 10 × 3 = 30 profiles
  Have: 50 profiles
  Leftover: 20 profiles → wasted

Monthly quota: 50 × 30 = 1,500 profiles
Ideal monthly capacity: 1,500 / 3 = 500 companies
If only applying to 200/month → 300 companies worth of quota wasted
```

### The fix — build a recruiter reserve

```
Quiet days: use leftover quota to pre-scrape
            prospective companies

Heavy days: prospective companies already have
            recruiters → zero quota needed
            → all 50 quota goes to NEW companies

Result: heavy days covered without quota deficit
```

---

## How It Works

### Adding prospective companies

```bash
# Bulk import from text file (one company per line)
python pipeline.py --import-prospects prospects.txt

# prospects.txt example:
Google
Meta
Apple
Microsoft
Stripe
Netflix
Uber
Airbnb
Spotify
LinkedIn
Figma
Notion
Vercel
...
```

### Automatic scraping during --find-only

```
Step 3 in --find-only (leftover quota utilization):

  Priority 1: Top up under-stocked active companies
    → Companies you applied to with < MIN_RECRUITERS
    → Most urgent — outreach is waiting

  Priority 2: Pre-scrape prospective companies
    → Only runs if quota remains after Priority 1
    → Sorted by priority score (set at import time)
    → Max 3 recruiters per company
    → Stops when quota = 0
```

### Converting prospective → active

```
You apply to a prospective company:
  python pipeline.py --add
    Company: Google
    Job URL: https://careers.google.com/jobs/123

  System detects Google already in prospective DB:
    → Recruiters already scraped ✓
    → Status: prospective → active
    → Outreach scheduled immediately
    → Zero CareerShift quota used ✓
    → No waiting for overnight --find-only run
```

---

## Why Big Tech Works Perfectly

Prospective list is designed for well-known large companies:

```
Large employee base:
  → CareerShift has hundreds of their employees
  → Search "Google" + "Recruiter" → only Google results
  → No noisy data from similarly named companies
  → Confidence score near 100% every time

Consistent email domains:
  → All Google recruiters → @google.com
  → All Stripe recruiters → @stripe.com
  → Buffer sees all same domain → insert all
  → No domain tiebreaker ever needed

Examples:
  Google    → @google.com      ✓ clean
  Meta      → @fb.com          ✓ clean (consistent even if not @meta.com)
  Apple     → @apple.com       ✓ clean
  Microsoft → @microsoft.com   ✓ clean
  Stripe    → @stripe.com      ✓ clean
  Netflix   → @netflix.com     ✓ clean
```

No `expected_domain` needed for prospective companies — buffer
consistency check alone is sufficient for big tech.

---

## Status Flow

```
'pending'   → added to prospective list, not yet scraped
'scraped'   → recruiters found and stored in DB
'converted' → you applied → status moved to active application
'exhausted' → CareerShift couldn't find recruiters
              (rare for big tech)
```

---

## Quota Distribution with Prospective

### Example — quiet day

```
Applications today:     10
Quota available:        50
Distribution (Step 2):
  base = 50 // 10 = 5 → capped at 3
  All 10 companies get 3 recruiters = 30 quota used
  Remaining: 20 quota

Step 3 Priority 1 (under-stocked active):
  All active companies fully stocked = 0 quota used

Step 3 Priority 2 (prospective):
  20 quota remaining
  20 // 3 = 6 prospective companies scraped
  (first 6 in priority order get 3 recruiters each)
  Remaining: 2 quota → saved for tomorrow
```

### Example — heavy day

```
Applications today:     30
Quota available:        50
Distribution (Step 2):
  base = 50 // 30 = 1, extra = 20
  (20 × 2) + (10 × 1) = 50 quota used
  Remaining: 0

BUT 8 of those 30 companies were already prospective:
  → 8 companies used 0 quota (already scraped)
  → Only 22 new companies needed scraping
  → Distribution: calculate_distribution(50, 22)
  → base = 50 // 22 = 2, extra = 6
  → (6 × 3) + (16 × 2) = 18 + 32 = 50
  → All 22 new companies covered ✓
  → Total: 30 companies covered with 50 quota ✓
```

---

## DB Schema

### prospective_companies table

```sql
CREATE TABLE IF NOT EXISTS prospective_companies (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company     TEXT NOT NULL UNIQUE,
    priority    INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'pending',
    scraped_at  TIMESTAMP,
    converted_at TIMESTAMP,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Recruiter storage

```
Prospective recruiters stored in existing recruiters table
with a placeholder application linked:

applications table:
  company:  "Google"
  job_url:  "prospective://google"  (placeholder)
  status:   'prospective'

recruiters table:
  company:  "Google"
  email:    "john@google.com"
  confidence: "auto"
  recruiter_status: "active"

When converted:
  → Real job URL replaces placeholder
  → status: 'prospective' → 'active'
  → expected_domain extracted from real URL
  → Outreach scheduled immediately
```

---

## Configuration

```python
# config.py

# ─────────────────────────────────────────
# RECRUITER THRESHOLDS
# ─────────────────────────────────────────
MIN_RECRUITERS_PER_COMPANY = 1   # minimum to start outreach
                                  # flagged as under-stocked below this
MAX_CONTACTS_HARD_CAP      = 3   # target ceiling per company
                                  # prospective companies scraped to this target
```

### Threshold design

```
MIN = 1: ensures outreach starts as soon as 1 recruiter found
         critical on heavy application days (40 companies, 50 quota)
         all companies get ≥ 1 recruiter from distribution

MAX = 3: prospective companies pre-scraped to this target
         active companies topped up to this over time via leftover quota
         never visit more than MAX profiles per company per run
```

---

## CLI Commands

```bash
# Bulk import from text file
python pipeline.py --import-prospects prospects.txt

# Check prospective pipeline status
python pipeline.py --prospects-status

# Output example:
# Prospective Companies Status
# ─────────────────────────────────────────
# pending:   45 companies (not yet scraped)
# scraped:   12 companies (recruiters ready)
# converted: 8  companies (now active)
# exhausted: 2  companies (no data found)
#
# Total recruiters pre-scraped: 36
# Estimated quota saved on heavy days: ~36 profiles
```

---

## prospects.txt Format

```
# One company name per line
# Lines starting with # are comments
# Blank lines ignored

# Big Tech
Google
Meta
Apple
Microsoft
Amazon
Netflix

# Fintech
Stripe
Plaid
Robinhood
Coinbase

# SaaS / Dev Tools
Figma
Notion
Vercel
Linear
Datadog
```

---

## Complete Flow Summary

```
Quiet period (building reserve):

  python pipeline.py --import-prospects prospects.txt
    → 50 companies added to prospective_companies table
    → status = 'pending'

  --find-only runs nightly:
    Step 2: Scrape today's new applications
    Step 3: Priority 1 → top up active companies
            Priority 2 → scrape pending prospective companies
                         using remaining quota
    → Gradually builds recruiter reserve

Heavy application period:

  You apply to Google:
    python pipeline.py --add
      → Detects Google already prospective + scraped
      → status: prospective → active
      → Outreach scheduled immediately
      → 0 CareerShift quota used

  New unknown company:
    → Normal scraping flow
    → Quota focused only on genuinely new companies
    → Heavy day covered without deficit
```