# Recruiter Verification & Data Freshness

## Overview

Recruiter data goes stale over time — people change jobs, get promoted, or
leave companies. This document covers how the pipeline keeps recruiter data
fresh and what happens when recruiters become inactive.

The key insight: **CareerShift profile re-visits are free** (served from cache).
This means verification costs zero CareerShift quota regardless of how many
recruiters are checked.

---

## The Problem With Stale Data

```
Day 0:   hcruz@collective.com inserted — active recruiter
Day 45:  Heidi leaves Collective for another company
Day 90:  Pipeline sends outreach to hcruz@collective.com
         → Hard bounce or email to wrong person
         → Damages sender reputation
         → Wasted outreach opportunity
```

Without proactive verification, recruiter data becomes unreliable over time
regardless of whether you're actively applying to new jobs.

---

## Two-Layer Verification Strategy

### Layer 1 — `--find-only` (existing behavior)
Runs tiered verification as Step 1 every time `--find-only` executes.
Tied to job search activity — only runs when adding new applications.

### Layer 2 — `--verify-only` (new)
Runs tiered verification independently of job search activity.
Ensures recruiter data stays fresh even during quiet periods.

```
Together they provide complete coverage:
  --verify-only  → proactive weekly freshness check
  --find-only    → verification on active application days
  --outreach-only→ bounce detection catches missed cases
```

---

## Tiered Verification System

All verification uses cached profiles — zero CareerShift quota used.

### Tier 1 — Trust (verified < 30 days ago)
```
Action:  Skip entirely
Cost:    0 seconds, 0 quota
Reason:  Recently verified, very unlikely to have changed
```

### Tier 2 — Lightweight check (verified 30-60 days ago)
```
Action:  Search company page, check if recruiter name appears
Cost:    ~7-11 seconds per recruiter, 0 quota
Outcome:
  Name found   → update verified_at → falls back to Tier 1
  Name missing → escalate to Tier 3
```

### Tier 3 — Full profile visit (verified > 60 days ago)
```
Action:  Visit cached profile, check company/email/title
Cost:    ~15-26 seconds per recruiter, 0 quota
Outcome:
  Still at company → update email/title if changed
                   → update verified_at → falls back to Tier 1
  Left company     → mark recruiter_status = inactive
                   → cancel all pending outreach
```

### Tier 0 — Bounce detection (always active)
```
Action:  Triggered automatically during email sending
Cost:    0 (detected during normal outreach)
Outcome: Hard bounce → mark inactive → cancel pending outreach
Benefit: Catches job changes even within 30-day Tier 1 window
```

---

## `--verify-only` Command

### What it does
```
Step 1: Run full tiered verification for all active recruiters
Step 2: Check for under-stocked companies after verification
Step 3: Print summary report
```

### Under-stocked detection
After verification marks some recruiters inactive, companies that drop
below MIN_RECRUITERS_PER_COMPANY are flagged automatically:

```
Example:
  Collective had 3 active recruiters → fully stocked
  Tier 3 verification: Heidi left → marked inactive
  Active count drops to 2 → still OK (above MIN=2)

  Stripe had 2 active recruiters
  Tier 3 verification: both left → marked inactive
  Active count drops to 0 → UNDER-STOCKED → flagged

Output:
  [WARNING] 1 company under-stocked after verification:
    - Stripe: 0 active recruiters (needs 2 more)
  [INFO] Run --find-only to top up under-stocked companies
```

### Under-stocked top-up flow
```
After --verify-only flags under-stocked companies:

  --find-only (next run):
    Step 1: Tiered verification (again — belt and suspenders)
    Step 2: Scrape companies needing new recruiters
            → Includes under-stocked companies automatically
            → get_unique_companies_needing_scraping() 
              picks up companies with < MIN_RECRUITERS active
    Step 3: Leftover quota tops up remaining gaps
```

No manual intervention needed — under-stocked companies are
automatically picked up by the next `--find-only` run.

### Time estimate
```
Tier 1 (skip):         0 seconds
Tier 2 per recruiter: ~10 seconds
Tier 3 per recruiter: ~20 seconds

20 companies × 2 recruiters = 40 recruiters:
  All Tier 2: ~7 minutes
  All Tier 3: ~13 minutes
  Mixed:      ~10 minutes

30 companies × 2 recruiters = 60 recruiters:
  All Tier 2: ~10 minutes
  All Tier 3: ~20 minutes
  Mixed:      ~15 minutes
```

---

## Recommended Schedule

```
Daily  (9 AM):  python pipeline.py --outreach-only
                → sends scheduled emails
                → bounce detection catches stale recruiters

Weekly (Monday):python pipeline.py --verify-only
                → proactive freshness check
                → flags under-stocked companies

As needed:      python pipeline.py --add
                → add new job application
                python pipeline.py --find-only
                → verify existing + scrape new + generate content
                → tops up under-stocked companies from --verify-only
```

---

## What Happens to Outreach When Recruiter Goes Inactive

```
Recruiter marked inactive (any trigger):
  1. recruiter_status = 'inactive'
  2. All pending outreach → status = 'cancelled'
  3. No further emails sent to this recruiter
  4. Active recruiter count for company decreases
  5. Company becomes eligible for top-up on next --find-only
```

---

## Recruiter Status Values

```
'active'   → verified, included in outreach
'inactive' → left company / bounced / manually deactivated
             excluded from all outreach and verification
```

---

## Verification Configuration

```python
# config.py

# Tiered verification thresholds (days)
TIER1_DAYS = 30   # trust — skip verification
TIER2_DAYS = 60   # lightweight search check
# > TIER2_DAYS  → full profile visit (Tier 3)

# Minimum recruiters per company before flagging under-stocked
MIN_RECRUITERS_PER_COMPANY = 2
```

---

## DB Columns Relevant to Verification

```sql
recruiters table:
  verified_at       — timestamp of last successful verification
                      updated by Tier 2/3 checks and at insert time
  recruiter_status  — 'active' or 'inactive'
  last_scraped_at   — when company was last scraped on CareerShift
  used_search_terms — JSON array of HR terms already tried

applications table:
  status            — 'active', 'exhausted', 'closed'
  exhausted_at      — timestamp when marked exhausted
  expected_domain   — extracted from job URL at --add time
                      used as domain reference during scraping
```