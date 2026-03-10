# Quota Management

## Overview

The pipeline manages three quotas:

| Quota | Limit | Resets |
|---|---|---|
| CareerShift profile views | 50 new contacts/day | Daily |
| Gemini AI calls | 40 calls/day (20 per model) | Daily |
| Serper API credits | 2500 total (one-time) | Never |
| Brave Search API calls | 1000/month (hard stop: 950) | Monthly |
| AWS Athena queries | ~$0.00024/query | Pay-per-use |

Both quotas are tracked locally in the database and synced with real values at runtime.

---

## CareerShift Quota

### How it works

CareerShift allows viewing up to **50 new contact profiles per day**. The key word is **new** — re-visiting a previously viewed profile is served from cache and does not count against the limit.

This means:
- First visit to a profile → costs 1 credit
- Re-visit same profile → free (cached)
- Tiered recruiter verification → free (all cached profiles)

### Quota tracking

At the start of every `--find-only` run, the pipeline navigates to the CareerShift account page and reads the real remaining quota. This syncs the local `careershift_quota` table with the actual value, accounting for any manual browsing done outside the pipeline.

### Quota distribution

When multiple new companies need recruiters, the quota is distributed fairly:

```
base = remaining_quota // new_companies
extra = remaining_quota % new_companies

First `extra` companies get base + 1 contacts
Remaining companies get base contacts
Total = remaining_quota (fully utilized)
Max per company capped at MAX_CONTACTS_HARD_CAP (3)
```

Example with 50 quota and 20 new companies:
```
base = 50 // 20 = 2
extra = 50 % 20 = 10
→ first 10 companies get 3 contacts
→ last 10 companies get 2 contacts
→ total used = 50
```

Example with 50 quota and 40 new companies (heavy day):
```
base = 50 // 40 = 1
extra = 50 % 40 = 10
→ first 10 companies get 2 contacts
→ last 30 companies get 1 contact
→ total used = 50

With MIN_RECRUITERS_PER_COMPANY = 1:
  → All 40 companies get ≥ 1 recruiter ✓
  → Outreach starts immediately for all 40 ✓
  → Leftover quota on future days tops up to MAX (3)
  → Prospective pipeline reduces new companies needed
```

### Leftover quota utilization

After scraping new companies, any remaining quota is used to top up under-stocked companies:

```
Priority 1: Companies with 1 recruiter  (most urgent)
Priority 2: Companies with 2 recruiters (top up to 3)

Scoring: (MAX_CONTACTS_HARD_CAP - recruiter_count) × recency_weight
→ Companies with fewer recruiters AND more recent applications get priority
```

**Smart search term tracking:** Each company tracks which HR search terms have already been tried (`used_search_terms` column). On subsequent runs, only untried terms are used — preventing redundant searches.

**Duplicate prevention:** Before visiting any profile, the email is checked against the database. If already known, the profile visit is skipped — saving quota.

### 3-pass search strategy

For each company, the scraper tries three passes in order:

```
Pass 1: HR title filter + RequireEmail + exclude senior titles (ideal)
Pass 2: HR title filter + RequireEmail + include senior titles (fallback)
Pass 3: No filters + exclude senior titles                     (last resort)
```

Senior titles excluded in Pass 1 and 3:
CEO, CTO, COO, CFO, CMO, CIO, Founder, President, Board Member, EVP, SVP, VP

---

## Gemini AI Quota

### How it works

Gemini AI is used to generate personalized email content. The pipeline uses two models with a primary/fallback pattern:

| Model | Daily limit | Role |
|---|---|---|
| `gemini-2.5-flash-lite` | 20 calls | Primary |
| `gemini-2.5-flash` | 20 calls | Fallback |

Each application requires **one AI call** which generates all three email stages at once (initial, follow-up 1, follow-up 2).

### Cache strategy

Generated content is cached in the `ai_cache` table for 21 days — exactly covering the full outreach cycle (3 emails × 7 days). The cache key is a SHA256 hash of `company + job_title + job_text`.

On re-runs, cached content is returned immediately without an API call. This means after the first generation, subsequent `--find-only` runs for the same application use zero Gemini quota.

### Fallback generation

If job description scraping fails, the pipeline falls back to role-based generation using only company name and job title. This uses a separate cache key (`fallback-{company}-{job_title}`) so it doesn't collide with JD-based cache.

```
JD available    → generate_all_content(company, title, job_text)  [best]
JD unavailable  → generate_all_content_without_jd(company, title) [good]
Quota exhausted → skip, retry tomorrow                             [last resort]
```

### Leftover quota utilization

After generating content for new applications, remaining Gemini calls are used to fill gaps:

```
Priority: Applications with no ai_cache entry
→ generate content using leftover calls
→ stop when quota exhausted or all applications covered
```

---

## Tiered Recruiter Verification

Since re-visiting cached profiles is free, the pipeline verifies existing recruiters on every run at zero quota cost. Verification is tiered to balance accuracy vs time:

### Tier 1 — Trust (verified < 30 days ago)
```
Action: Skip entirely
Cost: 0 time, 0 quota
Reasoning: Recently verified, unlikely to have changed
```

### Tier 2 — Lightweight check (verified 30-60 days ago)
```
Action: Search company page, look for recruiter name in results
Cost: ~3-5 seconds, 0 quota
Outcome:
  Name found → update verified_at timestamp
  Name missing → escalate to Tier 3
```

### Tier 3 — Full profile visit (verified > 60 days ago)
```
Action: Visit cached profile (free — not counted against quota)
Cost: ~10-15 seconds, 0 quota
Outcome:
  Still at company → update email/title if changed
  Not at company  → mark recruiter_status = inactive
                  → cancel all pending outreach
```

---

## Email Bounce Detection (Tier 0)

The fastest and cheapest detection method — triggered during email sending:

```
SMTPRecipientsRefused detected during send
→ mark outreach status = bounced
→ mark recruiter_status = inactive
→ cancel all pending outreach for this recruiter
→ cost: 0 (detected automatically)
```

This catches job changes even within the 30-day Tier 1 trust window.

---

## Quota Health Monitoring

The pipeline monitors quota health and sends email alerts when patterns suggest configuration adjustment is needed.

### Alert conditions

| Condition | Trigger | Consecutive days |
|---|---|---|
| Underutilized | usage < 40% of daily limit | 3 days |
| Exhausted | remaining = 0 | 3 days |

### Applies to both quotas

- CareerShift: 50/day limit
- Gemini: 40/day combined limit

### Auto-calculated suggestions

When an alert triggers, the pipeline calculates a suggested `MAX_CONTACTS_HARD_CAP` adjustment:

**Underutilized:**
```python
utilization_rate = avg_used / total_limit
suggested_cap = round(current_cap / utilization_rate)
suggested_cap = min(suggested_cap, 10)  # hard ceiling
```

**Exhausted:**
```python
avg_companies_per_day = avg(new_companies_added_per_day)
suggested_cap = floor(total_limit / avg_companies_per_day)
suggested_cap = max(suggested_cap, 1)  # hard floor
```

### Alert email format

```
Subject: Quota Alert — Action Required

CAREERSHIFT QUOTA — Underutilized (3 days)
  2026-02-26: used 6/50  (12%)
  2026-02-27: used 8/50  (16%)
  2026-02-28: used 6/50  (12%)
  Recommendation: Increase MAX_CONTACTS_HARD_CAP from 3 to 6

GEMINI QUOTA — Exhausted (3 days)
  2026-02-26: used 40/40 (0 remaining)
  2026-02-27: used 40/40 (0 remaining)
  2026-02-28: used 40/40 (0 remaining)
  Recommendation: Reduce daily applications or upgrade Gemini plan
```

### Duplicate alert prevention

Once an alert is sent (`notified = 1`), no further alerts are sent for the same condition until the streak resets. This prevents receiving the same alert every day during a prolonged period of under/over utilization.

### How to trigger check

**Automatic:** Runs at end of every `--find-only` run.

**Manual (on demand):**
```bash
python pipeline.py --quota-report
```

---

## Serper API Credits (ATS Detection)

### How it works

Serper.dev is used for Phase 3b of ATS detection — finding Workday
and Oracle HCM tenants via Google search. Unlike CareerShift and
Gemini, Serper uses a one-time credit pool (not daily resets).

| Credit type | Amount | Resets |
|---|---|---|
| Serper free credits | 2500 (one-time signup) | Never |

### Credit usage

```text
Per company (Phase 3b only):
  Query 1: "{company} site:myworkdayjobs.com"  → 1 credit
  Query 2: "{company} site:fa.oraclecloud.com" → 1 credit
  Total:   2 credits per company
```

Most companies are detected in Phases 1-3a at zero cost.
Serper credits are only spent when all free phases fail.

**Expected lifetime usage:**
```text
Initial detection (134 companies):
  ~25 reach Phase 3b (others caught by sitemap/API/redirect)
  25 × 2 = 50 credits used

Monthly re-detection:
  ~3-5 companies trigger re-detection per month
  5 × 2 = 10 credits/month

2500 credits ÷ 50 initial ÷ 10/month = ~245 months
Effectively free forever.
```

### Low credit alert

An email alert is sent automatically when credits drop below 50:

```text
Subject: [Alert] Serper API — only 47 credits remaining

Time to arrange alternative for Workday/Oracle detection.
Options:
  1. Buy more credits at serper.dev ($50 for 50k queries)
  2. Switch to Brave Search API ($3-5 per 1000 queries)
  3. Use SeleniumBase UC Mode (free, browser-based)
```

The alert is sent once and not repeated until credits are replenished
and the alert flag is reset via `reset_low_credit_alert()`.

### Companies that skip Serper entirely

The following companies use fully custom ATS platforms and are
stored as `custom` immediately — no Serper credits consumed:

```text
Amazon, Apple, Google, Meta, Microsoft,
Netflix, Uber, Lyft, X, Twitter
```

### Checking credit balance

```bash
python pipeline.py --monitor-status
```

Output includes:
```text
Serper API credits: 2450/2500 remaining
  Used: 50  │  Limit: 2500  │  Alert threshold: 50
```

---


---

## Brave Search API (ATS Discovery)

> **Note:** Microsoft retired Bing Search API on August 11, 2025.
> Replaced with Brave Search API (free tier: 1,000 queries/month).
> Sign up: https://api.search.brave.com/

### How it works

Brave Search API fills gaps for ATS platforms that Common Crawl does not index well:

| Platform | Reason CC misses it |
|---|---|
| Lever | Migrated to JS rendering after CC-MAIN-2025-47 |
| Oracle HCM | Very sparse CC coverage |
| iCIMS | JS-heavy job boards |

Brave is used exclusively in `build_ats_slug_list.py` — not during daily job monitoring.

### Quota tracking

```text
Monthly limit:   1000 calls (Bing free tier)
Hard stop:        950 calls (50 call safety buffer)
Resets:          1st of each month (auto-detected by month change)
Stored in:       data/brave_quota.json
Increments:      only on HTTP 200 success
Checked:         before every page request
```

### Monthly budget breakdown

```text
Lever:     3 queries × 20 pages = 60 calls
Oracle:    2 queries × 20 pages = 40 calls
iCIMS:     2 queries × 20 pages = 40 calls
─────────────────────────────────────────
Total per run:                   140 calls
Monthly runs:          1 (normal refresh)
Safety buffer:                    50 calls
─────────────────────────────────────────
Monthly usage:    ~140/1000 (14% of limit)
Well within free tier ✓
```

### Checking Bing quota

```bash
python build_ats_slug_list.py --test
# Shows: [BRAVE] Quota: 140/950 used (2026-03), 810 remaining
```

---

## AWS Athena (ATS Discovery)

### How it works

AWS Athena queries the Common Crawl columnar index (Parquet files on S3) to discover new ATS company slugs. Replaces the old CDX API approach which caused rate limit timeouts.

### Cost model

```text
Pricing:         $5.00 per TB scanned
Data scanned:    ~52 MB per query (6 ATS domains from 300 GB index)
Cost per query:  ~$0.00026

Monthly cost:    1 query × $0.00026 = $0.00026/month
Annual cost:     $0.003/year
```

### Smart refresh — only queries new crawls

```text
Sliding window:  last 3 crawls e.g. [2026-08, 2026-04, 2025-51]
scanned_crawls:  tracks which crawls already processed
Unscanned:       [2026-08]  ← only this needs Athena

Normal monthly run = 1 Athena query = $0.00026
```

### S3 result handling

```text
After Athena query:
  1. CSV saved locally: data/athena_CC-MAIN-2026-08_2026-03-09.csv
  2. S3 result deleted immediately (zero storage cost)
  3. Old CSVs (>2 days) deleted on next run

Recovery (if script crashed):
  python build_ats_slug_list.py --from-csv data/athena_*.csv
```

### AWS setup (one-time)

```bash
# In AWS Athena console (us-east-1 region):
CREATE DATABASE ccindex

CREATE EXTERNAL TABLE IF NOT EXISTS ccindex (...)  # see docs
STORED AS parquet
LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';

MSCK REPAIR TABLE ccindex  # run monthly for new crawls

# In .env:
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
ATHENA_DATABASE=ccindex
ATHENA_TABLE=ccindex
ATHENA_S3_OUTPUT=s3://your-bucket/athena-results/
```

## `--verify-only` and Quota

The `--verify-only` command runs tiered verification independently
of job search activity. Key quota facts:

```
CareerShift quota used: 0
  → All profile re-visits are cached (free)
  → Only first-time profile visits count against quota

Gemini quota used: 0
  → No AI generation during verification
  → Only checks recruiter status

Time cost:
  ~10 seconds per Tier 2 recruiter (search only)
  ~20 seconds per Tier 3 recruiter (profile visit)
  Typical weekly run: 5-15 minutes
```

This means `--verify-only` can run as frequently as needed
at zero cost to either quota.