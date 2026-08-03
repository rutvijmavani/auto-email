# Quota Management

## Overview

The pipeline manages three quotas:

| Quota | Limit | Resets | Per-user? |
|---|---|---|---|
| CareerShift profile views | 50 new contacts/day | Daily | ✅ Yes — each user has their own 50/day limit |
| Gemini AI calls (email content) | 40 calls/day (20 per model) | Daily | ✅ Yes — each user has their own Gemini API key |
| Gemini AI calls (ATS detection) | Shared pool | Daily | ❌ No — shared across all users |
| Google KG API (H-1B discovery) | 100,000 calls/day | Daily | ❌ No — shared |
| Serper API credits | 2500 total (one-time) | Never | ❌ No — shared |
| Brave Search API calls | 1000/month (hard stop: 950) | Monthly | ❌ No — shared (build_ats_slug_list + discover_h1b_ats) |
| AWS Athena queries | ~$0.00024/query | Pay-per-use | ❌ No — shared |

All quotas are tracked locally in the database and synced with real values at runtime.

> **What "per-user" means in practice:** Each user's CareerShift scraping and email outreach runs independently. User 1 consuming their full CareerShift quota for the day has no effect on User 2's ability to scrape contacts. Similarly, User 1 generating 20 emails does not reduce the Gemini quota available to User 2. The shared quotas (Serper, Brave, Athena) are used only for ATS platform discovery — a background task that runs once per day and is not tied to any individual user.

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

Each user's quota is tracked separately. The `careershift_quota` table stores one row per user per day — so if User 1 uses 30 of their 50 credits and User 2 uses 10 of their 50 credits, both rows coexist in the table for that day without interfering with each other.

### Profile caching and the "found by" user

CareerShift caches recruiter profiles per account. When the pipeline first visits a recruiter's profile, that visit is recorded in the `recruiters` table with a `found_by_user_id` column — the id of the user whose CareerShift account made the original visit.

This matters for quota efficiency:

```text
Scenario: User 1 first visits Recruiter A's profile (costs 1 credit from User 1's quota)

Later, the pipeline needs to verify Recruiter A is still at their company:
  → Using User 1's account: FREE  (profile is cached for User 1)
  → Using User 2's account: costs 1 credit from User 2's quota
                             (different account → different cache → not free)
```

The pipeline always uses the `found_by_user_id` account for re-verification, so cached profile re-visits are free. The first-visit cost is the only time a profile ever burns quota.

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

Gemini AI is used for two separate jobs in the pipeline, each with its own quota pool:

**1. Email content generation (per-user)**

When the pipeline prepares outreach emails, it uses the Gemini key belonging to the user who will send those emails (`GEMINI_API_KEY_USER_{id}` in `.env`). The quota for this is per-user and completely independent between users.

| Model | Daily limit | Role |
|---|---|---|
| `gemini-2.5-flash-lite` | 20 calls | Primary |
| `gemini-2.5-flash` | 20 calls | Fallback |

Each application requires **one AI call** which generates all three email stages at once (initial, follow-up 1, follow-up 2).

**2. ATS detection (shared)**

When the pipeline detects which Applicant Tracking System (ATS) a company uses, it uses a shared Gemini key (`GEMINI_API_KEY` in `.env`) that is not tied to any user. This prevents ATS detection from accidentally consuming any individual user's email generation quota.

The two pools are tracked separately in the `model_usage` table using a `use_case` column:
- `use_case = "email_content"` → per-user pool (tied to a specific `user_id`)
- `use_case = "ats_detection"` → shared pool (no `user_id`)

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

### Applies to both per-user quotas

Quota alerts are evaluated **per user**. If User 1 is consistently exhausting their CareerShift quota but User 2 is not, the pipeline sends an alert only about User 1's account — User 2 is unaffected and does not receive an alert.

- CareerShift: 50/day per user
- Gemini: 40/day per user (email content quota only; ATS detection has its own shared pool)

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

### Where Brave is used

Brave is used in **two scripts**:

| Script | Purpose | When |
|---|---|---|
| `build_ats_slug_list.py` | Discover new ATS slugs (Lever/Oracle/iCIMS) via CC gaps | Monthly |
| `discover_h1b_ats.py --brave-pass` | Career page fallback for H-1B sponsors with website but no ATS pattern | Monthly |

Both scripts share the same `data/brave_quota.json` counter and the same 950-call monthly cap.

### Quota tracking

```text
Monthly limit:   1000 calls (Brave free tier)
Hard stop:        950 calls (50 call safety buffer)
Resets:          1st of each month (auto-detected by month change)
Stored in:       data/brave_quota.json
Increments:      only on HTTP 200 success
Checked:         before every call
```

### Monthly budget breakdown

```text
build_ats_slug_list.py:
  Lever:     3 queries × 20 pages = 60 calls
  Oracle:    2 queries × 20 pages = 40 calls
  iCIMS:     2 queries × 20 pages = 40 calls
  ─────────────────────────────────────────
  Subtotal:                        140 calls

discover_h1b_ats.py --brave-pass:
  Only companies with website_url + no careers_url (pass 1 probe found nothing)
  Typical: 100–300 calls/month depending on dataset size
  ─────────────────────────────────────────
  Subtotal:                    ~100–300 calls

Safety buffer:                      50 calls
─────────────────────────────────────────────
Total safe budget: 950 calls/month
```

### Two-pass design — KG and Brave never contend

`discover_h1b_ats.py` separates enrichment into two independent passes so KG quota (100k/day) and Brave quota (950/month) never block each other:

- **Pass 1 (default `--top N`):** Google KG + SPARQL + 19-pattern probe. No Brave calls. Runs daily. Sets `last_checked`.
- **Pass 2 (`--brave-pass`):** Brave only, targeting companies where Pass 1 completed but found no careers URL. Runs monthly. Sets `brave_checked_at`.

This means a day's KG run of 900 companies costs 0 Brave calls, leaving the full 950/month for the targeted monthly sweep.

### Checking Brave quota

```bash
cat data/brave_quota.json
# {"month": "2026-08", "calls": 11}

python build_ats_slug_list.py --test
# Shows: [BRAVE] Quota: 140/950 used (2026-08), 810 remaining
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

---

## Google KG + SPARQL Rate Limiting

`scripts/discover_h1b_ats.py` uses two external APIs for company enrichment.

### Google Knowledge Graph API

```text
Daily limit:   100,000 calls
RPM limit:     600 RPM (enforced via db/quota.py can_call/increment_usage)
Cost:          Free (Google Cloud project quota)
Tracked in:    db/quota.py ("kg_api" key)
```

Progressive retry means up to 4 calls per company (original query + 3 word-drops on `/g/` MID). Worst case: 25,000 companies/day. In practice most companies resolve in 1–2 calls.

### Wikidata SPARQL (P646+P10311+P856 batch)

```text
Rate limit:    30 RPM client-side (enforced via _sparql_limiter = _RateLimiter(rpm=30))
Cost:          Free
Endpoint:      https://query.wikidata.org/sparql
Batch size:    100 MIDs per query (_SPARQL_CHUNK_SIZE=100)
```

One SPARQL query fetches QID + jobs URL (P10311) + official website (P856) for up to 100 companies simultaneously. A `--top 900` run needs ≤9 SPARQL queries total — negligible.

### `_RateLimiter` design

Both `_sparql_limiter` and any future in-process rate limiters use the same pattern:

```python
class _RateLimiter:
    """Thread-safe sliding-window rate limiter."""
    def __init__(self, rpm: int) -> None:
        self._rpm    = rpm
        self._window = deque()
        self._lock   = threading.Lock()

    def acquire(self, api_name: str) -> None:
        while True:
            now = time.time()
            with self._lock:
                while self._window and self._window[0] < now - 60:
                    self._window.popleft()
                if len(self._window) < self._rpm:
                    self._window.append(now)
                    return
            log.debug("%s RPM limit — waiting 3s", api_name)
            time.sleep(3)
```

Key properties: sliding 60s window, thread-safe, blocks caller (no busy-spin), honours `Retry-After` on 429.

### Quota summary

| API | Daily limit | Monthly limit | Cost |
|---|---|---|---|
| Google KG API | 100,000 calls | — | Free (GCP quota) |
| Wikidata SPARQL | No hard cap | No hard cap | Free |
| Brave Search | — | 950 calls | Free tier |