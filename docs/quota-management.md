# Quota Management

## Overview

The pipeline manages two separate daily quotas:

| Quota | Limit | Resets |
|---|---|---|
| CareerShift profile views | 50 new contacts/day | Daily |
| Gemini AI calls | 40 calls/day (20 per model) | Daily |

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
```

Example with 50 quota and 20 new companies:
```
base = 50 // 20 = 2
extra = 50 % 20 = 10
→ first 10 companies get 3 contacts
→ last 10 companies get 2 contacts
→ total used = 50
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