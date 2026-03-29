# Recruiter Validation & Pipeline Performance Design

## Overview

This document captures the finalized design decisions for recruiter email
validation, company name matching, buffer-based domain consistency,
and pipeline performance monitoring.

---

## What Changes vs Existing Code

Only `scrape_company()` in `careershift/scraper.py` is replaced.
Everything else remains unchanged:
- Session verification
- Tiered recruiter verification (Tier 1, 2, 3)
- `calculate_distribution()`
- `submit_search()`
- `scan_and_collect()`
- `run()` overall structure

---

## Helper Functions

### normalize()
Cleans company name string for comparison.
Does NOT remove legal suffixes — only cleans formatting.

```python
def normalize(company_name):
    name = company_name.lower()
    name = re.sub(r'[.,\-]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

# Examples:
normalize("Collective")          → "collective"
normalize("Collective Inc")      → "collective inc"
normalize("Collective LLC")      → "collective llc"
normalize("Collective, Inc.")    → "collective inc"
normalize("Collective Junction") → "collective junction"
normalize("Collective Health")   → "collective health"
```

### is_suffix_variation()
Returns True if card company = expected company + legal suffix only.

```python
LEGAL_SUFFIXES = [
    "inc", "llc", "ltd", "corp", "co", "lp",
    "plc", "gmbh", "pte", "incorporated",
    "corporation", "limited"
]

def is_suffix_variation(normalized_card, normalized_expected):
    if not normalized_card.startswith(normalized_expected):
        return False
    remainder = normalized_card[len(normalized_expected):].strip()
    return remainder in LEGAL_SUFFIXES

# Examples:
is_suffix_variation("collective inc", "collective")      → True
is_suffix_variation("collective llc", "collective")      → True
is_suffix_variation("collective junction", "collective") → False
is_suffix_variation("collective health", "collective")   → False
is_suffix_variation("ilovecollective", "collective")     → False
```

### domain_matches_expected()
Checks if email domain root matches expected domain.

```python
def domain_matches_expected(email, expected_domain):
    domain = email.split("@")[1]    # "collective.com"
    root = domain.split(".")[0]     # "collective"
    return root == expected_domain

# Examples:
domain_matches_expected("john@collective.com", "collective")      → True
domain_matches_expected("jane@collective-la.com", "collective")   → False
domain_matches_expected("bob@collectiveinc.com", "collective")    → False
domain_matches_expected("alice@ilovecollective.com", "collective") → False
```

### analyze_buffer()
Analyzes visited profiles for domain consistency before DB insertion.
Uses existing DB domain as reference if available, otherwise expected_domain.

```python
def analyze_buffer(buffer, expected_domain, existing_db_domain=None):
    """
    reference = existing DB domain (trusted) OR expected_domain (from job URL)
    DB domain takes priority — already verified and potentially used for outreach.
    """
    if not buffer:
        return []

    reference = existing_db_domain if existing_db_domain else expected_domain

    domains = [entry["email"].split("@")[1] for entry in buffer]
    unique_domains = set(domains)

    # All same domain in buffer
    if len(unique_domains) == 1:
        buffer_domain_root = domains[0].split(".")[0]

        if buffer_domain_root == reference:
            # Consistent + matches reference → insert all
            return buffer
        else:
            # Consistent but conflicts with reference (DB or expected)
            # → Trust DB, discard buffer silently
            # → Log to coverage_stats (reason: buffer_domain_conflict)
            return []

    # Mixed domains in buffer
    # → Use reference as tiebreaker
    matched = [r for r in buffer
               if domain_matches_expected(r["email"], reference)]

    if matched:
        return matched  # insert matching, discard rest

    return []  # nothing matches → discard all
```

---

## Hashmap Design

Tracks exact company matches and suffix variations across all HR terms.
Completely ignores unrelated companies (Collective Junction, Collective Health etc).

```
Initialize once per company search session:
  hashmap = {"collective": 0}

For each card:
  normalize(card_company) == normalize(expected)
    → exact match → hashmap["collective"] += 1

  is_suffix_variation(card_company, expected) == True
    → suffix variation → hashmap["collective inc"] += 1 (append if new)

  else
    → completely different company → ignore, don't touch hashmap
```

---

## Confidence Scoring

Calculated per batch of cards, per HR search term.
Uses actual cards returned — handles fewer than SAMPLE_SIZE gracefully.

```python
actual_count = len(cards)
sample_size  = min(actual_count, CAREERSHIFT_SAMPLE_SIZE)

if sample_size == 0:
    continue  # no results → skip to next HR term

confidence = (cnt / sample_size) * 100

# Examples:
# 3 cards returned, all correct:
#   sample_size = min(3, 10) = 3
#   confidence = (3/3) * 100 = 100%  ← correct

# 7 cards returned, 5 correct:
#   sample_size = min(7, 10) = 7
#   confidence = (5/7) * 100 = 71.4% ← correct
```

---

## scrape_company() Pseudocode

```python
def scrape_company(page, company, max_contacts, expected_domain):
    """
    max_contacts:     from calculate_distribution — controls quota usage per company
    expected_domain:  from application.expected_domain — used as domain tiebreaker
    """
    normalized_expected = normalize(company)
    hashmap = {normalized_expected: 0}
    all_exact_profiles = []
    skip_remaining = False

    # ── Step 1: Fetch cards and build signal ──

    for hr_term in HR_SEARCH_TERMS:
        if skip_remaining:
            break

        cnt = 0
        cards = fetch_cards(page, company, hr_term, limit=CAREERSHIFT_SAMPLE_SIZE)

        actual_count = len(cards)
        sample_size = min(actual_count, CAREERSHIFT_SAMPLE_SIZE)

        if sample_size == 0:
            continue  # no results for this term

        for card in cards:
            normalized_card = normalize(card["company"])

            if normalized_card == normalized_expected:
                cnt += 1
                hashmap[normalized_expected] += 1
                all_exact_profiles.append(card)

            elif is_suffix_variation(normalized_card, normalized_expected):
                if normalized_card not in hashmap:
                    hashmap[normalized_card] = 0
                hashmap[normalized_card] += 1

            else:
                pass  # different company → ignore

        confidence = (cnt / sample_size) * 100

        if confidence >= CAREERSHIFT_HIGH_CONFIDENCE:
            skip_remaining = True   # strong signal → skip remaining terms

        elif confidence >= CAREERSHIFT_MEDIUM_CONFIDENCE:
            continue                # medium signal → try next term

        else:
            continue                # low signal → keep accumulating

    # ── After all HR terms ──

    if hashmap[normalized_expected] == 0:
        return []  # no exact matches found → exhaust application

    # ── Step 2: Visit profiles ──

    # Respect quota allocation — never visit more than max_contacts
    # CAREERSHIFT_MAX_PROFILES is the hard ceiling
    visit_limit = min(max_contacts, CAREERSHIFT_MAX_PROFILES)
    profiles_to_visit = all_exact_profiles[:visit_limit]

    buffer = []

    for profile in profiles_to_visit:

        # Stop if quota exhausted during visits
        if get_remaining_quota() == 0:
            print("[INFO] Quota exhausted — stopping profile visits early")
            break

        human_delay(2.0, 4.0)
        detail = visit_profile(page, profile["url"])

        if not detail:
            continue
        if normalize(detail["company"]) != normalized_expected:
            continue
        if not detail["email"]:
            continue

        buffer.append({
            "name":     detail["name"],
            "position": detail["position"],
            "email":    detail["email"],
            "company":  company,
        })

    # ── Step 3: Get existing DB domain for this company (top-up scenario) ──

    existing_db_domain = get_existing_domain_for_company(company)
    # Returns root domain of first active recruiter in DB for this company
    # Returns None if no existing recruiters

    # ── Step 4: Analyze buffer ──

    # Special case: single profile visit (max_contacts = 1 due to quota)
    if visit_limit == 1:
        if not buffer:
            return []

        single = buffer[0]
        total_cards_seen = max(sum(hashmap.values()), 1)
        hashmap_confidence = (hashmap[normalized_expected] / total_cards_seen) * 100

        reference = existing_db_domain if existing_db_domain else expected_domain

        if hashmap_confidence >= CAREERSHIFT_MEDIUM_CONFIDENCE:
            # Enough signal → use reference domain as gate
            if domain_matches_expected(single["email"], reference):
                return [single]   # confident + domain matches → insert
            else:
                return []         # domain mismatch → exhaust
        else:
            # Weak signal → not worth inserting single record
            return []             # skip → retry tomorrow (not exhaust)

    # Normal case: multiple profiles
    verified = analyze_buffer(buffer, expected_domain, existing_db_domain)
    return verified
```

---

## Buffer Analysis — All Cases

### Fresh company (no existing DB records):

```
reference = expected_domain (from job URL)

Buffer 3 records, all same domain matching expected:
  → Insert all ✓

Buffer 3 records, majority domain matches expected (2+1):
  → Insert 2 matching, discard 1 ✓

Buffer 3 records, all different, 1 matches expected:
  → Insert 1 matching, discard 2 ✓

Buffer 3 records, none match expected:
  → Discard all → exhaust application

Buffer 2 records, same domain matching expected:
  → Insert both ✓

Buffer 2 records, different domains:
  → Apply expected_domain tiebreaker
  → Keep matching, discard other

Buffer 1 record (quota = 1):
  → hashmap confidence >= 70% AND domain matches expected → insert
  → hashmap confidence < 70% → skip (retry tomorrow, not exhaust)
  → domain doesn't match → exhaust

Buffer empty:
  → Exhaust application
```

### Top-up scenario (existing DB records):

```
reference = existing_db_domain (trusted, takes priority)

Buffer records, all match DB domain:
  → Insert all ✓

Buffer records, mixed — some match DB domain:
  → Insert matching only, discard rest ✓

Buffer records, all same domain but conflicts with DB:
  → Buffer consistent but untrusted
  → Discard buffer silently
  → Log to coverage_stats (reason: buffer_domain_conflict)
  → Leave DB unchanged ✓

Buffer records, none match DB domain:
  → Discard all buffer
  → Log to coverage_stats
  → DB unchanged ✓
```

---

## Quota Distribution with MIN_BUFFER_SIZE

```
Total quota: 50
Companies to scrape: 30

calculate_distribution(50, 30):
  base  = 50 // 30 = 1
  extra = 50 % 30  = 20

  Company 1-20  → max_contacts = 2
  Company 21-30 → max_contacts = 1

visit_limit = min(max_contacts, CAREERSHIFT_MAX_PROFILES)

Company with max_contacts = 1:
  → visit_limit = 1
  → Single profile visit path
  → hashmap confidence + domain check as gate

Company with max_contacts = 2:
  → visit_limit = 2
  → Normal buffer analysis path
```

---

## Performance Metrics

### Metric 1 — Find-Only Pipeline Performance
```
Formula: (auto confident companies found / companies attempted) * 100

Example:
  Applied to 10 companies
  Already have recruiters for 4 → excluded
  Attempted: 6 companies
  Found with auto confidence: 4

  Metric 1 = (4/6) * 100 = 66.7%

Thresholds:
  Green:  >= 70%  → healthy
  Yellow: 50-70%  → degrading, monitor
  Red:    < 50%   → alert fires
```

### Metric 2 — Outreach Pipeline Performance
```
Formula: (companies with sendable recruiters / total applications) * 100

Example:
  Total applications: 10
  Already had recruiters: 4
  Newly found (auto): 4
  Total ready for outreach: 8

  Metric 2 = (8/10) * 100 = 80%

Thresholds:
  Green:  >= 75%  → healthy
  Yellow: 60-75%  → degrading, monitor
  Red:    < 60%   → alert fires
```

### Alert conditions
```
Metric 1 < 50% for 3 consecutive days → alert
Metric 2 < 60% for 3 consecutive days → alert
```

---

## Exhaust vs Skip Decision

```
Exhaust application (status = 'exhausted'):
  → hashmap[expected] == 0 after ALL HR terms
     (CareerShift has no data for this company)
  → buffer empty after profile visits
  → domain mismatch with no fallback
  → Single profile: hashmap confident BUT domain doesn't match

Skip (retry tomorrow, not exhausted):
  → Single profile: hashmap confidence < 70%
     (weak signal, might find more tomorrow)
  → Quota exhausted mid-visit
     (incomplete data, not company's fault)

Before exhausting — check metrics:
  Metric 1 >= 50% AND Metric 2 >= 60%
    → exhaust silently, log to coverage_stats

  Metric 1 < 50% OR Metric 2 < 60%
    → do NOT exhaust
    → fire alert email
    → human intervention required
```

---

## Status Values

### applications.status
```
'active'     → normal, included in all pipeline steps
'exhausted'  → no recruiters found after all validation
'closed'     → job closed/withdrawn by user
```

### recruiters.confidence
```
'auto' → validated, proceed to outreach
```

### recruiters.recruiter_status
```
'active'   → sendable, included in outreach
'inactive' → bounced or left company, excluded
```

---

## New DB Tables Required

### coverage_stats

Tracks daily recruiter pipeline performance. Written by `--find-only` and read by `--performance-report` and the alert system. One row per day (UNIQUE on date).

```sql
CREATE TABLE IF NOT EXISTS coverage_stats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    date                DATE NOT NULL UNIQUE,
    total_applications  INTEGER,   -- active applications that day
    companies_attempted INTEGER,   -- companies where scraping was tried
    auto_found          INTEGER,   -- companies where auto-confidence recruiters found
    rejected_count      INTEGER,   -- companies where buffer discarded (domain mismatch etc.)
    exhausted_count     INTEGER,   -- companies marked exhausted (no CareerShift data)
    metric1             REAL,      -- find-only performance %
    metric2             REAL,      -- outreach coverage %
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_coverage_stats_date
ON coverage_stats(date);
```

**Implementation status:** Schema created and deployed. The writer in `careershift/find_emails.py` populates this table at the end of every `--find-only` run. `metric1` and `metric2` are now being written by the pipeline.

**Retention:** Add `RETENTION_COVERAGE_STATS` to `config.py` and implement `_cleanup_coverage_stats()` in `db/schema.py`. Suggested value: 60 days (same as `monitor_stats`).

---

### api_health

Tracks per-platform ATS API reliability during `--monitor-jobs` runs. One row per platform per day (UNIQUE on date+platform). Designed to surface degrading APIs — e.g. a platform returning 429s all morning would show up here immediately rather than silently causing an empty digest.

```sql
CREATE TABLE IF NOT EXISTS api_health (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            DATE    NOT NULL,
    platform        TEXT    NOT NULL,   -- e.g. 'greenhouse', 'workday', 'lever'

    -- Request counts
    requests_made   INTEGER DEFAULT 0,  -- total API calls attempted
    requests_ok     INTEGER DEFAULT 0,  -- 200 responses
    requests_429    INTEGER DEFAULT 0,  -- rate limit responses
    requests_404    INTEGER DEFAULT 0,  -- not found (detection miss)
    requests_error  INTEGER DEFAULT 0,  -- timeouts, connection errors, malformed JSON

    -- Timing (milliseconds)
    avg_response_ms INTEGER DEFAULT 0,
    max_response_ms INTEGER DEFAULT 0,
    total_ms        INTEGER DEFAULT 0,

    -- Rate limit details
    first_429_at    TIMESTAMP,          -- when first rate limit hit occurred
    backoff_total_s INTEGER DEFAULT 0,  -- total seconds spent in backoff/retry

    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, platform)
);
CREATE INDEX IF NOT EXISTS idx_api_health_date_platform
ON api_health(date, platform);
```

**Key derived metrics:**
```text
Success rate:    requests_ok / requests_made        (target > 90%)
Rate limit rate: requests_429 / requests_made       (target < 5%)
Error rate:      requests_error / requests_made     (target < 10%)
Avg backoff/req: backoff_total_s / requests_made
```

**Why this matters in practice:**
```text
Without api_health — Greenhouse API returns 429 all morning:
  You see: 0 new jobs from 40 Greenhouse companies
  You think: nobody posted today
  Reality: you missed all of them silently

With api_health — same event:
  You see: requests_made=40, requests_ok=0,
           requests_429=40, backoff_total_s=2400
  → Shown in PDF digest health section
  → pipeline_alerts row created → email alert sent
```

**Implementation status:** Schema created and deployed. All columns are currently 0 — the writer in `jobs/job_monitor.py` has not been implemented yet. When implemented, a row will be upserted per platform at the end of each `--monitor-jobs` run.

**Retention:** No cleanup function yet. Add `RETENTION_API_HEALTH` to `config.py` and `_cleanup_api_health()` in `db/schema.py` when the writer is added. Suggested value: 60 days (same as `monitor_stats`).

---

### pipeline_alerts

Unified alert table for all pipeline-level threshold breaches. Designed as a single place where any automated alert is recorded and tracked before being emailed. More flexible than the recruiter-specific `quota_alerts` table — covers job monitoring failures, ATS API degradation, recruiter pipeline performance drops, and any other configurable threshold.

```sql
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type   TEXT    NOT NULL,   -- e.g. 'metric1_low', 'api_failure_rate'
    severity     TEXT    NOT NULL,   -- 'warning' or 'critical'
    platform     TEXT,               -- ATS platform if platform-specific, else NULL
    value        REAL,               -- actual metric value that triggered alert
    threshold    REAL,               -- threshold that was breached
    message      TEXT,               -- human-readable description
    notified     INTEGER DEFAULT 0,  -- 0 = email not sent, 1 = email sent
    notified_at  TIMESTAMP,          -- when email was sent
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Planned alert types:**
```text
metric1_low      → find-only performance < 50% for 3 consecutive days
metric2_low      → outreach coverage < 60% for 3 consecutive days
api_failure_rate → platform error rate > 10% for 3 consecutive days
api_rate_limited → platform 429 rate causing backoff > threshold
coverage_drop    → --monitor-jobs companies_with_results / companies_monitored < 70%
```

**Alert email format:**
```
Subject: Pipeline Performance Alert - Action Required

FIND-ONLY PERFORMANCE (Metric 1) - DEGRADED
  2026-02-26: 3/6 companies found (50%)
  2026-02-27: 2/6 companies found (33%)
  2026-02-28: 2/5 companies found (40%)

  Average: 41% - below 50% threshold

Most exhausted companies (last 7 days):
  - Collective: buffer domain mismatch
  - Stripe: no exact match found
  - Linear: low confidence, 0 exact matches

Recommendation:
  Review CAREERSHIFT_HIGH_CONFIDENCE (currently 90)
  Review CAREERSHIFT_MEDIUM_CONFIDENCE (currently 70)
  Or manually reactivate exhausted applications:
    python pipeline.py --reactivate "Collective"
```

**Implementation status:** Schema created and deployed. Rows are created and read by `db/pipeline_alerts.py` and `pipeline.py`. The system writes alerts when performance thresholds are breached during `--find-only` and `--monitor-jobs` runs.

**Retention:** Add `RETENTION_PIPELINE_ALERTS` to `config.py` and implement `_cleanup_pipeline_alerts()` in `db/schema.py`. Suggested value: 30 days (same as `quota_alerts`).

---

## Configuration (config.py)

```python
# ─────────────────────────────────────────
# RECRUITER VALIDATION SETTINGS
# ─────────────────────────────────────────
CAREERSHIFT_SAMPLE_SIZE       = 10   # cards per batch
CAREERSHIFT_HIGH_CONFIDENCE   = 90   # skip remaining HR terms
CAREERSHIFT_MEDIUM_CONFIDENCE = 70   # continue to next HR term
CAREERSHIFT_MAX_PROFILES      = 3    # hard cap — never visit more than this
MIN_BUFFER_SIZE               = 2    # minimum for domain consistency check
MIN_RECRUITERS_PER_COMPANY    = 1    # minimum to start outreach (not exhaust)
MAX_CONTACTS_HARD_CAP         = 3    # target ceiling — topped up over time
GEMINI_VERIFY_RETRY_DAYS      = 5    # days to retry Gemini verification

# ─────────────────────────────────────────
# PIPELINE PERFORMANCE THRESHOLDS
# ─────────────────────────────────────────
METRIC1_ALERT_THRESHOLD       = 50   # find-only performance % (Red)
METRIC2_ALERT_THRESHOLD       = 60   # outreach coverage % (Red)
METRIC_ALERT_CONSECUTIVE_DAYS = 3    # days before alert fires

# ─────────────────────────────────────────
# DATA RETENTION (add when writers implemented)
# ─────────────────────────────────────────
# RETENTION_COVERAGE_STATS    = 60   # days (pending writer implementation)
# RETENTION_API_HEALTH        = 60   # days (pending writer implementation)
# RETENTION_PIPELINE_ALERTS   = 30   # days (pending writer implementation)
```

---

## New CLI Commands

```bash
# Check pipeline performance metrics
python pipeline.py --performance-report

# Reactivate exhausted application
python pipeline.py --reactivate "CompanyName"
```

---

## Complete Flow Summary

```
--find-only run:

  For each company needing scraping:

    scrape_company(page, company, max_contacts, expected_domain):

      Step 1: Card analysis across HR terms
        For each HR term:
          Fetch cards (up to CAREERSHIFT_SAMPLE_SIZE)
          sample_size = min(actual_cards, CAREERSHIFT_SAMPLE_SIZE)
          if sample_size == 0 → skip term

          For each card:
            exact match      → cnt++, hashmap[expected]++, add to profiles
            suffix variation → hashmap[variation]++, ignore for profiles
            different company→ ignore completely

          confidence = (cnt / sample_size) * 100
          >= 90% → skip remaining terms
          >= 70% → continue to next term
          <  70% → continue accumulating

      Step 2: Profile visits
        visit_limit = min(max_contacts, CAREERSHIFT_MAX_PROFILES)
        profiles_to_visit = all_exact_profiles[:visit_limit]

        For each profile:
          get_remaining_quota() == 0 → break (stop early)
          visit profile → validate company name → collect email → buffer

      Step 3: Get existing DB domain for this company
        existing_db_domain = get_existing_domain_for_company(company)

      Step 4: Buffer analysis

        visit_limit == 1 (quota forced single visit):
          hashmap_confidence >= 70% AND domain matches reference
            → insert single record
          hashmap_confidence < 70%
            → skip (retry tomorrow, not exhaust)
          domain doesn't match reference
            → exhaust

        visit_limit > 1 (normal case):
          analyze_buffer(buffer, expected_domain, existing_db_domain):
            reference = existing_db_domain OR expected_domain

            All same domain, matches reference     → insert all
            All same domain, conflicts reference   → discard, log
            Mixed domains                          → keep matching reference
            Nothing matches reference              → discard all

          Empty result → exhaust or skip based on metrics

  At end of run:
    Record coverage_stats row (careershift/find_emails.py)
    Check metric1 + metric2 against thresholds
    If below threshold for 3 consecutive days:
      → Create pipeline_alerts row (db/pipeline_alerts.py)
      → Send alert email (pipeline.py)
```