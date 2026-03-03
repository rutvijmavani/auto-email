# Recruiter Validation & Pipeline Performance Design

## Overview

This document captures the finalized design decisions for recruiter email
validation, company name matching, buffer-based domain consistency,
and pipeline performance monitoring.

---

## Validation Pipeline

### What changes vs existing code

Only `scrape_company()` in `find_emails.py` is replaced.
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
normalize("Collective")         → "collective"
normalize("Collective Inc")     → "collective inc"
normalize("Collective LLC")     → "collective llc"
normalize("Collective, Inc.")   → "collective inc"
normalize("Collective Junction")→ "collective junction"
normalize("Collective Health")  → "collective health"
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

### analyze_buffer()
Analyzes visited profiles for domain consistency before DB insertion.

```python
def analyze_buffer(buffer):
    if len(buffer) < MIN_BUFFER_SIZE:
        return []  # too few records, discard all

    domains = [entry["email"].split("@")[1] for entry in buffer]
    unique_domains = set(domains)

    # All same domain → insert all
    if len(unique_domains) == 1:
        return buffer

    # Find majority domain
    majority_domain = Counter(domains).most_common(1)[0][0]
    majority_records = [r for r in buffer
                        if r["email"].split("@")[1] == majority_domain]

    # All different domains (no majority) → discard all
    if len(majority_records) == 1:
        return []

    # 2+ same domain wins → insert majority, discard minority
    return majority_records
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

Calculated per batch of 10 cards, per HR search term.

```
cnt       = exact "collective" matches in current batch of 10
confidence = (cnt / CAREERSHIFT_SAMPLE_SIZE) * 100

Examples:
  7/10 exact matches → confidence = 70%
  9/10 exact matches → confidence = 90%
  3/10 exact matches → confidence = 30%
```

---

## scrape_company() Pseudocode

```
def scrape_company(page, company, max_contacts):

  normalized_expected = normalize(company)
  hashmap = {normalized_expected: 0}
  all_exact_profiles = []
  skip_remaining = False

  FOR each HR term in HR_SEARCH_TERMS:

    if skip_remaining: break

    cnt = 0
    cards = fetch_cards(page, company, hr_term, limit=10)

    FOR each card:
      normalized_card = normalize(card.company)

      if normalized_card == normalized_expected:
        cnt += 1
        hashmap[normalized_expected] += 1
        all_exact_profiles.append(card)

      elif is_suffix_variation(normalized_card, normalized_expected):
        hashmap[normalized_card] += 1  ← track but don't add to profiles

      else:
        ignore  ← completely different company

    confidence = (cnt / 10) * 100

    if confidence >= 90% (HIGH):
      skip_remaining = True  ← strong signal, skip remaining HR terms

    elif confidence >= 70% (MEDIUM):
      continue  ← move to next HR term, keep accumulating

    else:
      continue  ← low signal, keep accumulating

  # ── After all HR terms processed ──

  if hashmap[normalized_expected] == 0:
    return []  ← no exact matches found at all → exhaust application

  profiles_to_visit = all_exact_profiles[:CAREERSHIFT_MAX_PROFILES]
  # Note: Python slicing is safe even if list has fewer than MAX_PROFILES items

  # ── Visit profiles, collect into buffer ──
  buffer = []

  FOR each profile in profiles_to_visit:
    human_delay(2.0, 4.0)
    detail = visit_profile(page, profile.url)

    if not detail: continue
    if normalize(detail.company) != normalized_expected: continue
    if not detail.email: continue

    buffer.append({
      name:     detail.name,
      position: detail.position,
      email:    detail.email,
      company:  company,
    })

  # ── Analyze buffer for domain consistency ──
  verified_records = analyze_buffer(buffer)

  if not verified_records:
    return []  ← buffer empty or all different domains → exhaust application

  return verified_records
```

---

## Buffer Analysis — All Cases

```
Buffer has 3+ records, all same domain:
  john@collective.com
  jane@collective.com
  bob@collective.com
  → Insert all 3 ✓

Buffer has 3 records, majority domain (2+1):
  john@collective.com     ← majority
  jane@collective.com     ← majority
  bob@collective-la.com   ← minority
  → Insert john + jane, discard bob ✓

Buffer has 3 records, all different domains:
  john@collective.com
  jane@collective-la.com
  bob@collectivehq.com
  → Discard all → exhaust application

Buffer has 2 records, same domain:
  john@collective.com
  jane@collective.com
  → Insert both ✓

Buffer has 2 records, different domains:
  john@collective.com
  jane@collective-la.com
  → Discard both → exhaust application

Buffer has 1 record:
  → Too risky, can't verify domain consistency
  → Discard → exhaust application

Buffer is empty:
  → No profiles visited or all failed validation
  → Exhaust application
```

---

## Exhaust Application Logic

Triggered when `scrape_company()` returns empty list.

```
Before marking exhausted:
  Check pipeline performance metrics:

  Metric 1 >= 50% AND Metric 2 >= 60%
    → Mark application status = 'exhausted' silently
    → Log to coverage_stats
    → No alert

  Metric 1 < 50% OR Metric 2 < 60%
    → Do NOT exhaust yet
    → Fire alert email
    → Human intervention required
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

## Status Values

### applications.status
```
'active'     → normal, included in all pipeline steps
'exhausted'  → no recruiters found after all validation, excluded from scraping
'closed'     → job closed/withdrawn by user
```

### recruiters.confidence
```
'auto' → validated, proceed to outreach
```

### recruiters.recruiter_status
```
'active'   → sendable, included in outreach
'inactive' → bounced or left company, excluded from outreach
```

---

## New DB Tables Required

### coverage_stats
```sql
CREATE TABLE coverage_stats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    date                DATE NOT NULL,
    total_applications  INTEGER,
    companies_attempted INTEGER,
    auto_found          INTEGER,
    rejected_count      INTEGER,
    exhausted_count     INTEGER,
    metric1             REAL,
    metric2             REAL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### applications table — new columns
```sql
ALTER TABLE applications ADD COLUMN expected_domain TEXT;
ALTER TABLE applications ADD COLUMN exhausted_at TIMESTAMP;
```

---

## Configuration (config.py)

```python
# ─────────────────────────────────────────
# RECRUITER VALIDATION SETTINGS
# ─────────────────────────────────────────
CAREERSHIFT_SAMPLE_SIZE       = 10   # cards per batch
CAREERSHIFT_HIGH_CONFIDENCE   = 90   # skip remaining HR terms
CAREERSHIFT_MEDIUM_CONFIDENCE = 70   # continue to next HR term
CAREERSHIFT_MAX_PROFILES      = 3    # max profiles to visit per company
MIN_BUFFER_SIZE               = 2    # minimum records before trusting domain

# ─────────────────────────────────────────
# PIPELINE PERFORMANCE THRESHOLDS
# ─────────────────────────────────────────
METRIC1_ALERT_THRESHOLD       = 50   # find-only performance % (Red)
METRIC2_ALERT_THRESHOLD       = 60   # outreach coverage % (Red)
METRIC_ALERT_CONSECUTIVE_DAYS = 3    # days before alert fires
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

## Alert Email Format

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
  Review MIN_BUFFER_SIZE (currently 2)
  Or manually reactivate exhausted applications:
    python pipeline.py --reactivate "Collective"
```

---

## Complete Flow Summary

```
--find-only run:

  For each company needing scraping:

    scrape_company():

      For each HR term:
        Fetch 10 cards (sample)
        Classify each card:
          exact match      → cnt++, hashmap[expected]++, add to profiles
          suffix variation → hashmap[variation]++, ignore for profiles
          different company → ignore completely

        confidence = (cnt/10) * 100
        >= 90% → skip remaining terms (high confidence)
        >= 70% → continue to next term (medium confidence)
        < 70%  → continue accumulating (low confidence)

      After all terms:
        hashmap[expected] == 0 → no exact matches → exhaust

        Visit up to 3 exact match profiles
        Collect into buffer

        analyze_buffer():
          < 2 records          → discard all → exhaust
          all same domain      → insert all
          majority domain      → insert majority, discard minority
          all different domains→ discard all → exhaust

  Coverage stats recorded daily
  Metrics checked → alert if below threshold for 3 consecutive days
```


## Overview

This document captures the finalized design decisions for recruiter email
validation, domain verification, and pipeline performance monitoring.

---

## Validation Pipeline (4 Layers)

### Layer 1 — Company Name Fuzzy Match
Runs BEFORE visiting CareerShift profile (saves quota on obvious mismatches).

```
Similarity >= 0.85  → visit profile (costs quota)
Similarity < 0.85   → reject silently, log to coverage_stats
```

### Layer 2 — Email Domain Validation
Runs AFTER profile visit, compares recruiter email domain against job URL.

```
Domain root exact/starts-with expected  → confidence = 'auto'
Domain root contains expected           → confidence = 'manual_review'
Domain root unrelated                   → reject, log to coverage_stats
```

**Expected domain extraction:**
```
Job URL: https://jobs.ashbyhq.com/collective/54259edc
  → ATS slug: "collective"
  → expected root: "collective"

Job URL: https://collective.com/careers/engineer
  → direct domain: "collective.com"
  → expected root: "collective"
```

**Examples:**
```
Expected: "collective"

john@collective.com        → root "collective" == "collective"      → auto
alice@collectiveinc.com    → root "collectiveinc" starts with       → auto
jane@ilovecollective.com   → root "ilovecollective" contains        → manual_review
bob@thecollective-la.com   → root "thecollective-la" contains       → manual_review
dave@unrelated.com         → root "unrelated" no match              → rejected
```

### Layer 3 — Gemini Verification
Runs for manual_review recruiters ONLY, using leftover Gemini quota
after normal AI content generation.

```
Gemini says YES (domain valid)
  → upgrade all recruiters for that company to 'auto'
  → store AI content in ai_cache
  → proceed to outreach normally

Gemini says NO (domain invalid)
  → check performance metrics (Layer 4)
  → discard or alert based on metrics

Gemini quota exhausted
  → retry for up to GEMINI_VERIFY_RETRY_DAYS (5 days)
  → after retry window expires → check metrics → discard or alert
```

**Gemini prompt does double duty:**
- Generates email content (as normal)
- Verifies if recruiter email domain matches the company

### Layer 4 — Discard Decision
Runs when Gemini says NO or retry window expires.

```
Check metrics before discarding:

  Metric 1 >= 50% AND Metric 2 >= 60%
    → discard silently
    → mark application status = 'exhausted'
    → log to coverage_stats
    → no alert

  Metric 1 < 50% OR Metric 2 < 60%
    → do NOT discard
    → fire alert email
    → human intervention required
```

---

## Performance Metrics

### Metric 1 — Find-Only Pipeline Performance
Measures how well the scraper performed on companies it attempted.

```
Formula: (auto confident companies found / companies attempted) * 100

Example:
  Applied to 10 companies
  Already have recruiters for 4 → excluded from attempt
  Attempted: 6 companies
  Found with auto confidence: 4

  Metric 1 = (4/6) * 100 = 66.7%
```

**Thresholds:**
```
Green:  >= 70%  → healthy, no intervention
Yellow: 50-70%  → degrading, monitor
Red:    < 50%   → alert fires for 3 consecutive days
```

### Metric 2 — Outreach Pipeline Performance
Measures overall coverage across ALL applications.

```
Formula: (companies with sendable recruiters / total applications) * 100

Example:
  Total applications: 10
  Already had recruiters: 4
  Newly found (auto): 4
  Total ready for outreach: 8

  Metric 2 = (8/10) * 100 = 80%
```

**Thresholds:**
```
Green:  >= 75%  → healthy, no intervention
Yellow: 60-75%  → degrading, monitor
Red:    < 60%   → alert fires for 3 consecutive days
```

---

## Discard Behavior

```
What gets discarded:
  - recruiter rows for that company
  - application_recruiters links

What gets KEPT:
  - application record (status changed to 'exhausted')
  - job description cache
  - coverage_stats log entry

Future behavior:
  - exhausted applications excluded from find-only scraping
  - excluded from Metric 2 denominator? TBD

Reactivation (manual):
  python pipeline.py --reactivate "CompanyName"
  → resets application status to 'active'
  → next find-only run retries scraping
```

---

## Status Values

### applications.status
```
'active'     → normal, included in all pipeline steps
'exhausted'  → no recruiters found after all retries, excluded from scraping
'closed'     → job closed/withdrawn by user
```

### recruiters.confidence
```
'auto'          → validated, proceed to outreach
'manual_review' → pending Gemini domain verification
'rejected'      → domain mismatch, excluded from outreach
```

### recruiters.recruiter_status
```
'active'   → sendable, included in outreach
'inactive' → bounced or left company, excluded from outreach
```

---

## Configuration (config.py)

```python
# ─────────────────────────────────────────
# RECRUITER VALIDATION SETTINGS
# ─────────────────────────────────────────
SIMILARITY_THRESHOLD          = 0.85   # company name fuzzy match cutoff
DOMAIN_MATCH_THRESHOLD        = 0.80   # email domain validation cutoff
GEMINI_VERIFY_RETRY_DAYS      = 5      # days to retry Gemini verification

# ─────────────────────────────────────────
# PIPELINE PERFORMANCE THRESHOLDS
# ─────────────────────────────────────────
METRIC1_ALERT_THRESHOLD       = 50     # find-only performance % (Red threshold)
METRIC2_ALERT_THRESHOLD       = 60     # outreach coverage % (Red threshold)
METRIC_ALERT_CONSECUTIVE_DAYS = 3      # consecutive days before alert fires
```

---

## New DB Tables Required

### coverage_stats
Tracks daily pipeline performance metrics.

```sql
CREATE TABLE coverage_stats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    date                DATE NOT NULL,
    total_applications  INTEGER,
    companies_attempted INTEGER,
    auto_found          INTEGER,
    manual_review_found INTEGER,
    rejected_count      INTEGER,
    exhausted_count     INTEGER,
    metric1             REAL,   -- find-only performance %
    metric2             REAL,   -- outreach coverage %
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### recruiter_verification_queue
Tracks manual_review recruiters pending Gemini verification.

```sql
CREATE TABLE recruiter_verification_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    recruiter_id    INTEGER NOT NULL REFERENCES recruiters(id),
    application_id  INTEGER NOT NULL REFERENCES applications(id),
    expected_domain TEXT,
    similarity_score REAL,
    domain_score    REAL,
    retry_count     INTEGER DEFAULT 0,
    first_attempted DATE,
    last_attempted  DATE,
    status          TEXT DEFAULT 'pending',  -- pending/verified/rejected/expired
    gemini_verdict  TEXT,                    -- yes/no/unknown
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### applications table — new column
```sql
ALTER TABLE applications ADD COLUMN expected_domain TEXT;
ALTER TABLE applications ADD COLUMN exhausted_at TIMESTAMP;
```

---

## New CLI Commands

```bash
# Check pipeline performance metrics
python pipeline.py --performance-report

# Reactivate exhausted application
python pipeline.py --reactivate "CompanyName"

# Show pending verification queue status
python pipeline.py --verification-status
```

---

## Alert Email Format

### Performance Alert
```
Subject: Pipeline Performance Alert - Action Required

FIND-ONLY PERFORMANCE (Metric 1) - DEGRADED
  2026-02-26: 3/6 companies found (50%)
  2026-02-27: 2/6 companies found (33%)
  2026-02-28: 2/5 companies found (40%)

  Average: 41% - below 50% threshold

Most rejected companies (last 7 days):
  - Collective: 5 rejections (domain mismatch)
  - Stripe: 3 rejections (name similarity 0.71)

Recommendation:
  Review SIMILARITY_THRESHOLD (currently 0.85)
  Review DOMAIN_MATCH_THRESHOLD (currently 0.80)
  Or manually reactivate exhausted applications:
    python pipeline.py --reactivate "Collective"
```

---

## Summary Flow

```
--find-only run:

  For each company needing scraping:

    Layer 1: fuzzy name match
      < 0.85  → reject, log, skip
      >= 0.85 → visit profile

    Layer 2: domain validation
      exact/starts-with → auto
      contains          → manual_review → queue for Gemini
      unrelated         → reject, log

  AI generation step:
    auto companies → generate normally
    manual_review  → use leftover Gemini quota to verify + generate

  Gemini verification:
    YES → upgrade to auto, store content, proceed
    NO  → check metrics → discard/exhaust or alert
    exhausted → retry up to 5 days → check metrics → discard/exhaust or alert

  Coverage stats recorded daily
  Metrics checked → alert if below threshold for 3 consecutive days
```