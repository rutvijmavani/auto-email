# Multi-User Architecture — Design Document

> **Status:** Design only — no code written yet.  
> **Date:** 2026-07-14  
> **Initial scope:** 2 users (operator + fiancée). Design is extensible to more.

---

## Table of Contents

1. [Scope](#1-scope)
2. [Shared vs Per-User Boundary](#2-shared-vs-per-user-boundary)
3. [Google Form — User Attribution](#3-google-form--user-attribution)
4. [Database Schema Changes](#4-database-schema-changes)
5. [CareerShift Quota Logic](#5-careershift-quota-logic)
6. [Recruiter Verification Routing](#6-recruiter-verification-routing)
7. [Gemini Quota Logic](#7-gemini-quota-logic)
8. [AI Cache](#8-ai-cache)
9. [Metrics & Monitoring](#9-metrics--monitoring)
10. [What Does NOT Change](#10-what-does-not-change)
11. [Implementation Order](#11-implementation-order)

---

## 1. Scope

Initial deployment targets **2 users** — the operator and their fiancée. All design decisions below are made with 3–10 future users in mind so the system doesn't need re-architecting when more users are added.

### In Scope

| Feature | Detail |
|---|---|
| User management | Admin-managed only. No self-service, no web UI, no auth system. Operator adds users via admin script. |
| Per-user applications | Each user tracks their own job applications independently. |
| Per-user outreach | Each user sends their own recruiter emails; tracked separately. |
| CareerShift quota pooling | Each user has their own account. Quota is used for own applications first, then leftover is pooled. See §5. |
| Recruiter account tracking | Track which CareerShift account found each recruiter so verification always routes to the same account. |
| Per-user Gemini key (email) | Each user's outreach email content is generated from their own Gemini API key. |
| Gemini pool (ATS detection) | ATS structure detection uses a shared pool across both users' Gemini keys. |
| Shared job digest | Both users receive the same job digest email. No per-user watchlist filtering. |
| Shared prospective companies | Both users add to the same pool. Both benefit from each other's additions. |

### Explicitly Out of Scope (for now)

| Feature | Reason deferred |
|---|---|
| Per-user job digest / watchlist | Both users get the same digest. Add per-user filtering only when users have diverging interests. |
| User-facing web UI | Phase 3. |
| Self-service user management | Phase 3. |
| Authentication / login | Phase 3. |
| Per-user job status (seen/saved) | Not needed for 2 users. |
| Recruiter attribution to user | Recruiters are shared knowledge. `found_by_user_id` is for account routing only, not attribution. |

---

## 2. Shared vs Per-User Boundary

**Core principle:** jobs and recruiters are non-rivalrous shared knowledge. Application history and outreach are personal.

| Component | Boundary | Reason |
|---|---|---|
| `job_postings` | **Shared** | Same jobs exist regardless of who is watching. No per-user rows. |
| `prospective_companies` | **Shared** + submitter pointer | Adding a company benefits everyone. Add `added_by_user_id` for attribution only — does not affect processing. |
| System alert emails (watchdog, log monitor, find/verify reports) | **Operator inbox only** | System-level alerts go to operator email only. Never to User 2. |
| Quota alert emails (CareerShift, Gemini) | **Per-user inbox** | Each user gets their own quota alerts so they can act on them independently. Sent FROM operator SMTP; delivered TO `GMAIL_USER_{id}_EMAIL`. |
| Resume attachment | **Per-user** | Each user has their own resume. `RESUME_PATH_USER_{id}` env var. `email_sender.py` resolves at send time from `user_id`. |
| `recruiters` | **Shared** + account pointer | Recruiter emails are shared knowledge. Add `found_by_user_id` for verification routing only (see §6). |
| `seen_job_ids` | **Shared** | Global dedup — no need to re-scan jobs both users have already seen. |
| `ai_cache` | **Shared** | Cache key is `SHA256(company + job_title + job_text)`. Content is generic outreach copy, not personalised to the sender. Both users benefit from cached entries. |
| ATS detection Gemini key | **Shared pool** | Structure detection output is identical regardless of user. Pool both users' Gemini keys for 2× daily quota. |
| Job digest email | **Shared content** | Both users receive the same digest. Revisit when users have diverging interests. |
| `applications` | **Per-user** | Each user applies independently to different companies. |
| `outreach` | **Per-user** | Each user contacts recruiters under their own name and email. `user_id` determines which Gmail account sends the email. |
| `application_recruiters` | **Per-user** (no new column needed) | Links applications to recruiters. User context is always derivable via `JOIN applications ON applications.id = application_recruiters.application_id`. Adding `user_id` here would duplicate what's already on `applications` and risk going out of sync. |
| `careershift_quota` | **Per-user** | Each CareerShift account has its own 50/day quota. Tracked separately. |
| `model_usage` | **Per-user** (email) + **shared** (ATS) | Split by `use_case`: `email_content` is per-user, `ats_detection` is shared pool. |
| `coverage_stats` | **Per-user** | Metric 1 and Metric 2 are computed from a specific user's application set. Meaningless when mixed. |
| `quota_alerts` | **Per-user** | Alerts are about a specific user's quota usage patterns. |

---

## 3. Google Form — User Attribution

### Applications form

Both users submit applications through the **same existing Google Form**. A single dropdown field added as the **first question** identifies who is applying.

**Form change:** Add question 1 — *"Who is applying?"* — dropdown with the two users' names (e.g. `Rutvi`, `Fiancée`). All existing questions shift down by one; their order relative to each other does not change.

**Why this approach over alternatives:**
- *Two separate forms* — extra configuration, two Sheets to read, field order can drift independently between forms.
- *Google "Collect email addresses"* — works only when the submitter is signed into the correct Google account. Easy to accidentally submit from the wrong account silently.
- *Dropdown on shared form* — explicit, no Google account dependency, single form to maintain, trivially fixable if the wrong name is selected (edit the Sheet row).

**Pipeline change:** When reading the Sheet, column 1 is now the `user_id` lookup:

```python
USER_NAME_MAP = {
    "Rutvi":    1,
    "Fiancée":  2,
}

def resolve_user_id(name: str) -> int:
    uid = USER_NAME_MAP.get(name.strip())
    if uid is None:
        raise ValueError(f"Unknown user name in form submission: {name!r}")
    return uid
```

The resolved `user_id` is attached to the `applications` row at insert time.

> **Adding a new user later:** Add their name to `USER_NAME_MAP`, add them to the form dropdown, add them to the `users` table. That's the full change.

### Prospective companies form

The same pattern applies to `prospective_form_sync.py`. Add a *"Who is adding?"* dropdown as the **first question** on the Prospective tab/form. The resolved `user_id` is stored in a new `added_by_user_id` column on `prospective_companies` (attribution only — does not affect how the company is processed or monitored; the table remains fully shared).

```python
def resolve_user_id(name: str) -> int:
    # Same USER_NAME_MAP as the applications form.
    uid = USER_NAME_MAP.get(name.strip())
    if uid is None:
        raise ValueError(f"Unknown user name in prospective form submission: {name!r}")
    return uid
```

**`prospective_companies` schema change:**

```sql
ALTER TABLE prospective_companies
  ADD COLUMN IF NOT EXISTS added_by_user_id INT REFERENCES users(id) ON DELETE SET NULL;
-- No backfill needed — existing rows stay NULL (pre-multi-user era).
```

> **`log_monitor.py` env var inconsistency:** `log_monitor.py` reads `GMAIL_EMAIL` and `GMAIL_APP_PASSWORD` while the rest of the codebase uses `EMAIL` and `APP_PASSWORD`. Standardise to `EMAIL` / `APP_PASSWORD` during implementation (or to the new `GMAIL_USER_1_*` naming if system alerts are refactored at the same time).

### System alert emails

All system-level alert emails (watchdog, log monitor, find/verify reports, pipeline alerts) go to the **operator's inbox only** (`users.id = 1`, env var `EMAIL`). These are infrastructure alerts — User 2 does not receive them. No code change needed for routing; the existing single-address scheme is correct. The only change is standardising `log_monitor.py`'s env var names to match the rest of the codebase.

---

## 4. Database Schema Changes

### 4.1 New Tables

#### `users`

```sql
CREATE TABLE users (
    id          SERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,           -- digest + quota alert email
    name        TEXT NOT NULL,                  -- display name (used in alert subjects)
    resume_path TEXT NOT NULL DEFAULT 'Resume.pdf', -- filename at repo root
    is_active   BOOLEAN DEFAULT TRUE,           -- soft-disable without deleting
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
```

> **No authentication columns.** Phase 1 is operator-managed only. No passwords, tokens, or sessions.
>
> **Non-secret per-user config belongs here.** Resume path is not a secret — storing it in the DB means adding a new user requires only one `INSERT`, with zero env var or code changes. Secrets (SMTP passwords, API keys) stay in env vars where they can't be queried from SQL.
>
> **Never hard-delete users.** All child tables (`applications`, `outreach`, `model_usage`, `coverage_stats`, `quota_alerts`, `careershift_quota`) use `ON DELETE RESTRICT`, so a `DELETE FROM users` will fail if any history exists. Use `UPDATE users SET is_active = FALSE` to disable a user. This preserves all historical data and prevents accidental cascade-wipe of application/outreach history.

---

### 4.2 Modified Tables

#### `applications` — add `user_id`

```sql
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS user_id INT REFERENCES users(id) ON DELETE RESTRICT;

-- Backfill: assign all existing rows to operator (user_id = 1)
UPDATE applications SET user_id = 1 WHERE user_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_applications_user_id ON applications(user_id);

-- After backfill is confirmed:
ALTER TABLE applications ALTER COLUMN user_id SET NOT NULL;
```

> **Important:** All pipeline queries must filter by `user_id` going forward. A query without `user_id` will silently return all users' applications — a dangerous bug.

---

#### `outreach` — add `user_id`

```sql
ALTER TABLE outreach
  ADD COLUMN IF NOT EXISTS user_id INT REFERENCES users(id) ON DELETE RESTRICT;

UPDATE outreach SET user_id = 1 WHERE user_id IS NULL;

ALTER TABLE outreach ALTER COLUMN user_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_outreach_user_id ON outreach(user_id);
```

`user_id` drives two things: (1) whose name/signature appears in the email body, and (2) which Gmail SMTP credentials are used to send it. The pipeline resolves sender credentials at send time via `applications.user_id → users → env vars`:

```python
def get_smtp_creds(user_id: int) -> tuple[str, str]:
    return (
        os.environ[f"GMAIL_USER_{user_id}_EMAIL"],
        os.environ[f"GMAIL_USER_{user_id}_APP_PASS"],
    )
```

Required env vars per user:
```bash
GMAIL_USER_1_EMAIL=rutvi@gmail.com
GMAIL_USER_1_APP_PASS=xxxx          # Gmail App Password (not account password)
GMAIL_USER_2_EMAIL=fiancee@gmail.com
GMAIL_USER_2_APP_PASS=xxxx
```

---

#### Per-user resume attachment

Each user has their own resume. `email_sender.py` currently attaches a single hardcoded `RESUME_PATH = "Resume.pdf"` from `config.py`. In multi-user mode, the correct resume must be selected at send time based on `user_id`.

**Resume path lives in the `users` table — NOT in env vars.**

Env vars are right for secrets (passwords, API keys — never queryable from SQL). Resume path is not a secret. Storing it in the DB means adding user 3 later requires only one `INSERT` row — no env var changes, no code changes.

Add `resume_path` column to the `users` table (see §4.1 for full DDL — `resume_path TEXT NOT NULL DEFAULT 'Resume.pdf'` is already included there).

Both resume files sit at the repo root alongside the existing `Resume.pdf`. The `DEFAULT 'Resume.pdf'` is the fallback for any user whose resume hasn't been set.

**Resolution helper** (add to `db/users.py` or `email_sender.py`):
```python
def get_resume_path(user_id: int) -> str:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT resume_path FROM users WHERE id = %s", (user_id,)
        ).fetchone()
    if not row:
        raise ValueError(f"get_resume_path: no user found with id={user_id}")
    return row["resume_path"]
```

> The DB column has `DEFAULT 'Resume.pdf'` so every `INSERT` without an explicit `resume_path` gets the fallback — no Python code needs a fallback. Raising on a missing `user_id` catches misconfiguration before a wrong resume is attached to an email.

**`send_email()` signature change:**
```python
# Before (single-user)
def send_email(to_email, body, company, subject=None):
    with open(RESUME_PATH, "rb") as f: ...

# After (multi-user — user_id required, no default)
def send_email(to_email, body, company, subject=None, *, user_id: int):
    with open(get_resume_path(user_id), "rb") as f: ...
```

The caller (`outreach_engine.py`) already knows `user_id` from the application row — it just passes it through. The `add_user.py` admin script accepts `--resume-path Resume_Fiancee.pdf` and writes it directly to the `users` row.

**General rule for per-user config:**
| Type | Where | Reason |
|---|---|---|
| Secrets (SMTP passwords, API keys, CareerShift passwords) | Env vars (`GMAIL_USER_{id}_APP_PASS`) | Never in DB — not queryable, compatible with secret managers |
| Non-secret config (resume path, display name, future preferences) | `users` table column | Scales to N users with zero code or env changes |

> **`ai_cache` is not affected.** The cache key is `SHA256(company + job_title + job_text)` and the cached content is the AI-generated email body — resume is a file attachment, not part of the email body, so cache sharing across users remains valid.

---

> **`application_recruiters` needs no new column.** User context is always one JOIN away through `application_recruiters → applications.user_id`. Adding `user_id` directly would be redundant data that can go out of sync.

---

#### `recruiters` — add `found_by_user_id`

```sql
ALTER TABLE recruiters
  ADD COLUMN IF NOT EXISTS found_by_user_id INT REFERENCES users(id) ON DELETE SET NULL;

-- Backfill: all existing recruiters were found using User 2's CareerShift account
-- (operator ran find_emails.py with her credentials before multi-user was implemented)
UPDATE recruiters SET found_by_user_id = 2 WHERE found_by_user_id IS NULL;
```

> **Note:** `found_by_user_id` is NOT about attribution — it is purely a routing key so recruiter verification always uses the CareerShift account that originally cached this profile. Re-visiting a profile from a different account counts as a new visit and burns that account's quota. The `recruiters` table itself remains shared with no `user_id` partition.
>
> **Why user_id=2 for the backfill:** All recruiter scraping before multi-user was done using User 2's (fiancée's) CareerShift credentials. Those profiles are cached under her account. Routing verification to User 1's account would burn quota re-visiting already-cached profiles.

---

#### `careershift_quota` — add `user_id`, change unique key

```sql
ALTER TABLE careershift_quota
  ADD COLUMN IF NOT EXISTS user_id INT REFERENCES users(id) ON DELETE RESTRICT;

-- Backfill: existing quota rows tracked User 2's account (her credentials were in use)
UPDATE careershift_quota SET user_id = 2 WHERE user_id IS NULL;

ALTER TABLE careershift_quota ALTER COLUMN user_id SET NOT NULL;

-- Replace (date UNIQUE) with (user_id, date) composite unique key
ALTER TABLE careershift_quota DROP CONSTRAINT IF EXISTS careershift_quota_date_key;
ALTER TABLE careershift_quota ADD CONSTRAINT careershift_quota_user_date_key
  UNIQUE (user_id, date);
```

Each user now has their own row per day:
```
user_id=1, date='2026-07-14', used=12, remaining=38
user_id=2, date='2026-07-14', used=0,  remaining=50
```

---

#### `model_usage` — add `user_id` + `use_case`

```sql
ALTER TABLE model_usage
  ADD COLUMN IF NOT EXISTS user_id   INT  REFERENCES users(id) ON DELETE RESTRICT,
  ADD COLUMN IF NOT EXISTS use_case  TEXT NOT NULL DEFAULT 'email_content',
  ADD COLUMN IF NOT EXISTS key_slot  TEXT;
-- user_id:  NULL for shared ats_detection rows; set for per-user email_content rows
-- use_case: 'email_content' | 'ats_detection'
-- key_slot: 'primary' | 'fallback' for ats_detection rows; NULL for email_content rows

-- Backfill existing rows (all were email_content on behalf of operator)
UPDATE model_usage SET user_id = 1, use_case = 'email_content'
WHERE user_id IS NULL;

-- PRIMARY KEY cannot include a nullable column in PostgreSQL.
-- Use partial unique indexes instead so NULL user_id (shared ATS rows) is supported.
ALTER TABLE model_usage DROP CONSTRAINT IF EXISTS model_usage_pkey;

-- Per-user rows: unique on (model, date, use_case, user_id)
CREATE UNIQUE INDEX IF NOT EXISTS model_usage_per_user
  ON model_usage(model, date, use_case, user_id)
  WHERE user_id IS NOT NULL;

-- Shared ATS rows: unique on (model, date, use_case, key_slot)
CREATE UNIQUE INDEX IF NOT EXISTS model_usage_shared
  ON model_usage(model, date, use_case, key_slot)
  WHERE user_id IS NULL;
```

`get_ats_gemini_key()` in §7 must check each slot separately:
```python
def get_ats_gemini_key() -> str:
    """Return the ATS Gemini key with remaining quota; raise QuotaExhausted if both are empty."""
    if get_model_usage_remaining(use_case='ats_detection', key_slot='primary') > 0:
        return os.environ["GEMINI_ATS_KEY_PRIMARY"]
    if get_model_usage_remaining(use_case='ats_detection', key_slot='fallback') > 0:
        return os.environ["GEMINI_ATS_KEY_FALLBACK"]
    raise QuotaExhausted("both ATS Gemini keys exhausted for today")
```

---

#### `coverage_stats` — add `user_id`

```sql
ALTER TABLE coverage_stats
  ADD COLUMN IF NOT EXISTS user_id INT REFERENCES users(id) ON DELETE RESTRICT;

UPDATE coverage_stats SET user_id = 1 WHERE user_id IS NULL;

ALTER TABLE coverage_stats ALTER COLUMN user_id SET NOT NULL;

DROP INDEX IF EXISTS idx_coverage_stats_date;
CREATE UNIQUE INDEX idx_coverage_stats_user_date ON coverage_stats(user_id, date);
```

---

#### `quota_alerts` — add `user_id`

```sql
ALTER TABLE quota_alerts
  ADD COLUMN IF NOT EXISTS user_id INT REFERENCES users(id) ON DELETE RESTRICT;

UPDATE quota_alerts SET user_id = 1 WHERE user_id IS NULL;

ALTER TABLE quota_alerts ALTER COLUMN user_id SET NOT NULL;
```

---

### 4.3 Credential Storage

**Credentials are NOT stored in the database.** CareerShift login credentials and Gemini API keys are secrets — they live in `.env` only. The `users.id` is the routing key; the actual credentials are resolved at runtime.

**Environment variable naming convention:**

```bash
# CareerShift credentials (one set per user)
CAREERSHIFT_USER_1_EMAIL=rutvi@njit.edu
CAREERSHIFT_USER_1_PASS=secret
CAREERSHIFT_USER_2_EMAIL=fiancee@neu.edu
CAREERSHIFT_USER_2_PASS=secret

# Gemini API keys (one per user for email content)
GEMINI_API_KEY_USER_1=AIza...
GEMINI_API_KEY_USER_2=AIza...

# Shared Gemini pool for ATS detection
# Tries PRIMARY first; falls back to FALLBACK when PRIMARY is exhausted.
# These may point to the same keys as the per-user keys above.
GEMINI_ATS_KEY_PRIMARY=AIza...   # user_id=1's key
GEMINI_ATS_KEY_FALLBACK=AIza...  # user_id=2's key
```

**Helper functions:**

```python
def get_careershift_creds(user_id: int) -> tuple[str, str]:
    email = os.environ[f"CAREERSHIFT_USER_{user_id}_EMAIL"]
    passw = os.environ[f"CAREERSHIFT_USER_{user_id}_PASS"]
    return email, passw

def get_gemini_key(user_id: int) -> str:
    return os.environ[f"GEMINI_API_KEY_USER_{user_id}"]
```

---

## 5. CareerShift Quota Logic

### 4.1 Cron Job Flow (1 AM daily)

The existing `--find-only` cron at 1 AM processes **all users in a single run**. No separate scheduling per user. The run is split into two sequential phases.

```
1 AM cron: pipeline.py --find-only
│
├── PHASE 1: Each user's own account for own applications
│   ├── Login to User 1's CareerShift account
│   │   Sync User 1's remaining quota → careershift_quota
│   │   Run tiered verification (zero-cost — cached profiles)
│   │   Run scrape_company() for User 1's applications needing recruiters
│   │   Record final remaining quota for User 1
│   │
│   └── Login to User 2's CareerShift account
│       Sync User 2's remaining quota → careershift_quota
│       Run tiered verification (zero-cost — cached profiles)
│       Run scrape_company() for User 2's applications needing recruiters
│       Record final remaining quota for User 2
│
├── PHASE 2: Pool confirmed leftover quota → shared priority queue
│   Pool = User 1 remaining + User 2 remaining
│   Run shared priority queue (see §5.3) until pool = 0
│   Use whichever account has credits (exhaust-one-first strategy)
│
└── END: Write coverage_stats per user, check quota_alerts per user
```

### 4.2 Phase 1 — Own Applications

Each user is processed in sequence, sorted by `users.id ASC` (User 1 always before User 2).

**For each user:**

1. Login to this user's CareerShift account using credentials from env.
2. Sync real remaining quota from the CareerShift account page → write to `careershift_quota` for this user.
3. Run tiered recruiter verification — always routes to the account that **found** each recruiter (`recruiters.found_by_user_id`), not necessarily this user's account. Verification is zero-cost (cached profiles). See §6.
4. Get this user's applications needing recruiter scraping: `applications WHERE user_id = ? AND status = 'active'` filtered to those with fewer than `MIN_RECRUITERS_PER_COMPANY` active recruiters.
5. Run `calculate_distribution(remaining_quota, new_companies)` — same fair distribution logic already in place, scoped to this user's application set.
6. Run `scrape_company()` for each application. New recruiters inserted into shared `recruiters` table with `found_by_user_id = current_user.id`.
7. Record final remaining quota.

> **Overflow tracking:** If quota runs out before all of this user's applications are served, the unserved applications are recorded as *overflow*. They are not dropped — they are eligible for Phase 2 as the lowest-priority work.

### 4.3 Phase 2 — Pooled Leftover Queue

After Phase 1 completes for all users, confirmed remaining quota is pooled. Spent on the following priority queue in strict order.

**Account selection within Phase 2:** Exhaust-one-first. Use User 1's account until it hits 0, then switch to User 2's account (and so on for more users). Minimises login switches. If User 1 has 0 remaining, start directly with User 2.

### 4.4 Phase 2 Priority Queue

| Priority | Work item | Description |
|---|---|---|
| **1** | Top-up under-stocked companies | Companies (from any user's applications) where active recruiter count is below `MAX_CONTACTS_HARD_CAP` (currently 3). Shared benefit — more contacts per company helps all users. Scored by `(MAX_CONTACTS_HARD_CAP − recruiter_count) × recency_weight`. |
| **2** | Prospective companies | Companies in `prospective_companies` with no recruiter yet. Shared benefit — new companies added to the pool help all users. |
| **3** | Overflow new applications | Any user whose Phase 1 ran out of quota before all applications were served. Only reached after Priorities 1 and 2 are exhausted. Within Priority 3, process users in `users.id ASC` order. |

> **Why overflow is last:** Personal applications are personal work — each user's quota is their own responsibility. Shared work (top-up, prospective) benefits everyone and takes precedence over one user's overflow. Overflow work only consumes truly idle quota that would otherwise go unused that day.
>
> **When to borrow:** Borrowing only happens in Phase 2 with *confirmed* leftover quota — i.e., what remains after every user's Phase 1 has fully completed. There is no pre-emptive borrowing. A user's Phase 1 quota is never at risk from another user's run.

---

## 6. Recruiter Verification Routing

CareerShift's "free re-visit" applies **per account**. A profile cached by User 1's account is free to re-visit from User 1's session. Visiting the same profile from User 2's session counts as a new profile visit and burns 1 credit from User 2's quota.

**Routing rule:** Always verify a recruiter using the same CareerShift account that originally found them. `recruiters.found_by_user_id` is the authoritative routing key.

**Within Phase 1, per-user verification loop:**

```python
# When logged into current_user's CareerShift account during Phase 1:

# Only verify recruiters found by THIS account (free re-visit)
active_recruiters = db.query("""
    SELECT * FROM recruiters
    WHERE recruiter_status = 'active'
    AND found_by_user_id = %s
""", [current_user.id])

for recruiter in active_recruiters:
    if days_since(recruiter.verified_at) < TIER1_DAYS:
        continue                   # Tier 1: skip
    elif days_since(recruiter.verified_at) < TIER2_DAYS:
        verify_tier2(recruiter)    # lightweight search, 0 quota
    else:
        verify_tier3(recruiter)    # cached profile visit, 0 quota

# Recruiters found by OTHER users are verified during THEIR Phase 1 turn.
# Total verification coverage is complete after all users finish Phase 1.
```

> **Backfill edge case:** Recruiters migrated from before multi-user will have `found_by_user_id = 1`. This is correct — they were found during the operator's single-user sessions and their profiles are cached under that account.

---

## 7. Gemini Quota Logic

| Use case | Strategy | Key source | Tracked in |
|---|---|---|---|
| Email content generation | Per-user | `GEMINI_API_KEY_USER_{id}` | `model_usage WHERE use_case='email_content' AND user_id=?` |
| ATS structure detection | Shared pool | `GEMINI_ATS_KEY_PRIMARY` → `GEMINI_ATS_KEY_FALLBACK` | `model_usage WHERE use_case='ats_detection' AND user_id IS NULL` |

### Email Content (Per-User)

When generating outreach email content for an application, use the API key belonging to the user who owns that application (`applications.user_id`). Track usage in `model_usage` with `user_id = applications.user_id` and `use_case = 'email_content'`.

Leftover Gemini quota (after new applications are covered) fills `ai_cache` gaps — same existing behaviour, but scoped to the user whose key has remaining quota.

### ATS Detection (Shared Pool)

With 2 users each having their own Gemini key, ATS structure detection has 2× the daily quota (1,500 calls per key × 2 = up to 3,000 detections per day). Pool uses exhaust-one-first strategy:

```python
def get_ats_gemini_key() -> str:
    primary_remaining = get_model_usage_remaining(use_case='ats_detection', key='primary')
    if primary_remaining > 0:
        return os.environ["GEMINI_ATS_KEY_PRIMARY"]
    return os.environ["GEMINI_ATS_KEY_FALLBACK"]

# Usage tracked in model_usage with user_id=NULL and use_case='ats_detection'
# so it doesn't collide with either user's personal email quota.
```

---

## 8. AI Cache

**Decision: `ai_cache` stays fully shared. No schema changes needed.**

The cache key is `SHA256(company + job_title + job_text)`. Generated content (email intro, follow-ups, subject lines) is generic professional outreach copy — it does not embed the sender's name, resume, or personal details.

- If User 1 generates content for *Stripe / Senior Engineer* → cached for 21 days.
- If User 2 applies to the same role → cache hit → 0 Gemini quota consumed.
- Both users get identical email body copy; each sends it from their own email address.

> **Future:** If users want different tones, resume-aware intros, or name-embedded copy, the cache key will need a `user_id` component. Defer until users explicitly request personalisation.

---

## 9. Metrics & Monitoring

| Table | Change | Why |
|---|---|---|
| `coverage_stats` | Add `user_id`. Unique on `(user_id, date)`. | Metric 1 (find-only %) and Metric 2 (outreach coverage %) are computed per user's application set. Mixing users produces a meaningless number. |
| `quota_alerts` | Add `user_id`. | Underutilisation / exhaustion alerts are per-account. |
| `pipeline_alerts` | No change. | ATS API failures, scheduler issues — system-level, not per-user. |
| `monitor_stats` | No change. | Job monitoring is shared. |
| Watchdog / log monitor / find reports | No change to routing. | System-level operational alerts → operator inbox only. Standardise `log_monitor.py` to use `EMAIL`/`APP_PASSWORD` instead of `GMAIL_EMAIL`/`GMAIL_APP_PASSWORD` during implementation. |

### Quota alert email routing

Quota alerts (CareerShift underutilisation, Gemini exhaustion) are personal — only the affected user can act on them (apply fewer companies, use a different key, etc.). They must land in **that user's inbox**, not the operator's.

**Routing rule:**
| Alert type | Recipient |
|---|---|
| CareerShift quota exhausted / underutilised — User N | `users.email` (from DB) |
| Gemini quota exhausted — User N | `users.email` (from DB) |
| Watchdog worker death | Operator inbox (`users.email` where `id=1`) |
| Log monitor ERROR/WARNING | Operator inbox |
| ATS API health / pipeline alerts | Operator inbox |
| Find / verify / outreach reports | Operator inbox |
| Weekly summary digest | Both users (same content) |

**Helper function** (add to `outreach/report_templates/base.py` or a new `db/users.py`):
```python
def get_user_email(user_id: int) -> str:
    """Return the notification recipient email from the users table."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT email FROM users WHERE id = %s", (user_id,)
        ).fetchone()
    if not row:
        raise ValueError(f"get_user_email: no user found with id={user_id}")
    return row["email"]

def get_user_name(user_id: int) -> str:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT name FROM users WHERE id = %s", (user_id,)
        ).fetchone()
    if not row:
        raise ValueError(f"get_user_name: no user found with id={user_id}")
    return row["name"]
```

> **`users.email` is the canonical notification address.** It is stored in the DB at `add_user` time and is the single source of truth for where alerts are delivered. `GMAIL_USER_{id}_EMAIL` is for SMTP authentication (send-from) only and must not be used as the delivery address — the two may differ (e.g., alias vs primary address).

**`send_quota_alert(user_id, subject, body)`** — calls `get_user_email(user_id)` as the `to_email`. The alert is sent FROM the operator SMTP account (no need to use the user's App Password — this is an internal notification, not an outreach email).

Alert email subject format:
```
Subject: Quota Alert [Rutvi] — CareerShift Underutilized (3 days)
Subject: Quota Alert [Fiancée] — Gemini Exhausted
```

> **Serper quota alert** — Serper is a shared API key. Its credit exhaustion alert goes to the operator inbox only (it's a system-level resource, not per-user).

---

## 10. What Does NOT Change

| Component | Reason |
|---|---|
| Job monitoring pipeline (`--monitor-jobs`) | Shared. Scans all prospective companies, writes to shared `job_postings`. |
| Email digest | Both users receive the same digest. No per-user filtering yet. |
| Adaptive scheduler, workers, Redis streams | All infrastructure is shared and stateless with respect to users. |
| Tiered verification logic | Algorithm unchanged. Only account routing changes (§6). |
| `calculate_distribution()` | Unchanged. Still called per-user in Phase 1 with that user's quota. |
| `scrape_company()` | Unchanged. Called per-user in Phase 1. New recruiters get `found_by_user_id` set. |
| `ai_cache` | Shared, no changes (§8). |
| `seen_job_ids` | Global dedup, no changes. |
| `prospective_companies` | Shared pool. Only change: add `added_by_user_id` column + "Who is adding?" dropdown on the form (see §3). |
| ATS detection logic | Unchanged. Only Gemini key selection changes (§7). |

---

## 11. Implementation Order

Steps are ordered to avoid breaking production between them. Each step leaves the system in a working state.

JSON logging (surgical Option D) is woven into each step alongside the multi-user changes — wherever a file is already being opened, logging is upgraded at the same time to avoid touching files twice. Files not touched by multi-user retain existing logging until Phase 2 (multi-server).

1. **DB migrations** — Add `users` table. Add `user_id` columns to `applications`, `outreach`, `coverage_stats`, `quota_alerts`, `careershift_quota`, `model_usage`. Add `found_by_user_id` to `recruiters`. Add `added_by_user_id` to `prospective_companies`. Run as `ADD COLUMN IF NOT EXISTS` in `init_db()`. Backfill values per §4 (note: `recruiters.found_by_user_id` and `careershift_quota.user_id` backfill to 2, not 1). System continues working single-user throughout.

2. **Admin script: `scripts/add_user.py`** — CLI to insert a `users` row (with `name`, `email`, `resume_path`) and print the exact env vars to add to `.env`. This is the checklist that prevents silent omissions when onboarding a new user.

Example:
```
$ python scripts/add_user.py --name "Fiancée" --email fiancee@gmail.com --resume-path Resume_Fiancee.pdf

User created (id=2). Add these to .env:

  GMAIL_USER_2_EMAIL=fiancee@gmail.com
  GMAIL_USER_2_APP_PASS=<gmail-app-password>
  GEMINI_API_KEY_USER_2=<gemini-key>
  CAREERSHIFT_USER_2_EMAIL=<careershift-login>
  CAREERSHIFT_USER_2_PASS=<careershift-password>
```

**Why env vars scale fine for secrets:** The code reads `os.environ[f"GMAIL_USER_{user_id}_APP_PASS"]` dynamically — `user_id` comes from the DB at runtime, never hardcoded. Adding user 3 is one `INSERT` + five `.env` lines + zero code changes. The `.env` grows with users, which is fine for a personal tool at this scale. If the project ever grows to many users, the pattern is identical under a proper secret manager (AWS Secrets Manager, Vault) — both surface secrets as env vars, so no code changes even then.

3. **JSON logging infrastructure** — `logger.py` already exists at repo root with `init_logging()`, `get_logger()`, TTY detection, and file rotation. No new file needed. Changes: (a) add `JsonFormatter` class to `logger.py`; (b) swap file handler formatter from pipe-delimited to JSON inside `init_logging()`; (c) add `jq` aliases to `~/.bashrc` on the server (`logwatch`, `logerr`); (d) update `log_monitor.py`: replace `line.split('|', 3)` level parsing with `json.loads(line)["level"]` in BOTH `_is_flagged()` and `_is_suppressed()` (both must change together or WARNING suppression silently breaks); (e) add `init_logging()` to entry points that currently lack it (`pipeline.py` non-scheduler branches, `workers/watchdog.py`, `scripts/reschedule_on_deploy.py`); (f) remove module-level `logging.basicConfig()` from `jobs/job_scraper.py` (line 26) and `workers/watchdog.py` (line 2418).

4. **Update all DB query functions to filter by `user_id`** — Every function that reads `applications` or `outreach` must accept a `user_id` param. Audit: `db/applications.py`, `db/outreach.py`, `db/application_recruiters.py`, `db/cache.py`, the CareerShift find-emails flow, `pipeline.py`. Add assertions to catch unfiltered queries in dev.

   Key functions identified during full repo scan:
   - `db/outreach.py` — `get_pending_outreach()`, `schedule_outreach()`, `schedule_next_outreach()` all need `user_id` param; without it `get_pending_outreach()` returns every user's pending outreach to `outreach_engine.py`
   - `db/application_recruiters.py` — `get_companies_needing_scraping()` and `get_companies_needing_more_recruiters()` JOIN through `applications` but don't filter by `user_id`; the CareerShift Phase 1 loop must pass `user_id` so each user only scrapes their own applications
   - `db/cache.py` — `get_applications_missing_ai_cache()` queries the `applications` table unfiltered; without `user_id` scoping, AI content generation in Step 6 would try to fill cache for ALL users using the current user's Gemini key (wrong key, wrong quota tracking)

5. **Multi-user `--find-only` cron loop** — Refactor Phase 1 to iterate over all active users. Each iteration: login to that user's CareerShift account, sync quota, run verification (routed via `found_by_user_id`), scrape that user's applications, track overflow. After all users complete Phase 1, run Phase 2 pooled queue. Add structured logging throughout (`find_emails.py`, `quota_manager.py`, `careershift/`).

6. **Multi-user Gemini key selection** — Update email content generation to use `get_gemini_key(user_id)`. Update ATS detection to use pool logic. Update `model_usage` writes to include `user_id` and `use_case`. Add structured logging to `ai_full_personalizer.py` (currently has no logging).

7. **Per-user metrics and alerts** — Update `coverage_stats` writes to include `user_id`. Update quota alert checks to run per-user. Route quota alert emails to `GMAIL_USER_{id}_EMAIL` (not operator inbox) with user name in subject. Serper alert stays operator-only (shared resource).

8. **Per-user Gmail SMTP + resume** — Add `GMAIL_USER_{id}_EMAIL` and `GMAIL_USER_{id}_APP_PASS` to `.env` for each user (SMTP credentials only — resume path comes from `users.resume_path` in the DB, not an env var). Update `email_sender.py`: make `user_id` a required keyword argument with no default, resolve SMTP credentials from env and resume path from DB at send time. Add `get_resume_path(user_id)` helper that raises if user not found. Add structured logging to `email_sender.py` and `outreach_engine.py` (currently has no logging). Each user generates their own Gmail App Password via Google account settings → Security → App Passwords.

   Also update `workers/startup.py`: the `_GMAIL_ENV_KEYS` list currently checks for `GMAIL_EMAIL` and `GMAIL_APP_PASSWORD` (the single-user names). With multi-user these are replaced by `GMAIL_USER_{id}_EMAIL` / `GMAIL_USER_{id}_APP_PASS`. Update the `check_gmail=True` path to validate that at least `GMAIL_USER_1_EMAIL` and `GMAIL_USER_1_APP_PASS` are present, or iterate over all active `user_id`s from the DB.

9. **Frontend** — Build after the full multi-user backend is verified working. At this point both users' data exists in the DB, making the frontend immediately useful: browse all 39K+ jobs with filters, see per-user application and outreach status, verify job descriptions and locations are storing correctly. Tool: NocoDB (zero code, connects directly to PostgreSQL) or Retool (drag-and-drop, more polished).

---

## JSON Logging — Design

### Storage format
Log files on disk always store JSON. The file handler always uses `JsonFormatter` regardless of how the script is invoked.

### What changes in `logger.py` (modify existing file — do NOT create `utils/logging.py`)

`logger.py` already exists at the repo root and provides `init_logging()`, `get_logger()`, TTY detection, file rotation, and 14/35-day log cleanup. It is the centralized logging system. The JSON migration requires **two targeted changes** to this file only:

**1. Add `JsonFormatter` class** (alongside the existing `logging.Formatter` setup):
```python
import json   # add to existing imports

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "time":   self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)
```

**2. Swap file handler formatter** inside `init_logging()`. The file handlers (`file_handler` and `catchall`) currently use the pipe-delimited `formatter`. Change them to `JsonFormatter()`. The TTY console handler keeps the existing `formatter` (human-readable pipe-delimited) — TTY detection is already implemented via `sys.stdout.isatty()`.

```python
# Current (pipe-delimited for all handlers):
formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
...
file_handler.setFormatter(formatter)
catchall.setFormatter(formatter)

# After (JSON for file handlers; human-readable for TTY console):
human_formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
json_formatter  = JsonFormatter()
...
# console handler (TTY only) — stays human-readable:
console.setFormatter(human_formatter)
# file handlers — always JSON:
file_handler.setFormatter(json_formatter)
catchall.setFormatter(json_formatter)
```

No other files need touching — every module that calls `get_logger(__name__)` inherits the JSON formatter from the root logger automatically.

### Reading logs on the server

| Scenario | What you see |
|---|---|
| `python pipeline.py --find-only` in SSH (TTY) | Human-readable pipe-delimited (console handler) |
| systemd / cron run | JSON in log file only (no TTY → no console handler) |
| `cat logs/find_2026-07-14.log` | Raw JSON lines |
| `logwatch` alias | Human-readable (jq converts on the fly) |
| `logerr` alias | Human-readable, ERROR/WARNING only |

```bash
# Add to ~/.bashrc on the server
# Install first: sudo dnf install jq

alias logwatch='tail -f logs/pipeline_$(date +%Y-%m-%d).log | jq -r "[.time,.level,.logger,.msg]|join(\" | \")"'
alias logerr='tail -f logs/pipeline_$(date +%Y-%m-%d).log | jq -r "select(.level==\"ERROR\" or .level==\"WARNING\")|[.time,.level,.logger,.msg]|join(\" | \")"'
```

> Use `logs/pipeline_$(date +%Y-%m-%d).log` as the default tail target — it is the catch-all daily file that every command writes to. To tail a specific command's log (e.g. `find`), use `logs/find_$(date +%Y-%m-%d).log`.

### log_monitor.py upgrade (Option C → D)
```python
# Before (Option C — field-split, fragile)
parts = line.split('|', 3)
level = parts[1].strip() if len(parts) >= 2 else ""

# After (Option D — JSON parse, with plain-text fallback for tracebacks)
def _parse_log_level(line: str) -> str:
    try:
        return json.loads(line).get("level", "")
    except (json.JSONDecodeError, ValueError):
        # Tracebacks and bare exception lines are not JSON — fall back to regex
        m = re.search(r'\b(DEBUG|INFO|WARNING|ERROR|CRITICAL)\b', line)
        return m.group(1) if m else ""
```

### Scope — full migration, entry points only

Because Python logging propagates from the root logger, **only entry points need `init_logging()`** (the existing function in `logger.py`). Every other file (`db/`, `careershift/`, `outreach/`, `jobs/`, `workers/` modules) inherits JSON formatting automatically — zero changes needed in those files.

---

#### 1. Files with hardcoded `logging.basicConfig()` — must fix

| File | Line | Issue |
|---|---|---|
| `workers/watchdog.py` | 2418 | `logging.basicConfig(level=INFO, format=...)` inside `__main__` — replace with `init_logging("watchdog")` |
| `jobs/job_scraper.py` | 26 | **Module-level** `logging.basicConfig()` — fires on import before any entry point can configure the root logger. Remove entirely (it's redundant once `init_logging` runs at the entry point). Highest-risk file in the migration. |
| `scripts/test_wayfair.py` | 31 | One-off diagnostic script — exclude from migration, leave as-is. |

---

#### 2. Production entry points — add `init_logging()` call

**Systemd daemons:**

| File | Invoked by | Status |
|---|---|---|
| `pipeline.py` | All cron wrappers (`run_sync.sh`, `run_nightly.sh`, `run_outreach.sh`, `run_monitor.sh`, etc.) | Partial — `init_logging("scheduler")` exists only for `--scheduler` mode (line 914). All other flags (`--find-only`, `--outreach-only`, `--sync-forms`, `--verify-filled`, `--monitor-jobs`, `--weekly-summary`, `--detect-ats`) run with NO logging configured. Fix: add `init_logging(command_name)` to each `elif` branch in `main()`. |
| `workers/watchdog.py` | `recruiter-watchdog.service` | NO — has `logging.basicConfig()` instead (line 2418). Replace with `init_logging("watchdog")`. |

**Cron scripts:**

| File | Schedule | Status |
|---|---|---|
| `scripts/log_monitor.py` | `*/15 * * * *` | NO — uses `print()` only. Add `init_logging("log_monitor")` + upgrade parser (see §below). |
| `scripts/reschedule_on_deploy.py` | Deploy hook | NO — uses `get_logger` + `logger.info/error` but never calls `init_logging()`. All log output silently dropped. Add `init_logging("reschedule_on_deploy")`. |
| `build_ats_slug_list.py` | Monthly chain | Already done — `init_logging("build_ats_slug_list")` at line 962. |
| `enrich_ats_companies.py` | Daily 3 AM + monthly | Already done — `init_logging("enrich_ats_companies")` at line 822. |
| `scripts/backup_db.py` | Nightly chains | Uses `print()` only — not Python logging. Leave as-is (backup output doesn't need JSON). |
| `scripts/redis_signal.py` | Multiple chains | Uses `print()` only. Leave as-is. |

**Workers (spawned as multiprocessing children — inherit root logger from scheduler in production):**

Their `__main__` blocks are only reached when run standalone by an operator for debugging. Add `init_logging()` so standalone runs produce structured output:

| File | `__main__` line |
|---|---|
| `workers/scan_worker.py` | 971 |
| `workers/detail_worker.py` | 1257 |
| `workers/fullscan.py` | 1543 |
| `workers/__main__.py` | 5 — dead in production (no systemd unit uses it), but operators may invoke `python -m workers` accidentally |

**One-shot systemd (OnFailure):**

| File | Status |
|---|---|
| `scripts/startup_failure_alert.py` | Uses `print()` only — output goes to journald via `StandardOutput=journal`. Leave as-is; `print()` is sufficient for one-shot failure alerts. |

---

#### 3. `log_monitor.py` — two locations need parser upgrade (C → D)

Both `_is_flagged()` and `_is_suppressed()` do `line.split('|', 3)` to extract level. After JSON migration, pipe delimiters disappear. **Both must be updated together** or WARNING suppression silently breaks (all suppressed warnings start firing as alerts).

```python
# Shared helper — replace both field-split blocks with this
def _parse_log_level(line: str) -> str:
    try:
        return json.loads(line).get("level", "")
    except (json.JSONDecodeError, ValueError):
        # Tracebacks and bare exception lines are not JSON — regex fallback
        m = re.search(r'\b(DEBUG|INFO|WARNING|ERROR|CRITICAL)\b', line)
        return m.group(1) if m else ""
```

Also standardise env vars: `GMAIL_EMAIL` → `EMAIL`, `GMAIL_APP_PASSWORD` → `APP_PASSWORD` (lines 699–700).

---

#### 4. Not touched

- `tests/` — all test files
- `scripts/test_*.py`, `scripts/diagnose_*.py`, `scripts/check_thundering_herd.py`, `scripts/health_check.py` — one-off operator tools, not production log producers
- `scripts/test_wayfair.py` — has `basicConfig` but is a diagnostic script; excluded

**Total files to edit: 10.** All other files (`db/`, `careershift/`, `outreach/`, `jobs/`) get JSON logging for free with zero changes.

---

## 12. CareerShift Session Files

### Problem

CareerShift login does not work in headless mode. The existing flow is:

1. **Local machine** (headed Chrome): `python careershift/auth_njit.py` → browser opens → manual login → session saved to `data/careershift_session.json`
2. **SCP to server**: `scp data/careershift_session.json opc@server:/home/opc/mail/data/`
3. **Server** (headless): `--find-only` loads the session file into Playwright — no login needed, just cookie replay

In multi-user mode each CareerShift account needs its own session file. A single shared file would be overwritten mid-run if both accounts are logged in sequentially.

### Solution: Per-user session files

```
data/careershift_session_1.json   # User 1 (operator)
data/careershift_session_2.json   # User 2 (fiancée)
data/careershift_session_N.json   # User N — scales to any number of users
```

### Code changes

**`careershift/constants.py`** — replace `SESSION_FILE` constant with a function:

```python
def get_session_file(user_id: int) -> str:
    return os.path.join(os.path.dirname(__file__), "..", "data", f"careershift_session_{user_id}.json")
```

**`careershift/auth_njit.py` + `careershift/auth.py`** — accept `--user-id` CLI flag:

```bash
python careershift/auth_njit.py --user-id 1   # saves data/careershift_session_1.json
python careershift/auth_njit.py --user-id 2   # saves data/careershift_session_2.json
```

**`careershift/find_emails.py`** — in Phase 1 loop, load the correct session file per user:

```python
context = browser.new_context(
    storage_state=get_session_file(user.id),
    ...
)
```

**`.gitignore`** — update pattern to cover all users:

```
# Before
careershift_session.json

# After
careershift_session_*.json
```

### Migration from single-user

The existing `data/careershift_session.json` was generated using User 2's credentials (her account was used for all scraping before multi-user). On migration:

```bash
# On server — rename existing session to user 2
mv data/careershift_session.json data/careershift_session_2.json

# On local machine — authenticate user 1's account fresh
python careershift/auth_njit.py --user-id 1
scp data/careershift_session_1.json opc@server:/home/opc/mail/data/
```

### Ongoing session renewal (~30 days)

Sessions expire after approximately 30 days. When a session expires:

```bash
# On local machine
python careershift/auth_njit.py --user-id N
scp data/careershift_session_N.json opc@server:/home/opc/mail/data/
```

`add_user.py` prints these exact commands when a new user is created so the operator always knows what to run.

### Session file check in find_emails.py

Phase 1 should check that each user's session file exists before starting, and skip (with a warning) rather than crash if it's missing — this prevents one missing session from blocking all other users:

```python
session_path = get_session_file(user.id)
if not os.path.exists(session_path):
    logger.warning("find_emails: session file missing for user_id=%d (%s) — skipping. "
                   "Run auth_njit.py --user-id %d and SCP to server.",
                   user.id, user.name, user.id)
    continue
```
