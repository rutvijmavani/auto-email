# Configuration

All configuration lives in `config.py` in the project root. Change values here — the rest of the codebase reads from this file.

---

## Email Credentials

```python
EMAIL        = os.getenv("EMAIL")         # your Gmail address
APP_PASSWORD = os.getenv("APP_PASSWORD")  # Gmail app password (not account password)
RESUME_PATH  = "Resume.pdf"               # path to your resume file
```

**Gmail App Password setup:**
1. Enable 2-factor authentication on your Google account
2. Go to Google Account → Security → App Passwords
3. Generate a password for "Mail"
4. Add it to your `.env` file as `APP_PASSWORD`

---

## Outreach Settings

```python
SEND_INTERVAL_DAYS = 7  # days between each email stage
```

Controls the gap between initial → follow-up 1 → follow-up 2:
```
Day 0:  initial email
Day 7:  follow-up 1
Day 14: follow-up 2
Day 21: sequence complete
```

**Recommended range:** 5-10 days. Below 5 is too aggressive. Above 10 and recruiters may forget who you are.

---

## Send Window Settings

```python
SEND_WINDOW_START  = 9    # 9:00 AM — start sending
SEND_WINDOW_END    = 11   # 11:00 AM — preferred end
GRACE_PERIOD_HOURS = 1    # hard cutoff at 12:00 PM
SEND_TIMEZONE      = "America/New_York"
```

**Behavior:**
- Before 9 AM → pipeline waits until 9 AM then starts
- 9 AM - 11 AM → sends normally
- 11 AM - 12 PM → grace period, continues if emails still pending
- After 12 PM → hard cutoff, remaining emails rescheduled to tomorrow

**Why 9-11 AM?** Recruiters typically check email first thing in the morning. Sending in this window maximizes the chance your email is seen at the top of their inbox.

**Timezone:** Set to your local timezone. Important if running on a server in a different timezone.

---

## CareerShift Quota Settings

```python
MAX_CONTACTS_HARD_CAP        = 3  # maximum recruiters to scrape per company
MAX_RECRUITERS_PER_APPLICATION = 3  # maximum recruiters linked per application
```

`MAX_CONTACTS_HARD_CAP` controls how many profiles CareerShift visits per company per run. Adjust based on quota health alerts.

`MAX_RECRUITERS_PER_APPLICATION` caps how many recruiters are linked to a single application via `application_recruiters`. Enforced at DB level — applies universally to scraping, manual imports, sync form, and prospective conversion. Even if more recruiters exist for a company, only the best N (auto confidence first, then oldest) are linked to each new application.

| Situation | Action |
|---|---|
| Quota underutilized for 3 days | Increase MAX_CONTACTS_HARD_CAP (e.g. 3 → 5) |
| Quota exhausted for 3 days | Decrease MAX_CONTACTS_HARD_CAP (e.g. 3 → 2) |
| Applying to many companies | Decrease MAX_CONTACTS_HARD_CAP |
| Applying to few companies | Increase MAX_CONTACTS_HARD_CAP |

**Recommended range:** 2-5. Below 2 gives insufficient coverage. Above 5 rarely improves response rates significantly.

---

## Quota Health Monitor Settings

```python
QUOTA_UNDERUTILIZED_THRESHOLD = 0.40   # alert if using < 40% of daily limit
QUOTA_EXHAUSTED_THRESHOLD     = 0      # alert if remaining = 0
QUOTA_ALERT_CONSECUTIVE_DAYS  = 3      # number of consecutive days to trigger alert
```

**Underutilized threshold (0.40):**
Means usage below 40% of the daily limit triggers an underutilization alert. At 50 quota/day, this means using fewer than 20 credits triggers an alert.

**Exhausted threshold (0):**
Alert triggers when remaining quota hits exactly 0. Change to 5 if you want an early warning before complete exhaustion.

**Consecutive days (3):**
Prevents false alarms from one-off unusual days. 3 days of consistent pattern is a reliable signal.

---

## Gemini AI Rate Limit Settings

```python
DAILY_LIMITS = {
    "gemini-2.5-flash-lite": 20,
    "gemini-2.5-flash":      20,
}

RPM_LIMITS = {
    "gemini-2.5-flash-lite": 10,   # max requests per minute
    "gemini-2.5-flash":       5,   # max requests per minute
}
```

These settings enforce two independent rate limits on every Gemini API call:

1. **Daily limit** — tracked in the `model_usage` database table, persists across process restarts. If today's count for a model reaches its daily limit, that model is skipped for the rest of the day.

2. **RPM limit** — tracked in-memory using a 60-second sliding window. If more than `RPM_LIMITS[model]` calls have been made in the last 60 seconds, the pipeline pauses for 60 seconds and retries rather than switching to a less capable model. This prevents hitting Google's per-minute quotas which became strictly enforced in early 2026.

**Why RPM matters:**
Even if you have daily quota remaining, sending too many requests in a single minute triggers API errors. The RPM enforcement makes the pipeline self-throttling — it slows down naturally when generating content for many companies at once rather than crashing with a quota error.

**Current free tier limits (Google AI Studio as of 2026):**
```
gemini-2.5-flash-lite:  10 RPM,  250K TPM,  20 RPD
gemini-2.5-flash:        5 RPM,  250K TPM,  20 RPD
```
If you upgrade to a paid Gemini plan with higher limits, increase these constants accordingly.

---

## Data Retention Settings

All values in days. Change here and all cleanup functions automatically use the new values.

```python
# Application lifecycle
APPLICATION_AUTO_CLOSE_DAYS = 60  # auto-close active applications after 60 days
                                   # assumes no response within 60 days = closed
                                   # triggers cascade cleanup of application_recruiters

# Outreach table — status-aware cleanup
RETENTION_OUTREACH_SENT     = 30  # keep sent email history for 30 days
RETENTION_OUTREACH_PENDING  = 30  # delete stale pending emails after 30 days
RETENTION_OUTREACH_FAILED   = 30  # delete failed/bounced/cancelled after 30 days

# Cache tables
RETENTION_AI_CACHE          = 21  # must be >= full outreach cycle (3 × SEND_INTERVAL_DAYS)
RETENTION_JOB_CACHE         = 21  # keep job descriptions for 21 days

# Quota and monitoring tables
RETENTION_MODEL_USAGE       = 21  # gemini usage history
RETENTION_CAREERSHIFT_QUOTA = 30  # daily quota records
RETENTION_QUOTA_ALERTS      = 30  # alert history
RETENTION_MONITOR_STATS     = 60  # job monitoring daily stats
RETENTION_VERIFY_FILLED_STATS = 60  # verify-filled daily stats
```

**Important constraint:**
```
RETENTION_AI_CACHE >= SEND_INTERVAL_DAYS × 3
```
If you increase `SEND_INTERVAL_DAYS`, increase `RETENTION_AI_CACHE` proportionally. Otherwise AI cache may expire before the final follow-up email is sent.

Example:
```
SEND_INTERVAL_DAYS = 7  →  RETENTION_AI_CACHE >= 21  (default)
SEND_INTERVAL_DAYS = 10 →  RETENTION_AI_CACHE >= 30
SEND_INTERVAL_DAYS = 14 →  RETENTION_AI_CACHE >= 42
```

**Application auto-close cascade:**
```
APPLICATION_AUTO_CLOSE_DAYS = 60
  → applications.status set to 'closed' after 60 days from applied_date
  → application_recruiters rows deleted for closed applications
  → Both run in same init_db() call, in order
```

---

## Verify Filled Settings

Controls the `--verify-filled` pipeline that detects and removes job postings for positions that have been filled. This pipeline runs automatically at the end of every nightly chain (after `--find-only`) so you don't need to run it manually.

```python
VERIFY_FILLED_BATCH_SIZE   = 200  # max HTTP verifications per nightly run
VERIFY_FILLED_MISSING_DAYS = 3    # days URL must be missing before verification
VERIFY_FILLED_RETENTION    = 7    # days to keep confirmed-filled rows before deleting
```

**How this works in plain terms:**

Every day during `--monitor-jobs`, the pipeline compares the list of jobs returned by each company's ATS API against the list of jobs already in the database. If a job URL that was in the database is no longer being returned by the API, it increments a counter (`consecutive_missing_days`) for that job.

After 3 consecutive days of a URL being absent, `--verify-filled` picks up that job and makes a direct HTTP request to its URL:
- If the URL returns a 404 error → the position is confirmed filled → row marked `filled`, description cleared
- If the URL still loads → it was a temporary API glitch → counter reset to 0, job kept active
- If the request times out or errors → inconclusive → retried the next night

After 7 days as `filled`, the row is deleted entirely to keep the database clean.

**Tuning `VERIFY_FILLED_BATCH_SIZE`:**
At 200 jobs per run with a 1-second delay between requests, the verify step takes ~3-4 minutes. If your `remaining` count in `verify_filled_stats` is consistently high (many jobs waiting to be verified), increase this to 400-500. At 1000+ companies this may need to be 1000+.

**Tuning `VERIFY_FILLED_MISSING_DAYS`:**
3 days is conservative — avoids false positives from temporary API pagination gaps or rate limit responses that cause some jobs to be omitted from a single day's scan. Decrease to 2 for faster cleanup, increase to 5 if you're seeing too many false positives.

---

## Serper API Settings (ATS Detection)

```python
SERPER_API_KEY               = os.getenv("SERPER_API_KEY", "")
SERPER_API_URL               = "https://google.serper.dev/search"
SERPER_TOTAL_LIMIT           = 2500   # free credits on signup
SERPER_LOW_CREDIT_THRESHOLD  = 50     # email alert when below this
```

Used only for Phase 3b of ATS detection — searching for Workday and
Oracle HCM tenants when all other phases fail.

**Setup:**
1. Sign up at [serper.dev](https://serper.dev) (2500 free credits)
2. Copy your API key from the dashboard
3. Add to `.env`:
   ```
   SERPER_API_KEY=your_key_here
   ```

**Credit usage:**
- 2 queries per company (Workday + Oracle)
- Most companies detected in Phase 1-3a (free) — Serper rarely needed
- Email alert sent when fewer than 50 credits remain
- 2500 free credits covers ~1250 companies at 2 queries each

**Companies that skip Serper entirely:**
Amazon, Apple, Google, Meta, Microsoft, Netflix, Uber, Lyft, X/Twitter.
These use fully custom ATS platforms and will never appear on
Workday/Oracle — stored as `custom` immediately.

---

## Google Sheets Integration

```python
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
```

Set in `.env` file. The Sheet ID is found in the Google Sheets URL:
```
https://docs.google.com/spreadsheets/d/SHEET_ID_HERE/edit
```

---

## Full config.py Reference

```python
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# EMAIL CREDENTIALS
# ─────────────────────────────────────────
EMAIL        = os.getenv("EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")
RESUME_PATH  = "Resume.pdf"

# ─────────────────────────────────────────
# OUTREACH SETTINGS
# ─────────────────────────────────────────
SEND_INTERVAL_DAYS = 7

# ─────────────────────────────────────────
# SEND WINDOW SETTINGS
# ─────────────────────────────────────────
SEND_WINDOW_START  = 9
SEND_WINDOW_END    = 11
GRACE_PERIOD_HOURS = 1
SEND_TIMEZONE      = "America/New_York"

# ─────────────────────────────────────────
# CAREERSHIFT QUOTA SETTINGS
# ─────────────────────────────────────────
MAX_CONTACTS_HARD_CAP          = 3  # max recruiters to scrape per company
MAX_RECRUITERS_PER_APPLICATION = 3  # max recruiters linked per application

# ─────────────────────────────────────────
# GEMINI AI RATE LIMIT SETTINGS
# ─────────────────────────────────────────
DAILY_LIMITS = {
    "gemini-2.5-flash-lite": 20,
    "gemini-2.5-flash":      20,
}
RPM_LIMITS = {
    "gemini-2.5-flash-lite": 10,   # requests per minute
    "gemini-2.5-flash":       5,   # requests per minute
}

# ─────────────────────────────────────────
# QUOTA HEALTH MONITOR SETTINGS
# ─────────────────────────────────────────
QUOTA_UNDERUTILIZED_THRESHOLD = 0.40
QUOTA_EXHAUSTED_THRESHOLD     = 0
QUOTA_ALERT_CONSECUTIVE_DAYS  = 3

# ─────────────────────────────────────────
# DATA RETENTION SETTINGS (days)
# ─────────────────────────────────────────
APPLICATION_AUTO_CLOSE_DAYS    = 60  # auto-close active applications
RETENTION_OUTREACH_SENT        = 30  # sent email history
RETENTION_OUTREACH_PENDING     = 30  # stale pending emails
RETENTION_OUTREACH_FAILED      = 30  # failed/bounced/cancelled
RETENTION_AI_CACHE             = 21  # must be >= SEND_INTERVAL_DAYS × 3
RETENTION_JOB_CACHE            = 21  # cached job descriptions
RETENTION_MODEL_USAGE          = 21  # gemini usage history
RETENTION_CAREERSHIFT_QUOTA    = 30  # daily quota records
RETENTION_QUOTA_ALERTS         = 30  # alert history
RETENTION_MONITOR_STATS        = 60  # job monitoring daily stats
RETENTION_VERIFY_FILLED_STATS  = 60  # verify-filled daily stats

# ─────────────────────────────────────────
# VERIFY FILLED SETTINGS
# ─────────────────────────────────────────
VERIFY_FILLED_BATCH_SIZE   = 200  # max HTTP verifications per nightly run
VERIFY_FILLED_MISSING_DAYS = 3    # days URL must be absent before verification
VERIFY_FILLED_RETENTION    = 7    # days to keep confirmed-filled rows before delete

# ─────────────────────────────────────────
# SERPER API (ATS Detection — Phase 3b)
# ─────────────────────────────────────────
SERPER_API_KEY              = os.getenv("SERPER_API_KEY", "")
SERPER_API_URL              = "https://google.serper.dev/search"
SERPER_TOTAL_LIMIT          = 2500
SERPER_LOW_CREDIT_THRESHOLD = 50

# ─────────────────────────────────────────
# KNOWN CUSTOM ATS COMPANIES
# ─────────────────────────────────────────
KNOWN_CUSTOM_ATS = {
    "Amazon", "Apple", "Google", "Meta",
    "Microsoft", "Netflix", "Uber", "Lyft", "X", "Twitter",
}

# ─────────────────────────────────────────
# GOOGLE SHEETS INTEGRATION
# ─────────────────────────────────────────
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
```