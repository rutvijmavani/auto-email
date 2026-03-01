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
MAX_CONTACTS_HARD_CAP = 3  # maximum recruiters to find per company
```

This is the primary lever for quota utilization. Adjust based on quota health alerts:

| Situation | Action |
|---|---|
| Quota underutilized for 3 days | Increase (e.g. 3 → 5) |
| Quota exhausted for 3 days | Decrease (e.g. 3 → 2) |
| Applying to many companies | Decrease |
| Applying to few companies | Increase |

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

## Data Retention Settings

All values in days. Change here and all cleanup functions automatically use the new values.

```python
# Outreach table — status-aware cleanup
RETENTION_OUTREACH_SENT     = 90  # keep sent email history for 90 days
RETENTION_OUTREACH_PENDING  = 30  # delete stale pending emails after 30 days
RETENTION_OUTREACH_FAILED   = 30  # delete failed/bounced/cancelled after 30 days

# Cache tables
RETENTION_AI_CACHE          = 21  # must be >= full outreach cycle (3 × SEND_INTERVAL_DAYS)
RETENTION_JOB_CACHE         = 21  # keep job descriptions for 21 days

# Quota and monitoring tables
RETENTION_MODEL_USAGE       = 21  # gemini usage history
RETENTION_CAREERSHIFT_QUOTA = 30  # daily quota records
RETENTION_QUOTA_ALERTS      = 30  # alert history
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
MAX_CONTACTS_HARD_CAP = 3

# ─────────────────────────────────────────
# QUOTA HEALTH MONITOR SETTINGS
# ─────────────────────────────────────────
QUOTA_UNDERUTILIZED_THRESHOLD = 0.40
QUOTA_EXHAUSTED_THRESHOLD     = 0
QUOTA_ALERT_CONSECUTIVE_DAYS  = 3

# ─────────────────────────────────────────
# DATA RETENTION SETTINGS (days)
# ─────────────────────────────────────────
RETENTION_OUTREACH_SENT        = 90
RETENTION_OUTREACH_PENDING     = 30
RETENTION_OUTREACH_FAILED      = 30
RETENTION_AI_CACHE             = 21
RETENTION_JOB_CACHE            = 21
RETENTION_MODEL_USAGE          = 21
RETENTION_CAREERSHIFT_QUOTA    = 30
RETENTION_QUOTA_ALERTS         = 30

# ─────────────────────────────────────────
# GOOGLE SHEETS INTEGRATION
# ─────────────────────────────────────────
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
```