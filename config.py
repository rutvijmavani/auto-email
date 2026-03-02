import os
from dotenv import load_dotenv

load_dotenv()

EMAIL = os.getenv("GMAIL_EMAIL")
APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

RESUME_PATH = "Resume.pdf"


# MAX_EMAIL_COUNT = 3
# JOB_TEXT_LIMIT = 4000

SEND_WINDOW_START    = 9     # 9:00 AM
SEND_WINDOW_END      = 11    # 11:00 AM preferred end
GRACE_PERIOD_HOURS   = 1     # hard cutoff at 12:00 PM
SEND_TIMEZONE        = "America/New_York"
SEND_INTERVAL_DAYS   = 7

# ─────────────────────────────────────────
# CAREERSHIFT QUOTA SETTINGS
# ─────────────────────────────────────────
MAX_CONTACTS_HARD_CAP = 3    # maximum recruiters to find per company

# ─────────────────────────────────────────
# QUOTA HEALTH MONITOR SETTINGS
# ─────────────────────────────────────────
QUOTA_UNDERUTILIZED_THRESHOLD = 0.40   # alert if using < 40% of daily limit
QUOTA_EXHAUSTED_THRESHOLD     = 0      # alert if remaining = 0
QUOTA_ALERT_CONSECUTIVE_DAYS  = 3      # consecutive days to trigger alert

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