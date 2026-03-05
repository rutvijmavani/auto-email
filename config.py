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

# ─────────────────────────────────────────
# RECRUITER VALIDATION SETTINGS
# ─────────────────────────────────────────
CAREERSHIFT_SAMPLE_SIZE       = 10   # cards per batch
CAREERSHIFT_HIGH_CONFIDENCE   = 90   # skip remaining HR terms
CAREERSHIFT_MEDIUM_CONFIDENCE = 70   # continue to next HR term
CAREERSHIFT_MAX_PROFILES      = 3    # hard cap — never visit more than this
MIN_BUFFER_SIZE               = 2    # minimum for domain consistency check
MIN_RECRUITERS_PER_COMPANY    = 1    # minimum active recruiters to start outreach
                                      # pipeline tops up to MAX_CONTACTS_HARD_CAP over time
GEMINI_VERIFY_RETRY_DAYS      = 5    # days to retry Gemini verification

# ─────────────────────────────────────────
# PIPELINE PERFORMANCE THRESHOLDS
# ─────────────────────────────────────────
METRIC1_ALERT_THRESHOLD       = 50   # find-only performance % (Red)
METRIC2_ALERT_THRESHOLD       = 60   # outreach coverage % (Red)
METRIC_ALERT_CONSECUTIVE_DAYS = 3    # days before alert fires

# ─────────────────────────────────────────
# JOB MONITORING SETTINGS
# ─────────────────────────────────────────

# Priority 1 — Job title keywords (broad match)
TARGET_JOB_TITLES = [
    "software engineer",
    "software developer",
    "software development engineer",
    "backend engineer",
    "frontend engineer",
    "full stack engineer",
    "full stack developer",
    "web developer",
    "platform engineer",
    "application engineer",
    "member of technical staff",
    "swe",
]

# Priority 2 — Seniority (soft score only — no hard reject)
TARGET_SENIORITY = [
    "senior", "staff", "principal", "lead",
]

# Priority 3 — Skills (soft score — description match)
TARGET_SKILLS = [
    "python", "javascript", "typescript",
    "react", "node.js", "aws", "go",
    "java", "kubernetes", "docker",
]

# Priority 4 — USA location keywords
USA_LOCATION_KEYWORDS = [
    "united states", "usa", "u.s.", "remote",
    "new york", "san francisco", "seattle",
    "austin", "boston", "chicago", "denver",
    "los angeles", "atlanta", "miami", "dallas",
    "new jersey", "washington", "virginia",
    "texas", "california",
]
EXCLUDE_LOCATIONS = [
    "canada", "toronto", "uk", "london",
    "india", "bangalore", "germany", "berlin",
    "australia", "singapore", "ireland", "dublin",
    "poland", "netherlands", "france", "paris",
    "mexico", "brazil", "japan", "china",
]

# Freshness
JOB_MONITOR_DAYS_FRESH        = 3    # days to consider a job fresh
JOB_MONITOR_REDETECT_DAYS     = 14   # re-detect ATS after X consecutive empty days
JOB_MONITOR_PDF_RETENTION     = 30   # days to keep PDF digest files
JOB_MONITOR_MAX_JOBS          = 0    # 0 = no cap (show ALL matching jobs)
JOB_MONITOR_API_TIMEOUT       = 10   # seconds per API request

# ATS detection
ATS_PLATFORMS = ["greenhouse", "lever", "ashby",
                 "smartrecruiters", "workday"]

# Alert thresholds
MONITOR_COVERAGE_ALERT        = 0.70  # alert if < 70% companies returned jobs
MONITOR_ATS_UNKNOWN_ALERT     = 0.20  # alert if > 20% companies unknown ATS
MONITOR_RELIABILITY_ALERT     = 0.90  # alert if < 90% runs succeed (7 days)
MONITOR_MATCH_RATE_LOW_ALERT  = 0.05  # alert if < 5% fetched jobs match filters
MONITOR_MATCH_RATE_HIGH_ALERT = 0.60  # alert if > 60% fetched jobs match filters