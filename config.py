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
MAX_RECRUITERS_PER_APPLICATION = 3  # max recruiters linked per application

# ─────────────────────────────────────────
# QUOTA HEALTH MONITOR SETTINGS
# ─────────────────────────────────────────
QUOTA_UNDERUTILIZED_THRESHOLD = 0.40   # alert if using < 40% of daily limit
QUOTA_EXHAUSTED_THRESHOLD     = 0      # alert if remaining = 0
QUOTA_ALERT_CONSECUTIVE_DAYS  = 3      # consecutive days to trigger alert

# ─────────────────────────────────────────
# DATA RETENTION SETTINGS (days)
# ─────────────────────────────────────────
RETENTION_OUTREACH_SENT        = 30
RETENTION_OUTREACH_PENDING     = 30
RETENTION_OUTREACH_FAILED      = 30
RETENTION_AI_CACHE             = 21
RETENTION_JOB_CACHE            = 21
RETENTION_MODEL_USAGE          = 21
RETENTION_CAREERSHIFT_QUOTA    = 30
RETENTION_QUOTA_ALERTS         = 30
RETENTION_MONITOR_STATS        = 60
RETENTION_VERIFY_FILLED_STATS  = 60
APPLICATION_AUTO_CLOSE_DAYS    = 60
RETENTION_COVERAGE_STATS       = 60
RETENTION_API_HEALTH           = 60
RETENTION_PIPELINE_ALERTS      = 30
DIAGNOSTICS_AUTO_RESOLVED_DAYS = 60
RETENTION_CUSTOM_ATS_DIAGNOSTIC= 30

# ─────────────────────────────────────────
# Companies known to use fully custom ATS — skip Serper entirely
# These will never appear on Workday/Oracle/Greenhouse/Lever/Ashby
KNOWN_CUSTOM_ATS = {
    "Amazon",           # jobs.amazon.com
    "Apple",            # jobs.apple.com
    "Google",           # careers.google.com
    "Meta",             # metacareers.com
    "Microsoft",        # careers.microsoft.com
    "Netflix",          # jobs.netflix.com (custom)
    "Uber",             # uber.com/careers (custom)
    "Lyft",             # lyft.com/careers (custom)
    "Twitter",          # careers.twitter.com
    "X",                # same as Twitter
}

# SERPER.DEV SEARCH API (used for Workday + Oracle detection only)
SERPER_API_KEY        = os.getenv("SERPER_API_KEY", "")
SERPER_API_URL        = "https://google.serper.dev/search"
SERPER_TOTAL_LIMIT    = 2500  # total free credits on signup
SERPER_LOW_CREDIT_THRESHOLD = 50  # send email alert when below this
DETECT_ATS_BATCH_SIZE = 10    # companies per --detect-ats --batch run

# GOOGLE CUSTOM SEARCH ENGINE (PSE) — kept for CX reference
GOOGLE_CX             = os.getenv("GOOGLE_CX", "")

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
# PARALLEL JOB MONITORING (Phase 1)
# ─────────────────────────────────────────
 
# Number of companies processed in parallel (thread pool size).
# Real throttling is done by MONITOR_PLATFORM_CONCURRENCY semaphores below —
# MAX_WORKERS just controls how many companies are in-flight at once.
# 20 is fine here because the per-platform semaphores prevent any single
# ATS from being hammered even when all 20 slots are busy.
MONITOR_MAX_WORKERS = 20
 
# Max concurrent requests per ATS platform.
# Platforms sharing one API domain (Greenhouse, SmartRecruiters)
# are throttled more aggressively than per-subdomain platforms (Workday).
MONITOR_PLATFORM_CONCURRENCY = {
    # Workday: each company has its own subdomain BUT parallel fetches
    # still share the OS TCP connection pool.  At concurrency=20 with
    # 36 companies each making multiple paginated requests, the pool
    # saturates → requests_error spikes → companies return 0 jobs →
    # coverage drops.  Observed: concurrency=20 → 32 errors, 14 companies
    # lost from coverage (98→84).  concurrency=5 keeps max simultaneous
    # Workday connections to ~5 and eliminates the errors.
    "workday":          5,
    "greenhouse":       5,   # all hit boards-api.greenhouse.io
    "lever":            5,   # all hit api.lever.co
    "smartrecruiters":  5,   # all hit api.smartrecruiters.com
    "ashby":            5,   # all hit api.ashbyhq.com
    "oracle_hcm":       5,   # per-tenant subdomain
    "icims":            5,   # per-tenant subdomain
    "talentbrew":       3,
    "phenom":           3,
    "jobvite":          3,
    "successfactors":   2,   # slow avg (7161ms) — keep low
    "avature":          2,
    "custom":           5,   # varies per company
    # default for unlisted: 5
}
MONITOR_PLATFORM_CONCURRENCY_DEFAULT = 5
 

# ─────────────────────────────────────────
# PIPELINE PERFORMANCE THRESHOLDS
# ─────────────────────────────────────────
METRIC1_ALERT_THRESHOLD       = 50   # find-only performance % (Red)
METRIC2_ALERT_THRESHOLD       = 60   # outreach coverage % (Red)
METRIC_ALERT_CONSECUTIVE_DAYS = 3    # days before alert fires
API_FAILURE_RATE_THRESHOLD     = 0.10 # api_failures / requests_made
API_FAILURE_CONSECUTIVE_DAYS   = 3

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

# Priority 4 — USA location filtering
# Handled dynamically in job_filter.py via:
#   - SimpleMaps US city dataset   (data/uscities.csv)
#   - geonamescache state codes/names
#   - pycountry country name/ISO detection

# Freshness
JOB_MONITOR_DAYS_FRESH        = 3    # days to consider a job fresh
JOB_MONITOR_REDETECT_DAYS     = 14   # re-detect ATS after X consecutive empty days
JOB_MONITOR_PDF_RETENTION     = 30   # days to keep PDF digest files
JOB_MONITOR_MAX_JOBS          = 0    # 0 = no cap (show ALL matching jobs)
JOB_MONITOR_API_TIMEOUT       = 10   # seconds per API request


# Job filled variables
VERIFY_FILLED_BATCH_SIZE   = 200  # max verifications per run
VERIFY_FILLED_MISSING_DAYS = 3    # days missing before verification
VERIFY_FILLED_RETENTION    = 7    # days to keep 'filled' before delete

# ATS detection
ATS_PLATFORMS = ["greenhouse", "lever", "ashby",
                 "smartrecruiters", "workday"]

# ATS confidence scoring
ATS_DETECTION_THRESHOLD = 50    # min final_score to accept any detection
ATS_MIN_CONFIDENCE      = 80    # min confidence% standalone (0-100)
ATS_CLOSE_CALL_GAP      = 10    # % gap between top two — below = close call
ATS_SAMPLE_SIZE         = 20    # max jobs to sample when scoring response

# Tie-break order by date field reliability (best → worst)
ATS_DATE_RELIABILITY = [
    "ashby",           # publishedAt  — exact original date ✓✓✓
    "lever",           # createdAt    — Unix ms, never changes ✓✓✓
    "oracle_hcm",      # PostedDate   — original date ✓✓✓
    "workday",         # postedOn     — original date ✓✓
    "smartrecruiters", # releasedDate — original date ✓✓
    "greenhouse",      # updated_at   — changes on edit ✗ (last resort)
]

# ATS platforms that require Google detection (can't be slug-guessed)
ATS_GOOGLE_ONLY = ["oracle_hcm", "successfactors"]  # icims now supported

# ATS detection status values
ATS_STATUS_DETECTED       = "detected"     # found via Google, supported ✓
ATS_STATUS_UNSUPPORTED    = "unsupported"  # found via Google, not yet supported
ATS_STATUS_CLOSE_CALL     = "close_call"   # API buffer close call (legacy)
ATS_STATUS_UNKNOWN        = "unknown"      # not found anywhere
ATS_STATUS_CUSTOM         = "custom"       # uses fully custom ATS (no standard URL)
ATS_STATUS_MANUAL         = "manual"       # manually overridden, never re-detected

# Stop words excluded from company keyword extraction
ATS_KEYWORD_STOP_WORDS = {
    "inc", "corp", "llc", "ltd", "co", "the", "and",
    "jobs", "careers", "group", "holding", "holdings",
    "technologies", "technology", "tech", "systems",
    "solutions", "services", "america", "usa", "us",
    "global", "international", "national", "interactive",
}

# Alert thresholds
MONITOR_COVERAGE_ALERT        = 0.70  # alert if < 70% companies returned jobs
MONITOR_ATS_UNKNOWN_ALERT     = 0.20  # alert if > 20% companies unknown ATS
MONITOR_RELIABILITY_ALERT     = 0.90  # alert if < 90% runs succeed (7 days)
MONITOR_MATCH_RATE_LOW_ALERT  = 0.05  # alert if < 5% fetched jobs match filters
MONITOR_MATCH_RATE_HIGH_ALERT = 0.60  # alert if > 60% fetched jobs match filters

# ─────────────────────────────────────────
# RATE LIMITING & API HEALTH
# ─────────────────────────────────────────

# Alert thresholds
RATE_LIMIT_CRITICAL_THRESHOLD = 10   # % 429s in one run → immediate email
RATE_LIMIT_WARNING_THRESHOLD  = 2    # % 429s → warning in daily digest
SLOW_RESPONSE_THRESHOLD_MS    = 3000 # avg ms → warning in daily digest
SERPER_CRITICAL_THRESHOLD     = 0    # credits remaining → immediate email
SERPER_WARNING_THRESHOLD      = 50   # credits remaining → warning in digest

# Per-platform delays (seconds) — minimal, evidence-based
# Increase only if 429s appear in api_health table
PLATFORM_DELAYS = {
    "greenhouse":      {"base": 0.2, "jitter": 0.1},
    "lever":           {"base": 0.3, "jitter": 0.1},
    "ashby":           {"base": 0.2, "jitter": 0.1},
    "smartrecruiters": {"base": 0.3, "jitter": 0.1},
    "workday":         {"base": 1.0, "jitter": 0.3},
    "oracle_hcm":      {"base": 0.5, "jitter": 0.2},
    "icims":           {"base": 0.5, "jitter": 0.2},
}

# Delay between companies during --monitor-jobs
MONITOR_BETWEEN_COMPANIES = {"base": 0.5, "jitter": 0.2}

# Enrichment daily limits per platform
# Increase only after 30 days of clean api_health data
ENRICH_DAILY_LIMITS = {
    "greenhouse":      300,
    "ashby":           300,
    "lever":           150,
    "icims":           100,
    "workday":          30,
    "oracle_hcm":       30,
    "smartrecruiters":  50,
}
ENRICH_WINDOW_HOURS = 18   # spread requests over this many hours

# Alert deduplication window
ALERT_DEDUP_HOURS = 24     # don't re-send same alert within N hours