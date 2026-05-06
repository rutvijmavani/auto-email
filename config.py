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

# ─────────────────────────────────────────
# REDIS / ADAPTIVE POLLING
# ─────────────────────────────────────────
REDIS_URL         = os.getenv("REDIS_URL", "redis://localhost:6379/0")
WORKER_BLOCK_SECS = 5      # BLPOP timeout — keeps workers responsive to Ctrl+C

# ── Redis key names (Section 15) ──────────────────────────────────────────────
REDIS_POLL_ADAPTIVE    = "poll:adaptive"          # ZSET — next adaptive poll time per company
REDIS_POLL_FULLSCAN    = "poll:fullscan"           # ZSET — next full scan time per company
REDIS_DETAIL_ADAPTIVE  = "queue:detail:adaptive"  # LIST — high priority detail fetches
REDIS_DETAIL_FULLSCAN  = "queue:detail:fullscan"  # LIST — low priority detail fetches
REDIS_CYCLE_START      = "cycle:start"             # STRING — Unix ts of today's cycle start
REDIS_PAUSE_CHANNEL    = "pipeline:pause"          # PubSub — nightly maintenance pause
REDIS_RESUME_CHANNEL   = "pipeline:resume"         # PubSub — nightly maintenance resume
REDIS_CRONCHAIN_ALIVE  = "cronchain:alive"         # STRING TTL=300 — cron chain heartbeat
REDIS_DB_MAINTENANCE   = "db:maintenance"          # STRING no-TTL — maintenance flag

# Scan queue (scan_worker skeleton → replaced by scheduler in Phase 4)
SCAN_QUEUE        = "scan:queue"
RESULT_CHANNEL    = "scan:results"

# ── Adaptive interval engine (Section 6) ──────────────────────────────────────
# Recency-biased weights for the 5-poll rolling window (oldest → newest)
ADAPTIVE_WEIGHTS       = [0.10, 0.15, 0.20, 0.25, 0.30]
ADAPTIVE_MIN_POLLS     = 3      # minimum polls before score is trusted
ADAPTIVE_CAP_PER_POLL  = 10     # max new_jobs contribution per poll (burst cap)
ADAPTIVE_SMOOTHING     = 0.3    # EMA factor for dormancy decay (going quiet)
                                # reactivation (going active) uses no smoothing

# Band → interval mapping (seconds).
# ADAPTIVE_BANDS is kept for backward-compat imports; the live thresholds are
# computed daily by recalibrate_band_thresholds() and stored in Redis under
# REDIS_BAND_THRESHOLDS.  DEFAULT_THRESHOLDS (in adaptive.py) mirrors these
# values and is used as the cold-start fallback.
ADAPTIVE_BANDS = [
    # (score_threshold, interval_seconds)
    (1.5,  9 * 3600),   # < 1.5  →  9h low activity
    (3.5,  6 * 3600),   # < 3.5  →  6h moderate
    (6.0,  4 * 3600),   # < 6.0  →  4h active
]
ADAPTIVE_MIN_INTERVAL     = 3 * 3600    # 3h — very active floor
ADAPTIVE_DEFAULT_INTERVAL = 12 * 3600  # 12h — before 3 polls of history

# Score-tiered MAX_INTERVAL caps (Section 8)
ADAPTIVE_MAX_INTERVAL_ACTIVE   = 6 * 3600   # moderate+ companies → 6h cap
ADAPTIVE_MAX_INTERVAL_DORMANT  = 12 * 3600  # dormant/low companies → 12h cap

# ── Rank-based band calibration (Section 6 — dynamic thresholds) ─────────────
# Instead of comparing a company's score against hardcoded absolute values,
# we rank active companies against each other.  Daily,
# recalibrate_band_thresholds() queries all active scores, Winsorizes the top
# 5% to neutralise outliers, then computes the score at each rank boundary and
# writes those three values to Redis so every worker uses the same live
# thresholds.
#
# Target distribution of *active* companies (score > 0) across bands:
#   Top    10%  →  3h  (exceptional, consistent new postings)
#   Next   15%  →  4h  (clearly above-average hiring activity)
#   Next   25%  →  6h  (moderate — worth checking twice a day)
#   Bottom 50%  →  9h  (quiet relative to peers, baseline service)
#
# Companies with score=None (insufficient history) or score=0 (no new jobs
# in rolling window) always get ADAPTIVE_DEFAULT_INTERVAL (12h) and are
# excluded from ranking entirely.
ADAPTIVE_BAND_TOP_PCT            = 0.10   # top 10%  → 3h / 4h boundary
ADAPTIVE_BAND_ACTIVE_PCT         = 0.15   # next 15% → 4h / 6h boundary
ADAPTIVE_BAND_MODERATE_PCT       = 0.25   # next 25% → 6h / 9h boundary
# remaining 50% → 9h (no constant needed — it is the catch-all)
ADAPTIVE_WINSORIZE_PCT           = 0.05   # cap top 5% before rank computation
ADAPTIVE_MIN_COMPANIES_CALIBRATE = 5      # need >= 5 active cos to calibrate
ADAPTIVE_CALIBRATION_LOOKBACK_DAYS = 30   # ignore scores older than N days

# Redis key where live band thresholds are stored (hash: low/moderate/active)
REDIS_BAND_THRESHOLDS = "adaptive:band_thresholds"

# ── Scheduler (Section 5) ─────────────────────────────────────────────────────
SCHEDULER_DAWN_PATROL_WINDOW   = 4 * 3600   # redistribute polls due after +4h
SCHEDULER_DAWN_PATROL_SPREAD   = 2 * 3600   # spread them across 2h window
SCHEDULER_FULL_SCAN_BUFFER_S   = 300        # 5-min buffer after adaptive → full scan
SCHEDULER_FULL_SCAN_INTERVAL_S = 86400      # default full scan every 24h
SCHEDULER_HEARTBEAT_TTL        = 300        # worker heartbeat TTL (seconds)
SCHEDULER_FULL_SCAN_LOCK_TTL   = 3600       # full scan exclusive lock TTL
SCHEDULER_TICK_SECS            = 1.0        # scheduler loop tick interval

# ── Detail queue backpressure (Section 15) ────────────────────────────────────
DETAIL_QUEUE_MAX_ADAPTIVE      = 5000       # pause listing scan if adaptive queue > this
DETAIL_QUEUE_MAX_FULLSCAN      = 2000       # pause full scan if fullscan queue > this

# ── Smart early exit (Section 11) ─────────────────────────────────────────────
PAGINATOR_OVERLAP_THRESHOLD    = 0.80       # 80% of page already seen → overlap
PAGINATOR_CONFIRM_PAGES        = 2          # consecutive overlap pages to stop
PAGINATOR_UNSORTED_CUTOFF_DAYS = 3          # time-based cutoff for non-sorted ATS

# ── Dynamic concurrency (Section 19) ──────────────────────────────────────────
CONCURRENCY_ERROR_RATE_REDUCE   = 0.10      # > 10% errors → reduce concurrency
CONCURRENCY_ERROR_RATE_INCREASE = 0.02      # < 2% errors → increase concurrency
CONCURRENCY_WINDOW_MINUTES      = 10        # sliding window bucket size (minutes)
CONCURRENCY_WINDOW_TTL          = 1200      # errwin key TTL (2 × 600s buckets)
CONCURRENCY_BACKOFF_BASE        = 0.5       # initial back-off before retry (seconds)
CONCURRENCY_BACKOFF_MAX         = 5.0       # max back-off jitter ceiling (seconds)
CONCURRENCY_MAX_RETRIES         = 4         # max semaphore-acquire retries before giving up
CONCURRENCY_WORKDAY_DEFAULT     = 3         # conservative start for each Workday DC
REDIS_ERRWIN_PREFIX             = "errwin"          # sliding window key prefix
REDIS_CONCURRENCY_ACTIVE_PREFIX = "concurrency:active"   # in-flight counter prefix
REDIS_CONCURRENCY_LIMIT_PREFIX  = "concurrency:limit"    # current allowed max prefix

# Per-platform concurrency floor and ceiling for the feedback loop.
# Floor = never go below this (prevents starvation).
# Ceil  = never go above this (prevents hammering even when error-free).
CONCURRENCY_FLOOR = {
    "workday":         1,
    "greenhouse":      1,
    "lever":           1,
    "smartrecruiters": 1,
    "ashby":           1,
    "oracle_hcm":      1,
    "icims":           1,
    "talentbrew":      1,
    "phenom":          1,
    "jobvite":         1,
    "successfactors":  1,
    "avature":         1,
    "custom":          1,
}
CONCURRENCY_FLOOR_DEFAULT = 1

CONCURRENCY_CEIL = {
    "workday":         6,
    "greenhouse":      8,
    "lever":           8,
    "smartrecruiters": 8,
    "ashby":           8,
    "oracle_hcm":      6,
    "icims":           6,
    "talentbrew":      4,
    "phenom":          4,
    "jobvite":         4,
    "successfactors":  3,
    "avature":         3,
    "custom":          6,
}
CONCURRENCY_CEIL_DEFAULT = 6

# ── Baseline error-rate cache (Section 20 — Fix 2) ────────────────────────────
# 30-day historical error rate per platform cached in Redis for real-time use.
REDIS_BASELINE_PREFIX             = "baseline:error_rate"  # key: {prefix}:{platform}
BASELINE_CACHE_TTL                = 3600    # 1h — refresh from api_health on miss
CONCURRENCY_BASELINE_MIN_DAYS     = 7       # min days of api_health history before
                                            # spike_factor is used; below this, only
                                            # raw error_rate threshold applies
CONCURRENCY_SPIKE_FACTOR_THRESHOLD = 5.0   # spike_factor > this → concurrency-induced
                                            # → aggressive reduction (drop limit by 2)
                                            # ≤ this → normal variance → cautious (by 1)

# ── Dynamic worker pools (Section 9) ──────────────────────────────────────────
# Scheduler manages scan + detail worker processes via multiprocessing.Process.
# Both pool sizes are calculated at 7 AM from historical api_health averages.
# MONITOR_MAX_WORKERS is the cold-start fallback ceiling only (day 1, no history).
WORKER_SHUTDOWN_TIMEOUT_S         = 30     # seconds before forced SIGKILL on shutdown
WORKER_FAST_CHECK_INTERVAL_S      = 300    # 5 min — error-triggered worker reduction
WORKER_SLOW_CHECK_INTERVAL_S      = 1800   # 30 min — throughput-driven scaling
WORKER_POOL_SCAN_FRACTION         = 0.6    # 60% of combined DB pool for scan workers
WORKER_POOL_DETAIL_FRACTION       = 0.4    # 40% of combined DB pool for detail workers
WORKER_FLOOR                      = 2      # minimum workers per pool (redundancy)
WORKER_DEPRIORITISE_SECS          = 300    # seconds to push erroring platform's
                                           # companies forward in poll:adaptive
DETAIL_QUEUE_HIGH_WATERMARK       = 1000   # cascade trigger — detail queue above this
                                           # → stop adding scan workers, drain first
                                           # (hard emergency brake is DETAIL_QUEUE_MAX_ADAPTIVE=5000)

# ── Phase 11 — Monitoring and alerting thresholds ────────────────────────────

# Error streak: fire a WARNING alert when a company's adaptive scan has failed
# this many times in a row without a single success.  Individual company, not
# platform-wide — severity stays WARNING so it doesn't page at 3 AM.
WORKER_ERROR_STREAK_THRESHOLD     = 5

# Reactivation lag alert: fire when a company recovers (first success after
# N consecutive errors) but was dark for longer than this many hours.
# A long lag means the backoff / outage recovery was slow and we may have
# missed jobs during the blackout window.  4h is a good default — it covers
# one missed full-poll cycle without being too noisy for brief 1-2h gaps.
REACTIVATION_LAG_ALERT_HR         = 4.0

# Detail queue depth alert: fire when queue:detail:adaptive stays above
# DETAIL_QUEUE_HIGH_WATERMARK for this many consecutive slow-check cycles
# (each cycle = WORKER_SLOW_CHECK_INTERVAL_S = 30 min).
# Default 3 cycles = 90 min of sustained high depth before alerting.
DETAIL_QUEUE_ALERT_CYCLES         = 3

# Redis memory alert: fire a CRITICAL alert when Redis used_memory exceeds
# this percentage of maxmemory.  Hitting the limit under noeviction policy
# causes write failures — the poll queues and seen-sets would stop accepting
# new entries, silently dropping jobs.  0 = Redis has no maxmemory limit set.
REDIS_MEMORY_ALERT_PCT            = 80

# DB connection pool size (must match maxconn in db/connection.py).
# Used by scheduler.py to compute the combined worker ceiling:
#   scan_ceil + detail_ceil ≤ DB_POOL_MAXCONN - 3 (3 reserved for scheduler)
DB_POOL_MAXCONN                   = 25

# ── Phase 10 — Adaptive protection + resilience hardening (Section 18) ────────
#
# Exponential backoff per-company per-operation (scan / detail / fullscan).
# Formula: min(BASE * 2**retry_count, CAP).  After CAP is exceeded (retry ≥ 5)
# the company is pushed 24h forward — effectively skipped for the rest of today.
# All backoff counters expire automatically at 86400s (next cycle, no reset needed).
WORKER_BACKOFF_BASE_S              = 300     # 5 min  — delay after 1st failure
WORKER_BACKOFF_CAP_S               = 3600    # 1h     — maximum per-retry cap
WORKER_BACKOFF_GIVEUP_S            = 86400   # 24h    — give-up after cap exceeded
REDIS_BACKOFF_PREFIX               = "retry:backoff"  # key: {prefix}:{op}:{company}

# ATS outage detection.
# When WORKER_CONSEC_REDUCTIONS_THRESHOLD consecutive worker reductions fail to
# improve a platform's error rate, the platform enters outage mode: all dispatches
# for that platform are paused for WORKER_OUTAGE_TTL_S seconds.
# At WORKER_CANARY_INTERVAL_S into the outage, one canary dispatch is attempted
# for early recovery detection.
WORKER_OUTAGE_TTL_S                = 3600    # 60-min dispatch pause on outage
WORKER_CANARY_INTERVAL_S           = 1800    # 30 min into outage → try canary dispatch
WORKER_CONSEC_REDUCTIONS_THRESHOLD = 3       # ineffective reductions before outage
WORKER_CONSEC_REDUCTIONS_TTL       = 3600    # TTL for consec_reductions counter (1h)

# Scaling lock: fast_error_check_loop sets this after any error-triggered action.
# slow_throughput_check_loop skips scale-up for any platform with an active lock,
# preventing it from immediately undoing a deliberate error-driven reduction.
WORKER_SCALING_LOCK_TTL            = 1800    # 30 min = WORKER_SLOW_CHECK_INTERVAL_S

# Per-DC in-flight scan tracking (drift-proof ZSET).
# Score = dispatch timestamp.  Entries older than INFLIGHT_STALE_WINDOW_S are
# removed before ZCARD to prevent drift from crashed workers.
REDIS_INFLIGHT_PREFIX              = "inflight:scans"  # key: {prefix}:{dc_key}
INFLIGHT_STALE_WINDOW_S            = 600     # 10 min = 2× max scan timeout

# ── Full scan (Section 9) ─────────────────────────────────────────────────────
FULLSCAN_BLOOM_TTL             = 36 * 3600  # 36h bloom filter TTL
FULLSCAN_BLOOM_ERROR_RATE      = 0.001      # 0.1% false positive rate