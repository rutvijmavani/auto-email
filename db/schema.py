# db/schema.py — Database schema creation and cleanup (PostgreSQL)
#
# All DDL uses PostgreSQL syntax:
#   BIGSERIAL PRIMARY KEY   instead of INTEGER PRIMARY KEY AUTOINCREMENT
#   BYTEA                   instead of BLOB
#   CITEXT                  instead of TEXT … COLLATE NOCASE (citext extension)
#   CURRENT_DATE            instead of DATE('now')
#   ADD COLUMN IF NOT EXISTS instead of try/except OperationalError
#   is_valid_json()         custom PL/pgSQL function replaces json_valid()
#   json_extract_text()     custom PL/pgSQL function replaces json_extract()
#
# Phase 1 tables added here (previously planned for separate migration):
#   seen_job_ids        — incremental dedup: one row per (company, job_id)
#   company_poll_stats  — per-company adaptive polling state (Phase 5)
#   adaptive_poll_metrics — daily observability metrics (Phase 8)

import time
from datetime import datetime, timedelta

from db.connection import get_conn
from config import (
    RETENTION_OUTREACH_SENT,
    RETENTION_OUTREACH_PENDING,
    RETENTION_OUTREACH_FAILED,
    RETENTION_JOB_CACHE,
    RETENTION_MODEL_USAGE,
    RETENTION_CAREERSHIFT_QUOTA,
    RETENTION_QUOTA_ALERTS,
    RETENTION_MONITOR_STATS,
    APPLICATION_AUTO_CLOSE_DAYS,
    VERIFY_FILLED_RETENTION,
    RETENTION_VERIFY_FILLED_STATS,
    RETENTION_COVERAGE_STATS,
    RETENTION_API_HEALTH,
    RETENTION_PIPELINE_ALERTS,
    RETENTION_CUSTOM_ATS_DIAGNOSTIC,
    DIAGNOSTICS_AUTO_RESOLVED_DAYS,
)


# ─────────────────────────────────────────
# CLEANUP HELPERS
# ─────────────────────────────────────────

def _cleanup_monitor_stats(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_MONITOR_STATS)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM monitor_stats WHERE date < %s", (cutoff,))


def _cleanup_auto_close_applications(c):
    """Auto-close active applications older than APPLICATION_AUTO_CLOSE_DAYS."""
    cutoff = (datetime.now() - timedelta(days=APPLICATION_AUTO_CLOSE_DAYS)).strftime("%Y-%m-%d")
    c.execute("""
        UPDATE applications
        SET status = 'closed'
        WHERE status = 'active'
        AND applied_date < %s
    """, (cutoff,))


def _cleanup_closed_application_recruiters(c):
    """
    Delete application_recruiters rows for closed applications.
    Runs after _cleanup_auto_close_applications to catch newly closed ones.
    """
    c.execute("""
        DELETE FROM application_recruiters
        WHERE application_id IN (
            SELECT id FROM applications
            WHERE status = 'closed'
        )
    """)


def _cleanup_mark_resolved_diagnostics(c):
    """Mark diagnostics resolved older than DIAGNOSTICS_AUTO_RESOLVED_DAYS."""
    cutoff = (datetime.now() - timedelta(days=DIAGNOSTICS_AUTO_RESOLVED_DAYS)).strftime("%Y-%m-%d")
    c.execute("""
        UPDATE custom_ats_diagnostics
        SET resolved = 1,
            resolved_at = NOW()
        WHERE resolved = 0
        AND created_at < %s
    """, (cutoff,))


def _cleanup_resolved_diagnostics(c):
    """
    Delete resolved diagnostics older than RETENTION_CUSTOM_ATS_DIAGNOSTIC days.
    Retention is measured from resolved_at (when it was resolved), not created_at.
    """
    cutoff = (datetime.now() - timedelta(days=RETENTION_CUSTOM_ATS_DIAGNOSTIC)).isoformat()
    c.execute("""
        DELETE FROM custom_ats_diagnostics
        WHERE resolved = 1
          AND COALESCE(resolved_at, created_at) < %s
    """, (cutoff,))


def _cleanup_expired_ai_cache(c):
    c.execute("DELETE FROM ai_cache WHERE expires_at <= NOW()")


def _cleanup_expired_jobs(c):
    cutoff = int(time.time()) - RETENTION_JOB_CACHE * 86400
    c.execute("DELETE FROM jobs WHERE created_at <= %s", (cutoff,))


def _cleanup_old_model_usage(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_MODEL_USAGE)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM model_usage WHERE date < %s", (cutoff,))


def _cleanup_job_postings(c):
    """Archive expired job postings and remove old dismissed ones."""
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    # Archive new postings older than 7 days (clear description to save space)
    c.execute("""
        UPDATE job_postings
        SET status = 'expired', description = NULL
        WHERE status = 'new'
        AND first_seen < %s
    """, (week_ago,))
    # Archive digested postings older than 7 days
    c.execute("""
        UPDATE job_postings
        SET status = 'expired', description = NULL
        WHERE status = 'digested'
        AND first_seen < %s
    """, (week_ago,))
    # Delete dismissed postings older than 30 days
    month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    c.execute("""
        DELETE FROM job_postings
        WHERE status = 'dismissed'
        AND first_seen < %s
    """, (month_ago,))
    # Delete applied postings (already in applications table)
    c.execute("DELETE FROM job_postings WHERE status = 'applied'")
    # Delete filled postings older than retention period
    filled_cutoff = (datetime.now() - timedelta(days=VERIFY_FILLED_RETENTION)).strftime("%Y-%m-%d")
    c.execute("""
        DELETE FROM job_postings
        WHERE status = 'filled'
        AND stale_since < %s
    """, (filled_cutoff,))


def _cleanup_verify_filled_stats(c):
    """Delete verify_filled_stats older than retention period."""
    cutoff = (datetime.now() - timedelta(days=RETENTION_VERIFY_FILLED_STATS)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM verify_filled_stats WHERE date < %s", (cutoff,))


def _cleanup_outreach(c):
    """Tiered outreach cleanup based on status."""
    cutoff_sent    = (datetime.now() - timedelta(days=RETENTION_OUTREACH_SENT)).strftime("%Y-%m-%d")
    cutoff_pending = (datetime.now() - timedelta(days=RETENTION_OUTREACH_PENDING)).strftime("%Y-%m-%d")
    cutoff_failed  = (datetime.now() - timedelta(days=RETENTION_OUTREACH_FAILED)).strftime("%Y-%m-%d")

    c.execute("DELETE FROM outreach WHERE status = 'sent' AND sent_at < %s",
              (cutoff_sent,))
    c.execute("DELETE FROM outreach WHERE status = 'pending' AND scheduled_for < %s",
              (cutoff_pending,))
    c.execute("""
        DELETE FROM outreach WHERE status IN ('failed', 'bounced', 'cancelled')
        AND created_at < %s
    """, (cutoff_failed,))


def _cleanup_careershift_quota(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_CAREERSHIFT_QUOTA)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM careershift_quota WHERE date < %s", (cutoff,))


def _cleanup_quota_alerts(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_QUOTA_ALERTS)).strftime("%Y-%m-%d")
    c.execute("""
        DELETE FROM quota_alerts
        WHERE created_at < %s
    """, (cutoff,))


def _cleanup_coverage_stats(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_COVERAGE_STATS)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM coverage_stats WHERE date < %s", (cutoff,))


def _cleanup_api_health(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_API_HEALTH)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM api_health WHERE date < %s", (cutoff,))


def _cleanup_worker_scaling_events(c):
    """Delete worker_scaling_events older than 90 days (Phase 10)."""
    cutoff = (datetime.now() - timedelta(days=90)).isoformat()
    c.execute(
        "DELETE FROM worker_scaling_events WHERE occurred_at < %s", (cutoff,)
    )


def _cleanup_pipeline_alerts(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_PIPELINE_ALERTS)).strftime("%Y-%m-%d")
    c.execute("""
        DELETE FROM pipeline_alerts
        WHERE notified = 1
        AND notified_at < %s
    """, (cutoff,))


def _cleanup_custom_ats_inspection(c):
    """
    Remove inspection rows for companies no longer in prospective_companies.
    CITEXT company column makes the comparison case-insensitive automatically.
    """
    c.execute("""
        DELETE FROM custom_ats_inspection
        WHERE company NOT IN (
            SELECT company FROM prospective_companies
        )
    """)


def _cleanup_seen_job_ids(c):
    """
    Remove seen_job_ids entries for companies no longer in prospective_companies.
    Keeps the table lean as companies are removed.
    Also prune entries not polled in 90 days (dormant cleanup).
    """
    c.execute("""
        DELETE FROM seen_job_ids
        WHERE company NOT IN (
            SELECT company FROM prospective_companies
        )
    """)
    cutoff = (datetime.now() - timedelta(days=90)).isoformat()
    c.execute("DELETE FROM seen_job_ids WHERE last_polled < %s", (cutoff,))


# ─────────────────────────────────────────
# SCHEMA INIT
# ─────────────────────────────────────────

def init_db():
    conn = get_conn()
    c = conn.cursor()

    # ── Serialize concurrent DDL ──────────────────────────────────────────────
    # Worker processes are spawned via multiprocessing.Process and all call
    # init_db() at startup.  CREATE OR REPLACE FUNCTION / CREATE EXTENSION
    # lock PostgreSQL system catalog rows; concurrent calls race on those locks
    # producing "tuple concurrently updated".
    #
    # pg_advisory_xact_lock(N) is transaction-scoped: it auto-releases when
    # this transaction commits (conn.commit() at the end of this function), so
    # there is no risk of leaving a stale lock.  Workers queue up here and each
    # run the DDL in turn — idempotent statements (IF NOT EXISTS / OR REPLACE)
    # make the repeated executions harmless.
    c.execute("SELECT pg_advisory_xact_lock(7387641)")   # arbitrary unique int

    # ── Extensions ────────────────────────────────────────────────────────────
    # citext: case-insensitive TEXT type (replaces COLLATE NOCASE)
    c.execute("CREATE EXTENSION IF NOT EXISTS citext")

    # ── Helper functions ──────────────────────────────────────────────────────
    # is_valid_json() — replaces SQLite json_valid()
    # Returns TRUE if the text is valid JSON, FALSE otherwise.
    c.execute("""
        CREATE OR REPLACE FUNCTION is_valid_json(p_text TEXT) RETURNS BOOLEAN AS $func$
        BEGIN
            IF p_text IS NULL OR p_text = '' THEN
                RETURN FALSE;
            END IF;
            PERFORM p_text::jsonb;
            RETURN TRUE;
        EXCEPTION WHEN OTHERS THEN
            RETURN FALSE;
        END;
        $func$ LANGUAGE plpgsql IMMUTABLE
    """)

    # json_extract_text() — replaces SQLite json_extract(col, '$.key')
    # Supports simple single-level paths: '$.url', '$.slug', etc.
    c.execute("""
        CREATE OR REPLACE FUNCTION json_extract_text(p_json TEXT, p_path TEXT)
        RETURNS TEXT AS $func$
        DECLARE
            v_key TEXT;
        BEGIN
            IF p_json IS NULL OR p_json = '' THEN
                RETURN NULL;
            END IF;
            -- Strip leading '$.' from path (e.g. '$.url' -> 'url')
            v_key := REGEXP_REPLACE(p_path, '^\\$\\.', '');
            RETURN (p_json::jsonb)->>v_key;
        EXCEPTION WHEN OTHERS THEN
            RETURN NULL;
        END;
        $func$ LANGUAGE plpgsql IMMUTABLE
    """)

    # ── Core pipeline tables ──────────────────────────────────────────────────

    c.execute("""
        CREATE TABLE IF NOT EXISTS applications (
            id              BIGSERIAL PRIMARY KEY,
            company         TEXT NOT NULL,
            job_url         TEXT NOT NULL UNIQUE,
            job_title       TEXT,
            applied_date    DATE DEFAULT CURRENT_DATE,
            status          TEXT DEFAULT 'active',
            expected_domain TEXT,
            exhausted_at    TIMESTAMP,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS recruiters (
            id                BIGSERIAL PRIMARY KEY,
            company           TEXT NOT NULL,
            name              TEXT,
            position          TEXT,
            email             TEXT UNIQUE,
            confidence        TEXT,
            recruiter_status  TEXT DEFAULT 'active',
            last_scraped_at   TIMESTAMP,
            used_search_terms TEXT DEFAULT '[]',
            verified_at       TIMESTAMP,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS application_recruiters (
            id             BIGSERIAL PRIMARY KEY,
            application_id BIGINT NOT NULL REFERENCES applications(id),
            recruiter_id   BIGINT NOT NULL REFERENCES recruiters(id),
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(application_id, recruiter_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS outreach (
            id             BIGSERIAL PRIMARY KEY,
            recruiter_id   BIGINT NOT NULL REFERENCES recruiters(id),
            application_id BIGINT NOT NULL REFERENCES applications(id),
            stage          TEXT DEFAULT 'initial',
            status         TEXT DEFAULT 'pending',
            replied        INTEGER DEFAULT 0,
            scheduled_for  DATE,
            sent_at        TIMESTAMP,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS careershift_quota (
            id          BIGSERIAL PRIMARY KEY,
            date        DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE,
            total_limit INTEGER DEFAULT 50,
            used        INTEGER DEFAULT 0,
            remaining   INTEGER DEFAULT 50
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_cache (
            id                BIGSERIAL PRIMARY KEY,
            cache_key         TEXT NOT NULL UNIQUE,
            company           TEXT NOT NULL,
            job_title         TEXT NOT NULL,
            subject_initial   TEXT,
            subject_followup1 TEXT,
            subject_followup2 TEXT,
            intro             TEXT,
            followup1         TEXT,
            followup2         TEXT,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at        TIMESTAMP NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            url_hash   TEXT PRIMARY KEY,
            job_url    TEXT,
            content    BYTEA,
            created_at BIGINT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS model_usage (
            model TEXT,
            date  TEXT,
            count INTEGER,
            PRIMARY KEY (model, date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS quota_alerts (
            id            BIGSERIAL PRIMARY KEY,
            alert_type    TEXT NOT NULL,
            quota_type    TEXT NOT NULL,
            start_date    DATE NOT NULL,
            end_date      DATE NOT NULL,
            avg_used      REAL,
            avg_remaining REAL,
            suggested_cap INTEGER,
            notified      INTEGER DEFAULT 0,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS coverage_stats (
            id                  BIGSERIAL PRIMARY KEY,
            date                DATE NOT NULL UNIQUE,
            total_applications  INTEGER,
            companies_attempted INTEGER,
            auto_found          INTEGER,
            rejected_count      INTEGER,
            exhausted_count     INTEGER,
            metric1             REAL,
            metric2             REAL,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # prospective_companies uses CITEXT for case-insensitive company matching
    c.execute("""
        CREATE TABLE IF NOT EXISTS prospective_companies (
            id                   BIGSERIAL PRIMARY KEY,
            company              CITEXT NOT NULL UNIQUE,
            priority             INTEGER DEFAULT 0,
            status               TEXT DEFAULT 'pending',
            scraped_at           TIMESTAMP,
            converted_at         TIMESTAMP,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ats_platform         TEXT DEFAULT 'unknown',
            ats_slug             TEXT,
            ats_detected_at      TIMESTAMP,
            first_scanned_at     TIMESTAMP,
            last_checked_at      TIMESTAMP,
            consecutive_empty_days INTEGER DEFAULT 0,
            domain               TEXT,
            listing_curl_raw     TEXT,
            detail_curl_raw      TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS custom_ats_diagnostics (
            id            BIGSERIAL PRIMARY KEY,
            company       TEXT NOT NULL,
            step          TEXT NOT NULL,
            severity      TEXT NOT NULL,
            pattern_hint  TEXT,
            raw_response  TEXT,
            notes         TEXT,
            resolved      INTEGER DEFAULT 0,
            resolved_at   TIMESTAMP,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_custom_ats_diag_company_resolved
        ON custom_ats_diagnostics(company, resolved)
    """)

    # Partial unique index — prevents duplicate open diagnostics.
    # LOWER(company) provides case-insensitive matching for the unique constraint.
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_diag_unique_open
        ON custom_ats_diagnostics(LOWER(company), step, COALESCE(pattern_hint, ''))
        WHERE resolved = 0
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS job_postings (
            id                       BIGSERIAL PRIMARY KEY,
            company                  TEXT NOT NULL,
            title                    TEXT NOT NULL,
            job_url                  TEXT UNIQUE NOT NULL,
            content_hash             TEXT,
            location                 TEXT,
            posted_at                TIMESTAMP,
            description              TEXT,
            skill_score              INTEGER DEFAULT 0,
            status                   TEXT DEFAULT 'new',
            first_seen               DATE NOT NULL,
            consecutive_missing_days INTEGER DEFAULT 0,
            stale_since              DATE,
            created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            -- Phase 1 additions: incremental filter + observability
            job_id                   TEXT,
            ats_platform             TEXT,
            found_by                 TEXT
        )
    """)

    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_job_postings_hash
        ON job_postings(content_hash)
        WHERE content_hash IS NOT NULL
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_postings_status_seen
        ON job_postings(status, first_seen)
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS monitor_stats (
            id                     BIGSERIAL PRIMARY KEY,
            date                   DATE NOT NULL UNIQUE,
            companies_monitored    INTEGER DEFAULT 0,
            companies_with_results INTEGER DEFAULT 0,
            companies_unknown_ats  INTEGER DEFAULT 0,
            api_failures           INTEGER DEFAULT 0,
            total_jobs_fetched     INTEGER DEFAULT 0,
            new_jobs_found         INTEGER DEFAULT 0,
            jobs_matched_filters   INTEGER DEFAULT 0,
            run_duration_seconds   INTEGER DEFAULT 0,
            pdf_generated          INTEGER DEFAULT 0,
            email_sent             INTEGER DEFAULT 0,
            created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS google_api_quota (
            date           DATE PRIMARY KEY,
            queries_used   INTEGER DEFAULT 0,
            queries_limit  INTEGER DEFAULT 100,
            last_updated   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS serper_quota (
            id                    INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            credits_used          INTEGER DEFAULT 0,
            credits_limit         INTEGER DEFAULT 2500,
            low_credit_alert_sent INTEGER DEFAULT 0,
            last_updated          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS api_health (
            id              BIGSERIAL PRIMARY KEY,
            date            DATE    NOT NULL,
            platform        TEXT    NOT NULL,

            -- Request counts
            requests_made      INTEGER DEFAULT 0,
            requests_ok        INTEGER DEFAULT 0,
            requests_429       INTEGER DEFAULT 0,
            requests_404       INTEGER DEFAULT 0,
            requests_error     INTEGER DEFAULT 0,

            -- Error sub-type breakdown (Fix 1 — adaptive polling architecture)
            requests_timeout   INTEGER DEFAULT 0,
            requests_conn_err  INTEGER DEFAULT 0,
            requests_5xx       INTEGER DEFAULT 0,
            requests_other_err INTEGER DEFAULT 0,

            -- Timing (milliseconds)
            avg_response_ms INTEGER DEFAULT 0,
            max_response_ms INTEGER DEFAULT 0,
            total_ms        BIGINT  DEFAULT 0,

            -- Rate limit details
            first_429_at    TIMESTAMP,
            backoff_total_s INTEGER DEFAULT 0,

            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(date, platform)
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_api_health_date_platform
        ON api_health(date, platform)
    """)

    # worker_scaling_events: append-only audit log for every worker pool
    # scaling decision (Phase 10 — Section 16).
    # Effectiveness is derived by querying adjacent events within a time
    # window — no row is ever updated.
    # Retention: managed by _cleanup_worker_scaling_events (90 days).
    c.execute("""
        CREATE TABLE IF NOT EXISTS worker_scaling_events (
            id                    BIGSERIAL PRIMARY KEY,
            occurred_at           TIMESTAMP NOT NULL DEFAULT NOW(),
            event_type            TEXT NOT NULL,
            trigger_layer         TEXT,
            platform              TEXT,
            dc_key                TEXT,
            worker_type           TEXT,
            scan_workers_before   INTEGER,
            scan_workers_after    INTEGER,
            detail_workers_before INTEGER,
            detail_workers_after  INTEGER,
            error_rate            REAL,
            baseline_error_rate   REAL,
            spike_factor          REAL,
            scan_queue_depth      INTEGER,
            detail_queue_depth    INTEGER,
            inflight_count        INTEGER,
            learned_ceiling       INTEGER,
            consec_reductions     INTEGER,
            notes                 TEXT
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS wse_platform_ts
        ON worker_scaling_events(platform, occurred_at)
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS wse_type_ts
        ON worker_scaling_events(event_type, occurred_at)
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_alerts (
            id           BIGSERIAL PRIMARY KEY,
            alert_type   TEXT    NOT NULL,
            severity     TEXT    NOT NULL,
            platform     TEXT,
            value        REAL,
            threshold    REAL,
            message      TEXT,
            notified     INTEGER DEFAULT 0,
            notified_at  TIMESTAMP,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS verify_filled_stats (
            id                         BIGSERIAL PRIMARY KEY,
            date                       DATE NOT NULL UNIQUE,
            verified                   INTEGER DEFAULT 0,
            filled                     INTEGER DEFAULT 0,
            active                     INTEGER DEFAULT 0,
            inconclusive               INTEGER DEFAULT 0,
            inconclusive_timeout       INTEGER DEFAULT 0,
            inconclusive_conn_error    INTEGER DEFAULT 0,
            inconclusive_other_status  INTEGER DEFAULT 0,
            inconclusive_exception     INTEGER DEFAULT 0,
            -- JSON map of status_code → count for inconclusive_other_status
            status_code_breakdown      TEXT DEFAULT '{}',
            remaining                  INTEGER DEFAULT 0,
            run_duration_secs          INTEGER DEFAULT 0,
            created_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_type_platform
        ON pipeline_alerts(alert_type, platform, created_at)
    """)

    # custom_ats_inspection uses CITEXT for case-insensitive company name
    c.execute("""
        CREATE TABLE IF NOT EXISTS custom_ats_inspection (
            id                   BIGSERIAL PRIMARY KEY,
            company              CITEXT NOT NULL UNIQUE,
            listing_url          TEXT,
            format               TEXT,
            array_path           TEXT,
            total_jobs           INTEGER,
            total_field          TEXT,
            all_numeric_fields   TEXT,
            page_size            INTEGER,
            pagination           TEXT,
            session_strategy     TEXT,
            first_job_raw        TEXT,
            field_map            TEXT,
            field_map_override   TEXT,
            sample_normalized    TEXT,
            last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_custom_ats_inspection_company
        ON custom_ats_inspection(company)
    """)

    # ── Phase 1: Incremental dedup tables ────────────────────────────────────

    # seen_job_ids: one row per (company, job_id) pair.
    # On each poll, all fetched job_ids are upserted here.
    # New jobs = fetched_ids − seen_ids (set difference in Python).
    # This reduces detail-fetches from ~146k/day to ~500 (only new jobs).
    c.execute("""
        CREATE TABLE IF NOT EXISTS seen_job_ids (
            id           BIGSERIAL PRIMARY KEY,
            company      TEXT NOT NULL,
            job_id       TEXT NOT NULL,
            job_url      TEXT,
            ats_platform TEXT,
            first_seen   TIMESTAMP NOT NULL DEFAULT NOW(),
            last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
            last_polled  TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(company, job_id)
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_seen_job_ids_company
        ON seen_job_ids(company)
    """)

    # company_poll_stats: per-company adaptive polling state.
    # Scaffold for Phase 5 (adaptive interval engine).
    # current_interval_s: last computed poll interval in seconds.
    # next_poll_at: when to schedule the next poll.
    # adaptive_score: 0.0–1.0 velocity/recency/consistency score.
    c.execute("""
        CREATE TABLE IF NOT EXISTS company_poll_stats (
            id                    BIGSERIAL PRIMARY KEY,
            company               TEXT NOT NULL UNIQUE,
            ats_platform          TEXT,
            current_interval_s    INTEGER DEFAULT 86400,
            next_poll_at          TIMESTAMP,
            last_poll_at          TIMESTAMP,
            adaptive_score        REAL DEFAULT 0.0,
            consecutive_empty     INTEGER DEFAULT 0,
            total_polls           BIGINT DEFAULT 0,
            total_new_jobs        BIGINT DEFAULT 0,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # adaptive_poll_metrics: daily per-company observability snapshot.
    # Populated by Phase 8 to answer: "Is adaptive polling finding all jobs?"
    # See architecture doc Section 17 for metric definitions.
    c.execute("""
        CREATE TABLE IF NOT EXISTS adaptive_poll_metrics (
            id                   BIGSERIAL PRIMARY KEY,
            date                 DATE NOT NULL,
            company              TEXT NOT NULL,
            ats_platform         TEXT,
            total_polls          INTEGER DEFAULT 0,
            total_new_jobs       INTEGER DEFAULT 0,
            tier1_new_jobs       INTEGER DEFAULT 0,
            tier2_new_jobs       INTEGER DEFAULT 0,
            found_within_1hr     INTEGER DEFAULT 0,
            found_within_4hr     INTEGER DEFAULT 0,
            found_within_24hr    INTEGER DEFAULT 0,
            found_after_24hr     INTEGER DEFAULT 0,
            avg_detection_hrs    REAL,
            reactivation_lag_hr  REAL,
            wasted_polls         INTEGER DEFAULT 0,
            http_requests_made   INTEGER DEFAULT 0,
            cost_per_new_job     REAL,
            early_exit_triggered INTEGER DEFAULT 0,
            early_exit_missed    INTEGER DEFAULT 0,
            avg_poll_interval_s  INTEGER,
            score_oscillation    REAL,
            error_streak         INTEGER DEFAULT 0,
            tier_crossed         BOOLEAN DEFAULT FALSE,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (date, company)
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_adaptive_metrics_date
        ON adaptive_poll_metrics(date)
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_adaptive_metrics_company
        ON adaptive_poll_metrics(company)
    """)

    # ── Migrations: add columns to existing tables ────────────────────────────
    # PostgreSQL supports ADD COLUMN IF NOT EXISTS — no try/except needed.

    # job_postings: adaptive polling columns (Phase 2 + 3)
    for col, defn in [
        ("job_id",          "TEXT"),
        ("ats_platform",    "TEXT"),
        ("found_by",        "TEXT"),
        ("first_published", "DATE"),
        ("last_updated",    "TIMESTAMP"),
        ("last_polled",     "TIMESTAMP"),
        ("_country_code",   "CHAR(2)"),
    ]:
        c.execute(f"ALTER TABLE job_postings ADD COLUMN IF NOT EXISTS {col} {defn}")

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_postings_company_jobid
        ON job_postings (company, job_id)
    """)

    # prospective_companies: all ATS detection columns (safe to re-run)
    for col, defn in [
        ("ats_platform",           "TEXT DEFAULT 'unknown'"),
        ("ats_slug",               "TEXT"),
        ("ats_detected_at",        "TIMESTAMP"),
        ("first_scanned_at",       "TIMESTAMP"),
        ("last_checked_at",        "TIMESTAMP"),
        ("consecutive_empty_days", "INTEGER DEFAULT 0"),
        ("domain",                 "TEXT"),
        ("listing_curl_raw",       "TEXT"),
        ("detail_curl_raw",        "TEXT"),
    ]:
        c.execute(
            f"ALTER TABLE prospective_companies ADD COLUMN IF NOT EXISTS {col} {defn}"
        )

    # company_poll_stats: full adaptive engine columns (Phase 4 + 5 + 6)
    for col, defn in [
        # Adaptive interval engine (Phase 5)
        ("recent_poll_counts",  "TEXT DEFAULT '[]'"),
        # Full scan state (Phase 6)
        ("next_full_scan_at",   "TIMESTAMP"),
        ("last_full_scan_at",   "TIMESTAMP"),
        ("full_scan_interval_s","INTEGER DEFAULT 86400"),
        ("full_scan_deferred",  "BOOLEAN DEFAULT FALSE"),
        ("full_scan_interrupted","BOOLEAN DEFAULT FALSE"),
        ("interrupted_at_page", "INTEGER"),
        ("interrupted_at",      "TIMESTAMP"),
        # Health tracking (Phase 10)
        ("consecutive_errors",  "INTEGER DEFAULT 0"),
        ("last_error_at",       "TIMESTAMP"),
        ("last_success_at",     "TIMESTAMP"),
        # WARMING lifecycle — new company onboarding (Section 25 redesign)
        # warming_polls_remaining: NULL=STABLE; 3/2/1=WARMING; decremented by
        #   on_adaptive_complete(). Set to WARMING_POLLS_COUNT by on_fullscan_complete()
        #   when last_poll_at IS NULL (first full scan done).
        # initial_slot_offset_s: slot_offset(batch_position) stored at registration.
        #   Used to schedule first adaptive poll at a deterministic daily slot so
        #   companies are spread evenly across the 24h window. Survives restarts.
        ("warming_polls_remaining", "SMALLINT DEFAULT NULL"),
        ("initial_slot_offset_s",   "INTEGER DEFAULT NULL"),
        # Phase 2 — fullscan duration EMA for thundering herd prevention.
        # _pick_schedule_time() uses avg_fullscan_duration_s to skip gap
        # midpoints where the scan cannot finish before the 7 AM digest:
        #     skip if midpoint + avg_fullscan_duration_s >= next_7am_deadline
        # EMA formula (a=0.3): new = 0.3 * last_duration + 0.7 * prev_avg
        ("last_fullscan_duration_s", "INTEGER"),
        ("avg_fullscan_duration_s",  "DOUBLE PRECISION DEFAULT 1800.0"),
    ]:
        c.execute(
            f"ALTER TABLE company_poll_stats ADD COLUMN IF NOT EXISTS {col} {defn}"
        )

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_poll_stats_next
        ON company_poll_stats (next_poll_at)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_poll_stats_fullscan
        ON company_poll_stats (next_full_scan_at)
    """)

    # ── Backfill initial_slot_offset_s for any legacy rows that have NULL ────
    # This happens when company_poll_stats rows were created before the
    # initial_slot_offset_s column was introduced (ALTER TABLE gives them NULL).
    # upsert_poll_stats() uses COALESCE to backfill on next scan, but we also
    # do it here at startup so no company ever runs with a missing slot offset.
    null_rows = c.execute("""
        SELECT company FROM company_poll_stats
        WHERE initial_slot_offset_s IS NULL
    """).fetchall()
    if null_rows:
        from workers.slot import slot_offset
        for row in null_rows:
            co = row["company"] if hasattr(row, "keys") else row[0]
            offset_s = slot_offset(co)
            c.execute("""
                UPDATE company_poll_stats
                SET initial_slot_offset_s = %s
                WHERE company = %s AND initial_slot_offset_s IS NULL
            """, (offset_s, co))
        import logging
        logging.getLogger(__name__).info(
            "init_db: backfilled initial_slot_offset_s for %d legacy companies: %s",
            len(null_rows),
            [r["company"] if hasattr(r, "keys") else r[0] for r in null_rows],
        )

    # company_config: per-company overrides for adaptive engine (Phase 4)
    c.execute("""
        CREATE TABLE IF NOT EXISTS company_config (
            id                BIGSERIAL PRIMARY KEY,
            company           TEXT NOT NULL UNIQUE,
            sorted_by_recency BOOLEAN,
            refresh_window_hr INTEGER,
            min_interval      INTEGER,
            max_interval      INTEGER,
            force_interval    INTEGER,
            is_pinned         BOOLEAN DEFAULT FALSE,
            is_suspended      BOOLEAN DEFAULT FALSE,
            notes             TEXT,
            created_at        TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)

    # api_health: error sub-type columns (Fix 1 — adaptive polling architecture)
    for col in ["requests_timeout", "requests_conn_err",
                "requests_5xx", "requests_other_err"]:
        c.execute(
            f"ALTER TABLE api_health ADD COLUMN IF NOT EXISTS {col} INTEGER DEFAULT 0"
        )

    # api_health: context column (Phase 10 — baseline purity)
    # Values: 'normal' | 'backoff' | 'canary'
    # Baseline queries filter WHERE context='normal' so managed-error periods
    # do not distort the 30-day historical average.
    c.execute("""
        ALTER TABLE api_health
        ADD COLUMN IF NOT EXISTS context TEXT NOT NULL DEFAULT 'normal'
    """)

    # api_health: migrate unique constraint from (date, platform)
    #             to (date, platform, context) — Phase 10.
    # Uses a DO block so the migration is idempotent on repeated init_db() runs.
    c.execute("""
        DO $$
        BEGIN
            -- Drop the old (date, platform) unique constraint if it still exists.
            -- PostgreSQL auto-names it 'api_health_date_platform_key'.
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'api_health_date_platform_key'
                  AND conrelid = 'api_health'::regclass
            ) THEN
                ALTER TABLE api_health
                DROP CONSTRAINT api_health_date_platform_key;
            END IF;

            -- Add the new (date, platform, context) unique constraint.
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'api_health_date_platform_context_key'
                  AND conrelid = 'api_health'::regclass
            ) THEN
                ALTER TABLE api_health
                ADD CONSTRAINT api_health_date_platform_context_key
                UNIQUE (date, platform, context);
            END IF;
        END $$
    """)

    # custom_ats_diagnostics: resolved_at for retention measurement
    c.execute("""
        ALTER TABLE custom_ats_diagnostics
        ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP
    """)

    # Back-fill resolved_at so COALESCE(resolved_at, created_at) is accurate
    c.execute("""
        UPDATE custom_ats_diagnostics
        SET resolved_at = created_at
        WHERE resolved = 1 AND resolved_at IS NULL
    """)

    # ── Misc index / constraint migrations ───────────────────────────────────

    # Ensure coverage_stats.date has a unique index
    # (deduplicate any pre-existing rows first)
    c.execute("""
        DELETE FROM coverage_stats
        WHERE id NOT IN (
            SELECT MAX(id) FROM coverage_stats GROUP BY date
        )
    """)
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_coverage_stats_date
        ON coverage_stats(date)
    """)

    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_prospective_company_nocase
        ON prospective_companies(company)
    """)

    # ── Cleanup pass ─────────────────────────────────────────────────────────
    _cleanup_auto_close_applications(c)
    _cleanup_closed_application_recruiters(c)
    _cleanup_expired_ai_cache(c)
    _cleanup_expired_jobs(c)
    _cleanup_old_model_usage(c)
    _cleanup_outreach(c)
    _cleanup_job_postings(c)
    _cleanup_careershift_quota(c)
    _cleanup_quota_alerts(c)
    _cleanup_monitor_stats(c)
    _cleanup_verify_filled_stats(c)
    _cleanup_coverage_stats(c)
    _cleanup_api_health(c)
    _cleanup_worker_scaling_events(c)
    _cleanup_pipeline_alerts(c)
    _cleanup_mark_resolved_diagnostics(c)
    _cleanup_resolved_diagnostics(c)
    _cleanup_custom_ats_inspection(c)
    _cleanup_seen_job_ids(c)

    conn.commit()
    conn.close()
    print("[OK] Database initialized: PostgreSQL recruiter_pipeline")
