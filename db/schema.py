# db/schema.py — Database schema creation and cleanup

import sqlite3
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
)

def _cleanup_monitor_stats(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_MONITOR_STATS)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM monitor_stats WHERE date < ?", (cutoff,))


def _cleanup_auto_close_applications(c):
    """Auto-close active applications older than APPLICATION_AUTO_CLOSE_DAYS."""
    c.execute("""
        UPDATE applications
        SET status = 'closed'
        WHERE status = 'active'
        AND applied_date < DATE('now', ?)
    """, (f"-{APPLICATION_AUTO_CLOSE_DAYS} days",))

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

def _cleanup_expired_ai_cache(c):
    c.execute("DELETE FROM ai_cache WHERE expires_at <= CURRENT_TIMESTAMP")


def _cleanup_expired_jobs(c):
    c.execute("DELETE FROM jobs WHERE created_at <= ?",
              (int(time.time()) - RETENTION_JOB_CACHE * 86400,))


def _cleanup_old_model_usage(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_MODEL_USAGE)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM model_usage WHERE date < ?", (cutoff,))


def _cleanup_job_postings(c):
    """Archive expired job postings and remove old dismissed ones."""
    # Archive new postings older than 7 days (clear description to save space)
    c.execute("""
        UPDATE job_postings
        SET status = 'expired', description = NULL
        WHERE status = 'new'
        AND first_seen < DATE('now', '-7 days')
    """)
    # Archive digested postings older than 7 days
    c.execute("""
        UPDATE job_postings
        SET status = 'expired', description = NULL
        WHERE status = 'digested'
        AND first_seen < DATE('now', '-7 days')
    """)
    # Delete dismissed postings older than 30 days
    c.execute("""
        DELETE FROM job_postings
        WHERE status = 'dismissed'
        AND first_seen < DATE('now', '-30 days')
    """)
    # Delete applied postings (already in applications table)
    c.execute("DELETE FROM job_postings WHERE status = 'applied'")
    # Delete filled postings older than retention period
    c.execute("""
        DELETE FROM job_postings
        WHERE status = 'filled'
        AND stale_since < DATE('now', ?)
    """, (f"-{VERIFY_FILLED_RETENTION} days",))


def _cleanup_verify_filled_stats(c):
    """Delete verify_filled_stats older than retention period."""
    c.execute("""
        DELETE FROM verify_filled_stats
        WHERE date < DATE('now', ?)
    """, (f"-{RETENTION_VERIFY_FILLED_STATS} days",))


def _cleanup_outreach(c):
    """Tiered outreach cleanup based on status."""
    c.execute("""
        DELETE FROM outreach WHERE status = 'sent'
        AND sent_at < DATE('now', ?)
    """, (f"-{RETENTION_OUTREACH_SENT} days",))

    c.execute("""
        DELETE FROM outreach WHERE status = 'pending'
        AND scheduled_for < DATE('now', ?)
    """, (f"-{RETENTION_OUTREACH_PENDING} days",))

    c.execute("""
        DELETE FROM outreach WHERE status IN ('failed', 'bounced', 'cancelled')
        AND created_at < DATE('now', ?)
    """, (f"-{RETENTION_OUTREACH_FAILED} days",))


def _cleanup_careershift_quota(c):
    cutoff = (datetime.now() - timedelta(days=RETENTION_CAREERSHIFT_QUOTA)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM careershift_quota WHERE date < ?", (cutoff,))


def _cleanup_quota_alerts(c):
    c.execute("""
        DELETE FROM quota_alerts
        WHERE created_at < DATE('now', ?)
    """, (f"-{RETENTION_QUOTA_ALERTS} days",))

    
def _cleanup_coverage_stats(c):
    c.execute("""
        DELETE FROM coverage_stats
        WHERE date < DATE('now', ?)
    """, (f"-{RETENTION_COVERAGE_STATS} days",))

def _cleanup_api_health(c):
    c.execute("""
        DELETE FROM api_health
        WHERE date < DATE('now', ?)
    """, (f"-{RETENTION_API_HEALTH} days",))

def _cleanup_pipeline_alerts(c):
    c.execute("""
        DELETE FROM pipeline_alerts
        WHERE notified = 1
        AND created_at < DATE('now', ?)
    """, (f"-{RETENTION_PIPELINE_ALERTS} days",))


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS applications (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            company        TEXT NOT NULL,
            job_url        TEXT NOT NULL UNIQUE,
            job_title      TEXT,
            applied_date   DATE DEFAULT (DATE('now')),
            status         TEXT DEFAULT 'active',
            expected_domain TEXT,
            exhausted_at   TIMESTAMP,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS recruiters (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            application_id INTEGER NOT NULL REFERENCES applications(id),
            recruiter_id   INTEGER NOT NULL REFERENCES recruiters(id),
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(application_id, recruiter_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS outreach (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            recruiter_id   INTEGER NOT NULL REFERENCES recruiters(id),
            application_id INTEGER NOT NULL REFERENCES applications(id),
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
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        DATE NOT NULL UNIQUE DEFAULT (DATE('now')),
            total_limit INTEGER DEFAULT 50,
            used        INTEGER DEFAULT 0,
            remaining   INTEGER DEFAULT 50
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_cache (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
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
            content    BLOB,
            created_at INTEGER
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
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
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

    c.execute("""
        CREATE TABLE IF NOT EXISTS prospective_companies (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            company      TEXT NOT NULL UNIQUE,
            priority     INTEGER DEFAULT 0,
            status       TEXT DEFAULT 'pending',
            scraped_at   TIMESTAMP,
            converted_at TIMESTAMP,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS job_postings (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
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
            created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id                    INTEGER PRIMARY KEY DEFAULT 1
                                  CHECK (id = 1),
            credits_used          INTEGER DEFAULT 0,
            credits_limit         INTEGER DEFAULT 2500,
            low_credit_alert_sent INTEGER DEFAULT 0,
            last_updated          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS api_health (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            DATE    NOT NULL,
            platform        TEXT    NOT NULL,

            -- Request counts
            requests_made   INTEGER DEFAULT 0,
            requests_ok     INTEGER DEFAULT 0,
            requests_429    INTEGER DEFAULT 0,
            requests_404    INTEGER DEFAULT 0,
            requests_error  INTEGER DEFAULT 0,

            -- Timing (milliseconds)
            avg_response_ms INTEGER DEFAULT 0,
            max_response_ms INTEGER DEFAULT 0,
            total_ms        INTEGER DEFAULT 0,

            -- Rate limit details
            first_429_at    TIMESTAMP,
            backoff_total_s INTEGER DEFAULT 0,

            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(date, platform)
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_api_health_date_platform
        ON api_health(date, platform)
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_alerts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                DATE NOT NULL UNIQUE,
            verified            INTEGER DEFAULT 0,
            filled              INTEGER DEFAULT 0,
            active              INTEGER DEFAULT 0,
            inconclusive        INTEGER DEFAULT 0,
            inconclusive_timeout      INTEGER DEFAULT 0,
            inconclusive_conn_error   INTEGER DEFAULT 0,
            inconclusive_other_status INTEGER DEFAULT 0,
            inconclusive_exception    INTEGER DEFAULT 0,
            remaining           INTEGER DEFAULT 0,
            run_duration_secs   INTEGER DEFAULT 0,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_pipeline_alerts_type_platform
        ON pipeline_alerts(alert_type, platform, created_at)
    """)

    # Migration: add ATS detection columns to prospective_companies
    for col, definition in [
        ("ats_platform",          "TEXT DEFAULT 'unknown'"),
        ("ats_slug",              "TEXT"),
        ("ats_detected_at",       "TIMESTAMP"),
        ("first_scanned_at",      "TIMESTAMP"),
        ("last_checked_at",       "TIMESTAMP"),
        ("consecutive_empty_days","INTEGER DEFAULT 0"),
        ("domain",                "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE prospective_companies "
                      f"ADD COLUMN {col} {definition}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise

    # Migration: add expected_domain and exhausted_at to applications if missing
    try:
        c.execute("ALTER TABLE applications ADD COLUMN expected_domain TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            raise
    try:
        c.execute("ALTER TABLE applications ADD COLUMN exhausted_at TIMESTAMP")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            raise
    try:
        c.execute("ALTER TABLE job_postings ADD COLUMN consecutive_missing_days INTEGER DEFAULT 0;")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            raise
    try:
        c.execute("ALTER TABLE job_postings ADD COLUMN stale_since DATE;")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            raise

    # Migration: ensure coverage_stats.date has a unique index
    # Deduplicates existing rows keeping latest per date before creating index
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
    _cleanup_pipeline_alerts(c)
    conn.commit()
    conn.close()
    print("[OK] Database initialized: data/recruiter_pipeline.db")