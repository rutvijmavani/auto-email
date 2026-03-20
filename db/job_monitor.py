# db/job_monitor.py — DB operations for job monitoring pipeline

from datetime import datetime
from db.connection import get_conn


def job_url_exists(job_url):
    """Check if job URL already exists in job_postings (any status)."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM job_postings WHERE job_url = ?",
            (job_url,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def job_hash_exists(content_hash):
    """
    Check if content hash already exists.
    Handles same job reposted with different URL.
    """
    if not content_hash:
        return False
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM job_postings WHERE content_hash = ?",
            (content_hash,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def save_job_posting(job, status="new"):
    """
    Save a job posting to DB.
    Returns True if inserted, False if duplicate.
    status: 'new' | 'pre_existing'
    """
    conn = get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        posted_at = job.get("posted_at")
        if isinstance(posted_at, datetime):
            posted_at = posted_at.isoformat()

        conn.execute("""
            INSERT OR IGNORE INTO job_postings
              (company, title, job_url, content_hash, location,
               posted_at, description, skill_score, status, first_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.get("company", ""),
            job.get("title", ""),
            job.get("job_url", ""),
            job.get("content_hash"),
            job.get("location", ""),
            posted_at,
            job.get("description", ""),
            job.get("skill_score", 0),
            status,
            today,
        ))
        conn.commit()
        inserted = conn.execute("SELECT changes()").fetchone()[0]
        return inserted > 0
    finally:
        conn.close()


def get_new_postings_for_digest():
    """
    Return all new job postings for today's digest.
    Sorted by company ASC, then skill_score DESC within company.
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM job_postings
            WHERE status = 'new'
            AND first_seen = DATE('now')
            ORDER BY company ASC, skill_score DESC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_first_scan_complete(company):
    """Mark company as having completed first scan."""
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE prospective_companies
            SET first_scanned_at = CURRENT_TIMESTAMP
            WHERE company = ?
        """, (company,))
        conn.commit()
    finally:
        conn.close()


def update_company_check(company, found_jobs):
    """
    Update last_checked_at and consecutive_empty_days
    after each monitoring run for a company.
    """
    conn = get_conn()
    try:
        if found_jobs:
            conn.execute("""
                UPDATE prospective_companies
                SET last_checked_at         = CURRENT_TIMESTAMP,
                    consecutive_empty_days  = 0
                WHERE company = ?
            """, (company,))
        else:
            conn.execute("""
                UPDATE prospective_companies
                SET last_checked_at        = CURRENT_TIMESTAMP,
                    consecutive_empty_days = COALESCE(
                        consecutive_empty_days, 0
                    ) + 1
                WHERE company = ?
            """, (company,))
        conn.commit()
    finally:
        conn.close()


def get_all_monitored_companies():
    """
    Return all companies from prospective_companies table.
    Includes ATS detection info.
    Used by --monitor-status and --detect-ats.
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, ats_platform, ats_slug,
                   ats_detected_at, first_scanned_at,
                   last_checked_at, consecutive_empty_days,
                   domain
            FROM prospective_companies
            ORDER BY company ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_detection_queue(batch_size=10):
    """
    Get next batch of companies for ATS detection.
    Ordered by priority:

    Priority 1: Never detected (ats_detected_at IS NULL)
                → Newly added companies
                → Sorted by created_at ASC (oldest first)

    Priority 2: Active companies gone quiet (14+ empty days)
                → May have switched ATS
                → Sorted by consecutive_empty_days DESC

    Priority 3: Unknown for longest time
                → ats_platform = 'unknown', tried before
                → Sorted by ats_detected_at ASC

    Priority 4: Custom (unsupported ATS, retry periodically)
                → ats_platform = 'custom'
                → Sorted by ats_detected_at ASC
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, ats_platform, ats_slug,
                   ats_detected_at, consecutive_empty_days,
                   created_at, domain,
                   CASE
                     WHEN ats_detected_at IS NULL THEN 1
                     WHEN consecutive_empty_days >= 14 THEN 2
                     WHEN ats_platform = 'unknown' THEN 3
                     WHEN ats_platform = 'custom'  THEN 4
                     ELSE 99
                   END AS priority
            FROM prospective_companies
            WHERE (
                ats_detected_at IS NULL
                OR consecutive_empty_days >= 14
                OR ats_platform IN ('unknown', 'custom')
            )
            ORDER BY
                priority ASC,
                CASE WHEN priority = 1
                     THEN created_at END ASC,
                CASE WHEN priority = 2
                     THEN consecutive_empty_days END DESC,
                CASE WHEN priority IN (3, 4)
                     THEN ats_detected_at END ASC
            LIMIT ?
        """, (batch_size,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_detection_queue_stats():
    """
    Get counts for each priority bucket.
    Uses same CASE expression as get_detection_queue()
    to avoid double-counting — matches real selection logic.
    Used by --monitor-status.
    """
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN priority = 1 THEN 1 ELSE 0 END), 0)
                    AS priority1_new,
                COALESCE(SUM(CASE WHEN priority = 2 THEN 1 ELSE 0 END), 0)
                    AS priority2_quiet,
                COALESCE(SUM(CASE WHEN priority = 3 THEN 1 ELSE 0 END), 0)
                    AS priority3_unknown,
                COALESCE(SUM(CASE WHEN priority = 4 THEN 1 ELSE 0 END), 0)
                    AS priority4_custom
            FROM (
                SELECT
                    CASE
                        WHEN ats_detected_at IS NULL THEN 1
                        WHEN consecutive_empty_days >= 14 THEN 2
                        WHEN ats_platform = 'unknown' THEN 3
                        WHEN ats_platform = 'custom'  THEN 4
                        ELSE 99
                    END AS priority
                FROM prospective_companies
                WHERE (
                    ats_detected_at IS NULL
                    OR consecutive_empty_days >= 14
                    OR ats_platform IN ('unknown', 'custom')
                )
            ) sub
            WHERE priority < 99
        """).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_monitorable_companies():
    """
    Return only companies ready for daily job monitoring.

    Excludes:
      - unknown: never detected
      - custom:  uses unsupported ATS (out of scope)
      - NULL slug: detection incomplete

    Includes:
      - detected: found via Google ✓
      - manual:   user-overridden ✓
      - close_call: legacy API buffer result
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, ats_platform, ats_slug,
                   ats_detected_at, first_scanned_at,
                   last_checked_at, consecutive_empty_days
            FROM prospective_companies
            WHERE ats_platform IS NOT NULL
            AND ats_platform NOT IN ('unknown', 'custom', 'unsupported')
            AND ats_slug IS NOT NULL
            ORDER BY company ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def save_monitor_stats(stats):
    """
    Save daily monitoring run stats.
    Uses INSERT OR REPLACE to handle re-runs on same day.
    """
    conn = get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute("""
            INSERT OR REPLACE INTO monitor_stats
              (date, companies_monitored, companies_with_results,
               companies_unknown_ats, api_failures,
               total_jobs_fetched, new_jobs_found,
               jobs_matched_filters, run_duration_seconds,
               pdf_generated, email_sent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            today,
            stats.get("companies_monitored", 0),
            stats.get("companies_with_results", 0),
            stats.get("companies_unknown_ats", 0),
            stats.get("api_failures", 0),
            stats.get("total_jobs_fetched", 0),
            stats.get("new_jobs_found", 0),
            stats.get("jobs_matched_filters", 0),
            stats.get("run_duration_seconds", 0),
            stats.get("pdf_generated", 0),
            stats.get("email_sent", 0),
        ))
        conn.commit()
    finally:
        conn.close()


def get_monitor_stats(days=7):
    """Return last N days of monitor stats."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM monitor_stats
            WHERE date >= DATE('now', ?)
            ORDER BY date DESC
        """, (f"-{days} days",)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_pipeline_reliability(days=7):
    """
    Calculate pipeline reliability over last N days.
    Returns float between 0 and 1.
    """
    stats = get_monitor_stats(days)
    if not stats:
        return 1.0
    # A successful run has pdf_generated = 1 OR
    # ran and found 0 jobs (valid outcome)
    successful = sum(
        1 for s in stats
        if s.get("pdf_generated", 0) == 1
        or s.get("companies_monitored", 0) == 0
    )
    return successful / len(stats)