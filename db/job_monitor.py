"""
db/job_monitor.py — DB operations for job monitoring pipeline.

Key change: get_monitorable_companies() now includes custom platform
companies when they have a valid ats_slug (curl has been captured).
"""

from datetime import datetime
from db.connection import get_conn


def job_url_exists(job_url):
    """
    Check if job URL already exists in job_postings (any status).
    Returns (exists, is_filled) tuple.
    """
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, status FROM job_postings WHERE job_url = ?",
            (job_url,)
        ).fetchone()
        if row is None:
            return False, False
        return True, row["status"] == "filled"
    finally:
        conn.close()


def reactivate_job(job_url, job):
    """Reactivate a previously filled job that has reappeared."""
    conn = get_conn()
    try:
        posted_at = job.get("posted_at")
        if isinstance(posted_at, datetime):
            posted_at = posted_at.isoformat()
        conn.execute("""
            UPDATE job_postings
            SET status                   = 'pre_existing',
                consecutive_missing_days = 0,
                stale_since              = NULL,
                description              = ?,
                posted_at                = ?,
                skill_score              = ?
            WHERE job_url = ?
        """, (
            job.get("description", ""),
            posted_at,
            job.get("skill_score", 0),
            job_url,
        ))
        conn.commit()
    finally:
        conn.close()


def job_hash_exists(content_hash, legacy_hash=None):
    """
    Check if content hash already exists.
    Checks both new and legacy hash during rollout period.
    """
    if not content_hash:
        return False
    conn = get_conn()
    try:
        if legacy_hash and legacy_hash != content_hash:
            row = conn.execute(
                "SELECT id FROM job_postings "
                "WHERE content_hash IN (?, ?)",
                (content_hash, legacy_hash)
            ).fetchone()
        else:
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
    """
    conn = get_conn()
    try:
        today     = datetime.now().strftime("%Y-%m-%d")
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
    """Return all new job postings for today's digest."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM job_postings
            WHERE status = 'new'
            ORDER BY company ASC, skill_score DESC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_postings_digested():
    """Mark all current 'new' postings as 'digested'."""
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE job_postings SET status = 'digested'
            WHERE status = 'new'
        """)
        conn.commit()
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
    """Update last_checked_at and consecutive_empty_days."""
    conn = get_conn()
    try:
        if found_jobs:
            conn.execute("""
                UPDATE prospective_companies
                SET last_checked_at        = CURRENT_TIMESTAMP,
                    consecutive_empty_days = 0
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
    """Return all companies from prospective_companies table."""
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


def get_monitorable_companies():
    """
    Return companies ready for daily job monitoring.

    Includes:
      - All standard detected platforms (greenhouse, lever, ashby etc.)
      - custom WITH valid ats_slug containing a 'url' field
        (curl has been captured and parsed successfully)
      - manual overrides (any platform with _manual flag)

    Excludes:
      - unknown (ATS never detected)
      - custom WITHOUT valid ats_slug (needs curl capture)
      - unsupported (out of scope platforms)
      - NULL slug (detection incomplete)
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, ats_platform, ats_slug,
                   ats_detected_at, first_scanned_at,
                   last_checked_at, consecutive_empty_days, domain
            FROM prospective_companies
            WHERE ats_platform IS NOT NULL
              AND ats_platform NOT IN ('unknown', 'unsupported')
              AND ats_slug IS NOT NULL
              AND (
                  -- Standard platforms: include as long as slug present
                  ats_platform != 'custom'
                  OR
                  -- Custom: only include when slug has captured URL
                  -- json_extract returns NULL if key missing or not JSON
                  (ats_platform = 'custom'
                   AND json_extract(ats_slug, '$.url') IS NOT NULL)
              )
            ORDER BY company ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_detection_queue(batch_size=10):
    """
    Get next batch of companies for ATS detection.
    Priority order:
      1. Never detected (ats_detected_at IS NULL)
      2. Active companies gone quiet (14+ empty days)
      3. Unknown for longest time
      4. Custom without valid slug (needs curl capture)
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
                     WHEN ats_platform = 'custom'
                          AND json_extract(ats_slug, '$.url') IS NULL
                          THEN 4
                     ELSE 99
                   END AS priority
            FROM prospective_companies
            WHERE (
                ats_detected_at IS NULL
                OR consecutive_empty_days >= 14
                OR ats_platform = 'unknown'
                OR (ats_platform = 'custom'
                    AND (ats_slug IS NULL
                         OR json_extract(ats_slug, '$.url') IS NULL))
            )
            ORDER BY
                priority ASC,
                CASE WHEN priority = 1 THEN created_at END ASC,
                CASE WHEN priority = 2 THEN consecutive_empty_days END DESC,
                CASE WHEN priority IN (3, 4) THEN ats_detected_at END ASC
            LIMIT ?
        """, (batch_size,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_detection_queue_stats():
    """Get counts for each priority bucket."""
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
                    AS priority4_custom_nocurl
            FROM (
                SELECT
                    CASE
                        WHEN ats_detected_at IS NULL THEN 1
                        WHEN consecutive_empty_days >= 14 THEN 2
                        WHEN ats_platform = 'unknown' THEN 3
                        WHEN ats_platform = 'custom'
                             AND (ats_slug IS NULL
                             OR json_extract(ats_slug, '$.url') IS NULL)
                             THEN 4
                        ELSE 99
                    END AS priority
                FROM prospective_companies
                WHERE (
                    ats_detected_at IS NULL
                    OR consecutive_empty_days >= 14
                    OR ats_platform = 'unknown'
                    OR (ats_platform = 'custom'
                        AND (ats_slug IS NULL
                             OR json_extract(ats_slug, '$.url') IS NULL))
                )
            ) sub
            WHERE priority < 99
        """).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def save_monitor_stats(stats):
    """Save daily monitoring run stats."""
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
            stats.get("companies_monitored",    0),
            stats.get("companies_with_results",  0),
            stats.get("companies_unknown_ats",   0),
            stats.get("api_failures",            0),
            stats.get("total_jobs_fetched",      0),
            stats.get("new_jobs_found",          0),
            stats.get("jobs_matched_filters",    0),
            stats.get("run_duration_seconds",    0),
            stats.get("pdf_generated",           0),
            stats.get("email_sent",              0),
        ))
        conn.commit()
    finally:
        conn.close()


def save_verify_filled_stats(stats):
    """Save --verify-filled run stats."""
    conn = get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute("""
            INSERT OR REPLACE INTO verify_filled_stats
              (date, verified, filled, active,
               inconclusive, inconclusive_timeout,
               inconclusive_conn_error, inconclusive_other_status,
               inconclusive_exception, remaining, run_duration_secs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            today,
            stats.get("verified",                  0),
            stats.get("filled",                    0),
            stats.get("active",                    0),
            stats.get("inconclusive",              0),
            stats.get("inconclusive_timeout",      0),
            stats.get("inconclusive_conn_error",   0),
            stats.get("inconclusive_other_status", 0),
            stats.get("inconclusive_exception",    0),
            stats.get("remaining",                 0),
            stats.get("run_duration_secs",         0),
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
    """Calculate pipeline reliability over last N days."""
    stats = get_monitor_stats(days)
    if not stats:
        return 1.0
    successful = sum(
        1 for s in stats
        if s.get("pdf_generated", 0) == 1
        or s.get("companies_monitored", 0) == 0
    )
    return successful / len(stats)


def get_stale_jobs(min_missing_days):
    """Return jobs missing from API for min_missing_days+ consecutive days."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT id, company, job_url, title,
                   consecutive_missing_days, stale_since
            FROM job_postings
            WHERE status NOT IN ('filled', 'expired', 'dismissed', 'applied')
            AND consecutive_missing_days >= ?
            ORDER BY consecutive_missing_days DESC
        """, (min_missing_days,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def increment_missing_days(job_ids):
    """Increment consecutive_missing_days for jobs absent from today's scan."""
    if not job_ids:
        return
    conn = get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(f"""
            UPDATE job_postings
            SET consecutive_missing_days = consecutive_missing_days + 1,
                stale_since = CASE
                    WHEN stale_since IS NULL THEN ?
                    ELSE stale_since
                END
            WHERE id IN ({','.join('?' * len(job_ids))})
        """, [today] + list(job_ids))
        conn.commit()
    finally:
        conn.close()


def reset_missing_days(job_ids):
    """Reset consecutive_missing_days for jobs present in today's scan."""
    if not job_ids:
        return
    conn = get_conn()
    try:
        conn.execute(f"""
            UPDATE job_postings
            SET consecutive_missing_days = 0,
                stale_since = NULL
            WHERE id IN ({','.join('?' * len(job_ids))})
        """, list(job_ids))
        conn.commit()
    finally:
        conn.close()


def mark_job_filled(job_id):
    """Mark job as filled — confirmed gone via verification."""
    conn = get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute("""
            UPDATE job_postings
            SET status      = 'filled',
                description = NULL,
                stale_since = ?
            WHERE id = ?
        """, (today, job_id))
        conn.commit()
    finally:
        conn.close()


def get_tracked_urls_for_company(company):
    """Return dict of {job_url: id} for all tracked jobs for a company."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT id, job_url FROM job_postings
            WHERE company = ?
            AND status NOT IN ('expired', 'dismissed', 'applied')
        """, (company,)).fetchall()
        return {r["job_url"]: r["id"] for r in rows}
    finally:
        conn.close()