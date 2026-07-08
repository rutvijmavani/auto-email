"""
db/job_monitor.py — DB operations for job monitoring pipeline.

Key change: get_monitorable_companies() now includes custom platform
companies when they have a valid ats_slug (curl has been captured).
"""

from datetime import datetime, date, timedelta
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

        # ON CONFLICT DO NOTHING replaces INSERT OR IGNORE (SQLite).
        # cursor.rowcount replaces SELECT changes() (SQLite-specific).
        cursor = conn.execute("""
            INSERT INTO job_postings
              (company, title, job_url, content_hash, location,
               posted_at, description, skill_score, status, first_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
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
        return cursor.rowcount > 0
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
    """
    Mark company as having completed first scan.

    Idempotent — only sets first_scanned_at when it is NULL so calling it
    multiple times (e.g. after update_company_check already set it) is safe.
    """
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE prospective_companies
            SET first_scanned_at = CASE
                    WHEN first_scanned_at IS NULL THEN CURRENT_TIMESTAMP
                    ELSE first_scanned_at
                END
            WHERE company = ?
        """, (company,))
        conn.commit()
    finally:
        conn.close()


def update_company_check(company, found_jobs):
    """
    Update company timestamps and consecutive_empty_days after each monitor run.

    first_scanned_at — set on the very first call (when NULL); never overwritten.
    last_checked_at  — updated on every call.
    consecutive_empty_days — reset to 0 when jobs found, incremented otherwise.

    Combining first_scanned_at into this call ensures it is set even when the
    company returns 0 jobs on its first scan (early-return path in _process_company).
    """
    conn = get_conn()
    try:
        if found_jobs:
            conn.execute("""
                UPDATE prospective_companies
                SET first_scanned_at = CASE
                        WHEN first_scanned_at IS NULL THEN CURRENT_TIMESTAMP
                        ELSE first_scanned_at
                    END,
                    last_checked_at        = CURRENT_TIMESTAMP,
                    consecutive_empty_days = 0
                WHERE company = ?
            """, (company,))
        else:
            conn.execute("""
                UPDATE prospective_companies
                SET first_scanned_at = CASE
                        WHEN first_scanned_at IS NULL THEN CURRENT_TIMESTAMP
                        ELSE first_scanned_at
                    END,
                    last_checked_at        = CURRENT_TIMESTAMP,
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
        # json_extract_text() is a safe PL/pgSQL helper defined in init_db()
        # that returns NULL for non-JSON input (catches cast exceptions).
        # Using it here avoids the "invalid input syntax for type json" error
        # that occurs when PostgreSQL eagerly evaluates ats_slug::json for
        # non-custom rows whose ats_slug is a plain string (e.g. "amazon").
        rows = conn.execute("""
            SELECT company, ats_platform, ats_slug,
                   ats_detected_at, first_scanned_at,
                   last_checked_at, consecutive_empty_days
            FROM prospective_companies
            WHERE ats_platform IS NOT NULL
              AND ats_platform NOT IN ('unknown', 'unsupported')
              AND ats_slug IS NOT NULL
              AND (
                  -- Standard platforms: include as long as slug present
                  ats_platform != 'custom'
                  OR
                  -- Custom: only include when slug has a captured URL.
                  -- json_extract_text() safely returns NULL for non-JSON input.
                  (ats_platform = 'custom'
                   AND json_extract_text(ats_slug, '$.url') IS NOT NULL
                   AND json_extract_text(ats_slug, '$.url') <> '')
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
      1. Platform unknown — never been through detection (ats_platform IS NULL)
      2. Active companies gone quiet (14+ empty days)
      3. Unknown for longest time (ats_platform = 'unknown')
      4. Custom without valid slug (needs curl capture)

    Note: ats_detected_at is always stamped at INSERT time to prevent stale
    NULL values triggering unintended re-detection. Priority 1 therefore uses
    ats_platform IS NULL (no platform ever detected) not ats_detected_at IS NULL.
    """
    conn = get_conn()
    try:
        # json_valid() / json_extract() replaced with PostgreSQL equivalents.
        rows = conn.execute("""
            SELECT company, ats_platform, ats_slug,
                   ats_detected_at, consecutive_empty_days,
                   created_at, domain,
                   CASE
                     WHEN ats_platform IS NULL THEN 1
                     WHEN consecutive_empty_days >= 14 THEN 2
                     WHEN ats_platform = 'unknown' THEN 3
                     WHEN ats_platform = 'custom'
                          AND (ats_slug IS NULL
                               OR (ats_slug::json->>'url') IS NULL
                               OR (ats_slug::json->>'url') = '')
                          THEN 4
                     ELSE 99
                   END AS priority
            FROM prospective_companies
            WHERE (
                ats_platform IS NULL
                OR consecutive_empty_days >= 14
                OR ats_platform = 'unknown'
                OR (ats_platform = 'custom'
                    AND (ats_slug IS NULL
                         OR (ats_slug::json->>'url') IS NULL
                         OR (ats_slug::json->>'url') = ''))
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
                        WHEN ats_platform IS NULL THEN 1
                        WHEN consecutive_empty_days >= 14 THEN 2
                        WHEN ats_platform = 'unknown' THEN 3
                        WHEN ats_platform = 'custom'
                             AND (ats_slug IS NULL
                             OR (ats_slug::json->>'url') IS NULL
                             OR (ats_slug::json->>'url') = '')
                             THEN 4
                        ELSE 99
                    END AS priority
                FROM prospective_companies
                WHERE (
                    ats_platform IS NULL
                    OR consecutive_empty_days >= 14
                    OR ats_platform = 'unknown'
                    OR (ats_platform = 'custom'
                        AND (ats_slug IS NULL
                             OR (ats_slug::json->>'url') IS NULL
                             OR (ats_slug::json->>'url') = ''))
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
        # ON CONFLICT(date) DO UPDATE replaces INSERT OR REPLACE (SQLite).
        conn.execute("""
            INSERT INTO monitor_stats
              (date, companies_monitored, companies_with_results,
               companies_unknown_ats, api_failures,
               total_jobs_fetched, new_jobs_found,
               jobs_matched_filters, run_duration_seconds,
               pdf_generated, email_sent,
               in_flight, fallback_scanned)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                companies_monitored    = EXCLUDED.companies_monitored,
                companies_with_results = EXCLUDED.companies_with_results,
                companies_unknown_ats  = EXCLUDED.companies_unknown_ats,
                api_failures           = EXCLUDED.api_failures,
                total_jobs_fetched     = EXCLUDED.total_jobs_fetched,
                new_jobs_found         = EXCLUDED.new_jobs_found,
                jobs_matched_filters   = EXCLUDED.jobs_matched_filters,
                run_duration_seconds   = EXCLUDED.run_duration_seconds,
                pdf_generated          = EXCLUDED.pdf_generated,
                email_sent             = EXCLUDED.email_sent,
                in_flight              = EXCLUDED.in_flight,
                fallback_scanned       = EXCLUDED.fallback_scanned
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
            stats.get("in_flight",               0),
            stats.get("fallback_scanned",        0),
        ))
        conn.commit()
    finally:
        conn.close()


def save_verify_filled_stats(stats):
    """Save --verify-filled run stats."""
    import json
    conn = get_conn()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        breakdown = json.dumps(stats.get("status_code_breakdown", {}))
        conn.execute("""
            INSERT INTO verify_filled_stats
              (date, verified, filled, active,
               inconclusive, inconclusive_timeout,
               inconclusive_conn_error, inconclusive_other_status,
               inconclusive_exception, status_code_breakdown,
               remaining, run_duration_secs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                verified                   = EXCLUDED.verified,
                filled                     = EXCLUDED.filled,
                active                     = EXCLUDED.active,
                inconclusive               = EXCLUDED.inconclusive,
                inconclusive_timeout       = EXCLUDED.inconclusive_timeout,
                inconclusive_conn_error    = EXCLUDED.inconclusive_conn_error,
                inconclusive_other_status  = EXCLUDED.inconclusive_other_status,
                inconclusive_exception     = EXCLUDED.inconclusive_exception,
                status_code_breakdown      = EXCLUDED.status_code_breakdown,
                remaining                  = EXCLUDED.remaining,
                run_duration_secs          = EXCLUDED.run_duration_secs
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
            breakdown,
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
        # DATE('now', '-N days') is SQLite-specific.
        # Compute the cutoff date in Python and pass as a parameter.
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        rows = conn.execute("""
            SELECT * FROM monitor_stats
            WHERE date >= ?
            ORDER BY date DESC
        """, (cutoff,)).fetchall()
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


# ─────────────────────────────────────────
# ADAPTIVE POLLING HELPERS
# ─────────────────────────────────────────

def get_company_row(company: str) -> "dict | None":
    """
    Fetch a single company row by name for the scan worker.
    Returns the same shape as get_monitorable_companies() rows,
    plus domain (used by re-detection logging in _process_company).
    Returns None if company not found.
    """
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT company, ats_platform, ats_slug,
                   ats_detected_at, first_scanned_at,
                   last_checked_at, consecutive_empty_days,
                   domain
            FROM prospective_companies
            WHERE company = ?
        """, (company,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def save_pending_detail(company: str, platform: str, job: dict,
                        found_by: str = "tier1_adaptive") -> bool:
    """
    Insert a job as status='pending_detail' for the detail queue.

    Called by scan_worker (found_by='tier1_adaptive') after a DB check
    confirms the job_id is new, and by fullscan_worker
    (found_by='tier2_fullscan') for jobs found during a full scan.

    The detail_worker will later UPDATE this to 'new' after fetching full
    detail and applying filters.

    Returns True if inserted (new row), False if job_url already exists.
    """
    from datetime import datetime
    conn = get_conn()
    today = datetime.now().strftime("%Y-%m-%d")
    posted_at = job.get("posted_at")
    if isinstance(posted_at, datetime):
        posted_at = posted_at.isoformat()

    try:
        cursor = conn.execute("""
            INSERT INTO job_postings
              (company, title, job_url, job_id, ats_platform,
               location, posted_at, description, skill_score,
               status, found_by, first_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'pending_detail', %s, %s)
            ON CONFLICT (job_url) DO NOTHING
        """, (
            company,
            job.get("title", ""),
            job.get("job_url", ""),
            job.get("job_id"),
            platform,
            job.get("location", ""),
            posted_at,
            job.get("description", ""),
            job.get("skill_score", 0),
            found_by,
            today,
        ))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def save_pre_existing_listing(company: str, platform: str, job: dict) -> bool:
    """
    Persist a job from the first-scan run as status='pre_existing'.

    Called by scan_worker for every job on a company's first ever scan.
    The DB is the source of truth; adaptive_seen:{company} is populated
    directly by _handle_first_scan after all jobs are persisted.

    Returns True if inserted, False if job_url already exists.
    """
    from datetime import datetime
    conn = get_conn()
    today = datetime.now().strftime("%Y-%m-%d")
    posted_at = job.get("posted_at")
    if isinstance(posted_at, datetime):
        posted_at = posted_at.isoformat()

    try:
        cursor = conn.execute("""
            INSERT INTO job_postings
              (company, title, job_url, job_id, ats_platform,
               location, posted_at, status, found_by, first_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    'pre_existing', 'tier1_adaptive', %s)
            ON CONFLICT (job_url) DO NOTHING
        """, (
            company,
            job.get("title", ""),
            job.get("job_url", ""),
            job.get("job_id"),
            platform,
            job.get("location", ""),
            posted_at,
            today,
        ))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def complete_pending_detail(company: str, job_id: str, job: dict,
                             status: str = "new") -> bool:
    """
    Update a pending_detail row to its final status after detail fetch.

    Called by detail_worker after full job data is available and filters pass.
    If status='new' → job is emitted in the next digest.
    If status='pre_existing' → job was seen before and will not be re-emitted.

    Returns True if updated, False if no matching pending_detail row found.
    """
    from datetime import datetime
    posted_at = job.get("posted_at")
    if isinstance(posted_at, datetime):
        posted_at = posted_at.isoformat()

    conn = get_conn()
    try:
        cursor = conn.execute("""
            UPDATE job_postings SET
                status        = %s,
                title         = %s,
                description   = %s,
                location      = %s,
                posted_at     = %s,
                content_hash  = %s,
                skill_score   = %s,
                found_by      = %s,
                _country_code = %s,
                last_polled   = NOW()
            WHERE company = %s
              AND job_id  = %s
              AND status  = 'pending_detail'
        """, (
            status,
            job.get("title", ""),
            job.get("description", ""),
            job.get("location", ""),
            posted_at,
            job.get("content_hash"),
            job.get("skill_score", 0),
            job.get("found_by", "tier1_adaptive"),
            job.get("_country_code"),
            company,
            job_id,
        ))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def delete_pending_detail(company: str, job_id: str) -> None:
    """
    Remove a pending_detail row that failed filters.

    Called by detail_worker when a job is fetched but does not match
    location / freshness filters — so it never enters the digest.
    The job_id remains in the bloom filter / adaptive_seen so it won't be re-detected.
    """
    conn = get_conn()
    try:
        conn.execute("""
            DELETE FROM job_postings
            WHERE company = %s AND job_id = %s AND status = 'pending_detail'
        """, (company, job_id))
        conn.commit()
    finally:
        conn.close()


def upsert_poll_stats(company: str, platform: str,
                      new_jobs: int, duration_ms: int) -> None:
    """
    Create or update the company_poll_stats row after a scan completes.

    On first scan: inserts a new row with initial_slot_offset_s pre-populated
    using slot_offset(company).  This deterministic MD5-based offset spreads
    companies evenly across the 24h window so the WARMING bootstrap
    (on_fullscan_complete) can schedule the first adaptive poll without
    falling back to a runtime computation.

    On subsequent scans: increments total_polls, total_new_jobs,
    resets or increments consecutive_empty based on whether new_jobs > 0.
    initial_slot_offset_s uses COALESCE on conflict: the existing value is
    preserved if already set (original registration slot survives restarts),
    but NULL legacy rows (added before the column existed) are backfilled
    automatically on the next scan.

    duration_ms is stored for scheduler latency tracking but not
    persisted here — the scheduler reads it from the result payload.
    next_poll_at and adaptive_score are left for the scheduler to set.
    """
    from workers.slot import slot_offset as _slot_offset
    offset_s = _slot_offset(company)

    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO company_poll_stats
                (company, ats_platform, last_poll_at,
                 total_polls, total_new_jobs,
                 consecutive_empty, initial_slot_offset_s, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP, 1, ?, 0, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(company) DO UPDATE SET
                ats_platform          = EXCLUDED.ats_platform,
                last_poll_at          = CURRENT_TIMESTAMP,
                total_polls           = company_poll_stats.total_polls + 1,
                total_new_jobs        = company_poll_stats.total_new_jobs
                                        + EXCLUDED.total_new_jobs,
                consecutive_empty     = CASE
                    WHEN EXCLUDED.total_new_jobs > 0
                    THEN 0
                    ELSE company_poll_stats.consecutive_empty + 1
                END,
                updated_at            = CURRENT_TIMESTAMP,
                initial_slot_offset_s = COALESCE(
                    company_poll_stats.initial_slot_offset_s,
                    EXCLUDED.initial_slot_offset_s
                )
        """, (company, platform, new_jobs, offset_s))
        # NOTE: initial_slot_offset_s uses COALESCE so the original
        # registration slot is preserved once set, but legacy NULL rows
        # (created before the column existed) are backfilled on next scan.
        conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(
            "upsert_poll_stats failed for %r: %s", company, e, exc_info=True
        )
    finally:
        conn.close()
