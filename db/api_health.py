# db/api_health.py — CRUD for api_health table
#
# Tracks every ATS API request per platform per day.
# Used to detect rate limiting and performance issues.
# Powers --monitor-status health table and alert emails.

import json
from datetime import datetime, date, timedelta
from db.connection import get_conn


# ─────────────────────────────────────────
# RECORD REQUESTS
# ─────────────────────────────────────────

def record_request(platform, status_code, response_ms,
                   backoff_s=0):
    """
    Record one API request in api_health.
    Called by request_with_tracking() in base.py
    after every ATS API call.

    Args:
        platform:     e.g. "greenhouse"
        status_code:  HTTP status code (200/429/404/500 etc.)
        response_ms:  response time in milliseconds
        backoff_s:    seconds waited due to rate limit
    """
    today = date.today().isoformat()
    conn  = get_conn()
    try:
        # Upsert row for today + platform
        conn.execute("""
            INSERT INTO api_health (date, platform)
            VALUES (?, ?)
            ON CONFLICT(date, platform) DO NOTHING
        """, (today, platform))

        # Determine which counter to increment
        if status_code == 200:
            ok_inc    = 1
            e429_inc  = 0
            e404_inc  = 0
            err_inc   = 0
        elif status_code == 429:
            ok_inc    = 0
            e429_inc  = 1
            e404_inc  = 0
            err_inc   = 0
        elif status_code == 404:
            ok_inc    = 0
            e429_inc  = 0
            e404_inc  = 1
            err_inc   = 0
        else:
            ok_inc    = 0
            e429_inc  = 0
            e404_inc  = 0
            err_inc   = 1

        # Update counters + timing
        conn.execute("""
            UPDATE api_health SET
                requests_made   = requests_made   + 1,
                requests_ok     = requests_ok     + ?,
                requests_429    = requests_429    + ?,
                requests_404    = requests_404    + ?,
                requests_error  = requests_error  + ?,
                total_ms        = total_ms        + ?,
                max_response_ms = MAX(max_response_ms, ?),
                backoff_total_s = backoff_total_s + ?,
                first_429_at    = CASE
                    WHEN ? = 1 AND first_429_at IS NULL
                    THEN CURRENT_TIMESTAMP
                    ELSE first_429_at
                END
            WHERE date = ? AND platform = ?
        """, (
            ok_inc, e429_inc, e404_inc, err_inc,
            response_ms, response_ms,
            backoff_s,
            e429_inc,
            today, platform
        ))

        # Recalculate avg_response_ms
        conn.execute("""
            UPDATE api_health SET
                avg_response_ms = CASE
                    WHEN requests_made > 0
                    THEN total_ms / requests_made
                    ELSE 0
                END
            WHERE date = ? AND platform = ?
        """, (today, platform))

        conn.commit()

    finally:
        conn.close()


# ─────────────────────────────────────────
# QUERY
# ─────────────────────────────────────────

def get_platform_stats(platform, for_date=None):
    """
    Get api_health stats for one platform on one date.
    Defaults to today.
    """
    for_date = for_date or date.today().isoformat()
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT * FROM api_health
            WHERE date = ? AND platform = ?
        """, (for_date, platform)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_health_summary(days=7):
    """
    Get aggregated health stats per platform
    for the last N days.

    Returns list of dicts sorted by platform:
    [{
        platform, total_requests, total_429s,
        rate_429_pct, avg_response_ms, max_response_ms,
        total_errors, error_pct, days_with_data
    }]
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                platform,
                SUM(requests_made)   AS total_requests,
                SUM(requests_ok)     AS total_ok,
                SUM(requests_429)    AS total_429s,
                SUM(requests_404)    AS total_404s,
                SUM(requests_error)  AS total_errors,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN ROUND(
                        100.0 * SUM(requests_429)
                        / SUM(requests_made), 1)
                    ELSE 0
                END AS rate_429_pct,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN ROUND(
                        100.0 * SUM(requests_error)
                        / SUM(requests_made), 1)
                    ELSE 0
                END AS error_pct,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN SUM(total_ms) / SUM(requests_made)
                    ELSE 0
                END AS avg_response_ms,
                MAX(max_response_ms) AS max_response_ms,
                COUNT(DISTINCT date) AS days_with_data
            FROM api_health
            WHERE date >= ?
            GROUP BY platform
            ORDER BY platform ASC
        """, (since,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_todays_stats():
    """Get all platform stats for today."""
    today = date.today().isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM api_health
            WHERE date = ?
            ORDER BY platform ASC
        """, (today,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_run_429_rate(platform, since_minutes=60):
    """
    Get 429 rate for a platform in the last N minutes.
    Used to check if we're being rate limited RIGHT NOW.
    Returns percentage (0-100).
    """
    conn = get_conn()
    try:
        # Use today's data as proxy
        today = date.today().isoformat()
        row   = conn.execute("""
            SELECT requests_made, requests_429
            FROM api_health
            WHERE date = ? AND platform = ?
        """, (today, platform)).fetchone()

        if not row or row["requests_made"] == 0:
            return 0.0

        return round(
            100.0 * row["requests_429"] / row["requests_made"],
            1
        )
    finally:
        conn.close()