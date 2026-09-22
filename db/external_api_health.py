# db/external_api_health.py — CRUD for external_api_health table
#
# Tracks every third-party enrichment/discovery API request per service per
# day: certspotter, crtsh, brave, kg. Modeled on db/api_health.py (which
# tracks per-ATS-platform job-scan requests) — reuses its write mechanics
# (GREATEST() for max, recomputed avg_response_ms) and query shapes where
# they apply, diverges where they don't:
#   - column is `service`, not `platform`
#   - no `context` column — that's job-scan's adaptive-vs-fullscan/backoff/
#     canary machinery; these are one-shot enrichment API calls, it doesn't apply
#   - has a dedicated `requests_403` bucket (401/403 = "key revoked" for
#     certspotter/crt.sh, alert-worthy on its own, not lumped into other_err)
#   - no `requests_timeout`/`requests_conn_err` sub-type columns — those are
#     folded into requests_other_err here (call volume is far lower than
#     ATS job-scanning, doesn't need the same fan-out)
#
# See docs/enrichment_discovery_design.md §11 "External API Health Tracking"
# for the full finalized schema + retention policy.
#
# Write pattern: synchronous, not queued. api_health's background writer
# thread exists to survive 20 concurrent job-scan threads; these 4 services
# are called from a handful of enrichment/discovery worker processes at
# API-imposed rate limits (certspotter ~10/hr, Brave/KG monthly quotas), so
# volume never approaches the level that justifies a queue. Modeled instead
# on db/api_health.py::record_scaling_event's synchronous retry-once pattern.

import logging
from datetime import date, timedelta
from db.connection import get_conn

logger = logging.getLogger(__name__)


def _classify_status(status_code):
    """
    Map an HTTP status code (0 for non-HTTP errors, e.g. timeout/conn-refused)
    to the (ok, 429, 403, 404, 5xx, other_err) increment tuple.

    401 is folded into the 403 bucket — certspotter/crt.sh both use 401/403
    interchangeably for "key revoked/rejected", and both are alert-worthy in
    the same way (see docs/enrichment_discovery_design.md §11).
    """
    if status_code == 200:
        return 1, 0, 0, 0, 0, 0
    if status_code == 429:
        return 0, 1, 0, 0, 0, 0
    if status_code in (401, 403):
        return 0, 0, 1, 0, 0, 0
    if status_code == 404:
        return 0, 0, 0, 1, 0, 0
    if 500 <= status_code < 600:
        return 0, 0, 0, 0, 1, 0
    return 0, 0, 0, 0, 0, 1   # 0 (timeout/conn err) or any other code


def record_external_request(service, status_code, response_ms, backoff_s=0):
    """
    Record one third-party API request in external_api_health.

    Synchronous write with one retry on transient connection loss — see
    module docstring for why this doesn't need api_health's background
    writer queue.

    Args:
        service:      'certspotter' | 'crtsh' | 'brave' | 'kg'
        status_code:  HTTP status code (0 for non-HTTP errors, e.g. timeout)
        response_ms:  response time in milliseconds
        backoff_s:    seconds waited due to rate limit / Retry-After
    """
    today = date.today().isoformat()
    ok_inc, e429_inc, e403_inc, e404_inc, e5xx_inc, other_inc = _classify_status(status_code)

    def _do_write():
        conn = get_conn()
        try:
            conn.execute("""
                INSERT INTO external_api_health (date, service)
                VALUES (?, ?)
                ON CONFLICT(date, service) DO NOTHING
            """, (today, service))

            # GREATEST() replaces MAX() in UPDATE context (MAX() does not work
            # as a two-argument comparison function in PostgreSQL UPDATE).
            conn.execute("""
                UPDATE external_api_health SET
                    requests_made       = requests_made       + 1,
                    requests_ok         = requests_ok         + ?,
                    requests_429        = requests_429        + ?,
                    requests_403        = requests_403        + ?,
                    requests_404        = requests_404        + ?,
                    requests_5xx        = requests_5xx        + ?,
                    requests_other_err  = requests_other_err  + ?,
                    total_ms            = total_ms            + ?,
                    max_response_ms     = GREATEST(max_response_ms, ?),
                    backoff_total_s     = backoff_total_s     + ?,
                    first_429_at        = CASE
                        WHEN ? = 1 AND first_429_at IS NULL
                        THEN CURRENT_TIMESTAMP
                        ELSE first_429_at
                    END
                WHERE date = ? AND service = ?
            """, (
                ok_inc, e429_inc, e403_inc, e404_inc, e5xx_inc, other_inc,
                response_ms, response_ms,
                backoff_s,
                e429_inc,
                today, service,
            ))

            conn.execute("""
                UPDATE external_api_health SET
                    avg_response_ms = CASE
                        WHEN requests_made > 0
                        THEN total_ms / requests_made
                        ELSE 0
                    END
                WHERE date = ? AND service = ?
            """, (today, service))

            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    for _attempt in range(2):
        try:
            _do_write()
            return
        except Exception as exc:
            if _attempt == 0:
                continue   # transient connection loss — retry once
            logger.warning(
                "external_api_health: record_external_request failed "
                "(service=%r status_code=%r): %s",
                service, status_code, exc,
            )


# ─────────────────────────────────────────
# QUERY FUNCTIONS
# ─────────────────────────────────────────

def get_service_stats(service, for_date=None):
    """Get external_api_health stats for one service on one date."""
    for_date = for_date or date.today().isoformat()
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT * FROM external_api_health
            WHERE date = ? AND service = ?
        """, (for_date, service)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_external_health_summary(days=7):
    """
    Get aggregated health stats per service for the last N days.
    Returns list of dicts sorted by service. Powers the pipeline_metrics.py
    "External API Health" report section.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                service,
                SUM(requests_made)      AS total_requests,
                SUM(requests_ok)        AS total_ok,
                SUM(requests_429)       AS total_429s,
                SUM(requests_403)       AS total_403s,
                SUM(requests_404)       AS total_404s,
                SUM(requests_5xx)       AS total_5xx,
                SUM(requests_other_err) AS total_other_err,
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
                        100.0 * (SUM(requests_403) + SUM(requests_404)
                                 + SUM(requests_5xx) + SUM(requests_other_err))
                        / SUM(requests_made), 1)
                    ELSE 0
                END AS error_pct,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN ROUND(SUM(total_ms) / SUM(requests_made))::int
                    ELSE 0
                END AS avg_response_ms,
                MAX(max_response_ms) AS max_response_ms,
                SUM(backoff_total_s) AS total_backoff_s,
                MIN(first_429_at)    AS earliest_429_at,
                COUNT(DISTINCT date) AS days_with_data
            FROM external_api_health
            WHERE date >= ?
            GROUP BY service
            ORDER BY service ASC
        """, (since,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _month_bounds(year_month=None):
    """
    [start, end) calendar-month date bounds as (date, date), end exclusive.
    Pure — split out from get_month_request_count so the month-rollover /
    year-rollover arithmetic (esp. December → January) is directly unit
    testable without a DB.

    year_month: "YYYY-MM" string, defaults to the current calendar month.
    """
    if year_month:
        year, month = (int(x) for x in year_month.split("-"))
        start = date(year, month, 1)
    else:
        start = date.today().replace(day=1)
    end = date(start.year + 1, 1, 1) if start.month == 12 else date(start.year, start.month + 1, 1)
    return start, end


def get_month_request_count(service, year_month=None):
    """
    Atomic month-to-date request count for a service — backs Brave's
    1000/month quota gate (scripts/discover_h1b_ats.py::_brave_load_quota,
    build_ats_slug_list.py::_load_brave_quota).

    Mirrors db/quota.py's can_call()/model_usage date-window pattern, summed
    across external_api_health's per-day rows (this table buckets by day for
    the health report, not by month) instead of a single per-month row.
    Every requests_made increment behind this sum comes from
    record_external_request()'s atomic `requests_made = requests_made + 1`
    UPDATE, so — unlike the local-JSON-file counter this replaced — no
    concurrent caller (multiple worker processes, multiple scripts sharing
    the same Brave key) can lose an increment to a race.

    year_month: "YYYY-MM" string, defaults to the current calendar month.
    """
    start, end = _month_bounds(year_month)

    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT COALESCE(SUM(requests_made), 0) AS total
            FROM external_api_health
            WHERE service = ? AND date >= ? AND date < ?
        """, (service, start.isoformat(), end.isoformat())).fetchone()
        return row["total"] if row else 0
    finally:
        conn.close()


def get_day_request_count(service, for_date=None):
    """
    Atomic today's (or a given day's) request count for a service — backs the
    CF probe Worker's daily quota gate (scripts/discover_h1b_ats.py and
    jobs/ats/career_detector.py, both call sites of _fetch_via_worker()).

    Sibling to get_month_request_count() above, windowed to a single day
    instead of a calendar month — the Workers free-tier plan resets daily
    (see config.CF_WORKER_DAILY_LIMIT), unlike Brave's monthly cap. Reads the
    same requests_made column that record_external_request() increments
    atomically on every call, so this is race-proof the same way.

    for_date: date object, defaults to today.
    """
    day = (for_date or date.today()).isoformat()

    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT COALESCE(SUM(requests_made), 0) AS total
            FROM external_api_health
            WHERE service = ? AND date = ?
        """, (service, day)).fetchone()
        return row["total"] if row else 0
    finally:
        conn.close()


def get_todays_external_stats():
    """Get all service stats for today."""
    today = date.today().isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM external_api_health
            WHERE date = ?
            ORDER BY service ASC
        """, (today,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
