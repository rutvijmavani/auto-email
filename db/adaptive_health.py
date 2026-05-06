# db/adaptive_health.py — Queries for the weekly adaptive polling health report
#
# Three independent query functions, each readable on its own:
#
#   query_miss_rate_by_platform()   — adaptive vs full-scan detection share
#   query_api_error_rates()         — per-platform error rates (normal context only)
#   query_scaling_event_summary()   — worker scaling decision counts
#
# Called by monitor_report._build_adaptive_health_section() every Monday.
# All functions return empty lists / dicts gracefully when data is sparse
# so the report renders "Insufficient data" rather than crashing.
#
# Miss-rate definition:
#   miss_rate = tier2_new_jobs / total_new_jobs
#
#   tier2_new_jobs  = jobs first seen by full scan (adaptive MISSED them)
#   total_new_jobs  = tier1 + tier2 combined
#   A miss rate of 0% means adaptive caught everything before full scan ran.
#   A miss rate of 10% means 1-in-10 new jobs slipped past adaptive.
#
# Error rate definition (baseline purity):
#   Queries api_health WHERE context = 'normal' only.
#   Backoff retries and canary probes are excluded so managed-error periods
#   don't inflate the historical baseline.

import logging
from datetime import date, timedelta
from db.connection import get_conn

logger = logging.getLogger(__name__)

# ── Miss-rate thresholds for colour coding in the report ─────────────────────
MISS_RATE_WARN  = 5.0    # %  — amber above this
MISS_RATE_CRIT  = 15.0   # %  — red above this

# ── Error-rate thresholds ─────────────────────────────────────────────────────
ERROR_RATE_WARN = 5.0    # %
ERROR_RATE_CRIT = 15.0   # %


# ─────────────────────────────────────────
# MISS RATE
# ─────────────────────────────────────────

def query_miss_rate_by_platform(days: int = 7) -> list:
    """
    Compute miss rate per ATS platform over the last `days` days.

    Miss rate = jobs first found by full scan / total new jobs found.
    A high miss rate means adaptive polling is not catching jobs quickly
    enough and the full scan is doing corrective work.

    Platforms with zero total_new_jobs are excluded (no activity to measure).

    Returns:
        List of dicts sorted by miss_rate_pct DESC, each with:
            platform        — ATS name (e.g. "greenhouse")
            total_new_jobs  — combined tier1 + tier2 new jobs
            tier1_new_jobs  — caught by adaptive scan
            tier2_new_jobs  — first seen by full scan (the "misses")
            miss_rate_pct   — float, 0.0–100.0
            days_with_data  — how many days had at least one row
        Empty list if adaptive_poll_metrics has no rows for this window.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                ats_platform                                    AS platform,
                SUM(total_new_jobs)                             AS total_new_jobs,
                SUM(tier1_new_jobs)                             AS tier1_new_jobs,
                SUM(tier2_new_jobs)                             AS tier2_new_jobs,
                CASE
                    WHEN SUM(total_new_jobs) > 0
                    THEN ROUND(
                        100.0 * SUM(tier2_new_jobs) / SUM(total_new_jobs),
                        1
                    )
                    ELSE NULL
                END                                             AS miss_rate_pct,
                COUNT(DISTINCT date)                            AS days_with_data
            FROM adaptive_poll_metrics
            WHERE date >= %s
              AND ats_platform IS NOT NULL
            GROUP BY ats_platform
            HAVING SUM(total_new_jobs) > 0
            ORDER BY miss_rate_pct DESC NULLS LAST
        """, (since,)).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────
# API ERROR RATES (normal context only)
# ─────────────────────────────────────────

def query_api_error_rates(days: int = 7) -> list:
    """
    Compute per-platform error rates from api_health for the last `days` days.

    Only rows with context = 'normal' are included.  Backoff retries and
    canary probes are excluded — they are expected to have high error rates
    by design and would distort the baseline if included.

    Combined error signal = requests_timeout + requests_5xx
                          + requests_429 + requests_404

    Returns:
        List of dicts sorted by error_rate_pct DESC, each with:
            platform         — ATS name
            requests_made    — total requests in window
            total_errors     — combined error count
            error_rate_pct   — float, 0.0–100.0
            avg_response_ms  — average response time (ms)
            days_with_data   — days with at least one row
        Empty list if api_health has no normal-context rows in this window.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                platform,
                SUM(requests_made)                              AS requests_made,
                SUM(requests_timeout + requests_5xx
                    + requests_429 + requests_404)              AS total_errors,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN ROUND(
                        100.0
                        * SUM(requests_timeout + requests_5xx
                              + requests_429 + requests_404)
                        / SUM(requests_made),
                        1
                    )
                    ELSE 0.0
                END                                             AS error_rate_pct,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN SUM(total_ms) / SUM(requests_made)
                    ELSE 0
                END                                             AS avg_response_ms,
                COUNT(DISTINCT date)                            AS days_with_data
            FROM api_health
            WHERE date >= %s
              AND context = 'normal'
              AND requests_made > 0
            GROUP BY platform
            ORDER BY error_rate_pct DESC
        """, (since,)).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────
# SCALING EVENT SUMMARY
# ─────────────────────────────────────────

def query_scaling_event_summary(days: int = 7) -> dict:
    """
    Count worker scaling events by type over the last `days` days.

    Returns:
        Dict mapping event_type → count, e.g.:
            {
                "worker_add":      6,
                "worker_remove":   4,
                "outage_start":    2,
                "outage_end":      2,
                "canary_probe":    4,
                "ceiling_learned": 1,
            }
        All expected keys are always present (default 0) so templates
        can reference them without KeyError checks.
        Also includes "total_events" for a quick non-zero check.
    """
    known_types = {
        "worker_add", "worker_remove",
        "outage_start", "outage_end",
        "canary_probe", "ceiling_learned",
    }
    # Pre-fill with zeros so the template always gets every key
    summary: dict = {t: 0 for t in known_types}
    summary["total_events"] = 0

    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT event_type, COUNT(*) AS cnt
            FROM worker_scaling_events
            WHERE occurred_at >= %s
            GROUP BY event_type
        """, (since,)).fetchall()
    finally:
        conn.close()

    for row in rows:
        et  = row["event_type"]
        cnt = int(row["cnt"])
        summary[et]             = cnt
        summary["total_events"] += cnt

    return summary


# ─────────────────────────────────────────
# DETECTION AGE DISTRIBUTION
# ─────────────────────────────────────────

def query_detection_age_distribution(days: int = 7) -> dict:
    """
    Compute portfolio-wide detection age distribution over the last `days` days.

    Buckets (cumulative share of all new jobs):
        found_within_1hr   — adaptive saw the job within 1 h of it going live
        found_within_4hr   — within 4 h
        found_within_24hr  — within 24 h
        found_after_24hr   — only found by the next full-scan cycle (>24 h)

    Returns:
        Dict with keys:
            total_new_jobs      — int, combined across all platforms
            within_1hr          — int
            within_4hr          — int
            within_24hr         — int
            after_24hr          — int
            within_1hr_pct      — float 0–100, or None if no data
            within_4hr_pct      — float 0–100, or None if no data
            within_24hr_pct     — float 0–100, or None if no data
            after_24hr_pct      — float 0–100, or None if no data
        All counts default to 0; pct values are None when total_new_jobs == 0.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        row = conn.execute("""
            SELECT
                SUM(total_new_jobs)     AS total_new_jobs,
                SUM(found_within_1hr)   AS within_1hr,
                SUM(found_within_4hr)   AS within_4hr,
                SUM(found_within_24hr)  AS within_24hr,
                SUM(found_after_24hr)   AS after_24hr
            FROM adaptive_poll_metrics
            WHERE date >= %s
              AND ats_platform IS NOT NULL
        """, (since,)).fetchone()
    finally:
        conn.close()

    if not row:
        row = {}

    total      = int(row.get("total_new_jobs") or 0)
    within_1hr  = int(row.get("within_1hr")    or 0)
    within_4hr  = int(row.get("within_4hr")    or 0)
    within_24hr = int(row.get("within_24hr")   or 0)
    after_24hr  = int(row.get("after_24hr")    or 0)

    def _pct(n):
        return round(100.0 * n / total, 1) if total else None

    return {
        "total_new_jobs":  total,
        "within_1hr":      within_1hr,
        "within_4hr":      within_4hr,
        "within_24hr":     within_24hr,
        "after_24hr":      after_24hr,
        "within_1hr_pct":  _pct(within_1hr),
        "within_4hr_pct":  _pct(within_4hr),
        "within_24hr_pct": _pct(within_24hr),
        "after_24hr_pct":  _pct(after_24hr),
    }


# ─────────────────────────────────────────
# WASTED POLL RATE
# ─────────────────────────────────────────

def query_wasted_poll_rate(days: int = 7) -> list:
    """
    Compute wasted poll rate per platform over the last `days` days.

    Wasted poll rate = wasted_polls / total_polls
        wasted_polls — adaptive requests that returned no new jobs
        total_polls  — all adaptive requests made (tier1 requests)

    A high wasted-poll rate means the scheduler is checking companies
    too frequently relative to how often they actually post new jobs.
    Ideal: score learning should have already reduced polling frequency
    for low-activity companies; a persistently high rate suggests the
    score function needs attention.

    Platforms with zero total_polls are excluded.

    Returns:
        List of dicts sorted by wasted_rate_pct DESC, each with:
            platform         — ATS name
            total_polls      — adaptive requests in window
            wasted_polls     — polls that yielded no new jobs
            wasted_rate_pct  — float 0–100
            days_with_data   — days with at least one row
        Empty list if no rows in window.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                ats_platform                                    AS platform,
                SUM(total_polls)                                AS total_polls,
                SUM(wasted_polls)                               AS wasted_polls,
                CASE
                    WHEN SUM(total_polls) > 0
                    THEN ROUND(
                        100.0 * SUM(wasted_polls) / SUM(total_polls),
                        1
                    )
                    ELSE NULL
                END                                             AS wasted_rate_pct,
                COUNT(DISTINCT date)                            AS days_with_data
            FROM adaptive_poll_metrics
            WHERE date >= %s
              AND ats_platform IS NOT NULL
            GROUP BY ats_platform
            HAVING SUM(total_polls) > 0
            ORDER BY wasted_rate_pct DESC NULLS LAST
        """, (since,)).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────
# SCORE OSCILLATION
# ─────────────────────────────────────────

def query_score_oscillation(days: int = 14) -> list:
    """
    Compute score oscillation (stddev of daily poll score) per platform
    over the last `days` days (default 14 — two full weeks gives a meaningful
    spread).

    High oscillation means the adaptive score is fluctuating wildly rather
    than converging.  This suggests either:
        • the platform's posting cadence is genuinely irregular, or
        • the score-update logic is overcorrecting (learning-rate too high).

    Uses a 14-day window by default (longer than other queries) because a
    7-day window is too short to distinguish noise from a genuine trend.

    Returns:
        List of dicts sorted by score_stddev DESC, each with:
            platform        — ATS name
            days_with_data  — rows used (≤ days)
            avg_score       — mean daily poll score
            min_score       — minimum daily score seen
            max_score       — maximum daily score seen
            score_stddev    — population stddev of daily score  (0 = perfectly stable)
            score_range     — max_score − min_score (simple spread)
        Empty list if fewer than 3 days of data for every platform.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                ats_platform                            AS platform,
                COUNT(DISTINCT date)                    AS days_with_data,
                ROUND(AVG(poll_score),      2)          AS avg_score,
                MIN(poll_score)                         AS min_score,
                MAX(poll_score)                         AS max_score,
                ROUND(STDDEV_POP(poll_score), 3)        AS score_stddev,
                MAX(poll_score) - MIN(poll_score)       AS score_range
            FROM adaptive_poll_metrics
            WHERE date >= %s
              AND ats_platform IS NOT NULL
              AND poll_score IS NOT NULL
            GROUP BY ats_platform
            HAVING COUNT(DISTINCT date) >= 3
            ORDER BY score_stddev DESC NULLS LAST
        """, (since,)).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────
# EARLY-EXIT MISS RATE
# ─────────────────────────────────────────

def query_early_exit_stats(days: int = 7) -> list:
    """
    Compute early-exit missed job counts per platform over the last `days` days.

    early_exit_missed counts jobs that a scan worker skipped because the
    paginator decided it had already seen enough overlap (smart early exit).
    If a company's posting cadence is unusually bursty — posting many jobs
    between two consecutive scans — early exit can cause the paginator to
    stop before reading all the new pages, missing real jobs.

    A high missed_rate_pct (misses / total_new_jobs) indicates the early-exit
    threshold is too aggressive for that platform.

    Platforms with zero total_new_jobs AND zero early_exit_missed are excluded.

    Returns:
        List of dicts sorted by total_missed DESC, each with:
            platform        — ATS name
            total_missed    — sum of early_exit_missed in window
            total_new_jobs  — sum of total_new_jobs (tier1 + tier2)
            missed_rate_pct — float 0–100, or None if total_new_jobs == 0
            days_with_data  — days with at least one row
        Empty list if no data in window.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                ats_platform                                        AS platform,
                SUM(early_exit_missed)                              AS total_missed,
                SUM(total_new_jobs)                                 AS total_new_jobs,
                CASE
                    WHEN SUM(total_new_jobs) > 0
                    THEN ROUND(
                        100.0 * SUM(early_exit_missed) / SUM(total_new_jobs),
                        1
                    )
                    ELSE NULL
                END                                                 AS missed_rate_pct,
                COUNT(DISTINCT date)                                AS days_with_data
            FROM adaptive_poll_metrics
            WHERE date >= %s
              AND ats_platform IS NOT NULL
            GROUP BY ats_platform
            HAVING SUM(early_exit_missed) > 0 OR SUM(total_new_jobs) > 0
            ORDER BY total_missed DESC NULLS LAST,
                     missed_rate_pct DESC NULLS LAST
        """, (since,)).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────
# WORKER SCALING EFFECTIVENESS
# ─────────────────────────────────────────

def query_scaling_effectiveness(days: int = 7) -> list:
    """
    Summarise worker-reduction effectiveness per platform over the last `days` days.

    For each platform, reports:
        reductions            — worker_remove events (each is one deliberate reduction)
        effective_reductions  — ceiling_learned events (proof reduction worked)
        escalated_to_outage   — outage_start events (reductions exhausted, ATS declared down)
        outages_resolved      — outage_end events (canary succeeded or TTL expired)
        avg_error_at_reduce   — mean error_rate at moment of reduction decision
        avg_error_at_recovery — mean error_rate when a ceiling was learned (post-improvement)

    effectiveness_pct = effective_reductions / reductions × 100
        100% = every reduction improved the error rate before escalating
          0% = all reductions were ineffective (outage was declared each time)

    Only platforms that had at least one reduction or outage event are included.

    Returns:
        List of dicts sorted by reductions DESC, each with the fields above plus
        effectiveness_pct (float 0–100, None if reductions == 0).
        Empty list if no qualifying events in window.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                platform,
                SUM(CASE WHEN event_type = 'worker_remove'   THEN 1 ELSE 0 END)
                                                                    AS reductions,
                SUM(CASE WHEN event_type = 'ceiling_learned' THEN 1 ELSE 0 END)
                                                                    AS effective_reductions,
                SUM(CASE WHEN event_type = 'outage_start'    THEN 1 ELSE 0 END)
                                                                    AS escalated_to_outage,
                SUM(CASE WHEN event_type = 'outage_end'      THEN 1 ELSE 0 END)
                                                                    AS outages_resolved,
                ROUND(
                    AVG(CASE WHEN event_type = 'worker_remove'
                             THEN error_rate ELSE NULL END) * 100,
                    1
                )                                                   AS avg_error_at_reduce,
                ROUND(
                    AVG(CASE WHEN event_type = 'ceiling_learned'
                             THEN error_rate ELSE NULL END) * 100,
                    1
                )                                                   AS avg_error_at_recovery
            FROM worker_scaling_events
            WHERE occurred_at >= NOW() - (%s * INTERVAL '1 day')
              AND platform IS NOT NULL
            GROUP BY platform
            HAVING
                SUM(CASE WHEN event_type IN (
                    'worker_remove', 'outage_start', 'ceiling_learned'
                ) THEN 1 ELSE 0 END) > 0
            ORDER BY reductions DESC NULLS LAST
        """, (days,)).fetchall()
    finally:
        conn.close()

    result = []
    for row in rows:
        d = dict(row)
        reductions          = int(d.get("reductions") or 0)
        effective           = int(d.get("effective_reductions") or 0)
        d["reductions"]     = reductions
        d["effective_reductions"] = effective
        d["escalated_to_outage"]  = int(d.get("escalated_to_outage") or 0)
        d["outages_resolved"]     = int(d.get("outages_resolved") or 0)
        d["effectiveness_pct"] = (
            round(100.0 * effective / reductions, 1) if reductions > 0 else None
        )
        result.append(d)

    return result


# ─────────────────────────────────────────
# COMBINED REPORT DATA
# ─────────────────────────────────────────

def build_weekly_health_data(days: int = 7) -> dict:
    """
    Collect all three data sets for the weekly health report.

    Returns:
        {
            "days":           7,
            "since":          "2026-04-25",
            "miss_rates":     [...],   # from query_miss_rate_by_platform()
            "error_rates":    [...],   # from query_api_error_rates()
            "scaling_events": {...},   # from query_scaling_event_summary()
            "has_data":       bool,    # False → render "insufficient data" banner
        }

    Never raises — all exceptions are caught and logged so a DB hiccup
    on Monday morning never prevents the digest email from sending.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    _empty_age = {
        "total_new_jobs": 0,
        "within_1hr": 0,   "within_4hr": 0,
        "within_24hr": 0,  "after_24hr": 0,
        "within_1hr_pct": None, "within_4hr_pct": None,
        "within_24hr_pct": None, "after_24hr_pct": None,
    }
    result = {
        "days":             days,
        "since":            since,
        "miss_rates":       [],
        "error_rates":      [],
        "scaling_events":   {t: 0 for t in (
            "worker_add", "worker_remove", "outage_start", "outage_end",
            "canary_probe", "ceiling_learned", "total_events",
        )},
        "detection_age":       _empty_age,
        "wasted_poll_rates":   [],
        "score_oscillation":   [],
        "early_exit_stats":    [],
        "scaling_effectiveness": [],
        "has_data":            False,
    }

    try:
        result["miss_rates"] = query_miss_rate_by_platform(days)
    except Exception as exc:
        logger.warning("adaptive_health: miss_rate query failed: %s", exc)

    try:
        result["error_rates"] = query_api_error_rates(days)
    except Exception as exc:
        logger.warning("adaptive_health: error_rates query failed: %s", exc)

    try:
        result["scaling_events"] = query_scaling_event_summary(days)
    except Exception as exc:
        logger.warning("adaptive_health: scaling_events query failed: %s", exc)

    try:
        result["detection_age"] = query_detection_age_distribution(days)
    except Exception as exc:
        logger.warning("adaptive_health: detection_age query failed: %s", exc)

    try:
        result["wasted_poll_rates"] = query_wasted_poll_rate(days)
    except Exception as exc:
        logger.warning("adaptive_health: wasted_poll_rates query failed: %s", exc)

    try:
        # Score oscillation uses a 14-day window regardless of `days` to
        # ensure enough points for a meaningful stddev.
        result["score_oscillation"] = query_score_oscillation(days=14)
    except Exception as exc:
        logger.warning("adaptive_health: score_oscillation query failed: %s", exc)

    try:
        result["early_exit_stats"] = query_early_exit_stats(days)
    except Exception as exc:
        logger.warning("adaptive_health: early_exit_stats query failed: %s", exc)

    try:
        result["scaling_effectiveness"] = query_scaling_effectiveness(days)
    except Exception as exc:
        logger.warning("adaptive_health: scaling_effectiveness query failed: %s", exc)

    result["has_data"] = bool(
        result["miss_rates"] or result["error_rates"]
        or result["scaling_events"]["total_events"]
        or result["detection_age"]["total_new_jobs"]
        or result["wasted_poll_rates"]
    )

    return result
