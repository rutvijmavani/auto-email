"""
db/adaptive_metrics.py
----------------------
Nightly writer for adaptive_poll_metrics (one row per company per day), the single
data source behind the Monday "Adaptive Polling Health" block.
Design: docs/adaptive-metrics-snapshot-design.md.

Two independent upserts, both idempotent (ON CONFLICT (date, company) DO UPDATE):

  snapshot_counters(day)  poll counters, score, interval, error streak. Read from
                          company_poll_stats AT RUN TIME, so it is only valid for
                          "today". Daily value = running total now minus the
                          *_cum stored on the company's previous row. First row for
                          a company writes a delta of 0 (no spike). A negative delta
                          (row deleted and recreated) is clamped to 0 and logged.

  snapshot_jobs(day)      tier1/tier2 counts, detection-age buckets and likely
                          early-exit misses, derived from job_postings rows created
                          on `day` (status 'pre_existing' and each company's 24h cold
                          start excluded). Safe to recompute for any past day.

run_nightly(today) is called from the manager's nightly recompute: it writes today's
counters, refreshes today (partial: the run is at 23:00) and yesterday (re-read, not
guaranteed final: rows can change status or be deleted later and are not reconciled)
and fills any day missing inside the backfill window.

Known limits (also in the design doc):
  * created_at is when the SCAN inserted the pending_detail row (complete_pending_detail
    does not change it), so detection age is posting time to scan-insert time and does
    NOT include detail-queue delay.
  * Age buckets use only platforms in ADAPTIVE_TIMED_POSTED_AT_PLATFORMS (real time of
    day in posted_at). Other platforms contribute to counts, not to buckets.
  * early_exit_missed is an INDICATOR, not a bound: fullscan inserted it and it was posted
    more than ADAPTIVE_MISS_LAG_HOURS earlier (listing-date-reliable platforms only).
    Dated jobs the fullscan inserted within that lag of their posting are excluded, and
    other causes (long poll interval, failed scan, filter error) can also qualify.
  * Counters of days skipped (manager down) are attributed to the next snapshot day.
"""

from datetime import date, timedelta

from config import (
    ADAPTIVE_MISS_LAG_HOURS,
    ADAPTIVE_SNAPSHOT_BACKFILL_DAYS,
    ADAPTIVE_TIMED_POSTED_AT_PLATFORMS,
)
from db.connection import get_conn
from logger import get_logger

logger = get_logger(__name__)

_COLD_START_HOURS = 24

_COUNTERS_SQL = """
INSERT INTO adaptive_poll_metrics
    (date, company, ats_platform,
     total_polls, wasted_polls, early_exit_triggered,
     polls_cum, empty_polls_cum, early_exit_cum,
     adaptive_score, avg_poll_interval_s, error_streak)
SELECT %(d)s, cps.company, cps.ats_platform,
       CASE WHEN prev.company IS NULL THEN 0
            ELSE GREATEST(COALESCE(cps.total_polls, 0) - prev.polls_cum, 0)::int END,
       CASE WHEN prev.company IS NULL THEN 0
            ELSE GREATEST(COALESCE(cps.empty_polls, 0) - prev.empty_polls_cum, 0)::int END,
       CASE WHEN prev.company IS NULL THEN 0
            ELSE GREATEST(COALESCE(cps.early_exit_polls, 0) - prev.early_exit_cum, 0)::int END,
       COALESCE(cps.total_polls, 0),
       COALESCE(cps.empty_polls, 0),
       COALESCE(cps.early_exit_polls, 0),
       cps.adaptive_score, cps.current_interval_s, COALESCE(cps.consecutive_errors, 0)
FROM company_poll_stats cps
LEFT JOIN LATERAL (
    SELECT m.company, m.polls_cum, m.empty_polls_cum, m.early_exit_cum
    FROM adaptive_poll_metrics m
    WHERE m.company = cps.company AND m.date < %(d)s AND m.polls_cum IS NOT NULL
    ORDER BY m.date DESC
    LIMIT 1
) prev ON TRUE
ON CONFLICT (date, company) DO UPDATE SET
    ats_platform         = COALESCE(EXCLUDED.ats_platform, adaptive_poll_metrics.ats_platform),
    total_polls          = EXCLUDED.total_polls,
    wasted_polls         = EXCLUDED.wasted_polls,
    early_exit_triggered = EXCLUDED.early_exit_triggered,
    polls_cum            = EXCLUDED.polls_cum,
    empty_polls_cum      = EXCLUDED.empty_polls_cum,
    early_exit_cum       = EXCLUDED.early_exit_cum,
    adaptive_score       = EXCLUDED.adaptive_score,
    avg_poll_interval_s  = EXCLUDED.avg_poll_interval_s,
    error_streak         = EXCLUDED.error_streak
"""

_NEGATIVE_SQL = """
SELECT COUNT(*) AS n
FROM company_poll_stats cps
JOIN LATERAL (
    SELECT m.polls_cum
    FROM adaptive_poll_metrics m
    WHERE m.company = cps.company AND m.date < %(d)s AND m.polls_cum IS NOT NULL
    ORDER BY m.date DESC
    LIMIT 1
) prev ON TRUE
WHERE COALESCE(cps.total_polls, 0) < prev.polls_cum
"""

_JOBS_SQL = """
WITH first_row AS (
    SELECT company, MIN(created_at) AS t0 FROM job_postings GROUP BY company
),
rows AS (
    SELECT p.company, p.ats_platform, p.found_by, p.posted_at, p.created_at,
           (p.posted_at IS NOT NULL AND p.ats_platform = ANY(%(timed)s)) AS timed,
           (p.posted_at IS NOT NULL AND p.ats_platform = ANY(%(reliable)s)) AS reliable
    FROM job_postings p
    JOIN first_row f ON f.company = p.company
    WHERE p.created_at >= %(d)s::date
      AND p.created_at <  %(d)s::date + 1
      AND p.created_at >  f.t0 + %(cold)s * INTERVAL '1 hour'
      AND p.found_by IN ('tier1_adaptive', 'tier2_fullscan')
      AND p.status <> 'pre_existing'
)
INSERT INTO adaptive_poll_metrics
    (date, company, ats_platform, tier1_new_jobs, tier2_new_jobs, total_new_jobs,
     found_within_1hr, found_within_4hr, found_within_24hr, found_after_24hr,
     early_exit_missed)
SELECT %(d)s, company, MAX(ats_platform),
       COUNT(*) FILTER (WHERE found_by = 'tier1_adaptive'),
       COUNT(*) FILTER (WHERE found_by = 'tier2_fullscan'),
       COUNT(*),
       COUNT(*) FILTER (WHERE timed AND created_at - posted_at <= INTERVAL '1 hour'),
       COUNT(*) FILTER (WHERE timed AND created_at - posted_at <= INTERVAL '4 hours'),
       COUNT(*) FILTER (WHERE timed AND created_at - posted_at <= INTERVAL '24 hours'),
       COUNT(*) FILTER (WHERE timed AND created_at - posted_at >  INTERVAL '24 hours'),
       COUNT(*) FILTER (WHERE found_by = 'tier2_fullscan' AND reliable
                          AND posted_at < created_at - %(lag)s * INTERVAL '1 hour')
FROM rows
GROUP BY company
ON CONFLICT (date, company) DO UPDATE SET
    ats_platform      = COALESCE(EXCLUDED.ats_platform, adaptive_poll_metrics.ats_platform),
    tier1_new_jobs    = EXCLUDED.tier1_new_jobs,
    tier2_new_jobs    = EXCLUDED.tier2_new_jobs,
    total_new_jobs    = EXCLUDED.total_new_jobs,
    found_within_1hr  = EXCLUDED.found_within_1hr,
    found_within_4hr  = EXCLUDED.found_within_4hr,
    found_within_24hr = EXCLUDED.found_within_24hr,
    found_after_24hr  = EXCLUDED.found_after_24hr,
    early_exit_missed = EXCLUDED.early_exit_missed
"""

# Days in [lo, hi] that have no row at all in adaptive_poll_metrics.
_MISSING_DAYS_SQL = """
SELECT g::date AS d
FROM generate_series(%(lo)s::date, %(hi)s::date, INTERVAL '1 day') g
WHERE NOT EXISTS (SELECT 1 FROM adaptive_poll_metrics m WHERE m.date = g::date)
ORDER BY d
"""


def snapshot_counters(day: date) -> int:
    """Write poll counters/score for `day` (must be today: reads current totals)."""
    conn = get_conn()
    try:
        neg = conn.execute(_NEGATIVE_SQL, {"d": day}).fetchone()
        if neg and int(neg["n"] or 0) > 0:
            logger.warning(
                "adaptive_metrics: %d companies have total_polls below their last "
                "snapshot (row deleted/recreated?) - daily delta clamped to 0",
                int(neg["n"]),
            )
        cur = conn.execute(_COUNTERS_SQL, {"d": day})
        n = cur.rowcount
        conn.commit()
        return n
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def snapshot_jobs(day: date) -> int:
    """Write job-derived columns for `day` from job_postings. Safe for any past day."""
    from jobs.ats.registry import LISTING_DATE_RELIABLE
    params = {
        "d":        day,
        "cold":     _COLD_START_HOURS,
        "lag":      ADAPTIVE_MISS_LAG_HOURS,
        "timed":    list(ADAPTIVE_TIMED_POSTED_AT_PLATFORMS),
        "reliable": sorted(LISTING_DATE_RELIABLE),
    }
    conn = get_conn()
    try:
        cur = conn.execute(_JOBS_SQL, params)
        n = cur.rowcount
        conn.commit()
        return n
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _missing_days(lo: date, hi: date) -> list:
    if lo > hi:
        return []
    conn = get_conn()
    try:
        rows = conn.execute(_MISSING_DAYS_SQL, {"lo": lo, "hi": hi}).fetchall()
        return [r["d"] for r in rows]
    finally:
        conn.close()


def run_nightly(today: date) -> dict:
    """
    Nightly entry point. Each part is isolated: a failure is logged and the others
    still run. Returns {"counters": n, "jobs_days": [...], "errors": n}.
    """
    out = {"counters": 0, "jobs_days": [], "errors": 0}

    try:
        out["counters"] = snapshot_counters(today)
    except Exception as exc:
        out["errors"] += 1
        logger.error("adaptive_metrics: counters snapshot failed: %s", exc, exc_info=True)

    yesterday = today - timedelta(days=1)
    lo = today - timedelta(days=ADAPTIVE_SNAPSHOT_BACKFILL_DAYS)
    try:
        missing = _missing_days(lo, yesterday)
    except Exception as exc:
        missing = []
        out["errors"] += 1
        logger.error("adaptive_metrics: missing-day lookup failed: %s", exc, exc_info=True)

    days = sorted({today, yesterday, *missing})
    for d in days:
        try:
            snapshot_jobs(d)
            out["jobs_days"].append(d.isoformat())
        except Exception as exc:
            out["errors"] += 1
            logger.error("adaptive_metrics: jobs snapshot %s failed: %s", d, exc, exc_info=True)

    logger.info(
        "adaptive_metrics: nightly snapshot done | counters_rows=%d jobs_days=%s errors=%d",
        out["counters"], out["jobs_days"], out["errors"],
    )
    return out
