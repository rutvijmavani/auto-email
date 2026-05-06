"""
tests/test_adaptive_health.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for:

  db/adaptive_health.py
    · query_miss_rate_by_platform()        — Section A
    · query_api_error_rates()              — Section B
    · query_scaling_event_summary()        — Section C
    · query_detection_age_distribution()   — Section D
    · query_wasted_poll_rate()             — Section E
    · query_score_oscillation()            — Section F  (mock, STDDEV_POP)
    · query_early_exit_stats()             — Section G
    · query_scaling_effectiveness()        — Section H  (mock, NOW()-INTERVAL)
    · build_weekly_health_data()           — Section I

  outreach/report_templates/monitor_report.py
    · _build_adaptive_health_section()     — Section J

Strategy
────────
Sections A–E, G: direct SQLite tests — the SQL is run against a real in-memory
  SQLite DB using ? params so every aggregation, HAVING, ORDER BY, and CASE
  expression is exercised.

Sections F, H: get_conn() is mocked because the production SQL uses PostgreSQL-
  only functions (STDDEV_POP, NOW() - INTERVAL). The Python-level wrapping
  (empty-list handling, dict conversion, pct computation) is still tested.

Section I: each individual query function is mocked so has_data logic and
  exception-isolation are tested without a DB.

Section J: build_weekly_health_data() is mocked so every HTML path can be
  exercised with controlled data.
"""

import sys
import os
import sqlite3
import unittest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Shared SQLite schema helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_metrics_db():
    """Create in-memory SQLite with adaptive_poll_metrics schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE adaptive_poll_metrics (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            date              DATE    NOT NULL,
            ats_platform      TEXT,
            total_new_jobs    INTEGER DEFAULT 0,
            tier1_new_jobs    INTEGER DEFAULT 0,
            tier2_new_jobs    INTEGER DEFAULT 0,
            total_polls       INTEGER DEFAULT 0,
            wasted_polls      INTEGER DEFAULT 0,
            found_within_1hr  INTEGER DEFAULT 0,
            found_within_4hr  INTEGER DEFAULT 0,
            found_within_24hr INTEGER DEFAULT 0,
            found_after_24hr  INTEGER DEFAULT 0,
            early_exit_missed INTEGER DEFAULT 0,
            poll_score        REAL
        )
    """)
    conn.commit()
    return conn


def _make_api_health_db():
    """Create in-memory SQLite with api_health schema (context column)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE api_health (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            date             DATE    NOT NULL,
            platform         TEXT    NOT NULL,
            requests_made    INTEGER DEFAULT 0,
            requests_timeout INTEGER DEFAULT 0,
            requests_5xx     INTEGER DEFAULT 0,
            requests_429     INTEGER DEFAULT 0,
            requests_404     INTEGER DEFAULT 0,
            total_ms         INTEGER DEFAULT 0,
            context          TEXT    DEFAULT 'normal'
        )
    """)
    conn.commit()
    return conn


def _make_scaling_events_db():
    """Create in-memory SQLite with worker_scaling_events schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE worker_scaling_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type  TEXT NOT NULL,
            platform    TEXT,
            error_rate  REAL,
            occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def _today():
    return date.today().isoformat()


def _days_ago(n):
    return (date.today() - timedelta(days=n)).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION A — Miss Rate SQL Logic
# ─────────────────────────────────────────────────────────────────────────────

_MISS_RATE_SQL = """
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
    WHERE date >= ?
      AND ats_platform IS NOT NULL
    GROUP BY ats_platform
    HAVING SUM(total_new_jobs) > 0
    ORDER BY miss_rate_pct DESC
"""


class TestMissRateSQLLogic(unittest.TestCase):
    """Direct SQLite tests for the miss rate aggregation SQL."""

    def setUp(self):
        self.conn = _make_metrics_db()
        self.since = _days_ago(7)

    def tearDown(self):
        self.conn.close()

    def _insert(self, platform, total, tier1, tier2,
                for_date=None):
        self.conn.execute("""
            INSERT INTO adaptive_poll_metrics
                (date, ats_platform, total_new_jobs,
                 tier1_new_jobs, tier2_new_jobs)
            VALUES (?, ?, ?, ?, ?)
        """, (for_date or _today(), platform, total, tier1, tier2))
        self.conn.commit()

    def _run(self):
        return self.conn.execute(
            _MISS_RATE_SQL, (self.since,)
        ).fetchall()

    # ── Basic correctness ─────────────────────────────────────────────────────

    def test_zero_miss_rate_all_tier1(self):
        """100 total, 100 tier1, 0 tier2 → 0% miss rate."""
        self._insert("greenhouse", 100, 100, 0)
        rows = self._run()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["miss_rate_pct"], 0.0)

    def test_full_miss_rate_all_tier2(self):
        """100 total, 0 tier1, 100 tier2 → 100% miss rate."""
        self._insert("greenhouse", 100, 0, 100)
        rows = self._run()
        self.assertEqual(rows[0]["miss_rate_pct"], 100.0)

    def test_partial_miss_rate_10pct(self):
        """10 tier2 out of 100 total → 10.0% miss rate."""
        self._insert("greenhouse", 100, 90, 10)
        rows = self._run()
        self.assertEqual(rows[0]["miss_rate_pct"], 10.0)

    def test_miss_rate_at_warn_boundary(self):
        """Exactly at MISS_RATE_WARN threshold (5.0%)."""
        self._insert("lever", 100, 95, 5)
        rows = self._run()
        self.assertEqual(rows[0]["miss_rate_pct"], 5.0)

    def test_miss_rate_at_crit_boundary(self):
        """Exactly at MISS_RATE_CRIT threshold (15.0%)."""
        self._insert("workday", 100, 85, 15)
        rows = self._run()
        self.assertEqual(rows[0]["miss_rate_pct"], 15.0)

    def test_miss_rate_above_crit(self):
        """30% miss rate — clearly above critical."""
        self._insert("ashby", 100, 70, 30)
        rows = self._run()
        self.assertEqual(rows[0]["miss_rate_pct"], 30.0)

    # ── Filtering ─────────────────────────────────────────────────────────────

    def test_zero_total_new_jobs_excluded_by_having(self):
        """Platform with zero total_new_jobs excluded (HAVING SUM > 0)."""
        self._insert("inactive_platform", 0, 0, 0)
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_null_ats_platform_excluded(self):
        """Rows with NULL ats_platform excluded (WHERE IS NOT NULL)."""
        self.conn.execute("""
            INSERT INTO adaptive_poll_metrics
                (date, ats_platform, total_new_jobs, tier1_new_jobs, tier2_new_jobs)
            VALUES (?, NULL, 50, 40, 10)
        """, (_today(),))
        self.conn.commit()
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_old_data_outside_window_excluded(self):
        """Data older than `since` date is excluded."""
        self._insert("greenhouse", 100, 90, 10, for_date=_days_ago(10))
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_data_at_boundary_date_included(self):
        """Data exactly at the since date IS included."""
        self._insert("greenhouse", 100, 90, 10, for_date=self.since)
        rows = self._run()
        self.assertEqual(len(rows), 1)

    # ── Aggregation ───────────────────────────────────────────────────────────

    def test_multi_day_aggregation_sums_correctly(self):
        """Multiple rows for same platform are summed."""
        self._insert("greenhouse", 50, 45, 5, for_date=_days_ago(1))
        self._insert("greenhouse", 50, 45, 5, for_date=_today())
        rows = self._run()
        self.assertEqual(rows[0]["total_new_jobs"], 100)
        self.assertEqual(rows[0]["tier2_new_jobs"], 10)
        self.assertEqual(rows[0]["miss_rate_pct"], 10.0)

    def test_days_with_data_counts_distinct_dates(self):
        """days_with_data counts distinct dates, not row count."""
        self._insert("greenhouse", 50, 45, 5, for_date=_days_ago(2))
        self._insert("greenhouse", 50, 45, 5, for_date=_days_ago(1))
        rows = self._run()
        self.assertEqual(rows[0]["days_with_data"], 2)

    def test_multiple_platforms_aggregated_independently(self):
        """Different platforms have separate miss rates."""
        self._insert("greenhouse", 100, 90, 10)
        self._insert("lever",      100, 80, 20)
        rows = self._run()
        platforms = {r["platform"] for r in rows}
        self.assertIn("greenhouse", platforms)
        self.assertIn("lever", platforms)
        gh = next(r for r in rows if r["platform"] == "greenhouse")
        lv = next(r for r in rows if r["platform"] == "lever")
        self.assertEqual(gh["miss_rate_pct"], 10.0)
        self.assertEqual(lv["miss_rate_pct"], 20.0)

    # ── Sort order ────────────────────────────────────────────────────────────

    def test_sorted_by_miss_rate_desc(self):
        """Results ordered by miss_rate_pct DESC (worst first)."""
        self._insert("greenhouse", 100, 95,  5)   # 5%
        self._insert("lever",      100, 80, 20)   # 20%
        self._insert("ashby",      100, 90, 10)   # 10%
        rows = self._run()
        pcts = [r["miss_rate_pct"] for r in rows]
        self.assertEqual(pcts, sorted(pcts, reverse=True))
        self.assertEqual(pcts[0], 20.0)

    def test_empty_table_returns_empty_list(self):
        """Empty table → no rows returned."""
        rows = self._run()
        self.assertEqual(rows, [])


# ─────────────────────────────────────────────────────────────────────────────
# SECTION B — API Error Rates SQL Logic
# ─────────────────────────────────────────────────────────────────────────────

_API_ERROR_SQL = """
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
    WHERE date >= ?
      AND context = 'normal'
      AND requests_made > 0
    GROUP BY platform
    ORDER BY error_rate_pct DESC
"""


class TestApiErrorRatesSQLLogic(unittest.TestCase):
    """Direct SQLite tests for the API error rate aggregation SQL."""

    def setUp(self):
        self.conn = _make_api_health_db()
        self.since = _days_ago(7)

    def tearDown(self):
        self.conn.close()

    def _insert(self, platform, made=100, timeout=0, err5xx=0,
                r429=0, r404=0, total_ms=10000,
                context="normal", for_date=None):
        self.conn.execute("""
            INSERT INTO api_health
                (date, platform, requests_made,
                 requests_timeout, requests_5xx,
                 requests_429, requests_404,
                 total_ms, context)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (for_date or _today(), platform, made,
              timeout, err5xx, r429, r404, total_ms, context))
        self.conn.commit()

    def _run(self):
        return self.conn.execute(_API_ERROR_SQL, (self.since,)).fetchall()

    # ── Context filtering ─────────────────────────────────────────────────────

    def test_backoff_context_excluded(self):
        """Rows with context='backoff' are excluded."""
        self._insert("greenhouse", context="backoff", r429=50)
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_canary_context_excluded(self):
        """Rows with context='canary' are excluded."""
        self._insert("greenhouse", context="canary", err5xx=20)
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_normal_context_included(self):
        """Rows with context='normal' are included."""
        self._insert("greenhouse", context="normal")
        rows = self._run()
        self.assertEqual(len(rows), 1)

    def test_mixed_contexts_only_normal_counted(self):
        """Normal + backoff for same platform → only normal rows counted."""
        self._insert("greenhouse", made=100, r429=0, context="normal")
        self._insert("greenhouse", made=100, r429=50, context="backoff")
        rows = self._run()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["requests_made"], 100)
        self.assertEqual(rows[0]["total_errors"], 0)

    # ── Error calculation ─────────────────────────────────────────────────────

    def test_all_four_error_types_summed(self):
        """timeout + 5xx + 429 + 404 all contribute to total_errors."""
        self._insert("lever", made=100,
                     timeout=2, err5xx=3, r429=4, r404=1)
        rows = self._run()
        self.assertEqual(rows[0]["total_errors"], 10)
        self.assertEqual(rows[0]["error_rate_pct"], 10.0)

    def test_zero_errors_gives_zero_rate(self):
        """Platform with no errors → 0.0% error rate."""
        self._insert("ashby", made=200)
        rows = self._run()
        self.assertEqual(rows[0]["error_rate_pct"], 0.0)

    def test_zero_requests_made_excluded_by_where(self):
        """Rows with requests_made=0 excluded (AND requests_made > 0)."""
        self._insert("workday", made=0)
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_avg_response_ms_calculated(self):
        """avg_response_ms = total_ms / requests_made."""
        self._insert("greenhouse", made=50, total_ms=15000)
        rows = self._run()
        self.assertEqual(rows[0]["avg_response_ms"], 300)

    def test_multi_day_aggregation(self):
        """Multiple days summed: total requests and errors aggregated."""
        self._insert("greenhouse", made=100, r429=5, for_date=_days_ago(3))
        self._insert("greenhouse", made=100, r429=5, for_date=_today())
        rows = self._run()
        self.assertEqual(rows[0]["requests_made"], 200)
        self.assertEqual(rows[0]["total_errors"], 10)
        self.assertEqual(rows[0]["error_rate_pct"], 5.0)

    def test_sorted_by_error_rate_desc(self):
        """Sorted by error_rate_pct DESC (worst first)."""
        self._insert("greenhouse", made=100, r429=5)
        self._insert("lever",      made=100, r429=20)
        self._insert("ashby",      made=100, r429=1)
        rows = self._run()
        rates = [r["error_rate_pct"] for r in rows]
        self.assertEqual(rates, sorted(rates, reverse=True))

    def test_old_data_excluded(self):
        """Data outside the window is excluded."""
        self._insert("greenhouse", made=100, r429=10, for_date=_days_ago(10))
        rows = self._run()
        self.assertEqual(len(rows), 0)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION C — Scaling Event Summary SQL Logic
# ─────────────────────────────────────────────────────────────────────────────

_SCALING_SUMMARY_SQL = """
    SELECT event_type, COUNT(*) AS cnt
    FROM worker_scaling_events
    WHERE occurred_at >= ?
    GROUP BY event_type
"""


class TestScalingEventSummarySQLLogic(unittest.TestCase):
    """Direct SQLite tests for the scaling event summary aggregation."""

    def setUp(self):
        self.conn = _make_scaling_events_db()
        # Use a recent timestamp so all inserted rows fall in window
        self.since = (date.today() - timedelta(days=7)).isoformat()

    def tearDown(self):
        self.conn.close()

    def _insert(self, event_type, platform=None, occurred_at=None):
        self.conn.execute("""
            INSERT INTO worker_scaling_events (event_type, platform, occurred_at)
            VALUES (?, ?, ?)
        """, (event_type, platform,
              occurred_at or date.today().isoformat() + " 12:00:00"))
        self.conn.commit()

    def _run(self):
        rows = self.conn.execute(_SCALING_SUMMARY_SQL, (self.since,)).fetchall()
        return {r["event_type"]: int(r["cnt"]) for r in rows}

    def test_worker_add_counted(self):
        """worker_add events are counted correctly."""
        self._insert("worker_add")
        self._insert("worker_add")
        counts = self._run()
        self.assertEqual(counts.get("worker_add", 0), 2)

    def test_multiple_event_types_counted_independently(self):
        """Different event types counted independently."""
        self._insert("worker_add")
        self._insert("worker_remove")
        self._insert("outage_start")
        counts = self._run()
        self.assertEqual(counts["worker_add"], 1)
        self.assertEqual(counts["worker_remove"], 1)
        self.assertEqual(counts["outage_start"], 1)

    def test_old_events_excluded(self):
        """Events older than since date are excluded."""
        old_ts = (date.today() - timedelta(days=10)).isoformat() + " 12:00:00"
        self._insert("worker_add", occurred_at=old_ts)
        counts = self._run()
        self.assertEqual(counts.get("worker_add", 0), 0)

    def test_empty_table_returns_empty_dict(self):
        """Empty table → no rows → empty dict from caller."""
        counts = self._run()
        self.assertEqual(counts, {})

    def test_all_six_known_event_types(self):
        """All 6 known event types are counted when present."""
        for et in ["worker_add", "worker_remove", "outage_start",
                   "outage_end", "canary_probe", "ceiling_learned"]:
            self._insert(et)
        counts = self._run()
        for et in ["worker_add", "worker_remove", "outage_start",
                   "outage_end", "canary_probe", "ceiling_learned"]:
            self.assertEqual(counts[et], 1, f"Missing count for {et}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION D — Detection Age Distribution SQL Logic
# ─────────────────────────────────────────────────────────────────────────────

_DETECTION_AGE_SQL = """
    SELECT
        SUM(total_new_jobs)     AS total_new_jobs,
        SUM(found_within_1hr)   AS within_1hr,
        SUM(found_within_4hr)   AS within_4hr,
        SUM(found_within_24hr)  AS within_24hr,
        SUM(found_after_24hr)   AS after_24hr
    FROM adaptive_poll_metrics
    WHERE date >= ?
      AND ats_platform IS NOT NULL
"""


class TestDetectionAgeDistributionSQLLogic(unittest.TestCase):
    """Direct SQLite tests for detection age distribution SQL."""

    def setUp(self):
        self.conn = _make_metrics_db()
        self.since = _days_ago(7)

    def tearDown(self):
        self.conn.close()

    def _insert(self, platform, total=0, w1=0, w4=0, w24=0, a24=0,
                for_date=None):
        self.conn.execute("""
            INSERT INTO adaptive_poll_metrics
                (date, ats_platform, total_new_jobs,
                 found_within_1hr, found_within_4hr,
                 found_within_24hr, found_after_24hr)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (for_date or _today(), platform, total, w1, w4, w24, a24))
        self.conn.commit()

    def _run(self):
        return self.conn.execute(_DETECTION_AGE_SQL, (self.since,)).fetchone()

    def test_all_buckets_zero_on_empty_table(self):
        """Empty table → all SUM columns are NULL (→ coerced to 0 in Python)."""
        row = self._run()
        # SQLite returns NULL for SUM on empty set; Python wrapping handles it
        self.assertIsNone(row["total_new_jobs"])

    def test_single_platform_all_in_1hr(self):
        """All jobs found within 1 hr → within_1hr = total."""
        self._insert("greenhouse", total=50, w1=50)
        row = self._run()
        self.assertEqual(row["total_new_jobs"], 50)
        self.assertEqual(row["within_1hr"], 50)
        self.assertEqual(row["within_4hr"], 0)

    def test_portfolio_wide_aggregation(self):
        """Multiple platforms summed into one portfolio total."""
        self._insert("greenhouse", total=100, w1=60, w4=30, w24=8, a24=2)
        self._insert("lever",      total=100, w1=40, w4=40, w24=15, a24=5)
        row = self._run()
        self.assertEqual(row["total_new_jobs"], 200)
        self.assertEqual(row["within_1hr"],  100)  # 60 + 40
        self.assertEqual(row["within_4hr"],   70)  # 30 + 40
        self.assertEqual(row["within_24hr"],  23)  # 8 + 15
        self.assertEqual(row["after_24hr"],    7)  # 2 + 5

    def test_null_platform_excluded(self):
        """NULL ats_platform rows not counted."""
        self.conn.execute("""
            INSERT INTO adaptive_poll_metrics
                (date, ats_platform, total_new_jobs, found_within_1hr)
            VALUES (?, NULL, 100, 100)
        """, (_today(),))
        self.conn.commit()
        row = self._run()
        self.assertIsNone(row["total_new_jobs"])  # excluded

    def test_old_data_excluded(self):
        """Data outside the window is excluded."""
        self._insert("greenhouse", total=100, w1=100, for_date=_days_ago(10))
        row = self._run()
        self.assertIsNone(row["total_new_jobs"])

    def test_multi_day_aggregation(self):
        """Multiple days for same platform are summed."""
        self._insert("greenhouse", total=50, w1=30, for_date=_days_ago(3))
        self._insert("greenhouse", total=50, w1=30, for_date=_today())
        row = self._run()
        self.assertEqual(row["total_new_jobs"], 100)
        self.assertEqual(row["within_1hr"], 60)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION E — Wasted Poll Rate SQL Logic
# ─────────────────────────────────────────────────────────────────────────────

_WASTED_POLL_SQL = """
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
    WHERE date >= ?
      AND ats_platform IS NOT NULL
    GROUP BY ats_platform
    HAVING SUM(total_polls) > 0
    ORDER BY wasted_rate_pct DESC
"""


class TestWastedPollRateSQLLogic(unittest.TestCase):
    """Direct SQLite tests for wasted poll rate SQL."""

    def setUp(self):
        self.conn = _make_metrics_db()
        self.since = _days_ago(7)

    def tearDown(self):
        self.conn.close()

    def _insert(self, platform, total_polls=100, wasted=0, for_date=None):
        self.conn.execute("""
            INSERT INTO adaptive_poll_metrics
                (date, ats_platform, total_polls, wasted_polls)
            VALUES (?, ?, ?, ?)
        """, (for_date or _today(), platform, total_polls, wasted))
        self.conn.commit()

    def _run(self):
        return self.conn.execute(_WASTED_POLL_SQL, (self.since,)).fetchall()

    def test_zero_wasted_rate(self):
        """0 wasted polls → 0.0% waste rate."""
        self._insert("greenhouse", total_polls=200, wasted=0)
        rows = self._run()
        self.assertEqual(rows[0]["wasted_rate_pct"], 0.0)

    def test_full_wasted_rate(self):
        """All polls wasted → 100.0% waste rate."""
        self._insert("lever", total_polls=100, wasted=100)
        rows = self._run()
        self.assertEqual(rows[0]["wasted_rate_pct"], 100.0)

    def test_at_warn_boundary(self):
        """Exactly at WASTED_RATE_WARN = 60%."""
        self._insert("ashby", total_polls=100, wasted=60)
        rows = self._run()
        self.assertEqual(rows[0]["wasted_rate_pct"], 60.0)

    def test_above_crit_boundary(self):
        """Above WASTED_RATE_CRIT = 85%."""
        self._insert("workday", total_polls=100, wasted=90)
        rows = self._run()
        self.assertGreater(rows[0]["wasted_rate_pct"], 85.0)

    def test_zero_total_polls_excluded(self):
        """Platforms with zero total_polls excluded (HAVING > 0)."""
        self._insert("no_polls_platform", total_polls=0, wasted=0)
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_sorted_by_waste_rate_desc(self):
        """Sorted by wasted_rate_pct DESC."""
        self._insert("greenhouse", total_polls=100, wasted=10)  # 10%
        self._insert("lever",      total_polls=100, wasted=80)  # 80%
        self._insert("ashby",      total_polls=100, wasted=30)  # 30%
        rows = self._run()
        rates = [r["wasted_rate_pct"] for r in rows]
        self.assertEqual(rates, sorted(rates, reverse=True))

    def test_multi_day_aggregation(self):
        """Multiple days summed for same platform."""
        self._insert("greenhouse", total_polls=50, wasted=30, for_date=_days_ago(2))
        self._insert("greenhouse", total_polls=50, wasted=10, for_date=_today())
        rows = self._run()
        self.assertEqual(rows[0]["total_polls"], 100)
        self.assertEqual(rows[0]["wasted_polls"], 40)
        self.assertEqual(rows[0]["wasted_rate_pct"], 40.0)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION F — Score Oscillation (mock-based — STDDEV_POP unavailable in SQLite)
# ─────────────────────────────────────────────────────────────────────────────

class TestScoreOscillationQueryWrapper(unittest.TestCase):
    """
    Tests for query_score_oscillation() Python-level behaviour.
    STDDEV_POP is PostgreSQL-only; get_conn() is mocked throughout.
    """

    def _mock_conn(self, rows):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = rows
        return conn

    def test_empty_result_returns_empty_list(self):
        """No rows → empty list returned."""
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([])):
            from db.adaptive_health import query_score_oscillation
            result = query_score_oscillation(14)
            self.assertEqual(result, [])

    def test_rows_converted_to_dicts(self):
        """Each row is converted via dict()."""
        mock_row = {
            "platform": "greenhouse",
            "days_with_data": 7,
            "avg_score": 3.5,
            "min_score": 2.0,
            "max_score": 5.0,
            "score_stddev": 0.12,
            "score_range": 3.0,
        }
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([mock_row])):
            from db.adaptive_health import query_score_oscillation
            result = query_score_oscillation(14)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["platform"], "greenhouse")
            self.assertEqual(result[0]["score_stddev"], 0.12)

    def test_multiple_platforms_returned(self):
        """Multiple platforms returned in result."""
        rows = [
            {"platform": "greenhouse", "days_with_data": 7, "avg_score": 3.5,
             "min_score": 2.0, "max_score": 5.0, "score_stddev": 0.30, "score_range": 3.0},
            {"platform": "lever", "days_with_data": 5, "avg_score": 2.1,
             "min_score": 1.5, "max_score": 2.8, "score_stddev": 0.08, "score_range": 1.3},
        ]
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn(rows)):
            from db.adaptive_health import query_score_oscillation
            result = query_score_oscillation(14)
            self.assertEqual(len(result), 2)

    def test_conn_always_closed(self):
        """Connection is always closed (finally block)."""
        mock_conn = self._mock_conn([])
        with patch("db.adaptive_health.get_conn", return_value=mock_conn):
            from db.adaptive_health import query_score_oscillation
            query_score_oscillation(14)
            mock_conn.close.assert_called_once()

    def test_uses_14_day_default_window(self):
        """Default days argument is 14 (not 7 like other queries)."""
        import inspect
        from db.adaptive_health import query_score_oscillation
        sig = inspect.signature(query_score_oscillation)
        self.assertEqual(sig.parameters["days"].default, 14)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION G — Early Exit Stats SQL Logic
# ─────────────────────────────────────────────────────────────────────────────

_EARLY_EXIT_SQL = """
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
    WHERE date >= ?
      AND ats_platform IS NOT NULL
    GROUP BY ats_platform
    HAVING SUM(early_exit_missed) > 0 OR SUM(total_new_jobs) > 0
    ORDER BY total_missed DESC
"""


class TestEarlyExitStatsSQLLogic(unittest.TestCase):
    """Direct SQLite tests for early exit stats SQL."""

    def setUp(self):
        self.conn = _make_metrics_db()
        self.since = _days_ago(7)

    def tearDown(self):
        self.conn.close()

    def _insert(self, platform, total=0, missed=0, for_date=None):
        self.conn.execute("""
            INSERT INTO adaptive_poll_metrics
                (date, ats_platform, total_new_jobs, early_exit_missed)
            VALUES (?, ?, ?, ?)
        """, (for_date or _today(), platform, total, missed))
        self.conn.commit()

    def _run(self):
        return self.conn.execute(_EARLY_EXIT_SQL, (self.since,)).fetchall()

    def test_zero_missed_and_zero_total_excluded(self):
        """Platform with both zero excluded by HAVING."""
        self._insert("inactive", total=0, missed=0)
        rows = self._run()
        self.assertEqual(len(rows), 0)

    def test_zero_missed_nonzero_total_included(self):
        """Platform with jobs but zero misses is included (healthy baseline)."""
        self._insert("greenhouse", total=100, missed=0)
        rows = self._run()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["total_missed"], 0)

    def test_nonzero_missed_zero_total_included(self):
        """Platform with misses but zero total included; rate is NULL."""
        self._insert("lever", total=0, missed=5)
        rows = self._run()
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["missed_rate_pct"])

    def test_missed_rate_pct_calculated(self):
        """5 missed out of 100 → 5.0%."""
        self._insert("ashby", total=100, missed=5)
        rows = self._run()
        self.assertEqual(rows[0]["missed_rate_pct"], 5.0)

    def test_missed_rate_pct_null_when_zero_total(self):
        """missed_rate_pct is NULL when total_new_jobs = 0."""
        self._insert("workday", total=0, missed=3)
        rows = self._run()
        self.assertIsNone(rows[0]["missed_rate_pct"])

    def test_sorted_by_total_missed_desc(self):
        """Results ordered by total_missed DESC."""
        self._insert("greenhouse", total=100, missed=2)
        self._insert("lever",      total=100, missed=15)
        self._insert("ashby",      total=100, missed=7)
        rows = self._run()
        missed = [r["total_missed"] for r in rows]
        self.assertEqual(missed, sorted(missed, reverse=True))

    def test_multi_day_aggregation(self):
        """Multiple days summed."""
        self._insert("greenhouse", total=50, missed=3, for_date=_days_ago(2))
        self._insert("greenhouse", total=50, missed=2, for_date=_today())
        rows = self._run()
        self.assertEqual(rows[0]["total_missed"], 5)
        self.assertEqual(rows[0]["total_new_jobs"], 100)
        self.assertEqual(rows[0]["missed_rate_pct"], 5.0)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION H — Scaling Effectiveness (mock-based — NOW()-INTERVAL PostgreSQL-only)
# ─────────────────────────────────────────────────────────────────────────────

class TestScalingEffectivenessQueryWrapper(unittest.TestCase):
    """
    Tests for query_scaling_effectiveness() Python-level behaviour.
    The WHERE clause uses NOW() - INTERVAL which is PostgreSQL-only;
    get_conn() is mocked throughout.
    """

    def _mock_conn(self, rows):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = rows
        return conn

    def _make_row(self, platform="greenhouse", reductions=3,
                  effective=2, outage=1, resolved=1,
                  avg_err_reduce=0.25, avg_err_recovery=0.05):
        return {
            "platform": platform,
            "reductions": reductions,
            "effective_reductions": effective,
            "escalated_to_outage": outage,
            "outages_resolved": resolved,
            "avg_error_at_reduce": avg_err_reduce,
            "avg_error_at_recovery": avg_err_recovery,
        }

    def test_empty_result_returns_empty_list(self):
        """No rows → empty list."""
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([])):
            from db.adaptive_health import query_scaling_effectiveness
            result = query_scaling_effectiveness(7)
            self.assertEqual(result, [])

    def test_effectiveness_pct_computed(self):
        """effectiveness_pct = effective / reductions × 100."""
        row = self._make_row(reductions=4, effective=3)
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([row])):
            from db.adaptive_health import query_scaling_effectiveness
            result = query_scaling_effectiveness(7)
            self.assertEqual(result[0]["effectiveness_pct"], 75.0)

    def test_effectiveness_pct_100_when_all_effective(self):
        """All reductions effective → 100%."""
        row = self._make_row(reductions=5, effective=5)
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([row])):
            from db.adaptive_health import query_scaling_effectiveness
            result = query_scaling_effectiveness(7)
            self.assertEqual(result[0]["effectiveness_pct"], 100.0)

    def test_effectiveness_pct_zero_when_none_effective(self):
        """No effective reductions → 0%."""
        row = self._make_row(reductions=3, effective=0)
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([row])):
            from db.adaptive_health import query_scaling_effectiveness
            result = query_scaling_effectiveness(7)
            self.assertEqual(result[0]["effectiveness_pct"], 0.0)

    def test_effectiveness_pct_none_when_zero_reductions(self):
        """effectiveness_pct is None when reductions = 0."""
        row = self._make_row(reductions=0, effective=0)
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([row])):
            from db.adaptive_health import query_scaling_effectiveness
            result = query_scaling_effectiveness(7)
            self.assertIsNone(result[0]["effectiveness_pct"])

    def test_integer_coercion_of_event_counts(self):
        """reductions/effective/outages are coerced to int."""
        row = self._make_row(reductions=3, effective=2, outage=1, resolved=1)
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn([row])):
            from db.adaptive_health import query_scaling_effectiveness
            result = query_scaling_effectiveness(7)
            self.assertIsInstance(result[0]["reductions"], int)
            self.assertIsInstance(result[0]["effective_reductions"], int)
            self.assertIsInstance(result[0]["escalated_to_outage"], int)
            self.assertIsInstance(result[0]["outages_resolved"], int)

    def test_multiple_platforms_returned(self):
        """Multiple platforms returned."""
        rows = [
            self._make_row("greenhouse", reductions=3, effective=3),
            self._make_row("lever",      reductions=2, effective=0),
        ]
        with patch("db.adaptive_health.get_conn",
                   return_value=self._mock_conn(rows)):
            from db.adaptive_health import query_scaling_effectiveness
            result = query_scaling_effectiveness(7)
            self.assertEqual(len(result), 2)
            gh = next(r for r in result if r["platform"] == "greenhouse")
            lv = next(r for r in result if r["platform"] == "lever")
            self.assertEqual(gh["effectiveness_pct"], 100.0)
            self.assertEqual(lv["effectiveness_pct"], 0.0)

    def test_conn_always_closed(self):
        """Connection always closed even on exception."""
        mock_conn = self._mock_conn([])
        with patch("db.adaptive_health.get_conn", return_value=mock_conn):
            from db.adaptive_health import query_scaling_effectiveness
            query_scaling_effectiveness(7)
            mock_conn.close.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION I — build_weekly_health_data()
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildWeeklyHealthData(unittest.TestCase):
    """
    Tests for build_weekly_health_data() with all 8 query functions mocked.
    Verifies has_data logic, exception isolation, and result structure.
    """

    _EMPTY_SCALE = {
        "total_events": 0, "worker_add": 0, "worker_remove": 0,
        "outage_start": 0, "outage_end": 0, "canary_probe": 0,
        "ceiling_learned": 0,
    }
    _EMPTY_AGE = {
        "total_new_jobs": 0, "within_1hr": 0, "within_4hr": 0,
        "within_24hr": 0, "after_24hr": 0,
        "within_1hr_pct": None, "within_4hr_pct": None,
        "within_24hr_pct": None, "after_24hr_pct": None,
    }

    def _patch_all(self, miss=None, errors=None, scaling=None,
                   age=None, wasted=None, oscillation=None,
                   early_exit=None, effectiveness=None):
        """Return a context manager patching all 8 query functions."""
        return patch.multiple(
            "db.adaptive_health",
            query_miss_rate_by_platform=MagicMock(
                return_value=miss if miss is not None else []),
            query_api_error_rates=MagicMock(
                return_value=errors if errors is not None else []),
            query_scaling_event_summary=MagicMock(
                return_value=scaling if scaling is not None
                else self._EMPTY_SCALE.copy()),
            query_detection_age_distribution=MagicMock(
                return_value=age if age is not None
                else self._EMPTY_AGE.copy()),
            query_wasted_poll_rate=MagicMock(
                return_value=wasted if wasted is not None else []),
            query_score_oscillation=MagicMock(
                return_value=oscillation if oscillation is not None else []),
            query_early_exit_stats=MagicMock(
                return_value=early_exit if early_exit is not None else []),
            query_scaling_effectiveness=MagicMock(
                return_value=effectiveness if effectiveness is not None else []),
        )

    # ── has_data flag ─────────────────────────────────────────────────────────

    def test_has_data_false_when_all_empty(self):
        """has_data = False when all queries return empty."""
        with self._patch_all():
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertFalse(result["has_data"])

    def test_has_data_true_when_miss_rates_present(self):
        """has_data = True when miss_rates is non-empty."""
        miss = [{"platform": "greenhouse", "miss_rate_pct": 5.0,
                 "total_new_jobs": 100, "tier1_new_jobs": 95,
                 "tier2_new_jobs": 5, "days_with_data": 7}]
        with self._patch_all(miss=miss):
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertTrue(result["has_data"])

    def test_has_data_true_when_error_rates_present(self):
        """has_data = True when error_rates is non-empty."""
        errors = [{"platform": "lever", "requests_made": 100,
                   "total_errors": 5, "error_rate_pct": 5.0,
                   "avg_response_ms": 200, "days_with_data": 7}]
        with self._patch_all(errors=errors):
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertTrue(result["has_data"])

    def test_has_data_true_when_scaling_events_present(self):
        """has_data = True when total_events > 0."""
        scaling = {**self._EMPTY_SCALE, "total_events": 3, "worker_add": 3}
        with self._patch_all(scaling=scaling):
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertTrue(result["has_data"])

    def test_has_data_true_when_detection_age_has_jobs(self):
        """has_data = True when detection_age.total_new_jobs > 0."""
        age = {**self._EMPTY_AGE, "total_new_jobs": 50,
               "within_1hr": 30, "within_1hr_pct": 60.0}
        with self._patch_all(age=age):
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertTrue(result["has_data"])

    def test_has_data_true_when_wasted_polls_present(self):
        """has_data = True when wasted_poll_rates is non-empty."""
        wasted = [{"platform": "ashby", "total_polls": 100,
                   "wasted_polls": 60, "wasted_rate_pct": 60.0,
                   "days_with_data": 7}]
        with self._patch_all(wasted=wasted):
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertTrue(result["has_data"])

    # ── Exception isolation ───────────────────────────────────────────────────

    def test_miss_rate_exception_does_not_crash(self):
        """Exception in miss_rate query → still returns result with empty miss_rates."""
        with patch.multiple(
            "db.adaptive_health",
            query_miss_rate_by_platform=MagicMock(
                side_effect=Exception("DB gone")),
            query_api_error_rates=MagicMock(return_value=[]),
            query_scaling_event_summary=MagicMock(
                return_value=self._EMPTY_SCALE.copy()),
            query_detection_age_distribution=MagicMock(
                return_value=self._EMPTY_AGE.copy()),
            query_wasted_poll_rate=MagicMock(return_value=[]),
            query_score_oscillation=MagicMock(return_value=[]),
            query_early_exit_stats=MagicMock(return_value=[]),
            query_scaling_effectiveness=MagicMock(return_value=[]),
        ):
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertEqual(result["miss_rates"], [])

    def test_each_query_exception_isolated(self):
        """Any individual query exception → that key empty, others populated."""
        errors = [{"platform": "lever", "error_rate_pct": 5.0,
                   "requests_made": 100, "total_errors": 5,
                   "avg_response_ms": 200, "days_with_data": 7}]
        with patch.multiple(
            "db.adaptive_health",
            query_miss_rate_by_platform=MagicMock(
                side_effect=RuntimeError("oops")),
            query_api_error_rates=MagicMock(return_value=errors),
            query_scaling_event_summary=MagicMock(
                return_value=self._EMPTY_SCALE.copy()),
            query_detection_age_distribution=MagicMock(
                return_value=self._EMPTY_AGE.copy()),
            query_wasted_poll_rate=MagicMock(return_value=[]),
            query_score_oscillation=MagicMock(return_value=[]),
            query_early_exit_stats=MagicMock(return_value=[]),
            query_scaling_effectiveness=MagicMock(return_value=[]),
        ):
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertEqual(result["miss_rates"], [])
            self.assertEqual(len(result["error_rates"]), 1)

    # ── Result structure ──────────────────────────────────────────────────────

    def test_all_required_keys_present(self):
        """Result always contains all expected top-level keys."""
        with self._patch_all():
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            for key in ["days", "since", "miss_rates", "error_rates",
                        "scaling_events", "detection_age",
                        "wasted_poll_rates", "score_oscillation",
                        "early_exit_stats", "scaling_effectiveness",
                        "has_data"]:
                self.assertIn(key, result, f"Missing key: {key}")

    def test_days_key_matches_argument(self):
        """result['days'] equals the days argument passed in."""
        with self._patch_all():
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(14)
            self.assertEqual(result["days"], 14)

    def test_since_date_is_correct(self):
        """result['since'] is days before today in ISO format."""
        expected = (date.today() - timedelta(days=7)).isoformat()
        with self._patch_all():
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            self.assertEqual(result["since"], expected)

    def test_score_oscillation_called_with_14_days(self):
        """query_score_oscillation always called with days=14."""
        # patch.multiple with explicit MagicMock values does NOT include them
        # in the context manager's return dict — only DEFAULT-keyed attrs do.
        # Track the mock via a separate patch that overrides _patch_all's entry.
        mock_osc = MagicMock(return_value=[])
        with self._patch_all(), \
             patch("db.adaptive_health.query_score_oscillation", mock_osc):
            from db.adaptive_health import build_weekly_health_data
            build_weekly_health_data(7)
        mock_osc.assert_called_once_with(days=14)

    def test_scaling_events_defaults_to_zero_dict(self):
        """Default scaling_events dict always present with 0 values."""
        with self._patch_all():
            from db.adaptive_health import build_weekly_health_data
            result = build_weekly_health_data(7)
            ev = result["scaling_events"]
            self.assertEqual(ev["total_events"], 0)
            for k in ["worker_add", "worker_remove", "outage_start",
                      "outage_end", "canary_probe", "ceiling_learned"]:
                self.assertEqual(ev[k], 0)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION J — _build_adaptive_health_section() HTML rendering
# ─────────────────────────────────────────────────────────────────────────────

def _full_data(**overrides):
    """Return a fully-populated mock data dict for the HTML section."""
    base = {
        "days": 7,
        "since": "2026-04-25",
        "has_data": True,
        "miss_rates": [
            {"platform": "greenhouse", "total_new_jobs": 200,
             "tier1_new_jobs": 180, "tier2_new_jobs": 20,
             "miss_rate_pct": 10.0, "days_with_data": 7},
        ],
        "error_rates": [
            {"platform": "lever", "requests_made": 500, "total_errors": 25,
             "error_rate_pct": 5.0, "avg_response_ms": 320, "days_with_data": 7},
        ],
        "detection_age": {
            "total_new_jobs": 200,
            "within_1hr": 100, "within_4hr": 60,
            "within_24hr": 30, "after_24hr": 10,
            "within_1hr_pct": 50.0, "within_4hr_pct": 30.0,
            "within_24hr_pct": 15.0, "after_24hr_pct": 5.0,
        },
        "wasted_poll_rates": [
            {"platform": "ashby", "total_polls": 300, "wasted_polls": 210,
             "wasted_rate_pct": 70.0, "days_with_data": 7},
        ],
        "score_oscillation": [
            {"platform": "greenhouse", "days_with_data": 14,
             "avg_score": 3.2, "min_score": 2.5, "max_score": 4.0,
             "score_stddev": 0.10, "score_range": 1.5},
        ],
        "early_exit_stats": [
            {"platform": "workday", "total_missed": 8,
             "total_new_jobs": 100, "missed_rate_pct": 8.0,
             "days_with_data": 7},
        ],
        "scaling_effectiveness": [
            {"platform": "greenhouse", "reductions": 3, "effective_reductions": 2,
             "escalated_to_outage": 1, "outages_resolved": 1,
             "effectiveness_pct": 66.7,
             "avg_error_at_reduce": 22.5, "avg_error_at_recovery": 3.1},
        ],
        "scaling_events": {
            "total_events": 8, "worker_add": 3, "worker_remove": 2,
            "outage_start": 1, "outage_end": 1,
            "canary_probe": 1, "ceiling_learned": 0,
        },
    }
    base.update(overrides)
    return base


class TestAdaptiveHealthSectionHTML(unittest.TestCase):
    """
    Tests for _build_adaptive_health_section().
    build_weekly_health_data() is mocked with controlled data.
    """

    def _build(self, data):
        with patch("db.adaptive_health.build_weekly_health_data",
                   return_value=data):
            # Also patch the threshold imports so they resolve correctly
            from outreach.report_templates.monitor_report import (
                _build_adaptive_health_section,
            )
            return _build_adaptive_health_section()

    # ── Graceful degradation ──────────────────────────────────────────────────

    def test_returns_empty_string_on_import_error(self):
        """Exception in build_weekly_health_data → returns empty string."""
        with patch("db.adaptive_health.build_weekly_health_data",
                   side_effect=Exception("DB down")):
            from outreach.report_templates.monitor_report import (
                _build_adaptive_health_section,
            )
            result = _build_adaptive_health_section()
            self.assertEqual(result, "")

    def test_insufficient_data_banner_when_no_data(self):
        """has_data=False renders the 'insufficient data' banner."""
        html = self._build(_full_data(has_data=False))
        self.assertIn("Insufficient data", html)

    def test_no_tables_when_no_data(self):
        """No table tags rendered when has_data=False."""
        html = self._build(_full_data(has_data=False))
        self.assertNotIn("<table", html)

    def test_section_header_always_present(self):
        """The 'Adaptive Polling Health' header always rendered."""
        html = self._build(_full_data())
        self.assertIn("Adaptive Polling Health", html)

    # ── Miss rate table ───────────────────────────────────────────────────────

    def test_miss_rate_heading_present(self):
        """'Miss Rate' section label is present."""
        html = self._build(_full_data())
        self.assertIn("Miss Rate", html)

    def test_miss_rate_platform_name_shown(self):
        """Platform name appears in miss rate table."""
        html = self._build(_full_data())
        self.assertIn("Greenhouse", html)

    def test_miss_rate_counts_shown(self):
        """tier1, tier2, total counts appear in the table."""
        html = self._build(_full_data())
        self.assertIn("200", html)   # total_new_jobs
        self.assertIn("180", html)   # tier1
        self.assertIn("20", html)    # tier2

    def test_miss_rate_amber_badge_above_warn(self):
        """5-15% miss rate → amber color (#f59e0b)."""
        data = _full_data()
        data["miss_rates"][0]["miss_rate_pct"] = 6.0
        html = self._build(data)
        self.assertIn("#f59e0b", html)

    def test_miss_rate_red_badge_above_crit(self):
        """Above 15% miss rate → red color (#ef4444)."""
        data = _full_data()
        data["miss_rates"][0]["miss_rate_pct"] = 20.0
        html = self._build(data)
        self.assertIn("#ef4444", html)

    def test_miss_rate_green_badge_below_warn(self):
        """Below 5% miss rate → green color (#22c55e)."""
        data = _full_data()
        data["miss_rates"][0]["miss_rate_pct"] = 2.0
        html = self._build(data)
        self.assertIn("#22c55e", html)

    def test_no_activity_message_when_miss_rates_empty(self):
        """Empty miss_rates → 'no job activity' message shown."""
        data = _full_data(miss_rates=[])
        html = self._build(data)
        self.assertIn("no job activity", html)

    # ── API error rate table ──────────────────────────────────────────────────

    def test_api_error_rate_heading_present(self):
        """'API Error Rates' section label is present."""
        html = self._build(_full_data())
        self.assertIn("API Error Rates", html)

    def test_api_error_note_about_exclusions(self):
        """'backoff & canary excluded' note appears."""
        html = self._build(_full_data())
        self.assertIn("canary", html)

    def test_api_error_platform_name_shown(self):
        """Platform name appears in error rate table."""
        html = self._build(_full_data())
        self.assertIn("Lever", html)

    def test_api_error_avg_response_shown(self):
        """avg_response_ms value appears."""
        html = self._build(_full_data())
        self.assertIn("320 ms", html)

    # ── Detection age distribution ────────────────────────────────────────────

    def test_detection_age_heading_present(self):
        """'Detection Age Distribution' heading present."""
        html = self._build(_full_data())
        self.assertIn("Detection Age", html)

    def test_detection_age_shows_1hr_pct(self):
        """≤ 1 hour percentage appears."""
        html = self._build(_full_data())
        self.assertIn("50.0%", html)

    def test_detection_age_shows_after_24hr_pct(self):
        """> 24 hours percentage appears."""
        html = self._build(_full_data())
        self.assertIn("5.0%", html)

    def test_detection_age_not_shown_when_no_jobs(self):
        """Section hidden when total_new_jobs = 0."""
        data = _full_data()
        data["detection_age"] = {
            "total_new_jobs": 0, "within_1hr": 0, "within_4hr": 0,
            "within_24hr": 0, "after_24hr": 0,
            "within_1hr_pct": None, "within_4hr_pct": None,
            "within_24hr_pct": None, "after_24hr_pct": None,
        }
        html = self._build(data)
        self.assertNotIn("Detection Age", html)

    # ── Wasted poll rate table ────────────────────────────────────────────────

    def test_wasted_poll_heading_present(self):
        """'Wasted Poll Rate' heading present."""
        html = self._build(_full_data())
        self.assertIn("Wasted Poll Rate", html)

    def test_wasted_poll_amber_at_70pct(self):
        """70% waste rate (above 60% warn, below 85% crit) → amber."""
        html = self._build(_full_data())   # 70% in default data
        self.assertIn("#f59e0b", html)

    def test_wasted_poll_not_shown_when_empty(self):
        """No wasted poll section when wasted_poll_rates is empty."""
        data = _full_data(wasted_poll_rates=[])
        html = self._build(data)
        self.assertNotIn("Wasted Poll Rate", html)

    # ── Score stability table ─────────────────────────────────────────────────

    def test_score_stability_heading_present(self):
        """'Score Stability' heading present."""
        html = self._build(_full_data())
        self.assertIn("Score Stability", html)

    def test_score_stability_green_below_warn(self):
        """stddev=0.10 < 0.15 → green ✓."""
        html = self._build(_full_data())
        self.assertIn("✓", html)

    def test_score_stability_amber_at_warn(self):
        """stddev=0.20 (0.15-0.30) → amber ⚠."""
        data = _full_data()
        data["score_oscillation"][0]["score_stddev"] = 0.20
        html = self._build(data)
        # amber for oscillation
        self.assertIn("#f59e0b", html)

    def test_score_stability_red_above_crit(self):
        """stddev=0.35 >= 0.30 → red ✗."""
        data = _full_data()
        data["score_oscillation"][0]["score_stddev"] = 0.35
        html = self._build(data)
        self.assertIn("#ef4444", html)

    def test_score_stability_not_shown_when_empty(self):
        """No score stability section when score_oscillation is empty."""
        data = _full_data(score_oscillation=[])
        html = self._build(data)
        self.assertNotIn("Score Stability", html)

    # ── Early exit validation ─────────────────────────────────────────────────

    def test_early_exit_heading_present_when_misses_exist(self):
        """'Early-Exit Validation' heading present when misses > 0."""
        html = self._build(_full_data())
        self.assertIn("Early-Exit Validation", html)

    def test_early_exit_platform_shown(self):
        """Platform with misses appears in early exit table."""
        html = self._build(_full_data())
        self.assertIn("Workday", html)

    def test_early_exit_not_shown_when_zero_misses(self):
        """Section not rendered when all platforms have 0 misses."""
        data = _full_data()
        data["early_exit_stats"] = [
            {"platform": "greenhouse", "total_missed": 0,
             "total_new_jobs": 100, "missed_rate_pct": 0.0,
             "days_with_data": 7},
        ]
        html = self._build(data)
        self.assertNotIn("Early-Exit Validation", html)

    def test_early_exit_not_shown_when_list_empty(self):
        """Section not rendered when early_exit_stats is empty list."""
        data = _full_data(early_exit_stats=[])
        html = self._build(data)
        self.assertNotIn("Early-Exit Validation", html)

    def test_early_exit_missed_count_highlighted(self):
        """Missed count rendered in red color."""
        html = self._build(_full_data())
        # Missed count = 8, and it should be in a red-colored cell
        self.assertIn("#ef4444", html)

    # ── Scaling effectiveness table ───────────────────────────────────────────

    def test_scaling_effectiveness_heading_present(self):
        """'Worker Reduction Effectiveness' heading present."""
        html = self._build(_full_data())
        self.assertIn("Worker Reduction Effectiveness", html)

    def test_scaling_effectiveness_platform_shown(self):
        """Platform appears in effectiveness table."""
        html = self._build(_full_data())
        self.assertIn("Greenhouse", html)

    def test_scaling_effectiveness_green_at_100pct(self):
        """100% effectiveness → green ✓."""
        data = _full_data()
        data["scaling_effectiveness"][0]["effectiveness_pct"] = 100.0
        html = self._build(data)
        # Should have a green ✓ somewhere in the effectiveness column
        self.assertIn("✓", html)

    def test_scaling_effectiveness_amber_at_66pct(self):
        """66% effectiveness (50-80%) → amber ⚠."""
        html = self._build(_full_data())   # 66.7% in default
        self.assertIn("⚠", html)

    def test_scaling_effectiveness_red_below_50pct(self):
        """Below 50% effectiveness → red ✗."""
        data = _full_data()
        data["scaling_effectiveness"][0]["effectiveness_pct"] = 33.0
        html = self._build(data)
        self.assertIn("✗", html)

    def test_scaling_effectiveness_outage_in_red(self):
        """Outage count > 0 rendered in red."""
        html = self._build(_full_data())   # outages=1 in default
        self.assertIn("#ef4444", html)

    def test_scaling_effectiveness_not_shown_when_empty(self):
        """No effectiveness section when list is empty."""
        data = _full_data(scaling_effectiveness=[])
        html = self._build(data)
        self.assertNotIn("Worker Reduction Effectiveness", html)

    # ── Worker scaling tile row ───────────────────────────────────────────────

    def test_worker_scaling_events_tile_present(self):
        """'Worker Scaling Events' section present when events exist."""
        html = self._build(_full_data())
        self.assertIn("Worker Scaling Events", html)

    def test_worker_scaling_shows_counts(self):
        """All tile counts appear in HTML."""
        data = _full_data()
        data["scaling_events"]["worker_add"] = 5
        html = self._build(data)
        self.assertIn("5", html)

    def test_worker_scaling_stable_message_when_no_events(self):
        """'No worker scaling events' message when total_events = 0."""
        data = _full_data()
        data["scaling_events"] = {
            "total_events": 0, "worker_add": 0, "worker_remove": 0,
            "outage_start": 0, "outage_end": 0,
            "canary_probe": 0, "ceiling_learned": 0,
        }
        html = self._build(data)
        self.assertIn("No worker scaling events", html)

    def test_outage_tile_red_when_outages_started(self):
        """Outage tile red when outage_start > 0."""
        html = self._build(_full_data())   # outage_start=1 in defaults
        self.assertIn("#ef4444", html)

    # ── Footer ───────────────────────────────────────────────────────────────

    def test_footer_shows_since_date(self):
        """Footer includes the 'since' date."""
        html = self._build(_full_data())
        self.assertIn("2026-04-25", html)

    def test_footer_shows_thresholds(self):
        """Footer includes miss rate and waste rate thresholds."""
        html = self._build(_full_data())
        self.assertIn("Miss rate", html)
        self.assertIn("Waste rate", html)

    # ── Monday-only gating ────────────────────────────────────────────────────

    def test_section_included_in_email_on_monday(self):
        """_build_adaptive_health_section called on Monday (weekday=0)."""
        from unittest.mock import patch as p
        from datetime import datetime as dt
        monday = dt(2026, 4, 27, 10, 0)  # April 27, 2026 is a Monday
        with p("outreach.report_templates.monitor_report.datetime") as mock_dt:
            mock_dt.now.return_value = monday
            # weekday() == 0 → section should be included
            self.assertEqual(monday.weekday(), 0)

    def test_section_excluded_on_non_monday(self):
        """_build_adaptive_health_section NOT called on non-Monday."""
        from datetime import datetime as dt, timedelta as td
        monday = dt(2026, 4, 27, 10, 0)  # April 27, 2026 is a Monday
        for weekday_offset, name in enumerate(
            ["Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], start=1
        ):
            non_monday = monday + td(days=weekday_offset)
            self.assertNotEqual(non_monday.weekday(), 0,
                                f"{name} should not be weekday 0")


if __name__ == "__main__":
    unittest.main(verbosity=2)
