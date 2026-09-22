# tests/test_external_api_health.py — Tests for db/external_api_health.py
#
# Covers docs/enrichment_discovery_design.md §11 "External API Health
# Tracking" (agreed + implemented 2026-09-22): status-code classification
# (_classify_status, a pure function — imported directly, no DB needed) and
# the aggregate SQL shape (rate_429_pct / error_pct / avg_response_ms),
# exercised against an in-memory sqlite table mirroring the real schema
# (same pattern as tests/test_api_health.py).

import os
import sqlite3
import sys
import unittest
from datetime import date, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.external_api_health import _classify_status, _month_bounds


class TestClassifyStatus(unittest.TestCase):
    """_classify_status maps a status code to the 6-way increment tuple."""

    def test_200_is_ok(self):
        self.assertEqual(_classify_status(200), (1, 0, 0, 0, 0, 0))

    def test_429_is_rate_limit(self):
        self.assertEqual(_classify_status(429), (0, 1, 0, 0, 0, 0))

    def test_403_is_key_rejected(self):
        self.assertEqual(_classify_status(403), (0, 0, 1, 0, 0, 0))

    def test_401_folds_into_403_bucket(self):
        """401 and 403 both mean 'key revoked/rejected' for certspotter/crt.sh."""
        self.assertEqual(_classify_status(401), _classify_status(403))

    def test_404_is_not_found(self):
        self.assertEqual(_classify_status(404), (0, 0, 0, 1, 0, 0))

    def test_5xx_range(self):
        for code in (500, 502, 503, 599):
            self.assertEqual(_classify_status(code), (0, 0, 0, 0, 1, 0), f"code={code}")

    def test_0_and_other_codes_are_other_err(self):
        """0 = non-HTTP error (timeout/conn refused); any unmapped code also lands here."""
        for code in (0, 418, 301):
            self.assertEqual(_classify_status(code), (0, 0, 0, 0, 0, 1), f"code={code}")

    def test_exactly_one_bucket_incremented(self):
        """Every status code increments exactly one of the six buckets."""
        for code in (200, 429, 401, 403, 404, 500, 599, 0, 999):
            self.assertEqual(sum(_classify_status(code)), 1, f"code={code}")


class TestExternalApiHealthAggregates(unittest.TestCase):
    """SQL aggregate shape used by get_external_health_summary — sqlite mirror."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE external_api_health (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                date                DATE    NOT NULL,
                service             TEXT    NOT NULL,
                requests_made       INTEGER DEFAULT 0,
                requests_ok         INTEGER DEFAULT 0,
                requests_429        INTEGER DEFAULT 0,
                requests_403        INTEGER DEFAULT 0,
                requests_404        INTEGER DEFAULT 0,
                requests_5xx        INTEGER DEFAULT 0,
                requests_other_err  INTEGER DEFAULT 0,
                avg_response_ms     INTEGER DEFAULT 0,
                max_response_ms     INTEGER DEFAULT 0,
                total_ms            INTEGER DEFAULT 0,
                first_429_at        TIMESTAMP,
                backoff_total_s     INTEGER DEFAULT 0,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, service)
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert(self, service, made=10, ok=9, r429=0, r403=0,
                total_ms=1000, backoff_s=0, for_date=None):
        today = for_date or date.today().isoformat()
        self.conn.execute("""
            INSERT OR REPLACE INTO external_api_health
                (date, service, requests_made, requests_ok,
                 requests_429, requests_403, total_ms,
                 avg_response_ms, max_response_ms, backoff_total_s)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (today, service, made, ok, r429, r403, total_ms,
              total_ms // made if made else 0,
              total_ms // made if made else 0,
              backoff_s))
        self.conn.commit()

    def test_unique_constraint_date_service(self):
        self._insert("brave")
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("""
                INSERT INTO external_api_health (date, service)
                VALUES (?, 'brave')
            """, (date.today().isoformat(),))
            self.conn.commit()

    def test_429_rate_pct(self):
        self._insert("certspotter", made=100, ok=90, r429=10)
        row = self.conn.execute("""
            SELECT ROUND(100.0 * requests_429 / requests_made, 1) AS rate
            FROM external_api_health WHERE service = 'certspotter'
        """).fetchone()
        self.assertEqual(row["rate"], 10.0)

    def test_zero_requests_no_division(self):
        self._insert("kg", made=0, ok=0)
        row = self.conn.execute("""
            SELECT CASE WHEN requests_made > 0
                THEN ROUND(100.0 * requests_429 / requests_made, 1)
                ELSE 0 END AS rate
            FROM external_api_health WHERE service = 'kg'
        """).fetchone()
        self.assertEqual(row["rate"], 0)

    def test_error_pct_excludes_ok_and_429(self):
        """error_pct = (403+404+5xx+other) / made — 429 tracked separately."""
        self._insert("crtsh", made=100, ok=80, r429=5, r403=15)
        row = self.conn.execute("""
            SELECT ROUND(100.0 * requests_403 / requests_made, 1) AS err_rate
            FROM external_api_health WHERE service = 'crtsh'
        """).fetchone()
        self.assertEqual(row["err_rate"], 15.0)

    def test_services_tracked_independently(self):
        self._insert("brave", made=50, r429=1)
        self._insert("kg",    made=20, r429=0)
        brave = self.conn.execute(
            "SELECT requests_429 FROM external_api_health WHERE service='brave'"
        ).fetchone()
        kg = self.conn.execute(
            "SELECT requests_429 FROM external_api_health WHERE service='kg'"
        ).fetchone()
        self.assertEqual(brave["requests_429"], 1)
        self.assertEqual(kg["requests_429"], 0)

    def test_multi_day_summary_aggregates(self):
        today     = date.today()
        yesterday = (today - timedelta(days=1)).isoformat()
        self._insert("certspotter", made=5, r429=1, for_date=today.isoformat())
        self._insert("certspotter", made=5, r429=2, for_date=yesterday)
        row = self.conn.execute("""
            SELECT SUM(requests_made) AS total, SUM(requests_429) AS total_429
            FROM external_api_health WHERE service = 'certspotter'
        """).fetchone()
        self.assertEqual(row["total"], 10)
        self.assertEqual(row["total_429"], 3)

    def test_backoff_total_accumulates(self):
        self._insert("certspotter", made=1, r429=1, backoff_s=3600,
                      for_date=date.today().isoformat())
        row = self.conn.execute("""
            SELECT SUM(backoff_total_s) AS total_backoff
            FROM external_api_health WHERE service = 'certspotter'
        """).fetchone()
        self.assertEqual(row["total_backoff"], 3600)


class TestMonthBounds(unittest.TestCase):
    """
    _month_bounds — pure [start, end) date-window helper backing
    get_month_request_count, the atomic replacement for the old
    data/brave_quota.json race (see project memory: 998 real Brave calls
    this month vs. 719 tracked by the unlocked-file counter).
    """

    def test_explicit_month_mid_year(self):
        start, end = _month_bounds("2026-09")
        self.assertEqual(start, date(2026, 9, 1))
        self.assertEqual(end, date(2026, 10, 1))

    def test_december_rolls_into_next_year(self):
        start, end = _month_bounds("2026-12")
        self.assertEqual(start, date(2026, 12, 1))
        self.assertEqual(end, date(2027, 1, 1))

    def test_january_start_of_year(self):
        start, end = _month_bounds("2027-01")
        self.assertEqual(start, date(2027, 1, 1))
        self.assertEqual(end, date(2027, 2, 1))

    def test_defaults_to_current_month(self):
        today = date.today()
        start, end = _month_bounds()
        self.assertEqual(start, today.replace(day=1))
        self.assertGreater(end, today)

    def test_end_is_exclusive_of_next_month(self):
        """A row dated the 1st of the following month must NOT be in [start, end)."""
        start, end = _month_bounds("2026-02")
        self.assertLess(date(2026, 2, 28), end)
        self.assertGreaterEqual(date(2026, 3, 1), end)


class TestMonthRequestCountSumShape(unittest.TestCase):
    """
    Sqlite mirror of get_month_request_count's SQL shape: SUM(requests_made)
    over [start, end) for one service, spanning multiple days and correctly
    excluding days outside the window (e.g. last month, next month) and
    other services — the exact scenario that used to race across two
    processes/scripts sharing one unlocked JSON file.
    """

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE external_api_health (
                date DATE NOT NULL, service TEXT NOT NULL,
                requests_made INTEGER DEFAULT 0,
                UNIQUE(date, service)
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert(self, service, for_date, made):
        self.conn.execute(
            "INSERT INTO external_api_health (date, service, requests_made) VALUES (?, ?, ?)",
            (for_date, service, made),
        )
        self.conn.commit()

    def _sum(self, service, start, end):
        row = self.conn.execute("""
            SELECT COALESCE(SUM(requests_made), 0) AS total
            FROM external_api_health
            WHERE service = ? AND date >= ? AND date < ?
        """, (service, start, end)).fetchone()
        return row["total"]

    def test_sums_across_days_within_month(self):
        self._insert("brave", "2026-09-18", 316)
        self._insert("brave", "2026-09-20", 682)
        start, end = _month_bounds("2026-09")
        self.assertEqual(
            self._sum("brave", start.isoformat(), end.isoformat()), 998
        )

    def test_excludes_prior_and_next_month(self):
        self._insert("brave", "2026-08-31", 50)   # last day of prior month
        self._insert("brave", "2026-09-15", 10)
        self._insert("brave", "2026-10-01", 999)  # first day of next month
        start, end = _month_bounds("2026-09")
        self.assertEqual(
            self._sum("brave", start.isoformat(), end.isoformat()), 10
        )

    def test_excludes_other_services(self):
        self._insert("brave", "2026-09-10", 5)
        self._insert("kg", "2026-09-10", 500)
        start, end = _month_bounds("2026-09")
        self.assertEqual(
            self._sum("brave", start.isoformat(), end.isoformat()), 5
        )

    def test_no_data_returns_zero_not_null(self):
        start, end = _month_bounds("2026-09")
        self.assertEqual(
            self._sum("brave", start.isoformat(), end.isoformat()), 0
        )

    def test_two_writers_same_day_same_service_accumulate(self):
        """
        Mirrors discover_h1b_ats.py and build_ats_slug_list.py both calling
        record_external_request('brave', ...) on the same day — each is its
        own atomic UPDATE ... SET requests_made = requests_made + 1 against
        the same (date, service) row, so both contribute to one true total
        instead of two scripts each keeping a separate, divergent count.
        """
        for _ in range(3):
            self.conn.execute("""
                INSERT INTO external_api_health (date, service, requests_made)
                VALUES ('2026-09-10', 'brave', 1)
                ON CONFLICT(date, service) DO UPDATE SET requests_made = requests_made + 1
            """)
        self.conn.commit()
        start, end = _month_bounds("2026-09")
        self.assertEqual(
            self._sum("brave", start.isoformat(), end.isoformat()), 3
        )


class TestDayRequestCountSumShape(unittest.TestCase):
    """
    Sqlite mirror of get_day_request_count's SQL shape: SUM(requests_made)
    for one (date, service) — backs the CF probe Worker's daily quota gate
    (config.CF_WORKER_DAILY_LIMIT), sibling to TestMonthRequestCountSumShape
    above but windowed to a single day since the Workers free-tier resets
    daily instead of monthly.
    """

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE external_api_health (
                date DATE NOT NULL, service TEXT NOT NULL,
                requests_made INTEGER DEFAULT 0,
                UNIQUE(date, service)
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert(self, service, for_date, made):
        self.conn.execute(
            "INSERT INTO external_api_health (date, service, requests_made) VALUES (?, ?, ?)",
            (for_date, service, made),
        )
        self.conn.commit()

    def _sum(self, service, day):
        row = self.conn.execute("""
            SELECT COALESCE(SUM(requests_made), 0) AS total
            FROM external_api_health
            WHERE service = ? AND date = ?
        """, (service, day)).fetchone()
        return row["total"]

    def test_returns_todays_count(self):
        self._insert("cf_worker", "2026-09-22", 40000)
        self.assertEqual(self._sum("cf_worker", "2026-09-22"), 40000)

    def test_excludes_other_days(self):
        self._insert("cf_worker", "2026-09-21", 85000)
        self._insert("cf_worker", "2026-09-22", 10)
        self.assertEqual(self._sum("cf_worker", "2026-09-22"), 10)

    def test_excludes_other_services(self):
        self._insert("cf_worker", "2026-09-22", 5)
        self._insert("brave", "2026-09-22", 999)
        self.assertEqual(self._sum("cf_worker", "2026-09-22"), 5)

    def test_no_data_returns_zero_not_null(self):
        self.assertEqual(self._sum("cf_worker", "2026-09-22"), 0)

    def test_two_implementations_same_day_accumulate(self):
        """
        Mirrors scripts/discover_h1b_ats.py and jobs/ats/career_detector.py
        both calling record_external_request('cf_worker', ...) on the same
        day against the same shared Cloudflare Worker/account — each is its
        own atomic UPDATE ... SET requests_made = requests_made + 1, so both
        contribute to one true daily total instead of racing.
        """
        for _ in range(3):
            self.conn.execute("""
                INSERT INTO external_api_health (date, service, requests_made)
                VALUES ('2026-09-22', 'cf_worker', 1)
                ON CONFLICT(date, service) DO UPDATE SET requests_made = requests_made + 1
            """)
        self.conn.commit()
        self.assertEqual(self._sum("cf_worker", "2026-09-22"), 3)


class TestRetentionConstant(unittest.TestCase):

    def test_retention_matches_api_health(self):
        """external_api_health retention deliberately matches api_health (60 days)."""
        from config import RETENTION_API_HEALTH, RETENTION_EXTERNAL_API_HEALTH
        self.assertEqual(RETENTION_EXTERNAL_API_HEALTH, RETENTION_API_HEALTH)
        self.assertEqual(RETENTION_EXTERNAL_API_HEALTH, 60)


if __name__ == "__main__":
    unittest.main(verbosity=2)
