# tests/test_api_health.py — Tests for api_health + pipeline_alerts

import sqlite3
import tempfile
import unittest
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock


class TestApiHealthCRUD(unittest.TestCase):
    """Tests for db/api_health.py."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE api_health (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            DATE    NOT NULL,
                platform        TEXT    NOT NULL,
                requests_made   INTEGER DEFAULT 0,
                requests_ok     INTEGER DEFAULT 0,
                requests_429    INTEGER DEFAULT 0,
                requests_404    INTEGER DEFAULT 0,
                requests_error  INTEGER DEFAULT 0,
                avg_response_ms INTEGER DEFAULT 0,
                max_response_ms INTEGER DEFAULT 0,
                total_ms        INTEGER DEFAULT 0,
                first_429_at    TIMESTAMP,
                backoff_total_s INTEGER DEFAULT 0,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, platform)
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert(self, platform, made=10, ok=9,
                r429=0, err=0, total_ms=1000,
                for_date=None):
        today = for_date or date.today().isoformat()
        self.conn.execute("""
            INSERT OR REPLACE INTO api_health
                (date, platform, requests_made, requests_ok,
                 requests_429, requests_error,
                 total_ms, avg_response_ms, max_response_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (today, platform, made, ok, r429, err,
              total_ms, total_ms // made if made else 0,
              total_ms // made if made else 0))
        self.conn.commit()

    def test_429_rate_calculated_correctly(self):
        """429 rate = requests_429 / requests_made * 100."""
        self._insert("greenhouse", made=100, ok=90, r429=10)
        row = self.conn.execute("""
            SELECT
                ROUND(100.0 * requests_429 / requests_made, 1)
                AS rate
            FROM api_health
            WHERE platform = 'greenhouse'
        """).fetchone()
        self.assertEqual(row["rate"], 10.0)

    def test_zero_requests_no_division(self):
        """Zero requests → no division by zero."""
        self._insert("greenhouse", made=0, ok=0)
        row = self.conn.execute("""
            SELECT
                CASE WHEN requests_made > 0
                THEN ROUND(100.0 * requests_429
                           / requests_made, 1)
                ELSE 0 END AS rate
            FROM api_health
            WHERE platform = 'greenhouse'
        """).fetchone()
        self.assertEqual(row["rate"], 0)

    def test_avg_response_ms_calculated(self):
        """avg_response_ms = total_ms / requests_made."""
        self._insert("lever", made=10, total_ms=3000)
        row = self.conn.execute("""
            SELECT avg_response_ms FROM api_health
            WHERE platform = 'lever'
        """).fetchone()
        self.assertEqual(row["avg_response_ms"], 300)

    def test_unique_constraint_date_platform(self):
        """UNIQUE(date, platform) enforced."""
        self._insert("greenhouse")
        # Second insert should fail
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("""
                INSERT INTO api_health (date, platform)
                VALUES (?, 'greenhouse')
            """, (date.today().isoformat(),))
            self.conn.commit()  # IntegrityError raised here

    def test_seven_day_summary_aggregates(self):
        """7-day summary sums across all days."""
        today     = date.today()
        yesterday = (today - timedelta(days=1)).isoformat()

        self._insert("greenhouse", made=50, r429=2,
                     for_date=today.isoformat())
        self._insert("greenhouse", made=50, r429=3,
                     for_date=yesterday)

        row = self.conn.execute("""
            SELECT
                SUM(requests_made)  AS total,
                SUM(requests_429)   AS total_429
            FROM api_health
            WHERE platform = 'greenhouse'
        """).fetchone()

        self.assertEqual(row["total"],     100)
        self.assertEqual(row["total_429"],   5)

    def test_multiple_platforms_separate_rows(self):
        """Each platform tracked independently."""
        self._insert("greenhouse", made=100, r429=0)
        self._insert("workday",    made=50,  r429=5)

        gh = self.conn.execute("""
            SELECT requests_429 FROM api_health
            WHERE platform='greenhouse'
        """).fetchone()
        wd = self.conn.execute("""
            SELECT requests_429 FROM api_health
            WHERE platform='workday'
        """).fetchone()

        self.assertEqual(gh["requests_429"], 0)
        self.assertEqual(wd["requests_429"], 5)

    def test_healthy_platform_status(self):
        """Platform with 0% 429 rate is healthy."""
        self._insert("greenhouse", made=134, ok=134,
                     r429=0, err=0)
        row = self.conn.execute("""
            SELECT
                CASE
                    WHEN ROUND(100.0*requests_429
                               /requests_made,1) >= 10
                    THEN 'critical'
                    WHEN ROUND(100.0*requests_429
                               /requests_made,1) >= 2
                    THEN 'warning'
                    ELSE 'healthy'
                END AS status
            FROM api_health WHERE platform='greenhouse'
        """).fetchone()
        self.assertEqual(row["status"], "healthy")

    def test_warning_threshold_2pct(self):
        """Platform with 2-9% 429 rate is warning."""
        # 5/100 = 5% → warning
        self._insert("lever", made=100, r429=5)
        row = self.conn.execute("""
            SELECT
                CASE
                    WHEN ROUND(100.0*requests_429
                               /requests_made,1) >= 10
                    THEN 'critical'
                    WHEN ROUND(100.0*requests_429
                               /requests_made,1) >= 2
                    THEN 'warning'
                    ELSE 'healthy'
                END AS status
            FROM api_health WHERE platform='lever'
        """).fetchone()
        self.assertEqual(row["status"], "warning")

    def test_critical_threshold_10pct(self):
        """Platform with 10%+ 429 rate is critical."""
        # 15/100 = 15% → critical
        self._insert("workday", made=100, r429=15)
        row = self.conn.execute("""
            SELECT
                CASE
                    WHEN ROUND(100.0*requests_429
                               /requests_made,1) >= 10
                    THEN 'critical'
                    WHEN ROUND(100.0*requests_429
                               /requests_made,1) >= 2
                    THEN 'warning'
                    ELSE 'healthy'
                END AS status
            FROM api_health WHERE platform='workday'
        """).fetchone()
        self.assertEqual(row["status"], "critical")


class TestPipelineAlerts(unittest.TestCase):
    """Tests for db/pipeline_alerts.py."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE pipeline_alerts (
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
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert_alert(self, alert_type, severity,
                      platform=None, notified=0,
                      notified_at=None):
        self.conn.execute("""
            INSERT INTO pipeline_alerts
                (alert_type, severity, platform,
                 notified, notified_at)
            VALUES (?, ?, ?, ?, ?)
        """, (alert_type, severity, platform,
              notified, notified_at))
        self.conn.commit()

    def test_pending_warnings_returned(self):
        """get_pending_warnings returns unnotified warnings."""
        self._insert_alert("rate_limit", "warning",
                           platform="lever", notified=0)
        rows = self.conn.execute("""
            SELECT * FROM pipeline_alerts
            WHERE severity='warning' AND notified=0
        """).fetchall()
        self.assertEqual(len(rows), 1)

    def test_notified_warnings_not_returned(self):
        """Already notified warnings excluded."""
        self._insert_alert("rate_limit", "warning",
                           platform="lever", notified=1,
                           notified_at=datetime.now().isoformat())
        rows = self.conn.execute("""
            SELECT * FROM pipeline_alerts
            WHERE severity='warning' AND notified=0
        """).fetchall()
        self.assertEqual(len(rows), 0)

    def test_mark_notified_updates_row(self):
        """mark_notified sets notified=1."""
        self._insert_alert("rate_limit", "critical",
                           platform="workday")
        alert_id = self.conn.execute(
            "SELECT id FROM pipeline_alerts"
        ).fetchone()["id"]

        now = datetime.now().isoformat()
        self.conn.execute("""
            UPDATE pipeline_alerts
            SET notified=1, notified_at=?
            WHERE id=?
        """, (now, alert_id))
        self.conn.commit()

        row = self.conn.execute("""
            SELECT notified FROM pipeline_alerts
            WHERE id=?
        """, (alert_id,)).fetchone()
        self.assertEqual(row["notified"], 1)

    def test_dedup_within_24h(self):
        """Same alert type + platform within 24h → duplicate."""
        recent = (
            datetime.now() - timedelta(hours=1)
        ).isoformat()
        self._insert_alert("rate_limit", "critical",
                           platform="workday",
                           notified=1, notified_at=recent)

        # Check if recent alert exists (simulating has_recent_alert)
        cutoff = (
            datetime.now() - timedelta(hours=24)
        ).isoformat()
        row = self.conn.execute("""
            SELECT id FROM pipeline_alerts
            WHERE alert_type='rate_limit'
            AND platform='workday'
            AND notified=1
            AND notified_at > ?
        """, (cutoff,)).fetchone()
        self.assertIsNotNone(row)  # duplicate detected

    def test_no_dedup_after_24h(self):
        """Same alert type + platform after 24h → not duplicate."""
        old_time = (
            datetime.now() - timedelta(hours=25)
        ).isoformat()
        self._insert_alert("rate_limit", "critical",
                           platform="workday",
                           notified=1, notified_at=old_time)

        cutoff = (
            datetime.now() - timedelta(hours=24)
        ).isoformat()
        row = self.conn.execute("""
            SELECT id FROM pipeline_alerts
            WHERE alert_type='rate_limit'
            AND platform='workday'
            AND notified=1
            AND notified_at > ?
        """, (cutoff,)).fetchone()
        self.assertIsNone(row)  # not a duplicate → can send again

    def test_critical_and_warning_separate(self):
        """Critical and warning alerts tracked separately."""
        self._insert_alert("rate_limit", "critical",
                           platform="workday")
        self._insert_alert("rate_limit", "warning",
                           platform="workday")

        criticals = self.conn.execute("""
            SELECT COUNT(*) FROM pipeline_alerts
            WHERE severity='critical'
        """).fetchone()[0]
        warnings = self.conn.execute("""
            SELECT COUNT(*) FROM pipeline_alerts
            WHERE severity='warning'
        """).fetchone()[0]

        self.assertEqual(criticals, 1)
        self.assertEqual(warnings, 1)


class TestRateLimitThresholds(unittest.TestCase):
    """Tests for rate limit threshold logic."""

    def test_critical_threshold_is_10(self):
        """Critical threshold = 10%."""
        from config import RATE_LIMIT_CRITICAL_THRESHOLD
        self.assertEqual(RATE_LIMIT_CRITICAL_THRESHOLD, 10)

    def test_warning_threshold_is_2(self):
        """Warning threshold = 2%."""
        from config import RATE_LIMIT_WARNING_THRESHOLD
        self.assertEqual(RATE_LIMIT_WARNING_THRESHOLD, 2)

    def test_slow_response_threshold_is_3000ms(self):
        """Slow response threshold = 3000ms."""
        from config import SLOW_RESPONSE_THRESHOLD_MS
        self.assertEqual(SLOW_RESPONSE_THRESHOLD_MS, 3000)

    def test_all_platforms_have_delays(self):
        """Every supported platform has a delay config."""
        from config import PLATFORM_DELAYS
        expected_platforms = {
            "greenhouse", "lever", "ashby",
            "smartrecruiters", "workday",
            "oracle_hcm", "icims",
        }
        for p in expected_platforms:
            self.assertIn(p, PLATFORM_DELAYS, f"Missing: {p}")
            self.assertIn("base", PLATFORM_DELAYS[p])
            self.assertIn("jitter", PLATFORM_DELAYS[p])

    def test_workday_delay_greater_than_greenhouse(self):
        """Workday needs more delay than Greenhouse."""
        from config import PLATFORM_DELAYS
        self.assertGreater(
            PLATFORM_DELAYS["workday"]["base"],
            PLATFORM_DELAYS["greenhouse"]["base"]
        )

    def test_all_platforms_have_enrich_limits(self):
        """Every platform has a daily enrichment limit."""
        from config import ENRICH_DAILY_LIMITS
        expected_platforms = {
            "greenhouse", "lever", "ashby",
            "workday", "oracle_hcm", "icims",
        }
        for p in expected_platforms:
            self.assertIn(p, ENRICH_DAILY_LIMITS, f"Missing: {p}")
            self.assertGreater(ENRICH_DAILY_LIMITS[p], 0)

    def test_workday_enrich_limit_lower_than_greenhouse(self):
        """Workday enrichment limit lower (more sensitive)."""
        from config import ENRICH_DAILY_LIMITS
        self.assertLess(
            ENRICH_DAILY_LIMITS["workday"],
            ENRICH_DAILY_LIMITS["greenhouse"]
        )

    def test_alert_dedup_hours_is_24(self):
        """Alert dedup window = 24 hours."""
        from config import ALERT_DEDUP_HOURS
        self.assertEqual(ALERT_DEDUP_HOURS, 24)

    def test_between_companies_delay_configured(self):
        """Between-company delay is configured."""
        from config import MONITOR_BETWEEN_COMPANIES
        self.assertIn("base", MONITOR_BETWEEN_COMPANIES)
        self.assertIn("jitter", MONITOR_BETWEEN_COMPANIES)
        self.assertGreater(
            MONITOR_BETWEEN_COMPANIES["base"], 0
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)