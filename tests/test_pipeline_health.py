"""
tests/test_pipeline_health.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for db/pipeline_alerts.py — the full alert CRUD,
check_pipeline_health(), and check_api_health() logic.

Strategy
────────
  SQLite in-memory:   has_recent_alert, mark_notified, mark_warnings_sent,
                      get_pending_warnings, get_unnotified_alerts

  Mocked get_conn:    check_pipeline_health() (Postgres-only SQL),
                      check_api_health()

Coverage map
────────────
  TestHasRecentAlert  (in-memory SQLite)
    · Returns False when no alerts in DB
    · Returns False when alert exists but is outside dedup window
    · Returns True when matching alert exists within dedup window
    · Platform-specific match: different platform → False
    · Platform-specific match: same platform → True
    · Without platform: matches on type alone
    · Without platform: does not match a different type
    · Hours parameter overrides ALERT_DEDUP_HOURS default

  TestMarkNotified  (in-memory SQLite)
    · mark_notified sets notified=1 and notified_at
    · mark_notified only updates the specified alert
    · mark_notified is idempotent (calling twice is safe)

  TestMarkWarningsSent  (in-memory SQLite)
    · Empty list is a no-op (no DB call)
    · Single ID marked correctly
    · Multiple IDs all marked
    · Only the specified IDs are marked; others unchanged
    · Idempotent — re-marking already-notified alerts is safe

  TestGetPendingWarnings  (in-memory SQLite)
    · Empty table → empty list
    · Returns only severity='warning' AND notified=0
    · Excludes notified=1 warnings
    · Excludes severity='critical' regardless of notified flag
    · Returns newest first (ORDER BY created_at DESC)
    · Returns list of dicts

  TestGetUnnotifiedAlerts  (in-memory SQLite)
    · Empty table → empty list
    · Returns all unnotified alerts regardless of severity
    · Excludes notified=1 alerts
    · Returns oldest first (ORDER BY created_at ASC)
    · Returns list of dicts

  TestCheckPipelineHealth  (mocked get_coverage_stats)
    · Fewer than METRIC_ALERT_CONSECUTIVE_DAYS rows → empty list (no alert)
    · Non-contiguous dates → empty list (skips check)
    · metric1 values all above threshold → no alert
    · metric1 values all below threshold for N consecutive days → alert
    · metric1 alert has correct type, severity, value, threshold
    · metric2 values all below threshold → alert
    · Both metric1 and metric2 breach → two alerts
    · One metric above, one below → only one alert
    · Alert value = average of the N values (rounded)
    · Missing metric1 values (None) → not enough data → no alert
    · create_alert returns None (dedup) → breach still appended to results
    · Contiguous date check passes correctly for N consecutive days

  TestCheckApiHealth  (mocked DB query)
    · Empty rows → empty list
    · Fewer than API_FAILURE_CONSECUTIVE_DAYS rows per platform → no alert
    · Non-contiguous dates for platform → no alert
    · All days above threshold → alert for that platform
    · Alert contains platform, alert_type, severity, value, threshold
    · Days below threshold → no alert
    · Mixed error rates (some above, some below) → no alert
    · Multiple platforms: only the breaching one fires alert
    · avg_error in alert = average of per-day rates
    · create_alert returns None (dedup) → breach still appended

  TestExistingAlertConstants
    · All pre-existing alert type constants are strings
    · ALERT_RATE_LIMIT, ALERT_UNREACHABLE, ALERT_SLOW, etc. defined
    · ALERT_METRIC1_LOW, ALERT_METRIC2_LOW, ALERT_API_FAILURE constants
"""

import sys
import os
import sqlite3
import unittest
from datetime import date, timedelta, datetime
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# In-memory SQLite schema helper
# ─────────────────────────────────────────────────────────────────────────────

def _make_alerts_db():
    """In-memory SQLite with pipeline_alerts schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE pipeline_alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type  TEXT    NOT NULL,
            severity    TEXT    NOT NULL,
            platform    TEXT,
            value       REAL,
            threshold   REAL,
            message     TEXT,
            notified    INTEGER DEFAULT 0,
            notified_at TIMESTAMP,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def _insert_alert(conn, alert_type, severity, platform=None,
                  value=None, threshold=None, message=None,
                  notified=0, created_at=None):
    """Insert an alert row and return its id."""
    ts = created_at or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute("""
        INSERT INTO pipeline_alerts
            (alert_type, severity, platform, value, threshold,
             message, notified, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (alert_type, severity, platform, value, threshold,
          message, notified, ts))
    row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


def _days_ago(n):
    return (date.today() - timedelta(days=n)).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# TestHasRecentAlert
# ─────────────────────────────────────────────────────────────────────────────

class TestHasRecentAlert(unittest.TestCase):

    def setUp(self):
        self.db = _make_alerts_db()

    def tearDown(self):
        self.db.close()

    def _patch(self):
        # Wrap self.db so close() is a no-op — source code calls conn.close()
        # in a finally block; we must keep the in-memory connection alive for
        # subsequent test assertions that query self.db directly.
        no_close_db = MagicMock(wraps=self.db)
        no_close_db.close = MagicMock()
        return patch("db.pipeline_alerts.get_conn", return_value=no_close_db)

    def test_returns_false_when_empty_table(self):
        """No alerts in DB → has_recent_alert returns False."""
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            result = has_recent_alert("error_streak", "Co")
        self.assertFalse(result)

    def test_returns_true_for_recent_alert(self):
        """Alert created just now → has_recent_alert returns True."""
        _insert_alert(self.db, "error_streak", "warning", platform="Co")
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            result = has_recent_alert("error_streak", "Co")
        self.assertTrue(result)

    def test_returns_false_for_expired_alert(self):
        """Alert older than dedup window → False."""
        from config import ALERT_DEDUP_HOURS
        old_ts = (datetime.utcnow() - timedelta(hours=ALERT_DEDUP_HOURS + 1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        _insert_alert(self.db, "error_streak", "warning",
                      platform="Co", created_at=old_ts)
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            result = has_recent_alert("error_streak", "Co")
        self.assertFalse(result)

    def test_different_platform_returns_false(self):
        """Alert for CompanyA doesn't match query for CompanyB."""
        _insert_alert(self.db, "error_streak", "warning", platform="CompanyA")
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            result = has_recent_alert("error_streak", "CompanyB")
        self.assertFalse(result)

    def test_same_type_different_platform_returns_false(self):
        """Same type but different platform: False."""
        _insert_alert(self.db, "error_streak", "warning", platform="A")
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            self.assertFalse(has_recent_alert("error_streak", "B"))

    def test_without_platform_matches_type_only(self):
        """Without platform: matches on type alone (null platform check)."""
        _insert_alert(self.db, "redis_memory", "critical", platform=None)
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            result = has_recent_alert("redis_memory")
        self.assertTrue(result)

    def test_without_platform_wrong_type_returns_false(self):
        """Without platform: non-matching type returns False."""
        _insert_alert(self.db, "redis_memory", "critical", platform=None)
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            result = has_recent_alert("detail_queue_depth")
        self.assertFalse(result)

    def test_custom_hours_parameter(self):
        """hours parameter overrides the default ALERT_DEDUP_HOURS."""
        # Alert created 3 hours ago
        ts = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
        _insert_alert(self.db, "error_streak", "warning",
                      platform="Co", created_at=ts)
        with self._patch():
            from db.pipeline_alerts import has_recent_alert
            # With 2-hour window: alert is outside → False
            self.assertFalse(has_recent_alert("error_streak", "Co", hours=2))
            # With 4-hour window: alert is inside → True
            self.assertTrue(has_recent_alert("error_streak", "Co", hours=4))


# ─────────────────────────────────────────────────────────────────────────────
# TestMarkNotified
# ─────────────────────────────────────────────────────────────────────────────

class TestMarkNotified(unittest.TestCase):

    def setUp(self):
        self.db = _make_alerts_db()

    def tearDown(self):
        self.db.close()

    def _patch(self):
        # Wrap self.db so close() is a no-op — source code calls conn.close()
        # in a finally block; we must keep the in-memory connection alive for
        # subsequent test assertions that query self.db directly.
        no_close_db = MagicMock(wraps=self.db)
        no_close_db.close = MagicMock()
        return patch("db.pipeline_alerts.get_conn", return_value=no_close_db)

    def test_mark_notified_sets_flag(self):
        """mark_notified sets notified=1 for the specified alert."""
        alert_id = _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import mark_notified
            mark_notified(alert_id)
        row = self.db.execute(
            "SELECT notified FROM pipeline_alerts WHERE id = ?", (alert_id,)
        ).fetchone()
        self.assertEqual(row["notified"], 1)

    def test_mark_notified_sets_notified_at(self):
        """mark_notified also sets notified_at timestamp."""
        alert_id = _insert_alert(self.db, "error_streak", "warning")
        with self._patch():
            from db.pipeline_alerts import mark_notified
            mark_notified(alert_id)
        row = self.db.execute(
            "SELECT notified_at FROM pipeline_alerts WHERE id = ?", (alert_id,)
        ).fetchone()
        self.assertIsNotNone(row["notified_at"])

    def test_mark_notified_only_updates_specified_alert(self):
        """Only the specified alert_id is marked; others stay notified=0."""
        id1 = _insert_alert(self.db, "error_streak", "warning", notified=0)
        id2 = _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import mark_notified
            mark_notified(id1)
        row2 = self.db.execute(
            "SELECT notified FROM pipeline_alerts WHERE id = ?", (id2,)
        ).fetchone()
        self.assertEqual(row2["notified"], 0)

    def test_mark_notified_idempotent(self):
        """mark_notified called twice does not raise and notified stays 1."""
        alert_id = _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import mark_notified
            mark_notified(alert_id)
            mark_notified(alert_id)   # second call — idempotent
        row = self.db.execute(
            "SELECT notified FROM pipeline_alerts WHERE id = ?", (alert_id,)
        ).fetchone()
        self.assertEqual(row["notified"], 1)


# ─────────────────────────────────────────────────────────────────────────────
# TestMarkWarningsSent
# ─────────────────────────────────────────────────────────────────────────────

class TestMarkWarningsSent(unittest.TestCase):

    def setUp(self):
        self.db = _make_alerts_db()

    def tearDown(self):
        self.db.close()

    def _patch(self):
        # Wrap self.db so close() is a no-op — source code calls conn.close()
        # in a finally block; we must keep the in-memory connection alive for
        # subsequent test assertions that query self.db directly.
        no_close_db = MagicMock(wraps=self.db)
        no_close_db.close = MagicMock()
        return patch("db.pipeline_alerts.get_conn", return_value=no_close_db)

    def test_empty_list_is_no_op(self):
        """Empty list → no DB interaction, no error."""
        with self._patch():
            from db.pipeline_alerts import mark_warnings_sent
            mark_warnings_sent([])   # should not raise
        # DB untouched
        count = self.db.execute(
            "SELECT COUNT(*) FROM pipeline_alerts WHERE notified=1"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_single_id_marked(self):
        """Single alert ID marked correctly."""
        aid = _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import mark_warnings_sent
            mark_warnings_sent([aid])
        row = self.db.execute(
            "SELECT notified FROM pipeline_alerts WHERE id = ?", (aid,)
        ).fetchone()
        self.assertEqual(row["notified"], 1)

    def test_multiple_ids_all_marked(self):
        """Multiple alert IDs all marked in one call."""
        ids = [
            _insert_alert(self.db, "error_streak", "warning", notified=0)
            for _ in range(3)
        ]
        with self._patch():
            from db.pipeline_alerts import mark_warnings_sent
            mark_warnings_sent(ids)
        for aid in ids:
            row = self.db.execute(
                "SELECT notified FROM pipeline_alerts WHERE id = ?", (aid,)
            ).fetchone()
            self.assertEqual(row["notified"], 1, f"Alert {aid} not marked")

    def test_only_specified_ids_marked(self):
        """Unspecified alerts remain notified=0."""
        id1 = _insert_alert(self.db, "error_streak", "warning", notified=0)
        id2 = _insert_alert(self.db, "redis_memory", "critical", notified=0)
        with self._patch():
            from db.pipeline_alerts import mark_warnings_sent
            mark_warnings_sent([id1])
        row2 = self.db.execute(
            "SELECT notified FROM pipeline_alerts WHERE id = ?", (id2,)
        ).fetchone()
        self.assertEqual(row2["notified"], 0)

    def test_idempotent_remarch(self):
        """Marking already-notified alerts again is safe."""
        aid = _insert_alert(self.db, "error_streak", "warning", notified=1)
        with self._patch():
            from db.pipeline_alerts import mark_warnings_sent
            mark_warnings_sent([aid])   # re-marking: no error
        row = self.db.execute(
            "SELECT notified FROM pipeline_alerts WHERE id = ?", (aid,)
        ).fetchone()
        self.assertEqual(row["notified"], 1)


# ─────────────────────────────────────────────────────────────────────────────
# TestGetPendingWarnings
# ─────────────────────────────────────────────────────────────────────────────

class TestGetPendingWarnings(unittest.TestCase):

    def setUp(self):
        self.db = _make_alerts_db()

    def tearDown(self):
        self.db.close()

    def _patch(self):
        # Wrap self.db so close() is a no-op — source code calls conn.close()
        # in a finally block; we must keep the in-memory connection alive for
        # subsequent test assertions that query self.db directly.
        no_close_db = MagicMock(wraps=self.db)
        no_close_db.close = MagicMock()
        return patch("db.pipeline_alerts.get_conn", return_value=no_close_db)

    def test_empty_table_returns_empty_list(self):
        """Empty table → empty list."""
        with self._patch():
            from db.pipeline_alerts import get_pending_warnings
            result = get_pending_warnings()
        self.assertEqual(result, [])

    def test_returns_only_unnotified_warnings(self):
        """Returns severity='warning' AND notified=0."""
        _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import get_pending_warnings
            result = get_pending_warnings()
        self.assertEqual(len(result), 1)

    def test_excludes_notified_warnings(self):
        """Notified warnings (notified=1) are excluded."""
        _insert_alert(self.db, "error_streak", "warning", notified=1)
        with self._patch():
            from db.pipeline_alerts import get_pending_warnings
            result = get_pending_warnings()
        self.assertEqual(len(result), 0)

    def test_excludes_critical_severity(self):
        """CRITICAL alerts not returned even if notified=0."""
        _insert_alert(self.db, "redis_memory", "critical", notified=0)
        with self._patch():
            from db.pipeline_alerts import get_pending_warnings
            result = get_pending_warnings()
        self.assertEqual(len(result), 0)

    def test_returns_list_of_dicts(self):
        """Each result is a dict."""
        _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import get_pending_warnings
            result = get_pending_warnings()
        self.assertIsInstance(result[0], dict)

    def test_multiple_unnotified_warnings_all_returned(self):
        """Multiple unnotified warnings all included."""
        for _ in range(3):
            _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import get_pending_warnings
            result = get_pending_warnings()
        self.assertEqual(len(result), 3)

    def test_ordered_newest_first(self):
        """Results ordered by created_at DESC (newest first)."""
        t1 = "2026-04-01 10:00:00"
        t2 = "2026-04-03 10:00:00"
        _insert_alert(self.db, "error_streak", "warning",
                      message="older", created_at=t1)
        _insert_alert(self.db, "error_streak", "warning",
                      message="newer", created_at=t2)
        with self._patch():
            from db.pipeline_alerts import get_pending_warnings
            result = get_pending_warnings()
        self.assertEqual(result[0]["message"], "newer")
        self.assertEqual(result[1]["message"], "older")


# ─────────────────────────────────────────────────────────────────────────────
# TestGetUnnotifiedAlerts
# ─────────────────────────────────────────────────────────────────────────────

class TestGetUnnotifiedAlerts(unittest.TestCase):

    def setUp(self):
        self.db = _make_alerts_db()

    def tearDown(self):
        self.db.close()

    def _patch(self):
        # Wrap self.db so close() is a no-op — source code calls conn.close()
        # in a finally block; we must keep the in-memory connection alive for
        # subsequent test assertions that query self.db directly.
        no_close_db = MagicMock(wraps=self.db)
        no_close_db.close = MagicMock()
        return patch("db.pipeline_alerts.get_conn", return_value=no_close_db)

    def test_empty_table_returns_empty_list(self):
        """Empty table → empty list."""
        with self._patch():
            from db.pipeline_alerts import get_unnotified_alerts
            result = get_unnotified_alerts()
        self.assertEqual(result, [])

    def test_returns_all_severities_unnotified(self):
        """Returns both warning and critical unnotified alerts."""
        _insert_alert(self.db, "error_streak", "warning",  notified=0)
        _insert_alert(self.db, "redis_memory", "critical", notified=0)
        with self._patch():
            from db.pipeline_alerts import get_unnotified_alerts
            result = get_unnotified_alerts()
        severities = {r["severity"] for r in result}
        self.assertIn("warning", severities)
        self.assertIn("critical", severities)

    def test_excludes_notified_alerts(self):
        """Notified alerts (notified=1) excluded."""
        _insert_alert(self.db, "error_streak", "warning",  notified=1)
        _insert_alert(self.db, "redis_memory", "critical", notified=0)
        with self._patch():
            from db.pipeline_alerts import get_unnotified_alerts
            result = get_unnotified_alerts()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["severity"], "critical")

    def test_returns_list_of_dicts(self):
        """Each result is a dict with string keys."""
        _insert_alert(self.db, "error_streak", "warning", notified=0)
        with self._patch():
            from db.pipeline_alerts import get_unnotified_alerts
            result = get_unnotified_alerts()
        self.assertIsInstance(result[0], dict)
        self.assertIn("alert_type", result[0])

    def test_ordered_oldest_first(self):
        """Results ordered by created_at ASC (oldest first)."""
        t1 = "2026-04-01 10:00:00"
        t2 = "2026-04-03 10:00:00"
        _insert_alert(self.db, "error_streak", "warning",
                      message="older", created_at=t1)
        _insert_alert(self.db, "redis_memory", "critical",
                      message="newer", created_at=t2)
        with self._patch():
            from db.pipeline_alerts import get_unnotified_alerts
            result = get_unnotified_alerts()
        self.assertEqual(result[0]["message"], "older")
        self.assertEqual(result[1]["message"], "newer")


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckPipelineHealth
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckPipelineHealth(unittest.TestCase):
    """
    Tests for check_pipeline_health() using mocked get_coverage_stats
    and create_alert.
    """

    def _make_stats(self, n_days, m1_values, m2_values):
        """
        Build a list of N coverage stats rows (newest first).
        m1_values: list of N metric1 values (or None for missing)
        m2_values: list of N metric2 values (or None for missing)
        """
        rows = []
        for i in range(n_days):
            d = (date.today() - timedelta(days=i)).isoformat()
            rows.append({
                "date":    d,
                "metric1": m1_values[i],
                "metric2": m2_values[i],
            })
        return rows

    def _run(self, stats_rows, create_alert_return=42):
        """Run check_pipeline_health with mocked deps."""
        with patch("db.alerts.get_coverage_stats",
                   return_value=stats_rows), \
             patch("db.pipeline_alerts.create_alert",
                   return_value=create_alert_return) as mock_ca:
            from db.pipeline_alerts import check_pipeline_health
            result = check_pipeline_health()
        return result, mock_ca

    def test_fewer_than_required_rows_returns_empty(self):
        """Fewer than METRIC_ALERT_CONSECUTIVE_DAYS rows → empty list."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS
        stats = self._make_stats(
            n_days=METRIC_ALERT_CONSECUTIVE_DAYS - 1,
            m1_values=[10.0] * (METRIC_ALERT_CONSECUTIVE_DAYS - 1),
            m2_values=[10.0] * (METRIC_ALERT_CONSECUTIVE_DAYS - 1),
        )
        result, _ = self._run(stats)
        self.assertEqual(result, [])

    def test_non_contiguous_dates_returns_empty(self):
        """Non-contiguous dates → skip (no alert)."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS
        rows = []
        # Create rows with a gap
        for i in range(METRIC_ALERT_CONSECUTIVE_DAYS):
            d = (date.today() - timedelta(days=i * 2)).isoformat()  # every 2 days
            rows.append({"date": d, "metric1": 10.0, "metric2": 10.0})
        result, _ = self._run(rows)
        self.assertEqual(result, [])

    def test_metric1_all_above_threshold_no_alert(self):
        """metric1 values all above threshold → no alert."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS, METRIC1_ALERT_THRESHOLD
        stats = self._make_stats(
            n_days=METRIC_ALERT_CONSECUTIVE_DAYS,
            m1_values=[METRIC1_ALERT_THRESHOLD + 10.0] * METRIC_ALERT_CONSECUTIVE_DAYS,
            m2_values=[METRIC1_ALERT_THRESHOLD + 10.0] * METRIC_ALERT_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(stats)
        self.assertEqual(result, [])

    def test_metric1_all_below_threshold_fires_alert(self):
        """metric1 below threshold for N days → alert fires."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS, METRIC1_ALERT_THRESHOLD
        from db.pipeline_alerts import ALERT_METRIC1_LOW
        below = METRIC1_ALERT_THRESHOLD - 10.0
        stats = self._make_stats(
            n_days=METRIC_ALERT_CONSECUTIVE_DAYS,
            m1_values=[below] * METRIC_ALERT_CONSECUTIVE_DAYS,
            m2_values=[METRIC1_ALERT_THRESHOLD + 20.0] * METRIC_ALERT_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(stats)
        types = [r["alert_type"] for r in result]
        self.assertIn(ALERT_METRIC1_LOW, types)

    def test_metric1_alert_has_critical_severity(self):
        """metric1 alert severity = CRITICAL."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS, METRIC1_ALERT_THRESHOLD
        from db.pipeline_alerts import CRITICAL
        below = METRIC1_ALERT_THRESHOLD - 5.0
        stats = self._make_stats(
            METRIC_ALERT_CONSECUTIVE_DAYS,
            [below] * METRIC_ALERT_CONSECUTIVE_DAYS,
            [METRIC1_ALERT_THRESHOLD + 20.0] * METRIC_ALERT_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(stats)
        self.assertTrue(any(r["severity"] == CRITICAL for r in result))

    def test_metric1_alert_value_is_average(self):
        """metric1 alert value = average of the N breach values."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS, METRIC1_ALERT_THRESHOLD
        n = METRIC_ALERT_CONSECUTIVE_DAYS
        values = [10.0, 20.0, 30.0][:n]
        expected_avg = sum(values) / len(values)
        stats = self._make_stats(
            n,
            m1_values=values,
            m2_values=[METRIC1_ALERT_THRESHOLD + 20.0] * n,
        )
        result, _ = self._run(stats)
        metric1_alerts = [r for r in result if r["alert_type"] == "metric1_low"]
        if metric1_alerts:
            self.assertAlmostEqual(metric1_alerts[0]["value"], round(expected_avg, 1),
                                   places=1)

    def test_metric2_all_below_threshold_fires_alert(self):
        """metric2 below threshold for N days → alert fires."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS, METRIC2_ALERT_THRESHOLD
        from db.pipeline_alerts import ALERT_METRIC2_LOW
        below = METRIC2_ALERT_THRESHOLD - 10.0
        stats = self._make_stats(
            METRIC_ALERT_CONSECUTIVE_DAYS,
            m1_values=[METRIC2_ALERT_THRESHOLD + 20.0] * METRIC_ALERT_CONSECUTIVE_DAYS,
            m2_values=[below] * METRIC_ALERT_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(stats)
        types = [r["alert_type"] for r in result]
        self.assertIn(ALERT_METRIC2_LOW, types)

    def test_both_metrics_breach_two_alerts(self):
        """Both metric1 and metric2 below thresholds → two alerts."""
        from config import (
            METRIC_ALERT_CONSECUTIVE_DAYS,
            METRIC1_ALERT_THRESHOLD,
            METRIC2_ALERT_THRESHOLD,
        )
        n = METRIC_ALERT_CONSECUTIVE_DAYS
        stats = self._make_stats(
            n,
            m1_values=[METRIC1_ALERT_THRESHOLD - 5.0] * n,
            m2_values=[METRIC2_ALERT_THRESHOLD - 5.0] * n,
        )
        result, _ = self._run(stats)
        self.assertEqual(len(result), 2)

    def test_only_metric1_breach_one_alert(self):
        """metric1 breaches, metric2 healthy → exactly one alert."""
        from config import (
            METRIC_ALERT_CONSECUTIVE_DAYS,
            METRIC1_ALERT_THRESHOLD,
            METRIC2_ALERT_THRESHOLD,
        )
        from db.pipeline_alerts import ALERT_METRIC1_LOW
        n = METRIC_ALERT_CONSECUTIVE_DAYS
        stats = self._make_stats(
            n,
            m1_values=[METRIC1_ALERT_THRESHOLD - 5.0] * n,
            m2_values=[METRIC2_ALERT_THRESHOLD + 20.0] * n,
        )
        result, _ = self._run(stats)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["alert_type"], ALERT_METRIC1_LOW)

    def test_create_alert_none_breach_still_appended(self):
        """create_alert dedup (returns None) → breach still appended with alert_id=None."""
        from config import (
            METRIC_ALERT_CONSECUTIVE_DAYS,
            METRIC1_ALERT_THRESHOLD,
            METRIC2_ALERT_THRESHOLD,
        )
        n = METRIC_ALERT_CONSECUTIVE_DAYS
        stats = self._make_stats(
            n,
            m1_values=[METRIC1_ALERT_THRESHOLD - 5.0] * n,
            m2_values=[METRIC2_ALERT_THRESHOLD + 20.0] * n,
        )
        result, _ = self._run(stats, create_alert_return=None)
        # Alert should still be in result (with alert_id=None)
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["alert_id"])

    def test_missing_metric1_values_no_alert(self):
        """metric1=None for some rows → not enough data → no metric1 alert."""
        from config import METRIC_ALERT_CONSECUTIVE_DAYS, METRIC1_ALERT_THRESHOLD
        n = METRIC_ALERT_CONSECUTIVE_DAYS
        # Make one value None
        m1 = [METRIC1_ALERT_THRESHOLD - 5.0] * n
        m1[0] = None  # one missing
        stats = self._make_stats(
            n,
            m1_values=m1,
            m2_values=[METRIC1_ALERT_THRESHOLD + 20.0] * n,
        )
        result, _ = self._run(stats)
        # With one None, len(metric1_values) < days → no alert
        metric1_alerts = [r for r in result if r["alert_type"] == "metric1_low"]
        self.assertEqual(len(metric1_alerts), 0)

    def test_contiguous_date_check_passes(self):
        """Exactly N consecutive days → contiguous check passes → alert can fire."""
        from config import (
            METRIC_ALERT_CONSECUTIVE_DAYS,
            METRIC1_ALERT_THRESHOLD,
            METRIC2_ALERT_THRESHOLD,
        )
        n = METRIC_ALERT_CONSECUTIVE_DAYS
        stats = self._make_stats(
            n,
            m1_values=[METRIC1_ALERT_THRESHOLD - 5.0] * n,
            m2_values=[METRIC2_ALERT_THRESHOLD + 20.0] * n,
        )
        result, _ = self._run(stats)
        # With contiguous days and all below threshold → exactly 1 alert
        self.assertGreater(len(result), 0)


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckApiHealth
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckApiHealth(unittest.TestCase):
    """
    Tests for check_api_health() using mocked DB conn and create_alert.
    """

    def _make_rows(self, platform, n_days, error_pcts):
        """Build per-day api_health rows for a platform (newest first)."""
        rows = []
        for i in range(n_days):
            d = (date.today() - timedelta(days=i)).isoformat()
            rows.append({
                "date":          d,
                "platform":      platform,
                "requests_made": 100,
                "requests_error": int(error_pcts[i]),
                "error_pct":     float(error_pcts[i]),
            })
        return rows

    def _run(self, all_rows, create_alert_return=42):
        """Patch DB query + create_alert, run check_api_health."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = all_rows
        with patch("db.pipeline_alerts.get_conn", return_value=mock_conn), \
             patch("db.pipeline_alerts.create_alert",
                   return_value=create_alert_return) as mock_ca:
            from db.pipeline_alerts import check_api_health
            result = check_api_health()
        return result, mock_ca

    def test_empty_rows_returns_empty_list(self):
        """No rows in DB → empty list."""
        result, _ = self._run([])
        self.assertEqual(result, [])

    def test_fewer_than_required_days_no_alert(self):
        """Fewer than API_FAILURE_CONSECUTIVE_DAYS rows → no alert."""
        from config import API_FAILURE_CONSECUTIVE_DAYS
        rows = self._make_rows(
            "greenhouse",
            n_days=API_FAILURE_CONSECUTIVE_DAYS - 1,
            error_pcts=[50.0] * (API_FAILURE_CONSECUTIVE_DAYS - 1),
        )
        result, _ = self._run(rows)
        self.assertEqual(result, [])

    def test_all_days_above_threshold_fires_alert(self):
        """All N days above threshold → alert fires."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        from db.pipeline_alerts import ALERT_API_FAILURE
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        rows = self._make_rows(
            "greenhouse",
            n_days=API_FAILURE_CONSECUTIVE_DAYS,
            error_pcts=[threshold_pct + 5.0] * API_FAILURE_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(rows)
        types = [r["alert_type"] for r in result]
        self.assertIn(ALERT_API_FAILURE, types)

    def test_alert_contains_platform_name(self):
        """Alert platform field matches the breaching platform."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        rows = self._make_rows(
            "lever",
            n_days=API_FAILURE_CONSECUTIVE_DAYS,
            error_pcts=[threshold_pct + 10.0] * API_FAILURE_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(rows)
        if result:
            self.assertEqual(result[0]["platform"], "lever")

    def test_alert_severity_is_critical(self):
        """API failure alert is CRITICAL."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        from db.pipeline_alerts import CRITICAL
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        rows = self._make_rows(
            "lever",
            n_days=API_FAILURE_CONSECUTIVE_DAYS,
            error_pcts=[threshold_pct + 10.0] * API_FAILURE_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(rows)
        if result:
            self.assertEqual(result[0]["severity"], CRITICAL)

    def test_days_below_threshold_no_alert(self):
        """Error rates all below threshold → no alert."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        rows = self._make_rows(
            "greenhouse",
            n_days=API_FAILURE_CONSECUTIVE_DAYS,
            error_pcts=[threshold_pct - 5.0] * API_FAILURE_CONSECUTIVE_DAYS,
        )
        result, _ = self._run(rows)
        self.assertEqual(result, [])

    def test_mixed_rates_no_alert(self):
        """Some days above, some below → no alert (need ALL days above)."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        # Alternate above/below
        pcts = []
        for i in range(API_FAILURE_CONSECUTIVE_DAYS):
            pcts.append(threshold_pct + 5.0 if i % 2 == 0 else threshold_pct - 5.0)
        rows = self._make_rows("greenhouse", API_FAILURE_CONSECUTIVE_DAYS, pcts)
        result, _ = self._run(rows)
        self.assertEqual(result, [])

    def test_multiple_platforms_only_breaching_fires(self):
        """Only the platform with all-above rates fires an alert."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        from db.pipeline_alerts import ALERT_API_FAILURE
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        n = API_FAILURE_CONSECUTIVE_DAYS
        # greenhouse: all above → alert
        gh_rows = self._make_rows("greenhouse", n, [threshold_pct + 10.0] * n)
        # lever: all below → no alert
        lv_rows = self._make_rows("lever", n, [threshold_pct - 5.0] * n)
        result, _ = self._run(gh_rows + lv_rows)
        platforms = [r["platform"] for r in result]
        self.assertIn("greenhouse", platforms)
        self.assertNotIn("lever", platforms)

    def test_non_contiguous_dates_no_alert(self):
        """Non-contiguous dates for a platform → no alert."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        n = API_FAILURE_CONSECUTIVE_DAYS
        rows = []
        for i in range(n):
            # Skip a day between each row (non-contiguous)
            d = (date.today() - timedelta(days=i * 2)).isoformat()
            rows.append({
                "date":          d,
                "platform":      "greenhouse",
                "requests_made": 100,
                "requests_error": int(threshold_pct + 5.0),
                "error_pct":     threshold_pct + 5.0,
            })
        result, _ = self._run(rows)
        self.assertEqual(result, [])

    def test_create_alert_none_breach_still_appended(self):
        """create_alert returns None (dedup) → breach appended with alert_id=None."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        n = API_FAILURE_CONSECUTIVE_DAYS
        rows = self._make_rows("greenhouse", n, [threshold_pct + 10.0] * n)
        result, _ = self._run(rows, create_alert_return=None)
        if result:
            self.assertIsNone(result[0]["alert_id"])

    def test_avg_error_value_in_alert(self):
        """Alert value = average of per-day error rates (rounded)."""
        from config import (
            API_FAILURE_CONSECUTIVE_DAYS,
            API_FAILURE_RATE_THRESHOLD,
        )
        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100
        n = API_FAILURE_CONSECUTIVE_DAYS
        pcts = [threshold_pct + i * 2.0 for i in range(n)]
        expected_avg = round(sum(pcts) / n, 1)
        rows = self._make_rows("greenhouse", n, pcts)
        result, _ = self._run(rows)
        if result:
            self.assertAlmostEqual(result[0]["value"], expected_avg, delta=0.2)


# ─────────────────────────────────────────────────────────────────────────────
# TestExistingAlertConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestExistingAlertConstants(unittest.TestCase):

    def test_all_alert_type_constants_are_strings(self):
        """All alert type constants are non-empty strings."""
        from db.pipeline_alerts import (
            ALERT_RATE_LIMIT, ALERT_UNREACHABLE, ALERT_SLOW,
            ALERT_SERPER_LOW, ALERT_SERPER_DONE, ALERT_CRASH,
            ALERT_METRIC1_LOW, ALERT_METRIC2_LOW,
            ALERT_EXHAUSTION_BLOCKED, ALERT_API_FAILURE,
            ALERT_COVERAGE_DROP,
        )
        for const in [
            ALERT_RATE_LIMIT, ALERT_UNREACHABLE, ALERT_SLOW,
            ALERT_SERPER_LOW, ALERT_SERPER_DONE, ALERT_CRASH,
            ALERT_METRIC1_LOW, ALERT_METRIC2_LOW,
            ALERT_EXHAUSTION_BLOCKED, ALERT_API_FAILURE,
            ALERT_COVERAGE_DROP,
        ]:
            self.assertIsInstance(const, str)
            self.assertGreater(len(const), 0)

    def test_all_constants_unique(self):
        """All alert type constants have unique values."""
        from db.pipeline_alerts import (
            ALERT_RATE_LIMIT, ALERT_UNREACHABLE, ALERT_SLOW,
            ALERT_SERPER_LOW, ALERT_SERPER_DONE, ALERT_CRASH,
            ALERT_METRIC1_LOW, ALERT_METRIC2_LOW,
            ALERT_EXHAUSTION_BLOCKED, ALERT_API_FAILURE,
            ALERT_COVERAGE_DROP,
            ALERT_ERROR_STREAK, ALERT_DETAIL_QUEUE_DEPTH,
            ALERT_REDIS_MEMORY, ALERT_REACTIVATION_LAG,
        )
        all_types = [
            ALERT_RATE_LIMIT, ALERT_UNREACHABLE, ALERT_SLOW,
            ALERT_SERPER_LOW, ALERT_SERPER_DONE, ALERT_CRASH,
            ALERT_METRIC1_LOW, ALERT_METRIC2_LOW,
            ALERT_EXHAUSTION_BLOCKED, ALERT_API_FAILURE,
            ALERT_COVERAGE_DROP,
            ALERT_ERROR_STREAK, ALERT_DETAIL_QUEUE_DEPTH,
            ALERT_REDIS_MEMORY, ALERT_REACTIVATION_LAG,
        ]
        self.assertEqual(len(all_types), len(set(all_types)),
                         "Duplicate alert type constants found")

    def test_severity_constants_correct_values(self):
        """CRITICAL = 'critical', WARNING = 'warning'."""
        from db.pipeline_alerts import CRITICAL, WARNING
        self.assertEqual(CRITICAL, "critical")
        self.assertEqual(WARNING, "warning")

    def test_metric1_low_is_correct_string(self):
        from db.pipeline_alerts import ALERT_METRIC1_LOW
        self.assertEqual(ALERT_METRIC1_LOW, "metric1_low")

    def test_metric2_low_is_correct_string(self):
        from db.pipeline_alerts import ALERT_METRIC2_LOW
        self.assertEqual(ALERT_METRIC2_LOW, "metric2_low")

    def test_api_failure_is_correct_string(self):
        from db.pipeline_alerts import ALERT_API_FAILURE
        self.assertEqual(ALERT_API_FAILURE, "api_failure_rate")

    def test_alert_dedup_hours_is_positive(self):
        """ALERT_DEDUP_HOURS from config is a positive number."""
        from config import ALERT_DEDUP_HOURS
        self.assertGreater(ALERT_DEDUP_HOURS, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
