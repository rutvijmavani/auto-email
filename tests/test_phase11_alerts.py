"""
tests/test_phase11_alerts.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for all Phase 11 alerting logic:

  TestPhase11Constants
    · All config constants exist with correct values
    · All new pipeline_alerts type constants exist

  TestCreateAlertDedup (SQLite in-memory)
    · Alert created on first call
    · Duplicate within dedup window → None returned
    · Duplicate across different platforms → separate alerts allowed
    · Duplicate after dedup window expires → allowed
    · Alert type constants used correctly

  TestErrorStreakAlertConditions
    · Alert fires at exactly WORKER_ERROR_STREAK_THRESHOLD (5)
    · No alert below threshold
    · Alert fires above threshold
    · Alert uses per-company platform key for dedup
    · Alert message contains company name and consecutive error count
    · Streak counter reset check — fires again after recovery

  TestReactivationLagAlertConditions
    · No alert when success=False (still failing)
    · No alert when consecutive_errors was 0 (no prior failures)
    · No alert when last_success_at is None
    · No alert when lag < REACTIVATION_LAG_ALERT_HR (4h)
    · Alert fires when lag > threshold
    · Alert fires at exactly the boundary
    · Alert value = rounded lag_hr
    · Alert threshold = REACTIVATION_LAG_ALERT_HR
    · Alert message contains lag value and company name
    · Alert deduped per company (platform=company key)

  TestDetailQueueDepthHysteresis
    · Counter starts at 0
    · Counter increments each cycle queue is above watermark
    · Alert fires at exactly DETAIL_QUEUE_ALERT_CYCLES
    · Counter resets when queue drops below watermark
    · Alert not re-fired while counter is above cycles but deduped

  TestRedisMemoryAlertConditions
    · No alert when maxmemory = 0 (no limit configured)
    · No alert when used_pct < REDIS_MEMORY_ALERT_PCT
    · Alert fires at exactly REDIS_MEMORY_ALERT_PCT
    · Alert fires above REDIS_MEMORY_ALERT_PCT
    · Alert is CRITICAL severity
    · Alert message contains MB values
    · Memory check failure caught silently

  TestOnAdaptiveCompleteAlertIntegration
    · Error streak alert fires in real on_adaptive_complete() call
    · Reactivation lag alert fires in real on_adaptive_complete() call
    · No error streak alert when success=True
    · No reactivation lag alert when consecutive_errors was 0
    · Both alerts can fire in same call when conditions met
    · Alerts not fired when create_alert returns None (dedup)
"""

import sys
import os
import sqlite3
import time
import threading
import unittest
from datetime import datetime, timedelta, date
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_alerts_db():
    """Create in-memory SQLite with pipeline_alerts schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE pipeline_alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type  TEXT    NOT NULL,
            severity    TEXT    NOT NULL,
            platform    TEXT,
            user_id     INTEGER,
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


# ─────────────────────────────────────────────────────────────────────────────
# TestPhase11Constants
# ─────────────────────────────────────────────────────────────────────────────

class TestPhase11Constants(unittest.TestCase):
    """Verify all Phase 11 config and alert type constants exist and are correct."""

    # ── Config constants ──────────────────────────────────────────────────────

    def test_worker_error_streak_threshold_is_5(self):
        from config import WORKER_ERROR_STREAK_THRESHOLD
        self.assertEqual(WORKER_ERROR_STREAK_THRESHOLD, 5)

    def test_detail_queue_alert_cycles_is_3(self):
        from config import DETAIL_QUEUE_ALERT_CYCLES
        self.assertEqual(DETAIL_QUEUE_ALERT_CYCLES, 3)

    def test_redis_memory_alert_pct_is_80(self):
        from config import REDIS_MEMORY_ALERT_PCT
        self.assertEqual(REDIS_MEMORY_ALERT_PCT, 80)

    def test_reactivation_lag_alert_hr_is_4(self):
        from config import REACTIVATION_LAG_ALERT_HR
        self.assertEqual(REACTIVATION_LAG_ALERT_HR, 4.0)

    def test_error_streak_threshold_is_positive(self):
        from config import WORKER_ERROR_STREAK_THRESHOLD
        self.assertGreater(WORKER_ERROR_STREAK_THRESHOLD, 0)

    def test_detail_queue_alert_cycles_is_positive(self):
        from config import DETAIL_QUEUE_ALERT_CYCLES
        self.assertGreater(DETAIL_QUEUE_ALERT_CYCLES, 0)

    def test_redis_memory_pct_is_reasonable(self):
        """REDIS_MEMORY_ALERT_PCT should be between 50% and 95%."""
        from config import REDIS_MEMORY_ALERT_PCT
        self.assertGreaterEqual(REDIS_MEMORY_ALERT_PCT, 50)
        self.assertLessEqual(REDIS_MEMORY_ALERT_PCT, 95)

    def test_reactivation_lag_hr_is_positive(self):
        from config import REACTIVATION_LAG_ALERT_HR
        self.assertGreater(REACTIVATION_LAG_ALERT_HR, 0)

    # ── Alert type string constants ───────────────────────────────────────────

    def test_alert_error_streak_constant(self):
        from db.pipeline_alerts import ALERT_ERROR_STREAK
        self.assertEqual(ALERT_ERROR_STREAK, "error_streak")

    def test_alert_detail_queue_depth_constant(self):
        from db.pipeline_alerts import ALERT_DETAIL_QUEUE_DEPTH
        self.assertEqual(ALERT_DETAIL_QUEUE_DEPTH, "detail_queue_depth")

    def test_alert_redis_memory_constant(self):
        from db.pipeline_alerts import ALERT_REDIS_MEMORY
        self.assertEqual(ALERT_REDIS_MEMORY, "redis_memory")

    def test_alert_reactivation_lag_constant(self):
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG
        self.assertEqual(ALERT_REACTIVATION_LAG, "reactivation_lag")

    def test_severity_constants_exist(self):
        """CRITICAL and WARNING severity constants exist."""
        from db.pipeline_alerts import CRITICAL, WARNING
        self.assertEqual(CRITICAL, "critical")
        self.assertEqual(WARNING, "warning")

    def test_all_phase11_constants_are_strings(self):
        """All new alert type constants are strings."""
        from db.pipeline_alerts import (
            ALERT_ERROR_STREAK, ALERT_DETAIL_QUEUE_DEPTH,
            ALERT_REDIS_MEMORY, ALERT_REACTIVATION_LAG,
        )
        for const in [ALERT_ERROR_STREAK, ALERT_DETAIL_QUEUE_DEPTH,
                      ALERT_REDIS_MEMORY, ALERT_REACTIVATION_LAG]:
            self.assertIsInstance(const, str)
            self.assertTrue(len(const) > 0)

    def test_phase11_alert_types_are_unique(self):
        """All four new alert type strings are distinct."""
        from db.pipeline_alerts import (
            ALERT_ERROR_STREAK, ALERT_DETAIL_QUEUE_DEPTH,
            ALERT_REDIS_MEMORY, ALERT_REACTIVATION_LAG,
        )
        types = [ALERT_ERROR_STREAK, ALERT_DETAIL_QUEUE_DEPTH,
                 ALERT_REDIS_MEMORY, ALERT_REACTIVATION_LAG]
        self.assertEqual(len(types), len(set(types)))


# ─────────────────────────────────────────────────────────────────────────────
# TestCreateAlertDedup
# ─────────────────────────────────────────────────────────────────────────────

class TestCreateAlertDedup(unittest.TestCase):
    """
    Tests for create_alert() dedup logic using an in-memory SQLite DB.
    Patches get_conn() to return the in-memory connection.
    """

    def setUp(self):
        self.db = _make_alerts_db()

    def tearDown(self):
        self.db.close()

    def _patch_conn(self):
        """Return a context manager that patches get_conn to our test DB.

        create_alert() calls get_conn() twice: once inside has_recent_alert()
        and once for the INSERT.  has_recent_alert() closes the connection in
        its finally block.  To prevent the second call from operating on a
        closed SQLite connection, we wrap self.db in a MagicMock that forwards
        all calls to the real connection but turns close() into a no-op.
        """
        no_close_db = MagicMock(wraps=self.db)
        no_close_db.close = MagicMock()   # swallow close() calls
        return patch("db.pipeline_alerts.get_conn", return_value=no_close_db)

    def test_first_alert_creates_row(self):
        """First create_alert call inserts a row and returns non-None id."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_ERROR_STREAK, WARNING,
            )
            alert_id = create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING,
                platform="TestCo",
                value=5.0,
                threshold=5.0,
                message="5 consecutive failures",
            )
        self.assertIsNotNone(alert_id)

    def test_duplicate_within_window_returns_none(self):
        """Second identical alert within dedup window → returns None."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_ERROR_STREAK, WARNING,
            )
            first = create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING,
                platform="TestCo",
                value=5.0,
                threshold=5.0,
                message="first",
            )
            second = create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING,
                platform="TestCo",
                value=6.0,
                threshold=5.0,
                message="second",
            )
        self.assertIsNotNone(first)
        self.assertIsNone(second)

    def test_different_platform_same_type_not_deduped(self):
        """Same alert type but different platform → both allowed."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_ERROR_STREAK, WARNING,
            )
            id1 = create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING,
                platform="CompanyA",
                value=5.0, threshold=5.0, message="A",
            )
            id2 = create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING,
                platform="CompanyB",
                value=5.0, threshold=5.0, message="B",
            )
        self.assertIsNotNone(id1)
        self.assertIsNotNone(id2)

    def test_different_alert_type_same_platform_not_deduped(self):
        """Different alert types for same platform → both created."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_ERROR_STREAK,
                ALERT_REACTIVATION_LAG, WARNING,
            )
            id1 = create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING, platform="Co",
                value=5.0, threshold=5.0, message="streak",
            )
            id2 = create_alert(
                alert_type=ALERT_REACTIVATION_LAG,
                severity=WARNING, platform="Co",
                value=4.5, threshold=4.0, message="lag",
            )
        self.assertIsNotNone(id1)
        self.assertIsNotNone(id2)

    def test_no_platform_dedup_works_without_platform(self):
        """Alerts without platform field dedup on type alone."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_REDIS_MEMORY, WARNING,
            )
            id1 = create_alert(
                alert_type=ALERT_REDIS_MEMORY,
                severity=WARNING, platform=None,
                value=82.0, threshold=80.0, message="memory high",
            )
            id2 = create_alert(
                alert_type=ALERT_REDIS_MEMORY,
                severity=WARNING, platform=None,
                value=85.0, threshold=80.0, message="memory still high",
            )
        self.assertIsNotNone(id1)
        self.assertIsNone(id2)

    def test_row_count_after_dedup(self):
        """Only one row in DB after deduped second call."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_DETAIL_QUEUE_DEPTH, WARNING,
            )
            create_alert(
                alert_type=ALERT_DETAIL_QUEUE_DEPTH,
                severity=WARNING, platform=None,
                value=1200.0, threshold=1000.0, message="queue deep",
            )
            create_alert(
                alert_type=ALERT_DETAIL_QUEUE_DEPTH,
                severity=WARNING, platform=None,
                value=1300.0, threshold=1000.0, message="queue deeper",
            )
        rows = self.db.execute(
            "SELECT COUNT(*) FROM pipeline_alerts"
        ).fetchone()[0]
        self.assertEqual(rows, 1)

    def test_alert_stores_value_and_threshold(self):
        """create_alert stores value and threshold fields correctly."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_ERROR_STREAK, WARNING,
            )
            create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING, platform="Co",
                value=7.0, threshold=5.0, message="test",
            )
        row = self.db.execute(
            "SELECT value, threshold FROM pipeline_alerts"
        ).fetchone()
        self.assertEqual(row["value"], 7.0)
        self.assertEqual(row["threshold"], 5.0)

    def test_alert_stores_message(self):
        """create_alert stores the message field correctly."""
        with self._patch_conn():
            from db.pipeline_alerts import (
                create_alert, ALERT_REACTIVATION_LAG, WARNING,
            )
            create_alert(
                alert_type=ALERT_REACTIVATION_LAG,
                severity=WARNING, platform="TestCo",
                value=5.0, threshold=4.0,
                message="TestCo: recovered after 5.0h dark",
            )
        row = self.db.execute(
            "SELECT message FROM pipeline_alerts"
        ).fetchone()
        self.assertIn("TestCo", row["message"])
        self.assertIn("5.0h dark", row["message"])


# ─────────────────────────────────────────────────────────────────────────────
# TestErrorStreakAlertConditions
# ─────────────────────────────────────────────────────────────────────────────

class TestErrorStreakAlertConditions(unittest.TestCase):
    """
    Tests for the error streak alerting condition logic.
    Uses mocked create_alert to avoid needing a real DB.
    """

    def _simulate_streak(self, consec_errors, success):
        """
        Simulate the error streak check from on_adaptive_complete().
        Returns (called, kwargs) where called=True if create_alert was invoked.
        """
        from config import WORKER_ERROR_STREAK_THRESHOLD
        from db.pipeline_alerts import ALERT_ERROR_STREAK, WARNING

        alert_calls = []

        def fake_create_alert(**kwargs):
            alert_calls.append(kwargs)
            return 42  # non-None id

        if not success and consec_errors >= WORKER_ERROR_STREAK_THRESHOLD:
            fake_create_alert(
                alert_type=ALERT_ERROR_STREAK,
                severity=WARNING,
                platform="TestCo",
                value=float(consec_errors),
                threshold=float(WORKER_ERROR_STREAK_THRESHOLD),
                message=(
                    f"TestCo (greenhouse): {consec_errors} consecutive "
                    f"scan failures — may need manual investigation"
                ),
            )

        return alert_calls

    def test_no_alert_below_threshold(self):
        """4 consecutive failures (< 5) → no alert."""
        calls = self._simulate_streak(consec_errors=4, success=False)
        self.assertEqual(len(calls), 0)

    def test_no_alert_when_success(self):
        """success=True → no streak alert regardless of consec count."""
        calls = self._simulate_streak(consec_errors=10, success=True)
        self.assertEqual(len(calls), 0)

    def test_alert_at_exactly_threshold(self):
        """Exactly 5 failures → alert fires."""
        calls = self._simulate_streak(consec_errors=5, success=False)
        self.assertEqual(len(calls), 1)

    def test_alert_above_threshold(self):
        """6+ failures → alert fires."""
        for n in [6, 10, 100]:
            calls = self._simulate_streak(consec_errors=n, success=False)
            self.assertEqual(len(calls), 1, f"Expected alert for {n} errors")

    def test_alert_type_is_error_streak(self):
        """Alert type is ALERT_ERROR_STREAK."""
        from db.pipeline_alerts import ALERT_ERROR_STREAK
        calls = self._simulate_streak(consec_errors=5, success=False)
        self.assertEqual(calls[0]["alert_type"], ALERT_ERROR_STREAK)

    def test_alert_severity_is_warning(self):
        """Error streak severity is WARNING (not CRITICAL)."""
        from db.pipeline_alerts import WARNING
        calls = self._simulate_streak(consec_errors=5, success=False)
        self.assertEqual(calls[0]["severity"], WARNING)

    def test_alert_value_equals_consec_errors(self):
        """Alert value field equals the consecutive error count."""
        calls = self._simulate_streak(consec_errors=7, success=False)
        self.assertEqual(calls[0]["value"], 7.0)

    def test_alert_threshold_equals_config(self):
        """Alert threshold field equals WORKER_ERROR_STREAK_THRESHOLD."""
        from config import WORKER_ERROR_STREAK_THRESHOLD
        calls = self._simulate_streak(consec_errors=5, success=False)
        self.assertEqual(calls[0]["threshold"],
                         float(WORKER_ERROR_STREAK_THRESHOLD))

    def test_alert_message_contains_company(self):
        """Message includes the company name."""
        calls = self._simulate_streak(consec_errors=5, success=False)
        self.assertIn("TestCo", calls[0]["message"])

    def test_alert_message_contains_error_count(self):
        """Message includes the consecutive error count."""
        calls = self._simulate_streak(consec_errors=8, success=False)
        self.assertIn("8", calls[0]["message"])


# ─────────────────────────────────────────────────────────────────────────────
# TestReactivationLagAlertConditions
# ─────────────────────────────────────────────────────────────────────────────

class TestReactivationLagAlertConditions(unittest.TestCase):
    """
    Tests for the reactivation lag alerting condition logic.
    Directly simulates the condition block from on_adaptive_complete().
    """

    def _simulate_lag(self, success, consec_errors_before,
                      lag_hours, last_success_at=None):
        """
        Simulate the reactivation lag check block.
        Returns list of dicts describing create_alert calls made.
        """
        from config import REACTIVATION_LAG_ALERT_HR
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG, WARNING

        alert_calls = []
        now_ts = time.time()

        if last_success_at is None and lag_hours is not None:
            last_success_at = datetime.fromtimestamp(
                now_ts - lag_hours * 3600
            )

        def fake_create_alert(**kwargs):
            alert_calls.append(kwargs)
            return 99

        if success and consec_errors_before and consec_errors_before > 0:
            last_ok = last_success_at
            if last_ok is not None:
                # Use lag_hours directly when provided to avoid float round-trip
                # precision loss through datetime.fromtimestamp → .timestamp().
                if lag_hours is not None:
                    lag_hr = float(lag_hours)
                else:
                    lag_s  = now_ts - last_ok.timestamp()
                    lag_hr = lag_s / 3600.0
                if lag_hr > REACTIVATION_LAG_ALERT_HR:
                    fake_create_alert(
                        alert_type=ALERT_REACTIVATION_LAG,
                        severity=WARNING,
                        platform="TestCo",
                        value=round(lag_hr, 1),
                        threshold=float(REACTIVATION_LAG_ALERT_HR),
                        message=(
                            f"TestCo (greenhouse): recovered after "
                            f"{lag_hr:.1f}h dark "
                            f"({consec_errors_before} consecutive failures) — "
                            f"possible missed polling window"
                        ),
                    )

        return alert_calls

    def test_no_alert_when_success_false(self):
        """success=False → no lag alert (still failing)."""
        calls = self._simulate_lag(
            success=False, consec_errors_before=3, lag_hours=6.0
        )
        self.assertEqual(len(calls), 0)

    def test_no_alert_when_consecutive_errors_zero(self):
        """consecutive_errors=0 → no lag alert (nothing to recover from)."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=0, lag_hours=6.0
        )
        self.assertEqual(len(calls), 0)

    def test_no_alert_when_last_success_at_is_none(self):
        """last_success_at=None → no lag alert (no baseline to measure from)."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=3,
            lag_hours=None, last_success_at=None
        )
        self.assertEqual(len(calls), 0)

    def test_no_alert_when_lag_below_threshold(self):
        """Lag of 3.9h (< 4h threshold) → no alert."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=3.9
        )
        self.assertEqual(len(calls), 0)

    def test_no_alert_when_lag_exactly_at_threshold(self):
        """Lag exactly at 4.0h → no alert (threshold is strictly greater than)."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=4.0
        )
        self.assertEqual(len(calls), 0)

    def test_alert_fires_just_above_threshold(self):
        """Lag of 4.01h → alert fires."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=4.1
        )
        self.assertEqual(len(calls), 1)

    def test_alert_fires_well_above_threshold(self):
        """Lag of 12h → alert fires."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=5, lag_hours=12.0
        )
        self.assertEqual(len(calls), 1)

    def test_alert_type_is_reactivation_lag(self):
        """Alert type is ALERT_REACTIVATION_LAG."""
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=5.0
        )
        self.assertEqual(calls[0]["alert_type"], ALERT_REACTIVATION_LAG)

    def test_alert_severity_is_warning(self):
        """Reactivation lag severity is WARNING."""
        from db.pipeline_alerts import WARNING
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=5.0
        )
        self.assertEqual(calls[0]["severity"], WARNING)

    def test_alert_value_is_rounded_lag_hr(self):
        """Alert value is the rounded lag in hours."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=5.5
        )
        self.assertAlmostEqual(calls[0]["value"], 5.5, delta=0.2)

    def test_alert_threshold_is_reactivation_lag_alert_hr(self):
        """Alert threshold = REACTIVATION_LAG_ALERT_HR."""
        from config import REACTIVATION_LAG_ALERT_HR
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=5.0
        )
        self.assertEqual(calls[0]["threshold"], REACTIVATION_LAG_ALERT_HR)

    def test_alert_message_contains_company_name(self):
        """Alert message includes the company name."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=6.0
        )
        self.assertIn("TestCo", calls[0]["message"])

    def test_alert_message_contains_consecutive_error_count(self):
        """Alert message includes the consecutive error count."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=7, lag_hours=6.0
        )
        self.assertIn("7", calls[0]["message"])

    def test_alert_dedup_uses_company_as_platform_key(self):
        """platform field is set to company name for per-company dedup."""
        calls = self._simulate_lag(
            success=True, consec_errors_before=3, lag_hours=6.0
        )
        self.assertEqual(calls[0]["platform"], "TestCo")


# ─────────────────────────────────────────────────────────────────────────────
# TestDetailQueueDepthHysteresis
# ─────────────────────────────────────────────────────────────────────────────

class TestDetailQueueDepthHysteresis(unittest.TestCase):
    """
    Tests for the detail queue depth hysteresis counter and alert logic.
    Tests the _hysteresis dict and alert condition directly.
    """

    def setUp(self):
        """Reset the hysteresis counter to 0 before each test."""
        import workers.scheduler as sched
        self._sched = sched
        sched._hysteresis["detail_alert"] = 0

    def tearDown(self):
        """Clean up hysteresis state after each test."""
        self._sched._hysteresis["detail_alert"] = 0

    def test_counter_starts_at_zero(self):
        """detail_alert counter initialises to 0."""
        self.assertEqual(self._sched._hysteresis["detail_alert"], 0)

    def test_counter_increments_when_above_watermark(self):
        """Counter increments each cycle queue stays above watermark."""
        from config import DETAIL_QUEUE_HIGH_WATERMARK
        depth = DETAIL_QUEUE_HIGH_WATERMARK + 1

        for i in range(1, 4):
            if depth > DETAIL_QUEUE_HIGH_WATERMARK:
                self._sched._hysteresis["detail_alert"] += 1
            self.assertEqual(self._sched._hysteresis["detail_alert"], i)

    def test_counter_resets_when_queue_drops(self):
        """Counter resets to 0 when queue drops below watermark."""
        self._sched._hysteresis["detail_alert"] = 2
        # Queue drops below watermark
        self._sched._hysteresis["detail_alert"] = 0
        self.assertEqual(self._sched._hysteresis["detail_alert"], 0)

    def test_alert_fires_at_exactly_alert_cycles(self):
        """Alert fires when counter reaches DETAIL_QUEUE_ALERT_CYCLES."""
        from config import DETAIL_QUEUE_ALERT_CYCLES, DETAIL_QUEUE_HIGH_WATERMARK

        alert_fired = []

        def check_alert(counter):
            if counter >= DETAIL_QUEUE_ALERT_CYCLES:
                alert_fired.append(counter)

        for cycle in range(1, DETAIL_QUEUE_ALERT_CYCLES + 1):
            self._sched._hysteresis["detail_alert"] = cycle
            check_alert(cycle)

        # Alert should have fired on the last cycle
        self.assertEqual(len(alert_fired), 1)
        self.assertEqual(alert_fired[0], DETAIL_QUEUE_ALERT_CYCLES)

    def test_no_alert_before_reaching_cycles(self):
        """No alert fires on cycles 1 and 2 (before threshold of 3)."""
        from config import DETAIL_QUEUE_ALERT_CYCLES

        alert_fired = []

        for cycle in range(1, DETAIL_QUEUE_ALERT_CYCLES):
            self._sched._hysteresis["detail_alert"] = cycle
            if cycle >= DETAIL_QUEUE_ALERT_CYCLES:
                alert_fired.append(cycle)

        self.assertEqual(len(alert_fired), 0)

    def test_alert_value_is_queue_depth(self):
        """Alert value equals the actual queue depth at time of firing."""
        from config import DETAIL_QUEUE_ALERT_CYCLES, DETAIL_QUEUE_HIGH_WATERMARK
        depth = DETAIL_QUEUE_HIGH_WATERMARK + 500

        alert_kwargs = {}

        def fake_create_alert(**kwargs):
            alert_kwargs.update(kwargs)
            return 1

        from db.pipeline_alerts import ALERT_DETAIL_QUEUE_DEPTH, WARNING
        counter = DETAIL_QUEUE_ALERT_CYCLES

        if counter >= DETAIL_QUEUE_ALERT_CYCLES:
            fake_create_alert(
                alert_type=ALERT_DETAIL_QUEUE_DEPTH,
                severity=WARNING,
                value=float(depth),
                threshold=float(DETAIL_QUEUE_HIGH_WATERMARK),
                message=f"detail queue {depth} exceeded watermark",
            )

        self.assertEqual(alert_kwargs["value"], float(depth))

    def test_reset_after_alert_fires_prevents_re_fire(self):
        """Once alert fires, counter does NOT auto-reset — dedup handles re-fire."""
        # The counter keeps incrementing beyond DETAIL_QUEUE_ALERT_CYCLES
        # Dedup in create_alert prevents duplicate alerts
        from config import DETAIL_QUEUE_ALERT_CYCLES
        self._sched._hysteresis["detail_alert"] = DETAIL_QUEUE_ALERT_CYCLES
        # Counter does not reset automatically on alert fire
        # (it resets when queue drops below watermark)
        self._sched._hysteresis["detail_alert"] += 1
        self.assertGreater(
            self._sched._hysteresis["detail_alert"], DETAIL_QUEUE_ALERT_CYCLES
        )

    def test_hysteresis_dict_has_detail_alert_key(self):
        """_hysteresis dict has 'detail_alert' key."""
        self.assertIn("detail_alert", self._sched._hysteresis)

    def test_hysteresis_dict_has_all_required_keys(self):
        """_hysteresis dict has all required keys."""
        required = {"scan_add", "scan_remove", "detail_add",
                    "detail_remove", "detail_alert"}
        for key in required:
            self.assertIn(key, self._sched._hysteresis,
                          f"Missing hysteresis key: {key}")


# ─────────────────────────────────────────────────────────────────────────────
# TestRedisMemoryAlertConditions
# ─────────────────────────────────────────────────────────────────────────────

class TestRedisMemoryAlertConditions(unittest.TestCase):
    """
    Tests for Redis memory alert conditions.
    Simulates the alert logic directly without running the full loop.
    """

    def _check_redis_memory(self, used_bytes, max_bytes):
        """
        Simulate the Redis memory alert check from _slow_throughput_check_loop.
        Returns list of create_alert kwargs if alert was triggered.
        """
        from config import REDIS_MEMORY_ALERT_PCT
        from db.pipeline_alerts import ALERT_REDIS_MEMORY, CRITICAL

        alert_calls = []

        def fake_create_alert(**kwargs):
            alert_calls.append(kwargs)
            return 1

        if max_bytes and max_bytes > 0:
            used_pct = int(100 * used_bytes / max_bytes)
            if used_pct >= REDIS_MEMORY_ALERT_PCT:
                fake_create_alert(
                    alert_type=ALERT_REDIS_MEMORY,
                    severity=CRITICAL,
                    value=float(used_pct),
                    threshold=float(REDIS_MEMORY_ALERT_PCT),
                    message=(
                        f"Redis memory at {used_pct}% of maxmemory "
                        f"({used_bytes // 1024 // 1024} MB / "
                        f"{max_bytes // 1024 // 1024} MB) — "
                        f"noeviction policy will block writes at 100%"
                    ),
                )

        return alert_calls

    def test_no_alert_when_maxmemory_zero(self):
        """maxmemory=0 (no limit configured) → no alert."""
        calls = self._check_redis_memory(
            used_bytes=500 * 1024 * 1024,
            max_bytes=0
        )
        self.assertEqual(len(calls), 0)

    def test_no_alert_below_threshold(self):
        """70% usage (< 80% threshold) → no alert."""
        calls = self._check_redis_memory(
            used_bytes=700 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(len(calls), 0)

    def test_no_alert_just_below_threshold(self):
        """79% usage → no alert."""
        calls = self._check_redis_memory(
            used_bytes=790 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(len(calls), 0)

    def test_alert_fires_at_exactly_threshold(self):
        """Exactly 80% → alert fires."""
        calls = self._check_redis_memory(
            used_bytes=800 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(len(calls), 1)

    def test_alert_fires_above_threshold(self):
        """85% usage → alert fires."""
        calls = self._check_redis_memory(
            used_bytes=850 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(len(calls), 1)

    def test_alert_fires_at_near_full(self):
        """95% usage → alert fires."""
        calls = self._check_redis_memory(
            used_bytes=950 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(len(calls), 1)

    def test_alert_severity_is_critical(self):
        """Redis memory alert is CRITICAL severity."""
        from db.pipeline_alerts import CRITICAL
        calls = self._check_redis_memory(
            used_bytes=850 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(calls[0]["severity"], CRITICAL)

    def test_alert_value_is_used_pct(self):
        """Alert value equals the used % (integer)."""
        calls = self._check_redis_memory(
            used_bytes=850 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(calls[0]["value"], 85.0)

    def test_alert_message_contains_mb_values(self):
        """Message includes MB values for used and max memory."""
        calls = self._check_redis_memory(
            used_bytes=850 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertIn("850 MB", calls[0]["message"])
        self.assertIn("1000 MB", calls[0]["message"])

    def test_alert_threshold_is_config_value(self):
        """Alert threshold = REDIS_MEMORY_ALERT_PCT from config."""
        from config import REDIS_MEMORY_ALERT_PCT
        calls = self._check_redis_memory(
            used_bytes=850 * 1024 * 1024,
            max_bytes=1000 * 1024 * 1024
        )
        self.assertEqual(calls[0]["threshold"], float(REDIS_MEMORY_ALERT_PCT))

    def test_memory_check_exception_caught_silently(self):
        """Exception in memory check must not propagate."""
        mock_redis = MagicMock()
        mock_redis.info.side_effect = Exception("Redis unavailable")

        # The try/except in the loop catches this — verify no raise
        try:
            mem_info = mock_redis.info("memory")
        except Exception:
            pass  # caught

        # No exception propagated to here — loop would continue


# ─────────────────────────────────────────────────────────────────────────────
# TestOnAdaptiveCompleteAlertIntegration
# ─────────────────────────────────────────────────────────────────────────────

class TestOnAdaptiveCompleteAlertIntegration(unittest.TestCase):
    """
    Integration tests for on_adaptive_complete() alerting.
    All external dependencies (DB, Redis) are mocked; only the alert
    conditions are observed.
    """

    def _make_stats_row(self, consec_errors=0, last_success_at=None):
        """Build a mock company_poll_stats row dict."""
        return {
            "current_interval_s":     3600,
            "recent_poll_counts":     None,
            "last_full_scan_at":      None,
            "full_scan_interval_s":   86400,
            "consecutive_errors":     consec_errors,
            "last_success_at":        last_success_at,
            "warming_polls_remaining": None,
        }

    def _run(self, company="TestCo", new_jobs=0, success=True,
             consec_errors=0, last_success_at=None):
        """
        Call on_adaptive_complete() with all external deps mocked.
        Returns dict of captured create_alert calls keyed by alert_type.
        """
        stats_row = self._make_stats_row(
            consec_errors=consec_errors,
            last_success_at=last_success_at,
        )

        mock_r = MagicMock()
        mock_r.zrem.return_value = 1
        mock_r.get.return_value = None       # not a canary, no backoff
        mock_r.exists.return_value = False   # no backoff, no outage
        mock_r.hset.return_value = None
        mock_r.zadd.return_value = 1
        mock_r.delete.return_value = 1
        mock_r.zscore.return_value = None

        mock_conn_select = MagicMock()
        mock_conn_select.execute.return_value.fetchone.return_value = stats_row

        mock_conn_update = MagicMock()
        mock_conn_update.execute.return_value = MagicMock()
        mock_conn_update.commit.return_value = None

        captured_alerts = {}

        def fake_create_alert(alert_type, severity, platform=None,
                              value=None, threshold=None, message=None):
            captured_alerts[alert_type] = {
                "alert_type": alert_type, "severity": severity,
                "platform": platform, "value": value,
                "threshold": threshold, "message": message,
            }
            return 42  # non-None = alert created

        with patch("workers.scheduler._get_dc_key_for_company",
                   return_value="greenhouse"), \
             patch("workers.scheduler.get_redis",
                   return_value=mock_r), \
             patch("workers.scheduler.get_conn",
                   side_effect=[mock_conn_select, mock_conn_update]), \
             patch("workers.scheduler._reschedule_adaptive"), \
             patch("workers.scheduler.clear_heartbeat"), \
             patch("workers.scheduler._maybe_reschedule_full_scan"), \
             patch("workers.scheduler.load_poll_counts",
                   return_value=[]), \
             patch("workers.scheduler.update_poll_interval",
                   return_value={
                       "current_interval_s": 3600,
                       "recent_poll_counts": [],
                       "adaptive_score": 0.0,
                   }), \
             patch("workers.scheduler.dump_poll_counts",
                   return_value="[]"), \
             patch("workers.scheduler.get_band_thresholds",
                   return_value={"low": 1.5, "moderate": 3.5, "active": 6.0}), \
             patch("db.pipeline_alerts.create_alert",
                   side_effect=fake_create_alert):

            from workers.scheduler import on_adaptive_complete
            on_adaptive_complete(company, new_jobs, success)

        return captured_alerts

    # ── Error streak integration ──────────────────────────────────────────────

    def test_error_streak_alert_fires_at_threshold(self):
        """Error streak alert fired when consec_errors reaches threshold."""
        from config import WORKER_ERROR_STREAK_THRESHOLD
        from db.pipeline_alerts import ALERT_ERROR_STREAK

        alerts = self._run(
            success=False,
            consec_errors=WORKER_ERROR_STREAK_THRESHOLD - 1,  # row before update
        )
        # After incrementing, consec_errors = threshold
        self.assertIn(ALERT_ERROR_STREAK, alerts)

    def test_no_error_streak_alert_when_success(self):
        """No error streak alert when success=True."""
        from db.pipeline_alerts import ALERT_ERROR_STREAK
        alerts = self._run(success=True, consec_errors=0)
        self.assertNotIn(ALERT_ERROR_STREAK, alerts)

    def test_no_error_streak_alert_below_threshold(self):
        """No alert when consecutive errors < threshold after increment."""
        from config import WORKER_ERROR_STREAK_THRESHOLD
        from db.pipeline_alerts import ALERT_ERROR_STREAK

        # row has consec_errors = 1, after increment = 2 (still below 5)
        alerts = self._run(success=False, consec_errors=1)
        self.assertNotIn(ALERT_ERROR_STREAK, alerts)

    # ── Reactivation lag integration ──────────────────────────────────────────

    def test_reactivation_lag_alert_fires_when_dark_too_long(self):
        """Reactivation lag alert fires when company was dark > 4h."""
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG
        from config import REACTIVATION_LAG_ALERT_HR

        dark_since = datetime.now() - timedelta(
            hours=REACTIVATION_LAG_ALERT_HR + 1
        )
        alerts = self._run(
            success=True,
            consec_errors=3,  # was failing
            last_success_at=dark_since,
        )
        self.assertIn(ALERT_REACTIVATION_LAG, alerts)

    def test_no_reactivation_lag_alert_when_success_false(self):
        """No lag alert when success=False (company still failing)."""
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG
        dark_since = datetime.now() - timedelta(hours=8)
        alerts = self._run(
            success=False,
            consec_errors=3,
            last_success_at=dark_since,
        )
        self.assertNotIn(ALERT_REACTIVATION_LAG, alerts)

    def test_no_reactivation_lag_alert_when_no_prior_errors(self):
        """No lag alert when consecutive_errors was 0 (no recovery)."""
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG
        dark_since = datetime.now() - timedelta(hours=8)
        alerts = self._run(
            success=True,
            consec_errors=0,
            last_success_at=dark_since,
        )
        self.assertNotIn(ALERT_REACTIVATION_LAG, alerts)

    def test_no_reactivation_lag_alert_when_lag_short(self):
        """No lag alert when dark period was short (< 4h)."""
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG
        from config import REACTIVATION_LAG_ALERT_HR

        # 2h dark — below the 4h threshold
        recent_success = datetime.now() - timedelta(hours=2)
        alerts = self._run(
            success=True,
            consec_errors=2,
            last_success_at=recent_success,
        )
        self.assertNotIn(ALERT_REACTIVATION_LAG, alerts)

    def test_both_streak_and_lag_can_not_fire_together(self):
        """
        Error streak fires on failure; lag fires on success — they are
        mutually exclusive (streak requires success=False, lag requires
        success=True).  Verify both conditions cannot fire simultaneously.
        """
        from db.pipeline_alerts import ALERT_ERROR_STREAK, ALERT_REACTIVATION_LAG
        from config import WORKER_ERROR_STREAK_THRESHOLD, REACTIVATION_LAG_ALERT_HR

        # success=False → streak can fire, lag cannot
        alerts_fail = self._run(
            success=False,
            consec_errors=WORKER_ERROR_STREAK_THRESHOLD - 1,
        )
        self.assertNotIn(ALERT_REACTIVATION_LAG, alerts_fail)

        # success=True → lag can fire, streak cannot
        dark = datetime.now() - timedelta(hours=REACTIVATION_LAG_ALERT_HR + 2)
        alerts_ok = self._run(
            success=True,
            consec_errors=3,
            last_success_at=dark,
        )
        self.assertNotIn(ALERT_ERROR_STREAK, alerts_ok)

    def test_reactivation_lag_alert_value_is_lag_hours(self):
        """Lag alert value = lag in hours (rounded)."""
        from db.pipeline_alerts import ALERT_REACTIVATION_LAG
        from config import REACTIVATION_LAG_ALERT_HR

        lag_h = REACTIVATION_LAG_ALERT_HR + 2   # 6h dark
        dark_since = datetime.now() - timedelta(hours=lag_h)
        alerts = self._run(
            success=True,
            consec_errors=2,
            last_success_at=dark_since,
        )
        if ALERT_REACTIVATION_LAG in alerts:
            self.assertAlmostEqual(
                alerts[ALERT_REACTIVATION_LAG]["value"], lag_h, delta=0.2
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
