"""
tests/test_api_health_p95.py
─────────────────────────────────────────────────────────────────────────────
Tests for db/api_health.py — query_p95_response_ms(scan_type).

All DB interactions mocked; no real DB connection needed.

The function:
    - Queries AVG(max_response_ms) from api_health for the last 7 days
    - Applies multiplier: 1.5× for "listing_scan", 3.0× for everything else
    - Returns conservative defaults when no data is available
    - Is used by adaptive_loop / fullscan_loop for XAUTOCLAIM idle timeout

Coverage map
────────────
  TestQueryP95NoData
    · listing_scan + no rows → 30 000 ms default
    · full_scan + no rows → 120 000 ms default
    · listing_scan + avg_max_ms = None → 30 000
    · listing_scan + avg_max_ms = 0 → 30 000
    · full_scan + avg_max_ms = None → 120 000
    · full_scan + avg_max_ms = 0 → 120 000

  TestQueryP95WithData
    · listing_scan, avg=10 000 → 15 000 (10 000 × 1.5)
    · full_scan, avg=10 000 → 30 000 (10 000 × 3.0)
    · listing_scan, avg=20 000 → 30 000 (20 000 × 1.5)
    · full_scan, avg=40 000 → 120 000 (40 000 × 3.0)
    · unknown scan_type, avg=10 000 → 30 000 (uses 3.0× fallback)

  TestQueryP95ReturnType
    · Always returns int (never float)
    · Never returns 0
    · Never returns negative

  TestQueryP95FloorEnforcement
    · Very small avg (100 ms) → at least 1 000 ms floor for listing_scan
    · Very small avg (100 ms) → at least 1 000 ms floor for full_scan

  TestQueryP95ErrorHandling
    · DB exception during execute → returns 30 000 (safe fallback)
    · DB exception during get_conn → returns 30 000
    · conn.close() always called in finally block

  TestQueryP95DBQuery
    · SQL query uses correct time window (last 7 days)
    · context = 'normal' filter applied in query
    · max_response_ms > 0 filter applied
"""

import sys
import os
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _make_conn_mock(avg_max_ms=None, raise_on_execute=False):
    """Build a mock DB connection that returns a row with the given avg."""
    conn = MagicMock()

    if raise_on_execute:
        conn.execute.side_effect = Exception("DB error")
    else:
        row = MagicMock()
        row.__getitem__ = lambda self, key: avg_max_ms if key == "avg_max_ms" else None
        row.__bool__    = lambda self: True
        conn.execute.return_value.fetchone.return_value = row if avg_max_ms is not None else None

    return conn


# ─────────────────────────────────────────────────────────────────────────────
# TestQueryP95NoData
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryP95NoData(unittest.TestCase):

    def _run(self, scan_type, avg_max_ms=None, row_is_none=False):
        conn = MagicMock()
        if row_is_none:
            conn.execute.return_value.fetchone.return_value = None
        else:
            row = {"avg_max_ms": avg_max_ms}
            conn.execute.return_value.fetchone.return_value = row
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            return query_p95_response_ms(scan_type)

    def test_listing_scan_no_rows_returns_30000(self):
        """listing_scan + no rows → 30 000 ms."""
        result = self._run("listing_scan", row_is_none=True)
        self.assertEqual(result, 30_000)

    def test_full_scan_no_rows_returns_120000(self):
        """full_scan + no rows → 120 000 ms."""
        result = self._run("full_scan", row_is_none=True)
        self.assertEqual(result, 120_000)

    def test_listing_scan_avg_none_returns_30000(self):
        """listing_scan + avg_max_ms=None → 30 000."""
        result = self._run("listing_scan", avg_max_ms=None)
        self.assertEqual(result, 30_000)

    def test_listing_scan_avg_zero_returns_30000(self):
        """listing_scan + avg_max_ms=0 → 30 000 (treated as no data)."""
        result = self._run("listing_scan", avg_max_ms=0)
        self.assertEqual(result, 30_000)

    def test_full_scan_avg_none_returns_120000(self):
        """full_scan + avg_max_ms=None → 120 000."""
        result = self._run("full_scan", avg_max_ms=None)
        self.assertEqual(result, 120_000)

    def test_full_scan_avg_zero_returns_120000(self):
        """full_scan + avg_max_ms=0 → 120 000."""
        result = self._run("full_scan", avg_max_ms=0)
        self.assertEqual(result, 120_000)


# ─────────────────────────────────────────────────────────────────────────────
# TestQueryP95WithData
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryP95WithData(unittest.TestCase):

    def _run(self, scan_type, avg_max_ms):
        conn = MagicMock()
        row = {"avg_max_ms": avg_max_ms}
        conn.execute.return_value.fetchone.return_value = row
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            return query_p95_response_ms(scan_type)

    def test_listing_scan_10000ms(self):
        """listing_scan, avg=10 000 → 15 000 (× 1.5)."""
        self.assertEqual(self._run("listing_scan", 10_000), 15_000)

    def test_full_scan_10000ms(self):
        """full_scan, avg=10 000 → 30 000 (× 3.0)."""
        self.assertEqual(self._run("full_scan", 10_000), 30_000)

    def test_listing_scan_20000ms(self):
        """listing_scan, avg=20 000 → 30 000 (× 1.5)."""
        self.assertEqual(self._run("listing_scan", 20_000), 30_000)

    def test_full_scan_40000ms(self):
        """full_scan, avg=40 000 → 120 000 (× 3.0)."""
        self.assertEqual(self._run("full_scan", 40_000), 120_000)

    def test_unknown_scan_type_uses_3x_multiplier(self):
        """Unknown scan_type → 3.0× multiplier (non listing_scan branch)."""
        result = self._run("other_type", 10_000)
        self.assertEqual(result, 30_000)   # 10_000 * 3.0 = 30_000

    def test_listing_scan_large_value(self):
        """Larger avg → multiplied correctly."""
        result = self._run("listing_scan", 60_000)
        self.assertEqual(result, 90_000)   # 60_000 * 1.5

    def test_full_scan_large_value(self):
        result = self._run("full_scan", 60_000)
        self.assertEqual(result, 180_000)  # 60_000 * 3.0


# ─────────────────────────────────────────────────────────────────────────────
# TestQueryP95ReturnType
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryP95ReturnType(unittest.TestCase):

    def _run(self, scan_type, avg_max_ms):
        conn = MagicMock()
        row  = {"avg_max_ms": avg_max_ms}
        conn.execute.return_value.fetchone.return_value = row
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            return query_p95_response_ms(scan_type)

    def test_always_returns_int_with_data(self):
        """Result is always int (not float) when data present."""
        result = self._run("listing_scan", 10_000)
        self.assertIsInstance(result, int)

    def test_always_returns_int_no_data(self):
        """Result is always int (not float) when no data."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = None
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            result = query_p95_response_ms("listing_scan")
        self.assertIsInstance(result, int)

    def test_never_returns_zero(self):
        """Result is never 0."""
        for st in ("listing_scan", "full_scan"):
            conn = MagicMock()
            conn.execute.return_value.fetchone.return_value = None
            with patch("db.connection.get_conn", return_value=conn):
                from db.api_health import query_p95_response_ms
                result = query_p95_response_ms(st)
            self.assertGreater(result, 0, msg=f"scan_type={st!r} returned 0")

    def test_never_negative(self):
        """Result is always positive."""
        for st in ("listing_scan", "full_scan"):
            conn = MagicMock()
            conn.execute.return_value.fetchone.return_value = None
            with patch("db.connection.get_conn", return_value=conn):
                from db.api_health import query_p95_response_ms
                result = query_p95_response_ms(st)
            self.assertGreater(result, 0)


# ─────────────────────────────────────────────────────────────────────────────
# TestQueryP95FloorEnforcement
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryP95FloorEnforcement(unittest.TestCase):
    """max(1_000, int(avg * multiplier)) ensures a minimum 1s result."""

    def _run(self, scan_type, avg_max_ms):
        conn = MagicMock()
        row  = {"avg_max_ms": avg_max_ms}
        conn.execute.return_value.fetchone.return_value = row
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            return query_p95_response_ms(scan_type)

    def test_listing_scan_tiny_avg_floored_to_1000(self):
        """listing_scan + avg=100ms → at least 1 000ms (floor enforced)."""
        result = self._run("listing_scan", 100)
        self.assertGreaterEqual(result, 1_000)

    def test_full_scan_tiny_avg_floored_to_1000(self):
        """full_scan + avg=100ms → at least 1 000ms."""
        result = self._run("full_scan", 100)
        self.assertGreaterEqual(result, 1_000)

    def test_exactly_at_floor_boundary(self):
        """avg=667ms × 1.5 = 1000.5 → int(1000.5) = 1000 >= floor(1000)."""
        result = self._run("listing_scan", 667)
        self.assertGreaterEqual(result, 1_000)


# ─────────────────────────────────────────────────────────────────────────────
# TestQueryP95ErrorHandling
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryP95ErrorHandling(unittest.TestCase):

    def test_db_execute_exception_returns_30000(self):
        """Exception during conn.execute → returns 30 000 (safe fallback)."""
        conn = MagicMock()
        conn.execute.side_effect = Exception("connection reset")
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            result = query_p95_response_ms("listing_scan")
        self.assertEqual(result, 30_000)

    def test_get_conn_exception_full_scan_returns_120000(self):
        """Exception in get_conn itself with full_scan → returns 120 000 (correct default)."""
        with patch("db.connection.get_conn", side_effect=Exception("DB down")):
            from db.api_health import query_p95_response_ms
            result = query_p95_response_ms("full_scan")
        self.assertEqual(result, 120_000)

    def test_get_conn_exception_listing_scan_returns_30000(self):
        """Exception in get_conn itself with listing_scan → returns 30 000."""
        with patch("db.connection.get_conn", side_effect=Exception("DB down")):
            from db.api_health import query_p95_response_ms
            result = query_p95_response_ms("listing_scan")
        self.assertEqual(result, 30_000)

    def test_conn_close_called_on_success(self):
        """conn.close() called in finally block even on success."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = None
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            query_p95_response_ms("listing_scan")
        conn.close.assert_called_once()

    def test_conn_close_called_on_exception(self):
        """conn.close() called even when execute raises (once per attempt; retry=2)."""
        conn = MagicMock()
        conn.execute.side_effect = Exception("timeout")
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            query_p95_response_ms("listing_scan")
        # The retry loop calls _query() twice on failure; each attempt closes its
        # own connection — the mock returns the same object so close() == 2 total.
        self.assertEqual(conn.close.call_count, 2)


# ─────────────────────────────────────────────────────────────────────────────
# TestQueryP95DBQuery
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryP95DBQuery(unittest.TestCase):

    def _capture_sql(self, scan_type):
        """Run the function and capture the SQL + args passed to conn.execute."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = None
        with patch("db.connection.get_conn", return_value=conn):
            from db.api_health import query_p95_response_ms
            query_p95_response_ms(scan_type)
        return conn.execute.call_args

    def test_query_uses_7_day_window(self):
        """SQL passes a date 7 days ago (not older, not newer)."""
        from datetime import date, timedelta
        call_args = self._capture_sql("listing_scan")
        # The second positional arg is (platforms_list, since_date_str)
        # since = (date.today() - timedelta(days=7)).isoformat()
        args = call_args[0]   # positional args to execute
        sql_params = args[1]  # (list_of_platforms, since_str)
        since_str = sql_params[1]
        expected_since = (date.today() - timedelta(days=7)).isoformat()
        self.assertEqual(since_str, expected_since)

    def test_query_filters_context_normal(self):
        """SQL includes context = 'normal' filter."""
        call_args = self._capture_sql("listing_scan")
        sql_text = call_args[0][0]
        self.assertIn("context", sql_text)
        self.assertIn("normal", sql_text)

    def test_query_filters_positive_max_response_ms(self):
        """SQL includes max_response_ms > 0 filter."""
        call_args = self._capture_sql("listing_scan")
        sql_text = call_args[0][0]
        self.assertIn("max_response_ms", sql_text)
        self.assertIn("> 0", sql_text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
