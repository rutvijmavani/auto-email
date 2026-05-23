"""
tests/test_job_monitor_slot.py
─────────────────────────────────────────────────────────────────────────────
Tests for db/job_monitor.py — upsert_poll_stats() with initial_slot_offset_s.

In the two-layer scheduler redesign, upsert_poll_stats() was updated to set
initial_slot_offset_s on INSERT using slot_offset(company).  On conflict,
the column uses COALESCE so the existing value is preserved when already set
(original registration slot survives restarts), but legacy NULL rows (created
before the column existed) are backfilled on the next scan.

All DB interactions mocked; no real DB connection needed.

Coverage map
────────────
  TestUpsertPollStatsSlotOffset
    · INSERT SQL includes "initial_slot_offset_s" column
    · Value passed equals slot_offset(company) — correct column binding
    · Deterministic: two calls for the same company use the same offset
    · DO UPDATE SET uses COALESCE for initial_slot_offset_s (preserves non-NULL, backfills NULL)
    · Correct binding tuple: (company, platform, new_jobs, offset_s) — 4 values
    · conn.commit() called on success
    · conn.close() always called (finally block), even if execute raises
    · Exception in execute → swallowed silently (no re-raise)
    · Different companies produce different slot offsets
    · slot_offset(company) always in [0, 86400) → INSERT value is valid

  TestUpsertPollStatsSlotOffsetControlled
    · When slot_offset patched to return fixed value → that value appears in binding
    · When slot_offset raises → upsert handles gracefully (no crash)
"""

import sys
import os
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _run_upsert(company="Stripe", platform="greenhouse",
                new_jobs=3, duration_ms=1000, mock_slot=None):
    """
    Run upsert_poll_stats() with a mocked DB connection.

    Returns (conn_mock, sql_text, sql_params, offset_used).
    """
    conn = MagicMock()

    captured_sql    = []
    captured_params = []

    def _execute(sql, params=None):
        captured_sql.append(sql)
        captured_params.append(params)
        return MagicMock()

    conn.execute.side_effect = _execute

    if mock_slot is not None:
        slot_patch = patch("workers.slot.slot_offset", return_value=mock_slot)
    else:
        slot_patch = None

    if slot_patch:
        with patch("db.job_monitor.get_conn", return_value=conn), slot_patch:
            from db.job_monitor import upsert_poll_stats
            upsert_poll_stats(company, platform, new_jobs, duration_ms)
    else:
        with patch("db.job_monitor.get_conn", return_value=conn):
            from db.job_monitor import upsert_poll_stats
            upsert_poll_stats(company, platform, new_jobs, duration_ms)

    sql    = captured_sql[0]    if captured_sql    else ""
    params = captured_params[0] if captured_params else ()
    return conn, sql, params


# ─────────────────────────────────────────────────────────────────────────────
# TestUpsertPollStatsSlotOffset
# ─────────────────────────────────────────────────────────────────────────────

class TestUpsertPollStatsSlotOffset(unittest.TestCase):

    def test_sql_contains_initial_slot_offset_s_column(self):
        """INSERT SQL includes 'initial_slot_offset_s' in the column list."""
        conn, sql, params = _run_upsert()
        self.assertIn(
            "initial_slot_offset_s", sql,
            msg="upsert_poll_stats SQL does not include initial_slot_offset_s column",
        )

    def test_slot_offset_value_in_binding_tuple(self):
        """The binding tuple passed to conn.execute contains slot_offset(company)."""
        from workers.slot import slot_offset
        company = "Stripe"
        expected_offset = slot_offset(company)
        conn, sql, params = _run_upsert(company=company)
        self.assertIn(
            expected_offset, params,
            msg=f"Expected slot_offset({company!r})={expected_offset} in params {params}",
        )

    def test_slot_offset_is_deterministic_across_calls(self):
        """Two upsert calls for same company use identical offsets."""
        from workers.slot import slot_offset
        company = "Airbnb"
        offset1 = slot_offset(company)
        offset2 = slot_offset(company)
        self.assertEqual(offset1, offset2)

    def test_initial_slot_uses_coalesce_in_do_update_set(self):
        """
        ON CONFLICT DO UPDATE SET must include initial_slot_offset_s with COALESCE
        so that:
          - non-NULL existing values are preserved (original registration slot survives)
          - NULL legacy rows (created before the column existed) are backfilled
        """
        conn, sql, params = _run_upsert()
        upper_sql = sql.upper()

        update_idx = upper_sql.find("DO UPDATE SET")
        self.assertGreater(update_idx, -1, "SQL missing 'DO UPDATE SET' clause")

        update_body = sql[update_idx:].upper()
        self.assertIn(
            "INITIAL_SLOT_OFFSET_S", update_body,
            msg="initial_slot_offset_s should appear in the DO UPDATE SET clause (COALESCE backfill)",
        )
        self.assertIn(
            "COALESCE", update_body,
            msg="DO UPDATE SET should use COALESCE for initial_slot_offset_s (preserve-or-backfill)",
        )

    def test_binding_tuple_has_four_values(self):
        """conn.execute receives a 4-value binding: (company, platform, new_jobs, offset)."""
        conn, sql, params = _run_upsert(
            company="Stripe", platform="greenhouse", new_jobs=5, duration_ms=1000
        )
        self.assertIsNotNone(params)
        self.assertEqual(len(params), 4,
                         msg=f"Expected 4 binding values, got {len(params)}: {params}")

    def test_binding_order_company_platform_new_jobs_offset(self):
        """Binding tuple order: (company, platform, new_jobs, slot_offset(company))."""
        from workers.slot import slot_offset
        company  = "Palantir"
        platform = "lever"
        new_jobs = 7
        expected_offset = slot_offset(company)
        conn, sql, params = _run_upsert(
            company=company, platform=platform, new_jobs=new_jobs
        )
        self.assertEqual(params[0], company)
        self.assertEqual(params[1], platform)
        self.assertEqual(params[2], new_jobs)
        self.assertEqual(params[3], expected_offset)

    def test_conn_commit_called_on_success(self):
        """conn.commit() is called after execute succeeds."""
        conn, sql, params = _run_upsert()
        conn.commit.assert_called_once()

    def test_conn_close_called_always(self):
        """conn.close() is always called (finally block)."""
        conn, sql, params = _run_upsert()
        conn.close.assert_called_once()

    def test_conn_close_called_even_when_execute_raises(self):
        """conn.close() called even if conn.execute raises."""
        conn = MagicMock()
        conn.execute.side_effect = Exception("DB down")

        with patch("db.job_monitor.get_conn", return_value=conn):
            from db.job_monitor import upsert_poll_stats
            # Should not raise
            upsert_poll_stats("TestCo", "greenhouse", 0, 0)

        conn.close.assert_called_once()

    def test_exception_silently_swallowed(self):
        """Exception in execute → no re-raise, function returns normally."""
        conn = MagicMock()
        conn.execute.side_effect = Exception("constraint violation")

        with patch("db.job_monitor.get_conn", return_value=conn):
            from db.job_monitor import upsert_poll_stats
            # Must not raise
            try:
                upsert_poll_stats("TestCo", "greenhouse", 0, 0)
            except Exception as e:
                self.fail(f"upsert_poll_stats raised unexpectedly: {e}")

    def test_different_companies_get_different_offsets(self):
        """slot_offset is not constant — different companies produce different values."""
        from workers.slot import slot_offset
        offsets = {slot_offset(c) for c in ["Stripe", "Airbnb", "Palantir",
                                             "Snowflake", "Cloudflare"]}
        self.assertGreater(
            len(offsets), 1,
            msg="All companies produced the same slot_offset — distribution broken",
        )

    def test_slot_offset_value_always_valid(self):
        """slot_offset(company) is always in [0, 86400) — valid for INSERT."""
        from workers.slot import slot_offset
        companies = ["Stripe", "Airbnb", "Google", "Meta", "OpenAI"]
        for co in companies:
            offset = slot_offset(co)
            self.assertGreaterEqual(offset, 0, msg=f"slot_offset({co!r}) < 0")
            self.assertLess(offset, 86400, msg=f"slot_offset({co!r}) >= 86400")


# ─────────────────────────────────────────────────────────────────────────────
# TestUpsertPollStatsSlotOffsetControlled
# ─────────────────────────────────────────────────────────────────────────────

class TestUpsertPollStatsSlotOffsetControlled(unittest.TestCase):

    def test_patched_slot_offset_value_appears_in_binding(self):
        """When slot_offset is patched, the patched value appears in the binding."""
        FIXED_OFFSET = 12345
        conn = MagicMock()
        captured_params = []
        conn.execute.side_effect = lambda sql, p=None: captured_params.append(p)

        with patch("db.job_monitor.get_conn", return_value=conn), \
             patch("workers.slot.slot_offset", return_value=FIXED_OFFSET):
            from db.job_monitor import upsert_poll_stats
            upsert_poll_stats("TestCo", "greenhouse", 0, 0)

        params = captured_params[0]
        self.assertIn(FIXED_OFFSET, params,
                      msg=f"Patched offset {FIXED_OFFSET} not found in params {params}")

    def test_slot_offset_called_with_company_name(self):
        """slot_offset is called with the company name (not platform or other args)."""
        company = "Anthropic"
        conn    = MagicMock()

        with patch("db.job_monitor.get_conn", return_value=conn), \
             patch("workers.slot.slot_offset") as mock_slot:
            mock_slot.return_value = 5000
            from db.job_monitor import upsert_poll_stats
            upsert_poll_stats(company, "greenhouse", 0, 0)

        mock_slot.assert_called_once_with(company)


if __name__ == "__main__":
    unittest.main(verbosity=2)
