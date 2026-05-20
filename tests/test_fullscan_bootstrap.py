"""
tests/test_fullscan_bootstrap.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/fullscan.py — _ensure_consumer_group() and
_bootstrap_warming_adaptive(), plus the integration between _run_fullscan()
and the bootstrap (is_first_fullscan check).

All Redis/DB interactions are mocked.  No live Redis or DB connections.

Coverage map
────────────
  TestFullscanEnsureConsumerGroup
    · xgroup_create called with REDIS_STREAM_FULLSCAN, STREAM_CONSUMER_GROUP,
      id="$", mkstream=True
    · BUSYGROUP exception → silently swallowed (no re-raise, no warning)
    · Other exception → warning logged, not re-raised
    · Success → no exception propagates

  TestBootstrapWarmingAdaptive
    · initial_slot_offset_s set in DB row → uses that value as offset_s
    · initial_slot_offset_s = None in DB row → falls back to slot_offset(row["id"])
    · No DB row at all → falls back to slot_offset(company)
    · first_poll_at > now_ts → r.zadd called with first_poll_at (no +86400)
    · first_poll_at <= now_ts → r.zadd called with first_poll_at + 86400
    · warming_polls_remaining = WARMING_POLLS_COUNT written to DB UPDATE
    · r.zadd called with correct args: (REDIS_POLL_ADAPTIVE, {company: first_poll_at})
    · DB UPDATE commits (conn.commit() called)
    · Both conn.close() calls made (two separate DB queries: SELECT then UPDATE)
    · slot_offset(row["id"]) used for legacy rows (not slot_offset(company))
    · slot_offset(company) used when no row exists

  TestBootstrapWarmingAdaptiveSlot
    · Offset values are always in [0, 86400) → first_poll_at is bounded
    · Same company always produces the same first_poll_at (deterministic)

  TestRunFullscanBootstrapIntegration
    · is_first_fullscan = True (last_poll_at is None before scan) →
      _bootstrap_warming_adaptive IS called
    · is_first_fullscan = False (last_poll_at is not None) →
      _bootstrap_warming_adaptive NOT called
    · _bootstrap_warming_adaptive failure → fallback ZADD still called,
      no exception propagates from _run_fullscan
"""

import sys
import os
import time
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# TestFullscanEnsureConsumerGroup
# ─────────────────────────────────────────────────────────────────────────────

class TestFullscanEnsureConsumerGroup(unittest.TestCase):

    def _run(self, side_effect=None):
        r = MagicMock()
        if side_effect is not None:
            r.xgroup_create.side_effect = side_effect
        with patch("workers.fullscan.get_redis", return_value=r):
            # Import fresh each time to avoid module-level caching
            import importlib
            import workers.fullscan as fs
            importlib.reload(fs)
            fs._ensure_consumer_group(r)
        return r

    def test_xgroup_create_called_with_correct_args(self):
        """xgroup_create called with stream key, group, id='$', mkstream=True."""
        from config import REDIS_STREAM_FULLSCAN, STREAM_CONSUMER_GROUP
        r = MagicMock()
        with patch("workers.fullscan.get_redis", return_value=r):
            from workers.fullscan import _ensure_consumer_group
            _ensure_consumer_group(r)
        r.xgroup_create.assert_called_once_with(
            REDIS_STREAM_FULLSCAN,
            STREAM_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )

    def test_busygroup_silently_ignored(self):
        """BUSYGROUP exception → no re-raise and no warning logged."""
        r = MagicMock()
        r.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group name already exists")
        with patch("workers.fullscan.logger") as mock_log:
            from workers.fullscan import _ensure_consumer_group
            # Must not raise
            _ensure_consumer_group(r)
        # No warning should have been logged for BUSYGROUP
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        busygroup_warnings = [c for c in warning_calls if "xgroup_create" in c.lower()
                              or "BUSYGROUP" in c]
        self.assertEqual(len(busygroup_warnings), 0)

    def test_other_exception_logs_warning_not_raised(self):
        """Non-BUSYGROUP exception → warning logged but not re-raised."""
        r = MagicMock()
        r.xgroup_create.side_effect = Exception("NOAUTH Authentication required")
        with patch("workers.fullscan.logger") as mock_log:
            from workers.fullscan import _ensure_consumer_group
            _ensure_consumer_group(r)   # must not raise
        mock_log.warning.assert_called_once()

    def test_success_no_exception(self):
        """Successful xgroup_create → no exception."""
        r = MagicMock()
        r.xgroup_create.return_value = "OK"
        from workers.fullscan import _ensure_consumer_group
        _ensure_consumer_group(r)   # must not raise


# ─────────────────────────────────────────────────────────────────────────────
# Helpers for _bootstrap_warming_adaptive tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_row(initial_slot_offset_s="PRESENT", row_id=42):
    """
    Build a mock DB row for company_poll_stats.

    initial_slot_offset_s:
        "PRESENT" → sets a specific value (default 27291 from slot_offset(1))
        None      → row exists but column is NULL
    row_id: the "id" column value
    """
    row = MagicMock()
    if initial_slot_offset_s == "PRESENT":
        row.__getitem__ = lambda self, key: (
            27291 if key == "initial_slot_offset_s"
            else row_id if key == "id"
            else None
        )
        row.__bool__ = lambda self: True
    elif initial_slot_offset_s is None:
        row.__getitem__ = lambda self, key: (
            None if key == "initial_slot_offset_s"
            else row_id if key == "id"
            else None
        )
        row.__bool__ = lambda self: True
    return row


def _run_bootstrap(company="Stripe", db_row="PRESENT", row_id=42,
                   now_ts=None, today_midnight_ts=None):
    """
    Run _bootstrap_warming_adaptive with mocked DB + time + pytz.

    db_row: "PRESENT" (has offset), None (offset is NULL), "MISSING" (no row)
    """
    from config import REDIS_POLL_ADAPTIVE

    r = MagicMock()

    # Control time
    fixed_now = now_ts if now_ts is not None else 1_700_000_000.0
    fixed_midnight = today_midnight_ts if today_midnight_ts is not None else 1_699_920_000.0

    call_count = [0]

    def _make_conn():
        conn = MagicMock()
        if db_row == "MISSING":
            conn.execute.return_value.fetchone.return_value = None
        elif db_row is None:
            conn.execute.return_value.fetchone.return_value = _make_row(
                initial_slot_offset_s=None, row_id=row_id)
        else:
            conn.execute.return_value.fetchone.return_value = _make_row(
                initial_slot_offset_s="PRESENT", row_id=row_id)
        call_count[0] += 1
        return conn

    conns_made = []

    def _track_conn():
        c = _make_conn()
        conns_made.append(c)
        return c

    # Build a fake "today_midnight" datetime whose .timestamp() returns fixed_midnight
    import datetime as _dt_module

    class FakeMidnight:
        def timestamp(self):
            return fixed_midnight
        def replace(self, **kwargs):
            return self

    class FakeEasternNow:
        def replace(self, **kwargs):
            return FakeMidnight()

    fake_tz  = MagicMock()
    fake_tz_inst = MagicMock()

    with patch("workers.fullscan.get_conn", side_effect=_track_conn), \
         patch("workers.fullscan.time") as mock_time, \
         patch("workers.fullscan.pytz") as mock_pytz:

        mock_time.time.return_value = fixed_now

        mock_pytz.timezone.return_value = fake_tz
        # datetime.now(tz) → FakeEasternNow
        import workers.fullscan as fs_module
        # Patch the _dt inside the function's local import
        with patch("builtins.__import__", wraps=__import__):
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive(company, r)

    return r, conns_made


# ─────────────────────────────────────────────────────────────────────────────
# TestBootstrapWarmingAdaptive
# ─────────────────────────────────────────────────────────────────────────────

class TestBootstrapWarmingAdaptive(unittest.TestCase):

    def test_uses_initial_slot_offset_s_when_present(self):
        """When DB row has initial_slot_offset_s, that value is used as offset."""
        from config import WARMING_POLLS_COUNT
        r = MagicMock()

        row = {"initial_slot_offset_s": 30000, "id": 10}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()

        conns = iter([conn1, conn2])

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time, \
             patch("workers.slot.slot_offset") as mock_slot:
            mock_time.time.return_value = 0.0   # midnight = very early, slot=30000 is future
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        # slot_offset should NOT have been called (offset was read from DB)
        mock_slot.assert_not_called()

    def test_falls_back_to_slot_offset_id_when_offset_null(self):
        """When initial_slot_offset_s is NULL, slot_offset(row['id']) is used."""
        r = MagicMock()

        row_id = 99
        row = {"initial_slot_offset_s": None, "id": row_id}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        called_with = []

        def _slot(x):
            called_with.append(x)
            return 12345

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time, \
             patch("workers.slot.slot_offset", side_effect=_slot):
            mock_time.time.return_value = 0.0
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        self.assertIn(row_id, called_with,
                      msg=f"Expected slot_offset({row_id}) called; got {called_with}")

    def test_falls_back_to_slot_offset_company_when_no_row(self):
        """When no DB row, slot_offset(company) is used."""
        r = MagicMock()

        company = "NewCo"
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = None
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        called_with = []

        def _slot(x):
            called_with.append(x)
            return 9999

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time, \
             patch("workers.slot.slot_offset", side_effect=_slot):
            mock_time.time.return_value = 0.0
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive(company, r)

        self.assertIn(company, called_with,
                      msg=f"Expected slot_offset({company!r}) called; got {called_with}")

    def test_future_slot_not_pushed(self):
        """first_poll_at > now_ts → r.zadd called with first_poll_at (no +86400 added)."""
        from config import REDIS_POLL_ADAPTIVE
        r = MagicMock()

        midnight_ts = 1_700_000_000.0
        offset_s    = 3600       # 1h into the day
        now_ts      = midnight_ts + 1800   # only 30 min into day → slot is future

        first_poll_at_expected = midnight_ts + offset_s  # 2h mark

        row = {"initial_slot_offset_s": offset_s, "id": 1}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time:
            mock_time.time.return_value = now_ts
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        r.zadd.assert_called_once()
        zadd_call = r.zadd.call_args
        # zadd(REDIS_POLL_ADAPTIVE, {company: first_poll_at})
        zadd_key   = zadd_call[0][0]
        zadd_scores = zadd_call[0][1]
        self.assertEqual(zadd_key, REDIS_POLL_ADAPTIVE)
        score = list(zadd_scores.values())[0]
        self.assertAlmostEqual(score, first_poll_at_expected, delta=1)

    def test_past_slot_pushed_to_tomorrow(self):
        """first_poll_at <= now_ts → +86400 added to push to tomorrow."""
        from config import REDIS_POLL_ADAPTIVE
        r = MagicMock()

        midnight_ts = 1_700_000_000.0
        offset_s    = 3600       # 1h into the day
        now_ts      = midnight_ts + 7200   # 2h into day → slot already passed

        first_poll_at_expected = midnight_ts + offset_s + 86400

        row = {"initial_slot_offset_s": offset_s, "id": 1}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time:
            mock_time.time.return_value = now_ts
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        zadd_scores = r.zadd.call_args[0][1]
        score = list(zadd_scores.values())[0]
        self.assertAlmostEqual(score, first_poll_at_expected, delta=1)

    def test_warming_polls_remaining_set_to_warming_polls_count(self):
        """DB UPDATE sets warming_polls_remaining = WARMING_POLLS_COUNT (3)."""
        from config import WARMING_POLLS_COUNT
        r = MagicMock()

        row = {"initial_slot_offset_s": 3600, "id": 1}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        update_params = []
        conn2.execute.side_effect = lambda sql, p=None: update_params.append(p)

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time:
            mock_time.time.return_value = 0.0
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        # The UPDATE params: (WARMING_POLLS_COUNT, first_poll_dt, company)
        self.assertTrue(len(update_params) > 0)
        params = update_params[0]
        self.assertEqual(params[0], WARMING_POLLS_COUNT)

    def test_db_update_commits(self):
        """conn.commit() called after UPDATE."""
        r = MagicMock()

        row = {"initial_slot_offset_s": 3600, "id": 1}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time:
            mock_time.time.return_value = 0.0
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        conn2.commit.assert_called_once()

    def test_both_conn_close_called(self):
        """Both DB connections are closed (finally blocks)."""
        r = MagicMock()

        row = {"initial_slot_offset_s": 3600, "id": 1}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time:
            mock_time.time.return_value = 0.0
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        conn1.close.assert_called_once()
        conn2.close.assert_called_once()

    def test_zadd_called_with_poll_adaptive_key(self):
        """r.zadd is called with REDIS_POLL_ADAPTIVE as the key."""
        from config import REDIS_POLL_ADAPTIVE
        r = MagicMock()

        row = {"initial_slot_offset_s": 3600, "id": 1}
        conn1 = MagicMock()
        conn1.execute.return_value.fetchone.return_value = row
        conn2 = MagicMock()
        conns = iter([conn1, conn2])

        with patch("workers.fullscan.get_conn", side_effect=lambda: next(conns)), \
             patch("workers.fullscan.time") as mock_time:
            mock_time.time.return_value = 0.0
            from workers.fullscan import _bootstrap_warming_adaptive
            _bootstrap_warming_adaptive("Stripe", r)

        r.zadd.assert_called_once()
        key_used = r.zadd.call_args[0][0]
        self.assertEqual(key_used, REDIS_POLL_ADAPTIVE)


# ─────────────────────────────────────────────────────────────────────────────
# TestBootstrapWarmingAdaptiveSlot
# ─────────────────────────────────────────────────────────────────────────────

class TestBootstrapWarmingAdaptiveSlot(unittest.TestCase):

    def test_slot_offset_values_always_in_range(self):
        """slot_offset produces values in [0, 86400) for company names."""
        from workers.slot import slot_offset
        for co in ["Stripe", "Airbnb", "Google", "Meta", "Palantir"]:
            o = slot_offset(co)
            self.assertGreaterEqual(o, 0)
            self.assertLess(o, 86400)

    def test_deterministic_per_company(self):
        """Same company always produces the same slot offset."""
        from workers.slot import slot_offset
        for co in ["Stripe", "Airbnb"]:
            self.assertEqual(slot_offset(co), slot_offset(co))


# ─────────────────────────────────────────────────────────────────────────────
# TestRunFullscanBootstrapIntegration
# ─────────────────────────────────────────────────────────────────────────────

class TestRunFullscanBootstrapIntegration(unittest.TestCase):
    """
    Verify that _run_fullscan() calls _bootstrap_warming_adaptive() when
    the company has never had an adaptive scan (last_poll_at is None before
    the scan runs).
    """

    def _make_fs_state(self, last_poll_at):
        return {
            "full_scan_interrupted": False,
            "interrupted_at_page":   None,
            "full_scan_interval_s":  86400,
            "last_poll_at":          last_poll_at,
            "last_full_scan_at":     None,
        }

    def _run_fullscan_patched(self, last_poll_at_value, bootstrap_mock):
        """
        Call _run_fullscan with everything mocked.
        bootstrap_mock is the mock for _bootstrap_warming_adaptive.
        """
        company = "TestCo"
        r = MagicMock()

        # Minimal mocks to let _run_fullscan reach the bootstrap call
        with patch("workers.fullscan._get_fullscan_state",
                   return_value=self._make_fs_state(last_poll_at_value)), \
             patch("workers.fullscan._acquire_lock", return_value=True), \
             patch("workers.fullscan._release_lock"), \
             patch("workers.fullscan.get_company_row",
                   return_value={"company": company, "ats_platform": "greenhouse",
                                 "ats_slug": "test-co", "first_scanned_at": None}), \
             patch("workers.fullscan.get_ats_module") as mock_ats, \
             patch("workers.fullscan.get_config", return_value={}), \
             patch("workers.fullscan.parse_slug", return_value="test-co"), \
             patch("workers.fullscan.set_heartbeat"), \
             patch("workers.fullscan.set_progress"), \
             patch("workers.fullscan.clear_heartbeat"), \
             patch("workers.fullscan._BloomPair") as mock_bloom, \
             patch("workers.fullscan.get_conn") as mock_conn_fn, \
             patch("workers.fullscan._bootstrap_warming_adaptive", bootstrap_mock), \
             patch("workers.fullscan.r") if hasattr(__import__("workers.fullscan", fromlist=["r"]), "r") else patch("builtins.id", wraps=id):
            # Set up ATS module to return empty job list
            mock_ats_inst = MagicMock()
            mock_ats_inst.fetch_jobs.return_value = []
            mock_ats.return_value = mock_ats_inst

            # Set up bloom to not crash
            bloom_inst = MagicMock()
            bloom_inst.old_exists.return_value = False
            mock_bloom.return_value = bloom_inst

            # DB conn for upserts
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = None
            mock_conn_fn.return_value = mock_conn

            from workers.fullscan import _run_fullscan
            _run_fullscan(company, r)

    def test_first_fullscan_calls_bootstrap(self):
        """last_poll_at=None before scan → _bootstrap_warming_adaptive IS called."""
        bootstrap = MagicMock()
        self._run_fullscan_patched(last_poll_at_value=None,
                                   bootstrap_mock=bootstrap)
        bootstrap.assert_called_once()

    def test_non_first_fullscan_skips_bootstrap(self):
        """last_poll_at is not None → _bootstrap_warming_adaptive NOT called."""
        import datetime
        bootstrap = MagicMock()
        self._run_fullscan_patched(
            last_poll_at_value=datetime.datetime(2024, 1, 1),
            bootstrap_mock=bootstrap,
        )
        bootstrap.assert_not_called()

    def test_bootstrap_failure_does_not_propagate(self):
        """
        If _bootstrap_warming_adaptive raises, _run_fullscan logs and falls
        back to a direct ZADD.  The exception must not propagate.
        """
        from config import REDIS_POLL_ADAPTIVE

        bootstrap = MagicMock(side_effect=Exception("DB timeout"))
        r = MagicMock()

        with patch("workers.fullscan._get_fullscan_state",
                   return_value=self._make_fs_state(None)), \
             patch("workers.fullscan._acquire_lock", return_value=True), \
             patch("workers.fullscan._release_lock"), \
             patch("workers.fullscan.get_company_row",
                   return_value={"company": "TestCo", "ats_platform": "greenhouse",
                                 "ats_slug": "test-co", "first_scanned_at": None}), \
             patch("workers.fullscan.get_ats_module") as mock_ats, \
             patch("workers.fullscan.get_config", return_value={}), \
             patch("workers.fullscan.parse_slug", return_value="test-co"), \
             patch("workers.fullscan.set_heartbeat"), \
             patch("workers.fullscan.set_progress"), \
             patch("workers.fullscan.clear_heartbeat"), \
             patch("workers.fullscan._BloomPair") as mock_bloom, \
             patch("workers.fullscan.get_conn") as mock_conn_fn, \
             patch("workers.fullscan._bootstrap_warming_adaptive", bootstrap):
            mock_ats_inst = MagicMock()
            mock_ats_inst.fetch_jobs.return_value = []
            mock_ats.return_value = mock_ats_inst
            bloom_inst = MagicMock()
            bloom_inst.old_exists.return_value = False
            mock_bloom.return_value = bloom_inst
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = None
            mock_conn_fn.return_value = mock_conn

            from workers.fullscan import _run_fullscan
            _run_fullscan("TestCo", r)   # must not raise — bootstrap exc is non-fatal

        # Verify fallback ZADD to poll:adaptive fired (company not lost)
        zadd_calls = [c[0][0] for c in r.zadd.call_args_list]
        self.assertIn(
            REDIS_POLL_ADAPTIVE, zadd_calls,
            msg="Fallback r.zadd(poll:adaptive, ...) not called when bootstrap fails",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
