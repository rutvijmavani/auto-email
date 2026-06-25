"""
tests/test_detail_worker_inflight.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/detail_worker.py — at-least-once delivery via per-PID
LMOVE processing lists.

Coverage map
────────────
  TestInflightKeyConstants
    · After run_worker() initialises them, _INFLIGHT_ADAPTIVE contains PID
    · After run_worker() initialises them, _INFLIGHT_FULLSCAN contains PID
    · Keys follow the {queue}:inflight:{pid} pattern
    · _INFLIGHT_KEY map has both source queue keys after initialisation
    · _INFLIGHT_KEY[REDIS_DETAIL_ADAPTIVE] == _INFLIGHT_ADAPTIVE
    · _INFLIGHT_KEY[REDIS_DETAIL_FULLSCAN] == _INFLIGHT_FULLSCAN

  TestRecoverStuckJobs
    · No inflight keys found → no crash, no unnecessary work
    · Own PID key is always skipped (even if heartbeat absent)
    · Dead peer adaptive items → rpop/rpush back to adaptive source
    · Dead peer fullscan items → rpop/rpush back to fullscan source
    · Live peer items (heartbeat present) → never touched
    · Multiple items → all recovered until list exhausted (while-loop)
    · Loop stops when rpop returns None (no infinite loop)

  TestPopWithInflight
    · Both queues empty → returns None after timeout expires
    · Adaptive tried before fullscan (first lmove call is on adaptive source)
    · Returns (REDIS_DETAIL_ADAPTIVE, payload) when adaptive has an item
    · Returns (REDIS_DETAIL_FULLSCAN, payload) when only fullscan has an item
    · Adaptive takes priority: when both have items, adaptive is returned
    · LMOVE destination is the per-PID inflight key (not the source queue)
    · Adaptive inflight key used for adaptive source
    · Fullscan inflight key used for fullscan source
"""

import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_FAKE_PID = 77777   # synthetic PID used by all tests that need globals set


def _init_inflight_globals(pid=_FAKE_PID):
    """
    Simulate what run_worker() does: set per-PID inflight globals on the module.
    Call this in setUp(); call _reset_inflight_globals() in tearDown().
    """
    import workers.detail_worker as dw
    from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN
    dw._INFLIGHT_ADAPTIVE = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{pid}"
    dw._INFLIGHT_FULLSCAN = f"{REDIS_DETAIL_FULLSCAN}:inflight:{pid}"
    dw._INFLIGHT_KEY = {
        REDIS_DETAIL_ADAPTIVE: dw._INFLIGHT_ADAPTIVE,
        REDIS_DETAIL_FULLSCAN: dw._INFLIGHT_FULLSCAN,
    }


def _reset_inflight_globals():
    """Reset module-level inflight globals back to empty (as at import time)."""
    import workers.detail_worker as dw
    dw._INFLIGHT_ADAPTIVE = ""
    dw._INFLIGHT_FULLSCAN = ""
    dw._INFLIGHT_KEY = {}


# ─────────────────────────────────────────────────────────────────────────────
# TestInflightKeyConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestInflightKeyConstants(unittest.TestCase):
    """
    Inflight key names are set by run_worker() to include the worker's PID.
    Module-level defaults are empty strings; tests simulate the run_worker()
    initialisation with a fixed fake PID.
    """

    def setUp(self):
        _init_inflight_globals(_FAKE_PID)

    def tearDown(self):
        _reset_inflight_globals()

    def test_inflight_adaptive_contains_pid(self):
        """_INFLIGHT_ADAPTIVE embeds the worker PID after initialisation."""
        from workers.detail_worker import _INFLIGHT_ADAPTIVE
        self.assertIn(str(_FAKE_PID), _INFLIGHT_ADAPTIVE,
                      "_INFLIGHT_ADAPTIVE must contain the worker PID")

    def test_inflight_adaptive_follows_pattern(self):
        """_INFLIGHT_ADAPTIVE = '{REDIS_DETAIL_ADAPTIVE}:inflight:{pid}'."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_ADAPTIVE
        expected_prefix = f"{REDIS_DETAIL_ADAPTIVE}:inflight:"
        self.assertTrue(
            _INFLIGHT_ADAPTIVE.startswith(expected_prefix),
            f"Expected prefix {expected_prefix!r}, got {_INFLIGHT_ADAPTIVE!r}",
        )

    def test_inflight_fullscan_contains_pid(self):
        """_INFLIGHT_FULLSCAN embeds the worker PID after initialisation."""
        from workers.detail_worker import _INFLIGHT_FULLSCAN
        self.assertIn(str(_FAKE_PID), _INFLIGHT_FULLSCAN,
                      "_INFLIGHT_FULLSCAN must contain the worker PID")

    def test_inflight_fullscan_follows_pattern(self):
        """_INFLIGHT_FULLSCAN = '{REDIS_DETAIL_FULLSCAN}:inflight:{pid}'."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_FULLSCAN
        expected_prefix = f"{REDIS_DETAIL_FULLSCAN}:inflight:"
        self.assertTrue(
            _INFLIGHT_FULLSCAN.startswith(expected_prefix),
            f"Expected prefix {expected_prefix!r}, got {_INFLIGHT_FULLSCAN!r}",
        )

    def test_inflight_key_map_contains_adaptive(self):
        """_INFLIGHT_KEY maps REDIS_DETAIL_ADAPTIVE after initialisation."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_KEY
        self.assertIn(REDIS_DETAIL_ADAPTIVE, _INFLIGHT_KEY)

    def test_inflight_key_map_contains_fullscan(self):
        """_INFLIGHT_KEY maps REDIS_DETAIL_FULLSCAN after initialisation."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_KEY
        self.assertIn(REDIS_DETAIL_FULLSCAN, _INFLIGHT_KEY)

    def test_inflight_key_map_adaptive_value_matches_constant(self):
        """_INFLIGHT_KEY[REDIS_DETAIL_ADAPTIVE] == _INFLIGHT_ADAPTIVE."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_KEY, _INFLIGHT_ADAPTIVE
        self.assertEqual(_INFLIGHT_KEY[REDIS_DETAIL_ADAPTIVE], _INFLIGHT_ADAPTIVE)

    def test_inflight_key_map_fullscan_value_matches_constant(self):
        """_INFLIGHT_KEY[REDIS_DETAIL_FULLSCAN] == _INFLIGHT_FULLSCAN."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_KEY, _INFLIGHT_FULLSCAN
        self.assertEqual(_INFLIGHT_KEY[REDIS_DETAIL_FULLSCAN], _INFLIGHT_FULLSCAN)

    def test_two_different_pids_produce_different_keys(self):
        """Per-PID design: different PIDs produce non-overlapping inflight keys."""
        from config import REDIS_DETAIL_ADAPTIVE
        key_a = f"{REDIS_DETAIL_ADAPTIVE}:inflight:1234"
        key_b = f"{REDIS_DETAIL_ADAPTIVE}:inflight:5678"
        self.assertNotEqual(key_a, key_b,
                            "Different PIDs must produce different inflight keys")


# ─────────────────────────────────────────────────────────────────────────────
# TestRecoverStuckJobs
# ─────────────────────────────────────────────────────────────────────────────

class TestRecoverStuckJobs(unittest.TestCase):
    """
    _recover_stuck_jobs(r, own_pid) scans for per-PID inflight keys, skips
    live peers (heartbeat present) and own key, and drains dead peers' items.
    """

    _OWN_PID  = 1000
    _DEAD_PID = 9001   # peer with no heartbeat
    _LIVE_PID = 9002   # peer with active heartbeat

    def _build_redis_mock(
        self,
        dead_adaptive_items=None,
        dead_fullscan_items=None,
        include_live_peer=False,
        include_own_key=False,
    ):
        """
        Build a Redis mock for _recover_stuck_jobs tests.

        Returns (r, dead_adp_key, dead_fs_key, live_adp_key).
        """
        from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN

        dead_adaptive_items = list(dead_adaptive_items or [])
        dead_fullscan_items = list(dead_fullscan_items or [])

        dead_adp_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._DEAD_PID}"
        dead_fs_key  = f"{REDIS_DETAIL_FULLSCAN}:inflight:{self._DEAD_PID}"
        live_adp_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._LIVE_PID}"
        own_adp_key  = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._OWN_PID}"

        r = MagicMock()

        # scan: return per-PID keys appropriate to each pattern
        def _scan(cursor, match="*", count=100):
            adp_pat = f"{REDIS_DETAIL_ADAPTIVE}:inflight:"
            fs_pat  = f"{REDIS_DETAIL_FULLSCAN}:inflight:"
            if adp_pat in match:
                keys = [dead_adp_key.encode()]
                if include_live_peer:
                    keys.append(live_adp_key.encode())
                if include_own_key:
                    keys.append(own_adp_key.encode())
                return (0, keys)
            if fs_pat in match:
                return (0, [dead_fs_key.encode()])
            return (0, [])
        r.scan.side_effect = _scan

        # exists: dead PID → 0 (no heartbeat), live PID → 1 (heartbeat active)
        def _exists(key):
            key_s = key.decode() if isinstance(key, bytes) else key
            if str(self._DEAD_PID) in key_s:
                return 0
            if str(self._LIVE_PID) in key_s:
                return 1
            return 0
        r.exists.side_effect = _exists

        # lmove: simulate draining dead peer's inflight items one at a time,
        # returning None when the list is exhausted.
        adp_items = list(dead_adaptive_items)
        fs_items  = list(dead_fullscan_items)

        def _lmove(src, dst, src_dir, dst_dir):
            src_s = src.decode() if isinstance(src, bytes) else src
            if src_s == dead_adp_key and adp_items:
                return adp_items.pop(0)
            if src_s == dead_fs_key and fs_items:
                return fs_items.pop(0)
            return None
        r.lmove.side_effect = _lmove

        return r, dead_adp_key, dead_fs_key, live_adp_key

    # ── Basic / empty ─────────────────────────────────────────────────────────

    def test_no_inflight_keys_no_crash(self):
        """No inflight keys found → returns cleanly without error."""
        r = MagicMock()
        r.scan.return_value = (0, [])
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)

    def test_no_inflight_keys_lmove_never_called(self):
        """No inflight keys → lmove is never called."""
        r = MagicMock()
        r.scan.return_value = (0, [])
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)
        r.lmove.assert_not_called()

    # ── Own PID is always skipped ─────────────────────────────────────────────

    def test_own_pid_key_never_drained(self):
        """Own PID inflight key is skipped — even when heartbeat is absent."""
        r, _, _, _ = self._build_redis_mock(include_own_key=True)
        from config import REDIS_DETAIL_ADAPTIVE
        own_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._OWN_PID}"
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)

        own_calls = [c for c in r.lmove.call_args_list
                     if (c[0][0].decode() if isinstance(c[0][0], bytes)
                         else c[0][0]) == own_key]
        self.assertEqual(len(own_calls), 0,
                         "Must never drain own PID's inflight key")

    # ── Dead peer recovery ────────────────────────────────────────────────────

    def test_dead_peer_adaptive_item_moved_to_adaptive_source(self):
        """Dead peer's adaptive inflight item → lmove back to adaptive source."""
        from config import REDIS_DETAIL_ADAPTIVE
        r, dead_adp_key, _, _ = self._build_redis_mock(
            dead_adaptive_items=[b"job1"]
        )
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)

        lmove_calls = [(c[0][0], c[0][1]) for c in r.lmove.call_args_list]
        self.assertTrue(any(
            (src.decode() if isinstance(src, bytes) else src) == dead_adp_key
            and dst == REDIS_DETAIL_ADAPTIVE
            for src, dst in lmove_calls
        ), "Expected lmove(dead_adp_key → adaptive source) for dead peer item")

    def test_dead_peer_fullscan_item_moved_to_fullscan_source(self):
        """Dead peer's fullscan inflight item → lmove back to fullscan source."""
        from config import REDIS_DETAIL_FULLSCAN
        r, _, dead_fs_key, _ = self._build_redis_mock(
            dead_fullscan_items=[b"job2"]
        )
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)

        lmove_calls = [(c[0][0], c[0][1]) for c in r.lmove.call_args_list]
        self.assertTrue(any(
            (src.decode() if isinstance(src, bytes) else src) == dead_fs_key
            and dst == REDIS_DETAIL_FULLSCAN
            for src, dst in lmove_calls
        ), "Expected lmove(dead_fs_key → fullscan source) for dead peer item")

    def test_multiple_items_all_recovered(self):
        """All items in a dead peer's inflight are drained (loop runs until None)."""
        r, dead_adp_key, _, _ = self._build_redis_mock(
            dead_adaptive_items=[b"j1", b"j2", b"j3"]
        )
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)

        adp_lmove_calls = [c for c in r.lmove.call_args_list
                           if (c[0][0].decode() if isinstance(c[0][0], bytes)
                               else c[0][0]) == dead_adp_key]
        # 3 items moved + 1 final None check = 4 calls
        self.assertEqual(len(adp_lmove_calls), 4,
                         "Drain loop must run until lmove returns None")

    # ── Live peer is never touched ────────────────────────────────────────────

    def test_live_peer_items_not_touched(self):
        """Live peer's inflight key (heartbeat present) → rpop never called on it."""
        r, _, _, live_adp_key = self._build_redis_mock(include_live_peer=True)
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)

        live_calls = [c for c in r.lmove.call_args_list
                      if (c[0][0].decode() if isinstance(c[0][0], bytes)
                          else c[0][0]) == live_adp_key]
        self.assertEqual(len(live_calls), 0,
                         "Must not drain a live peer's inflight items")

    def test_heartbeat_check_performed_per_pid(self):
        """r.exists is called for each peer PID found, not for own PID."""
        r, _, _, _ = self._build_redis_mock()
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_PID)

        exists_calls = [c[0][0] for c in r.exists.call_args_list]
        # Should have checked heartbeat for the dead peer
        self.assertTrue(
            any(str(self._DEAD_PID) in str(k) for k in exists_calls),
            "exists() must be called to check the dead peer's heartbeat",
        )
        # Must NOT have checked own PID (it's excluded before exists() is called)
        self.assertFalse(
            any(str(self._OWN_PID) in str(k) for k in exists_calls),
            "exists() must not be called for own PID",
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestPopWithInflight
# ─────────────────────────────────────────────────────────────────────────────

class TestPopWithInflight(unittest.TestCase):
    """
    _pop_with_inflight(r, timeout) returns (source_key, raw_payload) or None.

    Adaptive queue has priority: it is checked first every poll cycle.
    Items are moved atomically from source → per-PID inflight via LMOVE.
    """

    def setUp(self):
        """Initialise per-PID inflight globals as run_worker() would."""
        _init_inflight_globals(_FAKE_PID)

    def tearDown(self):
        _reset_inflight_globals()

    def test_both_empty_returns_none_on_timeout(self):
        """Both queues empty → returns None after timeout expires."""
        r = MagicMock()
        r.lmove.return_value = None
        from workers.detail_worker import _pop_with_inflight
        result = _pop_with_inflight(r, timeout=0.01)
        self.assertIsNone(result)

    def test_adaptive_checked_before_fullscan(self):
        """Adaptive source is the first lmove source tried each poll cycle."""
        from config import REDIS_DETAIL_ADAPTIVE
        r = MagicMock()
        first_src = [None]

        def _lmove(src, dst, sd, dd):
            if first_src[0] is None:
                first_src[0] = src
            return None

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        _pop_with_inflight(r, timeout=0.01)
        self.assertEqual(first_src[0], REDIS_DETAIL_ADAPTIVE)

    def test_returns_adaptive_source_key_and_payload(self):
        """Returns (REDIS_DETAIL_ADAPTIVE, raw_payload) when adaptive has an item."""
        from config import REDIS_DETAIL_ADAPTIVE

        r = MagicMock()

        def _lmove(src, dst, sd, dd):
            if src == REDIS_DETAIL_ADAPTIVE:
                return b'{"job_id": "123"}'
            return None

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        result = _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], REDIS_DETAIL_ADAPTIVE)
        self.assertEqual(result[1], b'{"job_id": "123"}')

    def test_returns_fullscan_when_adaptive_empty(self):
        """Returns fullscan item when adaptive queue is empty."""
        from config import REDIS_DETAIL_FULLSCAN

        r = MagicMock()

        def _lmove(src, dst, sd, dd):
            if src == REDIS_DETAIL_FULLSCAN:
                return b'{"job_id": "456"}'
            return None

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        result = _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], REDIS_DETAIL_FULLSCAN)
        self.assertEqual(result[1], b'{"job_id": "456"}')

    def test_adaptive_takes_priority_over_fullscan(self):
        """Adaptive is returned when both queues have items."""
        from config import REDIS_DETAIL_ADAPTIVE

        r = MagicMock()

        def _lmove(src, dst, sd, dd):
            return b'{"job_id": "999"}'   # both queues non-empty

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        result = _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], REDIS_DETAIL_ADAPTIVE,
                         "Adaptive must take priority over fullscan")

    def test_adaptive_item_moved_to_per_pid_adaptive_inflight(self):
        """Adaptive pop: LMOVE destination is the per-PID _INFLIGHT_ADAPTIVE."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_ADAPTIVE

        r = MagicMock()
        pop_call = [None]

        def _lmove(src, dst, sd, dd):
            if src == REDIS_DETAIL_ADAPTIVE and pop_call[0] is None:
                pop_call[0] = (src, dst)
                return b'{"job_id": "1"}'
            return None

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(pop_call[0])
        self.assertEqual(pop_call[0][1], _INFLIGHT_ADAPTIVE,
                         "Adaptive pop destination must be per-PID _INFLIGHT_ADAPTIVE")
        self.assertIn(str(_FAKE_PID), _INFLIGHT_ADAPTIVE,
                      "Inflight key must embed the worker PID")

    def test_fullscan_item_moved_to_per_pid_fullscan_inflight(self):
        """Fullscan pop: LMOVE destination is the per-PID _INFLIGHT_FULLSCAN."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_FULLSCAN

        r = MagicMock()
        fs_pop_call = [None]

        def _lmove(src, dst, sd, dd):
            if src == REDIS_DETAIL_FULLSCAN and fs_pop_call[0] is None:
                fs_pop_call[0] = (src, dst)
                return b'{"job_id": "2"}'
            return None

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(fs_pop_call[0])
        self.assertEqual(fs_pop_call[0][1], _INFLIGHT_FULLSCAN,
                         "Fullscan pop destination must be per-PID _INFLIGHT_FULLSCAN")
        self.assertIn(str(_FAKE_PID), _INFLIGHT_FULLSCAN,
                      "Inflight key must embed the worker PID")

    def test_return_type_is_tuple(self):
        """Return value is a 2-tuple (source_key, raw_payload) on success."""
        from config import REDIS_DETAIL_ADAPTIVE

        r = MagicMock()

        def _lmove(src, dst, sd, dd):
            if src == REDIS_DETAIL_ADAPTIVE:
                return b"raw"
            return None

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        result = _pop_with_inflight(r, timeout=0.1)

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
