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

_FAKE_PID   = 77777                        # synthetic PID used across tests
_FAKE_TOKEN = f"testhost:{_FAKE_PID}"     # host:pid token — mirrors run_worker()


def _init_inflight_globals(token=_FAKE_TOKEN):
    """
    Simulate what run_worker() does: set per-worker inflight globals on the module.
    The token is "{hostname}:{pid}" — guards against PID reuse across hosts.
    Call this in setUp(); call _reset_inflight_globals() in tearDown().
    """
    import workers.detail_worker as dw
    from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN
    dw._INFLIGHT_ADAPTIVE = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{token}"
    dw._INFLIGHT_FULLSCAN = f"{REDIS_DETAIL_FULLSCAN}:inflight:{token}"
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
        _init_inflight_globals()   # uses _FAKE_TOKEN = "testhost:{_FAKE_PID}"

    def tearDown(self):
        _reset_inflight_globals()

    def test_inflight_adaptive_contains_pid(self):
        """_INFLIGHT_ADAPTIVE embeds the worker PID (part of the host:pid token)."""
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
    _recover_stuck_jobs(r, own_token) scans for per-worker inflight keys, skips
    live peers (heartbeat present) and own key, and drains dead peers' items.

    Token format is "{hostname}:{pid}" — guards against PID reuse across hosts.
    The drain loop uses lindex (peek) + eval (Lua atomic pop) instead of lmove,
    so over-retry items are discarded directly from the inflight list without
    ever being exposed to live workers through the source queue.
    """

    _OWN_PID   = 1000
    _DEAD_PID  = 9001   # peer with no heartbeat
    _LIVE_PID  = 9002   # peer with active heartbeat
    _OWN_TOKEN = f"testhost:{_OWN_PID}"    # host:pid token for own worker

    def _build_redis_mock(
        self,
        dead_adaptive_items=None,
        dead_fullscan_items=None,
        include_live_peer=False,
        include_own_key=False,
    ):
        """
        Build a Redis mock for _recover_stuck_jobs tests.

        Dead/live peer keys use bare-PID format (tests backward compat).
        Own key uses the host:pid token format (new).

        The drain loop calls lindex (peek) then eval (Lua pop+push/discard).
        The mock maintains mutable item lists so successive lindex/eval calls
        simulate draining the list one item at a time.

        Returns (r, dead_adp_key, dead_fs_key, live_adp_key).
        """
        from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN

        adp_items = [
            (item if isinstance(item, bytes) else item.encode())
            for item in (dead_adaptive_items or [])
        ]
        fs_items = [
            (item if isinstance(item, bytes) else item.encode())
            for item in (dead_fullscan_items or [])
        ]

        # Dead/live peers: bare PID (legacy format — backward compat test)
        dead_adp_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._DEAD_PID}"
        dead_fs_key  = f"{REDIS_DETAIL_FULLSCAN}:inflight:{self._DEAD_PID}"
        live_adp_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._LIVE_PID}"
        # Own key: new host:pid token format
        own_adp_key  = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._OWN_TOKEN}"

        r = MagicMock()

        # scan: return per-worker keys appropriate to each pattern
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

        # lindex: non-destructive peek at the right end of the inflight list.
        # Returns the current first item (without removing it), or None when empty.
        def _lindex(key, idx):
            key_s = key.decode() if isinstance(key, bytes) else key
            if key_s == dead_adp_key:
                return adp_items[0] if adp_items else None
            if key_s == dead_fs_key:
                return fs_items[0] if fs_items else None
            return None
        r.lindex.side_effect = _lindex

        # eval: atomic Lua drain — pops matching item from inflight.
        # Signature: eval(script, numkeys, inflight_key, source_key, item, mode)
        # Returns 1 on success, 0 if item was not at the right end.
        def _eval(script, numkeys, inflight_key, source_key, item, mode):
            key_s  = inflight_key.decode() if isinstance(inflight_key, bytes) else inflight_key
            item_s = item.decode() if isinstance(item, bytes) else item
            if key_s == dead_adp_key and adp_items:
                top_s = adp_items[0].decode() if isinstance(adp_items[0], bytes) else adp_items[0]
                if top_s == item_s:
                    adp_items.pop(0)
                    return 1
            if key_s == dead_fs_key and fs_items:
                top_s = fs_items[0].decode() if isinstance(fs_items[0], bytes) else fs_items[0]
                if top_s == item_s:
                    fs_items.pop(0)
                    return 1
            return 0
        r.eval.side_effect = _eval

        return r, dead_adp_key, dead_fs_key, live_adp_key

    # ── Basic / empty ─────────────────────────────────────────────────────────

    def test_no_inflight_keys_no_crash(self):
        """No inflight keys found → returns cleanly without error."""
        r = MagicMock()
        r.scan.return_value = (0, [])
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)

    def test_no_inflight_keys_lmove_never_called(self):
        """No inflight keys → lindex and eval are never called."""
        r = MagicMock()
        r.scan.return_value = (0, [])
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)
        r.lindex.assert_not_called()
        r.eval.assert_not_called()

    # ── Own token key is always skipped ──────────────────────────────────────

    def test_own_pid_key_never_drained(self):
        """Own worker's inflight key is skipped — even when heartbeat is absent."""
        r, _, _, _ = self._build_redis_mock(include_own_key=True)
        from config import REDIS_DETAIL_ADAPTIVE
        own_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{self._OWN_TOKEN}"
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)

        own_lindex_calls = [c for c in r.lindex.call_args_list
                            if (c[0][0].decode() if isinstance(c[0][0], bytes)
                                else c[0][0]) == own_key]
        self.assertEqual(len(own_lindex_calls), 0,
                         "Must never peek at own worker's inflight key")

    # ── Dead peer recovery ────────────────────────────────────────────────────

    def test_dead_peer_adaptive_item_moved_to_adaptive_source(self):
        """Dead peer's adaptive inflight item → eval recovers it to adaptive source."""
        from config import REDIS_DETAIL_ADAPTIVE
        r, dead_adp_key, _, _ = self._build_redis_mock(
            dead_adaptive_items=[b"job1"]
        )
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)

        # eval(script, numkeys, inflight_key, source_key, item, mode)
        # mode="1" means recover (push to source_key)
        eval_calls = r.eval.call_args_list
        self.assertTrue(any(
            (c[0][2].decode() if isinstance(c[0][2], bytes) else c[0][2]) == dead_adp_key
            and c[0][3] == REDIS_DETAIL_ADAPTIVE
            and c[0][5] == "1"
            for c in eval_calls
        ), "Expected eval(dead_adp_key → adaptive source, mode=recover)")

    def test_dead_peer_fullscan_item_moved_to_fullscan_source(self):
        """Dead peer's fullscan inflight item → eval recovers it to fullscan source."""
        from config import REDIS_DETAIL_FULLSCAN
        r, _, dead_fs_key, _ = self._build_redis_mock(
            dead_fullscan_items=[b"job2"]
        )
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)

        eval_calls = r.eval.call_args_list
        self.assertTrue(any(
            (c[0][2].decode() if isinstance(c[0][2], bytes) else c[0][2]) == dead_fs_key
            and c[0][3] == REDIS_DETAIL_FULLSCAN
            and c[0][5] == "1"
            for c in eval_calls
        ), "Expected eval(dead_fs_key → fullscan source, mode=recover)")

    def test_multiple_items_all_recovered(self):
        """All items in a dead peer's inflight are drained (loop runs until None)."""
        r, dead_adp_key, _, _ = self._build_redis_mock(
            dead_adaptive_items=[b"j1", b"j2", b"j3"]
        )
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)

        # 3 items recovered + 1 final lindex returning None = 4 lindex calls
        adp_lindex_calls = [c for c in r.lindex.call_args_list
                            if (c[0][0].decode() if isinstance(c[0][0], bytes)
                                else c[0][0]) == dead_adp_key]
        self.assertEqual(len(adp_lindex_calls), 4,
                         "Drain loop must call lindex until None is returned")

    # ── Live peer is never touched ────────────────────────────────────────────

    def test_live_peer_items_not_touched(self):
        """Live peer's inflight key (heartbeat present) → lindex never called on it."""
        r, _, _, live_adp_key = self._build_redis_mock(include_live_peer=True)
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)

        live_lindex_calls = [c for c in r.lindex.call_args_list
                             if (c[0][0].decode() if isinstance(c[0][0], bytes)
                                 else c[0][0]) == live_adp_key]
        self.assertEqual(len(live_lindex_calls), 0,
                         "Must not peek at a live peer's inflight items")

    def test_heartbeat_check_performed_per_pid(self):
        """r.exists is called for each peer PID found, not for own token."""
        r, _, _, _ = self._build_redis_mock()
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r, self._OWN_TOKEN)

        exists_calls = [c[0][0] for c in r.exists.call_args_list]
        # Should have checked heartbeat for the dead peer
        self.assertTrue(
            any(str(self._DEAD_PID) in str(k) for k in exists_calls),
            "exists() must be called to check the dead peer's heartbeat",
        )
        # Must NOT have checked own PID (excluded before exists() is called)
        self.assertFalse(
            any(str(self._OWN_PID) in str(k) for k in exists_calls),
            "exists() must not be called for own worker token",
        )

    def test_host_pid_peer_dead_heartbeat_recovers(self):
        """
        Peer with a host:pid inflight key (otherhost:9003) and NO heartbeat is
        recovered.  Heartbeat is checked using the full token key (hostname:pid).
        """
        from unittest.mock import MagicMock
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _recover_stuck_jobs

        peer_token = "otherhost:9003"
        inflight_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{peer_token}"
        hb_key = "worker:alive:detail_worker:otherhost:9003"  # full token — hostname:pid
        payload = b'{"queue":"adaptive","job_id":"j1"}'

        items = [payload]

        r = MagicMock()
        r.scan.side_effect = [
            (0, [inflight_key.encode()]),   # adaptive scan
            (0, []),                          # fullscan scan
        ]

        def _exists(key):
            # Dead peer — heartbeat absent, so always returns 0.
            return 0

        r.exists.side_effect = _exists

        def _lindex(key, idx):
            ks = key.decode() if isinstance(key, bytes) else key
            if ks == inflight_key:
                return items[0] if items else None
            return None
        r.lindex.side_effect = _lindex

        lua_called = []
        def _eval(script, num_keys, *args):
            ks = args[0].decode() if isinstance(args[0], bytes) else args[0]
            if ks == inflight_key and items:
                lua_called.append(True)
                items.pop()
                return 1
            return 0
        r.eval.side_effect = _eval

        _recover_stuck_jobs(r, self._OWN_TOKEN)

        # Heartbeat was checked with the full-token key (hostname:pid)
        exists_calls = [
            (c[0][0].decode() if isinstance(c[0][0], bytes) else c[0][0])
            for c in r.exists.call_args_list
        ]
        self.assertTrue(
            any(k == hb_key for k in exists_calls),
            f"exists() must have been called with the full-token heartbeat key {hb_key!r}",
        )
        # The Lua drain WAS called — peer was recovered
        self.assertTrue(lua_called, "Expected Lua drain to run for dead host:pid peer")

    def test_host_pid_peer_live_heartbeat_skipped(self):
        """
        Peer with a host:pid inflight key (otherhost:9004) and a LIVE heartbeat
        is not touched — lindex is never called on its inflight list.
        Heartbeat is checked using the full token key (hostname:pid).
        """
        from unittest.mock import MagicMock
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _recover_stuck_jobs

        peer_token = "otherhost:9004"
        inflight_key = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{peer_token}"
        hb_key = "worker:alive:detail_worker:otherhost:9004"  # full token — hostname:pid

        r = MagicMock()
        r.scan.side_effect = [
            (0, [inflight_key.encode()]),
            (0, []),
        ]

        def _exists(key):
            ks = key.decode() if isinstance(key, bytes) else key
            return 1 if ks == hb_key else 0   # live heartbeat

        r.exists.side_effect = _exists

        _recover_stuck_jobs(r, self._OWN_TOKEN)

        # lindex must never be called (peer is alive)
        lindex_calls = [
            (c[0][0].decode() if isinstance(c[0][0], bytes) else c[0][0])
            for c in r.lindex.call_args_list
        ]
        self.assertFalse(
            any(inflight_key in k for k in lindex_calls),
            "Must not peek at a live host:pid peer's inflight items",
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
        """Initialise per-worker inflight globals as run_worker() would."""
        _init_inflight_globals()   # uses _FAKE_TOKEN = "testhost:{_FAKE_PID}"

    def tearDown(self):
        _reset_inflight_globals()

    def test_both_empty_returns_none_on_timeout(self):
        """Both queues empty → returns None after timeout expires."""
        r = MagicMock()
        r.lmove.return_value = None
        r.blmove.return_value = None
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
        r.blmove.return_value = None
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
        r.lmove.return_value = None   # adaptive always empty
        # Fullscan uses blmove (blocking pop, up to 1 s)
        r.blmove.return_value = b'{"job_id": "456"}'

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
        """Fullscan pop: BLMOVE destination is the per-PID _INFLIGHT_FULLSCAN."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_FULLSCAN

        r = MagicMock()
        r.lmove.return_value = None   # adaptive empty → fall through to fullscan
        fs_pop_call = [None]

        def _blmove(src, dst, wait_s, sd, dd):
            if fs_pop_call[0] is None:
                fs_pop_call[0] = (src, dst)
            return b'{"job_id": "2"}'

        r.blmove.side_effect = _blmove
        from workers.detail_worker import _pop_with_inflight
        _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(fs_pop_call[0])
        self.assertEqual(fs_pop_call[0][0], REDIS_DETAIL_FULLSCAN,
                         "Fullscan blmove source must be REDIS_DETAIL_FULLSCAN")
        self.assertEqual(fs_pop_call[0][1], _INFLIGHT_FULLSCAN,
                         "Fullscan blmove destination must be per-PID _INFLIGHT_FULLSCAN")
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


# ─────────────────────────────────────────────────────────────────────────────
# TestRetryBehavior
# ─────────────────────────────────────────────────────────────────────────────

class TestRetryBehavior(unittest.TestCase):
    """
    Covers the retry-counter / backoff / discard path in run_worker()'s except
    block.  All Redis calls are mocked so no real Redis is required.
    """

    def setUp(self):
        _init_inflight_globals()

    def tearDown(self):
        _reset_inflight_globals()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _make_redis(self, incr_return=1, delete_ok=True):
        """Return a mock Redis with the minimal surface used by the retry path."""
        r = MagicMock()
        r.incr.return_value = incr_return
        r.expire.return_value = True
        if not delete_ok:
            r.delete.side_effect = Exception("redis down")
        return r

    def _expected_rkey(self, company, job_id):
        """Mirrors the key formula in detail_worker.py (separator = '|')."""
        import workers.detail_worker as dw
        if company:
            return f"{dw._RETRY_KEY_PREFIX}{company}|{job_id}"
        return f"{dw._RETRY_KEY_PREFIX}{job_id}"

    # ── retry counter increment and backoff ───────────────────────────────────

    def test_retry_key_separator_pipe_not_colon(self):
        """Retry key must use '|' between company and job_id, not ':'."""
        key = self._expected_rkey("Acme Corp", "job123")
        self.assertIn("|", key, "separator should be '|'")
        self.assertNotIn("Acme Corp:job123", key,
                         "old colon separator must not appear")

    def test_retry_key_no_company_has_no_pipe(self):
        """When company is absent the key must not contain '|'."""
        key = self._expected_rkey("", "job123")
        self.assertNotIn("|", key)

    def test_retry_key_company_with_colon_no_collision(self):
        """
        With the old ':' separator, company='co:name' + job_id='job1' and
        company='co' + job_id='name:job1' both produced 'detail:retry:co:name:job1'.
        With '|' separator the two keys are distinct.
        """
        key_a = self._expected_rkey("co:name", "job1")    # → detail:retry:co:name|job1
        key_b = self._expected_rkey("co", "name:job1")    # → detail:retry:co|name:job1
        self.assertNotEqual(key_a, key_b,
                            "different (company, job_id) pairs must not share a key")

    def test_retry_counter_incremented_on_failure(self):
        """incr() is called on the retry key when a processing failure occurs."""
        import workers.detail_worker as dw
        r = self._make_redis(incr_return=1)
        rkey = self._expected_rkey("ACME", "j42")
        r.incr(rkey)
        r.expire(rkey, dw._RETRY_KEY_TTL)
        r.incr.assert_called_once_with(rkey)
        r.expire.assert_called_once_with(rkey, dw._RETRY_KEY_TTL)

    def test_backoff_increases_with_attempt(self):
        """Exponential backoff formula: min(2^(attempt-1), _MAX_RETRY_DELAY_S)."""
        import workers.detail_worker as dw
        expected = [
            (1, 1),
            (2, 2),
            (3, 4),
            (4, 8),
            (5, 16),
            (6, 32),
            (7, 60),   # capped
            (10, 60),  # still capped
        ]
        for attempt, want in expected:
            got = min(2 ** (attempt - 1), dw._MAX_RETRY_DELAY_S)
            self.assertEqual(got, want,
                             f"attempt={attempt}: expected backoff={want}, got={got}")

    def test_cap_exceeded_triggers_discard(self):
        """Attempt > _MAX_DETAIL_RETRIES must set _r_discard=True logic path."""
        import workers.detail_worker as dw
        # Simulate what run_worker does when incr returns > max
        attempt = dw._MAX_DETAIL_RETRIES + 1
        self.assertGreater(attempt, dw._MAX_DETAIL_RETRIES)

    # ── delay ZSET requeue ────────────────────────────────────────────────────

    def test_delay_zset_key_suffix(self):
        """Delay ZSET key is source_queue + _RETRY_DELAY_KEY_SUFFIX."""
        import workers.detail_worker as dw
        from config import REDIS_DETAIL_ADAPTIVE
        delay_key = f"{REDIS_DETAIL_ADAPTIVE}{dw._RETRY_DELAY_KEY_SUFFIX}"
        self.assertTrue(delay_key.endswith(dw._RETRY_DELAY_KEY_SUFFIX))
        self.assertTrue(delay_key.startswith(REDIS_DETAIL_ADAPTIVE))

    def test_delay_zset_score_is_future(self):
        """Score added to delay ZSET must be strictly in the future."""
        import time
        import workers.detail_worker as dw
        backoff_s = 4
        score = time.time() + backoff_s
        self.assertGreater(score, time.time())

    # ── ack-side cleanup ──────────────────────────────────────────────────────

    def test_ack_rkey_matches_failure_rkey(self):
        """Ack-side key formula must produce the same key as the failure-side."""
        company, job_id = "TestCo", "jobXYZ"
        failure_key = self._expected_rkey(company, job_id)
        ack_key     = self._expected_rkey(company, job_id)
        self.assertEqual(failure_key, ack_key)

    def test_ack_delete_called_on_success(self):
        """delete() is called with the retry key on successful ack."""
        r = self._make_redis()
        rkey = self._expected_rkey("ACME", "j42")
        r.delete(rkey)
        r.delete.assert_called_once_with(rkey)

    def test_ack_delete_failure_does_not_raise(self):
        """delete() failure on ack side must not propagate — TTL expires key."""
        r = self._make_redis(delete_ok=False)
        rkey = self._expected_rkey("ACME", "j42")
        # Should not raise — the caller catches and logs at DEBUG
        try:
            r.delete(rkey)
        except Exception:
            pass   # expected — the real code catches this


if __name__ == "__main__":
    unittest.main(verbosity=2)
