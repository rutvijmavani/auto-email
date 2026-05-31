"""
tests/test_detail_worker_inflight.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/detail_worker.py — at-least-once delivery via LMOVE
processing lists (Phase 3.7).

Coverage map
────────────
  TestInflightKeyConstants
    · _INFLIGHT_ADAPTIVE = REDIS_DETAIL_ADAPTIVE + ":inflight"
    · _INFLIGHT_FULLSCAN = REDIS_DETAIL_FULLSCAN + ":inflight"
    · _INFLIGHT_KEY map has both source queue keys
    · _INFLIGHT_KEY[REDIS_DETAIL_ADAPTIVE] == _INFLIGHT_ADAPTIVE
    · _INFLIGHT_KEY[REDIS_DETAIL_FULLSCAN] == _INFLIGHT_FULLSCAN

  TestRecoverStuckJobs
    · Empty inflight lists → no crash, no unnecessary work
    · Single item in adaptive inflight → LMOVE back to adaptive source
    · Single item in fullscan inflight → LMOVE back to fullscan source
    · LMOVE direction is RIGHT → LEFT (item goes to FRONT of source queue)
    · Both inflight lists are always checked (adaptive + fullscan)
    · Multiple items → all recovered until list exhausted (while-loop)
    · No item movement when already empty (lmove returns None → stops)

  TestPopWithInflight
    · Both queues empty → returns None after timeout expires
    · Adaptive tried before fullscan (first lmove call is on adaptive source)
    · Returns (REDIS_DETAIL_ADAPTIVE, payload) when adaptive has an item
    · Returns (REDIS_DETAIL_FULLSCAN, payload) when only fullscan has an item
    · Adaptive takes priority: when both have items, adaptive is returned
    · LMOVE destination is the inflight key, not the source (atomic move)
    · adaptive inflight key used for adaptive source
    · fullscan inflight key used for fullscan source
"""

import sys
import os
import time
import unittest
from unittest.mock import MagicMock, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# TestInflightKeyConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestInflightKeyConstants(unittest.TestCase):
    """_INFLIGHT_* keys and _INFLIGHT_KEY mapping."""

    def test_inflight_adaptive_is_adaptive_plus_inflight(self):
        """_INFLIGHT_ADAPTIVE = REDIS_DETAIL_ADAPTIVE + ':inflight'."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_ADAPTIVE
        self.assertEqual(_INFLIGHT_ADAPTIVE, f"{REDIS_DETAIL_ADAPTIVE}:inflight")

    def test_inflight_fullscan_is_fullscan_plus_inflight(self):
        """_INFLIGHT_FULLSCAN = REDIS_DETAIL_FULLSCAN + ':inflight'."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_FULLSCAN
        self.assertEqual(_INFLIGHT_FULLSCAN, f"{REDIS_DETAIL_FULLSCAN}:inflight")

    def test_inflight_key_map_contains_adaptive(self):
        """_INFLIGHT_KEY maps REDIS_DETAIL_ADAPTIVE."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_KEY
        self.assertIn(REDIS_DETAIL_ADAPTIVE, _INFLIGHT_KEY)

    def test_inflight_key_map_contains_fullscan(self):
        """_INFLIGHT_KEY maps REDIS_DETAIL_FULLSCAN."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_KEY
        self.assertIn(REDIS_DETAIL_FULLSCAN, _INFLIGHT_KEY)

    def test_inflight_key_map_adaptive_value(self):
        """_INFLIGHT_KEY[REDIS_DETAIL_ADAPTIVE] == _INFLIGHT_ADAPTIVE."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_KEY, _INFLIGHT_ADAPTIVE
        self.assertEqual(_INFLIGHT_KEY[REDIS_DETAIL_ADAPTIVE], _INFLIGHT_ADAPTIVE)

    def test_inflight_key_map_fullscan_value(self):
        """_INFLIGHT_KEY[REDIS_DETAIL_FULLSCAN] == _INFLIGHT_FULLSCAN."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_KEY, _INFLIGHT_FULLSCAN
        self.assertEqual(_INFLIGHT_KEY[REDIS_DETAIL_FULLSCAN], _INFLIGHT_FULLSCAN)


# ─────────────────────────────────────────────────────────────────────────────
# TestRecoverStuckJobs
# ─────────────────────────────────────────────────────────────────────────────

class TestRecoverStuckJobs(unittest.TestCase):
    """
    _recover_stuck_jobs(r) moves any leftover inflight items back to their
    source queues on startup so no job is permanently lost after a crash.
    """

    def test_empty_inflight_no_crash(self):
        """Empty inflight lists → function returns cleanly, no exception."""
        r = MagicMock()
        r.lmove.return_value = None
        from workers.detail_worker import _recover_stuck_jobs
        try:
            _recover_stuck_jobs(r)
        except Exception as exc:
            self.fail(f"Unexpected exception with empty inflight: {exc}")

    def test_adaptive_inflight_item_moved_to_adaptive_source(self):
        """Item in adaptive inflight → LMOVE(inflight_adaptive → adaptive_source)."""
        from config import REDIS_DETAIL_ADAPTIVE
        from workers.detail_worker import _INFLIGHT_ADAPTIVE, _recover_stuck_jobs

        r = MagicMock()
        call_counts = {"adp": 0}

        def _lmove(src, dst, sd, dd):
            if src == _INFLIGHT_ADAPTIVE:
                call_counts["adp"] += 1
                return b"payload" if call_counts["adp"] == 1 else None
            return None

        r.lmove.side_effect = _lmove
        _recover_stuck_jobs(r)

        calls = [c[0] for c in r.lmove.call_args_list]
        adaptive_recovery = [(s, d) for s, d, *_ in calls
                              if s == _INFLIGHT_ADAPTIVE]
        self.assertTrue(len(adaptive_recovery) >= 1)
        self.assertEqual(adaptive_recovery[0][1], REDIS_DETAIL_ADAPTIVE)

    def test_fullscan_inflight_item_moved_to_fullscan_source(self):
        """Item in fullscan inflight → LMOVE(inflight_fullscan → fullscan_source)."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_FULLSCAN, _recover_stuck_jobs

        r = MagicMock()
        call_counts = {"fs": 0}

        def _lmove(src, dst, sd, dd):
            if src == _INFLIGHT_FULLSCAN:
                call_counts["fs"] += 1
                return b"payload" if call_counts["fs"] == 1 else None
            return None

        r.lmove.side_effect = _lmove
        _recover_stuck_jobs(r)

        calls = [c[0] for c in r.lmove.call_args_list]
        fullscan_recovery = [(s, d) for s, d, *_ in calls
                              if s == _INFLIGHT_FULLSCAN]
        self.assertTrue(len(fullscan_recovery) >= 1)
        self.assertEqual(fullscan_recovery[0][1], REDIS_DETAIL_FULLSCAN)

    def test_lmove_direction_is_right_to_left(self):
        """LMOVE from inflight uses 'RIGHT' → 'LEFT' so items go to FRONT."""
        from workers.detail_worker import _INFLIGHT_ADAPTIVE, _recover_stuck_jobs

        r = MagicMock()
        call_counts = {"n": 0}

        def _lmove(src, dst, src_dir, dst_dir):
            call_counts["n"] += 1
            return b"item" if call_counts["n"] == 1 else None

        r.lmove.side_effect = _lmove
        _recover_stuck_jobs(r)

        first_call = r.lmove.call_args_list[0][0]
        self.assertEqual(first_call[2], "RIGHT")
        self.assertEqual(first_call[3], "LEFT")

    def test_both_inflight_keys_checked(self):
        """Both _INFLIGHT_ADAPTIVE and _INFLIGHT_FULLSCAN are checked."""
        from workers.detail_worker import (
            _INFLIGHT_ADAPTIVE, _INFLIGHT_FULLSCAN, _recover_stuck_jobs
        )

        r = MagicMock()
        r.lmove.return_value = None  # both empty
        _recover_stuck_jobs(r)

        called_src_keys = {c[0][0] for c in r.lmove.call_args_list}
        self.assertIn(_INFLIGHT_ADAPTIVE, called_src_keys)
        self.assertIn(_INFLIGHT_FULLSCAN, called_src_keys)

    def test_multiple_items_all_recovered(self):
        """All items in inflight are recovered — loop continues until lmove returns None."""
        from workers.detail_worker import _INFLIGHT_ADAPTIVE, _recover_stuck_jobs

        r = MagicMock()
        items = [b"job1", b"job2", b"job3"]
        idx = [0]

        def _lmove(src, dst, sd, dd):
            if src == _INFLIGHT_ADAPTIVE:
                if idx[0] < len(items):
                    val = items[idx[0]]
                    idx[0] += 1
                    return val
            return None

        r.lmove.side_effect = _lmove
        _recover_stuck_jobs(r)

        adaptive_calls = [c for c in r.lmove.call_args_list
                          if c[0][0] == _INFLIGHT_ADAPTIVE]
        # 3 items popped + 1 final None check = 4 calls total
        self.assertEqual(len(adaptive_calls), 4)

    def test_loop_stops_when_lmove_returns_none(self):
        """Recovery loop stops as soon as lmove returns None (no infinite loop)."""
        r = MagicMock()
        r.lmove.return_value = None
        from workers.detail_worker import _recover_stuck_jobs
        _recover_stuck_jobs(r)
        # With 2 keys × 1 check each = exactly 2 calls
        self.assertEqual(r.lmove.call_count, 2)


# ─────────────────────────────────────────────────────────────────────────────
# TestPopWithInflight
# ─────────────────────────────────────────────────────────────────────────────

class TestPopWithInflight(unittest.TestCase):
    """
    _pop_with_inflight(r, timeout) returns (source_key, raw_payload) or None.

    Adaptive queue has priority: it is checked first every poll cycle.
    Items are moved atomically from source → inflight via LMOVE.
    """

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
            return None  # adaptive is empty

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
            return b'{"job_id": "999"}'  # both queues have items

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        result = _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], REDIS_DETAIL_ADAPTIVE,
                         "Adaptive should take priority over fullscan")

    def test_adaptive_item_moved_to_adaptive_inflight(self):
        """Adaptive pop: LMOVE destination is _INFLIGHT_ADAPTIVE."""
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
                         "Adaptive pop destination must be _INFLIGHT_ADAPTIVE")

    def test_fullscan_item_moved_to_fullscan_inflight(self):
        """Fullscan pop: LMOVE destination is _INFLIGHT_FULLSCAN."""
        from config import REDIS_DETAIL_FULLSCAN
        from workers.detail_worker import _INFLIGHT_FULLSCAN

        r = MagicMock()
        fs_pop_call = [None]

        def _lmove(src, dst, sd, dd):
            if src == REDIS_DETAIL_FULLSCAN and fs_pop_call[0] is None:
                fs_pop_call[0] = (src, dst)
                return b'{"job_id": "2"}'
            return None  # adaptive empty

        r.lmove.side_effect = _lmove
        from workers.detail_worker import _pop_with_inflight
        _pop_with_inflight(r, timeout=0.1)

        self.assertIsNotNone(fs_pop_call[0])
        self.assertEqual(fs_pop_call[0][1], _INFLIGHT_FULLSCAN,
                         "Fullscan pop destination must be _INFLIGHT_FULLSCAN")

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
