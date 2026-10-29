"""Stalled-worker helpers: progress.is_stalled/terminate, scheduler pool awareness."""

import json
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers import progress


class _R:
    def __init__(self, cur=None):
        self.cur = cur or {}
        self.deleted = []

    def get(self, key):
        c = key[len("worker:cur:"):]
        return json.dumps(self.cur[c]) if c in self.cur else None

    def delete(self, key):
        self.deleted.append(key)


class TestIsStalled(unittest.TestCase):
    def test_idle_worker_is_not_stalled(self):
        self.assertFalse(progress.is_stalled(_R(), "c"))

    def test_deadline_in_future_is_running(self):
        r = _R({"c": {"msg_id": "1-0", "alive_until": time.time() + 60}})
        self.assertFalse(progress.is_stalled(r, "c"))

    def test_deadline_passed_is_stalled(self):
        r = _R({"c": {"msg_id": "1-0", "alive_until": time.time() - 1}})
        self.assertTrue(progress.is_stalled(r, "c"))


class TestTerminate(unittest.TestCase):
    def test_same_host_kills_and_clears_report(self):
        r = _R()
        with patch("os.kill") as kill:
            progress.terminate(r, "c", "h", 123, "h")
        kill.assert_called_once()
        self.assertEqual(r.deleted, ["worker:cur:c"])

    def test_other_host_clears_report_without_kill(self):
        r = _R()
        with patch("os.kill") as kill:
            progress.terminate(r, "c", "other", 123, "h")
        kill.assert_not_called()
        self.assertEqual(r.deleted, ["worker:cur:c"])


class TestPoolStalled(unittest.TestCase):
    def test_stalled_pids_only_alive_stalled(self):
        from workers import scheduler
        p1, p2 = MagicMock(pid=11), MagicMock(pid=12)
        p1.is_alive.return_value = p2.is_alive.return_value = True
        stalled_key = scheduler._worker_consumer("scan", 11)
        r = _R({stalled_key: {"msg_id": "1-0", "alive_until": time.time() - 5}})
        got = scheduler._stalled_pids(r, "scan", [(p1, None), (p2, None)])
        self.assertEqual(got, {11})

    def test_detail_consumer_name(self):
        from workers import scheduler
        self.assertTrue(scheduler._worker_consumer("detail", 5).startswith("detail_worker:"))


if __name__ == "__main__":
    unittest.main()
