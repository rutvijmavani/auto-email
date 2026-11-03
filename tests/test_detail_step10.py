"""
Step 10 of docs/detail-pool-ceiling-th-design.md: RMIDLE (remove stalled, then idle,
then the most recently started busy worker) and SEATCAP-DROP (lower the seat total
when the live Postgres seat cap falls).
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import workers.manager as mgr
import workers.scheduler as sched


class TestPickRemoval(unittest.TestCase):

    def test_stalled_first_even_if_others_idle(self):
        self.assertEqual(
            sched._pick_removal_index([1, 2, 3], {1}, {1: 100.0, 2: None, 3: None}), 0)

    def test_idle_before_busy_latest_idle_wins(self):
        busy = {1: None, 2: 500.0, 3: None, 4: 900.0}
        self.assertEqual(sched._pick_removal_index([1, 2, 3, 4], set(), busy), 2)

    def test_all_busy_removes_most_recent_start(self):
        busy = {1: 100.0, 2: 900.0, 3: 500.0}
        self.assertEqual(sched._pick_removal_index([1, 2, 3], set(), busy), 1)

    def test_no_heartbeat_reading_is_picked_last(self):
        busy = {2: 50.0}                      # pid 1 and 3 unread -> treated as oldest start
        self.assertEqual(sched._pick_removal_index([1, 2, 3], set(), busy), 1)

    def test_nothing_known_takes_last_entry(self):
        self.assertEqual(sched._pick_removal_index([1, 2, 3], set(), {}), 2)


class TestBusySince(unittest.TestCase):

    def test_reads_pid_from_key_and_null_means_idle(self):
        r = MagicMock()
        store = {
            b"worker:alive:detail_worker:host:11": json.dumps({"busy_since": None}),
            b"worker:alive:detail_worker:host:12": json.dumps({"busy_since": 77.5}),
        }
        r.scan_iter.return_value = list(store)
        r.get.side_effect = lambda k: store[k]
        self.assertEqual(sched._busy_since_by_pid(r, "detail"), {11: None, 12: 77.5})


class TestDropDbTotal(unittest.TestCase):

    def _drop(self, stored, cap, probe=True):
        r = MagicMock()
        r.get.return_value = str(stored) if stored else None
        with patch.object(mgr, "_db_capacity",
                          return_value={"seat_cap": cap} if probe else None), \
             patch.object(mgr, "WORKER_SEAT_BUDGET", 50):
            return mgr._drop_db_total(r), r

    def test_lowers_to_cap(self):
        dropped, r = self._drop(70, 60)
        self.assertTrue(dropped)
        r.set.assert_called_once_with("manager:db_pool_maxconn", 60)

    def test_cap_under_budget_removes_raised_value(self):
        dropped, r = self._drop(70, 40)
        self.assertTrue(dropped)
        r.delete.assert_called_once_with("manager:db_pool_maxconn")

    def test_no_change_when_cap_covers_total_or_probe_fails(self):
        self.assertFalse(self._drop(70, 80)[0])
        self.assertFalse(self._drop(70, 60, probe=False)[0])
        self.assertFalse(self._drop(None, 10)[0])          # at budget already


if __name__ == "__main__":
    unittest.main()
