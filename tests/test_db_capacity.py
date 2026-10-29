"""
tests/test_db_capacity.py — live Postgres connection capacity (workers/db_capacity.py)
and the manager's seat arithmetic built on it.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import workers.manager as mgr
from workers import db_capacity


def _redis_with_workers(pids):
    r = MagicMock()
    r.scan.return_value = (0, [f"worker:alive:detail:host:{p}" for p in pids])
    return r


def _conn(max_conn, reserved, rows):
    """get_conn() stand-in answering SHOW max_connections / reserved / the group-by."""
    cur = MagicMock()
    answers = [{"max_connections": str(max_conn)},
               {"superuser_reserved_connections": str(reserved)}]
    cur.fetchone.side_effect = answers
    cur.fetchall.return_value = rows
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cur
    return conn


class TestProbe(unittest.TestCase):

    def test_seat_cap_from_live_numbers(self):
        rows = [{"application_name": "pid10", "n": 2},   # worker
                {"application_name": "pid11", "n": 2},   # worker
                {"application_name": "pid99", "n": 2},   # manager (not a worker)
                {"application_name": "", "n": 1}]        # unlabelled
        with patch("db.db.get_conn", return_value=_conn(100, 3, rows)):
            p = db_capacity.probe(_redis_with_workers([10, 11]))
        self.assertEqual(p["cap"], 100 - 3 - db_capacity.DB_CAPACITY_MARGIN)
        self.assertEqual(p["used"], 7)
        self.assertEqual(p["worker_conns"], 4)
        self.assertEqual(p["non_worker"], 3)
        self.assertEqual(p["per_worker"], 2)
        self.assertEqual(p["seat_cap"], (p["cap"] - 3) // 2)

    def test_per_worker_follows_the_busiest_worker(self):
        rows = [{"application_name": "pid10", "n": 2},
                {"application_name": "pid11", "n": 5}]
        with patch("db.db.get_conn", return_value=_conn(100, 3, rows)):
            p = db_capacity.probe(_redis_with_workers([10, 11]))
        self.assertEqual(p["per_worker"], 5)

    def test_probe_failure_returns_none(self):
        with patch("db.db.get_conn", side_effect=RuntimeError("down")):
            self.assertIsNone(db_capacity.probe(_redis_with_workers([])))


class TestManagerSeats(unittest.TestCase):

    def setUp(self):
        self.r = MagicMock()
        self.r.get.return_value = None

    def _capacity(self, value):
        patcher = patch.object(mgr, "_db_capacity", return_value=value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_raise_step_is_connections_over_per_worker(self):
        self._capacity({"per_worker": 2, "seat_cap": 40})
        self.assertEqual(mgr._raise_step_seats(self.r), max(1, mgr.DB_POOL_RAISE_STEP // 2))

    def test_failed_probe_stops_raising(self):
        self._capacity(None)
        self.assertEqual(mgr._raise_step_seats(self.r), 0)
        self.assertEqual(mgr._seat_cap(self.r), mgr.WORKER_SEAT_BUDGET)
        self.assertFalse(mgr._raise_db_total(self.r, "detail"))
        self.r.set.assert_not_called()

    def test_raise_stops_at_postgres_seat_cap(self):
        self._capacity({"per_worker": 2, "seat_cap": mgr.WORKER_SEAT_BUDGET})
        self.assertFalse(mgr._raise_db_total(self.r, "detail"))

    def test_raise_never_passes_seat_cap(self):
        cap = mgr.WORKER_SEAT_BUDGET + 1
        self._capacity({"per_worker": 2, "seat_cap": cap})
        self.assertTrue(mgr._raise_db_total(self.r, "detail"))
        self.r.set.assert_any_call("manager:db_pool_maxconn", cap)


if __name__ == "__main__":
    unittest.main()
