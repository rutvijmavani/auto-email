"""
tests/test_detail_drainer.py — detail admission token bucket and drainer (TH step 7).
"""

import json
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers import detail_drainer as dd


class FakeRedis:
    def __init__(self, events):
        self.kv, self.lists, self.events = {}, {}, events

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None, **_):
        self.kv[k] = v

    def llen(self, k):
        return len(self.lists.get(k, []))

    def lpush(self, k, v):
        self.events.append(("lpush", k))
        self.lists.setdefault(k, []).insert(0, v)

    def scan(self, cursor, match=None, count=None):
        return 0, []


class Cur:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows


class FakeConn:
    """Held rows in discovery order; answers the drainer's three queries."""

    def __init__(self, held, events):
        self.held, self.events = list(held), events
        self.claimed = []

    def execute(self, sql, params=None):
        if "COUNT(*)" in sql:
            return Cur([{"c": len(self.held), "age": 90.0 if self.held else None,
                         "oldest_id": self.held[0]["id"] if self.held else None}])
        if sql.lstrip().startswith("SELECT id"):
            return Cur(self.held[:params[0]])
        ids = set(params[0])
        if "pushed_at = NULL" in sql:         # re-hold after a failed push
            self.events.append(("rehold", tuple(params[0])))
            self.held = sorted(self.held + [c for c in self.claimed if c["id"] in ids],
                               key=lambda h: h["id"])
            return Cur([])
        self.events.append(("update", tuple(params[0])))
        self.claimed += [h for h in self.held if h["id"] in ids]
        out = [{"id": h["id"]} for h in self.held if h["id"] in ids]
        self.held = [h for h in self.held if h["id"] not in ids]
        return Cur(out)

    def commit(self):
        self.events.append(("commit",))

    def close(self):
        pass


def _row(i, found_by="tier2_adaptive"):
    return {"id": i, "company": "acme", "job_url": f"https://x/{i}", "job_id": str(i),
            "ats_platform": "greenhouse", "detail_payload": None, "found_by": found_by}


def _setup(held, n=2.0, s=7.0, d=700.0, tokens=None, now=1000.0):
    events = []
    r = FakeRedis(events)
    r.kv[dd.BUCKET_PARAMS_KEY] = json.dumps({"n": n, "s": s, "d": d})
    if tokens is not None:
        r.kv[dd.TOKENS_KEY] = json.dumps({"tokens": tokens, "ts": now})
    conn = FakeConn(held, events)
    return r, conn, events


class TestHelpers(unittest.TestCase):

    def test_tick_capacity_refill(self):
        self.assertAlmostEqual(dd.tick_s(2, 7), 3.5)
        self.assertAlmostEqual(dd.capacity(2, 7, 700), 200.0)
        self.assertEqual(dd.capacity(1, 100, 1), 1.0)
        self.assertEqual(dd.refill(0, 7, 3.5, 200), 2.0)
        self.assertEqual(dd.refill(199, 100, 3.5, 200), 200)
        # capacity reduced below the stored tokens: kept, not cut, and no further refill
        self.assertEqual(dd.refill(150, 100, 3.5, 100), 150)

    def test_wake_k(self):
        with patch.object(dd, "DRAIN_MIN_WAKE_S", 1.0):
            self.assertEqual(dd.wake_k(3.5), 1)      # tick >= 1 s: every tick
            self.assertEqual(dd.wake_k(0.7), 2)      # 1.4 s, 2 tokens
            self.assertEqual(dd.wake_k(0.175), 6)    # 1.05 s, 6 tokens


class TestDrainOnce(unittest.TestCase):

    def _run(self, r, conn, now=1000.0):
        with patch.object(dd, "get_conn", return_value=conn):
            return dd.drain_once(r, now=now)

    def test_releases_min_of_tokens_and_held_oldest_first(self):
        r, conn, events = _setup([_row(i) for i in range(1, 6)], tokens=3.0)
        out = self._run(r, conn)
        self.assertEqual(out["released"], 3)
        self.assertEqual(out["held"], 2)
        self.assertEqual(events[0], ("update", (1, 2, 3)))
        self.assertEqual(len(r.lists["queue:detail:adaptive"]), 3)
        self.assertAlmostEqual(json.loads(r.kv[dd.TOKENS_KEY])["tokens"], 0.0)

    def test_pushed_at_is_committed_before_any_push(self):
        r, conn, events = _setup([_row(1), _row(2)], tokens=5.0)
        self._run(r, conn)
        kinds = [e[0] for e in events]
        self.assertLess(kinds.index("commit"), kinds.index("lpush"))

    def test_refills_from_elapsed_time(self):
        r, conn, _ = _setup([_row(i) for i in range(1, 9)], tokens=0.0, now=1000.0)
        out = self._run(r, conn, now=1007.0)          # 7 s at 3.5 s/token = 2 tokens
        self.assertEqual(out["released"], 2)

    def test_stream_follows_found_by(self):
        r, conn, _ = _setup([_row(1, "tier2_fullscan"), _row(2)], tokens=5.0)
        self._run(r, conn)
        self.assertEqual(len(r.lists["queue:detail:fullscan"]), 1)
        self.assertEqual(len(r.lists["queue:detail:adaptive"]), 1)

    def test_first_start_seeds_capacity_minus_queued(self):
        r, conn, _ = _setup([_row(i) for i in range(1, 400)])
        r.lists["queue:detail:adaptive"] = ["x"] * 150
        out = self._run(r, conn)                      # capacity 200 - 150 queued
        self.assertEqual(out["released"], 50)

    def test_no_params_releases_nothing_but_publishes_held(self):
        r, conn, events = _setup([_row(1)], tokens=5.0)
        del r.kv[dd.BUCKET_PARAMS_KEY]
        out = self._run(r, conn)
        self.assertEqual(out["released"], 0)
        self.assertIsNone(out["wake_s"])
        self.assertEqual(json.loads(r.kv[dd.HELD_KEY])["count"], 1)
        self.assertEqual(events, [])

    def test_unbuildable_row_is_released_without_token(self):
        bad = {**_row(1), "ats_platform": "workday"}
        r, conn, _ = _setup([bad, _row(2)], tokens=5.0)
        out = self._run(r, conn)
        self.assertEqual(out["released"], 1)
        self.assertAlmostEqual(out["tokens"], 4.0)
        self.assertEqual(conn.held, [])

    def test_push_failure_reholds_unpushed_rows_and_keeps_tokens(self):
        r, conn, events = _setup([_row(1), _row(2), _row(3)], tokens=5.0)
        real_lpush, calls = r.lpush, []

        def flaky(k, v):
            calls.append(k)
            if len(calls) == 2:
                raise ConnectionError("redis down")
            real_lpush(k, v)

        r.lpush = flaky
        with self.assertRaises(ConnectionError):
            self._run(r, conn)
        self.assertEqual([h["id"] for h in conn.held], [2, 3])   # row 1 was pushed
        self.assertIn(("rehold", (2, 3)), events)
        self.assertEqual(json.loads(r.kv[dd.TOKENS_KEY])["tokens"], 5.0)

    def test_held_figures_published(self):
        r, conn, _ = _setup([_row(1), _row(2)], tokens=0.0)
        self._run(r, conn)
        held = json.loads(r.kv[dd.HELD_KEY])
        self.assertEqual((held["count"], held["oldest_age_s"], held["oldest_id"]), (2, 90.0, 1))


if __name__ == "__main__":
    unittest.main()
