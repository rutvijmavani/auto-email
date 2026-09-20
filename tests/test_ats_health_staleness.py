"""
tests/test_ats_health_staleness.py
─────────────────────────────────────────────────────────────────────────────
  · staleness_checker.refresh_planner_stats — ANALYZE per configured table,
    commit on success, rollback + swallow on failure, runs before the passes
  · config.STALENESS_ANALYZE_TABLES identifier validation
  · health_check.check_ats_lane — worker/queue rows, DLQ threshold,
    pending-work-with-no-workers warning, idle-is-OK, stale heartbeat warning
  · staleness-checker.service TimeoutStartSec
"""

import fnmatch
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scripts.health_check as hc
import scripts.staleness_checker as sc


class FakeRedis:
    def __init__(self, strings=None, lists=None, zsets=None):
        self.strings = strings or {}
        self.lists = lists or {}
        self.zsets = zsets or {}

    def _keys(self):
        return list(self.strings) + list(self.lists) + list(self.zsets)

    def scan_iter(self, match, count=50):
        return iter([k for k in self._keys() if fnmatch.fnmatchcase(k, match)])

    def get(self, key):
        return self.strings.get(key)

    def llen(self, key):
        return len(self.lists.get(key, []))

    def zcard(self, key):
        return len(self.zsets.get(key, {}))


NOW = 1_000_000.0


def _hb(ts, processed):
    return json.dumps({"pid": 1, "ts": ts, "processed": processed})


class TestPlannerStats(unittest.TestCase):
    def test_analyzes_each_table_then_commits(self):
        conn = MagicMock()
        with patch.object(sc, "STALENESS_ANALYZE_TABLES", ("a", "b")):
            sc.refresh_planner_stats(conn)
        self.assertEqual([c.args[0] for c in conn.execute.call_args_list],
                         ["ANALYZE a", "ANALYZE b"])
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()

    def test_failure_rolls_back_and_is_swallowed(self):
        conn = MagicMock()
        conn.execute.side_effect = RuntimeError("boom")
        with patch.object(sc, "STALENESS_ANALYZE_TABLES", ("a",)):
            sc.refresh_planner_stats(conn)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_main_analyzes_before_passes(self):
        order = []
        conn = MagicMock()
        r = MagicMock()
        args = MagicMock(dry_run=True, enrichment_only=False,
                         discovery_only=False, redetect_only=False)
        with patch.object(sc, "get_redis", return_value=r), \
             patch.object(sc, "_is_maintenance", return_value=False), \
             patch.object(sc, "get_conn", return_value=conn), \
             patch.object(sc, "refresh_planner_stats", side_effect=lambda c: order.append("analyze")), \
             patch.object(sc, "run_enrichment_staleness", side_effect=lambda *a, **k: order.append("enrich") or 0), \
             patch.object(sc, "run_discovery_staleness", side_effect=lambda *a, **k: order.append("disc") or 0), \
             patch.object(sc, "run_redetect_staleness", side_effect=lambda *a, **k: order.append("redetect") or 0), \
             patch.object(sc, "run_stale_purge", side_effect=lambda *a, **k: order.append("purge") or 0):
            sc.main(args)
        self.assertEqual(order[0], "analyze")
        self.assertIn("enrich", order)

    def test_maintenance_skips_analyze(self):
        with patch.object(sc, "get_redis", return_value=MagicMock()), \
             patch.object(sc, "_is_maintenance", return_value=True), \
             patch.object(sc, "get_conn") as gc, \
             patch.object(sc, "refresh_planner_stats") as rp:
            sc.main(MagicMock())
        gc.assert_not_called()
        rp.assert_not_called()

    def test_config_default_tables_are_plain_identifiers(self):
        import config
        self.assertIn("fein_domain_map", config.STALENESS_ANALYZE_TABLES)
        for t in config.STALENESS_ANALYZE_TABLES:
            self.assertRegex(t, r"^[a-z_][a-z0-9_]*$")

    def test_config_rejects_injection(self):
        import importlib
        import config
        with patch.dict(os.environ, {"STALENESS_ANALYZE_TABLES": "x; DROP TABLE y"}):
            try:
                with self.assertRaises(ValueError):
                    importlib.reload(config)
            finally:
                os.environ.pop("STALENESS_ANALYZE_TABLES", None)
                importlib.reload(config)

    def test_service_timeout(self):
        path = os.path.join(os.path.dirname(__file__), "..", "deploy", "systemd",
                            "staleness-checker.service")
        with open(path, encoding="utf-8") as f:
            self.assertIn("TimeoutStartSec=1800", f.read())


class TestAtsLaneHealth(unittest.TestCase):
    def lane(self, name):
        return next(l for l in hc._ats_lanes() if l["name"] == name)

    def check(self, r, name="head-check", dlq_warn=50):
        return hc.check_ats_lane(r, self.lane(name), NOW, dlq_warn)

    def test_idle_zero_workers_is_ok(self):
        rows = self.check(FakeRedis())
        self.assertEqual([lv for lv, _, _ in rows], ["OK", "OK"])
        self.assertIn("idle", rows[0][2])

    def test_live_workers_sum_processed_and_oldest_age(self):
        r = FakeRedis(strings={
            "worker:alive:head_check_worker@1:h:10": _hb(NOW - 5, 7),
            "worker:alive:head_check_worker@2:h:11": _hb(NOW - 20, 3),
        })
        rows = self.check(r)
        self.assertEqual(rows[0][0], "OK")
        self.assertIn("2 live", rows[0][2])
        self.assertIn("processed=10", rows[0][2])
        self.assertIn("oldest heartbeat 20s", rows[0][2])

    def test_no_instance_key_format_counted(self):
        r = FakeRedis(strings={"worker:alive:head_check_worker:h:10": _hb(NOW - 1, 1)})
        self.assertIn("1 live", self.check(r)[0][2])

    def test_backlog_without_workers_warns(self):
        r = FakeRedis(lists={"head_check:batch": ["x"] * 3})
        rows = self.check(r)
        self.assertEqual(rows[0][0], "WARNING")
        self.assertIn("no live workers", rows[0][2])

    def test_backlog_with_workers_ok(self):
        r = FakeRedis(strings={"worker:alive:head_check_worker@1:h:10": _hb(NOW - 1, 0)},
                      lists={"head_check:batch": ["x"] * 3})
        self.assertEqual([lv for lv, _, _ in self.check(r)], ["OK", "OK"])

    def test_stale_heartbeat_warns(self):
        stale_age = 2 * self.lane("head-check")["heartbeat_s"] + 5
        r = FakeRedis(strings={"worker:alive:head_check_worker@1:h:10": _hb(NOW - stale_age, 0)})
        rows = self.check(r)
        self.assertEqual(rows[0][0], "WARNING")
        self.assertIn("STALE", rows[0][2])

    def test_dlq_threshold(self):
        at = FakeRedis(lists={"enrichment:dlq": ["x"] * 50})
        over = FakeRedis(lists={"enrichment:dlq": ["x"] * 51})
        self.assertEqual(self.check(at, "enrichment")[1][0], "OK")
        rows = self.check(over, "enrichment")
        self.assertEqual(rows[1][0], "WARNING")
        self.assertIn("dlq=51", rows[1][2])

    def test_enrichment_counts_all_structures(self):
        r = FakeRedis(
            lists={"enrichment:on_demand": ["a"]},
            zsets={"enrichment:batch": {"b": 1, "c": 2},
                   "enrichment:delayed": {"d": 1},
                   "domain_enrichment:inflight:1": {"e": 1}},
        )
        rows = self.check(r, "enrichment")
        self.assertIn("queued=3 (on_demand=1, batch=2)", rows[1][2])
        self.assertIn("delayed=1", rows[1][2])
        self.assertIn("in-flight=1", rows[1][2])
        self.assertEqual(rows[0][0], "WARNING")  # backlog, no workers

    def test_discovery_and_head_check_inflight(self):
        r = FakeRedis(zsets={"discovery:redetect": {"a": 1}, "discovery:inflight:1": {"b": 1}})
        self.assertIn("redetect=1", self.check(r, "discovery")[1][2])
        self.assertIn("in-flight=1", self.check(r, "discovery")[1][2])
        r2 = FakeRedis(lists={"head_check:inflight:instance:1:batch": ["x", "y"]})
        self.assertIn("in-flight=2", self.check(r2)[1][2])

    def test_lanes_cover_three_workers(self):
        self.assertEqual({l["worker"] for l in hc._ats_lanes()},
                         {"head_check_worker", "domain_enrichment_worker",
                          "discover_h1b_ats_worker"})


if __name__ == "__main__":
    unittest.main()
