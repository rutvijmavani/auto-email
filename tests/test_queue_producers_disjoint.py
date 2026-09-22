"""
tests/test_queue_producers_disjoint.py — enrichment producers must select disjoint FEIN sets

The trigger is part of the enrichment:batch ZSET member JSON, so two producers selecting the
same FEIN queue it twice (ZADD dedups only identical members) and the company is enriched
twice. Contract after the 2026-09-21 fix:
  · fuzzy_match._populate_enrichment_queue  → last_enriched_at IS NULL only
  · staleness_checker.run_enrichment_staleness → last_enriched_at older than the window only
  · staleness_checker no longer feeds discovery:batch
  · scripts.dedupe_queues_once removes the already-doubled members
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scripts.dedupe_queues_once as dq
import scripts.fuzzy_match_uscis_dol as fm
import scripts.staleness_checker as sc


def _norm(sql):
    return " ".join(sql.split())


class TestPopulateEnrichmentQueue(unittest.TestCase):
    def test_selects_only_never_enriched(self):
        cur = MagicMock()
        cur.__iter__.return_value = iter([{"employer_fein": "11-1", "petition_count": 7}])
        conn = MagicMock()
        conn.named_cursor.return_value.__enter__.return_value = cur
        r = MagicMock()

        fm._populate_enrichment_queue(conn, r)

        sql = _norm(cur.execute.call_args.args[0])
        self.assertIn("f.last_enriched_at IS NULL", sql)
        self.assertNotIn("interval", sql.lower())
        self.assertNotIn(" OR ", sql)
        # no interval parameter is passed any more
        self.assertEqual(len(cur.execute.call_args.args), 1)

        mapping = r.pipeline.return_value.zadd.call_args.args[1]
        member = json.loads(next(iter(mapping)))
        self.assertEqual(member["trigger"], "enrichment")
        self.assertEqual(member["fein"], "11-1")


class TestStalenessSelectsOnlyStale(unittest.TestCase):
    def _capture(self):
        calls = []

        def fake(conn, r, sql, params, queue_key, **kw):
            calls.append((_norm(sql), params, queue_key, kw))
            return 0

        with patch.object(sc, "_stream_and_zadd", side_effect=fake):
            sc.run_enrichment_staleness(MagicMock(), MagicMock())
        return calls

    def test_both_passes_select_only_enriched_and_older_than_window(self):
        calls = self._capture()
        self.assertEqual(len(calls), 2)
        for sql, params, _q, _kw in calls:
            self.assertIn("f.last_enriched_at < NOW() - %s::interval", sql)
            self.assertNotIn("last_enriched_at IS NULL", sql)
            self.assertNotIn("public_domain", sql)
            self.assertNotIn("ANY(", sql)         # monitored-FEIN branch is gone
            self.assertEqual(params, (f"{sc.ENRICH_STALENESS_DAYS} days",))

    def test_split_by_careers_url(self):
        (sql_a, _pa, q_a, kw_a), (sql_b, _pb, q_b, kw_b) = self._capture()
        self.assertIn("f.careers_url IS NULL", sql_a)
        self.assertEqual(q_a, sc.ENRICHMENT_BATCH)
        self.assertEqual(kw_a["trigger"], "staleness")
        self.assertIn("f.careers_url IS NOT NULL", sql_b)
        self.assertEqual(q_b, sc.HEAD_CHECK_BATCH)
        self.assertTrue(kw_b["use_list"])

    def test_no_discovery_producer_left(self):
        self.assertFalse(hasattr(sc, "run_discovery_staleness"))
        self.assertFalse(hasattr(sc, "DISCOVERY_BATCH"))


class FakeRedis:
    def __init__(self, enrichment=None, discovery=None):
        self.z = {"enrichment:batch": dict(enrichment or {}),
                  "discovery:batch": dict(discovery or {})}

    def zscan_iter(self, key, count=None):
        return iter(list(self.z[key].items()))

    def zrem(self, key, *members):
        n = 0
        for m in members:
            n += self.z[key].pop(m, None) is not None
        return n

    def pipeline(self, transaction=False):
        outer = self

        class P:
            def __init__(self):
                self.ops = []

            def zadd(self, key, mapping, gt=False):
                self.ops.append((key, mapping))

            def execute(self):
                for key, mapping in self.ops:
                    outer.z[key].update(mapping)
        return P()


def _m(fein, trigger, source=None, tier=None):
    d = {"fein": fein, "trigger": trigger, "source": source}
    if tier:
        d["tier"] = tier
    return json.dumps(d)


class TestDedupeQueuesOnce(unittest.TestCase):
    def setUp(self):
        self.never = {"A", "B"}

    def test_enrichment_twin_removed_only_for_never_enriched(self):
        r = FakeRedis(enrichment={
            _m("A", "enrichment", tier="batch"): 5, _m("A", "staleness", tier="batch"): 5,
            _m("C", "enrichment", tier="batch"): 3, _m("C", "staleness", tier="batch"): 3,  # enriched: untouched
        })
        dq.dedupe_enrichment(r, self.never, dry_run=False)
        keys = set(r.z["enrichment:batch"])
        self.assertIn(_m("A", "enrichment", tier="batch"), keys)
        self.assertNotIn(_m("A", "staleness", tier="batch"), keys)
        self.assertIn(_m("C", "staleness", tier="batch"), keys)
        self.assertEqual(len(keys), 3)

    def test_lone_staleness_member_is_converted_not_lost(self):
        r = FakeRedis(enrichment={_m("B", "staleness", tier="batch"): 9})
        dq.dedupe_enrichment(r, self.never, dry_run=False)
        keys = list(r.z["enrichment:batch"])
        self.assertEqual(len(keys), 1)
        self.assertEqual(json.loads(keys[0])["trigger"], "enrichment")
        self.assertEqual(r.z["enrichment:batch"][keys[0]], 9)

    def test_dry_run_changes_nothing(self):
        r = FakeRedis(
            enrichment={_m("A", "enrichment", tier="batch"): 5, _m("A", "staleness", tier="batch"): 5},
            discovery={_m("A", "staleness"): 5})
        before = {k: dict(v) for k, v in r.z.items()}
        dq.dedupe_enrichment(r, self.never, dry_run=True)
        dq.dedupe_discovery(r, self.never, dry_run=True)
        self.assertEqual(r.z, before)

    def test_discovery_removes_only_plain_members_of_never_enriched(self):
        keep = [
            _m("A", "redetect"), _m("A", "re_detection"), _m("A", "manual"),
            _m("A", "staleness", source="company_ats"), _m("A", "enrichment", source="prospective"),
            _m("C", "staleness"),                                    # already enriched
        ]
        drop = [_m("A", "staleness"), _m("B", "enrichment")]
        r = FakeRedis(discovery={m: 1 for m in keep + drop})
        r.z["discovery:batch"]["legacy-bare-fein"] = 1
        dq.dedupe_discovery(r, self.never, dry_run=False)
        left = set(r.z["discovery:batch"])
        for m in keep:
            self.assertIn(m, left)
        for m in drop:
            self.assertNotIn(m, left)
        self.assertIn("legacy-bare-fein", left)


if __name__ == "__main__":
    unittest.main()
