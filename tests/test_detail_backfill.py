"""
Step 9b of docs/detail-pool-ceiling-th-design.md: the cut-over backfill stamps
pushed_at only for jobs Redis already holds; rebuild pushes only released rows.
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_DLQ, REDIS_DETAIL_FULLSCAN
from scripts import backfill_detail_pushed_at as bf


def _p(url):
    return json.dumps({"job_url": url}).encode()


def _dlq(url):
    """A DLQ entry as detail_worker._push_to_dlq writes it."""
    return json.dumps({"_dlq_added_at": "2026-01-01T00:00:00+00:00",
                       "payload": {"job_url": url}}).encode()


def _redis(lists=None, zsets=None):
    lists, zsets = lists or {}, zsets or {}
    r = MagicMock()
    r.lrange.side_effect = lambda k, a, b: lists.get(k, [])
    r.zrange.side_effect = lambda k, a, b: zsets.get(k, [])
    r.scan_iter.side_effect = lambda match, count: [
        k for k in lists if k.startswith(match[:-1])
    ]
    return r


class TestCollect(unittest.TestCase):

    def test_reads_queue_inflight_retry_and_dlq(self):
        r = _redis(
            lists={
                REDIS_DETAIL_ADAPTIVE: [_p("a"), _p("b")],
                f"{REDIS_DETAIL_ADAPTIVE}:inflight:host:1": [_p("c")],
                REDIS_DETAIL_FULLSCAN: [_p("d")],
                REDIS_DETAIL_DLQ: [_dlq("e"), _p("g"), b"not json", b'{"payload": 5}'],
            },
            zsets={f"{REDIS_DETAIL_FULLSCAN}:retry_delay": [_p("f")]},
        )
        found = bf.collect_queued_urls(r)
        union = set().union(*found.values())
        self.assertEqual(union, {"a", "b", "c", "d", "e", "f", "g"})


class TestStamp(unittest.TestCase):

    def test_update_only_touches_held_rows_in_batches(self):
        conn = MagicMock()
        conn.execute.return_value.rowcount = 2
        urls = [f"u{i}" for i in range(bf._UPDATE_BATCH + 1)]
        self.assertEqual(bf.stamp_urls(conn, urls, dry_run=False), 4)
        sql = conn.execute.call_args_list[0][0][0]
        self.assertIn("pushed_at IS NULL", sql)
        self.assertIn("status = 'pending_detail'", sql)
        conn.commit.assert_called_once()

    def test_dry_run_writes_nothing(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = {"n": 3}
        self.assertEqual(bf.stamp_urls(conn, ["x"], dry_run=True), 3)
        self.assertNotIn("UPDATE", conn.execute.call_args[0][0])
        conn.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
