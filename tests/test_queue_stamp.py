"""
tests/test_queue_stamp.py — queued_at stamp (Signal bug STALEQ).

Covers:
  · stamp_queued_at adds queued_at without mutating the input or touching enqueued_at
  · payload_wait_s prefers queued_at, falls back to enqueued_at, returns None when unknown
  · rebuild_detail_queue adds a fresh queued_at, keeps enqueued_at, and orders
    same-day rows by the stored enqueued_at (oldest pushed first)
"""

import json
import os
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.queue_stamp import payload_wait_s, stamp_queued_at


class TestStampHelpers(unittest.TestCase):

    def test_stamp_adds_queued_at_and_keeps_input(self):
        src = {"job_id": "1", "enqueued_at": "2026-01-01T00:00:00+00:00"}
        out = stamp_queued_at(src)
        self.assertIn("queued_at", out)
        self.assertNotIn("queued_at", src)
        self.assertEqual(out["enqueued_at"], src["enqueued_at"])

    def test_wait_prefers_queued_at(self):
        now = datetime.now(timezone.utc)
        raw = json.dumps({
            "enqueued_at": (now - timedelta(hours=6)).isoformat(),
            "queued_at":   (now - timedelta(seconds=30)).isoformat(),
        })
        wait = payload_wait_s(raw, now.timestamp())
        self.assertAlmostEqual(wait, 30.0, delta=1.0)

    def test_wait_falls_back_to_enqueued_at(self):
        now = datetime.now(timezone.utc)
        raw = json.dumps({"enqueued_at": (now - timedelta(seconds=90)).isoformat()})
        self.assertAlmostEqual(payload_wait_s(raw, now.timestamp()), 90.0, delta=1.0)

    def test_wait_unknown_is_none_not_zero(self):
        self.assertIsNone(payload_wait_s(json.dumps({"job_id": "1"}), 1.0))
        self.assertIsNone(payload_wait_s("not json", 1.0))
        self.assertIsNone(payload_wait_s(None, 1.0))


class TestRebuildStamp(unittest.TestCase):

    def _run_rebuild(self, rows):
        r = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = rows
        with patch("workers.rebuild.get_redis", return_value=r), \
             patch("workers.rebuild.get_conn", return_value=conn):
            from workers.rebuild import rebuild_detail_queue
            count = rebuild_detail_queue()
        pushed = [json.loads(c.args[1]) for c in r.lpush.call_args_list]
        return count, pushed

    def test_rebuild_adds_queued_at_keeps_enqueued_at_orders_same_day(self):
        day = date(2026, 10, 1)
        newer = {"job_id": "new", "enqueued_at": "2026-10-01T10:00:00+00:00"}
        older = {"job_id": "old", "enqueued_at": "2026-10-01T02:00:00+00:00"}
        rows = [
            {"company": "A", "job_url": "u1", "job_id": "new",
             "ats_platform": "greenhouse", "detail_payload": newer, "first_seen": day,
             "found_by": "tier1_adaptive"},
            {"company": "A", "job_url": "u2", "job_id": "old",
             "ats_platform": "greenhouse", "detail_payload": json.dumps(older),
             "first_seen": day, "found_by": "tier1_adaptive"},
            {"company": "B", "job_url": "u3", "job_id": "legacy",
             "ats_platform": "greenhouse", "detail_payload": None, "first_seen": day,
             "found_by": None},
        ]
        count, pushed = self._run_rebuild(rows)
        self.assertEqual(count, 3)
        self.assertTrue(all("queued_at" in p for p in pushed))
        ids = [p["job_id"] for p in pushed]
        self.assertLess(ids.index("old"), ids.index("new"))
        by_id = {p["job_id"]: p for p in pushed}
        self.assertEqual(by_id["old"]["enqueued_at"], older["enqueued_at"])
        self.assertEqual(by_id["new"]["enqueued_at"], newer["enqueued_at"])


if __name__ == "__main__":
    unittest.main()
