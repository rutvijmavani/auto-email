"""
tests/test_detail_payload.py — workers.detail_payload.build_detail_push and the
queue choice rebuild_detail_queue makes with it (TH step 3, bug H5).

Covers:
  · found_by picks the queue: tier2_fullscan -> fullscan, everything else -> adaptive
  · the row's real found_by is kept (stored payload as is; legacy rows get it too)
  · NULL found_by on a legacy row falls back to "rebuild"
  · legacy rows of ephemeral-key platforms are skipped (None), stored payloads are not
  · rebuild pushes a fullscan row to the fullscan queue and counts the arrival there
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN
from workers.detail_payload import build_detail_push, queue_for_found_by


def _row(**kw):
    row = {"company": "A", "job_url": "u1", "job_id": "j1",
           "ats_platform": "greenhouse", "detail_payload": None,
           "found_by": "tier1_adaptive"}
    row.update(kw)
    return row


class TestQueueChoice(unittest.TestCase):

    def test_fullscan_goes_to_fullscan_queue(self):
        self.assertEqual(queue_for_found_by("tier2_fullscan"), REDIS_DETAIL_FULLSCAN)

    def test_others_go_to_adaptive_queue(self):
        for fb in ("tier1_adaptive", "first_scan_fresh", "rebuild", None, ""):
            self.assertEqual(queue_for_found_by(fb), REDIS_DETAIL_ADAPTIVE, fb)


class TestBuildDetailPush(unittest.TestCase):

    def test_stored_payload_dict_keeps_keys_and_gets_queued_at(self):
        stored = {"job_id": "j1", "found_by": "tier2_fullscan",
                  "enqueued_at": "2026-10-01T02:00:00+00:00", "_slug": "x"}
        queue, payload = build_detail_push(
            _row(detail_payload=stored, found_by="tier2_fullscan"))
        out = json.loads(payload)
        self.assertEqual(queue, REDIS_DETAIL_FULLSCAN)
        self.assertEqual(out["_slug"], "x")
        self.assertEqual(out["found_by"], "tier2_fullscan")
        self.assertEqual(out["enqueued_at"], stored["enqueued_at"])
        self.assertIn("queued_at", out)
        self.assertNotIn("queued_at", stored)

    def test_stored_payload_json_string(self):
        stored = json.dumps({"job_id": "j1", "found_by": "first_scan_fresh"})
        queue, payload = build_detail_push(
            _row(detail_payload=stored, found_by="first_scan_fresh"))
        self.assertEqual(queue, REDIS_DETAIL_ADAPTIVE)
        self.assertIn("queued_at", json.loads(payload))

    def test_unparsable_stored_payload_is_pushed_raw(self):
        queue, payload = build_detail_push(_row(detail_payload="not json"))
        self.assertEqual((queue, payload), (REDIS_DETAIL_ADAPTIVE, "not json"))

    def test_legacy_row_keeps_real_found_by(self):
        queue, payload = build_detail_push(_row(found_by="tier2_fullscan"))
        out = json.loads(payload)
        self.assertEqual(queue, REDIS_DETAIL_FULLSCAN)
        self.assertEqual(out["found_by"], "tier2_fullscan")
        self.assertEqual(out["job_url"], "u1")
        self.assertIn("queued_at", out)

    def test_legacy_row_null_found_by_falls_back_to_rebuild(self):
        queue, payload = build_detail_push(_row(found_by=None))
        self.assertEqual(queue, REDIS_DETAIL_ADAPTIVE)
        self.assertEqual(json.loads(payload)["found_by"], "rebuild")

    def test_legacy_ephemeral_platform_is_skipped(self):
        for platform in ("workday", "taleo", "smartrecruiters", "icims", "jobvite"):
            self.assertIsNone(build_detail_push(_row(ats_platform=platform)), platform)

    def test_stored_payload_for_ephemeral_platform_is_pushed(self):
        push = build_detail_push(
            _row(ats_platform="workday", detail_payload={"job_id": "j1", "_wd": "w"}))
        self.assertIsNotNone(push)


class TestRebuildQueueChoice(unittest.TestCase):

    def test_fullscan_row_goes_to_fullscan_queue_and_is_counted_there(self):
        from datetime import date
        day = date(2026, 10, 1)
        rows = [
            _row(job_id="a", found_by="tier1_adaptive", first_seen=day),
            _row(job_id="f", found_by="tier2_fullscan", first_seen=day),
        ]
        r = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = rows
        with patch("workers.rebuild.get_redis", return_value=r), \
             patch("workers.rebuild.get_conn", return_value=conn), \
             patch("workers.rebuild.count_arrival") as count_arrival:
            from workers.rebuild import rebuild_detail_queue
            count = rebuild_detail_queue()
        self.assertEqual(count, 2)
        pushed = {json.loads(c.args[1])["job_id"]: c.args[0] for c in r.lpush.call_args_list}
        self.assertEqual(pushed["a"], REDIS_DETAIL_ADAPTIVE)
        self.assertEqual(pushed["f"], REDIS_DETAIL_FULLSCAN)
        counted = [(c.args[1], c.args[2]) for c in count_arrival.call_args_list]
        self.assertIn((REDIS_DETAIL_FULLSCAN, "rebuild"), counted)
        self.assertIn((REDIS_DETAIL_ADAPTIVE, "rebuild"), counted)


if __name__ == "__main__":
    unittest.main()
