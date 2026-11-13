"""sweep_inflight: slots are freed only when their stream message is gone."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.scheduler import sweep_inflight, _release_inflight


class _R:
    def __init__(self, zsets, msgs, last="100-0", pending=()):
        self.zsets = {k: list(v) for k, v in zsets.items()}
        self.kv = dict(msgs)
        self.last = last
        self.pending = set(pending)
        self.deleted = []

    def xinfo_groups(self, stream):
        return [{"name": "g", "last-delivered-id": self.last}]

    def scan_iter(self, match):
        prefix = match[:-1]
        return [k for k in self.zsets if k.startswith(prefix)]

    def zrange(self, key, a, b):
        return list(self.zsets.get(key, []))

    def zscore(self, key, member):
        return 0.0 if member in self.zsets.get(key, []) else None

    def get(self, key):
        return self.kv.get(key)

    def xpending_range(self, stream, group, min, max, count):
        return [{"message_id": min}] if min in self.pending else []

    def zrem(self, key, member):
        if member in self.zsets.get(key, []):
            self.zsets[key].remove(member)

    def delete(self, key):
        self.deleted.append(key)
        self.kv.pop(key, None)


ZK = "inflight:scans:dc1"


class TestSweepInflight(unittest.TestCase):
    def test_slot_without_message_is_released(self):
        r = _R({ZK: ["A"]}, {})
        sweep_inflight(r, "scan", "stream", "g")
        self.assertEqual(r.zsets[ZK], [])

    def test_undelivered_message_keeps_slot(self):
        r = _R({ZK: ["A"]}, {"inflight:msg:scan:A": "200-0"})
        sweep_inflight(r, "scan", "stream", "g")
        self.assertEqual(r.zsets[ZK], ["A"])

    def test_pending_message_keeps_slot_for_reaper(self):
        r = _R({ZK: ["A"]}, {"inflight:msg:scan:A": "50-0"}, pending={"50-0"})
        sweep_inflight(r, "scan", "stream", "g")
        self.assertEqual(r.zsets[ZK], ["A"])

    def test_delivered_and_acked_message_releases_slot(self):
        r = _R({ZK: ["A"]}, {"inflight:msg:scan:A": "50-0"})
        sweep_inflight(r, "scan", "stream", "g")
        self.assertEqual(r.zsets[ZK], [])
        self.assertIn("inflight:msg:scan:A", r.deleted)

    def test_fullscan_release_drops_lock_and_global_slot(self):
        dc = "inflight:fullscans:dc1"
        r = _R({dc: ["A"], "inflight:fullscan": ["A"]}, {})
        sweep_inflight(r, "fullscan", "stream", "g")
        self.assertEqual(r.zsets[dc], [])
        self.assertEqual(r.zsets["inflight:fullscan"], [])
        self.assertIn("fullscan:lock:A", r.deleted)

    def test_reap_release_drops_fullscan_lock(self):
        dc = "inflight:fullscans:dc1"
        r = _R({dc: ["A"], "inflight:fullscan": ["A"]}, {})
        _release_inflight(r, "fullscan", "A", "dc1")
        self.assertIn("fullscan:lock:A", r.deleted)


if __name__ == "__main__":
    unittest.main()
