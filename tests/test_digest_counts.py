"""Tests for workers/digest_counts.py (worker full-scan counts for the digest)."""
import unittest
from unittest.mock import MagicMock

from config import REDIS_DIGEST_FULLSCAN_COUNTS
from workers import digest_counts as dc


class _FakeRedis:
    def __init__(self):
        self.h, self.ttl = {}, None

    def hset(self, key, field, value):
        assert key == REDIS_DIGEST_FULLSCAN_COUNTS
        self.h[field] = value

    def expire(self, key, ttl):
        self.ttl = ttl

    def hgetall(self, key):
        return dict(self.h)

    def delete(self, key):
        self.h.clear()


class TestDigestCounts(unittest.TestCase):

    def test_sums_latest_count_per_company(self):
        r = _FakeRedis()
        dc.record_fullscan_counts(r, "A", 100, 10)
        dc.record_fullscan_counts(r, "B", 50, 5)
        dc.record_fullscan_counts(r, "A", 120, 12)   # rescan overwrites, never adds
        self.assertEqual(dc.read_fullscan_totals(r),
                         {"fetched": 170, "matched": 17, "companies": 2})
        self.assertIsNotNone(r.ttl)

    def test_exclude_skips_fallback_companies(self):
        r = _FakeRedis()
        dc.record_fullscan_counts(r, "A", 100, 10)
        dc.record_fullscan_counts(r, "B", 50, 5)
        self.assertEqual(dc.read_fullscan_totals(r, exclude={"B"})["fetched"], 100)

    def test_bytes_and_bad_values_tolerated(self):
        r = MagicMock()
        r.hgetall.return_value = {b"A": b"7:2", b"B": b"garbage", "C": "3:1"}
        self.assertEqual(dc.read_fullscan_totals(r),
                         {"fetched": 10, "matched": 3, "companies": 2})

    def test_reset_clears(self):
        r = _FakeRedis()
        dc.record_fullscan_counts(r, "A", 1, 1)
        dc.reset_fullscan_counts(r)
        self.assertEqual(dc.read_fullscan_totals(r)["companies"], 0)

    def test_redis_failures_never_raise(self):
        r = MagicMock()
        r.hset.side_effect = r.hgetall.side_effect = r.delete.side_effect = RuntimeError("down")
        dc.record_fullscan_counts(r, "A", 1, 1)
        self.assertEqual(dc.read_fullscan_totals(r)["fetched"], 0)
        dc.reset_fullscan_counts(r)
        self.assertEqual(dc.read_fullscan_totals(None)["fetched"], 0)


if __name__ == "__main__":
    unittest.main()
