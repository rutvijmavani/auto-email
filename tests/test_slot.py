"""
tests/test_slot.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for workers/slot.py — slot_offset(identifier).

The key design goal: uses MD5 rather than Python's built-in hash() so that
small integers (batch_position=1,2,3…) are distributed uniformly across
[0, 86400) instead of clustering at the start of midnight.

Coverage map
────────────
  TestSlotOffsetReturnType
    · Returns int for int input
    · Returns int for string input
    · Returns int for float input

  TestSlotOffsetRange
    · Result always in [0, 86400) for ints 0–99
    · Result always in [0, 86400) for company name strings
    · Result always in [0, 86400) for large integers
    · Result never equals 86400 (upper bound is exclusive)
    · Result never negative

  TestSlotOffsetDeterminism
    · Same int input → same output across repeated calls
    · Same string input → same output across repeated calls
    · Matches manually computed MD5 value

  TestSlotOffsetNotIdentity
    · slot_offset(1) ≠ 1  (avoids Python hash(1)==1 clustering)
    · slot_offset(2) ≠ 2
    · slot_offset(3) ≠ 3
    · slot_offset(0) ≠ 0 (or if equal, passes anyway — key test is distribution)

  TestSlotOffsetDistribution
    · First 10 consecutive batch positions all produce distinct offsets
    · 100 consecutive inputs: no single 10-minute window (600s) holds more than
      10% of values (basic anti-clustering check)
    · Mean offset across 100 inputs is roughly in the middle third of [0, 86400)
      (not biased toward midnight)

  TestSlotOffsetEdgeCases
    · Works with negative integer
    · Works with 0
    · Works with very large integer (>2^64)
    · Works with None input (str(None) = "None")
    · Works with float (3.7 → "3.7" as string key)
    · Works with empty string (edge: str of empty = "")

  TestSlotOffsetDocstringExamples
    · Docstring says slot_offset(1) → 27 291 — verify dynamically against MD5
    · Docstring says slot_offset(2) → 68 104 — verify dynamically
    · batch_position values 1–5 all produce positive offsets within range
"""

import hashlib
import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from workers.slot import slot_offset


def _expected(identifier) -> int:
    """Reference implementation matching slot.py exactly."""
    digest = hashlib.md5(str(identifier).encode()).hexdigest()
    return int(digest, 16) % 86400


# ─────────────────────────────────────────────────────────────────────────────
# TestSlotOffsetReturnType
# ─────────────────────────────────────────────────────────────────────────────

class TestSlotOffsetReturnType(unittest.TestCase):

    def test_returns_int_for_int(self):
        """slot_offset(int) returns int."""
        self.assertIsInstance(slot_offset(1), int)

    def test_returns_int_for_string(self):
        """slot_offset(str) returns int."""
        self.assertIsInstance(slot_offset("Stripe"), int)

    def test_returns_int_for_float(self):
        """slot_offset(float) returns int."""
        self.assertIsInstance(slot_offset(3.7), int)


# ─────────────────────────────────────────────────────────────────────────────
# TestSlotOffsetRange
# ─────────────────────────────────────────────────────────────────────────────

class TestSlotOffsetRange(unittest.TestCase):

    def test_in_range_for_consecutive_ints(self):
        """slot_offset(0–99) always in [0, 86400)."""
        for i in range(100):
            result = slot_offset(i)
            self.assertGreaterEqual(result, 0, msg=f"slot_offset({i}) < 0")
            self.assertLess(result, 86400, msg=f"slot_offset({i}) >= 86400")

    def test_in_range_for_company_names(self):
        """slot_offset(company_name) always in [0, 86400)."""
        companies = ["Stripe", "Airbnb", "Google", "Meta", "Accenture",
                     "Cloudflare", "OpenAI", "Anthropic", "Palantir", "Snowflake"]
        for name in companies:
            result = slot_offset(name)
            self.assertGreaterEqual(result, 0)
            self.assertLess(result, 86400)

    def test_in_range_for_large_int(self):
        """slot_offset(very large int) in [0, 86400)."""
        result = slot_offset(10 ** 20)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 86400)

    def test_never_equals_upper_bound(self):
        """86400 itself should never be returned (upper bound exclusive)."""
        for i in range(200):
            self.assertNotEqual(slot_offset(i), 86400)

    def test_never_negative(self):
        """No input should produce a negative offset."""
        for x in [0, 1, 100, -1, -100, "test", None, 0.5]:
            self.assertGreaterEqual(slot_offset(x), 0)


# ─────────────────────────────────────────────────────────────────────────────
# TestSlotOffsetDeterminism
# ─────────────────────────────────────────────────────────────────────────────

class TestSlotOffsetDeterminism(unittest.TestCase):

    def test_same_int_same_result(self):
        """Same integer input always produces the same offset."""
        for i in [1, 5, 42, 999]:
            self.assertEqual(slot_offset(i), slot_offset(i))

    def test_same_string_same_result(self):
        """Same string input always produces the same offset."""
        for s in ["Stripe", "Greenhouse", "Lever"]:
            self.assertEqual(slot_offset(s), slot_offset(s))

    def test_matches_manual_md5_int(self):
        """slot_offset(n) matches int(md5(str(n)).hexdigest(), 16) % 86400."""
        for i in range(20):
            self.assertEqual(slot_offset(i), _expected(i))

    def test_matches_manual_md5_string(self):
        """slot_offset(s) matches the reference MD5 implementation for strings."""
        for s in ["Stripe", "Anthropic", "hello"]:
            self.assertEqual(slot_offset(s), _expected(s))


# ─────────────────────────────────────────────────────────────────────────────
# TestSlotOffsetNotIdentity
# ─────────────────────────────────────────────────────────────────────────────

class TestSlotOffsetNotIdentity(unittest.TestCase):
    """
    Key design invariant: slot_offset(n) ≠ n for small integers.
    Python's built-in hash(1)==1, hash(2)==2, etc., which causes clustering.
    MD5 breaks this — batch positions 1..10 should NOT equal 1..10.
    """

    def test_not_identity_for_1(self):
        """slot_offset(1) ≠ 1 (not Python's hash identity)."""
        self.assertNotEqual(slot_offset(1), 1)

    def test_not_identity_for_2(self):
        self.assertNotEqual(slot_offset(2), 2)

    def test_not_identity_for_3(self):
        self.assertNotEqual(slot_offset(3), 3)

    def test_not_identity_for_small_batch(self):
        """None of slot_offset(1..10) equals its own input."""
        for i in range(1, 11):
            self.assertNotEqual(
                slot_offset(i), i,
                msg=f"slot_offset({i}) == {i} — hash-identity clustering detected!",
            )

    def test_spread_across_day_for_small_ints(self):
        """
        slot_offset(1..5) produces values spread widely across [0, 86400),
        not all within the first 10 seconds (which hash(n) would cause).
        """
        offsets = [slot_offset(i) for i in range(1, 6)]
        # If all 5 values are within 10 seconds of each other, we have clustering.
        spread = max(offsets) - min(offsets)
        self.assertGreater(
            spread, 100,
            msg=f"Offsets {offsets} are suspiciously clustered (spread={spread}s)",
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestSlotOffsetDistribution
# ─────────────────────────────────────────────────────────────────────────────

class TestSlotOffsetDistribution(unittest.TestCase):

    def test_first_10_positions_all_distinct(self):
        """First 10 consecutive batch positions all produce distinct offsets."""
        offsets = [slot_offset(i) for i in range(1, 11)]
        self.assertEqual(len(offsets), len(set(offsets)),
                         msg=f"Duplicate offsets in first 10: {offsets}")

    def test_no_excessive_clustering_in_any_10min_window(self):
        """
        For 100 inputs, no single 10-minute (600s) window should hold
        more than 10 entries (10% of 100) — a basic anti-clustering test.
        """
        offsets = [slot_offset(i) for i in range(1, 101)]
        window_size = 600   # 10 minutes
        buckets = [0] * (86400 // window_size + 1)
        for o in offsets:
            buckets[o // window_size] += 1
        max_in_window = max(buckets)
        self.assertLessEqual(
            max_in_window, 10,
            msg=(f"Clustering detected: {max_in_window} of 100 offsets "
                 f"fell in the same 10-min window — check MD5 distribution"),
        )

    def test_mean_offset_in_middle_third(self):
        """
        Mean offset across 100 inputs should fall in the middle third of
        [0, 86400) — i.e. between 28800 (8h) and 57600 (16h) — confirming
        the distribution is not heavily biased toward midnight or end-of-day.
        """
        offsets = [slot_offset(i) for i in range(1, 101)]
        mean_offset = sum(offsets) / len(offsets)
        self.assertGreater(mean_offset, 86400 // 3,   # > 28800s (8h)
                           msg=f"Mean offset {mean_offset:.0f}s < 28800s (8h) — biased toward midnight")
        self.assertLess(mean_offset, 2 * 86400 // 3,  # < 57600s (16h)
                        msg=f"Mean offset {mean_offset:.0f}s > 57600s (16h) — biased toward end-of-day")

    def test_all_100_in_valid_range(self):
        """Sanity: all 100 outputs are still in [0, 86400)."""
        for i in range(1, 101):
            r = slot_offset(i)
            self.assertGreaterEqual(r, 0)
            self.assertLess(r, 86400)


# ─────────────────────────────────────────────────────────────────────────────
# TestSlotOffsetEdgeCases
# ─────────────────────────────────────────────────────────────────────────────

class TestSlotOffsetEdgeCases(unittest.TestCase):

    def test_negative_integer(self):
        """Negative integer input works and returns value in range."""
        result = slot_offset(-1)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 86400)

    def test_zero(self):
        """slot_offset(0) in [0, 86400)."""
        result = slot_offset(0)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 86400)

    def test_very_large_integer(self):
        """Integer larger than 2^64 works without crash."""
        result = slot_offset(2 ** 128 + 7)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 86400)

    def test_none_input(self):
        """None input: str(None)='None' — should not raise."""
        result = slot_offset(None)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 86400)

    def test_float_input(self):
        """Float input: str(3.7)='3.7' — should work."""
        result = slot_offset(3.7)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 86400)

    def test_empty_string(self):
        """Empty string input should work."""
        result = slot_offset("")
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 86400)

    def test_negative_int_matches_reference(self):
        """slot_offset(-5) matches reference MD5 computation."""
        self.assertEqual(slot_offset(-5), _expected(-5))


# ─────────────────────────────────────────────────────────────────────────────
# TestSlotOffsetDocstringExamples
# ─────────────────────────────────────────────────────────────────────────────

class TestSlotOffsetDocstringExamples(unittest.TestCase):
    """
    The docstring lists example values for batch_positions 1–5.
    We verify them dynamically (not hardcoded) so a hash-function change
    would immediately fail here.
    """

    def test_slot_offset_1_matches_docstring(self):
        """slot_offset(1) → 27 291 s as per docstring (verified via MD5)."""
        expected = _expected(1)
        self.assertEqual(slot_offset(1), expected)
        # The docstring says 27 291 — verify this matches our reference too
        self.assertEqual(expected, 27291,
                         msg=f"Docstring example changed: expected 27291, got {expected}")

    def test_slot_offset_2_matches_docstring(self):
        """slot_offset(2) → 68 104 s as per docstring."""
        expected = _expected(2)
        self.assertEqual(slot_offset(2), expected)
        self.assertEqual(expected, 68104,
                         msg=f"Docstring example changed: expected 68104, got {expected}")

    def test_batch_positions_1_to_5_all_in_range(self):
        """Docstring example: batch_positions 1–5 all in [0, 86400)."""
        for i in range(1, 6):
            r = slot_offset(i)
            self.assertGreaterEqual(r, 0)
            self.assertLess(r, 86400)

    def test_batch_positions_1_to_5_all_positive(self):
        """All documented batch positions produce positive offsets (> 0)."""
        for i in range(1, 6):
            self.assertGreater(slot_offset(i), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
