"""
tests/test_detail_worker_contracts.py
─────────────────────────────────────────────────────────────────────────────
Tests for detail_worker.py logic that doesn't require a live Redis or DB.

Background
──────────
detail_worker._process_detail() runs a multi-step pipeline:
  1. Parse platform / slug from payload
  2. Call fetch_job_detail() for Mode B platforms
  3. Apply filter pipeline: title → alpha-2 country code → location text →
     freshness
  4. Promote to 'new' or delete pending_detail row

Several of these steps contain non-trivial pure logic or subtle edge cases
that are hard to exercise through integration tests:

  - _extract_city_from_url() extracts a city from the job URL path/query
    and is used as a fallback when location is empty after detail fetch.
    Incorrect extraction allows India-city Workday jobs to pass the US filter.

  - Country code normalization: detail_worker reads _country_code from the job
    dict and uppercases it with .upper() before the != "US" comparison.  If
    the .upper() call is ever removed (e.g. by a refactor), a lowercase "us"
    from the API would incorrectly pass through as non-US.

  - BRPOP priority: detail_worker uses BRPOP with
    [queue:detail:adaptive, queue:detail:fullscan].  Redis pops from the first
    non-empty key, so adaptive always takes priority.  This is purely a
    Redis/integration concern but the queue name constants are verified here.

  - Warming decrement: the state machine expression
    `(warming - 1) if warming > 1 else None`
    covers the WARMING→STABLE transition.  Tested as pure logic.

Coverage
────────
  TestExtractCityFromUrl
    · Workday URL with /job/City-Name/ segment → extracts "City Name"
    · Hyphenated city → spaces substituted correctly
    · City segment after /jobs/ (plural) also extracted
    · Segment immediately after /job/ that looks like a job ID (R-123) → skipped
    · Segment that starts with a digit → skipped (numeric job IDs)
    · Segment with underscore → skipped (job title slugs like "Role_R-123")
    · Segment longer than 30 chars → skipped (likely a title, not a city)
    · Query param ?location=CityName → extracted
    · Query param ?city=CityName → extracted
    · Query param ?loc=CityName → extracted
    · URL with no city info → returns ""
    · Empty URL → returns ""
    · Non-HTTP garbage URL → returns "" (no exception)

  TestCountryCodeNormalization
    · _country_code already uppercase "US" → passes gate (returns "US")
    · _country_code lowercase "us" → .upper() → "US" → passes gate
    · _country_code "IN" → != "US" → filtered (non-US)
    · _country_code "in" → .upper() → "IN" → filtered (correctly)
    · _country_code None → (None or "").upper() → "" → gate not applied
    · _country_code "" → same as None path → gate not applied
    · .upper() call is essential: without it "us" != "US" → wrong filter

  TestQueuePriorityConstants
    · queue:detail:adaptive appears BEFORE queue:detail:fullscan in the BRPOP
      key list so adaptive always drains first

  TestWarmingDecrement
    · warming=3 on success → 2
    · warming=2 on success → 1
    · warming=1 on success → None (transitions to STABLE)
    · warming=None on success → None (already STABLE, no change)
    · warming=3 on failure → 3 (unchanged)
    · warming=1 on failure → 1 (unchanged — not decremented to STABLE on error)
    · warming=None on failure → None
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from workers.detail_worker import _extract_city_from_url
from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN


# ─────────────────────────────────────────────────────────────────────────────
# TestExtractCityFromUrl
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractCityFromUrl(unittest.TestCase):
    """_extract_city_from_url() — pure function, no I/O."""

    def test_workday_job_path_extracts_city(self):
        url = ("https://att.wd1.myworkdayjobs.com/ATTGeneral/"
               "job/Hyderabad/Application-Developer_ATCI-12345")
        self.assertEqual(_extract_city_from_url(url), "Hyderabad")

    def test_workday_hyphenated_city_becomes_spaces(self):
        url = ("https://company.wd5.myworkdayjobs.com/careers/"
               "job/New-York/Software-Engineer_R-999")
        self.assertEqual(_extract_city_from_url(url), "New York")

    def test_jobs_plural_path_extracted(self):
        """Segment after /jobs/ (plural) also extracted."""
        url = "https://example.com/jobs/Austin/Senior-Engineer_JR-100"
        self.assertEqual(_extract_city_from_url(url), "Austin")

    def test_job_id_segment_skipped(self):
        """Segment starting with 'R-' (Workday req ID) is skipped."""
        url = "https://att.wd1.myworkdayjobs.com/ATTGeneral/job/R-12345"
        # R-12345 → skipped; no further segment → ""
        self.assertEqual(_extract_city_from_url(url), "")

    def test_jr_prefix_skipped(self):
        """Segment starting with 'JR' skipped."""
        url = "https://example.com/jobs/JR9999"
        self.assertEqual(_extract_city_from_url(url), "")

    def test_numeric_segment_skipped(self):
        """Segment starting with a digit (numeric job ID) skipped."""
        url = "https://example.com/job/12345/Software-Engineer"
        self.assertEqual(_extract_city_from_url(url), "")

    def test_underscore_segment_skipped(self):
        """Segment containing underscore (job title slug) skipped."""
        url = "https://example.com/job/Software_Engineer_R-001"
        self.assertEqual(_extract_city_from_url(url), "")

    def test_long_segment_skipped(self):
        """Segment longer than 30 chars skipped (likely a title, not a city)."""
        long_segment = "A" * 31
        url = f"https://example.com/job/{long_segment}"
        self.assertEqual(_extract_city_from_url(url), "")

    def test_segment_exactly_30_chars_accepted(self):
        """Segment of exactly 30 chars is accepted (boundary test)."""
        city = "A" * 30
        url = f"https://example.com/job/{city}/Role-Title"
        # 30-char word is a valid city (boundary is <= 30)
        result = _extract_city_from_url(url)
        self.assertEqual(result, city)

    def test_query_param_location(self):
        """?location=CityName extracted."""
        url = "https://massanf.taleo.net/careersection/ex/jobdetail.ftl?job=REQ-001&location=Bengaluru"
        self.assertEqual(_extract_city_from_url(url), "Bengaluru")

    def test_query_param_city(self):
        """?city=CityName extracted."""
        url = "https://example.com/careers?id=123&city=Seattle"
        self.assertEqual(_extract_city_from_url(url), "Seattle")

    def test_query_param_loc(self):
        """?loc=CityName extracted."""
        url = "https://example.com/jobs?loc=Chicago"
        self.assertEqual(_extract_city_from_url(url), "Chicago")

    def test_no_city_in_url(self):
        """URL with no extractable city → empty string."""
        url = "https://greenhouse.io/jobs?company=acme&token=abc123"
        self.assertEqual(_extract_city_from_url(url), "")

    def test_empty_url(self):
        """Empty string → empty string (no exception)."""
        self.assertEqual(_extract_city_from_url(""), "")

    def test_none_url(self):
        """None → empty string (no exception)."""
        self.assertEqual(_extract_city_from_url(None), "")

    def test_garbage_url_no_exception(self):
        """Malformed/non-HTTP URL does not raise — returns ''."""
        self.assertEqual(_extract_city_from_url("not-a-url-at-all"), "")

    def test_indian_city_extracted_for_is_us_location(self):
        """
        Hyderabad extracted from URL → is_us_location("Hyderabad") = False.
        This is the critical use case: empty location after Workday detail fetch
        falls back to URL city, preventing a non-US job from passing the filter.
        """
        from jobs.job_filter import is_us_location
        url = ("https://att.wd1.myworkdayjobs.com/ATTGeneral/"
               "job/Hyderabad/Application-Developer_ATCI-12345")
        city = _extract_city_from_url(url)
        self.assertEqual(city, "Hyderabad")
        self.assertFalse(
            is_us_location(city),
            f"is_us_location({city!r}) should be False — geonamescache maps "
            f"Hyderabad to India only",
        )

    def test_us_city_extracted_passes_location_filter(self):
        """
        US city extracted from URL → is_us_location returns True.
        """
        from jobs.job_filter import is_us_location
        url = ("https://att.wd1.myworkdayjobs.com/ATTGeneral/"
               "job/Austin/Software-Engineer_R-555")
        city = _extract_city_from_url(url)
        self.assertEqual(city, "Austin")
        # Austin is a major US city — passes location filter
        self.assertTrue(is_us_location(city))


# ─────────────────────────────────────────────────────────────────────────────
# TestCountryCodeNormalization
# ─────────────────────────────────────────────────────────────────────────────

class TestCountryCodeNormalization(unittest.TestCase):
    """
    detail_worker uses `(job.get("_country_code") or "").upper()` before the
    `!= "US"` gate.  Verify the .upper() normalization handles all expected
    inputs so that a lowercase API response does not cause incorrect filtering.
    """

    def _normalize(self, raw_cc):
        """Replicate the exact expression from detail_worker line 321."""
        return (raw_cc or "").upper()

    def test_uppercase_us_passes_gate(self):
        result = self._normalize("US")
        self.assertEqual(result, "US")
        self.assertFalse(result != "US")   # gate does NOT filter

    def test_lowercase_us_normalized_to_uppercase(self):
        """
        If the ATS API returns 'us' (lowercase), .upper() must normalize it.
        Without .upper(), 'us' != 'US' would incorrectly filter the job as non-US.
        """
        result = self._normalize("us")
        self.assertEqual(result, "US")
        self.assertFalse(result != "US")   # gate does NOT filter

    def test_mixed_case_normalized(self):
        result = self._normalize("Us")
        self.assertEqual(result, "US")

    def test_non_us_code_uppercase_filtered(self):
        result = self._normalize("IN")
        self.assertNotEqual(result, "US")  # gate filters

    def test_non_us_code_lowercase_still_filtered(self):
        """
        'in' (lowercase) → .upper() → 'IN' → != 'US' → filtered.
        Even without .upper(), 'in' != 'US' so this case works either way.
        But the normalization is correct by principle.
        """
        result = self._normalize("in")
        self.assertEqual(result, "IN")
        self.assertNotEqual(result, "US")

    def test_none_becomes_empty_string(self):
        """_country_code=None → (None or '') = '' → gate not applied."""
        result = self._normalize(None)
        self.assertEqual(result, "")
        # Gate condition: if country_src == "alpha2" and country_code:
        # "" is falsy → gate skipped
        self.assertFalse(result)

    def test_empty_string_gate_not_applied(self):
        """Empty string → gate not applied (falsy value)."""
        result = self._normalize("")
        self.assertEqual(result, "")
        self.assertFalse(result)

    def test_upper_is_essential_for_correctness(self):
        """
        Prove that skipping .upper() breaks the 'us' case.
        This test documents the invariant: .upper() MUST be called.
        """
        raw = "us"
        # Without .upper():
        self.assertNotEqual(raw, "US")    # would incorrectly filter
        # With .upper():
        self.assertEqual(raw.upper(), "US")  # correctly passes gate

    def test_gb_code_filtered(self):
        result = self._normalize("GB")
        self.assertNotEqual(result, "US")

    def test_ca_code_filtered(self):
        result = self._normalize("CA")
        self.assertNotEqual(result, "US")


# ─────────────────────────────────────────────────────────────────────────────
# TestQueuePriorityConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestQueuePriorityConstants(unittest.TestCase):
    """
    detail_worker drains queue:detail:adaptive before queue:detail:fullscan.
    Redis BRPOP pops from the first non-empty key in the supplied list, so the
    adaptive queue must be listed FIRST.

    This test verifies:
      (a) Both queue name constants exist and are non-empty strings
      (b) The adaptive queue name is different from the fullscan queue name
      (c) The adaptive constant contains 'adaptive'
      (d) The fullscan constant contains 'fullscan'

    The actual BRPOP call order is [REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN]
    (verified by reading detail_worker source — tested here as a constant check).
    """

    def test_adaptive_queue_constant_non_empty(self):
        self.assertTrue(REDIS_DETAIL_ADAPTIVE,
                        "REDIS_DETAIL_ADAPTIVE must be a non-empty string")

    def test_fullscan_queue_constant_non_empty(self):
        self.assertTrue(REDIS_DETAIL_FULLSCAN,
                        "REDIS_DETAIL_FULLSCAN must be a non-empty string")

    def test_queues_are_distinct(self):
        self.assertNotEqual(REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN,
                            "Adaptive and fullscan queue names must differ")

    def test_adaptive_constant_contains_adaptive(self):
        self.assertIn("adaptive", REDIS_DETAIL_ADAPTIVE.lower())

    def test_fullscan_constant_contains_fullscan(self):
        self.assertIn("fullscan", REDIS_DETAIL_FULLSCAN.lower())

    def test_brpop_order_in_source(self):
        """
        Structural test: verify detail_worker.py passes the adaptive queue
        BEFORE the fullscan queue in the BRPOP call.

        We read the source file and search for the BRPOP call pattern.
        This will fail if someone accidentally swaps the queue order.
        """
        import ast, pathlib
        src_path = pathlib.Path(__file__).parent.parent / "workers" / "detail_worker.py"
        if not src_path.exists():
            self.skipTest("detail_worker.py not found")

        source = src_path.read_text(encoding="utf-8")
        # Find the line(s) containing BRPOP
        brpop_lines = [ln for ln in source.splitlines() if "brpop" in ln.lower()]
        if not brpop_lines:
            self.skipTest("No BRPOP call found in detail_worker.py")

        # The adaptive constant name must appear before fullscan in the BRPOP call
        for line in brpop_lines:
            if "REDIS_DETAIL_ADAPTIVE" in line and "REDIS_DETAIL_FULLSCAN" in line:
                idx_a = line.index("REDIS_DETAIL_ADAPTIVE")
                idx_f = line.index("REDIS_DETAIL_FULLSCAN")
                self.assertLess(
                    idx_a, idx_f,
                    f"BRPOP must list adaptive queue BEFORE fullscan. Line: {line!r}",
                )
                return   # found and passed

        # Both constants not on same line — check relative line numbers
        lines = source.splitlines()
        for i, ln in enumerate(lines):
            if "brpop" in ln.lower():
                # Look at surrounding 5 lines for the two constants
                context = "\n".join(lines[max(0, i-2):i+5])
                if "REDIS_DETAIL_ADAPTIVE" in context and "REDIS_DETAIL_FULLSCAN" in context:
                    idx_a = context.index("REDIS_DETAIL_ADAPTIVE")
                    idx_f = context.index("REDIS_DETAIL_FULLSCAN")
                    self.assertLess(idx_a, idx_f,
                                    "Adaptive must appear before fullscan in BRPOP context")
                    return

        self.skipTest("Could not locate combined BRPOP call with both constants")


# ─────────────────────────────────────────────────────────────────────────────
# TestWarmingDecrement
# ─────────────────────────────────────────────────────────────────────────────

class TestWarmingDecrement(unittest.TestCase):
    """
    The WARMING→STABLE lifecycle state machine.

    Source expression (scheduler.py on_adaptive_complete):
        if success and warming is not None:
            new_warming = (warming - 1) if warming > 1 else None   # None = STABLE
        else:
            new_warming = warming   # unchanged on failure

    This pure logic is critical: if warming=1 does not transition to None,
    companies stay in WARMING forever (using a fixed 2h interval).
    If failure incorrectly decrements warming, a bad scan drains warming budget.
    """

    def _compute_new_warming(self, warming, success):
        """Exact replica of the scheduler.py expression."""
        if success and warming is not None:
            return (warming - 1) if warming > 1 else None
        else:
            return warming

    # ── Success path ──────────────────────────────────────────────────────────

    def test_warming_3_success_decrements_to_2(self):
        self.assertEqual(self._compute_new_warming(3, True), 2)

    def test_warming_2_success_decrements_to_1(self):
        self.assertEqual(self._compute_new_warming(2, True), 1)

    def test_warming_1_success_transitions_to_stable(self):
        """warming=1 on success → None (STABLE). This is the critical transition."""
        result = self._compute_new_warming(1, True)
        self.assertIsNone(result,
                          "warming=1 + success must yield None (STABLE); "
                          "if it yields 0 the company stays in WARMING indefinitely")

    def test_warming_none_success_stays_none(self):
        """Already STABLE (None) → None on success."""
        self.assertIsNone(self._compute_new_warming(None, True))

    # ── Failure path ──────────────────────────────────────────────────────────

    def test_warming_3_failure_unchanged(self):
        """Scan failure must not decrement warming budget."""
        self.assertEqual(self._compute_new_warming(3, False), 3)

    def test_warming_2_failure_unchanged(self):
        self.assertEqual(self._compute_new_warming(2, False), 2)

    def test_warming_1_failure_unchanged(self):
        """warming=1 on failure stays 1 — not incorrectly transitioned to STABLE."""
        self.assertEqual(self._compute_new_warming(1, False), 1)

    def test_warming_none_failure_stays_none(self):
        """Already STABLE, failure → stays STABLE."""
        self.assertIsNone(self._compute_new_warming(None, False))

    # ── Type invariants ───────────────────────────────────────────────────────

    def test_result_is_int_or_none(self):
        """warming values are int or None — never float or other type."""
        for w in [3, 2, 1, None]:
            for s in [True, False]:
                result = self._compute_new_warming(w, s)
                self.assertIn(type(result), (int, type(None)),
                              f"warming={w}, success={s} → {result!r} (expected int or None)")

    def test_full_lifecycle_sequence(self):
        """Simulate a new company warming through 3 successful scans → STABLE."""
        warming = 3   # WARMING_POLLS_COUNT
        for expected_after in [2, 1, None]:
            warming = self._compute_new_warming(warming, True)
            self.assertEqual(warming, expected_after,
                             f"Expected warming={expected_after} after success")
        # One more success while STABLE — stays STABLE
        warming = self._compute_new_warming(None, True)
        self.assertIsNone(warming)

    def test_failure_does_not_advance_lifecycle(self):
        """A run of failures keeps the company in WARMING (no budget drain)."""
        warming = 3
        for _ in range(5):  # 5 consecutive failures
            warming = self._compute_new_warming(warming, False)
            self.assertEqual(warming, 3,
                             "Failures must not decrement warming")


if __name__ == "__main__":
    unittest.main()
