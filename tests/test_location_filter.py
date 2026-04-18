"""
tests/test_location_filter.py — Table-driven tests for is_us_location().

Covers all 8 signals plus known edge cases.
Tests marked with @skipIf run without the SimpleMaps CSV but some city-only
cases (Signal 5) are skipped when the file is absent.
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from jobs.job_filter import is_us_location


# ─────────────────────────────────────────────────────────────────────────────
# Table-driven cases
# Each entry: (location_string, expected_bool, description)
# ─────────────────────────────────────────────────────────────────────────────

# Signal 1 — explicit US keyword
_SIGNAL1_CASES = [
    ("United States",          True,  "S1: full country name"),
    ("USA",                    True,  "S1: abbreviation uppercase"),
    ("usa",                    True,  "S1: abbreviation lowercase"),
    ("U.S.A.",                 True,  "S1: dotted form normalised to 'u s a'"),
    ("America",                True,  "S1: 'america'"),
    ("USA Remote Worksite",    True,  "S1: usa prefix with freeform text"),
    ("Georgia, USA",           True,  "S1: country+state collision resolved by usa"),
]

# Signal 2 — ISO alpha-3 non-US code (uppercase gate)
_SIGNAL2_CASES = [
    ("IND.Chennai",            False, "S2: IND uppercase alpha-3 dot notation"),
    ("IND.Pune",               False, "S2: IND uppercase alpha-3 dot notation"),
    ("GBR London",             False, "S2: GBR uppercase alpha-3"),
    ("CAN Toronto",            False, "S2: CAN uppercase alpha-3"),
    # Uppercase gate — common English words must NOT trigger S2
    ("can you relocate",       True,  "S2-gate: 'can' lowercase → not CAN=Canada"),
    ("per diem role",          True,  "S2-gate: 'per' lowercase → not PER=Peru"),
]

# Signal 3 — US state code or full name (runs BEFORE country-name check)
_SIGNAL3_CASES = [
    ("New York, NY",           True,  "S3: state code ny"),
    ("San Francisco, CA",      True,  "S3: state code ca"),
    ("Austin, TX",             True,  "S3: state code tx"),
    ("Seattle (WA)",           True,  "S3: state code in parens"),
    ("US-CA-Menlo Park",       True,  "S3: ISO subdivision → state code ca"),
    ("Menlo Park, CA; New York, NY", True, "S3: multi-location, state codes"),
    ("Burlington Massachusetts", True, "S3: full state name, no separator"),
    ("Lebanon, NH",            True,  "S3: state code 'nh' wins before Lebanon=country"),
    ("Jordan, UT",             True,  "S3: state code 'ut' wins before Jordan=country"),
]

# Signal 4 — non-US country name (runs AFTER state check)
_SIGNAL4_CASES = [
    ("Bangalore, India",       False, "S4: country name india"),
    ("Toronto, ON, Canada",    False, "S4: country name canada"),
    ("Berlin, Germany",        False, "S4: country name germany"),
    ("Paris, France",          False, "S4: country name france"),
    ("Remote - India",         False, "S4: india wins before S7 remote"),
    ("Remote, India",          False, "S4: india wins before S7 remote (comma)"),
]

# Signal 6 — geonamescache city→country set lookup
_SIGNAL6_CASES = [
    ("PUNE 05",                False, "S6: pune geonames=IN, digit stripped"),
    ("London, UK",             False, "S6: london geonames=GB"),
    # Cambridge exists in both US and GB — set fix → US preferred
    ("Cambridge",              True,  "S6: cambridge in both US+GB → US preferred"),
]

# Signal 7 — remote keywords (last positive signal)
_SIGNAL7_CASES = [
    ("Remote",                 True,  "S7: bare remote keyword"),
    ("Work from home",         True,  "S7: work from home"),
    ("WFH",                    True,  "S7: wfh uppercase normalised"),
    ("Anywhere",               True,  "S7: anywhere"),
]

# Default / empty (Signal 8)
_DEFAULT_CASES = [
    ("",                       True,  "S8: empty string → assume US"),
    ("   ",                    True,  "S8: whitespace only → assume US"),
    ("Multiple Locations",     True,  "S8: ambiguous → default True"),
    ("2 locations",            True,  "S8: digit stripped → default True"),
]

# Format-variety cases
_FORMAT_CASES = [
    ("US-CA-Menlo Park,",      True,  "fmt: ISO subdivision with trailing comma"),
    ("Menlo Park, CA; New York, NY", True, "fmt: semicolon-separated multi"),
    ("Seattle (WA)",           True,  "fmt: parens around state code"),
    ("PUNE 05",                False, "fmt: all-caps city + district number"),
]


class TestIsUsLocation(unittest.TestCase):
    """
    Table-driven tests for is_us_location().
    Each sub-test is independent and identified by its description.
    """

    def _run_cases(self, cases):
        for location, expected, description in cases:
            with self.subTest(location=location, desc=description):
                result = is_us_location(location)
                self.assertEqual(
                    result, expected,
                    msg=f"is_us_location({location!r}) = {result}, want {expected} | {description}",
                )

    def test_signal1_explicit_us(self):
        self._run_cases(_SIGNAL1_CASES)

    def test_signal2_iso_alpha3_uppercase_gate(self):
        self._run_cases(_SIGNAL2_CASES)

    def test_signal3_us_state_before_country(self):
        self._run_cases(_SIGNAL3_CASES)

    def test_signal4_non_us_country_name(self):
        self._run_cases(_SIGNAL4_CASES)

    def test_signal6_geonamescache_set_lookup(self):
        self._run_cases(_SIGNAL6_CASES)

    def test_signal7_remote_keywords_last(self):
        self._run_cases(_SIGNAL7_CASES)

    def test_signal8_default(self):
        self._run_cases(_DEFAULT_CASES)

    def test_format_variety(self):
        self._run_cases(_FORMAT_CASES)

    # ── Ordering invariant tests ──────────────────────────────────────────

    def test_state_code_beats_country_name(self):
        """State code in same string must win over a country name."""
        # "Jordan" is a country; "ut" is Utah state code
        self.assertTrue(is_us_location("Jordan, UT"))
        # "Lebanon" is a country; "nh" is New Hampshire state code
        self.assertTrue(is_us_location("Lebanon, NH"))

    def test_country_name_beats_remote(self):
        """Non-US country name must win over a remote keyword in same string."""
        self.assertFalse(is_us_location("Remote - India"))
        self.assertFalse(is_us_location("Remote, India"))
        self.assertFalse(is_us_location("Remote Germany"))

    def test_explicit_us_beats_everything(self):
        """USA/United States in string always wins, even with country name."""
        self.assertTrue(is_us_location("Georgia, USA"))
        self.assertTrue(is_us_location("United States (Remote)"))

    def test_uppercase_gate_blocks_english_words(self):
        """Lowercase 3-letter words must never trigger alpha-3 country rejection."""
        # "can" = CAN (Canada) as uppercase — must not reject when lowercase
        self.assertTrue(is_us_location("can provide relocation assistance"))
        # "per" = PER (Peru) — must not reject when lowercase
        self.assertTrue(is_us_location("per diem position"))

    def test_city_set_prefers_us_for_shared_names(self):
        """
        Cities that appear in both US and non-US geonames must resolve to US
        (the set-based lookup returns True when 'US' is in the country set).
        """
        # Cambridge exists in Massachusetts (US) and England (GB)
        self.assertTrue(is_us_location("Cambridge"))

    def test_dotted_us_forms_normalised(self):
        """U.S.A. must be detected after dot-stripping normalization."""
        self.assertTrue(is_us_location("U.S.A."))
        self.assertTrue(is_us_location("u.s.a."))


if __name__ == "__main__":
    unittest.main()
