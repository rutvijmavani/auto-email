"""
tests/test_validation.py — Comprehensive tests for recruiter validation pipeline

Covers:
  normalize()
    - Lowercases, strips punctuation and whitespace
    - Does NOT remove legal suffixes
    - Handles empty string and special characters

  is_suffix_variation()
    - Exact + legal suffix → True
    - Different company → False
    - Substring but not suffix → False
    - All legal suffixes recognized

  domain_matches_expected()
    - Exact root match → True
    - Contains but not root → False
    - Subdomain handling
    - Missing @ symbol handled gracefully
    - Empty/None inputs

  analyze_buffer()
    - Empty buffer → []
    - All same domain, matches reference → insert all
    - All same domain, conflicts reference → discard all
    - Mixed domains, some match reference → keep matching
    - Mixed domains, none match reference → discard all
    - All different domains, 1 matches → keep that 1
    - All different domains, none match → discard all
    - No reference domain → fallback behavior
    - existing_db_domain takes priority over expected_domain
    - Top-up scenario: buffer matches DB domain → insert
    - Top-up scenario: buffer conflicts DB domain → discard
    - Top-up scenario: partial match → keep matching only

  extract_expected_domain()
    - Ashby ATS URL → company slug
    - Greenhouse ATS URL → company slug
    - Lever ATS URL → company slug
    - Direct company domain → root
    - Careers subdomain → root
    - WWW subdomain → root
    - Empty/None URL → None
    - Invalid URL → None

  get_existing_domain_for_company()
    - No existing recruiters → None
    - Existing recruiter → domain root
    - Inactive recruiter → not counted

  DB integration
    - add_application stores expected_domain
    - mark_application_exhausted sets status + timestamp
    - reactivate_application restores active status
    - update_application_expected_domain updates correctly
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tests.conftest import cleanup_db

TEST_DB = "data/test_pipeline.db"

import db.db as db_module
import db.connection as db_connection

# Override DB_FILE at module level — before any test runs
db_connection.DB_FILE = TEST_DB

from careershift.scraper import (
    normalize,
    is_suffix_variation,
    domain_matches_expected,
    analyze_buffer,
)
from pipeline import extract_expected_domain


# ─────────────────────────────────────────
# TEST: normalize()
# ─────────────────────────────────────────

class TestNormalize(unittest.TestCase):

    def test_lowercases(self):
        self.assertEqual(normalize("Collective"), "collective")

    def test_strips_leading_trailing_whitespace(self):
        self.assertEqual(normalize("  Collective  "), "collective")

    def test_removes_punctuation(self):
        self.assertEqual(normalize("Collective, Inc."), "collective inc")

    def test_removes_hyphen(self):
        self.assertEqual(normalize("Collective-LA"), "collectivela")

    def test_collapses_extra_whitespace(self):
        self.assertEqual(normalize("Collective  Inc"), "collective inc")

    def test_does_not_remove_legal_suffixes(self):
        # normalize preserves suffix — is_suffix_variation handles suffix logic
        self.assertEqual(normalize("Collective Inc"), "collective inc")
        self.assertEqual(normalize("Collective LLC"), "collective llc")
        self.assertEqual(normalize("Collective Ltd"), "collective ltd")

    def test_empty_string(self):
        self.assertEqual(normalize(""), "")

    def test_already_normalized(self):
        self.assertEqual(normalize("collective"), "collective")

    def test_special_company_names(self):
        self.assertEqual(normalize("Stripe, Inc."), "stripe inc")
        self.assertEqual(normalize("The Collective"), "the collective")


# ─────────────────────────────────────────
# TEST: is_suffix_variation()
# ─────────────────────────────────────────

class TestIsSuffixVariation(unittest.TestCase):

    def test_inc_suffix(self):
        self.assertTrue(is_suffix_variation("collective inc", "collective"))

    def test_llc_suffix(self):
        self.assertTrue(is_suffix_variation("collective llc", "collective"))

    def test_ltd_suffix(self):
        self.assertTrue(is_suffix_variation("collective ltd", "collective"))

    def test_corp_suffix(self):
        self.assertTrue(is_suffix_variation("collective corp", "collective"))

    def test_co_suffix(self):
        self.assertTrue(is_suffix_variation("collective co", "collective"))

    def test_incorporated_suffix(self):
        self.assertTrue(is_suffix_variation("collective incorporated", "collective"))

    def test_corporation_suffix(self):
        self.assertTrue(is_suffix_variation("collective corporation", "collective"))

    def test_limited_suffix(self):
        self.assertTrue(is_suffix_variation("collective limited", "collective"))

    def test_lp_suffix(self):
        self.assertTrue(is_suffix_variation("collective lp", "collective"))

    def test_gmbh_suffix(self):
        self.assertTrue(is_suffix_variation("collective gmbh", "collective"))

    def test_pte_suffix(self):
        self.assertTrue(is_suffix_variation("collective pte", "collective"))

    def test_plc_suffix(self):
        self.assertTrue(is_suffix_variation("collective plc", "collective"))

    def test_different_company_false(self):
        self.assertFalse(is_suffix_variation("collective junction", "collective"))

    def test_different_company_health_false(self):
        self.assertFalse(is_suffix_variation("collective health", "collective"))

    def test_contains_but_not_prefix_false(self):
        self.assertFalse(is_suffix_variation("ilovecollective", "collective"))

    def test_exact_match_no_suffix_false(self):
        # Exact match is not a suffix variation
        self.assertFalse(is_suffix_variation("collective", "collective"))

    def test_unrelated_company_false(self):
        self.assertFalse(is_suffix_variation("stripe inc", "collective"))

    def test_non_legal_word_false(self):
        self.assertFalse(is_suffix_variation("collective agency", "collective"))

    def test_empty_remainder_false(self):
        # Only spaces after expected → not a valid suffix
        self.assertFalse(is_suffix_variation("collective ", "collective"))


# ─────────────────────────────────────────
# TEST: domain_matches_expected()
# ─────────────────────────────────────────

class TestDomainMatchesExpected(unittest.TestCase):

    def test_exact_root_match(self):
        self.assertTrue(domain_matches_expected("john@collective.com", "collective"))

    def test_exact_root_match_different_tld(self):
        self.assertTrue(domain_matches_expected("john@collective.io", "collective"))

    def test_contains_but_not_root_false(self):
        self.assertFalse(domain_matches_expected("jane@ilovecollective.com", "collective"))

    def test_hyphenated_domain_false(self):
        self.assertFalse(domain_matches_expected("jane@collective-la.com", "collective"))

    def test_prefixed_domain_false(self):
        self.assertFalse(domain_matches_expected("bob@thecollective.com", "collective"))

    def test_suffixed_domain_false(self):
        self.assertFalse(domain_matches_expected("alice@collectiveinc.com", "collective"))

    def test_completely_unrelated_false(self):
        self.assertFalse(domain_matches_expected("dave@gmail.com", "collective"))

    def test_empty_email_false(self):
        self.assertFalse(domain_matches_expected("", "collective"))

    def test_none_email_false(self):
        self.assertFalse(domain_matches_expected(None, "collective"))

    def test_no_at_symbol_false(self):
        self.assertFalse(domain_matches_expected("notanemail", "collective"))

    def test_stripe_match(self):
        self.assertTrue(domain_matches_expected("hr@stripe.com", "stripe"))

    def test_stripe_mismatch(self):
        self.assertFalse(domain_matches_expected("hr@stripehq.com", "stripe"))


# ─────────────────────────────────────────
# TEST: analyze_buffer()
# ─────────────────────────────────────────

class TestAnalyzeBuffer(unittest.TestCase):

    def _record(self, email, name="John"):
        return {
            "name":       name,
            "position":   "Recruiter",
            "email":      email,
            "company":    "Collective",
            "confidence": "auto",
        }

    # ── Empty buffer ──

    def test_empty_buffer_returns_empty(self):
        result = analyze_buffer([], "collective")
        self.assertEqual(result, [])

    # ── All same domain, matches reference ──

    def test_all_same_domain_matches_expected(self):
        buffer = [
            self._record("john@collective.com"),
            self._record("jane@collective.com", "Jane"),
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(len(result), 2)

    def test_three_records_same_domain_matches(self):
        buffer = [
            self._record("john@collective.com"),
            self._record("jane@collective.com", "Jane"),
            self._record("bob@collective.com", "Bob"),
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(len(result), 3)

    # ── All same domain, conflicts reference ──

    def test_all_same_domain_conflicts_expected(self):
        buffer = [
            self._record("john@collectiveagency.com"),
            self._record("jane@collectiveagency.com", "Jane"),
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(result, [])

    def test_all_same_domain_conflicts_db_domain(self):
        buffer = [
            self._record("john@collectiveagency.com"),
            self._record("jane@collectiveagency.com", "Jane"),
        ]
        # DB has collective.com — buffer has collectiveagency.com
        result = analyze_buffer(buffer, "collective", existing_db_domain="collective")
        self.assertEqual(result, [])

    # ── Mixed domains ──

    def test_mixed_domains_one_matches_reference(self):
        buffer = [
            self._record("john@collective.com"),       # matches
            self._record("jane@collective-la.com", "Jane"),  # doesn't match
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "john@collective.com")

    def test_mixed_domains_multiple_match_reference(self):
        buffer = [
            self._record("john@collective.com"),
            self._record("jane@collective.com", "Jane"),
            self._record("bob@collectiveagency.com", "Bob"),
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(len(result), 2)
        emails = [r["email"] for r in result]
        self.assertIn("john@collective.com", emails)
        self.assertIn("jane@collective.com", emails)
        self.assertNotIn("bob@collectiveagency.com", emails)

    def test_mixed_domains_none_match_reference(self):
        buffer = [
            self._record("john@collectiveagency.com"),
            self._record("jane@ilovecollective.com", "Jane"),
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(result, [])

    def test_all_different_domains_one_matches(self):
        buffer = [
            self._record("john@collective.com"),
            self._record("jane@collective-la.com", "Jane"),
            self._record("bob@ilovecollective.com", "Bob"),
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "john@collective.com")

    def test_all_different_domains_none_match(self):
        buffer = [
            self._record("john@collective-la.com"),
            self._record("jane@ilovecollective.com", "Jane"),
            self._record("bob@collectiveagency.com", "Bob"),
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(result, [])

    # ── DB domain takes priority ──

    def test_db_domain_takes_priority_over_expected(self):
        # DB has "collectivela", expected is "collective"
        # Buffer has collectivela.com → should match DB domain
        buffer = [
            self._record("john@collectivela.com"),
            self._record("jane@collective.com", "Jane"),
        ]
        result = analyze_buffer(buffer, "collective",
                                existing_db_domain="collectivela")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "john@collectivela.com")

    def test_no_reference_domain_empty_string(self):
        # No reference at all — all same domain → insert all
        buffer = [
            self._record("john@collective.com"),
            self._record("jane@collective.com", "Jane"),
        ]
        result = analyze_buffer(buffer, "")
        # All same domain — consistent signal regardless of reference
        # buffer_root = "collective", reference = "" → not equal → discard
        self.assertEqual(result, [])

    # ── Top-up scenario ──

    def test_topup_buffer_matches_db_domain(self):
        # DB already has collective.com, new buffer also collective.com
        buffer = [self._record("jane@collective.com", "Jane")]
        result = analyze_buffer(buffer, "collective",
                                existing_db_domain="collective")
        self.assertEqual(len(result), 1)

    def test_topup_buffer_all_conflict_db_domain(self):
        # DB has collective.com, buffer has collectiveagency.com
        buffer = [
            self._record("jane@collectiveagency.com", "Jane"),
            self._record("bob@collectiveagency.com", "Bob"),
        ]
        result = analyze_buffer(buffer, "collective",
                                existing_db_domain="collective")
        self.assertEqual(result, [])

    def test_topup_partial_match_db_domain(self):
        # DB has collective.com, buffer is mixed
        buffer = [
            self._record("jane@collective.com", "Jane"),      # matches DB
            self._record("bob@collectiveagency.com", "Bob"),  # doesn't match
        ]
        result = analyze_buffer(buffer, "collective",
                                existing_db_domain="collective")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "jane@collective.com")

    def test_quota_exhausted_scenario_two_different_domains(self):
        # Quota exhausted after 2 profiles, both different domains
        # expected_domain is tiebreaker
        buffer = [
            self._record("john@collective.com"),            # matches expected
            self._record("jane@collective-la.com", "Jane"), # doesn't match
        ]
        result = analyze_buffer(buffer, "collective")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "john@collective.com")


# ─────────────────────────────────────────
# TEST: extract_expected_domain()
# ─────────────────────────────────────────

class TestExtractExpectedDomain(unittest.TestCase):

    def test_ashby_ats_url(self):
        url = "https://jobs.ashbyhq.com/collective/54259edc"
        self.assertEqual(extract_expected_domain(url), "collective")

    def test_greenhouse_boards_url(self):
        url = "https://boards.greenhouse.io/collectiveinc/jobs/123"
        self.assertEqual(extract_expected_domain(url), "collectiveinc")

    def test_greenhouse_job_boards_url(self):
        url = "https://job-boards.greenhouse.io/stripe/jobs/456"
        self.assertEqual(extract_expected_domain(url), "stripe")

    def test_lever_url(self):
        url = "https://jobs.lever.co/stripe/abc123"
        self.assertEqual(extract_expected_domain(url), "stripe")

    def test_direct_company_domain(self):
        url = "https://collective.com/careers/engineer"
        self.assertEqual(extract_expected_domain(url), "collective")

    def test_careers_subdomain(self):
        url = "https://careers.stripe.com/jobs/123"
        self.assertEqual(extract_expected_domain(url), "stripe")

    def test_www_subdomain(self):
        url = "https://www.collective.com/careers"
        self.assertEqual(extract_expected_domain(url), "collective")

    def test_jobs_subdomain(self):
        url = "https://jobs.stripe.com/positions/123"
        self.assertEqual(extract_expected_domain(url), "stripe")

    def test_empty_url(self):
        self.assertIsNone(extract_expected_domain(""))

    def test_none_url(self):
        self.assertIsNone(extract_expected_domain(None))

    def test_invalid_url(self):
        self.assertIsNone(extract_expected_domain("not-a-url"))

    def test_ashby_with_company_slug_only(self):
        url = "https://jobs.ashbyhq.com/linear"
        self.assertEqual(extract_expected_domain(url), "linear")

    def test_greenhouse_eu_url(self):
        url = "https://boards.eu.greenhouse.io/acmecorp/jobs/789"
        self.assertEqual(extract_expected_domain(url), "acmecorp")


# ─────────────────────────────────────────
# TEST: DB Integration
# ─────────────────────────────────────────

class TestDBIntegration(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def test_add_application_stores_expected_domain(self):
        app_id, created = db_module.add_application(
            company="Collective",
            job_url="https://jobs.ashbyhq.com/collective/123",
            job_title="SWE",
            expected_domain="collective",
        )
        self.assertTrue(created)
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT expected_domain FROM applications WHERE id = ?", (app_id,))
        row = c.fetchone()
        conn.close()
        self.assertEqual(row["expected_domain"], "collective")

    def test_add_application_without_expected_domain(self):
        app_id, created = db_module.add_application(
            company="Stripe",
            job_url="https://stripe.com/jobs/123",
        )
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT expected_domain FROM applications WHERE id = ?", (app_id,))
        row = c.fetchone()
        conn.close()
        self.assertIsNone(row["expected_domain"])

    def test_mark_application_exhausted(self):
        app_id, _ = db_module.add_application(
            "Collective", "https://collective.com/jobs/1", "SWE"
        )
        db_module.mark_application_exhausted(app_id)
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status, exhausted_at FROM applications WHERE id = ?",
                  (app_id,))
        row = c.fetchone()
        conn.close()
        self.assertEqual(row["status"], "exhausted")
        self.assertIsNotNone(row["exhausted_at"])

    def test_exhausted_application_excluded_from_active(self):
        app_id, _ = db_module.add_application(
            "Collective", "https://collective.com/jobs/1", "SWE"
        )
        db_module.mark_application_exhausted(app_id)
        apps = db_module.get_all_active_applications()
        ids = [a["id"] for a in apps]
        self.assertNotIn(app_id, ids)

    def test_reactivate_application(self):
        app_id, _ = db_module.add_application(
            "Collective", "https://collective.com/jobs/1", "SWE"
        )
        db_module.mark_application_exhausted(app_id)
        db_module.reactivate_application("Collective")
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status, exhausted_at FROM applications WHERE id = ?",
                  (app_id,))
        row = c.fetchone()
        conn.close()
        self.assertEqual(row["status"], "active")
        self.assertIsNone(row["exhausted_at"])

    def test_update_application_expected_domain(self):
        app_id, _ = db_module.add_application(
            "Collective", "https://collective.com/jobs/1", "SWE"
        )
        db_module.update_application_expected_domain(app_id, "collective")
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT expected_domain FROM applications WHERE id = ?", (app_id,))
        row = c.fetchone()
        conn.close()
        self.assertEqual(row["expected_domain"], "collective")

    def test_get_existing_domain_for_company_no_recruiters(self):
        db_module.add_application(
            "Collective", "https://collective.com/jobs/1", "SWE"
        )
        result = db_module.get_existing_domain_for_company("Collective")
        self.assertIsNone(result)

    def test_get_existing_domain_for_company_with_recruiter(self):
        app_id, _ = db_module.add_application(
            "Collective", "https://collective.com/jobs/1", "SWE"
        )
        rid = db_module.add_recruiter(
            "Collective", "John", "Recruiter", "john@collective.com", "auto"
        )
        db_module.link_recruiter_to_application(app_id, rid)
        result = db_module.get_existing_domain_for_company("Collective")
        self.assertEqual(result, "collective")

    def test_get_existing_domain_for_company_inactive_not_counted(self):
        app_id, _ = db_module.add_application(
            "Collective", "https://collective.com/jobs/1", "SWE"
        )
        rid = db_module.add_recruiter(
            "Collective", "John", "Recruiter", "john@collective.com", "auto"
        )
        db_module.link_recruiter_to_application(app_id, rid)
        db_module.mark_recruiter_inactive(rid, "test")
        result = db_module.get_existing_domain_for_company("Collective")
        self.assertIsNone(result)

    def test_get_existing_domain_extracts_root_correctly(self):
        app_id, _ = db_module.add_application(
            "Stripe", "https://stripe.com/jobs/1", "SWE"
        )
        rid = db_module.add_recruiter(
            "Stripe", "Jane", "Recruiter", "jane@stripe.com", "auto"
        )
        db_module.link_recruiter_to_application(app_id, rid)
        result = db_module.get_existing_domain_for_company("Stripe")
        self.assertEqual(result, "stripe")  # root only, not "stripe.com"


# ─────────────────────────────────────────
# TEST: scrape_company() — mocked
# ─────────────────────────────────────────

class TestScrapeCompanyValidation(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _make_mock_page(self, cards_per_term, emails_per_profile):
        """
        Build a mock page that returns given cards and emails.
        cards_per_term: list of card lists per HR term call
        emails_per_profile: list of emails returned per profile visit
        """
        from careershift.scraper import scrape_company
        page = MagicMock()
        page.url = "https://www.careershift.com/App/Contacts/Search"
        page.goto.return_value = None
        page.wait_for_selector.return_value = None
        page.content.return_value = "<html></html>"
        return page

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_no_exact_matches_returns_empty(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """hashmap[expected] == 0 after all terms → exhaust"""
        from careershift.scraper import scrape_company

        # All cards are different companies
        mock_cards.return_value = [
            ("John", "Collective Junction", "Recruiter",
             "https://cs.com/1", True),
            ("Jane", "Collective Health", "HR Manager",
             "https://cs.com/2", True),
        ]

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        self.assertEqual(result, [])

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_exact_matches_all_same_domain_inserted(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """All profiles same domain matching expected → insert all"""
        from careershift.scraper import scrape_company

        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
            ("Jane", "Collective", "HR Manager", "https://cs.com/2", True),
        ]
        mock_visit.side_effect = [
            {"name": "John", "position": "Recruiter",
             "email": "john@collective.com", "confidence": "auto"},
            {"name": "Jane", "position": "HR Manager",
             "email": "jane@collective.com", "confidence": "auto"},
        ]

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        self.assertEqual(len(result), 2)
        emails = [r["email"] for r in result]
        self.assertIn("john@collective.com", emails)
        self.assertIn("jane@collective.com", emails)

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_mixed_domains_tiebreaker_applied(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Mixed domains → expected_domain as tiebreaker → keep matching"""
        from careershift.scraper import scrape_company

        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
            ("Jane", "Collective", "HR", "https://cs.com/2", True),
        ]
        mock_visit.side_effect = [
            {"name": "John", "position": "Recruiter",
             "email": "john@collective.com", "confidence": "auto"},
            {"name": "Jane", "position": "HR",
             "email": "jane@collective-la.com", "confidence": "auto"},
        ]

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "john@collective.com")

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=0)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_quota_exhausted_stops_profile_visits(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Quota = 0 before profile visits → no profiles visited"""
        from careershift.scraper import scrape_company

        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
        ]

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        mock_visit.assert_not_called()

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_visit_limit_respects_max_contacts(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """max_contacts=1 → only 1 profile visited regardless of available profiles"""
        from careershift.scraper import scrape_company

        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
            ("Jane", "Collective", "HR", "https://cs.com/2", True),
            ("Bob", "Collective", "TA", "https://cs.com/3", True),
        ]
        mock_visit.return_value = {
            "name": "John", "position": "Recruiter",
            "email": "john@collective.com", "confidence": "auto"
        }

        page = MagicMock()
        result = scrape_company(page, "Collective", 1, "collective")
        # Only 1 profile should be visited
        self.assertEqual(mock_visit.call_count, 1)

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_single_visit_high_confidence_domain_matches(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Single visit, high hashmap confidence, domain matches → insert"""
        from careershift.scraper import scrape_company

        # 9 out of 10 cards are exact match → 90% confidence
        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", f"https://cs.com/{i}", True)
            for i in range(9)
        ] + [("Jane", "Collective Junction", "HR", "https://cs.com/9", True)]

        mock_visit.return_value = {
            "name": "John", "position": "Recruiter",
            "email": "john@collective.com", "confidence": "auto"
        }

        page = MagicMock()
        result = scrape_company(page, "Collective", 1, "collective")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "john@collective.com")

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_single_visit_high_confidence_domain_mismatch(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Single visit, high confidence, domain mismatch → exhaust (return [])"""
        from careershift.scraper import scrape_company

        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", f"https://cs.com/{i}", True)
            for i in range(9)
        ] + [("Jane", "Collective Junction", "HR", "https://cs.com/9", True)]

        mock_visit.return_value = {
            "name": "John", "position": "Recruiter",
            "email": "john@ilovecollective.com", "confidence": "auto"
        }

        page = MagicMock()
        result = scrape_company(page, "Collective", 1, "collective")
        self.assertEqual(result, [])

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_single_visit_low_confidence_returns_none(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Single visit, low hashmap confidence → skip (return None)"""
        from careershift.scraper import scrape_company

        # 3/10 exact "Collective" + 7/10 suffix "Collective Inc" per term
        # hashmap_confidence = total_exact / total_cards_seen
        # = (3*5) / (10*5) = 15/50 = 30% < 70% threshold → return None
        # Note: "Collective Junction" would be IGNORED (not suffix variation)
        # Must use suffix variations to populate hashmap and affect confidence
        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", f"https://cs.com/{i}", True)
            for i in range(3)
        ] + [
            ("Jane", "Collective Inc", "HR", f"https://cs.com/inc{i}", True)
            for i in range(7)
        ]

        mock_visit.return_value = {
            "name": "John", "position": "Recruiter",
            "email": "john@collective.com", "confidence": "auto"
        }

        page = MagicMock()
        result = scrape_company(page, "Collective", 1, "collective")
        self.assertIsNone(result)  # None = skip, not exhaust

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company",
           return_value="collective")
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_topup_buffer_conflicts_db_domain_discarded(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Top-up: buffer consistent but conflicts DB domain → discard"""
        from careershift.scraper import scrape_company

        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
            ("Jane", "Collective", "HR", "https://cs.com/2", True),
        ]
        mock_visit.side_effect = [
            {"name": "John", "position": "Recruiter",
             "email": "john@collectiveagency.com", "confidence": "auto"},
            {"name": "Jane", "position": "HR",
             "email": "jane@collectiveagency.com", "confidence": "auto"},
        ]

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        # DB has "collective", buffer has "collectiveagency" → discard
        self.assertEqual(result, [])

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company",
           return_value="collective")
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_topup_partial_match_keeps_matching_only(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Top-up: mixed buffer → keep only records matching DB domain"""
        from careershift.scraper import scrape_company

        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
            ("Jane", "Collective", "HR", "https://cs.com/2", True),
        ]
        mock_visit.side_effect = [
            {"name": "John", "position": "Recruiter",
             "email": "john@collective.com", "confidence": "auto"},  # matches DB
            {"name": "Jane", "position": "HR",
             "email": "jane@collectiveagency.com", "confidence": "auto"},  # doesn't
        ]

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["email"], "john@collective.com")

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_fewer_than_sample_size_cards_handled(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """CareerShift returns 3 cards instead of 10 — confidence calculated correctly"""
        from careershift.scraper import scrape_company

        # Only 3 cards returned, all correct → 3/3 = 100% confidence
        # Positions must pass classify_title (HR and TA return None)
        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
            ("Jane", "Collective", "HR Manager", "https://cs.com/2", True),
            ("Bob", "Collective", "Talent Acquisition", "https://cs.com/3", True),
        ]
        mock_visit.side_effect = [
            {"name": "John", "position": "Recruiter",
             "email": "john@collective.com", "confidence": "auto"},
            {"name": "Jane", "position": "HR Manager",
             "email": "jane@collective.com", "confidence": "auto"},
            {"name": "Bob", "position": "Talent Acquisition",
             "email": "bob@collective.com", "confidence": "auto"},
        ]

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        self.assertEqual(len(result), 3)

    @patch("careershift.scraper.submit_search", return_value=True)
    @patch("careershift.scraper.parse_cards_from_html")
    @patch("careershift.scraper.visit_and_extract")
    @patch("careershift.scraper.get_remaining_quota", return_value=10)
    @patch("careershift.scraper.get_existing_domain_for_company", return_value=None)
    @patch("careershift.scraper.mark_search_term_used")
    @patch("careershift.scraper.update_company_last_scraped")
    @patch("careershift.scraper.human_delay")
    def test_suffix_variations_tracked_not_visited(
            self, mock_delay, mock_update, mock_mark, mock_db_domain,
            mock_quota, mock_visit, mock_cards, mock_search):
        """Suffix variations (Collective Inc) tracked in hashmap but not visited"""
        from careershift.scraper import scrape_company

        # 1 exact "Collective" + 2 suffix variations per term
        # confidence = 1/3 = 33% per term → below 90% → all 5 terms run
        # But deduplication by detail_url → John only added once
        # Jane (Collective Inc) and Bob (Collective LLC) → suffix → hashmap only
        mock_cards.return_value = [
            ("John", "Collective", "Recruiter", "https://cs.com/1", True),
            ("Jane", "Collective Inc", "HR Manager", "https://cs.com/2", True),
            ("Bob", "Collective LLC", "Talent Acquisition", "https://cs.com/3", True),
        ]
        mock_visit.return_value = {
            "name": "John", "position": "Recruiter",
            "email": "john@collective.com", "confidence": "auto"
        }

        page = MagicMock()
        result = scrape_company(page, "Collective", 3, "collective")
        # Only John (exact match) visited — Jane and Bob are suffix variations
        # URL deduplication prevents John from being visited multiple times
        self.assertEqual(mock_visit.call_count, 1)




# ─────────────────────────────────────────
# TEST: Under-stocked detection after verification
# ─────────────────────────────────────────

class TestUnderStockedDetection(unittest.TestCase):
    """
    Tests that companies correctly appear as under-stocked
    after recruiters are marked inactive.
    """

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _make_app(self, company, url):
        app_id, _ = db_module.add_application(company, url, "SWE")
        return app_id

    def _make_recruiter(self, company, email):
        return db_module.add_recruiter(company, "John", "Recruiter", email, "auto")

    def test_fully_stocked_company_not_under_stocked(self):
        """Company with MAX_CONTACTS recruiters not flagged."""
        app_id = self._make_app("Collective", "https://c.com/1")
        for i in range(3):
            rid = self._make_recruiter("Collective", f"r{i}@collective.com")
            db_module.link_recruiter_to_application(app_id, rid)
        # MIN = 1, so 3 recruiters = fully stocked
        under = db_module.get_unique_companies_needing_scraping(1)
        self.assertNotIn("Collective", under)

    def test_company_with_one_recruiter_not_under_stocked(self):
        """Company with 1 recruiter (MIN=1) is NOT under-stocked."""
        app_id = self._make_app("Collective", "https://c.com/1")
        rid = self._make_recruiter("Collective", "r1@collective.com")
        db_module.link_recruiter_to_application(app_id, rid)

        # MIN = 1 → 1 recruiter = satisfied
        under = db_module.get_unique_companies_needing_scraping(1)
        self.assertNotIn("Collective", under)

    def test_company_under_stocked_after_all_recruiters_inactive(self):
        """Company with 0 active recruiters flagged as under-stocked."""
        app_id = self._make_app("Stripe", "https://s.com/1")
        rid1 = self._make_recruiter("Stripe", "r1@stripe.com")
        rid2 = self._make_recruiter("Stripe", "r2@stripe.com")
        db_module.link_recruiter_to_application(app_id, rid1)
        db_module.link_recruiter_to_application(app_id, rid2)

        # Mark all inactive → 0 active → under-stocked
        db_module.mark_recruiter_inactive(rid1, "left")
        db_module.mark_recruiter_inactive(rid2, "left")

        under = db_module.get_unique_companies_needing_scraping(1)
        self.assertIn("Stripe", under)

    def test_company_under_stocked_after_last_recruiter_inactive(self):
        """Company drops to 0 after last recruiter goes inactive → flagged."""
        app_id = self._make_app("Collective", "https://c.com/1")
        rid = self._make_recruiter("Collective", "r1@collective.com")
        db_module.link_recruiter_to_application(app_id, rid)

        # Initially OK (1 = MIN)
        under = db_module.get_unique_companies_needing_scraping(1)
        self.assertNotIn("Collective", under)

        # Last recruiter leaves → 0 active → under-stocked
        db_module.mark_recruiter_inactive(rid, "left company")
        under = db_module.get_unique_companies_needing_scraping(1)
        self.assertIn("Collective", under)

    def test_multiple_companies_some_under_stocked(self):
        """Only under-stocked companies flagged, not fully stocked ones."""
        # Collective — has 1 recruiter (MIN=1 satisfied)
        app1 = self._make_app("Collective", "https://c.com/1")
        rid_c = self._make_recruiter("Collective", "c1@collective.com")
        db_module.link_recruiter_to_application(app1, rid_c)

        # Stripe — 0 active (under-stocked)
        app2 = self._make_app("Stripe", "https://s.com/1")
        rid_s = self._make_recruiter("Stripe", "s1@stripe.com")
        db_module.link_recruiter_to_application(app2, rid_s)
        db_module.mark_recruiter_inactive(rid_s, "left")

        under = db_module.get_unique_companies_needing_scraping(1)
        self.assertNotIn("Collective", under)
        self.assertIn("Stripe", under)

    def test_exhausted_application_not_included(self):
        """Exhausted applications not included in under-stocked check."""
        app_id = self._make_app("Linear", "https://l.com/1")
        db_module.mark_application_exhausted(app_id)

        under = db_module.get_unique_companies_needing_scraping(2)
        self.assertNotIn("Linear", under)
if __name__ == "__main__":
    unittest.main(verbosity=2)