"""
tests/test_prospective.py — Comprehensive tests for prospective companies feature

Covers:

  TestProspectiveDB
    - add_prospective_company inserts correctly
    - Duplicate add silently ignored (INSERT OR IGNORE)
    - get_pending_prospective returns only pending
    - get_pending_prospective ordered by priority DESC then created_at ASC
    - get_prospective_companies returns all statuses
    - get_prospective_companies filtered by status
    - mark_prospective_scraped updates status + scraped_at
    - mark_prospective_exhausted updates status
    - mark_prospective_converted updates status + converted_at
    - is_prospective returns True only for scraped status
    - is_prospective returns False for pending/exhausted/converted
    - get_prospective_status_summary counts correctly
    - get_prospective_company returns single record or None

  TestConvertProspectiveToActive
    - Converts placeholder → real URL + status active
    - Updates job_title if provided
    - Updates expected_domain if provided
    - Returns app_id on success
    - Returns None if no prospective found for company
    - Recruiters still linked after conversion
    - Does not affect other companies

  TestImportProspects
    - Valid file imports all companies
    - Blank lines and # comments skipped
    - Duplicate companies not re-added
    - Missing file prints error
    - Returns correct added/skipped counts
    - Whitespace around company names stripped

  TestProspectsCLIFlags
    - --import-prospects calls run_import_prospects
    - --prospects-status calls run_prospects_status
    - --import-prospects with custom filepath passed correctly
    - Other flags don't trigger prospective functions

  TestAddJobProspectiveDetection
    - Company in prospective (scraped) → converts, skips normal add
    - Company in prospective (pending) → normal add flow (not ready)
    - Company not in prospective → normal add flow
    - Conversion marks prospective as converted
    - After conversion outreach can be scheduled
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from io import StringIO
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tests.conftest import cleanup_db

TEST_DB = "data/test_pipeline.db"

import db.db as db_module
import db.connection as db_connection

# Override DB_FILE at module level — before any test runs
db_connection.DB_FILE = TEST_DB


# ─────────────────────────────────────────
# TEST: Prospective DB operations
# ─────────────────────────────────────────

class TestProspectiveDB(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def test_add_prospective_company_inserts(self):
        """New company inserted with status pending."""
        result = db_module.add_prospective_company("Google")
        self.assertTrue(result)
        company = db_module.get_prospective_company("Google")
        self.assertIsNotNone(company)
        self.assertEqual(company["status"], "pending")
        self.assertEqual(company["company"], "Google")

    def test_add_prospective_company_default_priority_zero(self):
        """Default priority is 0."""
        db_module.add_prospective_company("Google")
        company = db_module.get_prospective_company("Google")
        self.assertEqual(company["priority"], 0)

    def test_add_prospective_company_custom_priority(self):
        """Custom priority stored correctly."""
        db_module.add_prospective_company("Google", priority=5)
        company = db_module.get_prospective_company("Google")
        self.assertEqual(company["priority"], 5)

    def test_add_prospective_duplicate_silently_ignored(self):
        """Duplicate company returns False, not an error."""
        db_module.add_prospective_company("Google")
        result = db_module.add_prospective_company("Google")
        self.assertFalse(result)
        # Still only one row
        all_companies = db_module.get_prospective_companies()
        google_count = sum(1 for c in all_companies if c["company"] == "Google")
        self.assertEqual(google_count, 1)

    def test_add_prospective_strips_whitespace(self):
        """Company name whitespace stripped before insert."""
        db_module.add_prospective_company("  Google  ")
        company = db_module.get_prospective_company("Google")
        self.assertIsNotNone(company)
        self.assertEqual(company["company"], "Google")

    def test_get_pending_prospective_returns_only_pending(self):
        """Only pending companies returned."""
        db_module.add_prospective_company("Google")
        db_module.add_prospective_company("Stripe")
        db_module.mark_prospective_scraped("Stripe")

        pending = db_module.get_pending_prospective()
        companies = [p["company"] for p in pending]
        self.assertIn("Google", companies)
        self.assertNotIn("Stripe", companies)

    def test_get_pending_prospective_ordered_by_priority(self):
        """Higher priority companies returned first."""
        db_module.add_prospective_company("Google", priority=1)
        db_module.add_prospective_company("Stripe", priority=5)
        db_module.add_prospective_company("Netflix", priority=3)

        pending = db_module.get_pending_prospective()
        companies = [p["company"] for p in pending]
        self.assertEqual(companies[0], "Stripe")   # priority 5
        self.assertEqual(companies[1], "Netflix")  # priority 3
        self.assertEqual(companies[2], "Google")   # priority 1

    def test_get_pending_prospective_with_limit(self):
        """Limit parameter respected."""
        for i in range(5):
            db_module.add_prospective_company(f"Company{i}")
        pending = db_module.get_pending_prospective(limit=3)
        self.assertEqual(len(pending), 3)

    def test_get_prospective_companies_returns_all(self):
        """get_prospective_companies returns all statuses."""
        db_module.add_prospective_company("Google")
        db_module.add_prospective_company("Stripe")
        db_module.mark_prospective_scraped("Stripe")

        all_companies = db_module.get_prospective_companies()
        self.assertEqual(len(all_companies), 2)

    def test_get_prospective_companies_filtered_by_status(self):
        """Filtered by status returns only matching."""
        db_module.add_prospective_company("Google")
        db_module.add_prospective_company("Stripe")
        db_module.mark_prospective_scraped("Stripe")

        scraped = db_module.get_prospective_companies(status="scraped")
        self.assertEqual(len(scraped), 1)
        self.assertEqual(scraped[0]["company"], "Stripe")

    def test_mark_prospective_scraped(self):
        """Status changes to scraped with scraped_at timestamp."""
        db_module.add_prospective_company("Google")
        db_module.mark_prospective_scraped("Google")

        company = db_module.get_prospective_company("Google")
        self.assertEqual(company["status"], "scraped")
        self.assertIsNotNone(company["scraped_at"])

    def test_mark_prospective_exhausted(self):
        """Status changes to exhausted."""
        db_module.add_prospective_company("Google")
        db_module.mark_prospective_exhausted("Google")

        company = db_module.get_prospective_company("Google")
        self.assertEqual(company["status"], "exhausted")

    def test_mark_prospective_converted(self):
        """Status changes to converted with converted_at timestamp."""
        db_module.add_prospective_company("Google")
        db_module.mark_prospective_scraped("Google")
        db_module.mark_prospective_converted("Google")

        company = db_module.get_prospective_company("Google")
        self.assertEqual(company["status"], "converted")
        self.assertIsNotNone(company["converted_at"])

    def test_is_prospective_true_for_scraped(self):
        """is_prospective returns True only when status is scraped."""
        db_module.add_prospective_company("Google")
        db_module.mark_prospective_scraped("Google")
        self.assertTrue(db_module.is_prospective("Google"))

    def test_is_prospective_false_for_pending(self):
        """is_prospective returns False for pending (not yet scraped)."""
        db_module.add_prospective_company("Google")
        self.assertFalse(db_module.is_prospective("Google"))

    def test_is_prospective_false_for_exhausted(self):
        """is_prospective returns False for exhausted."""
        db_module.add_prospective_company("Google")
        db_module.mark_prospective_exhausted("Google")
        self.assertFalse(db_module.is_prospective("Google"))

    def test_is_prospective_false_for_converted(self):
        """is_prospective returns False after conversion."""
        db_module.add_prospective_company("Google")
        db_module.mark_prospective_scraped("Google")
        db_module.mark_prospective_converted("Google")
        self.assertFalse(db_module.is_prospective("Google"))

    def test_is_prospective_false_for_unknown_company(self):
        """is_prospective returns False for company not in list."""
        self.assertFalse(db_module.is_prospective("UnknownCompany"))

    def test_get_prospective_status_summary(self):
        """Summary counts match actual data."""
        db_module.add_prospective_company("Google")
        db_module.add_prospective_company("Stripe")
        db_module.add_prospective_company("Netflix")
        db_module.mark_prospective_scraped("Stripe")
        db_module.mark_prospective_exhausted("Netflix")

        summary = db_module.get_prospective_status_summary()
        self.assertEqual(summary.get("pending", 0), 1)
        self.assertEqual(summary.get("scraped", 0), 1)
        self.assertEqual(summary.get("exhausted", 0), 1)

    def test_get_prospective_company_returns_none_for_unknown(self):
        """None returned for company not in list."""
        result = db_module.get_prospective_company("UnknownCompany")
        self.assertIsNone(result)

    def test_get_prospective_company_returns_dict(self):
        """Returns dict with all fields."""
        db_module.add_prospective_company("Google")
        result = db_module.get_prospective_company("Google")
        self.assertIsInstance(result, dict)
        self.assertIn("id", result)
        self.assertIn("company", result)
        self.assertIn("status", result)
        self.assertIn("priority", result)


# ─────────────────────────────────────────
# TEST: convert_prospective_to_active
# ─────────────────────────────────────────

class TestConvertProspectiveToActive(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _setup_prospective_with_recruiter(self, company="Google"):
        """Create prospective app + recruiter for testing conversion."""
        placeholder_url = f"prospective://{company.lower().replace(' ', '-')}"
        app_id, _ = db_module.add_application(
            company=company,
            job_url=placeholder_url,
            status_override="prospective",
        )
        rid = db_module.add_recruiter(
            company, "John", "Recruiter", f"john@{company.lower()}.com", "auto"
        )
        db_module.link_recruiter_to_application(app_id, rid)
        db_module.add_prospective_company(company)
        db_module.mark_prospective_scraped(company)
        return app_id, rid

    def test_conversion_returns_app_id(self):
        """Successful conversion returns the application id."""
        self._setup_prospective_with_recruiter("Google")
        result = db_module.convert_prospective_to_active(
            "Google", "https://careers.google.com/jobs/123"
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, int)

    def test_conversion_updates_status_to_active(self):
        """Application status changes from prospective to active."""
        app_id, _ = self._setup_prospective_with_recruiter("Google")
        db_module.convert_prospective_to_active(
            "Google", "https://careers.google.com/jobs/123"
        )
        app = db_module.get_application_by_id(app_id)
        self.assertEqual(app["status"], "active")

    def test_conversion_updates_job_url(self):
        """Placeholder URL replaced with real job URL."""
        app_id, _ = self._setup_prospective_with_recruiter("Google")
        real_url = "https://careers.google.com/jobs/123"
        db_module.convert_prospective_to_active("Google", real_url)
        app = db_module.get_application_by_id(app_id)
        self.assertEqual(app["job_url"], real_url)

    def test_conversion_updates_job_title(self):
        """Job title updated on conversion if provided."""
        app_id, _ = self._setup_prospective_with_recruiter("Google")
        db_module.convert_prospective_to_active(
            "Google", "https://careers.google.com/jobs/123",
            job_title="Senior SWE"
        )
        app = db_module.get_application_by_id(app_id)
        self.assertEqual(app["job_title"], "Senior SWE")

    def test_conversion_updates_expected_domain(self):
        """Expected domain updated on conversion if provided."""
        app_id, _ = self._setup_prospective_with_recruiter("Google")
        db_module.convert_prospective_to_active(
            "Google", "https://careers.google.com/jobs/123",
            expected_domain="google"
        )
        app = db_module.get_application_by_id(app_id)
        self.assertEqual(app["expected_domain"], "google")

    def test_conversion_returns_none_if_no_prospective(self):
        """Returns None when company has no prospective application."""
        result = db_module.convert_prospective_to_active(
            "NonExistent", "https://nonexistent.com/jobs/1"
        )
        self.assertIsNone(result)

    def test_recruiters_still_linked_after_conversion(self):
        """Recruiters remain linked to application after conversion."""
        app_id, rid = self._setup_prospective_with_recruiter("Google")
        db_module.convert_prospective_to_active(
            "Google", "https://careers.google.com/jobs/123"
        )
        recruiters = db_module.get_recruiters_for_application(app_id)
        self.assertEqual(len(recruiters), 1)
        self.assertEqual(recruiters[0]["id"], rid)

    def test_conversion_appears_in_active_applications(self):
        """Converted application appears in get_all_active_applications."""
        self._setup_prospective_with_recruiter("Google")
        db_module.convert_prospective_to_active(
            "Google", "https://careers.google.com/jobs/123"
        )
        active = db_module.get_all_active_applications()
        companies = [a["company"] for a in active]
        self.assertIn("Google", companies)

    def test_conversion_does_not_affect_other_companies(self):
        """Converting Google doesn't affect Stripe prospective."""
        self._setup_prospective_with_recruiter("Google")
        self._setup_prospective_with_recruiter("Stripe")
        db_module.convert_prospective_to_active(
            "Google", "https://careers.google.com/jobs/123"
        )
        stripe = db_module.get_prospective_company("Stripe")
        self.assertEqual(stripe["status"], "scraped")


# ─────────────────────────────────────────
# TEST: --import-prospects
# ─────────────────────────────────────────

class TestImportProspects(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _write_temp_file(self, content):
        """Write content to temp file, return path."""
        f = tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='utf-8'
        )
        f.write(content)
        f.close()
        return f.name

    def test_import_valid_file(self):
        """Valid file imports all companies."""
        import pipeline
        path = self._write_temp_file("Google\nStripe\nNetflix\n")
        try:
            with patch("sys.stdout", new_callable=StringIO):
                pipeline.run_import_prospects(path)
            all_companies = db_module.get_prospective_companies()
            names = [c["company"] for c in all_companies]
            self.assertIn("Google", names)
            self.assertIn("Stripe", names)
            self.assertIn("Netflix", names)
        finally:
            os.unlink(path)

    def test_import_skips_blank_lines(self):
        """Blank lines not imported."""
        import pipeline
        path = self._write_temp_file("Google\n\n\nStripe\n")
        try:
            with patch("sys.stdout", new_callable=StringIO):
                pipeline.run_import_prospects(path)
            all_companies = db_module.get_prospective_companies()
            self.assertEqual(len(all_companies), 2)
        finally:
            os.unlink(path)

    def test_import_skips_comment_lines(self):
        """Lines starting with # skipped."""
        import pipeline
        path = self._write_temp_file(
            "# Big Tech\nGoogle\n# Fintech\nStripe\n"
        )
        try:
            with patch("sys.stdout", new_callable=StringIO):
                pipeline.run_import_prospects(path)
            all_companies = db_module.get_prospective_companies()
            names = [c["company"] for c in all_companies]
            self.assertNotIn("# Big Tech", names)
            self.assertNotIn("# Fintech", names)
            self.assertIn("Google", names)
            self.assertIn("Stripe", names)
        finally:
            os.unlink(path)

    def test_import_strips_whitespace(self):
        """Whitespace around company names stripped."""
        import pipeline
        path = self._write_temp_file("  Google  \n  Stripe  \n")
        try:
            with patch("sys.stdout", new_callable=StringIO):
                pipeline.run_import_prospects(path)
            all_companies = db_module.get_prospective_companies()
            names = [c["company"] for c in all_companies]
            self.assertIn("Google", names)
            self.assertIn("Stripe", names)
            self.assertNotIn("  Google  ", names)
        finally:
            os.unlink(path)

    def test_import_skips_duplicates(self):
        """Already existing companies not re-added."""
        import pipeline
        db_module.add_prospective_company("Google")
        path = self._write_temp_file("Google\nStripe\n")
        try:
            with patch("sys.stdout", new_callable=StringIO) as mock_out:
                pipeline.run_import_prospects(path)
                output = mock_out.getvalue()
            # Only 1 new (Stripe), 1 skipped (Google)
            self.assertIn("Added: 1", output)
            self.assertIn("Already existed: 1", output)
        finally:
            os.unlink(path)

    def test_import_missing_file_prints_error(self):
        """Missing file prints error message."""
        import pipeline
        with patch("sys.stdout", new_callable=StringIO) as mock_out:
            pipeline.run_import_prospects("nonexistent_file.txt")
            output = mock_out.getvalue()
        self.assertIn("[ERROR]", output)
        self.assertIn("not found", output)

    def test_import_missing_file_does_not_add_companies(self):
        """Missing file doesn't add anything to DB."""
        import pipeline
        with patch("sys.stdout", new_callable=StringIO):
            pipeline.run_import_prospects("nonexistent_file.txt")
        all_companies = db_module.get_prospective_companies()
        self.assertEqual(len(all_companies), 0)

    def test_import_prints_added_count(self):
        """Output shows correct added count."""
        import pipeline
        path = self._write_temp_file("Google\nStripe\nNetflix\n")
        try:
            with patch("sys.stdout", new_callable=StringIO) as mock_out:
                pipeline.run_import_prospects(path)
                output = mock_out.getvalue()
            self.assertIn("Added: 3", output)
        finally:
            os.unlink(path)


# ─────────────────────────────────────────
# TEST: CLI flags
# ─────────────────────────────────────────

class TestProspectsCLIFlags(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _run_main_with_args(self, args):
        import pipeline
        with patch.object(sys, "argv", ["pipeline.py"] + args):
            with patch("pipeline.init_db"):
                pipeline.main()

    @patch("pipeline.run_import_prospects")
    def test_import_prospects_flag(self, mock_import):
        """--import-prospects calls run_import_prospects."""
        self._run_main_with_args(["--import-prospects"])
        mock_import.assert_called_once()

    @patch("pipeline.run_import_prospects")
    def test_import_prospects_custom_filepath(self, mock_import):
        """--import-prospects with custom path passes it to function."""
        self._run_main_with_args(["--import-prospects", "mylist.txt"])
        mock_import.assert_called_once_with("mylist.txt")

    @patch("pipeline.run_import_prospects")
    def test_import_prospects_default_filepath(self, mock_import):
        """--import-prospects without path uses prospects.txt default."""
        self._run_main_with_args(["--import-prospects"])
        mock_import.assert_called_once_with("prospects.txt")

    @patch("pipeline.run_prospects_status")
    def test_prospects_status_flag(self, mock_status):
        """--prospects-status calls run_prospects_status."""
        self._run_main_with_args(["--prospects-status"])
        mock_status.assert_called_once()

    @patch("pipeline.run_import_prospects")
    @patch("pipeline.run_find_emails")
    def test_find_only_does_not_trigger_import(self, mock_find, mock_import):
        """--find-only does not call run_import_prospects."""
        self._run_main_with_args(["--find-only"])
        mock_import.assert_not_called()

    @patch("pipeline.run_prospects_status")
    @patch("pipeline.run_outreach")
    def test_outreach_only_does_not_trigger_status(self, mock_outreach, mock_status):
        """--outreach-only does not call run_prospects_status."""
        self._run_main_with_args(["--outreach-only"])
        mock_status.assert_not_called()


# ─────────────────────────────────────────
# TEST: --add prospective detection
# ─────────────────────────────────────────

class TestAddJobProspectiveDetection(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _setup_scraped_prospective(self, company="Google"):
        """Set up a scraped prospective company with a recruiter."""
        placeholder_url = f"prospective://{company.lower().replace(' ', '-')}"
        app_id, _ = db_module.add_application(
            company=company,
            job_url=placeholder_url,
            status_override="prospective",
        )
        rid = db_module.add_recruiter(
            company, "John", "Recruiter",
            f"john@{company.lower()}.com", "auto"
        )
        db_module.link_recruiter_to_application(app_id, rid)
        db_module.add_prospective_company(company)
        db_module.mark_prospective_scraped(company)
        return app_id, rid

    def test_add_detects_scraped_prospective_and_converts(self):
        """--add detects scraped prospective and converts instead of normal add."""
        import pipeline
        self._setup_scraped_prospective("Google")

        with patch("builtins.input", side_effect=[
            "Google", "https://careers.google.com/jobs/123",
            "Senior SWE", ""
        ]), patch("sys.stdout", new_callable=StringIO) as mock_out:
            pipeline.add_job_interactively()
            output = mock_out.getvalue()

        self.assertIn("prospective pipeline", output)
        self.assertIn("Converted", output)

    def test_add_conversion_changes_application_to_active(self):
        """After conversion application status is active."""
        import pipeline
        app_id, _ = self._setup_scraped_prospective("Google")

        with patch("builtins.input", side_effect=[
            "Google", "https://careers.google.com/jobs/123",
            "", ""
        ]), patch("sys.stdout", new_callable=StringIO):
            pipeline.add_job_interactively()

        app = db_module.get_application_by_id(app_id)
        self.assertEqual(app["status"], "active")

    def test_add_conversion_marks_prospective_converted(self):
        """After --add conversion, prospective status = converted."""
        import pipeline
        self._setup_scraped_prospective("Google")

        with patch("builtins.input", side_effect=[
            "Google", "https://careers.google.com/jobs/123",
            "", ""
        ]), patch("sys.stdout", new_callable=StringIO):
            pipeline.add_job_interactively()

        company = db_module.get_prospective_company("Google")
        self.assertEqual(company["status"], "converted")

    def test_add_pending_prospective_uses_normal_flow(self):
        """Company in prospective but only pending → normal add flow."""
        import pipeline
        # Add as pending only (not yet scraped)
        db_module.add_prospective_company("Google")

        with patch("builtins.input", side_effect=[
            "Google", "https://careers.google.com/jobs/123",
            "", ""
        ]), patch("sys.stdout", new_callable=StringIO) as mock_out, \
           patch("jobs.job_fetcher.fetch_job_description", return_value=None):
            pipeline.add_job_interactively()
            output = mock_out.getvalue()

        # Should use normal add (not prospective conversion)
        self.assertNotIn("prospective pipeline", output)
        self.assertIn("[OK] Added", output)

    def test_add_unknown_company_uses_normal_flow(self):
        """Company not in prospective list → normal add flow."""
        import pipeline

        with patch("builtins.input", side_effect=[
            "Stripe", "https://stripe.com/jobs/123",
            "", ""
        ]), patch("sys.stdout", new_callable=StringIO) as mock_out, \
           patch("jobs.job_fetcher.fetch_job_description", return_value=None):
            pipeline.add_job_interactively()
            output = mock_out.getvalue()

        self.assertNotIn("prospective pipeline", output)
        self.assertIn("[OK] Added", output)

    def test_add_converted_company_has_recruiters_for_outreach(self):
        """After conversion recruiters are linked and outreach can start."""
        import pipeline
        app_id, rid = self._setup_scraped_prospective("Google")

        with patch("builtins.input", side_effect=[
            "Google", "https://careers.google.com/jobs/123",
            "", ""
        ]), patch("sys.stdout", new_callable=StringIO):
            pipeline.add_job_interactively()

        recruiters = db_module.get_recruiters_for_application(app_id)
        self.assertEqual(len(recruiters), 1)


# ─────────────────────────────────────────
# TEST: mark_applications_exhausted (bulk)
# ─────────────────────────────────────────

class TestMarkApplicationsExhaustedBulk(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _make_app(self, company, url):
        app_id, _ = db_module.add_application(company, url, "SWE")
        return app_id

    def test_bulk_exhausts_all_ids(self):
        """All provided IDs marked exhausted in one call."""
        id1 = self._make_app("Google", "https://g.com/1")
        id2 = self._make_app("Google", "https://g.com/2")
        db_module.mark_applications_exhausted([id1, id2])
        for app_id in [id1, id2]:
            app = db_module.get_application_by_id(app_id)
            self.assertEqual(app["status"], "exhausted")
            self.assertIsNotNone(app["exhausted_at"])

    def test_bulk_empty_list_no_error(self):
        """Empty list does not raise."""
        db_module.mark_applications_exhausted([])

    def test_bulk_does_not_affect_other_apps(self):
        """Only specified IDs exhausted, others untouched."""
        id1 = self._make_app("Google", "https://g.com/1")
        id2 = self._make_app("Stripe", "https://s.com/1")
        db_module.mark_applications_exhausted([id1])
        app2 = db_module.get_application_by_id(id2)
        self.assertEqual(app2["status"], "active")

    def test_bulk_single_id(self):
        """Single ID in list works correctly."""
        id1 = self._make_app("Google", "https://g.com/1")
        db_module.mark_applications_exhausted([id1])
        app = db_module.get_application_by_id(id1)
        self.assertEqual(app["status"], "exhausted")


if __name__ == "__main__":
    unittest.main(verbosity=2)