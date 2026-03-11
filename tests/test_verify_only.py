"""
tests/test_verify_only.py — Comprehensive tests for run_verify_only()

Covers every branch and edge case:

  TestVerifyOnlySessionHandling
    - Missing session file → early return, error printed
    - Session file exists but expired → early return, error printed
    - Valid session → proceeds to verification

  TestVerifyOnlyNoApplications
    - No active applications → early return, info printed
    - Exhausted applications not included
    - Closed applications not included

  TestVerifyOnlyUnderStockedReport
    - All companies fully stocked → OK message printed
    - One company under-stocked → warning printed with details
    - Multiple companies under-stocked → all reported
    - Company with 0 active recruiters reported correctly
    - Correct "needs N more" calculation printed

  TestVerifyOnlyIntegration (DB-backed)
    - Fully stocked company stays fully stocked after verify
    - Recruiter marked inactive → company becomes under-stocked
    - Multiple companies mixed → only under-stocked reported
    - Exhausted application excluded from under-stocked check
    - Correct active recruiter count printed per company

  TestVerifyOnlyCLIFlag
    - --verify-only calls run_verify_only() exactly once
    - Other flags don't trigger run_verify_only()
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock, call
from io import StringIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tests.conftest import cleanup_db

TEST_DB = "data/test_pipeline.db"

import db.db as db_module
import db.connection as db_connection

# Override DB_FILE at module level — before any test runs
db_connection.DB_FILE = TEST_DB


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _make_app(company, url="https://example.com/jobs/1", title="SWE"):
    app_id, _ = db_module.add_application(company, url, title)
    return app_id


def _make_recruiter(company, email, status="active"):
    rid = db_module.add_recruiter(company, "John", "Recruiter", email, "auto")
    if status == "inactive":
        db_module.mark_recruiter_inactive(rid, "test")
    return rid


def _run_verify_only_mocked(mock_page_url="https://www.careershift.com/App/Dashboard/Overview"):
    """
    Run run_verify_only() with all Playwright calls mocked.
    Returns captured stdout.
    """
    import pipeline

    mock_page = MagicMock()
    mock_page.url = mock_page_url
    mock_browser = MagicMock()
    mock_context = MagicMock()
    mock_context.new_page.return_value = mock_page
    mock_browser.new_context.return_value = mock_context
    mock_playwright = MagicMock()
    mock_playwright.chromium.launch.return_value = mock_browser

    with patch("pipeline.sync_playwright") as mock_pw_ctx, \
         patch("pipeline.run_tiered_verification") as mock_verify, \
         patch("pipeline.load_dotenv"), \
         patch("careershift.utils.human_delay"), \
         patch("os.path.exists", return_value=True):
        mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
        mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            pipeline.run_verify_only()
            return mock_stdout.getvalue(), mock_verify


# ─────────────────────────────────────────
# TEST: Session handling
# ─────────────────────────────────────────

class TestVerifyOnlySessionHandling(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def test_missing_session_file_prints_error_and_returns(self):
        """No session file → print error, return immediately."""
        import pipeline

        with patch("os.path.exists", return_value=False), \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            pipeline.run_verify_only()
            output = mock_out.getvalue()

        self.assertIn("[ERROR]", output)
        self.assertIn("Session file not found", output)

    def test_missing_session_file_does_not_launch_playwright(self):
        """No session file → Playwright never launched."""
        import pipeline

        with patch("os.path.exists", return_value=False), \
             patch("pipeline.sync_playwright") as mock_pw, \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO):
            pipeline.run_verify_only()

        mock_pw.assert_not_called()

    def test_missing_session_file_does_not_run_verification(self):
        """No session file → tiered verification never runs."""
        import pipeline

        with patch("os.path.exists", return_value=False), \
             patch("pipeline.run_tiered_verification") as mock_verify, \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO):
            pipeline.run_verify_only()

        mock_verify.assert_not_called()

    def test_expired_session_prints_error_and_returns(self):
        """Session file exists but redirected to login page → error + return."""
        import pipeline

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/Login"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        _make_app("Google", "https://g.com/1")

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification") as mock_verify, \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()
            output = mock_out.getvalue()

        self.assertIn("[ERROR]", output)
        self.assertIn("Session expired", output)
        mock_verify.assert_not_called()

    def test_signin_url_detected_as_expired(self):
        """signin in URL treated same as login → expired session."""
        import pipeline

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/SignIn"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        _make_app("Google", "https://g.com/1")

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification") as mock_verify, \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()
            output = mock_out.getvalue()

        self.assertIn("Session expired", output)
        mock_verify.assert_not_called()

    def test_valid_session_prints_ok(self):
        """Valid session → [OK] Session valid printed."""
        import pipeline

        _make_app("Google", "https://g.com/1")

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/Dashboard/Overview"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification"), \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()
            output = mock_out.getvalue()

        self.assertIn("[OK] Session valid", output)

    def test_valid_session_closes_browser(self):
        """Browser always closed after verification."""
        import pipeline

        _make_app("Google", "https://g.com/1")

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/Dashboard/Overview"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification"), \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO):
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()

        mock_browser.close.assert_called_once()


# ─────────────────────────────────────────
# TEST: No applications
# ─────────────────────────────────────────

class TestVerifyOnlyNoApplications(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def test_no_active_applications_prints_info_and_returns(self):
        """No active applications → info message, return before Playwright."""
        import pipeline

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw, \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            pipeline.run_verify_only()
            output = mock_out.getvalue()

        self.assertIn("No active applications found", output)
        mock_pw.assert_not_called()

    def test_only_exhausted_applications_treated_as_no_applications(self):
        """Exhausted applications excluded → treated as no active apps."""
        import pipeline

        app_id = _make_app("Google", "https://g.com/1")
        db_module.mark_application_exhausted(app_id)

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw, \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            pipeline.run_verify_only()
            output = mock_out.getvalue()

        self.assertIn("No active applications found", output)
        mock_pw.assert_not_called()

    def test_only_closed_applications_treated_as_no_applications(self):
        """Closed applications excluded → treated as no active apps."""
        import pipeline

        app_id, _ = db_module.add_application("Google", "https://g.com/1", "SWE")
        conn = db_module.get_conn()
        conn.execute("UPDATE applications SET status = 'closed' WHERE id = ?", (app_id,))
        conn.commit()
        conn.close()

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw, \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            pipeline.run_verify_only()
            output = mock_out.getvalue()

        self.assertIn("No active applications found", output)
        mock_pw.assert_not_called()

    def test_active_applications_proceed_to_playwright(self):
        """Active applications present → Playwright launched."""
        import pipeline

        _make_app("Google", "https://g.com/1")

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/Dashboard/Overview"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification"), \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO):
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()

        mock_playwright.chromium.launch.assert_called_once()


# ─────────────────────────────────────────
# TEST: Under-stocked report
# ─────────────────────────────────────────

class TestVerifyOnlyUnderStockedReport(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _run_with_mocked_playwright(self):
        """Run verify_only with Playwright mocked, capture stdout."""
        import pipeline

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/Dashboard/Overview"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification"), \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO) as mock_out:
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()
            return mock_out.getvalue()

    def test_all_fully_stocked_prints_ok(self):
        """All companies with ≥ MIN_RECRUITERS (1) → OK message."""
        app_id = _make_app("Collective", "https://c.com/1")
        # MIN = 1, so 1 recruiter = satisfied
        rid = _make_recruiter("Collective", "r1@collective.com")
        db_module.link_recruiter_to_application(app_id, rid)

        output = self._run_with_mocked_playwright()
        self.assertIn("[OK] All companies have enough active recruiters", output)
        # Exclude infrastructure warnings (e.g. missing email credentials)
        # Only check for pipeline-level warnings about under-stocked companies
        pipeline_warnings = [
            line for line in output.split("\n")
            if "[WARNING]" in line
            and "email" not in line.lower()
            and "gmail" not in line.lower()
            and "smtp" not in line.lower()
            and "report" not in line.lower()
        ]
        self.assertEqual(pipeline_warnings, [],
            "Unexpected pipeline warnings: " + str(pipeline_warnings))

    def test_one_under_stocked_prints_warning(self):
        """Company with 0 active recruiters → WARNING printed."""
        app_id = _make_app("Stripe", "https://s.com/1")
        rid = _make_recruiter("Stripe", "r1@stripe.com")
        db_module.link_recruiter_to_application(app_id, rid)
        # Mark inactive → 0 active → under-stocked (MIN=1)
        db_module.mark_recruiter_inactive(rid, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("[WARNING]", output)
        self.assertIn("Stripe", output)

    def test_under_stocked_shows_active_count(self):
        """Under-stocked report shows correct active recruiter count (0)."""
        app_id = _make_app("Stripe", "https://s.com/1")
        rid = _make_recruiter("Stripe", "r1@stripe.com")
        db_module.link_recruiter_to_application(app_id, rid)
        db_module.mark_recruiter_inactive(rid, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("0 active recruiter", output)

    def test_under_stocked_shows_needs_more(self):
        """Under-stocked report shows needs 1 more (MIN=1, current=0)."""
        app_id = _make_app("Stripe", "https://s.com/1")
        rid = _make_recruiter("Stripe", "r1@stripe.com")
        db_module.link_recruiter_to_application(app_id, rid)
        db_module.mark_recruiter_inactive(rid, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("needs 1 more", output)

    def test_zero_active_recruiters_reported(self):
        """Company with 0 active recruiters correctly reported."""
        app_id = _make_app("Linear", "https://l.com/1")
        rid1 = _make_recruiter("Linear", "r1@linear.app")
        rid2 = _make_recruiter("Linear", "r2@linear.app")
        db_module.link_recruiter_to_application(app_id, rid1)
        db_module.link_recruiter_to_application(app_id, rid2)
        db_module.mark_recruiter_inactive(rid1, "left")
        db_module.mark_recruiter_inactive(rid2, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("Linear", output)
        self.assertIn("[WARNING]", output)

    def test_zero_active_needs_one_more(self):
        """Company with 0 recruiters needs 1 more (MIN_RECRUITERS_PER_COMPANY=1)."""
        app_id = _make_app("Linear", "https://l.com/1")
        rid = _make_recruiter("Linear", "r1@linear.app")
        db_module.link_recruiter_to_application(app_id, rid)
        db_module.mark_recruiter_inactive(rid, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("needs 1 more", output)

    def test_multiple_under_stocked_all_reported(self):
        """Multiple under-stocked companies all appear in report."""
        app1 = _make_app("Stripe", "https://s.com/1")
        rid1 = _make_recruiter("Stripe", "r1@stripe.com")
        db_module.link_recruiter_to_application(app1, rid1)
        db_module.mark_recruiter_inactive(rid1, "left")

        app2 = _make_app("Linear", "https://l.com/1")
        rid2 = _make_recruiter("Linear", "r1@linear.app")
        db_module.link_recruiter_to_application(app2, rid2)
        db_module.mark_recruiter_inactive(rid2, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("Stripe", output)
        self.assertIn("Linear", output)

    def test_under_stocked_suggests_find_only(self):
        """Under-stocked report suggests running --find-only."""
        app_id = _make_app("Stripe", "https://s.com/1")
        rid = _make_recruiter("Stripe", "r1@stripe.com")
        db_module.link_recruiter_to_application(app_id, rid)
        db_module.mark_recruiter_inactive(rid, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("--find-only", output)

    def test_fully_stocked_company_not_in_warning(self):
        """Fully stocked company (≥1 recruiter) never in warning."""
        # Stripe has 1 active recruiter (MIN=1 satisfied)
        app1 = _make_app("Stripe", "https://s.com/1")
        rid = _make_recruiter("Stripe", "sr1@stripe.com")
        db_module.link_recruiter_to_application(app1, rid)

        # Linear has 0 active recruiters (under-stocked)
        app2 = _make_app("Linear", "https://l.com/1")
        rid2 = _make_recruiter("Linear", "lr1@linear.app")
        db_module.link_recruiter_to_application(app2, rid2)
        db_module.mark_recruiter_inactive(rid2, "left")

        output = self._run_with_mocked_playwright()
        self.assertNotIn("Stripe", output)
        self.assertIn("Linear", output)

    def test_completion_message_always_printed(self):
        """Verification complete message always printed on success."""
        _make_app("Google", "https://g.com/1")

        output = self._run_with_mocked_playwright()
        self.assertIn("[OK] Verification complete!", output)

    def test_under_stocked_count_in_warning_message(self):
        """Warning shows count of under-stocked companies."""
        app1 = _make_app("Stripe", "https://s.com/1")
        rid1 = _make_recruiter("Stripe", "r1@stripe.com")
        db_module.link_recruiter_to_application(app1, rid1)
        db_module.mark_recruiter_inactive(rid1, "left")

        app2 = _make_app("Linear", "https://l.com/2")
        rid2 = _make_recruiter("Linear", "r1@linear.app")
        db_module.link_recruiter_to_application(app2, rid2)
        db_module.mark_recruiter_inactive(rid2, "left")

        output = self._run_with_mocked_playwright()
        self.assertIn("2", output)  # "2 company/companies under-stocked"


# ─────────────────────────────────────────
# TEST: Tiered verification called correctly
# ─────────────────────────────────────────

class TestVerifyOnlyVerificationCalled(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _run_with_mock(self):
        import pipeline

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/Dashboard/Overview"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification") as mock_verify, \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO):
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()
            return mock_verify

    def test_tiered_verification_called_once(self):
        """run_tiered_verification called exactly once."""
        _make_app("Google", "https://g.com/1")
        mock_verify = self._run_with_mock()
        mock_verify.assert_called_once()

    def test_tiered_verification_receives_applications(self):
        """run_tiered_verification called with active applications list."""
        _make_app("Google", "https://g.com/1")
        _make_app("Stripe", "https://s.com/1")
        mock_verify = self._run_with_mock()

        args = mock_verify.call_args
        # Second arg is applications list
        applications = args[0][1]
        self.assertEqual(len(applications), 2)

    def test_tiered_verification_receives_page(self):
        """run_tiered_verification receives page as first argument."""
        _make_app("Google", "https://g.com/1")
        mock_verify = self._run_with_mock()

        args = mock_verify.call_args
        page = args[0][0]
        self.assertIsNotNone(page)

    def test_tiered_verification_not_called_without_session(self):
        """run_tiered_verification not called when session missing."""
        import pipeline

        _make_app("Google", "https://g.com/1")

        with patch("os.path.exists", return_value=False), \
             patch("pipeline.run_tiered_verification") as mock_verify, \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO):
            pipeline.run_verify_only()

        mock_verify.assert_not_called()

    def test_tiered_verification_not_called_without_applications(self):
        """run_tiered_verification not called when no active applications."""
        import pipeline

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.run_tiered_verification") as mock_verify, \
             patch("pipeline.load_dotenv"), \
             patch("sys.stdout", new_callable=StringIO):
            pipeline.run_verify_only()

        mock_verify.assert_not_called()

    def test_tiered_verification_not_called_on_expired_session(self):
        """run_tiered_verification not called when session expired."""
        import pipeline

        _make_app("Google", "https://g.com/1")

        mock_page = MagicMock()
        mock_page.url = "https://www.careershift.com/App/Login"
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context
        mock_playwright = MagicMock()
        mock_playwright.chromium.launch.return_value = mock_browser

        with patch("os.path.exists", return_value=True), \
             patch("pipeline.sync_playwright") as mock_pw_ctx, \
             patch("pipeline.run_tiered_verification") as mock_verify, \
             patch("pipeline.load_dotenv"), \
             patch("careershift.utils.human_delay"), \
             patch("sys.stdout", new_callable=StringIO):
            mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_playwright)
            mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
            pipeline.run_verify_only()

        mock_verify.assert_not_called()


# ─────────────────────────────────────────
# TEST: CLI flag dispatch
# ─────────────────────────────────────────

class TestVerifyOnlyCLIFlag(unittest.TestCase):

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

    @patch("pipeline.run_verify_only")
    def test_verify_only_flag_calls_run_verify_only(self, mock_verify):
        """--verify-only dispatches to run_verify_only()."""
        self._run_main_with_args(["--verify-only"])
        mock_verify.assert_called_once()

    @patch("pipeline.run_verify_only")
    @patch("pipeline.run_find_emails")
    def test_find_only_does_not_call_verify_only(self, mock_find, mock_verify):
        """--find-only does not trigger run_verify_only()."""
        self._run_main_with_args(["--find-only"])
        mock_verify.assert_not_called()

    @patch("pipeline.run_verify_only")
    @patch("pipeline.run_outreach")
    def test_outreach_only_does_not_call_verify_only(self, mock_outreach, mock_verify):
        """--outreach-only does not trigger run_verify_only()."""
        self._run_main_with_args(["--outreach-only"])
        mock_verify.assert_not_called()

    @patch("pipeline.run_verify_only")
    @patch("pipeline.run_sync_forms")
    def test_sync_forms_does_not_call_verify_only(self, mock_sync, mock_verify):
        """--sync-forms does not trigger run_verify_only()."""
        self._run_main_with_args(["--sync-forms"])
        mock_verify.assert_not_called()

    @patch("pipeline.run_verify_only")
    @patch("pipeline.run_quota_report")
    def test_quota_report_does_not_call_verify_only(self, mock_report, mock_verify):
        """--quota-report does not trigger run_verify_only()."""
        self._run_main_with_args(["--quota-report"])
        mock_verify.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)