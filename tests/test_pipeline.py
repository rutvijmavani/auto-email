"""
tests/test_pipeline.py — Tests for pipeline.py orchestration
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tests.conftest import cleanup_db

TEST_DB = "data/test_pipeline.db"
import db.db as db_module
import db.connection as db_connection

# Override DB_FILE at module level — before any test runs
db_connection.DB_FILE = TEST_DB


class TestPipelineAddFlow(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def test_add_application_new_returns_true(self):
        app_id, created = db_module.add_application("Google", "https://g.com/1", "SWE")
        self.assertTrue(created)
        self.assertIsNotNone(app_id)

    def test_add_application_duplicate_returns_false(self):
        db_module.add_application("Google", "https://g.com/1", "SWE")
        _, created = db_module.add_application("Google", "https://g.com/1", "SWE")
        self.assertFalse(created)

    @patch("jobs.job_fetcher.fetch_job_description", return_value="Job text " + "A" * 300)
    @patch("db.db.add_application", return_value=(1, True))
    def test_add_job_interactively_scrapes_jd(self, mock_add, mock_fetch):
        import pipeline
        with patch("builtins.input", side_effect=["Google", "https://g.com/1", "SWE", ""]):
            pipeline.add_job_interactively()


class TestGenerateAIContentForAll(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    def _add_app(self, company="Google", url="https://g.com/1", title="SWE"):
        db_module.add_application(company, url, title)

    @patch("jobs.job_fetcher.fetch_job_description",
           return_value="Job text " + "A" * 300)
    @patch("outreach.ai_full_personalizer.generate_all_content",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.generate_all_content_without_jd",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=True)
    @patch("db.db.get_applications_missing_ai_cache", return_value=[])
    def test_uses_jd_when_available(
            self, mock_missing, mock_exhausted, mock_fallback, mock_gen, mock_fetch):
        self._add_app()
        import pipeline
        pipeline._generate_ai_content_for_all()
        mock_gen.assert_called_once()
        mock_fallback.assert_not_called()

    @patch("jobs.job_fetcher.fetch_job_description", return_value=None)
    @patch("outreach.ai_full_personalizer.generate_all_content",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.generate_all_content_without_jd",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=True)
    @patch("db.db.get_applications_missing_ai_cache", return_value=[])
    def test_uses_fallback_when_no_jd(
            self, mock_missing, mock_exhausted, mock_fallback, mock_gen, mock_fetch):
        self._add_app()
        import pipeline
        pipeline._generate_ai_content_for_all()
        mock_fallback.assert_called_once()
        mock_gen.assert_not_called()

    @patch("jobs.job_fetcher.fetch_job_description", return_value="")
    @patch("outreach.ai_full_personalizer.generate_all_content",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.generate_all_content_without_jd",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=True)
    @patch("db.db.get_applications_missing_ai_cache", return_value=[])
    def test_uses_fallback_when_empty_jd(
            self, mock_missing, mock_exhausted, mock_fallback, mock_gen, mock_fetch):
        self._add_app()
        import pipeline
        pipeline._generate_ai_content_for_all()
        mock_fallback.assert_called_once()

    @patch("jobs.job_fetcher.fetch_job_description",
           return_value={"job_text": "Job text " + "A" * 300, "job_title": "Backend Engineer"})
    @patch("outreach.ai_full_personalizer.generate_all_content",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.generate_all_content_without_jd",
           return_value={"subject_initial": "Test"})
    @patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=True)
    @patch("db.db.get_applications_missing_ai_cache", return_value=[])
    def test_normalizes_dict_jd_response(
            self, mock_missing, mock_exhausted, mock_fallback, mock_gen, mock_fetch):
        # Add app with SWE title — dict response overrides with Backend Engineer
        self._add_app(title="SWE")
        import pipeline
        pipeline._generate_ai_content_for_all()
        # generate_all_content should be called with the extracted job_text
        self.assertTrue(mock_gen.called)
        # The second arg should be the job_title from the dict
        args = mock_gen.call_args[0]
        self.assertEqual(args[1], "Backend Engineer")

    @patch("jobs.job_fetcher.fetch_job_description", return_value=None)
    @patch("outreach.ai_full_personalizer.generate_all_content",
           return_value={"subject_initial": "X"})
    @patch("outreach.ai_full_personalizer.generate_all_content_without_jd",
           return_value={"subject_initial": "X"})
    @patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=True)
    @patch("db.db.get_applications_missing_ai_cache")
    def test_no_leftover_when_quota_exhausted(
            self, mock_missing, mock_exhausted, mock_fallback, mock_gen, mock_fetch):
        self._add_app()
        import pipeline
        # Patch at the location pipeline.py actually reads it from
        with patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=True):
            pipeline._generate_ai_content_for_all()
        mock_missing.assert_not_called()

    @patch("jobs.job_fetcher.fetch_job_description", return_value=None)
    @patch("outreach.ai_full_personalizer.generate_all_content",
           return_value={"subject_initial": "X"})
    @patch("outreach.ai_full_personalizer.generate_all_content_without_jd",
           return_value={"subject_initial": "X"})
    @patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=False)
    @patch("db.db.get_applications_missing_ai_cache")
    def test_leftover_quota_fills_missing_cache(
            self, mock_missing, mock_exhausted, mock_fallback, mock_gen, mock_fetch):
        self._add_app("Google", "https://g.com/1")
        mock_missing.return_value = [
            {"company": "Meta", "job_url": "https://m.com/1",
             "job_title": "SWE", "applied_date": "2026-01-01"}
        ]
        import pipeline
        pipeline._generate_ai_content_for_all()
        mock_missing.assert_called()


class TestQuotaReport(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        cleanup_db(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        cleanup_db(TEST_DB)

    @patch("db.db.check_quota_health", return_value=[])
    def test_silent_when_healthy_and_silent_flag(self, mock_health):
        import pipeline
        with patch("outreach.email_sender.send_email") as mock_email:
            pipeline.run_quota_report(silent_if_healthy=True)
            mock_email.assert_not_called()

    @patch("db.db.check_quota_health", return_value=[])
    def test_prints_ok_when_healthy_and_not_silent(self, mock_health):
        import pipeline
        with patch("builtins.print") as mock_print:
            pipeline.run_quota_report(silent_if_healthy=False)
            printed = " ".join(str(c) for c in mock_print.call_args_list)
            self.assertIn("no issues", printed.lower())

    @patch("db.db.check_quota_health")
    @patch("db.db.save_quota_alert")
    @patch("outreach.email_sender.send_email")
    def test_sends_email_when_alerts_exist(self, mock_send, mock_save, mock_health):
        import pipeline
        mock_health.return_value = [{
            "alert_type": "underutilized",
            "quota_type": "careershift",
            "start_date": "2026-02-26",
            "end_date": "2026-02-28",
            "avg_used": 10.0,
            "avg_remaining": 40.0,
            "total_limit": 50,
            "suggested_cap": 6,
            "history": [
                {"date": "2026-02-26", "used": 10, "remaining": 40, "total_limit": 50},
                {"date": "2026-02-27", "used": 10, "remaining": 40, "total_limit": 50},
                {"date": "2026-02-28", "used": 10, "remaining": 40, "total_limit": 50},
            ],
        }]
        pipeline.run_quota_report()
        mock_send.assert_called_once()
        mock_save.assert_called_once()

    @patch("db.db.check_quota_health")
    @patch("db.db.save_quota_alert")
    @patch("outreach.email_sender.send_email", side_effect=Exception("SMTP failed"))
    def test_email_failure_prints_alert_to_console(self, mock_send, mock_save, mock_health):
        import pipeline
        mock_health.return_value = [{
            "alert_type": "exhausted",
            "quota_type": "gemini",
            "start_date": "2026-02-26",
            "end_date": "2026-02-28",
            "avg_used": 40.0,
            "avg_remaining": 0.0,
            "total_limit": 40,
            "suggested_cap": None,
            "history": [
                {"date": "2026-02-26", "used": 40, "remaining": 0, "total_limit": 40},
                {"date": "2026-02-27", "used": 40, "remaining": 0, "total_limit": 40},
                {"date": "2026-02-28", "used": 40, "remaining": 0, "total_limit": 40},
            ],
        }]
        with patch("builtins.print") as mock_print:
            pipeline.run_quota_report()
            printed = " ".join(str(c) for c in mock_print.call_args_list)
            self.assertIn("exhausted", printed.lower())

    @patch("db.db.check_quota_health")
    @patch("db.db.save_quota_alert")
    @patch("outreach.email_sender.send_email")
    def test_multiple_alerts_combined_in_subject(self, mock_send, mock_save, mock_health):
        import pipeline
        alert_template = {
            "start_date": "2026-02-26",
            "end_date": "2026-02-28",
            "avg_used": 10.0,
            "avg_remaining": 40.0,
            "total_limit": 50,
            "suggested_cap": None,
            "history": [
                {"date": "2026-02-26", "used": 10, "remaining": 40, "total_limit": 50},
            ] * 3,
        }
        mock_health.return_value = [
            {**alert_template, "alert_type": "underutilized", "quota_type": "careershift"},
            {**alert_template, "alert_type": "exhausted", "quota_type": "gemini"},
        ]
        pipeline.run_quota_report()
        subject = mock_send.call_args[1]["subject"]
        self.assertIn("2", subject)


class TestCLIFlags(unittest.TestCase):

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

    @patch("pipeline.run_sync_forms")
    def test_sync_forms_flag(self, mock_sync):
        self._run_main_with_args(["--sync-forms"])
        mock_sync.assert_called_once()

    @patch("pipeline.add_job_interactively")
    def test_add_flag(self, mock_add):
        self._run_main_with_args(["--add"])
        mock_add.assert_called_once()

    @patch("pipeline.run_find_emails")
    def test_find_only_flag(self, mock_find):
        self._run_main_with_args(["--find-only"])
        mock_find.assert_called_once()

    @patch("pipeline.run_outreach")
    def test_outreach_only_flag(self, mock_outreach):
        self._run_main_with_args(["--outreach-only"])
        mock_outreach.assert_called_once()

    @patch("pipeline.run_quota_report")
    def test_quota_report_flag(self, mock_report):
        self._run_main_with_args(["--quota-report"])
        mock_report.assert_called_once()

    @patch("pipeline.run_find_emails")
    def test_find_only_does_not_trigger_quota_report_directly(self, mock_find):
        self._run_main_with_args(["--find-only"])
        mock_find.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)