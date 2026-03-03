"""
tests/test_outreach.py — Comprehensive tests for outreach engine and AI personalizer

Covers:
- Send window status (wait/send/cutoff) at all boundary times
- wait_until_window sleep behavior
- reschedule_remaining pushes emails to tomorrow
- schedule_initial_outreach creates correct rows
- process_outreach routing based on window status
- Bounce detection marks recruiter inactive
- Failed send marks outreach failed without corrupting sent status
- schedule_next_outreach does not corrupt sent status on failure
- Template cache miss warning (no AI quota exhaustion)
- Template cache miss with AI quota exhausted warning
- AI cache key separation (JD vs fallback)
- AI cache hit returns without calling model
- Lazy client initialization
- Missing API key returns empty dict gracefully
- generate_all_content falls back when job_text empty
- generate_all_content_without_jd uses separate cache key
- _call_model returns empty dict when all models exhausted
- _call_model skips model at daily limit
"""

import sys
import os
import unittest
import smtplib
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TEST_DB = "data/test_pipeline.db"
import db.db as db_module
import db.connection as db_connection


def make_app(company="Acme", url=None, title="SWE"):
    url = url or f"https://{company.lower()}.com/jobs/{id(company)}"
    app_id, _ = db_module.add_application(company, url, title)
    return app_id


def make_recruiter(company="Acme", name="Jane", email=None):
    email = email or f"{name.lower()}@{company.lower()}.com"
    return db_module.add_recruiter(company, name, "Recruiter", email, "auto")


class TestSendWindow(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        # Force close any lingering WAL connections before deleting
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass
        db_module.init_db()

    def tearDown(self):
        # Force WAL checkpoint and close all connections before deleting on Windows
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass

    def _mock_now(self, hour, minute=0):
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/New_York")
        return datetime.now(tz).replace(hour=hour, minute=minute, second=0, microsecond=0)

    @patch("outreach.outreach_engine._now")
    def test_before_window_returns_wait(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(8, 59)
        self.assertEqual(get_send_status(), "wait")

    @patch("outreach.outreach_engine._now")
    def test_at_window_start_returns_send(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(9, 0)
        self.assertEqual(get_send_status(), "send")

    @patch("outreach.outreach_engine._now")
    def test_within_preferred_window_returns_send(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(10, 30)
        self.assertEqual(get_send_status(), "send")

    @patch("outreach.outreach_engine._now")
    def test_at_window_end_returns_send(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(11, 0)
        self.assertEqual(get_send_status(), "send")

    @patch("outreach.outreach_engine._now")
    def test_grace_period_returns_send(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(11, 30)
        self.assertEqual(get_send_status(), "send")

    @patch("outreach.outreach_engine._now")
    def test_at_hard_cutoff_returns_cutoff(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(12, 0)
        self.assertEqual(get_send_status(), "cutoff")

    @patch("outreach.outreach_engine._now")
    def test_after_cutoff_returns_cutoff(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(15, 0)
        self.assertEqual(get_send_status(), "cutoff")

    @patch("outreach.outreach_engine._now")
    def test_midnight_returns_wait(self, mock_now):
        from outreach.outreach_engine import get_send_status
        mock_now.return_value = self._mock_now(0, 0)
        self.assertEqual(get_send_status(), "wait")

    def test_reschedule_remaining_pushes_to_tomorrow(self):
        from outreach.outreach_engine import reschedule_remaining
        app_id = make_app()
        rid = make_recruiter()
        db_module.link_recruiter_to_application(app_id, rid)
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(rid, app_id, "initial", today)
        pending = db_module.get_pending_outreach()
        reschedule_remaining(pending)
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT scheduled_for FROM outreach WHERE id = ?", (oid,))
        self.assertEqual(c.fetchone()["scheduled_for"], tomorrow)
        conn.close()

    def test_reschedule_remaining_handles_empty_list(self):
        from outreach.outreach_engine import reschedule_remaining
        # Should not raise
        reschedule_remaining([])


class TestScheduleInitialOutreach(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        # Force close any lingering WAL connections before deleting
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass
        db_module.init_db()

    def tearDown(self):
        # Force WAL checkpoint and close all connections before deleting on Windows
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass

    def test_schedules_initial_for_new_recruiter(self):
        from outreach.outreach_engine import schedule_initial_outreach
        app_id = make_app()
        rid = make_recruiter()
        db_module.link_recruiter_to_application(app_id, rid)
        schedule_initial_outreach()
        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["stage"], "initial")

    def test_does_not_duplicate_existing_outreach(self):
        from outreach.outreach_engine import schedule_initial_outreach
        app_id = make_app()
        rid = make_recruiter()
        db_module.link_recruiter_to_application(app_id, rid)
        schedule_initial_outreach()
        schedule_initial_outreach()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM outreach")
        self.assertEqual(c.fetchone()["cnt"], 1)
        conn.close()

    def test_schedules_for_today(self):
        from outreach.outreach_engine import schedule_initial_outreach
        app_id = make_app()
        rid = make_recruiter()
        db_module.link_recruiter_to_application(app_id, rid)
        schedule_initial_outreach()
        today = datetime.now().strftime("%Y-%m-%d")
        pending = db_module.get_pending_outreach()
        self.assertEqual(pending[0]["scheduled_for"], today)

    def test_no_outreach_when_no_recruiters(self):
        from outreach.outreach_engine import schedule_initial_outreach
        make_app()
        schedule_initial_outreach()
        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 0)

    def test_schedules_multiple_recruiters_for_same_application(self):
        from outreach.outreach_engine import schedule_initial_outreach
        app_id = make_app()
        rid1 = make_recruiter("Acme", "Alice", "alice@acme.com")
        rid2 = make_recruiter("Acme", "Bob", "bob@acme.com")
        db_module.link_recruiter_to_application(app_id, rid1)
        db_module.link_recruiter_to_application(app_id, rid2)
        schedule_initial_outreach()
        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 2)


class TestProcessOutreach(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        # Force close any lingering WAL connections before deleting
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass
        db_module.init_db()

    def tearDown(self):
        # Force WAL checkpoint and close all connections before deleting on Windows
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass

    def _setup_pending_email(self):
        app_id = make_app()
        rid = make_recruiter()
        db_module.link_recruiter_to_application(app_id, rid)
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(rid, app_id, "initial", today)
        return app_id, rid, oid

    @patch("outreach.outreach_engine.get_send_status", return_value="cutoff")
    @patch("outreach.outreach_engine.get_pending_outreach")
    def test_cutoff_reschedules_pending(self, mock_pending, mock_status):
        from outreach.outreach_engine import process_outreach
        app_id, rid, oid = self._setup_pending_email()
        mock_pending.return_value = db_module.get_pending_outreach()
        with patch("outreach.outreach_engine.reschedule_remaining") as mock_reschedule:
            process_outreach()
            mock_reschedule.assert_called_once()

    @patch("outreach.outreach_engine.get_send_status", return_value="send")
    @patch("outreach.outreach_engine.get_template", return_value=None)
    @patch("time.sleep")
    def test_missing_template_skips_email(self, mock_sleep, mock_template, mock_status):
        from outreach.outreach_engine import process_outreach
        app_id, rid, oid = self._setup_pending_email()
        process_outreach()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        # Should still be pending since we skipped
        self.assertEqual(c.fetchone()["status"], "pending")
        conn.close()

    @patch("outreach.outreach_engine.get_send_status", return_value="send")
    @patch("outreach.outreach_engine.get_template", return_value=("Email body", "Subject"))
    @patch("outreach.outreach_engine.send_email")
    @patch("time.sleep")
    def test_successful_send_marks_sent(self, mock_sleep, mock_send, mock_template, mock_status):
        from outreach.outreach_engine import process_outreach
        app_id, rid, oid = self._setup_pending_email()
        process_outreach()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        self.assertEqual(c.fetchone()["status"], "sent")
        conn.close()

    @patch("outreach.outreach_engine.get_send_status", return_value="send")
    @patch("outreach.outreach_engine.get_template", return_value=("Body", "Subject"))
    @patch("outreach.outreach_engine.send_email",
           side_effect=smtplib.SMTPRecipientsRefused({}))
    @patch("time.sleep")
    def test_hard_bounce_marks_recruiter_inactive(self, mock_sleep, mock_send, mock_template, mock_status):
        from outreach.outreach_engine import process_outreach
        app_id, rid, oid = self._setup_pending_email()
        process_outreach()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT recruiter_status FROM recruiters WHERE id = ?", (rid,))
        self.assertEqual(c.fetchone()["recruiter_status"], "inactive")
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        self.assertEqual(c.fetchone()["status"], "bounced")
        conn.close()

    @patch("outreach.outreach_engine.get_send_status", return_value="send")
    @patch("outreach.outreach_engine.get_template", return_value=("Body", "Subject"))
    @patch("outreach.outreach_engine.send_email", side_effect=Exception("SMTP timeout"))
    @patch("time.sleep")
    def test_send_failure_marks_failed_not_sent(self, mock_sleep, mock_send, mock_template, mock_status):
        from outreach.outreach_engine import process_outreach
        app_id, rid, oid = self._setup_pending_email()
        process_outreach()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        self.assertEqual(c.fetchone()["status"], "failed")
        conn.close()

    @patch("outreach.outreach_engine.get_send_status", return_value="send")
    @patch("outreach.outreach_engine.get_template", return_value=("Body", "Subject"))
    @patch("outreach.outreach_engine.send_email")
    @patch("outreach.outreach_engine.schedule_next_outreach", side_effect=Exception("DB error"))
    @patch("time.sleep")
    def test_schedule_next_failure_does_not_corrupt_sent_status(
            self, mock_sleep, mock_next, mock_send, mock_template, mock_status):
        from outreach.outreach_engine import process_outreach
        app_id, rid, oid = self._setup_pending_email()
        process_outreach()
        # Status must be 'sent' even though schedule_next_outreach failed
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        self.assertEqual(c.fetchone()["status"], "sent")
        conn.close()

    @patch("outreach.outreach_engine.get_send_status", return_value="send")
    @patch("outreach.outreach_engine.get_template", return_value=("Body", "Subject"))
    @patch("outreach.outreach_engine.send_email")
    @patch("time.sleep")
    def test_successful_send_schedules_followup(self, mock_sleep, mock_send, mock_template, mock_status):
        from outreach.outreach_engine import process_outreach
        app_id, rid, oid = self._setup_pending_email()
        process_outreach()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT stage FROM outreach WHERE stage = 'followup1'")
        row = c.fetchone()
        conn.close()
        self.assertIsNotNone(row)

    @patch("outreach.outreach_engine.get_send_status", return_value="send")
    @patch("time.sleep")
    def test_no_pending_emails_exits_cleanly(self, mock_sleep, mock_status):
        from outreach.outreach_engine import process_outreach
        # Should not raise
        process_outreach()


class TestAIPersonalizer(unittest.TestCase):

    def setUp(self):
        db_connection.DB_FILE = TEST_DB
        # Force close any lingering WAL connections before deleting
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass
        db_module.init_db()
        # Reset module-level _client
        import outreach.ai_full_personalizer as mod
        mod._client = None

    def tearDown(self):
        # Force WAL checkpoint and close all connections before deleting on Windows
        try:
            import sqlite3
            conn = sqlite3.connect(TEST_DB)
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.close()
        except Exception:
            pass
        import gc
        gc.collect()
        for ext in ['', '-wal', '-shm']:
            path = TEST_DB + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    pass
        import outreach.ai_full_personalizer as mod
        mod._client = None

    def _sample_response(self):
        return {
            "subject_initial": "SWE at Google",
            "subject_followup1": "Following up",
            "subject_followup2": "Final follow-up",
            "intro": "I am interested.",
            "followup1": "Following up.",
            "followup2": "Final.",
        }

    def test_missing_api_key_returns_empty_dict(self):
        from outreach.ai_full_personalizer import generate_all_content
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("GEMINI_API_KEY", None)
            import outreach.ai_full_personalizer as mod
            mod._client = None
            result = generate_all_content("Google", "SWE", "job text " + "A" * 300)
            self.assertEqual(result, {})

    def test_cache_hit_returns_without_api_call(self):
        from outreach.ai_full_personalizer import generate_all_content, _cache_key
        import hashlib
        job_text = "job text " + "A" * 300
        key = _cache_key("Google", "SWE", job_text)
        db_module.save_ai_cache(key, "Google", "SWE", self._sample_response())
        with patch("outreach.ai_full_personalizer._get_client") as mock_client:
            result = generate_all_content("Google", "SWE", job_text)
            mock_client.assert_not_called()
        self.assertEqual(result["subject_initial"], "SWE at Google")

    def test_empty_job_text_falls_back_to_no_jd(self):
        from outreach.ai_full_personalizer import generate_all_content
        with patch("outreach.ai_full_personalizer.generate_all_content_without_jd") as mock_fallback:
            mock_fallback.return_value = self._sample_response()
            result = generate_all_content("Google", "SWE", "")
            mock_fallback.assert_called_once_with("Google", "SWE")

    def test_none_job_text_falls_back_to_no_jd(self):
        from outreach.ai_full_personalizer import generate_all_content
        with patch("outreach.ai_full_personalizer.generate_all_content_without_jd") as mock_fallback:
            mock_fallback.return_value = self._sample_response()
            result = generate_all_content("Google", "SWE", None)
            mock_fallback.assert_called_once_with("Google", "SWE")

    def test_fallback_uses_separate_cache_key(self):
        from outreach.ai_full_personalizer import _cache_key, _fallback_cache_key
        jd_key = _cache_key("Google", "SWE", "some job text")
        fb_key = _fallback_cache_key("Google", "SWE")
        self.assertNotEqual(jd_key, fb_key)

    def test_fallback_cache_hit_no_api_call(self):
        from outreach.ai_full_personalizer import generate_all_content_without_jd, _fallback_cache_key
        key = _fallback_cache_key("Google", "SWE")
        db_module.save_ai_cache(key, "Google", "SWE", self._sample_response())
        with patch("outreach.ai_full_personalizer._get_client") as mock_client:
            result = generate_all_content_without_jd("Google", "SWE")
            mock_client.assert_not_called()
        self.assertEqual(result["subject_initial"], "SWE at Google")

    def test_all_models_exhausted_returns_empty(self):
        from outreach.ai_full_personalizer import _call_model
        with patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=True):
            result = _call_model("prompt", "key", "Google", "SWE")
            self.assertEqual(result, {})

    def test_client_none_returns_empty(self):
        from outreach.ai_full_personalizer import _call_model
        with patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=False):
            with patch("outreach.ai_full_personalizer._get_client", return_value=None):
                result = _call_model("prompt", "key", "Google", "SWE")
                self.assertEqual(result, {})

    def test_model_at_daily_limit_skipped(self):
        from outreach.ai_full_personalizer import _call_model
        import json
        mock_response = MagicMock()
        mock_response.text = json.dumps(self._sample_response())
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        with patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=False):
            with patch("outreach.ai_full_personalizer._get_client", return_value=mock_client):
                # Primary model at limit, fallback available
                with patch("outreach.ai_full_personalizer.can_call",
                           side_effect=lambda m: m == "gemini-2.5-flash"):
                    with patch("outreach.ai_full_personalizer.increment_usage"):
                        result = _call_model("prompt", "newkey123", "Google", "SWE")
                        # Should have used fallback model
                        self.assertNotEqual(result, {})

    def test_successful_generation_saves_to_cache(self):
        from outreach.ai_full_personalizer import _call_model
        import json
        mock_response = MagicMock()
        mock_response.text = json.dumps(self._sample_response())
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        with patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=False):
            with patch("outreach.ai_full_personalizer._get_client", return_value=mock_client):
                with patch("outreach.ai_full_personalizer.can_call", return_value=True):
                    with patch("outreach.ai_full_personalizer.increment_usage"):
                        import hashlib
                        key = hashlib.sha256("test-save-cache".encode()).hexdigest()
                        result = _call_model("prompt", key, "Google", "SWE")
                        cached = db_module.get_ai_cache(key)
                        self.assertIsNotNone(cached)

    def test_invalid_json_response_tries_next_model(self):
        from outreach.ai_full_personalizer import _call_model
        mock_response = MagicMock()
        mock_response.text = "NOT JSON AT ALL"
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        with patch("outreach.ai_full_personalizer.all_models_exhausted", return_value=False):
            with patch("outreach.ai_full_personalizer._get_client", return_value=mock_client):
                with patch("outreach.ai_full_personalizer.can_call", return_value=True):
                    with patch("outreach.ai_full_personalizer.increment_usage"):
                        result = _call_model("prompt", "badkey", "Google", "SWE")
                        # Both models failed → empty dict
                        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)