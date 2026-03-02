"""
tests/test_integration.py — End-to-end integration tests

Covers:
- Full pipeline: add → find → outreach
- Two applications at same company share recruiters
- Full 3-stage outreach sequence completes correctly
- Recruiter inactivation stops outreach sequence
- Email bounce detection stops outreach automatically
- Leftover CareerShift quota tops up under-stocked companies
- AI cache reused across multiple runs (no duplicate API calls)
- Quota health check triggers after find-only
- Search term tracking persists across runs
- Data retention cleanup runs on init_db
"""

import sys
import os
import json
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TEST_DB = "data/test_pipeline.db"
import db.db as db_module
import db.connection as db_connection


class TestFullPipelineFlow(unittest.TestCase):

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

    def test_add_find_outreach_full_flow(self):
        """Complete flow: add application → find recruiter → send email."""
        import hashlib

        # Step 1: Add application
        app_id, created = db_module.add_application(
            "Google", "https://g.com/1", "Backend Engineer", "2026-02-28"
        )
        self.assertTrue(created)

        # Step 2: Add JD to cache
        db_module.save_job("https://g.com/1", "Job Title: Backend Engineer\n" + "A" * 300)

        # Step 3: Add recruiter and link
        rid = db_module.add_recruiter("Google", "John", "Recruiter", "john@g.com", "auto")
        db_module.link_recruiter_to_application(app_id, rid)

        # Step 4: Add AI cache
        key = hashlib.sha256(f"Google-Backend Engineer-".encode()).hexdigest()
        db_module.save_ai_cache(key, "Google", "Backend Engineer", {
            "subject_initial": "Backend Engineer at Google",
            "subject_followup1": "Following Up",
            "subject_followup2": "Final Follow-Up",
            "intro": "I am interested.",
            "followup1": "Following up.",
            "followup2": "Final.",
        })

        # Step 5: Schedule outreach
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(rid, app_id, "initial", today)
        self.assertIsNotNone(oid)

        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["company"], "Google")

        # Step 6: Mark sent and schedule followup
        db_module.mark_outreach_sent(oid)
        next_id = db_module.schedule_next_outreach(rid, app_id)
        self.assertIsNotNone(next_id)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT stage FROM outreach WHERE id = ?", (next_id,))
        self.assertEqual(c.fetchone()["stage"], "followup1")
        conn.close()


class TestSharedRecruiters(unittest.TestCase):

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

    def test_two_applications_share_same_recruiter(self):
        app1, _ = db_module.add_application("Google", "https://g.com/1", "SWE")
        app2, _ = db_module.add_application("Google", "https://g.com/2", "SRE")
        rid = db_module.add_recruiter("Google", "John", "Recruiter", "john@g.com", "auto")
        db_module.link_recruiter_to_application(app1, rid)
        db_module.link_recruiter_to_application(app2, rid)

        r1 = db_module.get_recruiters_for_application(app1)
        r2 = db_module.get_recruiters_for_application(app2)
        self.assertEqual(len(r1), 1)
        self.assertEqual(len(r2), 1)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM recruiters")
        self.assertEqual(c.fetchone()["cnt"], 1)
        conn.close()

    def test_two_applications_both_get_outreach(self):
        app1, _ = db_module.add_application("Google", "https://g.com/1", "SWE")
        app2, _ = db_module.add_application("Google", "https://g.com/2", "SRE")
        rid = db_module.add_recruiter("Google", "John", "Recruiter", "john@g.com", "auto")
        db_module.link_recruiter_to_application(app1, rid)
        db_module.link_recruiter_to_application(app2, rid)

        today = datetime.now().strftime("%Y-%m-%d")
        db_module.schedule_outreach(rid, app1, "initial", today)
        db_module.schedule_outreach(rid, app2, "initial", today)

        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 2)


class TestOutreachSequence(unittest.TestCase):

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
        self.app_id, _ = db_module.add_application("Acme", "https://acme.com/1", "SWE")
        self.rid = db_module.add_recruiter("Acme", "Jane", "Recruiter", "jane@acme.com", "auto")
        db_module.link_recruiter_to_application(self.app_id, self.rid)

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

    def test_full_3_stage_sequence(self):
        today = datetime.now().strftime("%Y-%m-%d")
        oid1 = db_module.schedule_outreach(self.rid, self.app_id, "initial", today)
        db_module.mark_outreach_sent(oid1)

        oid2 = db_module.schedule_next_outreach(self.rid, self.app_id)
        db_module.mark_outreach_sent(oid2)

        oid3 = db_module.schedule_next_outreach(self.rid, self.app_id)
        db_module.mark_outreach_sent(oid3)

        result = db_module.schedule_next_outreach(self.rid, self.app_id)
        self.assertIsNone(result)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT stage, status FROM outreach ORDER BY id")
        rows = c.fetchall()
        conn.close()

        self.assertEqual([r["stage"] for r in rows], ["initial", "followup1", "followup2"])
        self.assertTrue(all(r["status"] == "sent" for r in rows))

    def test_reply_stops_sequence(self):
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(self.rid, self.app_id, "initial", today)
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("UPDATE outreach SET replied = 1 WHERE id = ?", (oid,))
        conn.commit()
        conn.close()

        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 0)

    def test_recruiter_inactive_stops_sequence(self):
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(self.rid, self.app_id, "initial", today)
        db_module.mark_recruiter_inactive(self.rid, "left company")

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        self.assertEqual(c.fetchone()["status"], "cancelled")
        conn.close()

    def test_bounce_stops_sequence_and_inactivates_recruiter(self):
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(self.rid, self.app_id, "initial", today)
        db_module.mark_outreach_bounced(oid, self.rid)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        self.assertEqual(c.fetchone()["status"], "bounced")
        c.execute("SELECT recruiter_status FROM recruiters WHERE id = ?", (self.rid,))
        self.assertEqual(c.fetchone()["recruiter_status"], "inactive")
        conn.close()


class TestLeftoverQuotaIntegration(unittest.TestCase):

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

    def test_companies_needing_more_prioritizes_shortage(self):
        # Google has 1 recruiter (shortage 2), Meta has 0 (shortage 3)
        app1, _ = db_module.add_application("Google", "https://g.com/1", "SWE", "2026-01-01")
        app2, _ = db_module.add_application("Meta", "https://m.com/1", "SWE", "2026-01-01")
        rid = db_module.add_recruiter("Google", "John", "Recruiter", "j@g.com", "auto")
        db_module.link_recruiter_to_application(app1, rid)

        result = db_module.get_companies_needing_more_recruiters()
        companies = [r["company"] for r in result]
        self.assertEqual(companies[0], "Meta")

    def test_companies_needing_more_excludes_inactive_recruiters(self):
        app1, _ = db_module.add_application("Google", "https://g.com/1", "SWE")
        for i in range(3):
            rid = db_module.add_recruiter("Google", f"P{i}", "R", f"p{i}@g.com", "auto")
            db_module.link_recruiter_to_application(app1, rid)
            if i == 2:
                db_module.mark_recruiter_inactive(rid, "test")

        # Google now has 2 active, 1 inactive → still needs more
        result = db_module.get_companies_needing_more_recruiters()
        companies = [r["company"] for r in result]
        self.assertIn("Google", companies)

    def test_full_company_excluded_from_leftover(self):
        app1, _ = db_module.add_application("Google", "https://g.com/1", "SWE")
        for i in range(3):
            rid = db_module.add_recruiter("Google", f"P{i}", "R", f"p{i}@g.com", "auto")
            db_module.link_recruiter_to_application(app1, rid)

        result = db_module.get_companies_needing_more_recruiters()
        companies = [r["company"] for r in result]
        self.assertNotIn("Google", companies)


class TestAICacheIntegration(unittest.TestCase):

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

    @patch("outreach.ai_full_personalizer._get_client")
    def test_ai_cache_reused_no_duplicate_api_calls(self, mock_get_client):
        from outreach.ai_full_personalizer import generate_all_content, _cache_key
        import hashlib

        job_text = "Build scalable systems. " + "A" * 300
        key = _cache_key("Apple", "SWE", job_text)
        db_module.save_ai_cache(key, "Apple", "SWE", {
            "subject_initial": "SWE at Apple",
            "subject_followup1": "Following up",
            "subject_followup2": "Final",
            "intro": "I am interested.",
            "followup1": "Following up.",
            "followup2": "Final.",
        })

        result = generate_all_content("Apple", "SWE", job_text)
        mock_get_client.assert_not_called()
        self.assertEqual(result["subject_initial"], "SWE at Apple")

    def test_applications_missing_cache_detected(self):
        db_module.add_application("Google", "https://g.com/1", "SWE")
        db_module.add_application("Meta", "https://m.com/1", "SWE")
        import hashlib
        key = hashlib.sha256("Google-SWE-".encode()).hexdigest()
        db_module.save_ai_cache(key, "Google", "SWE", {
            "subject_initial": "X", "subject_followup1": "X", "subject_followup2": "X",
            "intro": "X", "followup1": "X", "followup2": "X",
        })
        missing = db_module.get_applications_missing_ai_cache()
        companies = [r["company"] for r in missing]
        self.assertIn("Meta", companies)
        self.assertNotIn("Google", companies)


class TestQuotaHealthIntegration(unittest.TestCase):

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

    def _insert_quota(self, days_ago, used, remaining=None):
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        remaining = remaining if remaining is not None else 50 - used
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO careershift_quota (date, total_limit, used, remaining)
            VALUES (?, 50, ?, ?)
        """, (date, used, remaining))
        conn.commit()
        conn.close()

    def test_underutilized_alert_triggers_and_saves(self):
        self._insert_quota(0, 5)
        self._insert_quota(1, 5)
        self._insert_quota(2, 5)
        alerts = db_module.check_quota_health()
        cs_alerts = [a for a in alerts if a["quota_type"] == "careershift"]
        self.assertEqual(len(cs_alerts), 1)
        # Save and verify no duplicate
        for alert in alerts:
            db_module.save_quota_alert(alert)
        alerts2 = db_module.check_quota_health()
        cs_alerts2 = [a for a in alerts2 if a["quota_type"] == "careershift"]
        self.assertEqual(len(cs_alerts2), 0)

    def test_normal_usage_no_alert(self):
        self._insert_quota(0, 25)
        self._insert_quota(1, 30)
        self._insert_quota(2, 28)
        alerts = db_module.check_quota_health()
        cs_alerts = [a for a in alerts if a["quota_type"] == "careershift"]
        self.assertEqual(len(cs_alerts), 0)

    def test_suggested_cap_underutilized_is_reasonable(self):
        from config import MAX_CONTACTS_HARD_CAP
        self._insert_quota(0, 5)
        self._insert_quota(1, 5)
        self._insert_quota(2, 5)
        alerts = db_module.check_quota_health()
        cs_alerts = [a for a in alerts if a["quota_type"] == "careershift"]
        self.assertTrue(cs_alerts[0]["suggested_cap"] > MAX_CONTACTS_HARD_CAP)
        self.assertLessEqual(cs_alerts[0]["suggested_cap"], 10)


class TestSearchTermPersistence(unittest.TestCase):

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
        db_module.add_recruiter("Google", "John", "Recruiter", "john@g.com", "auto")

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

    def test_search_terms_persist_across_calls(self):
        db_module.mark_search_term_used("Google", "Recruiter")
        db_module.mark_search_term_used("Google", "Talent Acquisition")
        result = db_module.get_used_search_terms("Google")
        self.assertIn("Recruiter", result)
        self.assertIn("Talent Acquisition", result)

    def test_search_terms_not_duplicated(self):
        for _ in range(5):
            db_module.mark_search_term_used("Google", "Recruiter")
        result = db_module.get_used_search_terms("Google")
        self.assertEqual(result.count("Recruiter"), 1)

    def test_search_terms_isolated_per_company(self):
        db_module.add_recruiter("Meta", "Jane", "HR", "jane@meta.com", "auto")
        db_module.mark_search_term_used("Google", "Recruiter")
        result_meta = db_module.get_used_search_terms("Meta")
        self.assertNotIn("Recruiter", result_meta)


class TestDataRetentionIntegration(unittest.TestCase):

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
        self.app_id, _ = db_module.add_application("Acme", "https://acme.com/1", "SWE")
        self.rid = db_module.add_recruiter("Acme", "Jane", "Recruiter", "jane@acme.com", "auto")
        db_module.link_recruiter_to_application(self.app_id, self.rid)

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

    def test_applications_survive_cleanup(self):
        db_module.init_db()
        apps = db_module.get_all_active_applications()
        self.assertEqual(len(apps), 1)

    def test_recruiters_survive_cleanup(self):
        db_module.init_db()
        result = db_module.get_recruiters_by_company("Acme")
        self.assertEqual(len(result), 1)

    def test_application_recruiters_survive_cleanup(self):
        db_module.init_db()
        result = db_module.get_recruiters_for_application(self.app_id)
        self.assertEqual(len(result), 1)

    def test_old_sent_outreach_cleaned_on_init(self):
        old = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d %H:%M:%S")
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO outreach (recruiter_id, application_id, stage, status, sent_at, created_at)
            VALUES (?, ?, 'initial', 'sent', ?, ?)
        """, (self.rid, self.app_id, old, old))
        conn.commit()
        conn.close()
        # Re-initialize triggers cleanup
        db_module.init_db()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM outreach WHERE status = 'sent'")
        self.assertEqual(c.fetchone()["cnt"], 0)
        conn.close()

    def test_recent_sent_outreach_survives_cleanup(self):
        today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO outreach (recruiter_id, application_id, stage, status, sent_at, created_at)
            VALUES (?, ?, 'initial', 'sent', ?, ?)
        """, (self.rid, self.app_id, today, today))
        conn.commit()
        conn.close()
        db_module.init_db()
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM outreach WHERE status = 'sent'")
        self.assertEqual(c.fetchone()["cnt"], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)