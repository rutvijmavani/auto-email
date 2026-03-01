"""
tests/test_integration.py — Integration tests for full pipeline

Tests:
    1. Full flow: --add → --find-only → --outreach-only
    2. Two applications at same company share recruiters
    3. Outreach sequence runs correctly over 3 stages
    4. CareerShift quota tracked correctly
    5. AI cache used on second run (no duplicate AI calls)
    6. Expired AI cache triggers regeneration
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TEST_DB = "data/test_pipeline.db"
import db.db as db_module
db_module.DB_FILE = TEST_DB


class TestIntegration(unittest.TestCase):

    def setUp(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    # ─────────────────────────────────────────
    # TEST 1: Full pipeline flow
    # ─────────────────────────────────────────
    def test_full_pipeline_flow(self):
        """Complete flow: add → find → outreach."""

        # --- STEP 1: --add ---
        app_id = db_module.add_application(
            "Google", "https://google.com/jobs/1", "Backend Engineer"
        )
        db_module.save_job(
            "https://google.com/jobs/1",
            "Job Title: Backend Engineer\nDescription: Build scalable systems at Google."
        )
        self.assertIsNotNone(app_id)

        # --- STEP 2: --find-only (mock CareerShift + AI) ---
        rid = db_module.add_recruiter(
            "Google", "John Smith", "Technical Recruiter", "john@google.com", "auto"
        )
        db_module.link_recruiter_to_application(app_id, rid)

        import hashlib
        job_text = db_module.get_job("https://google.com/jobs/1")
        cache_key = hashlib.md5(f"Google-Backend Engineer-{job_text}".encode()).hexdigest()
        db_module.save_ai_cache(cache_key, "Google", "Backend Engineer", {
            "subject_initial": "Backend Engineer at Google",
            "subject_followup1": "Following Up: Google Application",
            "subject_followup2": "Final Follow-Up: Google",
            "intro": "I am writing to express my interest...",
            "followup1": "Following up on my application...",
            "followup2": "Final follow-up...",
        })

        # --- STEP 3: --outreach-only ---
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(rid, app_id, "initial", today)
        self.assertIsNotNone(oid)

        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["name"], "John Smith")
        self.assertEqual(pending[0]["company"], "Google")

        # Simulate send
        db_module.mark_outreach_sent(oid)
        next_oid = db_module.schedule_next_outreach(rid, app_id)
        self.assertIsNotNone(next_oid)

        # Verify followup1 scheduled
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT stage FROM outreach WHERE id = ?", (next_oid,))
        row = c.fetchone()
        conn.close()
        self.assertEqual(row["stage"], "followup1")

        print("[OK] TEST 1 PASSED: Full pipeline flow works end to end")

    # ─────────────────────────────────────────
    # TEST 2: Two applications share recruiters
    # ─────────────────────────────────────────
    def test_two_applications_share_recruiters(self):
        """Two Google applications share same recruiter, both get outreach."""
        app1 = db_module.add_application("Google", "https://google.com/jobs/1", "Backend")
        app2 = db_module.add_application("Google", "https://google.com/jobs/2", "Platform")

        rid = db_module.add_recruiter(
            "Google", "John Smith", "Recruiter", "john@google.com", "auto"
        )
        db_module.link_recruiter_to_application(app1, rid)
        db_module.link_recruiter_to_application(app2, rid)

        today = datetime.now().strftime("%Y-%m-%d")
        db_module.schedule_outreach(rid, app1, "initial", today)
        db_module.schedule_outreach(rid, app2, "initial", today)

        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 2)

        # Only 1 recruiter row
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM recruiters")
        count = c.fetchone()["cnt"]
        conn.close()
        self.assertEqual(count, 1)

        print("[OK] TEST 2 PASSED: Two applications share recruiter correctly")

    # ─────────────────────────────────────────
    # TEST 3: Full 3-stage outreach sequence
    # ─────────────────────────────────────────
    def test_full_outreach_sequence(self):
        """All 3 stages scheduled and sent correctly."""
        app_id = db_module.add_application("Meta", "https://meta.com/jobs/1", "SWE")
        rid = db_module.add_recruiter("Meta", "Jane Doe", "HR", "jane@meta.com", "auto")
        db_module.link_recruiter_to_application(app_id, rid)

        today = datetime.now().strftime("%Y-%m-%d")

        # Stage 1: initial
        oid1 = db_module.schedule_outreach(rid, app_id, "initial", today)
        db_module.mark_outreach_sent(oid1)

        # Stage 2: followup1
        oid2 = db_module.schedule_next_outreach(rid, app_id)
        self.assertIsNotNone(oid2)
        db_module.mark_outreach_sent(oid2)

        # Stage 3: followup2
        oid3 = db_module.schedule_next_outreach(rid, app_id)
        self.assertIsNotNone(oid3)
        db_module.mark_outreach_sent(oid3)

        # Done — no more stages
        result = db_module.schedule_next_outreach(rid, app_id)
        self.assertIsNone(result)

        # Verify all 3 rows in DB
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT stage, status FROM outreach ORDER BY id")
        rows = c.fetchall()
        conn.close()

        stages = [r["stage"] for r in rows]
        self.assertEqual(stages, ["initial", "followup1", "followup2"])
        self.assertTrue(all(r["status"] == "sent" for r in rows))
        print("[OK] TEST 3 PASSED: Full 3-stage outreach sequence works")

    # ─────────────────────────────────────────
    # TEST 4: CareerShift quota tracked
    # ─────────────────────────────────────────
    def test_careershift_quota_tracking(self):
        """Quota decrements correctly with each profile visit."""
        initial = db_module.get_remaining_quota()
        self.assertEqual(initial, 50)

        db_module.increment_quota_used(1)
        self.assertEqual(db_module.get_remaining_quota(), 49)

        db_module.increment_quota_used(5)
        self.assertEqual(db_module.get_remaining_quota(), 44)
        print("[OK] TEST 4 PASSED: CareerShift quota tracked correctly")

    # ─────────────────────────────────────────
    # TEST 5: AI cache reused on second run
    # ─────────────────────────────────────────
    @patch("outreach.ai_full_personalizer.client")
    def test_ai_cache_reused(self, mock_client):
        """AI API not called when cache is warm."""
        from outreach.ai_full_personalizer import generate_all_content
        import hashlib

        job_text = "Job Title: SWE\nDescription: Build things."
        cache_key = hashlib.md5(f"Apple-SWE-{job_text}".encode()).hexdigest()

        # Pre-populate cache
        db_module.save_ai_cache(cache_key, "Apple", "SWE", {
            "subject_initial": "SWE at Apple",
            "subject_followup1": "Following up",
            "subject_followup2": "Final follow-up",
            "intro": "I am interested...",
            "followup1": "Following up...",
            "followup2": "Final...",
        })

        result = generate_all_content("Apple", "SWE", job_text)

        # AI API should NOT have been called
        mock_client.models.generate_content.assert_not_called()
        self.assertEqual(result["subject_initial"], "SWE at Apple")
        print("[OK] TEST 5 PASSED: AI cache reused correctly, no duplicate API calls")

    # ─────────────────────────────────────────
    # TEST 6: Expired AI cache triggers regen
    # ─────────────────────────────────────────
    def test_expired_ai_cache_returns_none(self):
        """Expired cache entry returns None from get_ai_cache."""
        import hashlib
        from datetime import timedelta

        cache_key = hashlib.md5("Test-key".encode()).hexdigest()

        # Insert with already-expired date
        conn = db_module.get_conn()
        c = conn.cursor()
        expired = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""
            INSERT INTO ai_cache (
                cache_key, company, job_title,
                subject_initial, subject_followup1, subject_followup2,
                intro, followup1, followup2, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (cache_key, "Test", "Test Role",
              "s1", "s2", "s3", "i", "f1", "f2", expired))
        conn.commit()
        conn.close()

        result = db_module.get_ai_cache(cache_key)
        self.assertIsNone(result)
        print("[OK] TEST 6 PASSED: Expired AI cache returns None correctly")


if __name__ == "__main__":
    unittest.main(verbosity=2)