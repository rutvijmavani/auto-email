"""
tests/test_find_only.py — Unit tests for pipeline.py --find-only

Tests:
    1. Recruiter added at company level (not application level)
    2. Recruiter linked to application via join table
    3. Same recruiter linked to multiple applications (no duplication)
    4. Existing recruiter reused when email matches
    5. get_unique_companies_needing_scraping returns correct companies
    6. AI cache populated after find-only
    7. Quota distribution calculated correctly
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TEST_DB = "data/test_pipeline.db"
import db.db as db_module


class TestFindOnly(unittest.TestCase):

    def setUp(self):
        db_module.DB_FILE = TEST_DB
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        db_module.init_db()

        # Add test applications
        self.app1_id, _ = db_module.add_application("Google", "https://google.com/jobs/1", "Backend Engineer")
        self.app2_id, _ = db_module.add_application("Google", "https://google.com/jobs/2", "Platform Engineer")
        self.app3_id, _ = db_module.add_application("Meta", "https://meta.com/jobs/1", "SWE")

    def tearDown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    # ─────────────────────────────────────────
    # TEST 1: Recruiter added at company level
    # ─────────────────────────────────────────
    def test_recruiter_added_at_company_level(self):
        """Recruiter row has no application_id — company level only."""
        rid = db_module.add_recruiter(
            company="Google",
            name="John Smith",
            position="Technical Recruiter",
            email="john@google.com",
            confidence="auto",
        )

        self.assertIsNotNone(rid)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT * FROM recruiters WHERE id = ?", (rid,))
        row = c.fetchone()
        conn.close()

        self.assertEqual(row["company"], "Google")
        self.assertEqual(row["email"], "john@google.com")
        self.assertNotIn("application_id", dict(row))
        print("[OK] TEST 1 PASSED: Recruiter added at company level")

    # ─────────────────────────────────────────
    # TEST 2: Recruiter linked to application
    # ─────────────────────────────────────────
    def test_recruiter_linked_to_application(self):
        """Recruiter correctly linked to application via join table."""
        rid = db_module.add_recruiter("Google", "John Smith", "Recruiter", "john@google.com", "auto")
        db_module.link_recruiter_to_application(self.app1_id, rid)

        recruiters = db_module.get_recruiters_for_application(self.app1_id)
        self.assertEqual(len(recruiters), 1)
        self.assertEqual(recruiters[0]["email"], "john@google.com")
        print("[OK] TEST 2 PASSED: Recruiter linked to application correctly")

    # ─────────────────────────────────────────
    # TEST 3: Same recruiter linked to multiple applications
    # ─────────────────────────────────────────
    def test_same_recruiter_multiple_applications(self):
        """Same recruiter linked to 2 Google applications — no duplication in recruiters table."""
        rid = db_module.add_recruiter("Google", "John Smith", "Recruiter", "john@google.com", "auto")

        db_module.link_recruiter_to_application(self.app1_id, rid)
        db_module.link_recruiter_to_application(self.app2_id, rid)

        # Only 1 row in recruiters table
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM recruiters WHERE email = 'john@google.com'")
        count = c.fetchone()["cnt"]
        conn.close()
        self.assertEqual(count, 1)

        # But linked to both applications
        r1 = db_module.get_recruiters_for_application(self.app1_id)
        r2 = db_module.get_recruiters_for_application(self.app2_id)
        self.assertEqual(len(r1), 1)
        self.assertEqual(len(r2), 1)
        print("[OK] TEST 3 PASSED: Same recruiter linked to multiple applications without duplication")

    # ─────────────────────────────────────────
    # TEST 4: Existing recruiter reused by email
    # ─────────────────────────────────────────
    def test_existing_recruiter_reused(self):
        """Adding recruiter with existing email returns existing id."""
        rid1 = db_module.add_recruiter("Google", "John Smith", "Recruiter", "john@google.com", "auto")
        rid2 = db_module.add_recruiter("Google", "John Smith", "Recruiter", "john@google.com", "auto")

        self.assertEqual(rid1, rid2)
        print("[OK] TEST 4 PASSED: Existing recruiter reused correctly")

    # ─────────────────────────────────────────
    # TEST 5: Companies needing scraping
    # ─────────────────────────────────────────
    def test_companies_needing_scraping(self):
        """Returns companies with fewer than min_recruiters."""
        # Google has 0 recruiters → needs scraping
        # Meta has 0 recruiters → needs scraping
        companies = db_module.get_unique_companies_needing_scraping(min_recruiters=2)
        self.assertIn("Google", companies)
        self.assertIn("Meta", companies)

        # Add 2 recruiters to Google and link to BOTH Google applications
        rid1 = db_module.add_recruiter("Google", "John", "Recruiter", "john@google.com", "auto")
        rid2 = db_module.add_recruiter("Google", "Jane", "HR Manager", "jane@google.com", "auto")

        # Link to both Google applications
        for app_id in [self.app1_id, self.app2_id]:
            db_module.link_recruiter_to_application(app_id, rid1)
            db_module.link_recruiter_to_application(app_id, rid2)

        # Now only Meta needs scraping
        companies = db_module.get_unique_companies_needing_scraping(min_recruiters=2)
        self.assertNotIn("Google", companies)
        self.assertIn("Meta", companies)
        print("[OK] TEST 5 PASSED: Companies needing scraping identified correctly")

    # ─────────────────────────────────────────
    # TEST 6: Quota distribution
    # ─────────────────────────────────────────
    def test_quota_distribution(self):
        """Quota distributed correctly across companies."""
        from careershift.find_emails import calculate_distribution

        # 50 quota / 20 companies → base=2, extra=10
        # first 10 get 3, last 10 get 2 → total = 50
        counts = calculate_distribution(50, 20)
        self.assertEqual(len(counts), 20)
        self.assertEqual(sum(counts), 50)
        self.assertEqual(counts[:10], [3] * 10)
        self.assertEqual(counts[10:], [2] * 10)

        # 50 quota / 5 companies → base=10, capped at 3
        counts = calculate_distribution(50, 5)
        self.assertEqual(len(counts), 5)
        self.assertTrue(all(c <= 3 for c in counts))

        # 10 quota / 20 companies → base=0, extra=10
        # first 10 get 1, last 10 get 0 → total = 10
        counts = calculate_distribution(10, 20)
        self.assertEqual(len(counts), 20)
        self.assertEqual(sum(counts), 10)
        self.assertEqual(counts[:10], [1] * 10)
        self.assertEqual(counts[10:], [0] * 10)
        print("[OK] TEST 6 PASSED: Quota distribution calculated correctly")

    # ─────────────────────────────────────────
    # TEST 7: AI cache saved and retrieved
    # ─────────────────────────────────────────
    def test_ai_cache_roundtrip(self):
        """AI content saved and retrieved from SQLite cache."""
        import hashlib
        cache_key = hashlib.sha256("Google-Backend Engineer-job text".encode()).hexdigest()

        data = {
            "subject_initial": "Backend Engineer at Google",
            "subject_followup1": "Following up",
            "subject_followup2": "Final follow-up",
            "intro": "I am writing to express interest...",
            "followup1": "Following up on my application...",
            "followup2": "Final follow-up regarding...",
        }

        db_module.save_ai_cache(cache_key, "Google", "Backend Engineer", data)
        retrieved = db_module.get_ai_cache(cache_key)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved["subject_initial"], data["subject_initial"])
        self.assertEqual(retrieved["intro"], data["intro"])
        print("[OK] TEST 7 PASSED: AI cache roundtrip works correctly")


if __name__ == "__main__":
    unittest.main(verbosity=2)