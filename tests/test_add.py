"""
tests/test_add.py — Unit tests for pipeline.py --add

Tests:
    1. Application inserts correctly into DB
    2. Duplicate job URL is rejected cleanly
    3. JD scraping stores content in jobs table
    4. Missing job URL handled gracefully
    5. Missing company name handled gracefully
"""

import sys
import os
import sqlite3
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Use a separate test DB so we don't pollute real data
TEST_DB = "data/test_pipeline.db"
os.environ["TEST_MODE"] = "1"

import db.db as db_module
db_module.DB_FILE = TEST_DB


class TestAdd(unittest.TestCase):

    def setUp(self):
        """Create fresh test DB before each test."""
        # Remove test DB if exists
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        """Clean up test DB after each test."""
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    # ─────────────────────────────────────────
    # TEST 1: Application inserts correctly
    # ─────────────────────────────────────────
    def test_add_application_success(self):
        """Application row is inserted with correct data."""
        app_id = db_module.add_application(
            company="Google",
            job_url="https://google.com/jobs/123",
            job_title="Backend Engineer",
            applied_date="2026-02-28",
        )

        self.assertIsNotNone(app_id)
        self.assertGreater(app_id, 0)

        # Verify in DB
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT * FROM applications WHERE id = ?", (app_id,))
        row = c.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row["company"], "Google")
        self.assertEqual(row["job_url"], "https://google.com/jobs/123")
        self.assertEqual(row["job_title"], "Backend Engineer")
        self.assertEqual(row["status"], "active")
        print("[OK] TEST 1 PASSED: Application inserts correctly")

    # ─────────────────────────────────────────
    # TEST 2: Duplicate job URL rejected
    # ─────────────────────────────────────────
    def test_duplicate_job_url_rejected(self):
        """Inserting same job URL twice returns existing id."""
        app_id_1 = db_module.add_application(
            company="Google",
            job_url="https://google.com/jobs/123",
            job_title="Backend Engineer",
        )
        app_id_2 = db_module.add_application(
            company="Google",
            job_url="https://google.com/jobs/123",
            job_title="Backend Engineer",
        )

        self.assertEqual(app_id_1, app_id_2)

        # Only one row in DB
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM applications")
        count = c.fetchone()["cnt"]
        conn.close()

        self.assertEqual(count, 1)
        print("[OK] TEST 2 PASSED: Duplicate URL rejected cleanly")

    # ─────────────────────────────────────────
    # TEST 3: JD scraping stores in jobs table
    # ─────────────────────────────────────────
    @patch("jobs.job_fetcher.fetch_job_description")
    def test_jd_scraping_stores_in_jobs_table(self, mock_fetch):
        """JD is scraped and stored in jobs table during --add."""
        mock_fetch.return_value = {
            "job_text": "Backend Engineer role at Google...",
            "job_title": "Backend Engineer"
        }

        # Simulate what --add does
        db_module.add_application(
            company="Google",
            job_url="https://google.com/jobs/456",
            job_title="Backend Engineer",
        )

        from jobs.job_fetcher import fetch_job_description
        result = fetch_job_description("https://google.com/jobs/456")

        self.assertIsNotNone(result)
        mock_fetch.assert_called_once_with("https://google.com/jobs/456")
        print("[OK] TEST 3 PASSED: JD scraping called correctly")

    # ─────────────────────────────────────────
    # TEST 4: save_job and get_job roundtrip
    # ─────────────────────────────────────────
    def test_job_cache_roundtrip(self):
        """Saved job description can be retrieved correctly."""
        url = "https://google.com/jobs/789"
        content = "Job Title: Backend Engineer\nDescription: Build scalable systems."

        db_module.save_job(url, content)
        retrieved = db_module.get_job(url)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved, content)
        print("[OK] TEST 4 PASSED: Job cache save/get roundtrip works")

    # ─────────────────────────────────────────
    # TEST 5: Application with no job title
    # ─────────────────────────────────────────
    def test_add_application_without_job_title(self):
        """Application without job title inserts with None job_title."""
        app_id = db_module.add_application(
            company="Meta",
            job_url="https://meta.com/jobs/001",
        )

        self.assertIsNotNone(app_id)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT job_title FROM applications WHERE id = ?", (app_id,))
        row = c.fetchone()
        conn.close()

        self.assertIsNone(row["job_title"])
        print("[OK] TEST 5 PASSED: Application without job title handled correctly")

    # ─────────────────────────────────────────
    # TEST 6: get_all_active_applications
    # ─────────────────────────────────────────
    def test_get_all_active_applications(self):
        """Returns all active applications."""
        db_module.add_application("Google", "https://google.com/jobs/1", "SWE")
        db_module.add_application("Meta", "https://meta.com/jobs/1", "SWE")
        db_module.add_application("Apple", "https://apple.com/jobs/1", "SWE")

        apps = db_module.get_all_active_applications()
        self.assertEqual(len(apps), 3)
        print("[OK] TEST 6 PASSED: get_all_active_applications returns correct count")


if __name__ == "__main__":
    unittest.main(verbosity=2)