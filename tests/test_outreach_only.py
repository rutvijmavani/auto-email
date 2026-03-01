"""
tests/test_outreach_only.py — Unit tests for pipeline.py --outreach-only

Tests:
    1. Initial outreach scheduled for today
    2. No duplicate outreach scheduled for same recruiter+application
    3. schedule_next_outreach correctly schedules followup1 after initial
    4. schedule_next_outreach correctly schedules followup2 after followup1
    5. sequence stops after followup2
    6. mark_outreach_sent updates status and sent_at
    7. mark_outreach_failed updates status
    8. get_pending_outreach returns only due emails
    9. Send window status returns correct state
    10. Replied outreach not included in pending
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TEST_DB = "data/test_pipeline.db"
import db.db as db_module


class TestOutreachOnly(unittest.TestCase):

    def setUp(self):
        db_module.DB_FILE = TEST_DB
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        db_module.init_db()

        # Setup: application + recruiter + link
        self.app_id, _ = db_module.add_application(
            "Google", "https://google.com/jobs/1", "Backend Engineer"
        )
        self.recruiter_id = db_module.add_recruiter(
            "Google", "John Smith", "Technical Recruiter", "john@google.com", "auto"
        )
        db_module.link_recruiter_to_application(self.app_id, self.recruiter_id)

    def tearDown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    # ─────────────────────────────────────────
    # TEST 1: Initial outreach scheduled for today
    # ─────────────────────────────────────────
    def test_initial_outreach_scheduled_today(self):
        """Initial outreach row created with today's date."""
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(
            self.recruiter_id, self.app_id, "initial", today
        )

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT * FROM outreach WHERE id = ?", (oid,))
        row = c.fetchone()
        conn.close()

        self.assertEqual(row["stage"], "initial")
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["scheduled_for"], today)
        self.assertEqual(row["replied"], 0)
        print("[OK] TEST 1 PASSED: Initial outreach scheduled for today")

    # ─────────────────────────────────────────
    # TEST 2: No duplicate outreach
    # ─────────────────────────────────────────
    def test_no_duplicate_outreach(self):
        """has_pending_or_sent_outreach prevents duplicate scheduling."""
        today = datetime.now().strftime("%Y-%m-%d")
        db_module.schedule_outreach(self.recruiter_id, self.app_id, "initial", today)

        has_outreach = db_module.has_pending_or_sent_outreach(
            self.recruiter_id, self.app_id
        )
        self.assertTrue(has_outreach)
        print("[OK] TEST 2 PASSED: Duplicate outreach prevention works")

    # ─────────────────────────────────────────
    # TEST 3: schedule_next_outreach after initial
    # ─────────────────────────────────────────
    def test_schedule_next_after_initial(self):
        """followup1 scheduled correctly after initial is sent."""
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(
            self.recruiter_id, self.app_id, "initial", today
        )
        db_module.mark_outreach_sent(oid)

        next_oid = db_module.schedule_next_outreach(self.recruiter_id, self.app_id)
        self.assertIsNotNone(next_oid)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT stage, scheduled_for FROM outreach WHERE id = ?", (next_oid,))
        row = c.fetchone()
        conn.close()

        self.assertEqual(row["stage"], "followup1")

        expected_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        self.assertEqual(row["scheduled_for"], expected_date)
        print("[OK] TEST 3 PASSED: followup1 scheduled after initial sent")

    # ─────────────────────────────────────────
    # TEST 4: schedule_next_outreach after followup1
    # ─────────────────────────────────────────
    def test_schedule_next_after_followup1(self):
        """followup2 scheduled correctly after followup1 is sent."""
        today = datetime.now().strftime("%Y-%m-%d")

        oid1 = db_module.schedule_outreach(self.recruiter_id, self.app_id, "initial", today)
        db_module.mark_outreach_sent(oid1)

        oid2 = db_module.schedule_next_outreach(self.recruiter_id, self.app_id)
        db_module.mark_outreach_sent(oid2)

        oid3 = db_module.schedule_next_outreach(self.recruiter_id, self.app_id)
        self.assertIsNotNone(oid3)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT stage FROM outreach WHERE id = ?", (oid3,))
        row = c.fetchone()
        conn.close()

        self.assertEqual(row["stage"], "followup2")
        print("[OK] TEST 4 PASSED: followup2 scheduled after followup1 sent")

    # ─────────────────────────────────────────
    # TEST 5: Sequence stops after followup2
    # ─────────────────────────────────────────
    def test_sequence_stops_after_followup2(self):
        """schedule_next_outreach returns None after followup2 is sent."""
        today = datetime.now().strftime("%Y-%m-%d")

        oid1 = db_module.schedule_outreach(self.recruiter_id, self.app_id, "initial", today)
        db_module.mark_outreach_sent(oid1)

        oid2 = db_module.schedule_next_outreach(self.recruiter_id, self.app_id)
        db_module.mark_outreach_sent(oid2)

        oid3 = db_module.schedule_next_outreach(self.recruiter_id, self.app_id)
        db_module.mark_outreach_sent(oid3)

        result = db_module.schedule_next_outreach(self.recruiter_id, self.app_id)
        self.assertIsNone(result)
        print("[OK] TEST 5 PASSED: Sequence stops after followup2")

    # ─────────────────────────────────────────
    # TEST 6: mark_outreach_sent
    # ─────────────────────────────────────────
    def test_mark_outreach_sent(self):
        """mark_outreach_sent updates status to sent and sets sent_at."""
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(self.recruiter_id, self.app_id, "initial", today)
        db_module.mark_outreach_sent(oid)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status, sent_at FROM outreach WHERE id = ?", (oid,))
        row = c.fetchone()
        conn.close()

        self.assertEqual(row["status"], "sent")
        self.assertIsNotNone(row["sent_at"])
        print("[OK] TEST 6 PASSED: mark_outreach_sent works correctly")

    # ─────────────────────────────────────────
    # TEST 7: mark_outreach_failed
    # ─────────────────────────────────────────
    def test_mark_outreach_failed(self):
        """mark_outreach_failed updates status to failed."""
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(self.recruiter_id, self.app_id, "initial", today)
        db_module.mark_outreach_failed(oid)

        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("SELECT status FROM outreach WHERE id = ?", (oid,))
        row = c.fetchone()
        conn.close()

        self.assertEqual(row["status"], "failed")
        print("[OK] TEST 7 PASSED: mark_outreach_failed works correctly")

    # ─────────────────────────────────────────
    # TEST 8: get_pending_outreach due dates
    # ─────────────────────────────────────────
    def test_get_pending_outreach_due_today(self):
        """Only emails due today or earlier returned as pending."""
        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

        # Schedule one for today, one for future
        db_module.schedule_outreach(self.recruiter_id, self.app_id, "initial", today)

        # Add second recruiter for future email
        rid2 = db_module.add_recruiter("Google", "Jane Doe", "HR", "jane@google.com", "auto")
        db_module.link_recruiter_to_application(self.app_id, rid2)
        db_module.schedule_outreach(rid2, self.app_id, "initial", future)

        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["stage"], "initial")
        print("[OK] TEST 8 PASSED: get_pending_outreach returns only due emails")

    # ─────────────────────────────────────────
    # TEST 9: Send window status
    # ─────────────────────────────────────────
    @patch("outreach.outreach_engine._now")
    def test_send_window_status(self, mock_now):
        """get_send_status returns correct state based on time."""
        from outreach.outreach_engine import get_send_status
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("America/New_York")

        # Before window — 8 AM
        mock_now.return_value = datetime.now(tz).replace(hour=8, minute=0)
        self.assertEqual(get_send_status(), "wait")

        # In window — 10 AM
        mock_now.return_value = datetime.now(tz).replace(hour=10, minute=0)
        self.assertEqual(get_send_status(), "send")

        # In grace period — 11:30 AM
        mock_now.return_value = datetime.now(tz).replace(hour=11, minute=30)
        self.assertEqual(get_send_status(), "send")

        # Past cutoff — 12:01 PM
        mock_now.return_value = datetime.now(tz).replace(hour=12, minute=1)
        self.assertEqual(get_send_status(), "cutoff")
        print("[OK] TEST 9 PASSED: Send window status returns correct states")

    # ─────────────────────────────────────────
    # TEST 10: Replied outreach not in pending
    # ─────────────────────────────────────────
    def test_replied_outreach_excluded(self):
        """Emails with replied=1 not returned in get_pending_outreach."""
        today = datetime.now().strftime("%Y-%m-%d")
        oid = db_module.schedule_outreach(self.recruiter_id, self.app_id, "initial", today)

        # Mark as replied
        conn = db_module.get_conn()
        c = conn.cursor()
        c.execute("UPDATE outreach SET replied = 1 WHERE id = ?", (oid,))
        conn.commit()
        conn.close()

        pending = db_module.get_pending_outreach()
        self.assertEqual(len(pending), 0)
        print("[OK] TEST 10 PASSED: Replied outreach excluded from pending")


if __name__ == "__main__":
    unittest.main(verbosity=2)