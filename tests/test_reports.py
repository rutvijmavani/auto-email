"""
tests/test_reports.py — Tests for HTML email report templates

Covers:

  TestBaseComponents
    - stat_card renders value and label
    - stat_row wraps cards in table
    - section_header renders title
    - badge renders text and color
    - table_row renders cells
    - table_header_row renders headers
    - alert_box renders message
    - info_box uses accent color

  TestOutreachReport
    - Subject includes sent count
    - Subject includes bounced warning when bounced > 0
    - Subject includes failed warning when failed > 0
    - HTML contains recruiter name
    - HTML contains company name
    - HTML contains stage badge
    - HTML contains sequence stats
    - Empty emails list shows info box
    - Report calls send_report_email

  TestFindReport
    - Subject includes found count and quota
    - Subject includes exhausted warning
    - Quota bar renders percentage
    - HTML contains company name
    - HTML contains found/exhausted/skipped badges
    - Prospective section shown when scraped > 0
    - Prospective section hidden when both zero
    - AI stats rendered correctly
    - Report calls send_report_email

  TestVerifyReport
    - Subject includes checked count
    - Subject includes inactive warning
    - Subject includes under-stocked warning
    - Tier breakdown table rendered
    - Changes table shown when changes exist
    - Changes table hidden when empty
    - Under-stocked section shown when companies present
    - Under-stocked section hidden when empty
    - Alert shown for inactive recruiters
    - Alert shown for under-stocked companies
    - Report calls send_report_email

  TestSendReportEmail
    - Sends to self (EMAIL → EMAIL)
    - Returns True on success
    - Returns False on SMTP failure
    - Skips gracefully when EMAIL not configured
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────
# TEST: Base HTML components
# ─────────────────────────────────────────

class TestBaseComponents(unittest.TestCase):

    def setUp(self):
        from outreach.report_templates.base import (
            stat_card, stat_row, section_header, badge,
            table_row, table_header_row, alert_box, info_box, COLORS,
        )
        self.stat_card      = stat_card
        self.stat_row       = stat_row
        self.section_header = section_header
        self.badge          = badge
        self.table_row      = table_row
        self.table_header_row = table_header_row
        self.alert_box      = alert_box
        self.info_box       = info_box
        self.COLORS         = COLORS

    def test_stat_card_renders_value(self):
        html = self.stat_card(42, "Sent")
        self.assertIn("42", html)

    def test_stat_card_renders_label(self):
        html = self.stat_card(42, "Sent")
        self.assertIn("Sent", html)

    def test_stat_card_uses_custom_color(self):
        html = self.stat_card(5, "Failed", "#ef4444")
        self.assertIn("#ef4444", html)

    def test_stat_row_wraps_cards(self):
        card = self.stat_card(1, "Test")
        html = self.stat_row([card, card])
        self.assertIn("<table", html)
        self.assertIn("<tr>", html)

    def test_section_header_renders_title(self):
        html = self.section_header("Emails Sent")
        self.assertIn("Emails Sent", html)

    def test_badge_renders_text(self):
        html = self.badge("initial", "#3b82f6")
        self.assertIn("initial", html)

    def test_badge_renders_color(self):
        html = self.badge("initial", "#3b82f6")
        self.assertIn("#3b82f6", html)

    def test_table_row_renders_cells(self):
        html = self.table_row(["John", "Google", "Sent"])
        self.assertIn("John", html)
        self.assertIn("Google", html)
        self.assertIn("Sent", html)

    def test_table_header_row_renders_headers(self):
        html = self.table_header_row(["Name", "Company", "Status"])
        self.assertIn("Name", html)
        self.assertIn("Company", html)
        self.assertIn("Status", html)

    def test_alert_box_renders_message(self):
        html = self.alert_box("Something went wrong")
        self.assertIn("Something went wrong", html)

    def test_info_box_uses_accent_color(self):
        html = self.info_box("No emails today")
        self.assertIn("No emails today", html)
        self.assertIn(self.COLORS["accent"], html)


# ─────────────────────────────────────────
# TEST: Outreach report
# ─────────────────────────────────────────

class TestOutreachReport(unittest.TestCase):

    def _stats(self, **kwargs):
        base = {
            "date": "March 4, 2026",
            "sent": 3,
            "failed": 0,
            "bounced": 0,
            "skipped": 0,
            "emails": [
                {"name": "John Cruz", "company": "Collective",
                 "stage": "initial", "status": "sent"},
                {"name": "Jane Smith", "company": "Stripe",
                 "stage": "followup1", "status": "sent"},
            ],
            "active_sequences": 12,
            "completed_sequences": 3,
            "pending_reply": 9,
        }
        base.update(kwargs)
        return base

    def _build(self, stats):
        from outreach.report_templates.outreach_report import build_outreach_report
        with patch("outreach.report_templates.outreach_report.send_report_email",
                   return_value=True) as mock_send:
            build_outreach_report(stats)
            return mock_send.call_args

    def test_subject_includes_sent_count(self):
        call = self._build(self._stats(sent=3))
        subject = call[0][0]
        self.assertIn("3", subject)
        self.assertIn("Sent", subject)

    def test_subject_includes_bounced_warning(self):
        call = self._build(self._stats(bounced=1))
        subject = call[0][0]
        self.assertIn("bounced", subject.lower())

    def test_subject_includes_failed_warning(self):
        call = self._build(self._stats(failed=2))
        subject = call[0][0]
        self.assertIn("failed", subject.lower())

    def test_subject_clean_when_no_issues(self):
        call = self._build(self._stats(sent=3, failed=0, bounced=0))
        subject = call[0][0]
        self.assertNotIn("⚠", subject)

    def test_html_contains_recruiter_name(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("John Cruz", html)

    def test_html_contains_company_name(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("Collective", html)

    def test_html_contains_stage(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("initial", html)

    def test_html_contains_sequence_stats(self):
        call = self._build(self._stats(
            active_sequences=12, completed_sequences=3, pending_reply=9
        ))
        html = call[0][1]
        self.assertIn("12", html)
        self.assertIn("3", html)
        self.assertIn("9", html)

    def test_empty_emails_shows_info_box(self):
        call = self._build(self._stats(emails=[]))
        html = call[0][1]
        self.assertIn("No emails sent", html)

    def test_report_calls_send_report_email(self):
        from outreach.report_templates.outreach_report import build_outreach_report
        with patch("outreach.report_templates.outreach_report.send_report_email",
                   return_value=True) as mock_send:
            build_outreach_report(self._stats())
            mock_send.assert_called_once()

    def test_bounced_alert_shown(self):
        call = self._build(self._stats(bounced=2))
        html = call[0][1]
        self.assertIn("bounce", html.lower())

    def test_failed_alert_shown(self):
        call = self._build(self._stats(failed=1))
        html = call[0][1]
        self.assertIn("SMTP", html)


# ─────────────────────────────────────────
# TEST: Find report
# ─────────────────────────────────────────

class TestFindReport(unittest.TestCase):

    def _stats(self, **kwargs):
        base = {
            "date": "March 4, 2026",
            "quota_used": 12,
            "quota_total": 50,
            "companies": [
                {"name": "Collective", "status": "found", "count": 2},
                {"name": "Stripe",     "status": "found", "count": 3},
                {"name": "Figma",      "status": "exhausted", "count": 0},
                {"name": "Linear",     "status": "skipped", "count": 0},
            ],
            "prospective_scraped": 0,
            "prospective_exhausted": 0,
            "ai_generated": 4,
            "ai_cached": 2,
            "ai_failed": 0,
        }
        base.update(kwargs)
        return base

    def _build(self, stats):
        from outreach.report_templates.find_report import build_find_report
        with patch("outreach.report_templates.find_report.send_report_email",
                   return_value=True) as mock_send:
            build_find_report(stats)
            return mock_send.call_args

    def test_subject_includes_found_count(self):
        call = self._build(self._stats())
        subject = call[0][0]
        self.assertIn("Found", subject)
        self.assertIn("2", subject)  # 2 found companies

    def test_subject_includes_quota(self):
        call = self._build(self._stats(quota_used=12))
        subject = call[0][0]
        self.assertIn("12", subject)
        self.assertIn("50", subject)

    def test_subject_includes_exhausted_warning(self):
        call = self._build(self._stats())
        subject = call[0][0]
        self.assertIn("exhausted", subject.lower())

    def test_subject_clean_when_no_issues(self):
        call = self._build(self._stats(companies=[
            {"name": "Google", "status": "found", "count": 3}
        ], ai_failed=0))
        subject = call[0][0]
        self.assertNotIn("⚠", subject)

    def test_html_contains_company_name(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("Collective", html)
        self.assertIn("Stripe", html)

    def test_html_contains_status_badges(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("Found", html)
        self.assertIn("Exhausted", html)
        self.assertIn("Skipped", html)

    def test_quota_bar_rendered(self):
        call = self._build(self._stats(quota_used=25))
        html = call[0][1]
        self.assertIn("50%", html)  # 25/50 = 50%

    def test_prospective_section_shown(self):
        call = self._build(self._stats(prospective_scraped=3))
        html = call[0][1]
        self.assertIn("Prospective", html)
        self.assertIn("Pre-scraped", html)

    def test_prospective_section_hidden_when_zero(self):
        call = self._build(self._stats(
            prospective_scraped=0, prospective_exhausted=0
        ))
        html = call[0][1]
        self.assertNotIn("Pre-scraped", html)

    def test_ai_stats_rendered(self):
        call = self._build(self._stats(ai_generated=4, ai_cached=2))
        html = call[0][1]
        self.assertIn("4", html)
        self.assertIn("2", html)

    def test_ai_failed_alert(self):
        call = self._build(self._stats(ai_failed=1))
        html = call[0][1]
        self.assertIn("AI generation", html)

    def test_report_calls_send_report_email(self):
        from outreach.report_templates.find_report import build_find_report
        with patch("outreach.report_templates.find_report.send_report_email",
                   return_value=True) as mock_send:
            build_find_report(self._stats())
            mock_send.assert_called_once()


# ─────────────────────────────────────────
# TEST: Verify report
# ─────────────────────────────────────────

class TestVerifyReport(unittest.TestCase):

    def _stats(self, **kwargs):
        base = {
            "date": "March 4, 2026",
            "tier1_count": 8,
            "tier2_count": 4,
            "tier2_verified": 4,
            "tier3_count": 2,
            "tier3_verified": 1,
            "tier3_inactive": 1,
            "changes": [
                {"name": "Bob Lee", "company": "Linear",
                 "action": "marked inactive"},
            ],
            "under_stocked": [
                {"company": "Linear", "active_count": 0, "needed": 1},
            ],
        }
        base.update(kwargs)
        return base

    def _build(self, stats):
        from outreach.report_templates.verify_report import build_verify_report
        with patch("outreach.report_templates.verify_report.send_report_email",
                   return_value=True) as mock_send:
            build_verify_report(stats)
            return mock_send.call_args

    def test_subject_includes_checked_count(self):
        call = self._build(self._stats())
        subject = call[0][0]
        # tier2 + tier3 = 6 checked
        self.assertIn("6", subject)

    def test_subject_includes_inactive_warning(self):
        call = self._build(self._stats(tier3_inactive=1))
        subject = call[0][0]
        self.assertIn("inactive", subject.lower())

    def test_subject_includes_under_stocked_warning(self):
        call = self._build(self._stats())
        subject = call[0][0]
        self.assertIn("under-stocked", subject.lower())

    def test_subject_clean_when_no_issues(self):
        call = self._build(self._stats(
            tier3_inactive=0, changes=[], under_stocked=[]
        ))
        subject = call[0][0]
        self.assertNotIn("⚠", subject)

    def test_tier_breakdown_rendered(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("Tier 1", html)
        self.assertIn("Tier 2", html)
        self.assertIn("Tier 3", html)

    def test_tier_counts_rendered(self):
        call = self._build(self._stats(
            tier1_count=8, tier2_count=4, tier3_count=2
        ))
        html = call[0][1]
        self.assertIn("8", html)
        self.assertIn("4", html)
        self.assertIn("2", html)

    def test_changes_table_shown(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("Bob Lee", html)
        self.assertIn("Linear", html)
        self.assertIn("marked inactive", html)

    def test_changes_table_hidden_when_empty(self):
        call = self._build(self._stats(changes=[]))
        html = call[0][1]
        self.assertNotIn("Recruiter Changes", html)

    def test_under_stocked_section_shown(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("Under-stocked", html)
        self.assertIn("Linear", html)

    def test_under_stocked_section_hidden_when_empty(self):
        call = self._build(self._stats(under_stocked=[]))
        html = call[0][1]
        self.assertNotIn("Under-stocked Companies", html)

    def test_inactive_alert_shown(self):
        call = self._build(self._stats(tier3_inactive=1))
        html = call[0][1]
        self.assertIn("inactive", html.lower())

    def test_under_stocked_alert_shown(self):
        call = self._build(self._stats())
        html = call[0][1]
        self.assertIn("--find-only", html)

    def test_report_calls_send_report_email(self):
        from outreach.report_templates.verify_report import build_verify_report
        with patch("outreach.report_templates.verify_report.send_report_email",
                   return_value=True) as mock_send:
            build_verify_report(self._stats())
            mock_send.assert_called_once()


# ─────────────────────────────────────────
# TEST: send_report_email
# ─────────────────────────────────────────

class TestSendReportEmail(unittest.TestCase):

    def test_returns_true_on_success(self):
        from outreach.report_templates.base import send_report_email
        with patch("outreach.report_templates.base.smtplib.SMTP") as mock_smtp, \
             patch("outreach.report_templates.base.EMAIL", "test@gmail.com"), \
             patch("outreach.report_templates.base.APP_PASSWORD", "password"):
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
            mock_smtp.return_value.__exit__ = MagicMock(return_value=False)
            result = send_report_email("Test Subject", "<html>Test</html>")
            self.assertTrue(result)

    def test_returns_false_on_smtp_failure(self):
        from outreach.report_templates.base import send_report_email
        import smtplib
        with patch("outreach.report_templates.base.smtplib.SMTP",
                   side_effect=smtplib.SMTPException("Connection failed")), \
             patch("outreach.report_templates.base.EMAIL", "test@gmail.com"), \
             patch("outreach.report_templates.base.APP_PASSWORD", "password"):
            result = send_report_email("Test Subject", "<html>Test</html>")
            self.assertFalse(result)

    def test_skips_when_email_not_configured(self):
        from outreach.report_templates.base import send_report_email
        with patch("outreach.report_templates.base.EMAIL", None), \
             patch("outreach.report_templates.base.APP_PASSWORD", None), \
             patch("outreach.report_templates.base.smtplib.SMTP") as mock_smtp:
            result = send_report_email("Test Subject", "<html>Test</html>")
            self.assertFalse(result)
            mock_smtp.assert_not_called()

    def test_sends_to_self(self):
        from outreach.report_templates.base import send_report_email
        with patch("outreach.report_templates.base.smtplib.SMTP") as mock_smtp, \
             patch("outreach.report_templates.base.EMAIL", "me@gmail.com"), \
             patch("outreach.report_templates.base.APP_PASSWORD", "password"):
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
            mock_smtp.return_value.__exit__ = MagicMock(return_value=False)
            send_report_email("Test", "<html></html>")
            # send_message called with msg that has To == EMAIL
            call_args = mock_server.send_message.call_args
            msg = call_args[0][0]
            self.assertEqual(msg["To"], "me@gmail.com")


if __name__ == "__main__":
    unittest.main(verbosity=2)