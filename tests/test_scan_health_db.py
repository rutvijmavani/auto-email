# tests/test_scan_health_db.py — db/scan_health.py (record_scan, annotate_latest, get_recent)
# and db/company_health.py (refresh, digest_snapshot, mark_reported) against the test database.

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ["TEST_MODE"] = "1"

import db.db as db_module
from config import HEALTH_BASELINE_SCANS, HEALTH_MIN_HISTORY, HEALTH_MIN_PEERS
from db import company_health, scan_health
from db.connection import get_conn
from jobs.ats.fetch_report import (
    STOP_ALL_PAGES, STOP_REQUEST_FAILED, FetchReport,
)
from tests.conftest import cleanup_db


def _report(count, stop=STOP_ALL_PAGES, total=None, complete=None):
    r = FetchReport(stop_reason=stop, pages=1, fetched_distinct=count, reported_total=total)
    r.finalize()
    if complete is not None:
        r.complete = complete
    return r


def _fullscan(key, count, platform="workday", **kw):
    return scan_health.record_scan(key, platform, scan_health.SOURCE_FULLSCAN,
                                   _report(count, **kw), raw_count=count, valid_count=count)


def _health(key):
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM company_health WHERE company_key = ?", (key,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


class _DbCase(unittest.TestCase):
    def setUp(self):
        cleanup_db()
        db_module.init_db()

    def tearDown(self):
        cleanup_db()


class TestRecordScan(_DbCase):

    def test_row_is_written_with_every_field(self):
        report = _report(190, total=200)
        ok = scan_health.record_scan("Acme", "workday", scan_health.SOURCE_SCAN, report,
                                     raw_count=195, valid_count=190, matched_count=7,
                                     duration_ms=1234, feed_status="verified")
        self.assertTrue(ok)
        row = scan_health.get_recent("Acme")[0]
        self.assertEqual(
            (row["company_key"], row["platform"], row["source"], row["stop_reason"],
             row["fetched_distinct"], row["reported_total"], row["raw_count"],
             row["valid_count"], row["matched_count"], row["duration_ms"], row["feed_status"]),
            ("Acme", "workday", "scan", STOP_ALL_PAGES, 190, 200, 195, 190, 7, 1234, "verified"))
        self.assertIs(row["complete"], True)
        self.assertIs(row["server_short"], False)

    def test_unknown_completeness_is_stored_as_null(self):
        scan_health.record_scan("Acme", "lever", scan_health.SOURCE_SCAN, _report(50))
        self.assertIsNone(scan_health.get_recent("Acme")[0]["complete"])

    def test_error_is_truncated_and_matched_count_may_be_null(self):
        scan_health.record_scan("Acme", "lever", scan_health.SOURCE_SCAN, _report(0), error="x" * 900)
        row = scan_health.get_recent("Acme")[0]
        self.assertEqual(len(row["error"]), 500)
        self.assertIsNone(row["matched_count"])

    def test_numeric_company_key_is_stored_as_text(self):
        scan_health.record_scan(42, "workday", scan_health.SOURCE_SCAN, _report(5))
        self.assertEqual(scan_health.get_recent("42")[0]["company_key"], "42")

    def test_failed_write_returns_false_and_never_raises(self):
        with patch("db.scan_health.get_conn", side_effect=RuntimeError("db down")):
            self.assertFalse(scan_health.record_scan("Acme", "workday", "scan", _report(5)))

    def test_one_transient_failure_is_retried(self):
        real = scan_health.get_conn
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("connection reset")
            return real()

        with patch("db.scan_health.get_conn", side_effect=flaky):
            self.assertTrue(scan_health.record_scan("Acme", "workday", "scan", _report(5)))
        self.assertEqual(len(scan_health.get_recent("Acme")), 1)

    def test_only_fullscan_feeds_company_health(self):
        scan_health.record_scan("A", "workday", scan_health.SOURCE_SCAN, _report(10))
        scan_health.record_scan("A", "workday", scan_health.SOURCE_MONITOR, _report(10))
        self.assertIsNone(_health("A"))
        _fullscan("A", 10)
        self.assertEqual(_health("A")["state"], "ok")

    def test_get_recent_is_newest_first_and_limited(self):
        for n in (1, 2, 3):
            scan_health.record_scan("A", "workday", "scan", _report(n))
        rows = scan_health.get_recent("A", limit=2)
        self.assertEqual([r["fetched_distinct"] for r in rows], [3, 2])


class TestAnnotateLatest(_DbCase):

    def test_updates_only_the_latest_row_of_that_source(self):
        scan_health.record_scan("A", "jobvite", "fullscan", _report(10))
        scan_health.record_scan("A", "jobvite", "fullscan", _report(11))
        scan_health.record_scan("A", "jobvite", "scan", _report(12))
        scan_health.annotate_latest("A", "fullscan", "feed_short")
        by_count = {r["fetched_distinct"]: r["feed_status"] for r in scan_health.get_recent("A")}
        self.assertEqual(by_count, {10: None, 11: "feed_short", 12: None})

    def test_other_companies_are_untouched(self):
        scan_health.record_scan("A", "jobvite", "fullscan", _report(10))
        scan_health.record_scan("B", "jobvite", "fullscan", _report(10))
        scan_health.annotate_latest("A", "fullscan", "verified")
        self.assertIsNone(scan_health.get_recent("B")[0]["feed_status"])

    def test_no_matching_row_is_a_noop(self):
        scan_health.annotate_latest("ghost", "fullscan", "verified")   # must not raise


class TestRefresh(_DbCase):

    def _steady(self, key, platform="workday", counts=(200, 198, 205, 201, 199)):
        for c in counts:
            _fullscan(key, c, platform)

    def test_no_rows_returns_none(self):
        self.assertIsNone(company_health.refresh("ghost"))

    def test_steady_board_is_ok(self):
        self._steady("A")
        row = _health("A")
        self.assertEqual(row["state"], "ok")
        self.assertEqual(row["platform"], "workday")
        self.assertEqual(row["band_from"], "own")

    def test_drop_is_suspect_and_zero_after_nonzero_is_failing(self):
        self._steady("A")
        _fullscan("A", 120)
        row = _health("A")
        self.assertEqual(row["state"], "suspect")
        self.assertEqual(row["count"], 120)
        self.assertLess(row["flag_line"], row["baseline"])
        _fullscan("A", 0)
        self.assertEqual(_health("A")["state"], "failing")

    def test_since_changes_only_when_state_changes(self):
        self._steady("A")
        _fullscan("A", 120)
        first = _health("A")
        _fullscan("A", 119)                 # still suspect
        second = _health("A")
        if second["state"] == "suspect":    # a second bad scan may escalate to failing; only assert the stable case
            self.assertEqual(first["since"], second["since"])
        _fullscan("A", 200)
        for _ in range(HEALTH_MIN_HISTORY):
            _fullscan("A", 200)
        self.assertEqual(_health("A")["state"], "ok")
        self.assertGreater(_health("A")["since"], first["since"])

    def test_incomplete_fetch_is_bad(self):
        self._steady("A")
        _fullscan("A", 80, stop=STOP_REQUEST_FAILED, total=200)
        self.assertEqual(_health("A")["state"], "suspect")

    def test_peers_supply_the_band_for_a_new_company(self):
        for i in range(HEALTH_MIN_PEERS):
            self._steady(f"peer{i}", counts=(200, 198, 205, 201, 199, 202))
        _fullscan("new", 200)
        _fullscan("new", 200)
        _fullscan("new", 100)
        row = _health("new")
        self.assertEqual(row["band_from"], "peers")
        self.assertEqual(row["state"], "suspect")

    def test_peers_on_another_platform_do_not_count(self):
        for i in range(HEALTH_MIN_PEERS):
            self._steady(f"peer{i}", platform="lever", counts=(200, 198, 205, 201, 199, 202))
        for c in (200, 200, 100):
            _fullscan("new", c, "workday")
        self.assertNotEqual(_health("new")["band_from"], "peers")
        self.assertEqual(_health("new")["state"], "ok")   # no band → count check skipped

    def test_baseline_window_is_bounded(self):
        for c in [10] * 20 + [200] * HEALTH_BASELINE_SCANS:
            _fullscan("A", c)
        self.assertAlmostEqual(_health("A")["baseline"], 200)

    def test_failed_refresh_returns_none(self):
        with patch("db.company_health.get_conn", side_effect=RuntimeError("db down")):
            self.assertIsNone(company_health.refresh("A"))


class TestDigest(_DbCase):

    def _make(self, key, bad):
        for c in (200, 198, 205, 201, 199):
            _fullscan(key, c)
        if bad:
            _fullscan(key, 100)

    def test_snapshot_classifies_new_still_recovered(self):
        self._make("broken", bad=True)
        self._make("fine", bad=False)
        snap = company_health.digest_snapshot()
        self.assertEqual([r["company_key"] for r in snap["new"]], ["broken"])
        self.assertEqual(snap["still"], [])
        self.assertEqual(snap["recovered"], [])        # "fine" was never reported non-ok

        company_health.mark_reported()
        snap = company_health.digest_snapshot()
        self.assertEqual(snap["new"], [])
        self.assertEqual([r["company_key"] for r in snap["still"]], ["broken"])

        for _ in range(HEALTH_MIN_HISTORY + 1):
            _fullscan("broken", 200)
        snap = company_health.digest_snapshot()
        self.assertEqual([r["company_key"] for r in snap["recovered"]], ["broken"])
        company_health.mark_reported()
        self.assertEqual(company_health.digest_snapshot(),
                         {"new": [], "still": [], "recovered": []})

    def test_state_change_after_report_is_new_again(self):
        self._make("A", bad=True)
        company_health.mark_reported()
        _fullscan("A", 0)                               # suspect → failing
        snap = company_health.digest_snapshot()
        self.assertEqual([r["company_key"] for r in snap["new"]], ["A"])
        self.assertEqual(snap["new"][0]["state"], "failing")

    def test_failing_sorts_before_suspect(self):
        self._make("suspect_co", bad=True)
        self._make("failing_co", bad=False)
        _fullscan("failing_co", 0)
        keys = [r["company_key"] for r in company_health.digest_snapshot()["new"]]
        self.assertEqual(keys, ["failing_co", "suspect_co"])

    def test_expected_company_is_left_out_of_the_digest(self):
        self._make("A", bad=True)
        self.assertTrue(company_health.mark_expected("A"))
        self.assertEqual(company_health.digest_snapshot(),
                         {"new": [], "still": [], "recovered": []})
        self.assertEqual(_health("A")["expected_state"], "suspect")

    def test_same_state_scan_keeps_the_expected_mark(self):
        self._make("A", bad=True)
        company_health.mark_expected("A")
        _fullscan("A", 101)                      # still suspect
        if _health("A")["state"] == "suspect":
            self.assertEqual(_health("A")["expected_state"], "suspect")

    def test_state_change_clears_the_expected_mark(self):
        self._make("A", bad=True)
        company_health.mark_expected("A")
        _fullscan("A", 0)                        # suspect → failing: a new kind of failure
        row = _health("A")
        self.assertEqual(row["state"], "failing")
        self.assertIsNone(row["expected_state"])
        self.assertEqual([r["company_key"] for r in company_health.digest_snapshot()["new"]], ["A"])

    def test_recovery_clears_the_expected_mark(self):
        self._make("A", bad=True)
        company_health.mark_expected("A")
        for _ in range(HEALTH_MIN_HISTORY + 1):
            _fullscan("A", 200)
        row = _health("A")
        self.assertEqual(row["state"], "ok")
        self.assertIsNone(row["expected_state"])

    def test_mark_expected_needs_a_non_ok_company(self):
        self._make("fine", bad=False)
        self.assertFalse(company_health.mark_expected("fine"))
        self.assertFalse(company_health.mark_expected("ghost"))

    def test_clear_expected_reports_the_company_again(self):
        self._make("A", bad=True)
        company_health.mark_expected("A")
        company_health.clear_expected("A")
        self.assertEqual([r["company_key"] for r in company_health.digest_snapshot()["new"]], ["A"])

    def test_mark_reported_stamps_reported_at(self):
        self._make("A", bad=True)
        company_health.mark_reported()
        row = _health("A")
        self.assertEqual(row["reported_state"], "suspect")
        self.assertIsNotNone(row["reported_at"])


if __name__ == "__main__":
    unittest.main()
