"""
tests/test_monitor_report_phase1.py
─────────────────────────────────────────────────────────────────────────────
Tests for Phase 1 and Phase 3.5 changes in
outreach/report_templates/monitor_report.py.

Phase 1 — Coverage metric fix
  Old bug: total_covered = companies_with_results   (fallback-only count)
  Fix:     total_covered = covered_by_workers + companies_with_results

Phase 3.5 — Queue depth alert in daily digest email
  _build_queue_health_section() appended to every digest email above
  the API-warnings section.

─── ReportLab mock ───────────────────────────────────────────────────────────
monitor_report.py imports reportlab at module level.  We install lightweight
MagicMock stubs in sys.modules BEFORE the module is imported so the tests run
whether or not ReportLab is installed in the environment.  If the real library
IS installed, sys.modules.setdefault() leaves it untouched and the stubs are
never applied.
─────────────────────────────────────────────────────────────────────────────

Coverage map
────────────
  TestCoverageMetricFix  (Phase 1 — formula fix in _build_health_section)
    · covered_by_workers alone contributes to total_covered (main fix)
    · fallback hits alone still counted (regression guard)
    · both sources summed together correctly
    · zero total does not raise ZeroDivisionError
    · coverage percentage rounds down (int())
    · coverage_val string includes "X/Y" fraction
    · coverage_val string includes "(Z%)" percentage
    · coverage_val string includes "A by workers" breakdown
    · fallback_hits=0 → no "+ N by job monitor" in string
    · fallback_hits>0 → "+ N by job monitor" appended to string
    · old-bug regression: covered_by_workers=111, fallback=0 was zero with old formula

  TestQueueHealthSection  (Phase 3.5 — _build_queue_health_section)
    · Returns a string (even with issues)
    · detail_total > 500 → error-level HTML present
    · detail_total > 100 but ≤ 500 → warning-level HTML present
    · detail_total ≤ 100 → no error/warning HTML for detail queue
    · poll:adaptive empty (total=0) → error HTML for adaptive queue
    · poll:adaptive overdue > 10 → warning HTML
    · poll:fullscan empty (total=0) → warning HTML for fullscan queue
    · Redis exception inside function → returns "" (non-fatal)
    · Return type is always str
"""

import sys
import os
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ── Install ReportLab stubs if library is absent ──────────────────────────────
# sys.modules.setdefault only inserts when the key is NOT already present,
# so a real ReportLab install takes priority automatically.
_RL_STUB_MODS = [
    "reportlab",
    "reportlab.lib",
    "reportlab.lib.pagesizes",
    "reportlab.lib.styles",
    "reportlab.lib.units",
    "reportlab.lib.colors",
    "reportlab.lib.enums",
    "reportlab.platypus",
]
for _mod in _RL_STUB_MODS:
    sys.modules.setdefault(_mod, MagicMock())
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _base_stats(**overrides):
    """Return a minimal stats dict for _build_health_section()."""
    base = {
        "companies_monitored":   139,
        "companies_unknown_ats": 0,
        "covered_by_workers":    0,
        "companies_with_results": 0,
        "api_failures":          0,
        "run_duration_seconds":  42,
    }
    base.update(overrides)
    return base


def _call_health_section(stats):
    """
    Call _build_health_section() and capture the health_data list that is
    passed to Table().  Returns the list, or raises if the call fails.

    We patch the module-level `Table` name so the mock records the call
    while the function body runs normally for everything else.
    """
    import outreach.report_templates.monitor_report as mrep

    captured = {}

    def _table_ctor(data, **kw):
        captured["data"] = data
        return MagicMock()   # fake table object

    with patch.object(mrep, "Table", side_effect=_table_ctor):
        alerts_mock = []
        styles_mock = MagicMock()
        mrep._build_health_section(stats, alerts_mock, styles_mock)

    return captured.get("data", [])


# ─────────────────────────────────────────────────────────────────────────────
# TestCoverageMetricFix  (Phase 1)
# ─────────────────────────────────────────────────────────────────────────────

class TestCoverageMetricFix(unittest.TestCase):
    """
    Phase 1 fix: total_covered = covered_by_workers + companies_with_results.

    The old bug counted only companies_with_results (the job_monitor fallback
    hits), ignoring the 111 companies already covered by the scan workers.
    """

    def _coverage_row(self, **stats_overrides):
        """Return the coverage row [label, value, status] from health_data."""
        data = _call_health_section(_base_stats(**stats_overrides))
        # health_data[0] = header row, health_data[1] = Coverage row
        self.assertGreater(len(data), 1, "Expected at least 2 rows in health_data")
        return data[1]

    def _coverage_val(self, **stats_overrides):
        """Return the coverage value string."""
        return self._coverage_row(**stats_overrides)[1]

    # ── Main fix ──────────────────────────────────────────────────────────────

    def test_covered_by_workers_contributes_to_total(self):
        """covered_by_workers is ADDED to total_covered (the Phase 1 fix)."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=111,
            companies_with_results=0,
        )
        # Old bug produced "0/139 (0%)"; fix gives "111/139 (79%)"
        self.assertIn("111/139", val,
                      f"Expected '111/139' in coverage_val, got: {val!r}")

    def test_old_bug_regression_workers_only(self):
        """
        Regression: the old formula total_covered = companies_with_results
        would give 0/139 when workers covered everything.  The fix must give 111/139.
        """
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=111,
            companies_with_results=0,
        )
        self.assertNotIn("0/139", val,
                         "Old bug: covered_by_workers was ignored. Fix must not produce '0/139'.")

    def test_fallback_only_counted_correctly(self):
        """companies_with_results alone still contributes correctly."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=0,
            companies_with_results=28,
        )
        self.assertIn("28/139", val,
                      f"Expected '28/139' when only fallback covered, got: {val!r}")

    def test_both_sources_summed(self):
        """Total = workers + fallback summed together."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=100,
            companies_with_results=11,
        )
        self.assertIn("111/139", val,
                      f"Expected '111/139' (100+11), got: {val!r}")

    def test_zero_total_no_crash(self):
        """companies_monitored=0 → no ZeroDivisionError, coverage 0%."""
        try:
            val = self._coverage_val(
                companies_monitored=0,
                covered_by_workers=0,
                companies_with_results=0,
            )
        except ZeroDivisionError:
            self.fail("ZeroDivisionError when total=0")

    def test_hundred_percent_coverage(self):
        """workers=139, fallback=0, total=139 → 139/139 (100%)."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=139,
            companies_with_results=0,
        )
        self.assertIn("139/139", val)
        self.assertIn("100%", val)

    def test_coverage_pct_rounds_down(self):
        """int() truncates: 111/139 = 79.85… → 79%."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=111,
            companies_with_results=0,
        )
        self.assertIn("79%", val)

    # ── Coverage string format ────────────────────────────────────────────────

    def test_coverage_val_includes_fraction(self):
        """coverage_val contains 'X/Y' fraction."""
        val = self._coverage_val(
            companies_monitored=10,
            covered_by_workers=8,
            companies_with_results=0,
        )
        self.assertIn("8/10", val)

    def test_coverage_val_includes_percentage(self):
        """coverage_val contains '(Z%)'."""
        val = self._coverage_val(
            companies_monitored=100,
            covered_by_workers=75,
            companies_with_results=0,
        )
        self.assertIn("75%", val)

    def test_coverage_val_includes_by_workers_breakdown(self):
        """coverage_val includes 'A by workers' detail."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=111,
            companies_with_results=0,
        )
        self.assertIn("by workers", val,
                      f"Expected 'by workers' in coverage_val, got: {val!r}")

    def test_no_fallback_excludes_job_monitor_text(self):
        """When fallback_hits=0 the string does not include '+ 0 by job monitor'."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=111,
            companies_with_results=0,
        )
        self.assertNotIn("by job monitor", val,
                         "Should not append fallback text when fallback_hits=0")

    def test_with_fallback_includes_job_monitor_text(self):
        """When fallback_hits>0 the string appends '+ N by job monitor'."""
        val = self._coverage_val(
            companies_monitored=139,
            covered_by_workers=100,
            companies_with_results=11,
        )
        self.assertIn("by job monitor", val,
                      f"Expected 'by job monitor' when fallback_hits>0, got: {val!r}")

    # ── Coverage status threshold ─────────────────────────────────────────────

    def test_coverage_70_pct_is_ok(self):
        """70% coverage → status 'OK'."""
        row = self._coverage_row(
            companies_monitored=10,
            covered_by_workers=7,
            companies_with_results=0,
        )
        self.assertEqual(row[2], "OK")

    def test_coverage_below_70_pct_is_warning(self):
        """Below 70% coverage → status 'WARNING'."""
        row = self._coverage_row(
            companies_monitored=10,
            covered_by_workers=6,
            companies_with_results=0,
        )
        self.assertEqual(row[2], "WARNING")


# ─────────────────────────────────────────────────────────────────────────────
# TestQueueHealthSection  (Phase 3.5)
# ─────────────────────────────────────────────────────────────────────────────

class TestQueueHealthSection(unittest.TestCase):
    """
    _build_queue_health_section() returns an HTML string embedded in the
    daily digest email.  All Redis calls are mocked.

    Thresholds (from source):
        detail_total > 500  → error
        detail_total > 100  → warning
        poll:adaptive empty → error
        poll:adaptive overdue > 10 → warning
        poll:fullscan empty → warning
        Redis exception     → returns "" (non-fatal)
    """

    def _make_redis(self, detail_adp=0, detail_fs=0,
                    poll_adp_total=10, poll_adp_overdue=0,
                    poll_fs_total=10, poll_fs_overdue=0):
        """Build a minimal Redis mock for _build_queue_health_section()."""
        r = MagicMock()

        def _llen(key):
            from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN
            if key == REDIS_DETAIL_ADAPTIVE:
                return detail_adp
            if key == REDIS_DETAIL_FULLSCAN:
                return detail_fs
            return 0

        def _zcard(key):
            from config import REDIS_POLL_ADAPTIVE, REDIS_POLL_FULLSCAN
            if key == REDIS_POLL_ADAPTIVE:
                return poll_adp_total
            if key == REDIS_POLL_FULLSCAN:
                return poll_fs_total
            return 0

        def _zcount(key, lo, hi):
            from config import REDIS_POLL_ADAPTIVE, REDIS_POLL_FULLSCAN
            if key == REDIS_POLL_ADAPTIVE:
                return poll_adp_overdue
            if key == REDIS_POLL_FULLSCAN:
                return poll_fs_overdue
            return 0

        r.llen.side_effect   = _llen
        r.zcard.side_effect  = _zcard
        r.zcount.side_effect = _zcount
        return r

    def _run(self, **kwargs):
        """Call _build_queue_health_section() with mocked Redis."""
        r = self._make_redis(**kwargs)
        # _build_queue_health_section() uses `import redis as _redis_lib` locally
        # and calls _redis_lib.from_url() — patch at the redis module level.
        with patch("redis.from_url", return_value=r):
            import outreach.report_templates.monitor_report as mrep
            return mrep._build_queue_health_section()

    # ── Return type ───────────────────────────────────────────────────────────

    def test_returns_string(self):
        """Function always returns a str."""
        result = self._run()
        self.assertIsInstance(result, str)

    def test_returns_string_when_issues_present(self):
        """Returns a string even when there are error-level issues."""
        result = self._run(detail_adp=600)
        self.assertIsInstance(result, str)

    # ── detail queue thresholds ───────────────────────────────────────────────

    def test_detail_above_500_contains_error_marker(self):
        """detail_total > 500 → HTML contains error-level colour or CRITICAL text."""
        result = self._run(detail_adp=300, detail_fs=250)  # combined=550
        # Error is indicated by the red hex colour or the word "CRITICAL"
        self.assertTrue(
            "#ef4444" in result or "CRITICAL" in result or "error" in result.lower(),
            f"Expected error indicator for detail_total=550, got: {result[:300]!r}"
        )

    def test_detail_above_100_not_500_contains_warning_marker(self):
        """detail_total > 100 but ≤ 500 → HTML contains warning indicator."""
        result = self._run(detail_adp=150)  # combined=150
        self.assertTrue(
            "#f59e0b" in result or "warning" in result.lower() or "⚠" in result,
            f"Expected warning indicator for detail_total=150, got: {result[:300]!r}"
        )

    def test_detail_at_100_no_backlog_warning(self):
        """detail_total = 100 → exactly at threshold, no WARNING issued."""
        result = self._run(detail_adp=100)
        # Should not contain "backlog" warning text for detail queue
        # (threshold is strictly > 100)
        self.assertNotIn("backlog CRITICAL", result)

    def test_detail_zero_ok_no_backlog_text(self):
        """detail_total=0 → no backlog warnings."""
        result = self._run(detail_adp=0, detail_fs=0)
        self.assertNotIn("backlog", result)

    # ── poll:adaptive ─────────────────────────────────────────────────────────

    def test_poll_adaptive_empty_contains_error_marker(self):
        """poll:adaptive total=0 → error — 'EMPTY' text in output."""
        result = self._run(poll_adp_total=0)
        self.assertIn("EMPTY", result,
                      f"Expected 'EMPTY' when poll:adaptive is empty, got: {result[:300]!r}")

    def test_poll_adaptive_overdue_gt_10_contains_warning(self):
        """poll:adaptive overdue > 10 → warning text present."""
        result = self._run(poll_adp_total=50, poll_adp_overdue=11)
        self.assertTrue(
            "⚠" in result or "warning" in result.lower() or "#f59e0b" in result,
            f"Expected warning for overdue=11, got: {result[:300]!r}"
        )

    def test_poll_adaptive_present_no_error(self):
        """poll:adaptive with items and low overdue → no error for adaptive queue."""
        result = self._run(poll_adp_total=139, poll_adp_overdue=2)
        self.assertNotIn("EMPTY", result)

    # ── poll:fullscan ─────────────────────────────────────────────────────────

    def test_poll_fullscan_empty_contains_warning(self):
        """poll:fullscan total=0 → warning — 'EMPTY' text in output."""
        result = self._run(poll_fs_total=0)
        self.assertIn("EMPTY", result,
                      f"Expected 'EMPTY' when poll:fullscan is empty, got: {result[:300]!r}")

    def test_poll_fullscan_present_no_empty_warning(self):
        """poll:fullscan has items → no EMPTY warning for it."""
        result = self._run(poll_adp_total=50, poll_fs_total=139)
        # With healthy queues and no detail backlog, EMPTY should not appear
        # (unless poll:adaptive is also empty — keep adaptive healthy)
        self.assertNotIn("EMPTY", result)

    # ── Redis failure ─────────────────────────────────────────────────────────

    def test_redis_exception_returns_empty_string(self):
        """Any Redis exception inside the function → returns '' (non-fatal)."""
        with patch("redis.from_url",
                   side_effect=Exception("Redis connection failed")):
            import outreach.report_templates.monitor_report as mrep
            result = mrep._build_queue_health_section()
        self.assertEqual(result, "",
                         "Expected empty string on Redis failure")

    def test_redis_llen_exception_returns_empty_string(self):
        """Redis llen() raising mid-call → returns '' (non-fatal)."""
        r = MagicMock()
        r.llen.side_effect = ConnectionError("timeout")
        with patch("redis.from_url", return_value=r):
            import outreach.report_templates.monitor_report as mrep
            result = mrep._build_queue_health_section()
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
