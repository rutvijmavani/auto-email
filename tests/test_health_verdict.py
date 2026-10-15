# tests/test_health_verdict.py — jobs/health_verdict.py (pure) and the digest fetch-health blocks

import unittest
from datetime import datetime, timedelta

from jobs import health_verdict as hv


def _row(count, complete=True, error=None, stop="all_pages", total=None):
    return {"fetched_distinct": count, "complete": complete, "error": error,
            "stop_reason": stop, "reported_total": total}


def _rows(*counts):
    """Newest first, all good."""
    return [_row(c) for c in counts]


class TestStats(unittest.TestCase):

    def test_spread_floor_is_poisson_for_a_steady_board(self):
        values = [198, 205, 201, 199, 204, 202]
        self.assertAlmostEqual(hv.spread(values), 201.5 ** 0.5, places=6)   # MAD*1.48 = 3.7 < sqrt(m)

    def test_spread_uses_own_mad_for_a_volatile_board(self):
        values = [120, 260, 180, 240, 150, 230]
        self.assertAlmostEqual(hv.spread(values), 1.4826 * 45, places=6)

    def test_percentile(self):
        self.assertEqual(hv.percentile([1, 2, 3, 4, 5], 50), 3)
        self.assertEqual(hv.percentile([1, 2, 3, 4, 5], 100), 5)
        self.assertAlmostEqual(hv.percentile([0, 10], 90), 9)

    def test_peer_spread_needs_enough_peers(self):
        peers = [[100, 101, 99, 100]] * 3
        self.assertIsNone(hv.peer_relative_spread(peers, min_peers=5))
        self.assertIsNotNone(hv.peer_relative_spread(peers, min_peers=3))

    def test_peers_with_short_history_do_not_count(self):
        self.assertIsNone(hv.peer_relative_spread([[100]] * 10, min_peers=2))


class TestEvaluate(unittest.TestCase):

    def test_no_rows(self):
        self.assertEqual(hv.evaluate([]).state, hv.STATE_OK)

    def test_steady_board_inside_band_is_ok(self):
        v = hv.evaluate(_rows(190, 198, 205, 201, 199, 204, 202))
        self.assertEqual(v.state, hv.STATE_OK)
        self.assertAlmostEqual(v.flag_line, 201.5 - 3 * 201.5 ** 0.5, places=6)

    def test_steady_board_below_band_is_suspect(self):
        v = hv.evaluate(_rows(150, 198, 205, 201, 199, 204, 202))
        self.assertEqual(v.state, hv.STATE_SUSPECT)
        self.assertEqual(v.band_from, "own")

    def test_volatile_board_same_drop_is_ok(self):
        v = hv.evaluate(_rows(100, 120, 260, 180, 240, 150, 230))
        self.assertEqual(v.state, hv.STATE_OK)

    def test_small_board(self):
        self.assertEqual(hv.evaluate(_rows(12, 19, 21, 20, 22, 18, 20)).state, hv.STATE_OK)
        self.assertEqual(hv.evaluate(_rows(5, 19, 21, 20, 22, 18, 20)).state, hv.STATE_SUSPECT)

    def test_zero_after_nonzero_is_failing(self):
        v = hv.evaluate(_rows(0, 50, 52, 49, 51))
        self.assertEqual(v.state, hv.STATE_FAILING)

    def test_zero_without_history_is_not_judged(self):
        self.assertEqual(hv.evaluate(_rows(0)).state, hv.STATE_OK)

    def test_first_incomplete_is_suspect_second_is_failing(self):
        one = hv.evaluate([_row(480, complete=False, stop="request_failed", total=640)] + _rows(640, 640))
        self.assertEqual(one.state, hv.STATE_SUSPECT)
        two = hv.evaluate([_row(480, complete=False), _row(470, complete=False)] + _rows(640))
        self.assertEqual(two.state, hv.STATE_FAILING)

    def test_scan_error_counts_as_bad(self):
        v = hv.evaluate([_row(0, complete=False, error="Timeout: x")] + _rows(50, 50))
        self.assertEqual(v.state, hv.STATE_SUSPECT)
        self.assertIn("Timeout", v.reason)

    def test_unknown_completeness_is_not_bad(self):
        rows = [_row(200, complete=None)] + [_row(c, complete=None) for c in (198, 205, 201, 199, 204)]
        self.assertEqual(hv.evaluate(rows).state, hv.STATE_OK)

    def test_bad_scans_are_excluded_from_the_baseline(self):
        # the incomplete 30-job scan in the middle must not drag the median down
        rows = _rows(200) + [_row(30, complete=False)] + _rows(198, 205, 201, 199, 204)
        v = hv.evaluate(rows)
        self.assertGreater(v.median, 195)

    def test_recovery_after_bad_scan(self):
        rows = _rows(200) + [_row(30, complete=False)] + _rows(198, 205, 201, 199, 204)
        self.assertEqual(hv.evaluate(rows).state, hv.STATE_OK)

    def test_cold_start_uses_peers(self):
        # 2 good scans of history: too few for an own band, peers say 8% relative spread
        v = hv.evaluate(_rows(100, 200, 200), peer_rel_spread=0.08)
        self.assertEqual(v.band_from, "peers")
        self.assertEqual(v.state, hv.STATE_SUSPECT)
        self.assertAlmostEqual(v.spread, max(0.08 * 200, 200 ** 0.5))

    def test_cold_start_without_peers_skips_count_check(self):
        v = hv.evaluate(_rows(10, 200, 200), peer_rel_spread=None)
        self.assertEqual(v.state, hv.STATE_OK)
        self.assertIsNone(v.flag_line)

    def test_first_good_scan_has_no_baseline(self):
        self.assertEqual(hv.evaluate(_rows(500)).state, hv.STATE_OK)


class TestDigestBlocks(unittest.TestCase):

    def _row(self, state="failing", **kw):
        base = {"company_key": "Acme", "platform": "workday", "state": state,
                "reason": "empty board", "since": datetime.now() - timedelta(days=3)}
        base.update(kw)
        return base

    def test_email_block_empty_when_nothing_to_say(self):
        from outreach.report_templates.monitor_report import _build_fetch_health_email
        self.assertEqual(_build_fetch_health_email({"new": [], "still": [], "recovered": []}), "")

    def test_email_block_counts_and_truncation(self):
        from config import HEALTH_DIGEST_MAX_LISTED
        from outreach.report_templates.monitor_report import _build_fetch_health_email
        rows = [self._row(company_key=f"C{i}") for i in range(HEALTH_DIGEST_MAX_LISTED + 3)]
        html = _build_fetch_health_email({"new": rows, "still": [], "recovered": [self._row("ok")]})
        self.assertIn(f"{len(rows)} new", html)
        self.assertIn("1 recovered", html)
        self.assertIn("3 more", html)
        self.assertNotIn(f"C{HEALTH_DIGEST_MAX_LISTED + 2}", html)

    def test_line_includes_age_and_reason(self):
        from outreach.report_templates.monitor_report import _health_line
        line = _health_line(self._row())
        self.assertIn("3d", line)
        self.assertIn("empty board", line)

    def test_line_prefers_display_name(self):
        from outreach.report_templates.monitor_report import _health_line
        line = _health_line(self._row(company_key="ca:42", display_name="Acme Corp"))
        self.assertTrue(line.startswith("Acme Corp ["))
        self.assertTrue(_health_line(self._row(company_key="ca:42")).startswith("ca:42 ["))


if __name__ == "__main__":
    unittest.main()
