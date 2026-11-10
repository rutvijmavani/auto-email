"""Sustained-429 trend alert (manager) and 429 recording in jobs/ats/base.py."""
import unittest
from unittest import mock

from workers import manager
from jobs.ats import base


def _rows(platform, pairs):
    return [{"platform": platform, "date": f"d{i}", "requests": n, "e429": e}
            for i, (n, e) in enumerate(pairs)]


class TestRateLimitTrend(unittest.TestCase):
    def _run(self, rows):
        with mock.patch("db.api_health.get_daily_429_rates", return_value=rows), \
             mock.patch("db.pipeline_alerts.create_alert", return_value=7) as ca:
            manager._check_rate_limit_trend()
        return ca

    def test_fires_when_enough_days_hit(self):
        # 2.6%, 0.8%, 2.4%, 2.2%, 0.1% -> 3 of 5 days >= 1%
        rows = _rows("workday", [(1000, 26), (1000, 8), (1000, 24), (1000, 22), (1000, 1)])
        ca = self._run(rows)
        self.assertEqual(ca.call_count, 1)
        self.assertEqual(ca.call_args.kwargs["platform"], "workday")

    def test_no_alert_below_days_hit(self):
        rows = _rows("workday", [(1000, 26), (1000, 8), (1000, 24), (1000, 1), (1000, 1)])
        self.assertEqual(self._run(rows).call_count, 0)

    def test_low_volume_days_ignored(self):
        rows = _rows("lever", [(100, 50)] * 5)
        self.assertEqual(self._run(rows).call_count, 0)


class TestPostRecording(unittest.TestCase):
    def _resp(self, code):
        r = mock.Mock()
        r.status_code = code
        r.ok = code == 200
        r.json.return_value = {}
        return r

    def test_429_recorded_once_with_backoff_and_alert_checked(self):
        with mock.patch.object(base.requests, "post",
                               side_effect=[self._resp(429), self._resp(200)]), \
             mock.patch.object(base.time, "sleep"), \
             mock.patch.object(base, "_record") as rec, \
             mock.patch.object(base, "_check_rate_limit_alert") as chk:
            base.fetch_json_post("http://x", platform="workday")
        calls = [c.args[1] for c in rec.call_args_list]
        self.assertEqual(calls, [429, 200])
        self.assertEqual(rec.call_args_list[0].kwargs["backoff_s"], 60)
        chk.assert_called_once_with("workday")

    def test_get_429_not_double_counted(self):
        with mock.patch.object(base.requests, "get",
                               side_effect=[self._resp(429), self._resp(200)]), \
             mock.patch.object(base.time, "sleep"), \
             mock.patch.object(base, "platform_delay"), \
             mock.patch.object(base, "_record") as rec, \
             mock.patch.object(base, "_check_rate_limit_alert"):
            base.fetch_json("http://x", platform="greenhouse")
        self.assertEqual([c.args[1] for c in rec.call_args_list], [429, 200])

    def test_5xx_and_timeout_error_types(self):
        self.assertEqual(base._status_error_type(503), "requests_5xx")
        self.assertEqual(base._status_error_type(403), "requests_other_err")
        self.assertIsNone(base._status_error_type(200))
        with mock.patch.object(base.requests, "post",
                               side_effect=base.requests.exceptions.Timeout()), \
             mock.patch.object(base.time, "sleep"), \
             mock.patch.object(base, "_record") as rec:
            base.fetch_json_post("http://x", platform="workday", retries=0)
        self.assertEqual(rec.call_args.kwargs["error_type"], "requests_timeout")


if __name__ == "__main__":
    unittest.main()
