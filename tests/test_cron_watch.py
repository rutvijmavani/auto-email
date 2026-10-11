"""
tests/test_cron_watch.py — scripts/cron_watch.py (design: docs/cron-monitoring-design.md)
"""
import json
import os
import sys
import tempfile
import time
import unittest
from unittest import mock
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import cron_watch as cw  # noqa: E402


def ts(y, mo, d, h=0, mi=0, s=0):
    return time.mktime(datetime(y, mo, d, h, mi, s).timetuple())


def stamp(t):
    return datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")


NOW = ts(2026, 10, 12, 7, 30)          # Monday 07:30 local
INIT = ts(2026, 10, 1)                  # watcher initialised long ago


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.logs = self.dir / "logs"
        self.logs.mkdir()
        self.state_path = self.dir / "state.json"
        self.state_path.write_text(json.dumps({"initialized_at": INIT, "jobs": {}, "sent": {},
                                               "known_job_keys": []}))
        self.sent = []

    def tearDown(self):
        self.tmp.cleanup()

    def send(self, subject, body):
        self.sent.append((subject, body))
        return True

    def wrapper(self, name="run_monitor.sh", prefix="monitor", fmt="%Y-%m-%d"):
        p = self.dir / name
        p.write_text(
            f'#!/bin/bash\nPROJECT_DIR="{self.dir}"\nLOG_DIR="$PROJECT_DIR/logs"\n'
            f'LOG_FILE="$LOG_DIR/{prefix}_$(date +{fmt}).log"\n'
            f'echo "[CRON] x started at $(date)" >> "$LOG_FILE"\n')
        p.chmod(0o755)
        return p

    def log(self, prefix, day, lines, fmt="%Y-%m-%d"):
        p = self.logs / f"{prefix}_{datetime.fromtimestamp(day).strftime(fmt)}.log"
        p.write_text("\n".join(lines) + "\n")
        os.utime(p, (NOW, NOW))     # logs are "last written" at the fake clock, not wall time
        return p

    def run_watch(self, crontab, now=NOW):
        return cw.run(self.send, now=now, crontab=crontab, state_path=self.state_path)


class TestParsing(unittest.TestCase):
    def test_parse_crontab_tz_and_env(self):
        text = "MAILTO=x\nCRON_TZ=America/New_York\n# c\n0 7 * * * /a/run_a.sh\n@daily /a/run_b.sh\n"
        jobs = cw.parse_crontab(text)
        self.assertEqual([j["expr"] for j in jobs], ["0 7 * * *", "@daily"])
        self.assertEqual(jobs[0]["tz"], "America/New_York")
        self.assertEqual(jobs[0]["command"], "/a/run_a.sh")

    def test_ignore_patterns_default(self):
        pats = cw.ignore_patterns()
        self.assertTrue(cw.is_ignored("0 9 * * * grep -q 'exit=0' /x.log || /x/run_monitor.sh", pats))
        self.assertTrue(cw.is_ignored('0 */4 * * * python3 -c "import hashlib; ..."', pats))
        self.assertTrue(cw.is_ignored("*/15 * * * * /v/python /m/scripts/log_monitor.py >> /l", pats))
        self.assertFalse(cw.is_ignored("0 7 * * * /home/opc/mail/run_monitor.sh", pats))

    def test_ignore_patterns_keep_commas_in_regex(self):
        with mock.patch.object(cw.config, "CRON_WATCH_IGNORE_PATTERNS", r"foo\d{1,3}bar;;baz"):
            pats = cw.ignore_patterns()
        self.assertEqual(pats, [r"foo\d{1,3}bar", "baz"])
        self.assertTrue(cw.is_ignored("x foo12bar", pats))

    def test_wrapper_log_template_daily_and_monthly(self):
        text = 'PROJECT_DIR="/p"\nLOG_DIR="$PROJECT_DIR/logs"\nLOG_FILE="$LOG_DIR/monthly_$(date +%Y-%m).log"\n'
        tpl = cw.wrapper_log_template(text)
        self.assertEqual(tpl, "/p/logs/monthly_$(date +%Y-%m).log")
        self.assertEqual(str(cw.log_path_for(tpl, datetime(2026, 10, 1))).replace("\\", "/"),
                         "/p/logs/monthly_2026-10.log")

    def test_expand_date_unescapes_percent(self):
        self.assertEqual(cw.expand_date(r"/l/a_$(date +\%Y-\%m-\%d).log", datetime(2026, 1, 2)), "/l/a_2026-01-02.log")

    def test_due_slots_cron_tz(self):
        # 07:00 New York is 11:00 or 12:00 UTC; exactly one slot in a 24 h window
        since = ts(2026, 10, 11, 0)
        slots = cw.due_slots("0 7 * * *", "America/New_York", since, since + 86400 * 1.5)
        self.assertGreaterEqual(len(slots), 1)
        for s in slots:
            self.assertEqual(datetime.fromtimestamp(s, __import__("zoneinfo").ZoneInfo("America/New_York")).hour, 7)


class TestChecks(Base):
    def test_started_ok_no_alert(self):
        self.wrapper()
        self.log("monitor", NOW, [f"[CRON] --monitor-jobs started at {stamp(ts(2026, 10, 12, 7, 0, 3))}",
                                  f"[CRON] --monitor-jobs finished at {stamp(ts(2026, 10, 12, 7, 5))} | exit=0"])
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n")
        self.assertEqual(alerts, [])
        self.assertEqual(self.sent, [])

    def test_missing_wrapper_alerts_wrapper_and_start(self):
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n")
        kinds = {a["kind"] for a in alerts}
        self.assertEqual(kinds, {"wrapper", "start"})
        self.assertEqual(len(self.sent), 1)          # batched into one email
        self.assertIn("run_monitor.sh", self.sent[0][1])

    def test_no_start_line_is_missed_start(self):
        self.wrapper()
        self.log("monitor", NOW, ["unrelated line"])
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n")
        self.assertEqual([a["kind"] for a in alerts], ["start"])

    def test_manual_run_later_does_not_mask_missed_slot(self):
        self.wrapper()
        self.log("monitor", NOW, [f"[CRON] m started at {stamp(ts(2026, 10, 12, 7, 20))}",
                                  f"[CRON] m finished at {stamp(ts(2026, 10, 12, 7, 25))} | exit=0"])
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n")
        self.assertEqual([a["kind"] for a in alerts], ["start"])

    def test_slot_inside_grace_not_yet_checked(self):
        self.wrapper()
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n", now=ts(2026, 10, 12, 7, 5))
        self.assertEqual(alerts, [])

    def test_first_run_has_no_backlog(self):
        self.state_path.write_text(json.dumps({"jobs": {}, "sent": {}, "known_job_keys": []}))
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n")   # wrapper missing, but init_at == now
        self.assertEqual([a["kind"] for a in alerts], ["wrapper"])

    def test_nonzero_exit_alerts_once_per_run(self):
        self.wrapper()
        start = ts(2026, 10, 12, 7, 0, 3)
        self.log("monitor", NOW, [f"[CRON] m started at {stamp(start)}",
                                  f"[CRON] m finished at {stamp(start + 60)} | exit=3"])
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n")
        self.assertEqual([a["kind"] for a in alerts], ["exit"])
        self.assertIn("exit=3", alerts[0]["text"])
        again = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n", now=NOW + 900)
        self.assertEqual(again, [])                  # recorded, not repeated

    def test_finished_run_in_older_slot_log_is_recorded(self):
        self.wrapper()
        start = ts(2026, 10, 9, 7, 0, 3)             # 3 days before NOW, outside today/yesterday logs
        self.log("monitor", start, [f"[CRON] m started at {stamp(start)}",
                                    f"[CRON] m finished at {stamp(start + 60)} | exit=4"])
        with mock.patch.object(cw.config, "CRON_WATCH_LOOKBACK_HOURS", 96):
            alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n")
        self.assertIn("exit", [a["kind"] for a in alerts])

    def test_failure_before_watcher_existed_is_ignored(self):
        self.wrapper()
        start = ts(2026, 10, 12, 7, 0, 3)
        self.state_path.write_text(json.dumps({"initialized_at": start + 3600, "jobs": {}, "sent": {},
                                               "known_job_keys": []}))
        self.log("monitor", NOW, [f"[CRON] m started at {stamp(start)}",
                                  f"[CRON] m finished at {stamp(start + 60)} | exit=1"])
        self.assertEqual(self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n", now=start + 7200), [])

    def test_dedup_and_realert(self):
        self.wrapper()
        cron = f"0 7 * * * {self.dir}/run_monitor.sh\n"
        self.log("monitor", NOW, ["nothing"])
        self.run_watch(cron)
        self.assertEqual(len(self.sent), 1)
        self.run_watch(cron, now=NOW + 900)
        self.assertEqual(len(self.sent), 1)          # suppressed
        self.run_watch(cron, now=NOW + 25 * 3600)
        self.assertEqual(len(self.sent), 2)          # window passed, still failing

    def test_failed_email_is_retried(self):
        self.wrapper()
        cron = f"0 7 * * * {self.dir}/run_monitor.sh\n"
        calls = []
        cw.run(lambda s, b: calls.append(1) or False, now=NOW, crontab=cron, state_path=self.state_path)
        cw.run(lambda s, b: calls.append(1) or True, now=NOW + 900, crontab=cron, state_path=self.state_path)
        self.assertEqual(len(calls), 2)

    def test_failed_email_keeps_one_time_exit_alert(self):
        self.wrapper()
        cron = f"0 7 * * * {self.dir}/run_monitor.sh\n"
        start = ts(2026, 10, 12, 7, 0, 3)
        self.log("monitor", NOW, [f"[CRON] m started at {stamp(start)}",
                                  f"[CRON] m finished at {stamp(start + 60)} | exit=3"])
        cw.run(lambda s, b: False, now=NOW, crontab=cron, state_path=self.state_path)
        sent = []
        cw.run(lambda s, b: sent.append(b) or True, now=NOW + 900, crontab=cron, state_path=self.state_path)
        self.assertEqual(len(sent), 1)
        self.assertIn("exit=3", sent[0])
        cw.run(lambda s, b: sent.append(b) or True, now=NOW + 1800, crontab=cron, state_path=self.state_path)
        self.assertEqual(len(sent), 1)               # delivered once, pending cleared

    def test_monthly_log_resolution(self):
        self.wrapper("run_monthly.sh", "monthly", "%Y-%m")
        self.log("monthly", ts(2026, 10, 1), [f"[CRON] monthly started at {stamp(ts(2026, 10, 1, 1, 0, 2))}"], "%Y-%m")
        alerts = self.run_watch(f"0 1 1 * * {self.dir}/run_monthly.sh\n", now=ts(2026, 10, 1, 1, 30))
        self.assertEqual(alerts, [])

    def test_unmonitored_and_ignored_lines(self):
        cron = ("0 5 * * * /usr/bin/something --flag\n"
                '0 */4 * * * python3 -c "import hashlib" >> /dev/null 2>&1\n')
        alerts = self.run_watch(cron)
        self.assertEqual([a["kind"] for a in alerts], ["unmonitored"])
        self.assertIn("/usr/bin/something", alerts[0]["text"])

    def test_empty_crontab_alerts(self):
        alerts = self.run_watch("# only comments\n")
        self.assertEqual([a["key"] for a in alerts], ["crontab:unreadable"])

    def test_removed_job_alerts_once(self):
        self.wrapper()
        self.wrapper("run_other.sh", "other")
        self.log("monitor", NOW, [f"[CRON] m started at {stamp(ts(2026, 10, 12, 7, 0, 1))}"])
        both = f"0 7 * * * {self.dir}/run_monitor.sh\n0 7 * * * {self.dir}/run_other.sh\n"
        self.log("other", NOW, [f"[CRON] o started at {stamp(ts(2026, 10, 12, 7, 0, 1))}"])
        self.run_watch(both)
        alerts = self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n", now=NOW + 900)
        self.assertEqual([a["kind"] for a in alerts], ["crontab"])
        self.assertEqual(self.run_watch(f"0 7 * * * {self.dir}/run_monitor.sh\n", now=NOW + 1800), [])


class TestProgress(Base):
    CRON = None

    def setUp(self):
        super().setUp()
        self.wrapper()
        self.cron = f"0 7 * * * {self.dir}/run_monitor.sh\n"

    def _in_progress(self, start, mtime):
        p = self.log("monitor", NOW, [f"[CRON] m started at {stamp(start)}"])
        os.utime(p, (mtime, mtime))

    def test_active_run_ok(self):
        start = ts(2026, 10, 12, 7, 0, 3)
        self._in_progress(start, NOW - 300)
        self.assertEqual(self.run_watch(self.cron), [])

    def test_stall_uses_floor_without_history(self):
        start = ts(2026, 10, 12, 7, 0, 3)
        late = ts(2026, 10, 12, 8, 30)
        self._in_progress(start, late - 31 * 60)
        alerts = self.run_watch(self.cron, now=late)
        self.assertEqual([a["kind"] for a in alerts], ["stall"])
        self.assertIn("no history yet", alerts[0]["text"])

    def test_learned_window_is_longer_than_floor(self):
        hist = [{"start": INIT + i, "duration_s": 3600, "max_silence_s": 40 * 60} for i in range(3)]
        self.state_path.write_text(json.dumps({"initialized_at": INIT, "sent": {}, "known_job_keys": [],
                                               "jobs": {"run_monitor.sh": {"history": hist, "current": None,
                                                                           "recorded_starts": []}}}))
        start = ts(2026, 10, 12, 7, 0, 3)
        late = ts(2026, 10, 12, 9, 0)
        self._in_progress(start, late - 45 * 60)      # 45 min silent < 2 x 40 min
        self.assertEqual(self.run_watch(self.cron, now=late), [])
        self._in_progress(start, late - 85 * 60)      # > 80 min
        self.assertEqual([a["kind"] for a in self.run_watch(self.cron, now=late + 1)], ["stall"])

    def test_runaway_needs_history(self):
        hist = [{"start": INIT + i, "duration_s": 300, "max_silence_s": 10} for i in range(3)]
        self.state_path.write_text(json.dumps({"initialized_at": INIT, "sent": {}, "known_job_keys": [],
                                               "jobs": {"run_monitor.sh": {"history": hist, "current": None,
                                                                           "recorded_starts": []}}}))
        start = ts(2026, 10, 12, 7, 0, 3)
        self._in_progress(start, NOW - 60)            # still logging, but 30 min > 3 x 5 min
        alerts = self.run_watch(self.cron)
        self.assertEqual([a["kind"] for a in alerts], ["runaway"])

    def test_successful_run_enters_history(self):
        start = ts(2026, 10, 12, 7, 0, 3)
        self.log("monitor", NOW, [f"[CRON] m started at {stamp(start)}",
                                  f"[CRON] m finished at {stamp(start + 120)} | exit=0"])
        self.run_watch(self.cron)
        state = json.loads(self.state_path.read_text())
        hist = state["jobs"]["run_monitor.sh"]["history"]
        self.assertEqual(len(hist), 1)
        self.assertEqual(hist[0]["duration_s"], 120)


class TestState(Base):
    def test_corrupt_state_recovers(self):
        self.state_path.write_text("{not json")
        st = cw.load_state(self.state_path)
        self.assertEqual(st["jobs"], {})

    def test_run_safe_never_raises_and_emails_crash_once(self):
        orig_run, orig_flag = cw.run, cw.ERROR_FLAG
        cw.ERROR_FLAG = self.dir / "flag"
        cw.run = lambda send: (_ for _ in ()).throw(RuntimeError("boom"))
        try:
            cw.run_safe(self.send)
            cw.run_safe(self.send)
        finally:
            cw.run, cw.ERROR_FLAG = orig_run, orig_flag
        self.assertEqual(len(self.sent), 1)

    def test_crash_alert_retried_when_email_fails(self):
        orig_run, orig_flag = cw.run, cw.ERROR_FLAG
        cw.ERROR_FLAG = self.dir / "flag"
        cw.run = lambda send: (_ for _ in ()).throw(RuntimeError("boom"))
        calls = []
        try:
            cw.run_safe(lambda s, b: calls.append(1) or False)
            self.assertFalse(cw.ERROR_FLAG.exists())     # failed send must not arm the suppression flag
            cw.run_safe(lambda s, b: calls.append(1) or True)
            self.assertTrue(cw.ERROR_FLAG.exists())
        finally:
            cw.run, cw.ERROR_FLAG = orig_run, orig_flag
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
