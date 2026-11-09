"""scripts/logs_merge.py: merges per-command JSON logs into one time-ordered view."""
import json
import os
import re
from datetime import datetime

from scripts import logs_merge as lm


def _line(t, level, logger, msg, ms=0, pid=1):
    return json.dumps({"time": t, "tz": "+0000", "pid": pid, "ms": ms, "level": level, "logger": logger, "msg": msg})


def _setup(tmp_path):
    (tmp_path / "scheduler.log").write_text(
        _line("2026-11-08 10:00:01", "INFO", "sched", "LEVER 1 FIRED", ms=5) + "\n"
        "not json garbage\n"
        + _line("2026-11-08 10:00:09", "DEBUG", "sched", "tick") + "\n", encoding="utf-8")
    (tmp_path / "watchdog.log").write_text(
        _line("2026-11-08 10:00:03", "ERROR", "wd", "restart scheduler") + "\n", encoding="utf-8")
    (tmp_path / "pipeline_2026-11-08.log").write_text(
        _line("2026-11-08 10:00:02", "INFO", "x", "catchall dup") + "\n", encoding="utf-8")
    return str(tmp_path)


def test_merge_orders_by_time_across_files_and_skips_catchall(tmp_path):
    d = _setup(tmp_path)
    rows = lm.merge(d, datetime(2026, 11, 8, 0, 0))
    assert [r["msg"] for r in rows] == ["LEVER 1 FIRED", "restart scheduler", "tick"]
    assert [r["_src"] for r in rows] == ["scheduler.log", "watchdog.log", "scheduler.log"]


def test_filters_level_grep_only_and_window(tmp_path):
    d = _setup(tmp_path)
    since = datetime(2026, 11, 8, 0, 0)
    assert [r["msg"] for r in lm.merge(d, since, min_level=40)] == ["restart scheduler"]
    assert [r["msg"] for r in lm.merge(d, since, pattern=re.compile("lever", re.I))] == ["LEVER 1 FIRED"]
    assert [r["msg"] for r in lm.merge(d, since, only=["watchdog"])] == ["restart scheduler"]
    assert [r["msg"] for r in lm.merge(d, since, until=datetime(2026, 11, 8, 10, 0, 5))] == ["LEVER 1 FIRED", "restart scheduler"]


def test_old_files_by_mtime_are_not_read(tmp_path):
    d = _setup(tmp_path)
    old = datetime(2020, 1, 1).timestamp()
    os.utime(tmp_path / "watchdog.log", (old, old))
    assert [r["msg"] for r in lm.merge(d, datetime(2026, 11, 8, 0, 0))] == ["LEVER 1 FIRED", "tick"]


def test_parse_when_and_format():
    now = datetime(2026, 11, 8, 12, 0, 0)
    assert lm.parse_when("2h", now) == datetime(2026, 11, 8, 10, 0, 0)
    assert lm.parse_when("2026-11-08 09:30") == datetime(2026, 11, 8, 9, 30)
    line = lm.format_entry({"time": "t", "tz": "+0000", "level": "INFO", "_src": "a.log", "pid": 7,
                            "logger": "l", "msg": "m", "exc": "Trace\nline2"})
    assert line.startswith("t +0000 | INFO") and "\n    Trace\n    line2" in line
