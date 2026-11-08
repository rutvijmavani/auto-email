"""RO / F-ROTRACE: several processes' handlers on one log file must rotate it exactly once."""
import logging
import os
import time

import pytest

from logger import SharedTimedRotatingFileHandler


def _rec(msg):
    return logging.LogRecord("t", logging.INFO, __file__, 1, msg, None, None)


def _write(h, msg):
    h.acquire()
    try:
        if h.shouldRollover(_rec(msg)):
            h.doRollover()
        h.stream.write(msg + "\n")
        h.stream.flush()
    finally:
        h.release()


@pytest.mark.skipif(os.name == "nt", reason="renaming a file another handler holds open is POSIX-only (production is Linux)")
def test_two_handlers_rotate_once_and_keep_all_lines(tmp_path):
    base = tmp_path / "scheduler.log"
    a = SharedTimedRotatingFileHandler(base, when="midnight", backupCount=0, encoding="utf-8")
    b = SharedTimedRotatingFileHandler(base, when="midnight", backupCount=0, encoding="utf-8")
    try:
        _write(a, "a-old")
        _write(b, "b-old")
        # Pretend midnight passed: the file was last written before the boundary.
        boundary = int(time.time()) - 5
        a.rolloverAt = b.rolloverAt = boundary
        old = boundary - 3600
        os.utime(base, (old, old))

        _write(a, "a-new")      # A rotates
        _write(b, "b-new")      # B must NOT rename A's fresh file, and must reopen it
        _write(a, "a-new2")

        backups = [p for p in tmp_path.iterdir() if p.name.startswith("scheduler.log.") and p.suffix != ".lock"
                   and not p.name.endswith(".lock")]
        assert len(backups) == 1
        assert backups[0].read_text(encoding="utf-8").split() == ["a-old", "b-old"]
        assert base.read_text(encoding="utf-8").split() == ["a-new", "b-new", "a-new2"]
    finally:
        a.close()
        b.close()
