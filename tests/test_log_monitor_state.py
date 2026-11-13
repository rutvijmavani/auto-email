"""
tests/test_log_monitor_state.py — log_monitor (path, inode) notebook, rotated-backup
matching, pruning, v1 migration and the seen-lines safety net.

Real files in a temp dir are used so inode numbers are the filesystem's own; tests that
need an inode to be reused rename a file onto the inode of another (POSIX semantics are
emulated by constructing notebook entries with the inode of a real file).
"""

import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

try:
    import fcntl  # noqa: F401  (real module on POSIX)
except ImportError:
    sys.modules["fcntl"] = types.ModuleType("fcntl")   # Windows: stub only when absent
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import scripts.log_monitor as lm

NOW = 1_800_000_000.0


def _err(msg, sec=0):
    return json.dumps({"time": f"2026-11-10 12:00:{sec:02d}", "pid": 1, "ms": 5.0,
                       "level": "ERROR", "logger": "x", "msg": msg}) + "\n"


class _Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.logs = Path(self._tmp.name)
        self._orig = lm.LOGS_DIR
        lm.LOGS_DIR = self.logs

    def tearDown(self):
        lm.LOGS_DIR = self._orig
        self._tmp.cleanup()

    def write(self, name, text=""):
        p = self.logs / name
        p.write_text(text)
        os.utime(p, (NOW, NOW))
        return p

    def key(self, p):
        return (str(p), p.stat().st_ino)

    def collect(self, entries):
        return lm.collect_raw_findings(entries, NOW)


class TestBackupMatching(_Base):

    def test_midnight_backup_takes_live_files_offset_not_stale_same_inode_entry(self):
        # The proven incident: dead build_email_patterns entry shares the backup's inode.
        live = self.write("manager.log", _err("a") + _err("b", 1))
        entries, _, _ = self.collect({})
        ino = live.stat().st_ino
        entries[("/x/build_email_patterns_2026-10-11.log", ino)] = {"offset": 9_999_999, "scanned_at": 1.0}
        entries[(str(live), ino)]["scanned_at"] = NOW - 900

        # midnight: live -> backup (same inode), new live file
        backup = self.logs / "manager.log.2026-11-10"
        live.rename(backup)
        with open(backup, "a") as fh:
            fh.write(_err("late", 2))
        os.utime(backup, (NOW, NOW))        # after the append, which bumps mtime
        self.write("manager.log", "")

        new, raw, _ = self.collect(entries)
        self.assertEqual([l for l, _ in raw["manager.log.2026-11-10"]],
                         [_err("late", 2).rstrip()])          # only the tail, no rescan
        self.assertEqual(new[self.key(backup)]["offset"], backup.stat().st_size)

    def test_prefix_mismatch_is_never_a_candidate(self):
        backup = self.write("manager.log.2026-11-10", _err("a"))
        entries = {("/x/pipeline_2026-07-17.log", backup.stat().st_ino): {"offset": 1, "scanned_at": NOW}}
        self.assertIsNone(lm._find_predecessor(entries, backup, backup.stat().st_ino,
                                               backup.stat().st_size, set()))

    def test_offset_beyond_backup_size_rejected(self):
        backup = self.write("manager.log.2026-11-10", _err("a"))
        ino = backup.stat().st_ino
        entries = {("/x/manager.log.2026-10-27", ino): {"offset": 10**7, "scanned_at": NOW}}
        self.assertIsNone(lm._find_predecessor(entries, backup, ino, backup.stat().st_size, set()))

    def test_newest_scanned_at_wins_among_same_family(self):
        backup = self.write("manager.log.2026-11-10", _err("a") * 3)
        ino = backup.stat().st_ino
        entries = {
            ("/x/manager.log.2026-10-27", ino): {"offset": 10, "scanned_at": 100.0},
            ("/x/manager.log", ino):            {"offset": 20, "scanned_at": 200.0},
        }
        self.assertEqual(lm._find_predecessor(entries, backup, ino, backup.stat().st_size, set()),
                         ("/x/manager.log", ino))

    def test_late_rotation_name_date_irrelevant(self):
        # watchdog-style: backup is named for yesterday although rotated hours later
        backup = self.write("watchdog.log.2026-11-10", _err("a") + _err("b", 1))
        ino = backup.stat().st_ino
        entries = {("/x/watchdog.log", ino): {"offset": len(_err("a")), "scanned_at": NOW - 60}}
        new, raw, _ = self.collect(entries)
        self.assertEqual(len(raw["watchdog.log.2026-11-10"]), 1)

    def test_two_backups_after_downtime_each_find_own_predecessor(self):
        a = self.write("manager.log.2026-11-09", _err("a"))
        b = self.write("manager.log.2026-11-10", _err("b"))
        ia, ib = a.stat().st_ino, b.stat().st_ino
        entries = {
            ("/x/manager.log", ia): {"offset": a.stat().st_size, "scanned_at": NOW - 5000},
            ("/x/manager.log", ib): {"offset": 0, "scanned_at": NOW - 100},
        }
        new, raw, _ = self.collect(entries)
        self.assertNotIn("manager.log.2026-11-09", raw)        # fully read already
        self.assertEqual(len(raw["manager.log.2026-11-10"]), 1)

    def test_no_candidate_starts_at_zero(self):
        self.write("manager.log.2026-11-10", _err("a"))
        new, raw, _ = self.collect({})
        self.assertEqual(len(raw["manager.log.2026-11-10"]), 1)


class TestExactMatchAndPrune(_Base):

    def test_exact_match_continues_from_offset(self):
        p = self.write("scheduler.log", _err("a"))
        e1, raw1, _ = self.collect({})
        self.assertEqual(len(raw1["scheduler.log"]), 1)
        with open(p, "a") as fh:
            fh.write(_err("b", 1))
        e2, raw2, _ = self.collect(e1)
        self.assertEqual(len(raw2["scheduler.log"]), 1)

    def test_replaced_live_file_new_inode_starts_at_zero(self):
        p = self.write("scheduler.log", _err("a"))
        e1, _, _ = self.collect({})
        p.unlink()
        self.write("scheduler.log", _err("z") + _err("y", 1))
        if self.key(p) in e1:       # filesystem reused the inode: nothing to assert
            self.skipTest("inode reused by the filesystem")
        _, raw, _ = self.collect(e1)
        self.assertEqual(len(raw["scheduler.log"]), 2)

    def test_prune_removes_entries_for_missing_files_keeps_existing(self):
        p = self.write("scheduler.log", _err("a"))
        e1, _, _ = self.collect({})
        e1[("/gone/old.log", 123)] = {"offset": 5, "scanned_at": 1.0}
        e2, _, _ = self.collect(e1)
        self.assertNotIn(("/gone/old.log", 123), e2)
        self.assertIn(self.key(p), e2)

    def test_prune_keeps_aged_out_file_that_still_exists(self):
        old = self.write("old.log", _err("a"))
        os.utime(old, (NOW - 10 * 86400, NOW - 10 * 86400))     # outside the 48 h window
        entries = {self.key(old): {"offset": 7, "scanned_at": 1.0}}
        new, _, _ = self.collect(entries)
        self.assertEqual(new[self.key(old)]["offset"], 7)

    def test_match_happens_before_prune(self):
        # (manager.log, X) no longer exists as a path+inode after the rename, yet the
        # backup must still find it.  Covered by the midnight test above; here directly:
        backup = self.write("manager.log.2026-11-10", _err("a") + _err("b", 1))
        ino = backup.stat().st_ino
        entries = {("/x/manager.log", ino): {"offset": len(_err("a")), "scanned_at": NOW - 10}}
        new, raw, _ = self.collect(entries)
        self.assertEqual(len(raw["manager.log.2026-11-10"]), 1)
        self.assertNotIn(("/x/manager.log", ino), new)


class TestStateFile(_Base):

    def setUp(self):
        super().setUp()
        self._orig_state = lm.STATE_FILE
        lm.STATE_FILE = self.logs / "state.json"

    def tearDown(self):
        lm.STATE_FILE = self._orig_state
        super().tearDown()

    def test_v1_migration_pairs_offsets_and_inodes(self):
        lm.STATE_FILE.write_text(json.dumps({
            "offsets": {"/l/manager.log": 100, "/l/noinode.log": 5},
            "inodes":  {"/l/manager.log": 42},
        }))
        e = lm._load_state()
        self.assertEqual(e[("/l/manager.log", 42)], {"offset": 100, "scanned_at": 0.0})
        self.assertEqual(e[("/l/noinode.log", 0)]["offset"], 5)

    def test_roundtrip(self):
        e = {("/l/a.log", 7): {"offset": 9, "scanned_at": 12.5}}
        lm._save_state(e)
        self.assertEqual(lm._load_state(), e)

    def test_corrupt_state_starts_empty(self):
        lm.STATE_FILE.write_text("{not json")
        self.assertEqual(lm._load_state(), {})


class _FakeRedis:
    def __init__(self):
        self.kv = {}

    def exists(self, k):
        return k in self.kv

    def set(self, k, v, ex=None):
        self.kv[k] = v

    def get(self, k):
        return self.kv.get(k)

    def ttl(self, k):
        return 100

    def lpush(self, *a):
        pass

    def ltrim(self, *a):
        pass

    def expire(self, *a):
        pass

    def lrange(self, *a):
        return []


class TestSeenLines(unittest.TestCase):

    def _raw(self, line):
        return {"manager.log": [(line.rstrip(), [])]}

    def test_reread_after_mark_seen_is_skipped(self):
        r, line = _FakeRedis(), _err("boom")
        seen = []
        first = lm.process_findings(r, self._raw(line), NOW, seen)
        self.assertEqual(len(first), 1)
        lm.mark_seen(r, seen)
        r.kv = {k: v for k, v in r.kv.items() if k.startswith(lm._PFX_SEEN)}   # err keys gone (swept)
        again = lm.process_findings(r, self._raw(line), NOW + 100, [])
        self.assertEqual(again, {})

    def test_not_marked_until_committed(self):
        r, line = _FakeRedis(), _err("boom")
        lm.process_findings(r, self._raw(line), NOW, [])        # run failed: never mark_seen
        r.kv.clear()
        again = lm.process_findings(r, self._raw(line), NOW + 100, [])
        self.assertEqual(len(again), 1)

    def test_untimestamped_line_never_tracked(self):
        r = _FakeRedis()
        raw = {"x.log": [("Traceback (most recent call last):", ["  File x"])]}
        seen = []
        lm.process_findings(r, raw, NOW, seen)
        self.assertEqual(seen, [])
        self.assertFalse(lm._is_timestamped("Traceback (most recent call last):"))
        self.assertTrue(lm._is_timestamped("2026-09-20 15:00:00.000 | ERROR | a | b"))


if __name__ == "__main__":
    unittest.main()
