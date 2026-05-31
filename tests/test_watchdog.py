"""
tests/test_watchdog.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/watchdog.py — queue velocity tracking, heartbeat liveness,
PEL consumer-PID checking, and stall-signal logic.

Phase 3 update
──────────────
Several functions were refactored or replaced in Phase 3:
  - check_hung_workers(r) now takes `r` as argument and returns Issue objects
  - check_pel_stats() was replaced by _check_pel_health() (private helper inside
    check_queue_health) with PID-based consumer liveness
  - check_queue_health() now uses velocity/delta tracking across cycles instead
    of absolute overdue counts
  - check_worker_heartbeats() detects two failure modes: missing key AND stale ts
  - WATCHDOG_INTERVAL_S = 300 (5 min), not 60

Coverage map
────────────
  TestCheckHungWorkers
    · No heartbeat keys → empty issues list
    · Heartbeat alive + progress exists → not hung
    · Heartbeat alive + progress missing → WARNING issue
    · Returns Issue object (not raw company string)
    · Multiple heartbeats: only missing-progress ones flagged

  TestWatchdogConstants
    · WATCHDOG_INTERVAL_S = 300 (5 minutes)
    · PEL_WARN_AGE_MS ≥ 60_000 (at least 1 minute)
    · HEARTBEAT_DEAD_AFTER contains all 4 worker types
    · HEARTBEAT_DEAD_AFTER thresholds are positive

  TestWatchdogHelperFunctions
    · _consumer_pid("worker-host-12345") → 12345
    · _consumer_pid("bad-format") → None
    · _consumer_pid(bytes) → None (bytes not supported; call site always decodes first)
    · _trend(-5) → "↓"
    · _trend(0)  → "→"
    · _trend(3)  → "↑"
    · _worker_processed: key missing → None
    · _worker_processed: key present → int count
    · _worker_processed: unparseable → None

  TestCheckWorkerHeartbeatsStale  (Phase 3 — two failure modes)
    · Key missing → ERROR ("MISSING" in issue.message)
    · Key present, ts fresh → OK
    · Key present, ts stale (age > threshold) → ERROR ("STALE" in issue.message)
    · Key present, unparseable JSON → OK (key exists, treat alive)
    · Correct thresholds per worker type (scheduler=20, scan=45, fullscan=1900)

  TestStallCount  (Phase 3 — pure stall signal logic)
    · All inputs None → (0, 0)
    · Signal 1: overdue == 0 → not counted
    · Signal 1: overdue > 0, not shrinking → stalls
    · Signal 1: overdue > 0, shrinking → progresses
    · Signal 2: same company + same score → stalls
    · Signal 2: different company → progresses
    · Signal 2: score changed ≥ 1.0 → progresses
    · Signal 2: score changed < 1.0 → unchanged → stalls
    · Signal 2: prev head None → not counted
    · Signal 3: proc unchanged → stalls
    · Signal 3: proc increased → progresses
    · Signal 3: prev proc None → not counted
    · All 3 stalling → (3, 3)
    · All 3 progressing → (0, 3)
    · 2 stalling, 1 progressing → (2, 3)

  TestCheckQueueHealthVelocity  (Phase 3 — snapshot-based stall detection)
    · No prior snapshot → baseline cycle, all issues are OK, snapshot written
    · Empty adaptive queue → ERROR regardless of velocity
    · 3/3 stall signals → ERROR STALL
    · 2/3 stall signals → WARNING DEGRADED
    · 0 stall signals → OK (making progress)
    · Fullscan lock active → stall signals suppressed for fullscan queue
    · Overdue == 0 → OK regardless of processed count
    · Detail queue depth=0 → OK/idle
    · Detail queue draining (delta<0) → OK even if elevated
    · Detail queue stalled at ALERT level → ERROR

  TestCheckPelHealthPID  (Phase 3 — consumer liveness, not just time)
    · 0 pending entries → OK
    · Consumer alive (same PID) → OK regardless of entry age
    · Consumer dead, age < WARN_AGE → OK (XAUTOCLAIM will handle)
    · Consumer dead, age > WARN_AGE but < ALERT_AGE → WARNING
    · Consumer dead, age > ALERT_AGE → ERROR
    · Consumer name doesn't parse to PID → treated as dead
    · xpending raises exception → WARNING logged, no crash

  TestIssueClass  (Phase 3 — Issue data class contract)
    · Level constants: OK, WARNING, ERROR, CRITICAL
    · Fields stored correctly: level, category, message, fix, alert_type
    · is_alertable() → True for ERROR/CRITICAL, False for OK/WARNING
    · emoji() → correct emoji per level (🟢 🟡 🔴)
    · __str__() includes level, category, message
    · __str__() appends fix line only when fix is non-empty
    · alert_type auto-derived from category (: and space → _)
    · alert_type uses provided value when explicitly given

  TestAdditionalHeartbeatThresholds  (Phase 3 — per-worker thresholds)
    · scheduler OK at 19s, STALE at 21s (threshold=20s)
    · detail_worker OK at 44s, STALE at 46s (threshold=45s)
    · fullscan_worker OK at 1899s (threshold=1900s)
    · All 4 worker types checked (4 issues returned)
"""

import sys
import os
import time
import json
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _hb_payload(pid=1234, ts=None, processed=10):
    """Build a heartbeat JSON payload."""
    return json.dumps({"pid": pid, "ts": ts or time.time(), "processed": processed})


def _make_redis(**overrides):
    """
    Build a minimal MagicMock Redis for watchdog tests.
    Override individual methods via kwargs.
    """
    r = MagicMock()
    for k, v in overrides.items():
        setattr(r, k, v)
    return r


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckHungWorkers
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckHungWorkers(unittest.TestCase):
    """check_hung_workers(r) — heartbeat-alive but no progress key = hung."""

    def _run(self, alive_heartbeats=None, progress_exists=None):
        r = MagicMock()
        hb_keys = [f"heartbeat:{c}".encode() for c in (alive_heartbeats or [])]
        r.scan.side_effect = [(0, hb_keys)]

        progress_map = progress_exists or {}
        def _exists(key):
            if key.startswith("progress:"):
                return int(progress_map.get(key.split(":", 1)[1], False))
            return 0
        r.exists.side_effect = _exists

        from workers.watchdog import check_hung_workers
        return check_hung_workers(r)

    def test_no_heartbeats_returns_empty(self):
        """No heartbeat keys → empty list."""
        self.assertEqual(self._run(alive_heartbeats=[]), [])

    def test_alive_with_progress_not_hung(self):
        """Heartbeat + progress key → not hung → empty list."""
        result = self._run(
            alive_heartbeats=["CompanyA"],
            progress_exists={"CompanyA": True},
        )
        self.assertEqual(result, [])

    def test_alive_without_progress_returns_warning_issue(self):
        """Heartbeat alive, progress key gone → WARNING Issue returned."""
        from workers.watchdog import Issue
        result = self._run(
            alive_heartbeats=["CompanyA"],
            progress_exists={"CompanyA": False},
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].level, Issue.WARNING)

    def test_issue_mentions_company_name(self):
        """Warning message includes the hung company name."""
        result = self._run(
            alive_heartbeats=["MyCompany"],
            progress_exists={"MyCompany": False},
        )
        self.assertTrue(
            any("MyCompany" in str(i) for i in result),
            "Expected 'MyCompany' in issue output",
        )

    def test_multiple_only_hung_ones_flagged(self):
        """Only companies with missing progress appear in warning."""
        from workers.watchdog import Issue
        result = self._run(
            alive_heartbeats=["Co1", "Co2", "Co3"],
            progress_exists={"Co1": True, "Co2": False, "Co3": True},
        )
        self.assertEqual(len(result), 1)
        self.assertIn("Co2", str(result[0]))


# ─────────────────────────────────────────────────────────────────────────────
# TestWatchdogConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestWatchdogConstants(unittest.TestCase):

    def test_watchdog_interval_is_300(self):
        """WATCHDOG_INTERVAL_S = 300 (5 minutes between checks)."""
        from workers.watchdog import WATCHDOG_INTERVAL_S
        self.assertEqual(WATCHDOG_INTERVAL_S, 300)

    def test_watchdog_interval_positive(self):
        from workers.watchdog import WATCHDOG_INTERVAL_S
        self.assertGreater(WATCHDOG_INTERVAL_S, 0)

    def test_pel_warn_age_ms_at_least_one_minute(self):
        from workers.watchdog import PEL_WARN_AGE_MS
        self.assertGreaterEqual(PEL_WARN_AGE_MS, 60_000)

    def test_heartbeat_dead_after_has_all_workers(self):
        from workers.watchdog import HEARTBEAT_DEAD_AFTER
        for w in ("scheduler", "scan_worker", "detail_worker", "fullscan_worker"):
            self.assertIn(w, HEARTBEAT_DEAD_AFTER,
                          f"Missing worker type: {w}")

    def test_heartbeat_dead_after_all_positive(self):
        from workers.watchdog import HEARTBEAT_DEAD_AFTER
        for w, v in HEARTBEAT_DEAD_AFTER.items():
            self.assertGreater(v, 0, f"{w} threshold must be > 0")

    def test_fullscan_threshold_generous(self):
        """fullscan_worker threshold ≥ 1800 s (scans take up to 30 min)."""
        from workers.watchdog import HEARTBEAT_DEAD_AFTER
        self.assertGreaterEqual(HEARTBEAT_DEAD_AFTER["fullscan_worker"], 1800)


# ─────────────────────────────────────────────────────────────────────────────
# TestWatchdogHelperFunctions  (Phase 3 — pure/near-pure helpers)
# ─────────────────────────────────────────────────────────────────────────────

class TestWatchdogHelperFunctions(unittest.TestCase):

    # _consumer_pid ──────────────────────────────────────────────────────────���─

    def test_consumer_pid_valid_format(self):
        from workers.watchdog import _consumer_pid
        self.assertEqual(_consumer_pid("worker-myhost-12345"), 12345)

    def test_consumer_pid_multi_segment_hostname(self):
        from workers.watchdog import _consumer_pid
        self.assertEqual(_consumer_pid("worker-host-name-99"), 99)

    def test_consumer_pid_bytes_input(self):
        # _consumer_pid is typed as `str`.  _check_pel_health always decodes bytes
        # before calling it, so bytes never reach this function in production.
        # Passing raw bytes is unsupported: str(b"...") wraps the repr, causing
        # int() to fail, and the function returns None.
        from workers.watchdog import _consumer_pid
        self.assertIsNone(_consumer_pid(b"worker-host-42"))

    def test_consumer_pid_no_trailing_int(self):
        from workers.watchdog import _consumer_pid
        self.assertIsNone(_consumer_pid("worker-host-abc"))

    def test_consumer_pid_empty_string(self):
        from workers.watchdog import _consumer_pid
        self.assertIsNone(_consumer_pid(""))

    # _trend ───────────────────────────────────────────────────────────────────

    def test_trend_negative_is_down_arrow(self):
        from workers.watchdog import _trend
        self.assertEqual(_trend(-5), "↓")

    def test_trend_zero_is_right_arrow(self):
        from workers.watchdog import _trend
        self.assertEqual(_trend(0), "→")

    def test_trend_positive_is_up_arrow(self):
        from workers.watchdog import _trend
        self.assertEqual(_trend(3), "↑")

    # _worker_processed ────────────────────────────────────────────────────────

    def test_worker_processed_key_missing(self):
        from workers.watchdog import _worker_processed
        r = MagicMock()
        r.get.return_value = None
        self.assertIsNone(_worker_processed(r, "scan_worker"))

    def test_worker_processed_key_present(self):
        from workers.watchdog import _worker_processed
        r = MagicMock()
        r.get.return_value = json.dumps({"pid": 1, "ts": time.time(), "processed": 42})
        self.assertEqual(_worker_processed(r, "scan_worker"), 42)

    def test_worker_processed_unparseable_returns_none(self):
        from workers.watchdog import _worker_processed
        r = MagicMock()
        r.get.return_value = b"not-json"
        self.assertIsNone(_worker_processed(r, "scan_worker"))


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckWorkerHeartbeatsStale  (Phase 3 — two failure modes)
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckWorkerHeartbeatsStale(unittest.TestCase):
    """
    check_worker_heartbeats() detects:
      1. Key MISSING  → ERROR ("MISSING" in message)
      2. Key PRESENT but ts too old → ERROR ("STALE" in message)
    """

    def _run(self, get_map):
        """
        get_map: {key: value_or_None} — controls what r.get returns.
        """
        r = MagicMock()
        r.get.side_effect = lambda k: get_map.get(k)
        with patch("workers.watchdog.time.time", return_value=1_700_000_000.0):
            from workers.watchdog import check_worker_heartbeats
            return check_worker_heartbeats(r)

    def _issues_for(self, issues, worker_type):
        return [i for i in issues if worker_type in i.category]

    def test_key_missing_produces_error(self):
        from workers.watchdog import Issue
        issues = self._run({"worker:alive:scan_worker": None})
        worker_issues = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.ERROR for i in worker_issues))
        self.assertTrue(any("MISSING" in i.message for i in worker_issues))

    def test_key_present_ts_fresh_produces_ok(self):
        from workers.watchdog import Issue, HEARTBEAT_DEAD_AFTER
        now = 1_700_000_000.0
        # ts = now - 5 s → well within 45s threshold
        payload = json.dumps({"pid": 1, "ts": now - 5, "processed": 0})
        issues = self._run({"worker:alive:scan_worker": payload.encode()})
        worker_issues = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.OK for i in worker_issues))

    def test_key_present_ts_stale_produces_error(self):
        from workers.watchdog import Issue, HEARTBEAT_DEAD_AFTER
        now = 1_700_000_000.0
        threshold = HEARTBEAT_DEAD_AFTER["scan_worker"]  # 45 s
        # ts = now - (threshold + 10) → stale
        payload = json.dumps({"pid": 1, "ts": now - (threshold + 10), "processed": 0})
        issues = self._run({"worker:alive:scan_worker": payload.encode()})
        worker_issues = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.ERROR for i in worker_issues))
        self.assertTrue(any("STALE" in i.message for i in worker_issues))

    def test_unparseable_json_treated_as_ok(self):
        from workers.watchdog import Issue
        issues = self._run({"worker:alive:scan_worker": b"bad-json"})
        worker_issues = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.OK for i in worker_issues))

    def test_fullscan_threshold_is_generous(self):
        """fullscan_worker stays OK up to 1900 s stale (scans take 20-30 min)."""
        from workers.watchdog import Issue, HEARTBEAT_DEAD_AFTER
        now = 1_700_000_000.0
        threshold = HEARTBEAT_DEAD_AFTER["fullscan_worker"]  # 1900 s
        # ts = now - (threshold - 10) → still within threshold → OK
        payload = json.dumps({"pid": 1, "ts": now - (threshold - 10), "processed": 0})
        issues = self._run({"worker:alive:fullscan_worker": payload.encode()})
        worker_issues = self._issues_for(issues, "fullscan_worker")
        self.assertTrue(any(i.level == Issue.OK for i in worker_issues))


# ─────────────────────────────────────────────────────────────────────────────
# TestStallCount  (Phase 3 — pure stall signal logic)
# ─────────────────────────────────────────────────────────────────────────────

class TestStallCount(unittest.TestCase):

    def _call(self, cur_ov=0, prev_ov=0,
              cur_hc=None, cur_hs=None,
              prev_hc=None, prev_hs=None,
              cur_p=None, prev_p=None):
        from workers.watchdog import _stall_count
        return _stall_count(cur_ov, prev_ov, cur_hc, cur_hs,
                            prev_hc, prev_hs, cur_p, prev_p)

    def test_all_none_returns_zero_zero(self):
        self.assertEqual(self._call(), (0, 0))

    # Signal 1 — overdue count ─────────────────────────────────────────────────

    def test_s1_overdue_zero_not_counted(self):
        stall, valid = self._call(cur_ov=0, prev_ov=0)
        self.assertEqual(valid, 0)
        self.assertEqual(stall, 0)

    def test_s1_not_shrinking_stalls(self):
        stall, valid = self._call(cur_ov=5, prev_ov=5)
        self.assertEqual(valid, 1)
        self.assertEqual(stall, 1)

    def test_s1_growing_stalls(self):
        stall, valid = self._call(cur_ov=7, prev_ov=5)
        self.assertEqual(stall, 1)

    def test_s1_shrinking_no_stall(self):
        stall, valid = self._call(cur_ov=4, prev_ov=5)
        self.assertEqual(valid, 1)
        self.assertEqual(stall, 0)

    # Signal 2 — queue head ────────────────────────────────────────────────────

    def test_s2_same_company_same_score_stalls(self):
        stall, valid = self._call(
            cur_hc="Acme",   cur_hs=1_000_000.0,
            prev_hc="Acme",  prev_hs=1_000_000.0,
        )
        self.assertEqual(valid, 1)
        self.assertEqual(stall, 1)

    def test_s2_different_company_no_stall(self):
        stall, valid = self._call(
            cur_hc="Stripe", cur_hs=1_000_000.0,
            prev_hc="Acme",  prev_hs=1_000_000.0,
        )
        self.assertEqual(stall, 0)

    def test_s2_score_changed_gt_1_no_stall(self):
        stall, _ = self._call(
            cur_hc="Acme", cur_hs=1_000_002.0,
            prev_hc="Acme", prev_hs=1_000_000.0,
        )
        self.assertEqual(stall, 0)

    def test_s2_score_changed_lt_1_stalls(self):
        stall, _ = self._call(
            cur_hc="Acme", cur_hs=1_000_000.5,
            prev_hc="Acme", prev_hs=1_000_000.0,
        )
        self.assertEqual(stall, 1)

    def test_s2_prev_head_none_not_counted(self):
        _, valid = self._call(cur_hc="Acme", cur_hs=1_000_000.0)
        self.assertEqual(valid, 0)

    # Signal 3 — processed count ───────────────────────────────────────────────

    def test_s3_unchanged_stalls(self):
        stall, valid = self._call(cur_p=100, prev_p=100)
        self.assertEqual(valid, 1)
        self.assertEqual(stall, 1)

    def test_s3_increased_no_stall(self):
        stall, valid = self._call(cur_p=105, prev_p=100)
        self.assertEqual(valid, 1)
        self.assertEqual(stall, 0)

    def test_s3_prev_none_not_counted(self):
        _, valid = self._call(cur_p=100, prev_p=None)
        self.assertEqual(valid, 0)

    # Combined ─────────────────────────────────────────────────────────────────

    def test_all_3_stalling(self):
        stall, valid = self._call(
            cur_ov=5, prev_ov=5,
            cur_hc="Acme", cur_hs=1_000_000.0,
            prev_hc="Acme", prev_hs=1_000_000.0,
            cur_p=100, prev_p=100,
        )
        self.assertEqual(stall, 3)
        self.assertEqual(valid, 3)

    def test_all_3_progressing(self):
        stall, valid = self._call(
            cur_ov=3, prev_ov=5,
            cur_hc="Stripe", cur_hs=1_000_000.0,
            prev_hc="Acme",  prev_hs=1_000_000.0,
            cur_p=105, prev_p=100,
        )
        self.assertEqual(stall, 0)
        self.assertEqual(valid, 3)

    def test_2_stalling_1_progressing(self):
        stall, valid = self._call(
            cur_ov=5,  prev_ov=5,          # stall
            cur_hc="Acme", cur_hs=1_000_000.0,
            prev_hc="Acme", prev_hs=1_000_000.0,  # stall
            cur_p=105, prev_p=100,         # progress
        )
        self.assertEqual(stall, 2)
        self.assertEqual(valid, 3)


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckQueueHealthVelocity  (Phase 3 — snapshot-based stall detection)
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckQueueHealthVelocity(unittest.TestCase):
    """
    check_queue_health(r) uses a snapshot stored in Redis between cycles to
    detect whether queues are making forward progress.
    """

    _NOW = 1_700_000_000.0

    def _make_r(self, snap=None, adp_total=10, adp_overdue=2,
                adp_head=("Acme", 1_699_990_000.0),
                fs_total=10, fs_overdue=2,
                fs_head=("BigCorp", 1_699_980_000.0),
                fs_lock=False, detail_adp=0, detail_fs=0,
                scan_proc=50, fs_proc=5, detail_proc=200,
                xpending_total=0):
        """
        Build a Redis mock for check_queue_health().
        snap=None → no prior snapshot (baseline cycle).
        snap=dict → previous snapshot JSON.
        """
        from config import (
            REDIS_POLL_ADAPTIVE, REDIS_POLL_FULLSCAN,
            REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN,
            REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN,
            STREAM_CONSUMER_GROUP,
        )
        from workers.watchdog import WATCHDOG_SNAPSHOT_KEY

        r = MagicMock()

        # zcard
        def _zcard(key):
            if key == REDIS_POLL_ADAPTIVE:  return adp_total
            if key == REDIS_POLL_FULLSCAN:  return fs_total
            return 0
        r.zcard.side_effect = _zcard

        # zcount
        def _zcount(key, lo, hi):
            if key == REDIS_POLL_ADAPTIVE:  return adp_overdue
            if key == REDIS_POLL_FULLSCAN:  return fs_overdue
            return 0
        r.zcount.side_effect = _zcount

        # zrange (queue head)
        def _zrange(key, start, end, withscores=False):
            if key == REDIS_POLL_ADAPTIVE and adp_head:
                return [(adp_head[0].encode(), adp_head[1])]
            if key == REDIS_POLL_FULLSCAN and fs_head:
                return [(fs_head[0].encode(), fs_head[1])]
            return []
        r.zrange.side_effect = _zrange

        # scan (fullscan lock)
        r.scan.return_value = (0, [b"fullscan:lock:Co"] if fs_lock else [])

        # llen (detail queues)
        def _llen(key):
            if key == REDIS_DETAIL_ADAPTIVE: return detail_adp
            if key == REDIS_DETAIL_FULLSCAN: return detail_fs
            return 0
        r.llen.side_effect = _llen

        # get (heartbeats + snapshot)
        now = self._NOW
        def _get(key):
            if key == WATCHDOG_SNAPSHOT_KEY:
                return json.dumps(snap).encode() if snap else None
            if key == "worker:alive:scan_worker":
                return json.dumps({"pid": 1, "ts": now - 5, "processed": scan_proc})
            if key == "worker:alive:fullscan_worker":
                return json.dumps({"pid": 2, "ts": now - 5, "processed": fs_proc})
            if key == "worker:alive:detail_worker":
                return json.dumps({"pid": 3, "ts": now - 5, "processed": detail_proc})
            return None
        r.get.side_effect = _get

        # xpending (PEL — empty by default so PEL tests don't interfere)
        r.xpending.return_value = {"pending": xpending_total}

        return r

    def _run(self, **kwargs):
        r = self._make_r(**kwargs)
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import check_queue_health
            return check_queue_health(r), r

    def _level(self, issues, category):
        for i in issues:
            if category in i.category:
                return i.level
        return None

    # ── Baseline cycle ────────────────────────────────────────────────────────

    def test_no_prior_snapshot_baseline_all_ok(self):
        from workers.watchdog import Issue
        issues, _ = self._run(snap=None)
        adp_level = self._level(issues, "poll:adaptive")
        self.assertEqual(adp_level, Issue.OK)

    def test_no_prior_snapshot_writes_new_snapshot(self):
        _, r = self._run(snap=None)
        r.set.assert_called()
        set_key = r.set.call_args[0][0]
        from workers.watchdog import WATCHDOG_SNAPSHOT_KEY
        self.assertEqual(set_key, WATCHDOG_SNAPSHOT_KEY)

    # ── Empty queue ───────────────────────────────────────────────────────────

    def test_empty_adaptive_queue_always_error(self):
        from workers.watchdog import Issue
        snap = {"adp_total": 10, "adp_overdue": 2, "adp_head_c": "Acme",
                "adp_head_s": 1_699_990_000.0, "fs_total": 10, "fs_overdue": 1,
                "fs_head_c": "Co", "fs_head_s": 1.0, "detail_adp_depth": 0,
                "detail_fs_depth": 0, "scan_proc": 50, "fs_proc": 5, "detail_proc": 200}
        issues, _ = self._run(snap=snap, adp_total=0)
        self.assertEqual(self._level(issues, "poll:adaptive"), Issue.ERROR)

    # ── Stall detection ───────────────────────────────────────────────────────

    def test_three_stall_signals_produces_error(self):
        """All 3 signals agree: nothing moved → ERROR STALL."""
        from workers.watchdog import Issue
        snap = {
            "adp_total": 10, "adp_overdue": 5,   # overdue same → stall s1
            "adp_head_c": "Acme", "adp_head_s": 1_699_990_000.0,  # head same → stall s2
            "fs_total": 10, "fs_overdue": 1, "fs_head_c": "Co", "fs_head_s": 1.0,
            "detail_adp_depth": 0, "detail_fs_depth": 0,
            "scan_proc": 50,    # proc same → stall s3
            "fs_proc": 5, "detail_proc": 200,
        }
        issues, _ = self._run(
            snap=snap,
            adp_overdue=5,           # same as snap → not shrinking
            adp_head=("Acme", 1_699_990_000.0),   # same head
            scan_proc=50,            # same processed count
        )
        self.assertEqual(self._level(issues, "poll:adaptive"), Issue.ERROR)

    def test_two_stall_signals_produces_warning(self):
        """2/3 signals stalling → WARNING DEGRADED."""
        from workers.watchdog import Issue
        snap = {
            "adp_total": 10, "adp_overdue": 5,
            "adp_head_c": "Acme", "adp_head_s": 1_699_990_000.0,
            "fs_total": 10, "fs_overdue": 1, "fs_head_c": "Co", "fs_head_s": 1.0,
            "detail_adp_depth": 0, "detail_fs_depth": 0,
            "scan_proc": 50, "fs_proc": 5, "detail_proc": 200,
        }
        issues, _ = self._run(
            snap=snap,
            adp_overdue=5,            # same → stall s1
            adp_head=("Acme", 1_699_990_000.0),  # same → stall s2
            scan_proc=55,             # INCREASED → s3 progresses
        )
        self.assertEqual(self._level(issues, "poll:adaptive"), Issue.WARNING)

    def test_queue_making_progress_is_ok(self):
        """Overdue shrinking → queue is moving → OK."""
        from workers.watchdog import Issue
        snap = {
            "adp_total": 10, "adp_overdue": 5,
            "adp_head_c": "Acme", "adp_head_s": 1_699_990_000.0,
            "fs_total": 10, "fs_overdue": 1, "fs_head_c": "Co", "fs_head_s": 1.0,
            "detail_adp_depth": 0, "detail_fs_depth": 0,
            "scan_proc": 50, "fs_proc": 5, "detail_proc": 200,
        }
        issues, _ = self._run(
            snap=snap,
            adp_overdue=3,   # SHRINKING → s1 progresses → at most WARNING, likely OK
            scan_proc=55,    # INCREASED
        )
        level = self._level(issues, "poll:adaptive")
        self.assertNotEqual(level, "ERROR")

    # ── Fullscan lock exoneration ─────────────────────────────────────────────

    def test_fullscan_lock_active_suppresses_stall(self):
        """Lock active → fullscan stall signals suppressed → OK."""
        from workers.watchdog import Issue
        snap = {
            "adp_total": 10, "adp_overdue": 1, "adp_head_c": "Co", "adp_head_s": 1.0,
            "fs_total": 10, "fs_overdue": 3,   # overdue > 0
            "fs_head_c": "BigCorp", "fs_head_s": 1_699_980_000.0,  # same head → stall
            "detail_adp_depth": 0, "detail_fs_depth": 0,
            "scan_proc": 50, "fs_proc": 5,  # same proc → stall
            "detail_proc": 200,
        }
        issues, _ = self._run(
            snap=snap,
            fs_overdue=3,
            fs_head=("BigCorp", 1_699_980_000.0),
            fs_proc=5,
            fs_lock=True,   # LOCK ACTIVE
        )
        self.assertEqual(self._level(issues, "poll:fullscan"), Issue.OK)

    # ── Detail queue ──────────────────────────────────────────────────────────

    def test_detail_queue_idle_is_ok(self):
        from workers.watchdog import Issue
        snap = {
            "adp_total": 10, "adp_overdue": 0, "adp_head_c": None, "adp_head_s": None,
            "fs_total": 10, "fs_overdue": 0, "fs_head_c": None, "fs_head_s": None,
            "detail_adp_depth": 0, "detail_fs_depth": 0,
            "scan_proc": 50, "fs_proc": 5, "detail_proc": 200,
        }
        issues, _ = self._run(snap=snap, detail_adp=0, detail_fs=0)
        self.assertEqual(self._level(issues, "queue:detail:adaptive"), Issue.OK)

    def test_detail_queue_draining_ok(self):
        """depth decreased since last cycle → draining → OK."""
        from workers.watchdog import Issue
        snap = {
            "adp_total": 10, "adp_overdue": 0, "adp_head_c": None, "adp_head_s": None,
            "fs_total": 10, "fs_overdue": 0, "fs_head_c": None, "fs_head_s": None,
            "detail_adp_depth": 200, "detail_fs_depth": 0,  # was 200
            "scan_proc": 50, "fs_proc": 5, "detail_proc": 200,
        }
        issues, _ = self._run(snap=snap, detail_adp=150)  # now 150 < 200 → draining
        level = self._level(issues, "queue:detail:adaptive")
        self.assertNotEqual(level, "ERROR")

    def test_detail_queue_stalled_at_alert_level_is_error(self):
        """depth > DETAIL_QUEUE_ALERT and not draining → ERROR."""
        from workers.watchdog import Issue, DETAIL_QUEUE_ALERT
        depth = DETAIL_QUEUE_ALERT + 10
        snap = {
            "adp_total": 10, "adp_overdue": 0, "adp_head_c": None, "adp_head_s": None,
            "fs_total": 10, "fs_overdue": 0, "fs_head_c": None, "fs_head_s": None,
            "detail_adp_depth": depth, "detail_fs_depth": 0,  # same depth
            "scan_proc": 50, "fs_proc": 5, "detail_proc": 200,
        }
        issues, _ = self._run(snap=snap, detail_adp=depth, detail_proc=200)
        self.assertEqual(self._level(issues, "queue:detail:adaptive"), Issue.ERROR)


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckPelHealthPID  (Phase 3 — consumer liveness check)
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckPelHealthPID(unittest.TestCase):
    """
    _check_pel_health() compares PEL entry consumer PID against live heartbeat.
    Same PID → worker is alive, job in progress → OK regardless of age.
    Dead/mismatched PID → orphaned → time thresholds apply.
    """

    _NOW = 1_700_000_000.0

    def _make_r(self, pending=0, consumer_name=b"worker-host-1234",
                idle_ms=0, heartbeat_pid=1234, xpending_error=False):
        from config import REDIS_STREAM_ADAPTIVE
        r = MagicMock()

        def _xpending(stream, group):
            if xpending_error:
                raise Exception("NOGROUP")
            return {"pending": pending, "min": None, "max": None, "consumers": []}
        r.xpending.side_effect = _xpending

        def _xpending_range(stream, group, **kwargs):
            # _check_pel_health calls: xpending_range(key, group, min="-", max="+", count=1)
            # Accept **kwargs so keyword args (min, max, count) don't raise TypeError.
            return [{
                "consumer":             consumer_name,
                "time_since_delivered": idle_ms,
                "message_id":           b"1-1",
            }] if pending > 0 else []
        r.xpending_range.side_effect = _xpending_range

        def _get(key):
            if "scan_worker" in key or "fullscan_worker" in key:
                if heartbeat_pid is None:
                    return None
                return json.dumps({"pid": heartbeat_pid, "ts": self._NOW - 1,
                                   "processed": 0})
            return None
        r.get.side_effect = _get
        return r

    def _run(self, **kwargs):
        r = self._make_r(**kwargs)
        issues = []
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import _check_pel_health
            _check_pel_health(r, issues)
        return issues

    def test_zero_pending_is_ok(self):
        from workers.watchdog import Issue
        issues = self._run(pending=0)
        ok = [i for i in issues if i.level == Issue.OK]
        self.assertTrue(len(ok) > 0)

    def test_consumer_alive_same_pid_is_ok_regardless_of_age(self):
        """Consumer PID matches heartbeat → OK even if entry is 60 minutes old."""
        from workers.watchdog import Issue, PEL_ALERT_AGE_MS
        issues = self._run(
            pending=1,
            consumer_name=b"worker-host-1234",
            idle_ms=PEL_ALERT_AGE_MS * 2,  # well past the ERROR threshold
            heartbeat_pid=1234,             # same PID → alive
        )
        stream_issues = [i for i in issues if "PEL" in i.category]
        self.assertTrue(any(i.level == Issue.OK for i in stream_issues),
                        f"Expected OK when consumer alive, got: {stream_issues}")

    def test_consumer_dead_age_below_warn_is_ok(self):
        """Consumer dead but entry very young → XAUTOCLAIM will handle → OK."""
        from workers.watchdog import Issue, PEL_WARN_AGE_MS
        issues = self._run(
            pending=1,
            consumer_name=b"worker-host-9999",
            idle_ms=PEL_WARN_AGE_MS // 2,  # below WARNING threshold
            heartbeat_pid=1234,             # different PID → dead consumer
        )
        stream_issues = [i for i in issues if "PEL" in i.category]
        self.assertTrue(any(i.level == Issue.OK for i in stream_issues))

    def test_consumer_dead_age_above_warn_is_warning(self):
        from workers.watchdog import Issue, PEL_WARN_AGE_MS, PEL_ALERT_AGE_MS
        issues = self._run(
            pending=1,
            consumer_name=b"worker-host-9999",
            idle_ms=PEL_WARN_AGE_MS + 1_000,
            heartbeat_pid=1234,
        )
        stream_issues = [i for i in issues if "PEL" in i.category]
        self.assertTrue(any(i.level == Issue.WARNING for i in stream_issues))

    def test_consumer_dead_age_above_alert_is_error(self):
        from workers.watchdog import Issue, PEL_ALERT_AGE_MS
        issues = self._run(
            pending=1,
            consumer_name=b"worker-host-9999",
            idle_ms=PEL_ALERT_AGE_MS + 1_000,
            heartbeat_pid=1234,
        )
        stream_issues = [i for i in issues if "PEL" in i.category]
        self.assertTrue(any(i.level == Issue.ERROR for i in stream_issues))

    def test_unparseable_consumer_name_treated_as_dead(self):
        """Consumer name without trailing PID → _consumer_pid returns None → treated as dead."""
        from workers.watchdog import Issue, PEL_ALERT_AGE_MS
        issues = self._run(
            pending=1,
            consumer_name=b"bad-format-no-pid",
            idle_ms=PEL_ALERT_AGE_MS + 1_000,
            heartbeat_pid=1234,
        )
        stream_issues = [i for i in issues if "PEL" in i.category]
        # Should NOT be OK (dead consumer path)
        self.assertFalse(all(i.level == Issue.OK for i in stream_issues))

    def test_xpending_error_produces_warning_not_crash(self):
        from workers.watchdog import Issue
        issues = self._run(xpending_error=True)
        # Must not raise; should produce WARNING
        warn = [i for i in issues if i.level == Issue.WARNING]
        self.assertTrue(len(warn) > 0)


# ─────────────────────────────────────────────────────────────────────────────
# TestIssueClass  (Phase 3 — Issue data class)
# ─────────────────────────────────────────────────────────────────────────────

class TestIssueClass(unittest.TestCase):
    """
    Issue is the return value of every check_*() function.
    Tests verify the class contract: fields, level constants, is_alertable(),
    emoji(), __str__(), and alert_type derivation.
    """

    def test_level_constants_exist(self):
        from workers.watchdog import Issue
        self.assertEqual(Issue.OK,       "OK")
        self.assertEqual(Issue.WARNING,  "WARNING")
        self.assertEqual(Issue.ERROR,    "ERROR")
        self.assertEqual(Issue.CRITICAL, "CRITICAL")

    def test_fields_stored_correctly(self):
        from workers.watchdog import Issue
        i = Issue(Issue.ERROR, "worker:scan_worker", "dead", "restart it",
                  alert_type="worker_scan_worker")
        self.assertEqual(i.level,    Issue.ERROR)
        self.assertEqual(i.category, "worker:scan_worker")
        self.assertEqual(i.message,  "dead")
        self.assertEqual(i.fix,      "restart it")
        self.assertEqual(i.alert_type, "worker_scan_worker")

    def test_is_alertable_true_for_error(self):
        from workers.watchdog import Issue
        i = Issue(Issue.ERROR, "cat", "msg")
        self.assertTrue(i.is_alertable())

    def test_is_alertable_true_for_critical(self):
        from workers.watchdog import Issue
        i = Issue(Issue.CRITICAL, "cat", "msg")
        self.assertTrue(i.is_alertable())

    def test_is_alertable_false_for_ok(self):
        from workers.watchdog import Issue
        i = Issue(Issue.OK, "cat", "msg")
        self.assertFalse(i.is_alertable())

    def test_is_alertable_false_for_warning(self):
        from workers.watchdog import Issue
        i = Issue(Issue.WARNING, "cat", "msg")
        self.assertFalse(i.is_alertable())

    def test_emoji_ok_is_green(self):
        from workers.watchdog import Issue
        self.assertIn("🟢", Issue(Issue.OK, "c", "m").emoji())

    def test_emoji_warning_is_yellow(self):
        from workers.watchdog import Issue
        self.assertIn("🟡", Issue(Issue.WARNING, "c", "m").emoji())

    def test_emoji_error_is_red(self):
        from workers.watchdog import Issue
        self.assertIn("🔴", Issue(Issue.ERROR, "c", "m").emoji())

    def test_emoji_critical_is_red(self):
        from workers.watchdog import Issue
        self.assertIn("🔴", Issue(Issue.CRITICAL, "c", "m").emoji())

    def test_str_includes_level_category_message(self):
        from workers.watchdog import Issue
        i = Issue(Issue.ERROR, "worker:scan_worker", "heartbeat MISSING")
        s = str(i)
        self.assertIn("ERROR", s)
        self.assertIn("worker:scan_worker", s)
        self.assertIn("heartbeat MISSING", s)

    def test_str_includes_fix_when_set(self):
        from workers.watchdog import Issue
        i = Issue(Issue.ERROR, "cat", "msg", fix="python restart.py")
        self.assertIn("python restart.py", str(i))

    def test_str_omits_fix_when_empty(self):
        from workers.watchdog import Issue
        i = Issue(Issue.OK, "cat", "msg", fix="")
        self.assertNotIn("Fix:", str(i))

    def test_alert_type_derived_from_category(self):
        """alert_type replaces ':' and ' ' with '_' when not supplied."""
        from workers.watchdog import Issue
        i = Issue(Issue.ERROR, "worker:scan_worker", "msg")
        self.assertEqual(i.alert_type, "worker_scan_worker")

    def test_alert_type_uses_provided_value(self):
        from workers.watchdog import Issue
        i = Issue(Issue.ERROR, "worker:scan_worker", "msg",
                  alert_type="custom_key")
        self.assertEqual(i.alert_type, "custom_key")

    def test_fix_defaults_to_empty_string(self):
        from workers.watchdog import Issue
        i = Issue(Issue.OK, "cat", "msg")
        self.assertEqual(i.fix, "")

    def test_alert_type_defaults_from_category_spaces(self):
        """Spaces in category are also replaced."""
        from workers.watchdog import Issue
        i = Issue(Issue.WARNING, "queue poll adaptive", "msg")
        self.assertEqual(i.alert_type, "queue_poll_adaptive")


# ─────────────────────────────────────────────────────────────────────────────
# TestAdditionalHeartbeatThresholds  (Phase 3 — per-worker dead thresholds)
# ─────────────────────────────────────────────────────────────────────────────

class TestAdditionalHeartbeatThresholds(unittest.TestCase):
    """Extra coverage for check_worker_heartbeats() per-worker threshold values."""

    _NOW = 1_700_000_000.0

    def _run(self, worker_type, age_s):
        """Run heartbeat check for one worker with the given key age."""
        payload = json.dumps({"pid": 1, "ts": self._NOW - age_s, "processed": 0})
        r = MagicMock()
        r.get.side_effect = lambda k: (
            payload.encode() if worker_type in k else None
        )
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import check_worker_heartbeats
            return check_worker_heartbeats(r)

    def test_scheduler_ok_at_19_seconds(self):
        """scheduler threshold=20s: key age 19s → OK."""
        from workers.watchdog import Issue
        issues = self._run("scheduler", age_s=19)
        scheduler_issues = [i for i in issues if "scheduler" in i.category]
        self.assertTrue(any(i.level == Issue.OK for i in scheduler_issues))

    def test_scheduler_stale_at_21_seconds(self):
        """scheduler threshold=20s: key age 21s → STALE ERROR."""
        from workers.watchdog import Issue
        issues = self._run("scheduler", age_s=21)
        scheduler_issues = [i for i in issues if "scheduler" in i.category
                            and "scan" not in i.category]
        self.assertTrue(any(i.level == Issue.ERROR for i in scheduler_issues))

    def test_detail_worker_ok_at_44_seconds(self):
        """detail_worker threshold=45s: key age 44s → OK."""
        from workers.watchdog import Issue
        issues = self._run("detail_worker", age_s=44)
        dw_issues = [i for i in issues if "detail_worker" in i.category]
        self.assertTrue(any(i.level == Issue.OK for i in dw_issues))

    def test_detail_worker_stale_at_46_seconds(self):
        """detail_worker threshold=45s: key age 46s → STALE ERROR."""
        from workers.watchdog import Issue
        issues = self._run("detail_worker", age_s=46)
        dw_issues = [i for i in issues if "detail_worker" in i.category]
        self.assertTrue(any(i.level == Issue.ERROR for i in dw_issues))

    def test_fullscan_ok_at_1899_seconds(self):
        """fullscan_worker threshold=1900s: key age 1899s → OK."""
        from workers.watchdog import Issue
        issues = self._run("fullscan_worker", age_s=1899)
        fs_issues = [i for i in issues if "fullscan_worker" in i.category]
        self.assertTrue(any(i.level == Issue.OK for i in fs_issues))

    def test_all_4_workers_checked(self):
        """check_worker_heartbeats returns issues for all 4 worker types."""
        r = MagicMock()
        r.get.return_value = None  # all keys missing → all ERROR
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import check_worker_heartbeats
            issues = check_worker_heartbeats(r)
        self.assertEqual(len(issues), 4)
        categories = {i.category for i in issues}
        for wt in ("scheduler", "scan_worker", "detail_worker", "fullscan_worker"):
            self.assertTrue(any(wt in c for c in categories))


if __name__ == "__main__":
    unittest.main(verbosity=2)
