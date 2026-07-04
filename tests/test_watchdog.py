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
    · _consumer_pid(bytes) → int  (bytes are decoded within the function)
    · _trend(-5) → "↓"
    · _trend(0)  → "→"
    · _trend(3)  → "↑"
    · _worker_processed: key missing → None
    · _worker_processed: key present → int count
    · _worker_processed: unparseable → None

  TestCheckWorkerHeartbeats  (Phase 3.3 — two-layer detection)
    · Scheduler key missing → single ERROR (returns early — workers presumed dead too)
    · Scheduler key stale → ERROR ("STALE" in message)
    · Scheduler alive, scheduler:health absent → WARNING for pool_health
    · Scheduler alive + clean health → OK for all 3 pool types
    · consecutive_deaths ≥ 5 → ERROR for that worker type
    · consecutive_deaths ≥ 3 → WARNING for that worker type
    · consecutive_deaths < 3 → OK
    · Unparseable scheduler:health JSON → WARNING (no crash)
    · All 4 categories covered when scheduler + health both present
    · Scheduler threshold 20 s: age 19 s → OK, age 21 s → STALE ERROR

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
    · HEARTBEAT_DEAD_AFTER dict still has all 4 worker types (used by scheduler check)
    · consecutive_deaths thresholds: WARN_DEATHS=3, ERR_DEATHS=5
    · All 4 worker categories present when scheduler+health both provided
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
        # _consumer_pid accepts bytes and decodes them before extracting the PID.
        from workers.watchdog import _consumer_pid
        self.assertEqual(_consumer_pid(b"worker-host-42"), 42)

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
    # _worker_processed now uses SCAN + GET across per-PID keys
    # (worker:alive:{type}:{pid}) instead of a single shared key.

    def test_worker_processed_key_missing(self):
        """No per-PID keys for this worker type → returns None."""
        from workers.watchdog import _worker_processed
        r = MagicMock()
        r.scan.return_value = (0, [])   # no keys found
        self.assertIsNone(_worker_processed(r, "scan_worker"))

    def test_worker_processed_key_present(self):
        """One per-PID key present → returns its processed count."""
        from workers.watchdog import _worker_processed
        r = MagicMock()
        r.scan.return_value = (0, [b"worker:alive:scan_worker:1234"])
        r.get.return_value = json.dumps({"pid": 1234, "ts": time.time(), "processed": 42})
        self.assertEqual(_worker_processed(r, "scan_worker"), 42)

    def test_worker_processed_multiple_workers_summed(self):
        """Multiple per-PID keys → processed counts are summed across all workers."""
        from workers.watchdog import _worker_processed
        r = MagicMock()
        keys = [b"worker:alive:scan_worker:111", b"worker:alive:scan_worker:222"]
        r.scan.return_value = (0, keys)

        def _get(key):
            if b"111" in key or "111" in str(key):
                return json.dumps({"pid": 111, "ts": time.time(), "processed": 30})
            return json.dumps({"pid": 222, "ts": time.time(), "processed": 15})
        r.get.side_effect = _get

        # Total = 30 + 15 = 45
        self.assertEqual(_worker_processed(r, "scan_worker"), 45)

    def test_worker_processed_unparseable_returns_none(self):
        """Unparseable payload raises inside the loop → caught → returns None."""
        from workers.watchdog import _worker_processed
        r = MagicMock()
        r.scan.return_value = (0, [b"worker:alive:scan_worker:1234"])
        r.get.return_value = b"not-json"
        self.assertIsNone(_worker_processed(r, "scan_worker"))


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckWorkerHeartbeats  (Phase 3.3 — two-layer detection model)
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckWorkerHeartbeats(unittest.TestCase):
    """
    check_worker_heartbeats() uses a two-layer detection model:

    Layer 1 — worker:alive:scheduler
        Fast single-key check (TTL=15s, written every ~1s).
        Missing or stale → ERROR immediately, returns early.

    Layer 2 — scheduler:health
        Rich pool state (JSON, TTL=10min) published by the scheduler on every
        pool event (death/respawn/scale).  Contains per-type consecutive_deaths
        counters.  Missing → WARNING.

    Individual per-PID keys (worker:alive:{type}:{pid}) are scanned for
    display/observability inside check_worker_heartbeats, but NO alerting
    decisions are made from them — all alerts come from the two keys above.
    """

    _NOW = 1_700_000_000.0

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _health_payload(self, scan_deaths=0, detail_deaths=0, fullscan_deaths=0,
                        scan_alive=3, detail_alive=4, fullscan_alive=3):
        return json.dumps({
            "ts": self._NOW - 5,
            "pool": {
                "scan":     {"alive": scan_alive,    "consecutive_deaths": scan_deaths,    "total_replacements": 0},
                "detail":   {"alive": detail_alive,  "consecutive_deaths": detail_deaths,  "total_replacements": 0},
                "fullscan": {"alive": fullscan_alive, "consecutive_deaths": fullscan_deaths,"total_replacements": 0},
            },
        })

    def _make_r(self, scheduler_raw=None, health_raw=None):
        r = MagicMock()
        def _get(key):
            if key == "worker:alive:scheduler":
                return scheduler_raw
            if key == "scheduler:health":
                return health_raw
            return None
        r.get.side_effect = _get
        r.scan.return_value = (0, [])   # per-PID display scan — empty is fine
        return r

    def _run(self, scheduler_raw=None, health_raw=None):
        r = self._make_r(scheduler_raw=scheduler_raw, health_raw=health_raw)
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import check_worker_heartbeats
            return check_worker_heartbeats(r)

    def _issues_for(self, issues, keyword):
        return [i for i in issues if keyword in i.category]

    # ── Layer 1: scheduler key ────────────────────────────────────────────────

    def test_scheduler_key_missing_returns_single_error(self):
        """
        Scheduler heartbeat absent → single ERROR, function returns immediately.
        Workers are all presumed dead — no point reading scheduler:health.
        """
        from workers.watchdog import Issue
        issues = self._run(scheduler_raw=None)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].level, Issue.ERROR)
        self.assertIn("MISSING", issues[0].message)

    def test_scheduler_key_stale_returns_error(self):
        """Scheduler key present but ts older than threshold → ERROR STALE."""
        from workers.watchdog import Issue, HEARTBEAT_DEAD_AFTER
        threshold = HEARTBEAT_DEAD_AFTER["scheduler"]
        payload = json.dumps({"pid": 1, "ts": self._NOW - (threshold + 10), "dispatched": 0})
        issues = self._run(scheduler_raw=payload.encode())
        sched_issues = self._issues_for(issues, "scheduler")
        self.assertTrue(any(i.level == Issue.ERROR and "STALE" in i.message
                            for i in sched_issues),
                        f"Expected STALE ERROR, got: {sched_issues}")

    def test_scheduler_ok_at_19_seconds(self):
        """scheduler alive at threshold-1s → OK."""
        from workers.watchdog import Issue, HEARTBEAT_DEAD_AFTER
        threshold = HEARTBEAT_DEAD_AFTER["scheduler"]
        payload = json.dumps({"pid": 1, "ts": self._NOW - (threshold - 1), "dispatched": 0})
        health  = self._health_payload()
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=health.encode())
        sched_issues = [i for i in issues if i.category == "worker:scheduler"]
        self.assertTrue(any(i.level == Issue.OK for i in sched_issues),
                        f"Expected OK at threshold-1s, got: {sched_issues}")

    def test_scheduler_stale_at_21_seconds(self):
        """scheduler stale at threshold+1s → STALE ERROR."""
        from workers.watchdog import Issue, HEARTBEAT_DEAD_AFTER
        threshold = HEARTBEAT_DEAD_AFTER["scheduler"]
        payload = json.dumps({"pid": 1, "ts": self._NOW - (threshold + 1), "dispatched": 0})
        # Even if health key exists, stale scheduler is an error
        issues = self._run(scheduler_raw=payload.encode())
        sched_issues = [i for i in issues if i.category == "worker:scheduler"]
        self.assertTrue(any(i.level == Issue.ERROR for i in sched_issues),
                        f"Expected ERROR at threshold+1s, got: {sched_issues}")

    def test_unparseable_scheduler_payload_treated_as_ok(self):
        """Unparseable scheduler key JSON (key exists) → treated as alive (OK)."""
        from workers.watchdog import Issue
        health = self._health_payload()
        issues = self._run(scheduler_raw=b"bad-json", health_raw=health.encode())
        sched_issues = [i for i in issues if i.category == "worker:scheduler"]
        self.assertTrue(any(i.level == Issue.OK for i in sched_issues))

    # ── Layer 2: scheduler:health pool state ─────────────────────────────────

    def test_health_key_missing_returns_warning(self):
        """Scheduler alive but scheduler:health absent → WARNING for pool_health."""
        from workers.watchdog import Issue
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=None)
        pool_issues = self._issues_for(issues, "pool_health")
        self.assertTrue(any(i.level == Issue.WARNING for i in pool_issues))

    def test_pool_healthy_all_ok(self):
        """Scheduler alive + zero consecutive_deaths → OK for all 3 pool types."""
        from workers.watchdog import Issue
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        health  = self._health_payload()
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=health.encode())
        pool_issues = [i for i in issues
                       if "worker:" in i.category
                       and "scheduler" not in i.category
                       and "pool_health" not in i.category]
        self.assertTrue(
            all(i.level == Issue.OK for i in pool_issues),
            f"Expected all OK with zero deaths, got: {pool_issues}",
        )

    def test_consecutive_deaths_5_produces_error(self):
        """consecutive_deaths ≥ 5 for scan → ERROR (pool cannot stabilize)."""
        from workers.watchdog import Issue
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        health  = self._health_payload(scan_deaths=5)
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=health.encode())
        scan_issues = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.ERROR for i in scan_issues),
                        f"Expected ERROR at deaths=5, got: {scan_issues}")

    def test_consecutive_deaths_3_produces_warning(self):
        """consecutive_deaths = 3 → WARNING (scheduler struggling)."""
        from workers.watchdog import Issue
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        health  = self._health_payload(scan_deaths=3)
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=health.encode())
        scan_issues = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.WARNING for i in scan_issues),
                        f"Expected WARNING at deaths=3, got: {scan_issues}")

    def test_consecutive_deaths_2_is_ok(self):
        """consecutive_deaths = 2 → OK (within noise tolerance, scheduler recovering)."""
        from workers.watchdog import Issue
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        health  = self._health_payload(scan_deaths=2)
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=health.encode())
        scan_issues = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.OK for i in scan_issues),
                        f"Expected OK at deaths=2, got: {scan_issues}")

    def test_detail_consecutive_deaths_threshold(self):
        """consecutive_deaths tracked independently per worker type."""
        from workers.watchdog import Issue
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        # detail deaths = 5 (ERROR), others clean
        health  = self._health_payload(detail_deaths=5)
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=health.encode())
        detail_issues = self._issues_for(issues, "detail_worker")
        scan_issues   = self._issues_for(issues, "scan_worker")
        self.assertTrue(any(i.level == Issue.ERROR for i in detail_issues))
        self.assertTrue(any(i.level == Issue.OK    for i in scan_issues))

    def test_unparseable_health_returns_warning(self):
        """Unparseable scheduler:health JSON → WARNING, not a crash."""
        from workers.watchdog import Issue
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=b"not-json")
        pool_issues = self._issues_for(issues, "pool_health")
        self.assertTrue(any(i.level == Issue.WARNING for i in pool_issues))

    def test_all_4_worker_categories_covered(self):
        """
        With scheduler alive and scheduler:health present, issues cover all
        4 worker categories: scheduler + scan_worker + detail_worker + fullscan_worker.
        """
        payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        health  = self._health_payload()
        issues  = self._run(scheduler_raw=payload.encode(), health_raw=health.encode())
        self.assertGreaterEqual(len(issues), 4,
            f"Expected ≥4 issues (scheduler + 3 pool types), got {len(issues)}: {issues}")
        categories = {i.category for i in issues}
        for expected in ("worker:scheduler", "worker:scan_worker",
                         "worker:detail_worker", "worker:fullscan_worker"):
            self.assertTrue(any(expected in c for c in categories),
                            f"Missing issue category containing '{expected}'")


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
        stall, _ = self._call(cur_ov=7, prev_ov=5)
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
        stall, _ = self._call(
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
            if key == REDIS_POLL_ADAPTIVE:
                return adp_total
            if key == REDIS_POLL_FULLSCAN:
                return fs_total
            return 0
        r.zcard.side_effect = _zcard

        # zcount
        def _zcount(key, lo, hi):
            if key == REDIS_POLL_ADAPTIVE:
                return adp_overdue
            if key == REDIS_POLL_FULLSCAN:
                return fs_overdue
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

        # scan — _worker_processed now uses per-PID keys; _fullscan_lock_active
        # uses the lock key.  Use side_effect (not return_value) so different
        # match= patterns return the right keys.
        now = self._NOW
        def _scan(cursor, match="*", count=100):
            if "fullscan:lock:" in match:
                return (0, [b"fullscan:lock:Co"] if fs_lock else [])
            if "worker:alive:scan_worker:" in match:
                return (0, [b"worker:alive:scan_worker:1"])
            if "worker:alive:fullscan_worker:" in match:
                return (0, [b"worker:alive:fullscan_worker:2"])
            if "worker:alive:detail_worker:" in match:
                return (0, [b"worker:alive:detail_worker:3"])
            return (0, [])
        r.scan.side_effect = _scan

        # llen (detail queues)
        def _llen(key):
            if key == REDIS_DETAIL_ADAPTIVE:
                return detail_adp
            if key == REDIS_DETAIL_FULLSCAN:
                return detail_fs
            return 0
        r.llen.side_effect = _llen

        # get — snapshot key + per-PID worker keys for _worker_processed
        def _get(key):
            key_s = key.decode() if isinstance(key, bytes) else key
            if key_s == WATCHDOG_SNAPSHOT_KEY:
                return json.dumps(snap).encode() if snap else None
            # per-PID heartbeat keys returned by _scan above
            if "worker:alive:scan_worker:" in key_s:
                return json.dumps({"pid": 1, "ts": now - 5, "processed": scan_proc}).encode()
            if "worker:alive:fullscan_worker:" in key_s:
                return json.dumps({"pid": 2, "ts": now - 5, "processed": fs_proc}).encode()
            if "worker:alive:detail_worker:" in key_s:
                return json.dumps({"pid": 3, "ts": now - 5, "processed": detail_proc}).encode()
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
        """Overdue shrinking (s1=False) + proc increasing (s3=False) → stall=1/3 < 2 → OK."""
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
            adp_overdue=3,   # SHRINKING → s1=False
            scan_proc=55,    # INCREASED → s3=False; s2=True (head same) → stall=1 < 2 → OK
        )
        self.assertEqual(self._level(issues, "poll:adaptive"), Issue.OK)

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

        # _consumer_pid_alive() calls r.exists(f"worker:alive:{type}:{hostname}:{pid}")
        # Require the full worker:alive:{type}:{hostname}:{pid} key shape so the
        # mock fails if the code falls back to a legacy PID-only key format.
        import socket as _socket
        _test_hostname = _socket.gethostname()

        def _exists(key):
            key_s = key.decode() if isinstance(key, bytes) else key
            if heartbeat_pid is None:
                return 0
            # Must match exact shape: worker:alive:{type}:{hostname}:{pid}
            # The code builds the key from consumer_name = "worker-{hostname}-{pid}"
            # via rpartition("-") + removeprefix("worker-").
            expected_suffix = f":{_test_hostname}:{heartbeat_pid}"
            if key_s.startswith("worker:alive:") and key_s.endswith(expected_suffix):
                return 1
            return 0
        r.exists.side_effect = _exists
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
        """Consumer name without trailing PID → _consumer_pid returns None → treated as dead → ERROR."""
        from workers.watchdog import Issue, PEL_ALERT_AGE_MS
        issues = self._run(
            pending=1,
            consumer_name=b"bad-format-no-pid",
            idle_ms=PEL_ALERT_AGE_MS + 1_000,
            heartbeat_pid=1234,
        )
        stream_issues = [i for i in issues if "PEL" in i.category]
        # Orphaned entry past PEL_ALERT_AGE_MS must escalate to ERROR
        self.assertTrue(any(i.level == Issue.ERROR for i in stream_issues),
                        f"Expected ERROR for dead consumer past PEL_ALERT_AGE_MS; got {[i.level for i in stream_issues]}")

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
# TestAdditionalHeartbeatThresholds  (Phase 3 — threshold and death constants)
# ─────────────────────────────────────────────────────────────────────────────

class TestAdditionalHeartbeatThresholds(unittest.TestCase):
    """
    Validates the threshold constants and boundary conditions for
    check_worker_heartbeats() under the two-layer detection model.

    Notes
    ─────
    · HEARTBEAT_DEAD_AFTER is still defined for all 4 worker types and is used
      directly for the scheduler heartbeat age check.
    · Worker pool (scan/detail/fullscan) liveness is now determined by
      consecutive_deaths in scheduler:health, not by per-worker key age.
      Those constants live inside check_worker_heartbeats() as WARN_DEATHS=3
      and ERR_DEATHS=5.
    """

    _NOW = 1_700_000_000.0

    # ── Scheduler heartbeat age threshold ─────────────────────────────────────

    def _run_scheduler_age(self, age_s):
        """Run heartbeat check with scheduler key at a specific age."""
        payload = json.dumps({"pid": 1, "ts": self._NOW - age_s, "dispatched": 0})
        r = MagicMock()
        def _get(key):
            if key == "worker:alive:scheduler":
                return payload.encode()
            if key == "scheduler:health":
                # Provide valid health so function doesn't return WARNING early
                return json.dumps({
                    "ts": self._NOW - 1,
                    "pool": {
                        "scan":     {"alive": 3, "consecutive_deaths": 0, "total_replacements": 0},
                        "detail":   {"alive": 4, "consecutive_deaths": 0, "total_replacements": 0},
                        "fullscan": {"alive": 3, "consecutive_deaths": 0, "total_replacements": 0},
                    },
                }).encode()
            return None
        r.get.side_effect = _get
        r.scan.return_value = (0, [])
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import check_worker_heartbeats
            return check_worker_heartbeats(r)

    def test_scheduler_ok_at_19_seconds(self):
        """scheduler threshold=20s: key age 19s → OK."""
        from workers.watchdog import Issue
        issues = self._run_scheduler_age(19)
        sched = [i for i in issues if i.category == "worker:scheduler"]
        self.assertTrue(any(i.level == Issue.OK for i in sched),
                        f"Expected OK at 19s, got: {sched}")

    def test_scheduler_stale_at_21_seconds(self):
        """scheduler threshold=20s: key age 21s → STALE ERROR."""
        from workers.watchdog import Issue
        issues = self._run_scheduler_age(21)
        sched = [i for i in issues if i.category == "worker:scheduler"]
        self.assertTrue(any(i.level == Issue.ERROR for i in sched),
                        f"Expected STALE ERROR at 21s, got: {sched}")

    # ── Pool consecutive_deaths threshold boundary ────────────────────────────

    def _run_deaths(self, ptype="scan", deaths=0):
        """Run heartbeat check with scheduler alive and given deaths count."""
        scheduler_payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        pool = {
            "scan":     {"alive": 3, "consecutive_deaths": 0, "total_replacements": 0},
            "detail":   {"alive": 4, "consecutive_deaths": 0, "total_replacements": 0},
            "fullscan": {"alive": 3, "consecutive_deaths": 0, "total_replacements": 0},
        }
        pool[ptype]["consecutive_deaths"] = deaths
        health = json.dumps({"ts": self._NOW - 1, "pool": pool})
        r = MagicMock()
        def _get(key):
            if key == "worker:alive:scheduler":
                return scheduler_payload.encode()
            if key == "scheduler:health":
                return health.encode()
            return None
        r.get.side_effect = _get
        r.scan.return_value = (0, [])
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import check_worker_heartbeats
            return check_worker_heartbeats(r)

    def test_warn_deaths_boundary_at_2(self):
        """deaths=2 → OK (below WARN threshold of 3)."""
        from workers.watchdog import Issue
        issues = self._run_deaths(ptype="scan", deaths=2)
        scan = [i for i in issues if "scan_worker" in i.category]
        self.assertTrue(any(i.level == Issue.OK for i in scan))

    def test_warn_deaths_boundary_at_3(self):
        """deaths=3 → WARNING (at WARN_DEATHS threshold)."""
        from workers.watchdog import Issue
        issues = self._run_deaths(ptype="scan", deaths=3)
        scan = [i for i in issues if "scan_worker" in i.category]
        self.assertTrue(any(i.level == Issue.WARNING for i in scan))

    def test_err_deaths_boundary_at_4(self):
        """deaths=4 → WARNING (below ERR_DEATHS=5)."""
        from workers.watchdog import Issue
        issues = self._run_deaths(ptype="scan", deaths=4)
        scan = [i for i in issues if "scan_worker" in i.category]
        self.assertTrue(any(i.level == Issue.WARNING for i in scan))

    def test_err_deaths_boundary_at_5(self):
        """deaths=5 → ERROR (at ERR_DEATHS threshold)."""
        from workers.watchdog import Issue
        issues = self._run_deaths(ptype="scan", deaths=5)
        scan = [i for i in issues if "scan_worker" in i.category]
        self.assertTrue(any(i.level == Issue.ERROR for i in scan))

    # ── All 4 categories present ──────────────────────────────────────────────

    def test_all_4_worker_categories_with_scheduler_alive(self):
        """
        When scheduler is alive and scheduler:health is present, check_worker_heartbeats
        returns issues covering all 4 categories (scheduler + 3 pool types).
        """
        scheduler_payload = json.dumps({"pid": 1, "ts": self._NOW - 5, "dispatched": 0})
        health = json.dumps({
            "ts": self._NOW - 1,
            "pool": {
                "scan":     {"alive": 3, "consecutive_deaths": 0, "total_replacements": 0},
                "detail":   {"alive": 4, "consecutive_deaths": 0, "total_replacements": 0},
                "fullscan": {"alive": 3, "consecutive_deaths": 0, "total_replacements": 0},
            },
        })
        r = MagicMock()
        def _get(key):
            if key == "worker:alive:scheduler":
                return scheduler_payload.encode()
            if key == "scheduler:health":
                return health.encode()
            return None
        r.get.side_effect = _get
        r.scan.return_value = (0, [])
        with patch("workers.watchdog.time.time", return_value=self._NOW):
            from workers.watchdog import check_worker_heartbeats
            issues = check_worker_heartbeats(r)
        categories = {i.category for i in issues}
        for expected in ("worker:scheduler", "worker:scan_worker",
                         "worker:detail_worker", "worker:fullscan_worker"):
            self.assertTrue(any(expected in c for c in categories),
                            f"Missing category containing '{expected}' — got: {categories}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
