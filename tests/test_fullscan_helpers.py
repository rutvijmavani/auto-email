"""
tests/test_fullscan_helpers.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for workers/fullscan.py helper functions and classes.

Redis interactions and ATS module imports are fully mocked so these tests run
without a live Redis or ATS module.

Coverage map
────────────
  TestBloomPairFallbackMode  (RedisBloom unavailable → SET fallback)
    · _probe() sets _use_bf=False when BF.EXISTS raises
    · old_exists() on fallback → calls sismember on old fallback key
    · new_add() on fallback → calls sadd on new fallback key
    · old/new key naming follows bloom:fullscan:{co} / bloom:fullscan:new:{co}
    · prepare_fresh() DELs new keys, BF.RESERVE new bf key
    · finalize() DELs old keys, RENAMEs new → old

  TestBloomPairBfMode  (RedisBloom available)
    · _probe() sets _use_bf=True when BF.EXISTS succeeds
    · old_exists() calls BF.EXISTS on old key
    · new_add() calls BF.ADD on new key
    · BF exception during new_add() → falls back to SET sadd
    · BF exception during old_exists() → falls back to sismember
    · probe result is cached

  TestIsPaused
    · Returns True when db:maintenance key exists
    · Returns False when db:maintenance key does not exist
    · Uses REDIS_DB_MAINTENANCE config constant for key name

  TestAcquireReleaseLock
    · _acquire_lock returns True when SET NX EX succeeds
    · _acquire_lock returns False when SET NX EX fails (key exists)
    · _release_lock deletes key only when current holder matches WORKER_ID
    · _release_lock does nothing when different worker holds the lock
    · Lock key is fullscan:lock:{company}

  TestDeferAdaptiveFirst
    · _defer_adaptive_first ZADDs company to poll:adaptive with score=now
    · _defer_adaptive_first ZADDs company to poll:fullscan with future score
    · Fullscan reschedule delay = FULLSCAN_ADAPTIVE_FIRST_DELAY_S (900s)

  TestGetCycleStart
    · Returns None when Redis key is not set
    · Returns float when Redis key is set
    · Handles string value correctly (converts to float)

  TestBuildDetailPayload
    · Base payload has required keys: company, ats_platform, job_id, job_url, etc.
    · found_by = 'tier2_fullscan'
    · posted_at: datetime.isoformat() called when it's a datetime
    · posted_at: string passed through unchanged
    · Platform-specific key workday → _external_path forwarded
    · Platform-specific key icims → _base_url, _feed_type forwarded
    · Platform-specific key jobvite → _slug forwarded
    · Platform-specific key taleo → _contest_no forwarded
    · Platform-specific key smartrecruiters → _company_slug forwarded
    · _country_code forwarded if present
    · Unknown platform: no extra keys (but no crash)
    · slug_info stored in payload

  TestFullscanConstants
    · FULLSCAN_CHUNK_SIZE = 50
    · FULLSCAN_BLOOM_CAPACITY = 10_000
    · FULLSCAN_ADAPTIVE_FIRST_DELAY_S = 900
"""

import sys
import os
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_redis_bf_available():
    """Redis mock where BF.EXISTS works."""
    r = MagicMock()
    r.execute_command.return_value = 0   # BF.EXISTS → 0 (not found)
    return r


def _make_redis_bf_unavailable():
    """Redis mock where BF.EXISTS raises (RedisBloom not loaded)."""
    r = MagicMock()
    def _cmd_raise(*args, **kwargs):
        if args and args[0] == "BF.EXISTS":
            raise Exception("ERR unknown command 'BF.EXISTS'")
        return MagicMock()
    r.execute_command.side_effect = _cmd_raise
    r.sismember.return_value = 0
    r.sadd.return_value = 1
    r.expire.return_value = True
    r.delete.return_value = 1
    return r


# ─────────────────────────────────────────────────────────────────────────────
# TestBloomPairFallbackMode
# ─────────────────────────────────────────────────────────────────────────────

class TestBloomPairFallbackMode(unittest.TestCase):
    """Tests for _BloomPair when RedisBloom is NOT available (SET fallback)."""

    def setUp(self):
        from workers.fullscan import _BloomPair
        self.r = _make_redis_bf_unavailable()
        self.r.rename = MagicMock()
        self.bp = _BloomPair(self.r, "TestCo")

    def test_probe_sets_use_bf_false_when_bf_unavailable(self):
        """_probe() returns False when BF.EXISTS raises."""
        result = self.bp._probe()
        self.assertFalse(result)
        self.assertFalse(self.bp._use_bf)

    def test_old_exists_calls_sismember_on_old_fallback_key(self):
        """old_exists() on fallback → sismember on bloom:fallback:{company}."""
        self.bp.old_exists("job_123")
        self.r.sismember.assert_called_once_with("bloom:fallback:TestCo", "job_123")

    def test_old_exists_returns_false_when_not_in_fallback(self):
        """old_exists() returns False when sismember returns 0."""
        self.r.sismember.return_value = 0
        self.assertFalse(self.bp.old_exists("job_xyz"))

    def test_old_exists_returns_true_when_in_fallback(self):
        """old_exists() returns True when sismember returns 1."""
        self.r.sismember.return_value = 1
        self.assertTrue(self.bp.old_exists("job_xyz"))

    def test_new_add_calls_sadd_on_new_fallback_key(self):
        """new_add() on fallback → sadd on bloom:fallback:new:{company}."""
        self.bp.new_add("job_456")
        self.r.sadd.assert_called_once_with("bloom:fallback:new:TestCo", "job_456")

    def test_old_key_naming(self):
        """OLD keys follow bloom:fullscan:{company} / bloom:fallback:{company}."""
        from workers.fullscan import _BloomPair
        bp = _BloomPair(self.r, "AcmeCorp")
        self.assertEqual(bp._old_bf, "bloom:fullscan:AcmeCorp")
        self.assertEqual(bp._old_fb, "bloom:fallback:AcmeCorp")

    def test_new_key_naming(self):
        """NEW keys follow bloom:fullscan:new:{company} / bloom:fallback:new:{company}."""
        from workers.fullscan import _BloomPair
        bp = _BloomPair(self.r, "AcmeCorp")
        self.assertEqual(bp._new_bf, "bloom:fullscan:new:AcmeCorp")
        self.assertEqual(bp._new_fb, "bloom:fallback:new:AcmeCorp")

    def test_prepare_fresh_deletes_new_keys(self):
        """prepare_fresh() DELs new bf and new fb keys."""
        self.bp.prepare_fresh()
        delete_calls = [c[0][0] for c in self.r.delete.call_args_list]
        self.assertIn("bloom:fullscan:new:TestCo", delete_calls)
        self.assertIn("bloom:fallback:new:TestCo", delete_calls)

    def test_finalize_deletes_old_keys(self):
        """finalize() DELs old bf and old fb keys."""
        self.bp.finalize()
        delete_calls = [c[0][0] for c in self.r.delete.call_args_list]
        self.assertIn("bloom:fullscan:TestCo", delete_calls)
        self.assertIn("bloom:fallback:TestCo", delete_calls)

    def test_finalize_renames_new_to_old(self):
        """finalize() RENAMEs new keys to old keys."""
        self.bp.finalize()
        rename_calls = [c[0] for c in self.r.rename.call_args_list]
        self.assertTrue(
            any(c[0] == "bloom:fullscan:new:TestCo" and c[1] == "bloom:fullscan:TestCo"
                for c in rename_calls)
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestBloomPairBfMode
# ─────────────────────────────────────────────────────────────────────────────

class TestBloomPairBfMode(unittest.TestCase):
    """Tests for _BloomPair when RedisBloom IS available."""

    def setUp(self):
        from workers.fullscan import _BloomPair
        self.r = _make_redis_bf_available()
        self.r.rename = MagicMock()
        self.bp = _BloomPair(self.r, "TestCo")

    def test_probe_sets_use_bf_true_when_bf_available(self):
        """_probe() returns True when BF.EXISTS succeeds."""
        result = self.bp._probe()
        self.assertTrue(result)
        self.assertTrue(self.bp._use_bf)

    def test_old_exists_calls_bf_exists_on_old_key(self):
        """old_exists() calls BF.EXISTS on bloom:fullscan:{company}."""
        self.bp.old_exists("job_001")
        self.r.execute_command.assert_called_with(
            "BF.EXISTS", "bloom:fullscan:TestCo", "job_001"
        )

    def test_old_exists_returns_false_when_bf_returns_0(self):
        """BF.EXISTS → 0 means job NOT in old bloom."""
        self.r.execute_command.return_value = 0
        self.assertFalse(self.bp.old_exists("job_002"))

    def test_old_exists_returns_true_when_bf_returns_1(self):
        """BF.EXISTS → 1 means job IS in old bloom."""
        self.r.execute_command.return_value = 1
        self.assertTrue(self.bp.old_exists("job_002"))

    def test_new_add_calls_bf_add_on_new_key(self):
        """new_add() calls BF.ADD on bloom:fullscan:new:{company}."""
        self.bp.new_add("job_003")
        add_calls = [c[0] for c in self.r.execute_command.call_args_list]
        self.assertTrue(
            any(c[0] == "BF.ADD" and c[1] == "bloom:fullscan:new:TestCo"
                for c in add_calls)
        )

    def test_bf_exception_in_old_exists_falls_back_to_sismember(self):
        """BF.EXISTS exception → fall back to sismember on old fallback key."""
        def _cmd(*args):
            if args[0] == "BF.EXISTS":
                raise Exception("BF error")
            return 0
        self.r.execute_command.side_effect = _cmd
        self.r.sismember.return_value = 0
        result = self.bp.old_exists("job_005")
        self.assertFalse(result)
        self.r.sismember.assert_called()

    def test_bf_exception_in_new_add_falls_back_to_sadd(self):
        """BF.ADD exception → fall back to sadd on new fallback key."""
        def _cmd(*args):
            if args[0] in ("BF.EXISTS", "BF.ADD"):
                raise Exception("BF error")
            return 0
        self.r.execute_command.side_effect = _cmd
        self.r.sadd.return_value = 1
        self.bp.new_add("job_006")
        self.r.sadd.assert_called()

    def test_probe_result_cached(self):
        """_probe() result is cached — execute_command called only once for probe."""
        self.bp._probe()
        call_count_after_first = self.r.execute_command.call_count
        self.bp._probe()
        self.assertEqual(self.r.execute_command.call_count, call_count_after_first)


# ─────────────────────────────────────────────────────────────────────────────
# TestIsPaused
# ─────────────────────────────────────────────────────────────────────────────

class TestIsPaused(unittest.TestCase):

    def _run(self, exists_return):
        r = MagicMock()
        r.exists.return_value = exists_return
        from workers.fullscan import _is_paused
        return _is_paused(r)

    def test_maintenance_key_exists_returns_true(self):
        """r.exists(db:maintenance) = 1 → True."""
        self.assertTrue(self._run(1))

    def test_maintenance_key_missing_returns_false(self):
        """r.exists(db:maintenance) = 0 → False."""
        self.assertFalse(self._run(0))

    def test_uses_redis_db_maintenance_key(self):
        """Correct key used: REDIS_DB_MAINTENANCE."""
        from config import REDIS_DB_MAINTENANCE
        r = MagicMock()
        r.exists.return_value = 0
        from workers.fullscan import _is_paused
        _is_paused(r)
        r.exists.assert_called_once_with(REDIS_DB_MAINTENANCE)


# ─────────────────────────────────────────────────────────────────────────────
# TestAcquireReleaseLock
# ─────────────────────────────────────────────────────────────────────────────

class TestAcquireReleaseLock(unittest.TestCase):

    def test_acquire_returns_true_when_nx_succeeds(self):
        """_acquire_lock returns True when SET NX EX succeeds."""
        r = MagicMock()
        r.set.return_value = True
        from workers.fullscan import _acquire_lock
        result = _acquire_lock("TestCo", r)
        self.assertTrue(result)

    def test_acquire_returns_false_when_nx_fails(self):
        """_acquire_lock returns False when SET NX fails (key already exists)."""
        r = MagicMock()
        r.set.return_value = None   # SET NX returns None if key exists
        from workers.fullscan import _acquire_lock
        result = _acquire_lock("TestCo", r)
        self.assertFalse(result)

    def test_acquire_uses_nx_and_ex_flags(self):
        """_acquire_lock uses nx=True and ex=TTL."""
        from config import SCHEDULER_FULL_SCAN_LOCK_TTL
        r = MagicMock()
        r.set.return_value = True
        from workers.fullscan import _acquire_lock
        _acquire_lock("TestCo", r)
        r.set.assert_called_once()
        kwargs = r.set.call_args[1]
        self.assertTrue(kwargs.get("nx"))
        self.assertEqual(kwargs.get("ex"), SCHEDULER_FULL_SCAN_LOCK_TTL)

    def test_acquire_lock_key_format(self):
        """Lock key is fullscan:lock:{company}."""
        r = MagicMock()
        r.set.return_value = True
        from workers.fullscan import _acquire_lock
        _acquire_lock("AcmeCo", r)
        args = r.set.call_args[0]
        self.assertEqual(args[0], "fullscan:lock:AcmeCo")

    def test_release_lock_deletes_key_when_holder_matches(self):
        """_release_lock deletes key when current holder = WORKER_ID."""
        from workers.fullscan import _release_lock, WORKER_ID
        r = MagicMock()
        # Redis returns bytes by default; _release_lock must decode before compare.
        r.get.return_value = WORKER_ID.encode() if isinstance(WORKER_ID, str) else WORKER_ID
        _release_lock("TestCo", r)
        r.delete.assert_called_once_with("fullscan:lock:TestCo")

    def test_release_lock_does_nothing_when_different_holder(self):
        """_release_lock does NOT delete when a different worker holds the lock."""
        from workers.fullscan import _release_lock
        r = MagicMock()
        r.get.return_value = b"other_worker:99999"
        _release_lock("TestCo", r)
        r.delete.assert_not_called()

    def test_release_lock_checks_correct_key(self):
        """_release_lock calls r.get on fullscan:lock:{company}."""
        from workers.fullscan import _release_lock
        r = MagicMock()
        r.get.return_value = b"some_worker"
        _release_lock("TestCo", r)
        r.get.assert_called_once_with("fullscan:lock:TestCo")


# ─────────────────────────────────────────────────────────────────────────────
# TestDeferAdaptiveFirst
# ─────────────────────────────────────────────────────────────────────────────

class TestDeferAdaptiveFirst(unittest.TestCase):

    def test_zadd_to_adaptive_with_score_now(self):
        """_defer_adaptive_first ZADDs company to poll:adaptive with score=now."""
        from config import REDIS_POLL_ADAPTIVE
        r = MagicMock()
        before = time.time()
        from workers.fullscan import _defer_adaptive_first
        _defer_adaptive_first("TestCo", r)
        after = time.time()

        # Find the zadd call for poll:adaptive
        adaptive_calls = [
            c for c in r.zadd.call_args_list
            if c[0][0] == REDIS_POLL_ADAPTIVE
        ]
        self.assertEqual(len(adaptive_calls), 1)
        # Score should be approximately now
        score = list(adaptive_calls[0][0][1].values())[0]
        self.assertGreaterEqual(score, before)
        self.assertLessEqual(score, after + 1)

    def test_zadd_to_fullscan_with_future_score(self):
        """_defer_adaptive_first ZADDs company to poll:fullscan with score=now+delay."""
        from config import REDIS_POLL_FULLSCAN
        from workers.fullscan import FULLSCAN_ADAPTIVE_FIRST_DELAY_S
        r = MagicMock()
        before = time.time()
        from workers.fullscan import _defer_adaptive_first
        _defer_adaptive_first("TestCo", r)

        fullscan_calls = [
            c for c in r.zadd.call_args_list
            if c[0][0] == REDIS_POLL_FULLSCAN
        ]
        self.assertEqual(len(fullscan_calls), 1)
        score = list(fullscan_calls[0][0][1].values())[0]
        expected_min = before + FULLSCAN_ADAPTIVE_FIRST_DELAY_S
        self.assertGreaterEqual(score, expected_min)

    def test_fullscan_delay_is_900_seconds(self):
        """FULLSCAN_ADAPTIVE_FIRST_DELAY_S = 900."""
        from workers.fullscan import FULLSCAN_ADAPTIVE_FIRST_DELAY_S
        self.assertEqual(FULLSCAN_ADAPTIVE_FIRST_DELAY_S, 900)

    def test_both_zadd_calls_made(self):
        """_defer_adaptive_first calls zadd exactly twice."""
        r = MagicMock()
        from workers.fullscan import _defer_adaptive_first
        _defer_adaptive_first("TestCo", r)
        self.assertEqual(r.zadd.call_count, 2)


# ─────────────────────────────────────────────────────────────────────────────
# TestGetCycleStart
# ─────────────────────────────────────────────────────────────────────────────

class TestGetCycleStart(unittest.TestCase):

    def test_returns_none_when_key_not_set(self):
        """r.get returns None → _get_cycle_start returns None."""
        r = MagicMock()
        r.get.return_value = None
        from workers.fullscan import _get_cycle_start
        result = _get_cycle_start(r)
        self.assertIsNone(result)

    def test_returns_float_when_key_set(self):
        """r.get returns bytes string → converted to float."""
        r = MagicMock()
        r.get.return_value = b"1714900000.0"
        from workers.fullscan import _get_cycle_start
        result = _get_cycle_start(r)
        self.assertAlmostEqual(result, 1714900000.0, places=1)

    def test_uses_cycle_start_key(self):
        """Uses REDIS_CYCLE_START config key."""
        from config import REDIS_CYCLE_START
        r = MagicMock()
        r.get.return_value = None
        from workers.fullscan import _get_cycle_start
        _get_cycle_start(r)
        r.get.assert_called_once_with(REDIS_CYCLE_START)


# ─────────────────────────────────────────────────────────────────────────────
# TestBuildDetailPayload
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildDetailPayload(unittest.TestCase):

    def _job(self, **kwargs):
        base = {
            "job_id":   "jid_001",
            "job_url":  "https://example.com/job/1",
            "title":    "Software Engineer",
            "location": "New York, NY",
            "posted_at": "2026-05-01",
            "description": "Great job",
            "content_hash": "abc123",
            "skill_score": 0.8,
        }
        base.update(kwargs)
        return base

    def _run(self, company="TestCo", platform="greenhouse",
             job=None, slug_info="slug123"):
        from workers.fullscan import _build_detail_payload
        return _build_detail_payload(
            company, platform, job or self._job(), slug_info,
        )

    # ── Required base keys ────────────────────────────────────────────────────

    def test_required_keys_present(self):
        """Payload has all required base keys."""
        payload = self._run()
        for key in ("company", "ats_platform", "job_id", "job_url",
                    "title", "location", "posted_at", "description",
                    "content_hash", "skill_score", "found_by",
                    "enqueued_at", "slug_info"):
            self.assertIn(key, payload, f"Missing key: {key}")

    def test_found_by_is_tier2_fullscan(self):
        """found_by is always 'tier2_fullscan'."""
        payload = self._run()
        self.assertEqual(payload["found_by"], "tier2_fullscan")

    def test_company_and_platform_stored(self):
        """company and ats_platform fields stored correctly."""
        payload = self._run(company="AcmeCorp", platform="lever")
        self.assertEqual(payload["company"],      "AcmeCorp")
        self.assertEqual(payload["ats_platform"], "lever")

    def test_job_id_stored(self):
        """job_id from job dict stored in payload."""
        payload = self._run(job=self._job(job_id="unique_job_123"))
        self.assertEqual(payload["job_id"], "unique_job_123")

    # ── posted_at handling ────────────────────────────────────────────────────

    def test_posted_at_datetime_converted_to_isoformat(self):
        """datetime posted_at → isoformat string."""
        dt = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        payload = self._run(job=self._job(posted_at=dt))
        self.assertIsInstance(payload["posted_at"], str)
        self.assertIn("2026-05-01", payload["posted_at"])

    def test_posted_at_string_passed_through(self):
        """String posted_at preserved as-is."""
        payload = self._run(job=self._job(posted_at="2026-05-01"))
        self.assertEqual(payload["posted_at"], "2026-05-01")

    # ── Platform-specific extra keys ─────────────────────────────────────────

    def test_workday_external_path_forwarded(self):
        """Workday _external_path forwarded."""
        job = self._job(_external_path="/d/1/2/3")
        payload = self._run(platform="workday", job=job)
        self.assertEqual(payload["_external_path"], "/d/1/2/3")

    def test_icims_base_url_and_feed_type_forwarded(self):
        """iCIMS _base_url and _feed_type forwarded."""
        job = self._job(_base_url="https://icims.example.com", _feed_type="rss")
        payload = self._run(platform="icims", job=job)
        self.assertEqual(payload["_base_url"],  "https://icims.example.com")
        self.assertEqual(payload["_feed_type"], "rss")

    def test_jobvite_slug_forwarded(self):
        """Jobvite _slug forwarded."""
        job = self._job(_slug="jobvite-slug-xyz")
        payload = self._run(platform="jobvite", job=job)
        self.assertEqual(payload["_slug"], "jobvite-slug-xyz")

    def test_taleo_contest_no_forwarded(self):
        """Taleo _contest_no forwarded."""
        job = self._job(_contest_no="CONTEST001")
        payload = self._run(platform="taleo", job=job)
        self.assertEqual(payload["_contest_no"], "CONTEST001")

    def test_smartrecruiters_company_slug_forwarded(self):
        """SmartRecruiters _company_slug forwarded."""
        job = self._job(_company_slug="acme-inc")
        payload = self._run(platform="smartrecruiters", job=job)
        self.assertEqual(payload["_company_slug"], "acme-inc")

    def test_unknown_platform_no_crash(self):
        """Unknown platform does not crash — no extra keys added."""
        job = self._job()
        payload = self._run(platform="custom", job=job)
        self.assertIsNotNone(payload)

    def test_greenhouse_no_platform_specific_keys(self):
        """Greenhouse (not in PLATFORM_DETAIL_KEYS) → no platform-specific keys."""
        job = self._job()
        payload = self._run(platform="greenhouse", job=job)
        for key in ("_external_path", "_base_url", "_feed_type",
                    "_slug", "_contest_no", "_company_slug"):
            self.assertNotIn(key, payload)

    def test_country_code_forwarded_when_present(self):
        """_country_code forwarded if present in job."""
        job = self._job(_country_code="US")
        payload = self._run(job=job)
        self.assertEqual(payload["_country_code"], "US")

    def test_country_code_not_added_when_absent(self):
        """_country_code not in payload when absent from job."""
        payload = self._run(job=self._job())
        self.assertNotIn("_country_code", payload)

    def test_platform_specific_key_not_added_when_none(self):
        """Platform-specific key NOT added when job value is None."""
        job = self._job(_external_path=None)
        payload = self._run(platform="workday", job=job)
        self.assertNotIn("_external_path", payload)

    def test_slug_info_stored_in_payload(self):
        """slug_info is stored in payload."""
        payload = self._run(slug_info={"board": "acme", "token": "abc"})
        self.assertEqual(payload["slug_info"], {"board": "acme", "token": "abc"})

    def test_enqueued_at_is_iso_string(self):
        """enqueued_at is an ISO format timestamp string."""
        payload = self._run()
        # Should parse without error
        from datetime import datetime
        datetime.fromisoformat(payload["enqueued_at"].replace("Z", "+00:00"))


# ─────────────────────────────────────────────────────────────────────────────
# TestFullscanConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestFullscanConstants(unittest.TestCase):

    def test_fullscan_chunk_size_is_50(self):
        """FULLSCAN_CHUNK_SIZE = 50."""
        from workers.fullscan import FULLSCAN_CHUNK_SIZE
        self.assertEqual(FULLSCAN_CHUNK_SIZE, 50)

    def test_bloom_capacity_is_10000(self):
        """FULLSCAN_BLOOM_CAPACITY = 10_000."""
        from workers.fullscan import FULLSCAN_BLOOM_CAPACITY
        self.assertEqual(FULLSCAN_BLOOM_CAPACITY, 10_000)

    def test_adaptive_first_delay_is_900(self):
        """FULLSCAN_ADAPTIVE_FIRST_DELAY_S = 900 (15 minutes)."""
        from workers.fullscan import FULLSCAN_ADAPTIVE_FIRST_DELAY_S
        self.assertEqual(FULLSCAN_ADAPTIVE_FIRST_DELAY_S, 900)

    def test_pause_poll_secs_positive(self):
        """FULLSCAN_PAUSE_POLL_SECS is a positive integer."""
        from workers.fullscan import FULLSCAN_PAUSE_POLL_SECS
        self.assertGreater(FULLSCAN_PAUSE_POLL_SECS, 0)

    def test_bloom_capacity_positive(self):
        """FULLSCAN_BLOOM_CAPACITY > 0."""
        from workers.fullscan import FULLSCAN_BLOOM_CAPACITY
        self.assertGreater(FULLSCAN_BLOOM_CAPACITY, 0)


# ─────────────────────────────────────────────────────────────────────────────
# TestCompleteFullscanDbEMA  (Phase 2 — duration EMA persisted to DB)
# ─────────────────────────────────────────────────────────────────────────────

class TestCompleteFullscanDbEMA(unittest.TestCase):
    """
    _complete_fullscan_db() now computes an EMA of scan durations and writes
    two new columns: last_fullscan_duration_s and avg_fullscan_duration_s.

    Formula: new_avg = 0.3 * duration_s + 0.7 * prev_avg_duration_s
    Default prev_avg = 30.0 s (for companies with no prior scan history).
    """

    _EMA_ALPHA = 0.3

    def _run(self, duration_s, prev_avg=30.0, new_jobs=0, company="Acme"):
        """
        Call _complete_fullscan_db() with a mocked DB connection.
        Returns the SQL params passed to conn.execute().
        """
        captured = {}

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_conn.__enter__ = lambda s: s
        mock_conn.__exit__ = MagicMock(return_value=False)

        def _capture_execute(sql, params=None):
            captured["sql"]    = sql
            captured["params"] = params
            return mock_cursor

        mock_conn.execute.side_effect = _capture_execute

        with patch("workers.fullscan.get_conn", return_value=mock_conn):
            from workers.fullscan import _complete_fullscan_db
            _complete_fullscan_db(
                company=company, platform="workday",
                new_jobs=new_jobs, interval_s=86400,
                duration_s=duration_s,
                prev_avg_duration_s=prev_avg,
            )

        return captured

    def test_first_scan_ema_from_default_prev(self):
        """
        First scan (prev_avg=30.0): new_avg = 0.3 * duration + 0.7 * 30.
        """
        duration = 600.0
        expected = self._EMA_ALPHA * duration + (1 - self._EMA_ALPHA) * 30.0
        result = self._run(duration_s=duration, prev_avg=30.0)
        params = result["params"]
        # params = (company, interval_s, new_jobs, int(duration_s), new_avg, ...)
        # avg_fullscan_duration_s (EMA) is at index 4
        actual_avg = params[4]
        self.assertAlmostEqual(actual_avg, expected, places=3)

    def test_subsequent_scan_ema_from_prev(self):
        """
        Subsequent scan: new_avg = 0.3 * duration + 0.7 * prev_avg.
        """
        duration = 1200.0
        prev_avg = 900.0
        expected = self._EMA_ALPHA * duration + (1 - self._EMA_ALPHA) * prev_avg
        result = self._run(duration_s=duration, prev_avg=prev_avg)
        actual_avg = result["params"][4]
        self.assertAlmostEqual(actual_avg, expected, places=3)

    def test_last_duration_written_as_int(self):
        """last_fullscan_duration_s is written as int(duration_s)."""
        duration = 123.7
        result = self._run(duration_s=duration)
        actual_last = result["params"][3]
        self.assertEqual(actual_last, int(duration))

    def test_both_ema_columns_in_sql(self):
        """SQL UPDATE includes both last_fullscan_duration_s and avg_fullscan_duration_s."""
        result = self._run(duration_s=500.0)
        sql = result["sql"]
        self.assertIn("last_fullscan_duration_s", sql)
        self.assertIn("avg_fullscan_duration_s", sql)

    def test_alpha_is_0_3(self):
        """Verify a=0.3 by checking a known computation."""
        duration = 1000.0
        prev_avg = 500.0
        # 0.3 * 1000 + 0.7 * 500 = 300 + 350 = 650
        expected = 650.0
        result = self._run(duration_s=duration, prev_avg=prev_avg)
        actual_avg = result["params"][3]
        self.assertAlmostEqual(actual_avg, expected, places=3)

    def test_zero_duration_valid(self):
        """Duration=0 is valid (very fast scan). EMA = 0.3*0 + 0.7*prev = 0.7*prev."""
        prev_avg = 60.0
        expected = 0.7 * prev_avg
        result = self._run(duration_s=0.0, prev_avg=prev_avg)
        actual_avg = result["params"][3]
        self.assertAlmostEqual(actual_avg, expected, places=3)


# ─────────────────────────────────────────────────────────────────────────────
# TestInflightFullscanLifecycle  (Phase 2 — inflight:fullscan ZSET tracking)
# ─────────────────────────────────────────────────────────────────────────────

class TestInflightFullscanLifecycle(unittest.TestCase):
    """
    _run_fullscan() writes company to inflight:fullscan ZSET at scan start
    and removes it in the finally block regardless of outcome.
    """

    def _make_minimal_r(self):
        """Redis mock that makes _run_fullscan() bail out early (lock failure)."""
        r = MagicMock()
        # SET NX EX → returns None (lock already held) → scan returns "skipped"
        r.set.return_value = None
        r.exists.return_value = False
        return r

    def test_inflight_zadd_written_at_scan_start(self):
        """
        ZADD to inflight:fullscan is called when the scan actually starts
        (after lock is acquired).  get_ats_module must return a non-None module
        so the scan proceeds past the early-exit at Step 2 and enters the
        try block where the inflight ZADD lives.
        """
        from config import REDIS_INFLIGHT_FULLSCAN

        r = MagicMock()
        r.set.return_value = True   # lock acquired
        r.exists.return_value = False  # maintenance not active
        r.zrangebyscore.return_value = []

        captured_zadd_calls = []

        original_zadd = r.zadd
        def _capture_zadd(key, mapping, **kw):
            captured_zadd_calls.append((key, mapping))
            return original_zadd(key, mapping, **kw)
        r.zadd = _capture_zadd

        # last_poll_at must be non-None to prevent _bootstrap_warming_adaptive DB call
        minimal_state = {
            "full_scan_interrupted": False,
            "interrupted_at_page": None,
            "full_scan_interval_s": 86400,
            "last_poll_at": 1_700_000_000.0 - 86400,
            "last_full_scan_at": None,
            "avg_fullscan_duration_s": 30.0,
        }
        company_row = {"ats_platform": "greenhouse", "ats_slug": "testco"}
        mock_ats = MagicMock()
        mock_ats.fetch_jobs.return_value = []

        with patch("workers.fullscan._get_fullscan_state", return_value=minimal_state), \
             patch("workers.fullscan.get_company_row", return_value=company_row), \
             patch("workers.fullscan.get_ats_module", return_value=mock_ats), \
             patch("workers.fullscan.parse_slug", return_value={}), \
             patch("workers.fullscan.get_config", return_value={}), \
             patch("workers.fullscan._complete_fullscan_db"), \
             patch("workers.fullscan._get_cycle_start", return_value=None), \
             patch("workers.fullscan.set_heartbeat"), \
             patch("workers.fullscan.set_progress"), \
             patch("workers.fullscan.clear_heartbeat"), \
             patch("workers.fullscan._release_lock"):
            from workers.fullscan import _run_fullscan
            _run_fullscan("TestCo", r)

        inflight_adds = [(k, m) for k, m in captured_zadd_calls
                         if k == REDIS_INFLIGHT_FULLSCAN]
        self.assertTrue(len(inflight_adds) >= 1,
                        "Expected at least one ZADD to inflight:fullscan")

    def test_inflight_zrem_called_in_finally(self):
        """
        ZREM from inflight:fullscan must be called in the finally block even
        when the scan completes normally.  get_ats_module must be non-None so
        the scan enters the try/finally block where ZADD and ZREM happen.
        """
        from config import REDIS_INFLIGHT_FULLSCAN

        r = MagicMock()
        r.set.return_value = True   # lock acquired
        r.exists.return_value = False
        r.zrangebyscore.return_value = []

        minimal_state = {
            "full_scan_interrupted": False, "interrupted_at_page": None,
            "full_scan_interval_s": 86400,
            "last_poll_at": 1_700_000_000.0 - 86400,
            "last_full_scan_at": None, "avg_fullscan_duration_s": 30.0,
        }
        mock_ats = MagicMock()
        mock_ats.fetch_jobs.return_value = []

        with patch("workers.fullscan._get_fullscan_state", return_value=minimal_state), \
             patch("workers.fullscan.get_company_row",
                   return_value={"ats_platform": "greenhouse", "ats_slug": "testco"}), \
             patch("workers.fullscan.get_ats_module", return_value=mock_ats), \
             patch("workers.fullscan.parse_slug", return_value={}), \
             patch("workers.fullscan.get_config", return_value={}), \
             patch("workers.fullscan._complete_fullscan_db"), \
             patch("workers.fullscan._get_cycle_start", return_value=None), \
             patch("workers.fullscan.set_heartbeat"), \
             patch("workers.fullscan.set_progress"), \
             patch("workers.fullscan.clear_heartbeat"), \
             patch("workers.fullscan._release_lock"):
            from workers.fullscan import _run_fullscan
            _run_fullscan("TestCo", r)

        # ZREM should have been called on REDIS_INFLIGHT_FULLSCAN
        zrem_calls = [c for c in r.zrem.call_args_list
                      if c[0][0] == REDIS_INFLIGHT_FULLSCAN]
        self.assertTrue(len(zrem_calls) >= 1,
                        "Expected ZREM on inflight:fullscan in finally block")

    def test_redis_unavailable_for_zadd_non_fatal(self):
        """
        If Redis ZADD raises for inflight tracking, the outer except block in
        _run_fullscan catches it and returns a result dict — no propagation.
        get_ats_module must be non-None to reach the try block.
        """
        from config import REDIS_INFLIGHT_FULLSCAN

        r = MagicMock()
        r.set.return_value = True
        r.exists.return_value = False
        r.zrangebyscore.return_value = []

        original_zadd = MagicMock()
        def _zadd(key, mapping, **kw):
            if key == REDIS_INFLIGHT_FULLSCAN:
                raise ConnectionError("Redis unavailable")
            return original_zadd(key, mapping, **kw)
        r.zadd.side_effect = _zadd

        minimal_state = {
            "full_scan_interrupted": False, "interrupted_at_page": None,
            "full_scan_interval_s": 86400,
            "last_poll_at": 1_700_000_000.0 - 86400,
            "last_full_scan_at": None, "avg_fullscan_duration_s": 30.0,
        }
        mock_ats = MagicMock()
        mock_ats.fetch_jobs.return_value = []

        try:
            with patch("workers.fullscan._get_fullscan_state", return_value=minimal_state), \
                 patch("workers.fullscan.get_company_row",
                       return_value={"ats_platform": "greenhouse", "ats_slug": "testco"}), \
                 patch("workers.fullscan.get_ats_module", return_value=mock_ats), \
                 patch("workers.fullscan.parse_slug", return_value={}), \
                 patch("workers.fullscan.get_config", return_value={}), \
                 patch("workers.fullscan._complete_fullscan_db"), \
                 patch("workers.fullscan._get_cycle_start", return_value=None), \
                 patch("workers.fullscan.set_heartbeat"), \
                 patch("workers.fullscan.set_progress"), \
                 patch("workers.fullscan.clear_heartbeat"), \
                 patch("workers.fullscan._release_lock"):
                from workers.fullscan import _run_fullscan
                result = _run_fullscan("TestCo", r)
            # Outer except catches the ConnectionError → returns result dict
            self.assertIsInstance(result, dict)
        except ConnectionError:
            self.fail("ConnectionError from ZADD should be caught by _run_fullscan")

    def test_inflight_key_name_matches_config(self):
        """REDIS_INFLIGHT_FULLSCAN constant is 'inflight:fullscan'."""
        from config import REDIS_INFLIGHT_FULLSCAN
        self.assertEqual(REDIS_INFLIGHT_FULLSCAN, "inflight:fullscan")


if __name__ == "__main__":
    unittest.main(verbosity=2)
