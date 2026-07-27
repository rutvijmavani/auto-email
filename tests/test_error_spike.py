"""
tests/test_error_spike.py — Error spike flag and outage state machine.

adjust_concurrency() (http_client.py):
  · spike flag written when error_rate > CONCURRENCY_ERROR_RATE_REDUCE
  · payload contains error_rate, baseline, spike_factor, ts
  · flag NOT written when error_rate is below threshold

_check_error_spikes() (manager.py):
  · platform already in outage → skipped entirely, no commands
  · no before_rate + concurrency at floor + errors high → deprioritize + snapshot
  · no before_rate + concurrency ABOVE floor → no deprioritize (loop has room)
  · before_rate exists + error resolved → consec_reductions deleted, no outage
  · before_rate exists + still erroring → consec_reductions incremented
  · consec_reductions reaches threshold → outage command sent, counter cleared
  · spike flag missing (TTL expired) → scan returns nothing, no commands sent
"""

import json
import sys
import os
import time
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    CONCURRENCY_ERROR_RATE_REDUCE,
    CONCURRENCY_FLOOR,
    CONCURRENCY_FLOOR_DEFAULT,
    WORKER_CONSEC_REDUCTIONS_THRESHOLD,
    REDIS_CONCURRENCY_LIMIT_PREFIX,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_spike_payload(platform, error_rate=0.25, baseline=0.05):
    spike_factor = error_rate / (baseline + 0.001)
    return json.dumps({
        "error_rate":   error_rate,
        "baseline":     baseline,
        "spike_factor": spike_factor,
        "ts":           time.time(),
    })


def _make_r_for_spikes(
    platform,
    *,
    error_rate=0.25,
    baseline=0.05,
    before_rate=None,
    consec=0,
    in_outage=False,
    concurrency_at_floor=True,
    spike_missing=False,
):
    """
    Build a mock Redis for _check_error_spikes scenarios.

    Keyed values returned by r.get():
      - spike flag    : JSON payload (or None if spike_missing)
      - before_rate   : before_rate float as string (or None)
      - consec key    : consec as string (or None if 0)
      - concurrency   : floor (at floor) or floor+2 (above floor)
    """
    spike_key  = f"manager:platform:{platform}:error_spike"
    before_key = f"worker:reduction:before_rate:{platform}"
    consec_key = f"worker:consec_reductions:{platform}"
    outage_key = f"worker:outage:{platform}"
    limit_key  = f"{REDIS_CONCURRENCY_LIMIT_PREFIX}:{platform}"
    floor      = CONCURRENCY_FLOOR.get(platform, CONCURRENCY_FLOOR_DEFAULT)

    r = MagicMock()
    r.scan.return_value = (0, [] if spike_missing else [spike_key])
    r.exists.side_effect = lambda key: 1 if (in_outage and key == outage_key) else 0

    spike_raw = _make_spike_payload(platform, error_rate, baseline)

    def get_side(key):
        k = key.decode() if isinstance(key, bytes) else key
        if k == spike_key:
            return spike_raw if not spike_missing else None
        if k == before_key:
            return str(before_rate) if before_rate is not None else None
        if k == consec_key:
            return str(consec) if consec > 0 else None
        if k == limit_key:
            return str(floor if concurrency_at_floor else floor + 2)
        return None

    r.get.side_effect = get_side

    _counter = [consec]

    def incr_side(key):
        _counter[0] += 1
        return _counter[0]

    r.incr.side_effect = incr_side

    return r


def _run_check(r):
    from workers.manager import _check_error_spikes
    with patch("db.api_health.record_scaling_event", return_value=None):
        _check_error_spikes(r)


# ─────────────────────────────────────────────────────────────────────────────
# adjust_concurrency() — spike flag
# ─────────────────────────────────────────────────────────────────────────────

class TestAdjustConcurrencySpike(unittest.TestCase):

    def _call(self, r, key, error_rate, baseline=0.05):
        from workers.http_client import adjust_concurrency
        with patch("workers.http_client.get_baseline_error_rate", return_value=baseline):
            adjust_concurrency(r, key, error_rate)

    def test_spike_flag_written_on_reduction(self):
        """Spike flag is SET when error_rate > CONCURRENCY_ERROR_RATE_REDUCE."""
        r = MagicMock()
        r.get.return_value = b"5"   # current limit
        self._call(r, "greenhouse", error_rate=CONCURRENCY_ERROR_RATE_REDUCE + 0.05)

        set_calls = [c for c in r.set.call_args_list
                     if "manager:platform:greenhouse:error_spike" in str(c)]
        self.assertEqual(len(set_calls), 1, "spike flag must be written once")
        payload = json.loads(set_calls[0].args[1])
        for field in ("error_rate", "baseline", "spike_factor", "ts"):
            self.assertIn(field, payload, f"payload missing '{field}'")

    def test_spike_flag_not_written_at_low_error_rate(self):
        """Spike flag is NOT written when error_rate < reduce threshold."""
        r = MagicMock()
        r.get.return_value = b"5"
        # Below the reduce threshold AND below the increase threshold too
        # → adjust_concurrency returns early with no flag writes
        low_rate = CONCURRENCY_ERROR_RATE_REDUCE - 0.05
        if low_rate < 0:
            low_rate = 0.0
        from config import CONCURRENCY_ERROR_RATE_INCREASE
        # Use a rate that's in the stable band (no reduce, no increase)
        stable_rate = (CONCURRENCY_ERROR_RATE_REDUCE + CONCURRENCY_ERROR_RATE_INCREASE) / 2
        if stable_rate >= CONCURRENCY_ERROR_RATE_REDUCE:
            stable_rate = CONCURRENCY_ERROR_RATE_REDUCE - 0.01
        self._call(r, "greenhouse", error_rate=max(0.0, stable_rate))

        spike_sets = [c for c in r.set.call_args_list if "error_spike" in str(c)]
        self.assertEqual(len(spike_sets), 0, "no spike flag below threshold")


# ─────────────────────────────────────────────────────────────────────────────
# _check_error_spikes() — skip outage
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckErrorSpikesOutage(unittest.TestCase):

    def test_skips_platform_already_in_outage(self):
        """Platform in outage → r.get(spike_key) is never read, no commands sent."""
        r = _make_r_for_spikes("greenhouse", in_outage=True)
        _run_check(r)
        r.rpush.assert_not_called()
        before_sets = [c for c in r.set.call_args_list if "before_rate" in str(c)]
        self.assertEqual(len(before_sets), 0)


# ─────────────────────────────────────────────────────────────────────────────
# _check_error_spikes() — deprioritize path
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckErrorSpikesDeprioritize(unittest.TestCase):

    def test_deprioritize_when_at_floor(self):
        """No before_rate + at concurrency floor + high errors → deprioritize command."""
        r = _make_r_for_spikes("greenhouse", error_rate=0.30, concurrency_at_floor=True)
        _run_check(r)

        before_sets = [c for c in r.set.call_args_list if "before_rate" in str(c)]
        self.assertGreaterEqual(len(before_sets), 1, "before_rate snapshot must be written")

        deprio = [c for c in r.rpush.call_args_list
                  if "platform:deprioritize:greenhouse" in str(c)]
        self.assertEqual(len(deprio), 1, "exactly one deprioritize command")

    def test_no_deprioritize_above_floor(self):
        """Concurrency above floor → feedback loop still has room, no deprioritize."""
        r = _make_r_for_spikes("greenhouse", error_rate=0.30, concurrency_at_floor=False)
        _run_check(r)

        deprio = [c for c in r.rpush.call_args_list
                  if "platform:deprioritize:greenhouse" in str(c)]
        self.assertEqual(len(deprio), 0, "no deprioritize above floor")


# ─────────────────────────────────────────────────────────────────────────────
# _check_error_spikes() — effectiveness check path
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckErrorSpikesEffectiveness(unittest.TestCase):

    def test_error_resolved_deletes_consec_counter(self):
        """before_rate present + error now below threshold → consec_reductions deleted."""
        r = _make_r_for_spikes(
            "workday",
            error_rate=CONCURRENCY_ERROR_RATE_REDUCE - 0.01,
            before_rate=0.30,
            consec=2,
        )
        _run_check(r)

        consec_key = "worker:consec_reductions:workday"
        deleted = [c for c in r.delete.call_args_list if consec_key in str(c)]
        self.assertGreaterEqual(len(deleted), 1, "consec counter must be deleted on resolution")
        r.incr.assert_not_called()

    def test_still_erroring_increments_consec(self):
        """before_rate present + still erroring → consec_reductions incremented."""
        r = _make_r_for_spikes(
            "workday",
            error_rate=CONCURRENCY_ERROR_RATE_REDUCE + 0.10,
            before_rate=0.25,
            consec=1,
        )
        _run_check(r)

        r.incr.assert_called_once_with("worker:consec_reductions:workday")

    def test_outage_declared_at_threshold(self):
        """consec_reductions reaches threshold → outage:set command + counter cleared."""
        consec_before_incr = WORKER_CONSEC_REDUCTIONS_THRESHOLD - 1

        r = _make_r_for_spikes(
            "workday",
            error_rate=CONCURRENCY_ERROR_RATE_REDUCE + 0.10,
            before_rate=0.30,
            consec=consec_before_incr,
        )
        _run_check(r)

        outage_cmds = [c for c in r.rpush.call_args_list
                       if "platform:outage:workday:set" in str(c)]
        self.assertEqual(len(outage_cmds), 1, "outage command must be sent")

        consec_key = "worker:consec_reductions:workday"
        cleared = [c for c in r.delete.call_args_list if consec_key in str(c)]
        self.assertGreaterEqual(len(cleared), 1, "consec counter must be cleared after outage")


# ─────────────────────────────────────────────────────────────────────────────
# _check_error_spikes() — missing flag (TTL expiry)
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckErrorSpikesMissingFlag(unittest.TestCase):

    def test_no_action_when_spike_flag_missing(self):
        """Spike flag TTL expired → scan returns empty list → no commands sent."""
        r = _make_r_for_spikes("greenhouse", spike_missing=True)
        _run_check(r)
        r.rpush.assert_not_called()
        before_sets = [c for c in r.set.call_args_list if "before_rate" in str(c)]
        self.assertEqual(len(before_sets), 0)


if __name__ == "__main__":
    unittest.main()
