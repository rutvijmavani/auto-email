"""
Tests for two 2026-10-11 changes:

  1. Monitor fallback (_process_company) must pass / track the real company
     name for company_ats rows ('ca:{id}' is only the tracking key).
  2. workers.scheduler._register_new_companies — live registration of newly
     enabled companies into poll:fullscan without a scheduler restart.
"""
import unittest
from unittest.mock import MagicMock, patch


class TestCompanyAtsDisplayName(unittest.TestCase):

    def _run(self, row):
        import jobs.job_monitor as jm
        ats_module = MagicMock()
        ats_module.fetch_jobs.return_value = []
        with patch.object(jm, "needs_redetection", return_value=False), \
             patch.object(jm, "get_config", return_value={}), \
             patch.object(jm, "get_ats_module", return_value=ats_module), \
             patch.object(jm, "_get_semaphore", return_value=MagicMock()), \
             patch.object(jm, "_parse_slug", return_value="slug"), \
             patch.object(jm, "get_tracked_urls_for_company",
                          return_value={}) as tracked, \
             patch.object(jm, "update_company_check") as upd:
            jm._process_company(row, 1, 1)
        return ats_module, tracked, upd

    def test_company_ats_row_uses_real_name_for_fetch_and_tracking(self):
        ats, tracked, upd = self._run({
            "company": "ca:34", "company_name": "Medtronic",
            "ats_platform": "workday", "ats_slug": "x",
        })
        ats.fetch_jobs.assert_called_once_with("slug", "Medtronic")
        tracked.assert_called_once_with("Medtronic")
        # write-back routing still uses the ca: key
        upd.assert_called_once_with("ca:34", found_jobs=False)

    def test_prospective_row_keeps_its_own_name(self):
        ats, tracked, _ = self._run({
            "company": "Acme", "company_name": None,
            "ats_platform": "workday", "ats_slug": "x",
        })
        ats.fetch_jobs.assert_called_once_with("slug", "Acme")
        tracked.assert_called_once_with("Acme")

    def test_missing_company_name_key_falls_back_to_company(self):
        ats, _, _ = self._run({
            "company": "Acme", "ats_platform": "workday", "ats_slug": "x",
        })
        ats.fetch_jobs.assert_called_once_with("slug", "Acme")


class TestSharedNameAbsenceGuard(unittest.TestCase):
    """Absence update is skipped when several monitorable entries share a name."""

    def _run(self, row):
        import jobs.job_monitor as jm
        ats_module = MagicMock()
        ats_module.fetch_jobs.return_value = [{"job_url": "u1", "title": "t"}]
        with patch.object(jm, "needs_redetection", return_value=False), \
             patch.object(jm, "get_config", return_value={}), \
             patch.object(jm, "get_ats_module", return_value=ats_module), \
             patch.object(jm, "_get_semaphore", return_value=MagicMock()), \
             patch.object(jm, "_parse_slug", return_value="slug"), \
             patch.object(jm, "get_tracked_urls_for_company",
                          return_value={"u1": 1, "other": 2}), \
             patch.object(jm, "reset_missing_days") as reset, \
             patch.object(jm, "increment_missing_days") as incr, \
             patch.object(jm, "update_company_check"):
            try:
                jm._process_company(row, 1, 1)
            except Exception:
                pass
        return reset, incr

    def test_shared_name_skips_increment_but_still_resets(self):
        reset, incr = self._run({
            "company": "ca:1", "company_name": "Acme", "name_shared": True,
            "ats_platform": "workday", "ats_slug": "x",
        })
        reset.assert_called_once_with([1])
        incr.assert_not_called()

    def test_unshared_name_still_increments_when_scan_complete(self):
        reset, incr = self._run({
            "company": "ca:1", "company_name": "Acme", "name_shared": False,
            "ats_platform": "workday", "ats_slug": "x",
        })
        reset.assert_called_once_with([1])
        incr.assert_called_once_with([2])


class TestRegisterNewCompanies(unittest.TestCase):

    def _redis(self, zfull=None, zadaptive=None, zinflight=None, markers=()):
        zfull, zadaptive, zinflight = zfull or {}, zadaptive or {}, zinflight or {}
        r = MagicMock()
        pipe = MagicMock()
        calls = []
        pipe.zscore.side_effect = lambda k, c: calls.append(("z", k, c))
        pipe.exists.side_effect = lambda k: calls.append(("e", k, None))

        def _execute():
            out = []
            for kind, key, comp in calls:
                if kind == "e":
                    out.append(1 if key.split(":", 2)[2] in markers else 0)
                else:
                    src = {"poll:fullscan": zfull, "poll:adaptive": zadaptive,
                           "inflight:fullscan": zinflight}[key]
                    out.append(src.get(comp))
            return out

        pipe.execute.side_effect = _execute
        r.pipeline.return_value = pipe
        return r

    def _go(self, r, monitorable, scanned, sched=1.0):
        import workers.scheduler as sch
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"company": c} for c in scanned
        ]
        with patch("db.job_monitor.get_monitorable_companies",
                   return_value=[{"company": c} for c in monitorable]), \
             patch.object(sch, "get_conn", return_value=conn), \
             patch.object(sch, "_atomic_schedule", return_value=sched) as sched_m:
            n = sch._register_new_companies(r)
        return n, sched_m

    def test_registers_only_never_scanned_companies(self):
        r = self._redis()
        n, sched = self._go(r, ["ca:1", "ca:2", "Old"], scanned=["Old"])
        self.assertEqual(n, 2)
        self.assertEqual({c.args[2] for c in sched.call_args_list}, {"ca:1", "ca:2"})
        for c in sched.call_args_list:
            self.assertTrue(c.kwargs["skip_if_future"])
            self.assertEqual(c.args[1], "poll:fullscan")

    def test_skips_companies_already_queued_inflight_or_marked(self):
        r = self._redis(zfull={"a": 5.0}, zadaptive={"b": 5.0},
                        zinflight={"c": 5.0}, markers=("d",))
        n, sched = self._go(r, ["a", "b", "c", "d", "e"], scanned=[])
        self.assertEqual(n, 1)
        self.assertEqual(sched.call_args_list[0].args[2], "e")

    def test_zero_score_member_counts_as_present(self):
        r = self._redis(zfull={"a": 0.0})
        n, _ = self._go(r, ["a"], scanned=[])
        self.assertEqual(n, 0)

    def test_batch_is_spread_not_clustered(self):
        r = self._redis()
        _, sched = self._go(r, ["a", "b", "c", "d"], scanned=[])
        targets = [c.args[3] for c in sched.call_args_list]
        self.assertEqual(targets, sorted(targets))
        self.assertGreater(targets[-1] - targets[0], 0)

    def test_marker_set_only_when_scheduled(self):
        r = self._redis()
        n, _ = self._go(r, ["a"], scanned=[], sched=None)
        self.assertEqual(n, 0)
        r.set.assert_not_called()

        r = self._redis()
        n, _ = self._go(r, ["a"], scanned=[])
        self.assertEqual(n, 1)
        self.assertEqual(r.set.call_args.args[0], "poll:registered:a")

    def test_nothing_monitorable_is_a_noop(self):
        r = self._redis()
        n, sched = self._go(r, [], scanned=[])
        self.assertEqual(n, 0)
        sched.assert_not_called()


if __name__ == "__main__":
    unittest.main()
