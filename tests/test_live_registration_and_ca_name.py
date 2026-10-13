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
             patch.object(jm, "filter_jobs", return_value=[]), \
             patch.object(jm, "update_company_check"):
            jm._process_company(row, 1, 1)
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

    def test_skips_companies_queued_unclaimed_in_stream(self):
        import workers.scheduler as sch
        r = self._redis()
        with patch.object(sch, "_get_stream_undelivered_companies",
                          return_value={"a"}):
            n, sched = self._go(r, ["a", "b"], scanned=[])
        self.assertEqual(n, 1)
        self.assertEqual(sched.call_args_list[0].args[2], "b")

    def test_unreadable_stream_registers_nothing(self):
        import workers.scheduler as sch
        r = self._redis()
        with patch.object(sch, "_get_stream_undelivered_companies",
                          return_value=None):
            n, sched = self._go(r, ["a"], scanned=[])
        self.assertEqual(n, 0)
        sched.assert_not_called()

    def test_undelivered_companies_reads_entries_after_last_delivered(self):
        import workers.scheduler as sch
        r = MagicMock()
        r.xinfo_groups.return_value = [
            {"name": sch.STREAM_CONSUMER_GROUP, "last-delivered-id": "5-0"}
        ]
        r.xrange.return_value = [("6-0", {"company": "x"}),
                                 (b"7-0", {b"company": b"y"})]
        got = sch._get_stream_undelivered_companies(r, "stream:fullscan")
        self.assertEqual(got, {"x", "y"})
        self.assertEqual(r.xrange.call_args.kwargs["min"], "(5-0")
        r.xinfo_groups.side_effect = RuntimeError("down")
        self.assertIsNone(sch._get_stream_undelivered_companies(r, "stream:fullscan"))


class TestDeregisterDisabledCompanies(unittest.TestCase):

    def _go(self, monitorable, zadaptive=(), zfull=(), stats=(), inflight=()):
        import workers.scheduler as sch
        r = MagicMock()
        r.zrange.side_effect = lambda k, a, b: (
            list(zadaptive) if k == "poll:adaptive" else list(zfull))
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        pipe.zscore.side_effect = lambda k, c: None
        # execute() is called twice: the inflight probe, then the cleanup batch.
        probe = []
        pipe.zscore.side_effect = lambda k, c: probe.append(c)
        pipe.execute.side_effect = lambda: (
            [1.0 if c in inflight else None for c in probe] if probe else [])
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"company": c} for c in stats]
        with patch("db.job_monitor.get_monitorable_companies",
                   return_value=[{"company": c} for c in monitorable]), \
             patch.object(sch, "get_conn", return_value=conn):
            n = sch._deregister_disabled_companies(r)
        return n, r, pipe, conn

    def test_cleans_disabled_company_everywhere_and_leaves_monitorable(self):
        n, r, pipe, conn = self._go(
            ["ca:1"], zadaptive=["ca:1", "ca:2"], zfull=["ca:2"], stats=["ca:1", "ca:2"])
        self.assertEqual(n, 1)
        zrems = {(c.args[0], c.args[1]) for c in pipe.zrem.call_args_list}
        self.assertEqual(zrems, {("poll:adaptive", "ca:2"), ("poll:fullscan", "ca:2"),
                                 ("inflight:fullscan", "ca:2")})
        deleted = set(pipe.delete.call_args.args)
        self.assertIn("fullscan:lock:ca:2", deleted)
        self.assertIn("poll:registered:ca:2", deleted)
        self.assertIn("stats:ca:2", deleted)
        self.assertFalse(any("ca:1" in k for k in deleted))
        self.assertEqual(pipe.hdel.call_args.args[1], "ca:2")
        sql, params = conn.execute.call_args_list[-1].args
        self.assertIn("DELETE FROM company_poll_stats", sql)
        self.assertEqual(params[0], ["ca:2"])

    def test_bloom_filters_are_kept(self):
        _, _, pipe, _ = self._go(["a"], zfull=["b"], stats=["b"])
        self.assertFalse(any("bloom" in k or "adaptive_seen" in k
                             for k in pipe.delete.call_args.args))

    def test_empty_monitorable_list_wipes_nothing(self):
        n, r, pipe, conn = self._go([], zadaptive=["x"], zfull=["y"], stats=["x"])
        self.assertEqual(n, 0)
        pipe.zrem.assert_not_called()
        conn.execute.assert_not_called()

    def test_company_currently_fullscanning_is_left_for_next_tick(self):
        n, _, pipe, conn = self._go(["a"], zfull=["b"], stats=["b"], inflight=["b"])
        self.assertEqual(n, 0)
        pipe.zrem.assert_not_called()

    def test_nothing_to_clean_is_a_noop(self):
        n, _, pipe, _ = self._go(["a", "b"], zadaptive=["a"], zfull=["b"], stats=["a", "b"])
        self.assertEqual(n, 0)
        pipe.zrem.assert_not_called()

    def test_reconcile_registers_even_if_deregister_fails(self):
        import workers.scheduler as sch
        with patch.object(sch, "_deregister_disabled_companies",
                          side_effect=RuntimeError("db down")), \
             patch.object(sch, "_register_new_companies") as reg:
            sch._reconcile_company_registry(MagicMock())
        reg.assert_called_once()


class TestWorkersSkipDisabledCompany(unittest.TestCase):

    def test_is_company_row_active(self):
        from db.job_monitor import is_company_row_active as act
        self.assertTrue(act({}))                                   # fail-open
        self.assertTrue(act({"is_monitored": True, "stale_since": None}))
        self.assertFalse(act({"is_monitored": False}))
        self.assertFalse(act({"is_monitored": None}))
        self.assertFalse(act({"is_monitored": True, "stale_since": "2026-10-01"}))

    def test_fullscan_skips_disabled_without_rescheduling(self):
        import workers.fullscan as fs
        r = MagicMock()
        row = {"is_monitored": False, "ats_platform": "greenhouse", "ats_slug": "x"}
        with patch.object(fs, "get_company_row", return_value=row):
            res = fs._run_fullscan("ca:9", r, dc_key="greenhouse")
        self.assertEqual(res["outcome"], "skipped")
        r.zadd.assert_not_called()
        r.zrem.assert_called_once_with("inflight:fullscans:greenhouse", "ca:9")

    def test_scan_worker_flags_disabled_and_skips_scan(self):
        import workers.scan_worker as sw
        row = {"is_monitored": True, "stale_since": "2026-10-01",
               "ats_platform": "greenhouse", "ats_slug": "x"}
        with patch.object(sw, "get_company_row", return_value=row), \
             patch.object(sw, "get_redis", return_value=MagicMock()), \
             patch.object(sw, "set_heartbeat") as hb:
            res = sw._run_listing_scan({"company": "ca:9", "scan_type": "adaptive"})
        self.assertTrue(res.get("disabled"))
        self.assertFalse(res["success"])
        hb.assert_not_called()


if __name__ == "__main__":
    unittest.main()
