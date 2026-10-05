"""scripts/pd_candidates.py — per-source clustering (oci | worker | relay) picks the right columns."""
import unittest
from unittest import mock

from scripts import pd_candidates as pc


def _sql(fn, *args, **kw):
    conn = mock.MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    fn(conn, *args, **kw)
    return conn.execute.call_args[0][0]


class TestSources(unittest.TestCase):
    def test_default_is_oci_and_uses_direct_columns(self):
        sql = _sql(pc.rule3_clusters, 3, 3)
        self.assertIn("body_hash", sql)
        self.assertIn("final_verdict", sql)
        self.assertNotIn("worker_body_hash", sql)
        self.assertNotIn("relay_body_hash", sql)

    def test_worker_source_clusters_on_worker_columns(self):
        sql = _sql(pc.rule3_clusters, 3, 3, "worker")
        self.assertIn("worker_body_hash", sql)
        self.assertIn("worker_verdict", sql)
        self.assertNotIn("final_verdict", sql)

    def test_relay_source_clusters_on_relay_columns(self):
        sql = _sql(pc.rule3_clusters, 3, 3, "relay")
        self.assertIn("relay_body_hash", sql)
        self.assertIn("relay_verdict", sql)

    def test_rule2_oci_filters_ext_refs_others_do_not(self):
        self.assertIn("ext_refs = 0", _sql(pc.rule2_titles, 3, "oci"))
        self.assertNotIn("ext_refs", _sql(pc.rule2_titles, 3, "relay"))
        self.assertIn("relay_title", _sql(pc.rule2_titles, 3, "relay"))

    def test_oci_rule3_requires_oci_decided_rows_everywhere(self):
        sql = _sql(pc.rule3_clusters, 3, 3, "oci")
        self.assertEqual(sql.count("resolved_by = 'oci'"), 3)   # count/open rows, verdicts + samples
        self.assertNotIn("resolved_by", _sql(pc.rule3_clusters, 3, 3, "relay"))

    def test_unknown_source_rejected(self):
        with self.assertRaises(KeyError):
            _sql(pc.rule3_clusters, 3, 3, "bogus")


def _cl(key, rule=3, source="oci", domains=5, title="Parked"):
    return {"key": key, "rule": rule, "source": source, "domains": domains,
            "title": title, "body_len": 100, "samples": "a.com, b.com"}


class TestCollectAndSplit(unittest.TestCase):
    def test_collect_keys_and_rule2_min_cluster(self):
        r3 = [{"body_hash": "abc", "domains": 4, "title": "T", "body_len": 9, "sample_domains": "a.com"}]
        r2 = [{"title": "big", "n": 5, "sample_domains": ["x.com"]},
              {"title": "small", "n": 2, "sample_domains": ["y.com"]}]
        with mock.patch.object(pc, "rule3_clusters", return_value=r3), \
             mock.patch.object(pc, "rule2_titles", return_value=r2):
            out = pc.collect_clusters(mock.MagicMock(), 3, 3)
        keys = {c["key"] for c in out}
        self.assertIn("3:oci:abc", keys)
        self.assertIn("2:relay:big", keys)
        self.assertNotIn("2:oci:small", keys)          # below min_cluster
        self.assertEqual(len(out), 3 * 2)              # (1 rule3 + 1 rule2) x 3 sources

    def test_split_new_known(self):
        conn = mock.MagicMock()
        conn.execute.return_value.fetchall.return_value = [{"cluster_key": "k1"}]
        new, known = pc.split_new(conn, [_cl("k1"), _cl("k2")])
        self.assertEqual([c["key"] for c in new], ["k2"])
        self.assertEqual([c["key"] for c in known], ["k1"])

    def test_split_empty_skips_query(self):
        conn = mock.MagicMock()
        self.assertEqual(pc.split_new(conn, []), ([], []))
        conn.execute.assert_not_called()

    def test_email_escapes_html(self):
        _, body = pc.build_email([_cl("k", title="<script>x</script>")])
        self.assertNotIn("<script>", body)


class TestRunNotify(unittest.TestCase):
    def _run(self, send, new, known):
        conn = mock.MagicMock()
        with mock.patch.object(pc, "collect_clusters", return_value=new + known), \
             mock.patch.object(pc, "split_new", return_value=(new, known)), \
             mock.patch.object(pc, "record_seen") as rec, \
             mock.patch.object(pc, "unnotified", return_value=[]), \
             mock.patch.object(pc, "mark_notified"), \
             mock.patch.object(pc, "prune_observations", return_value=1) as po, \
             mock.patch.object(pc, "prune_candidate_seen", return_value=2) as ps:
            rc = pc.run_notify(conn, 3, 3, send=send)
        return rc, rec, po, ps

    def test_sent_records_all_and_prunes(self):
        rc, rec, po, ps = self._run(lambda s, b: True, [_cl("n")], [_cl("k")])
        self.assertEqual(rc, 0)
        self.assertEqual([c["key"] for c in rec.call_args[0][1]], ["n", "k"])
        po.assert_called_once(); ps.assert_called_once()

    def test_send_failure_records_only_known_and_does_not_prune(self):
        for result in (False, None):
            rc, rec, po, ps = self._run(lambda s, b, r=result: r, [_cl("n")], [_cl("k")])
            self.assertEqual(rc, 1)
            self.assertEqual([c["key"] for c in rec.call_args[0][1]], ["k"])
            po.assert_not_called(); ps.assert_not_called()

    def test_nothing_new_sends_nothing_but_prunes(self):
        send = mock.MagicMock()
        rc, rec, po, ps = self._run(send, [], [_cl("k")])
        self.assertEqual(rc, 0)
        send.assert_not_called()
        po.assert_called_once(); ps.assert_called_once()

    def test_record_seen_new_only_stamps_notified_on_insert(self):
        conn = mock.MagicMock()
        pc.record_seen(conn, [_cl("k")])
        sql = conn.execute.call_args[0][0]
        self.assertIn("notified_at", sql.split("ON CONFLICT")[0])
        self.assertNotIn("notified_at", sql.split("DO UPDATE")[1])


def _rv(fein="1", name="Acme <b>", old="a.com", new="b.com"):
    return {"employer_fein": fein, "employer_name": name, "old_domain": old, "new_domain": new,
            "hint": "no-name-match", "source": "enrichment"}


class TestReviewSection(unittest.TestCase):
    def test_email_lists_review_pairs_escaped_and_pending_total(self):
        subject, body = pc.build_email([], [_rv()], 7)
        self.assertIn("held for name review", subject)
        self.assertNotIn("<b>", body)
        self.assertIn("b.com", body)
        self.assertIn("7 pending", body)

    def test_email_without_review_is_unchanged(self):
        subject, body = pc.build_email([_cl("k")])
        self.assertNotIn("name review", subject)
        self.assertNotIn("name gate", body)

    def _run(self, send, review, new=()):
        conn = mock.MagicMock()
        with mock.patch.object(pc, "collect_clusters", return_value=list(new)), \
             mock.patch.object(pc, "split_new", return_value=(list(new), [])), \
             mock.patch.object(pc, "record_seen"), \
             mock.patch.object(pc, "unnotified", return_value=review), \
             mock.patch.object(pc, "pending_count", return_value=3), \
             mock.patch.object(pc, "mark_notified") as mn, \
             mock.patch.object(pc, "prune_observations", return_value=0), \
             mock.patch.object(pc, "prune_candidate_seen", return_value=0):
            rc = pc.run_notify(conn, 3, 3, send=send)
        return rc, mn

    def test_review_only_week_sends_then_marks_notified(self):
        sent = []
        rc, mn = self._run(lambda s, b: sent.append(s) or True, [_rv()])
        self.assertEqual(rc, 0)
        self.assertEqual(len(sent), 1)
        self.assertEqual(mn.call_args[0][1], [("1", "a.com", "b.com")])

    def test_failed_send_leaves_review_pairs_unnotified(self):
        rc, mn = self._run(lambda s, b: False, [_rv()])
        self.assertEqual(rc, 1)
        mn.assert_not_called()


class TestReviewCommand(unittest.TestCase):
    def test_approve_and_reject_commit_and_set_exit_code(self):
        from scripts import pd_redirect_review as cmd
        for args, status, n, rc in ((["approve", "1", "b.com"], "approved", 1, 0),
                                    (["reject", "1"], "rejected", 0, 1)):
            conn = mock.MagicMock()
            with mock.patch.object(cmd, "get_conn", return_value=conn), \
                 mock.patch.object(cmd, "init_logging"), \
                 mock.patch.object(cmd, "decide", return_value=n) as d:
                self.assertEqual(cmd.main(args), rc)
            self.assertEqual(d.call_args[0][1:3], ("1", status))
            conn.commit.assert_called_once()


class TestPruneHelpers(unittest.TestCase):
    def test_prune_sql(self):
        from db import pd_probe
        conn = mock.MagicMock()
        conn.execute.return_value.rowcount = 4
        self.assertEqual(pd_probe.prune_observations(conn, 30), 4)
        self.assertIn("probed_at", conn.execute.call_args[0][0])
        self.assertEqual(pd_probe.prune_candidate_seen(conn, 90), 4)
        self.assertIn("last_seen_at", conn.execute.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
