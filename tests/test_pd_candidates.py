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

    def test_unknown_source_rejected(self):
        with self.assertRaises(KeyError):
            _sql(pc.rule3_clusters, 3, 3, "bogus")


if __name__ == "__main__":
    unittest.main()
