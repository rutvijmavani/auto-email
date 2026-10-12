"""Tests for db/company_ats_admin.py — Discover page company_ats maintenance."""
import json
import unittest
from unittest.mock import MagicMock, patch

import db.company_ats_admin as adm


def _conn(fetchone_seq):
    """MagicMock connection whose cursor().fetchone() yields the given sequence."""
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.fetchone.side_effect = list(fetchone_seq)
    return conn, cur


class TestValidate(unittest.TestCase):

    def test_custom_rejected(self):
        _, err = adm.validate_platform_slug("custom", '{"url": "x"}')
        self.assertIn("prospective form", err)

    def test_unknown_platform_rejected(self):
        self.assertIsNotNone(adm.validate_platform_slug("nope", "x")[1])

    def test_string_platform_accepts_plain_slug(self):
        self.assertEqual(adm.validate_platform_slug("greenhouse", " acme ")[0], "acme")

    def test_empty_slug_rejected(self):
        self.assertIsNotNone(adm.validate_platform_slug("greenhouse", "  ")[1])

    def test_json_platform_rejects_plain_string(self):
        self.assertIsNotNone(adm.validate_platform_slug("workday", "acme")[1])

    def test_json_platform_rejects_non_object_and_empty(self):
        self.assertIsNotNone(adm.validate_platform_slug("oracle_hcm", "[1]")[1])
        self.assertIsNotNone(adm.validate_platform_slug("oracle_hcm", "{}")[1])

    def test_workday_needs_slug_key(self):
        self.assertIsNotNone(adm.validate_platform_slug("workday", '{"wd": "wd5"}')[1])
        slug = json.dumps({"slug": "acme", "wd": "wd5", "path": "External"})
        self.assertEqual(adm.validate_platform_slug("workday", slug)[0], slug)

    def test_successfactors_needs_slug_dc_region(self):
        self.assertIsNotNone(adm.validate_platform_slug("successfactors", "acme")[1])
        self.assertIsNotNone(adm.validate_platform_slug("successfactors", '{"slug": "acme"}')[1])
        self.assertIsNotNone(adm.validate_platform_slug(
            "successfactors", '{"slug": "acme", "dc": "4"}')[1])
        slug = json.dumps({"slug": "acme", "dc": "4", "region": "com"})
        self.assertEqual(adm.validate_platform_slug("successfactors", slug)[0], slug)

    def test_editable_platforms_excludes_custom(self):
        plats = adm.editable_platforms()
        self.assertNotIn("custom", plats)
        self.assertIn("greenhouse", plats)


class TestMatchJobUrl(unittest.TestCase):

    def test_requires_full_url(self):
        self.assertIsNotNone(adm.match_job_url("boards.greenhouse.io/acme")[1])

    def test_no_match_returns_error_only(self):
        hit, err = adm.match_job_url("https://example.com/careers/123")
        self.assertIsNone(hit)
        self.assertIn("nothing was changed", err)

    def test_known_pattern_matches_and_validates(self):
        hit, err = adm.match_job_url("https://boards.greenhouse.io/acme/jobs/123")
        self.assertIsNone(err)
        self.assertEqual(hit["platform"], "greenhouse")
        self.assertEqual(hit["slug"], "acme")

    def test_incomplete_successfactors_urls_are_refused(self):
        for url in ("https://acme.jobs2web.com/job/x/1",
                    "https://acme.successfactors.com/careers"):
            hit, err = adm.match_job_url(url)
            self.assertIsNone(hit, url)
            self.assertIn("cannot be monitored", err)
        hit, err = adm.match_job_url(
            "https://career4.successfactors.com/careers?company=acme")
        self.assertIsNone(err)
        self.assertEqual(json.loads(hit["slug"]),
                         {"slug": "acme", "dc": "4", "region": "com", "path": "/careers"})

    def test_matched_custom_pattern_is_refused(self):
        with patch.object(adm, "match_ats_pattern",
                          return_value={"platform": "custom", "slug": '{"url": "x"}'}):
            hit, err = adm.match_job_url("https://x.example/jobs")
        self.assertIsNone(hit)
        self.assertIn("cannot be monitored", err)


class TestUpdateRow(unittest.TestCase):

    def test_overwrites_in_place_and_clears_stale(self):
        conn, cur = _conn([{"domain": "acme.com", "platform": "lever"}, None])
        ok, _ = adm.update_company_ats_row(7, "greenhouse", "acme", enable=True, conn=conn)
        self.assertTrue(ok)
        sql, params = cur.execute.call_args_list[-1].args
        self.assertIn("stale_since", sql)
        self.assertIn("consecutive_empty_days = 0", sql)
        self.assertIn("reviewed_at", sql)
        self.assertEqual(params, ("greenhouse", "acme", True, 7))
        conn.commit.assert_called_once()

    def test_same_platform_skips_collision_check(self):
        conn, cur = _conn([{"domain": "acme.com", "platform": "greenhouse"}])
        ok, _ = adm.update_company_ats_row(7, "greenhouse", "new", conn=conn)
        self.assertTrue(ok)
        self.assertEqual(cur.execute.call_count, 2)  # select row + update

    def test_collision_writes_nothing(self):
        conn, cur = _conn([{"domain": "acme.com", "platform": "lever"}, (1,)])
        ok, msg = adm.update_company_ats_row(7, "greenhouse", "acme", conn=conn)
        self.assertFalse(ok)
        self.assertIn("already has a greenhouse row", msg)
        conn.commit.assert_not_called()
        self.assertFalse(any("UPDATE company_ats" in c.args[0]
                             for c in cur.execute.call_args_list))

    def test_invalid_slug_never_touches_db(self):
        conn, cur = _conn([])
        ok, _ = adm.update_company_ats_row(7, "workday", "plain", conn=conn)
        self.assertFalse(ok)
        cur.execute.assert_not_called()

    def test_missing_row(self):
        conn, _ = _conn([None])
        ok, msg = adm.update_company_ats_row(7, "greenhouse", "acme", conn=conn)
        self.assertFalse(ok)
        self.assertIn("no longer exists", msg)


class TestInsertRow(unittest.TestCase):

    def test_inserts_manual_reviewed_row(self):
        conn, cur = _conn([None, {"petition_count": 42}])
        ok, _ = adm.insert_company_ats_row("123", "Acme.com", "Acme", "lever", "acme",
                                           enable=True, conn=conn)
        self.assertTrue(ok)
        sql, params = cur.execute.call_args_list[-1].args
        self.assertIn("'manual'", sql)
        self.assertEqual(params, ("123", "acme.com", "Acme", "lever", "acme", 42, True))

    def test_collision_and_missing_domain(self):
        conn, _ = _conn([(1,)])
        ok, msg = adm.insert_company_ats_row("1", "acme.com", "Acme", "lever", "a", conn=conn)
        self.assertFalse(ok)
        self.assertIn("already has", msg)
        conn, _ = _conn([])
        ok, msg = adm.insert_company_ats_row("1", "", "Acme", "lever", "a", conn=conn)
        self.assertFalse(ok)
        self.assertIn("No domain", msg)


class TestQueueRedetect(unittest.TestCase):

    def test_no_fein_refused(self):
        ok, _ = adm.queue_redetect("", r=MagicMock())
        self.assertFalse(ok)

    def test_pushes_enrichment_and_forced_redetect(self):
        r = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.fetchone.return_value = {"n": 500}
        with patch.object(adm, "get_conn", return_value=conn):
            ok, _ = adm.queue_redetect("123", r=r)
        self.assertTrue(ok)
        pushed = json.loads(r.lpush.call_args.args[1])
        self.assertEqual(pushed["fein"], "123")
        key, mapping = r.zadd.call_args.args
        self.assertEqual(key, adm.DISCOVERY_REDETECT)
        (member, score), = mapping.items()
        self.assertEqual(json.loads(member),
                         {"fein": "123", "trigger": "redetect", "source": "company_ats"})
        self.assertEqual(score, 500)


if __name__ == "__main__":
    unittest.main()
