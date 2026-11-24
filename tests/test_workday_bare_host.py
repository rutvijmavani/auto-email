"""Workday bare host (`"wd": ""`): host helper, URL builders and detail guard — no network."""
import unittest
from unittest.mock import patch

from jobs.ats import workday


class TestWdPresent(unittest.TestCase):
    def test_empty_string_is_present(self):
        self.assertTrue(workday.wd_present(""))

    def test_value_is_present(self):
        self.assertTrue(workday.wd_present("wd5"))

    def test_none_is_missing(self):
        self.assertFalse(workday.wd_present(None))

    def test_myworkdaysite_needs_a_value(self):
        self.assertFalse(workday.wd_present("", "myworkdaysite"))
        self.assertTrue(workday.wd_present("wd5", "myworkdaysite"))


class TestBuilders(unittest.TestCase):
    def test_bare_host_domain_and_url(self):
        si = {"slug": "nxp", "wd": "", "path": "careers"}
        self.assertEqual(workday._build_domain(si), "https://nxp.myworkdayjobs.com")
        self.assertEqual(workday._build_url(si),
                         "https://nxp.myworkdayjobs.com/wday/cxs/nxp/careers/jobs")

    def test_wd_host_unchanged(self):
        si = {"slug": "att", "wd": "wd1", "path": "ATTGeneral"}
        self.assertEqual(workday._build_domain(si), "https://att.wd1.myworkdayjobs.com")
        self.assertEqual(workday._build_url(si),
                         "https://att.wd1.myworkdayjobs.com/wday/cxs/att/ATTGeneral/jobs")

    def test_myworkdaysite_unchanged(self):
        si = {"slug": "fid", "wd": "wd1", "path": "P", "site": "myworkdaysite"}
        self.assertEqual(workday._build_url(si),
                         "https://wd1.myworkdaysite.com/wday/cxs/fid/P/jobs")

    def test_missing_wd_key_still_an_error(self):
        with self.assertRaises(KeyError):
            workday._build_url({"slug": "nxp", "path": "careers"})


class TestDetailGuard(unittest.TestCase):
    def _job(self, **kw):
        job = {"_slug": "nxp", "_wd": "", "_path": "careers", "_external_path": "/job/x_1",
               "company": "NXP", "job_id": "1"}
        job.update(kw)
        return job

    def test_empty_wd_fetches_from_bare_host(self):
        with patch.object(workday, "fetch_json", return_value={}) as fj:
            workday.fetch_job_detail(self._job())
        self.assertEqual(fj.call_args.args[0],
                         "https://nxp.myworkdayjobs.com/wday/cxs/nxp/careers/job/x_1")

    def test_missing_wd_key_skips_request(self):
        job = self._job()
        del job["_wd"]
        with patch.object(workday, "fetch_json") as fj:
            out = workday.fetch_job_detail(job)
        fj.assert_not_called()
        self.assertEqual(out, job)

    def test_missing_slug_still_skips(self):
        with patch.object(workday, "fetch_json") as fj:
            workday.fetch_job_detail(self._job(_slug=""))
        fj.assert_not_called()


if __name__ == "__main__":
    unittest.main()
