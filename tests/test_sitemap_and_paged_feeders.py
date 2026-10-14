# tests/test_sitemap_and_paged_feeders.py — FetchReport on sitemap, avature, ADP, iCIMS, Paycom, custom_career

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from jobs.ats import fetch_report as fr


def _resp(text):
    return SimpleNamespace(text=text)


def _urlset(*urls):
    body = "".join(f"<url><loc>{u}</loc></url>" for u in urls)
    return f"<?xml version='1.0'?><urlset>{body}</urlset>"


def _index(*locs):
    body = "".join(f"<sitemap><loc>{u}</loc></sitemap>" for u in locs)
    return f"<?xml version='1.0'?><sitemapindex>{body}</sitemapindex>"


class TestSitemapReport(unittest.TestCase):

    def test_helper_clean_read(self):
        stats = fr.new_sitemap_stats()
        stats["files"] = 2
        rep = fr.sitemap_report(stats, [{"job_id": "1"}, {"job_id": "2"}]).finalize()
        self.assertEqual(rep.stop_reason, fr.STOP_ALL_PAGES)
        self.assertIsNone(rep.complete)          # no total to compare against
        self.assertEqual(rep.fetched_distinct, 2)

    def test_helper_failed_file_is_incomplete(self):
        stats = {"files": 4, "failed": 1, "too_deep": 0}
        rep = fr.sitemap_report(stats, [{"job_id": "1"}]).finalize()
        self.assertEqual(rep.stop_reason, fr.STOP_REQUEST_FAILED)
        self.assertFalse(rep.complete)
        self.assertIn("1 of 4", rep.note)

    def test_helper_depth_cut_is_incomplete(self):
        rep = fr.sitemap_report({"files": 1, "failed": 0, "too_deep": 2}, []).finalize()
        self.assertFalse(rep.complete)

    def test_helper_empty_sitemap(self):
        rep = fr.sitemap_report({"files": 1, "failed": 0, "too_deep": 0}, []).finalize()
        self.assertEqual(rep.stop_reason, fr.STOP_EMPTY_PAGE)

    def test_sitemap_index_failed_child(self):
        from jobs.ats import sitemap
        pages = {
            "https://x.com/index.xml": _index("https://x.com/a.xml", "https://x.com/b.xml"),
            "https://x.com/a.xml":     _urlset("https://x.com/jobs/1234567", "https://x.com/jobs/2345678"),
        }
        with patch.object(sitemap, "fetch_html",
                          side_effect=lambda u, **k: _resp(pages[u]) if u in pages else None):
            jobs = sitemap.fetch_jobs({"url": "https://x.com/index.xml"}, "X")
        rep = fr.report_of(jobs)
        self.assertEqual(rep.stop_reason, fr.STOP_REQUEST_FAILED)
        self.assertFalse(rep.complete)

    def test_sitemap_index_all_children_ok(self):
        from jobs.ats import sitemap
        pages = {
            "https://x.com/index.xml": _index("https://x.com/a.xml"),
            "https://x.com/a.xml":     _urlset("https://x.com/jobs/1234567"),
        }
        with patch.object(sitemap, "fetch_html", side_effect=lambda u, **k: _resp(pages[u])):
            jobs = sitemap.fetch_jobs({"url": "https://x.com/index.xml"}, "X")
        rep = fr.report_of(jobs)
        self.assertEqual(rep.stop_reason, fr.STOP_ALL_PAGES)
        self.assertIsNone(rep.complete)

    def test_sitemap_request_failure_and_no_config(self):
        from jobs.ats import sitemap
        with patch.object(sitemap, "fetch_html", return_value=None):
            self.assertEqual(fr.report_of(sitemap.fetch_jobs({"url": "https://x.com/s.xml"}, "X")).stop_reason,
                             fr.STOP_REQUEST_FAILED)
        self.assertEqual(fr.report_of(sitemap.fetch_jobs({}, "X")).stop_reason, fr.STOP_NO_CONFIG)
        self.assertEqual(fr.report_of(sitemap.fetch_jobs({"x": 1}, "X")).stop_reason, fr.STOP_NO_CONFIG)

    def test_avature_no_config(self):
        from jobs.ats import avature
        self.assertEqual(fr.report_of(avature.fetch_jobs(None, "X")).stop_reason, fr.STOP_NO_CONFIG)
        self.assertEqual(fr.report_of(avature.fetch_jobs({"path": "careers"}, "X")).stop_reason,
                         fr.STOP_NO_CONFIG)


class TestAdp(unittest.TestCase):

    def _run(self, pages):
        """pages: list of (reqs, total) returned in order by _fetch_reqs."""
        from jobs.ats import adp
        it = iter(pages)

        def parse(reqs, slug, company):
            return [{"job_id": str(r), "title": "t", "job_url": f"u{r}"} for r in reqs]

        with patch.object(adp, "_make_session"), patch.object(adp, "_warm_session"), \
             patch.object(adp, "_fetch_config", return_value={"x": 1}), \
             patch.object(adp, "_fetch_reqs", side_effect=lambda *a, **k: next(it)), \
             patch.object(adp, "_parse_reqs", side_effect=parse):
            return adp.fetch_jobs("slug", "Co")

    def test_complete_with_total(self):
        rep = fr.report_of(self._run([([1, 2], 4), ([3, 4], 4)]))
        self.assertEqual(rep.stop_reason, fr.STOP_ALL_PAGES)
        self.assertTrue(rep.complete)

    def test_total_zero_keeps_paging(self):
        # count of 0/absent must not mean "stop after one page"
        res = self._run([([1, 2], 0), ([3], 0), ([], 0)])
        self.assertEqual(len(res), 3)
        rep = fr.report_of(res)
        self.assertEqual(rep.stop_reason, fr.STOP_EMPTY_PAGE)
        self.assertIsNone(rep.complete)

    def test_repeated_page(self):
        rep = fr.report_of(self._run([([1, 2], 10), ([1, 2], 10)]))
        self.assertEqual(rep.stop_reason, fr.STOP_REPEAT_PAGE)
        self.assertFalse(rep.complete)

    def test_request_failure_mid_way(self):
        rep = fr.report_of(self._run([([1, 2], 10), (None, None)]))
        self.assertEqual(rep.stop_reason, fr.STOP_REQUEST_FAILED)
        self.assertFalse(rep.complete)

    def test_no_slug(self):
        from jobs.ats import adp
        self.assertEqual(fr.report_of(adp.fetch_jobs("", "Co")).stop_reason, fr.STOP_NO_CONFIG)


class TestIcims(unittest.TestCase):

    def _run(self, listing):
        from jobs.ats import icims
        it = iter(listing)
        with patch.object(icims, "_resolve_base_url", return_value="https://b"), \
             patch.object(icims, "_fetch_listing_page", side_effect=lambda *a, **k: next(it)), \
             patch.object(icims.time, "sleep"):
            return icims.fetch_jobs("slug", "Co")

    def test_clean_end(self):
        rep = fr.report_of(self._run([[{"job_id": "1"}], [{"job_id": "2"}], []]))
        self.assertEqual(rep.stop_reason, fr.STOP_EMPTY_PAGE)
        self.assertEqual(rep.fetched_distinct, 2)
        self.assertIsNone(rep.complete)

    def test_timeout_page_is_not_end_of_results(self):
        rep = fr.report_of(self._run([[{"job_id": "1"}], None]))
        self.assertEqual(rep.stop_reason, fr.STOP_REQUEST_FAILED)
        self.assertFalse(rep.complete)

    def test_page_cap(self):
        from jobs.ats import icims
        n = {"i": 0}

        def listing(*a, **k):
            n["i"] += 1
            return [{"job_id": str(n["i"])}]

        with patch.object(icims, "_resolve_base_url", return_value="https://b"), \
             patch.object(icims, "_fetch_listing_page", side_effect=listing), \
             patch.object(icims.time, "sleep"):
            rep = fr.report_of(icims.fetch_jobs("slug", "Co"))
        self.assertEqual(rep.stop_reason, fr.STOP_PAGE_CAP)
        self.assertFalse(rep.complete)

    def test_no_url_variant(self):
        from jobs.ats import icims
        with patch.object(icims, "_resolve_base_url", return_value=None):
            self.assertEqual(fr.report_of(icims.fetch_jobs("slug", "Co")).stop_reason, fr.STOP_NO_CONFIG)


class TestPaycom(unittest.TestCase):

    def _run(self, batches):
        from jobs.ats import paycom
        it = iter(batches)
        with patch.object(paycom, "_parse_slug", return_value=("h", "k")), \
             patch.object(paycom, "_get_session", return_value=(object(), "jwt", "api")), \
             patch.object(paycom, "_search_page", side_effect=lambda *a, **k: next(it, [])), \
             patch.object(paycom, "_preview_to_stub",
                          side_effect=lambda p, h, k, c: {"job_id": str(p["jobId"]), "job_url": f"u{p['jobId']}"}):
            return paycom.fetch_jobs("slug", "Co"), paycom._TAKE

    def test_short_page_ends_clean(self):
        res, _ = self._run([[{"jobId": 1}, {"jobId": 2}]])
        rep = fr.report_of(res)
        self.assertEqual(rep.stop_reason, fr.STOP_EMPTY_PAGE)
        self.assertEqual(len(res), 2)

    def test_skip_ignored_terminates_as_repeat(self):
        from jobs.ats import paycom
        full = [{"jobId": i} for i in range(paycom._TAKE)]
        res, _ = self._run([full, full])
        rep = fr.report_of(res)
        self.assertEqual(rep.stop_reason, fr.STOP_REPEAT_PAGE)
        self.assertFalse(rep.complete)

    def test_no_config(self):
        from jobs.ats import paycom
        with patch.object(paycom, "_parse_slug", return_value=("", "")):
            self.assertEqual(fr.report_of(paycom.fetch_jobs("x", "Co")).stop_reason, fr.STOP_NO_CONFIG)


class TestCustomCareerPaginate(unittest.TestCase):

    def _paginate(self, pages, total, first=None, pag_type="offset"):
        from jobs.ats import custom_career as cc
        it = iter(pages)
        first = first or [{"id": 1}, {"id": 2}]
        outcome = {}
        slug_info = {"pagination": {"type": pag_type, "page_size": len(first)}}
        with patch.object(cc, "_fetch_page", side_effect=lambda *a, **k: next(it)), \
             patch.object(cc, "_extract_jobs_array", side_effect=lambda raw, si: raw), \
             patch.object(cc.time, "sleep"):
            extra = cc._paginate(None, slug_info, first, total, "Co", outcome=outcome)
        return extra, outcome

    def test_total_reached(self):
        extra, out = self._paginate([[{"id": 3}, {"id": 4}]], total=4)
        self.assertEqual(len(extra), 2)
        self.assertEqual(out["stop"], fr.STOP_ALL_PAGES)

    def test_exhaustion_ends_on_empty_page(self):
        extra, out = self._paginate([[{"id": 3}, {"id": 4}], []], total=None)
        self.assertEqual(len(extra), 2)
        self.assertEqual(out["stop"], fr.STOP_EMPTY_PAGE)

    def test_repeat_page(self):
        extra, out = self._paginate([[{"id": 1}, {"id": 2}]], total=10)
        self.assertEqual(extra, [])
        self.assertEqual(out["stop"], fr.STOP_REPEAT_PAGE)

    def test_failed_page(self):
        extra, out = self._paginate([None], total=10)
        self.assertEqual(out["stop"], fr.STOP_REQUEST_FAILED)

    def test_short_first_page_is_complete(self):
        from jobs.ats import custom_career as cc
        outcome = {}
        slug_info = {"pagination": {"type": "offset", "page_size": 10}}
        extra = cc._paginate(None, slug_info, [{"id": 1}], 1, "Co", outcome=outcome)
        self.assertEqual(extra, [])
        self.assertEqual(outcome["stop"], fr.STOP_ALL_PAGES)

    def test_fetch_jobs_no_config(self):
        from jobs.ats import custom_career as cc
        self.assertEqual(fr.report_of(cc.fetch_jobs(None, "Co")).stop_reason, fr.STOP_NO_CONFIG)
        self.assertEqual(fr.report_of(cc.fetch_jobs({"x": 1}, "Co")).stop_reason, fr.STOP_NO_CONFIG)


if __name__ == "__main__":
    unittest.main()
