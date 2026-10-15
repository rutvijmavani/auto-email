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
        self.assertEqual(rep.stop_reason, fr.STOP_PAGE_CAP)

    def test_helper_depth_cut_plus_failed_file_is_request_failed(self):
        rep = fr.sitemap_report({"files": 3, "failed": 1, "too_deep": 2}, []).finalize()
        self.assertFalse(rep.complete)
        self.assertEqual(rep.stop_reason, fr.STOP_REQUEST_FAILED)

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

    def test_sitemap_truncated_child_is_incomplete(self):
        from jobs.ats import sitemap
        cut = _urlset("https://x.com/jobs/1234567", "https://x.com/jobs/2345678")[:-len("</urlset>")]
        pages = {
            "https://x.com/index.xml": _index("https://x.com/a.xml"),
            "https://x.com/a.xml":     cut,
        }
        with patch.object(sitemap, "fetch_html", side_effect=lambda u, **k: _resp(pages[u])):
            jobs = sitemap.fetch_jobs({"url": "https://x.com/index.xml"}, "X")
        rep = fr.report_of(jobs).finalize()
        self.assertEqual(rep.stop_reason, fr.STOP_REQUEST_FAILED)
        self.assertFalse(rep.complete)
        self.assertIn("never closed", rep.note)

    def test_helper_truncated_counts_as_cut(self):
        stats = {"files": 2, "failed": 0, "too_deep": 0, "truncated": 1, "problems": ["root <urlset> never closed"]}
        rep = fr.sitemap_report(stats, [{"job_id": "1"}]).finalize()
        self.assertEqual(rep.stop_reason, fr.STOP_REQUEST_FAILED)
        self.assertFalse(rep.complete)
        self.assertIn("1 truncated", rep.note)


class TestCheckSitemapBody(unittest.TestCase):

    def _check(self, resp, soup=None):
        stats = fr.new_sitemap_stats()
        return fr.check_sitemap_body(resp, stats, soup=soup), stats

    @staticmethod
    def _soup(text, parser="html.parser"):
        from bs4 import BeautifulSoup
        return BeautifulSoup(text, parser)

    def test_whole_file_ok_and_bytes_counted(self):
        text = _urlset("https://x.com/a", "https://x.com/b")
        ok, stats = self._check(_resp(text), soup=self._soup(text))
        self.assertTrue(ok)
        self.assertEqual(stats["truncated"], 0)
        self.assertEqual(stats["bytes"], len(text.encode()))

    def test_unclosed_root(self):
        ok, stats = self._check(_resp(_urlset("https://x.com/a")[:-5]))
        self.assertFalse(ok)
        self.assertEqual(stats["truncated"], 1)
        self.assertIn("never closed", stats["problems"][0])

    def test_parser_dropped_elements_any_tag_name(self):
        text = "<feed><Job><id>1</id></Job><Job><id>2</id></Job><Job><id>3</id></Job></feed>"
        partial = self._soup("<feed><Job><id>1</id></Job></feed>")     # parser kept 1 of 3
        ok, stats = self._check(_resp(text), soup=partial)
        self.assertFalse(ok)
        self.assertIn("parsed 1 of 3", stats["problems"][0])
        self.assertIn("job", stats["problems"][0])

    def test_matching_counts_with_xml_parser_and_namespace(self):
        text = "<ns:feed><ns:Job>a</ns:Job><ns:Job>b</ns:Job></ns:feed>"
        ok, _ = self._check(_resp(text), soup=self._soup(text, "xml"))
        self.assertTrue(ok)

    def test_cdata_and_comment_markup_not_counted(self):
        text = "<feed><Job><d><![CDATA[<p>x</p><p>y</p>]]></d><!-- <Job> --></Job></feed>"
        ok, _ = self._check(_resp(text), soup=self._soup(text))
        self.assertTrue(ok)

    def test_compressed_wire_bytes_match_declared(self):
        text = _urlset("https://x.com/a")
        resp = SimpleNamespace(text=text, content=text.encode(), raw=SimpleNamespace(tell=lambda: 10),
                               headers={"Content-Length": "10", "Content-Encoding": "gzip"})
        ok, _ = self._check(resp, soup=self._soup(text))
        self.assertTrue(ok)

    def test_compressed_wire_bytes_short(self):
        text = _urlset("https://x.com/a")
        resp = SimpleNamespace(text=text, content=text.encode(), raw=SimpleNamespace(tell=lambda: 6),
                               headers={"Content-Length": "10", "Content-Encoding": "gzip"})
        ok, stats = self._check(resp, soup=self._soup(text))
        self.assertFalse(ok)
        self.assertIn("received 6 of 10", stats["problems"][0])

    def test_compressed_without_raw_is_not_compared(self):
        text = _urlset("https://x.com/a")
        resp = SimpleNamespace(text=text, content=text.encode(),
                               headers={"Content-Length": "999", "Content-Encoding": "gzip"})
        ok, _ = self._check(resp, soup=self._soup(text))
        self.assertTrue(ok)

    def test_uncompressed_short_vs_content_length(self):
        text = _urlset("https://x.com/a")
        resp = SimpleNamespace(text=text, content=text.encode(), headers={"Content-Length": str(len(text) + 50)})
        ok, stats = self._check(resp, soup=self._soup(text))
        self.assertFalse(ok)
        self.assertIn("received", stats["problems"][0])

    def test_uncompressed_exact_content_length_ok(self):
        text = _urlset("https://x.com/a")
        resp = SimpleNamespace(text=text, content=text.encode(), headers={"Content-Length": str(len(text.encode()))})
        ok, _ = self._check(resp, soup=self._soup(text))
        self.assertTrue(ok)

    def test_empty_self_closed_root_ok(self):
        ok, _ = self._check(_resp("<?xml version='1.0'?><urlset/>"))
        self.assertTrue(ok)

    def test_bom_prefixed_file_ok(self):
        text = "﻿" + _urlset("https://x.com/a")
        ok, _ = self._check(_resp(text), soup=self._soup(text))
        self.assertTrue(ok)

    def test_mock_without_headers_or_content(self):
        ok, stats = self._check(SimpleNamespace(text=_urlset("https://x.com/a")))
        self.assertTrue(ok)
        self.assertGreater(stats["bytes"], 0)


class TestSitemapBodyInFeeders(unittest.TestCase):

    def test_phenom_truncated_sitemap(self):
        from jobs.ats import phenom
        stats = fr.new_sitemap_stats()
        cut = _urlset("https://x.com/job/NYC/Engineer/1")[:-len("</urlset>")]
        with patch.object(phenom, "fetch_html", return_value=_resp(cut)):
            phenom._follow_sitemap("https://x.com/sitemap.xml", _stats=stats)
        self.assertGreaterEqual(stats["truncated"], 1)

    def test_talentbrew_truncated_sitemap(self):
        from jobs.ats import talentbrew
        stats = fr.new_sitemap_stats()
        cut = _urlset("https://x.com/job/nyc/engineer/1/2")[:-len("</urlset>")]
        with patch.object(talentbrew, "fetch_html", return_value=_resp(cut)):
            talentbrew._follow_sitemap("https://x.com/sitemap.xml", _stats=stats)
        self.assertGreaterEqual(stats["truncated"], 1)


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

    def _run_with_total(self, listing, pages_total):
        from jobs.ats import icims
        it = iter(listing)

        def page(*a, info=None, **k):
            if info is not None and pages_total is not None:
                info.setdefault("pages_total", pages_total)
            return next(it)

        with patch.object(icims, "_resolve_base_url", return_value="https://b"), \
             patch.object(icims, "_fetch_listing_page", side_effect=page), \
             patch.object(icims.time, "sleep"):
            return fr.report_of(icims.fetch_jobs("slug", "Co"))

    def test_all_pages_read_is_complete(self):
        rep = self._run_with_total([[{"job_id": "1"}], [{"job_id": "2"}], []], 2)
        self.assertEqual((rep.pages, rep.reported_pages), (2, 2))
        self.assertTrue(rep.complete)

    def test_server_ran_dry_before_last_page_is_incomplete(self):
        rep = self._run_with_total([[{"job_id": "1"}], []], 4)
        self.assertEqual(rep.stop_reason, fr.STOP_EMPTY_PAGE)
        self.assertFalse(rep.complete)
        self.assertEqual(rep.note, "read 1 of 4 pages")

    def test_page_cap_below_board_pages_is_incomplete_with_note(self):
        from jobs.ats import icims
        n = {"i": 0}

        def listing(*a, info=None, **k):
            n["i"] += 1
            if info is not None:
                info.setdefault("pages_total", icims.MAX_PAGES + 5)
            return [{"job_id": str(n["i"])}]

        with patch.object(icims, "_resolve_base_url", return_value="https://b"), \
             patch.object(icims, "_fetch_listing_page", side_effect=listing), \
             patch.object(icims.time, "sleep"):
            rep = fr.report_of(icims.fetch_jobs("slug", "Co"))
        self.assertEqual(rep.stop_reason, fr.STOP_PAGE_CAP)
        self.assertFalse(rep.complete)
        self.assertEqual(rep.note, f"read {icims.MAX_PAGES} of {icims.MAX_PAGES + 5} pages")

    def test_page_total_parsed_from_header(self):
        from bs4 import BeautifulSoup
        from jobs.ats import icims
        html = ('<div class="container-fluid iCIMS_SearchResultsHeader"><h2 class="iCIMS_SubHeader">'
                'Search Results\nPage 1 of 4 \n</h2></div><div class="iCIMS_Paging">Page 2 of 4</div>')
        self.assertEqual(icims._page_total(BeautifulSoup(html, "html.parser")), 4)
        self.assertIsNone(icims._page_total(BeautifulSoup("<p>Page 1 of 4</p>", "html.parser")))


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

    def _run_total(self, batches, total):
        from jobs.ats import paycom
        it = iter(batches)

        def search(*a, info=None, **k):
            if info is not None and total is not None:
                info.setdefault("total", total)
            return next(it, [])

        with patch.object(paycom, "_parse_slug", return_value=("h", "k")), \
             patch.object(paycom, "_get_session", return_value=(object(), "jwt", "api")), \
             patch.object(paycom, "_search_page", side_effect=search), \
             patch.object(paycom, "_preview_to_stub",
                          side_effect=lambda p, h, k, c: {"job_id": str(p["jobId"]), "job_url": f"u{p['jobId']}"}):
            return fr.report_of(paycom.fetch_jobs("slug", "Co"))

    def test_total_matched_is_complete(self):
        rep = self._run_total([[{"jobId": 1}, {"jobId": 2}]], 2)
        self.assertEqual((rep.reported_total, rep.total_source), (2, fr.TOTAL_ATS))
        self.assertTrue(rep.complete)

    def test_total_larger_than_fetched_is_incomplete(self):
        rep = self._run_total([[{"jobId": 1}, {"jobId": 2}]], 50)
        self.assertEqual(rep.stop_reason, fr.STOP_EMPTY_PAGE)
        self.assertFalse(rep.complete)

    def test_zero_total_empty_board_is_complete(self):
        self.assertTrue(self._run_total([[]], 0).complete)

    def test_search_page_reads_count_field(self):
        from jobs.ats import paycom

        class Resp:
            def raise_for_status(self): pass
            def json(self): return {"jobPostingPreviews": [{"jobId": 1}], "jobPostingPreviewsCount": 105}

        class Sess:
            def post(self, *a, **k): return Resp()

        info = {}
        self.assertEqual(paycom._search_page(Sess(), "j", "api", "h", "k", 0, info=info), [{"jobId": 1}])
        self.assertEqual(info, {"total": 105})


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

    def test_cursor_board_requests_page_two_with_first_cursor(self):
        from jobs.ats import custom_career as cc
        calls = []
        slug_info = {"pagination": {"type": "cursor", "cursor_field": "next", "page_size": 2}}

        def fake_fetch(session, si, page=1, offset=0, cursor=None):
            calls.append(cursor)
            return [{"id": 3}, {"id": 4}]
        outcome = {}
        with patch.object(cc, "_fetch_page", side_effect=fake_fetch), \
             patch.object(cc, "_extract_jobs_array", side_effect=lambda raw, si: raw), \
             patch.object(cc, "_detect_format", return_value=("json", {})), \
             patch.object(cc.time, "sleep"):
            extra = cc._paginate(None, slug_info, [{"id": 1}, {"id": 2}], None, "Co",
                                 outcome=outcome, first_cursor="C1")
        self.assertEqual(calls[0], "C1")
        self.assertEqual(len(extra), 2)

    def test_cursor_board_without_first_cursor_is_one_page(self):
        from jobs.ats import custom_career as cc
        outcome = {}
        slug_info = {"pagination": {"type": "cursor", "cursor_field": "next", "page_size": 2}}
        with patch.object(cc, "_fetch_page", side_effect=AssertionError("must not fetch")):
            extra = cc._paginate(None, slug_info, [{"id": 1}, {"id": 2}], None, "Co", outcome=outcome)
        self.assertEqual(extra, [])
        self.assertEqual(outcome["stop"], fr.STOP_ALL_PAGES)

    def test_fetch_jobs_no_config(self):
        from jobs.ats import custom_career as cc
        self.assertEqual(fr.report_of(cc.fetch_jobs(None, "Co")).stop_reason, fr.STOP_NO_CONFIG)
        self.assertEqual(fr.report_of(cc.fetch_jobs({"x": 1}, "Co")).stop_reason, fr.STOP_NO_CONFIG)


if __name__ == "__main__":
    unittest.main()
