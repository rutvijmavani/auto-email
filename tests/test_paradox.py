"""jobs/ats/paradox.py — listing crawl, completeness report, detail parse, fingerprints. Mocked, no network."""
import json
from unittest.mock import MagicMock, patch

import pytest

import config
from jobs.ats import paradox
from jobs.ats.fetch_report import (
    STOP_ALL_PAGES, STOP_EMPTY_PAGE, STOP_NO_CONFIG, STOP_PAGE_CAP, STOP_REPEAT_PAGE, STOP_REQUEST_FAILED,
    TOTAL_ATS, report_of,
)

HOST = "careers.example.com"


def _row(i, country="US", title=None, url=True):
    return {
        "reference": f"R{i}",
        "title": f"Job {i}" if title is None else title,
        "originalURL": f"job-{i}/job/ABC{i}" if url else "",
        "locations": [{"cityState": "Memphis, Tennessee", "country": "United States", "countryAbbr": country}],
    }


def _js(rows, total):
    return {"totalJob": total, "jobs": rows}


def _html(state):
    return f"<html><script>window.__PRELOAD_STATE__ = {json.dumps(state)};window.x=1;</script></html>"


def _crawl(pages, slug=None, **cfg):
    """Run fetch_jobs with listing pages served in order; a page that is an Exception is raised."""
    it = iter(pages)

    def fake(session, host, url, referer):
        nxt = next(it, _js([], 0))
        if isinstance(nxt, Exception):
            raise nxt
        return session, nxt

    with patch.object(paradox, "_get_listing_page", side_effect=fake), \
         patch.object(paradox, "_new_session", return_value=MagicMock()):
        for k, v in cfg.items():
            patch.object(config, k, v).start()
        try:
            return paradox.fetch_jobs(slug or {"host": HOST}, "Example Co")
        finally:
            patch.stopall()


# ── completeness ─────────────────────────────────────────────────────────────

def test_all_pages_complete():
    jobs = _crawl([_js([_row(1), _row(2)], 4), _js([_row(3), _row(4)], 4)])
    rep = report_of(jobs)
    assert len(jobs) == 4
    assert rep.stop_reason == STOP_ALL_PAGES and rep.complete is True
    assert rep.reported_total == 4 and rep.total_source == TOTAL_ATS and rep.pages == 2


def test_total_drift_within_tolerance_is_complete():
    rows = [_row(i) for i in range(1, 98)]
    rep = report_of(_crawl([_js(rows, 100), _js([], 100)]))
    assert rep.stop_reason == STOP_EMPTY_PAGE and rep.complete is True


def test_server_ran_dry_far_below_total_is_incomplete():
    rep = report_of(_crawl([_js([_row(1), _row(2)], 100), _js([], 100)]))
    assert rep.stop_reason == STOP_EMPTY_PAGE and rep.complete is False and rep.shortfall == 98


def test_repeat_page_is_incomplete():
    rep = report_of(_crawl([_js([_row(1)], 50), _js([_row(1)], 50)]))
    assert rep.stop_reason == STOP_REPEAT_PAGE and rep.complete is False


def test_failed_page_keeps_partial_jobs_but_is_incomplete():
    jobs = _crawl([_js([_row(1), _row(2)], 4), paradox._ParadoxError("HTTP 403")])
    rep = report_of(jobs)
    assert len(jobs) == 2 and rep.stop_reason == STOP_REQUEST_FAILED and rep.complete is False


def test_first_page_failure_is_failed_not_empty_complete():
    jobs = _crawl([paradox._ParadoxError("no __PRELOAD_STATE__")])
    rep = report_of(jobs)
    assert list(jobs) == [] and rep.stop_reason == STOP_REQUEST_FAILED and rep.complete is False


def test_genuinely_empty_board_is_complete():
    rep = report_of(_crawl([_js([], 0)]))
    assert rep.stop_reason == STOP_EMPTY_PAGE and rep.complete is True and rep.reported_total == 0


def test_page_cap_is_incomplete():
    pages = [_js([_row(i)], 1000) for i in range(1, 10)]
    rep = report_of(_crawl(pages, PARADOX_MAX_PAGES=3))
    assert rep.stop_reason == STOP_PAGE_CAP and rep.complete is False and rep.pages == 3


def test_time_budget_is_page_cap_incomplete():
    pages = [_js([_row(i)], 1000) for i in range(1, 10)]
    rep = report_of(_crawl(pages, PARADOX_TIME_BUDGET_S=-1))
    assert rep.stop_reason == STOP_PAGE_CAP and rep.complete is False


def test_unusable_rows_excluded_from_reported_total():
    rows = [_row(1), _row(2, title=" "), _row(3, url=False), _row(4)]
    rep = report_of(_crawl([_js(rows, 4)]))
    assert rep.unusable == 2 and rep.fetched_distinct == 2 and rep.reported_total == 2
    assert rep.complete is True


def test_duplicate_rows_across_pages_counted_once():
    jobs = _crawl([_js([_row(1), _row(2)], 3), _js([_row(2), _row(3)], 3)])
    assert [j["job_id"] for j in jobs] == ["R1", "R2", "R3"]
    assert report_of(jobs).complete is True


def test_no_host_is_no_config():
    rep = report_of(paradox.fetch_jobs({"host": ""}, "Co"))
    assert rep.stop_reason == STOP_NO_CONFIG and rep.complete is False


# ── page fetch / state validation ────────────────────────────────────────────

def _resp(status=200, text=""):
    r = MagicMock()
    r.status_code, r.text = status, text
    return r


def test_fetch_state_rejects_non_200_and_challenge_pages():
    sess = MagicMock()
    sess.get.return_value = _resp(403, "denied")
    with pytest.raises(paradox._ParadoxError):
        paradox._fetch_state(sess, f"https://{HOST}/jobs", None)
    sess.get.return_value = _resp(200, "<html>Access challenge</html>")
    with pytest.raises(paradox._ParadoxError):
        paradox._fetch_state(sess, f"https://{HOST}/jobs", None)
    sess.get.return_value = _resp(200, _html({"jobSearch": {"jobs": [], "totalJob": "x"}}))
    with pytest.raises(paradox._ParadoxError):
        paradox._fetch_state(sess, f"https://{HOST}/jobs", None)


def test_fetch_state_returns_job_search_and_sends_browser_headers():
    sess = MagicMock()
    sess.get.return_value = _resp(200, _html({"jobSearch": _js([_row(1)], 1)}))
    js = paradox._fetch_state(sess, f"https://{HOST}/jobs/page/2?page_size=100", f"https://{HOST}/jobs")
    assert js["totalJob"] == 1
    headers = sess.get.call_args.kwargs["headers"]
    assert headers["sec-fetch-site"] == "same-origin" and headers["sec-fetch-mode"] == "navigate"
    assert headers["referer"] == f"https://{HOST}/jobs"


def test_listing_page_retries_with_fresh_session_then_succeeds():
    calls = []

    def fake_fetch(session, url, referer):
        calls.append(session)
        if len(calls) == 1:
            raise paradox._ParadoxError("HTTP 403")
        return _js([_row(1)], 1)

    with patch.object(paradox, "_fetch_state", side_effect=fake_fetch), \
         patch.object(paradox, "_new_session", side_effect=lambda h: object()), \
         patch.object(paradox.time, "sleep"):
        session, js = paradox._get_listing_page(None, HOST, f"https://{HOST}/jobs", None)
    assert js["totalJob"] == 1 and len(calls) == 2 and calls[0] is not calls[1]


def test_listing_page_gives_up_after_retries():
    with patch.object(paradox, "_fetch_state", side_effect=paradox._ParadoxError("HTTP 403")), \
         patch.object(paradox, "_new_session", return_value=object()), \
         patch.object(paradox.time, "sleep"):
        with pytest.raises(paradox._ParadoxError):
            paradox._get_listing_page(None, HOST, f"https://{HOST}/jobs", None)


def test_page_urls():
    assert paradox._page_url(HOST, 1) == f"https://{HOST}/jobs?page_size={config.PARADOX_PAGE_SIZE}"
    assert paradox._page_url(HOST, 3) == f"https://{HOST}/jobs/page/3?page_size={config.PARADOX_PAGE_SIZE}"


# ── stub / country ───────────────────────────────────────────────────────────

def test_stub_fields_and_country():
    stub = paradox._to_stub(_row(7), HOST, "Co")
    assert stub["job_id"] == "R7" and stub["ats"] == "paradox"
    assert stub["job_url"] == f"https://{HOST}/job-7/job/ABC7"
    assert stub["_country_code"] == "US" and paradox.get_country_code(stub) == "US"
    assert stub["location"] == "Memphis, Tennessee, United States"


def test_country_any_us_location_wins_else_first():
    mixed = {**_row(1), "locations": [{"countryAbbr": "IN"}, {"countryAbbr": "us"}]}
    assert paradox._to_stub(mixed, HOST, "Co")["_country_code"] == "US"
    foreign = {**_row(2), "locations": [{"countryAbbr": "IN"}, {"countryAbbr": "GB"}]}
    assert paradox._to_stub(foreign, HOST, "Co")["_country_code"] == "IN"
    none = {**_row(3), "locations": []}
    assert paradox._to_stub(none, HOST, "Co")["_country_code"] == ""


def test_stub_rejects_unusable_rows():
    assert paradox._to_stub({"title": "x"}, HOST, "Co") is None
    assert paradox._to_stub("junk", HOST, "Co") is None


def test_slug_parsing():
    assert paradox._parse_host('{"host": "Careers.FedEx.com"}') == "careers.fedex.com"
    assert paradox._parse_host({"host": "https://careers.fedex.com/jobs"}) == "careers.fedex.com"
    assert paradox._parse_host("careers.fedex.com") == "careers.fedex.com"
    assert paradox._parse_host(None) == ""


# ── detail ───────────────────────────────────────────────────────────────────

_LD = {"@type": "JobPosting", "description": "<p>Do <b>things</b></p><p>More</p>",
       "datePosted": "2026-10-16T01:42:43.494567+00:00"}


def _detail_html(ld=_LD):
    return f'<html><script type="application/ld+json">{json.dumps(ld)}</script></html>'


def test_detail_fills_description_and_date():
    job = paradox._to_stub(_row(1), HOST, "Co")
    sess = MagicMock()
    sess.get.return_value = _resp(200, _detail_html())
    with patch.object(paradox, "_detail_session", return_value=sess):
        out = paradox.fetch_job_detail(job)
    assert "things" in out["description"] and "<" not in out["description"]
    assert out["posted_at"].year == 2026 and out is not job
    headers = sess.get.call_args.kwargs["headers"]
    assert headers["referer"] == f"https://{HOST}/jobs"


def test_detail_failures_raise_instead_of_storing_empty():
    job = paradox._to_stub(_row(1), HOST, "Co")
    sess = MagicMock()
    with patch.object(paradox, "_detail_session", return_value=sess):
        sess.get.return_value = _resp(404, "gone")
        with pytest.raises(paradox._ParadoxError):
            paradox.fetch_job_detail(job)
        sess.get.return_value = _resp(200, "<html>no ld</html>")
        with pytest.raises(paradox._ParadoxError):
            paradox.fetch_job_detail(job)


# ── fingerprints ─────────────────────────────────────────────────────────────

def test_has_paradox_state_is_strict():
    assert paradox.has_paradox_state(_html({"jobSearch": {"jobs": []}, "company": {}}))
    assert not paradox.has_paradox_state(_html({"company": {}}))                       # state without jobSearch
    assert not paradox.has_paradox_state("<html>paradox.ai chat widget olivia</html>")   # chat widget only
    assert not paradox.has_paradox_state("<script>window.__PRELOAD_STATE__ = {broken</script>")


def test_career_page_fingerprint_uses_page_host():
    from jobs.career_page import _scan_html
    html = _html({"jobSearch": _js([], 0)})
    assert _scan_html(html, "FedEx") is None                                  # no URL → host unknown
    r = _scan_html(html, "FedEx", page_url="https://careers.fedex.com/jobs")
    assert r == {"platform": "paradox", "slug": json.dumps({"host": "careers.fedex.com"})}


def test_career_page_fingerprint_never_relabels_other_platforms():
    from jobs.career_page import _scan_html
    html = ('<a href="https://boards.greenhouse.io/acme">jobs</a>'
            + _html({"jobSearch": _js([], 0)}))
    r = _scan_html(html, "Acme", page_url="https://careers.acme.com/jobs")
    assert r["platform"] == "greenhouse"


def test_career_detector_fingerprint():
    from jobs.ats.career_detector import _detect_paradox, scan
    html = _html({"jobSearch": _js([], 0)})
    assert scan(html) is None
    assert _detect_paradox(html, "https://careers.fedex.com/") == {
        "platform": "paradox", "slug": json.dumps({"host": "careers.fedex.com"})}
    assert _detect_paradox("<html>paradox.ai</html>", "https://careers.cinemark.com/") is None


def test_registered():
    from jobs.ats.registry import ATS_REGISTRY, SLUG_REQUIRED_KEYS
    cfg = ATS_REGISTRY["paradox"]
    assert cfg["module"] is paradox and cfg["has_detail"] and cfg["slug_type"] == "json"
    assert SLUG_REQUIRED_KEYS["paradox"] == ("host",)
