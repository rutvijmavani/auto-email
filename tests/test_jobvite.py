"""Jobvite: unfiltered paging with recorded stop reasons + XML-feed completeness check."""
from unittest.mock import patch

import requests

from jobs.ats import jobvite

ROW = '<table><tr><td><a href="/acme/job/o{id}">Job {id}</a></td><td>X</td></tr></table>'


class _Resp:
    def __init__(self, text="", status=200, url="", headers=None):
        self.text = text
        self.status_code = status
        self.url = url
        self.headers = headers or {}


def _page(*ids):
    return "".join(ROW.format(id=i) for i in ids)


def _board(pages, landing=""):
    """fetch_html fake: landing page (no params) + /search?p=N from `pages` (list of html)."""
    def fake(url, params=None, platform=None, **kw):
        if params is None:
            return _Resp(landing)
        p = params["p"]
        if p >= len(pages):
            return _Resp("")
        html = pages[p]
        return None if html is None else _Resp(html)
    return fake


def test_custom_landing_without_categories_is_fully_paged():
    """Marelli/Knight shape: landing has no 'Show More' links; /search must supply the jobs."""
    fake = _board([_page("A1", "A2"), _page("A3")], landing=_page("A1"))
    with patch.object(jobvite, "fetch_html", fake):
        jobs, rep = jobvite.fetch_board("acme", "Acme")
    assert {j["job_id"] for j in jobs} == {"oA1", "oA2", "oA3"}
    assert rep["stop_reason"] == jobvite.STOP_EMPTY_PAGE
    assert rep["pages"] == 2


def test_no_category_param_is_ever_sent():
    seen = []

    def fake(url, params=None, platform=None, **kw):
        if params is not None:
            seen.append(dict(params))
        return _Resp(_page("A1") if (params or {}).get("p") == 0 else "")

    with patch.object(jobvite, "fetch_html", fake):
        jobvite.fetch_board("acme", "Acme")
    assert seen and all(set(p) == {"p"} for p in seen)


def test_landing_only_job_is_kept():
    fake = _board([_page("A1")], landing=_page("A1", "ZZ"))
    with patch.object(jobvite, "fetch_html", fake):
        jobs, rep = jobvite.fetch_board("acme", "Acme")
    assert len(jobs) == 2 and rep["landing_only"] == 1


def test_request_failure_is_recorded():
    fake = _board([_page("A1"), None])
    with patch.object(jobvite, "fetch_html", fake):
        jobs, rep = jobvite.fetch_board("acme", "Acme")
    assert rep["stop_reason"] == jobvite.STOP_REQUEST_FAILED
    assert len(jobs) == 1


def test_repeat_page_stops():
    fake = _board([_page("A1")] * 5)
    with patch.object(jobvite, "fetch_html", fake):
        _, rep = jobvite.fetch_board("acme", "Acme")
    assert rep["stop_reason"] == jobvite.STOP_REPEAT_PAGE


def test_page_cap_is_recorded():
    pages = [_page(f"P{i}") for i in range(10)]
    with patch.object(jobvite, "fetch_html", _board(pages)), \
         patch.object(jobvite, "JOBVITE_MAX_PAGES", 3):
        jobs, rep = jobvite.fetch_board("acme", "Acme")
    assert rep["stop_reason"] == jobvite.STOP_PAGE_CAP and len(jobs) == 3


# ── feed verification ──

LANDING_EID = "<script>preloadedData = {companyEId: 'EID1'}</script>"
FEED = "<jobs><job><id>oA1</id></job><job><id>oA2-Cxyz</id></job><job><id>oFOR</id></job></jobs>"


def _verify(jobs, redirects, feed=FEED, feed_status=200):
    def fake_html(url, params=None, platform=None, **kw):
        return _Resp(LANDING_EID)

    def fake_get(url, params=None, headers=None, timeout=None):
        if "Xml.aspx" in url:
            return _Resp(feed, feed_status)
        return _Resp("", 200, url=redirects[params["j"]])

    with patch.object(jobvite, "fetch_html", fake_html), \
         patch.object(jobvite.requests, "get", fake_get):
        return jobvite.verify_fetch("acme", jobs)


def _jobs(*ids):
    return [{"job_id": i} for i in ids]


def test_foreign_careersite_job_is_ignored_and_suffix_normalized():
    v = _verify(_jobs("oA1", "oA2"),
                {"oFOR": "https://jobs.jobvite.com/careers/sibling/job/oFOR"})
    assert v["status"] == "verified" and v["foreign"] == 1


def test_feed_job_on_our_careersite_is_missing():
    v = _verify(_jobs("oA1"),
                {"oA2": "https://jobs.jobvite.com/acme/job/oA2",
                 "oFOR": "https://jobs.jobvite.com/sibling/job/oFOR"})
    assert v["status"] == "incomplete" and v["missing"] == ["oA2"]


def test_unresolvable_feed_job_is_unverified():
    v = _verify(_jobs("oA1", "oA2"), {"oFOR": "https://app.jobvite.com/error"})
    assert v["status"] == "unverified"


def test_feed_unavailable_is_unverified():
    assert _verify(_jobs("oA1"), {}, feed="", feed_status=500)["status"] == "unverified"


def test_empty_feed_with_jobs_on_board_is_unverified():
    v = _verify(_jobs("oA1"), {}, feed="<jobs></jobs>")
    assert v["status"] == "unverified" and "no jobs" in v["reason"]


def test_trace_budget_exhausted_counts_rest_as_unresolved():
    with patch.object(jobvite, "JOBVITE_FEED_TRACE_BUDGET_S", -1):
        v = _verify(_jobs("oA1"), {})
    assert v["status"] == "unverified" and v["unresolved"] == 2


def test_abnormal_stop_is_incomplete_without_any_request():
    assert jobvite.verify_fetch("acme", [], jobvite.STOP_PAGE_CAP)["status"] == "incomplete"


# ── 429 pacing ──

def test_retry_after_header_is_honoured():
    seq = [_Resp(status=429, headers={"Retry-After": "7"}), _Resp("ok")]
    with patch.object(jobvite.requests, "get", lambda *a, **k: seq.pop(0)), \
         patch.object(jobvite.time, "sleep") as sl:
        r = jobvite._get_with_retry("u")
    assert r.text == "ok"
    sl.assert_called_once_with(7)


def test_missing_retry_after_uses_config_default_and_cap_applies():
    seq = [_Resp(status=429), _Resp(status=429, headers={"Retry-After": "9999"}), _Resp("ok")]
    with patch.object(jobvite.requests, "get", lambda *a, **k: seq.pop(0)), \
         patch.object(jobvite.time, "sleep") as sl:
        jobvite._get_with_retry("u")
    waits = [c.args[0] for c in sl.call_args_list]
    assert waits == [jobvite.JOBVITE_FEED_DEFAULT_WAIT_S, jobvite.JOBVITE_FEED_MAX_WAIT_S]


def test_exhausted_429_returns_none():
    with patch.object(jobvite.requests, "get", lambda *a, **k: _Resp(status=429)), \
         patch.object(jobvite.time, "sleep"):
        assert jobvite._get_with_retry("u") is None


def test_request_exception_returns_none():
    def boom(*a, **k):
        raise requests.ConnectionError("x")
    with patch.object(jobvite.requests, "get", boom):
        assert jobvite._get_with_retry("u") is None
