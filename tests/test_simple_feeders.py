"""Completeness reports for Greenhouse / Lever / Ashby / SmartRecruiters — mocked fetch_json (no network)."""
from unittest.mock import patch

from jobs.ats import ashby, greenhouse, lever, smartrecruiters
from jobs.ats.fetch_report import (
    STOP_ALL_PAGES, STOP_EMPTY_PAGE, STOP_REPEAT_PAGE, STOP_REQUEST_FAILED, STOP_SINGLE_RESPONSE, report_of,
)


def _run(mod, fake, slug="acme"):
    with patch.object(mod, "fetch_json", side_effect=fake):
        return mod.fetch_jobs(slug, "Acme")


# ── Greenhouse ────────────────────────────────────────────────────────────────
def _gh(n, total=None):
    return {"jobs": [{"id": i, "title": f"T{i}", "absolute_url": f"https://x/{i}"} for i in range(n)],
            "meta": {"total": n if total is None else total}}


def test_greenhouse_whole_board_on_page_one_is_complete():
    jobs = _run(greenhouse, lambda *a, **k: _gh(895))
    rep = report_of(jobs)
    assert len(jobs) == 895 and rep.complete is True and rep.reported_total == 895
    assert rep.stop_reason == STOP_ALL_PAGES


def test_greenhouse_total_larger_than_served_with_repeat_page_is_incomplete():
    jobs = _run(greenhouse, lambda *a, **k: _gh(50, total=200))
    rep = report_of(jobs)
    assert len(jobs) == 50 and rep.stop_reason == STOP_REPEAT_PAGE and rep.complete is False


def test_greenhouse_failed_call_is_request_failed():
    rep = report_of(_run(greenhouse, lambda *a, **k: None))
    assert rep.stop_reason == STOP_REQUEST_FAILED and rep.complete is False


def test_greenhouse_empty_board_is_empty_page():
    rep = report_of(_run(greenhouse, lambda *a, **k: {"jobs": [], "meta": {"total": 0}}))
    assert rep.stop_reason == STOP_EMPTY_PAGE and rep.complete is None


# ── Lever ─────────────────────────────────────────────────────────────────────
def test_lever_single_response_has_no_total_so_unknown():
    data = [{"id": str(i), "text": f"T{i}", "hostedUrl": f"https://x/{i}"} for i in range(23)]
    jobs = _run(lever, lambda *a, **k: data)
    rep = report_of(jobs)
    assert len(jobs) == 23 and rep.stop_reason == STOP_SINGLE_RESPONSE and rep.complete is None


def test_lever_empty_list_is_a_valid_empty_board_not_a_failure():
    rep = report_of(_run(lever, lambda *a, **k: []))
    assert rep.stop_reason == STOP_SINGLE_RESPONSE and rep.complete is None


def test_lever_failed_call_is_request_failed():
    rep = report_of(_run(lever, lambda *a, **k: None))
    assert rep.stop_reason == STOP_REQUEST_FAILED and rep.complete is False


# ── Ashby ─────────────────────────────────────────────────────────────────────
def test_ashby_single_response():
    data = {"jobs": [{"id": str(i), "title": f"T{i}", "jobUrl": f"https://x/{i}"} for i in range(28)]}
    jobs = _run(ashby, lambda *a, **k: data)
    rep = report_of(jobs)
    assert len(jobs) == 28 and rep.stop_reason == STOP_SINGLE_RESPONSE and rep.complete is None


def test_ashby_failed_or_malformed_is_request_failed():
    assert report_of(_run(ashby, lambda *a, **k: None)).stop_reason == STOP_REQUEST_FAILED
    assert report_of(_run(ashby, lambda *a, **k: {"oops": 1})).complete is False


# ── SmartRecruiters ───────────────────────────────────────────────────────────
def _sr(total, per_page=100):
    def fake(url, params=None, platform=None):
        start = params["offset"]
        ids = range(start, min(start + per_page, total))
        return {"content": [{"id": str(i), "name": f"T{i}"} for i in ids], "totalFound": total}
    return fake


def test_smartrecruiters_pages_until_total():
    jobs = _run(smartrecruiters, _sr(673))
    rep = report_of(jobs)
    assert len(jobs) == 673 and rep.complete is True and rep.reported_total == 673
    assert rep.stop_reason == STOP_ALL_PAGES and rep.pages == 7


def test_smartrecruiters_failed_page_is_incomplete():
    fake = _sr(673)
    def flaky(url, params=None, platform=None):
        return None if params["offset"] == 200 else fake(url, params, platform)
    rep = report_of(_run(smartrecruiters, flaky))
    assert rep.stop_reason == STOP_REQUEST_FAILED and rep.complete is False and rep.fetched_distinct == 200


def test_smartrecruiters_ignored_offset_is_repeat_page():
    same = {"content": [{"id": str(i), "name": f"T{i}"} for i in range(100)], "totalFound": 500}
    rep = report_of(_run(smartrecruiters, lambda *a, **k: same))
    assert rep.stop_reason == STOP_REPEAT_PAGE and rep.complete is False
