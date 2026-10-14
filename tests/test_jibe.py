"""Tests for jobs/ats/jibe.py paging — mocked fetch_json (no network)."""
from unittest.mock import patch

from jobs.ats import jibe


def _item(i):
    return {"data": {"title": f"Engineer {i}", "req_id": str(i), "apply_url": f"https://x.test/j/{i}/login"}}


def _pages(total, per_page=100):
    """Return a fetch_json fake serving `total` jobs by the real `page=N` param (offset ignored)."""
    def fake(url, headers=None):
        page = int(url.split("page=")[1].split("&")[0]) if "page=" in url else 1
        start = (page - 1) * per_page
        ids = range(start, min(start + per_page, total))
        return {"jobs": [_item(i) for i in ids], "totalCount": total}
    return fake


def _run(fake):
    with patch.object(jibe, "fetch_json", side_effect=fake), patch.object(jibe.time, "sleep"):
        return jibe.fetch_jobs("careers.x.test", "Acme")


def test_pages_until_total_reached():
    assert len(_run(_pages(268))) == 268


def test_exact_multiple_of_page_size_makes_no_extra_call():
    calls = []
    fake = _pages(200)
    def counting(url, headers=None):
        calls.append(url)
        return fake(url)
    assert len(_run(counting)) == 200 and len(calls) == 2


def test_repeated_page_stops_and_does_not_duplicate():
    # old behaviour of an API ignoring the page param: every page is page 1
    same = {"jobs": [_item(i) for i in range(100)], "totalCount": 268}
    jobs = _run(lambda url, headers=None: same)
    assert len(jobs) == 100


def test_failed_page_returns_partial():
    fake = _pages(268)
    def flaky(url, headers=None):
        return None if "page=2" in url else fake(url)
    assert len(_run(flaky)) == 100


def test_small_board_single_call():
    assert len(_run(_pages(61))) == 61
