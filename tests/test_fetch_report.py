"""jobs/ats/fetch_report.py rules + the report each feeder attaches — mocked, no network."""
from unittest.mock import MagicMock, patch

from jobs.ats import eightfold, jibe, jobvite, oracle_hcm, successfactors as sf, taleo, workday
from jobs.ats.fetch_report import (
    FetchReport, STOP_ALL_PAGES, STOP_EMPTY_PAGE, STOP_NO_CONFIG, STOP_PAGE_CAP, STOP_REPEAT_PAGE,
    STOP_REQUEST_FAILED, STOP_SINGLE_RESPONSE, TOTAL_FEED_COUNT, attach, count_distinct, failed, report_of,
)
from tests import test_feeder_paging as fp
from tests import test_jibe as tj
from tests import test_workday_split as tw
from tests.test_successfactors import SLUG as SF_SLUG, _feed, _resp


# ── finalize rules ───────────────────────────────────────────────────────────

def _rep(**kw):
    return FetchReport(**kw).finalize()


def test_clean_stop_at_total_is_complete():
    assert _rep(stop_reason=STOP_ALL_PAGES, fetched_distinct=100, reported_total=100).complete is True


def test_within_tolerance_is_complete():
    with patch("jobs.ats.fetch_report.FETCH_TOLERANCE_PCT", 5.0):
        assert _rep(stop_reason=STOP_ALL_PAGES, fetched_distinct=96, reported_total=100).complete is True
        assert _rep(stop_reason=STOP_ALL_PAGES, fetched_distinct=94, reported_total=100).complete is False


def test_feed_count_total_has_no_tolerance():
    r = _rep(stop_reason=STOP_SINGLE_RESPONSE, fetched_distinct=99, reported_total=100,
             total_source=TOTAL_FEED_COUNT)
    assert r.complete is False and r.shortfall == 1


def test_failed_stops_are_incomplete_even_when_count_matches():
    for stop in (STOP_REPEAT_PAGE, STOP_REQUEST_FAILED, STOP_PAGE_CAP, STOP_NO_CONFIG):
        assert _rep(stop_reason=stop, fetched_distinct=100, reported_total=100).complete is False


def test_no_total_is_unknown_on_clean_stop():
    assert _rep(stop_reason=STOP_EMPTY_PAGE, fetched_distinct=10).complete is None


def test_unknown_stop_is_unknown():
    assert _rep(fetched_distinct=10, reported_total=10).complete is None


def test_advisory_total_server_short_is_complete_but_flagged():
    with patch("jobs.ats.fetch_report.FETCH_SERVER_SHORT_MIN_PCT", 50.0):
        r = _rep(stop_reason=STOP_EMPTY_PAGE, fetched_distinct=506, reported_total=649, advisory_total=True)
        assert r.complete is True and r.server_short is True
        r = _rep(stop_reason=STOP_EMPTY_PAGE, fetched_distinct=100, reported_total=649, advisory_total=True)
        assert r.complete is False and r.server_short is False


def test_count_distinct_ignores_repeats_and_falls_back_to_url():
    jobs = [{"job_id": "a"}, {"job_id": "a"}, {"job_url": "u"}, {}]
    assert count_distinct(jobs) == 2


def test_report_of_plain_list_is_unknown():
    r = report_of([{"job_id": "a"}, {"job_id": "b"}])
    assert r.complete is None and r.fetched_distinct == 2 and r.stop_reason == "unknown"


def test_attach_keeps_list_behaviour():
    out = attach([{"job_id": "a"}], FetchReport(stop_reason=STOP_EMPTY_PAGE))
    assert out == [{"job_id": "a"}] and report_of(out).stop_reason == STOP_EMPTY_PAGE


def test_failed_is_empty_and_incomplete():
    out = failed(STOP_NO_CONFIG, "x")
    assert out == [] and report_of(out).complete is False


# ── feeders ──────────────────────────────────────────────────────────────────

def test_successfactors_complete_feed():
    with patch.object(sf, "fetch_html", return_value=_resp(_feed(5, extra="<>1</>"))):
        r = report_of(sf.fetch_jobs(SF_SLUG, "Capgemini"))
    assert r.complete is True and r.reported_total == 5 and r.stop_reason == STOP_SINGLE_RESPONSE


def test_successfactors_parse_loss_is_incomplete():
    fake_soup = MagicMock()
    fake_soup.find_all.return_value = []
    with patch.object(sf, "fetch_html", return_value=_resp(_feed(3))), \
         patch.object(sf, "BeautifulSoup", return_value=fake_soup):
        r = report_of(sf.fetch_jobs(SF_SLUG, "Capgemini"))
    assert r.complete is False and r.reported_total == 3 and r.fetched_distinct == 0


def test_successfactors_no_config_and_no_feed():
    assert report_of(sf.fetch_jobs({}, "X")).stop_reason == STOP_NO_CONFIG
    with patch.object(sf, "fetch_html", return_value=None):
        assert report_of(sf.fetch_jobs(SF_SLUG, "X")).complete is False


def test_workday_small_board_complete():
    r = report_of(tw._run(tw._board({"eng": list(range(30))})))
    assert r.complete is True and r.reported_total == 30 and r.total_source == "ats"


def test_workday_split_uses_facet_sum():
    groups = {"eng": list(range(0, 1500)), "sales": list(range(1500, 2300)), "ops": list(range(2300, 2700))}
    r = report_of(tw._run(tw._board(groups)))
    assert r.complete is True and r.total_source == "facet_sum" and r.reported_total == 2700


def test_workday_unsplittable_slice_is_page_cap():
    groups = {"eng": list(range(0, 2500)), "sales": list(range(2500, 2600))}
    r = report_of(tw._run(tw._board(groups)))
    assert r.complete is False and r.stop_reason == STOP_PAGE_CAP and r.shortfall == 500


def test_workday_failed_page_is_incomplete():
    fake = tw._board({"eng": list(range(100))})
    r = report_of(tw._run(lambda url, body=None, **kw: None if body["offset"] >= 40 else fake(url, body=body, **kw)))
    assert r.complete is False and r.stop_reason == STOP_REQUEST_FAILED


def test_oracle_complete_and_failed():
    assert report_of(fp._oracle_run(fp._oracle_pages(450))).complete is True
    fake = fp._oracle_pages(600)
    r = report_of(fp._oracle_run(lambda url, platform=None: None if "offset%3D200" in url else fake(url)))
    assert r.complete is False and r.stop_reason == STOP_REQUEST_FAILED


def test_oracle_without_total_is_unknown():
    assert report_of(fp._oracle_run(fp._oracle_pages(450, report_total=False))).complete is None


def test_oracle_no_site_is_no_config():
    with patch.object(oracle_hcm, "_discover_site_id", return_value=""):
        assert report_of(oracle_hcm.fetch_jobs({"slug": "x"}, "X")).stop_reason == STOP_NO_CONFIG


def test_eightfold_complete_and_page_cap():
    assert report_of(fp._ef_run(fp._ef_pages(95))).complete is True
    r = report_of(fp._ef_run(fp._ef_pages(500), cap=5))
    assert r.complete is False and r.stop_reason == STOP_PAGE_CAP


def test_eightfold_repeat_page_is_incomplete():
    same = [fp._pos(i + 1) for i in range(10)]
    r = report_of(fp._ef_run(lambda *a: (same, 100, 200)))
    assert r.complete is False and r.stop_reason == STOP_REPEAT_PAGE and r.fetched_distinct == 10


def test_taleo_server_short_is_complete_but_flagged():
    def post(url, json=None, timeout=None):
        p = json["pageNo"]
        return fp._taleo_resp([fp._trow(i) for i in range((p - 1) * 25, min(p * 25, 60))], 100)
    r = report_of(fp._taleo_run(post))
    assert r.complete is True and r.server_short is True and r.fetched_distinct == 60


def test_taleo_full_and_failed_first_page():
    def post(url, json=None, timeout=None):
        p = json["pageNo"]
        return fp._taleo_resp([fp.taleo and fp._trow(i) for i in range((p - 1) * 25, min(p * 25, 60))], 60)
    r = report_of(fp._taleo_run(post))
    assert r.complete is True and r.server_short is False
    r = report_of(fp._taleo_run(lambda *a, **k: (_ for _ in ()).throw(taleo.requests.ConnectionError())))
    assert r.complete is False and r.stop_reason == STOP_REQUEST_FAILED


def test_jibe_complete_repeat_and_failed():
    assert report_of(tj._run(tj._pages(268))).complete is True
    same = {"jobs": [tj._item(i) for i in range(100)], "totalCount": 268}
    r = report_of(tj._run(lambda url, headers=None: same))
    assert r.complete is False and r.stop_reason == STOP_REPEAT_PAGE and r.fetched_distinct == 100
    fake = tj._pages(268)
    r = report_of(tj._run(lambda url, headers=None: None if "page=2" in url else fake(url)))
    assert r.complete is False and r.stop_reason == STOP_REQUEST_FAILED


def test_jibe_empty_slug_is_no_config():
    assert report_of(jibe.fetch_jobs("", "X")).stop_reason == STOP_NO_CONFIG


def test_jobvite_clean_end_is_unknown_and_failure_is_incomplete():
    from tests.test_jobvite import _board, _page
    with patch.object(jobvite, "fetch_html", _board([_page("A1", "A2"), _page("A3")])):
        r = report_of(jobvite.fetch_jobs("acme", "Acme"))
    assert r.complete is None and r.stop_reason == STOP_EMPTY_PAGE and r.fetched_distinct == 3
    with patch.object(jobvite, "fetch_html", _board([_page("A1"), None])):
        r = report_of(jobvite.fetch_jobs("acme", "Acme"))
    assert r.complete is False and r.stop_reason == STOP_REQUEST_FAILED
