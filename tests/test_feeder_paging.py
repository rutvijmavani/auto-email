"""Paging / completeness tests for the Eightfold, Oracle HCM and Taleo feeders — mocked, no network."""
import logging
from unittest.mock import MagicMock, patch

from jobs.ats import eightfold, oracle_hcm, taleo


# ─────────────────────────────────────────
# Eightfold
# ─────────────────────────────────────────

def _pos(i):
    return {"id": i, "name": f"Engineer {i}", "atsJobId": str(i), "positionUrl": f"/careers/job/{i}"}


def _ef_run(fetch_page, **cfg):
    with patch.object(eightfold, "_make_session"), \
         patch.object(eightfold, "_fetch_csrf_token", return_value="t"), \
         patch.object(eightfold, "_resolve_api_path_and_domain", return_value=("/api/pcsx/search", "x.com")), \
         patch.object(eightfold, "_fetch_page", side_effect=fetch_page), \
         patch.object(eightfold, "EIGHTFOLD_MAX_PAGES", cfg.get("cap", 5000)):
        return eightfold.fetch_jobs({"slug": "x"}, "Acme", stop_check=cfg.get("stop_check"))


def _ef_pages(total, per_page=10):
    def fake(session, slug, domain, base, start, tok, path):
        return [_pos(i + 1) for i in range(start, min(start + per_page, total))], total, 200
    return fake


def test_eightfold_pages_past_old_20000_cap(caplog):
    with caplog.at_level(logging.WARNING):
        jobs = _ef_run(_ef_pages(21507))
    assert len(jobs) == 21507
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_eightfold_page_cap_warns(caplog):
    with caplog.at_level(logging.WARNING):
        jobs = _ef_run(_ef_pages(500), cap=5)
    assert len(jobs) == 50
    assert any("page_cap" in r.getMessage() for r in caplog.records)


def test_eightfold_failed_page_warns_and_keeps_partial(caplog):
    fake = _ef_pages(100)
    def flaky(session, slug, domain, base, start, tok, path):
        return (None, 0, 500) if start >= 30 else fake(session, slug, domain, base, start, tok, path)
    with caplog.at_level(logging.WARNING):
        jobs = _ef_run(flaky)
    assert len(jobs) == 30
    assert any("request_failed" in r.getMessage() for r in caplog.records)


def test_eightfold_unreported_total_keeps_paging_to_empty_page():
    # total=0 means "count not reported" — must not end the loop after the first page
    real = _ef_pages(35)
    jobs = _ef_run(lambda s, sl, d, b, start, t, p: (real(s, sl, d, b, start, t, p)[0], 0, 200))
    assert len(jobs) == 35


def test_eightfold_repeated_page_stops_without_duplicates():
    same = [_pos(i + 1) for i in range(10)]
    jobs = _ef_run(lambda *a: (same, 100, 200))
    assert len(jobs) == 10


# ─────────────────────────────────────────
# Oracle HCM
# ─────────────────────────────────────────

def _req(i):
    return {"Id": str(i), "Title": f"Engineer {i}", "PrimaryLocationCountry": "US"}


def _oracle_pages(total, per_page=200, report_total=True):
    def fake(url, platform=None):
        offset = int(url.split("offset%3D")[1].split("%2C")[0])
        rows = [_req(i) for i in range(offset, min(offset + per_page, total))]
        item = {"requisitionList": rows}
        if report_total:
            item["TotalJobsCount"] = total
        return {"items": [item], "hasMore": False}
    return fake


def _oracle_run(fake):
    with patch.object(oracle_hcm, "fetch_json", side_effect=fake):
        return oracle_hcm.fetch_jobs({"slug": "jpmc", "site": "CX_1001"}, "JPMC")


def test_oracle_reads_total_and_pages_to_it():
    assert len(_oracle_run(_oracle_pages(7306))) == 7306


def test_oracle_exact_multiple_makes_no_extra_call():
    calls = []
    fake = _oracle_pages(400)
    def counting(url, platform=None):
        calls.append(url)
        return fake(url)
    assert len(_oracle_run(counting)) == 400 and len(calls) == 2


def test_oracle_short_page_before_total_continues():
    # server returns 150 rows/page though limit=200; the old code stopped after page 1
    jobs = _oracle_run(_oracle_pages(600, per_page=150))
    assert len(jobs) == 600


def test_oracle_failed_page_warns(caplog):
    fake = _oracle_pages(600)
    def flaky(url, platform=None):
        return None if "offset%3D200" in url else fake(url)
    with caplog.at_level(logging.WARNING):
        jobs = _oracle_run(flaky)
    assert len(jobs) == 200
    assert any("request_failed" in r.getMessage() for r in caplog.records)


def test_oracle_without_total_falls_back_to_short_page():
    assert len(_oracle_run(_oracle_pages(450, report_total=False))) == 450


def test_oracle_repeated_page_stops_without_duplicates():
    same = {"items": [{"requisitionList": [_req(i) for i in range(200)], "TotalJobsCount": 900}]}
    jobs = _oracle_run(lambda url, platform=None: same)
    assert len(jobs) == 200


def test_oracle_stop_check_ends_paging_early():
    from jobs.ats.fetch_report import STOP_EARLY_EXIT, report_of
    from workers.paginator import EarlyExitGate
    seen = {str(i) for i in range(200, 1000)}          # only page 1 holds unseen jobs
    gate = EarlyExitGate(seen.__contains__)
    with patch.object(oracle_hcm, "fetch_json", side_effect=_oracle_pages(1000)):
        jobs = oracle_hcm.fetch_jobs({"slug": "jpmc", "site": "CX_1001"}, "JPMC", stop_check=gate)
    rep = report_of(jobs)
    assert len(jobs) == 600                              # p1 new, p2 clean, p3 clean -> stop
    assert rep.stop_reason == STOP_EARLY_EXIT and rep.complete is None


def test_oracle_last_page_stays_all_pages_even_if_gate_would_stop():
    from jobs.ats.fetch_report import STOP_ALL_PAGES, report_of
    from workers.paginator import EarlyExitGate
    gate = EarlyExitGate(lambda _i: True)
    with patch.object(oracle_hcm, "fetch_json", side_effect=_oracle_pages(300)):
        jobs = oracle_hcm.fetch_jobs({"slug": "jpmc", "site": "CX_1001"}, "JPMC", stop_check=gate)
    rep = report_of(jobs)
    assert rep.stop_reason == STOP_ALL_PAGES and rep.complete is True and len(jobs) == 300


def test_oracle_page_cap_warns(caplog):
    with patch.object(oracle_hcm, "ORACLE_HCM_MAX_PAGES", 2), caplog.at_level(logging.WARNING):
        jobs = _oracle_run(_oracle_pages(1000))
    assert len(jobs) == 400
    assert any("page_cap" in r.getMessage() for r in caplog.records)


# ─────────────────────────────────────────
# Taleo
# ─────────────────────────────────────────

def _trow(i):
    return {"jobId": str(i), "contestNo": f"R{i}", "column": [f"Engineer {i}", "", "", "", "Dallas", ""]}


def _taleo_resp(rows, total, page_size=25):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = {"requisitionList": rows,
                           "pagingData": {"totalCount": total, "pageSize": page_size}}
    return r


def _taleo_run(post):
    session = MagicMock()
    session.post.side_effect = post
    with patch.object(taleo, "_make_session", return_value=session):
        return taleo.fetch_jobs({"company": "utsw", "portal_id": "1", "section": "ex"}, "UTSW")


def test_taleo_all_pages_no_warning(caplog):
    def post(url, json=None, timeout=None):
        p = json["pageNo"]
        return _taleo_resp([_trow(i) for i in range((p - 1) * 25, min(p * 25, 60))], 60)
    with caplog.at_level(logging.WARNING):
        jobs = _taleo_run(post)
    assert len(jobs) == 60
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_taleo_server_short_of_total_warns(caplog):
    # utsw shape: totalCount 649 but the list API runs dry after 506
    def post(url, json=None, timeout=None):
        p = json["pageNo"]
        return _taleo_resp([_trow(i) for i in range((p - 1) * 25, min(p * 25, 60))], 100)
    with caplog.at_level(logging.WARNING):
        jobs = _taleo_run(post)
    assert len(jobs) == 60
    assert any("60 distinct of 100" in r.getMessage() and "empty_page" in r.getMessage()
               for r in caplog.records)


def test_taleo_repeated_page_stops_without_duplicates():
    rows = [_trow(i) for i in range(25)]
    jobs = _taleo_run(lambda url, json=None, timeout=None: _taleo_resp(rows, 100))
    assert len(jobs) == 25


# ─────────────────────────────────────────
# Adaptive early exit — Eightfold, Workday
# ─────────────────────────────────────────

def test_eightfold_stop_check_ends_paging_early():
    from jobs.ats.fetch_report import STOP_EARLY_EXIT, report_of
    from workers.paginator import EarlyExitGate
    gate = EarlyExitGate({str(i) for i in range(11, 101)}.__contains__)   # only page 1 unseen
    jobs = _ef_run(_ef_pages(100), stop_check=gate)
    rep = report_of(jobs)
    assert len(jobs) == 30 and rep.stop_reason == STOP_EARLY_EXIT and rep.complete is None


def test_eightfold_last_page_stays_all_pages():
    from jobs.ats.fetch_report import STOP_ALL_PAGES, report_of
    from workers.paginator import EarlyExitGate
    jobs = _ef_run(_ef_pages(20), stop_check=EarlyExitGate(lambda _i: True))
    assert len(jobs) == 20 and report_of(jobs).stop_reason == STOP_ALL_PAGES


def _wd_post(i):
    return {"title": f"Eng {i}", "externalPath": f"/job/X/Eng_R-{i}", "bulletFields": [f"R-{i}"],
            "locationsText": "X"}


def _wd_post_fn(total, page=20):
    def fake(url, body=None, headers=None, platform=None, **kw):
        off = body["offset"]
        return {"total": total, "facets": [],
                "jobPostings": [_wd_post(i) for i in range(off, min(off + page, total))]}
    return fake


def test_workday_stop_check_ends_paging_early():
    from jobs.ats import workday
    from jobs.ats.fetch_report import STOP_EARLY_EXIT, report_of
    from workers.paginator import EarlyExitGate
    gate = EarlyExitGate({f"R-{i}" for i in range(20, 200)}.__contains__)
    with patch.object(workday, "WORKDAY_PAGE_SIZE", 20), \
         patch.object(workday, "fetch_json_post", side_effect=_wd_post_fn(200)):
        jobs = workday.fetch_jobs({"slug": "x", "wd": "wd1", "path": "careers"}, "Acme", stop_check=gate)
    rep = report_of(jobs)
    assert len(jobs) == 60 and rep.stop_reason == STOP_EARLY_EXIT and rep.complete is None


def test_workday_last_page_stays_all_pages():
    from jobs.ats import workday
    from jobs.ats.fetch_report import STOP_ALL_PAGES, report_of
    from workers.paginator import EarlyExitGate
    with patch.object(workday, "WORKDAY_PAGE_SIZE", 20), \
         patch.object(workday, "fetch_json_post", side_effect=_wd_post_fn(40)):
        jobs = workday.fetch_jobs({"slug": "x", "wd": "wd1", "path": "careers"}, "Acme",
                                  stop_check=EarlyExitGate(lambda _i: True))
    assert len(jobs) == 40 and report_of(jobs).stop_reason == STOP_ALL_PAGES
