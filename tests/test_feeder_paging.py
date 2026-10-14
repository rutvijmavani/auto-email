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
        return eightfold.fetch_jobs({"slug": "x"}, "Acme")


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
