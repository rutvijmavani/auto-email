"""Tests for scripts/audit_fetch_health.py — flag logic with mocked fetch (no network, no DB)."""
from unittest.mock import MagicMock, patch

from scripts import audit_fetch_health as audit


def _jobs(n, with_id=True):
    return [
        {"title": f"Software Engineer {i}", "job_url": f"https://x.test/j/{i}",
         "job_id": str(i) if with_id else ""}
        for i in range(n)
    ]


def _row(platform="workday", petitions=0, name=None):
    return {"company": "Acme", "company_name": name, "ats_platform": platform,
            "ats_slug": "slug", "petition_count": petitions}


def _run(raw_jobs, row=None, stored=None, slug_info=None, config=None, exc=None):
    module = MagicMock()
    if exc:
        module.fetch_jobs.side_effect = exc
    else:
        module.fetch_jobs.return_value = raw_jobs
    with patch.object(audit, "get_config", return_value=config or {}), \
         patch.object(audit, "get_ats_module", return_value=module), \
         patch.object(audit, "parse_slug", return_value=slug_info if slug_info is not None else {}):
        return audit._audit_one(row or _row(), stored or {}, {10, 20, 25, 50, 100})


def test_clean_company_has_no_flags():
    assert _run(_jobs(137), stored={"Acme": (130, 200, 5)})["flags"] == []


def test_fetch_error_is_flagged_not_raised():
    res = _run([], exc=RuntimeError("boom"))
    assert res["flags"] == ["FETCH_ERROR"] and "boom" in res["error"]


def test_zero_jobs_vs_zero_but_stored():
    assert "ZERO_JOBS" in _run([])["flags"]
    assert "ZERO_BUT_STORED" in _run([], stored={"Acme": (7, 9, 0)})["flags"]


def test_dropped_ratio_flag():
    mixed = _jobs(10, with_id=False) + _jobs(7)
    res = _run(mixed)
    assert "DROPPED_NO_ID" in res["flags"] and res["dropped"] == 10 and res["valid"] == 7


def test_round_count_and_page_size_flags():
    assert "FULL_PAGE_SUSPECT" in _run(_jobs(50))["flags"]
    res = _run(_jobs(17), slug_info={"pagination": {"type": "none", "page_size": 17}}, row=_row("custom"))
    assert "FULL_PAGE_SUSPECT" in res["flags"] and "CUSTOM_PAGINATION_NONE" in res["flags"]


def test_duplicate_urls_flag():
    jobs = _jobs(5) + _jobs(5)
    assert "DUPLICATE_URLS" in _run(jobs)["flags"]


def test_big_employer_few_jobs():
    assert "BIG_EMPLOYER_FEW_JOBS" in _run(_jobs(3), row=_row(petitions=500))["flags"]
    assert "BIG_EMPLOYER_FEW_JOBS" not in _run(_jobs(3), row=_row(petitions=2))["flags"]


def test_fewer_than_stored_uses_display_name():
    row = _row(platform="custom", name="Acme Inc")
    res = _run(_jobs(3), row=row, stored={"Acme Inc": (40, 60, 2)})
    assert "FEWER_THAN_STORED" in res["flags"] and res["stored_open"] == 40


def test_no_new_jobs_on_large_board():
    assert "NO_NEW_JOBS" in _run(_jobs(300), stored={"Acme": (0, 10, 0)})["flags"]


def test_slug_mutation_reported_not_persisted():
    info = {"a": 1}
    module = MagicMock()
    module.fetch_jobs.side_effect = lambda s, c: (s.update(a=2) or _jobs(3))
    with patch.object(audit, "get_config", return_value={}), \
         patch.object(audit, "get_ats_module", return_value=module), \
         patch.object(audit, "parse_slug", return_value=info):
        res = audit._audit_one(_row(), {}, set())
    assert "SLUG_MUTATED" in res["flags"]


def test_partial_result_from_incomplete_search():
    exc = audit.IncompleteSearchError(_jobs(4))
    assert "PARTIAL_FETCH" in _run([], exc=exc)["flags"]


def test_ceiling_retries_then_succeeds():
    module = MagicMock()
    module.fetch_jobs.side_effect = [audit.CeilingExceeded("x"), _jobs(3)]
    with patch.object(audit.time, "sleep"):
        jobs, partial = audit._fetch(module, {}, "Acme")
    assert len(jobs) == 3 and partial is False


def _with_report(n, **kw):
    from jobs.ats.fetch_report import FetchReport, attach
    return attach(_jobs(n), FetchReport(fetched_distinct=n, **kw))


def test_verdict_short_when_ats_total_exceeds_fetched():
    jobs = _with_report(10, stop_reason="all_pages", reported_total=1537, total_source="ats", pages=1)
    res = _run(jobs)
    assert res["verdict"] == "SHORT"
    assert res["reported_total"] == 1537 and res["shortfall"] == 1527 and res["fetched_distinct"] == 10


def test_verdict_ok_unverified_failed_and_error():
    assert _run(_with_report(10, stop_reason="all_pages", reported_total=10, total_source="ats"))["verdict"] == "OK"
    assert _run(_jobs(10))["verdict"] == "UNVERIFIED"
    assert _run(_with_report(10, stop_reason="page_cap"))["verdict"] == "FAILED_STOP"
    assert _run([], exc=RuntimeError("x"))["verdict"] == "ERROR"
    assert _run([], exc=audit.IncompleteSearchError(_jobs(4)))["verdict"] == "FAILED_STOP"


def test_filter_funnel_counts_and_rejected_titles():
    jobs = [
        {"title": "Software Engineer", "job_url": "u1", "job_id": "1", "location": "New York, NY"},
        {"title": "Software Engineer", "job_url": "u2", "job_id": "2", "location": "London, United Kingdom"},
        {"title": "Warehouse Associate", "job_url": "u3", "job_id": "3", "location": "New York, NY"},
        {"title": "Warehouse Associate", "job_url": "u4", "job_id": "4", "location": "Paris, France"},
    ]
    res = _run(jobs)
    assert res["n_title_pass"] == 2 and res["n_location_pass"] == 2 and res["n_both_pass"] == 1
    assert res["n_us_title_rejected"] == 1 and res["rejected_titles"] == {"warehouse associate": 1}
    assert res["filter_mode"] == "full"


def test_title_report_aggregates_across_companies(tmp_path):
    from collections import Counter
    results = [
        {"display_name": "A", "rejected_titles": Counter({"warehouse associate": 2, "chef": 1})},
        {"display_name": "B", "rejected_titles": Counter({"warehouse associate": 1})},
    ]
    path = str(tmp_path / "t.csv")
    assert audit._write_title_reports(results, path) == (2, 4)
    lines = open(path, encoding="utf-8").read().splitlines()
    assert lines[1].startswith("warehouse associate,3,2,A")


def test_manual_columns_and_careers_url():
    row = {**_row(), "employer_fein": "12-3456789", "domain": "acme.com"}
    module = MagicMock()
    module.fetch_jobs.return_value = _jobs(3)
    with patch.object(audit, "get_config", return_value={}), \
         patch.object(audit, "get_ats_module", return_value=module), \
         patch.object(audit, "parse_slug", return_value={}):
        res = audit._audit_one(row, {}, set(), {"12-3456789": "https://acme.com/careers"}, {"Acme": "healthy"})
    assert res["careers_url"] == "https://acme.com/careers" and res["health_state"] == "healthy"
    assert res["manual_count"] == "" and res["domain"] == "acme.com"
