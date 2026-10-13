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
