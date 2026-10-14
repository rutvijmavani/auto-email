"""Tests for jobs/ats/workday.py cap handling — mocked fetch_json_post (no network)."""
from unittest.mock import patch

from jobs.ats import workday

SLUG = {"slug": "acme", "wd": "wd5", "path": "Ext"}
PAGE = 20


def _job(i):
    return {"title": f"Job {i}", "externalPath": f"/job/X/Job-{i}_R-{i}", "bulletFields": [f"R-{i}"],
            "locationsText": "Austin, TX"}


def _board(groups, cap=2000, facet_extra=None):
    """Fake Workday server: `groups` = {group_id: [job indexes]}; a query is capped at `cap` like the real API."""
    allidx = [i for idx in groups.values() for i in idx]

    def facets_for(selected):
        vals = [{"descriptor": g, "id": g, "count": len(groups[g])} for g in groups]
        out = [{"facetParameter": "jobFamilyGroup", "descriptor": "Job Category", "values": vals}]
        return out + (facet_extra or [])

    def fake(url, body=None, headers=None, platform=None):
        applied = (body or {}).get("appliedFacets") or {}
        idx = allidx
        if "jobFamilyGroup" in applied:
            idx = [i for g in applied["jobFamilyGroup"] for i in groups[g]]
        total = min(len(idx), cap)
        off = body["offset"]
        window = idx[:total][off:off + body["limit"]]
        return {"total": total if off == 0 else 0, "jobPostings": [_job(i) for i in window],
                "facets": facets_for(applied) if off == 0 else []}
    return fake


def _run(fake):
    with patch.object(workday, "fetch_json_post", side_effect=fake):
        return workday.fetch_jobs(SLUG, "Acme")


def test_small_board_is_not_split():
    calls = []
    fake = _board({"eng": list(range(30))})
    def counting(url, body=None, **kw):
        calls.append(body)
        return fake(url, body=body, **kw)
    assert len(_run(counting)) == 30
    assert all("appliedFacets" not in b for b in calls)


def test_capped_board_is_split_by_family_and_deduped():
    groups = {"eng": list(range(0, 1500)), "sales": list(range(1500, 2300)), "ops": list(range(2300, 2700))}
    jobs = _run(_board(groups))
    assert len(jobs) == 2700 and len({j["job_id"] for j in jobs}) == 2700


def test_capped_board_does_not_page_the_unsplit_query():
    groups = {"eng": list(range(0, 1500)), "sales": list(range(1500, 2300)), "ops": list(range(2300, 2700))}
    fake = _board(groups)
    unsplit_offsets = []
    def counting(url, body=None, **kw):
        if "appliedFacets" not in body:
            unsplit_offsets.append(body["offset"])
        return fake(url, body=body, **kw)
    assert len(_run(counting)) == 2700
    assert unsplit_offsets == [0]


def test_slice_still_over_threshold_is_split_again_or_warned():
    # one family alone at the cap and no other facet to split by → returns what it can, warns
    groups = {"eng": list(range(0, 2500)), "sales": list(range(2500, 2600))}
    with patch.object(workday.logger, "warning") as warn:
        jobs = _run(_board(groups))
    assert len(jobs) == 2000 + 100
    assert any("cannot be split" in c.args[0] for c in warn.call_args_list)


def test_failed_page_midway_warns_and_returns_partial():
    fake = _board({"eng": list(range(100))})
    def flaky(url, body=None, **kw):
        return None if body["offset"] >= 40 else fake(url, body=body, **kw)
    with patch.object(workday.logger, "warning") as warn:
        assert len(_run(flaky)) == 40
    assert any("request failed" in c.args[0] for c in warn.call_args_list)


def test_empty_page_before_total_warns():
    fake = _board({"eng": list(range(100))})
    def short(url, body=None, **kw):
        d = fake(url, body=body, **kw)
        return {**d, "jobPostings": []} if body["offset"] >= 60 else d
    with patch.object(workday.logger, "warning") as warn:
        assert len(_run(short)) == 60
    assert any("empty page" in c.args[0] for c in warn.call_args_list)
