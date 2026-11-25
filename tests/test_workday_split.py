"""Tests for the Workday planner in jobs/ats/workday.py — a fake board stands in for fetch_json_post (no network)."""
from unittest.mock import patch

from jobs.ats import workday
from jobs.ats.fetch_report import STOP_EARLY_EXIT, STOP_NO_CONFIG, STOP_REQUEST_FAILED

SLUG = {"slug": "acme", "wd": "wd5", "path": "Ext"}
PAGE = 20


class Board:
    """Fake Workday list endpoint. `jobs` = [{"attrs": {facet: value}, "title": str}], in server order.
    cap=2000 models a capped tenant (offsets past the cap repeat page 1); cap=None pages to the real end."""

    def __init__(self, jobs, cap=2000, descriptors=None):
        self.jobs, self.cap, self.descriptors = jobs, cap, descriptors or {}
        self.calls = []

    def __call__(self, url, body=None, headers=None, platform=None):
        self.calls.append(body)
        applied = body.get("appliedFacets") or {}
        text = (body.get("searchText") or "").lower()
        rows = [(i, j) for i, j in enumerate(self.jobs)
                if all(j["attrs"].get(f) in ids for f, ids in applied.items())
                and (not text or text in j["title"].lower())]
        n = len(rows)
        capped = self.cap is not None and n > self.cap
        total = self.cap if capped else n
        off = body["offset"]
        window = rows[:total]
        eff = 0 if (capped and off >= self.cap) else off
        page = [_posting(i, j) for i, j in window[eff:eff + body["limit"]]]
        facets = []
        if off == 0:
            for name in sorted({f for _, j in rows for f in j["attrs"]}):
                counts = {}
                for _, j in rows:
                    if name in j["attrs"]:
                        counts[j["attrs"][name]] = counts.get(j["attrs"][name], 0) + 1
                facets.append({"facetParameter": name, "values": [
                    {"descriptor": self.descriptors.get(v, v), "id": v, "count": c} for v, c in counts.items()]})
        return {"total": total if off == 0 else 0, "jobPostings": page, "facets": facets}

    def split_calls(self):
        return [c for c in self.calls if c.get("appliedFacets")]


def _posting(i, j):
    return {"title": j["title"], "externalPath": f"/job/X/{j['title'].replace(' ', '-')}_R-{i}",
            "bulletFields": [f"R-{i}"], "locationsText": "Austin, TX"}


def _jobs(n, start=0, **attrs):
    return [{"attrs": dict(attrs), "title": f"Engineer {i}"} for i in range(start, start + n)]


def _run(board, slug=SLUG, gate=None):
    with patch.object(workday, "fetch_json_post", side_effect=board):
        return workday.fetch_jobs(slug, "Acme", stop_check=gate)


def _ids(jobs):
    return {j["job_id"] for j in jobs}


# ── board types ──────────────────────────────────────────────────────────────

def test_small_board_is_one_flat_leaf():
    board = Board(_jobs(30, fam="a"))
    jobs = _run(board)
    assert len(jobs) == 30 and not board.split_calls()
    assert jobs.report.complete is True


def test_uncapped_board_over_the_cap_pages_flat():
    board = Board(_jobs(2500, fam="a"), cap=None)   # BofA / Micron / CVS style
    jobs = _run(board)
    assert len(_ids(jobs)) == 2500 and not board.split_calls()
    assert [c["offset"] for c in board.calls].count(2000) == 2   # the probe, then the real page 2000
    assert jobs.report.complete is True and jobs.report.note == ""


def test_capped_board_is_split_and_deduped():
    jobs_ = _jobs(1500, fam="eng") + _jobs(800, 1500, fam="sales") + _jobs(400, 2300, fam="ops")
    board = Board(jobs_)
    out = _run(board)
    assert len(_ids(out)) == 2700 and out.report.complete is True
    assert [c["offset"] for c in board.calls if not c.get("appliedFacets")] == [0, 2000]   # page 1 + probe only


def test_cheapest_facet_is_chosen():
    # `fam` = 3 values (about 135 pages); `loc` = 100 values of 27 jobs (2 pages each = 200 pages)
    jobs_ = [{"attrs": {"fam": ("a", "b", "c")[i % 3 if i > 1200 else 0], "loc": f"L{i % 100}"}, "title": f"Engineer {i}"}
             for i in range(2700)]
    board = Board(jobs_)
    _run(board)
    assert board.split_calls() and all(set(c["appliedFacets"]) == {"fam"} for c in board.split_calls())


def test_slice_with_no_usable_facet_is_flagged():
    board = Board(_jobs(2500, fam="a") + _jobs(100, 2500, fam="b"))
    with patch.object(workday.logger, "warning"):
        jobs = _run(board)
    assert len(jobs) == 2000 + 100
    assert jobs.report.complete is False and jobs.report.stop_reason == "page_cap"


# ── recovery: node union -> window -> search ──────────────────────────────────

def _gap_board(hidden, hide_first=True):
    """One family of 2200 jobs split by city; `hidden` of them carry no city. They come first in server order."""
    rows = []
    for i in range(2200):
        attrs = {"fam": "a"}
        if not (hide_first and i < hidden):
            attrs["city"] = f"C{i % 4}"
        rows.append({"attrs": attrs, "title": f"Engineer {i}"})
    rows += _jobs(100, 2200, fam="b", city="C0")
    return Board(rows)


def test_gap_inside_tolerance_needs_no_recovery():
    board = _gap_board(8)
    jobs = _run(board)
    assert not any(c.get("searchText") for c in board.calls)
    assert len(jobs) == 2300 and jobs.report.complete is True   # the node's own page 1 holds the 8 city-less jobs


def test_gap_beyond_tolerance_is_closed_by_the_window():
    board = _gap_board(100)
    jobs = _run(board)
    assert len(_ids(jobs)) == 2300
    assert not any(c.get("searchText") for c in board.calls)


def test_gap_beyond_tolerance_and_no_window_is_closed_by_search():
    board = _gap_board(100)
    with patch.object(workday, "WORKDAY_WINDOW_MAX", 0):
        jobs = _run(board)
    assert len(_ids(jobs)) == 2300 and any(c.get("searchText") == "engineer" for c in board.calls)


def test_gap_that_nothing_closes_flags_the_report():
    board = _gap_board(100, hide_first=False)   # hidden jobs sit past the window and past the search results
    for j in board.jobs[2100:2200]:
        j["attrs"].pop("city")
        j["title"] = "Zzzz"
    with patch.object(workday, "WORKDAY_WINDOW_MAX", 0), patch.object(workday, "WORKDAY_SEARCH_IDLE_TERMS", 1):
        jobs = _run(board)
    assert jobs.report.complete is False and "nodes still short" in jobs.report.note


# ── Accenture US filter ──────────────────────────────────────────────────────

ACC = {"slug": "accenture", "wd": "wd103", "path": "AccentureCareers"}


def test_country_filter_is_applied_to_every_request():
    rows = _jobs(300, locationCountry="us-id") + _jobs(500, 300, locationCountry="in-id")
    board = Board(rows, descriptors={"us-id": "United States of America", "in-id": "India"})
    jobs = _run(board, slug=ACC)
    assert len(_ids(jobs)) == 300 and jobs.report.reported_total == 300
    assert all(c.get("appliedFacets") == {"locationCountry": ["us-id"]} for c in board.calls[1:])


def test_country_filter_survives_split_window_and_search():
    # 2500 US jobs (split by fam) + non-US jobs sharing the same fam values
    rows = (_jobs(1500, locationCountry="us-id", fam="a") + _jobs(1000, 1500, locationCountry="us-id", fam="b")
            + _jobs(400, 2500, locationCountry="in-id", fam="a"))
    board = Board(rows, descriptors={"us-id": "United States of America", "in-id": "India"})
    with patch.object(workday, "WORKDAY_WINDOW_MAX", 0):
        jobs = _run(board, slug=ACC)
    assert board.split_calls() and len(_ids(jobs)) == 2500
    assert all(c.get("appliedFacets", {}).get("locationCountry") == ["us-id"] for c in board.calls[1:])


def test_missing_country_value_fetches_nothing():
    board = Board(_jobs(300, locationCountry="x"))
    with patch.object(workday.logger, "error"):
        jobs = _run(board, slug=ACC)
    assert len(jobs) == 0 and len(board.calls) == 1
    assert jobs.report.stop_reason == STOP_NO_CONFIG and jobs.report.complete is False


# ── failures, gate, budget ───────────────────────────────────────────────────

def test_failed_page_midway_is_not_complete():
    board = Board(_jobs(100, fam="a"))

    def flaky(url, body=None, **kw):
        return None if body["offset"] >= 40 else board(url, body=body, **kw)
    with patch.object(workday.logger, "warning") as warn:
        jobs = _run(flaky)
    assert len(jobs) == 40 and jobs.report.stop_reason == STOP_REQUEST_FAILED and jobs.report.complete is False
    assert any("request failed" in c.args[0] for c in warn.call_args_list)


def test_short_page_is_retried_once_and_a_missing_job_is_not_complete():
    board = Board(_jobs(100, fam="a"))

    def short(url, body=None, **kw):
        d = board(url, body=body, **kw)
        return {**d, "jobPostings": []} if body["offset"] >= 60 else d
    jobs = _run(short)
    assert len(jobs) == 60 and jobs.report.complete is False


class _Gate:
    def fresh(self):
        return _Gate()

    def __call__(self, jobs):
        return True   # every non-final page looks "all seen"


def test_early_exit_stops_each_leaf_and_skips_recovery():
    board = _gap_board(8)
    jobs = _run(board, gate=_Gate())
    assert jobs.report.stop_reason == STOP_EARLY_EXIT
    assert len(jobs) < 2300 and not any(c.get("searchText") for c in board.calls)


def test_gate_never_sees_the_capped_unsplit_query():
    seen = []

    class Spy(_Gate):
        def fresh(self):
            return self

        def __call__(self, jobs):
            seen.append(len(jobs))
            return False
    _run(Board(_jobs(1500, fam="eng") + _jobs(800, 1500, fam="sales")), gate=Spy())
    assert seen and max(seen) <= PAGE   # only leaf pages; the 2000-job unsplit page 1 is not gated separately


def test_request_budget_is_a_hard_stop():
    board = Board(_jobs(1500, fam="a") + _jobs(900, 1500, fam="b"))
    with patch.object(workday, "WORKDAY_BUDGET_SLACK", -0.9), patch.object(workday.logger, "warning"):
        jobs = _run(board)
    assert jobs.report.complete is False and len(board.calls) < 125
    assert len(jobs) > 0          # rows fetched before the stop are kept


# ── helpers ──────────────────────────────────────────────────────────────────

def test_flatten_facets_expands_nested_location_groups():
    flat = workday._flatten_facets([
        {"facetParameter": "timeType", "values": [{"id": "ft", "count": 1}]},
        {"facetParameter": "locationMainGroup", "values": [
            {"facetParameter": "locationCountry", "values": [{"id": "in", "count": 1}]},
            {"facetParameter": "locations", "values": [{"id": "blr", "count": 1}]}]},
    ])
    assert [f["facetParameter"] for f in flat] == ["timeType", "locationCountry", "locations"]


def test_board_size_is_the_median_of_the_agreeing_sums():
    cands = [("a", [], 2700), ("b", [], 2710), ("c", [], 3152), ("d", [], 941)]
    assert workday._board_size(cands) == 2705


def test_job_key_falls_back_to_external_url():
    k = workday._job_key
    assert k({"externalUrl": "https://x.com/1", "title": "T"}) != k({"externalUrl": "https://x.com/2", "title": "T"})
    assert k({"externalPath": "/job/a", "externalUrl": "https://x.com/1"}) == "/job/a"
    # a generic (non "/job/") URL shared by postings must not beat the bullet id
    assert k({"externalUrl": "https://x.com/careers", "bulletFields": ["R-1"]}) == "R-1"
    # a posting URL keys like its externalPath: board-relative, host / locale / site prefix dropped
    assert k({"externalUrl": "https://x.com/en-US/Site/job/1", "bulletFields": ["R-1"]}) == "/job/1"
    assert k({"externalUrl": "https://x.com/job/1"}) == k({"externalPath": "/job/1"})
    assert k({"externalUrl": "https://x.com/job/1"}) != k({"externalUrl": "https://x.com/job/2"})


def test_job_id_rule():
    f = workday._job_req_id
    assert f({"bulletFields": ["R-1"]}, "/job/X/T_R-1", "u") == "R-1"
    assert f({"bulletFields": ["Spotlight Job", "JR0289044"]}, "/job/X/T_JR0289044-1", "u") == "JR0289044"   # Intel
    assert f({"bulletFields": ["Austin", "R5"]}, "/job/X/T_R5", "u") == "R5"                              # Motorola
    assert f({"bulletFields": ["Application Deadline: x"]}, "/job/X/T_R123", "u") == "R123"                # Wisconsin
    assert f({"bulletFields": ["R-9"]}, "", "u") == "R-9"                                                  # no path
