# jobs/ats/workday.py — Workday undocumented public API client
# Date field: postedOn (RELIABLE — original posting date)
# Risk: undocumented — monitor for structural changes

import collections
import json
import logging
import math
import re
import statistics
from datetime import datetime
from urllib.parse import urlparse
from config import (
    WORKDAY_BUDGET_PAD, WORKDAY_COUNTRY_FILTER_DESCRIPTOR, WORKDAY_COUNTRY_FILTER_FACET,
    WORKDAY_COUNTRY_FILTER_TENANT, WORKDAY_BUDGET_SLACK, WORKDAY_FACET_FLOOR, WORKDAY_FACET_MAXSHARE,
    WORKDAY_FACET_SUM_TOL, WORKDAY_GAP_MIN, WORKDAY_GAP_PENALTY, WORKDAY_GAP_TOL, WORKDAY_PAGE_SIZE,
    WORKDAY_PLAN_MAX_DEPTH, WORKDAY_RESULT_CAP, WORKDAY_SEARCH_CHUNK_PAGES, WORKDAY_SEARCH_IDLE_TERMS,
    WORKDAY_SEARCH_MIN_NEW, WORKDAY_SEARCH_STOPWORDS, WORKDAY_SEARCH_TERM_MIN_LEN,
    WORKDAY_WINDOW_MAX,
)
from jobs.ats.base import fetch_json, fetch_json_post, last_post_was_challenge, slugify, validate_company_match
from jobs.ats.fetch_report import (
    FetchReport, STOP_ALL_PAGES, STOP_EARLY_EXIT, STOP_NO_CONFIG, STOP_PAGE_CAP, STOP_REQUEST_FAILED, TOTAL_ATS, TOTAL_FACET_SUM,
    attach, count_distinct,
)
from jobs.utils import clean_html

logger = logging.getLogger(__name__)


WD_VARIANTS = [
    "wd5", "wd1", "wd2", "wd3", "wd4",
    "wd6", "wd7", "wd8", "wd10", "wd12",
]

# Base domain — used to construct full job URLs. A bare host ("wd": "") has no data-center label,
# e.g. https://nxp.myworkdayjobs.com (see workday_domain()).
BASE_DOMAIN      = "https://{slug}.{wd}.myworkdayjobs.com"
BASE_DOMAIN_BARE = "https://{slug}.myworkdayjobs.com"
BASE_DOMAIN_SITE = "https://{wd}.myworkdaysite.com"
API_PATH         = "/wday/cxs/{slug}/{path}/jobs"

WD_PATH_VARIANTS = [
    "careers",
    "External",
    "jobs",
    "Careers",
    "career",
]

# Workday requires full browser headers — plain User-Agent causes total=0 on page 2+
WORKDAY_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "application/json",
    "Content-Type":    "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.myworkdayjobs.com/",
}


def wd_present(wd, site=None):
    """
    True when a stored `wd` / `_wd` value is usable (design: presence check, 2026-11-23).

    The key must exist (None = missing = error) but the value may be the empty string, which means
    the bare host {slug}.myworkdayjobs.com (NXP). `myworkdaysite` tenants build the host FROM wd, so
    an empty value is never valid for them. Every guard that used a truthiness test on wd/_wd uses this.
    """
    if wd is None:
        return False
    if wd == "" and site == "myworkdaysite":
        return False
    return True


def workday_domain(slug, wd, site=None):
    """Base domain (no trailing slash) for a Workday tenant; the one place the host is formatted."""
    if site == "myworkdaysite":
        return BASE_DOMAIN_SITE.format(wd=wd)
    if wd == "":
        return BASE_DOMAIN_BARE.format(slug=slug)
    return BASE_DOMAIN.format(slug=slug, wd=wd)


def _build_url(slug_info):
    """Build correct API URL based on which Workday domain is used."""
    slug = slug_info["slug"]
    wd   = slug_info["wd"]
    path = slug_info.get("path", "careers")
    return workday_domain(slug, wd, slug_info.get("site")) + API_PATH.format(slug=slug, path=path)


def _build_domain(slug_info):
    """Build base domain for constructing full job URLs."""
    return workday_domain(slug_info["slug"], slug_info["wd"], slug_info.get("site"))


def detect(company):
    """
    Try to detect if company uses Workday.
    Returns (slug_info, sample_jobs) or (None, None).
    """
    for slug in slugify(company):
        for wd in WD_VARIANTS:
            for path in _get_path_variants(slug, company):
                url  = _build_url({"slug": slug, "wd": wd, "path": path})
                data = fetch_json_post(url, body={"limit": 20, "offset": 0},
                                       headers=WORKDAY_HEADERS,
                                       platform="workday", track=False)  # detection — don't track
                if data is None:
                    continue
                jobs = data.get("jobPostings", [])
                if not isinstance(jobs, list):
                    continue
                if len(jobs) == 0:
                    if "total" in data:
                        return {"slug": slug, "wd": wd, "path": path}, []
                    continue
                first_title = jobs[0].get("title") or ""
                # Support both externalPath (real API) and externalUrl (tests/legacy)
                first_path  = jobs[0].get("externalPath") or jobs[0].get("externalUrl") or ""
                if not validate_company_match(first_path + first_title, company):
                    continue
                return {"slug": slug, "wd": wd, "path": path}, jobs
    return None, None


def _get_path_variants(slug, company):
    import re
    words   = re.sub(r"[^a-zA-Z0-9\s]", "", company).split()
    camel   = "_".join(w.capitalize() for w in words if w)
    camel2  = "".join(w.capitalize() for w in words if w)
    slug_up = slug.capitalize()

    seen, variants = set(), []
    for v in WD_PATH_VARIANTS + [camel, camel2, slug_up, slug]:
        if v and v not in seen:
            seen.add(v)
            variants.append(v)
    return variants


def _country_filter(url, company, rep):
    """
    appliedFacets for the one tenant that is read for a single country (Accenture: US only; non-US jobs are
    deleted after the detail fetch anyway). The value id is looked up on page 1 of the unfiltered query each
    scan, so a re-keyed id cannot break it. Returns {facet: [id]}, or None when page 1 failed or the value
    is missing (rep["failed"] / rep["filter_missing"] say which).
    """
    data = _page1(url, {}, company, rep)
    if data is None:
        rep["failed"] = True
        logger.warning("workday country filter: page 1 failed for %r", company)
        return None
    wanted = WORKDAY_COUNTRY_FILTER_DESCRIPTOR.strip().lower()
    for f in _flatten_facets(data.get("facets") or []):
        if f.get("facetParameter") != WORKDAY_COUNTRY_FILTER_FACET:
            continue
        for v in f.get("values") or []:
            if isinstance(v, dict) and v.get("id") and str(v.get("descriptor", "")).strip().lower() == wanted:
                return {WORKDAY_COUNTRY_FILTER_FACET: [v["id"]]}
    rep["filter_missing"] = True
    logger.error("workday country filter: %r has no %r value in facet %s — not fetching the unfiltered board",
                 company, WORKDAY_COUNTRY_FILTER_DESCRIPTOR, WORKDAY_COUNTRY_FILTER_FACET)
    return None


def _filter_missing_result(rep):
    """Not-complete report for a scan that could not resolve its country filter (nothing fetched)."""
    stop = STOP_NO_CONFIG if rep.get("filter_missing") else STOP_REQUEST_FAILED
    return attach([], FetchReport(
        stop_reason=stop, pages=rep["pages"], fetched_distinct=0, unusable=0,
        reported_total=None, total_source=TOTAL_ATS,
        note=f"country filter {WORKDAY_COUNTRY_FILTER_DESCRIPTOR!r} not resolved",
    ))


def fetch_jobs(slug_info, company, stop_check=None):
    """
    Fetch all jobs for company from Workday.
    Plans, pages and recovers (see _plan_board). stop_check: adaptive-scan EarlyExitGate (fresh gate per leaf).
    Returns list of normalized job dicts.
    slug_info = {"slug": "att", "wd": "wd1", "path": "ATTGeneral"}
    """
    if not wd_present(slug_info.get("wd"), slug_info.get("site")):
        logger.warning("workday fetch: %r has no usable wd in %s — nothing fetched", company, slug_info)
        return attach([], FetchReport(
            stop_reason=STOP_NO_CONFIG, pages=0, fetched_distinct=0, unusable=0,
            reported_total=None, total_source=TOTAL_ATS, note="wd missing",
        ))
    url    = _build_url(slug_info)
    domain = _build_domain(slug_info)
    path   = slug_info.get("path", "careers")  # e.g. "ATTGeneral"

    # Workday caps some tenants' queries at WORKDAY_RESULT_CAP results (Nvidia reports total=2000 for 2700 jobs;
    # offsets past the cap repeat page 1). The planner (see _plan_board) probes the tenant once, splits a
    # capped board into leaves below the cap, pages them one after another (early-exit gate per leaf),
    # then closes gaps between promised and fetched counts (_recover). An uncapped board (BofA, Micron,
    # CVS) is a single leaf paged flat.
    rep = {"pages": 0, "failed": False, "capped": False, "expected": None, "capped_board": None}
    norm = None
    if stop_check is not None:
        def norm(j):
            return _normalize(j, company, domain, path, slug_info) if j.get("title") else None
    base = {}
    if str(slug_info.get("slug") or "").lower() == str(WORKDAY_COUNTRY_FILTER_TENANT).lower():
        base = _country_filter(url, company, rep)
        if base is None:   # filter value missing: never fall back to the unfiltered board
            return _filter_missing_result(rep)
    leaves, nodes = _plan_board(url, company, rep, base)
    merged, leaf_got = _fetch_leaves(url, leaves, company, rep, gate=stop_check, norm=norm)
    if rep.get("leaf_mismatch") and not nodes:
        rep["capped"] = True   # a flat board has no node recovery: rows its leaf lost stay lost
    _keep_node_page1(nodes, merged, leaf_got, rep)
    if not rep.get("blocked"):   # a Cloudflare challenge ends the scan: no recovery requests
        _recover(url, nodes, merged, leaf_got, company, rep)
    all_jobs = list(merged.values())

    results = []
    for j in all_jobs:
        if not j.get("title"):
            continue
        item = _normalize(j, company, domain, path, slug_info)
        if item is not None:
            results.append(item)

    if rep["failed"]:
        stop = STOP_REQUEST_FAILED
    elif rep["capped"]:
        stop = STOP_PAGE_CAP
    elif rep.get("early"):
        stop = STOP_EARLY_EXIT
    else:
        stop = STOP_ALL_PAGES
    split = bool(nodes)   # a board no facet could split is reported by its root total
    notes = ["split by facet"] if split else []
    if rep.get("capped_slices"):
        notes.append(f"{len(rep['capped_slices'])} slices over the cap with no usable facet")
    if rep.get("partial"):
        notes.append("nodes still short: " + "; ".join(f"{n} of {k}" for _a, k, n in rep["partial"]))
    if rep.get("leaf_mismatch"):
        notes.append(f"{len(rep['leaf_mismatch'])} leaves off their promised count")
    if rep.get("over_budget"):
        notes.append("request budget used up")
    return attach(results, FetchReport(
        stop_reason      = stop,
        pages            = rep["pages"],
        fetched_distinct = count_distinct(results),
        unusable         = len(all_jobs) - len(results),
        reported_total   = rep["expected"] if split else rep.get("reported", 0),
        total_source     = TOTAL_FACET_SUM if split else TOTAL_ATS,
        note             = "; ".join(notes),
    ))


def _job_key(posting):
    """Identity of a listing across facet slices: its relative path, else req id, else title+location."""
    if posting.get("externalPath"):
        return posting["externalPath"]
    url = posting.get("externalUrl") or ""
    bare = url.split("#", 1)[0].split("?", 1)[0]    # tracking query/fragment must not make one posting two keys
    # only a posting-specific URL counts, a generic one is shared. A "/job/" URL keys by its
    # board-relative path, the form externalPath has (host, locale and site prefix dropped); a legacy URL
    # ending in a posting id (".../1", "..._R-12") keys by the whole URL
    try:
        upath = urlparse(bare).path
    except ValueError:   # malformed URL (e.g. "http://[..."): not posting-specific, fall through
        upath = ""
    last = upath.rstrip("/").rsplit("/", 1)[-1]
    if "/job/" in upath:
        own = upath[upath.find("/job/"):]
    elif last.isdigit() or re.search(r"_(?:JR|R)-?\d+(?:-\d+)?$", last):
        own = bare
    else:
        own = ""
    return (own
            or (posting.get("bulletFields") or [""])[0]
            or url
            or f"{posting.get('title', '')}|{posting.get('locationsText', '')}")


def _board_is_capped(url, applied, first_jobs, rep, company):
    """
    Per-tenant yes/no cap probe (design P2). Called only when page 1 of a query reports a total at/above
    WORKDAY_RESULT_CAP. Requests the page at offset WORKDAY_RESULT_CAP and compares it with page 1: a
    capped tenant serves page 1 again (offsets past the cap repeat it), an uncapped one (BofA, Micron,
    CVS) serves the next real page, or none when the board is exactly cap-sized.

    The answer is per tenant, so it is cached on `rep["capped_board"]` for the whole scan: the first
    call (the unfiltered board) probes, every later call (a facet slice of that board) reuses it. A
    failed probe is treated as capped (the facet split is the safe path) and is not cached, so the next
    query retries it. With no `rep` there is nowhere to cache, so it probes each time.
    """
    cached = None if rep is None else rep.get("capped_board")
    if cached is not None:
        return cached
    first_keys = {_job_key(p) for p in first_jobs or []}
    body = {"limit": WORKDAY_PAGE_SIZE, "offset": WORKDAY_RESULT_CAP}
    if applied:
        body["appliedFacets"] = applied
    data = fetch_json_post(url, body=body, headers=WORKDAY_HEADERS, platform="workday",
                           before_retry=None if rep is None else lambda: _charge_retry(rep))
    if rep is not None:
        rep["pages"] += 1
    if not data:
        if rep is not None and last_post_was_challenge():
            rep["blocked"] = rep["failed"] = True
        logger.warning("workday fetch: cap probe at offset=%d failed for %r — treating board as capped",
                       WORKDAY_RESULT_CAP, company)
        return True
    probe_jobs = data.get("jobPostings") or []
    if rep is not None:
        # real rows of this query: kept for the result even if the scan stops before paging reaches them
        rep.setdefault("probe_rows", []).append((dict(applied or {}), probe_jobs))
    # capped = the probe page repeats page 1 (at least half of it); one shared key is not a repeat
    capped = bool(probe_jobs) and bool(first_keys) \
        and 2 * len(first_keys & {_job_key(p) for p in probe_jobs}) >= len(first_keys)
    if rep is not None:
        rep["capped_board"] = capped
    logger.info("workday fetch: %r cap probe at offset=%d -> %s", company, WORKDAY_RESULT_CAP,
                "capped" if capped else "uncapped (paged flat)")
    return capped


def _flatten_facets(facets):
    """
    Expand grouped facets. Some tenants (Accenture's `locationMainGroup`) hold no values of their own: they
    hold groups ("Country", "City"), each with its own facetParameter ("locationCountry", "locations") and
    id/count values. The group's facetParameter is what appliedFacets takes, so each group is yielded as a
    facet of its own; plain facets pass through unchanged.
    """
    out = []
    for f in facets:
        if not isinstance(f, dict):
            continue
        groups = [g for g in f.get("values") or []
                  if isinstance(g, dict) and g.get("facetParameter") and isinstance(g.get("values"), list)]
        out.extend(groups if groups else [f])
    return out


# ── Planner (design P3, step 9) ─────────────────────────────────────────────────────────────────────
# A capped board is split by the facet with the lowest request cost into leaves (each below the cap),
# the leaves are paged one after another, and every request counts against a hard budget. Not wired
# fetch_jobs runs it: plan -> fetch leaves -> recover.

class _OverBudget(Exception):
    """The scan used its request budget: planned requests x (1 + slack) + a pad per slice."""


def _budget_add(rep, cost):
    """Raise the request budget for `cost` planned requests (one pad per call = per slice)."""
    rep["budget"] = rep.get("budget", 0.0) + cost * (1 + WORKDAY_BUDGET_SLACK) + WORKDAY_BUDGET_PAD


def _charge_retry(rep):
    """Charge one retry inside fetch_json_post against the budget; False (no retry) once it is used up."""
    if rep.get("logical", 0) >= rep.get("budget", 0.0):
        logger.warning("workday fetch: request budget used up (%d requests) — retry skipped", rep.get("logical", 0))
        return False
    rep["logical"] = rep.get("logical", 0) + 1
    return True


def _post(url, applied, offset, company, rep, text=None, before_retry=None):
    """One list request; it and each of its retries count against the budget. Raises _OverBudget when
    nothing is left for it; returns the JSON or None. After a Cloudflare challenge nothing more is sent (None).
    `before_retry` replaces the default retry check; it must charge the budget itself (_charge_retry)."""
    if rep.get("blocked"):
        return None
    if rep.get("logical", 0) >= rep.get("budget", 0.0):
        rep["over_budget"] = True
        raise _OverBudget(f"{company}: {rep.get('logical', 0)} requests >= budget {rep.get('budget', 0.0):.0f}")
    rep["logical"] = rep.get("logical", 0) + 1
    rep["pages"] += 1
    body = {"limit": WORKDAY_PAGE_SIZE, "offset": offset}
    if applied:
        body["appliedFacets"] = applied
    if text:
        body["searchText"] = text
    data = fetch_json_post(url, body=body, headers=WORKDAY_HEADERS, platform="workday",
                           before_retry=before_retry or (lambda: _charge_retry(rep)))
    rep["challenge"] = (not data) and last_post_was_challenge()   # a Cloudflare challenge is not retried
    if rep["challenge"]:
        # the whole scan stops: no more pages, sibling leaves or recovery
        rep["blocked"] = rep["failed"] = True
    return data


def _page1(url, applied, company, rep):
    """Page 1 of a query (total, jobs, facets), cached per scan; None when the request failed."""
    cache = rep.setdefault("node_page1", {})
    k = json.dumps(applied, sort_keys=True)
    if k in cache:
        return cache[k]
    _budget_add(rep, 1)
    data = _post(url, applied, 0, company, rep)
    if data:
        cache[k] = data
    return data or None


def _facet_candidates(facets, used):
    """(name, values, sum) for every unused facet with 2+ non-empty values (nested groups flattened)."""
    out = []
    for f in _flatten_facets(facets or []):
        name = f.get("facetParameter")
        vals = [v for v in f.get("values") or [] if isinstance(v, dict) and v.get("id") and v.get("count", 0) > 0]
        if name and name not in used and len(vals) >= 2:
            out.append((name, vals, sum(v["count"] for v in vals)))
    return out


def _board_size(cands):
    """Median of the largest group of facet sums within WORKDAY_FACET_SUM_TOL of each other (tie: larger)."""
    sums = sorted(s for _, _, s in cands if s > WORKDAY_PAGE_SIZE)
    best = None
    for a in sums:
        members = [s for s in sums if abs(s - a) <= WORKDAY_FACET_SUM_TOL * a]
        key = (len(members), statistics.median(members))
        if best is None or key > best[0]:
            best = (key, members)
    return int(statistics.median(best[1])) if best else None


def _facet_score(values, total, size):
    """Request cost of splitting by this facet: its pages, +2 per value still at the cap, + a penalty
    for jobs the facet would miss (size - total)."""
    pages = sum(math.ceil(v["count"] / WORKDAY_PAGE_SIZE) for v in values)
    over = sum(1 for v in values if v["count"] >= WORKDAY_RESULT_CAP)
    return pages + 2 * over + WORKDAY_GAP_PENALTY * max(0, size - total) / WORKDAY_PAGE_SIZE


def _choose_facet(cands, size):
    """The cheapest candidate that covers most of the slice, preferring facets that really split it."""
    ok = [c for c in cands if c[2] >= WORKDAY_FACET_FLOOR * size]
    split = [c for c in ok if max(v["count"] for v in c[1]) <= WORKDAY_FACET_MAXSHARE * size] or ok
    ranked = sorted(split, key=lambda c: _facet_score(c[1], c[2], size))
    return ranked[0] if ranked else None


def _distinct(rows):
    """Number of different postings in a page: a repeated row does not fill the window."""
    return len({_job_key(p) for p in rows})


def _make_leaf(rep, applied, size, first=None):
    """A query below the cap that is paged to its end. `first` = page 1 when already fetched."""
    first = list(first or [])
    # a cached page 1 that came back short is fetched again from offset 0; a full one costs no request
    start = len(first) if _distinct(first) >= min(WORKDAY_PAGE_SIZE, size) else 0
    offs = list(range(start, size, WORKDAY_PAGE_SIZE))
    _budget_add(rep, len(offs))
    return {"applied": applied, "K": size, "first": first, "offs": offs}


def _plan_board(url, company, rep, base=None):
    """
    Iterative work list over the board. Returns (leaves, nodes): leaves are queries below the cap,
    nodes are the capped queries that were split (applied, K, facet, depth, sum, used) for the recovery steps.

    rep gets "reported" (page-1 total of the root query), "expected" (the board size, only when split),
    "capped" (the root itself is capped and no facet can split it), "capped_slices" (a child slice like
    that: _recover decides whether it leaves a gap), "failed" (a node's page 1 failed), "over_budget".
    `base` = filters applied to the whole board (the Accenture US filter).
    """
    base = dict(base or {})
    leaves, nodes = [], []
    work = [(base, None, tuple(base), 0)]
    try:
        while work:
            applied, size, used, depth = work.pop()
            data = _page1(url, applied, company, rep)
            if data is None:
                rep["failed"] = True
                if rep.get("blocked"):
                    logger.warning("workday plan: Cloudflare challenge for %r — planning stopped", company)
                    break
                logger.warning("workday plan: page 1 failed for %r applied=%s — slice skipped", company, applied)
                continue
            total = data.get("total", 0) or 0
            first = data.get("jobPostings") or []
            if depth == 0:
                rep["reported"] = total   # recorded before the retry below, which may skip the board
            if total > 0 and not first:
                # an empty page 1 must not decide flat vs split: ask once more
                _budget_add(rep, 1)
                again = _post(url, applied, 0, company, rep)
                if again and again.get("jobPostings"):
                    data = again
                    rep["node_page1"][json.dumps(applied, sort_keys=True)] = again
                    total = data.get("total", 0) or total
                    first = data["jobPostings"]
                elif total >= WORKDAY_RESULT_CAP:
                    rep["failed"] = True
                    logger.warning("workday plan: page 1 empty for %r applied=%s (total=%d) — slice skipped",
                                   company, applied, total)
                    continue
            if depth == 0:
                rep["reported"] = total
            capped = False
            if total >= WORKDAY_RESULT_CAP and first:
                if rep.get("capped_board") is None:
                    _budget_add(rep, 1)
                    rep["logical"] = rep.get("logical", 0) + 1   # the probe is one request
                capped = _board_is_capped(url, applied, first, rep, company)
            if not capped:
                leaves.append(_make_leaf(rep, applied, total, first))
                continue
            cands = _facet_candidates(data.get("facets"), used)
            if size is None:
                size = max(_board_size(cands) or 0, total)
                rep["expected"] = size
            best = _choose_facet(cands, size) if depth < WORKDAY_PLAN_MAX_DEPTH else None
            if best is None:
                keep = min(WORKDAY_RESULT_CAP, total)
                leaves.append(_make_leaf(rep, applied, keep, first))
                if depth:
                    # its parent node covers it: _recover sets "capped" if the node stays short
                    rep.setdefault("capped_slices", []).append((applied, size))
                else:
                    rep["capped"] = True   # nothing above the root can check coverage
                (logger.info if depth else logger.warning)(
                    "workday plan: %r slice %s has %d jobs and no usable facet — first %d only",
                    company, applied, size, keep)
                continue
            name, values, covered = best
            nodes.append({"applied": dict(applied), "K": size, "facet": name, "depth": depth,
                          "sum": covered, "used": used})
            logger.info("workday plan: %r depth=%d size=%d split by %s (%d values, sum=%d)",
                        company, depth, size, name, len(values), covered)
            for v in values:
                child = {**applied, name: [v["id"]]}
                if v["count"] < WORKDAY_RESULT_CAP:
                    leaves.append(_make_leaf(rep, child, v["count"]))
                else:
                    work.append((child, v["count"], used + (name,), depth + 1))
    except _OverBudget as e:
        rep["failed"] = True
        logger.warning("workday plan: request budget used up while planning (%s) — partial result", e)
    return leaves, nodes


def _fetch_leaf(url, leaf, company, rep, gate=None, norm=None):
    """
    Page one leaf sequentially. Returns {job_key: posting}. The early-exit gate (fresh per leaf) sees
    every non-final page; True ends the leaf and sets rep["early"]. Pages that failed or came back short
    are requested once more; a leaf that still does not match its promised count is recorded in
    rep["leaf_mismatch"] (leaf totals are exact).
    """
    got = {}
    leaf_gate = gate.fresh() if gate is not None and hasattr(gate, "fresh") else gate

    def take(jobs, final):
        for p in jobs:
            _merge_row(got, _job_key(p), p)
        if leaf_gate is None or norm is None:
            return False
        seen = [j for j in map(norm, jobs) if j]
        if final or len(seen) != len(jobs):
            # not a page the gate may stop on (last, short, retried or with titleless rows),
            # but an unseen job on it still breaks the streak
            if hasattr(leaf_gate, "reset_if_unseen"):
                leaf_gate.reset_if_unseen(seen)
            return False
        if leaf_gate(seen):
            rep["early"] = True
            return True
        return False

    offs = leaf["offs"]
    first = leaf["first"]
    asked = 0   # offsets of `offs` already requested
    # a cached page 1 shorter than it should be is not shown to the gate
    stopped = bool(first) and take(first, final=(not offs) or _distinct(first) < min(WORKDAY_PAGE_SIZE, leaf["K"]))
    try:
        for i, off in enumerate(offs):
            if stopped or rep.get("blocked"):
                break
            asked = i + 1
            expect = min(WORKDAY_PAGE_SIZE, leaf["K"] - off)   # the last page is legitimately short
            final = i == len(offs) - 1
            data = _post(url, leaf["applied"], off, company, rep)
            jobs = list(data.get("jobPostings") or []) if data else []
            retried = False
            if not data and rep.get("challenge"):
                # Cloudflare challenge: the same request can never pass, so no retry and no further request
                # for this scan (rep["blocked"]); keep what was fetched
                logger.warning("workday fetch: Cloudflare challenge at offset=%d for %r applied=%s — scan stopped, partial result",
                               off, company, leaf["applied"])
                break
            # distinct rows, on every page: a repeated row hides a missing one
            if not data or _distinct(jobs) < expect:
                # retried here, before the gate sees the page: a short page must not look "all seen"
                retried = True
                _budget_add(rep, 1)
                again = _post(url, leaf["applied"], off, company, rep)
                if again:
                    pos = {_job_key(p): n for n, p in enumerate(jobs)}
                    for p in again.get("jobPostings") or []:
                        k = _job_key(p)
                        if k not in pos:
                            pos[k] = len(jobs)
                            jobs.append(p)
                        elif p.get("title") and not jobs[pos[k]].get("title"):
                            jobs[pos[k]] = p   # a titled copy replaces a titleless one
                elif not data:
                    rep["failed"] = True
                    logger.warning("workday fetch: request failed at offset=%d for %r applied=%s — partial result",
                                   off, company, leaf["applied"])
                    continue
            # a page that needed a retry (two responses merged) or is still short keeps its rows but is not gated
            if take(jobs, final or _distinct(jobs) < expect or retried):
                stopped = True
                break
    except _OverBudget as e:
        e.got = got          # rows already fetched survive the hard stop
        raise
    if stopped:
        # only the pages never requested count as skipped on purpose; rows lost on a requested page
        # (short, or repeating an earlier one) stay a gap
        unrequested = sum(min(WORKDAY_PAGE_SIZE, leaf["K"] - o) for o in offs[asked:])
        rep.setdefault("early_leaves", []).append((leaf["applied"], len(got) + unrequested, len(got)))
    if not stopped and len(got) != leaf["K"]:
        rep.setdefault("leaf_mismatch", []).append((leaf["applied"], leaf["K"], len(got)))
    return got


def _merge_row(merged, k, p):
    """First row per key wins, except that a titled row replaces a titleless one."""
    if k not in merged or (p.get("title") and not merged[k].get("title")):
        merged[k] = p


def _fetch_leaves(url, leaves, company, rep, gate=None, norm=None):
    """Fetch the leaves one after another. Returns (merged {job_key: posting}, [(applied, {keys})])."""
    merged, leaf_got = {}, []

    def keep(leaf, got):
        for k, p in got.items():
            _merge_row(merged, k, p)
        leaf_got.append((leaf["applied"], set(got)))

    def planned(leaf):
        """The page 1 rows fetched while planning: kept, without a request, when no more can be sent."""
        got = {}
        for p in leaf["first"]:
            _merge_row(got, _job_key(p), p)
        return got

    done = 0
    try:
        for leaf in leaves:
            # after a Cloudflare challenge no more requests are sent
            keep(leaf, planned(leaf) if rep.get("blocked") else _fetch_leaf(url, leaf, company, rep, gate, norm))
            done += 1
    except _OverBudget as e:
        rep["failed"] = True
        keep(leaves[done], getattr(e, "got", None) or {})
        for leaf in leaves[done + 1:]:
            keep(leaf, planned(leaf))
        logger.warning("workday fetch: request budget used up (%s) — partial result", e)
    return merged, leaf_got


def _keep_node_page1(nodes, merged, leaf_got, rep):
    """A split node's own page 1 (already fetched while planning) holds real jobs, some of which may carry
    no value for the split facet and so appear in no leaf. Keep them and count them toward the node.
    The cap probe's page (rep["probe_rows"]) is kept the same way, under the filters it was asked with."""
    cache = rep.get("node_page1") or {}
    pages = [(node["applied"], (cache.get(json.dumps(node["applied"], sort_keys=True)) or {}).get("jobPostings") or [])
             for node in nodes]
    for applied, page in pages + (rep.get("probe_rows") or []):
        keys = set()
        for p in page:
            k = _job_key(p)
            _merge_row(merged, k, p)
            keys.add(k)
        if keys:
            leaf_got.append((dict(applied), keys))


def _node_union(applied, leaf_got, merged):
    """Keys of every fetched leaf that lies inside the node `applied` (its filters are a subset of the leaf's).
    Titleless rows are left out: they are dropped from the result, so they close no gap."""
    union = set()
    for leaf_applied, keys in leaf_got:
        if all(leaf_applied.get(a) == b for a, b in applied.items()):
            union |= keys
    return {k for k in union if (merged.get(k) or {}).get("title")}


def _search_node(url, node, merged, leaf_got, tol, company, rep, skipped=0):
    """
    Last recovery step for a node still short: searchText terms taken from the titles of jobs already
    found inside this node, each searched inside the node. New jobs join `merged` and the node's union.
    A term stops when a chunk adds under WORKDAY_SEARCH_MIN_NEW new jobs; the node stops after
    WORKDAY_SEARCH_IDLE_TERMS terms in a row add nothing. Sequential, budgeted.
    """
    applied, size = node["applied"], node["K"]
    have = _node_union(applied, leaf_got, merged)
    tried, idle = set(), 0
    # a search may spend what the facet would have cost to miss these jobs, no more
    allowance = WORKDAY_GAP_PENALTY * (size - len(have)) / WORKDAY_PAGE_SIZE
    spent = 0
    have0 = len(have)

    def gap(pending=0):
        # same rule as _recover: rows an early-exited leaf skipped on purpose are credited only while unseen
        found = len(have) + pending
        return size - found - max(0, skipped - (found - have0))

    def retry():
        """One retry of a search request, only while the node's allowance and the scan budget both cover it."""
        nonlocal spent
        if allowance - spent < 1 or not _charge_retry(rep):
            return False
        spent += 1
        return True

    while not rep.get("blocked") and gap() > tol and idle < WORKDAY_SEARCH_IDLE_TERMS and allowance - spent >= 1:
        words = collections.Counter()
        for k in have:
            title = (merged[k].get("title") or "").lower()
            for w in set(re.findall(r"[a-z]{%d,}" % WORKDAY_SEARCH_TERM_MIN_LEN, title)):
                if w not in WORKDAY_SEARCH_STOPWORDS and w not in tried:
                    words[w] += 1
        if not words:
            break
        term = words.most_common(1)[0][0]
        tried.add(term)
        new, off, ended = {}, 0, False
        try:
            while off < WORKDAY_RESULT_CAP and not ended and allowance - spent >= 1 and gap(len(new)) > tol:
                chunk = min(WORKDAY_SEARCH_CHUNK_PAGES, int(allowance - spent))   # never past the allowance
                _budget_add(rep, chunk)
                rows = fresh = 0
                end = min(off + chunk * WORKDAY_PAGE_SIZE, WORKDAY_RESULT_CAP)
                for o in range(off, end, WORKDAY_PAGE_SIZE):
                    if allowance - spent < 1:   # retries used up what was left of the chunk
                        break
                    spent += 1   # only requests actually sent are charged (retry() adds its own)
                    data = _post(url, applied, o, company, rep, text=term, before_retry=retry)
                    jobs = (data or {}).get("jobPostings") or []
                    if not jobs:
                        ended = True
                        break
                    for p in jobs:
                        rows += 1
                        k = _job_key(p)
                        if not p.get("title"):
                            # kept so a later titled copy can replace it; a copy already titled counts
                            p = merged.setdefault(k, p)
                        if p.get("title") and k not in have and k not in new:
                            new[k] = p
                            fresh += 1
                    if gap(len(new)) <= tol:
                        break
                off = end
                if rows == 0 or fresh / rows < WORKDAY_SEARCH_MIN_NEW:
                    break
        finally:   # rows already found survive a budget stop
            have |= set(new)
            for k, p in new.items():
                _merge_row(merged, k, p)
            if new:
                leaf_got.append((dict(applied), set(new)))
        idle = 0 if new else idle + 1
        logger.info("workday search: %r term=%r new=%d unique_now=%d gap=%d applied=%s",
                    company, term, len(new), len(have), size - len(have), applied)


def _recover(url, nodes, merged, leaf_got, company, rep):
    """
    Close gaps between a split node's promised count and the unique jobs its leaves returned, deepest
    node first: within tolerance = done; else a direct window (the node's first WORKDAY_RESULT_CAP jobs,
    only for nodes up to WORKDAY_WINDOW_MAX x the cap); else searchText. A node still short is recorded
    in rep["partial"] and sets rep["capped"]. What an early-exited leaf inside the node skipped on purpose
    (rep["early_leaves"]: applied, K, got) is not counted as a gap; short sibling leaves still are.
    """
    early = rep.get("early_leaves") or []
    try:
        for node in sorted(nodes, key=lambda n: -n["depth"]):
            applied, size = node["applied"], node["K"]
            # jobs an early-exited leaf inside this node skipped on purpose: not a gap
            skipped = sum(max(0, k - got) for e, k, got in early
                          if all(e.get(a) == b for a, b in applied.items()))
            have = len(_node_union(applied, leaf_got, merged))
            have0 = have
            tol = max(WORKDAY_GAP_MIN, WORKDAY_GAP_TOL * size)
            if size - have - skipped <= tol:
                continue
            if size <= WORKDAY_WINDOW_MAX * WORKDAY_RESULT_CAP:
                page1 = (rep.get("node_page1") or {}).get(json.dumps(applied, sort_keys=True)) or {}
                window = _make_leaf(rep, dict(applied), min(size, WORKDAY_RESULT_CAP), page1.get("jobPostings"))
                try:
                    got = _fetch_leaf(url, window, company, rep)   # never gated
                except _OverBudget as e:
                    for k, p in (getattr(e, "got", None) or {}).items():
                        _merge_row(merged, k, p)   # rows fetched before the budget stop are kept
                    raise
                for k, p in got.items():
                    _merge_row(merged, k, p)
                leaf_got.append((dict(applied), set(got)))
                after = len(_node_union(applied, leaf_got, merged))
                logger.info("workday window: %r size=%d unique %d -> %d applied=%s", company, size, have, after, applied)
                have = after
            # rows the window found may be the very rows the early leaf skipped: credit only what is still unseen
            skipped0 = skipped
            skipped = max(0, skipped0 - (have - have0))
            if size - have - skipped > tol:
                _search_node(url, node, merged, leaf_got, tol, company, rep, skipped)
                have = len(_node_union(applied, leaf_got, merged))
                skipped = max(0, skipped0 - (have - have0))
            if size - have - skipped > tol:
                rep.setdefault("partial", []).append((applied, size, have))
                rep["capped"] = True
                logger.warning("workday recover: %r node %s still short after window+search: %d of %d",
                               company, applied, have, size)
    except _OverBudget as e:
        rep["failed"] = True
        logger.warning("workday recover: request budget used up (%s) — partial result", e)


def fetch_job_detail(job):
    """
    Fetch full job description from Workday detail endpoint.
    Called only for NEW jobs (Option C strategy).

    Confirmed URL pattern (tested against AT&T/wd1, State Street/wd1, Red Hat/wd5):
      GET https://{slug}.{wd}.myworkdayjobs.com/wday/cxs/{slug}/{path}{externalPath}
      e.g. https://att.wd1.myworkdayjobs.com/wday/cxs/att/ATTGeneral/job/Mesa-Arizona/..._R-107385

    Response: jobPostingInfo.jobDescription  — HTML, present in all tenants tested.
    Requires WORKDAY_HEADERS (plain User-Agent returns 406).

    Args:
        job: job dict from fetch_jobs() — must contain _slug, _wd, _path, _external_path

    Returns:
        Updated job dict with description filled.
    """
    slug          = job.get("_slug", "")
    wd            = job.get("_wd")          # None = missing; "" = bare host (valid)
    path          = job.get("_path", "")
    external_path = job.get("_external_path", "")
    site          = job.get("_site")

    if not all([slug, path, external_path]) or not wd_present(wd, site):
        return job

    # Build URL based on which Workday domain variant is used
    url = f"{workday_domain(slug, wd, site)}/wday/cxs/{slug}/{path}{external_path}"

    data = fetch_json(url, platform="workday", headers=WORKDAY_HEADERS)
    if not data:
        logger.warning(
            "workday.fetch_job_detail: API returned None/empty for %r | %s | url=%s",
            job.get("company"), job.get("job_id"), url,
        )
        # DF: request failed (non-JSON 200, throttle, etc.) — tell detail_worker
        # this is a retryable empty, not "nothing to enrich".
        job = dict(job)
        job["_detail_failed"] = True
        return job

    info = data.get("jobPostingInfo", {})
    if not info:
        logger.warning(
            "workday.fetch_job_detail: jobPostingInfo absent/empty for %r | %s "
            "| top-level keys=%s",
            job.get("company"), job.get("job_id"), list(data.keys()),
        )
        job = dict(job)
        job["_detail_failed"] = True
        return job

    job = dict(job)

    # ── Description ───────────────────────────────────────────────────────
    desc = info.get("jobDescription", "")
    if desc:
        job["description"] = clean_html(desc)

    # ── Country code (Tier 1) ─────────────────────────────────────────────
    # jobRequisitionLocation.country.alpha2Code is the authoritative signal.
    # Confirmed present across tenants: "US" (Gartner), "IN" (Capital One).
    # Stored as _country_code; job_monitor uses it before text parsing.
    # Falls back to "" — job_monitor then uses descriptor-embedded location.
    req_loc = info.get("jobRequisitionLocation") or {}
    alpha2  = ((req_loc.get("country") or {}).get("alpha2Code") or "").upper()
    job["_country_code"] = alpha2

    # ── Location (display + Tier 3 fallback) ─────────────────────────────
    # _build_detail_location() embeds country.descriptor into the location
    # string for non-US jobs (e.g. "Bangalore, In" → "Bangalore, In, India")
    # so is_us_location() has the full country name if alpha2Code is absent.
    location = _build_detail_location(info)
    if location:
        job["location"] = location

    # ── Debug: log when both location and _country_code are absent ────────
    # This reveals ATCI / non-standard Workday job structures where the detail
    # API uses different field names than the parser expects.
    if not alpha2 and not location:
        logger.debug(
            "workday.fetch_job_detail: no location/alpha2 extracted for %r | %s "
            "| jobPostingInfo keys=%s | jobRequisitionLocation=%r "
            "| info.location=%r | info.country=%r",
            job.get("company"), job.get("job_id"),
            list(info.keys()),
            info.get("jobRequisitionLocation"),
            info.get("location"),
            info.get("country"),
        )

    return job


def _build_detail_location(info):
    """
    Build a clean location string from jobPostingInfo.

    Primary:   info["location"]             e.g. "Irving, TX"
    Additional: info["additionalLocations"] — two known formats:
      • Human-readable: "Fort Myers, FL"              (Gartner, most tenants)
      • Internal code:  "USA:AZ:Gilbert:addr:RET/RET" (AT&T)
    Country fallback: info["country"]["descriptor"]    e.g. "United States of America"
      Appended to primary when primary has no country context (e.g. bare "London").

    Returns semicolon-joined string of all unique locations, or "" if nothing found.
    """
    primary    = (info.get("location") or "").strip()
    additional = info.get("additionalLocations") or []
    country    = ((info.get("country") or {}).get("descriptor") or "").strip()

    parts = []

    if primary:
        # Append country descriptor when the country is non-US and not already
        # present in the primary string, e.g.:
        #   "London"       → "London, United Kingdom"
        #   "London, ENG"  → "London, ENG, United Kingdom"
        # Skip when country is US (state code already disambiguates) or when
        # the descriptor is already contained in the primary string.
        if country and "united states" not in country.lower() \
                and country.lower() not in primary.lower():
            primary = f"{primary}, {country}"
        parts.append(primary)

    for loc in additional:
        parsed = _parse_additional_location(loc)
        if parsed:
            # Append country descriptor to additional locations too, so
            # human-readable non-US entries like "Berlin" become "Berlin, Germany"
            # and is_us_location() can reject them via Signal 4.
            if country and "united states" not in country.lower() \
                    and country.lower() not in parsed.lower():
                parsed = f"{parsed}, {country}"
            if parsed not in parts:
                parts.append(parsed)

    return "; ".join(parts)


def _parse_additional_location(loc):
    """
    Parse one additionalLocations entry — handles two formats seen in the wild:

    Human-readable (Gartner, most tenants):
        "Fort Myers, FL"  →  "Fort Myers, FL"

    Internal code (AT&T):
        "USA:AZ:Gilbert:2224 E Williams Field Rd:RET/RET"
        →  parts[0]=country code, [1]=state, [2]=city
        →  US:  "Gilbert, AZ"          (country omitted — state code is enough for S3)
        →  Non-US: "London, ENG, GBR"  (country code kept in UPPERCASE so Signal 2
                                         alpha-3 gate recognises it as non-US)

    Returns clean string or "" if unparseable.
    """
    if not loc:
        return ""
    loc = loc.strip()
    parts = loc.split(":")
    if len(parts) >= 3:
        country_code = parts[0].strip().upper()
        state        = parts[1].strip()
        city         = parts[2].strip()
        if country_code == "USA":
            # US — state code alone is sufficient for Signal 3 detection
            if city and state:
                return f"{city}, {state}"
            return city or ""
        else:
            # Non-US — include country code in UPPERCASE so Signal 2 can
            # recognise the alpha-3 code (e.g. "GBR") and return False
            if city and state:
                return f"{city}, {state}, {country_code}"
            elif city:
                return f"{city}, {country_code}"
            return country_code or ""
    # Human-readable — use as-is
    return loc


def _job_req_id(job, external_path, job_url):
    """
    Requisition id of a listing (design P1). bulletFields[0] is the id on most tenants but not on Motorola
    (location), Intel ("Spotlight Job") or Wisconsin (no id bullet), where many jobs then share one id.
    The id is the text after the last "_" of the last externalPath segment. Use the bullet equal to it, or
    to it minus a trailing "-<digits>" (Intel JR0289044-1 -> JR0289044); else the token itself (Wisconsin).
    Without a usable path token: bulletFields[0] if it is shaped like an id (no spaces, has a digit);
    else the URL's own posting id (a numeric last segment or "_R-..."), so legacy ".../1" and ".../2"
    sharing a text bullet stay apart; else bulletFields[0] (shared landing URL).
    """
    bullets = [b for b in (job.get("bulletFields") or []) if isinstance(b, str)]
    segment = (external_path or "").rstrip("/").rsplit("/", 1)[-1]
    # only a posting path ("/job/...") carries a requisition token; a site/landing path does not
    token = segment.rsplit("_", 1)[-1].strip() if "_" in segment and "/job/" in (external_path or "") else ""
    if token:
        base = re.sub(r"-\d+$", "", token)
        for cand in (token, base):
            if cand in bullets:
                return cand
        return token
    first = bullets[0] if bullets else ""
    if re.fullmatch(r"\S*\d\S*", first):
        return first
    try:
        last = urlparse(job_url).path.rstrip("/").rsplit("/", 1)[-1]
    except ValueError:   # malformed URL: no posting id to read from its path
        last = ""
    if last.isdigit():
        return last
    m = re.search(r"_((?:JR|R)-?\d+(?:-\d+)?)", job_url)
    return m.group(1) if m else first


def _normalize(job, company, domain, path, slug_info=None):
    """
    Normalize Workday job to standard format.

    externalPath from API: /job/Reynoldsburg-Ohio/Sr-B2B-Sales_R-104046
    Full URL:  https://{domain}/{path}/job/Reynoldsburg-Ohio/Sr-B2B-Sales_R-104046
    e.g.:      https://att.wd1.myworkdayjobs.com/ATTGeneral/job/Reynoldsburg-Ohio/...
    """
    posted_at = None
    posted = job.get("postedOn", "")
    if posted:
        try:
            posted_lower = posted.lower().strip()
            # Check human-readable strings FIRST before ISO format
            # to avoid false matches (e.g. "T" in "Posted Today")
            if "today" in posted_lower:
                posted_at = datetime.utcnow()
            elif "yesterday" in posted_lower:
                from datetime import timedelta
                posted_at = datetime.utcnow() - timedelta(days=1)
            elif "30+" in posted:
                # W30: "Posted 30+ Days Ago" is only a lower bound.  The listing gives no
                # real date, so 30 days is the floor (oldest value the freshness filter can
                # act on), not the true age; ordering among such jobs is unknown.
                from datetime import timedelta
                posted_at = datetime.utcnow() - timedelta(days=30)
            elif "day" in posted_lower:
                import re as _re
                from datetime import timedelta
                m = _re.search(r"(\d+)", posted)
                if m:
                    posted_at = datetime.utcnow() - timedelta(days=int(m.group(1)))
            elif "/" in posted:
                posted_at = datetime.strptime(posted, "%m/%d/%Y")
            elif "T" in posted or posted.endswith("Z"):
                posted_at = datetime.fromisoformat(posted.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    # Support both externalPath (real API) and externalUrl (tests/legacy)
    # externalPath: /job/Reynoldsburg-Ohio/Sr-B2B-Sales_R-104046  (relative, needs domain+path prefix)
    # externalUrl:  https://jpmorgan.wd5.myworkdayjobs.com/1       (absolute, use as-is)
    external_path = (job.get("externalPath") or "").strip()
    external_url  = (job.get("externalUrl") or "").strip()

    # When externalPath is absent but externalUrl is a job-specific URL (not just
    # the careers homepage), derive external_path from it so fetch_job_detail()
    # can build the CXS detail endpoint.  This handles tenants that omit the
    # relative-path field but return an absolute job URL.
    _si = slug_info or {}
    _is_site = _si.get("site") == "myworkdaysite"
    if not external_path and external_url and external_url.startswith("http"):
        try:
            # e.g. "/en-US/NVIDIAExternalCareerSite/job/Loc/Title_R-1"
            _parsed_path = urlparse(external_url).path
        except ValueError:   # malformed URL (e.g. "http://[..."): treated as no URL at all
            external_url = _parsed_path = ""
        if _is_site:   # drop "/recruiting/{tenant}" first: a site named like the tenant must not match it
            _parsed_path = re.sub(r"^.*?/recruiting/[^/]+", "", _parsed_path)
        _path_prefix = "/" + path.strip("/")          # e.g. "/NVIDIAExternalCareerSite"
        # Use find so locale-prefixed URLs (e.g. /en-US/NVIDIAExternalCareerSite/...)
        # are handled the same as unprefixed ones (/NVIDIAExternalCareerSite/...).
        _path_idx = _parsed_path.find(_path_prefix + "/")
        if _path_idx != -1:
            external_path = _parsed_path[_path_idx + len(_path_prefix):]  # e.g. "/job/Loc/Title_R-1"

    # public career site root; myworkdaysite pages live under /recruiting/{tenant}/ (the CXS API does not)
    site_root = domain.rstrip("/")
    if _is_site:
        site_root += "/recruiting/" + _si.get("slug", "")
    site_root += "/" + path.strip("/")

    if external_path:
        # Relative path from real Workday API — prepend the career site root
        job_url = site_root + "/" + external_path.lstrip("/")
    elif external_url and external_url.startswith("http"):
        # Absolute URL provided directly (legacy/test data)
        job_url = external_url
    else:
        # Neither field present — fall back to the career site root
        # so we never silently drop a job with a valid title
        job_url = site_root

    _bullet_id = _job_req_id(job, external_path, job_url)

    return {
        "company":        company,
        "title":          job.get("title", ""),
        "job_url":        job_url,
        "location":       job.get("locationsText", ""),
        "posted_at":      posted_at,
        "description":    "",              # filled by fetch_job_detail
        "ats":            "workday",
        "job_id":         _bullet_id,
        # Detail-fetch fields — used by fetch_job_detail(), not stored in DB
        "_external_path": external_path,
        "_slug":          _si.get("slug", ""),
        "_wd":            _si.get("wd"),   # "" = bare host (valid); None = key missing
        "_path":          path,
        "_site":          _si.get("site"),
    }