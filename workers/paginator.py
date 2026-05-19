"""
workers/paginator.py — Smart early exit pagination for Tier 1 listing scans.

Implements the overlap-ratio early exit algorithm from Section 4 of the ATS
fetch strategy doc and Section 11 of the adaptive polling architecture doc.

SORTED platforms (Greenhouse, Lever, Ashby, SmartRecruiters, Eightfold,
Oracle HCM): results newest-first → stop at 80% overlap across 2 consecutive
pages (PAGINATOR_OVERLAP_THRESHOLD / PAGINATOR_CONFIRM_PAGES).

NON-SORTED platforms (Workday, iCIMS, Taleo, ADP, etc.): no order guarantee
→ stop at 100% overlap on any single page (simpler but still efficient).

Full scan of NON-SORTED platforms: NEVER exits early — must go all pages.

─── Phase 7 integration note ────────────────────────────────────────────────

Phase 7 will refactor individual ATS fetch_jobs() methods to support an
optional per-page callback so the paginator can halt mid-scan. Until then,
should_continue_paginating() is used AFTER fetch_jobs() returns all pages
to calculate what WOULD have been the early exit point for observability
and for platforms whose fetch_jobs() already exposes page-level control.

Usage (post Phase 7, per-page loop):

    # Pass a callable that checks bloom:fullscan:{company} for early exit.
    # The bloom filter contains ALL job IDs from the last full scan cycle,
    # making the 80% overlap threshold meaningful (see Section 11).
    def bloom_check(job_id: str) -> bool:
        try:
            return bool(r.execute_command("BF.EXISTS", f"bloom:fullscan:{company}", job_id))
        except Exception:
            return bool(r.sismember(f"bloom:fallback:{company}", job_id))

    overlap_pages = 0
    page = 0
    all_jobs = []

    while True:
        page_jobs = ats_module.fetch_listing_page(slug_info, company, page)
        if not page_jobs:
            break

        should_go_on, overlap_pages = should_continue_paginating(
            page_jobs, bloom_check, overlap_pages,
            sorted_by_recency=config.get("sorted_by_recency", False),
        )

        all_jobs.extend(page_jobs)
        page += 1

        if not should_go_on:
            break

Usage (current, post-fetch scan):

    # Call after fetch_jobs() returns the full listing to compute
    # at which page the early exit WOULD have triggered.
    depth = estimate_scan_depth(total_fetched, new_found, early_exit_triggered)
"""

from config import (
    PAGINATOR_OVERLAP_THRESHOLD,   # 0.80
    PAGINATOR_CONFIRM_PAGES,       # 2
    PAGINATOR_UNSORTED_CUTOFF_DAYS,
)


def should_continue_paginating(
    page_jobs: list,
    seen_ids,
    overlap_pages: int,
    sorted_by_recency: bool,
    id_key: str = "job_id",
) -> tuple:
    """
    Determine whether to continue fetching the next listing page.

    Args:
        page_jobs:          Jobs returned from the current listing page.
        seen_ids:           Either a set of known job IDs OR a callable
                            (job_id: str) -> bool that returns True if the
                            job is already known. Pass the bloom filter check
                            function (bloom:fullscan:{company}) for adaptive
                            early exit, or a plain set for backward compat.
        overlap_pages:      Consecutive high-overlap page count so far.
                            Pass 0 for the first page. Carry forward on each call.
        sorted_by_recency:  True  → SORTED platform (80%/2-page algorithm).
                            False → NON-SORTED (100%/1-page algorithm).
        id_key:             Dict key containing the job ID. Default 'job_id'.

    Returns:
        (should_continue: bool, updated_overlap_pages: int)

        should_continue = False means "stop paginating now".
        updated_overlap_pages should be passed as overlap_pages on the
        next call (it resets to 0 when new jobs are found).

    Examples:
        # SORTED — stop when 2 consecutive pages are 80%+ in bloom filter
        cont, pages = should_continue_paginating(page, bloom_check_fn, 0, True)

        # NON-SORTED — stop when a full page is 100% seen
        cont, pages = should_continue_paginating(page, bloom_check_fn, 0, False)
    """
    if not page_jobs:
        # Genuine empty page — end of results regardless of platform type
        return False, overlap_pages

    ids = [j.get(id_key) for j in page_jobs if j.get(id_key)]
    if not ids:
        # Page has no identifiable jobs → cannot decide, keep going
        return True, overlap_pages

    _is_seen = seen_ids if callable(seen_ids) else seen_ids.__contains__
    seen_count    = sum(1 for jid in ids if _is_seen(jid))
    overlap_ratio = seen_count / len(ids)

    if sorted_by_recency:
        # ── SORTED platform: 80% threshold, 2-page confirm ────────────────────
        # Jobs are newest-first, so a 80%+ overlap page means we are past the
        # "new job frontier". Require 2 consecutive pages to avoid false exits
        # caused by bumped/re-edited listings appearing near the top.
        if overlap_ratio >= PAGINATOR_OVERLAP_THRESHOLD:
            new_overlap_pages = overlap_pages + 1
            if new_overlap_pages >= PAGINATOR_CONFIRM_PAGES:
                # 2 consecutive high-overlap pages → stop
                return False, new_overlap_pages
            # First high-overlap page — keep going to confirm
            return True, new_overlap_pages
        else:
            # New jobs found → reset consecutive counter and keep going
            return True, 0

    else:
        # ── NON-SORTED platform: 100% overlap on 1 page → stop ───────────────
        # Jobs are unordered, so we cannot rely on seeing old-before-new.
        # We stop only when an entire page is fully known. This is less
        # precise (could miss jobs on future pages) but avoids fetching all
        # 200+ pages when most are pre-existing.
        #
        # For NON-SORTED adaptive polling, a time-based cutoff per the ATS
        # fetch strategy doc is the better heuristic — but the page-level
        # check here serves as a backstop.
        if overlap_ratio == 1.0:
            return False, overlap_pages + 1
        else:
            return True, 0


def would_have_exited_at(
    all_pages: list,
    seen_ids,
    sorted_by_recency: bool,
    id_key: str = "job_id",
) -> int:
    """
    Given all pages fetched, determine at which page the early exit WOULD
    have triggered.

    Useful for observability (Phase 7 prep): compares the wasted fetches
    of the current fetch_all approach against what the paginator would have
    done.

    Args:
        all_pages:          List of pages; each page is a list of job dicts.
        seen_ids:           Set of known job IDs OR callable (job_id) -> bool.
                            Pass the bloom filter check fn for accurate results.
        sorted_by_recency:  Platform sort order.
        id_key:             Job ID dict key.

    Returns:
        Page number (0-based) where exit would have occurred.
        Returns len(all_pages) if exit was never triggered (fully scanned).
    """
    overlap_pages = 0
    for page_num, page_jobs in enumerate(all_pages):
        cont, overlap_pages = should_continue_paginating(
            page_jobs, seen_ids, overlap_pages, sorted_by_recency, id_key,
        )
        if not cont:
            return page_num
    return len(all_pages)


def estimate_scan_depth(
    total_fetched: int,
    new_found: int,
    early_exit: bool,
) -> dict:
    """
    Summarise scan efficiency for logging and observability metrics.

    Args:
        total_fetched: Total jobs fetched across all pages.
        new_found:     Genuinely new job IDs detected.
        early_exit:    Whether the paginator triggered early exit.

    Returns:
        Dict with waste_ratio and efficiency stats.
    """
    waste_ratio = (total_fetched - new_found) / max(total_fetched, 1)
    return {
        "total_fetched": total_fetched,
        "new_found":     new_found,
        "wasted":        total_fetched - new_found,
        "waste_ratio":   round(waste_ratio, 3),
        "early_exit":    early_exit,
    }
