"""
scripts/audit_fetch_health.py — READ-ONLY audit: which monitored companies are silently under-fetched?

  python -m scripts.audit_fetch_health [--out data/audit_fetch_health.csv]
                                       [--platform workday] [--company "Deloitte"] [--limit 50]

For every monitorable company (prospective_companies + company_ats, same list the scheduler uses) this
re-fetches the full job list through the SAME ats_module.fetch_jobs the workers use, then compares what
came back with what is stored in job_postings. Nothing is written: no job rows, no slug updates (a slug
the module mutates in memory is only reported), no Redis state.

Flags (thresholds are AUDIT_* in config.py):
  FETCH_ERROR           fetch_jobs raised
  ZERO_JOBS             fetch returned nothing at all
  ZERO_BUT_STORED       fetch returned nothing although open jobs are stored
  PARTIAL_FETCH         the module returned a partial result (IncompleteSearchError)
  DROPPED_NO_ID         >= AUDIT_DROPPED_RATIO of raw jobs lack job_url/job_id (selector matches non-jobs, or id extraction broken)
  DUPLICATE_URLS        raw list contains repeated job_urls (a page was fetched twice)
  FULL_PAGE_SUSPECT     raw count equals one full page (custom pagination page_size, or a count in AUDIT_ROUND_COUNTS)
  CUSTOM_PAGINATION_NONE  custom ATS stored with pagination type 'none' while returning jobs
  FEWER_THAN_STORED     title-matched now < AUDIT_STORED_FETCHED_RATIO of stored open jobs (under-fetch OR stale rows)
  BIG_EMPLOYER_FEW_JOBS >= AUDIT_BIG_EMPLOYER_PETITIONS petitions but < AUDIT_BIG_EMPLOYER_MAX_RAW raw jobs
  NO_NEW_JOBS           large board (>= AUDIT_NO_NEW_MIN_RAW raw) with no job first seen in AUDIT_NO_NEW_DAYS days
  SLUG_MUTATED          the module changed slug_info in memory (informational)

A flag is a lead, not a verdict: open the company's careers page and compare the job count by eye.
"""
import argparse
import copy
import csv
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    AUDIT_BIG_EMPLOYER_MAX_RAW,
    AUDIT_BIG_EMPLOYER_PETITIONS,
    AUDIT_CEILING_RETRIES,
    AUDIT_CEILING_WAIT_S,
    AUDIT_DROPPED_RATIO,
    AUDIT_NO_NEW_DAYS,
    AUDIT_NO_NEW_MIN_RAW,
    AUDIT_ROUND_COUNTS,
    AUDIT_STORED_FETCHED_RATIO,
    AUDIT_STORED_MIN_OPEN,
    AUDIT_WORKERS,
)
from db.connection import get_conn
from db.job_monitor import get_monitorable_companies
from jobs.ats.avature import IncompleteSearchError
from jobs.ats.registry import get_config, parse_slug
from jobs.ats_detector import get_ats_module
from jobs.job_filter import filter_jobs, filter_jobs_title_only
from logger import get_logger, init_logging
from workers.http_client import CeilingExceeded

log = get_logger(__name__)

# Rows with these statuses are no longer "open" (filled / aged out).
_CLOSED_STATUSES = ("filled", "expired")

_COLUMNS = [
    "company", "display_name", "platform", "petitions", "flags", "raw", "valid", "dropped",
    "dropped_ratio", "title_matched", "stored_open", "stored_total", "stored_new_recent",
    "pagination", "seconds", "error",
]


def _round_counts() -> set:
    return {int(x) for x in AUDIT_ROUND_COUNTS.split(",") if x.strip().isdigit()}


def _load_stored_counts() -> dict:
    """{job_postings.company: (open, total, first_seen within AUDIT_NO_NEW_DAYS)} — one aggregate query."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT company,
                   COUNT(*) FILTER (WHERE status <> ALL(?))                               AS open_cnt,
                   COUNT(*)                                                               AS total_cnt,
                   COUNT(*) FILTER (WHERE first_seen >= CURRENT_DATE - ?::int)            AS recent_cnt
            FROM job_postings
            GROUP BY company
            """,
            (list(_CLOSED_STATUSES), AUDIT_NO_NEW_DAYS),
        ).fetchall()
        return {r["company"]: (r["open_cnt"], r["total_cnt"], r["recent_cnt"]) for r in rows}
    finally:
        conn.close()


def _fetch(ats_module, slug_info, company):
    """fetch_jobs with ceiling retries. Returns (raw_jobs, partial: bool)."""
    for attempt in range(AUDIT_CEILING_RETRIES + 1):
        try:
            return ats_module.fetch_jobs(slug_info, company), False
        except CeilingExceeded:
            if attempt == AUDIT_CEILING_RETRIES:
                raise
            time.sleep(AUDIT_CEILING_WAIT_S)
        except IncompleteSearchError as exc:
            if not exc.stubs:
                raise
            return exc.stubs, True
    return [], False  # unreachable


def _audit_one(row: dict, stored: dict, round_counts: set) -> dict:
    company = row["company"]
    display = row.get("company_name") or company
    platform = row.get("ats_platform")
    out = {
        "company": company, "display_name": display, "platform": platform,
        "petitions": row.get("petition_count") or 0, "flags": [], "raw": 0, "valid": 0, "dropped": 0,
        "dropped_ratio": 0.0, "title_matched": 0, "pagination": "", "seconds": 0.0, "error": "",
    }
    open_cnt, total_cnt, recent_cnt = stored.get(display, (0, 0, 0))
    out.update(stored_open=open_cnt, stored_total=total_cnt, stored_new_recent=recent_cnt)

    started = time.monotonic()
    try:
        config = get_config(platform)
        ats_module = get_ats_module(platform)
        if not ats_module:
            raise RuntimeError(f"no ATS module for platform={platform!r}")
        slug_info = parse_slug(platform, row["ats_slug"], config)
        before = copy.deepcopy(slug_info) if isinstance(slug_info, dict) else None
        raw_jobs, partial = _fetch(ats_module, slug_info, display)
    except Exception as exc:  # noqa: BLE001 — an audit must keep going and report every failure
        out["flags"].append("FETCH_ERROR")
        out["error"] = f"{type(exc).__name__}: {exc}"[:300]
        out["seconds"] = round(time.monotonic() - started, 1)
        return out
    out["seconds"] = round(time.monotonic() - started, 1)

    flags = out["flags"]
    if partial:
        flags.append("PARTIAL_FETCH")
    if before is not None and slug_info != before:
        flags.append("SLUG_MUTATED")

    valid = [j for j in raw_jobs if j.get("job_url") and j.get("job_id")]
    raw, nvalid = len(raw_jobs), len(valid)
    out.update(raw=raw, valid=nvalid, dropped=raw - nvalid, dropped_ratio=round((raw - nvalid) / raw, 2) if raw else 0.0)

    title_filter = filter_jobs_title_only if config.get("listing_filter", "full") == "title_only" else filter_jobs
    out["title_matched"] = len(title_filter(valid))

    pagination = slug_info.get("pagination") if isinstance(slug_info, dict) else None
    page_size = None
    if isinstance(pagination, dict):
        out["pagination"] = f"{pagination.get('type')}:{pagination.get('page_size')}"
        page_size = pagination.get("page_size")

    if raw == 0:
        flags.append("ZERO_BUT_STORED" if open_cnt else "ZERO_JOBS")
    else:
        if out["dropped_ratio"] >= AUDIT_DROPPED_RATIO:
            flags.append("DROPPED_NO_ID")
        if len({j.get("job_url") for j in valid}) < nvalid:
            flags.append("DUPLICATE_URLS")
        if raw in round_counts or (page_size and raw == page_size):
            flags.append("FULL_PAGE_SUSPECT")
        if platform == "custom" and isinstance(pagination, dict) and pagination.get("type") == "none":
            flags.append("CUSTOM_PAGINATION_NONE")
        if raw < AUDIT_BIG_EMPLOYER_MAX_RAW and out["petitions"] >= AUDIT_BIG_EMPLOYER_PETITIONS:
            flags.append("BIG_EMPLOYER_FEW_JOBS")
        if raw >= AUDIT_NO_NEW_MIN_RAW and recent_cnt == 0:
            flags.append("NO_NEW_JOBS")
        if open_cnt >= AUDIT_STORED_MIN_OPEN and out["title_matched"] < open_cnt * AUDIT_STORED_FETCHED_RATIO:
            flags.append("FEWER_THAN_STORED")
    return out


def main(argv=None) -> int:
    init_logging("audit_fetch_health")
    parser = argparse.ArgumentParser(description="Read-only audit of silently under-fetched companies")
    parser.add_argument("--out", default="data/audit_fetch_health.csv")
    parser.add_argument("--platform", help="only this ATS platform")
    parser.add_argument("--company", help="only companies whose key or display name contains this text (case-insensitive)")
    parser.add_argument("--limit", type=int, help="audit at most N companies (highest H1B petition count first)")
    parser.add_argument("--workers", type=int, default=AUDIT_WORKERS)
    args = parser.parse_args(argv)

    rows = get_monitorable_companies()
    if args.platform:
        rows = [r for r in rows if r.get("ats_platform") == args.platform]
    if args.company:
        needle = args.company.lower()
        rows = [r for r in rows if needle in (r["company"] or "").lower() or needle in (r.get("company_name") or "").lower()]
    rows.sort(key=lambda r: r.get("petition_count") or 0, reverse=True)
    if args.limit:
        rows = rows[: args.limit]
    if not rows:
        print("No monitorable companies match.")
        return 0

    stored = _load_stored_counts()
    round_counts = _round_counts()
    print(f"Auditing {len(rows)} companies with {args.workers} workers (read-only)...")

    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(_audit_one, r, stored, round_counts): r for r in rows}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            results.append(res)
            if i % 25 == 0 or i == len(rows):
                print(f"  {i}/{len(rows)} done")

    results.sort(key=lambda r: (-len(r["flags"]), -r["petitions"]))
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for res in results:
            writer.writerow({**res, "flags": " ".join(res["flags"])})

    flagged = [r for r in results if r["flags"]]
    by_flag, by_platform = Counter(), defaultdict(Counter)
    for res in flagged:
        for f in res["flags"]:
            by_flag[f] += 1
            by_platform[res["platform"]][f] += 1
    print(f"\n{len(flagged)} of {len(results)} companies flagged. CSV: {args.out}\n")
    print("Flag counts:")
    for flag, n in by_flag.most_common():
        print(f"  {flag:24s} {n}")
    print("\nBy platform (flagged companies):")
    for plat, counter in sorted(by_platform.items(), key=lambda kv: -sum(kv[1].values())):
        print(f"  {plat or '?':14s} " + ", ".join(f"{f}={n}" for f, n in counter.most_common()))
    print("\nTop flagged (most flags, then most H1B petitions):")
    for res in flagged[:30]:
        print(f"  {res['display_name'][:34]:34s} {res['platform'] or '?':12s} raw={res['raw']:<5d} "
              f"matched={res['title_matched']:<4d} stored_open={res['stored_open']:<4d} {' '.join(res['flags'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
