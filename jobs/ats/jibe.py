# jobs/ats/jibe.py — Jibe/iCIMS JSON API client
#
# Used by companies that migrated from *.icims.com subdomains
# to custom career pages powered by iCIMS's Jibe platform.
#
# API endpoint: https://{careers_domain}/api/jobs
# Pagination:   ?limit=100&page=N (1-based). `offset` is IGNORED by the API — it returns page 1
#               every time (verified live 2026-10-14: Docusign 100/100/68 = totalCount 268).
# Total:        totalCount (also count) in every response
# Date field:   posted_date — RELIABLE (ISO 8601)
# job_id:       req_id field
#
# Known companies:
#   Rivian:  careers.rivian.com
#
# Detection: career page HTML contains app.jibecdn.com script tag

import time
from datetime import datetime, timezone
from config import JIBE_MAX_PAGES
from jobs.ats.base import fetch_json
from jobs.ats.fetch_report import (
    FetchReport, STOP_ALL_PAGES, STOP_EMPTY_PAGE, STOP_NO_CONFIG, STOP_PAGE_CAP, STOP_REPEAT_PAGE,
    STOP_REQUEST_FAILED, TOTAL_ATS, attach, failed,
)
from logger import get_logger

logger = get_logger(__name__)

PAGE_SIZE = 100

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_jobs(slug, company):
    """
    Fetch all jobs for a Jibe-powered career page.

    Args:
        slug:    careers domain (e.g. "careers.rivian.com")
        company: company name (e.g. "Rivian")

    Returns:
        List of normalized job dicts.
    """
    if not slug:
        return failed(STOP_NO_CONFIG, "empty slug")

    base_url = f"https://{slug}/api/jobs"
    all_jobs = []
    seen_ids = set()   # distinct raw ids across pages — the API ignores unknown params and repeats page 1
    total    = None
    stop     = STOP_ALL_PAGES
    pages    = 0

    for page in range(1, JIBE_MAX_PAGES + 1):
        url  = f"{base_url}?limit={PAGE_SIZE}&page={page}"
        data = fetch_json(url, headers=HEADERS)
        pages += 1

        if not data:
            logger.warning("Jibe fetch: no data for %r page=%d — partial result (%d jobs)",
                           company, page, len(all_jobs))
            stop = STOP_REQUEST_FAILED
            break

        jobs_page = data.get("jobs", [])
        if not jobs_page:
            stop = STOP_EMPTY_PAGE
            break  # end of results

        new_ids = 0
        for item in jobs_page:
            job = item.get("data", {})
            if not job:
                continue

            raw_id = str(job.get("req_id") or job.get("slug") or "")
            if raw_id:
                if raw_id in seen_ids:
                    continue
                seen_ids.add(raw_id)
            new_ids += 1

            normalized = _normalize(job, company, slug)
            if normalized:
                all_jobs.append(normalized)

        logger.debug("Jibe page %d: %d jobs (%d new) for %r (total so far: %d)",
                     page, len(jobs_page), new_ids, company, len(all_jobs))

        if new_ids == 0:
            logger.warning("Jibe fetch: page %d of %r added no new jobs (repeated page) — stopping "
                           "with %d jobs", page, company, len(all_jobs))
            stop = STOP_REPEAT_PAGE
            break

        total = data.get("totalCount")
        if isinstance(total, int) and total > 0 and len(seen_ids) >= total:
            break
        if len(jobs_page) < PAGE_SIZE:
            stop = STOP_EMPTY_PAGE   # a short page is the server's last
            break

        time.sleep(0.3)
    else:
        stop = STOP_PAGE_CAP
        logger.warning("Jibe fetch: hit JIBE_MAX_PAGES=%d for %r — result truncated at %d jobs",
                       JIBE_MAX_PAGES, company, len(all_jobs))

    if isinstance(total, int) and total > len(seen_ids):
        logger.warning("Jibe fetch: %r reports %d jobs but only %d distinct fetched",
                       company, total, len(seen_ids))
    logger.info("Jibe fetch complete: %d jobs for %r", len(all_jobs), company)
    has_total = isinstance(total, int) and total > 0
    return attach(all_jobs, FetchReport(
        stop_reason      = stop,
        pages            = pages,
        fetched_distinct = len(seen_ids) if seen_ids else len(all_jobs),
        unusable         = max(len(seen_ids) - len(all_jobs), 0),
        reported_total   = total if has_total else None,
        total_source     = TOTAL_ATS if has_total else None,
        repeated_pages   = 1 if stop == STOP_REPEAT_PAGE else 0,
    ))


def _normalize(job, company, slug):
    """Normalize a Jibe job dict to pipeline standard format."""
    title = (job.get("title") or "").strip()
    if not title:
        return None

    req_id  = str(job.get("req_id") or job.get("slug") or "")
    job_url = _build_job_url(job, slug, req_id)
    if not job_url:
        return None

    # Location — prefer full_location, fall back to city+state+country
    location = (job.get("full_location") or "").strip()
    if not location:
        city    = job.get("city") or ""
        state   = job.get("state") or ""
        country = job.get("country") or ""
        parts   = [p for p in [city, state, country] if p]
        location = ", ".join(parts)

    # Country code (Tier 1 gate)
    # Jibe API provides country_code as ISO alpha-2 ("US", "IN", "GB" …)
    # on every job — far more reliable than text-parsing the location string.
    country_code = (job.get("country_code") or "").strip().upper()

    posted_at = _parse_date(job.get("posted_date"))

    description = _build_description(job)

    return {
        "company":       company,
        "title":         title,
        "job_url":       job_url,
        "job_id":        req_id,
        "location":      location,
        "posted_at":     posted_at,
        "description":   description,
        "ats":           "jibe",
        "_country_code": country_code,
    }


def _build_job_url(job, slug, req_id):
    """
    Build canonical job URL.
    Jibe uses apply_url which points to the iCIMS apply page —
    we prefer the careers domain URL for consistency.
    """
    # Try apply_url first — it's the most reliable
    apply_url = (job.get("apply_url") or "").strip()
    if apply_url and apply_url.startswith("http"):
        # Strip login suffix — use the job page not the apply page
        apply_url = apply_url.replace("/login", "/job")
        return apply_url

    # Fallback: build from slug and req_id
    if req_id:
        return f"https://{slug}/careers-home/jobs/{req_id}"

    return None


def _parse_date(date_str):
    """Parse ISO 8601 date string to datetime or None."""
    if not date_str:
        return None
    try:
        # Handle +0000 format (no colon in offset)
        normalized = date_str
        if len(date_str) > 19 and date_str[-5] in ("+", "-") and ":" not in date_str[-5:]:
            normalized = date_str[:-2] + ":" + date_str[-2:]
        return datetime.fromisoformat(normalized)
    except (ValueError, AttributeError):
        try:
            return datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, AttributeError):
            return None


def _build_description(job):
    """
    Build description from available Jibe fields.
    Combines description, responsibilities, qualifications.
    """
    parts = []
    for field in ("description", "responsibilities", "qualifications"):
        val = (job.get(field) or "").strip()
        if val:
            parts.append(val)
    return "\n\n".join(parts)[:5000] if parts else ""