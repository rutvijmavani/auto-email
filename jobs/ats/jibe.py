# jobs/ats/jibe.py — Jibe/iCIMS JSON API client
#
# Used by companies that migrated from *.icims.com subdomains
# to custom career pages powered by iCIMS's Jibe platform.
#
# API endpoint: https://{careers_domain}/api/jobs
# Pagination:   ?limit=100&offset=N
# Date field:   posted_date — RELIABLE (ISO 8601)
# job_id:       req_id field
#
# Known companies:
#   Rivian:  careers.rivian.com
#
# Detection: career page HTML contains app.jibecdn.com script tag

import time
from datetime import datetime, timezone
from jobs.ats.base import fetch_json
from logger import get_logger

logger = get_logger(__name__)

PAGE_SIZE = 100
MAX_PAGES = 20  # safety cap — 20 × 100 = 2000 jobs max

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
        return []

    base_url = f"https://{slug}/api/jobs"
    all_jobs = []
    offset   = 0

    for page in range(MAX_PAGES):
        url  = f"{base_url}?limit={PAGE_SIZE}&offset={offset}"
        data = fetch_json(url, headers=HEADERS, platform="jibe")

        if not data:
            logger.debug("Jibe fetch: no data for %r page=%d", company, page)
            break

        jobs_page = data.get("jobs", [])
        if not jobs_page:
            break  # end of results

        for item in jobs_page:
            job = item.get("data", {})
            if not job:
                continue

            normalized = _normalize(job, company, slug)
            if normalized:
                all_jobs.append(normalized)

        logger.debug("Jibe page %d: %d jobs for %r (total so far: %d)",
                     page, len(jobs_page), company, len(all_jobs))

        total = data.get("totalCount", 0)
        offset += PAGE_SIZE
        if offset >= total:
            break

        time.sleep(0.3)

    logger.info("Jibe fetch complete: %d jobs for %r", len(all_jobs), company)
    return all_jobs


def _normalize(job, company, slug):
    """Normalize a Jibe job dict to pipeline standard format."""
    title = (job.get("title") or "").strip()
    if not title:
        return None

    req_id  = str(job.get("req_id") or job.get("slug") or "")
    job_url = _build_job_url(job, slug, req_id)
    if not job_url:
        return None

    # Location — prefer full_location, fall back to city+state
    location = (job.get("full_location") or "").strip()
    if not location:
        city    = job.get("city") or ""
        state   = job.get("state") or ""
        country = job.get("country") or ""
        parts   = [p for p in [city, state, country] if p]
        location = ", ".join(parts)

    posted_at = _parse_date(job.get("posted_date"))

    description = _build_description(job)

    return {
        "company":     company,
        "title":       title,
        "job_url":     job_url,
        "job_id":      req_id,
        "location":    location,
        "posted_at":   posted_at,
        "description": description,
        "ats":         "jibe",
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
        # Normalize trailing "Z" to "+00:00" for older Python versions
        normalized = date_str
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        # Handle +0000 format (no colon in offset)
        elif len(normalized) > 19 and normalized[-5] in ("+", "-") and ":" not in normalized[-5:]:
            normalized = normalized[:-2] + ":" + normalized[-2:]
        return datetime.fromisoformat(normalized)
    except (ValueError, AttributeError):
        try:
            # Fallback: use normalized value (not date_str[:19]) to preserve timezone
            normalized_fallback = normalized
            if normalized_fallback.endswith("Z"):
                normalized_fallback = normalized_fallback[:-1] + "+00:00"
            return datetime.strptime(normalized_fallback[:19], "%Y-%m-%dT%H:%M:%S").replace(
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