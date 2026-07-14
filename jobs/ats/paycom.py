# jobs/ats/paycom.py — Paycom ATS job board scraper
#
# Paycom uses a Sprawl micro-frontend SPA. The HTML shell embeds a short-lived
# JWT (sessionJWT) and the API base URL (atsPortalMantleServiceUrl) inside a
# configsFromHost JS variable. All job data comes from two REST endpoints:
#
#   POST {api_base}/api/ats/job-posting-previews/search  — paginated listing
#   GET  {api_base}/api/ats/job-postings/{jobId}          — full job detail
#
# Both the API base URL and the portal host vary per tenant:
#   www.paycomonline.net  → portal-applicant-tracking.us-cent.paycomonline.net
#   pc00.paycomonline.com → portal-applicant-tracking.int.us-cent.paycomonline.com
# We extract api_base dynamically from the HTML rather than hardcoding it.
#
# Slug stored in DB (JSON):
#   {"host": "www.paycomonline.net", "key": "DCA25B1567B6D478C88CD24ACA0EA0E7"}
#   {"host": "pc00.paycomonline.com", "key": "A38173AIE92874820ALRE20847CDE927PIW76526"}
#   host: the Paycom portal hostname (varies by tenant cluster)
#   key:  portal key from the career-page URL (variable-length alphanumeric)
#
# Option C freshness strategy:
#   1. GET career-page HTML → extract JWT, api_base, session cookies
#   2. POST listing search endpoint, paginate until exhausted
#   3. Compare job IDs with DB → new IDs only go to detail fetch
#   4. GET detail for new jobs → description + authoritative location + date
#   5. Store in job_postings

import re
import json
import time
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_TAKE = 100  # jobs per listing request

# Cache: "{host}|{key}" → (session, jwt, api_base, expires_at_epoch)
# JWT server expiry is 2 hours; we refresh after 90 minutes.
_session_cache: dict = {}


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Paycom portal keys are not derivable from company name.
    Detection is done via prospective_form_sync (career-page URL pattern).
    """
    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing)
# ─────────────────────────────────────────

def fetch_jobs(slug, company):
    """
    Fetch all active job listings for a Paycom portal.

    slug: JSON string {"host": "...", "key": "..."} or plain portal-key string
          (plain string supported for backwards compatibility).
    Returns list of job stub dicts; description filled by fetch_job_detail().
    """
    host, key = _parse_slug(slug)
    if not host or not key:
        return []

    session, jwt, api_base = _get_session(host, key)
    if not jwt:
        return []

    previews = []
    skip = 0
    while True:
        batch = _search_page(session, jwt, api_base, host, key, skip)
        if not batch:
            break
        previews.extend(batch)
        if len(batch) < _TAKE:
            break
        skip += _TAKE

    return [_preview_to_stub(p, host, key, company) for p in previews]


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Enrich a job stub with description, authoritative location, and posted_at.

    Called only for new jobs (Option C — listing comparison skips existing IDs).
    The session is cached per portal so repeated detail calls reuse it.
    """
    host   = job.get("_portal_host", "")
    key    = job.get("_portal_key", "")
    job_id = job.get("job_id", "")
    if not host or not key or not job_id:
        return job

    session, jwt, api_base = _get_session(host, key)
    if not jwt:
        return job

    url     = f"{api_base}/api/ats/job-postings/{job_id}"
    headers = _api_headers(jwt, host, key, f"jobs/{job_id}")
    try:
        resp = session.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.debug("paycom detail %s → %s", job_id, resp.status_code)
            return job

        data = resp.json().get("jobPosting", {})
        job  = dict(job)

        raw_desc = data.get("description", "")
        if raw_desc:
            job["description"] = _strip_html(raw_desc)

        loc = data.get("location", "")
        if loc:
            job["location"] = loc

        # legalRevisionDate is the closest date Paycom exposes
        date_obj = data.get("legalRevisionDate") or {}
        if isinstance(date_obj, dict) and date_obj.get("date"):
            try:
                job["posted_at"] = datetime.strptime(
                    date_obj["date"][:10], "%Y-%m-%d"
                )
            except ValueError:
                pass

        return job
    except Exception:
        logger.debug("paycom detail fetch failed job_id=%s", job_id, exc_info=True)
        return job


# ─────────────────────────────────────────
# SESSION / AUTH
# ─────────────────────────────────────────

def _get_session(host: str, key: str) -> tuple:
    """
    Return (requests.Session, jwt, api_base) for the given portal.

    Caches the session, JWT, and api_base for 90 minutes. On miss or expiry,
    fetches a fresh JWT and api_base from the career-page HTML shell.
    """
    cache_key = f"{host}|{key}"
    cached = _session_cache.get(cache_key)
    if cached:
        session, jwt, api_base, expires_at = cached
        if time.time() < expires_at:
            return session, jwt, api_base

    career_url = f"https://{host}/v4/ats/web.php/portal/{key}/career-page"
    session    = requests.Session()
    session.headers.update({
        "User-Agent":      (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         f"https://{host}/",
    })
    try:
        resp = session.get(career_url, timeout=15)
        if resp.status_code != 200:
            logger.warning("paycom: career-page %s → %s", career_url, resp.status_code)
            return session, "", ""
        jwt      = _extract_jwt(resp.text)
        api_base = _extract_api_base(resp.text)
        if not jwt:
            logger.warning("paycom: sessionJWT not found in %s", career_url)
            return session, "", ""
        if not api_base:
            logger.warning("paycom: atsPortalMantleServiceUrl not found in %s", career_url)
            return session, "", ""
        _session_cache[cache_key] = (session, jwt, api_base, time.time() + 5400)
        return session, jwt, api_base
    except Exception:
        logger.warning("paycom: failed to fetch career-page %s", career_url, exc_info=True)
        return session, "", ""


def _extract_jwt(html: str) -> str:
    """Extract sessionJWT value from the configsFromHost JS object."""
    m = re.search(r'"sessionJWT"\s*:\s*"([A-Za-z0-9._-]+)"', html)
    return m.group(1) if m else ""


def _extract_api_base(html: str) -> str:
    """
    Extract atsPortalMantleServiceUrl from the libConfig JS object in the HTML shell.

    The URL is triple-escaped in the HTML (3 backslashes per slash separator).
    Two-step unescape: json.loads collapses 3 backslashes to 1, then replace backslash-slash.
    """
    m = re.search(
        r'atsPortalMantleServiceUrl\\":\\"(.*?)\\"[,}]', html
    )
    if not m:
        return ""
    try:
        step1 = json.loads('"' + m.group(1) + '"')  # 3-backslash-slash → 1
        step2 = step1.replace("\\/", "/")            # 1-backslash-slash → /
        return step2.rstrip("/")
    except Exception:
        return ""


def _api_headers(jwt: str, host: str, key: str, path_suffix: str) -> dict:
    portal_origin = f"https://{host}"
    return {
        "authorization":        jwt,
        "content-type":         "application/json",
        "locale":               "en-US",
        "origin":               portal_origin,
        "portal-host-referrer": (
            f"{portal_origin}/v4/ats/web.php/portal/{key}/{path_suffix}"
        ),
    }


# ─────────────────────────────────────────
# LISTING
# ─────────────────────────────────────────

def _search_page(session, jwt: str, api_base: str,
                 host: str, key: str, skip: int) -> list:
    """POST the listing search endpoint; return raw preview dicts."""
    body = {
        "skip": skip,
        "take": _TAKE,
        "filtersForQuery": {
            "distanceFrom":      0,
            "workEnvironments":  [],
            "positionTypes":     [],
            "educationLevels":   [],
            "categories":        [],
            "travelTypes":       [],
            "shiftTypes":        [],
            "otherFilters":      [],
            "keywordSearchText": "",
            "location":          "",
            "sortOption":        "",
        },
    }
    try:
        resp = session.post(
            f"{api_base}/api/ats/job-posting-previews/search",
            json=body,
            headers=_api_headers(jwt, host, key, "career-page"),
            timeout=20,
        )
        if resp.status_code != 200:
            logger.warning("paycom search skip=%s → %s", skip, resp.status_code)
            return []
        return resp.json().get("jobPostingPreviews", [])
    except Exception:
        logger.warning("paycom search failed skip=%s", skip, exc_info=True)
        return []


def _preview_to_stub(preview: dict, host: str, key: str, company: str) -> dict:
    """Convert a search preview to a canonical job stub dict."""
    job_id  = str(preview.get("jobId", ""))
    job_url = (
        f"https://{host}/v4/ats/web.php/portal/{key}/jobs/{job_id}"
        if job_id else ""
    )
    return {
        "company":      company,
        "title":        preview.get("jobTitle", ""),
        "job_url":      job_url,
        "job_id":       job_id,
        "location":     preview.get("locations", "") or "",
        "posted_at":    None,
        "description":  "",
        "ats":          "paycom",
        "_portal_host": host,
        "_portal_key":  key,
    }


# ─────────────────────────────────────────
# SLUG PARSING
# ─────────────────────────────────────────

def _parse_slug(slug) -> tuple:
    """
    Parse slug into (host, key).

    Accepts:
      JSON string: '{"host": "www.paycomonline.net", "key": "DCA..."}'
      Plain string (legacy): "DCA25B1567B6D478C88CD24ACA0EA0E7"
                             assumes www.paycomonline.net host.
    """
    if isinstance(slug, dict):
        return slug.get("host", ""), slug.get("key", "")
    if not isinstance(slug, str):
        return "", ""
    slug = slug.strip()
    try:
        data = json.loads(slug)
        return data.get("host", ""), data.get("key", "")
    except (json.JSONDecodeError, TypeError):
        # Legacy plain portal key — assume original .net host
        return "www.paycomonline.net", slug


# ─────────────────────────────────────────
# TEXT CLEANUP
# ─────────────────────────────────────────

def _strip_html(html_str: str) -> str:
    soup = BeautifulSoup(html_str, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:5000]
