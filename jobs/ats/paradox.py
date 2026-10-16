# jobs/ats/paradox.py — Paradox (Olivia) careers-site job board scraper
#
# Paradox tenants run on the tenant's own domain (careers.fedex.com, ...). Every listing page embeds the
# whole search state as JSON in the HTML:
#
#   GET https://{host}/jobs?page_size=100            — page 1
#   GET https://{host}/jobs/page/{n}?page_size=100   — page n
#       <script>window.__PRELOAD_STATE__ = {...}</script>
#         jobSearch.totalJob   board job total (drifts by a few between fetches)
#         jobSearch.jobs[]     reference, title, originalURL, locations[{city,cityState,country,countryAbbr}]
#   GET https://{host}/{originalURL}                 — detail page: JSON-LD JobPosting (description, datePosted)
#
# `page_size` is honored only for 50 and 100 (FedEx); any other value silently falls back to the tenant default
# (25), so the loop trusts the length of what came back, never the requested size. A page past the end is
# HTTP 200 with zero jobs.
#
# Slug stored in DB (JSON):  {"host": "careers.fedex.com"}
#
# HTTP client: warmed, cookie-carrying browser-like session (the adp pattern): curl_cffi with Chrome
# impersonation (requests fallback), a navigation GET of /jobs first so server-issued cookies (`ct`, and the
# Akamai ak_bmsc/_abck/bm_sv set where a tenant uses bot management) ride along on every later request.
# Cookies are never hardcoded. A response without a parseable __PRELOAD_STATE__ (a bot-management challenge,
# an HTML error page) is a FAILED fetch — never an empty "complete" board.
#
# Completeness: reported_total = page-1 jobSearch.totalJob (TOTAL_ATS, 5% drift tolerance in FetchReport).

import json
import re
import time
from datetime import datetime
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

import config
from jobs.ats.fetch_report import (
    FetchReport, STOP_ALL_PAGES, STOP_EMPTY_PAGE, STOP_NO_CONFIG, STOP_PAGE_CAP, STOP_REPEAT_PAGE,
    STOP_REQUEST_FAILED, TOTAL_ATS, attach, count_distinct, failed,
)
from logger import get_logger

from requests import Session as _PlainSession

try:
    from curl_cffi.requests import Session as _CurlSession
    _USE_CURL_CFFI = bool(config.PARADOX_IMPERSONATE)
except ImportError:
    _CurlSession   = None
    _USE_CURL_CFFI = False

logger = get_logger(__name__)

_DESC_MAX_CHARS    = 5000
_MAX_SESSION_CACHE = 20          # detail sessions kept per host; bounds memory as tenants accumulate

_STATE_RE = re.compile(r"window\.__PRELOAD_STATE__\s*=\s*")
_DECODER  = json.JSONDecoder()

_chrome_major = (re.search(r"\d+", config.PARADOX_IMPERSONATE or "") or re.search(r"\d+", "146")).group(0)
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    f"Chrome/{_chrome_major}.0.0.0 Safari/537.36"
)

_NAV_HEADERS = {
    "accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language":           "en-US,en;q=0.9",
    "upgrade-insecure-requests": "1",
    "sec-fetch-dest":            "document",
    "sec-fetch-mode":            "navigate",
    "sec-fetch-user":            "?1",
}
if not _USE_CURL_CFFI:
    # curl_cffi impersonation supplies the UA / sec-ch-ua set itself; plain requests must send them.
    _NAV_HEADERS.update({
        "user-agent":         _UA,
        "sec-ch-ua":          f'"Chromium";v="{_chrome_major}", "Not-A.Brand";v="24", "Google Chrome";v="{_chrome_major}"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    })

# host → warmed session reused by fetch_job_detail (the listing crawl always builds its own)
_detail_sessions: dict = {}


class _ParadoxError(RuntimeError):
    """A page came back unusable (HTTP error, challenge page, no parseable state)."""


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """Paradox hosts are the tenant's own domain — not derivable from the company name.
    Detection is done by the career-page fingerprint scanners (see has_paradox_state)."""
    return None, None


def parse_preload_state(html):
    """Return the decoded window.__PRELOAD_STATE__ dict from a page, or None."""
    if not html:
        return None
    m = _STATE_RE.search(html)
    if not m:
        return None
    try:
        state, _ = _DECODER.raw_decode(html[m.end():])
    except ValueError:
        return None
    return state if isinstance(state, dict) else None


def has_paradox_state(html) -> bool:
    """True when the page embeds a Paradox search state (jobSearch). Strict on purpose: the paradox.ai chat
    widget alone (Cinemark, Chipotle) is NOT a Paradox careers site and must not match."""
    state = parse_preload_state(html)
    return bool(state) and isinstance(state.get("jobSearch"), dict)


# ─────────────────────────────────────────
# FETCH JOBS (listing)
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch every job of a Paradox careers site.

    slug_info: {"host": "careers.fedex.com"} (dict or JSON string).
    Returns job stubs; description / posted_at are filled by fetch_job_detail().
    """
    host = _parse_host(slug_info)
    if not host:
        return failed(STOP_NO_CONFIG, "slug has no host")

    started  = time.monotonic()
    jobs     = []
    seen     = set()
    unusable = 0
    pages    = 0
    total    = None
    stop     = STOP_EMPTY_PAGE
    note     = ""
    session  = None
    prev_url = None
    page     = 0

    while True:
        page += 1
        if page > config.PARADOX_MAX_PAGES:
            stop, note = STOP_PAGE_CAP, f"page cap {config.PARADOX_MAX_PAGES} reached"
            logger.warning("paradox: %s hit PARADOX_MAX_PAGES=%s", host, config.PARADOX_MAX_PAGES)
            break
        if time.monotonic() - started > config.PARADOX_TIME_BUDGET_S:
            stop, note = STOP_PAGE_CAP, f"time budget {config.PARADOX_TIME_BUDGET_S}s exceeded at page {page}"
            logger.warning("paradox: %s exceeded PARADOX_TIME_BUDGET_S=%s at page %s",
                           host, config.PARADOX_TIME_BUDGET_S, page)
            break

        url = _page_url(host, page)
        try:
            session, js = _get_listing_page(session, host, url, prev_url)
        except _ParadoxError as exc:
            stop, note = STOP_REQUEST_FAILED, f"page {page}: {exc}"
            logger.warning("paradox: %s page %s failed: %s", host, page, exc)
            break
        prev_url = url

        if total is None:
            total = js["totalJob"]
        rows = js["jobs"]
        if not rows:
            stop = STOP_EMPTY_PAGE
            break
        pages += 1

        new = 0
        for row in rows:
            stub = _to_stub(row, host, company)
            if stub is None:
                unusable += 1
                continue
            if stub["job_id"] in seen:
                continue
            seen.add(stub["job_id"])
            jobs.append(stub)
            new += 1
        if not new:
            stop = STOP_REPEAT_PAGE        # the same rows again — paging param ignored (would loop forever)
            break
        if total is not None and len(seen) + unusable >= total:
            stop = STOP_ALL_PAGES
            break

    # Rows with no title/id are excluded from what the board can be expected to yield.
    reported = None if total is None else max(total - unusable, 0)
    return attach(jobs, FetchReport(
        stop_reason      = stop,
        pages            = pages,
        fetched_distinct = count_distinct(jobs),
        unusable         = unusable,
        reported_total   = reported,
        total_source     = TOTAL_ATS if reported is not None else None,
        repeated_pages   = 1 if stop == STOP_REPEAT_PAGE else 0,
        note             = note,
    ))


def _page_url(host, page):
    size = config.PARADOX_PAGE_SIZE
    if page == 1:
        return f"https://{host}/jobs?page_size={size}"
    return f"https://{host}/jobs/page/{page}?page_size={size}"


def _get_listing_page(session, host, url, referer):
    """GET one listing page, retrying with a freshly warmed session. Returns (session, jobSearch dict)."""
    last = None
    for attempt in range(config.PARADOX_PAGE_RETRIES + 1):
        if session is None:
            session = _new_session(host)
        try:
            return session, _fetch_state(session, url, referer)
        except _ParadoxError as exc:
            last = exc
        except Exception as exc:           # transport errors from curl_cffi / requests
            last = _ParadoxError(f"{type(exc).__name__}: {exc}")
        session = None                     # next attempt starts from a fresh warm-up (new cookies)
        if attempt < config.PARADOX_PAGE_RETRIES:
            time.sleep(config.PARADOX_RETRY_WAIT_S * (attempt + 1))
    raise last


def _fetch_state(session, url, referer):
    """GET url and return its validated jobSearch dict; raise _ParadoxError on anything unusable."""
    headers = dict(_NAV_HEADERS)
    headers["sec-fetch-site"] = "same-origin"
    headers["referer"]        = referer or url.split("/jobs", 1)[0] + "/jobs"
    resp = session.get(url, headers=headers, timeout=config.PARADOX_REQUEST_TIMEOUT_S)
    if resp.status_code != 200:
        raise _ParadoxError(f"HTTP {resp.status_code}")
    state = parse_preload_state(resp.text)
    js    = state.get("jobSearch") if state else None
    if not isinstance(js, dict) or not isinstance(js.get("jobs"), list):
        raise _ParadoxError("no __PRELOAD_STATE__.jobSearch in response (bot-management page?)")
    total = js.get("totalJob")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise _ParadoxError("jobSearch.totalJob missing or not an integer")
    return js


# ─────────────────────────────────────────
# SESSION
# ─────────────────────────────────────────

def _new_session(host):
    """Browser-like session warmed with a top-level navigation to /jobs so server-issued cookies are set."""
    if _USE_CURL_CFFI:
        session = _CurlSession(impersonate=config.PARADOX_IMPERSONATE)
    else:
        session = _PlainSession()
    headers = dict(_NAV_HEADERS)
    headers["sec-fetch-site"] = "none"
    try:
        session.get(f"https://{host}/jobs", headers=headers, timeout=config.PARADOX_REQUEST_TIMEOUT_S)
    except Exception as exc:
        logger.debug("paradox: session warm-up failed host=%r: %s", host, exc)
    return session


def _detail_session(host):
    session = _detail_sessions.get(host)
    if session is None:
        if len(_detail_sessions) >= _MAX_SESSION_CACHE:
            _detail_sessions.clear()
        session = _detail_sessions[host] = _new_session(host)
    return session


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Enrich a stub with the description and posted date from the detail page's JSON-LD JobPosting.

    Called only for new jobs. Raises on HTTP / parse failure so the caller retries later instead of storing
    an empty description.
    """
    job_url = job.get("job_url", "")
    host    = (urlsplit(job_url).hostname or "").lower()    # derived, so no extra key must survive the queue payload
    if not host:
        return job

    headers = dict(_NAV_HEADERS)
    headers["sec-fetch-site"] = "same-origin"
    headers["referer"]        = f"https://{host}/jobs"
    try:
        resp = _detail_session(host).get(job_url, headers=headers, timeout=config.PARADOX_REQUEST_TIMEOUT_S)
    except Exception:
        _detail_sessions.pop(host, None)    # re-warm next time
        raise
    if resp.status_code != 200:
        if resp.status_code in (403, 429):
            _detail_sessions.pop(host, None)
        raise _ParadoxError(f"detail HTTP {resp.status_code} for {job_url}")

    posting = _find_job_posting(resp.text)
    if posting is None:
        raise _ParadoxError(f"no JobPosting JSON-LD in {job_url}")

    job = dict(job)
    desc = posting.get("description")
    if isinstance(desc, str) and desc.strip():
        job["description"] = _strip_html(desc)
    posted = _parse_date(posting.get("datePosted"))
    if posted:
        job["posted_at"] = posted
    return job


def _find_job_posting(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except ValueError:
            continue
        for item in (data if isinstance(data, list) else [data]):
            if isinstance(item, dict) and item.get("@type") == "JobPosting":
                return item
    return None


def _parse_date(value):
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _strip_html(html_str):
    text = BeautifulSoup(html_str, "html.parser").get_text(separator="\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text).strip()[:_DESC_MAX_CHARS]


# ─────────────────────────────────────────
# NORMALIZE / COUNTRY
# ─────────────────────────────────────────

def _to_stub(row, host, company):
    """Convert one jobSearch row to a canonical stub, or None when it has no title / id / detail path."""
    if not isinstance(row, dict):
        return None
    job_id = str(row.get("reference") or row.get("requisitionID") or row.get("uniqueID") or "").strip()
    title  = (row.get("title") or "").strip()
    path   = (row.get("originalURL") or "").strip().lstrip("/")
    if not job_id or not title or not path:
        return None
    locs = [l for l in (row.get("locations") or []) if isinstance(l, dict)]
    return {
        "company":       company,
        "job_id":        job_id,
        "title":         title,
        "location":      _location_text(locs),
        "posted_at":     None,
        "description":   "",
        "job_url":       f"https://{host}/{path}",
        "ats":           "paradox",
        "_slug":         host,
        "_country_code": _country_of(locs),
    }


def _location_text(locs):
    parts = []
    for l in locs:
        text = ", ".join(p for p in (l.get("cityState") or l.get("city"), l.get("country")) if p)
        if text and text not in parts:
            parts.append(text)
    return "; ".join(parts)


def _country_of(locs):
    """Alpha-2 for the job: US if ANY location is in the US (the job is then relevant), else the first."""
    codes = [(l.get("countryAbbr") or "").strip().upper() for l in locs]
    codes = [c for c in codes if len(c) == 2 and c.isalpha()]
    if "US" in codes:
        return "US"
    return codes[0] if codes else ""


def get_country_code(job):
    """ISO alpha-2 country stored at listing time (job_monitor's listing-level country gate)."""
    return (job.get("_country_code") or "").upper()


# ─────────────────────────────────────────
# SLUG
# ─────────────────────────────────────────

def _parse_host(slug_info):
    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except ValueError:
            slug_info = {"host": slug_info}
    if not isinstance(slug_info, dict):
        return ""
    host = str(slug_info.get("host") or "").strip().lower()
    host = re.sub(r"^https?://", "", host).split("/", 1)[0]
    return host
