"""
jobs/ats/custom_career.py — Universal custom career page scraper.

Handles any company career page that doesn't fit a standard ATS platform.
Tested against: Amazon, Microsoft, Apple, Tesla, Meta, Wayfair, Siemens.
Designed to handle any future company generically.

─────────────────────────────────────────────────────────────────────────
SESSION REFRESH (self-healing)
─────────────────────────────────────────────────────────────────────────
Before every fetch, a fresh session is warmed by loading career_page_url.
Three strategies are auto-detected once and cached in slug_info:

  cookie_only   -- career page GET sets session cookies automatically.
                   Most common (Amazon, Tesla, Apple, Wayfair).

  csrf_token    -- CSRF token extracted from HTML hidden input,
                   meta tag, or JS variable. Injected into headers
                   and POST body. (Microsoft x-csrf-token, Wayfair CSN_CSRF)

  bearer_token  -- JWT or bearer token in page JS or __NEXT_DATA__.
                   Set as Authorization header.

  graphql       -- Meta-style: lsd + doc_id + __rev extracted from
                   career page JS. GraphQL POST body rebuilt fresh
                   on every run. Never stale.

  url_session   -- Session token embedded in URL query params
                   (Siemens ste_sid). Extracted from page links,
                   appended to all requests.

  none          -- No auth signals found. Proceeds without credentials.

─────────────────────────────────────────────────────────────────────────
DETAIL FETCH
─────────────────────────────────────────────────────────────────────────
If slug_info["detail"] is present, fetch_job_detail() is called for
each job that passes title+location filters. Only matched jobs get
detail fetched — not all N raw jobs.

Detail response types handled:
  JSON object (direct or wrapped in envelope)
  HTML page (BS4 extraction + JSON-LD + __NEXT_DATA__)
  XML

─────────────────────────────────────────────────────────────────────────
LISTING RESPONSE TYPES
─────────────────────────────────────────────────────────────────────────
  JSON object/array
  JSON wrapped in GraphQL envelope (edges/node, results/node)
  POST JSON body
  POST GraphQL (form-encoded or JSON)
  HTML + CSS class scraping
  HTML + JSON-LD
  HTML + __NEXT_DATA__
  XML/RSS
  JSONP
"""

import re
import json
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlunparse, unquote_plus

import requests
from bs4 import BeautifulSoup

from jobs.utils import (
    SKIP_HEADERS,
    REQUEST_TIMEOUT,
    UNIX_TS_MIN,
    UNIX_TS_MS_MIN,
    UNIX_TS_MS_MAX,
    is_json       as _is_json,
    is_valid_url,
    clean_html    as _clean_html,
    parse_salary_text as _parse_salary_text,
    parse_date_value  as _extract_date,
    extract_url_from_value as _extract_url,
    extract_job_id_from_path as _extract_id_from_path,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

MAX_PAGES            = 500
MAX_JOBS             = 5000
ANALYSIS_SAMPLE_SIZE = 10
PAGE_RETRY_DELAY     = 1.0

# REQUEST_TIMEOUT, SKIP_HEADERS, UNIX_TS_MIN, UNIX_TS_MS_MIN
# imported from jobs.utils

URL_PATTERN      = re.compile(r'^https?://', re.IGNORECASE)
ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2})?')

TITLE_HINTS    = {"title", "jobtitle", "job_title", "position",
                  "positionname", "rolename", "role", "name",
                  "requisitiontitle", "posting_name", "jobname",
                  "text", "headline"}
LOCATION_HINTS = {"location", "joblocation", "job_location", "city",
                  "citystate", "office", "site", "locationname",
                  "locations", "fulllocation", "jobcity", "jobstate",
                  "primarylocation", "worklocation"}
ID_HINTS       = {"id", "jobid", "job_id", "requisitionid", "reqid",
                  "externalid", "postingid", "uniqueid", "slug",
                  "ats_job_id", "display_job_id", "contest_no",
                  "jid", "pid", "refnum", "jobcode"}
URL_HINTS      = {"url", "joburl", "job_url", "applyurl",
                  "canonicalpositionurl", "detailurl", "link",
                  "href", "absoluteurl", "hostedurl", "joblink",
                  "apply_url", "external_url", "job_path",
                  "jobpath", "detailurl", "viewurl"}

# Fields that look like URLs but are apply/next-step links — deprioritize
# in favour of canonical job detail URLs
DEPRIORITIZE_URL_HINTS = {
    "urlnextstep", "applyurl", "applicationurl", "applynow",
    "applylink", "nextstep", "applyhere", "applicationlink",
}
DATE_HINTS     = {"posteddate", "posted_date", "postedat", "posted_at",
                  "postingdate", "dateposted", "createdat", "created_at",
                  "publishedat", "t_create", "t_update", "publishdate",
                  "dateadded", "startdate", "opendate",
                  "postedts", "posted_ts", "createts", "create_ts",
                  "updatedts", "updated_ts", "modifiedts", "modified_ts",
                  "postdate", "post_date", "activatedat", "activationdate"}

OFFSET_PARAMS  = ["start", "offset", "from", "skip",
                  "jobOffset", "recordsOffset", "jobRecordsOffset",
                  "startindex", "start_index", "page_offset"]
PAGE_PARAMS    = ["page", "pageNumber", "page_number", "pageNo",
                  "pageNum", "currentPage", "pg", "p"]
CURSOR_FIELDS  = ["nextCursor", "next_cursor", "nextPageToken",
                  "cursor", "after", "nextPage", "next"]
TOTAL_HINTS    = {"total", "totalcount", "total_count", "count",
                  "totalresults", "total_results", "totaljobs",
                  "totalrecords", "numfound", "nbhits", "hits",
                  "num_jobs", "jobcount", "totalItems", "recordCount"}

# SKIP_HEADERS imported from jobs.utils

# Token extraction patterns
JWT_PATTERN    = re.compile(
    r'eyJ[A-Za-z0-9_\-]{20,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}'
)
BEARER_PATTERN = re.compile(
    r'(?:bearer[_\-]?token|access[_\-]?token|authToken|apiToken)'
    r'["\s:=]+["\']([A-Za-z0-9+/=_\-\.]{20,})["\']',
    re.IGNORECASE
)
CSRF_INPUT_PATTERN = re.compile(
    r'<input[^>]+name=["\']'
    r'(?:csrf[_\-]?token|_token|authenticity_token|csrftoken)["\']'
    r'[^>]*value=["\']([^"\']{10,})["\']',
    re.IGNORECASE
)
CSRF_META_PATTERN = re.compile(
    r'<meta[^>]+name=["\']csrf-token["\'][^>]*content=["\']([^"\']{10,})["\']',
    re.IGNORECASE
)
CSRF_JS_PATTERN = re.compile(
    r'csrftoken["\s:=]+(["\']?)([A-Za-z0-9+/=_\-]{20,})\1',
    re.IGNORECASE
)
# Meta-specific extractions
META_LSD_PATTERN = re.compile(
    r'(?:'
    r'\["LSD",\[\],\{"token":"'             # current: ["LSD",[],{"token":"ABC"}
    r'|LSD\.set\(["\']'                     # legacy: LSD.set("ABC")
    r'|"lsd"["\s:,]+["\']'                  # JSON: "lsd":"ABC"
    r'|name=["\']lsd["\'][^>]*value=["\']'  # HTML input: name="lsd" value="ABC"
    r'|"x-fb-lsd"["\s:,]+"'               # header in JS: "x-fb-lsd":"ABC"
    r')'
    r'([A-Za-z0-9+/=_\-]{10,})',
    re.IGNORECASE
)

META_REV_PATTERN = re.compile(
    r'(?:"client_revision"|"__rev")["\s:,]+(\d{8,})'
)
META_DOCID_PATTERN = re.compile(
    r'(?:"doc_id"|"docid")["\s:,]+"?(\d{10,})"?'
)
# Siemens-style URL session token
URL_SESSION_PATTERN = re.compile(
    r'(?:href|src|action)["\s:=]+["\']?[^"\']*'
    r'[?&](?:ste_sid|session_id|sid|token)=([A-Za-z0-9+/=_\-]{16,})',
    re.IGNORECASE
)

# Description container selectors (tried in order)
DESCRIPTION_SELECTORS = [
    # JSON-LD JobPosting
    None,   # handled separately
    # Common class names
    {"class": re.compile(r'job[_\-]?desc|description|job[_\-]?detail|'
                         r'job[_\-]?content|job[_\-]?body|posting[_\-]?desc',
                         re.I)},
    # Common ids
    {"id":    re.compile(r'job[_\-]?desc|description|job[_\-]?detail|'
                         r'job[_\-]?content|posting[_\-]?body',
                         re.I)},
    # Fallback — largest text block
    None,
]


# ─────────────────────────────────────────
# SELF-HEALING SESSION
# ─────────────────────────────────────────

def _warm_session(slug_info, company):
    """
    Build a fresh requests.Session by loading career_page_url.
    Auto-detects session strategy on first call, caches in slug_info.

    Returns (session, strategy) on success.
    Returns (None, None) if career page completely unreachable.

    Strategies:
        cookie_only   -- cookies from page response
        csrf_token    -- CSRF extracted from HTML
        bearer_token  -- JWT/bearer from page JS
        graphql       -- Meta-style: lsd + doc_id + __rev
        url_session   -- session token in URL params (Siemens)
        none          -- nothing found, proceed bare
    """
    career_page_url = slug_info.get("career_page_url")
    if not career_page_url:
        return _build_legacy_session(slug_info), "none"

    session = requests.Session()

    # Apply structural headers (user-agent, accept etc.)
    # Skip cookies — getting fresh ones from page
    for k, v in slug_info.get("headers", {}).items():
        if k.lower() not in SKIP_HEADERS and k.lower() != "cookie":
            session.headers[k] = v

    # Ensure realistic user-agent
    if "user-agent" not in {k.lower() for k in session.headers}:
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )

    # Load career page
    try:
        logger.debug("custom_career: warming session for %r via %s",
                     company, career_page_url)
        resp = session.get(
            career_page_url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code >= 500:
            logger.warning(
                "custom_career: career page returned %d for %r",
                resp.status_code, company
            )
            _flag_diagnostic(
                company      = company,
                step         = "session_warm",
                severity     = "blocked",
                pattern_hint = f"career_page_{resp.status_code}",
                notes        = (
                    f"Career page returned HTTP {resp.status_code}. "
                    f"URL: {career_page_url}"
                ),
            )
            return None, None
    except requests.RequestException as e:
        hint = "career_page_timeout" if "timeout" in str(e).lower() \
               else "career_page_connection_error"
        logger.warning(
            "custom_career: career page unreachable for %r: %s",
            company, e
        )
        _flag_diagnostic(
            company      = company,
            step         = "session_warm",
            severity     = "blocked",
            pattern_hint = hint,
            notes        = (
                f"Career page unreachable: {e}. "
                f"URL: {career_page_url}"
            ),
        )
        return None, None

    html = resp.text

    # Use cached strategy if available
    cached_strategy    = slug_info.get("session_strategy")
    cached_token_field = slug_info.get("token_field")

    if cached_strategy and cached_strategy != "none":
        strategy = cached_strategy
        _apply_cached_strategy(
            session, slug_info, html, strategy,
            cached_token_field, company
        )
        logger.debug(
            "custom_career: session warmed for %r strategy=%s cookies=%d",
            company, strategy, len(session.cookies)
        )
        return session, strategy

    # First run — detect strategy
    strategy, token, token_field, extra = _detect_session_strategy(
        html, session, slug_info
    )

    # Cache for future runs
    slug_info["session_strategy"] = strategy
    slug_info["token_field"]      = token_field
    if extra:
        slug_info.update(extra)

    _apply_token(session, slug_info, token, strategy)

    logger.info(
        "custom_career: detected strategy for %r: %s "
        "(token_field=%s cookies=%d)",
        company, strategy, token_field, len(session.cookies)
    )
    return session, strategy


def _detect_session_strategy(html, session, slug_info):
    """
    Analyse career page HTML to determine auth strategy.
    Returns (strategy, token, token_field, extra_dict).
    extra_dict contains any extra data to store in slug_info.
    """
    # ── GraphQL (Meta-style) ──────────────────────────────────────
    if slug_info.get("graphql_config"):
        lsd, rev, doc_id = _extract_meta_tokens(html)
        extra = {}
        if lsd:
            extra["_lsd"] = lsd
        if rev:
            extra["_rev"] = rev
        if doc_id and not slug_info["graphql_config"].get("doc_id"):
            slug_info["graphql_config"]["doc_id"] = doc_id
        elif doc_id:
            # Update doc_id if it changed (deployment)
            slug_info["graphql_config"]["doc_id"] = doc_id
        return "graphql", lsd, "meta_lsd", extra

    # ── JWT / Bearer token ────────────────────────────────────────
    jwt_match = JWT_PATTERN.search(html)
    if jwt_match:
        return "bearer_token", jwt_match.group(0), "jwt", {}

    bearer_match = BEARER_PATTERN.search(html)
    if bearer_match:
        return "bearer_token", bearer_match.group(1), "bearer", {}

    # __NEXT_DATA__ token check
    next_tag = re.search(
        r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
    )
    if next_tag:
        try:
            next_data = json.loads(next_tag.group(1))
            token = _find_token_in_dict(next_data)
            if token:
                return "bearer_token", token, "next_data", {}
        except (json.JSONDecodeError, TypeError):
            pass

    # ── CSRF token ────────────────────────────────────────────────
    m = CSRF_INPUT_PATTERN.search(html)
    if m:
        return "csrf_token", m.group(1), "csrf_input", {}

    m = CSRF_META_PATTERN.search(html)
    if m:
        return "csrf_token", m.group(1), "csrf_meta", {}

    m = CSRF_JS_PATTERN.search(html)
    if m:
        return "csrf_token", m.group(2), "csrf_js", {}

    # ── URL session token (Siemens ste_sid style) ─────────────────
    m = URL_SESSION_PATTERN.search(html)
    if m:
        token = m.group(1)
        # Find the param name
        param_match = re.search(
            r'[?&](ste_sid|session_id|sid|token)=' + re.escape(token),
            html, re.IGNORECASE
        )
        param_name = param_match.group(1) if param_match else "ste_sid"
        extra = {"_url_session_param": param_name}
        return "url_session", token, param_name, extra

    # ── Cookie-only (most common) ─────────────────────────────────
    if len(session.cookies) > 0:
        return "cookie_only", None, None, {}

    return "none", None, None, {}


def _apply_cached_strategy(session, slug_info, html, strategy,
                            token_field, company):
    """Re-apply a known strategy using fresh page HTML."""
    if strategy == "graphql":
        lsd, rev, doc_id = _extract_meta_tokens(html)
        if lsd:
            slug_info["_lsd"] = lsd
            # Inject x-fb-lsd header — Meta requires this to match body lsd
            session.headers["x-fb-lsd"] = lsd
        if rev:
            slug_info["_rev"] = rev
        if doc_id:
            slug_info["graphql_config"]["doc_id"] = doc_id

    elif strategy == "bearer_token":
        token = _extract_token_by_field(html, token_field)
        if token:
            _apply_token(session, slug_info, token, strategy)

    elif strategy == "csrf_token":
        token = _extract_token_by_field(html, token_field)
        if token:
            _apply_token(session, slug_info, token, strategy)

    elif strategy == "url_session":
        m = URL_SESSION_PATTERN.search(html)
        if m:
            slug_info["_url_session_token"] = m.group(1)


def _extract_meta_tokens(html):
    """
    Extract Meta GraphQL tokens from career page HTML.
    Returns (lsd, rev, doc_id) — any may be None.
    """
    lsd    = None
    rev    = None
    doc_id = None

    m = META_LSD_PATTERN.search(html)
    if m:
        lsd = m.group(1)
    else:
        # Hidden input fallback
        m = re.search(
            r'<input[^>]+name=["\']lsd["\'][^>]*value=["\']([^"\']{8,})["\']',
            html, re.IGNORECASE
        )
        if m:
            lsd = m.group(1)

    m = META_REV_PATTERN.search(html)
    if m:
        rev = m.group(1)

    m = META_DOCID_PATTERN.search(html)
    if m:
        doc_id = m.group(1)

    return lsd, rev, doc_id


def _extract_token_by_field(html, token_field):
    """Re-extract a token using a known field type hint."""
    if not token_field:
        return None

    if token_field == "jwt":
        m = JWT_PATTERN.search(html)
        return m.group(0) if m else None

    if token_field == "bearer":
        m = BEARER_PATTERN.search(html)
        return m.group(1) if m else None

    if token_field == "next_data":
        next_tag = re.search(
            r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if next_tag:
            try:
                return _find_token_in_dict(json.loads(next_tag.group(1)))
            except (json.JSONDecodeError, TypeError):
                pass

    if token_field == "csrf_input":
        m = CSRF_INPUT_PATTERN.search(html)
        return m.group(1) if m else None

    if token_field == "csrf_meta":
        m = CSRF_META_PATTERN.search(html)
        return m.group(1) if m else None

    if token_field == "csrf_js":
        m = CSRF_JS_PATTERN.search(html)
        return m.group(2) if m else None

    return None


def _apply_token(session, slug_info, token, strategy):
    """Inject extracted token into session."""
    if not token or not strategy or strategy in ("cookie_only", "none",
                                                  "graphql", "url_session"):
        return

    if strategy == "bearer_token":
        session.headers["Authorization"] = f"Bearer {token}"
        logger.debug("custom_career: injected bearer token")

    elif strategy == "csrf_token":
        session.headers["X-CSRF-Token"] = token
        session.headers["X-CSRFToken"]  = token
        slug_info["_csrf_token"]        = token
        logger.debug("custom_career: injected CSRF token")


def _find_token_in_dict(data, depth=0):
    """Recursively search dict for a JWT or bearer token."""
    if depth > 4 or not isinstance(data, dict):
        return None
    for k, v in data.items():
        if isinstance(v, str) and JWT_PATTERN.match(v):
            return v
        if isinstance(v, str) and len(v) > 20 and k.lower() in {
            "token", "accesstoken", "access_token", "bearertoken",
            "authtoken", "apitoken", "idtoken", "id_token"
        }:
            return v
        if isinstance(v, dict):
            r = _find_token_in_dict(v, depth + 1)
            if r:
                return r
    return None


def _build_legacy_session(slug_info):
    """Fallback for slug_info without career_page_url."""
    session = requests.Session()
    for k, v in slug_info.get("headers", {}).items():
        if k.lower() not in SKIP_HEADERS:
            session.headers[k] = v
    for name, value in slug_info.get("cookies", {}).items():
        session.cookies.set(name, value)
    return session


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for a company using stored curl config.
    Warms session before every call — cookies never expire.
    """
    if not slug_info:
        return []
    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            logger.error("custom_career: invalid slug_info for %r", company)
            return []

    base_url = slug_info.get("url", "")
    if not base_url:
        logger.error("custom_career: no url in slug_info for %r", company)
        return []

    logger.info("custom_career: starting for %r at %s", company, base_url)
    # Store company name in slug_info so _fetch_page can use it in diagnostics
    slug_info["_company"] = company

    # Warm session
    session, strategy = _warm_session(slug_info, company)
    if session is None:
        logger.warning(
            "custom_career: career page unreachable for %r — "
            "trying legacy session", company
        )
        session  = _build_legacy_session(slug_info)
        strategy = "none"

    logger.debug("custom_career: session ready strategy=%s", strategy)

    initial_page   = _get_initial_page(slug_info)
    # Fetch page 1
    page1_response = _fetch_page(
        session, slug_info, page=initial_page, offset=0
    )
    if page1_response is None:
        logger.warning(
            "custom_career: page 1 failed for %r", company
        )
        _flag_expired(company)
        return []

    # Auto-detect structure or use cached
    needs_detection = not all([
        slug_info.get("format"),
        slug_info.get("array_path") is not None,
        slug_info.get("field_map"),
        slug_info.get("pagination") is not None,
    ])

    if needs_detection:
        detected = _detect_structure(page1_response, slug_info)
        if not detected:
            logger.warning(
                "custom_career: structure detection failed for %r", company
            )
            _flag_diagnostic(
                company      = company,
                step         = "structure_detect",
                severity     = "blocked",
                pattern_hint = "no_array_found",
                raw_response = page1_response,
                notes        = (
                    f"_find_jobs_array() returned None. "
                    f"Response format could not be detected or no "
                    f"recognizable jobs array found. "
                    f"Raw response stored — inspect to identify new pattern."
                ),
            )
            return []
        slug_info = {**slug_info, **detected}
        _save_enriched_config(company, slug_info)
        logger.info(
            "custom_career: saved config for %r format=%s pagination=%s "
            "strategy=%s",
            company, slug_info.get("format"),
            slug_info.get("pagination", {}).get("type"),
            slug_info.get("session_strategy"),
        )
    elif slug_info.get("session_strategy") is None:
        # Strategy just detected — persist it
        _save_enriched_config(company, slug_info)

    # Extract jobs from page 1
    jobs_arr = _extract_jobs_array(page1_response, slug_info)
    if not jobs_arr:
        logger.warning(
            "custom_career: no jobs on page 1 for %r", company
        )
        return []

    logger.info("custom_career: page 1 — %d jobs for %r",
                len(jobs_arr), company)

    # Detect total + collect all numeric fields for inspection
    total              = _detect_total(page1_response, slug_info)
    all_numeric_fields = _collect_numeric_fields(page1_response)
    total_field        = slug_info.get("pagination", {}).get("total_field")

    # ── Save inspection row ───────────────────────────────────────
    # Stores first raw job + all metadata for manual verification.
    # field_map_override in inspection table takes priority over
    # auto-detected field_map if set manually.
    _save_inspection_row(
        company            = company,
        base_url           = base_url,
        slug_info          = slug_info,
        jobs_arr           = jobs_arr,
        total              = total,
        total_field        = total_field,
        all_numeric_fields = all_numeric_fields,
    )

    # Check for manual field_map override in inspection table
    # This lets you fix wrong field detection without re-syncing
    from db.custom_ats_inspection import get_field_map_override
    field_map_override = get_field_map_override(company)
    if field_map_override:
        logger.info(
            "custom_career: using field_map_override for %r: %s",
            company, field_map_override
        )
        slug_info = {**slug_info, "field_map": field_map_override}

    # Paginate
    all_raw    = list(jobs_arr)
    pagination = slug_info.get("pagination", {})
    pag_type   = pagination.get("type", "none")

    if pag_type != "none" and total and len(jobs_arr) < total:
        extra = _paginate(session, slug_info, jobs_arr, total, company , initial_page = initial_page)
        all_raw.extend(extra)

    logger.info("custom_career: %d raw jobs for %r", len(all_raw), company)

    # Normalize
    field_map = slug_info.get("field_map", {})
    results   = []
    for raw in all_raw[:MAX_JOBS]:
        job = _normalize(raw, company, field_map, base_url, slug_info=slug_info)
        if job:
            results.append(job)

    logger.info("custom_career: %d normalized jobs for %r",
                len(results), company)
    return results


# ─────────────────────────────────────────
# DETAIL FETCH
# ─────────────────────────────────────────

def fetch_job_detail(job, slug_info, session=None):
    """
    Fetch full job description + salary for a single matched job.
    Called only for jobs that pass title+location filters.

    Uses slug_info["detail"] config stored at curl capture time.
    Reuses the warmed session from fetch_jobs() if provided.

    Supports:
      JSON object (direct or wrapped)
      HTML page (BS4 + JSON-LD + __NEXT_DATA__)
      XML

    Returns enriched job dict (original + description/salary fields).
    """
    detail_config = slug_info.get("detail") if isinstance(slug_info, dict) else None
    if not detail_config:
        return job

    company = job.get("company", "")

    # Build session if not provided
    if session is None:
        session, _ = _warm_session(slug_info, company)
        if session is None:
            session = _build_legacy_session(slug_info)

    # Build detail URL
    from jobs.curl_parser import build_detail_url
    url, params, body = build_detail_url(detail_config, job)

    if not url or "{job_id}" in url:
        logger.warning(
            "custom_career: could not build detail URL for %r/%s",
            company, job.get("job_id")
        )
        return job

    # Apply URL session token if needed (Siemens style)
    url_session_param = slug_info.get("_url_session_param")
    url_session_token = slug_info.get("_url_session_token")
    if url_session_param and url_session_token:
        params = dict(params or {})
        params[url_session_param] = url_session_token

    # Fetch detail page
    method = detail_config.get("method", "GET").upper()

    # Apply any extra headers specific to detail requests
    extra_headers = detail_config.get("headers", {})
    for k, v in extra_headers.items():
        if k.lower() not in SKIP_HEADERS:
            session.headers[k] = v

    try:
        if method == "POST":
            resp = session.post(
                url,
                params=params or None,
                json=json.loads(body) if body and _is_json(body) else None,
                data=body if body and not _is_json(body) else None,
                timeout=REQUEST_TIMEOUT,
            )
        else:
            resp = session.get(
                url,
                params=params or None,
                timeout=REQUEST_TIMEOUT,
            )

        if not resp.ok:
            logger.warning(
                "custom_career: detail fetch HTTP %d for %r/%s",
                resp.status_code, company, job.get("job_id")
            )
            return job

        raw_bytes = resp.content

    except requests.RequestException as e:
        logger.warning(
            "custom_career: detail fetch error for %r/%s: %s",
            company, job.get("job_id"), e
        )
        return job

    # Detect response format + extract detail fields
    needs_detection = not all([
        detail_config.get("format"),
        detail_config.get("field_map"),
    ])

    if needs_detection:
        detected = _detect_detail_structure(raw_bytes, url)
        if detected:
            detail_config.update(detected)
            # Persist updated detail config
            slug_info["detail"] = detail_config
            _save_enriched_config(company, slug_info)

    # Extract fields
    parsed_detail = _extract_detail_fields(
        raw_bytes, detail_config, url
    )

    if parsed_detail:
        job = dict(job)
        job.update(parsed_detail)
        logger.debug(
            "custom_career: detail enriched for %r/%s desc_len=%d",
            company, job.get("job_id"),
            len(job.get("description", "") or "")
        )

    return job


# ─────────────────────────────────────────
# DETAIL STRUCTURE DETECTION
# ─────────────────────────────────────────

def _detect_detail_structure(raw_bytes, url):
    """
    Detect the format and field map for a single job detail response.
    Returns dict with format, object_path, field_map.
    """
    if not raw_bytes:
        return None

    text = raw_bytes.decode("utf-8", errors="ignore").strip()

    # JSON response
    if text and text[0] in ('{', '['):
        try:
            data = json.loads(text)
            obj_path, job_obj = _find_job_object(data)
            if job_obj:
                field_map = _detect_detail_field_map(job_obj)
                if field_map.get("description") or field_map.get("title"):
                    return {
                        "format":      "json",
                        "object_path": obj_path,
                        "field_map":   field_map,
                    }
        except json.JSONDecodeError:
            pass

    # HTML response
    if '<html' in text.lower() or '<!doctype' in text.lower():
        # Try JSON-LD first
        soup = BeautifulSoup(text, "html.parser")
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict) and data.get("@type") in (
                    "JobPosting", "jobPosting"
                ):
                    field_map = _detect_detail_field_map(data)
                    if field_map.get("description"):
                        return {
                            "format":      "html_jsonld",
                            "object_path": None,
                            "field_map":   field_map,
                        }
            except (json.JSONDecodeError, TypeError):
                pass

        # __NEXT_DATA__
        next_tag = soup.find("script", id="__NEXT_DATA__")
        if next_tag:
            try:
                data     = json.loads(next_tag.string or "")
                obj_path, job_obj = _find_job_object(data)
                if job_obj:
                    field_map = _detect_detail_field_map(job_obj)
                    if field_map.get("description") or field_map.get("title"):
                        return {
                            "format":      "html_next",
                            "object_path": obj_path,
                            "field_map":   field_map,
                        }
            except (json.JSONDecodeError, TypeError):
                pass

        # Pure HTML scraping
        return {
            "format":      "html",
            "object_path": None,
            "field_map":   {},
        }

    # XML
    if text.startswith("<?xml") or text.startswith("<job"):
        return {
            "format":      "xml",
            "object_path": None,
            "field_map":   {},
        }

    return None


def _find_job_object(data, depth=0, path=""):
    """
    Find the single job object in a detail response.
    Different from _find_jobs_array — looks for a rich single object.
    Returns (path, object) or (None, None).
    """
    if depth > 6:
        return None, None

    if isinstance(data, dict):
        # Check if this dict looks like a job object
        if _looks_like_job_object(data):
            return path or "[root]", data

        # Search nested — prefer keys with job-like names
        job_key_hints = {
            "job", "position", "posting", "requisition",
            "data", "result", "node", "jobDetail",
            "jobDetails", "job_detail",
        }
        # Try job-hinted keys first
        for k in job_key_hints:
            if k in data:
                sub_path = f"{path}.{k}" if path else k
                r_path, r_obj = _find_job_object(data[k], depth+1, sub_path)
                if r_obj:
                    return r_path, r_obj

        # Try all dict values
        best_path, best_obj, best_score = None, None, 0
        for k, v in data.items():
            sub_path = f"{path}.{k}" if path else k
            r_path, r_obj = _find_job_object(v, depth+1, sub_path)
            if r_obj:
                score = _job_object_score(r_obj)
                if score > best_score:
                    best_path, best_obj, best_score = r_path, r_obj, score

        return best_path, best_obj

    # GraphQL edges/node pattern
    if isinstance(data, list) and len(data) == 1:
        return _find_job_object(data[0], depth+1, path)

    return None, None


def _looks_like_job_object(d):
    """Check if a dict looks like a single job detail object."""
    if not isinstance(d, dict) or len(d) < 3:
        return False
    keys_lower = {k.lower().replace("_", "").replace("-", "")
                  for k in d.keys()}
    # Must have at least a title-like field
    title_keys = {"title", "jobtitle", "name", "position",
                  "positionname", "heading"}
    if not keys_lower & title_keys:
        return False
    # Must have at least one of description/requirements/responsibilities
    desc_keys = {"description", "jobdescription", "summary",
                 "responsibilities", "requirements", "qualifications",
                 "overview", "details", "body", "content"}
    has_desc = bool(keys_lower & desc_keys)
    # Or has a long string value (>100 chars)
    has_long = any(
        isinstance(v, str) and len(v) > 100
        for v in d.values()
    )
    return has_desc or has_long


def _job_object_score(d):
    """Score a dict by how much it looks like a complete job object."""
    if not isinstance(d, dict):
        return 0
    score = 0
    keys_lower = {k.lower().replace("_", "").replace("-", "")
                  for k in d.keys()}
    score += len(keys_lower & {"title", "jobtitle", "name"}) * 3
    score += len(keys_lower & {"description", "jobdescription",
                               "summary", "responsibilities"}) * 5
    score += len(keys_lower & {"location", "city", "office"}) * 2
    score += len(keys_lower & {"salary", "compensation", "pay"}) * 2
    score += min(len(d), 20)
    return score


def _detect_detail_field_map(obj):
    """
    Detect field names in a single job detail object.
    Returns dict: {description, title, location, salary_min, salary_max,
                   salary_type, posted_at, job_id}
    """
    if not isinstance(obj, dict):
        return {}

    field_map = {}

    # Description — find longest text field
    desc_candidates = []
    for k, v in obj.items():
        kl = k.lower().replace("_", "").replace("-", "")
        if isinstance(v, str) and len(v) > 50:
            score = len(v)
            if kl in {"description", "jobdescription", "summary",
                      "responsibilities", "overview", "body",
                      "content", "details", "fulldescription"}:
                score += 10000
            desc_candidates.append((score, k))
    if desc_candidates:
        field_map["description"] = max(desc_candidates)[1]

    # Multi-field description (concatenate at extraction time)
    multi_desc_fields = []
    for k, v in obj.items():
        kl = k.lower().replace("_", "").replace("-", "")
        if kl in {"responsibilities", "requirements", "qualifications",
                  "minimumqualifications", "preferredqualifications",
                  "aboutthejob", "overview"} and isinstance(v, str):
            multi_desc_fields.append(k)
    if multi_desc_fields and not field_map.get("description"):
        field_map["description_parts"] = multi_desc_fields

    # Title
    for k, v in obj.items():
        if k.lower().replace("_", "").replace("-", "") in {
            "title", "jobtitle", "name", "positionname",
            "position", "heading", "rolename"
        } and isinstance(v, str):
            field_map["title"] = k
            break

    # Location
    for k, v in obj.items():
        kl = k.lower().replace("_", "").replace("-", "")
        if kl in {"location", "primarylocation", "joblocation",
                  "worklocation", "city", "office"} and (
            isinstance(v, (str, list, dict))
        ):
            field_map["location"] = k
            break

    # Salary — look for nested compensation or flat fields
    for k, v in obj.items():
        kl = k.lower().replace("_", "").replace("-", "")
        if kl in {"compensation", "salary", "pay", "salaryrange",
                  "payrange"}:
            if isinstance(v, dict):
                field_map["salary_obj"] = k
            elif isinstance(v, str):
                field_map["salary_text"] = k
            break
        if kl in {"salarymin", "minsalary", "salary_min",
                  "minsalaryvalue", "basesalarymin"}:
            field_map["salary_min"] = k
        if kl in {"salarymax", "maxsalary", "salary_max",
                  "maxsalaryvalue", "basesalarymax"}:
            field_map["salary_max"] = k

    return field_map


def _extract_detail_fields(raw_bytes, detail_config, url):
    """
    Extract description + salary + other fields from detail response.
    Returns dict of extracted fields, or None.
    """
    if not raw_bytes:
        return None

    fmt        = detail_config.get("format")
    obj_path   = detail_config.get("object_path")
    field_map  = detail_config.get("field_map", {})

    result = {
        "description":   "",
        "salary_min":    "",
        "salary_max":    "",
        "salary_type":   "",
    }

    text = raw_bytes.decode("utf-8", errors="ignore").strip()

    # ── JSON ──────────────────────────────────────────────────────
    if fmt in ("json", "jsonp", None) and text and text[0] in ('{', '[', '('):
        try:
            if text[0] == '(':
                # JSONP
                m = re.match(r'^[^(]+\((.*)\)\s*;?\s*$', text, re.DOTALL)
                data = json.loads(m.group(1)) if m else None
            else:
                data = json.loads(text)

            if data:
                # Walk to object_path
                obj = data
                if obj_path and obj_path != "[root]":
                    for part in obj_path.lstrip(".").split("."):
                        if isinstance(obj, dict):
                            obj = obj.get(part)
                        elif isinstance(obj, list) and obj:
                            obj = obj[0]
                        else:
                            break

                if not obj:
                    _, obj = _find_job_object(data)

                if isinstance(obj, dict):
                    return _map_detail_fields(obj, field_map, result)
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    # ── HTML ──────────────────────────────────────────────────────
    if fmt in ("html", "html_jsonld", "html_next", None):
        soup = BeautifulSoup(text, "html.parser")

        # JSON-LD JobPosting
        if fmt in ("html_jsonld", None):
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict) and data.get("@type") in (
                        "JobPosting", "jobPosting"
                    ):
                        desc = (data.get("description") or
                                data.get("responsibilities") or "")
                        if desc:
                            result["description"] = _clean_html(desc)
                            # Salary from JSON-LD
                            sb = data.get("baseSalary", {})
                            if isinstance(sb, dict):
                                sv = sb.get("value", {})
                                if isinstance(sv, dict):
                                    result["salary_min"]  = str(sv.get("minValue", ""))
                                    result["salary_max"]  = str(sv.get("maxValue", ""))
                                    result["salary_type"] = str(sv.get("unitText", ""))
                            return result
                except (json.JSONDecodeError, TypeError):
                    pass

        # __NEXT_DATA__
        if fmt in ("html_next", None):
            next_tag = soup.find("script", id="__NEXT_DATA__")
            if next_tag:
                try:
                    data     = json.loads(next_tag.string or "")
                    obj_path_, job_obj = _find_job_object(data)
                    if job_obj and isinstance(job_obj, dict):
                        fm = field_map or _detect_detail_field_map(job_obj)
                        return _map_detail_fields(job_obj, fm, result)
                except (json.JSONDecodeError, TypeError):
                    pass

        # HTML scraping — find description container
        desc_text = _scrape_description_from_html(soup)
        if desc_text:
            result["description"] = desc_text
            return result

    # ── XML ───────────────────────────────────────────────────────
    if fmt == "xml" or text.startswith("<?xml"):
        try:
            root = ET.fromstring(raw_bytes)
            for tag in ("description", "job_description",
                        "summary", "body"):
                el = root.find(f".//{tag}")
                if el is not None and el.text:
                    result["description"] = _clean_html(el.text)
                    return result
        except ET.ParseError:
            pass

    return result if result.get("description") else None


def _map_detail_fields(obj, field_map, result):
    """Map detected field names to standard output fields."""
    # Description
    desc_field = field_map.get("description")
    if desc_field and obj.get(desc_field):
        result["description"] = _clean_html(str(obj[desc_field]))

    # Multi-part description (Oracle-style split fields)
    desc_parts = field_map.get("description_parts", [])
    if desc_parts and not result["description"]:
        parts = []
        for part_key in desc_parts:
            val = obj.get(part_key)
            if val and isinstance(val, str):
                parts.append(val)
        if parts:
            result["description"] = _clean_html("\n\n".join(parts))

    # Salary object (nested)
    salary_obj_key = field_map.get("salary_obj")
    if salary_obj_key:
        sal = obj.get(salary_obj_key, {})
        if isinstance(sal, dict):
            result["salary_min"]  = str(sal.get("min", sal.get("minimum", sal.get("low", ""))))
            result["salary_max"]  = str(sal.get("max", sal.get("maximum", sal.get("high", ""))))
            result["salary_type"] = str(sal.get("currency", sal.get("type", sal.get("period", ""))))

    # Salary flat fields
    if field_map.get("salary_min"):
        result["salary_min"] = str(obj.get(field_map["salary_min"], ""))
    if field_map.get("salary_max"):
        result["salary_max"] = str(obj.get(field_map["salary_max"], ""))

    # Salary text (parse from string like "$120k-$160k")
    salary_text_key = field_map.get("salary_text")
    if salary_text_key and not result["salary_min"]:
        sal_text = str(obj.get(salary_text_key, ""))
        if sal_text:
            result["salary_min"], result["salary_max"] = _parse_salary_text(sal_text)
            result["salary_type"] = sal_text  # store original

    return result


def _scrape_description_from_html(soup):
    """
    Extract job description from HTML using common selectors.
    Returns cleaned plain text or empty string.
    """
    # Try common class/id patterns
    candidates = []

    for tag in soup.find_all(
        ["div", "section", "article"],
        class_=re.compile(
            r'job.?desc|description|job.?detail|job.?content|'
            r'job.?body|posting.?desc|job.?summary|job.?info',
            re.I
        )
    ):
        text = tag.get_text(separator="\n", strip=True)
        if len(text) > 100:
            candidates.append((len(text), text))

    for tag in soup.find_all(
        ["div", "section"],
        id=re.compile(
            r'job.?desc|description|job.?detail|job.?content|'
            r'job.?body|posting',
            re.I
        )
    ):
        text = tag.get_text(separator="\n", strip=True)
        if len(text) > 100:
            candidates.append((len(text), text))

    if candidates:
        # Return the longest match
        text = max(candidates)[1]
        return re.sub(r'\n{3,}', '\n\n', text).strip()[:8000]

    # Last resort — largest block element
    best_len, best_text = 0, ""
    for tag in soup.find_all(["div", "section", "article"]):
        # Skip nav/header/footer
        classes = " ".join(tag.get("class", []))
        if re.search(r'nav|header|footer|sidebar|menu|search|'
                     r'filter|banner|ad', classes, re.I):
            continue
        text = tag.get_text(separator="\n", strip=True)
        if len(text) > best_len and len(text) > 200:
            best_len  = len(text)
            best_text = text

    if best_text:
        return re.sub(r'\n{3,}', '\n\n', best_text).strip()[:8000]

    return ""


# Detect starting page number from stored body (0-indexed vs 1-indexed APIs)
def _get_initial_page(slug_info):
    """Return the page number stored in the original curl body/params."""
    pagination = slug_info.get("pagination", {})
    pag_param  = pagination.get("param", "page")
    body       = slug_info.get("body", "")
    params     = slug_info.get("params", {})

    if body and _is_json(body):
        try:
            val = json.loads(body).get(pag_param)
            if val is not None:
                return int(val)
        except (ValueError, TypeError):
            pass
    if pag_param in params:
        try:
            return int(params[pag_param])
        except (ValueError, TypeError):
            pass
    return 0  # default assume 0-indexed



# _parse_salary_text, _clean_html imported from jobs.utils


# ─────────────────────────────────────────
# PAGE FETCH
# ─────────────────────────────────────────

def _fetch_page(session, slug_info, page=1, offset=0, cursor=None):
    """
    Fetch one page of listing results.
    Handles GET, POST JSON, POST GraphQL, POST form-encoded.
    """
    url        = slug_info.get("url", "")
    method     = slug_info.get("method", "GET").upper()
    params     = dict(slug_info.get("params", {}))
    body       = slug_info.get("body")
    pagination = slug_info.get("pagination", {})
    pag_type   = pagination.get("type", "none")
    pag_param  = pagination.get("param", "")
    strategy   = slug_info.get("session_strategy", "none")

    # Apply pagination
    if pag_type == "offset" and pag_param:
        _inject_param(params, body, pag_param,
                      str(offset), pagination.get("location", "url"))
        if pagination.get("location") == "body" and body:
            body = _inject_into_body(body, pag_param, offset)
        else:
            params[pag_param] = str(offset)

    elif pag_type == "page" and pag_param:
        if pagination.get("location") == "body" and body:
            body = _inject_into_body(body, pag_param, page)
        else:
            params[pag_param] = str(page)

    elif pag_type == "cursor" and cursor:
        cursor_param = pagination.get("cursor_param", "cursor")
        params[cursor_param] = cursor

    # Apply URL session token (Siemens)
    url_session_param = slug_info.get("_url_session_param")
    url_session_token = slug_info.get("_url_session_token")
    if url_session_param and url_session_token:
        params[url_session_param] = url_session_token

    # Build GraphQL body fresh on every page
    if strategy == "graphql" and slug_info.get("graphql_config"):
        from jobs.curl_parser import build_graphql_body
        lsd  = slug_info.get("_lsd", "")
        rev  = slug_info.get("_rev", "")
        body = build_graphql_body(slug_info["graphql_config"], lsd, rev)
        method = "POST"
        # Meta requires x-fb-lsd header to match lsd value in body
        if lsd:
            session.headers["x-fb-lsd"] = lsd

    # Inject CSRF into POST body
    csrf_token = slug_info.get("_csrf_token")
    if csrf_token and method == "POST" and body and _is_json(body):
        try:
            body_data = json.loads(body)
            for csrf_key in ("csrftoken", "csrf_token", "_token"):
                if csrf_key not in body_data:
                    body_data[csrf_key] = csrf_token
                    break
            body = json.dumps(body_data)
        except (json.JSONDecodeError, TypeError):
            pass

    # Execute
    try:
        content_type = slug_info.get("headers", {}).get(
            "content-type", ""
        ).lower()
        is_form = "form" in content_type or (
            strategy == "graphql"
        )

        if method == "POST":
            if is_form and body and not _is_json(body):
                resp = session.post(
                    url,
                    params=params or None,
                    data=body,
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                resp = session.post(
                    url,
                    params=params or None,
                    json=json.loads(body) if body and _is_json(body) else None,
                    data=body if body and not _is_json(body) else None,
                    timeout=REQUEST_TIMEOUT,
                )
        else:
            resp = session.get(
                url,
                params=params or None,
                timeout=REQUEST_TIMEOUT,
            )

        if resp.status_code in (401, 403):
            strategy = slug_info.get("session_strategy", "unknown")
            logger.warning(
                "custom_career: auth error %d strategy=%s",
                resp.status_code, strategy
            )
            _flag_diagnostic(
                company      = slug_info.get("_company", "unknown"),
                step         = "auth_error",
                severity     = "blocked",
                pattern_hint = f"auth_failed_after_{strategy}",
                raw_response = resp.content,
                notes        = (
                    f"Listing fetch returned HTTP {resp.status_code} "
                    f"after session warm. Strategy tried: {strategy}. "
                    f"Career page may use an unrecognized auth pattern. "
                    f"Check career page HTML for new token patterns."
                ),
            )
            return None

        if resp.status_code == 429:
            logger.warning("custom_career: rate limited — waiting 60s")
            time.sleep(60)
            return None

        if not resp.ok:
            logger.warning("custom_career: HTTP %d for %s",
                           resp.status_code, url)
            return None

        return resp.content

    except requests.RequestException as e:
        logger.error("custom_career: request error: %s", e)
        return None


def _inject_param(params, body, param, value, location):
    if location == "url":
        params[param] = value


def _inject_into_body(body, param, value):
    try:
        data = json.loads(body)
        data[param] = value
        return json.dumps(data)
    except (json.JSONDecodeError, TypeError):
        return body


# _is_json imported from jobs.utils


# ─────────────────────────────────────────
# STRUCTURE DETECTION (listing)
# ─────────────────────────────────────────

def _detect_structure(raw_bytes, slug_info):
    """Detect listing response format, array path, field map, pagination."""
    fmt, data = _detect_format(raw_bytes)
    if fmt is None or data is None:
        return None

    jobs_arr, array_path = _find_jobs_array(data)
    if not jobs_arr:
        return None

    company  = slug_info.get("_company", "")
    base_url = slug_info.get("url", "")

    field_map    = None
    total_field  = None
    ai_available = False

    # ── AI detection (field map + total field in one call) ───────
    # data (the full parsed response) is passed as full_response so
    # the AI can identify the total count field from the envelope at
    # the same time as the per-job field map — one quota hit, two results.
    if company and jobs_arr:
        try:
            from outreach.ai_full_personalizer import detect_field_map_with_ai
            field_map, total_field, job_url_template, ai_available = \
                detect_field_map_with_ai(
                    company, jobs_arr[0], base_url,
                    full_response=data,
                    sample_job_url=slug_info.get("sample_job_url"),
                )
            if ai_available:
                if field_map:
                    logger.info(
                        "custom_career: AI field map accepted for %r: %s",
                        company, field_map
                    )
                if total_field:
                    logger.info(
                        "custom_career: AI total_field for %r: %r",
                        company, total_field
                    )
                # Store job_url_template in slug_info for _normalize
                if job_url_template:
                    slug_info["job_url_template"] = job_url_template
                    logger.info(
                        "custom_career: AI job_url_template for %r: %r",
                        company, job_url_template
                    )
            # ── job_url_template fallback ─────────────────────────────────
            # AI returned null or was unavailable — try pattern inference
            if not slug_info.get("job_url_template") and field_map:
                inferred = _infer_job_url_template(slug_info, jobs_arr, field_map)
                if inferred:
                    slug_info["job_url_template"] = inferred
                    logger.info(
                        "custom_career: inferred job_url_template for %r: %r",
                        company, inferred
                    )
            else:
                logger.debug(
                    "custom_career: AI unavailable for %r — "
                    "running pattern detection",
                    company
                )
        except Exception as e:
            logger.debug(
                "custom_career: AI detection error for %r: %s",
                company, e
            )
            ai_available = False

    if not ai_available:
        field_map   = _detect_field_map(jobs_arr)
        total_field = _find_total_field(data)
        logger.info(
            "custom_career: pattern field map for %r: %s  total_field=%r",
            company, field_map, total_field
        )

    if not field_map or not field_map.get("title"):
        return None

    # ── Pagination detection ─────────────────────────────────────
    pagination = _detect_pagination(slug_info, data, len(jobs_arr))

    # Inject total_field (from AI or pattern) if pagination didn't find one
    if total_field and not pagination.get("total_field"):
        pagination["total_field"] = total_field

    return {
        "format":     fmt,
        "array_path": array_path,
        "field_map":  field_map,
        "pagination": pagination,
    }


# ─────────────────────────────────────────
# FORMAT DETECTION
# ─────────────────────────────────────────

def _detect_format(raw_bytes):
    """Detect response format and parse into Python structure."""
    if not raw_bytes:
        return None, None

    text = raw_bytes.decode("utf-8", errors="ignore").strip()

    # JSON
    if text and text[0] in ('{', '['):
        try:
            return "json", json.loads(text)
        except json.JSONDecodeError:
            pass

    # JSONP
    jsonp_match = re.match(
        r'^[a-zA-Z_$][a-zA-Z0-9_$]*\s*\((.+)\)\s*;?\s*$',
        text, re.DOTALL
    )
    if jsonp_match:
        try:
            return "jsonp", json.loads(jsonp_match.group(1))
        except json.JSONDecodeError:
            pass

    # XML
    if (text.startswith('<?xml') or text.startswith('<feed') or
            text.startswith('<rss') or text.startswith('<jobs')):
        try:
            root = ET.fromstring(raw_bytes)
            data = _xml_to_jobs(root)
            if data:
                return "xml", data
        except ET.ParseError:
            pass

    # HTML
    if '<html' in text.lower() or '<!doctype' in text.lower():
        soup = BeautifulSoup(text, "html.parser")

        # JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, (dict, list)):
                    return "html_jsonld", data
            except (json.JSONDecodeError, TypeError):
                pass

        # __NEXT_DATA__
        next_tag = soup.find("script", id="__NEXT_DATA__")
        if next_tag:
            try:
                data = json.loads(next_tag.string or "")
                return "html_next", data
            except (json.JSONDecodeError, TypeError):
                pass

        # Raw HTML scraping — extract job cards
        jobs = _scrape_html_listing(soup)
        if jobs:
            return "html_scraped", jobs

    return None, None


def _xml_to_jobs(root):
    jobs = []
    for tag in ("item", "job", "position", "vacancy", "opening", "entry"):
        elements = root.findall(f".//{tag}")
        if elements:
            for el in elements:
                job = {}
                for child in el:
                    clean_tag = re.sub(r'\{[^}]+\}', '', child.tag)
                    job[clean_tag] = child.text or ""
                if job:
                    jobs.append(job)
            break
    if not jobs and len(root) > 0:
        for child in root:
            job = {}
            for el in child:
                clean_tag = re.sub(r'\{[^}]+\}', '', el.tag)
                job[clean_tag] = el.text or ""
            if job:
                jobs.append(job)
    return jobs if len(jobs) >= 2 else []


def _scrape_html_listing(soup):
    """
    Generic HTML listing scraper.
    Finds job cards by common patterns, extracts title/url/location/id.
    Handles Tesla, Siemens, Amazon, Google listing pages.
    """
    jobs = []

    # Strategy 1: Find all <a> tags with job-like hrefs
    job_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        # Must have text and look like a job URL
        if not text or len(text) < 3:
            continue
        if not re.search(
            r'/jobs?/|/careers?/|/positions?/|/openings?/|'
            r'/posting|/job-detail|/JobDetail|/requisition',
            href, re.I
        ):
            continue
        # Skip navigation links (too short or generic)
        if text.lower() in {"apply", "learn more", "view", "details",
                             "share", "save", "back", "home"}:
            continue
        if len(text) > 5:
            job_links.append((text, href))

    if not job_links:
        return []

    # Strategy 2: Group links by proximity to location/id info
    # Try to find a repeating card structure
    seen_hrefs = set()
    for title, href in job_links:
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)

        # Build minimal job dict
        job = {
            "title":   title,
            "job_url": href if href.startswith("http") else "",
            "job_id":  _extract_id_from_path(href),
            "location": "",
        }

        # Try to find location near this link in the DOM
        # (check parent elements for location spans)
        # Since we only have soup (no element reference),
        # this is done at normalize time via the listing URL pattern

        jobs.append(job)

    # Deduplicate by job_id if available
    seen_ids = set()
    unique   = []
    for job in jobs:
        jid = job.get("job_id") or job.get("job_url")
        if jid and jid not in seen_ids:
            seen_ids.add(jid)
            unique.append(job)

    return unique if len(unique) >= 2 else []


# _extract_id_from_path imported from jobs.utils as extract_job_id_from_path


# ─────────────────────────────────────────
# JOBS ARRAY EXTRACTION
# ─────────────────────────────────────────

def _find_jobs_array(data, depth=0, path=""):
    """
    Recursively find the largest array of job-like dicts.
    Handles standard JSON, GraphQL edges/node, results/node patterns.
    """
    if depth > 6:
        return None, None

    # Root list
    if isinstance(data, list):
        if len(data) >= 2 and data and isinstance(data[0], dict):
            str_fields = sum(
                1 for v in data[0].values()
                if isinstance(v, str) and len(v) > 1
            )
            if str_fields >= 2:
                return data, path or "[root]"
        return None, None

    if not isinstance(data, dict):
        return None, None

    best_arr, best_path, best_len = None, None, 1

    for key, val in data.items():
        current_path = f"{path}.{key}" if path else key

        # Direct array
        if isinstance(val, list) and len(val) > best_len:
            if val and isinstance(val[0], dict):
                # GraphQL edges pattern: [{node: {...}}, ...]
                if "node" in val[0]:
                    unwrapped = [
                        item["node"] for item in val
                        if isinstance(item.get("node"), dict)
                    ]
                    if len(unwrapped) > best_len:
                        best_arr  = unwrapped
                        best_path = f"{current_path}[node]"
                        best_len  = len(unwrapped)
                    continue

                str_fields = sum(
                    1 for v in val[0].values()
                    if isinstance(v, str) and len(v) > 1
                )
                if str_fields >= 2:
                    best_arr  = val
                    best_path = current_path
                    best_len  = len(val)

        elif isinstance(val, (dict, list)):
            sub_arr, sub_path = _find_jobs_array(
                val, depth + 1, current_path
            )
            if sub_arr and len(sub_arr) > best_len:
                best_arr  = sub_arr
                best_path = sub_path
                best_len  = len(sub_arr)

    return best_arr, best_path


def _extract_jobs_array(raw_bytes, slug_info):
    """Extract jobs array from raw bytes using cached config."""
    if isinstance(raw_bytes, bytes):
        _, data = _detect_format(raw_bytes)
    else:
        data = raw_bytes

    if data is None:
        return []

    # html_scraped format returns list directly
    if isinstance(data, list):
        return data

    array_path = slug_info.get("array_path")
    if not array_path or array_path == "[root]":
        if isinstance(data, list):
            return data
        arr, _ = _find_jobs_array(data)
        return arr or []

    # Walk dot-separated path
    # Handle GraphQL [node] suffix
    parts = array_path.lstrip(".").split(".")
    current = data
    for part in parts:
        if part == "[node]":
            # Unwrap GraphQL nodes
            if isinstance(current, list):
                current = [
                    item["node"] for item in current
                    if isinstance(item, dict) and "node" in item
                ]
            break
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = None
        if current is None:
            break

    if isinstance(current, list):
        return current

    # Path stale — re-detect
    arr, _ = _find_jobs_array(data)
    return arr or []


# ─────────────────────────────────────────
# FIELD MAP DETECTION (listing)
# ─────────────────────────────────────────

def _detect_field_map(jobs_arr):
    sample = [j for j in jobs_arr[:ANALYSIS_SAMPLE_SIZE]
              if isinstance(j, dict)]
    if not sample:
        return {}

    all_fields = set()
    for job in sample:
        all_fields.update(job.keys())

    field_scores = {
        f: {"title": 0, "url": 0, "location": 0, "date": 0, "id": 0}
        for f in all_fields
    }

    for job in sample:
        for field, value in job.items():
            vtype = _classify_value(field, value)
            # url_apply counts as url for scoring but will be deprioritized
            # in best_for() in favour of canonical job URLs
            if vtype == "url_apply":
                field_scores[field]["url"] += 1
            elif vtype in field_scores[field]:
                field_scores[field][vtype] += 1

    def best_for(category, exclude=None):
        exclude = exclude or set()
        candidates = [
            (f, scores[category])
            for f, scores in field_scores.items()
            if f not in exclude and scores[category] > 0
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1], reverse=True)
        top_score = candidates[0][1]
        top       = [(f, s) for f, s in candidates if s == top_score]
        hints = {
            "title":    TITLE_HINTS,
            "url":      URL_HINTS,
            "location": LOCATION_HINTS,
            "date":     DATE_HINTS,
            "id":       ID_HINTS,
        }
        hint_set = hints.get(category, set())
        for f, _ in top:
            if f.lower().replace("_", "").replace("-", "") in {
                h.replace("_", "").replace("-", "") for h in hint_set
            }:
                return f
        # For url category: skip apply/next-step URLs if canonical exists
        if category == "url":
            non_apply = [
                f for f, _ in top
                if f.lower().replace("_", "").replace("-", "")
                not in DEPRIORITIZE_URL_HINTS
            ]
            if non_apply:
                return non_apply[0]
        return top[0][0]

    used      = set()
    title     = best_for("title")
    if not title:
        return {}
    used.add(title)

    job_url   = best_for("url",      exclude=used)
    if job_url: used.add(job_url)
    posted_at = best_for("date",     exclude=used)
    if posted_at: used.add(posted_at)
    job_id    = best_for("id",       exclude=used)
    if job_id: used.add(job_id)
    location  = best_for("location", exclude=used)

    result = {
        "title":    title,
        "job_url":  job_url,
        "location": location,
        "posted_at":posted_at,
        "job_id":   job_id,
    }

    # Cross-validate URL and job_id using URL path content
    result = _verify_url_id_correlation(result, sample)

    return result


def _verify_url_id_correlation(field_map, sample_jobs, base_url=""):
    """
    Cross-validate job_url and job_id fields using URL path content.

    Many ATS systems embed the job ID in the URL path:
      positionUrl = "/careers/job/1970393556855691"
      id          = 1970393556855691   ← same value

    Two scenarios handled:

    1. Detected job_id value IS in job_url path
       → both confirmed correct, return unchanged

    2. Detected job_id value NOT in job_url path
       → look for any field whose value appears in the URL path
       → that field is likely the real job_id
       → update field_map["job_id"] to that field

    This fixes cases like Microsoft where:
      id = 1970393556855691 (position ID, misclassified as date)
      atsJobId = "200033787" (the real job ID string)
      positionUrl = "/careers/job/1970393556855691" (contains id, not atsJobId)
    So id should be job_id, not atsJobId.
    """
    url_field = field_map.get("job_url")
    id_field  = field_map.get("job_id")

    if not url_field:
        return field_map

    matches = {}   # field_name → how many samples matched

    for job in sample_jobs[:5]:
        url_val = str(job.get(url_field, "") or "")
        if not url_val:
            continue

        # Normalize to path only for comparison
        from urllib.parse import urlparse as _up
        path = _up(url_val).path if url_val.startswith("http") else url_val

        # Extract all numeric sequences from path (5+ digits)
        nums_in_path = set(re.findall(r'\d{5,}', path))
        if not nums_in_path:
            continue

        # Check every field to see if its value appears in the URL path
        for field, val in job.items():
            if field == url_field:
                continue
            str_val = str(val) if val is not None else ""
            if str_val and str_val in nums_in_path:
                matches[field] = matches.get(field, 0) + 1

    if not matches:
        return field_map

    # Field that appears in URL path most consistently across samples
    best_id_field = max(matches, key=lambda f: matches[f])
    best_count    = matches[best_id_field]

    # Only override if matched in at least 2 samples or all samples agree
    if best_count >= 2 or (len(sample_jobs) == 1 and best_count == 1):
        if best_id_field != id_field:
            logger.debug(
                "custom_career: URL/ID correlation — "
                "overriding job_id from %r to %r (matched in %d samples)",
                id_field, best_id_field, best_count
            )
            field_map = {**field_map, "job_id": best_id_field}

    return field_map


def _classify_value(field_name, value):
    fl = field_name.lower().replace("_", "").replace("-", "")

    if value is None or value == "" or value == []:
        return "empty"

    if isinstance(value, str):
        v = value.strip()
        if URL_PATTERN.match(v):
            # Deprioritize apply/next-step URLs — return url_apply
            # so best_for() can prefer canonical URLs over apply links
            fl_check = fl.replace("_", "").replace("-", "")
            if fl_check in DEPRIORITIZE_URL_HINTS:
                return "url_apply"
            return "url"
        # Relative job URL paths like /en/jobs/12345/title
        if (v.startswith("/") and len(v) > 5 and
                re.match(r'^/[a-zA-Z0-9/_\-]+$', v)):
            return "url"
        if ISO_DATE_PATTERN.match(v):
            return "date"
        if re.match(r'^\d{4,}$', v):
            return "id"
        if re.match(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
            r'-[0-9a-f]{4}-[0-9a-f]{12}$', v, re.I
        ):
            return "id"
        if len(v) > 200:
            return "long_text"
        # JSON-encoded string that looks like a location object
        if v.startswith('{') or (v.startswith('[') and 'city' in v.lower()):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, (dict, list)):
                    return "location"
            except (json.JSONDecodeError, TypeError):
                pass
        for hint_set, vtype in [
            (TITLE_HINTS, "title"), (LOCATION_HINTS, "location"),
            (URL_HINTS, "url"), (DATE_HINTS, "date"), (ID_HINTS, "id"),
        ]:
            if fl in {h.replace("_", "").replace("-", "")
                      for h in hint_set}:
                return vtype
        if 3 <= len(v) <= 200:
            return "title"

    if isinstance(value, int):
        # Use upper bound to exclude large internal IDs (e.g. Microsoft
        # position IDs like 1970393556855691 are above UNIX_TS_MS_MAX)
        if UNIX_TS_MS_MIN <= value <= UNIX_TS_MS_MAX:
            return "date"
        if UNIX_TS_MIN <= value < UNIX_TS_MS_MIN:
            return "date"
        # Anything else — treat as id
        if value > 0:
            return "id"

    if isinstance(value, dict) and value:
        # Nested location object: {city, country, region, ...}
        # Also handles protobuf Longs — but those are ints after resolution,
        # so a dict that reaches here is most likely a location.
        dict_keys = {k.lower().replace("_", "") for k in value.keys()}
        location_keys = {"city", "country", "region", "state",
                         "countryname", "locationname", "name"}
        if dict_keys & location_keys:
            return "location"
        # Hint-based fallback on field name
        if fl in {h.replace("_", "").replace("-", "") for h in LOCATION_HINTS}:
            return "location"
        return "empty"

    if isinstance(value, list) and value:
        if all(isinstance(v, str) for v in value[:3]):
            return "location"
        # List of location dicts (Uber's allLocations)
        if isinstance(value[0], dict):
            dict_keys = {k.lower().replace("_", "") for k in value[0].keys()}
            location_keys = {"city", "country", "region", "state",
                             "countryname", "locationname"}
            if dict_keys & location_keys:
                return "location"

    return "empty"


# ─────────────────────────────────────────
# PAGINATION DETECTION
# ─────────────────────────────────────────

def _detect_pagination(slug_info, data, page1_size):
    params = slug_info.get("params", {})
    body   = slug_info.get("body", "")

    all_params = dict(params)
    if body:
        try:
            body_data = json.loads(body)
            if isinstance(body_data, dict):
                all_params.update(body_data)
        except (json.JSONDecodeError, TypeError):
            pass

    # Also check GraphQL variables
    if slug_info.get("graphql_config"):
        variables = slug_info["graphql_config"].get("variables", {})
        if isinstance(variables, dict):
            all_params.update(variables)

    # Params that control how many results per page
    PAGE_SIZE_PARAMS = [
        "result_limit", "num", "size", "limit", "pageSize",
        "page_size", "count", "per_page", "perPage",
        "numResults", "numresults", "results_per_page",
    ]
    OPTIMAL_PAGE_SIZE = 100  # request up to 100 per page when possible

    def _detect_page_size_param(all_params, slug_info, body, location):
        """Find page size param and bump to OPTIMAL_PAGE_SIZE if small."""
        for ps_param in PAGE_SIZE_PARAMS:
            if ps_param in all_params:
                current = int(all_params.get(ps_param) or 10)
                if current < OPTIMAL_PAGE_SIZE:
                    if location == "body" and body and _is_json(body):
                        try:
                            bd = json.loads(body)
                            bd[ps_param] = OPTIMAL_PAGE_SIZE
                            slug_info["body"] = json.dumps(bd)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    else:
                        slug_info["params"][ps_param] = str(OPTIMAL_PAGE_SIZE)
                    return ps_param, OPTIMAL_PAGE_SIZE
                return ps_param, current
        return None, page1_size

    for param in OFFSET_PARAMS:
        if param in all_params:
            location = ("body" if body and param in (
                json.loads(body) if _is_json(body or "") else {}
            ) else "url")
            ps_param, effective_page_size = _detect_page_size_param(
                all_params, slug_info, body, location
            )
            return {
                "type":            "offset",
                "param":           param,
                "page_size":       effective_page_size,
                "page_size_param": ps_param,
                "location":        location,
                "total_field":     _find_total_field(data),
            }

    for param in PAGE_PARAMS:
        if param in all_params:
            location = ("body" if body and param in (
                json.loads(body) if _is_json(body or "") else {}
            ) else "url")
            ps_param, effective_page_size = _detect_page_size_param(
                all_params, slug_info, body, location
            )
            return {
                "type":            "page",
                "param":           param,
                "page_size":       effective_page_size,
                "page_size_param": ps_param,
                "location":        location,
                "total_field":     _find_total_field(data),
            }

    if isinstance(data, dict):
        for field in CURSOR_FIELDS:
            if field in data and data[field]:
                return {
                    "type":         "cursor",
                    "cursor_field": field,
                    "cursor_param": _guess_cursor_param(slug_info),
                    "page_size":    page1_size,
                }

    return {"type": "none", "page_size": page1_size}


def _find_total_field(data, depth=0, prefix=""):
    """
    Recursively search for a total job count field.
    Returns dot-separated path like "data.totalResults", not just the key name.
    """
    if depth > 3 or not isinstance(data, dict):
        return None

    hint_set = {t.replace("_", "") for t in TOTAL_HINTS}

    for k, v in data.items():
        kn       = k.lower().replace("_", "")
        fullpath = f"{prefix}.{k}" if prefix else k

        if isinstance(v, int) and v > 0 and kn in hint_set:
            return fullpath
        if isinstance(v, dict):
            resolved = _resolve_protobuf_long(v)
            if isinstance(resolved, int) and resolved > 0 and kn in hint_set:
                return fullpath
            r = _find_total_field(v, depth + 1, fullpath)
            if r:
                return r

    return None


def _guess_cursor_param(slug_info):
    params = slug_info.get("params", {})
    for p in ("cursor", "after", "nextCursor", "pageToken"):
        if p in params:
            return p
    return "cursor"


# ─────────────────────────────────────────
# TOTAL COUNT
# ─────────────────────────────────────────

def _detect_total(raw_bytes, slug_info):
    """
    Extract total job count from a page-1 response.

    Priority:
      1. Cached total_field from pagination config (fastest — set on first run
         by _detect_structure via AI or pattern detection)
      2. Protobuf-aware pattern scan across all hint-matching fields
      3. Returns None — _paginate handles missing total via exhaustion mode

    AI-based total field detection happens earlier in _detect_structure.
    By the time this is called the total_field is already cached in
    slug_info["pagination"]["total_field"] on all runs after the first.
    """
    if not isinstance(raw_bytes, bytes):
        return None
    _, data = _detect_format(raw_bytes)
    if not isinstance(data, dict):
        return None

    def _resolve_field_value(obj, field_name):
        """Walk dot-separated path and resolve protobuf Longs."""
        val = _deep_get(obj, field_name)
        if isinstance(val, int) and val > 0:
            return val
        if isinstance(val, dict):
            resolved = _resolve_protobuf_long(val)
            if isinstance(resolved, int) and resolved > 0:
                return resolved
        return None

    # ── 1. Cached total_field ───────────────────────────────────
    total_field = slug_info.get("pagination", {}).get("total_field")
    if total_field:
        val = _resolve_field_value(data, total_field)
        if val:
            return val

    # ── 2. Protobuf-aware pattern scan ──────────────────────────
    def search(obj, depth=0, prefix=""):
        if depth > 3 or not isinstance(obj, dict):
            return None, None
        hint_set = {t.replace("_", "") for t in TOTAL_HINTS}
        for k, v in obj.items():
            kn       = k.lower().replace("_", "")
            fullpath = f"{prefix}.{k}" if prefix else k
            if isinstance(v, int) and v > 0 and kn in hint_set:
                return v, fullpath
            if isinstance(v, dict):
                resolved = _resolve_protobuf_long(v)
                if isinstance(resolved, int) and resolved > 0 and kn in hint_set:
                    return resolved, fullpath
                r_val, r_key = search(v, depth + 1, fullpath)
                if r_val:
                    return r_val, r_key
        return None, None

    total, found_field = search(data)
    if total:
        if found_field and not total_field:
            pagination = slug_info.get("pagination", {})
            pagination["total_field"] = found_field
            slug_info["pagination"] = pagination
        return total

    # ── 3. None — exhaustion mode in _paginate ──────────────────
    logger.debug(
        "custom_career: could not detect total for %r — "
        "exhaustion pagination will be used",
        slug_info.get("_company", "unknown"),
    )
    return None


def _deep_get(data, field_path):
    parts   = field_path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _resolve_protobuf_long(v):
    """
    Convert a protobuf-style Long dict {low, high, unsigned} to a Python int.
    Returns the int if recognised, otherwise returns v unchanged.

    Protobuf JS Longs split a 64-bit integer into two 32-bit words.
    For all realistic job counts high==0, so the result is just low.
    We combine both words to be correct for any value.

    Examples:
        {"low": 1287, "high": 0, "unsigned": false}  -> 1287
        {"low": 0, "high": 1, "unsigned": false}      -> 4294967296
    """
    if not isinstance(v, dict):
        return v
    keys = {k.lower() for k in v}
    if "low" not in keys or "high" not in keys:
        return v
    try:
        low    = int(v.get("low")  or v.get("Low")  or 0)
        high   = int(v.get("high") or v.get("High") or 0)
        result = (high << 32) | (low & 0xFFFFFFFF)
        return result if result > 0 else v
    except (TypeError, ValueError):
        return v


# ─────────────────────────────────────────
# PAGINATION
# ─────────────────────────────────────────

def _paginate(session, slug_info, first_page_jobs, total, company, initial_page = 0):
    """
    Fetch all pages beyond page 1.

    Two modes:
      - total is known: stop when offset >= total (no overfetch)
      - total is None:  exhaustion mode — keep fetching until an empty
                        page or a page whose fingerprints are all already
                        seen. Handles any case where total is unavailable
                        regardless of why (novel protobuf format, API omits
                        it, AI returned null for it, etc.)
    """
    pagination      = slug_info.get("pagination", {})
    pag_type        = pagination.get("type", "none")
    configured_size = pagination.get("page_size", 10)
    actual_size     = len(first_page_jobs)
    page_size       = actual_size or configured_size

    # If page 1 returned fewer than configured page size,
    # we already have all jobs — no need to paginate
    if actual_size > 0 and actual_size < configured_size and pag_type == "offset":
        logger.debug(
            "custom_career: page 1 returned %d < configured %d — "
            "single page result for %r",
            actual_size, configured_size, company
        )
        return []

    all_extra       = []
    seen_ids        = _job_fingerprints(first_page_jobs)
    offset          = page_size
    page_num        = initial_page + 1
    cursor          = None
    exhaustion_mode = (total is None)

    if exhaustion_mode:
        logger.info(
            "custom_career: total unknown for %r — "
            "using exhaustion pagination (stop on empty page)",
            company
        )

    for _ in range(MAX_PAGES - 1):
        collected = len(first_page_jobs) + len(all_extra)
        if collected >= MAX_JOBS:
            break
        if not exhaustion_mode and offset >= total:
            break

        raw = _fetch_page(
            session, slug_info, page=page_num,
            offset=offset, cursor=cursor
        )
        if raw is None:
            logger.warning(
                "custom_career: page %d failed for %r — stopping",
                page_num, company
            )
            break

        jobs_arr = _extract_jobs_array(raw, slug_info)
        if not jobs_arr:
            logger.debug(
                "custom_career: empty page %d for %r — pagination complete",
                page_num, company
            )
            break

        new_ids = _job_fingerprints(jobs_arr)
        if new_ids and new_ids.issubset(seen_ids):
            logger.debug(
                "custom_career: pagination loop at page %d for %r",
                page_num, company
            )
            break

        seen_ids.update(new_ids)
        all_extra.extend(jobs_arr)

        logger.info(
            "custom_career: page %d — %d jobs (total so far %d) for %r",
            page_num, len(jobs_arr),
            len(first_page_jobs) + len(all_extra), company
        )

        offset   += len(jobs_arr)
        page_num += 1

        if pag_type == "cursor":
            _, cursor_data = _detect_format(raw)
            cursor_field   = pagination.get("cursor_field", "nextCursor")
            cursor = (
                (cursor_data or {}).get(cursor_field)
                if isinstance(cursor_data, dict) else None
            )
            if not cursor:
                break

        time.sleep(PAGE_RETRY_DELAY)

    return all_extra


def _walk_path(obj, path):
    """
    Walk a dot-separated path into a nested dict.
    Returns the scalar value at the path, or None if not found.

    Examples:
        _walk_path(job, "title")                    -> "Software Engineer"
        _walk_path(job, "city_info.en_name")        -> "Singapore"
        _walk_path(job, "city_info.parent.en_name") -> "Singapore"
    """
    if not path or obj is None:
        return None
    parts = path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and current:
            # If we hit a list, try first element
            current = current[0].get(part) if isinstance(current[0], dict) else None
        else:
            return None
        if current is None:
            return None
    # Only return scalar values — not dicts or lists
    if isinstance(current, (str, int, float, bool)):
        return current
    return None


def _job_fingerprints(jobs_arr):
    fps = set()
    for job in jobs_arr:
        if not isinstance(job, dict):
            continue
        for k, v in job.items():
            if isinstance(v, (int, str)) and str(v).strip():
                fps.add(f"{k}:{str(v)[:60]}")
                break
    return fps


# ─────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────

def _normalize(raw, company, field_map, source_url, slug_info=None):
    if not isinstance(raw, dict):
        return None

    title = _walk_path(raw, field_map.get("title")) or ""
    if not isinstance(title, str) or not title.strip():
        return None

    # job_url
    job_url_raw = _walk_path(raw, field_map.get("job_url"))
    job_url     = _extract_url(job_url_raw, source_url) if job_url_raw else ""

    # location — city, state, country with deduplication
    city    = str(_walk_path(raw, field_map.get("location_city"))    or "").strip()
    state   = str(_walk_path(raw, field_map.get("location_state"))   or "").strip()
    country = str(_walk_path(raw, field_map.get("location_country")) or "").strip()

    # Fallback: if field_map uses old-style "location" key (pattern detection fallback)
    if not city and not state and not country and field_map.get("location"):
        raw_loc = raw.get(field_map["location"])
        city = _extract_location(raw_loc)

    # Deduplicate — some APIs repeat same string at city/state/country level
    location_parts = []
    seen_parts     = set()
    for part in (city, state, country):
        part_lower = part.lower()
        if part and part_lower not in seen_parts:
            seen_parts.add(part_lower)
            location_parts.append(part)
    location = ", ".join(location_parts)

    # posted_at
    posted_at = _extract_date(_walk_path(raw, field_map.get("posted_at")))

    # job_id
    job_id = _walk_path(raw, field_map.get("job_id"))
    if not job_id and job_url:
        job_id = _extract_id_from_path(job_url)

    # job_url construction priority:
    # 1. Direct from listing API response field (job_url path in field_map)
    # 2. Detail curl template (slug_info["detail"]["url_template"])
    # 3. AI-detected template (slug_info["job_url_template"])
    # 4. Inferred template from career_page_url/_detect_structure
    # 5. Existing saved URL pattern from DB (runtime last resort)

    if not job_url and job_id:
        template = ""
        if slug_info:
            # Priority 1: detail curl template (captured from real request — most reliable)
            template = (
                (slug_info.get("detail") or {}).get("url_template")
                or (slug_info.get("detail") or {}).get("listing_url_template")
                # Priority 2: AI-detected or inferred template (fallback)
                or slug_info.get("job_url_template")
                or ""
            )
        if template and "{job_id}" in template:
            job_url = template.replace("{job_id}", str(job_id))

        # Priority 3: DB fallback
        if not job_url and slug_info:
            existing = _get_sample_url_for_company(slug_info.get("_company", ""))
            if existing:
                existing_id_match = re.search(
                    r'/([A-Za-z0-9_\-]{4,50})(?:/|$|\?)', existing
                )
                if existing_id_match:
                    old_id  = existing_id_match.group(1)
                    job_url = existing.replace(old_id, str(job_id), 1)

    # description — use from listing if present and substantial
    description = ""
    desc_path   = field_map.get("description")
    if desc_path:
        desc_val = _walk_path(raw, desc_path)
        if desc_val and isinstance(desc_val, str) and len(desc_val) > 50:
            description = _clean_html(desc_val)

    return {
        "company":     company,
        "title":       str(title).strip(),
        "job_url":     job_url or "",
        "job_id":      str(job_id).strip() if job_id is not None else "",
        "location":    location,
        "posted_at":   posted_at,
        "description": description,
        "ats":         "custom",
    }


def _infer_job_url_template(slug_info, sample_jobs, field_map):
    """
    Infer job URL template when AI returns null and no detail curl exists.

    Priority:
      1. sample_job_url + job_id cross-reference (most accurate)
         Find which sample job_id appears in the sample URL, replace with {job_id}
      2. career_page_url path append
         e.g. https://lifeattiktok.com/search → https://lifeattiktok.com/search/{job_id}
      3. API subdomain stripping
         e.g. api.lifeattiktok.com → lifeattiktok.com/search/{job_id}

    Returns template string with {job_id} or None.
    """
    sample_job_url  = slug_info.get("sample_job_url", "")
    career_page_url = slug_info.get("career_page_url", "")
    api_url         = slug_info.get("url", "")

    if not sample_jobs or not field_map.get("job_id"):
        return None

    # Collect sample job IDs from the first few jobs
    sample_ids = []
    for job in sample_jobs[:5]:
        val = _walk_path(job, field_map.get("job_id"))
        if val:
            sample_ids.append(str(val))

    if not sample_ids:
        return None

    # ── Strategy 1: sample_job_url + known job IDs ───────────────
    # Best approach: find which job ID appears in the sample URL,
    # then replace it with {job_id} to get the template.
    # Works for TikTok: sample URL contains the numeric id "7331234567890",
    # matches it against job dict fields, builds the template.
    if sample_job_url and sample_job_url.strip():
        for sid in sample_ids:
            if sid in sample_job_url:
                # Strip query params (session-specific noise)
                parsed   = urlparse(sample_job_url)
                clean    = urlunparse(parsed._replace(query="", fragment=""))
                template = clean.replace(sid, "{job_id}", 1)
                logger.info(
                    "custom_career: inferred template from sample_job_url "
                    "for %r: %r (matched id=%r)",
                    slug_info.get("_company"), template, sid
                )
                return template
        # sample_job_url provided but none of our job IDs appear in it
        # — either wrong field mapped as job_id, or URL uses different ID
        logger.debug(
            "custom_career: sample_job_url provided but no job_id "
            "value found in it for %r — trying career_page_url",
            slug_info.get("_company")
        )

    # ── Strategy 2: career_page_url path ─────────────────────────
    # e.g. https://lifeattiktok.com/search?... → https://lifeattiktok.com/search/{job_id}
    # Only use if sample_id looks like a valid URL path segment
    if career_page_url and sample_ids:
        sid    = sample_ids[0]
        parsed = urlparse(career_page_url)
        base   = f"{parsed.scheme}://{parsed.netloc}"
        path   = parsed.path.rstrip("/")

        if re.match(r'^[A-Za-z0-9_\-]{3,50}$', sid):
            template = f"{base}{path}/{{job_id}}" if path else f"{base}/{{job_id}}"
            logger.debug(
                "custom_career: inferred template from career_page_url "
                "for %r: %r",
                slug_info.get("_company"), template
            )
            return template

    # ── Strategy 3: strip API subdomain ──────────────────────────
    # e.g. api.lifeattiktok.com → lifeattiktok.com
    if api_url and sample_ids:
        sid         = sample_ids[0]
        parsed      = urlparse(api_url)
        career_host = re.sub(
            r'^(?:api|jobs-api|careers-api|apply)\.',
            '', parsed.netloc
        )
        if career_host != parsed.netloc and re.match(r'^[A-Za-z0-9_\-]{3,50}$', sid):
            template = f"{parsed.scheme}://{career_host}/search/{{job_id}}"
            logger.debug(
                "custom_career: inferred template from API domain "
                "for %r: %r",
                slug_info.get("_company"), template
            )
            return template

    return None
# _extract_url imported from jobs.utils as extract_url_from_value


def _extract_location(val):
    if isinstance(val, str):
        v = val.strip()
        # Try parsing JSON-encoded location object (Amazon locations array)
        if v.startswith("{"):
            try:
                parsed = json.loads(v)
                return _extract_location(parsed)
            except (json.JSONDecodeError, TypeError):
                pass
        return v
    if isinstance(val, list) and val:
        parts = []
        for item in val:
            if isinstance(item, str) and item.strip():
                loc = _extract_location(item)  # handles JSON strings
                if loc:
                    parts.append(loc)
            elif isinstance(item, dict):
                loc = _extract_location(item)
                if loc:
                    parts.append(loc)
        return ", ".join(parts[:3]) if parts else ""
    if isinstance(val, dict):
        # Amazon style: city + state name
        city  = val.get("city") or val.get("normalizedCityName", "")
        state = val.get("normalizedStateName") or val.get("region", "")
        if city and state:
            return f"{city}, {state}"
        if city:
            return city
        # Generic fallbacks
        for key in ("name", "text", "label", "display",
                    "normalizedLocation", "location",
                    "locationName", "location_name"):
            if val.get(key):
                return str(val[key]).strip()
    return ""


# _extract_date imported from jobs.utils as parse_date_value
# _extract_id_from_path imported from jobs.utils as extract_job_id_from_path


# ─────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────

def _collect_numeric_fields(raw_bytes, max_depth=2):
    """
    Collect all numeric fields from root-level response.
    Used to verify total_field detection is correct —
    stores all candidates so you can manually check in inspection table.
    Returns dict of {field_name: value}.
    """
    if not isinstance(raw_bytes, bytes):
        return {}
    try:
        _, data = _detect_format(raw_bytes)
        if not isinstance(data, dict):
            return {}
        result = {}
        def _walk(obj, depth):
            if depth > max_depth or not isinstance(obj, dict):
                return
            for k, v in obj.items():
                if isinstance(v, int) and not isinstance(v, bool) and v > 0:
                    result[k] = v
                elif isinstance(v, dict) and depth < max_depth:
                    _walk(v, depth + 1)
        _walk(data, 0)
        return result
    except Exception:
        return {}


def _save_inspection_row(company, base_url, slug_info, jobs_arr,
                         total, total_field, all_numeric_fields):
    """
    Save first-run inspection data to custom_ats_inspection table.
    Called after page 1 fetch + structure detection.
    Gives a permanent inspection point for manual verification.
    """
    try:
        from db.custom_ats_inspection import save_inspection
        field_map  = slug_info.get("field_map", {})
        pagination = slug_info.get("pagination", {})

        # Normalize first job for quick sanity check
        first_raw        = jobs_arr[0] if jobs_arr else {}
        sample_normalized = None
        if first_raw and field_map:
            sample_normalized = _normalize(
                first_raw, company, field_map, base_url
            )

            # Keep ALL keys — only shorten values over 500 chars
            truncated = {}
            for k, v in first_raw.items():
                if isinstance(v, str) and len(v) > 500:
                    truncated[k] = v[:500] + "...[truncated]"
                else:
                    truncated[k] = v
            raw_preview = json.dumps(truncated, indent=2)

        save_inspection(
            company            = company,
            listing_url        = base_url,
            fmt                = slug_info.get("format"),
            array_path         = slug_info.get("array_path"),
            total_jobs         = total,
            total_field        = total_field,
            all_numeric_fields = all_numeric_fields,
            page_size          = pagination.get("page_size"),
            pagination         = pagination,
            session_strategy   = slug_info.get("session_strategy"),
            first_job_raw      = raw_preview,  # first_raw,
            field_map          = field_map,
            sample_normalized  = sample_normalized,
        )
        logger.debug(
            "custom_career: inspection saved for %r "
            "total=%s total_field=%s",
            company, total, total_field
        )
    except Exception as e:
        # Never let inspection writes crash the main pipeline
        logger.debug(
            "custom_career: inspection save failed for %r: %s",
            company, e
        )


def _save_enriched_config(company, slug_info):
    """Save enriched slug_info back to DB."""
    try:
        from db.connection import get_conn
        conn = get_conn()
        conn.execute(
            "UPDATE prospective_companies SET ats_slug = ? "
            "WHERE company = ?",
            (json.dumps(slug_info), company)
        )
        conn.commit()
        conn.close()
        logger.debug("custom_career: saved config for %r", company)
    except Exception as e:
        logger.warning(
            "custom_career: could not save config: %s", e
        )


def _flag_diagnostic(company, step, severity, pattern_hint=None,
                     raw_response=None, notes=None):
    """
    Write a diagnostic row for any custom ATS failure or unknown pattern.
    Uses flag_diagnostic_once() so repeated daily runs don't flood the table.
    Also creates a pipeline_alert for blocked severity.
    """
    try:
        from db.custom_ats_diagnostics import (
            flag_diagnostic_once, BLOCKED
        )
        row_id = flag_diagnostic_once(
            company      = company,
            step         = step,
            severity     = severity,
            pattern_hint = pattern_hint,
            raw_response = raw_response,
            notes        = notes,
        )

        # Only create pipeline alert for blocked issues
        # and only when a new diagnostic row was written
        if row_id and severity == BLOCKED:
            try:
                from db.pipeline_alerts import create_alert
                create_alert(
                    alert_type = "custom_ats_blocked",
                    severity   = "warning",
                    platform   = "custom",
                    value      = 0,
                    threshold  = 0,
                    message    = (
                        f"{company}: custom ATS blocked — "
                        f"step={step} hint={pattern_hint}. "
                        f"Run: python pipeline.py --diagnostics"
                    ),
                )
            except Exception:
                pass

        logger.warning(
            "custom_career: diagnostic flagged for %r "
            "step=%s severity=%s hint=%s",
            company, step, severity, pattern_hint
        )
    except Exception as e:
        logger.debug(
            "custom_career: could not write diagnostic: %s", e
        )


def _flag_expired(company):
    """
    Legacy wrapper — kept for compatibility with set_custom_ats.py.
    Routes to _flag_diagnostic.
    """
    _flag_diagnostic(
        company      = company,
        step         = "auth_error",
        severity     = "blocked",
        pattern_hint = "auth_failed_session_refresh",
        notes        = (
            f"{company}: custom ATS session refresh failed — "
            f"career page may have changed its auth flow. "
            f"Re-capture the curl and re-run: "
            f"python pipeline.py --set-custom-ats "
            f'"{company}" --curl "..."'
        ),
    )

def _get_sample_url_for_company(company):
    """Get one existing job URL for this company from DB."""
    if not company:
        return None
    try:
        from db.connection import get_conn
        conn = get_conn()
        row  = conn.execute(
            "SELECT job_url FROM job_postings "
            "WHERE company = ? AND job_url != '' "
            "LIMIT 1",
            (company,)
        ).fetchone()
        conn.close()
        return row["job_url"] if row else None
    except Exception:
        return None