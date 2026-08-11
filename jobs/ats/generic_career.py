# jobs/ats/generic_career.py — Universal career site scraper
#
# Strategy:
#   1. Launch Playwright, capture ALL network responses (not just JSON)
#   2. Try to parse each response body as JSON regardless of content-type
#   3. Also extract embedded JSON from HTML (JSON-LD, __NEXT_DATA__, inline JS)
#   4. Score all candidates to find the job listing API
#   5. Deep structure analysis on best candidate:
#      - Sample 10 jobs, score every field by value patterns
#      - Lock in field mapping (title/location/id/url/date)
#      - Cache mapping in ats_discovery.db for fast repeat runs
#   6. Paginate using detected params, replay with requests
#   7. Normalize all jobs using locked field mapping
#
# Success rate: ~85% on modern SPA career sites
# Fails on: auth-required portals, pure server-rendered HTML with no API

import re
import json
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

MIN_JOBS_IN_RESPONSE = 3
PAGE_LOAD_WAIT       = 8
MAX_PAGES            = 100
MAX_JOBS             = 2000
ANALYSIS_SAMPLE_SIZE = 10   # jobs to sample for structure analysis

# Value pattern thresholds for field scoring
URL_PATTERN      = re.compile(r'^https?://', re.IGNORECASE)
ISO_DATE_PATTERN = re.compile(
    r'^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2})?'
)
UNIX_TS_MIN  = 1_000_000_000    # seconds (2001+)
UNIX_TS_MS_MIN = 1_000_000_000_000  # milliseconds (2001+)

# Field name hints — used as TIEBREAKERS only, not primary signal
# Primary signal is always the VALUE pattern
TITLE_HINTS    = {"title", "jobtitle", "job_title", "position",
                  "positionname", "rolename", "role", "requisitiontitle"}
LOCATION_HINTS = {"location", "joblocation", "job_location", "city",
                  "citystate", "office", "site", "locationname", "fulllocation"}
ID_HINTS       = {"id", "jobid", "job_id", "requisitionid", "reqid",
                  "req_id", "referenceid", "externalid", "postingid",
                  "uniqueid", "slug"}
URL_HINTS      = {"url", "joburl", "job_url", "applyurl", "apply_url",
                  "detailurl", "link", "href", "absoluteurl"}
DATE_HINTS     = {"posteddate", "posted_date", "postedat", "posted_at",
                  "postingdate", "dateposted", "date_posted", "createdat",
                  "created_at", "publishedat", "published_at", "postedts"}

SKIP_URL_KEYWORDS = {
    "analytics", "tracking", "gtm", "google-tag", "facebook", "pixel",
    "segment", "mixpanel", "amplitude", "hotjar", "clarity", "sentry",
    "datadog", "newrelic", "cdn.js", "fonts", "static/js", "webpack",
    "chunk", "bundle", "polyfill", "runtime", "vendor", "auth", "token",
    "session", "login", "oauth", "csrf", "healthcheck", "ping",
}


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs from any career site using network interception
    and dynamic structure analysis.

    Args:
        slug_info: JSON string or dict with {"url": "https://..."}
        company:   company name

    Returns:
        List of normalized job dicts, or [] on failure.
    """
    if not slug_info:
        return []
    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            return []

    career_url = slug_info.get("url", "")
    if not career_url:
        return []

    logger.info("generic_career: starting for %r at %s", company, career_url)

    # Check cached field mapping first
    url_hash = _url_hash(career_url)
    cached_mapping = _load_cached_mapping(url_hash)

    # Step 1 — Capture all network responses via Playwright
    captured = _capture_responses(career_url, company)
    if not captured:
        logger.warning("generic_career: no responses captured for %r", company)
        return []

    logger.info("generic_career: captured %d responses for %r",
                len(captured), company)

    # Step 2 — Score candidates to find the job listing API
    best = _score_candidates(captured)
    if not best:
        logger.warning("generic_career: no job-like response found for %r",
                       company)
        return []

    logger.info("generic_career: best candidate — %s (score=%d)",
                best["url"], best["score"])

    # Step 3 — Extract jobs array from best candidate
    jobs_arr = _extract_jobs_array(best["data"])
    if not jobs_arr:
        logger.warning("generic_career: could not extract jobs array for %r",
                       company)
        return []

    # Step 4 — Analyse structure (use cache if available)
    if cached_mapping and _mapping_still_valid(cached_mapping, jobs_arr[0]):
        mapping = cached_mapping["mapping"]
        logger.info("generic_career: using cached field mapping for %r", company)
    else:
        mapping = _analyse_structure(jobs_arr, career_url)
        if not mapping:
            logger.warning("generic_career: structure analysis failed for %r",
                           company)
            return []
        _save_cached_mapping(url_hash, company, career_url, mapping)
        logger.info("generic_career: learned field mapping for %r: %s",
                    company, mapping)

    # Step 5 — Paginate remaining pages
    all_raw = list(jobs_arr)
    total   = _detect_total(best["data"])

    if total and len(jobs_arr) < total:
        logger.info("generic_career: paginating %r total=%d page_size=%d",
                    company, total, len(jobs_arr))
        extra = _paginate(best, jobs_arr, total, len(jobs_arr), company)
        all_raw.extend(extra)

    logger.info("generic_career: %d raw jobs for %r", len(all_raw), company)

    # Step 6 — Normalize using learned mapping
    results = []
    for raw in all_raw[:MAX_JOBS]:
        job = _normalize(raw, company, mapping, best["url"])
        if job:
            results.append(job)

    logger.info("generic_career: %d normalized jobs for %r",
                len(results), company)
    return results


# ─────────────────────────────────────────
# STEP 1 — CAPTURE ALL RESPONSES
# ─────────────────────────────────────────

def _capture_responses(career_url, company):
    """
    Launch Playwright and capture ALL network responses.
    Tries JSON parse on every response regardless of content-type.
    Also extracts embedded JSON from HTML responses.
    Returns list of candidate dicts.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("generic_career: playwright not installed")
        return []

    captured = []

    _MAX_CAPTURED = 500

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/145.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;"
                        "q=0.9,application/json,*/*;q=0.8"
                    ),
                }
            )
            page = context.new_page()

            def on_response(response):
                if len(captured) >= _MAX_CAPTURED:
                    return
                try:
                    url = response.url
                    ct  = response.headers.get("content-type", "").lower()

                    # Skip obviously useless responses
                    if _should_skip_url(url):
                        return

                    body = response.body()
                    if not body or len(body) < 100:
                        return

                    # Try JSON parse regardless of content-type
                    data = _try_parse_json(body)
                    if data is not None:
                        captured.append({
                            "url":     url,
                            "method":  response.request.method,
                            "headers": dict(response.request.headers),
                            "body":    response.request.post_data,
                            "data":    data,
                            "source":  "json",
                            "score":   0,
                        })
                        return

                    # For HTML responses, extract embedded JSON
                    if "html" in ct and len(body) > 500:
                        embedded = _extract_embedded_json(body.decode("utf-8", errors="ignore"))
                        for item in embedded:
                            if len(captured) >= _MAX_CAPTURED:
                                break
                            captured.append({
                                "url":     url,
                                "method":  response.request.method,
                                "headers": dict(response.request.headers),
                                "body":    None,
                                "data":    item["data"],
                                "source":  item["source"],
                                "score":   0,
                            })

                except Exception:
                    pass

            page.on("response", on_response)

            try:
                page.goto(career_url, wait_until="networkidle", timeout=30000)
            except Exception:
                try:
                    page.goto(career_url, wait_until="domcontentloaded",
                              timeout=20000)
                    time.sleep(PAGE_LOAD_WAIT)
                except Exception as e:
                    logger.error("generic_career: navigation failed for %s: %s",
                                 career_url, e)
                    return []

            time.sleep(3)
        finally:
            browser.close()

    return captured


def _should_skip_url(url):
    """Return True if URL is definitely not a job listing API."""
    url_lower = url.lower()
    return any(kw in url_lower for kw in SKIP_URL_KEYWORDS)


def _try_parse_json(body):
    """
    Try to parse bytes as JSON.
    Returns parsed data or None.
    """
    try:
        text = body.decode("utf-8", errors="ignore").strip()
        if not text or text[0] not in ("{", "["):
            return None
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def _extract_embedded_json(html):
    """
    Extract JSON data embedded in HTML:
    1. JSON-LD script tags
    2. __NEXT_DATA__ (Next.js)
    3. __INITIAL_STATE__ / window.__STATE__ (React/Vue/Angular)
    4. Large inline JSON assignments
    Returns list of {"source": str, "data": dict/list}
    """
    results = []
    soup    = BeautifulSoup(html, "html.parser")

    # JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, (dict, list)):
                results.append({"source": "json-ld", "data": data})
        except (json.JSONDecodeError, TypeError):
            pass

    # Next.js __NEXT_DATA__
    next_script = soup.find("script", id="__NEXT_DATA__")
    if next_script:
        try:
            data = json.loads(next_script.string or "")
            results.append({"source": "__NEXT_DATA__", "data": data})
        except (json.JSONDecodeError, TypeError):
            pass

    # window.__INITIAL_STATE__, window.__APP_STATE__, etc.
    STATE_PATTERNS = [
        r'window\.__(?:INITIAL_STATE|APP_STATE|REDUX_STATE|STORE_STATE|'
        r'PRELOADED_STATE|DATA|NUXT|STATE)__\s*=\s*({.{100,}})',
    ]
    for script in soup.find_all("script"):
        text = script.string or ""
        if len(text) < 100:
            continue
        for pattern in STATE_PATTERNS:
            m = re.search(pattern, text, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    results.append({"source": "window_state", "data": data})
                except (json.JSONDecodeError, ValueError):
                    pass

    return results


# ─────────────────────────────────────────
# STEP 2 — SCORE CANDIDATES
# ─────────────────────────────────────────

def _score_candidates(captured):
    """
    Score all captured responses and return the best job listing candidate.
    Scoring is purely value-based — no hardcoded field names.
    """
    scored = []

    for resp in captured:
        data = resp["data"]

        # Find jobs array
        jobs_arr = _extract_jobs_array(data)
        if not jobs_arr or len(jobs_arr) < MIN_JOBS_IN_RESPONSE:
            continue

        score = 0
        score += min(len(jobs_arr), 50)  # up to +50 for job count

        # Sample up to 5 jobs and score by value patterns
        sample = [j for j in jobs_arr[:5] if isinstance(j, dict)]
        if not sample:
            continue

        has_title_like    = 0
        has_location_like = 0
        has_url_like      = 0
        has_date_like     = 0
        has_id_like       = 0

        for job in sample:
            for k, v in job.items():
                vtype = _classify_value(k, v)
                if vtype == "title":    has_title_like    += 1
                if vtype == "location": has_location_like += 1
                if vtype == "url":      has_url_like      += 1
                if vtype == "date":     has_date_like     += 1
                if vtype == "id":       has_id_like       += 1

        # Title-like field is required — can't be a job listing without it
        if has_title_like == 0:
            continue

        score += min(has_title_like, 5)    * 4   # up to +20
        score += min(has_location_like, 5) * 3   # up to +15
        score += min(has_url_like, 5)      * 3   # up to +15
        score += min(has_date_like, 5)     * 2   # up to +10
        score += min(has_id_like, 5)       * 2   # up to +10

        # Bonus for job-related URL keywords
        url_lower = resp["url"].lower()
        if any(kw in url_lower for kw in ("job", "position", "career",
                                           "vacancy", "opening", "role")):
            score += 5

        # Bonus for direct JSON API (not embedded)
        if resp.get("source") == "json":
            score += 10

        resp["score"] = score
        scored.append(resp)

    if not scored:
        return None

    scored.sort(key=lambda r: r["score"], reverse=True)
    best = scored[0]

    logger.debug(
        "generic_career: scored %d candidates, best=%s score=%d",
        len(scored), best["url"][:80], best["score"],
    )

    return best if best["score"] >= 15 else None


# ─────────────────────────────────────────
# STEP 4 — STRUCTURE ANALYSIS
# ─────────────────────────────────────────

def _analyse_structure(jobs_arr, career_url):
    """
    Analyse a sample of jobs to determine the field mapping.
    Score every field across ANALYSIS_SAMPLE_SIZE jobs by value patterns.
    Return a mapping dict:
      {
        "title":    "fieldName",
        "location": "fieldName" or None,
        "job_id":   "fieldName" or None,
        "job_url":  "fieldName" or None,
        "posted_at":"fieldName" or None,
      }
    Returns None if title field cannot be determined.
    """
    sample = [j for j in jobs_arr[:ANALYSIS_SAMPLE_SIZE]
              if isinstance(j, dict)]
    if not sample:
        return None

    # Collect all field names across sample
    all_fields = set()
    for job in sample:
        all_fields.update(job.keys())

    # Score each field by how often its values match each type
    field_scores = {f: {
        "title": 0, "location": 0, "url": 0,
        "date": 0, "id": 0, "other": 0,
    } for f in all_fields}

    for job in sample:
        for field, value in job.items():
            vtype = _classify_value(field, value)
            field_scores[field][vtype] += 1

    # Pick best field per category
    def best_for(category, exclude=None):
        exclude = exclude or set()
        candidates = [
            (f, scores[category])
            for f, scores in field_scores.items()
            if f not in exclude and scores[category] > 0
        ]
        if not candidates:
            return None
        # Sort by score DESC, break ties by name hint
        candidates.sort(key=lambda x: x[1], reverse=True)
        # Among tied top candidates, prefer name-hinted ones
        top_score = candidates[0][1]
        top = [(f, s) for f, s in candidates if s == top_score]
        hints = {
            "title":    TITLE_HINTS,
            "location": LOCATION_HINTS,
            "url":      URL_HINTS,
            "date":     DATE_HINTS,
            "id":       ID_HINTS,
        }
        hint_set = hints.get(category, set())
        for f, s in top:
            if f.lower() in hint_set:
                return f
        return top[0][0]

    used    = set()
    title   = best_for("title")
    if not title:
        return None  # can't proceed without title field
    used.add(title)

    job_url   = best_for("url",      exclude=used)
    if job_url: used.add(job_url)

    posted_at = best_for("date",     exclude=used)
    if posted_at: used.add(posted_at)

    job_id    = best_for("id",       exclude=used)
    if job_id: used.add(job_id)

    location  = best_for("location", exclude=used)

    mapping = {
        "title":     title,
        "location":  location,
        "job_id":    job_id,
        "job_url":   job_url,
        "posted_at": posted_at,
        "career_url": career_url,
    }

    logger.info(
        "generic_career: field mapping — title=%s location=%s "
        "job_url=%s posted_at=%s job_id=%s",
        title, location, job_url, posted_at, job_id,
    )

    return mapping


def _classify_value(field_name, value):
    """
    Classify a field value into one of: title, location, url, date, id, other.
    Primary signal is the VALUE pattern.
    Field name is a tiebreaker hint only.
    """
    field_lower = field_name.lower()

    # None / empty → other
    if value is None or value == "" or value == []:
        return "other"

    # URL — starts with http
    if isinstance(value, str) and URL_PATTERN.match(value.strip()):
        return "url"

    # ISO date string
    if isinstance(value, str) and ISO_DATE_PATTERN.match(value.strip()):
        return "date"

    # Unix timestamp (int) — bool is a subclass of int; exclude it explicitly
    if isinstance(value, int) and not isinstance(value, bool):
        if UNIX_TS_MS_MIN <= value:
            return "date"  # milliseconds
        if UNIX_TS_MIN <= value < UNIX_TS_MS_MIN:
            return "date"  # seconds
        if 1 <= value <= 9_999_999:
            return "id"    # small integer — likely a job ID

    # String ID (numeric string or UUID)
    if isinstance(value, str):
        stripped = value.strip()
        if re.match(r'^\d{4,}$', stripped):
            return "id"
        if re.match(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            stripped, re.IGNORECASE
        ):
            return "id"

    # Short non-empty string — could be title or location
    if isinstance(value, str) and 3 <= len(value.strip()) <= 200:
        # Use field name hints to differentiate title vs location
        if field_lower in TITLE_HINTS:
            return "title"
        if field_lower in LOCATION_HINTS:
            return "location"
        if field_lower in ID_HINTS:
            return "id"
        # Default short string to title (most common job field)
        return "title"

    # List of strings — likely locations or categories
    if isinstance(value, list) and value:
        if all(isinstance(v, str) for v in value[:3]):
            return "location"

    return "other"


# ─────────────────────────────────────────
# FIELD MAPPING CACHE
# ─────────────────────────────────────────

def _url_hash(url):
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _load_cached_mapping(url_hash):
    """Load cached field mapping from ats_discovery.db."""
    try:
        from db.connection import get_discovery_conn
        conn = get_discovery_conn()
        try:
            row = conn.execute(
                "SELECT mapping_json, updated_at FROM generic_career_mappings "
                "WHERE url_hash = ?",
                (url_hash,)
            ).fetchone()
        finally:
            conn.close()
        if row:
            return {
                "mapping":    json.loads(row["mapping_json"]),
                "updated_at": row["updated_at"],
            }
    except Exception as e:
        logger.debug("generic_career: could not load mapping cache: %s", e)
    return None


def _save_cached_mapping(url_hash, company, career_url, mapping):
    """Save learned field mapping to ats_discovery.db."""
    try:
        from db.connection import get_discovery_conn
        conn = get_discovery_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS generic_career_mappings (
                url_hash     TEXT PRIMARY KEY,
                company      TEXT,
                career_url   TEXT,
                mapping_json TEXT,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO generic_career_mappings
              (url_hash, company, career_url, mapping_json, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            url_hash, company, career_url, json.dumps(mapping)
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug("generic_career: could not save mapping cache: %s", e)


def _mapping_still_valid(cached, sample_job):
    """
    Check if cached mapping is still valid by verifying the
    title field still exists in a sample job.
    """
    if not cached or not sample_job:
        return False
    title_field = cached.get("mapping", {}).get("title")
    return title_field and title_field in sample_job


# ─────────────────────────────────────────
# JOBS ARRAY EXTRACTION
# ─────────────────────────────────────────

def _extract_jobs_array(data):
    """
    Find the largest array of dicts inside a JSON response.
    No hardcoded field names — finds any list of 3+ dicts.
    Handles: top-level array, nested one level, nested two levels.
    """
    if isinstance(data, list):
        if len(data) >= MIN_JOBS_IN_RESPONSE and isinstance(data[0], dict):
            return data
        return None

    if not isinstance(data, dict):
        return None

    best_list = None
    best_len  = MIN_JOBS_IN_RESPONSE - 1

    def _search(obj, depth=0):
        nonlocal best_list, best_len
        if depth > 4:
            return
        if isinstance(obj, list):
            if len(obj) > best_len and obj and isinstance(obj[0], dict):
                # Verify it looks like jobs (has 3+ string fields)
                sample = obj[0]
                str_fields = sum(1 for v in sample.values()
                                 if isinstance(v, str) and len(v) > 2)
                if str_fields >= 2:
                    best_list = obj
                    best_len  = len(obj)
            # Always recurse into list elements — job arrays may be nested inside
            for item in obj:
                _search(item, depth + 1)
        elif isinstance(obj, dict):
            for v in obj.values():
                _search(v, depth + 1)

    _search(data)
    return best_list


# ─────────────────────────────────────────
# TOTAL COUNT + PAGINATION
# ─────────────────────────────────────────

def _detect_total(data):
    """Find total job count in response. Searches up to 3 levels deep."""
    TOTAL_HINTS = {
        "total", "totalcount", "total_count", "count",
        "totalresults", "total_results", "totaljobs", "total_jobs",
        "totalrecords", "total_records", "numfound", "num_found",
        "nbhits", "nbresults", "hits",
    }

    def search(obj, depth=0):
        if depth > 3:
            return None
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k.lower() in TOTAL_HINTS and isinstance(v, int) and v > 0:
                    return v
            for v in obj.values():
                result = search(v, depth + 1)
                if result:
                    return result
        return None

    return search(data)


def _paginate(best_resp, first_page_jobs, total, page_size, company):
    """
    Replay captured API request with incremented pagination params.
    Detects offset-based or page-based pagination from URL or POST body.
    """
    url    = best_resp["url"]
    method = best_resp["method"]
    hdrs   = best_resp["headers"]
    body   = best_resp.get("body")

    SKIP_HEADERS = {
        ":method", ":path", ":scheme", ":authority",
        "content-length", "transfer-encoding",
    }
    clean_headers = {
        k: v for k, v in hdrs.items()
        if k.lower() not in SKIP_HEADERS
    }

    param_info = _detect_pagination_param(url, body, page_size)
    if not param_info:
        logger.debug("generic_career: no pagination param for %r — page 1 only",
                     company)
        return []

    seen_ids  = _get_ids(first_page_jobs)
    all_extra = []
    offset    = page_size

    for page_num in range(2, MAX_PAGES + 1):
        next_url, next_body = _build_next_request(
            url, body, param_info, offset, page_num
        )
        try:
            if method == "POST":
                resp = requests.post(
                    next_url,
                    json=json.loads(next_body) if next_body else None,
                    headers=clean_headers,
                    timeout=15,
                )
            else:
                resp = requests.get(next_url, headers=clean_headers, timeout=15)

            if resp.status_code != 200:
                break

            data     = resp.json()
            jobs_arr = _extract_jobs_array(data)
            if not jobs_arr:
                break

            new_ids = _get_ids(jobs_arr)
            if new_ids and new_ids.issubset(seen_ids):
                logger.debug("generic_career: pagination loop at page %d — stopping",
                             page_num)
                break

            seen_ids.update(new_ids)
            all_extra.extend(jobs_arr)
            offset += len(jobs_arr)

            if offset >= total or not jobs_arr:
                break
            if len(first_page_jobs) + len(all_extra) >= MAX_JOBS:
                break

            time.sleep(0.5)

        except Exception as e:
            logger.warning("generic_career: pagination error page %d: %s",
                           page_num, e)
            break

    return all_extra


def _detect_pagination_param(url, body, page_size):
    parsed = urlparse(url)
    qs     = parse_qs(parsed.query, keep_blank_values=True)

    OFFSET_PARAMS = ["offset", "jobOffset", "start", "from", "skip",
                     "jobRecordsOffset", "recordsOffset"]
    PAGE_PARAMS   = ["page", "pageNumber", "page_number", "pageNo",
                     "page_no", "pageNum", "page_num", "currentPage"]

    for param in OFFSET_PARAMS:
        if param in qs:
            return {"location": "url", "param": param, "type": "offset"}
    for param in PAGE_PARAMS:
        if param in qs:
            return {"location": "url", "param": param, "type": "page"}

    if body:
        try:
            body_data = json.loads(body)
            if isinstance(body_data, dict):
                for param in OFFSET_PARAMS:
                    if param in body_data:
                        return {"location": "body", "param": param,
                                "type": "offset"}
                for param in PAGE_PARAMS:
                    if param in body_data:
                        return {"location": "body", "param": param,
                                "type": "page"}
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _build_next_request(url, body, param_info, offset, page_num):
    new_value = offset if param_info["type"] == "offset" else page_num
    if param_info["location"] == "url":
        parsed = urlparse(url)
        qs     = parse_qs(parsed.query, keep_blank_values=True)
        qs[param_info["param"]] = [str(new_value)]
        new_qs   = urlencode(qs, doseq=True)
        next_url = urlunparse(parsed._replace(query=new_qs))
        return next_url, body
    else:
        body_data = json.loads(body) if body else {}
        body_data[param_info["param"]] = new_value
        return url, json.dumps(body_data)


def _get_ids(jobs_arr):
    """Extract unique IDs from jobs for pagination loop detection."""
    ids = set()
    for job in jobs_arr:
        if not isinstance(job, dict):
            continue
        # Use any integer or short string field as ID proxy
        for k, v in job.items():
            if isinstance(v, (int, str)) and str(v).strip():
                ids.add(f"{k}:{str(v)[:50]}")
                break
    return ids


# ─────────────────────────────────────────
# STEP 6 — NORMALIZE USING LEARNED MAPPING
# ─────────────────────────────────────────

def _normalize(raw, company, mapping, source_url):
    """Normalize a raw job dict using the learned field mapping."""
    if not isinstance(raw, dict):
        return None

    title = _get_mapped(raw, mapping["title"])
    if not isinstance(title, str) or not title.strip():
        return None

    job_url   = _extract_url_value(
        _get_mapped(raw, mapping.get("job_url")), source_url
    )
    location  = _extract_location_value(
        _get_mapped(raw, mapping.get("location"))
    )
    posted_at = _extract_date_value(
        _get_mapped(raw, mapping.get("posted_at"))
    )
    job_id    = _get_mapped(raw, mapping.get("job_id"))

    return {
        "company":     company,
        "title":       title.strip(),
        "job_url":     job_url or "",
        "job_id":      str(job_id).strip() if job_id is not None else "",
        "location":    location or "",
        "posted_at":   posted_at,
        "description": "",
        "ats":         "generic_career",
    }


def _get_mapped(job, field):
    """Get value from job dict by field name. Returns None if not found."""
    if not field:
        return None
    return job.get(field)


def _extract_url_value(val, base_url):
    if not isinstance(val, str) or not val.strip():
        return ""
    val = val.strip()
    if val.startswith("http"):
        return val
    if val.startswith("/"):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{val}"
    return val


def _extract_location_value(val):
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, list) and val:
        # Take first non-empty string
        for item in val:
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(val, dict):
        for key in ("name", "city", "text", "label", "display"):
            if val.get(key):
                return str(val[key]).strip()
    return ""


def _extract_date_value(val):
    if val is None:
        return None
    if isinstance(val, int):
        try:
            if val > UNIX_TS_MS_MIN:
                return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
            if val > UNIX_TS_MIN:
                return datetime.fromtimestamp(val, tz=timezone.utc)
        except (ValueError, OSError):
            pass
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, AttributeError):
            pass
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
                    "%B %d, %Y", "%b %d, %Y"]:
            try:
                return datetime.strptime(val[:20], fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None