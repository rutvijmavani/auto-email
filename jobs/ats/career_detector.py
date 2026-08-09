# jobs/ats/career_detector.py — Universal ATS detector
#
# Algorithm (same logic at every page level):
#
#   for each level (career → listing → JD → apply):
#       page_text = fetch(url)
#       result = scan(page_text)          ← full raw-text keyword search
#       if result: return result
#
#       for src in script_srcs(page_text):
#           result = scan(fetch(src))     ← JS bundle scan (catches Lever/Spotify)
#           if result: return result
#
#       next_url = find_next_page(page_text, current_url)   ← scoring-based
#
# No BeautifulSoup. No Playwright. Pure requests + re.

import re
import json
import logging
from html import unescape as _html_unescape
from urllib.parse import urljoin, urlparse

from jobs.career_page import CAREER_PATHS

logger = logging.getLogger(__name__)

# ─── Chrome impersonation ─────────────────────────────────────────────────────
# curl_cffi matches Chrome's TLS fingerprint (JA3) + HTTP/2 — urllib3 is
# fingerprinted immediately by Cloudflare/Akamai even with a Chrome UA.
try:
    from curl_cffi.requests import Session as _CurlSession
    _CURL_AVAILABLE = True
except ImportError:
    import requests as _requests
    _CURL_AVAILABLE = False

try:
    import requests as _requests_plain
    from config import CF_WORKER_URL as _CF_WORKER_URL, CF_WORKER_SECRET as _CF_WORKER_SECRET
except Exception:
    _requests_plain = None
    _CF_WORKER_URL = ""
    _CF_WORKER_SECRET = ""


def _fetch_via_worker(url: str) -> tuple[str, str] | None:
    """Proxy a URL fetch through the Cloudflare probe Worker.

    Used as fallback when career site IP-blocks the OCI/local IP (429/403).
    Returns (html_text, final_url) or None.
    """
    if not _CF_WORKER_URL or not _CF_WORKER_SECRET or not _requests_plain:
        return None
    try:
        resp = _requests_plain.post(
            _CF_WORKER_URL,
            json={"url": url, "max_bytes": 131072},
            headers={"Authorization": f"Bearer {_CF_WORKER_SECRET}"},
            timeout=30,
        )
        data = resp.json()
        if data.get("error") or (data.get("status") or 0) >= 400:
            logger.debug("[detector] CF Worker: %s → error=%s status=%s",
                         url, data.get("error"), data.get("status"))
            return None
        body = data.get("body") or ""
        final_url = data.get("final_url") or url
        logger.debug("[detector] CF Worker: %s → %s (status=%s)",
                     url, final_url, data.get("status"))
        return body, final_url
    except Exception as exc:
        logger.debug("[detector] CF Worker failed for %s: %s", url, exc)
        return None

def _make_session():
    if _CURL_AVAILABLE:
        return _CurlSession(impersonate="chrome124")
    return _requests.Session()

# Headers for HTML page navigation — mirrors what Chrome sends on a user click
_NAV_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language":           "en-US,en;q=0.9",
    "Cache-Control":             "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":            "document",
    "Sec-Fetch-Mode":            "navigate",
    "Sec-Fetch-User":            "?1",
    "Sec-Ch-Ua":                 '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile":          "?0",
    "Sec-Ch-Ua-Platform":        '"Windows"',
}

# Headers for <script src> bundle fetches
_SCRIPT_HEADERS = {
    "Accept":            "*/*",
    "Accept-Language":   "en-US,en;q=0.9",
    "Sec-Fetch-Dest":    "script",
    "Sec-Fetch-Mode":    "no-cors",
    "Sec-Ch-Ua":         '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile":  "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}

# Headers for XHR/fetch API calls made by JS — same-origin CORS requests
_API_HEADERS = {
    "Accept":            "application/json, text/plain, */*",
    "Accept-Language":   "en-US,en;q=0.9",
    "Sec-Fetch-Dest":    "empty",
    "Sec-Fetch-Mode":    "cors",
    "Sec-Ch-Ua":         '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile":  "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}

try:
    from config import (
        CAREER_DETECTOR_MAX_PAGES as _MAX_PAGES,
        FETCH_TIMEOUT as _FETCH_TIMEOUT,
        CAREER_DETECTOR_MAX_JS_BUNDLES as _MAX_JS_BUNDLES,
        CAREER_DETECTOR_MAX_API_PROBES as _MAX_API_PROBES,
    )
except Exception:
    _MAX_PAGES      = 25
    _FETCH_TIMEOUT  = 15
    _MAX_JS_BUNDLES = 15
    _MAX_API_PROBES = 10

FETCH_TIMEOUT  = _FETCH_TIMEOUT
MAX_JS_BUNDLES = _MAX_JS_BUNDLES
MAX_API_PROBES = _MAX_API_PROBES

# JS bundle URLs containing these strings are analytics/infra — skip them
# NOTE: do NOT add "chunk" here — webpack app bundles are named *.chunk.js
# and those ARE the files where ATS strings live
_BUNDLE_SKIP = (
    "analytics", "tracking", "gtm", "google-tag", "fonts",
    "recaptcha", "zendesk", "intercom", "hotjar", "segment",
    "sentry", "datadog", "polyfill",
)


# ─────────────────────────────────────────────────────────────────────────────
# API endpoint discovery — static analysis of JS bundles
#
# SPAs call internal APIs to load ATS config at runtime. The endpoint URL is a
# string constant in the bundle. We find it, call it with the session (which
# already has cookies from the page visit), and scan the JSON response — exactly
# what the JS would have done, without executing any JS.
# ─────────────────────────────────────────────────────────────────────────────

# Match string literals that look like internal API paths
_API_PATH_RE = re.compile(
    r'''["'`](\/(?:api|v\d+)\/[a-zA-Z0-9_./?=&%-]{3,100})["'`]''',
    re.IGNORECASE,
)

# Only probe paths that mention career/job concepts — avoids noise
_API_CAREER_KW = frozenset((
    "career", "job", "jobs", "recruit", "apply", "hire",
    "talent", "requisition", "ats", "position", "opening",
))


def _extract_api_paths(bundle_text):
    """
    Find candidate API endpoint paths in a JS bundle by looking for string
    literals under /api/ or /v{N}/ that contain career-related keywords.
    Returns deduplicated list capped at MAX_API_PROBES.
    """
    seen = set()
    results = []
    for m in _API_PATH_RE.finditer(bundle_text):
        path = m.group(1)
        if path in seen:
            continue
        seen.add(path)
        if any(kw in path.lower() for kw in _API_CAREER_KW):
            results.append(path)
            if len(results) >= MAX_API_PROBES:
                break
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Extractor functions — called after keyword match confirms the platform
# Each takes raw text, returns {"platform": ..., "slug": ...} or None
# ─────────────────────────────────────────────────────────────────────────────

def _extract_workday(text):
    m = re.search(
        r'([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com/([a-zA-Z0-9_%-][a-zA-Z0-9_%-]*)',
        text, re.IGNORECASE,
    )
    if m:
        slug = json.dumps({"slug": m.group(1), "wd": m.group(2), "path": m.group(3)})
        return {"platform": "workday", "slug": slug}
    # alternate myworkdaysite.com domain
    m = re.search(
        r'(wd\d+)\.myworkdaysite\.com/recruiting/([a-z0-9-]+)/([a-zA-Z0-9_%-]*)',
        text, re.IGNORECASE,
    )
    if m:
        slug = json.dumps({"slug": m.group(2), "wd": m.group(1), "path": m.group(3)})
        return {"platform": "workday", "slug": slug}
    return None


def _extract_greenhouse(text):
    # Pattern 1: for= query param — most reliable, covers both embed variants:
    #   job-boards.greenhouse.io/embed/job_app?for=<slug>   (more common)
    #   job-boards.greenhouse.io/embed/job_board?for=<slug>
    m = re.search(r'greenhouse\.io[^"\'<>\s]*[?&]for=([^&"\'<>\s]+)', text, re.IGNORECASE)
    if m:
        return {"platform": "greenhouse", "slug": m.group(1)}
    # Pattern 2: path-based boards URL — boards.greenhouse.io/<slug>/jobs
    m = re.search(r'boards\.greenhouse\.io/([a-zA-Z0-9_-]+)', text, re.IGNORECASE)
    if m:
        slug = m.group(1)
        if slug.lower() not in ("embed", "js"):  # guard against misparse
            return {"platform": "greenhouse", "slug": slug}
    # Pattern 3: __NEXT_DATA__ / JSON blob with greenhouseId
    m = re.search(r'"greenhouseId"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
    if m:
        return {"platform": "greenhouse", "slug": m.group(1)}
    return None


def _extract_successfactors(text):
    # j2w.init({ssoCompanyId: ..., ssoUrl: ...}) — canonical fingerprint
    m_slug = re.search(r'["\']?ssoCompanyId["\']?\s*:\s*["\']([^"\']+)["\']', text, re.IGNORECASE)
    m_url  = re.search(
        r'["\']?ssoUrl["\']?\s*:\s*["\']https?://career(\d+)\.successfactors\.(com|eu)["\']',
        text, re.IGNORECASE,
    )
    if m_slug and m_url:
        slug = json.dumps({"slug": m_slug.group(1), "dc": m_url.group(1), "region": m_url.group(2)})
        return {"platform": "successfactors", "slug": slug}
    # Hosted SF URL: career{N}.successfactors.com/careers?company={slug}
    m = re.search(
        r'career(\d+)\.successfactors\.(com|eu)/careers?\?[^"\'<>\s]*company=([^&"\'<>\s]+)',
        text, re.IGNORECASE,
    )
    if m:
        # Filter out staging slugs
        slug_val = m.group(3)
        if any(x in slug_val.upper() for x in ("UAT", "SUAT", "DEV", "STG", "TEST")):
            return None
        slug = json.dumps({"slug": slug_val, "dc": m.group(1), "region": m.group(2)})
        return {"platform": "successfactors", "slug": slug}
    return None


def _extract_lever(text):
    m = re.search(r'(?:jobs|hire)\.lever\.co/([a-zA-Z0-9_-]+)', text, re.IGNORECASE)
    if m:
        return {"platform": "lever", "slug": m.group(1)}
    return None


def _extract_smartrecruiters(text):
    m = re.search(r'jobs\.smartrecruiters\.com/([a-zA-Z0-9_-]+)/', text, re.IGNORECASE)
    if m:
        return {"platform": "smartrecruiters", "slug": m.group(1)}
    # careers.smartrecruiters.com/{slug}
    m = re.search(r'careers\.smartrecruiters\.com/([a-zA-Z0-9_-]+)', text, re.IGNORECASE)
    if m:
        return {"platform": "smartrecruiters", "slug": m.group(1)}
    return None


def _extract_eightfold(text):
    m = re.search(r'https?://([a-z0-9][a-z0-9-]*)\.eightfold\.ai/', text, re.IGNORECASE)
    if m:
        slug = json.dumps({"slug": m.group(1), "domain": ""})
        return {"platform": "eightfold", "slug": slug}
    return None


def _extract_ashby(text):
    m = re.search(r'jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)', text, re.IGNORECASE)
    if m:
        return {"platform": "ashby", "slug": m.group(1)}
    return None


def _extract_taleo(text):
    m = re.search(r'([a-z0-9-]+)\.taleo\.net', text, re.IGNORECASE)
    if m:
        return {"platform": "taleo", "slug": m.group(1)}
    return None


def _extract_phenom(text):
    # CDN domain: cdn.phenompeople.com or {slug}.phenompeople.com
    m = re.search(r'([a-z0-9-]+)\.phenompeople\.com', text, re.IGNORECASE)
    if m and m.group(1) != "cdn":
        return {"platform": "phenom", "slug": m.group(1)}
    # cdn presence alone confirms Phenom — slug comes from patterns.py at fetch time
    return {"platform": "phenom", "slug": ""}


def _extract_talentbrew(text):
    # TalentBrew tenant auto-detected from sitemap at fetch time
    m = re.search(r'([a-z0-9-]+)\.talentbrew\.com', text, re.IGNORECASE)
    if m:
        return {"platform": "talentbrew", "slug": m.group(1)}
    return {"platform": "talentbrew", "slug": ""}


def _extract_oracle_hcm(text):
    # Primary: {tenant}.fa.{region}.oraclecloud.com/hcmUI/.../sites/{site}
    m = re.search(
        r'([a-z0-9-]+)\.fa\.([a-z0-9-]+)\.oraclecloud\.com/hcmUI/[^"\'<>\s]*sites/([a-zA-Z0-9_-]+)',
        text, re.IGNORECASE,
    )
    if m:
        slug = json.dumps({"slug": m.group(1), "region": m.group(2), "site": m.group(3)})
        return {"platform": "oracle_hcm", "slug": slug}
    # JS fingerprint: {tenant}.fa.{region}.oraclecloud.com/hcmUI/ (no /sites/)
    m = re.search(
        r'([a-z0-9-]+)\.fa\.([a-z0-9-]+)\.oraclecloud\.com/hcmUI/',
        text, re.IGNORECASE,
    )
    if m:
        slug = json.dumps({"slug": m.group(1), "region": m.group(2), "site": ""})
        return {"platform": "oracle_hcm", "slug": slug}
    return None


def _extract_avature(text):
    # Hosted Avature tenant
    m = re.search(r'([a-z0-9-]+)\.avature\.net/([a-zA-Z0-9_/-]+)', text, re.IGNORECASE)
    if m:
        return {"platform": "avature", "slug": m.group(1)}
    # Custom career page with avatureReferrerQueryParam key — confirms Avature but no slug yet
    return {"platform": "avature", "slug": ""}


def _extract_icims(text):
    m = re.search(r'([a-z0-9-]+)\.icims\.com', text, re.IGNORECASE)
    if m:
        return {"platform": "icims", "slug": m.group(1)}
    # jibecdn.com is iCIMS Jibe product
    m = re.search(r'([a-z0-9-]+)\.jibecdn\.com', text, re.IGNORECASE)
    if m:
        return {"platform": "icims", "slug": m.group(1)}
    return {"platform": "icims", "slug": ""}


def _extract_jobvite(text):
    m = re.search(r'jobs\.jobvite\.com/([a-zA-Z0-9_-]+)', text, re.IGNORECASE)
    if m:
        return {"platform": "jobvite", "slug": m.group(1)}
    return {"platform": "jobvite", "slug": ""}


# ─────────────────────────────────────────────────────────────────────────────
# Keyword → extractor table
# Adding a new ATS = one line here + one extract_* function above
# ─────────────────────────────────────────────────────────────────────────────

ATS_KEYWORDS = {
    "myworkdayjobs":             _extract_workday,
    "myworkdaysite":             _extract_workday,
    "greenhouse":                _extract_greenhouse,
    "successfactors":            _extract_successfactors,
    "j2w.init":                  _extract_successfactors,
    "lever.co":                  _extract_lever,
    "smartrecruiters":           _extract_smartrecruiters,
    "eightfold.ai":              _extract_eightfold,
    "ashbyhq":                   _extract_ashby,
    "taleo.net":                 _extract_taleo,
    "phenompeople":              _extract_phenom,
    "talentbrew":                _extract_talentbrew,
    "oraclecloud.com/hcmUI":     _extract_oracle_hcm,
    "avature.net":               _extract_avature,
    "avatureReferrerQueryParam": _extract_avature,
    "icims.com":                 _extract_icims,
    "jibecdn.com":               _extract_icims,
    "jobvite.com":               _extract_jobvite,
}

# Eightfold is treated as tentative — many companies embed it as a widget
# without being Eightfold customers. Never return it if a harder ATS is found.
_TENTATIVE_PLATFORMS = {"eightfold"}


# ─────────────────────────────────────────────────────────────────────────────
# Core scan — runs on any raw string (HTML or JS bundle)
# ─────────────────────────────────────────────────────────────────────────────

def scan(text):
    """
    Scan raw text for any ATS keyword. Returns first non-tentative match,
    or tentative match if nothing harder found.
    """
    tentative = None
    for keyword, extractor in ATS_KEYWORDS.items():
        if keyword in text:
            result = extractor(text)
            if result:
                if result["platform"] in _TENTATIVE_PLATFORMS:
                    if tentative is None:
                        tentative = result
                else:
                    return result
    return tentative


# ─────────────────────────────────────────────────────────────────────────────
# HTTP fetch — full Chrome impersonation
# ─────────────────────────────────────────────────────────────────────────────

def _sec_fetch_site(target_url, referer_url):
    """Compute Sec-Fetch-Site exactly as Chrome does."""
    if not referer_url:
        return "none"
    tp = urlparse(target_url)
    rp = urlparse(referer_url)
    if tp.netloc == rp.netloc:
        return "same-origin"
    t_root = ".".join(tp.netloc.split(".")[-2:])
    r_root = ".".join(rp.netloc.split(".")[-2:])
    if t_root == r_root:
        return "same-site"
    return "cross-site"


def _fetch(url, session, referer=None, is_script=False, is_api=False):
    """
    Fetch url with full Chrome headers. Cookie jar is managed by the session
    automatically — same as a real browser maintaining state across pages.

    Args:
        referer:   URL of the page that linked here (sent as Referer header)
        is_script: True when fetching a JS bundle (<script src>)
        is_api:    True when replicating an XHR/fetch API call from JS
    """
    if is_api:
        base_headers = _API_HEADERS
    elif is_script:
        base_headers = _SCRIPT_HEADERS
    else:
        base_headers = _NAV_HEADERS
    headers = dict(base_headers)
    headers["Sec-Fetch-Site"] = _sec_fetch_site(url, referer)
    if referer:
        headers["Referer"] = referer

    def _get(target):
        return session.get(target, headers=headers, timeout=FETCH_TIMEOUT, allow_redirects=True)

    try:
        resp = _get(url)
        if resp.status_code == 200:
            return resp.text, resp.url
        logger.debug("[detector] %s → HTTP %s", url, resp.status_code)
        if resp.status_code in (429, 403):
            result = _fetch_via_worker(url)
            if result:
                return result
        return None, url
    except Exception as e:
        # SSL fallback to HTTP
        if "ssl" in str(e).lower() or "SSL" in type(e).__name__:
            try:
                resp = _get(url.replace("https://", "http://", 1))
                if resp.status_code == 200:
                    return resp.text, resp.url
            except Exception:
                pass
        logger.debug("[detector] fetch error %s: %s", url, e)
        # Network-level failure — try CF Worker (handles IP blocks, DNS fails)
        result = _fetch_via_worker(url)
        if result:
            return result
        return None, url


# ─────────────────────────────────────────────────────────────────────────────
# Script src extraction — only fetches relevant bundles
# ─────────────────────────────────────────────────────────────────────────────

def _script_srcs(html, base_url):
    """
    Extract <script src="..."> URLs from raw HTML.
    Skips analytics/tracking bundles. Prioritises bundles with career/job in
    the name (most likely to contain ATS config), then same-domain bundles,
    then CDN bundles. Capped at MAX_JS_BUNDLES.
    """
    base_domain = urlparse(base_url).netloc
    root = ".".join(base_domain.split(".")[-2:])

    priority = []   # career/job-named bundles first
    same_dom  = []  # same domain / same-root CDN
    external  = []  # fully external CDN

    for m in re.finditer(r'<script[^>]+src=["\']([^"\']+)["\']', html, re.IGNORECASE):
        src = m.group(1).strip()
        if src.startswith("data:") or src.startswith("#"):
            continue
        src_lower = src.lower()
        if any(skip in src_lower for skip in _BUNDLE_SKIP):
            continue
        absolute = urljoin(base_url, src)
        parsed   = urlparse(absolute)

        if any(kw in src_lower for kw in ("career", "job", "recruit", "apply", "hire")):
            priority.append(absolute)
        elif root in parsed.netloc:
            same_dom.append(absolute)
        else:
            external.append(absolute)

    return (priority + same_dom + external)[:MAX_JS_BUNDLES]


# ─────────────────────────────────────────────────────────────────────────────
# Scoring-based next-page navigation
# ─────────────────────────────────────────────────────────────────────────────

_URL_SCORES = {
    "job": 3, "jobs": 3, "career": 3, "careers": 3,
    "position": 2, "positions": 2, "opening": 2, "openings": 2,
    "hiring": 2, "role": 1, "roles": 1, "work": 1,
}

_ANCHOR_SCORES = {
    "view jobs": 5, "see jobs": 5, "explore jobs": 5, "find jobs": 5,
    "open positions": 5, "job openings": 5, "search jobs": 4,
    "browse jobs": 4, "view openings": 4, "apply now": 4,
    "all jobs": 3, "careers": 3, "opportunities": 2, "join us": 2,
    "explore roles": 3, "see openings": 4, "view roles": 3,
}

_SKIP_HREF = re.compile(
    r'^(?:#|mailto:|tel:|javascript:)|'
    r'(?:facebook|twitter|linkedin|instagram|youtube|tiktok)\.com|'
    r'\.(?:jpg|jpeg|png|gif|svg|webp|ico|pdf|zip|mp4|mp3|woff2?)(?:[?#]|$)',
    re.IGNORECASE,
)

# Path segments that indicate non-job content — never contain ATS signals
_PATH_DENYLIST = re.compile(
    r'/(?:blog|tech-blog|news|press|events|life-at|life|perks|benefits|'
    r'values|diversity|inclusion|awards|media|podcast|video|gallery|'
    r'photos|story|stories|leadership|board|culture|privacy|legal|terms|'
    r'accessibility|sitemap|contact|support|faq|help|'
    r'state-specific|disclosure|language|application-language)(?:/|$)',
    re.IGNORECASE,
)

# Already-tried top-level paths — don't cycle back to them
_TOP_LEVEL_PATHS = frozenset({
    "", "/", "/careers", "/careers/", "/jobs", "/jobs/",
    "/about/careers", "/company/careers", "/en/careers",
    "/us/careers", "/join-us", "/work-with-us", "/opportunities",
})


def find_next_pages(html, current_url, visited=None):
    """
    Score every <a href> in raw HTML and return ALL candidates with score >= 2,
    sorted highest first. Caller tries each in order until one resolves.

    Domain rule: allow same domain OR any domain that contains the brand keyword
    (e.g. wayfair.com → aboutwayfair.com, spotify.com → lifeatspotify.com).
    """
    parsed_base = urlparse(current_url)
    base_domain = parsed_base.netloc
    base_parts  = base_domain.split(".")
    brand       = base_parts[-2] if len(base_parts) >= 2 else base_domain

    pairs = re.findall(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html, re.IGNORECASE | re.DOTALL,
    )

    scored = {}  # url → score (dedup by url, keep highest)

    for raw_href, raw_anchor in pairs:
        href   = _html_unescape(raw_href.strip())
        anchor = re.sub(r'<[^>]+>', '', raw_anchor).strip().lower()

        if _SKIP_HREF.search(href):
            continue

        absolute = urljoin(current_url, href)
        parsed   = urlparse(absolute)

        # Belt-and-suspenders: filter image/media/doc files by parsed path extension
        _path_ext = parsed.path.rsplit(".", 1)[-1].lower() if "." in parsed.path else ""
        if _path_ext in {"jpg", "jpeg", "png", "gif", "svg", "webp", "ico",
                         "pdf", "zip", "mp4", "mp3", "woff", "woff2"}:
            continue

        # Allow same domain OR brand-family domain
        if parsed.netloc != base_domain and brand not in parsed.netloc:
            continue

        # Skip already-visited URLs
        if visited and absolute in visited:
            continue

        path = parsed.path.rstrip("/")

        # Skip content pages (blog, news, culture) — they never have ATS signals
        if _PATH_DENYLIST.search(path):
            continue
        if path in _TOP_LEVEL_PATHS:
            continue

        # Score URL path + domain tokens
        score = 0
        path_lower = (parsed.netloc + path).lower()
        for token, pts in _URL_SCORES.items():
            if token in path_lower:
                score += pts

        # Score anchor text
        for phrase, pts in _ANCHOR_SCORES.items():
            if phrase in anchor:
                score += pts

        if score >= 2:
            scored[absolute] = max(scored.get(absolute, 0), score)

    return sorted(scored, key=scored.get, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Main detect loop
# ─────────────────────────────────────────────────────────────────────────────

def detect(start_url, session=None, visited=None, _hits=None, _best=None, _referer=None, _max_pages=None):
    """
    Crawl start_url and accumulate every ATS platform found into _hits.

    Terminators:
      - len(visited) >= _max_pages  (page budget exhausted)
      - find_next_pages returns empty  (no more scored candidates)

    A complete hit (slug != "") is recorded but never stops the crawl — we
    exhaust all possibilities so multi-ATS companies (e.g. Nomura: TALapply
    for campus + SuccessFactors for experienced) are fully discovered.

    slug == ""  → partial hit; stored in _best as fallback, crawl continues.

    _hits / _best / _referer / _max_pages are internal — do not pass from outside.
    """
    if session is None:
        session = _make_session()
    if visited is None:
        visited = set()
    if _hits is None:
        _hits = {}   # (platform, slug) → {platform, slug, source_url}
    if _best is None:
        _best = [None]
    if _max_pages is None:
        _max_pages = _MAX_PAGES

    if start_url in visited:
        return
    if len(visited) >= _max_pages:
        logger.debug("[detector] page budget exhausted (%d pages)", len(visited))
        return

    # Skip binary resources before fetching — catches redirect destinations too
    _url_ext = start_url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in start_url else ""
    if _url_ext in {"jpg", "jpeg", "png", "gif", "svg", "webp", "ico",
                    "pdf", "zip", "mp4", "mp3", "woff", "woff2"}:
        return

    visited.add(start_url)

    html, final_url = _fetch(start_url, session, referer=_referer)
    if not html:
        return

    # Track final URL (post-redirect) — prevents re-crawling the same page
    # reached via different paths. Guard: only check when a redirect actually
    # occurred (final_url != start_url) — start_url is already in visited.
    if final_url != start_url and final_url in visited:
        return
    visited.add(final_url)
    logger.debug("[detector] page=%d url=%s", len(visited), final_url)

    def _handle(result, source_label):
        """Record a scan result. Complete hits go into _hits; partials into _best."""
        if not result:
            return
        if result["slug"]:
            key = (result["platform"], result["slug"])
            if key not in _hits:
                _hits[key] = {**result, "source_url": final_url}
                logger.info("[detector] HIT (%s) page=%d platform=%s slug=%s url=%s",
                            source_label, len(visited), result["platform"], result["slug"], final_url)
        elif _best[0] is None:
            logger.debug("[detector] PARTIAL (%s) page=%d platform=%s — continuing for slug",
                         source_label, len(visited), result["platform"])
            _best[0] = result

    # 1. Scan raw HTML
    _handle(scan(html), "HTML")

    # 2. Scan JS bundles — fetch with script headers + Referer = page that loaded them
    #    Also extract API endpoint paths from bundle source for step 2b.
    api_paths = []
    for src in _script_srcs(html, final_url):
        bundle, _ = _fetch(src, session, referer=final_url, is_script=True)
        if not bundle:
            continue
        _handle(scan(bundle), "JS bundle")
        api_paths.extend(_extract_api_paths(bundle))

    # 2b. Probe API endpoints discovered in bundles.
    #     Session already holds cookies from the page visit — same auth as the JS uses.
    #     This surfaces ATS config that SPAs fetch at runtime without us running any JS.
    seen_api = set()
    for path in api_paths:
        if path in seen_api:
            continue
        seen_api.add(path)
        api_url = urljoin(final_url, path)
        resp, _ = _fetch(api_url, session, referer=final_url, is_api=True)
        if not resp:
            continue
        logger.debug("[detector] API probe page=%d path=%s", len(visited), path)
        _handle(scan(resp), "API")

    # 3. Navigate deeper — recurse into ALL scored candidates, no early exit on hit
    candidates = find_next_pages(html, final_url, visited)
    logger.debug("[detector] candidates page=%d: %s", len(visited), candidates[:5])
    for next_url in candidates:
        detect(next_url, session, visited, _hits, _best, _referer=final_url, _max_pages=_max_pages)


def detect_company(company_domain, session=None):
    """
    Detect all ATS platforms for a company given only its domain.

    Probes all standard career paths then career subdomains. A shared visited
    set, _hits dict, and _best partial are threaded through every probe so we
    never re-crawl and always collect every platform encountered.

    Args:
        company_domain: e.g. "stripe.com" (no https://)
        session:        curl_cffi/requests Session (created if not provided)

    Returns:
        List of {"platform": ..., "slug": ..., "source_url": ...}
        — one entry per unique (platform, slug) pair found across the full crawl.
        — slug may be "" if the platform was detected but the tenant URL was not found.
        — empty list if nothing found.
    """
    domain = company_domain.lower().strip()
    domain = re.sub(r'^https?://', '', domain).rstrip('/')
    if session is None:
        session = _make_session()

    visited = set()   # shared — prevents re-crawling the same pages across probes
    hits    = {}      # (platform, slug) → result — accumulates every unique ATS found
    best    = [None]  # fallback partial (platform known, slug not found)

    # 1. Standard paths on main domain
    for path in CAREER_PATHS:
        url = f"https://{domain}{path}"
        detect(url, session, visited=visited, _hits=hits, _best=best)

    # 2. Career subdomains
    root = domain.split(".")[-2] + "." + domain.split(".")[-1]
    for subdomain in ("careers", "jobs", "talent", "apply", "hiring"):
        url = f"https://{subdomain}.{root}"
        detect(url, session, visited=visited, _hits=hits, _best=best)

    if hits:
        logger.info("[detector] DONE domain=%s found=%d platform(s): %s",
                    domain, len(hits), [v["platform"] for v in hits.values()])
        return list(hits.values())
    if best[0]:
        logger.info("[detector] DONE domain=%s partial platform=%s (no slug)",
                    domain, best[0]["platform"])
        return [best[0]]
    logger.info("[detector] DONE domain=%s — no ATS found", domain)
    return []
