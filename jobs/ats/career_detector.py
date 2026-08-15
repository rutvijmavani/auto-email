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

import tldextract as _tldextract_mod
_tldextract = _tldextract_mod.TLDExtract(suffix_list_urls=())

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

def _host_root(hostname: str) -> str:
    """Return the registrable domain using the PSL-aware offline tldextract instance."""
    return _tldextract.extract(hostname).registered_domain or hostname

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
        CONNECT_TIMEOUT as _CONNECT_TIMEOUT,
        CAREER_DETECTOR_MAX_JS_BUNDLES as _MAX_JS_BUNDLES,
        CAREER_DETECTOR_MAX_API_PROBES as _MAX_API_PROBES,
        CAREER_DETECTOR_LISTING_PAGES as _N_LISTING_PAGES,
        CAREER_DETECTOR_DETAIL_SAMPLE as _M_DETAIL_SAMPLE,
    )
except Exception:
    _MAX_PAGES        = 25
    _FETCH_TIMEOUT    = 15
    _CONNECT_TIMEOUT  = 5
    _MAX_JS_BUNDLES   = 15
    _MAX_API_PROBES   = 10
    _N_LISTING_PAGES  = 2   # paginated listing pages to process before stopping pagination
    _M_DETAIL_SAMPLE  = 3   # job detail pages to sample per URL template before stopping

FETCH_TIMEOUT   = _FETCH_TIMEOUT
CONNECT_TIMEOUT = _CONNECT_TIMEOUT
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


def _extract_tal(text):
    # Taleo Business Edition / TALapply — uses .tal.net subdomains
    m = re.search(r'([a-z0-9-]+)\.tal\.net', text, re.IGNORECASE)
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


def _extract_avature_portal(text):
    # Custom-domain Avature via avature.portal.* meta tags (e.g. L'Oreal, Lenovo)
    # Reconstruct {"base": "https://careers.loreal.com", "path": "en_US/content"}
    lang  = re.search(r'avature\.portal\.lang["\'][^>]+content=["\']([^"\']+)["\']',    text, re.IGNORECASE)
    upath = re.search(r'avature\.portal\.urlPath["\'][^>]+content=["\']([^"\']+)["\']', text, re.IGNORECASE)
    canon = re.search(r'rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']',           text, re.IGNORECASE)
    if not canon:
        canon = re.search(r'href=["\']([^"\']+)["\'][^>]+rel=["\']canonical["\']',      text, re.IGNORECASE)
    if lang and upath and canon:
        parsed = urlparse(canon.group(1))
        slug = json.dumps({"base": f"{parsed.scheme}://{parsed.netloc}", "path": f"{lang.group(1)}/{upath.group(1)}"})
        return {"platform": "avature", "slug": slug}
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
    "tal.net":                   _extract_tal,
    "phenompeople":              _extract_phenom,
    "talentbrew":                _extract_talentbrew,
    "oraclecloud.com/hcmUI":     _extract_oracle_hcm,
    "avature.net":               _extract_avature,
    "avatureReferrerQueryParam": _extract_avature,
    "avature.portal":            _extract_avature_portal,
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
    Scan raw text for any ATS keyword. Returns first non-tentative match with
    a non-empty slug, then a non-tentative partial (empty slug), then a tentative.
    """
    tentative = None
    partial   = None  # non-tentative platform detected but slug is empty
    for keyword, extractor in ATS_KEYWORDS.items():
        if keyword in text:
            result = extractor(text)
            if result:
                if result["platform"] in _TENTATIVE_PLATFORMS:
                    if tentative is None:
                        tentative = result
                elif result.get("slug"):
                    return result
                elif partial is None:
                    partial = result
    return partial or tentative


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
        return session.get(target, headers=headers, timeout=(CONNECT_TIMEOUT, FETCH_TIMEOUT), allow_redirects=True)

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
    # Use tldextract so ccTLDs (e.g. .co.jp) don't produce wrong brand ("co" instead of "nomura")
    brand = _tldextract.extract(base_domain).domain or base_domain

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

        # Allow same domain OR brand-family domain (bidirectional).
        # e.g. nomura.com ↔ nomuraholdings.com: "nomura" appears in both.
        target_brand = _tldextract.extract(parsed.hostname or parsed.netloc).domain or parsed.netloc
        if parsed.netloc != base_domain and brand not in parsed.netloc and target_brand not in base_domain:
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
# Single-page processor — fetch one URL, scan, return next candidates
# ─────────────────────────────────────────────────────────────────────────────

def _process_page(url, session, visited, hits, best, referer=None, company_root=None,
                  first_200_url=None):
    """
    Fetch url, scan HTML + JS bundles + API endpoints for ATS signals.
    Records hits into shared dicts. Returns scored next-page candidates.
    Does NOT recurse — BFS queue in detect_company drives traversal.

    first_200_url: mutable [None] container — set to the final URL of the first
                   page that returns HTML (200), so callers can capture career URL
                   even on a complete ATS miss.

    Leaf conditions (return [] immediately):
      Rule 1  — new complete ATS hit found → children share the same ATS, useless.
      Signal 1 — page is not company territory:
                   neither company brand in page domain
                   nor company root domain referenced anywhere in page HTML.
    """
    _url_ext = url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in url else ""
    if _url_ext in {"jpg", "jpeg", "png", "gif", "svg", "webp", "ico",
                    "pdf", "zip", "mp4", "mp3", "woff", "woff2"}:
        return []

    if url in visited:
        return []
    visited.add(url)

    html, final_url = _fetch(url, session, referer=referer)
    if not html:
        return []

    if final_url != url and final_url in visited:
        return []
    visited.add(final_url)
    logger.debug("[detector] page=%d url=%s", len(visited), final_url)

    def _handle(result, source_label):
        if not result:
            return
        if result["slug"]:
            key = (result["platform"], result["slug"])
            if key not in hits:
                hits[key] = {**result, "source_url": final_url}
                logger.info("[detector] HIT (%s) page=%d platform=%s slug=%s url=%s",
                            source_label, len(visited), result["platform"], result["slug"], final_url)
        elif best[0] is None:
            logger.debug("[detector] PARTIAL (%s) page=%d platform=%s — continuing for slug",
                         source_label, len(visited), result["platform"])
            best[0] = result

    # Always scan raw HTML — catches ATS slug even on external pages
    hits_before = len(hits)
    _handle(scan(html), "HTML")

    # Rule 1: new complete ATS hit in HTML → leaf
    if len(hits) > hits_before:
        logger.debug("[detector] rule1 (HTML): new hit — leaf %s", final_url)
        return []

    # ── Signal 1: company territory check ────────────────────────────────────
    # Company territory = brand name appears in the page's domain
    #                  OR company root domain is referenced anywhere in the HTML.
    # Both signals are derived from the email/company domain (e.g. "nomura.com"):
    #   brand      = "nomura"   — first segment, appears in brand-family domains
    #   company_root = "nomura.com" — full root, appears in cross-links and hrefs
    # Neither uses the legal entity name which never matches website content.
    if company_root:
        company_brand = company_root.split('.')[0]
        page_netloc   = urlparse(final_url).netloc.lower()
        in_domain     = company_brand in page_netloc
        in_html       = company_root in html.lower()
        if not in_domain and not in_html:
            logger.debug("[detector] signal1: not company territory — leaf %s", final_url)
            return []

    # Company territory confirmed — record this as the first successful company-territory URL.
    # Exclude root-path redirects landing on the main company domain (homepage redirects);
    # career subdomains (careers.company.com/) have a different netloc and are kept.
    if first_200_url is not None and first_200_url[0] is None:
        _fp    = urlparse(final_url)
        _fhost = (_fp.hostname or "").removeprefix("www.")
        if _fp.path.rstrip("/") or _fhost != company_root:
            first_200_url[0] = final_url

    # Full scan: JS bundles + API probes
    api_paths = []
    for src in _script_srcs(html, final_url):
        bundle, _ = _fetch(src, session, referer=final_url, is_script=True)
        if not bundle:
            continue
        hits_before_js = len(hits)
        _handle(scan(bundle), "JS bundle")
        if len(hits) > hits_before_js:
            logger.debug("[detector] rule1 (JS): new hit — leaf %s", final_url)
            return []
        api_paths.extend(_extract_api_paths(bundle))

    seen_api = set()
    api_probe_count = 0
    for path in api_paths:
        if api_probe_count >= _MAX_API_PROBES:
            break
        if path in seen_api:
            continue
        seen_api.add(path)
        api_url = urljoin(final_url, path)
        resp, _ = _fetch(api_url, session, referer=final_url, is_api=True)
        if not resp:
            continue
        api_probe_count += 1
        logger.debug("[detector] API probe page=%d path=%s", len(visited), path)
        hits_before_api = len(hits)
        _handle(scan(resp), "API")
        if len(hits) > hits_before_api:
            logger.debug("[detector] rule1 (API): new hit — leaf %s", final_url)
            return []

    # Company territory, no hit yet → follow links
    candidates = find_next_pages(html, final_url, visited)
    logger.debug("[detector] candidates page=%d: %s", len(visited), candidates[:5])
    return [(c, final_url) for c in candidates]


# ─────────────────────────────────────────────────────────────────────────────
# Listing-page sampling — prevent crawling 250 identical job detail pages
# ─────────────────────────────────────────────────────────────────────────────

_PAGINATION_PARAM_RE = re.compile(
    r'[?&](page|p|pg|offset|start|from)=\d+',
    re.IGNORECASE,
)
_PAGE_PATH_RE = re.compile(r'/page/\d+(?:/|$)', re.IGNORECASE)


def _url_template(url):
    """
    Normalise variable path segments so structurally identical job-listing URLs
    share a template string.

      /careers/listing/ai-engineer/8044460  →  .../careers/listing/{slug}/{id}
      /job/12345                             →  .../job/{id}
      /careers/americas/                     →  .../careers/americas/   (unchanged)
    """
    parsed = urlparse(url)
    parts  = [p for p in parsed.path.split('/') if p]
    out    = []
    for part in parts:
        if re.match(r'^\d+$', part):
            out.append('{id}')
        elif len(part) > 20 and part.count('-') >= 2:
            out.append('{slug}')
        else:
            out.append(part)
    return parsed.netloc + '/' + '/'.join(out)


def _pagination_root(url):
    """Strip page/offset params so paginated URLs collapse to their listing root."""
    cleaned = _PAGINATION_PARAM_RE.sub('', url)
    cleaned = _PAGE_PATH_RE.sub('/', cleaned)
    return re.sub(r'[?&]+$', '', cleaned).rstrip('/')


def _filter_listing_candidates(candidates, pagination_roots, sampled_patterns, confirmed_patterns):
    """
    Gate BFS candidates to prevent runaway crawling of paginated job listings.

    Three candidate types:
      1. Pagination links (?page=N, /page/N) — follow at most _N_LISTING_PAGES per root.
      2. Job detail links  — URL template appears 3+ times in one batch (cluster signal).
                            Sample at most _M_DETAIL_SAMPLE per template across all batches.
                            Once sampled, add to confirmed_patterns and drop all further matches.
      3. Everything else   — navigation, subdomains, regional sections — always pass through.

    Termination does NOT require a slug hit first. Pattern confirmation (same URL structure
    repeated across enough pages) is sufficient — if we've seen 3 sample detail pages and
    found nothing, the remaining 247 will almost certainly yield nothing either.
    """
    from collections import Counter

    # Detect which templates appear ≥ 3 times in this batch → job listing cluster
    template_counts = Counter(_url_template(url) for url, _ in candidates)
    batch_job_templates = {t for t, c in template_counts.items() if c >= 3}

    filtered = []
    for url, referer in candidates:
        # ── Pagination link ──────────────────────────────────────────────────
        if _PAGINATION_PARAM_RE.search(url) or _PAGE_PATH_RE.search(url):
            root = _pagination_root(url)
            seen = pagination_roots.get(root, 0)
            if seen >= _N_LISTING_PAGES:
                logger.debug("[detector] listing-cap: dropping pagination %s (root seen %d×)", url, seen)
                continue
            pagination_roots[root] = seen + 1
            filtered.append((url, referer))
            continue

        # ── Job detail link ──────────────────────────────────────────────────
        template = _url_template(url)
        is_job = (
            template in batch_job_templates
            or template in sampled_patterns
            or template in confirmed_patterns
        )
        if is_job:
            if template in confirmed_patterns:
                logger.debug("[detector] listing-cap: dropping confirmed-pattern %s", url)
                continue
            count = sampled_patterns.get(template, 0)
            if count >= _M_DETAIL_SAMPLE:
                confirmed_patterns.add(template)
                logger.debug("[detector] listing-cap: pattern confirmed %s — dropping %s", template, url)
                continue
            sampled_patterns[template] = count + 1

        # ── Navigation / everything else ─────────────────────────────────────
        filtered.append((url, referer))

    return filtered


# ─────────────────────────────────────────────────────────────────────────────
# BFS driver — breadth-first so sibling branches share the page budget
# ─────────────────────────────────────────────────────────────────────────────

def detect_company(company_domain, session=None, *, seed_url=None):
    """
    Detect all ATS platforms for a company given only its domain.

    seed_url: if provided, prioritize this URL at the front of the BFS queue;
              standard CAREER_PATHS and subdomain fallbacks are still enqueued
              after it. Use when careers_url is already known from Phase 6.

    Uses BFS so all candidates at depth N are explored before any at depth N+1.
    This guarantees siblings (e.g. nomura.com early-careers AND nomuraholdings.com)
    share the page budget rather than one branch consuming it all via DFS.

    Args:
        company_domain: e.g. "stripe.com" (no https://)
        session:        curl_cffi/requests Session (created if not provided)

    Returns:
        List of {"platform": ..., "slug": ..., "source_url": ...}
        — one entry per unique (platform, slug) pair found across the full crawl.
        — slug="" if platform detected but tenant URL not found (partial).
        — [{"platform": None, "slug": None, "source_url": url}] if no ATS found
          but a 200-OK career URL was discovered; callers must check platform is
          None before reading platform/slug.
        — [] if no ATS and no career URL found.
    """
    from collections import deque

    domain = company_domain.lower().strip()
    domain = re.sub(r'^https?://', '', domain).rstrip('/')
    if session is None:
        session = _make_session()

    company_root       = _host_root(domain)  # e.g. 'accenture.com' or 'amazon.co.uk'
    visited            = set()  # prevents re-fetching any URL
    hits               = {}     # (platform, slug) → {platform, slug, source_url}
    best               = [None] # fallback partial
    pagination_roots   = {}     # listing root → paginated pages seen
    sampled_patterns   = {}     # url template  → detail pages sampled
    confirmed_patterns = set()  # templates fully sampled — drop all further matches

    # Seed the BFS queue: seed_url first (if provided), then CAREER_PATHS + subdomain fallbacks
    queue = deque()
    seen_seeds: set = set()
    if seed_url:
        _seed_parsed = urlparse(seed_url)
        if _seed_parsed.scheme in ("http", "https") and _seed_parsed.hostname:
            queue.append((seed_url, None))
            seen_seeds.add(seed_url)
        else:
            logger.debug("career_detector: ignoring seed_url with invalid scheme/host: %r", seed_url)
    for path in CAREER_PATHS:
        candidate = f"https://{domain}{path}"
        if candidate not in seen_seeds:
            queue.append((candidate, None))
    if len(domain.split(".")) > 1:
        for subdomain in ("careers", "jobs", "talent", "apply", "hiring"):
            candidate = f"https://{subdomain}.{company_root}"
            if candidate not in seen_seeds:
                queue.append((candidate, None))

    first_200_url = [None]  # mutable — _process_page sets this on first successful fetch

    # BFS until queue drains. Two leaf conditions bound the crawl:
    #   Rule 1  — page yields a new ATS hit → don't enqueue its children
    #   Signal 1 — page not in company territory → scan only, no children
    # _filter_listing_candidates additionally caps job-listing clusters.
    while queue:
        if len(visited) >= _MAX_PAGES:
            break
        url, referer = queue.popleft()
        if url in visited:
            continue
        next_candidates = _process_page(
            url, session, visited, hits, best, referer,
            company_root=company_root,
            first_200_url=first_200_url,
        )
        filtered = _filter_listing_candidates(
            next_candidates, pagination_roots, sampled_patterns, confirmed_patterns
        )
        queue.extend(filtered)

    if hits:
        logger.info("[detector] DONE domain=%s found=%d platform(s): %s",
                    domain, len(hits), [v["platform"] for v in hits.values()])
        return list(hits.values())
    if best[0]:
        logger.info("[detector] DONE domain=%s partial platform=%s (no slug)",
                    domain, best[0]["platform"])
        return [best[0]]
    if first_200_url[0]:
        logger.info("[detector] DONE domain=%s — no ATS found, career URL: %s",
                    domain, first_200_url[0])
        return [{"platform": None, "slug": None, "source_url": first_200_url[0]}]
    logger.info("[detector] DONE domain=%s — no ATS found", domain)
    return []
