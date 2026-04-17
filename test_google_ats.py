#!/usr/bin/env python3
"""
test_google_ats.py — Google-based ATS detection using Playwright.

WHY PLAYWRIGHT:
  Google serves different HTML to requests vs real browsers even with
  matching cookies. The ATS URLs (e.g. capitalone.wd12.myworkdayjobs.com)
  are assembled by JavaScript into <a href> tags after page load.
  requests.get() gets a JS-skeleton that never renders those links.
  Playwright renders the full DOM — same as Chrome — so <a href> tags
  are fully populated and extractable.

SETUP (one-time):
  pip install playwright playwright-stealth beautifulsoup4
  playwright install chromium
  Then:
  1. Open Chrome → google.com → search anything
  2. DevTools → Network → right-click the /search request
     → Copy → Copy as cURL (cmd)
  3. Paste into: google_search.curl  (project root)

USAGE:
  python test_google_ats.py "Capital One"
  python test_google_ats.py "Charles Schwab"
  python test_google_ats.py "Stripe" --headful     # watch browser
  python test_google_ats.py "Stripe" --save-html   # save rendered HTML

Cookie persistence:
  After every page load Playwright's cookies are dumped to
  google_cookies.json. On the next run those override the curl's
  cookies — session tokens stay fresh automatically.
  Only recapture the curl when google_cookies.json stops working.
"""

import sys
import re
import json
import time
import random
import asyncio
import argparse
from pathlib import Path
from urllib.parse import urlparse, urlencode

# Force UTF-8 on the real stdout/stderr so unicode box-drawing chars work.
# Do this on the underlying streams before any Tee wrapping below.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


class _Tee:
    """
    Duplicate all writes to two streams simultaneously.
    Used for --output: prints to console AND writes to a log file
    without shell piping (which breaks Playwright's Node subprocess via EPIPE).
    """
    def __init__(self, stream_a, stream_b):
        self._a = stream_a
        self._b = stream_b

    def write(self, data):
        try:
            self._a.write(data)
        except Exception:
            pass
        try:
            self._b.write(data)
        except Exception:
            pass

    def flush(self):
        for s in (self._a, self._b):
            try:
                s.flush()
            except Exception:
                pass

    # Satisfy any code that checks for these attrs
    @property
    def encoding(self):
        return getattr(self._a, "encoding", "utf-8")

    def isatty(self):
        return False

# ── Project root on path ───────────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
from jobs.curl_parser import curl_to_slug_info
from jobs.ats.patterns import match_ats_pattern

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

CURL_FILE      = ROOT / "google_search.curl"
COOKIES_FILE   = ROOT / "google_cookies.json"
DEBUG_HTML     = ROOT / "google_debug.html"
PROSPECTS_FILE = ROOT / "prospects.txt"

SEARCH_URL  = "https://www.google.com/search"
GOOGLE_HOME = "https://www.google.com/"

# Phase 2 Google search strategy
# ─────────────────────────────────────────────────────────────────────────────
# WHY natural queries:
#   The old approach used a 11-site: OR query which Google immediately flags
#   as automated — no human ever types that. A simple "{Company} jobs" query
#   is indistinguishable from normal user behaviour. Google naturally surfaces
#   ATS job boards (Greenhouse, Workday, etc.) in the top results, so we still
#   collect enough ATS URLs to be confident.
#
# Trade-off: fewer ATS hits per page (mixed with LinkedIn/Indeed/etc.).
#   → Lower GOOGLE_MIN_HITS to 3 (vs old 5) since quality > quantity.
#   → Dedicated single-platform searches remain for iCIMS and Lever (still
#     uses site: but a single-domain filter looks far less robotic than 11).

# Phase 2 site-filter groups.
# Split into 2 groups of ~5 platforms each so each OR query is short enough
# to not look robotic, but comprehensive enough to cover all major platforms.
# iCIMS and Lever get dedicated single-site searches (they're crowded out
# even in a 5-term OR by heavier-traffic platforms).
SITE_FILTER_GROUP_A = [
    "site:boards.greenhouse.io",
    "site:job-boards.greenhouse.io",
    "site:jobs.ashbyhq.com",
    "site:myworkdayjobs.com",
    "site:myworkdaysite.com",
]
SITE_FILTER_GROUP_B = [
    "site:jobs.smartrecruiters.com",
    "site:oraclecloud.com",
    "site:jobs.jobvite.com",
    "site:successfactors.com",
    "site:successfactors.eu",
]
DEDICATED_SEARCHES = [
    ("icims",  "site:icims.com"),
    ("lever",  "site:lever.co"),
]

PAGE_SIZE         = 10
MAX_PAGES         = 2
PAGE_DELAY        = (4.0, 7.0)

# Delay between companies in batch mode (seconds).
# Long enough to look human; increases after bot detection.
INTER_COMPANY_DELAY         = (6.0, 10.0)
INTER_COMPANY_DELAY_BACKOFF = (45.0, 75.0)  # after bot detection

# Extra pause between Phase 1 (career page) and Phase 2 (Google search)
# within the same company, to reduce Google request rate.
PHASE_TRANSITION_DELAY = (3.0, 5.0)

# Text that means Google is blocking us
BOT_SIGNALS = [
    "detected unusual traffic",
    "our systems have detected",
    "recaptcha",
    "g-recaptcha",
    "/sorry/index",
    "unusual traffic from your computer",
]

# Playwright networkidle timeout (ms)
LOAD_TIMEOUT = 20_000


# ─────────────────────────────────────────
# CURL → COOKIE + HEADER PARSING
# ─────────────────────────────────────────

def _load_curl_config():
    """
    Parse google_search.curl and return (cookies_dict, headers_dict).
    Saved cookies from google_cookies.json take precedence over curl
    cookies — they are newer (refreshed by Google's Set-Cookie on last run).
    """
    if not CURL_FILE.exists():
        print(f"\n[ERROR] {CURL_FILE.name} not found.\n")
        print("  1. Open Chrome → google.com → search anything")
        print("  2. DevTools → Network → right-click the /search request")
        print("     → Copy → Copy as cURL (cmd)")
        print(f"  3. Paste into: {CURL_FILE}\n")
        sys.exit(1)

    try:
        slug_info = curl_to_slug_info(CURL_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ERROR] curl parse failed: {e}")
        sys.exit(1)

    curl_cookies = (
        slug_info.get("cookies") or
        slug_info.get("_fallback_cookies") or
        {}
    )

    saved_cookies = {}
    if COOKIES_FILE.exists():
        try:
            saved_cookies = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
            print(f"[INFO] Loaded {len(saved_cookies)} saved cookies "
                  f"from {COOKIES_FILE.name}")
        except Exception as e:
            print(f"[WARN] Could not load saved cookies: {e}")

    # saved > curl (saved are more recent)
    merged_cookies = {**curl_cookies, **saved_cookies}

    # Use curl headers as the base for browser context
    headers = dict(slug_info.get("headers", {}))

    return merged_cookies, headers


def _playwright_cookies(cookies_dict):
    """
    Convert flat name→value dict into Playwright cookie objects.
    All scoped to .google.com / path=/ to ensure they're sent on search.
    """
    return [
        {
            "name":     name,
            "value":    value,
            "domain":   ".google.com",
            "path":     "/",
            "secure":   True,
            "httpOnly": False,
            "sameSite": "None",
        }
        for name, value in cookies_dict.items()
    ]


async def _save_playwright_cookies(context):
    """
    Dump all current cookies from the Playwright context to
    google_cookies.json. Called after every page load.
    context.cookies() is a coroutine — must be awaited.
    """
    try:
        pw_cookies = await context.cookies("https://www.google.com")
        flat = {c["name"]: c["value"] for c in pw_cookies}
        COOKIES_FILE.write_text(json.dumps(flat, indent=2), encoding="utf-8")
        print(f"  [cookies] Saved {len(flat)} → {COOKIES_FILE.name}")
    except Exception as e:
        print(f"  [WARN] Could not save cookies: {e}")


# ─────────────────────────────────────────
# BOT DETECTION
# ─────────────────────────────────────────

def _is_bot_blocked(html):
    low = html[:8000].lower()
    return any(sig in low for sig in BOT_SIGNALS)


# ─────────────────────────────────────────
# URL EXTRACTION FROM RENDERED DOM
# ─────────────────────────────────────────

def _extract_result_urls(html):
    """
    Extract ATS URLs from Playwright's fully-rendered DOM HTML.
    Uses 3 strategies (Playwright renders JS so strategy 4 / regex is
    the fallback, not the primary):

    1. <a href> direct — Playwright-rendered DOM has actual https:// hrefs
    2. data-* attributes on result containers
    3. Regex scan as final safety net for any remaining embedded patterns
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    urls = []

    def _add(url):
        url = url.strip().rstrip("/")
        if not url or url in seen:
            return
        parsed = urlparse(url)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            if "google.com" not in parsed.netloc:
                seen.add(url)
                urls.append(url)

    # Strategy 1: <a href> — primary after Playwright renders DOM
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        # Google occasionally wraps in /url?q= even in rendered DOM
        if "/url?" in href:
            from urllib.parse import parse_qs
            parsed = urlparse(href)
            q_vals = parse_qs(parsed.query).get("q", [])
            if q_vals:
                href = q_vals[0]
        _add(href)

    # Strategy 2: data-* attributes
    for tag in soup.find_all(True):
        for attr in ("data-url", "data-href", "data-target-url"):
            val = tag.get(attr, "").strip()
            if val and val.startswith("http"):
                _add(val)

    # Strategy 3: regex on raw rendered HTML — catches anything in JS blobs
    ATS_PATTERNS = [
        r"https?://(?:boards|job-boards)\.greenhouse\.io/[^\s\"'\\<>]+",
        r"https?://(?:jobs|hire)\.lever\.co/[^\s\"'\\<>]+",
        r"https?://jobs\.ashbyhq\.com/[^\s\"'\\<>]+",
        r"https?://jobs\.smartrecruiters\.com/[^\s\"'\\<>]+",
        r"https?://[a-z0-9\-]+\.wd\d+\.myworkdayjobs\.com/[^\s\"'\\<>]+",
        r"https?://wd\d+\.myworkdaysite\.com/[^\s\"'\\<>]+",
        r"https?://[a-z0-9\-]+\.fa(?:\.ocs)?(?:\.[a-z0-9]+)?\.oraclecloud\.com/hcmUI/[^\s\"'\\<>]+",
        r"https?://(?:careers-)?[a-z0-9\-]+\.icims\.com/jobs[^\s\"'\\<>]*",
        r"https?://jobs\.jobvite\.com/[^\s\"'\\<>]+",
        r"https?://career\d+\.successfactors\.(?:com|eu)/[^\s\"'\\<>]+",
        r"https?://[a-z0-9]+\.jobs2web\.com/[^\s\"'\\<>]+",
    ]
    combined = re.compile("|".join(ATS_PATTERNS), re.IGNORECASE)
    for m in combined.finditer(html):
        raw = re.split(r'["\'\\\s<>]', m.group(0))[0]
        _add(raw)

    return urls


# ─────────────────────────────────────────
# QUERY + SCORING
# ─────────────────────────────────────────

# ─────────────────────────────────────────
# QUERY PERMUTATION
# ─────────────────────────────────────────
#
# WHY permute:
#   A fixed query structure like `"Stripe" (site:A OR site:B OR ...)` issued
#   repeatedly across hundreds of companies creates a statistically detectable
#   pattern even if each individual query looks plausible.  Google's abuse
#   system models *sequences* of queries, not just individual ones.
#
#   Permuting:
#   - whether the company name is quoted
#   - the order of site: OR terms (result-set is order-independent)
#   - whether a natural-language keyword is appended ("jobs", "careers", ...)
#   - whether site filters appear before or after the company name
#
#   ...breaks the structural fingerprint without changing what results Google
#   returns, so detection accuracy is unchanged.

_HUMAN_SUFFIXES = [
    "",           # most common — no suffix
    "",
    "jobs",
    "careers",
    "software engineer",
    "engineering jobs",
    "tech jobs",
    "openings",
]


def _build_group_query(company, site_filters):
    """
    Build a randomised OR-group query (replaces the old fixed-format builder).

    Structural variations each call:
    - Company name: 70% quoted ("Stripe"), 30% bare (Stripe)
    - site: filter order: randomly shuffled every time
    - Trailing human keyword: 25% chance (empty string most of the time)
    - Filter-first layout: 18% chance (e.g. `(site:A OR site:B) "Stripe"`)
    """
    c       = f'"{company}"' if random.random() < 0.70 else company
    filters = list(site_filters)
    random.shuffle(filters)
    or_clause = " OR ".join(filters)

    suffix = ""
    if random.random() < 0.25:
        suffix = " " + random.choice(_HUMAN_SUFFIXES).strip()
    suffix = suffix.rstrip()

    if random.random() < 0.18:
        return f"({or_clause}) {c}{suffix}"
    return f"{c}{suffix} ({or_clause})"


def _build_dedicated_query(company, site_filter):
    """
    Build a randomised single-platform dedicated query (iCIMS, Lever).

    Variations:
    - Company name: 70% quoted, 30% bare
    - Trailing human keyword: 30% chance
    - Filter-first layout: 15% chance
    """
    c = f'"{company}"' if random.random() < 0.70 else company

    suffix = ""
    if random.random() < 0.30:
        suffix = " " + random.choice(_HUMAN_SUFFIXES).strip()
    suffix = suffix.rstrip()

    if random.random() < 0.15:
        return f"{site_filter} {c}{suffix}"
    return f"{c}{suffix} {site_filter}"


def _primary_slug(platform, slug_str):
    """Extract company-identifying slug for grouping (unwrap Workday JSON)."""
    if not slug_str:
        return ""
    if slug_str.startswith("{"):
        try:
            return json.loads(slug_str).get("slug", slug_str)
        except Exception:
            pass
    return slug_str


def _score(ats_hits):
    """Group hits by (platform, primary_slug), return ranked list."""
    tally = {}
    for hit in ats_hits:
        platform = hit["platform"]
        primary  = _primary_slug(platform, hit["slug"])
        key      = (platform, primary)
        if key not in tally:
            tally[key] = [0, hit["slug"]]
        tally[key][0] += 1

    ranked = [
        (plat, pslug, count, sample)
        for (plat, pslug), (count, sample) in tally.items()
    ]
    ranked.sort(key=lambda x: x[2], reverse=True)
    return ranked


# ─────────────────────────────────────────
# SLUG → COMPANY FILTER (false-positive rejection)
# ─────────────────────────────────────────

def _slug_matches_company(primary_slug, company):
    """
    Reject false positives by checking that the primary slug
    contains at least one significant keyword from the company name.

    Why substring and not equality: slugs are typically concatenated
    keywords ("capitalone") or hyphenated ("charles-schwab"), not
    exact matches to individual words.

    Examples:
      "pgatour"    vs "Charles Schwab" → "charles"/"schwab" absent → False ✓
      "capitalone" vs "Capital One"    → "capital" substring → True  ✓
      "schwab"     vs "Charles Schwab" → "schwab" present    → True  ✓
      "stripe"     vs "Stripe"         → "stripe" present    → True  ✓

    Returns True if slug plausibly belongs to company, False to reject.
    """
    try:
        from config import ATS_KEYWORD_STOP_WORDS
    except ImportError:
        ATS_KEYWORD_STOP_WORDS = {
            "inc", "llc", "ltd", "corp", "co", "the", "and",
            "of", "for", "group", "holdings", "services",
        }

    slug_lower = primary_slug.lower().replace("-", "").replace("_", "")
    company_clean = re.sub(r'[^a-z0-9\s]', ' ', company.lower())
    company_words = [
        w for w in company_clean.split()
        if w not in ATS_KEYWORD_STOP_WORDS and len(w) >= 3
    ]

    if not company_words:
        return True  # can't filter — allow

    # At least one significant keyword must appear as substring in slug
    return any(kw in slug_lower for kw in company_words)


# ─────────────────────────────────────────
# PHASE 1: CAREER PAGE FINGERPRINTING
# ─────────────────────────────────────────
#
# Priority flow:
#   1. Google "{company} careers" (no site: filter) → find their career page
#   2. Visit career landing page → look for job listing link → navigate to it
#   3. Fingerprint ATS from the listing page HTML / final URL
#
# This catches all custom-domain ATS setups automatically:
#   - TalentBrew:   tbcdn.talentbrew.com/company/{tenant_id}/ in script src
#   - iCIMS custom: careers-{slug}.icims.com in iframe src or any href
#   - Redirects:    browser navigates to known ATS URL → match_ats_pattern
#   - Others:       Taleo, Oracle HCM, SuccessFactors, Phenom, Eightfold

# Domains to skip — aggregators, social media, news, or ATS platforms
# already covered by the Phase 2 site: queries.
# ─────────────────────────────────────────
# XHR / NETWORK REQUEST FINGERPRINTING
# ─────────────────────────────────────────
#
# Many companies host a custom career frontend (stripe.com/jobs, jobs.netflix.com)
# that calls a known ATS API under the hood via XHR.  Playwright can intercept
# all network requests while the page renders, exposing these API calls directly.
#
# This is more reliable than HTML or Google for custom frontends because:
#   - The API call always happens regardless of UI framework
#   - The slug is right there in the URL path
#   - Zero Google requests needed — no bot detection risk
#
# Examples:
#   stripe.com/jobs        → XHR → boards-api.greenhouse.io/v1/boards/stripe/jobs
#   jobs.netflix.com       → XHR → netflix.wd5.myworkdayjobs.com/...
#   careers.rivian.com     → CDN → app.jibecdn.com/...  (caught by HTML fingerprint)

# Extra API-endpoint patterns not covered by match_ats_pattern.
# Format: (compiled_regex, platform, slug_extractor_fn)
# match_ats_pattern handles the job-board URLs; these handle the raw API calls.
_XHR_EXTRA_PATTERNS = [
    # Greenhouse: API subdomain (boards-api.greenhouse.io vs boards.greenhouse.io)
    (
        re.compile(r'boards-api\.greenhouse\.io/v\d+/boards/([^/?#\s]+)/jobs', re.I),
        "greenhouse",
        lambda m: m.group(1).lower(),
    ),
    # Lever: API subdomain
    (
        re.compile(r'api\.lever\.co/v0/postings/([^/?#\s]+)', re.I),
        "lever",
        lambda m: m.group(1).lower(),
    ),
    # Ashby: API endpoint
    (
        re.compile(r'jobs\.ashbyhq\.com/api/(?:job-board/)?([^/?#\s]+)', re.I),
        "ashby",
        lambda m: m.group(1).lower(),
    ),
    # SmartRecruiters: API
    (
        re.compile(r'api\.smartrecruiters\.com/v\d+/companies/([^/?#\s]+)/postings', re.I),
        "smartrecruiters",
        lambda m: m.group(1).lower(),
    ),
    # Jobvite: API
    (
        re.compile(r'api\.jobvite\.com/api/(?:job|v\d+)/([^/?#\s]+)', re.I),
        "jobvite",
        lambda m: m.group(1).lower(),
    ),
    # Phenom: API
    (
        re.compile(r'([a-z0-9\-]+)\.phenompeople\.com/jobs', re.I),
        "phenom",
        lambda m: m.group(1).lower(),
    ),
]


def _fingerprint_xhr(request_urls, company):
    """
    Scan intercepted XHR/network request URLs for known ATS API patterns.

    Two passes:
    1. Run match_ats_pattern on each URL (handles standard job-board URLs that
       may appear as XHR — e.g. myworkdayjobs.com, lever.co, icims.com)
    2. Run _XHR_EXTRA_PATTERNS for API-specific subdomains not in patterns.py
       (e.g. boards-api.greenhouse.io, api.lever.co)

    Returns {platform, slug} or None.
    """
    seen_keys = set()

    for url in request_urls:
        # Pass 1: existing pattern set (Workday, iCIMS, Greenhouse job-board, etc.)
        result = match_ats_pattern(url)
        if result:
            primary = _primary_slug(result["platform"], result["slug"])
            key = (result["platform"], primary)
            if key not in seen_keys and _slug_matches_company(primary, company):
                seen_keys.add(key)
                return result

        # Pass 2: extra API-endpoint patterns
        for pattern, platform, slug_fn in _XHR_EXTRA_PATTERNS:
            m = pattern.search(url)
            if not m:
                continue
            slug    = slug_fn(m)
            primary = slug
            key     = (platform, primary)
            if key not in seen_keys and _slug_matches_company(primary, company):
                seen_keys.add(key)
                return {"platform": platform, "slug": slug}

    return None


def _fingerprint_inline_scripts(html, company):
    """
    Scan inline <script> content for ATS URLs and config objects.

    Catches:
    - Next.js: <script id="__NEXT_DATA__" type="application/json">
        {"props":{"jobs":[{"absoluteUrl":"https://boards.greenhouse.io/stripe/jobs/..."}]}}
    - Angular/React hydration: window.__INITIAL_STATE__ = {...}
    - Explicit configs: window.boardConfig = {greenhouseToken: "stripe"}
    - Any inline JSON / JS that embeds ATS API URLs

    Only scans *inline* scripts (no src="...") to avoid crawling entire JS bundles.
    Skips scripts larger than 500KB (minified app bundles not useful here).
    """
    from bs4 import BeautifulSoup
    soup    = BeautifulSoup(html, "html.parser")
    chunks  = []
    for script in soup.find_all("script"):
        if script.get("src"):          # external script — skip
            continue
        text = script.get_text(strip=True)
        if text and len(text) < 500_000:
            chunks.append(text)

    if not chunks:
        return None

    combined = "\n".join(chunks)

    # Method A: run XHR pattern-matching on all string content
    result = _fingerprint_xhr(
        re.findall(r'https?://[^\s"\'\\<>]+', combined),
        company,
    )
    if result:
        return result

    # Method B: run extra API patterns on the raw text
    for pattern, platform, slug_fn in _XHR_EXTRA_PATTERNS:
        m = pattern.search(combined)
        if not m:
            continue
        slug    = slug_fn(m)
        primary = slug
        if _slug_matches_company(primary, company):
            return {"platform": platform, "slug": slug}

    return None


def _fingerprint_json_ld(html, company):
    """
    Extract ATS URLs from JSON-LD JobPosting structured data blocks.

    Google requires <script type="application/ld+json"> JobPosting markup
    for job rich-snippets.  The "url" or "sameAs" field contains the canonical
    job URL — which is almost always the ATS job page.

    Example:
      {"@type": "JobPosting", "url": "https://boards.greenhouse.io/stripe/jobs/7656504"}
      → platform=greenhouse, slug=stripe
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text(strip=True))
        except Exception:
            continue

        # Handle both single object and @graph arrays
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            # Flatten @graph
            if item.get("@type") == "ItemList":
                items += item.get("itemListElement", [])
                continue

            candidate_types = {"JobPosting", "jobposting"}
            if str(item.get("@type", "")).lower() not in {t.lower() for t in candidate_types}:
                continue

            for field in ("url", "sameAs", "identifier", "mainEntityOfPage"):
                val = item.get(field)
                if not isinstance(val, str):
                    continue
                result = match_ats_pattern(val)
                if result:
                    primary = _primary_slug(result["platform"], result["slug"])
                    if _slug_matches_company(primary, company):
                        return result
                # Also check extra patterns
                for pattern, platform, slug_fn in _XHR_EXTRA_PATTERNS:
                    m = pattern.search(val)
                    if m:
                        slug = slug_fn(m)
                        if _slug_matches_company(slug, company):
                            return {"platform": platform, "slug": slug}
    return None


async def _fingerprint_via_job_detail(page, listing_html, listing_url, company):
    """
    Navigate to the first job on the listing page, then fingerprint:
    1. Apply button href → ATS URL (most reliable — this IS the ATS link)
    2. Rendered URLs in detail page DOM
    3. Inline scripts in detail page
    4. XHR requests from detail page

    This catches all cases where the listing page has no ATS signals but
    individual job detail pages link out to the ATS apply form.
    """
    from bs4 import BeautifulSoup
    from urllib.parse import parse_qs

    soup   = BeautifulSoup(listing_html, "html.parser")
    parsed = urlparse(listing_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    def _abs(href):
        if not href:
            return None
        href = href.strip()
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return origin + href
        return None

    # ── Find first job link ────────────────────────────────────────────
    job_url = None

    # Priority 1: links that look like individual job pages
    JOB_LINK_HINTS = [
        "/job/", "/jobs/", "/opening/", "/openings/",
        "/position/", "/careers/", "/requisition/",
    ]
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/url?" in href:
            q = parse_qs(urlparse(href).query).get("q", [])
            href = q[0] if q else href
        url = _abs(href)
        if not url:
            continue
        # Skip links back to the listing page itself
        if url.rstrip("/") == listing_url.rstrip("/"):
            continue
        # Skip external ATS job-board links (those are already caught above)
        if any(d in url for d in REDIRECT_ATS_DOMAINS):
            result = match_ats_pattern(url)
            if result:
                primary = _primary_slug(result["platform"], result["slug"])
                if _slug_matches_company(primary, company):
                    print(f"  [job-detail] Apply link in listing: {url[:70]}")
                    return result
            continue
        if any(hint in url.lower() for hint in JOB_LINK_HINTS):
            # Prefer same-domain links
            if parsed.netloc in url:
                job_url = url
                break

    if not job_url:
        return None

    print(f"  ── Job detail: {job_url[:80]}")

    # ── Visit job detail page, intercept requests ─────────────────────
    detail_requests = []

    def _on_req(req, _lst=detail_requests):
        _lst.append(req.url)

    page.on("request", _on_req)
    try:
        await page.goto(job_url, wait_until="domcontentloaded",
                        timeout=LOAD_TIMEOUT)
        await asyncio.sleep(3.0)
    except Exception as e:
        page.remove_listener("request", _on_req)
        print(f"  [WARN] Could not load job detail: {e}")
        return None
    page.remove_listener("request", _on_req)

    detail_url  = page.url
    detail_html = await page.content()
    detail_soup = BeautifulSoup(detail_html, "html.parser")

    print(f"  [xhr] Detail page: {len(detail_requests)} requests")

    # ── Apply button href — most reliable signal ───────────────────────
    APPLY_TEXT = ["apply", "apply now", "apply for this job",
                  "apply to this job", "submit application"]
    for a in detail_soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"].strip()
        if any(kw in text for kw in APPLY_TEXT):
            url  = _abs(href) or href
            result = match_ats_pattern(url)
            if result:
                primary = _primary_slug(result["platform"], result["slug"])
                if _slug_matches_company(primary, company):
                    print(f"  [job-detail] Apply button → {url[:70]}")
                    return result

    # ── All three fingerprint methods on detail page ───────────────────
    for fn_name, fn in [
        ("XHR",          lambda: _fingerprint_xhr(detail_requests, company)),
        ("rendered-URLs", lambda: _fingerprint_rendered_urls(detail_html, company)),
        ("inline-script", lambda: _fingerprint_inline_scripts(detail_html, company)),
        ("JSON-LD",       lambda: _fingerprint_json_ld(detail_html, company)),
        ("HTML",          lambda: _fingerprint_ats(detail_html, detail_url)),
    ]:
        result = fn()
        if result and result.get("slug"):
            print(f"  [job-detail] {fn_name} → platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

    return None


async def _fingerprint_via_robots_sitemap(page, base_url, company):
    """
    Probe robots.txt + sitemap chain for ATS fingerprints.

    Handles all three sitemap formats (matching sitemap.py):
      Format A — robots.txt with Sitemap: directives
      Format B — standard <urlset> sitemap with <loc> job URLs
      Format C — <sitemapindex> with sub-sitemaps (followed recursively, 2 levels)

    All fetches use page.evaluate(fetch()) — zero browser navigation.

    Why sub-sitemaps matter:
      Many companies split their sitemap.xml into a <sitemapindex> with
      sub-sitemaps by section (jobs, blog, product pages, ...).
      The root sitemap.xml has no job URLs — only sub-sitemap <loc> entries.
      We must follow those sub-sitemaps to find ATS job URLs.

      Example (Airbnb, many others):
        /sitemap.xml → <sitemapindex>
                          <sitemap><loc>/sitemap-jobs-1.xml</loc></sitemap>
        /sitemap-jobs-1.xml → <urlset>
                          <url><loc>https://boards.greenhouse.io/airbnb/jobs/...</loc></url>

    Stripe note:
      stripe.com/sitemap.xml is a sitemap index but the jobs sub-sitemap
      may be empty or redirect-only.  This function will still check the
      robots.txt Sitemap: directive and all sub-sitemaps — if Stripe's jobs
      sitemap contains boards.greenhouse.io <loc> URLs we will catch them.
      If all sitemaps are empty, we return None gracefully and fall through
      to Phase 2.

    Returns {platform, slug} or None.
    """
    from bs4 import BeautifulSoup

    origin = base_url.rstrip("/")

    # ── Shared helpers ────────────────────────────────────────────────────

    async def _fetch(url, timeout_ms=5000):
        """Fetch URL text via page context without browser navigation."""
        try:
            return await page.evaluate(
                f"""(async () => {{
                    try {{
                        const r = await fetch({json.dumps(url)},
                            {{credentials: 'omit',
                              signal: AbortSignal.timeout({timeout_ms})}});
                        if (r.ok) return await r.text();
                    }} catch(e) {{}}
                    return '';
                }})()"""
            )
        except Exception:
            return ""

    def _scan_text(text):
        """
        Run ATS fingerprinting on raw text (robots.txt, sitemap XML, etc.).
        Extracts all https?:// URLs, then checks both match_ats_pattern and
        _XHR_EXTRA_PATTERNS.  Returns {platform, slug} or None.
        """
        if not text:
            return None
        urls = re.findall(r'https?://[^\s"\'<>]+', text)
        result = _fingerprint_xhr(urls, company)
        if result:
            return result
        for pattern, platform, slug_fn in _XHR_EXTRA_PATTERNS:
            m = pattern.search(text)
            if m:
                slug = slug_fn(m)
                if _slug_matches_company(slug, company):
                    return {"platform": platform, "slug": slug}
        return None

    def _log(label, result):
        print(f"  [robots/sitemap] {label[:60]} → "
              f"platform={result['platform']}  "
              f"slug={_primary_slug(result['platform'], result['slug'])}")

    def _is_sitemap_index(soup):
        """True if this XML doc is a <sitemapindex> with <sitemap> children."""
        tags = soup.find_all("sitemap")
        return bool(tags and any(t.find("loc") for t in tags))

    def _sub_sitemap_locs(soup):
        """Extract <loc> URLs from a <sitemapindex>."""
        locs = []
        for tag in soup.find_all("sitemap"):
            loc = tag.find("loc")
            if loc:
                locs.append(loc.get_text(strip=True))
        return locs

    def _url_locs(soup):
        """Extract <loc> URLs from a standard <urlset> sitemap."""
        return [t.get_text(strip=True) for t in soup.find_all("loc")]

    async def _probe_sitemap(url, depth=0):
        """
        Fetch and scan one sitemap URL.
        depth=0 → root; depth=1 → sub-sitemap; depth=2 → sub-sub-sitemap.
        Returns {platform, slug} or None.
        """
        if depth > 2:
            return None
        text = await _fetch(url)
        if not text or len(text) < 20:
            return None

        # Direct URL scan on raw text first (catches plain-text job URLs)
        result = _scan_text(text)
        if result:
            return result

        try:
            soup = BeautifulSoup(text, "html.parser")
        except Exception:
            return None

        if _is_sitemap_index(soup):
            all_locs  = _sub_sitemap_locs(soup)
            # Prefer sub-sitemaps whose URL contains "job" / "career"
            job_locs  = [u for u in all_locs if any(
                kw in u.lower()
                for kw in ("job", "career", "position", "opening", "recruit")
            )]
            probe_order = (job_locs or all_locs)[:8]  # cap at 8 sub-sitemaps
            for sub_url in probe_order:
                r = await _probe_sitemap(sub_url, depth=depth + 1)
                if r:
                    return r
            return None

        # Standard sitemap — scan <loc> tags
        locs   = _url_locs(soup)
        result = _fingerprint_xhr(locs, company)
        if result:
            return result
        # Also scan the raw text (catches ATS URLs embedded in comments / XSL)
        return _scan_text(text)

    # ── 1. robots.txt ─────────────────────────────────────────────────────
    robots_text = await _fetch(origin + "/robots.txt")
    if robots_text and len(robots_text) > 10:
        # Sitemap: directives tell us exactly where the sitemaps live
        sitemap_directives = re.findall(
            r'(?i)^Sitemap:\s*(https?://\S+)', robots_text, re.MULTILINE
        )
        # Also check robots.txt itself for direct ATS references
        result = _scan_text(robots_text)
        if result:
            _log("/robots.txt", result)
            return result
    else:
        sitemap_directives = []

    # ── 2. Build sitemap probe list ───────────────────────────────────────
    # robots.txt directives first (most authoritative), then standard paths
    standard_paths = ["/sitemap.xml", "/sitemap_index.xml",
                      "/sitemap-jobs.xml", "/jobs-sitemap.xml"]
    seen  = set()
    queue = []
    for u in sitemap_directives + [origin + p for p in standard_paths]:
        if u not in seen:
            seen.add(u)
            queue.append(u)

    # ── 3. Probe each sitemap (with recursive sub-sitemap following) ───────
    for sitemap_url in queue[:6]:   # cap: never probe more than 6 root sitemaps
        result = await _probe_sitemap(sitemap_url, depth=0)
        if result:
            _log(sitemap_url, result)
            return result

    return None


def _fingerprint_rendered_urls(html, company):
    """
    Scan the fully-rendered DOM HTML for ATS URLs.

    Catches companies like Stripe where:
    - The career page is a custom React/Next.js frontend (no ATS scripts in HTML)
    - Jobs are fetched server-side (no ATS XHR calls from the browser)
    - BUT each job card in the rendered DOM links to the ATS apply page
      e.g. <a href="https://boards.greenhouse.io/stripe/jobs/7656504">Apply</a>

    Strategy: extract all URLs from rendered HTML via _extract_result_urls
    (which already handles /url?q= unwrapping, data-* attrs, regex fallback),
    then run match_ats_pattern + slug filter on each.

    Returns {platform, slug} from the most-frequent (platform, slug) pair,
    or None if no ATS URLs found.
    """
    urls  = _extract_result_urls(html)
    hits  = []
    tally = {}

    for url in urls:
        result = match_ats_pattern(url)
        if not result:
            continue
        primary = _primary_slug(result["platform"], result["slug"])
        if not _slug_matches_company(primary, company):
            continue
        hits.append(result)
        key = (result["platform"], primary)
        tally[key] = tally.get(key, 0) + 1

    if not tally:
        return None

    # Return the most-frequent (platform, slug) pair
    best_key   = max(tally, key=lambda k: tally[k])
    best_count = tally[best_key]
    for h in hits:
        if (_primary_slug(h["platform"], h["slug"]) == best_key[1]
                and h["platform"] == best_key[0]):
            print(f"  [url-scan] {best_count} ATS URL(s) → "
                  f"platform={h['platform']}  slug={best_key[1]}")
            return h
    return None


SKIP_DOMAINS = {
    "linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com",
    "monster.com", "careerbuilder.com", "simplyhired.com", "dice.com",
    "builtin.com", "wellfound.com", "crunchbase.com", "pitchbook.com",
    "twitter.com", "x.com", "facebook.com", "instagram.com", "youtube.com",
    "wikipedia.org", "bloomberg.com", "reuters.com", "cnbc.com",
    "greenhouse.io", "lever.co", "ashbyhq.com", "smartrecruiters.com",
    "myworkdayjobs.com", "myworkdaysite.com", "oraclecloud.com",
    "icims.com", "jobvite.com", "successfactors.com", "successfactors.eu",
    "jobs2web.com", "taleo.net", "eightfold.ai",
}

# HTML fingerprints: platform → signal strings (case-insensitive substring).
#
# ORDER MATTERS — listing platforms (Jibe, TalentBrew, Phenom, etc.) come
# BEFORE iCIMS because iCIMS often appears on the page as just an apply-link
# redirect (e.g. Rivian lists jobs via Jibe but apply redirects to icims.com).
# Checking listing platforms first prevents iCIMS from being returned as the
# primary ATS when the actual job board is powered by something else.
ATS_FINGERPRINTS = {
    # ── Listing platforms (check first) ──────────────────────────────
    "talentbrew":     ["tbcdn.talentbrew.com"],
    # Jibe CDN — careers.rivian.com, others. Slug = careers domain.
    "jibe":           ["app.jibecdn.com", "jibeapply.com", "jibecdn.com"],
    "phenom":         ["phenompeople.com", "phenom.com"],
    "eightfold":      ["eightfold.ai"],
    "avature":        ["avature.net"],
    "taleo":          ["taleo.net"],
    "oracle_hcm":     ["oraclecloud.com/hcmUI"],
    "successfactors": ["successfactors.com", "successfactors.eu"],
    "workday":        ["myworkdayjobs.com", "myworkdaysite.com"],
    # ── Custom-domain listing platforms ──────────────────────────────
    # Lever: companies like Netflix use jobs.netflix.com (custom frontend)
    # that calls the Lever API. The HTML embeds jobs.lever.co links or
    # lever.co script references.
    "lever":          ["jobs.lever.co", "lever.co/"],
    # ── Apply-redirect platforms (check last) ────────────────────────
    # iCIMS handled separately below — must come after all listing platforms
}

# When the browser lands on one of these domains we can extract the slug
# directly from the final URL via match_ats_pattern — no HTML fingerprinting
# needed.  iCIMS is intentionally NOT here: it often appears only as an
# apply-link redirect, so we let the full fingerprint logic decide whether
# iCIMS is truly the listing ATS or just a downstream apply handler.
REDIRECT_ATS_DOMAINS = [
    "greenhouse.io", "lever.co", "ashbyhq.com", "smartrecruiters.com",
    "myworkdayjobs.com", "myworkdaysite.com",
    "jobvite.com", "successfactors.com", "successfactors.eu",
    "oraclecloud.com", "taleo.net", "eightfold.ai",
]

# Link text that suggests "go to job listings" on a career landing page
LISTING_LINK_TEXT = [
    "search jobs", "view jobs", "browse jobs", "find jobs", "all jobs",
    "open positions", "view all", "see openings", "job search",
    "career opportunities", "explore jobs", "view openings",
    "current openings", "apply now", "job listings", "see all jobs",
]


def _fingerprint_ats(html, final_url):
    """
    Detect ATS from rendered page HTML + final URL after navigation.

    Returns {platform, slug} or None.

    Priority order (intentional):
    1. Final URL is a known ATS domain → match_ats_pattern on the URL
    2. TalentBrew CDN script → extract tenant_id + base domain
    3. Jibe CDN → slug = careers domain (e.g. "careers.rivian.com")
    4. Other listing platforms (Phenom, Eightfold, Taleo, etc.)
    5. iCIMS LAST — often appears only as an apply-link redirect even when
       the job board itself is powered by Jibe/Phenom/etc. Checking it last
       prevents a false "icims" result when the real listing ATS is Jibe.
       Also fixes slug: capture full subdomain from .icims.com/jobs URLs
       (e.g. "us-careers-rivian" not just "rivian").
    """
    # 1. Redirected to standard ATS URL
    for ats_domain in REDIRECT_ATS_DOMAINS:
        if ats_domain in final_url:
            result = match_ats_pattern(final_url)
            if result:
                return result
            break

    html_low = html.lower()

    # 2. TalentBrew — CDN script always present on listing page
    if "tbcdn.talentbrew.com" in html_low:
        m         = re.search(r'tbcdn\.talentbrew\.com/company/(\d+)/',
                              html, re.IGNORECASE)
        tenant_id = m.group(1) if m else ""
        parsed    = urlparse(final_url)
        base      = f"{parsed.scheme}://{parsed.netloc}"
        return {"platform": "talentbrew",
                "slug": json.dumps({"base": base, "tenant_id": tenant_id})}

    # 3. Jibe — app.jibecdn.com CDN scripts power the listing page.
    #    Slug = the careers domain itself (jibe.py uses it as the API base).
    if any(sig in html_low for sig in ("app.jibecdn.com", "jibeapply.com",
                                        "jibecdn.com")):
        parsed = urlparse(final_url)
        domain = parsed.netloc.lower().lstrip("www.")
        return {"platform": "jibe", "slug": domain}

    # 4. Other listing platforms (ordered: specific → generic)
    for platform, signals in ATS_FINGERPRINTS.items():
        if platform in ("talentbrew", "jibe"):
            continue   # already handled above
        if any(sig.lower() in html_low for sig in signals):
            # Try to extract slug from the final URL
            result = match_ats_pattern(final_url)
            if result and result["platform"] == platform:
                return result
            # Platform fingerprint found but slug not in URL → nightly XHR
            return {"platform": platform, "slug": ""}

    # 5. iCIMS — checked LAST because it appears as apply-link even when
    #    the listing ATS is Jibe/Phenom/etc.
    #    Capture the FULL subdomain before .icims.com (not just after "careers-")
    #    so "us-careers-rivian.icims.com/jobs" → slug = "us-careers-rivian"
    #    not the broken "rivian" from the old partial match.
    icims_m = re.search(
        r'(?:https?://)?([a-z0-9][a-z0-9\-]*)\.icims\.com/jobs',
        html, re.IGNORECASE,
    )
    if icims_m:
        return {"platform": "icims", "slug": icims_m.group(1)}

    return None


def _is_career_page(url, company):
    """
    True if this URL looks like the company's own career/jobs page.
    Rejects aggregators, social media, and ATS domains.
    """
    parsed       = urlparse(url)
    domain       = parsed.netloc.lower().lstrip("www.")
    path         = parsed.path.lower()

    for skip in SKIP_DOMAINS:
        if domain == skip or domain.endswith("." + skip):
            return False

    career_path = any(kw in path for kw in
                      ["/careers", "/jobs", "/work", "/join", "/openings",
                       "/opportunities", "/hiring", "/apply"])

    company_kws  = re.sub(r'[^a-z0-9]', '', company.lower())
    domain_clean = re.sub(r'[^a-z0-9]', '', domain)
    domain_match = (
        "career" in domain_clean or
        "jobs"   in domain_clean or
        company_kws[:6] in domain_clean
    )

    return career_path or domain_match


def _find_listing_link(html, base_url):
    """
    Find a link to the job listings page from a career landing page.

    Looks for:
    - Anchor text matching LISTING_LINK_TEXT ("Search Jobs", "View All", etc.)
    - Iframes pointing to known ATS domains (already embedded listing)
    - Links to /jobs or /careers sub-paths on the same domain

    Returns absolute URL string, or None.
    """
    from bs4 import BeautifulSoup
    from urllib.parse import parse_qs

    soup    = BeautifulSoup(html, "html.parser")
    parsed  = urlparse(base_url)
    origin  = f"{parsed.scheme}://{parsed.netloc}"

    def _abs(href):
        if not href:
            return None
        href = href.strip()
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return origin + href
        return None

    # Check iframes — listing may already be embedded
    for iframe in soup.find_all("iframe", src=True):
        src = iframe["src"]
        for ats_d in REDIRECT_ATS_DOMAINS + ["talentbrew", "icims"]:
            if ats_d in src:
                return _abs(src) or src

    # Anchor text match
    for a in soup.find_all("a", href=True):
        text = a.get_text(separator=" ", strip=True).lower()
        href = a["href"].strip()
        if "/url?" in href:
            q_vals = parse_qs(urlparse(href).query).get("q", [])
            href   = q_vals[0] if q_vals else href
        if any(kw in text for kw in LISTING_LINK_TEXT):
            url = _abs(href)
            if url:
                return url

    # Fallback: same-domain links with /jobs or /search in path
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/url?" in href:
            q_vals = parse_qs(urlparse(href).query).get("q", [])
            href   = q_vals[0] if q_vals else href
        url = _abs(href)
        if not url:
            continue
        p = urlparse(url)
        if p.netloc == parsed.netloc and any(
            kw in p.path.lower()
            for kw in ["/jobs", "/search", "/openings", "/positions"]
        ):
            return url

    return None


async def _phase1_career_fingerprint(company, page, domain=None):
    """
    Phase 1: Visit the company's career page and fingerprint the ATS.

    Two modes:
    A) Domain known (from prospects.txt)
       → Visit https://{domain} directly — NO Google search needed.
       → Huge benefit: saves 1 Google request per company, reducing bot risk.

    B) Domain unknown
       → Google "{company} careers" → visit top career page candidates.

    In both modes:
       → Fingerprint landing page
       → If no fingerprint: find job listing link → navigate → fingerprint
    """
    print(f"\n{'═' * 65}")
    print(f"  PHASE 1 — Career page fingerprinting")

    if domain:
        # Mode A: direct visit — no Google needed
        career_url = f"https://{domain}"
        print(f"  Domain: {career_url}  (from prospects.txt — skipping Google)")
        print(f"{'═' * 65}")
        career_urls = [career_url]
    else:
        # Mode B: Google search to find the career page
        query  = f'"{company}" careers'
        params = {"q": query, "num": 10, "hl": "en", "ie": "UTF-8"}
        print(f"  Query: {query}")
        print(f"{'═' * 65}")

        try:
            await page.goto(SEARCH_URL + "?" + urlencode(params),
                            wait_until="domcontentloaded", timeout=LOAD_TIMEOUT)
            await asyncio.sleep(2.0)
        except Exception as e:
            print(f"  [WARN] Google search failed: {e}")
            return None

        if _is_bot_blocked(await page.content()):
            print("  ⚠  BOT DETECTION on Phase 1 search — skipping")
            return None

        from bs4 import BeautifulSoup
        from urllib.parse import parse_qs
        soup      = BeautifulSoup(await page.content(), "html.parser")
        all_hrefs = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "/url?" in href:
                q_vals = parse_qs(urlparse(href).query).get("q", [])
                href   = q_vals[0] if q_vals else href
            if href.startswith("http") and "google.com" not in href:
                all_hrefs.append(href)

        seen, candidates = set(), []
        for u in all_hrefs:
            if u not in seen:
                seen.add(u)
                candidates.append(u)

        career_urls = [u for u in candidates if _is_career_page(u, company)]
        print(f"\n  {len(candidates)} total URLs, {len(career_urls)} career candidates:")
        for u in career_urls[:6]:
            print(f"    → {u[:90]}")

    # ── Visit candidates, intercept XHR, fingerprint ─────────────────────
    for career_url in career_urls[:4]:
        print(f"\n  ── Visiting: {career_url[:80]}")

        # ── Landing page ──────────────────────────────────────────────
        landing_requests = []

        def _on_request(req, _lst=landing_requests):
            _lst.append(req.url)

        page.on("request", _on_request)
        try:
            await page.goto(career_url, wait_until="domcontentloaded",
                            timeout=LOAD_TIMEOUT)
            # Extra wait: JS frameworks fire XHR after DOMContentLoaded
            await asyncio.sleep(3.0)
        except Exception as e:
            page.remove_listener("request", _on_request)
            print(f"  [WARN] Could not load: {e}")
            continue
        page.remove_listener("request", _on_request)

        landing_url  = page.url
        landing_html = await page.content()

        print(f"  [xhr] Captured {len(landing_requests)} network requests")

        # Method 1: XHR — catches companies whose browser calls ATS API directly
        #   e.g. career page → fetch(boards-api.greenhouse.io/...)
        result = _fingerprint_xhr(landing_requests, company)
        if result:
            print(f"  ✅ XHR fingerprint on landing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # Method 2: Rendered URL scan — catches server-side rendered frontends
        #   e.g. Stripe: fetches jobs server-side but job cards link to
        #   boards.greenhouse.io/stripe/jobs/... in the rendered DOM
        result = _fingerprint_rendered_urls(landing_html, company)
        if result:
            print(f"  ✅ Rendered-URL scan on landing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # Method 3: HTML script/CDN fingerprint — catches TalentBrew CDN,
        #   Jibe CDN, iCIMS embedded iframes, etc.
        result = _fingerprint_ats(landing_html, landing_url)
        if result and result.get("slug"):
            print(f"  ✅ HTML fingerprint on landing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        _html_result_landing = result   # save platform-only match as fallback

        # Method 4: Inline script scan — catches Next.js __NEXT_DATA__,
        #   window.__INITIAL_STATE__, and ATS config objects embedded in page JS.
        result = _fingerprint_inline_scripts(landing_html, company)
        if result:
            print(f"  ✅ Inline-script scan on landing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # Method 5: JSON-LD structured data — JobPosting url/sameAs fields
        #   contain canonical ATS job URL (required for Google rich-snippets).
        result = _fingerprint_json_ld(landing_html, company)
        if result:
            print(f"  ✅ JSON-LD on landing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # NOTE: robots.txt / sitemap.xml probe intentionally skipped.
        # In practice most company sitemaps are either empty, retail/product
        # sitemaps (not jobs), or stale (last updated 2021+). The false-positive
        # noise and extra fetch latency outweigh the rare hit.
        # _fingerprint_via_robots_sitemap() is kept as a standalone utility
        # for targeted manual investigation when needed.

        # All landing-page methods exhausted — look for listing link and navigate
        listing_url = _find_listing_link(landing_html, landing_url)
        if not listing_url or listing_url == landing_url:
            if not listing_url:
                print(f"  (no listing link found on {landing_url[:60]})")
            if _html_result_landing:
                print(f"  ⚠  Platform={_html_result_landing['platform']} detected "
                      f"but no slug — marking for nightly capture")
                return _html_result_landing
            continue

        # ── Listing page ──────────────────────────────────────────────
        print(f"  ── Listing page: {listing_url[:80]}")

        listing_requests = []

        def _on_request2(req, _lst=listing_requests):
            _lst.append(req.url)

        page.on("request", _on_request2)
        try:
            await page.goto(listing_url, wait_until="domcontentloaded",
                            timeout=LOAD_TIMEOUT)
            await asyncio.sleep(3.0)
        except Exception as e:
            page.remove_listener("request", _on_request2)
            print(f"  [WARN] Could not load listing page: {e}")
            continue
        page.remove_listener("request", _on_request2)

        final_url    = page.url
        listing_html = await page.content()

        print(f"  [xhr] Captured {len(listing_requests)} network requests")

        # XHR check
        result = _fingerprint_xhr(listing_requests, company)
        if result:
            print(f"  ✅ XHR fingerprint on listing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # Rendered URL scan
        result = _fingerprint_rendered_urls(listing_html, company)
        if result:
            print(f"  ✅ Rendered-URL scan on listing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # HTML script/CDN fingerprint
        result = _fingerprint_ats(listing_html, final_url)
        if result and result.get("slug"):
            print(f"  ✅ HTML fingerprint on listing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        _html_result_listing = result   # platform-only fallback (slug empty or None)

        # Method 4: Inline script scan on listing page
        result = _fingerprint_inline_scripts(listing_html, company)
        if result:
            print(f"  ✅ Inline-script scan on listing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # Method 5: JSON-LD structured data on listing page
        result = _fingerprint_json_ld(listing_html, company)
        if result:
            print(f"  ✅ JSON-LD on listing page: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # Method 6: Navigate to first job → intercept XHR + check Apply button.
        #   Last resort before giving up on this career_url candidate.
        result = await _fingerprint_via_job_detail(
            page, listing_html, final_url, company
        )
        if result:
            print(f"  ✅ Job-detail navigation: "
                  f"platform={result['platform']}  "
                  f"slug={_primary_slug(result['platform'], result['slug'])}")
            return result

        # All methods exhausted — if HTML fingerprint found a platform (no slug),
        # return it as a partial result so the batch summary can mark it for
        # nightly XHR capture rather than silently treating it as fully unknown.
        if _html_result_listing:
            print(f"  ⚠  Platform={_html_result_listing['platform']} detected "
                  f"but no slug — marking for nightly capture")
            return _html_result_listing

        print(f"  (no fingerprint on landing or listing page)")

    print(f"\n  Phase 1 complete — no fingerprint found")
    return None


# ─────────────────────────────────────────
# PHASE 2: GOOGLE SITE-FILTER SEARCH
# ─────────────────────────────────────────
#
# Fallback when Phase 1 finds nothing. Runs:
#   A) Combined OR query (Greenhouse, Workday, Ashby, etc.) — 3 pages
#   B) Dedicated single-site queries (iCIMS, Lever) — 1 page each
#
# Confidence: top candidate must have ≥80% of total hits AND ≥5 hits.
# Anything below 80% → unknown/custom (nightly XHR capture will handle it).

GOOGLE_CONFIDENCE_PCT = 80   # % threshold
GOOGLE_MIN_HITS       = 3    # min hits before % check (lower since natural queries
                             # return mixed results — fewer ATS hits per page)


def _check_google_confidence(all_hits):
    """
    Return (platform, slug, pct) if top candidate meets threshold, else None.
    Threshold: ≥80% of total hits AND ≥GOOGLE_MIN_HITS total.
    """
    if not all_hits:
        return None
    ranked = _score(all_hits)
    if not ranked:
        return None
    best_plat, best_pslug, best_count, best_slug = ranked[0]
    total = len(all_hits)
    pct   = int(best_count / total * 100)
    if total >= GOOGLE_MIN_HITS and pct >= GOOGLE_CONFIDENCE_PCT:
        return best_plat, best_slug, pct
    return None


async def _human_search(page, query):
    """
    Submit a Google search by *typing* into the search box like a real user.

    WHY typing instead of direct URL navigation:
      No human ever types a raw URL like
        `google.com/search?q="Stripe"+(site:greenhouse.io+OR+...)` into
      the address bar.  Real users sit on google.com and type a query.
      Submitting programmatic URL requests with long structured queries is
      a textbook bot fingerprint that Google's abuse system scores heavily.

    Approach:
      1. If already on google.com: find the existing search box, clear it,
         retype the new query (mirrors how a user edits a search).
      2. If not on google.com: navigate home first, then type.
      3. Char-by-char typing with per-keystroke random delays (40–150 ms).
         4% chance of a brief "hesitation" pause mid-query (200–600 ms) to
         simulate thinking or a near-typo correction.
      4. 400–1000 ms pause before pressing Enter (humans review the query).
      5. Falls back to direct URL navigation if the search box is not found
         (shouldn't happen with a real Google session).

    Returns True if the human-typing path succeeded, False on fallback.
    """
    SEARCH_SELECTORS = [
        'textarea[name="q"]',   # modern Google
        'input[name="q"]',      # legacy / some variants
        '[aria-label="Search"]',
    ]

    # Navigate to google.com if we're on a non-Google page
    if "google.com" not in page.url:
        try:
            await page.goto(GOOGLE_HOME, wait_until="domcontentloaded",
                            timeout=LOAD_TIMEOUT)
            await asyncio.sleep(random.uniform(0.6, 1.2))
        except Exception:
            pass

    for selector in SEARCH_SELECTORS:
        try:
            loc = page.locator(selector).first
            if not await loc.is_visible(timeout=2000):
                continue

            await loc.click()
            await asyncio.sleep(random.uniform(0.2, 0.5))

            # Select-all + Delete mirrors Ctrl+A then typing (not fill())
            await loc.press("Control+a")
            await asyncio.sleep(random.uniform(0.05, 0.12))
            await loc.press("Delete")
            await asyncio.sleep(random.uniform(0.1, 0.25))

            # Type character by character
            for char in query:
                # loc.type() is deprecated; use keyboard.type one char at a time
                await page.keyboard.type(char)
                delay = random.uniform(0.04, 0.15)
                # ~4% chance of brief hesitation per character
                if random.random() < 0.04:
                    delay += random.uniform(0.20, 0.60)
                await asyncio.sleep(delay)

            # Brief review pause before submit
            await asyncio.sleep(random.uniform(0.4, 1.0))
            await loc.press("Enter")
            await page.wait_for_load_state("domcontentloaded",
                                           timeout=LOAD_TIMEOUT)
            await asyncio.sleep(random.uniform(2.0, 3.5))
            return True

        except Exception:
            continue

    # Fallback: direct URL navigation
    try:
        await page.goto(
            SEARCH_URL + "?" + urlencode(
                {"q": query, "num": PAGE_SIZE, "hl": "en", "ie": "UTF-8"}
            ),
            wait_until="domcontentloaded", timeout=LOAD_TIMEOUT,
        )
        await asyncio.sleep(2.5)
    except Exception as e:
        print(f"  [WARN] Search navigation failed: {e}")
    return False


async def _phase2_google_search(company, page, save_html=False, _ctx=None):
    """
    Phase 2: Google site-filter search as fallback.

    _ctx: optional mutable dict — sets _ctx["bot_blocked"] = True when
          Google bot detection fires, so the batch loop can apply backoff.

    Returns (platform, slug, confidence_label, pages_used) or None.
    """
    print(f"\n{'═' * 65}")
    print(f"  PHASE 2 — Google site-filter search")
    print(f"{'═' * 65}")

    all_hits    = []
    bot_blocked = False

    async def _run_group(label, site_filters):
        """
        Run one permuted OR-group query using human-like typing.
        Query structure is randomised on every call — see _build_group_query.
        """
        nonlocal all_hits, bot_blocked
        query = _build_group_query(company, site_filters)
        print(f"\n{'─' * 65}")
        print(f"  [{label}]  {query}")

        typed = await _human_search(page, query)
        print(f"  [search] {'typed' if typed else 'url-nav'}")

        await _save_playwright_cookies(page.context)
        html = await page.content()

        if _is_bot_blocked(html):
            print(f"  ⚠  BOT DETECTION on {label}")
            bot_blocked = True
            if _ctx is not None:
                _ctx["bot_blocked"] = True
            return

        if save_html and label == "Group A":
            DEBUG_HTML.write_text(html, encoding="utf-8")
            print(f"  [debug] HTML saved ({len(html):,} bytes)")

        hits = _extract_ats_hits(html, company)
        all_hits.extend(hits)
        _print_hit_table(hits)
        _print_score_table(all_hits, label)

    # ── 2A: Group A (Greenhouse, Workday, Ashby) ─────────────────────
    await _run_group("Group A", SITE_FILTER_GROUP_A)
    if not bot_blocked:
        conf = _check_google_confidence(all_hits)
        if conf:
            plat, slug, pct = conf
            print(f"\n  ✅ HIGH CONFIDENCE — {pct}% ({len(all_hits)} hits)")
            return plat, slug, "HIGH", 1

    # ── 2B: Group B (SmartRecruiters, Oracle, Jobvite, SF) ───────────
    if not bot_blocked:
        await asyncio.sleep(random.uniform(*PAGE_DELAY))
        await _run_group("Group B", SITE_FILTER_GROUP_B)
        if not bot_blocked:
            conf = _check_google_confidence(all_hits)
            if conf:
                plat, slug, pct = conf
                print(f"\n  ✅ HIGH CONFIDENCE — {pct}% ({len(all_hits)} hits)")
                return plat, slug, "HIGH", 2

    # ── 2C: Dedicated searches (iCIMS, Lever) ────────────────────────
    if not bot_blocked:
        for hint, site_filter in DEDICATED_SEARCHES:
            ded_query = _build_dedicated_query(company, site_filter)
            print(f"\n{'─' * 65}")
            print(f"  [Dedicated: {hint}]  {ded_query}")
            await asyncio.sleep(random.uniform(*PAGE_DELAY))

            typed = await _human_search(page, ded_query)
            print(f"  [search] {'typed' if typed else 'url-nav'}")

            await _save_playwright_cookies(page.context)
            ded_html = await page.content()

            if _is_bot_blocked(ded_html):
                print(f"  ⚠  BOT DETECTION on {hint} search — skipping")
                continue

            ded_hits = _extract_ats_hits(ded_html, company)
            if not ded_hits:
                print(f"  (no {hint} hits for this company)")
                continue

            all_hits.extend(ded_hits)
            _print_hit_table(ded_hits)
            _print_score_table(all_hits, f"after {hint}")

            conf = _check_google_confidence(all_hits)
            if conf:
                plat, slug, pct = conf
                print(f"\n  ✅ HIGH CONFIDENCE via {hint} — {pct}%")
                return plat, slug, "HIGH", MAX_PAGES

    # Final: below threshold → unknown/custom
    if all_hits:
        ranked = _score(all_hits)
        best_plat, _, best_count, best_slug = ranked[0]
        total = len(all_hits)
        pct   = int(best_count / total * 100)
        print(f"\n  Best candidate: {best_plat} at {pct}% "
              f"({best_count}/{total}) — below {GOOGLE_CONFIDENCE_PCT}% threshold")
        print(f"  → Marking as UNKNOWN for nightly XHR capture")

    return None


# ─────────────────────────────────────────
# SHARED HIT HELPERS
# ─────────────────────────────────────────

def _extract_ats_hits(html, company):
    """Extract + filter ATS URL hits from rendered page HTML."""
    hits = []
    for url in _extract_result_urls(html):
        result = match_ats_pattern(url)
        if not result:
            continue
        primary = _primary_slug(result["platform"], result["slug"])
        if not _slug_matches_company(primary, company):
            continue
        hits.append(result)
    return hits


def _print_hit_table(hits):
    print()
    print(f"  {'URL':<55}  {'PLATFORM':<18}  SLUG")
    print(f"  {'─' * 55}  {'─' * 18}  {'─' * 25}")
    if not hits:
        print("  (none)")
        return
    for h in hits:
        primary = _primary_slug(h["platform"], h["slug"])
        print(f"  ✓ {'...':<55}  {h['platform']:<18}  {primary[:25]}")


def _print_score_table(all_hits, label):
    ranked = _score(all_hits)
    total  = len(all_hits)
    print()
    print(f"  ── Score after {label} ({total} hits total) ──")
    for plat, pslug, count, _ in ranked[:6]:
        pct = int(count / max(total, 1) * 100)
        bar = "█" * min(count, 10) + "░" * max(0, 10 - count)
        print(f"  {plat:<18}  {pslug:<28}  {bar}  {count}/{total} ({pct}%)")


# ─────────────────────────────────────────
# PLAYWRIGHT ORCHESTRATOR
# ─────────────────────────────────────────

# Delay between companies in batch mode (seconds).
# Long enough to look human, short enough to be practical.
INTER_COMPANY_DELAY = (4.0, 7.0)


async def _setup_browser(cookies_dict, headers_dict, headless, use_stealth,
                          playwright_instance):
    """
    Launch browser, inject cookies, warm Google session.
    Returns (browser, page) ready for searches.

    Stealth hardening (based on scrapeops.io/playwright-undetectable guide):
      • --disable-blink-features=AutomationControlled  removes navigator.webdriver flag
      • --enable-webgl / --use-gl=swiftshader          makes WebGL fingerprint realistic
      • --enable-accelerated-2d-canvas                 matches real Chrome behaviour
      • --no-first-run / --disable-extensions          suppresses first-run UI noise
      • timezoneId / locale / geolocation              full locale fingerprint
      • addInitScript: delete navigator.__proto__.webdriver  belt-and-suspenders removal
      • Warm-up includes a brief random mouse movement to seed human-behaviour signals
    """
    browser = await playwright_instance.chromium.launch(
        headless=headless,
        args=[
            # Core anti-detection
            "--disable-blink-features=AutomationControlled",
            # WebGL / canvas — make fingerprint look like a real GPU-backed browser
            "--enable-webgl",
            "--use-gl=swiftshader",
            "--enable-accelerated-2d-canvas",
            # Suppress first-run UI / extension prompts that look bot-like
            "--no-first-run",
            "--disable-extensions",
            "--disable-infobars",
            # Misc stability flags present in normal Chrome installs
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    )
    context = await browser.new_context(
        user_agent=headers_dict.get(
            "user-agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",   # realistic US timezone
        extra_http_headers={
            k: v for k, v in headers_dict.items()
            if k.lower() not in (
                "user-agent", "cookie", "host",
                "content-length", "transfer-encoding",
                "connection", "keep-alive",
            )
        },
    )

    # Belt-and-suspenders: remove navigator.webdriver at the JS level
    # before any page script runs (addInitScript fires before page JS).
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
            configurable: true,
        });
        // Also spoof plugins length — 0 plugins is a bot tell
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
    """)

    pw_cookies = _playwright_cookies(cookies_dict)
    if pw_cookies:
        await context.add_cookies(pw_cookies)
        print(f"[INFO] Injected {len(pw_cookies)} cookies")

    page = await context.new_page()

    if use_stealth:
        from playwright_stealth import stealth_async
        await stealth_async(page)
        print("[INFO] Stealth mode: enabled")

    print("[INFO] Warming session via google.com/ ...")
    try:
        await page.goto(GOOGLE_HOME, wait_until="domcontentloaded",
                        timeout=LOAD_TIMEOUT)
        # Random mouse movement during warm-up — seeds human-behaviour signals
        for _ in range(random.randint(3, 6)):
            await page.mouse.move(
                random.randint(100, 1100),
                random.randint(100, 700),
            )
            await asyncio.sleep(random.uniform(0.08, 0.22))
        await asyncio.sleep(random.uniform(1.5, 2.5))
        await _save_playwright_cookies(context)
        print("[INFO] Warm-up OK")
    except Exception as e:
        print(f"[WARN] Warm-up failed: {e} — continuing anyway")

    return browser, page


async def _detect_company(company, page, domain=None, save_html=False, _ctx=None):
    """
    Run Phase 1 + Phase 2 detection for a single company.
    Browser/page must already be set up and warmed.

    domain: career page domain from prospects.txt (e.g. "jobs.netflix.com").
            When provided, Phase 1 visits it directly — no Google search.
    _ctx:   mutable dict for side-channel signals (e.g. bot_blocked flag).

    Returns (platform, slug, confidence, pages) or None.
    """
    # Phase 1: career page fingerprinting (direct if domain known)
    result = await _phase1_career_fingerprint(company, page, domain=domain)
    if result:
        platform = result["platform"]
        slug     = result.get("slug", "")
        if slug:
            return platform, slug, "HIGH", 1
        else:
            return platform, "", "UNKNOWN", 1

    # Short pause before hitting Google (Phase 1 may have already used Google)
    delay = random.uniform(*PHASE_TRANSITION_DELAY)
    print(f"\n  [phase gap] Waiting {delay:.1f}s before Phase 2 Google search...")
    await asyncio.sleep(delay)

    # Phase 2: Google site-filter search
    result = await _phase2_google_search(company, page, save_html=save_html,
                                         _ctx=_ctx)
    if result:
        return result

    # Phase 3: unknown / custom → nightly XHR capture
    return None


async def _batch_detect_async(companies_with_domains, cookies_dict, headers_dict,
                               headless=True, save_html=False):
    """
    Detect ATS for multiple companies using a single shared browser session.
    Warmed up once. Saves cookies after each company.

    companies_with_domains: list of (company_name, domain_or_None)
      domain from prospects.txt lets Phase 1 skip the Google discovery search.

    Returns list of (company, result) where result is
    (platform, slug, confidence, pages) or None.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[ERROR] playwright not installed: pip install playwright && "
              "playwright install chromium")
        sys.exit(1)

    try:
        from playwright_stealth import stealth_async as _stealth   # noqa: F401
        use_stealth = True
    except ImportError:
        use_stealth = False
        print("[WARN] playwright-stealth not installed — bot detection risk higher")

    results      = []
    bot_streak   = 0   # consecutive bot-detection events → longer backoff

    async with async_playwright() as p:
        browser, page = await _setup_browser(
            cookies_dict, headers_dict, headless, use_stealth, p
        )

        for i, (company, domain) in enumerate(companies_with_domains):
            total = len(companies_with_domains)
            print(f"\n\n{'#' * 65}")
            print(f"  [{i+1}/{total}]  {company}"
                  + (f"  (domain: {domain})" if domain else ""))
            print(f"{'#' * 65}")

            _ctx = {}
            result = await _detect_company(company, page, domain=domain,
                                           save_html=(save_html and i == 0),
                                           _ctx=_ctx)
            results.append((company, result))

            # Track bot detection for backoff
            if _ctx.get("bot_blocked"):
                bot_streak += 2

            # Save cookies after each company (keeps session fresh)
            await _save_playwright_cookies(page.context)

            if i < total - 1:
                if bot_streak > 0:
                    delay = random.uniform(*INTER_COMPANY_DELAY_BACKOFF)
                    print(f"\n  [batch] Bot streak={bot_streak} — "
                          f"backing off {delay:.0f}s before next company...")
                    bot_streak = max(0, bot_streak - 1)
                else:
                    delay = random.uniform(*INTER_COMPANY_DELAY)
                    print(f"\n  [batch] Waiting {delay:.1f}s before next company...")
                await asyncio.sleep(delay)

        await browser.close()

    return results


# Single-company convenience wrapper (keeps CLI backward-compat)
async def _detect_ats_async(company, cookies_dict, headers_dict,
                             headless=True, save_html=False, domain=None):
    results = await _batch_detect_async(
        [(company, domain)], cookies_dict, headers_dict, headless, save_html
    )
    return results[0][1] if results else None


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

def _print_single_result(company, result):
    """Print result for one company."""
    print(f"\n{'=' * 65}")
    print(f"  RESULT  —  {company}")
    print(f"{'=' * 65}")

    if result:
        platform, slug, confidence, pages = result
        slug_display = slug
        if slug and slug.startswith("{"):
            try:
                slug_display = json.dumps(json.loads(slug), indent=4)
            except Exception:
                pass

        icon = ("✅" if confidence == "HIGH"
                else "⚠️" if confidence == "UNKNOWN"
                else "🟡")
        print(f"  {icon} Confidence: {confidence}")
        print(f"  Platform:   {platform}")
        print(f"  Slug:       {slug_display or '(not extracted)'}")
        print(f"  Pages used: {pages}")
        if confidence == "UNKNOWN":
            print("  → Platform detected but slug unknown — queued for nightly XHR capture.")
        elif confidence != "HIGH":
            print("  ⚠  Not high confidence — verify manually before storing.")
    else:
        print("  ✗ No ATS detected — queued for nightly XHR capture.")

    print()


def _print_batch_summary(all_results):
    """Print summary table for batch run."""
    print(f"\n\n{'═' * 75}")
    print(f"  BATCH SUMMARY  —  {len(all_results)} companies")
    print(f"{'═' * 75}")

    col_co   = 28
    col_plat = 16
    col_slug = 28
    col_conf =  8

    header = (f"  {'COMPANY':<{col_co}}  {'PLATFORM':<{col_plat}}"
              f"  {'SLUG':<{col_slug}}  CONF")
    print(header)
    print(f"  {'─' * col_co}  {'─' * col_plat}  {'─' * col_slug}  {'─' * col_conf}")

    high = unknown = failed = 0

    for company, result in all_results:
        co = company[:col_co]
        if result:
            platform, slug, confidence, _ = result
            # Unwrap Workday JSON slug for display
            slug_disp = slug
            if slug.startswith("{"):
                try:
                    d = json.loads(slug)
                    slug_disp = d.get("slug") or d.get("base", slug)
                except Exception:
                    pass
            plat = platform[:col_plat]
            sl   = slug_disp[:col_slug]
            conf = confidence[:col_conf]
            icon = ("✅" if confidence == "HIGH"
                    else "⚠️" if confidence == "UNKNOWN"
                    else "🟡")
            if confidence == "HIGH":
                high += 1
            else:
                unknown += 1
        else:
            plat, sl, conf, icon = "UNKNOWN/CUSTOM", "", "UNKNOWN", "✗ "
            failed += 1

        print(f"  {co:<{col_co}}  {icon} {plat:<{col_plat}}  "
              f"{sl:<{col_slug}}  {conf}")

    print(f"  {'─' * col_co}  {'─' * col_plat}  {'─' * col_slug}  {'─' * col_conf}")
    print(f"  High confidence: {high}  |  Unknown/partial: {unknown}  "
          f"|  No ATS: {failed}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Google-based ATS detection using Playwright.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_google_ats.py "Capital One"
  python test_google_ats.py "Charles Schwab" "Stripe" "Rivian" "ARM"
  python test_google_ats.py                          # uses prospects.txt
  python test_google_ats.py --file companies.txt
  python test_google_ats.py "Stripe" --headful --save-html

File format (prospects.txt or any --file):
  One company per line.  "Company,domain.com" also works — domain ignored.
  Lines starting with # are comments.
        """,
    )
    parser.add_argument(
        "companies",
        nargs="*",
        help='One or more company names e.g. "Capital One" "Stripe"',
    )
    parser.add_argument(
        "--file", "-f",
        metavar="FILE",
        help="Text file with one company name per line (# = comment)",
    )
    parser.add_argument("--headful", action="store_true",
                        help="Show browser window (useful for debugging CAPTCHA)")
    parser.add_argument("--save-html", action="store_true",
                        help=f"Save rendered page 1 HTML to {DEBUG_HTML.name}")
    parser.add_argument(
        "--output", "-o", metavar="FILE",
        help="Save all console output to FILE while still printing to screen. "
             "Use this instead of shell piping (Tee-Object / > file) which "
             "breaks Playwright's Node subprocess on Windows.",
    )
    args = parser.parse_args()

    # Install Tee FIRST — all subsequent prints go to console + file.
    _log_fh = None
    if args.output:
        try:
            _log_fh = open(args.output, "w", encoding="utf-8")
            sys.stdout = _Tee(sys.__stdout__, _log_fh)
            sys.stderr = _Tee(sys.__stderr__, _log_fh)
        except Exception as e:
            print(f"[WARN] Could not open output file {args.output}: {e}")

    # Collect (company, domain_or_None) pairs
    # CLI args have no domain; file entries may have "Company,domain.com"
    companies_with_domains = [
        (c.strip(), None) for c in args.companies if c.strip()
    ]

    # --file or default prospects.txt when no companies given on CLI
    file_path = Path(args.file) if args.file else (
        PROSPECTS_FILE if not companies_with_domains else None
    )
    if file_path:
        if not file_path.exists():
            if args.file:
                print(f"[ERROR] File not found: {file_path}")
                sys.exit(1)
        else:
            try:
                lines = file_path.read_text(encoding="utf-8").splitlines()
                for line in lines:
                    line = line.split("#")[0].strip()
                    if not line:
                        continue
                    parts  = [p.strip() for p in line.split(",")]
                    name   = parts[0]
                    domain = parts[1] if len(parts) > 1 else None
                    if name:
                        companies_with_domains.append((name, domain))
                print(f"[INFO] Loaded {len(companies_with_domains)} companies "
                      f"from {file_path.name} "
                      f"({sum(1 for _, d in companies_with_domains if d)} with domain)")
            except Exception as e:
                print(f"[ERROR] Could not read {file_path}: {e}")
                sys.exit(1)

    if not companies_with_domains:
        parser.print_help()
        sys.exit(0)

    print(f"\n{'=' * 65}")
    print(f"  ATS Detection via Google (Playwright)")
    print(f"  Companies: {len(companies_with_domains)}")
    print(f"{'=' * 65}")

    cookies, headers = _load_curl_config()
    print(f"[INFO] {len(cookies)} cookies, {len(headers)} headers loaded")

    all_results = asyncio.run(
        _batch_detect_async(
            companies_with_domains,
            cookies,
            headers,
            headless=not args.headful,
            save_html=args.save_html,
        )
    )

    # Print individual results
    for company, result in all_results:
        _print_single_result(company, result)

    # Summary table only meaningful for batch
    if len(all_results) > 1:
        _print_batch_summary(all_results)

    # Close log file if --output was used
    if _log_fh:
        try:
            _log_fh.flush()
            _log_fh.close()
        except Exception:
            pass
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f"[INFO] Output saved → {args.output}")


if __name__ == "__main__":
    main()
