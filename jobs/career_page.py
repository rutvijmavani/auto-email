# jobs/career_page.py — Phase 3a: Career page ATS scanner
#
# Three-layer detection per URL:
#   Layer 1 — HTTP redirect: company.com/careers → ats-domain.com/{slug}
#   Layer 2 — HTML deep scan: ATS URL found in any attribute (src, href,
#              action, data-*) or in inline <script> content
#   Layer 3 — Job link follow: individual job pages almost always embed or
#              link to the ATS directly (e.g. Greenhouse iframe, Workday
#              apply redirect).  When the top-level career page scan misses,
#              we extract 2-3 job listing links and re-run Layers 1+2 on each.
#
# This completely replaces the need for Serper/Google for most platforms.
# Greenhouse, Lever, Ashby, SmartRecruiters → caught by Phase 2 API probe.
# Workday, Oracle HCM, iCIMS, SuccessFactors → caught here.

import re
import requests
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup

from logger import get_logger
from jobs.ats.patterns import match_ats_pattern, validate_slug_for_company

logger = get_logger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TIMEOUT = 10

# Top-level career page paths to try first
CAREER_PATHS = [
    "/careers",
    "/careers/",
    "/jobs",
    "/jobs/",
    "/about/careers",
    "/company/careers",
    "/en/careers",
    "/us/careers",
    "/join-us",
    "/work-with-us",
    "/work-here",
    "/opportunities",
]

# ATS domains — used to fast-skip script content that can't contain ATS URLs
# (avoids parsing every inline script in the page)
_ATS_SCRIPT_HINTS = (
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "smartrecruiters.com",
    "myworkdayjobs.com",
    "myworkdaysite.com",
    "oraclecloud.com",
    "icims.com",
    "successfactors.com",
    "jobs2web.com",
    "jobvite.com",
    "taleo.net",
    "eightfold.ai",
    "avature.net",
    "phenompeople.com",
    "talentbrew.com",
    "jibecdn.com",
    "myjobs.adp.com",
    # Note: vscdn.net is Eightfold's CDN but not a reliable ATS signal on its own
)

# How many individual job links to follow when top-level scan misses
_MAX_JOB_LINKS = 3

# Platforms where the slug is opaque (not derived from company name) —
# skip slug-vs-company validation for these
_OPAQUE_SLUG_PLATFORMS = {"workday", "oracle_hcm"}

# Platforms where we detect presence but get the slug from a richer source
_RICH_SLUG_PLATFORMS = {"phenom", "talentbrew", "avature"}


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def detect_via_career_page(company, domain):
    """
    Phase 3a: Scan company career page for ATS fingerprints.

    Args:
        company: company name  e.g. "Stripe"
        domain:  company domain e.g. "stripe.com"

    Returns:
        {"platform": ..., "slug": ...}  if found
        None                            if not found
    """
    if not domain:
        logger.debug("[P3a] No domain for %r — skipping", company)
        return None

    domain = domain.lower().strip()
    if domain.startswith("http"):
        domain = re.sub(r'^https?://', '', domain).rstrip('/')

    logger.debug("[P3a] Scanning: company=%r domain=%s", company, domain)

    # ── Layer 1 + 2: scan standard career paths ────────────────────────────
    first_career_html = None
    first_career_url  = None
    # Eightfold is treated as tentative — many companies embed Eightfold
    # tracking / LinkedIn RMS scripts on their career page without actually
    # being Eightfold customers (e.g. Netflix uses Workday but loads an
    # Eightfold real-time-listing widget).  We keep scanning and only fall
    # back to the Eightfold result if no harder ATS is found in Layer 3.
    tentative_eightfold = None

    for path in CAREER_PATHS:
        url = f"https://{domain}{path}"
        result, html, final_url = _fetch_and_scan(url, company)
        if result:
            if result["platform"] == "eightfold":
                if tentative_eightfold is None:
                    logger.debug("[P3a tentative Eightfold] %r via %s — continuing scan",
                                 company, url)
                    tentative_eightfold = result
                # Always keep scanning past Eightfold — harder ATS may follow.
            else:
                logger.info("[P3a HIT] %r → %s / %s via %s",
                            company, result["platform"], result["slug"], url)
                return result
        if html is not None and first_career_html is None:
            first_career_html = html
            first_career_url  = final_url

    # ── Layer 3: follow job listing links ─────────────────────────────────
    # Individual job pages almost always link to or embed the ATS directly
    # (e.g. Greenhouse apply iframe, Workday apply redirect).
    if first_career_html and first_career_url:
        result = _follow_job_links(
            first_career_html, first_career_url, company, domain
        )
        if result:
            logger.info("[P3a HIT via job link] %r → %s / %s",
                        company, result["platform"], result["slug"])
            return result

    # ── Eightfold fallback ─────────────────────────────────────────────────
    # Nothing harder found — accept the tentative Eightfold result.
    if tentative_eightfold:
        logger.info("[P3a HIT Eightfold fallback] %r → %s / %s",
                    company, tentative_eightfold["platform"], tentative_eightfold["slug"])
        return tentative_eightfold

    logger.debug("[P3a MISS] %r (domain=%s)", company, domain)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Eightfold domain enrichment
# ─────────────────────────────────────────────────────────────────────────────

# Common career-page subdomain prefixes to strip when deriving company domain
_CAREER_PREFIXES = (
    "careers.", "career.", "jobs.", "job.", "work.", "apply.",
    "hiring.", "talent.", "join.", "opportunities.",
)

def _enrich_eightfold_domain(result, page_url):
    """
    After detecting an Eightfold slug, fill in the domain= field from the
    career page URL that was being scanned.

    The Eightfold pattern stores domain="" because the slug URL alone
    ({slug}.eightfold.ai) doesn't tell us the company's real domain.
    At detection time we recover it two ways (in priority order):

    1. domain= query param — Eightfold passes it explicitly in the URL:
         apply.starbucks.com/careers?domain=starbucks.com  → starbucks.com
    2. Hostname prefix strip — fallback when no query param present:
         careers.starbucks.com/jobs  → starbucks.com
         jobs.lamresearch.com/       → lamresearch.com

    Hosted tenants (slug.eightfold.ai) are skipped — the slug already
    encodes the subdomain, no separate domain field needed.

    Only applied when the result is Eightfold and domain is currently empty.
    Leaves all other platforms untouched.
    """
    if not result or result.get("platform") != "eightfold":
        return result
    if not page_url:
        return result

    import json as _json
    try:
        slug_info = _json.loads(result["slug"])
    except (ValueError, TypeError, KeyError):
        return result

    # Only enrich if domain is missing
    if slug_info.get("domain"):
        return result

    try:
        parsed = urlparse(page_url)

        # Prefer explicit domain= query param — Eightfold embeds it in the URL
        # e.g. apply.starbucks.com/careers?domain=starbucks.com
        qs     = parse_qs(parsed.query)
        domain = (qs.get("domain") or [""])[0].strip()

        if not domain:
            # Fallback: strip common career-page subdomain prefixes from hostname
            # e.g. careers.starbucks.com → starbucks.com
            host   = parsed.hostname or ""
            domain = host
            for prefix in _CAREER_PREFIXES:
                if host.startswith(prefix):
                    domain = host[len(prefix):]
                    break

        # Exclude eightfold.ai itself (hosted tenant — slug already IS the domain prefix)
        if domain.endswith(".eightfold.ai"):
            domain = ""

        if domain:
            slug_info["domain"] = domain
            result = dict(result)
            result["slug"] = _json.dumps(slug_info)
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# HTTP fetch + scan
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_and_scan(url, company):
    """
    Fetch URL, run Layers 1 + 2.

    Returns:
        (result, html, final_url)
        result   — {platform, slug} if found, else None
        html     — response text (None on error or non-200)
        final_url— URL after redirects
    """
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True
        )
        final_url = resp.url

        # Layer 1: redirect URL
        if final_url != url:
            logger.debug("[P3a] Redirect: %s → %s", url, final_url)
            r = match_ats_pattern(final_url)
            if r and _slug_ok(r, company):
                return _enrich_eightfold_domain(r, final_url), None, final_url

        if resp.status_code != 200:
            return None, None, None

        # Layer 2: deep HTML scan
        r = _scan_html(resp.text, company)
        if r:
            r = _enrich_eightfold_domain(r, final_url)
        return r, resp.text, final_url

    except requests.exceptions.SSLError:
        # Retry on HTTP
        try:
            http_url = url.replace("https://", "http://", 1)
            resp     = requests.get(
                http_url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True
            )
            if resp.url != http_url:
                r = match_ats_pattern(resp.url)
                if r and _slug_ok(r, company):
                    return r, None, resp.url
            if resp.status_code == 200:
                r = _scan_html(resp.text, company)
                return r, resp.text, resp.url
        except Exception:
            pass
        return None, None, None

    except requests.exceptions.Timeout:
        logger.debug("[P3a] Timeout: %s", url)
        return None, None, None
    except Exception as e:
        logger.debug("[P3a] Fetch error %s: %s", url, e)
        return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2: deep HTML scan
# ─────────────────────────────────────────────────────────────────────────────

def _scan_html(html, company):
    """
    Deep HTML scan using BeautifulSoup.

    Extracts candidate URLs from:
      • Every tag attribute that can hold a URL (src, href, action, data-*)
      • Inline <script> content — only scripts that mention an ATS domain
        (fast-path skip avoids parsing every analytics/tracking script)

    Runs each candidate through match_ats_pattern() → validate slug.
    """
    soup = BeautifulSoup(html, "html.parser")

    candidates = set()

    # ── Attribute URLs ─────────────────────────────────────────────────────
    URL_ATTRS = ("src", "href", "action", "data-src", "data-href",
                 "data-url", "data-apply-url", "data-job-url")
    for tag in soup.find_all(True):
        for attr in URL_ATTRS:
            val = tag.get(attr) or ""
            if isinstance(val, str) and val.startswith("http"):
                candidates.add(val.rstrip('.,;)"\'><'))

    # ── Inline script content ──────────────────────────────────────────────
    for script in soup.find_all("script"):
        content = script.string or ""
        if not content:
            continue
        # Skip scripts that don't mention any known ATS domain — fast path
        content_lower = content.lower()
        if not any(hint in content_lower for hint in _ATS_SCRIPT_HINTS):
            continue
        for raw_url in re.findall(r'https?://[^\s"\'\\<>]+', content):
            candidates.add(raw_url.rstrip('.,;)"\'><'))

    # ── Pattern match ──────────────────────────────────────────────────────
    for url in candidates:
        r = match_ats_pattern(url)
        if r and _slug_ok(r, company):
            return r

    # ── Eightfold footer fingerprint fallback ─────────────────────────────
    # Every Eightfold career page embeds a "Powered by eightfold.ai" footer
    # containing href="https://eightfold.ai" and an img from static.vscdn.net.
    # These signals confirm Eightfold is in use but don't carry the slug.
    # The {slug}.eightfold.ai URL is always present elsewhere in the page
    # (typically inside a JS config object) but may not surface as a clean
    # attribute URL — so we do a targeted raw-HTML regex search as a fallback.
    # Eightfold footer: every Eightfold-powered career page has a
    # "Powered by eightfold.ai" footer with href="https://eightfold.ai".
    # vscdn.net is their CDN but can appear on non-Eightfold pages —
    # only the eightfold.ai href is a reliable confirmation signal.
    if "eightfold.ai" in html.lower():
        m = re.search(
            r'https?://([a-z0-9][a-z0-9\-]*)\.eightfold\.ai/',
            html,
            re.IGNORECASE,
        )
        if m:
            r = match_ats_pattern(m.group(0))
            if r and _slug_ok(r, company):
                return r

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3: job link following
# ─────────────────────────────────────────────────────────────────────────────

def _follow_job_links(html, base_url, company, domain):
    """
    Extract individual job listing links from the career page and scan each.

    Why this works:
      • stripe.com/jobs  → lists jobs, no ATS embed
      • stripe.com/jobs/listing/{title}/{id}/apply
          → <iframe src="https://job-boards.greenhouse.io/embed/job_app?for=stripe">
      • jobs.netflix.com → lists jobs
      • jobs.netflix.com/jobs/{id}  → Apply button links to
          netflix.wd1.myworkdayjobs.com/Netflix_External_Site/...

    Individual job pages almost always contain a direct ATS signal.
    We try up to _MAX_JOB_LINKS to avoid excessive HTTP overhead.
    """
    job_links = _extract_job_links(html, base_url, domain)
    logger.debug("[P3a] Job links found on %s: %d", base_url, len(job_links))

    for link_url in job_links[:_MAX_JOB_LINKS]:
        logger.debug("[P3a] Following job link: %s", link_url)
        result, _, _ = _fetch_and_scan(link_url, company)
        if result:
            return result

    return None


def _extract_job_links(html, base_url, domain):
    """
    Extract internal links that look like individual job listings.

    Heuristic: path must contain a job-related segment AND not be one of
    the top-level career paths we already tried.  A numeric or slug-like
    final segment (len ≥ 4) confirms it's a detail page, not a root listing.

    Returns deduplicated list, most-specific paths first.
    """
    soup = BeautifulSoup(html, "html.parser")
    seen   = set()
    links  = []

    # Job-like path pattern:
    #   /jobs/listing/title/12345
    #   /careers/detail/12345
    #   /job/senior-engineer-12345
    #   /en/careers/openings/12345
    _JOB_PATH_RE = re.compile(
        r'/(?:jobs?|careers?|listing|opening|position|role|opportunit\w*|apply)'
        r'(?:/[a-zA-Z0-9_%-]+){1,}',
        re.IGNORECASE,
    )

    # Exclude bare top-level career paths already tried
    _TOP_LEVEL = frozenset({
        "", "/", "/careers", "/careers/", "/jobs", "/jobs/",
        "/about/careers", "/company/careers",
    })

    for a in soup.find_all("a", href=True):
        href = str(a.get("href", "")).strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        full_url = urljoin(base_url, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        parsed = urlparse(full_url)

        # Must stay on the company domain
        if domain not in parsed.netloc:
            continue

        path = parsed.path
        if path in _TOP_LEVEL:
            continue

        if not _JOB_PATH_RE.search(path):
            continue

        # Prefer paths that end with an ID-like segment (detail pages)
        last_seg = path.rstrip("/").split("/")[-1]
        if len(last_seg) >= 4:
            links.append(full_url)

    return links


# ─────────────────────────────────────────────────────────────────────────────
# Slug validation helper
# ─────────────────────────────────────────────────────────────────────────────

def _slug_ok(result, company):
    """
    Validate that the detected slug belongs to the expected company.
    Skips validation for platforms with opaque or rich slugs.

    Eightfold slugs are JSON-encoded dicts — we parse and validate the inner
    "slug" field rather than the raw JSON string.  This prevents false positives
    from pages that embed Eightfold RMS/LinkedIn tracking scripts without being
    real Eightfold customers (e.g. a Workday company whose career page loads
    an Eightfold real-time-listing widget).
    """
    import json as _json

    platform = result.get("platform", "")
    if platform in _OPAQUE_SLUG_PLATFORMS:
        return True
    if platform in _RICH_SLUG_PLATFORMS:
        return True

    slug = result.get("slug", "")

    # Eightfold stores slug as JSON — extract the inner slug for comparison
    if platform == "eightfold":
        try:
            slug = _json.loads(slug).get("slug", "")
        except (ValueError, TypeError):
            pass

    return validate_slug_for_company(slug, company)
