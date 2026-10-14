# jobs/ats/jobvite.py — Jobvite job board scraper
#
# Jobvite does not expose a public JSON API for job listings.
# Jobs are fetched via HTML scraping of the career site.
#
# URL pattern:  jobs.jobvite.com/{slug}/jobs   (landing page — some tenants show only a subset
#               and no "Show More" links: Marelli 4 of 6, Knight 4 of 30, verified 2026-10-14)
# Pagination:   jobs.jobvite.com/{slug}/search?p={page}   — unfiltered, 50 rows/page, 0-based,
#               an EMPTY page is the proven end (verified on 9 tenants, never lost a job)
# Job selector: table a[href*="/job/"]
#
# Completeness: Jobvite has no total on the page. The company-account XML feed
#               (app.jobvite.com/CompanyJobs/Xml.aspx?c={companyEId}) is the independent
#               cross-check — see verify_fetch(). The feed spans EVERY careersite of the
#               account (WebMD's lists internetbrands + pulsepoint; Knight's lists Swift's),
#               so a feed job is only "missing" when its redirect lands on OUR careersite.
#
# Option C freshness strategy:
#   1. Fetch listing page → extract all job IDs
#   2. Compare with DB → find new job IDs only
#   3. Fetch detail page ONLY for new jobs
#   4. Extract description + location from detail page
#   5. posted_at = None (Jobvite does not expose posting dates publicly)
#
# Date field: NOT AVAILABLE — Jobvite hides posting dates from public pages
#             posted_at will always be None
#             Freshness relies on URL deduplication only (first_seen)

import re
import time
import requests
from bs4 import BeautifulSoup
from config import (
    JOBVITE_MAX_PAGES,
    JOBVITE_FEED_MAX_ATTEMPTS,
    JOBVITE_FEED_DEFAULT_WAIT_S,
    JOBVITE_FEED_MAX_WAIT_S,
    JOBVITE_FEED_TIMEOUT_S,
    JOBVITE_FEED_TRACE_MAX,
)
from jobs.ats.base import fetch_html, slugify, validate_company_match
from logger import get_logger

logger = get_logger(__name__)

BASE_URL      = "https://jobs.jobvite.com/{slug}/jobs"
SEARCH_URL    = "https://jobs.jobvite.com/{slug}/search"
FEED_URL      = "https://app.jobvite.com/CompanyJobs/Xml.aspx"
JOB_REDIRECT_URL = "https://app.jobvite.com/CompanyJobs/Job.aspx"
JOB_ID_RE     = re.compile(r"/job/([A-Za-z0-9]+)(?:/|\?|$)")
COMPANY_EID_RE = re.compile(r"companyEId:\s*'([^']+)'")
FEED_ID_RE    = re.compile(r"<job><id>([^<]+)</id>")
# careersite name in a job URL: /{name}/job/{id} or /careers/{name}/job/{id}
SITE_NAME_RE  = re.compile(r"jobs\.jobvite\.com/(?:careers/)?([^/?]+)/job/")
HEADERS       = {"User-Agent": "Mozilla/5.0"}

# stop reasons of the paging loop (shared with fetch_report) — only EMPTY_PAGE proves the end of the board
from jobs.ats.fetch_report import (  # noqa: E402
    FetchReport, STOP_EMPTY_PAGE, STOP_NO_CONFIG, STOP_PAGE_CAP, STOP_REPEAT_PAGE,
    STOP_REQUEST_FAILED, attach, count_distinct, failed,
)


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses Jobvite.
    Returns (slug, sample_jobs) or (None, None).

    Probes jobs.jobvite.com/{slug}/jobs for each slug variant.
    Validates by checking job anchor links are present.
    """
    for slug in slugify(company):
        url  = BASE_URL.format(slug=slug)
        resp = fetch_html(url, platform="jobvite", track=False)

        if resp is None:
            continue

        soup    = BeautifulSoup(resp.text, "html.parser")
        anchors = _extract_job_anchors(soup)

        if not anchors:
            continue

        # Validate company match using first job title
        first_title = anchors[0].get_text(strip=True)
        if not validate_company_match(first_title, company):
            # Title match may fail (job titles rarely contain company name)
            # Fall back to URL — slug must contain a company keyword
            if not validate_company_match(slug, company):
                continue

        sample = _anchors_to_stubs(anchors[:3], slug, company)
        return slug, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing)
# ─────────────────────────────────────────

def fetch_board(slug, company):
    """
    Fetch the whole Jobvite board and record HOW the paging loop ended.

    Strategy:
      1. /jobs landing page — kept as a union source (jobs it shows that
         /search somehow lacks are still returned, and warned about)
      2. Unfiltered /search?p=N, 50 rows per page, 0-based, until an EMPTY page.
         No category filter: tenants with a custom landing page (Marelli, Knight)
         have no category links, so a category loop silently under-fetches them.

    Returns (jobs, report). report["stop_reason"] is one of STOP_*; only
    STOP_EMPTY_PAGE proves the board was read to its end.
    """
    report = {"stop_reason": STOP_EMPTY_PAGE, "pages": 0, "landing_only": 0}
    if not slug:
        return [], report

    all_jobs = []
    seen_ids = set()

    def _add(anchors):
        new = 0
        for anchor in anchors:
            job = _anchor_to_job(anchor, slug, company)
            if job and job["job_id"] not in seen_ids:
                seen_ids.add(job["job_id"])
                all_jobs.append(job)
                new += 1
        return new

    # ── Step 1: landing page ──
    landing_jobs = []
    landing = fetch_html(BASE_URL.format(slug=slug), platform="jobvite")
    if landing is not None:
        landing_anchors = _extract_job_anchors(BeautifulSoup(landing.text, "html.parser"))
        landing_jobs = [j for j in (_anchor_to_job(a, slug, company) for a in landing_anchors) if j]

    # ── Step 2: unfiltered paging ──
    report["stop_reason"] = STOP_PAGE_CAP
    for page in range(JOBVITE_MAX_PAGES):
        resp = fetch_html(SEARCH_URL.format(slug=slug), params={"p": page}, platform="jobvite")
        if resp is None:
            report["stop_reason"] = STOP_REQUEST_FAILED
            break
        anchors = _extract_job_anchors(BeautifulSoup(resp.text, "html.parser"))
        if not anchors:
            report["stop_reason"] = STOP_EMPTY_PAGE
            break
        report["pages"] += 1
        if _add(anchors) == 0:
            # non-empty page with nothing new = the site ignored p (loop would never end)
            report["stop_reason"] = STOP_REPEAT_PAGE
            break

    # ── Step 3: union with landing page ──
    for job in landing_jobs:
        if job["job_id"] not in seen_ids:
            seen_ids.add(job["job_id"])
            all_jobs.append(job)
            report["landing_only"] += 1
    if report["landing_only"] and report["stop_reason"] == STOP_EMPTY_PAGE:
        logger.warning(
            "[jobvite] %s: %d landing-page job(s) absent from /search paging", slug, report["landing_only"]
        )

    return all_jobs, report


def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from Jobvite (list of normalized job dicts).
    posted_at is always None — Jobvite hides posting dates.
    location and description filled by fetch_job_detail().
    """
    if not slug:
        return failed(STOP_NO_CONFIG, "empty slug")
    jobs, report = fetch_board(slug, company)
    if report["stop_reason"] != STOP_EMPTY_PAGE:
        logger.warning(
            "[jobvite] %s: paging ended by %s after %d page(s) — %d job(s) may be incomplete",
            slug, report["stop_reason"], report["pages"], len(jobs),
        )
    # No total on the page: complete stays None on a clean end (verify_fetch's feed check is the
    # independent cross-check); an abnormal stop is complete=False.
    return attach(jobs, FetchReport(
        stop_reason      = report["stop_reason"],
        pages            = report["pages"],
        fetched_distinct = count_distinct(jobs),
        repeated_pages   = 1 if report["stop_reason"] == STOP_REPEAT_PAGE else 0,
        note             = f"{report['landing_only']} landing-only" if report["landing_only"] else "",
    ))


# ─────────────────────────────────────────
# COMPLETENESS — XML FEED CROSS-CHECK
# ─────────────────────────────────────────

def _get_with_retry(url, params=None):
    """
    GET that obeys the server's own 429 pacing: wait Retry-After seconds
    (capped by JOBVITE_FEED_MAX_WAIT_S), else JOBVITE_FEED_DEFAULT_WAIT_S.
    Returns the response (any non-429 status) or None when attempts are exhausted
    or the request errors.
    """
    for attempt in range(JOBVITE_FEED_MAX_ATTEMPTS):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=JOBVITE_FEED_TIMEOUT_S)
        except requests.RequestException as e:
            logger.warning("[jobvite] request error %s: %s", url, e)
            return None
        if resp.status_code != 429:
            return resp
        if attempt == JOBVITE_FEED_MAX_ATTEMPTS - 1:
            break
        try:
            wait = int(resp.headers.get("Retry-After", ""))
        except ValueError:
            wait = JOBVITE_FEED_DEFAULT_WAIT_S
        time.sleep(min(max(wait, 0), JOBVITE_FEED_MAX_WAIT_S))
    return None


def _result(status, reason, **extra):
    out = {"status": status, "reason": reason}
    out.update(extra)
    return out


def verify_fetch(slug, jobs, stop_reason=STOP_EMPTY_PAGE):
    """
    Cross-check a finished board fetch against the account XML feed.

    status:
      verified    — paging ended on an empty page AND every feed job that
                    resolves to OUR careersite is in `jobs`
      unverified  — paging fine but the feed could not be used (no companyEId,
                    feed unavailable, or some feed-only jobs could not be traced)
      incomplete  — paging ended abnormally, or a feed job resolving to our
                    careersite is missing from `jobs`

    The feed spans the whole Jobvite account (sibling careersites included);
    feed-only jobs are resolved through app.jobvite.com/CompanyJobs/Job.aspx,
    whose redirect names the careersite they belong to. Jobs on another
    careersite are not ours and are ignored.
    """
    if stop_reason != STOP_EMPTY_PAGE:
        return _result("incomplete", f"paging ended by {stop_reason}")

    landing = fetch_html(BASE_URL.format(slug=slug), platform="jobvite", track=False)
    m = COMPANY_EID_RE.search(landing.text) if landing is not None else None
    if not m:
        return _result("unverified", "no companyEId on landing page")
    eid = m.group(1)

    feed = _get_with_retry(FEED_URL, params={"c": eid})
    if feed is None or feed.status_code != 200:
        return _result("unverified", "feed unavailable")
    feed_ids = {i.split("-")[0] for i in FEED_ID_RE.findall(feed.text)}
    ours_ids = {j["job_id"] for j in jobs}
    feed_only = sorted(feed_ids - ours_ids)

    ours_name = slug.split("/")[-1].lower()
    missing, foreign, unresolved = [], 0, 0
    for n, jid in enumerate(feed_only):
        if n >= JOBVITE_FEED_TRACE_MAX:
            unresolved += len(feed_only) - n
            break
        resp = _get_with_retry(JOB_REDIRECT_URL, params={"c": eid, "j": jid})
        nm = SITE_NAME_RE.search(resp.url) if resp is not None else None
        if not nm:
            unresolved += 1
        elif nm.group(1).lower() == ours_name:
            missing.append(jid)
        else:
            foreign += 1

    info = dict(feed_total=len(feed_ids), feed_only=len(feed_only), foreign=foreign,
                unresolved=unresolved, missing=missing)
    if missing:
        return _result("incomplete", f"{len(missing)} feed job(s) on our careersite not fetched", **info)
    if unresolved:
        return _result("unverified", f"{unresolved} feed-only job(s) could not be traced", **info)
    return _result("verified", "paging ended on empty page; feed cross-check clean", **info)


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job detail page for a single job.
    Extracts location and description.
    posted_at remains None — not available on Jobvite detail pages.

    Called only for NEW jobs (Option C strategy).

    Args:
        job: job dict from fetch_jobs()

    Returns:
        Updated job dict with location and description filled.
    """
    job_url = job.get("job_url", "")
    if not job_url:
        return job

    resp = fetch_html(job_url, platform="jobvite")
    if resp is None:
        return job

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        location    = _extract_location(soup)
        description = _extract_description(soup)

        job             = dict(job)
        # Only overwrite location if current value is empty
        if not job.get("location"):
            job["location"] = location or ""
        job["description"] = description or ""
        # posted_at stays None — Jobvite does not expose it

        return job

    except Exception:
        return job


# ─────────────────────────────────────────
# HELPERS — HTML PARSING
# ─────────────────────────────────────────

def _extract_job_anchors(soup):
    """
    Extract all job link anchors from a Jobvite listing page.
    Jobvite renders jobs as <a href="/slug/job/{id}"> inside tables.
    Excludes nav/utility links (Apply, Back, etc).
    """
    anchors = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not JOB_ID_RE.search(href):
            continue
        # Skip apply links (/job/{id}/apply) and other utility paths
        if "/apply" in href or "/referral" in href:
            continue
        text = a.get_text(strip=True)
        if not text or len(text) < 3:
            continue
        anchors.append(a)
    return anchors


def _extract_job_id(href):
    """Extract alphanumeric job ID from Jobvite URL."""
    m = JOB_ID_RE.search(href)
    return m.group(1) if m else None


def _clean_job_url(href, slug):
    """Build clean job URL without query params."""
    if href.startswith("http"):
        # Strip query string
        return href.split("?")[0].rstrip("/")
    if href.startswith("/"):
        return f"https://jobs.jobvite.com{href.split('?')[0].rstrip('/')}"
    return f"https://jobs.jobvite.com/{href.split('?')[0].rstrip('/')}"


def _anchor_to_job(anchor, slug, company):
    """Convert a single job anchor tag to a job stub dict."""
    href   = anchor.get("href", "")
    job_id = _extract_job_id(href)
    if not job_id:
        return None

    title = _clean_title(anchor.get_text(strip=True))
    if not title:
        return None

    # Try to extract location from sibling/parent td
    location = ""
    parent_tr = anchor.find_parent("tr")
    if parent_tr:
        tds = parent_tr.find_all("td")
        if len(tds) >= 2:
            location = tds[-1].get_text(strip=True)

    return {
        "company":     company,
        "title":       title,
        "job_url":     _clean_job_url(href, slug),
        "job_id":      job_id,
        "location":    location,
        "posted_at":   None,      # not available on Jobvite
        "description": "",        # filled by fetch_job_detail
        "ats":         "jobvite",
        "_slug":       slug,      # used for detail fetch
    }


def _anchors_to_stubs(anchors, slug, company):
    """Convert list of anchors to job stubs (used by detect())."""
    jobs = []
    for anchor in anchors:
        job = _anchor_to_job(anchor, slug, company)
        if job:
            jobs.append(job)
    return jobs


def _clean_title(raw_title):
    """Strip whitespace and normalize job title."""
    title = re.sub(r"\s+", " ", raw_title).strip()
    return title


def _extract_location(soup):
    """
    Extract location from Jobvite detail page.

    Jobvite detail page structure:
      <p class="jv-job-detail-meta">
        Accounting & Finance
        <span class='jv-inline-separator'></span>
        Bangalore, India
        <span class='jv-inline-separator'></span>Req.Num.: 30935
      </p>

    The inline separators divide: [Category] [City, State/Country] [Req.Num.]
    Location is always the segment between the 1st and 2nd separator.
    """
    # Strategy 1: parse .jv-job-detail-meta directly — split on separator spans.
    # Confirmed from DevTools: location is always between 1st and 2nd separator.
    meta = soup.select_one(".jv-job-detail-meta, .jv-job-detail-header")
    if meta:
        # Split the paragraph's content on the inline separator spans.
        # Each segment between spans is one field: [Category, Location, Req.Num.]
        segments = []
        current  = []
        for node in meta.children:
            if hasattr(node, "name") and node.name == "span" \
                    and "jv-inline-separator" in (node.get("class") or []):
                segments.append(" ".join(current).strip())
                current = []
            else:
                text = node.get_text(strip=True) if hasattr(node, "name") else node.strip()
                if text:
                    current.append(text)
        if current:
            segments.append(" ".join(current).strip())

        # segments = ["Accounting & Finance", "Bangalore,\nIndia", "Req.Num.: 30935"]
        # Location is index 1 (between first and second separator)
        if len(segments) >= 2:
            loc = re.sub(r"\s+", " ", segments[1]).strip()
            loc = re.sub(r"Req\.Num\..*$", "", loc, flags=re.IGNORECASE).strip()
            if loc:
                return loc

    # Strategy 2: fallback — scrape text from siblings after the h2 heading.
    # Catches non-standard Jobvite templates that don't use .jv-job-detail-meta.
    heading = soup.find("h2")
    if not heading:
        return ""

    raw_parts = []
    for el in heading.find_next_siblings():
        text = el.get_text(separator=" ", strip=True)
        if not text:
            continue
        if "Apply" in text or "← Back" in text:
            break
        raw_parts.append(text)
        if len(raw_parts) == 3:
            break

    full_text = " ".join(raw_parts)
    full_text = re.sub(r"\s+", " ", full_text).strip()
    full_text = re.sub(r"Req\.Num\..*$", "", full_text, flags=re.IGNORECASE).strip()

    # As last resort, strip the leading category word (everything before first comma)
    # only when the result is clearly category+location (e.g. "Engineering San Jose, CA")
    if "," in full_text:
        before_comma, after_comma = full_text.rsplit(",", 1)
        return f"{before_comma.strip()}, {after_comma.strip()}"

    if len(full_text) < 60:
        return full_text

    return ""

def _extract_description(soup):
    """Extract job description from Jobvite detail page."""
    # Jobvite detail page uses .jv-job-detail-description or similar
    for sel in [
        ".jv-job-detail-description",
        "#job-description",
        "[class*='description']",
        "[class*='job-detail']",
        ".content",
        "article",
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator="\n", strip=True)
            if len(text) > 100:
                return text[:5000]

    # Fallback: largest text block on page
    main = soup.select_one("main, #main, .main")
    if main:
        return main.get_text(separator="\n", strip=True)[:5000]

    return ""