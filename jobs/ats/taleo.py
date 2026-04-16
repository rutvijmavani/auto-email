# jobs/ats/taleo.py — Taleo career portal scraper
#
# Adapted from confirmed-working TaleoScraper + TALEO FULL DETAIL SCRAPER.
# Headers are CRITICAL — Taleo returns 500 if tz/tzname/sec-ch-ua missing.
#
# API:
#   Listing: POST https://{company}.taleo.net/careersection/rest/jobboard/searchjobs
#                 ?lang=en&portal={portal_id}
#   Detail:  GET  jobdetail.ftl  → extract CSRF token
#            POST jobdetail.ajax → pipe-delimited HTML response
#
# Slug format stored in DB (JSON):
#   {"company": "massanf", "portal_id": "101430233", "section": "ex"}
#   portal_id auto-discovered if empty.

import re
import json
import math
import logging
import requests
from urllib.parse import unquote
from datetime import datetime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONSTANTS — DO NOT CHANGE HEADERS
# Taleo returns 500 if tz/tzname/sec-ch-ua are missing
# ─────────────────────────────────────────

HEADERS = {
    "Accept":             "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":    "en-US,en;q=0.9",
    "Connection":         "keep-alive",
    "Content-Type":       "application/json",
    "X-Requested-With":   "XMLHttpRequest",
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "same-origin",
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua":          '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "tz":                 "GMT-04:00",
    "tzname":             "America/New_York",
}

COOKIES  = {"locale": "en"}
SECTIONS = ["ex", "cs", "2", "1", "campus", "external"]

DATE_FORMATS = ["%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]


# ─────────────────────────────────────────
# SESSION
# ─────────────────────────────────────────

def _make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.set("locale", "en")
    return session


# ─────────────────────────────────────────
# FETCH JOBS
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    if not slug_info:
        return []
    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            logger.error("taleo: invalid slug_info JSON")
            return []

    taleo_company = slug_info.get("company", "")
    portal_id     = slug_info.get("portal_id", "")
    section       = slug_info.get("section", "ex")

    if not taleo_company:
        return []

    if not portal_id:
        portal_id, section = _discover_portal_id(taleo_company)
        if not portal_id:
            return []
        slug_info["portal_id"] = portal_id
        slug_info["section"]   = section

    base_url   = f"https://{taleo_company}.taleo.net"
    search_url = (
        f"{base_url}/careersection/rest/jobboard/searchjobs"
        f"?lang=en&portal={portal_id}"
    )

    session = _make_session()
    session.headers.update({
        **HEADERS,
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Content-Type":     "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          f"{base_url}/careersection/{section}/jobsearch.ftl?lang=en",
        "Origin":           base_url,
        "Sec-Fetch-Dest":   "empty",
        "Sec-Fetch-Mode":   "cors",
        "Sec-Fetch-Site":   "same-origin",
    })

    # ── Page 1: get total count + first batch ─────────────────────
    try:
        resp = session.post(
            search_url,
            json=_build_payload(page=1),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as e:
        logger.error("taleo: HTTP %s page 1 for %s",
                     e.response.status_code, taleo_company)
        return []
    except (requests.RequestException, ValueError) as e:
        logger.error("taleo: page 1 error for %s: %s", taleo_company, e)
        return []

    # ── Extract pagination metadata ───────────────────────────────
    paging      = data.get("pagingData", {})
    total       = int(paging.get("totalCount", 0))
    page_size   = int(paging.get("pageSize",   25))
    total_pages = math.ceil(total / page_size) if total and page_size else 1

    print(f"  [taleo] {taleo_company}: "
          f"{total} jobs across {total_pages} pages", flush=True)

    # ── Parse all pages ───────────────────────────────────────────
    all_jobs = []

    def parse_page(data, page_no):
        """Parse one page of requisitionList into job dicts."""
        col_map  = _detect_column_map(data.get("requisitionList", []))
        jobs     = []

        for job in data.get("requisitionList", []):
            col = job.get("column", [])

            def c(i, _col=col):
                return _col[i].strip() if i < len(_col) and _col[i].strip() else ""

            title      = c(0)
            contest_no = str(job.get("contestNo") or job.get("jobId") or "")
            job_id     = str(job.get("jobId") or contest_no)

            if not title or not job_id:
                continue

            loc_idx   = col_map.get("location", 4)
            date_idx  = col_map.get("date",     5)
            location  = _parse_location(c(loc_idx))
            posted_at = _parse_date(c(date_idx))

            detail_url = (
                f"{base_url}/careersection/{section}/jobdetail.ftl"
                f"?job={contest_no}&lang=en"
            )
            apply_url = (
                f"{base_url}/careersection/application.jss"
                f"?type=1&lang=en&portal={portal_id}&reqNo={contest_no}"
            )

            jobs.append({
                "company":       company,
                "title":         title,
                "job_url":       detail_url,
                "apply_url":     apply_url,
                "job_id":        job_id,
                "contest_no":    contest_no,
                "schedule":      c(1),
                "category":      c(2),
                "organization":  c(3),
                "location":      location,
                "posted_at":     posted_at,
                "description":   "",
                "salary_min":    "",
                "salary_max":    "",
                "salary_type":   "",
                "contact":       "",
                "contact_phone": "",
                "full_location": "",
                "ats":           "taleo",
                "_base_url":     base_url,
                "_section":      section,
                "_contest_no":   contest_no,
                "_portal_id":    portal_id,
            })
        return jobs

    # Add page 1 results
    all_jobs.extend(parse_page(data, 1))
    print(f"  [taleo] page 1/{total_pages} — {len(all_jobs)} jobs so far",
          flush=True)

    # Fetch remaining pages 2..total_pages
    for page in range(2, total_pages + 1):
        print(f"  [taleo] page {page}/{total_pages}...", flush=True)
        try:
            resp = session.post(
                search_url,
                json=_build_payload(page),
                timeout=15,
            )
            resp.raise_for_status()
            data     = resp.json()
            page_jobs = parse_page(data, page)

            if not page_jobs:
                print(f"  [taleo] page {page} empty — stopping early",
                      flush=True)
                break

            all_jobs.extend(page_jobs)

        except requests.HTTPError as e:
            logger.error("taleo: HTTP %s page %d for %s",
                         e.response.status_code, page, taleo_company)
            break
        except (requests.RequestException, ValueError) as e:
            logger.error("taleo: page %d error for %s: %s",
                         page, taleo_company, e)
            break

    logger.info("taleo: fetched %d/%d jobs for %s",
                len(all_jobs), total, taleo_company)
    print(f"  [taleo] done — {len(all_jobs)}/{total} jobs", flush=True)
    return all_jobs

# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job description + salary via Taleo's jobdetail.ajax endpoint.

    Flow (confirmed from DevTools):
      1. GET jobdetail.ftl  → CSRF token + session cookie
      2. POST jobdetail.ajax with exact payload → pipe-delimited response
      3. Parse !*!-delimited sections → description + salary + contact
    """
    base_url   = job.get("_base_url", "")
    section    = job.get("_section", "ex")
    contest_no = job.get("_contest_no", "") or job.get("contest_no", "")
    job_id     = job.get("job_id", "")

    if not base_url or not contest_no:
        return job

    try:
        session = _make_session()

        csrf = _get_csrf_token(session, base_url, section, contest_no)
        if not csrf:
            logger.warning("taleo: no CSRF for %s", contest_no)
            return job

        raw = _post_detail_ajax(session, base_url, section, job_id, contest_no, csrf)
        if not raw:
            logger.warning("taleo: empty ajax response for %s", contest_no)
            return job

        # ── FIX: _parse_ajax_response now returns dict ────────────
        parsed          = _parse_ajax_response(raw)
        job             = dict(job)
        job["description"]   = parsed.get("description",   "")
        job["salary_min"]    = parsed.get("salary_min",    "")
        job["salary_max"]    = parsed.get("salary_max",    "")
        job["salary_type"]   = parsed.get("salary_type",   "")
        job["contact"]       = parsed.get("contact",       "")
        job["contact_phone"] = parsed.get("contact_phone", "")
        job["full_location"] = parsed.get("full_location", "")
        return job

    except Exception as e:
        logger.warning("taleo: fetch_job_detail failed for %s: %s", contest_no, e)
        return job


# ─────────────────────────────────────────
# HELPERS — DETAIL AJAX
# ─────────────────────────────────────────

def _get_csrf_token(session, base_url, section, contest_no):
    """GET jobdetail.ftl to obtain CSRF token and session cookie."""
    url = f"{base_url}/careersection/{section}/jobdetail.ftl?job={contest_no}&lang=en"
    try:
        r = session.get(url, timeout=12)
        # ── FIX: corrected regex — was double-escaped, never matched
        m = re.search(
            r'csrftoken["\s:=]+(["\']?)([A-Za-z0-9+/=_\-]{20,})\1',
            r.text
        )
        if m:
            return m.group(2)
        # Fallback — hidden input field
        soup = BeautifulSoup(r.text, "html.parser")
        el   = soup.find("input", {"name": "csrftoken"})
        if el:
            return el.get("value", "")
    except requests.RequestException as e:
        logger.warning("taleo: CSRF fetch failed for %s: %s", contest_no, e)
    return ""


def _post_detail_ajax(session, base_url, section, job_id, contest_no, csrf):
    """POST to jobdetail.ajax with confirmed payload. Returns raw response."""
    ajax_url = f"{base_url}/careersection/{section}/jobdetail.ajax"
    headers  = {
        **HEADERS,
        "Accept":           "*/*",
        "Content-Type":     "application/x-www-form-urlencoded",
        "Origin":           base_url,
        "Referer":          f"{base_url}/careersection/{section}/jobdetail.ftl?job={contest_no}&lang=en",
        "Sec-Fetch-Dest":   "empty",
        "Sec-Fetch-Mode":   "cors",
        "Sec-Fetch-Site":   "same-origin",
        "X-Requested-With": "XMLHttpRequest",
    }
    payload = {
        "ftlpageid":                   "requisitionDescriptionPage",
        "ftlinterfaceid":              "requisitionDescriptionInterface",
        "ftlcompid":                   "validateTimeZoneId",
        "jsfCmdId":                    "validateTimeZoneId",
        "ftlcompclass":                "InitTimeZoneAction",
        "ftlcallback":                 "requisition_restoreDatesValues",
        "ftlajaxid":                   "ftlx1",
        "tz":                          "GMT-04:00",
        "tzname":                      "America/New_York",
        "lang":                        "en",
        "requisitionno":               job_id,
        "csrftoken":                   csrf,
        "portal":                      "",
        "isListEmpty":                 "false",
        "calloutPageDisplayed":        "true",
        "displayAsMainHeader":         "false",
        "descRequisition.hasElements": "true",
        "descRequisition.nbElements":  "1",
        "descRequisition.size":        "1",
        "descRequisition.isEmpty":     "false",
        "isApplicantUser":             "true",
        "signedIn":                    "false",
    }
    try:
        r = session.post(ajax_url, data=payload, headers=headers, timeout=15)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        logger.warning("taleo: ajax POST failed for %s: %s", contest_no, e)
        return ""


def _parse_ajax_response(raw):
    """
    Parse Taleo jobdetail.ajax pipe-delimited response.

    Confirmed format from DevTools:
      TOKEN!|!...metadata...!*!HTML1!*!HTML2!*!...pipe fields...
      - Split on !*! for major sections
      - Sections 1+ are URL-encoded HTML chunks (%5C: escaped)
      - Salary/contact in pipe-delimited fields across all sections

    ── FIX: now returns dict (was returning str, losing salary/contact) ──
    """
    result = {
        "description":   "",
        "salary_min":    "",
        "salary_max":    "",
        "salary_type":   "",
        "contact":       "",
        "contact_phone": "",
        "full_location": "",
    }

    if not raw:
        return result

    try:
        star_sections    = raw.split("!*!")
        description_html = ""

        # ── Description: sections 1+ contain HTML ────────────────
        for section in star_sections[1:]:
            chunk = section.strip()
            if chunk.endswith("!|!"):
                chunk = chunk[:-3]
            chunk = chunk.strip()
            if not chunk:
                continue
            decoded = unquote(chunk)
            decoded = decoded.replace("%5C:", ":").replace("\\:", ":")
            if "<p" in decoded or "<ul" in decoded or "<li" in decoded:
                description_html += decoded + "\n"

        if description_html:
            soup = BeautifulSoup(description_html, "html.parser")
            text = soup.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            result["description"] = text[:5000]

        # ── Salary + contact: scan all pipe-delimited fields ──────
        all_fields = []
        for section in star_sections:
            if "!|!" in section:
                parts = [p.strip() for p in section.split("!|!")]
                all_fields.extend(parts)

        for i, field in enumerate(all_fields):
            # Salary pattern: "140,000.00" or "22.00"
            if re.match(r'^[\d,]+\.\d{2}$', field):
                result["salary_min"]    = all_fields[i + 1] if i + 1 < len(all_fields) else ""
                result["salary_max"]    = all_fields[i + 2] if i + 2 < len(all_fields) else field
                result["salary_type"]   = all_fields[i + 3] if i + 3 < len(all_fields) else ""
                result["contact"]       = all_fields[i + 4] if i + 4 < len(all_fields) else ""
                result["contact_phone"] = all_fields[i + 5] if i + 5 < len(all_fields) else ""
                break
            # Full location: "United States-Massachusetts-Boston"
            if field.startswith("United States-") and not result["full_location"]:
                result["full_location"] = unquote(field)

    except Exception as e:
        logger.warning("taleo: ajax parse error: %s", e)

    return result


# ─────────────────────────────────────────
# HELPERS — LISTING API
# ─────────────────────────────────────────

def _build_payload(page=1):
    """Build Taleo search API payload."""
    return {
        "multilineEnabled": True,
        "sortingSelection": {
            "sortBySelectionParam":  "3",
            "ascendingSortingOrder": "false",
        },
        "fieldData": {
            "fields": {"JOB_TITLE": "", "KEYWORD": "", "JOB_NUMBER": ""},
            "valid":  True,
        },
        "filterSelectionParam": {
            "searchFilterSelections": [
                {"id": "POSTING_DATE", "selectedValues": []},
                {"id": "LOCATION",     "selectedValues": []},
                {"id": "JOB_FIELD",    "selectedValues": []},
                {"id": "JOB_SCHEDULE", "selectedValues": []},
                {"id": "JOB_LEVEL",    "selectedValues": []},
                {"id": "JOB_SHIFT",    "selectedValues": []},
                {"id": "ORGANIZATION", "selectedValues": []},
            ],
        },
        "advancedSearchFiltersSelectionParam": {
            "searchFilterSelections": [
                {"id": "ORGANIZATION", "selectedValues": []},
                {"id": "LOCATION",     "selectedValues": []},
                {"id": "JOB_FIELD",    "selectedValues": []},
                {"id": "URGENT_JOB",   "selectedValues": []},
                {"id": "WILL_TRAVEL",  "selectedValues": []},
                {"id": "JOB_SHIFT",    "selectedValues": []},
            ],
        },
        "pageNo": page,
    }


def _discover_portal_id(taleo_company):
    """
    Auto-discover portal_id by scraping the Taleo jobsearch page.
    Returns (portal_id, section) or (None, None).
    """
    base_url = f"https://{taleo_company}.taleo.net"
    headers  = {**HEADERS, "Accept": "text/html,application/xhtml+xml,*/*"}
    session  = _make_session()

    for section in SECTIONS:
        url = f"{base_url}/careersection/{section}/jobsearch.ftl?lang=en"
        try:
            resp = session.get(
                url, headers=headers, timeout=12, allow_redirects=True
            )
            if resp.status_code != 200:
                continue
            matches = re.findall(r'portal[=:]\s*["\']?(\d{6,})', resp.text)
            if matches:
                portal_id = max(set(matches), key=matches.count)
                return portal_id, section
        except requests.RequestException:
            continue

    return None, None


def _detect_column_map(requisition_list):
    """
    Detect location and date column indices by scanning first 5 jobs.
    Returns {"location": idx, "date": idx}.
    Defaults: location=4, date=5 (confirmed for massanf).
    """
    loc_votes  = {}
    date_votes = {}

    for job in requisition_list[:5]:
        for i, val in enumerate(job.get("column", [])):
            if not val or not val.strip():
                continue
            v = val.strip()
            if v.startswith("[") and '"' in v:
                loc_votes[i] = loc_votes.get(i, 0) + 1
            elif re.match(
                r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}$"
                r"|^\d{1,2}/\d{1,2}/\d{4}$",
                v
            ):
                date_votes[i] = date_votes.get(i, 0) + 1

    return {
        "location": max(loc_votes,  key=loc_votes.get)  if loc_votes  else 4,
        "date":     max(date_votes, key=date_votes.get) if date_votes else 5,
    }


def _parse_location(raw):
    """Parse Taleo location JSON array string."""
    if not raw:
        return ""
    try:
        locs = json.loads(raw)
        return ", ".join(locs) if locs else ""
    except (json.JSONDecodeError, TypeError):
        return re.sub(r'[\[\]"]', "", raw).strip()


def _parse_date(date_str):
    """Parse Taleo posted date string to datetime."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    logger.debug("taleo: unrecognized date format: %r", date_str)
    return None