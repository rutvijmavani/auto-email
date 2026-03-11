"""
enrich_ats_companies.py — Enrich ats_discovery.db with company names.

Calls each ATS API to fill company_name, website, job_count
for all unenriched slugs in ats_discovery.db.

Run after build_ats_slug_list.py:
  python enrich_ats_companies.py
  python enrich_ats_companies.py --platform greenhouse
  python enrich_ats_companies.py --limit 100
  python enrich_ats_companies.py --test   (10 per platform)

Rate: ~100ms delay between calls → ~5,000 slugs in ~8 minutes.
Monthly incremental: only new unenriched slugs.
"""

import re
import time
import json
import argparse
import requests
from datetime import datetime

from db.schema_discovery import init_discovery_db
from db.ats_companies import (
    get_unenriched, upsert_company, delete_company
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}
TIMEOUT     = 8
# Delays now per-platform via config.py
# Loaded dynamically in _get_platform_delay()


# ─────────────────────────────────────────
# ENRICHMENT FUNCTIONS PER PLATFORM
# ─────────────────────────────────────────

def enrich_greenhouse(slug):
    """
    GET boards-api.greenhouse.io/v1/boards/{slug}
    Returns {"name": "Stripe", "jobs": [...]}
    """
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 404:
            return None, "inactive"
        if resp.status_code != 200:
            return None, "error"
        data = resp.json()
        name     = data.get("name", "")
        jobs     = data.get("jobs", [])
        # Try to get website from jobs
        website  = _extract_website_from_jobs(jobs)
        return {
            "company_name": name,
            "website":      website,
            "job_count":    len(jobs),
        }, "ok"
    except Exception:
        return None, "error"


def enrich_lever(slug):
    """
    GET api.lever.co/v0/postings/{slug}?mode=json
    Returns list of job postings.
    """
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 404:
            return None, "inactive"
        if resp.status_code != 200:
            return None, "error"
        data = resp.json()
        if not isinstance(data, list):
            return None, "error"
        # Company name from first job's hostedUrl or categories
        name    = _extract_lever_company_name(slug)
        website = _extract_lever_website(data)
        return {
            "company_name": name,
            "website":      website,
            "job_count":    len(data),
        }, "ok"
    except Exception:
        return None, "error"


def enrich_ashby(slug):
    """
    GET api.ashbyhq.com/posting-api/job-board/{slug}
    Public API returns {"jobs": [...]} only — no jobBoard.name.
    Extract company name from:
      1. organizationName field on job postings (most reliable)
      2. HTML page title of the job board page
      3. Fall back to slug title-cased
    """
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 404:
            return None, "inactive"
        if resp.status_code != 200:
            return None, "error"
        data = resp.json()
        jobs = data.get("jobs", [])

        name    = ""
        website = ""

        # 1. organizationName on job postings — most reliable
        if jobs:
            for job in jobs[:5]:  # check first 5 jobs
                org_name = (
                    job.get("organizationName", "") or
                    job.get("companyName", "") or
                    job.get("organization", {}).get("name", "")
                    if isinstance(job.get("organization"), dict)
                    else ""
                )
                if org_name:
                    name = org_name
                    break

        # 2. Try job board HTML page title
        if not name:
            try:
                page = requests.get(
                    f"https://jobs.ashbyhq.com/{slug}",
                    headers=HEADERS,
                    timeout=TIMEOUT,
                )
                if page.status_code == 200:
                    from bs4 import BeautifulSoup
                    soup  = BeautifulSoup(page.text, "html.parser")
                    title = soup.find("title")
                    if title and title.text:
                        # Title format: "Company Name - Jobs"
                        # or "Jobs at Company Name"
                        raw = title.text.strip()
                        for suffix in [
                            " - Jobs", " Jobs", " Careers",
                            " | Jobs", " | Careers"
                        ]:
                            if raw.endswith(suffix):
                                name = raw[:-len(suffix)].strip()
                                break
                        if not name:
                            for prefix in ["Jobs at ", "Careers at "]:
                                if raw.startswith(prefix):
                                    name = raw[len(prefix):].strip()
                                    break
                    # Try og:site_name meta tag
                    if not name:
                        og = soup.find(
                            "meta", property="og:site_name"
                        )
                        if og and og.get("content"):
                            name = og["content"].strip()
            except Exception:
                pass

        # 3. Fall back to slug title-cased
        if not name:
            name = slug.replace("-", " ").title()

        return {
            "company_name": name,
            "website":      website,
            "job_count":    len(jobs),
        }, "ok"
    except Exception:
        return None, "error"


def enrich_smartrecruiters(slug):
    """
    GET api.smartrecruiters.com/v1/companies/{slug}
    Returns {"name": "Adobe", "website": "..."}
    """
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 404:
            return None, "inactive"
        if resp.status_code != 200:
            return None, "error"
        data = resp.json()
        name    = data.get("name", "")
        website = data.get("website", "")
        return {
            "company_name": name,
            "website":      website,
            "job_count":    0,  # would need separate call
        }, "ok"
    except Exception:
        return None, "error"


def enrich_workday(slug):
    """
    Workday has no simple name API.
    slug is JSON: {"slug":"capitalone","wd":"wd12","path":"Capital_One"}
    Try fetching the careers page title.
    """
    try:
        if isinstance(slug, str) and slug.startswith("{"):
            slug_data = json.loads(slug)
        else:
            return None, "skip"  # plain string — not valid workday slug

        tenant  = slug_data.get("slug", "")
        wd      = slug_data.get("wd", "")
        path    = slug_data.get("path", "careers")

        if not tenant or not wd:
            return None, "skip"

        # Try fetching careers page for title
        url = f"https://{tenant}.{wd}.myworkdayjobs.com/{path}"
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT,
                           allow_redirects=True)

        if resp.status_code == 404:
            return None, "inactive"
        if resp.status_code != 200:
            return None, "error"

        # Extract company name from page title
        # Workday titles: "Jobs at Capital One | Workday"
        name = _extract_title_name(resp.text)
        return {
            "company_name": name,
            "website":      "",
            "job_count":    0,
        }, "ok"

    except Exception:
        return None, "error"


def enrich_oracle_hcm(slug):
    """
    Oracle HCM slug: {"slug":"jpmc","site":"CX_1001"}
    Try fetching careers page for company name.
    """
    try:
        if isinstance(slug, str) and slug.startswith("{"):
            slug_data = json.loads(slug)
        else:
            return None, "skip"

        tenant  = slug_data.get("slug", "")
        site_id = slug_data.get("site", "")

        if not tenant or not site_id:
            return None, "skip"

        url = (f"https://{tenant}.fa.oraclecloud.com/hcmUI/"
               f"CandidateExperience/en/sites/{site_id}/jobs")
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT,
                           allow_redirects=True)

        if resp.status_code == 404:
            return None, "inactive"
        if resp.status_code != 200:
            return None, "error"

        name = _extract_title_name(resp.text)
        return {
            "company_name": name,
            "website":      "",
            "job_count":    0,
        }, "ok"

    except Exception:
        return None, "error"


def enrich_icims(slug):
    """
    iCIMS slug: e.g. "schwab"
    Try fetching careers-{slug}.icims.com listing page.
    """
    url = (f"https://careers-{slug}.icims.com"
           f"/jobs/search?in_iframe=1")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 404:
            return None, "inactive"
        if resp.status_code != 200:
            return None, "error"

        if "iCIMS" not in resp.text and "icims" not in resp.text.lower():
            return None, "inactive"

        # Extract company name from page title
        name = _extract_title_name(resp.text)
        # Count job anchors
        job_count = resp.text.count("iCIMS_Anchor")

        return {
            "company_name": name,
            "website":      "",
            "job_count":    job_count,
        }, "ok"

    except Exception:
        return None, "error"


# Registry
ENRICHERS = {
    "greenhouse":      enrich_greenhouse,
    "lever":           enrich_lever,
    "ashby":           enrich_ashby,
    "smartrecruiters": enrich_smartrecruiters,
    "workday":         enrich_workday,
    "oracle_hcm":      enrich_oracle_hcm,
    "icims":           enrich_icims,
}


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _extract_website_from_jobs(jobs):
    """Try to extract company website from job URLs."""
    for job in jobs[:3]:
        url = job.get("absolute_url", "")
        if url and "greenhouse" not in url.lower():
            m = re.match(r"https?://([^/]+)", url)
            if m:
                return m.group(1)
    return ""


def _extract_lever_company_name(slug):
    """
    Lever doesn't return company name directly.
    Infer from slug or job URLs.
    """
    # Try page URL to get proper company name
    # Slug is already the best we have
    return slug.replace("-", " ").title()


def _extract_lever_website(jobs):
    """Extract company website from Lever job URLs."""
    for job in jobs[:3]:
        url = job.get("hostedUrl", "")
        if url:
            m = re.search(r"jobs\.lever\.co/([^/?]+)", url)
            if not m:
                # Check for company website in job description
                desc = job.get("descriptionPlain", "")
                m2 = re.search(r"https?://(?:www\.)?([a-z0-9-]+\.[a-z]{2,})",
                               desc)
                if m2:
                    return m2.group(0)
    return ""


def _extract_title_name(html):
    """
    Extract company name from HTML page title.
    Handles patterns like:
      "Jobs at Capital One | Workday"
      "Careers | JPMorgan Chase"
      "Software Engineer in Dublin | Careers at Encyclis"
    """
    import re
    title_m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
    if not title_m:
        return ""

    title = title_m.group(1).strip()

    # Remove common suffixes
    patterns = [
        r"\s*[|–-]\s*Workday\s*$",
        r"\s*[|–-]\s*Jobs?\s*$",
        r"\s*[|–-]\s*Careers?\s*$",
        r"^Jobs?\s+at\s+",
        r"^Careers?\s+at\s+",
        r"^Careers?\s*[|–-]\s*",
        r"\s*[|–-]\s*Career.*$",
    ]
    for p in patterns:
        title = re.sub(p, "", title, flags=re.IGNORECASE).strip()

    return title[:100] if title else ""


# ─────────────────────────────────────────
# MAIN ENRICHMENT RUNNER
# ─────────────────────────────────────────

def _enrich_delay(platform):
    """
    Apply per-platform delay with jitter during enrichment.
    Spread 910 requests over 18-hour window naturally.
    """
    import random
    from config import PLATFORM_DELAYS
    cfg   = PLATFORM_DELAYS.get(
        platform, {"base": 0.3, "jitter": 0.1}
    )
    delay = cfg["base"] + random.uniform(
        -cfg["jitter"], cfg["jitter"]
    )
    time.sleep(max(0.05, delay))


def run_priority_enrichment():
    """
    Phase A: Enrich YOUR prospect companies first.
    Run once — gives immediate Phase 1 benefit for
    your 134 monitored companies.
    """
    from db.connection import get_conn
    from db.ats_companies import get_discovery_conn

    init_discovery_db()

    # Get all prospect company domains
    try:
        conn = get_conn()
        prospects = conn.execute("""
            SELECT company, domain
            FROM prospective_companies
        """).fetchall()
        conn.close()
        prospect_names = {
            r["company"].lower() for r in prospects
        }
        prospect_domains = {
            r["domain"].lower().replace("www.", "")
            for r in prospects if r["domain"]
        }
    except Exception:
        print("[WARNING] Could not load prospects — "
              "running standard enrichment")
        return run_enrichment(limit=200)

    if not prospect_names:
        print("[INFO] No prospects found — "
              "running standard enrichment")
        return run_enrichment(limit=200)

    # Find slugs matching prospect companies
    disc_conn = get_discovery_conn()
    try:
        rows = disc_conn.execute("""
            SELECT platform, slug, company_name, website
            FROM ats_companies
            WHERE is_enriched = 0
            AND is_active = 1
            ORDER BY platform ASC
        """).fetchall()
    finally:
        disc_conn.close()

    # Build normalized prospect slug set for exact matching
    import difflib
    def _normalize(s):
        import re
        return re.sub(r'[^a-z0-9]', '', s.lower())

    prospect_slugs_norm = {
        _normalize(p) for p in prospect_names
    }

    # Filter to prospect matches — strict matching only
    priority_slugs = []
    for row in rows:
        name    = (row["company_name"] or "").lower()
        website = (row["website"] or "").lower().replace("www.", "")
        slug    = row["slug"].lower()
        slug_norm = _normalize(slug)

        matched = False

        # 1. Exact normalized slug match
        if slug_norm in prospect_slugs_norm:
            matched = True

        # 2. Website domain match
        if not matched and website in prospect_domains:
            matched = True

        # 3. Word-boundary name match (avoid false positives)
        if not matched and name:
            for prospect in prospect_names:
                if len(prospect) < 5:
                    continue
                # All significant words must appear as whole words
                words = [
                    w for w in prospect.split()
                    if len(w) > 3
                ]
                if words and all(
                    re.search(
                        rf'(?<![a-z0-9]){re.escape(w)}(?![a-z0-9])',
                        name
                    )
                    for w in words
                ):
                    matched = True
                    break

        # 4. Fuzzy fallback — only high confidence (>= 0.85)
        if not matched and name:
            for prospect in prospect_names:
                if len(prospect) < 5:
                    continue
                ratio = difflib.SequenceMatcher(
                    None,
                    _normalize(name),
                    _normalize(prospect)
                ).ratio()
                if ratio >= 0.85:
                    matched = True
                    break

        if matched:
            priority_slugs.append(dict(row))

    if not priority_slugs:
        print("[INFO] No prospect matches in discovery DB yet. "
              "Run build_ats_slug_list.py first.")
        return

    print(f"\n[Phase A] Priority enrichment: "
          f"{len(priority_slugs)} prospect-matching slugs")
    print(f"{'Platform':<15} {'Slug':<30} {'Result':<10} {'Name'}")
    print("─" * 80)

    stats = {"ok": 0, "inactive": 0, "error": 0}
    for row in priority_slugs:
        plat = row["platform"]
        slug = row["slug"]
        enricher = ENRICHERS.get(plat)
        if not enricher:
            continue
        data, status = enricher(slug)
        if status == "inactive":
            delete_company(plat, slug)
            stats["inactive"] += 1
            print(f"  {plat:<13} {slug[:28]:<30} DELETED (404)")
        elif status == "ok" and data:
            upsert_company(
                platform=plat, slug=slug,
                company_name=data.get("company_name"),
                website=data.get("website"),
                job_count=data.get("job_count"),
            )
            stats["ok"] += 1
            name = data.get("company_name", "")[:30]
            print(f"  {plat:<13} {slug[:28]:<30} OK         {name}")
        elif status == "error":
            stats["error"] += 1
        _enrich_delay(plat)

    print(f"\n[Phase A] Complete — "
          f"OK: {stats['ok']} | "
          f"Inactive: {stats['inactive']} | "
          f"Errors: {stats['error']}")


def run_platform_aware_enrichment(test_mode=False):
    """
    Phase B: Daily background enrichment with per-platform limits.
    Spread over 18-hour window — ~910 requests/day total.
    Start minimal, increase only after 30 days clean api_health.

    Daily limits (config.py ENRICH_DAILY_LIMITS):
      greenhouse:  300
      ashby:       300
      lever:       150
      icims:       100
      workday:      30
      oracle_hcm:   30
    """
    from config import ENRICH_DAILY_LIMITS

    init_discovery_db()

    print("\n[Phase B] Platform-aware daily enrichment")
    print(f"  Window: 18 hours | "
          f"Total: ~{sum(ENRICH_DAILY_LIMITS.values())}/day")
    print()

    total_stats = {"ok": 0, "inactive": 0, "error": 0}

    for platform, daily_limit in ENRICH_DAILY_LIMITS.items():
        if test_mode:
            daily_limit = 3

        slugs = get_unenriched(
            platform=platform, limit=daily_limit
        )

        if not slugs:
            print(f"  {platform:<15} — no unenriched slugs")
            continue

        print(f"  {platform:<15} {len(slugs):>4} slugs to enrich")

        for row in slugs:
            slug     = row["slug"]
            enricher = ENRICHERS.get(platform)
            if not enricher:
                continue

            data, status = enricher(slug)

            if status == "inactive":
                delete_company(platform, slug)
                total_stats["inactive"] += 1
            elif status == "ok" and data:
                upsert_company(
                    platform=platform,
                    slug=slug,
                    company_name=data.get("company_name"),
                    website=data.get("website"),
                    job_count=data.get("job_count"),
                )
                total_stats["ok"] += 1
            elif status == "error":
                total_stats["error"] += 1

            _enrich_delay(platform)

    print(f"\n[Phase B] Complete — "
          f"OK: {total_stats['ok']} | "
          f"Inactive: {total_stats['inactive']} | "
          f"Errors: {total_stats['error']}")
    return total_stats


def run_enrichment(platform=None, limit=500, test_mode=False):
    """
    Enrich all unenriched slugs in ats_discovery.db.

    Args:
        platform:  only enrich this platform (None = all)
        limit:     max slugs per run
        test_mode: only process 10 per platform
    """
    init_discovery_db()

    if test_mode:
        limit = 10

    slugs = get_unenriched(platform=platform, limit=limit)

    if not slugs:
        print("[OK] No unenriched slugs found.")
        return

    print(f"\nEnriching {len(slugs)} slugs...")
    print(f"{'Platform':<15} {'Slug':<30} {'Result':<10} {'Name'}")
    print("─" * 80)

    stats = {
        "ok":       0,
        "inactive": 0,
        "error":    0,
        "skip":     0,
    }

    for row in slugs:
        plat = row["platform"]
        slug = row["slug"]

        enricher = ENRICHERS.get(plat)
        if not enricher:
            stats["skip"] += 1
            continue

        data, status = enricher(slug)

        if status == "inactive":
            delete_company(plat, slug)
            stats["inactive"] += 1
            print(f"  {plat:<13} {slug[:28]:<30} DELETED (404)")

        elif status == "ok" and data:
            upsert_company(
                platform=plat,
                slug=slug,
                company_name=data.get("company_name"),
                website=data.get("website"),
                job_count=data.get("job_count"),
            )
            stats["ok"] += 1
            name = data.get("company_name", "")[:30]
            print(f"  {plat:<13} {slug[:28]:<30} OK         {name}")

        elif status == "error":
            stats["error"] += 1
            # Don't mark as enriched — retry next time

        elif status == "skip":
            stats["skip"] += 1

        _enrich_delay(plat)

    # Summary
    print(f"\n{'='*60}")
    print("ENRICHMENT COMPLETE")
    print(f"  OK:       {stats['ok']:,}")
    print(f"  Inactive: {stats['inactive']:,}")
    print(f"  Errors:   {stats['error']:,}")
    print(f"  Skipped:  {stats['skip']:,}")
    print(f"  Total:    {len(slugs):,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich ats_discovery.db with company names"
    )
    parser.add_argument("--platform",
                        choices=list(ENRICHERS.keys()),
                        default=None)
    parser.add_argument("--limit",   type=int, default=500)
    parser.add_argument("--test",    action="store_true")
    parser.add_argument(
        "--priority",
        action="store_true",
        help="Phase A: enrich prospect companies first"
    )
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Phase B: platform-aware daily enrichment"
    )
    args = parser.parse_args()

    if args.priority:
        run_priority_enrichment()
    elif args.daily:
        run_platform_aware_enrichment(test_mode=args.test)
    else:
        run_enrichment(
            platform=args.platform,
            limit=args.limit,
            test_mode=args.test,
        )