"""
scripts/diagnose_accenture.py — Root-cause diagnostic for India jobs leaking.

Run from the project root:
    python scripts/diagnose_accenture.py

Checks in order:
  1. What is stored in DB for today's new Accenture jobs
  2. pending_detail zombie row counts (all companies)
  3. Accenture slug config (ats_slug, ats_platform)
  4. Workday registry config for Accenture (listing_filter, has_detail, etc.)
  5. Reproduce: manually call fetch_job_detail for a real Accenture URL
  6. _extract_city_from_url on those URLs
  7. is_us_location on the extracted cities
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

SEP = "─" * 60

# ── 1. DB: today's new Accenture jobs ────────────────────────────────────────
print(f"\n{'═'*60}")
print("STEP 1 — Today's new Accenture jobs in DB")
print(SEP)
try:
    from db.db import get_conn, init_db
    init_db()
    conn = get_conn()
    rows = conn.execute("""
        SELECT job_id, job_url, location, status, first_seen
        FROM job_postings
        WHERE company = %s AND status = %s
        ORDER BY first_seen DESC
        LIMIT 10
    """, ("accenture", "new")).fetchall()
    print(f"Found {len(rows)} recent new Accenture jobs:")
    for r in rows:
        print(f"  job_id   : {r['job_id']}")
        print(f"  location : {r['location']!r}")
        print(f"  status   : {r['status']}")
        print(f"  first_seen: {r['first_seen']}")
        print(f"  job_url  : {r['job_url']}")
        print()
    conn.close()
except Exception as e:
    print(f"  ERROR: {e}")

# ── 2. DB: pending_detail zombie rows ─────────────────────────────────────────
print(f"\n{'═'*60}")
print("STEP 2 — pending_detail rows in DB (zombie check)")
print(SEP)
try:
    conn = get_conn()
    rows = conn.execute("""
        SELECT company, COUNT(*) as cnt
        FROM job_postings
        WHERE status = %s
        GROUP BY company
        ORDER BY cnt DESC
        LIMIT 15
    """, ("pending_detail",)).fetchall()
    if rows:
        print("Companies with pending_detail rows (should be near 0):")
        for r in rows:
            print(f"  {r['company']:<30}  {r['cnt']} rows")
    else:
        print("  No pending_detail rows — clean.")
    conn.close()
except Exception as e:
    print(f"  ERROR: {e}")

# ── 3. Accenture slug config ──────────────────────────────────────────────────
print(f"\n{'═'*60}")
print("STEP 3 — Accenture slug config in DB")
print(SEP)
try:
    from db.db import get_all_monitored_companies
    companies = get_all_monitored_companies()
    matches = [c for c in companies if "accenture" in c.get("company", "").lower()]
    if matches:
        for c in matches:
            print(f"  company     : {c['company']}")
            print(f"  ats_platform: {c.get('ats_platform')}")
            print(f"  ats_slug    : {c.get('ats_slug')}")
            print(f"  domain      : {c.get('domain')}")
    else:
        print("  Accenture not found in monitored companies.")
except Exception as e:
    print(f"  ERROR: {e}")

# ── 4. Workday registry config ────────────────────────────────────────────────
print(f"\n{'═'*60}")
print("STEP 4 — Workday registry config (listing_filter, has_detail, etc.)")
print(SEP)
try:
    from jobs.ats.registry import get_config, parse_slug
    config = get_config("workday")
    print(f"  listing_filter : {config.get('listing_filter')}")
    print(f"  has_detail     : {config.get('has_detail')}")
    print(f"  country_source : {config.get('country_source')}")
    print(f"  slug_type      : {config.get('slug_type')}")
    print(f"  full config    : {config}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── 5. Reproduce: manually call fetch_job_detail ───────────────────────────────
print(f"\n{'═'*60}")
print("STEP 5 — Reproduce: fetch_job_detail for a real Accenture Hyderabad URL")
print(SEP)

TEST_URLS = [
    "https://accenture.wd103.myworkdayjobs.com/AccentureCareers/job/Hyderabad/Custom-Software-Engineering-Lead_ATCI-5502584-S2008926-1",
    "https://accenture.wd103.myworkdayjobs.com/AccentureCareers/job/Bengaluru/Custom-Software-Engineering-Lead_ATCI-5545917-S2023433-1",
    "https://accenture.wd103.myworkdayjobs.com/AccentureCareers/job/Indore/Custom-Software-Engineering-Lead_ATCI-5548666-S2021939-1",
]

try:
    from jobs.ats_detector import get_ats_module
    from jobs.ats.registry import parse_slug
    import json

    ats_module = get_ats_module("workday")
    config     = get_config("workday")

    # Get Accenture slug from DB
    conn = get_conn()
    row  = conn.execute("""
        SELECT ats_slug FROM prospective_companies
        WHERE company = %s
        LIMIT 1
    """, ("accenture",)).fetchone()
    conn.close()

    raw_slug  = row["ats_slug"] if row else None
    slug_info = parse_slug("workday", raw_slug, config) if raw_slug else {}
    print(f"  slug_info: {slug_info}")
    print()

    for url in TEST_URLS:
        city_in_url = url.split("/job/")[1].split("/")[0] if "/job/" in url else "?"
        print(f"  Testing URL city={city_in_url!r}")
        print(f"  URL: {url}")

        # Build a job dict that mirrors what scan_worker would produce
        job_id = url.rstrip("/").split("_", 1)[-1] if "_" in url else url.split("/")[-1]
        job = {
            "company":      "accenture",
            "job_url":      url,
            "job_id":       job_id,
            "title":        "Custom Software Engineering Lead",
            "location":     "",
            "ats_platform": "workday",
        }
        # Workday detail needs _external_path — try to derive it
        # _external_path is typically the last path segment of the job URL
        job["_external_path"] = url.split("/")[-1]

        print(f"  _external_path set to: {job['_external_path']!r}")

        try:
            result = ats_module.fetch_job_detail(job)
            if result is job:
                print("  ⚠ fetch_job_detail returned the ORIGINAL dict unchanged")
                print("    (detail fetch silently failed — None response)")
            else:
                print(f"  _country_code : {result.get('_country_code')!r}")
                print(f"  location      : {result.get('location')!r}")
                print(f"  title         : {result.get('title')!r}")
                # Show any other populated keys
                extra = {k: v for k, v in result.items()
                         if k.startswith("_") and v}
                if extra:
                    print(f"  extra _ keys  : {extra}")
        except Exception as exc:
            print(f"  ✗ fetch_job_detail RAISED: {exc}")
        print()

except Exception as e:
    print(f"  ERROR setting up test: {e}")
    import traceback; traceback.print_exc()

# ── 6. URL city extraction ────────────────────────────────────────────────────
print(f"\n{'═'*60}")
print("STEP 6 — _extract_city_from_url on those URLs")
print(SEP)
try:
    from workers.detail_worker import _extract_city_from_url
    for url in TEST_URLS:
        city = _extract_city_from_url(url)
        print(f"  URL   : ...{url[-60:]}")
        print(f"  city  : {city!r}")
        print()
except ImportError:
    # Inline the function if import fails (old code path)
    from urllib.parse import urlparse, parse_qs
    def _extract_city_from_url(job_url):
        if not job_url:
            return ""
        try:
            parsed = urlparse(job_url)
            parts  = [p for p in parsed.path.split("/") if p]
            for i, part in enumerate(parts):
                if part.lower() in ("job", "jobs") and i + 1 < len(parts):
                    candidate = parts[i + 1]
                    if (candidate
                            and not candidate.startswith(("R-", "JR", "req", "REQ"))
                            and not candidate[:1].isdigit()
                            and "_" not in candidate):
                        city = candidate.replace("-", " ").strip()
                        if city and len(city) <= 30:
                            return city
        except Exception:
            pass
        return ""

    print("  (using inline fallback — _extract_city_from_url not found in detail_worker)")
    for url in TEST_URLS:
        city = _extract_city_from_url(url)
        print(f"  URL   : ...{url[-60:]}")
        print(f"  city  : {city!r}")
        print()

# ── 7. is_us_location on extracted cities ─────────────────────────────────────
print(f"\n{'═'*60}")
print("STEP 7 — is_us_location on extracted cities (should all be False)")
print(SEP)
try:
    from jobs.job_filter import is_us_location
    cities = ["Hyderabad", "Bengaluru", "Indore", "Bangalore", "Pune", ""]
    for city in cities:
        result = is_us_location(city)
        flag   = "✓ FILTERED" if not result else "✗ PASSES (potential leak!)"
        print(f"  is_us_location({city!r:<15}) = {str(result):<5}  {flag}")
except Exception as e:
    print(f"  ERROR: {e}")

print(f"\n{'═'*60}")
print("Diagnostic complete.")
print("═" * 60)
