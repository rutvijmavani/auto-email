"""
scripts/diagnose_accenture2.py — Targeted: do ATCI Indian jobs have _external_path?

Run:
    python scripts/diagnose_accenture2.py 2>&1 | tee diagnose_output3.txt
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db.db import init_db, get_conn
from jobs.ats_detector import get_ats_module
from jobs.ats.registry import get_config, parse_slug
from jobs.job_filter import is_us_location

init_db()

ats_module = get_ats_module("workday")
config     = get_config("workday")

if ats_module is None:
    print("ERROR: get_ats_module('workday') returned None — check ATS registry.")
    sys.exit(1)
if config is None:
    print("ERROR: get_config('workday') returned None — check ATS registry.")
    sys.exit(1)

conn = get_conn()
try:
    row = conn.execute("SELECT ats_slug FROM prospective_companies WHERE company = %s LIMIT 1",
                       ("accenture",)).fetchone()
finally:
    conn.close()

if row is None:
    print("ERROR: 'accenture' not found in prospective_companies — check company name.")
    sys.exit(1)

slug_info = parse_slug("workday", row["ats_slug"], config)

print("Fetching Accenture listing (2000 jobs)...")
raw_jobs = ats_module.fetch_jobs(slug_info, "accenture")
print(f"Total: {len(raw_jobs)} jobs\n")

# Split by ID type
atci_jobs = [j for j in raw_jobs if str(j.get("job_id","")).startswith("ATCI")]
r00_jobs  = [j for j in raw_jobs if str(j.get("job_id","")).startswith("R00")]
other     = [j for j in raw_jobs if j not in atci_jobs and j not in r00_jobs]

print("Job ID breakdown:")
print(f"  ATCI-* jobs : {len(atci_jobs)}")
print(f"  R00*  jobs  : {len(r00_jobs)}")
print(f"  Other       : {len(other)}")

# ── ATCI job _external_path analysis ─────────────────────────────────────────
print(f"\n{'─'*60}")
print("ATCI jobs — _external_path presence:")

atci_with_path    = [j for j in atci_jobs if j.get("_external_path")]
atci_without_path = [j for j in atci_jobs if not j.get("_external_path")]

print(f"  have  _external_path : {len(atci_with_path)}")
print(f"  MISSING _external_path: {len(atci_without_path)}")

print("\nFirst 3 ATCI jobs WITH _external_path:")
for j in atci_with_path[:3]:
    print(f"  job_id        : {j.get('job_id')}")
    print(f"  _external_path: {j.get('_external_path')!r}")
    print(f"  location      : {j.get('location')!r}")
    print(f"  job_url       : {j.get('job_url')}")
    print()

print("First 3 ATCI jobs WITHOUT _external_path:")
for j in atci_without_path[:3]:
    print(f"  job_id : {j.get('job_id')}")
    print(f"  job_url: {j.get('job_url')}")
    print()

# ── Try fetch_job_detail for one ATCI job with real _external_path ───────────
print(f"\n{'─'*60}")
print("fetch_job_detail on first ATCI job that HAS _external_path:")
if atci_with_path:
    test_job = dict(atci_with_path[0])
    print(f"  job_id        : {test_job.get('job_id')}")
    print(f"  _external_path: {test_job.get('_external_path')!r}")
    print(f"  job_url       : {test_job.get('job_url')}")
    try:
        result = ats_module.fetch_job_detail(test_job)
        if result is test_job:
            print("  ⚠  Returned ORIGINAL dict (silent fail — None from API)")
        else:
            print(f"  _country_code : {result.get('_country_code')!r}")
            print(f"  location      : {result.get('location')!r}")
            is_us = is_us_location(result.get("location", ""))
            cc    = (result.get("_country_code") or "").upper()
            print(f"  would_filter  : {cc != 'US' if cc else not is_us}")
    except Exception as e:
        print(f"  ✗ RAISED: {e}")
else:
    print("  No ATCI jobs with _external_path found.")

# ── Try fetch_job_detail for one ATCI job WITHOUT _external_path ─────────────
print(f"\n{'─'*60}")
print("fetch_job_detail on first ATCI job WITHOUT _external_path:")
if atci_without_path:
    test_job = dict(atci_without_path[0])
    print(f"  job_id : {test_job.get('job_id')}")
    print(f"  job_url: {test_job.get('job_url')}")
    # Derive _external_path from job_url the same way detail_worker does
    from workers.detail_worker import _extract_city_from_url
    url_city = _extract_city_from_url(test_job.get("job_url",""))
    print(f"  url_city (extract): {url_city!r}")
    print(f"  is_us_location(url_city): {is_us_location(url_city)}")
else:
    print("  All ATCI jobs have _external_path — good.")

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'═'*60}")
print("SUMMARY")
print(f"  Total ATCI jobs in listing     : {len(atci_jobs)}")
print(f"  ATCI with valid _external_path : {len(atci_with_path)}")
print(f"  ATCI missing _external_path    : {len(atci_without_path)}")
if atci_without_path:
    pct = len(atci_without_path) / len(atci_jobs) * 100 if atci_jobs else 0
    print(f"  → {pct:.0f}% of ATCI jobs have no path → detail fetch skipped →")
    print("    location stays '' → is_us_location('') = True → LEAK")
print("═" * 60)
