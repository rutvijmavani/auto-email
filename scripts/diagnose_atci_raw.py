"""
scripts/diagnose_atci_raw.py — Print the raw Workday API JSON for one ATCI job.

This answers: WHY does fetch_job_detail return empty location for ATCI-format jobs?
By dumping the full jobPostingInfo from the detail endpoint we can see exactly which
fields exist and whether the parser is reading from the wrong keys.

Run:
    python scripts/diagnose_atci_raw.py 2>&1 | tee diagnose_atci_raw.txt
"""

import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db.db import init_db, get_conn
from jobs.ats_detector import get_ats_module
from jobs.ats.registry import get_config, parse_slug
from jobs.ats.base import fetch_json, fetch_json_post
from jobs.ats.workday import WORKDAY_HEADERS

init_db()

ats_module = get_ats_module("workday")
config     = get_config("workday")

conn = get_conn()
row  = conn.execute("SELECT ats_slug FROM prospective_companies WHERE company = %s LIMIT 1",
                    ("accenture",)).fetchone()
conn.close()

if row is None:
    print("[ERROR] 'accenture' not found in prospective_companies — nothing to diagnose.")
    sys.exit(1)

slug_info = parse_slug("workday", row["ats_slug"], config)
slug = slug_info["slug"]
wd   = slug_info["wd"]
path = slug_info.get("path", "careers")

print(f"slug_info: {slug_info}")
print("Fetching Accenture listing to find ATCI and R00 jobs...")

raw_jobs = ats_module.fetch_jobs(slug_info, "accenture")
print(f"Total jobs: {len(raw_jobs)}\n")

atci_jobs = [j for j in raw_jobs if str(j.get("job_id","")).startswith("ATCI")]
r00_jobs  = [j for j in raw_jobs if str(j.get("job_id","")).startswith("R00")]

print(f"ATCI jobs: {len(atci_jobs)}, R00 jobs: {len(r00_jobs)}\n")

# ── Show what the LISTING response gives us for ATCI jobs ────────────────────
print("=" * 70)
print("LISTING RESPONSE — first 5 ATCI jobs")
print("-" * 70)
for j in atci_jobs[:5]:
    print(f"  job_id        : {j.get('job_id')}")
    print(f"  location      : {j.get('location')!r}")     # from locationsText
    print(f"  _external_path: {j.get('_external_path')!r}")
    print(f"  job_url       : {j.get('job_url')}")
    print()

# ── Fetch raw detail JSON for the first ATCI job that has _external_path ────
print("=" * 70)
print("RAW DETAIL API RESPONSE — first ATCI job with _external_path")
print("-" * 70)

atci_with_path = [j for j in atci_jobs if j.get("_external_path")]
atci_no_path   = [j for j in atci_jobs if not j.get("_external_path")]

print(f"ATCI jobs with _external_path : {len(atci_with_path)}")
print(f"ATCI jobs WITHOUT _external_path: {len(atci_no_path)}")
print()

if atci_with_path:
    job = atci_with_path[0]
    external_path = job["_external_path"]
    detail_url = (
        f"https://{slug}.{wd}.myworkdayjobs.com"
        f"/wday/cxs/{slug}/{path}{external_path}"
    )
    print(f"Detail URL: {detail_url}")
    print()

    raw_data = fetch_json(detail_url, platform="workday", headers=WORKDAY_HEADERS)
    if not raw_data:
        print("  ✗ fetch_json returned None/empty — API call failed")
    else:
        atci_info = raw_data.get("jobPostingInfo", {})
        print("── jobPostingInfo keys present ──")
        print(f"  {list(atci_info.keys())}")
        print()
        print("── Fields the parser reads ──")
        print(f"  atci_info['location']              : {atci_info.get('location')!r}")
        print(f"  atci_info['additionalLocations']   : {atci_info.get('additionalLocations')!r}")
        print(f"  atci_info['country']               : {atci_info.get('country')!r}")
        print(f"  atci_info['jobRequisitionLocation']: {atci_info.get('jobRequisitionLocation')!r}")
        print()
        print("── Full jobPostingInfo (pretty-printed) ──")
        print(json.dumps(atci_info, indent=2, default=str)[:4000])  # cap at 4000 chars
else:
    print("No ATCI jobs have _external_path — all will fail at the guard clause.")

# ── Same for a R00 job (for comparison) ──────────────────────────────────────
print()
print("=" * 70)
print("RAW DETAIL API RESPONSE — first R00 job (REFERENCE, should have location)")
print("-" * 70)
r00_with_path = [j for j in r00_jobs if j.get("_external_path")]
if r00_with_path:
    job = r00_with_path[0]
    external_path = job["_external_path"]
    detail_url = (
        f"https://{slug}.{wd}.myworkdayjobs.com"
        f"/wday/cxs/{slug}/{path}{external_path}"
    )
    print(f"Detail URL: {detail_url}")
    print(f"job_id    : {job.get('job_id')}")
    print()

    raw_data = fetch_json(detail_url, platform="workday", headers=WORKDAY_HEADERS)
    if not raw_data:
        print("  ✗ fetch_json returned None/empty — API call failed")
    else:
        r00_info = raw_data.get("jobPostingInfo", {})
        print("── jobPostingInfo keys present ──")
        print(f"  {list(r00_info.keys())}")
        print()
        print("── Fields the parser reads ──")
        print(f"  r00_info['location']              : {r00_info.get('location')!r}")
        print(f"  r00_info['additionalLocations']   : {r00_info.get('additionalLocations')!r}")
        print(f"  r00_info['country']               : {r00_info.get('country')!r}")
        print(f"  r00_info['jobRequisitionLocation']: {r00_info.get('jobRequisitionLocation')!r}")
        print()
        print("── Full jobPostingInfo (pretty-printed) ──")
        print(json.dumps(r00_info, indent=2, default=str)[:4000])
else:
    print("No R00 jobs with _external_path found.")

# ── Summary ───────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("SUMMARY")
print(f"  ATCI with _external_path : {len(atci_with_path)}")
print(f"  ATCI without _external_path: {len(atci_no_path)}")
if atci_no_path:
    print("  → ATCI jobs missing path will ALWAYS return original job unchanged")
    print("    (guard clause at fetch_job_detail line 174)")
if atci_with_path and 'atci_info' in dir():
    alpha2 = ((atci_info.get("jobRequisitionLocation") or {}).get("country") or {}).get("alpha2Code","")
    loc    = atci_info.get("location","")
    print(f"  → For ATCI jobs WITH path: alpha2Code={alpha2!r}, location={loc!r}")
print("=" * 70)
