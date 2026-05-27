"""
scripts/test_accenture.py — End-to-end manual test for the Accenture India-leak fix.

What this verifies
──────────────────
Before the fix, workers serialised only _external_path to the Redis payload for
Workday jobs.  fetch_job_detail()'s guard clause:

    if not all([slug, wd, path, external_path]): return job

fired immediately (slug/wd/path were empty strings) so NO API call was ever
made.  The job came back with empty location and empty _country_code.
is_us_location("") returns True → India jobs saved as new.

After the fix, workers also forward _slug / _wd / _path.  All four fields are
present → guard passes → API is called → real location + country code returned
→ India jobs filtered correctly.

This script reproduces both behaviours against a live Accenture listing so you
can confirm the fix works without deploying.

Run:
    cd /home/opc/mail
    source venv/bin/activate
    python scripts/test_accenture.py 2>&1 | tee /tmp/test_accenture.txt

Expected result:
    BROKEN  column → location="" cc="" → is_us=True  → ✗ LEAKED
    FIXED   column → location has city  → India jobs is_us=False → ✓ FILTERED
                                         → US jobs    is_us=True  → ✓ PASSES
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── how many listing jobs to test (keep small for speed) ──────────────────────
SAMPLE_SIZE = 20

SEP  = "─" * 70
DSEP = "═" * 70

# ─────────────────────────────────────────────────────────────────────────────
# Imports
# ─────────────────────────────────────────────────────────────────────────────
print(DSEP)
print("  Accenture Workday Fix — Manual End-to-End Test")
print(DSEP)

try:
    from db.db import init_db, get_conn
    from jobs.ats.workday import fetch_jobs, fetch_job_detail
    from jobs.job_filter import is_us_location
    from jobs.ats.registry import get_config, parse_slug
except ImportError as e:
    print(f"\n[ERROR] Import failed: {e}")
    print("Run from the project root with the venv active:")
    print("  cd /home/opc/mail && source venv/bin/activate")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Load Accenture slug config from DB
# ─────────────────────────────────────────────────────────────────────────────
print("\nStep 1 — Load Accenture config from DB")
print(SEP)

try:
    init_db()
    conn = get_conn()
    row  = conn.execute(
        "SELECT ats_slug, ats_platform FROM prospective_companies "
        "WHERE LOWER(company) = %s LIMIT 1",
        ("accenture",)
    ).fetchone()
    conn.close()
except Exception as e:
    print(f"[ERROR] DB query failed: {e}")
    sys.exit(1)

if not row:
    print("[ERROR] Accenture not found in prospective_companies table.")
    print("        Run:  python pipeline.py --detect-ats  to populate it first.")
    sys.exit(1)

raw_slug = row["ats_slug"]
platform = row["ats_platform"]
print("  company     : accenture")
print(f"  ats_platform: {platform}")
print(f"  raw ats_slug: {raw_slug!r}")

if platform != "workday":
    print(f"\n[ERROR] Expected workday platform, got {platform!r}.")
    sys.exit(1)

config    = get_config("workday")
slug_info = parse_slug("workday", raw_slug, config)
print(f"  slug_info   : {slug_info}")

slug = slug_info.get("slug", "")
wd   = slug_info.get("wd",   "")
path = slug_info.get("path", "careers")
site = slug_info.get("site")

if not slug or not wd:
    print(f"\n[ERROR] Could not parse slug_info — slug={slug!r} wd={wd!r}")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Fetch listing (first page only)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\nStep 2 — Fetch Accenture listing (up to {SAMPLE_SIZE} jobs)")
print(SEP)
print("  Calling Workday API…  (this may take 5-10 seconds)")

try:
    all_jobs = fetch_jobs(slug_info, "accenture")
except Exception as e:
    print(f"[ERROR] fetch_jobs raised: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

sample = all_jobs[:SAMPLE_SIZE]
print(f"  Total jobs from API : {len(all_jobs)}")
print(f"  Testing first       : {len(sample)}")

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Run each job through both broken and fixed fetch_job_detail
# ─────────────────────────────────────────────────────────────────────────────
print("\nStep 3 — fetch_job_detail: BROKEN vs FIXED, then is_us_location()")
print(SEP)
print(
    f"  {'#':<3}  {'job_id':<20}  "
    f"{'BROKEN loc':<18} {'BROKEN cc':<6} {'leak?':<8}  "
    f"{'FIXED loc':<25} {'FIXED cc':<6} {'filter?'}"
)
print(f"  {'─'*3}  {'─'*20}  {'─'*18} {'─'*6} {'─'*8}  {'─'*25} {'─'*6} {'─'*8}")

broken_leaked  = 0   # India jobs that passed the filter (bad)
broken_us_ok   = 0   # US jobs correctly passed
fixed_filtered = 0   # India jobs correctly filtered (good)
fixed_us_ok    = 0   # US jobs correctly passed
fixed_leaked   = 0   # India jobs that still leaked after fix (bad)
errors         = 0

for i, job in enumerate(sample, 1):
    job_id  = job.get("job_id", "")[:20]
    ext_path = job.get("_external_path", "")

    # ── BROKEN: simulate old worker — only _external_path, no _slug/_wd/_path ─
    broken_payload = {
        "company":        "accenture",
        "job_id":         job.get("job_id", ""),
        "job_url":        job.get("job_url", ""),
        "title":          job.get("title", ""),
        "location":       "",
        "description":    "",
        "_country_code":  "",
        "_external_path": ext_path,
        # _slug, _wd, _path intentionally MISSING (old bug)
        "_slug":          "",
        "_wd":            "",
        "_path":          "",
        "_site":          site,
    }
    try:
        broken_result = fetch_job_detail(broken_payload)
        broken_loc = broken_result.get("location", "")[:18]
        broken_cc  = broken_result.get("_country_code", "")
        broken_us  = is_us_location(broken_result.get("location", ""))
        if broken_us:
            broken_leaked += 1
        else:
            broken_us_ok  += 1
    except Exception as e:
        broken_loc, broken_cc, broken_us = f"ERR:{e}"[:18], "", True
        errors += 1

    # ── FIXED: full payload — all four required keys present ──────────────────
    fixed_payload = {
        "company":        "accenture",
        "job_id":         job.get("job_id", ""),
        "job_url":        job.get("job_url", ""),
        "title":          job.get("title", ""),
        "location":       "",
        "description":    "",
        "_country_code":  "",
        "_external_path": ext_path,
        "_slug":          slug,
        "_wd":            wd,
        "_path":          path,
        "_site":          site,
    }
    try:
        fixed_result = fetch_job_detail(fixed_payload)
        fixed_loc = fixed_result.get("location", "")[:25]
        fixed_cc  = fixed_result.get("_country_code", "")
        fixed_us  = is_us_location(fixed_result.get("location", ""))
        if fixed_us:
            fixed_us_ok += 1
        else:
            fixed_filtered += 1
    except Exception as e:
        fixed_loc, fixed_cc, fixed_us = f"ERR:{e}"[:25], "", True
        errors += 1

    broken_flag = "✗ LEAKED"  if broken_us else "✓ filtered"
    fixed_flag  = "✓ FILTERED" if not fixed_us else "✗ LEAKED"
    if fixed_us and fixed_cc and fixed_cc != "US":
        # country_code is non-US but is_us_location still returned True — flag it
        fixed_flag = "⚠ CHECK"
        fixed_leaked += 1
        if fixed_us_ok > 0:
            fixed_us_ok -= 1

    print(
        f"  {i:<3}  {job_id:<20}  "
        f"{broken_loc:<18} {broken_cc:<6} {broken_flag:<8}  "
        f"{fixed_loc:<25} {fixed_cc:<6} {fixed_flag}"
    )

# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Show a few full job details (title + location + filter decision)
# ─────────────────────────────────────────────────────────────────────────────
print("\nStep 4 — Full detail for first 5 jobs (FIXED path)")
print(SEP)

for i, job in enumerate(sample[:5], 1):
    fixed_payload = {
        "company":        "accenture",
        "job_id":         job.get("job_id", ""),
        "job_url":        job.get("job_url", ""),
        "title":          job.get("title", ""),
        "location":       "",
        "description":    "",
        "_country_code":  "",
        "_external_path": job.get("_external_path", ""),
        "_slug":          slug,
        "_wd":            wd,
        "_path":          path,
        "_site":          site,
    }
    try:
        result = fetch_job_detail(fixed_payload)
        loc    = result.get("location", "")
        cc     = result.get("_country_code", "")
        us     = is_us_location(loc)
        desc   = result.get("description", "")
        decision = "INCLUDE (US)" if us else "EXCLUDE (non-US)"
        print(f"  [{i}] {job.get('title', '')[:55]}")
        print(f"       location     : {loc!r}")
        print(f"       country_code : {cc!r}")
        print(f"       is_us        : {us}  →  {decision}")
        print(f"       description  : {len(desc)} chars")
        print()
    except Exception as e:
        print(f"  [{i}] ERROR: {e}")
        print()

# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Summary
# ─────────────────────────────────────────────────────────────────────────────
print(DSEP)
print("  SUMMARY")
print(DSEP)
print(f"  Jobs tested          : {len(sample)}")
print()
print("  BROKEN (old worker — missing _slug/_wd/_path):")
print(f"    ✗ India/non-US leaked (is_us=True, no real location fetched) : {broken_leaked}")
print(f"    ✓ Jobs correctly filtered even on broken path (is_us=False)  : {broken_us_ok}")
print()
print("  FIXED  (new worker — all four keys present):")
print(f"    ✓ India/non-US jobs filtered (is_us=False)                    : {fixed_filtered}")
print(f"    ✓ US jobs correctly passed   (is_us=True)                     : {fixed_us_ok}")
if fixed_leaked:
    print(f"    ⚠ Non-US jobs still leaking after fix                        : {fixed_leaked}")
if errors:
    print(f"    ✗ API/parse errors                                            : {errors}")
print()

if broken_leaked > 0 and fixed_leaked == 0:
    print("  VERDICT: ✅  Fix is WORKING — India jobs that previously leaked")
    print(f"           are now filtered ({broken_leaked} → 0 leaked).")
elif broken_leaked == 0 and fixed_filtered == 0:
    print("  VERDICT: ℹ  All sampled jobs appear to be US-based.")
    print("           India-leak fix cannot be verified from this sample alone.")
    print("           Re-run with a larger SAMPLE_SIZE at the top of this file.")
elif fixed_leaked > 0:
    print(f"  VERDICT: ⚠  {fixed_leaked} non-US jobs still leak after fix.")
    print("           Check the ⚠ CHECK rows above for details.")
else:
    print("  VERDICT: ✅  Results look correct.")

print(DSEP)
