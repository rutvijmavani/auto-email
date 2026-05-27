"""
scripts/test_detail_worker.py — Verify detail worker is called and what it returns.

What this tests
───────────────
The detail worker processes jobs from two Redis queues:
  queue:detail:adaptive   (high priority — from adaptive scan)
  queue:detail:fullscan   (low priority  — from full scan)

For each job it should:
  1. Pre-flight: check all required keys are present (guard clause check)
  2. Call fetch_job_detail()  →  fills location, _country_code, description
  3. Filter: is_us_location() decides INCLUDE / EXCLUDE

This script NON-DESTRUCTIVELY peeks at both queues (LRANGE, no pop),
then reproduces steps 1-3 for each peeked job so you can see exactly:
  • What payload the worker received
  • Whether the guard clause would fire (missing keys)
  • What fetch_job_detail() returned
  • Whether the API was actually called (before vs after diff)
  • What the final filter decision would be

If both queues are empty it falls back to pending_detail rows from the DB.

Run:
    cd /home/opc/mail
    source venv/bin/activate
    python scripts/test_detail_worker.py 2>&1 | tee /tmp/test_detail_worker.txt

Options (edit the constants below):
    PEEK_N       — how many jobs to peek per queue (default 5)
    DB_FALLBACK  — number of pending_detail rows to try if queues empty
"""

import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── tunables ──────────────────────────────────────────────────────────────────
PEEK_N      = 5    # how many jobs to peek from each Redis queue
DB_FALLBACK = 10   # pending_detail rows to use if queues are empty

SEP  = "─" * 72
DSEP = "═" * 72

# ── Required keys per platform (mirrors _REQUIRED_DETAIL_KEYS in detail_worker)
REQUIRED_DETAIL_KEYS = {
    "workday":         ["_slug", "_wd", "_path", "_external_path"],
    "taleo":           ["_base_url", "_contest_no"],
    "smartrecruiters": ["_company_slug"],
    "icims":           ["_base_url"],
    "jobvite":         ["_slug"],
}

# ─────────────────────────────────────────────────────────────────────────────
print(DSEP)
print("  Detail Worker — Call Verification & Return Value Test")
print(DSEP)

# ── Imports ───────────────────────────────────────────────────────────────────
try:
    from workers.redis_client import get_redis
    from jobs.ats_detector import get_ats_module
    from jobs.ats.registry import get_config, parse_slug, should_fetch_detail
    from jobs.job_filter import is_us_location
    from db.db import init_db, get_conn
    from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN
except ImportError as e:
    print(f"\n[ERROR] Import failed: {e}")
    print("Run from project root with venv active:")
    print("  cd /home/opc/mail && source venv/bin/activate")
    sys.exit(1)

init_db()


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Redis queue depths
# ─────────────────────────────────────────────────────────────────────────────
print("\nStep 1 — Redis queue depths")
print(SEP)

try:
    r = get_redis()
    adaptive_depth  = r.llen(REDIS_DETAIL_ADAPTIVE)
    fullscan_depth  = r.llen(REDIS_DETAIL_FULLSCAN)
    print(f"  queue:detail:adaptive  : {adaptive_depth:,} jobs")
    print(f"  queue:detail:fullscan  : {fullscan_depth:,} jobs")
except Exception as e:
    print(f"  [ERROR] Redis unavailable: {e}")
    print("  Falling back to DB pending_detail rows only.")
    r = None
    adaptive_depth = fullscan_depth = 0


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Collect jobs to test (peek queues + DB fallback)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\nStep 2 — Collect jobs to test (peek {PEEK_N} per queue, non-destructive)")
print(SEP)

jobs_to_test = []   # list of (source_label, payload_dict)

# ── peek adaptive queue (LRANGE doesn't consume items) ───────────────────────
if r and adaptive_depth > 0:
    raw_items = r.lrange(REDIS_DETAIL_ADAPTIVE, 0, PEEK_N - 1)
    for raw in raw_items:
        try:
            payload = json.loads(raw)
            jobs_to_test.append(("queue:detail:adaptive", payload))
        except (json.JSONDecodeError, ValueError) as exc:
            print(f"  [WARN] Malformed JSON in queue:detail:adaptive — skipping. "
                  f"error={exc!r}  raw={raw!r:.120}")
    print(f"  Peeked {len(raw_items)} items from queue:detail:adaptive")
else:
    print("  queue:detail:adaptive is empty — skipping")

# ── peek fullscan queue ───────────────────────────────────────────────────────
if r and fullscan_depth > 0:
    raw_items = r.lrange(REDIS_DETAIL_FULLSCAN, 0, PEEK_N - 1)
    for raw in raw_items:
        try:
            payload = json.loads(raw)
            jobs_to_test.append(("queue:detail:fullscan", payload))
        except (json.JSONDecodeError, ValueError) as exc:
            print(f"  [WARN] Malformed JSON in queue:detail:fullscan — skipping. "
                  f"error={exc!r}  raw={raw!r:.120}")
    print(f"  Peeked {len(raw_items)} items from queue:detail:fullscan")
else:
    print("  queue:detail:fullscan is empty — skipping")

# ── DB fallback: pending_detail rows ──────────────────────────────────────────
if not jobs_to_test:
    print(f"\n  Both queues empty — falling back to {DB_FALLBACK} pending_detail rows from DB")
    try:
        conn = get_conn()
        rows = conn.execute("""
            SELECT company, job_id, job_url, title, location, ats_platform, ats_slug
            FROM job_postings
            WHERE status = %s
            ORDER BY first_seen DESC
            LIMIT %s
        """, ("pending_detail", DB_FALLBACK)).fetchall()
        conn.close()

        if rows:
            print(f"  Found {len(rows)} pending_detail rows in DB")
            for row in rows:
                # Reconstruct best-effort payload from DB row
                # NOTE: underscore keys (_slug, _wd, etc.) are NOT in DB —
                # they live only in the Redis queue payload.
                # This means the guard clause WILL fire for Mode B platforms.
                # That is the bug this test helps diagnose.
                config    = get_config(row["ats_platform"] or "")
                slug_info = {}
                if row.get("ats_slug") and row.get("ats_platform"):
                    try:
                        slug_info = parse_slug(
                            row["ats_platform"], row["ats_slug"], config
                        )
                    except Exception as exc:
                        print(f"  [WARN] parse_slug failed for "
                              f"{row['company']!r} — {exc!r}")

                payload = {
                    "company":     row["company"],
                    "job_id":      row["job_id"],
                    "job_url":     row["job_url"] or "",
                    "title":       row["title"] or "",
                    "location":    row["location"] or "",
                    "description": "",
                    "ats_platform": row["ats_platform"] or "",
                    "ats_slug":    row["ats_slug"] or "",
                    "_country_code": "",
                    # Inject underscore keys from slug_info where possible
                    "_slug":       slug_info.get("slug", ""),
                    "_wd":         slug_info.get("wd", ""),
                    "_path":       slug_info.get("path", ""),
                    "_site":       slug_info.get("site"),
                    "_external_path": "",   # not in DB — will cause guard to fire
                    "_base_url":   "",
                    "_contest_no": "",
                    "_company_slug": "",
                    "_feed_type":  "",
                }
                jobs_to_test.append(("db:pending_detail (keys partial!)", payload))
        else:
            print("  No pending_detail rows found in DB either.")
            print("  The detail worker has fully drained the queue — nothing to test.")
            print("  Try running a monitor/fullscan first to generate new jobs.")
            sys.exit(0)
    except Exception as e:
        print(f"  [ERROR] DB query failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Process each job through fetch_job_detail
# ─────────────────────────────────────────────────────────────────────────────
print(f"\nStep 3 — Run fetch_job_detail on each job")
print(SEP)
print(f"  Testing {len(jobs_to_test)} job(s)…\n")

results = []   # collect for summary

for idx, (source, payload) in enumerate(jobs_to_test, 1):
    company  = payload.get("company", "?")
    job_id   = payload.get("job_id",  "?")
    platform = payload.get("ats_platform", "?")
    title    = payload.get("title",   "?")[:55]

    print(f"  {'─'*68}")
    print(f"  [{idx}] {company!r}  |  platform={platform}  |  job_id={job_id}")
    print(f"       title  : {title}")
    print(f"       source : {source}")
    print()

    # ── Show underscore keys present in payload ───────────────────────────────
    underscore_keys = {k: v for k, v in payload.items()
                       if k.startswith("_") and v not in (None, "", [])}
    missing_keys    = [k for k in REQUIRED_DETAIL_KEYS.get(platform, [])
                       if not payload.get(k)]

    print(f"       Underscore keys in payload:")
    if underscore_keys:
        for k, v in sorted(underscore_keys.items()):
            val = str(v)[:60]
            print(f"         {k:<22} = {val!r}")
    else:
        print(f"         (none)")

    if missing_keys:
        print(f"\n       ⚠  GUARD WILL FIRE — missing required keys: {missing_keys}")
        print(f"          fetch_job_detail will return original dict (NO API call)")
    else:
        required = REQUIRED_DETAIL_KEYS.get(platform, [])
        if required:
            print(f"\n       ✓  All required keys present: {required}")
        else:
            print(f"\n       ℹ  No required keys for platform={platform!r} (Mode A or unknown)")

    # ── Snapshot before ───────────────────────────────────────────────────────
    before_loc  = payload.get("location", "")
    before_cc   = payload.get("_country_code", "")
    before_desc = payload.get("description", "")

    # ── Call fetch_job_detail ──────────────────────────────────────────────────
    config     = get_config(platform)
    ats_module = get_ats_module(platform)

    if not ats_module:
        print(f"\n       [ERROR] No ATS module for platform={platform!r}")
        results.append({"company": company, "job_id": job_id,
                        "platform": platform, "outcome": "no_module"})
        continue

    detail_needed = should_fetch_detail(payload, platform, config,
                                         payload.get("slug_info"))
    print(f"\n       should_fetch_detail() : {detail_needed}")

    if not detail_needed:
        print(f"       → Mode A platform — detail fetch skipped by design")
        after_loc  = before_loc
        after_cc   = before_cc
        after_desc = before_desc
        api_called = False
    else:
        t0 = time.monotonic()
        try:
            if platform == "custom":
                after_job = ats_module.fetch_job_detail(
                    dict(payload), payload.get("slug_info")
                )
            else:
                after_job = ats_module.fetch_job_detail(dict(payload))
        except Exception as exc:
            print(f"       [ERROR] fetch_job_detail raised: {exc}")
            import traceback; traceback.print_exc()
            results.append({"company": company, "job_id": job_id,
                            "platform": platform, "outcome": "exception"})
            continue

        elapsed_ms = int((time.monotonic() - t0) * 1000)

        after_loc  = after_job.get("location", "")
        after_cc   = after_job.get("_country_code", "")
        after_desc = after_job.get("description", "")

        api_called = (
            after_loc  != before_loc
            or after_cc  != before_cc
            or after_desc != before_desc
        )

        print(f"       fetch_job_detail() took : {elapsed_ms} ms")

    # ── Show before / after ───────────────────────────────────────────────────
    print()
    print(f"       {'Field':<20}  {'BEFORE':<30}  {'AFTER'}")
    print(f"       {'─'*20}  {'─'*30}  {'─'*30}")
    print(f"       {'location':<20}  {repr(before_loc)[:30]:<30}  {repr(after_loc)[:30]}")
    print(f"       {'_country_code':<20}  {repr(before_cc)[:30]:<30}  {repr(after_cc)[:30]}")
    print(f"       {'description':<20}  {f'{len(before_desc)} chars':<30}  {f'{len(after_desc)} chars'}")

    # ── API call verdict ──────────────────────────────────────────────────────
    print()
    if not detail_needed:
        api_verdict = "ℹ  SKIPPED (Mode A — all data in listing)"
    elif missing_keys:
        api_verdict = "✗  NOT CALLED — guard fired (missing keys above)"
    elif api_called:
        api_verdict = "✓  API WAS CALLED — job was enriched"
    else:
        api_verdict = "⚠  CALLED but returned NO new data (API empty or parse failed)"

    print(f"       API call result : {api_verdict}")

    # ── Filter decision ───────────────────────────────────────────────────────
    loc_for_filter = after_loc or before_loc
    us = is_us_location(loc_for_filter)
    cc = (after_cc or "").upper()

    if cc and cc != "US":
        filter_result = "EXCLUDE — non-US country code (alpha2)"
        outcome       = "filtered"
    elif not us:
        filter_result = "EXCLUDE — is_us_location() returned False"
        outcome       = "filtered"
    elif us:
        filter_result = "INCLUDE — is_us_location() returned True"
        outcome       = "new"
    else:
        filter_result = "EXCLUDE"
        outcome       = "filtered"

    print(f"       Filter decision : {filter_result}")
    print(f"       (location used) : {loc_for_filter!r}")
    print()

    results.append({
        "company":    company,
        "job_id":     job_id,
        "platform":   platform,
        "outcome":    outcome,
        "api_called": api_called if detail_needed else None,
        "location":   after_loc,
        "cc":         after_cc,
        "missing":    missing_keys,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{DSEP}")
print("  SUMMARY")
print(DSEP)
print(f"  Jobs tested  : {len(results)}")
print()

mode_b = [r for r in results if r.get("api_called") is not None]
mode_a = [r for r in results if r.get("api_called") is None
          and r.get("outcome") not in ("no_module", "exception")]

api_called    = [r for r in mode_b if r.get("api_called") is True]
guard_fired   = [r for r in mode_b if r.get("api_called") is False
                  and r.get("missing")]
no_data       = [r for r in mode_b if r.get("api_called") is False
                  and not r.get("missing")]
promoted      = [r for r in results if r.get("outcome") == "new"]
filtered      = [r for r in results if r.get("outcome") == "filtered"]
errors        = [r for r in results if r.get("outcome") in ("exception", "no_module")]

if mode_a:
    print(f"  Mode A (no detail fetch needed)     : {len(mode_a)}")
if mode_b:
    print(f"  Mode B (detail fetch attempted)     : {len(mode_b)}")
    print(f"    ✓  API called + job enriched       : {len(api_called)}")
    if guard_fired:
        print(f"    ✗  Guard fired (missing keys)      : {len(guard_fired)}")
        for r in guard_fired:
            print(f"         {r['company']:<25} {r['platform']:<15} missing={r['missing']}")
    if no_data:
        print(f"    ⚠  Called but no new data returned: {len(no_data)}")
print()
print(f"  Filter outcomes:")
print(f"    INCLUDE (US, would be saved as 'new') : {len(promoted)}")
print(f"    EXCLUDE (non-US, would be deleted)    : {len(filtered)}")
if errors:
    print(f"    ERROR                                 : {len(errors)}")
print()

if guard_fired:
    print("  ⚠  PROBLEM DETECTED: guard clause fired for the jobs above.")
    print("     fetch_job_detail was NOT called — no API request was made.")
    print("     The payload is missing required underscore keys.")
    print("     Check that _build_detail_payload() in fullscan.py / scan_worker.py")
    print("     forwards all keys listed in PLATFORM_DETAIL_KEYS.")
elif mode_b and len(api_called) == len(mode_b):
    print("  ✅  All Mode B jobs had fetch_job_detail called successfully.")
elif no_data:
    print("  ⚠  fetch_job_detail was called but returned no new data for some jobs.")
    print("     Check the Workday/ATS detail API response or the parser logic.")

print(DSEP)
