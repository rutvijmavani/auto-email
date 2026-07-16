"""
Diagnostic script: trace the exact payload built for Accenture Workday jobs.

Tests BOTH the fullscan and adaptive scan payload builders side-by-side to
identify which path (if either) produces malformed payloads.

Checkpoints:
  1. DB row for Accenture — ats_platform, ats_slug
  2. parse_slug output — what slug_info dict looks like
  3. fetch_jobs sample — first 3 raw jobs from Workday API, with all _ keys
  4a. fullscan _build_detail_payload  (uses is-not-None check — includes empty strings)
  4b. adaptive _build_detail_payload  (uses truthy check — excludes empty strings)
  5. detail_worker simulation for both payloads

Run: python scripts/diagnose_accenture_payload.py
"""

import json
import sys
import os
import copy

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.db import init_db, get_conn
from jobs.ats.registry import get_config, parse_slug, should_fetch_detail
from jobs.ats_detector import get_ats_module
from workers.fullscan import _build_detail_payload as _build_detail_payload_fullscan
from workers.scan_worker import _build_detail_payload as _build_detail_payload_adaptive
from workers.detail_worker import _REQUIRED_DETAIL_KEYS

def _build_detail_payload(company, platform, job, slug_info):
    """Alias kept for compatibility — delegates to fullscan builder."""
    return _build_detail_payload_fullscan(company, platform, job, slug_info)

SEP  = "-" * 70
SEP2 = "=" * 70

COMPANY = "Accenture"


def checkpoint(n, title):
    print(f"\n{SEP2}")
    print(f"  CHECKPOINT {n}: {title}")
    print(SEP2)


def show(label, value):
    print(f"  {label}: {value!r}")


init_db()

# ── 1. DB row ─────────────────────────────────────────────────────────────────
checkpoint(1, f"DB row for {COMPANY}")
conn = get_conn()
row = conn.execute(
    "SELECT company, ats_platform, ats_slug FROM prospective_companies WHERE company ILIKE %s",
    (COMPANY,)
).fetchone()
conn.close()

if not row:
    print(f"  ERROR: '{COMPANY}' not found in prospective_companies")
    sys.exit(1)

raw_slug = row["ats_slug"]
platform = row["ats_platform"]
show("company",      row["company"])
show("ats_platform", platform)
show("ats_slug (raw from DB)", raw_slug)
show("type(ats_slug)", type(raw_slug).__name__)

# ── 2. parse_slug ─────────────────────────────────────────────────────────────
checkpoint(2, "parse_slug output")
config    = get_config(platform)
slug_info = parse_slug(platform, raw_slug, config)
show("config slug_type", config.get("slug_type"))
show("slug_info", slug_info)
show("type(slug_info)", type(slug_info).__name__)

if isinstance(slug_info, dict):
    show("slug_info['slug']", slug_info.get("slug"))
    show("slug_info['wd']",   slug_info.get("wd"))
    show("slug_info['path']", slug_info.get("path"))
    show("slug_info['site']", slug_info.get("site"))
else:
    print(f"  WARNING: slug_info is not a dict — {type(slug_info).__name__}: {slug_info!r}")

# ── 3. fetch_jobs sample ──────────────────────────────────────────────────────
checkpoint(3, "fetch_jobs — first 3 normalized jobs from Workday API")
ats_module = get_ats_module(platform)
if not ats_module:
    print(f"  ERROR: no ATS module for platform={platform!r}")
    sys.exit(1)

print(f"  Calling {platform}.fetch_jobs(slug_info, {COMPANY!r}) ...")
slug_info_before = copy.deepcopy(slug_info) if isinstance(slug_info, dict) else None
try:
    raw_jobs = ats_module.fetch_jobs(slug_info, COMPANY)
except Exception as exc:
    print(f"  ERROR in fetch_jobs: {exc}")
    sys.exit(1)

print(f"  fetch_jobs returned {len(raw_jobs)} jobs")
if slug_info_before is not None and slug_info != slug_info_before:
    print(f"  WARNING: slug_info was MUTATED by fetch_jobs!")
    show("before", slug_info_before)
    show("after",  slug_info)

if not raw_jobs:
    print("  No jobs returned — cannot continue")
    sys.exit(1)

print(f"\n  slug_info after fetch_jobs call:")
show("  slug_info", slug_info)

print(f"\n  Sample — first 3 jobs (all _ keys shown):")
for i, job in enumerate(raw_jobs[:3]):
    print(f"\n  job[{i}]:")
    underscore_keys = {k: v for k, v in job.items() if k.startswith("_")}
    for k, v in underscore_keys.items():
        print(f"    {k}: {v!r}  (truthy={bool(v)})")
    if not underscore_keys:
        print("    (no underscore keys found!)")

# Check for jobs with missing _external_path
missing_ext = [j for j in raw_jobs if not j.get("_external_path")]
print(f"\n  Jobs with empty/missing _external_path: {len(missing_ext)} / {len(raw_jobs)}")
if missing_ext:
    sample = missing_ext[0]
    print(f"  Sample with empty _external_path:")
    show("  title",   sample.get("title"))
    show("  job_id",  sample.get("job_id"))
    show("  job_url", sample.get("job_url"))
    for k, v in sample.items():
        if k.startswith("_"):
            print(f"    {k}: {v!r}")

# ── 4a/4b. _build_detail_payload — fullscan vs adaptive ──────────────────────
checkpoint(4, "_build_detail_payload: FULLSCAN vs ADAPTIVE comparison")

# Test against a job with empty _external_path if one exists; otherwise first job.
# Also test the first numeric-ID job if any can be found.
numeric_id_jobs = [j for j in raw_jobs if j.get("job_id", "").isdigit()]
all_samples = []
if missing_ext:
    all_samples.append(("missing _external_path", missing_ext[0]))
if numeric_id_jobs:
    all_samples.append(("numeric job_id", numeric_id_jobs[0]))
if not all_samples:
    all_samples.append(("first job", raw_jobs[0]))

for label, sample_job in all_samples:
    print(f"\n  ── Sample: {label} ──")
    print(f"  job_id={sample_job.get('job_id')!r}  title={sample_job.get('title')!r}")
    print(f"  Input _ keys:")
    for k, v in sample_job.items():
        if k.startswith("_"):
            print(f"    {k}: {v!r}  (truthy={bool(v)})")

    payload_fs  = _build_detail_payload_fullscan(COMPANY, platform, sample_job, slug_info)
    payload_adp = _build_detail_payload_adaptive(COMPANY, platform, sample_job, slug_info)

    for tag, payload in [("FULLSCAN (is not None check)", payload_fs),
                         ("ADAPTIVE (truthy check)     ", payload_adp)]:
        ukeys = {k: v for k, v in payload.items() if k.startswith("_")}
        missing = [k for k in _REQUIRED_DETAIL_KEYS.get(platform, []) if not payload.get(k)]
        status = "OK" if not missing else f"!! MISSING {missing}"
        print(f"\n    [{tag}]  → {status}")
        for k, v in ukeys.items():
            print(f"      {k}: {v!r}")
        if not ukeys:
            print("      (no underscore keys in payload)")

# Use fullscan payload for simulation (it's the stricter reference)
payload = _build_detail_payload_fullscan(COMPANY, platform,
                                         all_samples[0][1], slug_info)

# ── 5. detail_worker simulation ───────────────────────────────────────────────
checkpoint(5, "detail_worker simulation (fullscan payload)")

payload_json      = json.dumps(payload)
payload_roundtrip = json.loads(payload_json)
job_sim = dict(payload_roundtrip)

print(f"  After JSON round-trip (as detail_worker sees it):")
for k, v in job_sim.items():
    if k.startswith("_"):
        print(f"    {k}: {v!r}  (truthy: {bool(v)})")

payload_underscore_keys = [k for k in job_sim if k.startswith("_") and job_sim.get(k)]
detail_attempted        = should_fetch_detail(job_sim, platform, config,
                                              payload_roundtrip.get("slug_info"))
pre_missing = [k for k in _REQUIRED_DETAIL_KEYS.get(platform, []) if not job_sim.get(k)]

print(f"\n  payload_underscore_keys: {payload_underscore_keys}")
print(f"  should_fetch_detail:     {detail_attempted}")
print(f"  _pre_missing:            {pre_missing}")

print(f"\n{SEP2}")
print("  SUMMARY")
print(SEP2)
if not payload_underscore_keys:
    print("  !! payload_underscore_keys is EMPTY — new payloads also broken")
elif pre_missing:
    print(f"  !! Missing required keys for detail fetch: {pre_missing}")
    if "_external_path" in pre_missing:
        print("     → Accenture Workday API not returning externalPath or usable externalUrl")
else:
    print(f"  OK: payload has all required keys: {payload_underscore_keys}")
    print("  New fullscan payloads look correct")

print(f"\n  Adaptive path would produce missing_keys: "
      f"{[k for k in _REQUIRED_DETAIL_KEYS.get(platform, []) if not _build_detail_payload_adaptive(COMPANY, platform, all_samples[0][1], slug_info).get(k)]}")

if detail_attempted:
    print("  detail fetch would proceed (all required keys present)")
else:
    print("  !! detail fetch would be SKIPPED → job dropped by detail_worker")
print(SEP)
