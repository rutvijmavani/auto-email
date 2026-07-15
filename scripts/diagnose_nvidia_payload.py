"""
Diagnostic script: trace the exact payload built for Nvidia workday jobs.

Checkpoints:
  1. DB row for Nvidia — ats_platform, ats_slug
  2. parse_slug output — what slug_info dict looks like
  3. fetch_jobs sample — first 3 raw jobs from Workday API, with all _ keys
  4. _build_detail_payload output — exact payload pushed to Redis
  5. detail_worker simulation — payload_underscore_keys + should_fetch_detail

Run: python scripts/diagnose_nvidia_payload.py
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.db import init_db, get_conn
from jobs.ats.registry import get_config, parse_slug, should_fetch_detail
from jobs.ats_detector import get_ats_module
from workers.fullscan import _build_detail_payload


SEP  = "-" * 70
SEP2 = "=" * 70

COMPANY = "Nvidia"


def checkpoint(n, title):
    print(f"\n{SEP2}")
    print(f"  CHECKPOINT {n}: {title}")
    print(SEP2)


def show(label, value):
    print(f"  {label}: {value!r}")


# ── Init ──────────────────────────────────────────────────────────────────────
init_db()

# ── 1. DB row ─────────────────────────────────────────────────────────────────
checkpoint(1, "DB row for Nvidia")
conn = get_conn()
row = conn.execute(
    "SELECT company, ats_platform, ats_slug FROM prospective_companies WHERE company = ?",
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
    print(f"  WARNING: slug_info is not a dict — it is {type(slug_info).__name__}: {slug_info!r}")

# ── 3. fetch_jobs sample ──────────────────────────────────────────────────────
checkpoint(3, "fetch_jobs — first 3 normalized jobs from Workday API")
ats_module = get_ats_module(platform)
if not ats_module:
    print(f"  ERROR: no ATS module for platform={platform!r}")
    sys.exit(1)

print(f"  Calling {platform}.fetch_jobs(slug_info, {COMPANY!r}) ...")
import copy
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
        truthy = bool(v)
        print(f"    {k}: {v!r}  (truthy={truthy})")
    if not underscore_keys:
        print("    (no underscore keys found!)")

# ── Check for jobs with missing _external_path ────────────────────────────────
missing_ext = [j for j in raw_jobs if not j.get("_external_path")]
print(f"\n  Jobs with empty/missing _external_path: {len(missing_ext)} / {len(raw_jobs)}")
if missing_ext:
    sample = missing_ext[0]
    print(f"  Sample job with empty _external_path:")
    show("  title",     sample.get("title"))
    show("  job_id",    sample.get("job_id"))
    show("  job_url",   sample.get("job_url"))
    for k, v in sample.items():
        if k.startswith("_"):
            print(f"    {k}: {v!r}")

# ── 4. _build_detail_payload ──────────────────────────────────────────────────
checkpoint(4, "_build_detail_payload output")

# Use a job with missing _external_path if available, otherwise first job
sample_job = missing_ext[0] if missing_ext else raw_jobs[0]
print(f"  Building payload for: {sample_job.get('title')!r} (job_id={sample_job.get('job_id')!r})")
print(f"\n  Input job dict _ keys:")
for k, v in sample_job.items():
    if k.startswith("_"):
        print(f"    {k}: {v!r}  (is not None: {v is not None}, truthy: {bool(v)})")

payload = _build_detail_payload(COMPANY, platform, sample_job, slug_info)

print(f"\n  Output payload _ keys:")
found_any = False
for k, v in payload.items():
    if k.startswith("_"):
        found_any = True
        print(f"    {k}: {v!r}  (truthy: {bool(v)})")
if not found_any:
    print("    (no underscore keys in payload!)")

print(f"\n  slug_info in payload: {payload.get('slug_info')!r}")

# ── 5. detail_worker simulation ───────────────────────────────────────────────
checkpoint(5, "detail_worker simulation")

# Simulate json round-trip (Redis serialization)
payload_json    = json.dumps(payload)
payload_roundtrip = json.loads(payload_json)
job_sim = dict(payload_roundtrip)

print(f"  After JSON round-trip (as detail_worker sees it):")
for k, v in job_sim.items():
    if k.startswith("_"):
        print(f"    {k}: {v!r}  (truthy: {bool(v)})")

payload_underscore_keys = [k for k in job_sim if k.startswith("_") and job_sim.get(k)]
detail_attempted        = should_fetch_detail(job_sim, platform, config,
                                               payload_roundtrip.get("slug_info"))

print(f"\n  payload_underscore_keys: {payload_underscore_keys}")
print(f"  should_fetch_detail:     {detail_attempted}")

from workers.detail_worker import _REQUIRED_DETAIL_KEYS
pre_missing = [k for k in _REQUIRED_DETAIL_KEYS.get(platform, []) if not job_sim.get(k)]
print(f"  _pre_missing:            {pre_missing}")

print(f"\n{SEP2}")
print("  SUMMARY")
print(SEP2)
if not payload_underscore_keys:
    print("  !! payload_underscore_keys is EMPTY — matches the reported bug")
else:
    print(f"  payload_underscore_keys has {len(payload_underscore_keys)} key(s): {payload_underscore_keys}")
if pre_missing:
    print(f"  !! Missing required keys for detail fetch: {pre_missing}")
    if "_external_path" in pre_missing:
        print("     → Nvidia's Workday API is not returning externalPath for this job")
if detail_attempted:
    print("  detail fetch would proceed (all required keys present)")
else:
    print("  !! detail fetch would be SKIPPED → job dropped")
print(SEP)
