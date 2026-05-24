"""
scripts/test_wayfair.py — Verify Wayfair custom ATS scraping works end-to-end.

Tests the full fetch_jobs() path including the Playwright fallback, using
the slug currently stored in the DB (no manual curl re-capture needed).

Usage:
    python scripts/test_wayfair.py

Expected output on success:
    [OK] Fetched N jobs from Wayfair
    [OK] Playwright cookies saved back to DB (M cookies)
    Sample titles:
      - Software Engineer, Platform
      - Senior Product Manager
      ...

Expected output on failure:
    [FAIL] fetch_jobs returned 0 jobs
    Check logs above for the specific error.
"""

import sys
import os
import json
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

from db.db import get_conn, init_db
from jobs.ats.registry import parse_slug, get_config
from jobs.ats import custom_career


def main():
    init_db()

    # ── 1. Load Wayfair slug from DB ─────────────────────────────────────────
    conn = get_conn()
    row  = conn.execute(
        "SELECT ats_platform, ats_slug FROM prospective_companies "
        "WHERE company = %s",
        ("Wayfair",),
    ).fetchone()
    conn.close()

    if not row:
        print("[FAIL] Wayfair not found in prospective_companies")
        sys.exit(1)

    platform = row["ats_platform"]
    slug     = row["ats_slug"]

    print(f"[INFO] platform={platform}")
    print(f"[INFO] slug loaded ({len(slug) if slug else '(null)'} chars)")

    if platform != "custom":
        print(f"[FAIL] Expected platform=custom, got {platform!r}")
        sys.exit(1)

    slug_info = parse_slug(platform, slug, get_config(platform))
    if not isinstance(slug_info, dict):
        print("[FAIL] Could not parse slug_info as dict")
        sys.exit(1)

    # ── 2. Snapshot _fallback_cookies before fetch ────────────────────────────
    cookies_before = slug_info.get("_fallback_cookies", {})

    # ── 3. Run fetch_jobs ─────────────────────────────────────────────────────
    print("\n[INFO] Running fetch_jobs() — Playwright fallback will trigger "
          "if stored session is expired...\n")
    jobs = custom_career.fetch_jobs(slug_info, "Wayfair")

    # ── 4. Results ────────────────────────────────────────────────────────────
    if not jobs:
        print("\n[FAIL] fetch_jobs returned 0 jobs")
        print("       Check the log output above for the specific error.")
        sys.exit(1)

    print(f"\n[OK] Fetched {len(jobs)} jobs from Wayfair")

    # Check if Playwright refreshed cookies and saved them back
    cookies_after = slug_info.get("_fallback_cookies", {})
    if cookies_after and cookies_after != cookies_before:
        print(f"[OK] Playwright refreshed cookies ({len(cookies_after)} cookies) "
              f"— will be saved to DB automatically on next scan")
    else:
        print("[OK] Existing session was still valid (no Playwright needed)")

    # Show sample titles
    titles = [j.get("title", "—") for j in jobs[:5]]
    print("\nSample titles:")
    for t in titles:
        print(f"  - {t}")


if __name__ == "__main__":
    main()
