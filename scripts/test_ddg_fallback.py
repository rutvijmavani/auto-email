"""
scripts/test_ddg_fallback.py — Test DuckDuckGo Instant Answers as website fallback.

Queries DDG for every company in h1b_ats_discovery that has been checked (last_checked IS NOT NULL)
but has no website_url, to measure reliability before integrating into the main pipeline.

Usage:
    python scripts/test_ddg_fallback.py [--limit N] [--delay SECS]

Options:
    --limit N       Max companies to query (default: all)
    --delay SECS    Seconds between DDG requests (default: 1.0)
    --output FILE   Write CSV results to FILE (default: stdout summary)
"""

import argparse
import csv
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from db.connection import get_conn
from logger import get_logger, init_logging

DDG_API = "https://api.duckduckgo.com/"

log = get_logger(__name__)


def _ddg_lookup(query: str, session: requests.Session) -> dict:
    """Call DDG Instant Answers API and return relevant fields."""
    try:
        r = session.get(
            DDG_API,
            params={
                "q": query,
                "format": "json",
                "no_redirect": "1",
                "no_html": "1",
                "skip_disambig": "1",
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "official_site": data.get("OfficialSite") or "",
            "abstract_url":  data.get("AbstractURL") or "",
            "abstract_text": (data.get("AbstractText") or "")[:120],
            "entity":        data.get("Entity") or "",
            "type":          data.get("Type") or "",
            "heading":       data.get("Heading") or "",
        }
    except Exception as exc:
        return {"error": str(exc)}


def _has_website(d: dict) -> bool:
    return bool(d.get("official_site") or d.get("abstract_url"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    init_logging()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.employer_fein, d.employer_name, h.canonical_name, h.kg_mid
            FROM   h1b_sponsors d
            LEFT JOIN h1b_ats_discovery h ON h.employer_fein = d.employer_fein
            WHERE  h.last_checked IS NOT NULL
              AND  h.website_url  IS NULL
            ORDER BY d.employer_name
            """
            + ("LIMIT %s" % args.limit if args.limit else "")
        )
        rows = cur.fetchall()

    if not rows:
        print("No companies without website found — nothing to test.")
        return

    print(f"Testing DDG on {len(rows)} companies with no website ...\n")

    results = []
    hit = 0
    official_hit = 0
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; h1b-pipeline-test/1.0)"

    for i, (fein, legal_name, canonical_name, kg_mid) in enumerate(rows, 1):
        query = canonical_name or legal_name
        ddg = _ddg_lookup(query, session)

        found = _has_website(ddg)
        if found:
            hit += 1
            if ddg.get("official_site"):
                official_hit += 1

        result = {
            "fein":          fein,
            "legal_name":    legal_name,
            "canonical":     canonical_name or "",
            "kg_mid":        kg_mid or "",
            "official_site": ddg.get("official_site", ""),
            "abstract_url":  ddg.get("abstract_url", ""),
            "heading":       ddg.get("heading", ""),
            "entity":        ddg.get("entity", ""),
            "ddg_type":      ddg.get("type", ""),
            "abstract_text": ddg.get("abstract_text", ""),
            "error":         ddg.get("error", ""),
        }
        results.append(result)

        # Live progress
        site = ddg.get("official_site") or ddg.get("abstract_url") or "(no result)"
        mark = "✓" if found else "✗"
        print(f"[{i:>3}/{len(rows)}] {mark}  {query[:45]:<45}  →  {site[:60]}")

        if i < len(rows):
            time.sleep(args.delay)

    # Summary
    print(f"\n{'='*60}")
    print(f"Total tested : {len(rows)}")
    print(f"Any URL found: {hit:>4}  ({100*hit/len(rows):.1f}%)")
    print(f"OfficialSite : {official_hit:>4}  ({100*official_hit/len(rows):.1f}%)")
    print(f"Still missing: {len(rows)-hit:>4}  ({100*(len(rows)-hit)/len(rows):.1f}%)")

    # Write CSV if requested
    if args.output:
        fields = ["fein","legal_name","canonical","kg_mid","official_site",
                  "abstract_url","heading","entity","ddg_type","abstract_text","error"]
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(results)
        print(f"\nFull results written to: {args.output}")


if __name__ == "__main__":
    main()
