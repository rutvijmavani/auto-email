"""
scripts/careers_url_cleanup.py — READ-ONLY dry run: which stored careers_url values fail the ownership rules.

  python -m scripts.careers_url_cleanup [--out data/careers_url_cleanup_dryrun.csv]

Applies the same rules the live pipeline now enforces (jobs/careers_url_check.py) to every stored
fein_domain_map.careers_url:
  * all phases: vendor / challenge-vendor / aggregator host (blocked_reason)
  * phase4 (Brave): must be anchored to the stored public_domain (phase4_anchor_check); no pd = no anchor

Nothing is written to the database. The CSV lists every failing row with the reason, plus the blast radius a
later cleanup would have to handle: an ATS platform/slug that was
derived from the failing page (ats_source phase4/phase5/brave_pass), and company_ats rows for the employer.
"""
import argparse
import csv
import sys
from collections import Counter

from db.connection import get_conn
from jobs.careers_url_check import blocked_reason, phase4_anchor_check
from jobs.public_domain import _root
from logger import get_logger, init_logging

log = get_logger(__name__)

REASON_NO_ANCHOR = "no-public-domain-anchor"
_BRAVE_DERIVED_ATS_SOURCES = ("phase4", "phase5", "brave_pass")

_SQL = """
    SELECT f.employer_fein, e.employer_name, f.careers_url, f.careers_source, f.public_domain,
           h.detected_platform, h.detected_slug, h.ats_source,
           (SELECT COUNT(*) FROM company_ats c WHERE c.employer_fein = f.employer_fein) AS company_ats_rows
    FROM fein_domain_map f
    JOIN dol_h1b_employers e USING (employer_fein)
    LEFT JOIN h1b_ats_discovery h ON h.employer_fein = f.employer_fein
    WHERE f.careers_url IS NOT NULL
    ORDER BY f.careers_source, f.employer_fein
"""


def evaluate(row: dict, ats_roots) -> str:
    """'' when the stored careers_url passes every rule, else the failure reason."""
    name, url = row["employer_name"], row["careers_url"]
    why = blocked_reason(url, name)
    if why:
        return why
    if row["careers_source"] == "phase4":
        if not row["public_domain"]:
            return REASON_NO_ANCHOR
        ok, why = phase4_anchor_check(url, _root(row["public_domain"]), name, ats_roots)
        if not ok:
            return why
    return ""


def main(argv=None) -> int:
    init_logging("careers_url_cleanup")
    parser = argparse.ArgumentParser(description="Dry run: stored careers_url values failing the ownership rules")
    parser.add_argument("--out", default="data/careers_url_cleanup_dryrun.csv")
    args = parser.parse_args(argv)

    from scripts.discover_h1b_ats import _KNOWN_ATS_DOMAINS

    conn = get_conn()
    try:
        rows = [dict(r) for r in conn.execute(_SQL).fetchall()]
    finally:
        conn.close()

    by_reason, by_source, failing = Counter(), Counter(), []
    for r in rows:
        why = evaluate(r, _KNOWN_ATS_DOMAINS)
        if not why:
            continue
        r["reason"] = why
        r["ats_derived_from_page"] = bool(r["detected_platform"] and r["ats_source"] in _BRAVE_DERIVED_ATS_SOURCES)
        failing.append(r)
        by_reason[(r["careers_source"] or "-", why)] += 1
        by_source[r["careers_source"] or "-"] += 1

    cols = ["employer_fein", "employer_name", "careers_source", "reason", "careers_url", "public_domain",
            "detected_platform", "detected_slug", "ats_source",
            "ats_derived_from_page", "company_ats_rows"]
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(failing)

    print(f"stored careers_url rows checked: {len(rows)}")
    print(f"failing the ownership rules:     {len(failing)}  -> {args.out}")
    print("\nby careers_source:")
    for src, n in by_source.most_common():
        print(f"  {src:12} {n}")
    print("\nby (careers_source, reason):")
    for (src, why), n in by_reason.most_common():
        print(f"  {src:12} {why:26} {n}")
    derived = sum(1 for r in failing if r["ats_derived_from_page"])
    with_ats_rows = sum(1 for r in failing if r["company_ats_rows"])
    print(f"\nblast radius: {derived} failing rows have an ATS platform derived from the failing page; "
          f"{with_ats_rows} have company_ats rows")
    print("\nsample (first 15):")
    for r in failing[:15]:
        print(f"  {r['employer_fein']}  {(r['employer_name'] or '')[:30]:30} {(r['careers_source'] or '-'):8} "
              f"{r['reason']:22} {r['careers_url'][:70]}")
    print("\nNo database writes were made.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
