#!/usr/bin/env python3
"""
scripts/pd_gate_sample_recheck.py — One-off diagnostic for the Part 1 gate fix
(docs/discovery-pipeline-hardening.md).

Re-evaluates a random sample of existing fein_domain_map rows stored with
public_domain_method='same_domain' against the FIXED confirmation gate
(jobs.public_domain._redirect_domain — 2xx-only) to measure the false-confirmation
rate the old status<500/status<400 bug produced.

Read-only: makes live HTTP requests to each sampled company's domain but writes
NOTHING to the database. This is purely a measurement run — its output gates
whether a full re-scan of the ~12,100 existing same_domain rows is separately
commissioned (see Part 1 "Expected consequence", locked 2026-09-27).

Usage:
    python scripts/pd_gate_sample_recheck.py                # default 300 rows
    python scripts/pd_gate_sample_recheck.py --sample-size 500
    python scripts/pd_gate_sample_recheck.py --seed 42       # reproducible sample
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.connection import get_conn
from jobs.public_domain import _redirect_domain
from logger import get_logger, init_logging

log = get_logger(__name__)


def _sample_rows(conn, sample_size: int, seed: "float | None"):
    with conn.named_cursor("pd_gate_sample_recheck") as cur:
        cur.itersize = 500
        if seed is not None:
            conn.execute("SELECT setseed(%s)", (seed,))
        cur.execute("""
            SELECT employer_fein, assigned_domain, public_domain
            FROM fein_domain_map
            WHERE public_domain_method = 'same_domain'
              AND assigned_domain IS NOT NULL
            ORDER BY random()
            LIMIT %s
        """, (sample_size,))
        return list(cur)


def main(args: argparse.Namespace) -> None:
    conn = get_conn()
    try:
        rows = _sample_rows(conn, args.sample_size, args.seed)
    finally:
        conn.close()

    if not rows:
        log.info("no same_domain rows found — nothing to re-check")
        return

    log.info("re-checking %d same_domain rows against the fixed 2xx-only gate", len(rows))

    still_confirmed = 0   # gate agrees: a genuine 2xx was seen (redirect domain returned, not None)
    now_inconclusive = 0  # gate disagrees: final response was non-2xx (the bug's false positive)
    no_response = 0       # neither a 2xx nor an HTTP status at all — DNS/connection failure,
                           # not the false-confirmation bug (would have cascaded to root-fallback/
                           # CT log either way), but also not a genuine reconfirmation — counting
                           # it as "still_confirmed" would inflate that bucket with domains that
                           # may simply be offline now, unrelated to the gate fix being measured.
    status_counts: "dict[int, int]" = {}
    inconclusive_examples: list = []
    t0 = time.time()

    for i, row in enumerate(rows, 1):
        fein = row["employer_fein"]
        domain = (row["assigned_domain"] or "").lower().strip()
        if not domain:
            continue
        redir, status = _redirect_domain(domain)
        if status is None and redir is not None:
            # Genuine 2xx: redir == "" (same root) or a differing root — either way
            # _redirect_domain only returns status=None with a non-None redir after
            # an actual 2xx final response.
            still_confirmed += 1
        elif status is None:
            # redir is also None: no HTTP response was ever obtained (DNS failure,
            # connection error, or an unresolvable redirect chain) — not a confirmation.
            no_response += 1
        else:
            now_inconclusive += 1
            status_counts[status] = status_counts.get(status, 0) + 1
            if len(inconclusive_examples) < 20:
                inconclusive_examples.append((fein, domain, status))
        if i % 50 == 0:
            log.info("progress: %d/%d checked (%.0fs elapsed)", i, len(rows), time.time() - t0)

    total = still_confirmed + now_inconclusive + no_response
    false_confirmation_rate = (now_inconclusive / total * 100) if total else 0.0

    log.info(
        "pd_gate_sample_recheck done in %.0fs — sample=%d still_confirmed=%d "
        "now_inconclusive=%d no_response=%d false_confirmation_rate=%.1f%%",
        time.time() - t0, total, still_confirmed, now_inconclusive, no_response, false_confirmation_rate,
    )
    if status_counts:
        log.info("inconclusive status breakdown: %s", dict(sorted(status_counts.items())))
    for fein, domain, status in inconclusive_examples:
        log.info("  example false-confirmation: fein=%s domain=%s status=%s", fein, domain, status)

    log.info(
        "This rate is the basis for deciding whether a full re-scan of the ~12,100 "
        "existing same_domain rows is warranted — that decision is separate and explicit, "
        "not automated by this script (see Part 1, docs/discovery-pipeline-hardening.md)."
    )


if __name__ == "__main__":
    init_logging("pd_gate_sample_recheck")
    parser = argparse.ArgumentParser(
        description="Re-check a sample of existing same_domain rows against the fixed pd confirmation gate"
    )
    parser.add_argument("--sample-size", type=int, default=300,
                        help="Number of same_domain rows to re-check (default 300, doc range 200-500)")
    parser.add_argument("--seed", type=float, default=None,
                        help="Optional setseed() value (-1.0 to 1.0) for a reproducible sample")
    main(parser.parse_args())
