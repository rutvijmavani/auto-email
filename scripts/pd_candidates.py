"""
scripts/pd_candidates.py — Rule 2 / Rule 3 candidate mining over pd_probe_observation.

Report-only: never changes a verdict or any row. Lists the content clusters that Rule 1
(jobs/pd_classify.py) does not yet explain, so a human can sample-fetch them and promote
confirmed ones into pd_classify.py as new vendor signatures.

  Rule 3: body hashes shared by >= --min-cluster domains where some rows still ended ok/inconclusive.
  Rule 2: ok rows with a tiny body and no external script/stylesheet refs, grouped by title.

Usage:
    python scripts/pd_candidates.py [--min-cluster 3] [--samples 3]
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import PD_SMALL_BODY_BYTES
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)


def rule3_clusters(conn, min_cluster: int, samples: int) -> list:
    rows = conn.execute("""
        SELECT body_hash,
               COUNT(*)                                                        AS domains,
               COUNT(*) FILTER (WHERE final_verdict IN ('ok', 'inconclusive')) AS open_rows,
               MAX(body_len)                                                   AS body_len,
               MAX(title)                                                      AS title,
               (SELECT string_agg(final_verdict || ':' || n, ', ')
                  FROM (SELECT final_verdict, COUNT(*) AS n FROM pd_probe_observation q
                         WHERE q.body_hash = p.body_hash GROUP BY final_verdict) v)  AS verdicts,
               (SELECT string_agg(domain, ', ')
                  FROM (SELECT domain FROM pd_probe_observation q
                         WHERE q.body_hash = p.body_hash AND q.final_verdict IN ('ok', 'inconclusive')
                         ORDER BY domain LIMIT ?) s)                          AS sample_domains
        FROM pd_probe_observation p
        WHERE body_hash IS NOT NULL
        GROUP BY body_hash
        HAVING COUNT(*) >= ? AND COUNT(*) FILTER (WHERE final_verdict IN ('ok', 'inconclusive')) > 0
        ORDER BY open_rows DESC, domains DESC
    """, (samples, min_cluster)).fetchall()
    return [dict(r) for r in rows]


def rule2_titles(conn, samples: int) -> list:
    rows = conn.execute("""
        SELECT COALESCE(NULLIF(lower(title), ''), '(no title)') AS title,
               COUNT(*)                                          AS n,
               (array_agg(domain ORDER BY domain))[1:?]          AS sample_domains
        FROM pd_probe_observation
        WHERE final_verdict = 'ok' AND resolved_by = 'oci' AND ext_refs = 0 AND body_len < ?
        GROUP BY 1
        ORDER BY n DESC
    """, (samples, PD_SMALL_BODY_BYTES)).fetchall()
    return [dict(r) for r in rows]


def main():
    init_logging("pd_candidates")
    parser = argparse.ArgumentParser(description="Report uncovered Rule 2/3 clusters in pd_probe_observation")
    parser.add_argument("--min-cluster", type=int, default=3)
    parser.add_argument("--samples", type=int, default=3)
    args = parser.parse_args()

    conn = get_conn()
    try:
        r3 = rule3_clusters(conn, args.min_cluster, args.samples)
        print(f"\n== Rule 3 candidates: body hash on >= {args.min_cluster} domains, not fully explained by Rule 1 "
              f"({len(r3)} clusters) ==")
        for c in r3:
            print(f"  open={c['open_rows']:3} domains={c['domains']:3} len={c['body_len']!s:>6} "
                  f"hash={c['body_hash'][:8]} title={(c['title'] or '')[:40]!r} verdicts={c['verdicts']}\n"
                  f"      samples: {c['sample_domains']}")
        r2 = rule2_titles(conn, args.samples)
        print(f"\n== Rule 2 candidates: ok rows with tiny body and no external refs ({sum(r['n'] for r in r2)} rows) ==")
        for c in r2:
            print(f"  {c['n']:3}  {c['title'][:50]!r}  e.g. {', '.join(c['sample_domains'])}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
