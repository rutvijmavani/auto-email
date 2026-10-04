"""
scripts/pd_candidates.py — Rule 2 / Rule 3 candidate mining over pd_probe_observation.

Report-only: never changes a verdict or any row. Lists the content clusters that Rule 1
(jobs/pd_classify.py) does not yet explain, so a human can sample-fetch them and promote
confirmed ones into pd_classify.py as new vendor signatures.

  Rule 3: body hashes shared by >= --min-cluster domains where some rows still ended ok/inconclusive.
  Rule 2: ok rows with a tiny body and no external script/stylesheet refs, grouped by title.

Usage:
    python scripts/pd_candidates.py [--min-cluster 3] [--samples 3] [--source oci|worker|relay]

Promotion guidance: a cluster seen in OCI plus another source is strong; OCI-only is good (check it is
not a WAF block page); worker-only/relay-only is weak (IP-dependent) — sample-open it in a browser first.
Reject any candidate rule that matches a known-real company domain.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import PD_SMALL_BODY_BYTES
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)


# Clustering view per source. Each tier's response is clustered on its OWN columns — the three IPs can see
# different pages for the same domain, so mixing them would group a WAF wall with a real page. Column names
# below are fixed identifiers (never user input), so interpolating them into the SQL is safe.
#   oci    — the VM's direct fetch; verdict = final_verdict (as before)
#   worker — the CF Worker's response (worker_*); verdict = worker_verdict
#   relay  — the home relay's response (relay_*); verdict = relay_verdict
SOURCES = {
    "oci":    {"hash": "body_hash",        "title": "title",        "len": "body_len",        "verdict": "final_verdict"},
    "worker": {"hash": "worker_body_hash", "title": "worker_title", "len": "worker_body_len", "verdict": "worker_verdict"},
    "relay":  {"hash": "relay_body_hash",  "title": "relay_title",  "len": "relay_body_len",  "verdict": "relay_verdict"},
}


def rule3_clusters(conn, min_cluster: int, samples: int, source: str = "oci") -> list:
    s = SOURCES[source]
    h, t, ln, v = s["hash"], s["title"], s["len"], s["verdict"]
    # final_verdict describes the OCI response only when the OCI tier decided the row; rows decided by
    # the Worker/relay would pair an OCI hash with another tier's verdict (same filter as rule2_titles).
    own = "resolved_by = 'oci'" if source == "oci" else "TRUE"
    own_q = "q.resolved_by = 'oci'" if source == "oci" else "TRUE"
    rows = conn.execute(f"""
        SELECT {h}                                                        AS body_hash,
               COUNT(*)                                                   AS domains,
               COUNT(*) FILTER (WHERE {v} IN ('ok', 'inconclusive'))      AS open_rows,
               MAX({ln})                                                  AS body_len,
               MAX({t})                                                   AS title,
               (SELECT string_agg(vv || ':' || n, ', ')
                  FROM (SELECT {v} AS vv, COUNT(*) AS n FROM pd_probe_observation q
                         WHERE q.{h} = p.{h} AND {own_q} GROUP BY {v}) x) AS verdicts,
               (SELECT string_agg(domain, ', ')
                  FROM (SELECT domain FROM pd_probe_observation q
                         WHERE q.{h} = p.{h} AND {own_q} AND q.{v} IN ('ok', 'inconclusive')
                         ORDER BY domain LIMIT ?) sd)                     AS sample_domains
        FROM pd_probe_observation p
        WHERE {h} IS NOT NULL AND {own}
        GROUP BY {h}
        HAVING COUNT(*) >= ? AND COUNT(*) FILTER (WHERE {v} IN ('ok', 'inconclusive')) > 0
        ORDER BY open_rows DESC, domains DESC
    """, (samples, min_cluster)).fetchall()
    return [dict(r) for r in rows]


def rule2_titles(conn, samples: int, source: str = "oci") -> list:
    s = SOURCES[source]
    t, ln, v = s["title"], s["len"], s["verdict"]
    # ext_refs is only recorded for the direct (OCI) fetch; worker/relay rows rely on body length alone.
    extra = "AND resolved_by = 'oci' AND ext_refs = 0" if source == "oci" else ""
    rows = conn.execute(f"""
        SELECT COALESCE(NULLIF(lower({t}), ''), '(no title)') AS title,
               COUNT(*)                                          AS n,
               (array_agg(domain ORDER BY domain))[1:?]          AS sample_domains
        FROM pd_probe_observation
        WHERE {v} = 'ok' {extra} AND {ln} < ?
        GROUP BY 1
        ORDER BY n DESC
    """, (samples, PD_SMALL_BODY_BYTES)).fetchall()
    return [dict(r) for r in rows]


def main():
    init_logging("pd_candidates")
    parser = argparse.ArgumentParser(description="Report uncovered Rule 2/3 clusters in pd_probe_observation")
    parser.add_argument("--min-cluster", type=int, default=3)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--source", choices=sorted(SOURCES), default="oci",
                        help="which tier's response to cluster (default oci; compare sources before promoting a rule)")
    args = parser.parse_args()

    conn = get_conn()
    try:
        r3 = rule3_clusters(conn, args.min_cluster, args.samples, args.source)
        print(f"\n[source={args.source}]")
        print(f"\n== Rule 3 candidates: body hash on >= {args.min_cluster} domains, not fully explained by Rule 1 "
              f"({len(r3)} clusters) ==")
        for c in r3:
            print(f"  open={c['open_rows']:3} domains={c['domains']:3} len={c['body_len']!s:>6} "
                  f"hash={c['body_hash'][:8]} title={(c['title'] or '')[:40]!r} verdicts={c['verdicts']}\n"
                  f"      samples: {c['sample_domains']}")
        r2 = rule2_titles(conn, args.samples, args.source)
        print(f"\n== Rule 2 candidates: ok rows with tiny body and no external refs ({sum(r['n'] for r in r2)} rows) ==")
        for c in r2:
            print(f"  {c['n']:3}  {c['title'][:50]!r}  e.g. {', '.join(c['sample_domains'])}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
