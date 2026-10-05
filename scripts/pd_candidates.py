"""
scripts/pd_candidates.py — Rule 2 / Rule 3 candidate mining over pd_probe_observation.

Report-only: never changes a verdict. (--notify additionally records emailed clusters in pd_candidate_seen
and prunes old pd_probe_observation / pd_candidate_seen rows.) Lists the content clusters that Rule 1
(jobs/pd_classify.py) does not yet explain, so a human can sample-fetch them and promote
confirmed ones into pd_classify.py as new vendor signatures.

  Rule 3: body hashes shared by >= --min-cluster domains where some rows still ended ok/inconclusive.
  Rule 2: ok rows with a tiny body, grouped by title. For --source oci also requires no external
          script/stylesheet refs; ext_refs is only recorded for the direct fetch, so worker/relay skip that check.

Usage:
    python scripts/pd_candidates.py [--min-cluster 3] [--samples 3] [--source oci|worker|relay]

Promotion guidance: a cluster seen in OCI plus another source is strong; OCI-only is good (check it is
not a WAF block page); worker-only/relay-only is weak (IP-dependent) — sample-open it in a browser first.
Reject any candidate rule that matches a known-real company domain.
"""
import argparse
import html
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    PD_SMALL_BODY_BYTES, PD_PROBE_RETENTION_DAYS, PD_CANDIDATE_SEEN_RETENTION_DAYS,
    PD_CANDIDATE_TITLE_STORE_CHARS, PD_CANDIDATE_TITLE_EMAIL_CHARS,
)
from db.connection import get_conn
from db.pd_probe import prune_observations, prune_candidate_seen
from db.pd_redirect_review import mark_notified, pending_count, unnotified
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


def collect_clusters(conn, min_cluster: int, samples: int) -> list:
    """Every Rule 2/3 cluster across all sources as flat dicts with a stable `cluster_key`
    ('<rule>:<source>:<body_hash|title>'). Rule 2 groups below min_cluster are not clusters."""
    out = []
    for source in sorted(SOURCES):
        for c in rule3_clusters(conn, min_cluster, samples, source):
            out.append({"key": f"3:{source}:{c['body_hash']}", "rule": 3, "source": source,
                        "domains": c["domains"], "title": c["title"] or "", "body_len": c["body_len"],
                        "samples": c["sample_domains"] or ""})
        for c in rule2_titles(conn, samples, source):
            if c["n"] >= min_cluster:
                out.append({"key": f"2:{source}:{c['title']}", "rule": 2, "source": source,
                            "domains": c["n"], "title": c["title"], "body_len": None,
                            "samples": ", ".join(c["sample_domains"] or ())})
    return out


def split_new(conn, clusters: list) -> tuple:
    """-> (new, known) by presence of cluster_key in pd_candidate_seen."""
    if not clusters:
        return [], []
    rows = conn.execute("SELECT cluster_key FROM pd_candidate_seen WHERE cluster_key = ANY(?)",
                        ([c["key"] for c in clusters],)).fetchall()
    seen = {r["cluster_key"] for r in rows}
    return ([c for c in clusters if c["key"] not in seen],
            [c for c in clusters if c["key"] in seen])


def record_seen(conn, clusters: list) -> None:
    """Upsert clusters into pd_candidate_seen: a new key is stamped notified_at=NOW(); a known key only
    gets last_seen_at/domains refreshed (so an active cluster never ages out of retention)."""
    for c in clusters:
        conn.execute("""
            INSERT INTO pd_candidate_seen (cluster_key, rule, source, title, domains, notified_at)
            VALUES (?, ?, ?, ?, ?, NOW())
            ON CONFLICT (cluster_key) DO UPDATE SET last_seen_at = NOW(), domains = EXCLUDED.domains
        """, (c["key"], c["rule"], c["source"], c["title"][:PD_CANDIDATE_TITLE_STORE_CHARS], c["domains"]))


_SOURCE_HINT = {
    "oci":    "OCI only = good (check it is not a WAF block page); strong if another source shows it too",
    "worker": "Worker only = weak, IP-dependent — open a sample in a browser first",
    "relay":  "relay only = weak, IP-dependent — open a sample in a browser first",
}


def _review_section(review: list, pending_total: int) -> str:
    """HTML for NEW pd_redirect_review pairs (redirects held by the employer-name gate)."""
    if not review:
        return ""
    rows = "".join(
        f"<tr><td>{html.escape(str(r['employer_fein']))}</td><td>{html.escape(r['employer_name'] or '')}</td>"
        f"<td>{html.escape(r['old_domain'])}</td><td>{html.escape(r['new_domain'])}</td>"
        f"<td>{html.escape(r['hint'] or '')}</td><td>{html.escape(r['source'])}</td></tr>"
        for r in review)
    return (f"<h3>{len(review)} redirect(s) held by the employer-name gate</h3>"
            f"<p>Nothing is stored for these until you decide ({pending_total} pending in total, including earlier weeks).</p>"
            f"<table border='1' cellpadding='4' style='border-collapse:collapse'>"
            f"<tr><th>FEIN</th><th>Employer</th><th>Old domain</th><th>Redirects to</th><th>Hint</th><th>Source</th></tr>"
            f"{rows}</table>"
            f"<p>Decide with <code>python -m scripts.pd_redirect_review approve|reject &lt;fein&gt; [new_domain]</code>. "
            f"An approved pair is stored the next time that employer's domain is re-resolved "
            f"(scheduled re-detection or the backfill).</p>")


def build_email(new: list, review: list = (), pending_total: int = 0) -> tuple:
    """-> (subject, html) listing the NEW clusters (strongest first) and the NEW name-gate review pairs."""
    rows = "".join(
        f"<tr><td>Rule {c['rule']}</td><td>{html.escape(c['source'])}</td><td>{c['domains']}</td>"
        f"<td>{html.escape(c['title'][:PD_CANDIDATE_TITLE_EMAIL_CHARS])}</td><td>{html.escape(str(c['samples']))}</td></tr>"
        for c in sorted(new, key=lambda c: -c["domains"])
    )
    hints = "".join(f"<li>{html.escape(h)}</li>" for h in _SOURCE_HINT.values())
    body = ""
    parts = []
    if new:
        body = (f"<p>{len(new)} new Rule 2/3 public-domain cluster(s) not explained by Rule 1.</p>"
                f"<table border='1' cellpadding='4' style='border-collapse:collapse'>"
                f"<tr><th>Rule</th><th>Source</th><th>Domains</th><th>Title</th><th>Samples</th></tr>{rows}</table>"
                f"<p>Promotion guidance:</p><ul>{hints}</ul>"
                f"<p>Reject any candidate rule that matches a known-real company domain. "
                f"Run <code>python scripts/pd_candidates.py --source &lt;oci|worker|relay&gt;</code> for details.</p>")
        parts.append(f"{len(new)} new parked-page candidate cluster(s)")
    if review:
        body += _review_section(list(review), pending_total)
        parts.append(f"{len(review)} redirect(s) held for name review")
    return "Public-domain: " + ", ".join(parts), body


def run_notify(conn, min_cluster: int, samples: int, send=None) -> int:
    """Weekly job: email NEW clusters, remember them, then prune both tables. Returns a process exit code.

    The email is the commitment point: a new cluster is recorded in pd_candidate_seen only after it was sent,
    and nothing is pruned unless the send succeeded (or there was nothing to send), so evidence is never
    deleted before it has been reported. `send(subject, html)` -> True | False | None (injectable for tests).
    """
    clusters = collect_clusters(conn, min_cluster, samples)
    new, known = split_new(conn, clusters)
    review = unnotified(conn)
    log.info("pd_candidates: %d clusters (%d new, %d already reported); %d new name-gate review pair(s)",
             len(clusters), len(new), len(known), len(review))
    if new or review:
        if send is None:
            from scripts.log_monitor import _send_email as send   # lazy: log_monitor is Linux-only (fcntl)
        subject, body = build_email(new, review, pending_count(conn) if review else 0)
        if send(subject, body) is not True:
            log.error("pd_candidates: email not sent — %d new cluster(s) and %d review pair(s) left unrecorded "
                      "for next run", len(new), len(review))
            record_seen(conn, known)
            conn.commit()
            return 1
    record_seen(conn, new + known)
    mark_notified(conn, [(r["employer_fein"], r["old_domain"], r["new_domain"]) for r in review])
    obs_gone = prune_observations(conn, PD_PROBE_RETENTION_DAYS)
    seen_gone = prune_candidate_seen(conn, PD_CANDIDATE_SEEN_RETENTION_DAYS)
    conn.commit()
    log.info("pd_candidates: pruned %d pd_probe_observation row(s) (>%dd), %d pd_candidate_seen row(s) (>%dd)",
             obs_gone, PD_PROBE_RETENTION_DAYS, seen_gone, PD_CANDIDATE_SEEN_RETENTION_DAYS)
    return 0


def main():
    init_logging("pd_candidates")
    parser = argparse.ArgumentParser(description="Report uncovered Rule 2/3 clusters in pd_probe_observation")
    parser.add_argument("--min-cluster", type=int, default=3)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--source", choices=sorted(SOURCES), default="oci",
                        help="which tier's response to cluster (default oci; compare sources before promoting a rule)")
    parser.add_argument("--notify", action="store_true",
                        help="weekly mode: all sources, email only NEW clusters, then prune both tables")
    args = parser.parse_args()

    conn = get_conn()
    if args.notify:
        try:
            sys.exit(run_notify(conn, args.min_cluster, args.samples))
        finally:
            conn.close()
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
        # ext_refs is only recorded for the direct (OCI) fetch, so worker/relay rows are matched on body length alone.
        crit = "tiny body and no external refs" if args.source == "oci" else "tiny body (external refs not checked)"
        print(f"\n== Rule 2 candidates: ok rows with {crit} ({sum(r['n'] for r in r2)} rows) ==")
        for c in r2:
            print(f"  {c['n']:3}  {c['title'][:50]!r}  e.g. {', '.join(c['sample_domains'])}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
