"""
scripts/pipeline_metrics.py — H1B enrichment pipeline performance report.

Shows which phase found public_domain / careers_url / ATS for each company,
phase-level success rates, and regression detection (last 30 days vs prior 30 days).

Usage:
    python scripts/pipeline_metrics.py
    python scripts/pipeline_metrics.py --days 14
    python scripts/pipeline_metrics.py --no-signal-top 20
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

_SEP  = "─" * 70
_DSEP = "═" * 70


def _pct(n, total):
    return f"{n / total * 100:.1f}%" if total else "—"


def _phase_table(rows, total, label):
    print(f"\n  {label}  (total: {total})")
    print(f"  {'Phase':<18} {'Count':>8}  {'%':>7}")
    print(f"  {_SEP[:40]}")
    for r in rows:
        phase = list(r.values())[0] or "null/skipped"
        count = list(r.values())[1]
        print(f"  {phase:<18} {count:>8}  {_pct(count, total):>7}")


def _regression_block(conn, col, label, days):
    """Compare phase distribution: last N days vs prior N days."""
    cur = conn.execute(f"""
        SELECT
            period,
            {col},
            COUNT(*) AS n
        FROM (
            SELECT
                CASE
                    WHEN run_at > NOW() - INTERVAL '{days} days' THEN 'recent'
                    ELSE 'prior'
                END AS period,
                {col}
            FROM h1b_enrichment_metrics
            WHERE run_at > NOW() - INTERVAL '{days * 2} days'
              AND {col} IS NOT NULL
        ) sub
        GROUP BY period, {col}
        ORDER BY period DESC, n DESC
    """)
    rows = cur.fetchall()
    if not rows:
        return

    from collections import defaultdict
    by_period = defaultdict(dict)
    for r in rows:
        by_period[r["period"]][r[col] or "null"] = r["n"]

    recent = by_period.get("recent", {})
    prior  = by_period.get("prior", {})
    phases = sorted(set(list(recent.keys()) + list(prior.keys())))

    if not recent and not prior:
        return

    print(f"\n  {label} — regression check (recent {days}d vs prior {days}d)")
    print(f"  {'Phase':<18} {'Recent':>10}  {'Prior':>10}  {'Δ':>8}")
    print(f"  {_SEP[:52]}")

    r_total = sum(recent.values())
    p_total = sum(prior.values())

    for phase in phases:
        r_n = recent.get(phase, 0)
        p_n = prior.get(phase, 0)
        r_p = r_n / r_total * 100 if r_total else 0
        p_p = p_n / p_total * 100 if p_total else 0
        delta = r_p - p_p
        delta_str = f"{delta:+.1f}pp"
        flag = "  ⚠" if abs(delta) >= 10 else ""
        print(f"  {phase:<18} {r_n:>5} ({r_p:>4.0f}%)  {p_n:>5} ({p_p:>4.0f}%)  {delta_str:>8}{flag}")


def run_report(days: int = 7, no_signal_top: int = 10) -> None:
    conn = get_conn()
    try:
        print(f"\n{_DSEP}")
        print(f"  H1B ENRICHMENT PIPELINE METRICS  (last {days} days)")
        print(f"{_DSEP}")

        # ── PUBLIC DOMAIN ────────────────────────────────────────────────────
        print(f"\n  {_SEP}")
        print("  PUBLIC DOMAIN  (domain_enrichment_worker)")
        print(f"  {_SEP}")

        pd_rows = conn.execute("""
            SELECT public_domain_method, COUNT(*) AS n
            FROM h1b_enrichment_metrics
            WHERE worker = 'domain_enrichment'
              AND run_at > NOW() - INTERVAL %s
            GROUP BY public_domain_method
            ORDER BY n DESC
        """, (f"{days} days",)).fetchall()

        pd_total = sum(r["n"] for r in pd_rows)
        _phase_table(pd_rows, pd_total, "Resolution method")

        no_signal = next((r["n"] for r in pd_rows
                          if r["public_domain_method"] == "no_signal"), 0)
        if pd_total:
            print(f"\n  Coverage: {pd_total - no_signal}/{pd_total} resolved "
                  f"({_pct(pd_total - no_signal, pd_total)})  "
                  f"no_signal: {no_signal} ({_pct(no_signal, pd_total)})")

        # Top unresolved companies (high petition_count, no public domain)
        if no_signal_top > 0:
            unresolved = conn.execute("""
                SELECT m.employer_fein, e.employer_name,
                       COALESCE(u.petition_count, 0) AS petition_count,
                       f.assigned_domain
                FROM h1b_enrichment_metrics m
                JOIN dol_h1b_employers e ON e.employer_fein = m.employer_fein
                JOIN fein_domain_map f   ON f.employer_fein = m.employer_fein
                LEFT JOIN (
                    SELECT dh.employer_fein, COUNT(*) AS petition_count
                    FROM uscis_dol_fuzzy_map um
                    JOIN dol_h1b_employers dh ON dh.employer_fein = um.dol_fein
                    GROUP BY dh.employer_fein
                ) u ON u.employer_fein = m.employer_fein
                WHERE m.public_domain_method = 'no_signal'
                  AND m.run_at > NOW() - INTERVAL %s
                ORDER BY petition_count DESC
                LIMIT %s
            """, (f"{days} days", no_signal_top)).fetchall()

            if unresolved:
                print(f"\n  Top {no_signal_top} unresolved (no_signal) — high priority targets:")
                print(f"  {'FEIN':<14} {'Petitions':>10}  {'Domain':<25}  Name")
                print(f"  {_SEP}")
                for r in unresolved:
                    print(f"  {r['employer_fein']:<14} {r['petition_count']:>10}  "
                          f"{r['assigned_domain'] or '—':<25}  {r['employer_name']}")

        # ── CAREER URL ───────────────────────────────────────────────────────
        print(f"\n  {_SEP}")
        print("  CAREER URL  (both workers)")
        print(f"  {_SEP}")

        cu_rows = conn.execute("""
            SELECT careers_source, COUNT(*) AS n
            FROM h1b_enrichment_metrics
            WHERE careers_url IS NOT NULL
              AND run_at > NOW() - INTERVAL %s
            GROUP BY careers_source
            ORDER BY n DESC
        """, (f"{days} days",)).fetchall()

        cu_total = sum(r["n"] for r in cu_rows)
        _phase_table(cu_rows, cu_total, "Source phase")

        # Companies with careers_url but still no ATS
        no_ats_careers = conn.execute("""
            SELECT COUNT(DISTINCT employer_fein) AS n
            FROM h1b_enrichment_metrics
            WHERE careers_url IS NOT NULL
              AND ats_platform IS NULL
              AND run_at > NOW() - INTERVAL %s
        """, (f"{days} days",)).fetchone()["n"]
        if no_ats_careers:
            print(f"\n  ⚠  {no_ats_careers} companies have careers_url but no ATS detected "
                  f"— discovery worker may need another pass")

        # ── ATS DETECTION ────────────────────────────────────────────────────
        print(f"\n  {_SEP}")
        print("  ATS DETECTION  (both workers)")
        print(f"  {_SEP}")

        ats_rows = conn.execute("""
            SELECT ats_source, COUNT(*) AS n
            FROM h1b_enrichment_metrics
            WHERE ats_platform IS NOT NULL
              AND run_at > NOW() - INTERVAL %s
            GROUP BY ats_source
            ORDER BY n DESC
        """, (f"{days} days",)).fetchall()

        ats_total = sum(r["n"] for r in ats_rows)
        _phase_table(ats_rows, ats_total, "Detection phase")

        # ATS platform breakdown
        plat_rows = conn.execute("""
            SELECT ats_platform, COUNT(DISTINCT employer_fein) AS companies
            FROM h1b_enrichment_metrics
            WHERE ats_platform IS NOT NULL
              AND run_at > NOW() - INTERVAL %s
            GROUP BY ats_platform
            ORDER BY companies DESC
            LIMIT 15
        """, (f"{days} days",)).fetchall()

        if plat_rows:
            plat_total = sum(r["companies"] for r in plat_rows)
            print(f"\n  Platform breakdown  (top 15 by company count):")
            print(f"  {'Platform':<25} {'Companies':>10}  {'%':>7}")
            print(f"  {_SEP[:46]}")
            for r in plat_rows:
                print(f"  {r['ats_platform']:<25} {r['companies']:>10}  "
                      f"{_pct(r['companies'], plat_total):>7}")

        # ── REGRESSION DETECTION ─────────────────────────────────────────────
        print(f"\n  {_SEP}")
        print("  REGRESSION DETECTION")
        print(f"  {_SEP}")

        _regression_block(conn, "public_domain_method", "Public domain", days)
        _regression_block(conn, "careers_source",       "Career URL",    days)
        _regression_block(conn, "ats_source",           "ATS detection", days)

        # ── PERFORMANCE ──────────────────────────────────────────────────────
        print(f"\n  {_SEP}")
        print("  PROCESSING PERFORMANCE")
        print(f"  {_SEP}")

        perf = conn.execute("""
            SELECT
                worker,
                COUNT(*)                              AS runs,
                ROUND(AVG(duration_ms))               AS avg_ms,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms)) AS p50_ms,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)) AS p95_ms,
                MAX(duration_ms)                      AS max_ms
            FROM h1b_enrichment_metrics
            WHERE run_at > NOW() - INTERVAL %s
              AND duration_ms IS NOT NULL
            GROUP BY worker
            ORDER BY worker
        """, (f"{days} days",)).fetchall()

        if perf:
            print(f"\n  {'Worker':<25} {'Runs':>8}  {'Avg':>8}  {'p50':>8}  {'p95':>8}  {'Max':>8}")
            print(f"  {_SEP[:70]}")
            for r in perf:
                print(f"  {r['worker']:<25} {r['runs']:>8}  "
                      f"{r['avg_ms']:>6}ms  {r['p50_ms']:>6}ms  "
                      f"{r['p95_ms']:>6}ms  {r['max_ms']:>6}ms")

        print(f"\n{_DSEP}\n")

    finally:
        conn.close()


if __name__ == "__main__":
    init_logging("pipeline_metrics")
    parser = argparse.ArgumentParser(description="H1B enrichment pipeline metrics report")
    parser.add_argument("--days",           type=int, default=7,
                        help="Report window in days (default: 7)")
    parser.add_argument("--no-signal-top",  type=int, default=10,
                        help="Top N unresolved companies to show (default: 10)")
    args = parser.parse_args()
    run_report(days=args.days, no_signal_top=args.no_signal_top)
