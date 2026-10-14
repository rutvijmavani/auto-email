"""
scripts/probe_feeder_totals.py — READ-ONLY probe: which total/paging fields does each ATS really expose?

  python -m scripts.probe_feeder_totals [--platform eightfold] [--company "Starbucks"]
                                        [--out data/probe_feeder_totals.json]

For each ATS platform (custom excluded) it picks three monitored companies — the one with the most stored
open jobs, the median one and the smallest one (see _PICK_SQL) — runs the module's real
fetch_jobs, and records the FIRST --max-calls HTTP responses only (every module goes through
requests.sessions.Session.request, so one hook sees them all), then aborts the fetch. For each recorded
response it keeps: method, host+path, status, elapsed, size, and every scalar field (depth <= 3) plus the
length of every list — so total-style fields (count, total, totalFound, TotalJobsCount, pagingData...) and
the page size actually returned are visible. Nothing is written to the DB or Redis; slug mutations are
discarded.
"""
import argparse
import json
import os
import time
from urllib.parse import urlparse

import requests

from config import PROBE_MAX_CALLS, PROBE_MAX_SCALARS
from db.connection import get_conn
from jobs.ats.registry import get_config, parse_slug
from jobs.ats_detector import get_ats_module
from logger import get_logger, init_logging

log = get_logger(__name__)


class _ProbeStop(BaseException):
    """Raised after enough responses were recorded. BaseException so ATS modules' `except Exception` cannot swallow it."""


def _shape(obj, prefix="", depth=0, out=None):
    """Flatten scalars (depth <= 3) and list lengths into {dotted.path: value}."""
    out = {} if out is None else out
    if len(out) >= PROBE_MAX_SCALARS:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            _shape(v, f"{prefix}.{k}" if prefix else str(k), depth + 1, out)
    elif isinstance(obj, list):
        out[f"{prefix}[len]"] = len(obj)
        if obj and depth < 3 and isinstance(obj[0], (dict, list)):
            _shape(obj[0], f"{prefix}[0]", depth + 1, out)
    elif depth <= 4:
        out[prefix] = obj if not isinstance(obj, str) else obj[:80]
    return out


def _summarise(resp, elapsed):
    parsed = urlparse(resp.url)
    rec = {
        "method": resp.request.method, "url": f"{parsed.netloc}{parsed.path}",
        "query": parsed.query[:200], "status": resp.status_code, "bytes": len(resp.content),
        "elapsed_s": round(elapsed, 2),
    }
    body = resp.content
    try:
        rec["json"] = _shape(resp.json())
    except ValueError:
        text = resp.text
        rec["text_head"] = text[:200]
        rec["counts"] = {tag: text.count(tag) for tag in ("<Job>", "<url>", "<loc>", "article--result", "data-total")}
    return rec


def _probe_one(row):
    platform = row["ats_platform"]
    company = row["company_name"] or row["company"]
    calls, original = [], requests.sessions.Session.request

    def hooked(self, method, url, *args, **kwargs):
        started = time.monotonic()
        resp = original(self, method, url, *args, **kwargs)
        calls.append(_summarise(resp, time.monotonic() - started))
        if len(calls) >= PROBE_MAX_CALLS:
            raise _ProbeStop()
        return resp

    result = {"company": company, "platform": platform, "slug": str(row["ats_slug"])[:200], "calls": calls, "outcome": ""}
    requests.sessions.Session.request = hooked
    try:
        slug_info = parse_slug(platform, row["ats_slug"], get_config(platform))
        jobs = get_ats_module(platform).fetch_jobs(slug_info, company)
        result["outcome"] = f"fetch finished within {len(calls)} calls, {len(jobs)} jobs"
    except _ProbeStop:
        result["outcome"] = f"stopped after {PROBE_MAX_CALLS} calls (by design)"
    except Exception as exc:  # noqa: BLE001 — a probe reports every failure
        result["outcome"] = f"{type(exc).__name__}: {exc}"[:300]
    finally:
        requests.sessions.Session.request = original
    return result


# Per platform: the monitored company with the most stored open jobs, the median one and the smallest one
# (custom excluded — it is verified by hand). Same monitorable rules as db.job_monitor.get_monitorable_companies.
_PICK_SQL = r"""
WITH m AS (
    SELECT pc.company AS company, pc.company AS display, pc.ats_platform AS ats_platform, pc.ats_slug AS ats_slug
    FROM prospective_companies pc
    WHERE pc.ats_platform IS NOT NULL AND pc.ats_platform NOT IN ('unknown', 'unsupported', 'custom')
      AND pc.ats_slug IS NOT NULL AND pc.is_monitored = TRUE
    UNION ALL
    SELECT 'ca:' || ca.id::text, ca.company_name, ca.platform, ca.slug
    FROM company_ats ca
    WHERE ca.is_monitored = TRUE AND ca.stale_since IS NULL
      AND ca.platform IS NOT NULL AND ca.platform NOT IN ('unknown', 'unsupported', 'custom')
      AND ca.slug IS NOT NULL
), stored AS (
    SELECT company, COUNT(*) AS n FROM job_postings WHERE status <> ALL(?) GROUP BY company
), ranked AS (
    SELECT m.*, COALESCE(s.n, 0) AS stored_open,
           ROW_NUMBER() OVER (PARTITION BY m.ats_platform ORDER BY COALESCE(s.n, 0) DESC, m.company) AS rn,
           COUNT(*)     OVER (PARTITION BY m.ats_platform) AS cnt
    FROM m LEFT JOIN stored s ON s.company = m.display
)
SELECT company, display AS company_name, ats_platform, ats_slug, stored_open
FROM ranked
WHERE rn IN (1, (cnt + 1) / 2, cnt)
ORDER BY ats_platform, rn
"""


def _pick_companies():
    conn = get_conn()
    try:
        return [dict(r) for r in conn.execute(_PICK_SQL, (["filled", "expired"],)).fetchall()]
    finally:
        conn.close()


def main(argv=None) -> int:
    init_logging("probe_feeder_totals")
    parser = argparse.ArgumentParser(description="Read-only probe of the total/paging fields each ATS exposes")
    parser.add_argument("--platform")
    parser.add_argument("--company", help="only companies whose key or display name contains this text")
    parser.add_argument("--out", default="data/probe_feeder_totals.json")
    args = parser.parse_args(argv)

    rows = _pick_companies()
    if args.platform:
        rows = [r for r in rows if r["ats_platform"] == args.platform]
    if args.company:
        needle = args.company.lower()
        rows = [r for r in rows if needle in (r["company"] or "").lower() or needle in (r["company_name"] or "").lower()]
    results = []
    for row in rows:
        print(f"probing {row['ats_platform']}: {row['company_name'] or row['company']} (stored open jobs: {row['stored_open']})")
        res = _probe_one(row)
        res["stored_open"] = row["stored_open"]
        results.append(res)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=1, default=str)
    print(f"\n{len(results)} probes written to {args.out}")
    for res in results:
        print(f"\n== {res['platform']} / {res['company']} (stored open: {res['stored_open']}): {res['outcome']}")
        for c in res["calls"]:
            print(f"  {c['method']} {c['url']}?{c['query'][:80]} -> {c['status']} {c['bytes']}B {c['elapsed_s']}s")
            for k, v in (c.get("json") or {}).items():
                print(f"      {k} = {v}")
            if c.get("counts"):
                print(f"      counts = {c['counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
