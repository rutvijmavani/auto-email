"""
scripts/probe_mcloud_taleo.py — READ-ONLY probe, run on the server:

  python -m scripts.probe_mcloud_taleo [--skip-scan] [--skip-taleo] [--company "UT Southwestern"]
                                       [--out data/probe_mcloud_taleo.json]

Part A (m-cloud scan): for every monitored company (any ATS) fetch its career-page candidates
  (fein_domain_map.careers_url, https://<domain>, https://jobs.<domain>, https://careers.<domain>, plus up to
  MCLOUD_PROBE_MAX_LINKS same-host job/career links found on them) and look for the m-cloud search service
  (`jobsapi-google.m-cloud.io`) and its employer id (`companies/<uuid>`).
Part B (Taleo audit): for EVERY monitored Taleo company, run the real taleo.fetch_jobs, capture the portal's own
  pagingData.totalCount from the first response, and report reported vs fetched-distinct. When Part A found an
  m-cloud employer id for that company, also pull the m-cloud job list and compare its `ref`s to the Taleo
  contest numbers (m-cloud-only / taleo-only counts).
Nothing is written to the DB or Redis.
"""
import argparse
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests

from config import (MCLOUD_API_BASE, MCLOUD_MAX_PAGES, MCLOUD_PAGE_SIZE, MCLOUD_PROBE_MAX_LINKS,
                    MCLOUD_PROBE_TIMEOUT, MCLOUD_PROBE_WORKERS)
from db.connection import get_conn
from jobs.ats.registry import get_config, parse_slug
from jobs.ats import taleo
from logger import get_logger, init_logging

log = get_logger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; job-monitor-probe)"}
_MARKER = "m-cloud.io"
_UUID_RE = re.compile(r"companies/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I)
_LINK_RE = re.compile(r'href=["\']([^"\'#]+)["\']', re.I)
_LINK_HINT = re.compile(r"job|career|search|opening|position|work-?with|join", re.I)

_COMPANIES_SQL = r"""
SELECT pc.company AS key, pc.company AS name, pc.ats_platform AS platform, pc.ats_slug AS slug,
       pc.domain AS domain, NULL AS careers_url
FROM prospective_companies pc
WHERE pc.ats_platform IS NOT NULL AND pc.ats_platform NOT IN ('unknown', 'unsupported')
  AND pc.ats_slug IS NOT NULL AND pc.is_monitored = TRUE
UNION ALL
SELECT 'ca:' || ca.id::text, ca.company_name, ca.platform, ca.slug, ca.domain, f.careers_url
FROM company_ats ca
LEFT JOIN fein_domain_map f ON f.employer_fein = ca.employer_fein
WHERE ca.is_monitored = TRUE AND ca.stale_since IS NULL
  AND ca.platform IS NOT NULL AND ca.platform NOT IN ('unknown', 'unsupported')
  AND ca.slug IS NOT NULL
"""


def _get(url):
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=MCLOUD_PROBE_TIMEOUT, allow_redirects=True)
        return resp.text if resp.status_code == 200 else ""
    except requests.RequestException:
        return ""


def _candidates(row):
    urls = []
    if row["careers_url"]:
        urls.append(str(row["careers_url"]))
    domain = (row["domain"] or "").strip().lower()
    if domain:
        urls += [f"https://{domain}", f"https://jobs.{domain}", f"https://careers.{domain}"]
    return list(dict.fromkeys(urls))


def _scan_company(row):
    """Return {matched_url, uuids, pages_scanned} — matched_url empty when no page references m-cloud."""
    found = {"matched_url": "", "uuids": [], "pages_scanned": 0}
    queue, seen = _candidates(row), set()
    followed = 0
    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        html = _get(url)
        if not html:
            continue
        found["pages_scanned"] += 1
        if _MARKER in html:
            found["matched_url"] = url
            found["uuids"] = sorted(set(m.lower() for m in _UUID_RE.findall(html)))
            return found
        host = urlparse(url).netloc
        for href in _LINK_RE.findall(html):
            if followed >= MCLOUD_PROBE_MAX_LINKS:
                break
            full = urljoin(url, href)
            if urlparse(full).netloc == host and _LINK_HINT.search(urlparse(full).path) and full not in seen:
                queue.append(full)
                followed += 1
    return found


def _mcloud_jobs(uuid):
    """All m-cloud jobs for one employer id -> (jobs, totalHits)."""
    jobs, offset, total = [], 0, None
    for _ in range(MCLOUD_MAX_PAGES):
        try:
            resp = requests.get(MCLOUD_API_BASE, headers=_HEADERS, timeout=MCLOUD_PROBE_TIMEOUT,
                                params={"pageSize": MCLOUD_PAGE_SIZE, "offset": offset, "companyName": f"companies/{uuid}"})
            data = resp.json()
        except (requests.RequestException, ValueError):
            break
        total = data.get("totalHits", total)
        rows = data.get("searchResults") or []
        if not rows:
            break
        jobs += [r.get("job", {}) for r in rows]
        offset += len(rows)
        if total is not None and offset >= total:
            break
    return jobs, total


def _audit_taleo(row, uuids):
    """Real fetch_jobs + the portal's own totalCount (read from the first search response)."""
    reported, original = {}, requests.sessions.Session.request

    def hooked(self, method, url, *args, **kwargs):
        resp = original(self, method, url, *args, **kwargs)
        if "searchjobs" in url and "reported" not in reported:
            try:
                reported["reported"] = resp.json().get("pagingData", {}).get("totalCount")
            except ValueError:
                pass
        return resp

    rec = {"company": row["name"], "key": row["key"], "slug": str(row["slug"])[:120]}
    requests.sessions.Session.request = hooked
    try:
        slug_info = parse_slug("taleo", row["slug"], get_config("taleo"))
        jobs = taleo.fetch_jobs(slug_info, row["name"])
    except Exception as exc:  # noqa: BLE001 — a probe reports every failure
        rec["error"] = f"{type(exc).__name__}: {exc}"[:300]
        return rec
    finally:
        requests.sessions.Session.request = original
    contest = {j["contest_no"] for j in jobs}
    rec.update(reported=reported.get("reported"), fetched_distinct=len(contest))
    if rec["reported"] is not None:
        rec["gap"] = rec["reported"] - len(contest)
    for uuid in uuids:
        m_jobs, m_total = _mcloud_jobs(uuid)
        refs = {j.get("ref") for j in m_jobs if j.get("ref")}
        rec.setdefault("mcloud", []).append({
            "uuid": uuid, "total_hits": m_total, "distinct_refs": len(refs),
            "in_taleo": len(refs & contest), "mcloud_only": len(refs - contest), "taleo_only": len(contest - refs),
            "by_portal": {p: sum(1 for j in m_jobs if j.get("ats_portalid") == p)
                          for p in sorted({j.get("ats_portalid") for j in m_jobs if j.get("ats_portalid")})},
        })
    return rec


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--skip-scan", action="store_true", help="skip Part A (career-page m-cloud scan)")
    ap.add_argument("--skip-taleo", action="store_true", help="skip Part B (Taleo audit)")
    ap.add_argument("--company", help="substring filter on company name")
    ap.add_argument("--out", default=os.path.join("data", "probe_mcloud_taleo.json"))
    args = ap.parse_args(argv)
    init_logging("probe_mcloud_taleo")

    with get_conn() as conn:
        rows = [dict(r) for r in conn.execute(_COMPANIES_SQL).fetchall()]
    if args.company:
        rows = [r for r in rows if args.company.lower() in (r["name"] or "").lower()]
    log.info("probe: %d monitored companies (%d taleo)", len(rows), sum(r["platform"] == "taleo" for r in rows))

    scan = {}
    if not args.skip_scan:
        with ThreadPoolExecutor(MCLOUD_PROBE_WORKERS) as pool:
            futs = {pool.submit(_scan_company, r): r for r in rows}
            for fut in as_completed(futs):
                scan[futs[fut]["key"]] = fut.result()
        hits = [(r, scan[r["key"]]) for r in rows if scan.get(r["key"], {}).get("matched_url")]
        print(f"\n=== Part A: {len(hits)} of {len(rows)} companies reference m-cloud ===")
        for r, s in hits:
            print(f"  {r['name']} [{r['platform']}] {s['matched_url']} uuids={s['uuids']}")

    taleo_recs = []
    if not args.skip_taleo:
        taleo_rows = [r for r in rows if r["platform"] == "taleo"]
        with ThreadPoolExecutor(MCLOUD_PROBE_WORKERS) as pool:
            futs = [pool.submit(_audit_taleo, r, scan.get(r["key"], {}).get("uuids", [])) for r in taleo_rows]
            taleo_recs = [f.result() for f in futs]
        taleo_recs.sort(key=lambda x: -(x.get("gap") or 0))
        print(f"\n=== Part B: {len(taleo_recs)} Taleo companies (largest reported-vs-fetched gap first) ===")
        for t in taleo_recs:
            if "error" in t:
                print(f"  {t['company']}: ERROR {t['error']}")
                continue
            line = f"  {t['company']}: reported={t['reported']} fetched={t['fetched_distinct']} gap={t.get('gap')}"
            for m in t.get("mcloud", []):
                line += f" | m-cloud hits={m['total_hits']} mcloud_only={m['mcloud_only']} taleo_only={m['taleo_only']}"
            print(line)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump({"scan": scan, "taleo": taleo_recs}, fh, indent=1, default=str)
    print(f"\nfull report: {args.out}")


if __name__ == "__main__":
    main()
