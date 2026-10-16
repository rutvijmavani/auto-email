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
from jobs.ats.fetch_report import CLEAN_STOPS, report_of
from jobs.ats.registry import get_config, parse_slug
from jobs.ats import taleo
from jobs.http_safe import is_private_host
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


_MAX_REDIRECTS = 5


def _safe_url(url):
    """http(s) only, and the host must not resolve to a private/loopback address."""
    try:
        parts = urlparse(url)
        host = parts.hostname
    except ValueError:      # malformed (e.g. bad IPv6 bracket) -> not fetchable
        return False
    return parts.scheme in ("http", "https") and bool(host) and not is_private_host(host)


def _get(url):
    """GET with redirects followed by hand so the initial URL and every hop pass the private-host check.
    Returns (html, final_url); html is "" on any failure. Callers resolve links against final_url."""
    try:
        for _ in range(_MAX_REDIRECTS + 1):
            if not _safe_url(url):
                return "", url
            resp = requests.get(url, headers=_HEADERS, timeout=MCLOUD_PROBE_TIMEOUT, allow_redirects=False)
            if resp.is_redirect or resp.status_code in (301, 302, 303, 307, 308):
                location = resp.headers.get("Location")
                if not location:
                    return "", url
                url = urljoin(url, location)
                continue
            return (resp.text if resp.status_code == 200 else ""), url
        return "", url   # redirect chain too long
    except (requests.RequestException, ValueError):
        return "", url


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
        html, final_url = _get(url)
        if not html:
            continue
        found["pages_scanned"] += 1
        if _MARKER in html:
            if not found["matched_url"]:
                found["matched_url"] = url      # keep the first marker-only hit if no page yields an id
            uuids = sorted(set(m.lower() for m in _UUID_RE.findall(html)))
            if uuids:
                found["matched_url"] = url
                found["uuids"] = uuids
                return found
        try:
            host = urlparse(final_url).netloc      # links are relative to where the page actually came from
        except ValueError:
            continue
        for href in _LINK_RE.findall(html):
            if followed >= MCLOUD_PROBE_MAX_LINKS:
                break
            try:
                full = urljoin(final_url, href)
                parsed = urlparse(full)
            except ValueError:                      # malformed href — skip it, keep scanning
                continue
            if parsed.netloc == host and _LINK_HINT.search(parsed.path) and full not in seen:
                queue.append(full)
                followed += 1
    return found


def _mcloud_jobs(uuid):
    """All m-cloud jobs for one employer id -> (jobs, totalHits, complete). complete is False when any page
    failed (non-200, not JSON) or MCLOUD_MAX_PAGES ran out before the end, so counts must not be compared."""
    jobs, offset, total = [], 0, None
    for _ in range(MCLOUD_MAX_PAGES):
        try:
            resp = requests.get(MCLOUD_API_BASE, headers=_HEADERS, timeout=MCLOUD_PROBE_TIMEOUT,
                                params={"pageSize": MCLOUD_PAGE_SIZE, "offset": offset, "companyName": f"companies/{uuid}"})
            if resp.status_code != 200:
                log.warning("m-cloud %s: HTTP %s at offset %d", uuid, resp.status_code, offset)
                return jobs, total, False
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("m-cloud %s: page at offset %d failed: %s", uuid, offset, exc)
            return jobs, total, False
        if not isinstance(data, dict):
            return jobs, total, False
        total = data.get("totalHits", total)
        rows = data.get("searchResults")
        if rows is None or not isinstance(rows, list):
            log.warning("m-cloud %s: searchResults missing/not a list at offset %d", uuid, offset)
            return jobs, total, False
        if not rows:
            # an empty page only proves the end when the API told us the total and we have reached it
            return jobs, total, total is not None and offset >= total
        jobs += [r.get("job", {}) for r in rows]
        offset += len(rows)
        if total is not None and offset >= total:
            return jobs, total, True
    return jobs, total, False   # page cap hit


def _audit_taleo(row, uuids):
    """Real fetch_jobs + the portal's own totalCount, read from the fetch's own FetchReport (per call, so
    concurrent audits cannot cross-contaminate)."""
    rec = {"company": row["name"], "key": row["key"], "slug": str(row["slug"])[:120]}
    try:
        slug_info = parse_slug("taleo", row["slug"], get_config("taleo"))
        jobs = taleo.fetch_jobs(slug_info, row["name"])
    except Exception as exc:  # noqa: BLE001 — a probe reports every failure
        rec["error"] = f"{type(exc).__name__}: {exc}"[:300]
        return rec
    report = report_of(jobs)
    clean = report.stop_reason in CLEAN_STOPS
    # the gap vs Taleo's total is the signal this probe reports; only the m-cloud ref comparison needs a Taleo
    # list that is also judged complete (a short list would show phantom "mcloud_only" jobs)
    comparable = clean and report.complete is not False
    contest = {j["contest_no"] for j in jobs}
    rec.update(reported=report.reported_total, fetched_distinct=len(contest), stop_reason=report.stop_reason)
    if clean:
        if rec["reported"] is not None:
            rec["gap"] = rec["reported"] - len(contest)
    else:
        rec["fetch_failed"] = True      # a short read is not a real gap — no definitive gap recorded
    for uuid in uuids:
        m_jobs, m_total, m_complete = _mcloud_jobs(uuid)
        entry = {"uuid": uuid, "total_hits": m_total, "complete": m_complete}
        rec.setdefault("mcloud", []).append(entry)
        if not m_complete:
            continue                    # a partial m-cloud list cannot be compared
        refs = {j.get("ref") for j in m_jobs if j.get("ref")}
        if not comparable:
            entry["distinct_refs"] = len(refs)
            continue                    # nor can a failed or short Taleo fetch
        entry.update({
            "distinct_refs": len(refs),
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
            gap = f"FETCH FAILED ({t['stop_reason']})" if t.get("fetch_failed") else t.get("gap")
            line = f"  {t['company']}: reported={t['reported']} fetched={t['fetched_distinct']} gap={gap}"
            for m in t.get("mcloud", []):
                if "mcloud_only" in m:
                    line += f" | m-cloud hits={m['total_hits']} mcloud_only={m['mcloud_only']} taleo_only={m['taleo_only']}"
                else:
                    line += f" | m-cloud hits={m['total_hits']} not compared (incomplete fetch)"
            print(line)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump({"scan": scan, "taleo": taleo_recs}, fh, indent=1, default=str)
    print(f"\nfull report: {args.out}")


if __name__ == "__main__":
    main()
