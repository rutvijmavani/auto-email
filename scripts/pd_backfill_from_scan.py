# scripts/pd_backfill_from_scan.py — one-off backfill from a parked_domain_scan_v4 CSV.
#
# Does two things, per CSV row (domain = fein_domain_map.public_domain at scan time):
#   1. fein_domain_map: verdict parked -> clear public_domain + method + host + last_status/retry/attempt
#      (careers_url untouched); verdict ok/challenge -> fill a NULL public_domain_host from final_host when its
#      registrable root equals public_domain (the _write_domain invariant).
#   2. pd_probe_observation: one whole-snapshot evidence row per domain from the oci_* / worker_* / relay_*
#      columns (oci_hash -> body_hash), via db.pd_probe.backfill_probe (never overwrites a row probed at or
#      after the scan time).
# Rows re-resolved since the scan are left alone: a clear only fires while public_domain still equals the
# scanned domain AND last_enriched_at is not newer than the scan time. Every parked row is also re-probed
# LIVE with the production classifier (_probe_host) and cleared only if it is still parked (a scan is a
# snapshot; domains come back). Dry-run probes write no pd_probe_observation rows; --apply does, and those
# fresher rows then win over the scan's older evidence for the same domains.
# cross_domain rows are only reported. DRY RUN by default; nothing is written without --apply.
#
# Usage (on the VM, where the DB lives):
#   python -m scripts.pd_backfill_from_scan data/scan_result_4.csv            # dry-run report
#   python -m scripts.pd_backfill_from_scan data/scan_result_4.csv --apply

import argparse
import csv
import os
import sys
from collections import Counter
from datetime import datetime, timezone

import jobs.public_domain as pd
from config import PD_BACKFILL_RECHECK_WORKERS
from concurrent.futures import ThreadPoolExecutor
from db.connection import get_conn
from db.pd_probe import backfill_probe
from jobs.public_domain import _pd_host, _probe_host, _root
from logger import get_logger, init_logging

log = get_logger(__name__)

_SHARED = ("employer_fein", "final_verdict", "final_reason", "resolved_by", "final_host")
_OCI = {  # obs key -> CSV column
    "status": "oci_status", "final_url": "oci_final_url", "server": "oci_server", "fetch_via": "oci_fetch_via",
    "body_len": "oci_body_len", "ext_refs": "oci_ext_refs", "body_hash": "oci_hash", "title": "oci_title",
    "snippet": "oci_snippet", "cookie_names": "oci_cookie_names", "header_names": "oci_header_names",
    "error_type": "oci_error_type", "impersonate": "impersonate",
}
_WORKER = {
    "worker_status": "worker_status", "worker_verdict": "worker_verdict", "worker_body_len": "worker_body_len",
    "worker_title": "worker_title", "worker_body_hash": "worker_hash", "worker_cookie_names": "worker_cookie_names",
}
_RELAY = {
    "relay_status": "relay_status", "relay_verdict": "relay_verdict", "relay_body_len": "relay_body_len",
    "relay_title": "relay_title", "relay_body_hash": "relay_hash", "relay_cookie_names": "relay_cookie_names",
}
_INT_KEYS = frozenset({"status", "body_len", "ext_refs", "worker_status", "worker_body_len",
                       "relay_status", "relay_body_len"})
_CLEAR_VERDICT = "parked"
_FILL_VERDICTS = frozenset({"ok", "challenge"})
_SAMPLE_N = 8
_BATCH = 500


def _val(row: dict, col: str, key: str):
    v = (row.get(col) or "").strip()
    if v == "":
        return None
    if key in _INT_KEYS:
        try:
            return int(v)
        except ValueError:
            return None
    return v


def build_obs(row: dict) -> dict:
    """Whole-snapshot observation dict (every tier key present, None when the CSV cell is blank)."""
    obs = {k: _val(row, k, k) for k in _SHARED}
    if obs["resolved_by"] == "none":
        obs["resolved_by"] = None
    obs["cross_domain"] = (row.get("cross_domain") or "").strip().lower() == "yes"
    for table in (_OCI, _WORKER, _RELAY):
        for key, col in table.items():
            obs[key] = _val(row, col, key)
    return obs


def load_rows(path: str) -> list:
    csv.field_size_limit(sys.maxsize)
    with open(path, newline="", encoding="utf-8") as fh:
        return [r for r in csv.DictReader(fh) if (r.get("domain") or "").strip()]


def fetch_map(conn, feins: list) -> dict:
    out = {}
    for i in range(0, len(feins), _BATCH):
        rows = conn.execute(
            "SELECT employer_fein, public_domain, public_domain_host, last_enriched_at "
            "FROM fein_domain_map WHERE employer_fein = ANY(?)", (feins[i:i + _BATCH],)).fetchall()
        for r in rows:
            out[r["employer_fein"]] = r
    return out


def fetch_probed(conn, domains: list) -> dict:
    out = {}
    for i in range(0, len(domains), _BATCH):
        rows = conn.execute(
            "SELECT domain, probed_at FROM pd_probe_observation WHERE domain = ANY(?)",
            (domains[i:i + _BATCH],)).fetchall()
        for r in rows:
            out[r["domain"]] = r["probed_at"]
    return out


def recheck(domains: list, workers: int) -> dict:
    """Live re-probe with the production classifier (jobs.public_domain._probe_host). -> {domain: result}.

    A scan is a snapshot: a parked domain may have come back since. Each thread uses its own curl session
    (_probe_host's thread-local default). Probe failures are returned as None (treated as 'not confirmed
    parked', so the row is kept).
    """
    def one(domain):
        try:
            return domain, _probe_host(domain)
        except Exception as exc:
            log.warning("recheck failed for %s: %s", domain, exc)
            return domain, None
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return dict(ex.map(one, domains))


def plan(rows: list, fmap: dict, probed: dict, scan_ts: datetime, rechecked: "dict | None" = None) -> dict:
    """Classify every row. Returns {clear: [...], fill: [...], obs: [...], stats: Counter, samples: {...}}.

    rechecked ({domain: live _probe_host result}) gates the clears: a parked row is cleared only if the live
    probe is still parked; a live ok/challenge result keeps the row (and may fill the host instead).
    """
    rechecked = rechecked or {}
    p = {"clear": [], "fill": [], "obs": [], "stats": Counter(), "samples": {}}
    st = p["stats"]
    for row in rows:
        domain = row["domain"].strip().lower()
        fein = (row.get("employer_fein") or "").strip()
        verdict = (row.get("final_verdict") or "").strip()
        st["verdict:" + (verdict or "?")] += 1
        if (row.get("cross_domain") or "").strip().lower() == "yes":
            st["cross_domain (report only)"] += 1
            p["samples"].setdefault("cross_domain", []).append((domain, row.get("final_host")))
        existing = probed.get(domain)
        if verdict and (existing is None or existing < scan_ts):
            p["obs"].append((domain, build_obs(row)))
            st["obs insert" if existing is None else "obs update"] += 1
        elif verdict:
            st["obs skipped (fresher row)"] += 1
        cur = fmap.get(fein)
        if cur is None or (cur["public_domain"] or "").lower() != domain:
            st["map skipped (public_domain changed/missing)"] += 1
            continue
        if verdict == _CLEAR_VERDICT:
            if cur["last_enriched_at"] is not None and cur["last_enriched_at"] > scan_ts:
                st["clear skipped (re-enriched since scan)"] += 1
            else:
                live = rechecked.get(domain)
                if live is None or live["verdict"] != _CLEAR_VERDICT:
                    why = "probe failed" if live is None else f"{live['verdict']}:{live['reason']}"
                    st["clear KEPT (live recheck not parked)"] += 1
                    p["samples"].setdefault("kept (live recheck)", []).append((domain, row.get("final_reason"), why))
                    if live is not None and live["confirmed"] and not cur["public_domain_host"]:
                        host = _pd_host(domain, live)
                        if host:
                            p["fill"].append((fein, host))
                    continue
                p["clear"].append((fein, domain))
                p["samples"].setdefault("clear", []).append((domain, row.get("final_reason")))
        elif verdict in _FILL_VERDICTS and not cur["public_domain_host"]:
            host = (row.get("final_host") or "").strip().lower()
            if host and _root(host) == _root(domain):
                p["fill"].append((fein, host))
                p["samples"].setdefault("fill", []).append((domain, host))
            else:
                st["fill skipped (host root != domain / empty)"] += 1
    st["WOULD CLEAR public_domain"] = len(p["clear"])
    st["WOULD FILL public_domain_host"] = len(p["fill"])
    return p


def report(p: dict, scan_ts: datetime) -> None:
    print(f"scan time (freshness cut-off): {scan_ts.isoformat()}")
    for k, v in sorted(p["stats"].items()):
        print(f"  {k:48s} {v:>7}")
    for name, items in p["samples"].items():
        print(f"\n{name} samples ({len(items)} total):")
        for it in items[:_SAMPLE_N]:
            print("   ", it)


def apply(conn, p: dict) -> tuple:
    """Run the fein_domain_map clears and host fills; commits. Returns (cleared, filled)."""
    cleared = filled = 0
    for fein, domain in p["clear"]:
        cleared += conn.execute(
            "UPDATE fein_domain_map SET public_domain = NULL, public_domain_method = NULL, "
            "public_domain_host = NULL, public_domain_last_status = NULL, public_domain_retry_count = 0, "
            "public_domain_last_attempt_at = NULL "
            "WHERE employer_fein = ? AND public_domain = ?", (fein, domain)).rowcount
    for fein, host in p["fill"]:
        filled += conn.execute(
            "UPDATE fein_domain_map SET public_domain_host = ? "
            "WHERE employer_fein = ? AND public_domain_host IS NULL", (host, fein)).rowcount
    conn.commit()
    return cleared, filled


def main() -> None:
    init_logging("pd_backfill_from_scan")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv_path")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run report only)")
    ap.add_argument("--scan-ts", help="ISO timestamp the scan ran (default: CSV file mtime)")
    ap.add_argument("--workers", type=int, default=PD_BACKFILL_RECHECK_WORKERS,
                    help="threads for the live parked re-probe (default: config PD_BACKFILL_RECHECK_WORKERS)")
    args = ap.parse_args()

    scan_ts = (datetime.fromisoformat(args.scan_ts) if args.scan_ts
               else datetime.fromtimestamp(os.path.getmtime(args.csv_path)))
    if scan_ts.tzinfo is None:
        scan_ts = scan_ts.astimezone()
    rows = load_rows(args.csv_path)
    log.info("loaded %d rows from %s", len(rows), args.csv_path)

    conn = get_conn()
    try:
        fmap = fetch_map(conn, [r["employer_fein"].strip() for r in rows if r.get("employer_fein")])
        probed = fetch_probed(conn, [r["domain"].strip().lower() for r in rows])
        parked = sorted({r["domain"].strip().lower() for r in rows if r.get("final_verdict") == _CLEAR_VERDICT})
        if not args.apply:
            pd.PD_PROBE_RECORD_ENABLED = False   # dry-run: probe live but write no pd_probe_observation rows
        log.info("re-probing %d parked domains live (%d workers)", len(parked), args.workers)
        rechecked = recheck(parked, args.workers)
        p = plan(rows, fmap, probed, scan_ts, rechecked)
        report(p, scan_ts)
        if not args.apply:
            print("\nDRY RUN — nothing written. Re-run with --apply to write.")
            return
        cleared, filled = apply(conn, p)
        wrote = 0
        for i, (domain, obs) in enumerate(p["obs"], 1):
            wrote += backfill_probe(conn, domain, obs, scan_ts)
            if i % _BATCH == 0:
                conn.commit()
        conn.commit()
        log.info("applied: cleared=%d host_filled=%d obs_written=%d", cleared, filled, wrote)
        print(f"\nAPPLIED: cleared={cleared} host_filled={filled} obs_written={wrote}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
