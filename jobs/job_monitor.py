"""
jobs/job_monitor.py — Job monitoring pipeline orchestrator.

Phase 1 change: run() uses ThreadPoolExecutor for parallel company processing.
All other functions are identical to the original.

What changed in run():
  - Sequential for-loop replaced with ThreadPoolExecutor(max_workers=20)
  - Per-company logic extracted into _process_company() worker function
  - Stats accumulated thread-safely via threading.Lock
  - between_companies_delay() replaced by per-ATS semaphores
  - Progress printed as companies complete (unordered but timestamped)

What did NOT change:
  - All logic inside _process_company() is identical to the original loop body
  - All other functions: run_detect_ats, run_monitor_status, _build_alerts, etc.
  - All DB operations, filtering, digest generation
  - All imports
"""

import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from logger import get_logger, init_logging
from db.db import (
    init_db,
    get_all_monitored_companies,
    get_monitorable_companies,
    get_detection_queue,
    get_detection_queue_stats,
    job_url_exists,
    job_hash_exists,
    save_job_posting,
    get_new_postings_for_digest,
    mark_first_scan_complete,
    update_company_check,
    save_monitor_stats,
    get_monitor_stats,
    get_pipeline_reliability,
    mark_postings_digested,
    get_tracked_urls_for_company,
    increment_missing_days,
    reset_missing_days,
    reactivate_job,
)
from jobs.ats_detector import (
    detect_ats, needs_redetection, override_ats, get_ats_module,
)
from jobs.ats.registry import get_config
from jobs.job_filter import (
    filter_jobs, filter_jobs_title_only, is_us_location,
    is_fresh, make_legacy_content_hash,
)
from config import (
    JOB_MONITOR_REDETECT_DAYS,
    MONITOR_COVERAGE_ALERT,
    MONITOR_ATS_UNKNOWN_ALERT,
    MONITOR_RELIABILITY_ALERT,
    MONITOR_MATCH_RATE_LOW_ALERT,
    MONITOR_MATCH_RATE_HIGH_ALERT,
    MONITOR_MAX_WORKERS,
    MONITOR_PLATFORM_CONCURRENCY,
)

logger = get_logger(__name__)


def _record_cycle_start() -> None:
    """
    Write cycle:start to Redis after digest email is sent and jobs digested.

    Called immediately after mark_postings_digested() so the adaptive
    scheduler knows the daily cycle has officially begun. Imported lazily
    to avoid import loops — workers.scheduler is not loaded unless the
    scheduler is running.
    """
    try:
        from workers.scheduler import record_cycle_start
        ts = record_cycle_start()
        logger.info("cycle:start written (unix=%.0f)", ts)
    except Exception as exc:
        # Non-fatal — scheduler may not be running (e.g. daily batch mode)
        logger.warning(
            "Could not write cycle:start to Redis: %s "
            "(scheduler may not be running — this is OK in batch mode)",
            exc,
        )


# ─────────────────────────────────────────
# PER-ATS SEMAPHORES
# Built once at module load from config.
# Each semaphore limits how many threads can fetch from
# the same ATS domain simultaneously.
# ─────────────────────────────────────────
# Bounded wait for in-flight fullscans before building the digest.
# Companies actively scanning when --monitor-jobs starts may finish soon;
# waiting for them avoids missing their results in the digest.
_IN_FLIGHT_WAIT_S   = 5 * 60   # max wait: 5 minutes
_IN_FLIGHT_POLL_S   = 15        # check Redis every 15 seconds

_DEFAULT_CONCURRENCY = 10
_PLATFORM_SEMAPHORES = {
    platform: threading.Semaphore(limit)
    for platform, limit in MONITOR_PLATFORM_CONCURRENCY.items()
}
_DEFAULT_SEMAPHORE = threading.Semaphore(_DEFAULT_CONCURRENCY)


def _get_semaphore(platform):
    """Return the semaphore for this platform (or default)."""
    return _PLATFORM_SEMAPHORES.get(platform, _DEFAULT_SEMAPHORE)


# ─────────────────────────────────────────
# PER-COMPANY HELPERS (registry-driven)
# ─────────────────────────────────────────

def _parse_slug(platform, slug, config):
    """
    Parse the raw DB slug into the form each ATS module expects.

    Registry slug_type:
      "string" → pass slug as-is (str)
      "json"   → json.loads(slug); platform-specific defaults if parse fails

    Returns either a string (slug_type="string") or a dict (slug_type="json").
    """
    if config.get("slug_type") != "json":
        return slug
    try:
        slug_info = json.loads(slug)
        # Workday: ensure "path" key exists (older slugs may predate it)
        if platform == "workday" and "path" not in slug_info:
            slug_info["path"] = "careers"
        return slug_info
    except (json.JSONDecodeError, TypeError):
        defaults = {
            "workday":    {"slug": slug or "", "wd": "wd5", "path": "careers"},
            "oracle_hcm": {"slug": slug or "", "site": ""},
        }
        return defaults.get(platform, {})


def _should_fetch_detail(job, platform, config, slug_info=None):
    """
    Return True if fetch_job_detail() should be called for this job.

    Checks the registry has_detail flag and platform-specific preconditions
    (the job dict must contain the key the detail fetcher needs).
    """
    if not config.get("has_detail"):
        return False
    # Platforms that require a specific key in the job dict
    required_keys = {
        "icims":           "_base_url",
        "jobvite":         "_slug",
        "taleo":           "_contest_no",
        "smartrecruiters": "_company_slug",
        "workday":         "_external_path",
    }
    key = required_keys.get(platform)
    if key is not None:
        return bool(job.get(key))
    # sitemap: skip XML feeds (they already have all data in the feed)
    if platform == "sitemap":
        return bool(job.get("job_url")) and job.get("_feed_type") != "xml"
    # custom: only if detail config exists AND listing didn't already fill description
    if platform == "custom":
        return bool(
            slug_info and slug_info.get("detail") and not job.get("description")
        )
    # Default: fetch detail if a job URL is available
    return bool(job.get("job_url"))


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def _get_worker_missed_companies(companies: list) -> tuple:
    """
    Return (missed, in_flight_names) for the given company list.

    missed:          companies that background workers have NOT scanned within
                     the last 24 hours AND are not actively scanning right now.
                     These need a fallback re-fetch.

    in_flight_names: companies currently being scanned by a fullscan_worker.
                     They are excluded from `missed` (don't double-fetch) but
                     are also NOT considered "covered" yet — the scan is still
                     running.  Callers must track these separately so they are
                     not counted in the covered_by_workers coverage metric.

    Field checked: last_full_scan_at (written by on_fullscan_complete()).
    We do NOT check last_poll_at (adaptive scan) because the adaptive worker
    uses smart early exit and may not have scanned every page.  Only a
    completed fullscan guarantees the DB is comprehensive.

    Cycle boundary: 24 hours before now (rolling window).
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    from datetime import datetime as _dt, timedelta
    from config import CYCLE_START_HOUR, SEND_TIMEZONE
    from db.db import get_conn

    tz = ZoneInfo(SEND_TIMEZONE)
    now_dt = _dt.now(tz)

    # 24-hour rolling lookback: any scan within the last day counts as covered.
    # At 7:00 AM this equals yesterday-7-AM, giving overnight worker scans full
    # credit.  For manual mid-day runs it correctly credits scans from earlier
    # that same day without going all the way back to the previous morning.
    cycle_start_ts = (now_dt - timedelta(hours=24)).timestamp()

    company_names = [c["company"] for c in companies]

    # Take the Redis inflight snapshot FIRST so companies that start a fullscan
    # between the DB read and the Redis read cannot be misclassified as missed.
    inflight: set = set()
    try:
        from config import REDIS_INFLIGHT_FULLSCAN, REDIS_URL
        import redis as _redis_lib_jm
        # Only consider entries added within the last 2 hours.  The ZSET score
        # is the unix timestamp when the worker claimed the company.  Entries
        # older than 2 h come from workers that were killed without cleanup and
        # should not permanently exclude companies from the missed-jobs check.
        # Use a timeout-bound client so a Redis hang never blocks the monitor.
        _r_inflight = _redis_lib_jm.from_url(
            REDIS_URL, socket_timeout=5, socket_connect_timeout=3
        )
        try:
            stale_threshold = time.time() - 7200
            raw      = _r_inflight.zrangebyscore(REDIS_INFLIGHT_FULLSCAN, stale_threshold, "+inf")
            inflight = {(c.decode() if isinstance(c, bytes) else c) for c in (raw or [])}
            if inflight:
                logger.debug(
                    "_get_worker_missed_companies: %d companies in-flight, excluding from missed",
                    len(inflight),
                )
        finally:
            _r_inflight.close()
    except Exception as exc:
        logger.warning(
            "_get_worker_missed_companies: Redis unavailable for inflight exclusion "
            "(%s) — proceeding without exclusion (may do extra work)",
            exc,
        )

    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company,
                   EXTRACT(EPOCH FROM last_full_scan_at) AS last_full_scan_epoch
            FROM company_poll_stats
            WHERE company = ANY(%s)
        """, (company_names,)).fetchall()
    finally:
        conn.close()

    # last_full_scan_at: written by on_fullscan_complete() — exhaustive all-pages scan.
    # We do NOT use last_poll_at (adaptive scan) here because the adaptive worker uses
    # smart early exit and may not have seen every page.  Only a completed fullscan
    # guarantees the DB is comprehensive for this company's current board.
    scan_map = {r["company"]: (r["last_full_scan_epoch"] or 0) for r in rows}

    missed          = []
    in_flight_names = set()
    for company_row in companies:
        name      = company_row["company"]
        last_scan = scan_map.get(name, 0)
        if last_scan < cycle_start_ts:
            if name in inflight:
                # Active scan in progress — don't double-fetch, but also NOT
                # confirmed done yet.  Track separately for coverage accounting.
                in_flight_names.add(name)
            else:
                missed.append(company_row)

    return missed, in_flight_names


def run():
    """
    Main entry point for --monitor-jobs.

    Smart hybrid mode:
      - Companies already scanned by background workers (scan_worker /
        fullscan_worker) within the last 24 h are skipped — their results
        are already in the DB as status='new'.
      - Companies workers missed (crashed, backlog, never started) get a
        fallback re-fetch so the digest is never incomplete.
      - Digest is always generated from DB at the end regardless.

    Normal day (workers healthy):   0 re-fetches → email at ~7:02 AM
    Workers partially failed:       only missed companies re-fetched → still fast
    Workers completely down:        all companies re-fetched → email at ~7:30 AM
    """
    init_logging("monitor")
    start_time = time.time()
    logger.info("════════════════════════════════════════")
    logger.info("--monitor-jobs starting (smart hybrid mode, max_workers=%d)",
                MONITOR_MAX_WORKERS)

    init_db()

    companies = get_monitorable_companies()
    if not companies:
        logger.warning("No monitorable companies found")
        print("[INFO] No monitorable companies found.")
        print("[INFO] Run: python pipeline.py --import-prospects prospects.txt")
        print("[INFO] Then: python pipeline.py --detect-ats")
        return {}

    all_companies = get_all_monitored_companies()
    skipped = len(all_companies) - len(companies)
    if skipped > 0:
        logger.warning("%d companies skipped (unknown/unverified ATS)", skipped)
        print(f"[INFO] Skipping {skipped} company/companies with "
              f"unknown/unverified ATS — run --detect-ats to fix")

    # Capture scan horizon before the DB classification so any inflight scan
    # completing during _get_worker_missed_companies() is still credited below.
    _scan_horizon = time.time()

    # ── Split: covered by workers vs missed vs in-flight ─────────────────────
    missed, in_flight_names = _get_worker_missed_companies(companies)
    missed_names = {c["company"] for c in missed}   # O(1) lookups
    # covered = confirmed done (last_full_scan_at within 24 h)
    # in-flight companies are NOT in missed and NOT in covered — they are
    # actively scanning and not yet confirmed complete.  Counting them in
    # covered_by_workers inflates the metric and can suppress the coverage alert.
    covered = [
        c for c in companies
        if c["company"] not in missed_names
        and c["company"] not in in_flight_names
    ]

    logger.info(
        "Loaded %d monitorable companies (%d total in DB) | "
        "worker-covered=%d  in-flight=%d  missed=%d",
        len(companies), len(all_companies), len(covered),
        len(in_flight_names), len(missed),
    )

    print(f"\n{'='*55}")
    print(f"[INFO] Job Monitor — {datetime.now().strftime('%B %d, %Y')}")
    print(f"[INFO] {len(companies)} companies total | "
          f"{len(covered)} covered by workers | "
          f"{len(in_flight_names)} in-flight | "
          f"{len(missed)} need fallback fetch")
    if missed:
        print(f"[INFO] Fallback re-fetching: "
              f"{', '.join(c['company'] for c in missed[:5])}"
              f"{'...' if len(missed) > 5 else ''}")
    print(f"{'='*55}\n")

    # ── Shared stats — accumulated thread-safely via Lock ──
    stats = {
        "companies_monitored":    len(companies),      # fixed total denominator (covered + in_flight + missed)
        "covered_by_workers":     len(covered),        # confirmed-done by workers
        "in_flight":              len(in_flight_names),# scanning now — not confirmed yet
        "fallback_scanned":       0,              # fallback companies whose ATS was successfully queried
                                                  # (regardless of whether jobs were found)
        "companies_with_results": 0,              # subset of fallback_scanned that returned ≥1 job
        "companies_unknown_ats":  0,
        "api_failures":           0,
        "total_jobs_fetched":     0,
        "new_jobs_found":         0,
        "jobs_matched_filters":   0,
        "api_failure_list":       [],
    }
    stats_lock = threading.Lock()

    # ── Fallback re-fetch (only for companies workers missed) ─────────────────
    if missed:
        logger.info("Fallback re-fetching %d companies workers missed", len(missed))
        with ThreadPoolExecutor(max_workers=MONITOR_MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    _process_company, company_row, i + 1, len(missed)
                ): company_row["company"]
                for i, company_row in enumerate(missed)
            }

            for future in as_completed(futures):
                company = futures[future]
                try:
                    company_stats = future.result()
                except Exception as e:
                    logger.error("Unhandled error in fallback worker for %r: %s",
                                 company, e, exc_info=True)
                    company_stats = {
                        "monitored": 1, "with_results": 0,
                        "unknown_ats": 0, "failed": 1,
                        "fetched": 0, "matched": 0, "new": 0,
                        "failure_name": company,
                    }

                with stats_lock:
                    stats["fallback_scanned"]       += company_stats.get("fallback_scanned", 0)
                    stats["companies_with_results"] += company_stats.get("with_results",   0)
                    stats["companies_unknown_ats"]  += company_stats.get("unknown_ats",    0)
                    stats["api_failures"]           += company_stats.get("failed",         0)
                    stats["total_jobs_fetched"]     += company_stats.get("fetched",        0)
                    stats["jobs_matched_filters"]   += company_stats.get("matched",        0)
                    stats["new_jobs_found"]         += company_stats.get("new",            0)
                    if company_stats.get("failure_name"):
                        stats["api_failure_list"].append(
                            company_stats["failure_name"]
                        )
    else:
        logger.info("All %d companies covered by workers — skipping re-fetch",
                    len(covered))

    # ── Bounded wait for in-flight scans ─────────────────────────────────────
    # Companies that were mid-scan when --monitor-jobs started may finish before
    # the digest is built.  Poll inflight:fullscan for up to _IN_FLIGHT_WAIT_S
    # seconds so their results are included rather than silently skipped.
    if in_flight_names:
        _remaining_inflight = set(in_flight_names)
        _wait_start = time.time()   # elapsed-time gate for the polling loop below
        logger.info(
            "Waiting up to %ds for %d in-flight scan(s): %s%s",
            _IN_FLIGHT_WAIT_S,
            len(_remaining_inflight),
            ", ".join(sorted(_remaining_inflight)[:5]),
            "..." if len(_remaining_inflight) > 5 else "",
        )
        print(f"[INFO] Waiting up to {_IN_FLIGHT_WAIT_S}s for "
              f"{len(_remaining_inflight)} in-flight scan(s) to finish...")

        try:
            from config import REDIS_INFLIGHT_FULLSCAN, REDIS_URL
            import redis as _redis_lib_wait
            _r_wait = _redis_lib_wait.from_url(
                REDIS_URL, socket_timeout=5, socket_connect_timeout=3
            )

            while _remaining_inflight and (time.time() - _wait_start) < _IN_FLIGHT_WAIT_S:
                _stale_ts    = time.time() - 7200
                _active_raw  = _r_wait.zrangebyscore(
                    REDIS_INFLIGHT_FULLSCAN, _stale_ts, "+inf"
                )
                _still_active = {
                    c.decode() if isinstance(c, bytes) else c
                    for c in (_active_raw or [])
                }
                _newly_done = _remaining_inflight - _still_active
                if _newly_done:
                    # Verify via DB before removing from _remaining_inflight.
                    # Defer removal so a transient DB failure keeps these companies
                    # in the pending set and retries them on the next poll cycle.
                    # A scan that failed or was aborted may also disappear from
                    # the inflight ZSET, so membership disappearance alone is not
                    # proof of completion.  Use _scan_horizon (captured before the
                    # fallback fetch) so scans that finished during fallback are
                    # also credited correctly.
                    _confirmed_done: set = set()
                    _db_ok = False
                    try:
                        from db.db import get_conn as _get_conn_wait
                        _vconn = _get_conn_wait()
                        try:
                            _vrows = _vconn.execute(
                                "SELECT company FROM company_poll_stats "
                                "WHERE company = ANY(%s) "
                                "AND EXTRACT(EPOCH FROM last_full_scan_at) "
                                "    >= %s::double precision",
                                (list(_newly_done), _scan_horizon),
                            ).fetchall()
                            _confirmed_done = {r["company"] for r in _vrows}
                            _db_ok = True
                        finally:
                            _vconn.rollback()   # end implicit transaction before returning to pool
                            _vconn.close()
                    except Exception as _db_ver_err:  # noqa: BLE001
                        logger.warning(
                            "In-flight wait: DB verification failed (%s) — "
                            "keeping %d companies pending for retry",
                            _db_ver_err, len(_newly_done),
                        )

                    if _db_ok:
                        # Only remove from remaining after successful DB check
                        _remaining_inflight -= _newly_done
                        _unconfirmed = _newly_done - _confirmed_done
                        if _confirmed_done:
                            with stats_lock:
                                stats["covered_by_workers"] += len(_confirmed_done)
                                stats["in_flight"]           -= len(_confirmed_done)
                            logger.info(
                                "In-flight scans confirmed complete: %s (%d still waiting)",
                                ", ".join(sorted(_confirmed_done)), len(_remaining_inflight),
                            )
                            _names_str = ', '.join(sorted(_confirmed_done))
                            print(f"[INFO] {len(_confirmed_done)} scan(s) confirmed done: "
                                  f"{_names_str[:120]}"
                                  f"{'...' if len(_names_str) > 120 else ''}")
                        if _unconfirmed:
                            logger.warning(
                                "In-flight scans left ZSET without DB update "
                                "(possible failures): %s",
                                ", ".join(sorted(_unconfirmed)),
                            )
                            with stats_lock:
                                stats["in_flight"] -= len(_unconfirmed)

                if _remaining_inflight:
                    time.sleep(_IN_FLIGHT_POLL_S)

        except Exception as _wait_exc:
            logger.warning(
                "In-flight wait: Redis unavailable (%s) — proceeding with "
                "%d companies still counted as in-flight",
                _wait_exc, len(_remaining_inflight),
            )

        if _remaining_inflight:
            logger.info(
                "In-flight wait expired: %d scan(s) still active — "
                "their results may miss this digest",
                len(_remaining_inflight),
            )
            print(f"[INFO] {len(_remaining_inflight)} scan(s) still running "
                  f"after {int(time.time() - _wait_start)}s wait — may miss digest.")
        else:
            _elapsed = int(time.time() - _wait_start)
            logger.info("All in-flight scans completed within %ds.", _elapsed)
            print(f"[INFO] All in-flight scans completed in {_elapsed}s.")

    # ── Generate PDF digest (sequential — happens once) ────
    new_postings  = get_new_postings_for_digest()
    pdf_generated = False
    email_sent    = False
    duration      = int(time.time() - start_time)

    logger.info(
        "Run complete in %ds | companies=%d unknown_ats=%d failures=%d | "
        "fetched=%d matched=%d new=%d",
        duration,
        stats["companies_monitored"],
        stats["companies_unknown_ats"],
        stats["api_failures"],
        stats["total_jobs_fetched"],
        stats["jobs_matched_filters"],
        stats["new_jobs_found"],
    )

    print(f"\n{'='*55}")
    print(f"[INFO] Run complete in {duration}s "
          f"(was ~{duration * MONITOR_MAX_WORKERS // 2}s sequential estimate)")
    print(f"[INFO] Companies: {stats['companies_monitored']} monitored | "
          f"{stats['companies_unknown_ats']} unknown ATS | "
          f"{stats['api_failures']} failures")
    print(f"[INFO] Jobs: {stats['total_jobs_fetched']} fetched | "
          f"{stats['jobs_matched_filters']} matched | "
          f"{stats['new_jobs_found']} new")

    if new_postings:
        logger.info("Generating PDF digest (%d new jobs)", len(new_postings))
        print(f"\n[INFO] Generating PDF digest ({len(new_postings)} new jobs)...")
        try:
            from outreach.report_templates.monitor_report import (
                build_monitor_report
            )
            alerts        = _build_alerts(stats, len(companies))
            result        = build_monitor_report(new_postings, stats, alerts)
            pdf_generated = result.get("pdf_generated", False)
            email_sent    = result.get("email_sent", False)
            logger.info("PDF digest: pdf=%s email=%s",
                        pdf_generated, email_sent)
            if email_sent:
                mark_postings_digested()
                logger.info("Marked %d posting(s) as digested",
                            len(new_postings))
                _record_cycle_start()
        except Exception as e:
            logger.error("PDF generation failed: %s", e, exc_info=True)
            print(f"[ERROR] PDF generation failed: {e}")
            print("[INFO] Sending plain text digest instead...")
            email_sent = _send_text_fallback(new_postings)
            if email_sent:
                mark_postings_digested()
                _record_cycle_start()
    else:
        logger.info("No new jobs — sending no-jobs email")
        print(f"\n[INFO] No new matching jobs today.")
        alerts     = _build_alerts(stats, len(companies))
        email_sent = _send_no_jobs_email(alerts=alerts)

    final_stats = {
        **stats,
        "run_duration_seconds": duration,
        "pdf_generated":        int(pdf_generated),
        "email_sent":           int(email_sent),
    }
    save_monitor_stats(final_stats)
    _print_metric_alerts(stats, len(companies))

    # Flush any pending api_health writes before exit
    try:
        from db.api_health import flush as flush_api_health
        flush_api_health()
    except Exception:
        logger.error("flush_api_health failed — some health records may be lost",
                     exc_info=True)

    logger.info("════ --monitor-jobs finished ════")
    return final_stats


# ─────────────────────────────────────────
# WORKER — one company per thread call
# Logic is identical to the original sequential loop body.
# Only difference: uses semaphore instead of between_companies_delay().
# ─────────────────────────────────────────
_REDETECT_SEMAPHORE = threading.Semaphore(1)

def _process_company(company_row, position, total):
    """
    Process one company: fetch jobs, filter, save new ones.
    Called by ThreadPoolExecutor — one call per company.

    Returns dict of per-company stats for aggregation in run().
    Never raises — all exceptions caught and returned as failure.
    """
    company  = company_row["company"]
    platform = company_row.get("ats_platform", "unknown")
    slug     = company_row.get("ats_slug")

    result = {
        "monitored":        1,
        "fallback_scanned": 0,   # set to 1 when ATS fetch succeeds (0 or more jobs)
        "with_results":     0,   # set to 1 when fetch returns ≥1 job
        "unknown_ats":      0,
        "failed":           0,
        "fetched":          0,
        "matched":          0,
        "new":              0,
        "failure_name":     None,
    }

    logger.info("── [%d/%d] %r  platform=%s",
                position, total, company, platform)

    # ── ATS re-detection intentionally disabled ───────────
    # detect_ats() is unreliable and overwrites working configs with wrong
    # results when it misidentifies a company's ATS tenant (e.g. assigned
    # Gartner's Workday slug to SAP America after SF timeouts pushed
    # consecutive_empty_days to 14+). Run --detect-ats manually only.
    # Logging still fires so stale companies are visible in logs.
    if needs_redetection(company_row, JOB_MONITOR_REDETECT_DAYS):
        domain = company_row.get("domain")
        logger.warning(
            "Re-detection needed for %r (domain=%s, empty_days=%d) "
            "— skipped (inline re-detection disabled, run --detect-ats manually)",
            company, domain,
            company_row.get("consecutive_empty_days", 0),
        )

    if platform == "unknown" or not slug:
        logger.warning("Skipping %r — unknown ATS", company)
        result["unknown_ats"] = 1
        result["monitored"]   = 1
        print(f"  [{position}/{total}] {company} — [SKIP] Unknown ATS")
        return result

    # ── Get ATS module + registry config ─────────────────
    config     = get_config(platform)
    ats_module = get_ats_module(platform)
    if not ats_module:
        logger.error("No ATS module for platform=%s (%r)",
                     platform, company)
        result["failed"]       = 1
        result["failure_name"] = company
        return result

    # ── Acquire per-ATS semaphore ─────────────────────────
    # Limits concurrent requests to the same ATS domain.
    # Replaces between_companies_delay() from the sequential version.
    sem = _get_semaphore(platform)

    with sem:
        # ── Fetch jobs ────────────────────────────────────
        try:
            slug_info = _parse_slug(platform, slug, config)
            # custom: validate that the slug parsed to a usable dict
            if platform == "custom" and not isinstance(slug_info, dict):
                logger.error("custom: invalid slug JSON for %r", company)
                result["failed"]       = 1
                result["failure_name"] = company
                return result
            logger.debug("%s fetch: %r slug=%s", platform, company, slug_info)
            raw_jobs = ats_module.fetch_jobs(slug_info, company)
        except Exception as e:
            logger.error("API fetch failed for %r (platform=%s): %s",
                         company, platform, e, exc_info=True)
            print(f"  [{position}/{total}] {company} — [ERROR] {e}")
            result["failed"]       = 1
            result["failure_name"] = company
            return result

    # ── Post-fetch processing (outside listing semaphore) ──
    # NOTE: fetch_job_detail calls below re-acquire the semaphore individually
    # to throttle detail HTTP requests with the same per-platform limit.
    logger.debug("Fetched %d raw jobs for %r", len(raw_jobs), company)

    # Filter out entries missing job_url before processing
    initial_count = len(raw_jobs)
    raw_jobs = [job for job in raw_jobs if job.get("job_url")]
    dropped_count = initial_count - len(raw_jobs)
    if dropped_count > 0:
        logger.warning("Dropped %d jobs missing job_url for %r",
                      dropped_count, company)

    result["fetched"]          = len(raw_jobs)
    result["fallback_scanned"] = 1  # ATS responded — counts as scanned even if 0 jobs

    # URL presence tracking (DB reads — fast, no HTTP)
    fetched_urls = {job["job_url"] for job in raw_jobs}
    tracked      = get_tracked_urls_for_company(company)
    present_ids  = [tracked[url] for url in fetched_urls if url in tracked]
    missing_ids  = [tracked[url] for url in tracked
                    if url not in fetched_urls]
    if present_ids:
        reset_missing_days(present_ids)
    if missing_ids:
        increment_missing_days(missing_ids)
        logger.debug("Missing from scan: %d jobs for %r",
                     len(missing_ids), company)

    if not raw_jobs:
        logger.info("No jobs returned for %r", company)
        update_company_check(company, found_jobs=False)
        print(f"  [{position}/{total}] {company} — 0 jobs")
        return result

    result["with_results"] = 1
    update_company_check(company, found_jobs=True)

    # ── Filter ────────────────────────────────────────────
    # Registry listing_filter drives which filter to apply:
    #   "full"       → filter_jobs()             (title + location at listing)
    #   "title_only" → filter_jobs_title_only()  (location deferred to detail)
    if config.get("listing_filter") == "title_only":
        matched = filter_jobs_title_only(raw_jobs)
    else:
        matched = filter_jobs(raw_jobs)
    logger.debug("Filter: %d raw → %d matched for %r",
                 len(raw_jobs), len(matched), company)
    result["matched"] = len(matched)

    is_first_scan = company_row.get("first_scanned_at") is None
    if is_first_scan:
        logger.info("First scan for %r — all jobs marked pre_existing",
                    company)

    # ── Save new jobs ─────────────────────────────────────
    new_count = 0
    # slug_info is already parsed above — used by custom detail fetcher
    # and any other platform that needs it inside the per-job loop.
    slug_info_cached = slug_info if isinstance(slug_info, dict) else None

    for job in matched:
        exists, is_filled = job_url_exists(job["job_url"])
        if exists:
            if is_filled:
                reactivate_job(job["job_url"], job)
                logger.info("REACTIVATED: %r | %s",
                            company, job.get("title"))
            else:
                logger.debug("Duplicate URL: %s", job["job_url"])
            continue

        if job.get("content_hash") and \
                job_hash_exists(job.get("content_hash"),
                                job.get("content_hash_legacy")):
            logger.debug("Duplicate content_hash for %r", company)
            continue

        # ── Workday: early detail fetch + location gate ───────────────────────
        # Workday listing locations are too vague ("2 Locations", bare "London").
        # Fetch detail BEFORE freshness/pre_existing checks so non-US jobs are
        # never written to the DB at all (even as pre_existing).
        # All other has_detail platforms fetch detail for new jobs only (below).
        #
        # Location waterfall (most → least reliable):
        #   Tier 1: jobRequisitionLocation.country.alpha2Code ("US", "IN", "DE")
        #           → stored as _country_code by fetch_job_detail(); definitive.
        #   Tier 3: is_us_location() on descriptor-embedded location string
        #           → fallback when alpha2Code absent (older/custom tenants).
        if platform == "workday" and job.get("_external_path"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error(
                        "Workday fetch_job_detail failed %s/%s: %s",
                        company, job.get("job_id"), e, exc_info=True,
                    )
            alpha2 = (job.get("_country_code") or "").upper()
            if alpha2:
                # Tier 1: structured alpha-2 code — no text parsing needed
                if alpha2 != "US":
                    logger.debug(
                        "Workday non-US dropped (alpha2): %r | %s | country=%s",
                        company, job.get("title"), alpha2,
                    )
                    continue
            elif not is_us_location(job.get("location", "")):
                # Tier 3: alpha2Code absent → fall back to descriptor-embedded text
                logger.debug(
                    "Workday non-US dropped (text): %r | %s | %s",
                    company, job.get("title"), job.get("location"),
                )
                continue

        # ── Alpha-2 listing-level gate ────────────────────────────────────────
        # Platforms with country_source="alpha2" (SmartRecruiters) embed an ISO
        # alpha-2 code in every listing.  Drop non-US jobs before the detail
        # fetch — saves one HTTP call per non-US job.
        if config.get("country_source") == "alpha2":
            code = (job.get("_country_code") or "").lower()
            if code and code != "us":
                logger.debug(
                    "Listing-level alpha2 country dropped: %r | %s | country=%s",
                    company, job.get("title"), code,
                )
                continue

        if platform != "greenhouse" and not is_fresh(job, platform):
            logger.debug("Pre-existing (stale): %r | %s",
                         company, job.get("title"))
            save_job_posting(job, status="pre_existing")
            continue

        if is_first_scan:
            save_job_posting(job, status="pre_existing")
            continue

        # ── Detail fetch for new jobs ─────────────────────────────────────────
        # Registry has_detail + _should_fetch_detail() drive when to fetch.
        # Each call re-acquires the semaphore to throttle detail HTTP requests.
        # Workday excluded — detail was fetched in the early-fetch path above.
        # Custom uses a different signature (passes slug_info_cached).
        if platform == "custom":
            if _should_fetch_detail(job, platform, config, slug_info_cached):
                with sem:
                    try:
                        job = ats_module.fetch_job_detail(job, slug_info_cached)
                        logger.debug(
                            "Custom detail fetched for %r/%s desc_len=%d",
                            company, job.get("job_id"),
                            len(job.get("description", "") or ""),
                        )
                    except Exception as e:
                        logger.error(
                            "Custom fetch_job_detail failed %s/%s: %s",
                            company, job.get("job_id"), e, exc_info=True,
                        )
        elif platform != "workday" and _should_fetch_detail(job, platform, config):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error(
                        "%s fetch_job_detail failed %s/%s: %s",
                        platform, company, job.get("job_id"), e, exc_info=True,
                    )

        # ── Post-detail location gate ─────────────────────────────────────────
        # Applies to all platforms where detail fills/refines location
        # (registry has_detail=True), except Workday (gated above).
        #
        # country_source="alpha2"     → _country_code set by detail parse (iCIMS
        #                               JSON-LD); listing gate caught SmartRecruiters
        # country_source="text"       → location string via is_us_location()
        # country_source="descriptor" → full country name embedded by detail
        #                               normaliser; is_us_location() Signal 4 fires
        if config.get("has_detail") and platform != "workday":
            code = (job.get("_country_code") or "").upper()
            if code and code != "US":
                logger.debug(
                    "Post-detail country code dropped: %r | %s | country=%s",
                    company, job.get("title"), code,
                )
                continue
            loc = job.get("location", "")
            if loc and not is_us_location(loc):
                logger.debug(
                    "Post-detail location dropped: %r | %s | %s",
                    company, job.get("title"), loc,
                )
                continue

        if save_job_posting(job, status="new"):
            new_count += 1
            logger.info("NEW JOB: %r | %s | %s",
                        company, job.get("title"), job.get("location"))

    result["new"] = new_count

    logger.info("Done %r: fetched=%d matched=%d new=%d",
                company, len(raw_jobs), len(matched), new_count)
    print(f"  [{position}/{total}] {company} — "
          f"{len(raw_jobs)} fetched → {len(matched)} matched → {new_count} new")

    if is_first_scan:
        # first_scanned_at was already set atomically in update_company_check()
        # above — no separate DB call needed.
        logger.info("First scan complete for %r", company)
        print(f"  [{position}/{total}] {company} — "
              f"first scan complete (existing jobs pre_existing)")

    return result


# ─────────────────────────────────────────
# ALL FUNCTIONS BELOW ARE IDENTICAL TO ORIGINAL
# ─────────────────────────────────────────

def _build_alerts(stats, total_companies):
    """Build list of alert messages based on metric thresholds."""
    alerts = []

    if total_companies > 0:
        # covered_by_workers: confirmed done by background workers (data in DB).
        # fallback_scanned: of the missed companies, those whose ATS fetch
        #   completed (0 or more jobs).  Use this (not companies_with_results)
        #   for coverage — a 0-job result is still a successful scan.
        # in_flight companies are NOT included here — they are still scanning
        # and cannot be credited until confirmed in the DB.
        covered_count = stats.get("covered_by_workers", 0)
        companies_with_data = covered_count + stats.get("fallback_scanned", 0)
        coverage = companies_with_data / total_companies
        if coverage < MONITOR_COVERAGE_ALERT:
            pct = int(coverage * 100)
            logger.warning("Coverage alert: %d%%", pct)
            alerts.append({
                "level":   "warning",
                "message": f"Coverage {pct}% — only "
                           f"{companies_with_data}/"
                           f"{total_companies} companies have data",
            })

        unknown_rate = stats["companies_unknown_ats"] / total_companies
        if unknown_rate > MONITOR_ATS_UNKNOWN_ALERT:
            logger.warning("Unknown ATS alert: %d companies",
                           stats["companies_unknown_ats"])
            alerts.append({
                "level":   "warning",
                "message": f"{stats['companies_unknown_ats']} companies "
                           f"have unknown ATS — run --detect-ats",
            })

    if stats["api_failures"] > 0:
        logger.warning("API failures: %s", stats["api_failure_list"])
        names = ", ".join(stats["api_failure_list"][:5])
        extra = (f" (+{len(stats['api_failure_list'])-5} more)"
                 if len(stats["api_failure_list"]) > 5 else "")
        alerts.append({
            "level":   "error",
            "message": f"API failures: {names}{extra}",
        })

    if stats["total_jobs_fetched"] > 0:
        match_rate = (stats["jobs_matched_filters"] /
                      stats["total_jobs_fetched"])
        if match_rate < MONITOR_MATCH_RATE_LOW_ALERT:
            logger.warning("Match rate low: %.1f%%", match_rate * 100)
            alerts.append({
                "level":   "warning",
                "message": f"Filter match rate {int(match_rate*100)}% "
                           f"— filters may be too strict",
            })
        elif match_rate > MONITOR_MATCH_RATE_HIGH_ALERT:
            logger.info("Match rate high: %.1f%%", match_rate * 100)
            alerts.append({
                "level":   "info",
                "message": f"Filter match rate {int(match_rate*100)}% "
                           f"— consider tightening filters",
            })

    return alerts


def _print_metric_alerts(stats, total_companies):
    """Print metric alerts to console."""
    alerts = _build_alerts(stats, total_companies)
    if alerts:
        print(f"\n{'='*55}")
        print("[INFO] METRIC ALERTS:")
        for alert in alerts:
            level = alert["level"].upper()
            print(f"  [{level}] {alert['message']}")


def _send_no_jobs_email(alerts=None):
    """Send brief email when no new jobs found today."""
    from outreach.report_templates.base import send_report_email
    try:
        date_str = datetime.now().strftime("%B %-d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    alerts_html = ""
    if alerts:
        import html as _html
        rows = ""
        for a in alerts:
            level       = a["level"]
            msg         = a["message"]
            escaped_msg = _html.escape(msg, quote=True)
            colour  = ("#dc2626" if level == "error"
                       else "#d97706" if level == "warning"
                       else "#2563eb")
            icon    = "🔴" if level == "error" else "⚠️" if level == "warning" else "ℹ️"
            rows += (
                f"<tr><td style='padding:6px 12px;color:{colour};'>"
                f"{icon} {escaped_msg}</td></tr>"
            )
        alerts_html = f"""
        <h3 style="color:#1e293b;margin-top:24px;">Pipeline Alerts</h3>
        <table style="border-collapse:collapse;width:100%;
                      background:#fef9f0;border-radius:6px;">
          {rows}
        </table>"""

    alert_subject = " ⚠️" if alerts else ""
    subject = (
        f"[Digest] Job Digest · {date_str} · "
        f"No new jobs today{alert_subject}"
    )
    html = f"""
    <html><body style="font-family:sans-serif;padding:24px;">
      <h2>No New Jobs Today</h2>
      <p style="color:#64748b;">
        No new job postings matched your profile on {date_str}.
      </p>
      <p style="color:#64748b;">
        This is normal — check again tomorrow.
      </p>
      {alerts_html}
    </body></html>
    """
    return send_report_email(subject, html)


def _send_text_fallback(postings):
    """Send plain text email when PDF generation fails."""
    import html as html_lib
    from outreach.report_templates.base import send_report_email
    try:
        date_str = datetime.now().strftime("%B %-d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    subject  = f"[Digest] Job Digest · {date_str} · {len(postings)} new jobs (text)"
    lines    = [f"<h2>Job Digest — {date_str}</h2>",
                f"<p>{len(postings)} new jobs matching your profile:</p>",
                "<ul>"]
    for job in postings:
        company  = html_lib.escape(str(job.get("company", "")))
        title    = html_lib.escape(str(job.get("title", "")))
        location = html_lib.escape(str(job.get("location", "")))
        raw_url  = str(job.get("job_url", ""))
        from urllib.parse import urlparse
        parsed   = urlparse(raw_url)
        safe_url = html_lib.escape(raw_url) if parsed.scheme in ("http", "https") else "#"
        lines.append(
            f"<li><strong>{company}</strong> — "
            f"{title} ({location})<br>"
            f"<a href='{safe_url}'>{html_lib.escape(raw_url)}</a></li>"
        )
    lines.append("</ul>")
    html = "\n".join(lines)
    return send_report_email(subject, html)


def run_detect_ats(company=None, override_platform=None,
                   override_slug=None, batch=False):
    """
    Run ATS detection. Identical to original.
    """
    init_logging("detect")
    logger.info("════════════════════════════════════════")
    logger.info("--detect-ats starting: company=%r batch=%s",
                company, batch)

    from datetime import datetime
    from outreach.report_templates.detection_report import build_detection_report
    from config import DETECT_ATS_BATCH_SIZE

    init_db()
    companies = get_all_monitored_companies()

    if not companies:
        logger.warning("No companies in DB")
        print("[INFO] No companies found. Run --import-prospects first.")
        return

    if company and override_platform and override_slug:
        override_ats(company.strip(), override_platform, override_slug)
        return

    try:
        date_str = datetime.now().strftime("%B %d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    if company:
        company_normalized = company.strip()
        matches = [c for c in companies
                   if c["company"] == company_normalized]
        if not matches:
            logger.error("Company not found: %r", company_normalized)
            print(f"[ERROR] '{company}' not found.")
            return

        domain = matches[0].get("domain") if matches else None
        result = detect_ats(company_normalized, domain=domain)
        logger.info("Result: %s", result)
        build_detection_report([result], date_str)
        return

    if batch:
        to_detect = get_detection_queue(batch_size=DETECT_ATS_BATCH_SIZE)
        if not to_detect:
            logger.info("Detection queue empty")
            print("[OK] No companies pending detection.")
            _print_detection_queue_status()
            return

        logger.info("Batch: %d companies", len(to_detect))
        print(f"[INFO] Detecting {len(to_detect)} companies...\n")

        results = []
        for i, company_row in enumerate(to_detect, 1):
            comp   = company_row["company"]
            domain = company_row.get("domain")
            print(f"[{i}/{len(to_detect)}] {comp}")
            result = detect_ats(comp, domain=domain)
            results.append(result)

        logger.info("Batch complete: %d results", len(results))
        _print_detection_queue_status()
        if results:
            build_detection_report(results, date_str)
        logger.info("════ --detect-ats (batch) finished ════")
        return

    to_detect = [c for c in companies if needs_redetection(c)]
    if not to_detect:
        print("[OK] All companies have ATS detected.")
        _print_detection_queue_status()
        return

    print(f"[INFO] Detecting {len(to_detect)} companies...\n")

    results = []
    for i, company_row in enumerate(to_detect, 1):
        comp   = company_row["company"]
        domain = company_row.get("domain")
        print(f"[{i}/{len(to_detect)}] {comp}")
        result = detect_ats(comp, domain=domain)
        results.append(result)

    _print_detection_queue_status()
    if results:
        build_detection_report(results, date_str)
    logger.info("════ --detect-ats finished ════")


def _print_detection_queue_status():
    """Print detection queue status summary."""
    try:
        stats = get_detection_queue_stats()
        p1 = stats.get("priority1_new",          0) or 0
        p2 = stats.get("priority2_quiet",         0) or 0
        p3 = stats.get("priority3_unknown",       0) or 0
        p4 = stats.get("priority4_custom_nocurl", 0) or 0
        total = p1 + p2 + p3 + p4

        if total > 0:
            print(f"\n[INFO] Detection queue ({total} companies pending):")
            if p1: print(f"  Priority 1 (new):          {p1}")
            if p2: print(f"  Priority 2 (14+ empty):    {p2}")
            if p3: print(f"  Priority 3 (unknown):      {p3}")
            if p4: print(f"  Priority 4 (custom/retry): {p4}")
        else:
            print("[OK] Detection queue empty")
    except Exception:
        logger.debug("Detection queue status unavailable", exc_info=True)


def run_monitor_status():
    """Show monitoring status summary. Identical to original."""
    init_logging("monitor")
    init_db()
    companies     = get_all_monitored_companies()
    stats_history = get_monitor_stats(7)
    reliability   = get_pipeline_reliability(7)

    total         = len(companies)
    known         = sum(1 for c in companies
                        if c.get("ats_platform") not in ("unknown", None))
    unknown       = total - known
    never_scanned = sum(1 for c in companies
                        if not c.get("first_scanned_at"))

    logger.info("Monitor status: total=%d known=%d unknown=%d",
                total, known, unknown)

    print(f"\n{'='*55}")
    print("[INFO] Job Monitor Status")
    print(f"{'='*55}")
    print(f"\nCompanies:     {total}")
    print(f"ATS detected:  {known} ({int(known/total*100) if total else 0}%)")
    print(f"ATS unknown:   {unknown}")
    print(f"Never scanned: {never_scanned}")

    if stats_history:
        latest = stats_history[0]
        print(f"\nLast run ({latest['date']}):")
        print(f"  Fetched:  {latest['total_jobs_fetched']} jobs")
        print(f"  Matched:  {latest['jobs_matched_filters']} jobs")
        print(f"  New:      {latest['new_jobs_found']} jobs")
        print(f"  Duration: {latest['run_duration_seconds']}s")
        print(f"  PDF sent: {'Yes' if latest['pdf_generated'] else 'No'}")

    print(f"\n7-day reliability: {int(reliability*100)}%")
    print(f"{'='*55}")


# Diagnostics functions — identical to original
def run_diagnostics():
    """Print open custom ATS diagnostics."""
    init_logging("diagnostics")
    init_db()

    from db.custom_ats_diagnostics import (
        get_open_diagnostics, get_diagnostic_summary,
        get_raw_curl_for_company
    )

    summary = get_diagnostic_summary()

    print(f"\n{'='*60}")
    print("  Custom ATS Diagnostics")
    print(f"{'='*60}\n")

    if not summary:
        print("[OK] No open diagnostics.\n")
        return

    total = sum(s["count"] for s in summary)
    print(f"  {total} open issue(s):\n")
    for row in summary:
        print(f"  [{row['severity'].upper()}] "
              f"{row['count']} issue(s) — "
              f"{row['companies']} company/companies")

    issues = get_open_diagnostics(limit=50)
    current_severity = None
    for issue in issues:
        if issue["severity"] != current_severity:
            current_severity = issue["severity"]
            print(f"\n── {current_severity.upper()} ──")
        print(f"\n  [{issue['id']}] {issue['company']}")
        print(f"       Step:  {issue['step']}")
        print(f"       Since: {issue['created_at'][:10]}")
        if issue['notes']:
            print(f"       Notes: {issue['notes'][:120]}")
        print(f"       Resolve: python pipeline.py "
              f"--resolve-diagnostic {issue['id']}")

    print(f"\n{'='*60}")


def run_resolve_diagnostic(diagnostic_id=None, company=None):
    """Mark diagnostic(s) as resolved."""
    init_db()
    from db.custom_ats_diagnostics import (
        resolve_diagnostic, resolve_all_for_company
    )
    if company:
        count = resolve_all_for_company(company)
        print(f"[OK] Resolved {count} diagnostic(s) for {company}")
    elif diagnostic_id is not None:
        try:
            diagnostic_id = int(diagnostic_id)
        except (TypeError, ValueError):
            print("[ERROR] Diagnostic ID must be an integer.")
            return
        success = resolve_diagnostic(diagnostic_id)
        if success:
            print(f"[OK] Resolved diagnostic #{diagnostic_id}")
        else:
            print(f"[ERROR] Could not resolve #{diagnostic_id}")
    else:
        print("[ERROR] Provide --resolve-diagnostic <id> or --company <name>")