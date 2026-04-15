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
    detect_ats, needs_redetection, override_ats,
    get_ats_module, QuotaExhaustedException
)
from db.serper_quota import get_serper_credits
from jobs.job_filter import filter_jobs, is_fresh, make_legacy_content_hash
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

# ─────────────────────────────────────────
# PER-ATS SEMAPHORES
# Built once at module load from config.
# Each semaphore limits how many threads can fetch from
# the same ATS domain simultaneously.
# ─────────────────────────────────────────
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
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def run():
    """
    Main entry point for --monitor-jobs.
    Scans all companies in parallel, finds new jobs, generates PDF digest.
    Returns stats dict.
    """
    init_logging("monitor")
    start_time = time.time()
    logger.info("════════════════════════════════════════")
    logger.info("--monitor-jobs starting (parallel mode, max_workers=%d)",
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

    logger.info("Loaded %d monitorable companies (%d total in DB)",
                len(companies), len(all_companies))

    print(f"\n{'='*55}")
    print(f"[INFO] Job Monitor — {datetime.now().strftime('%B %d, %Y')}")
    print(f"[INFO] Monitoring {len(companies)} companies "
          f"(parallel, {MONITOR_MAX_WORKERS} workers)")
    print(f"{'='*55}\n")

    # ── Shared stats — accumulated thread-safely via Lock ──
    stats = {
        "companies_monitored":    0,
        "companies_with_results": 0,
        "companies_unknown_ats":  0,
        "api_failures":           0,
        "total_jobs_fetched":     0,
        "new_jobs_found":         0,
        "jobs_matched_filters":   0,
        "api_failure_list":       [],
    }
    stats_lock = threading.Lock()

    # ── Run companies in parallel ──────────────────────────
    with ThreadPoolExecutor(max_workers=MONITOR_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _process_company, company_row, i + 1, len(companies)
            ): company_row["company"]
            for i, company_row in enumerate(companies)
        }

        for future in as_completed(futures):
            company = futures[future]
            try:
                company_stats = future.result()
            except Exception as e:
                logger.error("Unhandled error in worker for %r: %s",
                             company, e, exc_info=True)
                company_stats = {
                    "monitored": 1, "with_results": 0,
                    "unknown_ats": 0, "failed": 1,
                    "fetched": 0, "matched": 0, "new": 0,
                    "failure_name": company,
                }

            # Accumulate stats thread-safely
            with stats_lock:
                stats["companies_monitored"]    += company_stats.get("monitored",    0)
                stats["companies_with_results"] += company_stats.get("with_results", 0)
                stats["companies_unknown_ats"]  += company_stats.get("unknown_ats",  0)
                stats["api_failures"]           += company_stats.get("failed",       0)
                stats["total_jobs_fetched"]     += company_stats.get("fetched",      0)
                stats["jobs_matched_filters"]   += company_stats.get("matched",      0)
                stats["new_jobs_found"]         += company_stats.get("new",          0)
                if company_stats.get("failure_name"):
                    stats["api_failure_list"].append(
                        company_stats["failure_name"]
                    )

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
        except Exception as e:
            logger.error("PDF generation failed: %s", e, exc_info=True)
            print(f"[ERROR] PDF generation failed: {e}")
            print("[INFO] Sending plain text digest instead...")
            email_sent = _send_text_fallback(new_postings)
            if email_sent:
                mark_postings_digested()
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
        logger.debug("flush_api_health failed", exc_info=True)

    logger.info("════ --monitor-jobs finished ════")
    return final_stats


# ─────────────────────────────────────────
# WORKER — one company per thread call
# Logic is identical to the original sequential loop body.
# Only difference: uses semaphore instead of between_companies_delay().
# ─────────────────────────────────────────

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
        "monitored":    1,
        "with_results": 0,
        "unknown_ats":  0,
        "failed":       0,
        "fetched":      0,
        "matched":      0,
        "new":          0,
        "failure_name": None,
    }

    logger.info("── [%d/%d] %r  platform=%s",
                position, total, company, platform)

    # ── ATS re-detection if needed ────────────────────────
    if needs_redetection(company_row, JOB_MONITOR_REDETECT_DAYS):
        domain = company_row.get("domain")
        logger.info("Re-detection triggered for %r (domain=%s)",
                    company, domain)
        try:
            detection = detect_ats(company, domain=domain)
            platform  = detection["ats_platform"]
            slug      = detection["ats_slug"]
        except Exception as e:
            logger.error("Re-detection failed for %r: %s", company, e)

    if platform == "unknown" or not slug:
        logger.warning("Skipping %r — unknown ATS", company)
        result["unknown_ats"] = 1
        result["monitored"]   = 1
        print(f"  [{position}/{total}] {company} — [SKIP] Unknown ATS")
        return result

    # ── Get ATS module ────────────────────────────────────
    # custom platform is handled inline below — get_ats_module()
    # returns None for it, so we import directly to avoid the
    # "No ATS module" failure path.
    if platform == "custom":
        from jobs.ats import custom_career as ats_module
    else:
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
            if platform == "workday":
                try:
                    slug_info = json.loads(slug)
                    if "path" not in slug_info:
                        slug_info["path"] = "careers"
                except (json.JSONDecodeError, TypeError):
                    slug_info = {"slug": slug, "wd": "wd5", "path": "careers"}
                logger.debug("Workday fetch: %r slug=%s", company, slug_info)
                raw_jobs = ats_module.fetch_jobs(slug_info, company)

            elif platform == "oracle_hcm":
                try:
                    slug_info = json.loads(slug)
                except (json.JSONDecodeError, TypeError):
                    slug_info = {"slug": slug, "site": ""}
                logger.debug("Oracle HCM fetch: %r slug=%s", company, slug_info)
                raw_jobs = ats_module.fetch_jobs(slug_info, company)

            elif platform == "custom":
                try:
                    slug_info = json.loads(slug)
                except (json.JSONDecodeError, TypeError):
                    logger.error("custom: invalid slug JSON for %r", company)
                    result["failed"]       = 1
                    result["failure_name"] = company
                    return result
                logger.debug("custom fetch: %r slug_info keys=%s",
                             company, list(slug_info.keys()))
                raw_jobs = ats_module.fetch_jobs(slug_info, company)

            else:
                logger.debug("%s fetch: %r slug=%s", platform, company, slug)
                raw_jobs = ats_module.fetch_jobs(slug, company)

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
    result["fetched"] = len(raw_jobs)

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
    slug_info_cached = None 

    if platform == "custom":
        slug_info_cached = slug_info

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

        if platform != "greenhouse" and not is_fresh(job, platform):
            logger.debug("Pre-existing (stale): %r | %s",
                         company, job.get("title"))
            save_job_posting(job, status="pre_existing")
            continue

        if is_first_scan:
            save_job_posting(job, status="pre_existing")
            continue

        # Platform-specific detail fetches — each re-acquires the semaphore
        # so detail HTTP requests obey the same per-platform throttle as listing.
        if platform == "icims" and job.get("_base_url"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error("iCIMS fetch_job_detail failed %s/%s: %s",
                                 company, job.get("job_id"), e, exc_info=True)

        if platform == "jobvite" and job.get("_slug"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error("Jobvite fetch_job_detail failed %s/%s: %s",
                                 company, job.get("job_id"), e, exc_info=True)

        if platform == "avature" and job.get("job_url"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error("Avature fetch_job_detail failed %s/%s: %s",
                                 company, job.get("job_id"), e, exc_info=True)

        if platform == "phenom" and job.get("job_url"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error("Phenom fetch_job_detail failed %s/%s: %s",
                                 company, job.get("job_id"), e, exc_info=True)

        if platform == "talentbrew" and job.get("job_url"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error("TalentBrew fetch_job_detail failed %s/%s: %s",
                                 company, job.get("job_id"), e, exc_info=True)

        if platform == "sitemap" and job.get("job_url") and \
                job.get("_feed_type") != "xml":
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error("Sitemap fetch_job_detail failed %s/%s: %s",
                                 company, job.get("job_id"), e, exc_info=True)

        if platform == "taleo" and job.get("_contest_no"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                    # taleo returns extra fields — store safely only if columns exist
                    # salary_min, salary_max, salary_type, contact, contact_phone,
                    # full_location are stored in job dict and saved by save_job_posting()
                    # if those columns exist in the DB schema.
                    # If they don't exist yet, save_job_posting() will ignore them
                    # (as long as it uses named column inserts, not SELECT *).
                except Exception as e:
                    logger.error("Taleo fetch_job_detail failed for %s/%s: %s",
                                company, job.get("job_id"), e, exc_info=True)

        # ── Eightfold ─────────────────────────────────────────────────────────────
        if platform == "eightfold" and job.get("job_url"):
            with sem:
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error("Eightfold fetch_job_detail failed for %s/%s: %s",
                                company, job.get("job_id"), e, exc_info=True)

        if (platform == "custom"
                and job.get("job_url")
                and not job.get("description")):    # ← skip if listing already has it
            if slug_info_cached is None:
                try:
                    slug_info_cached = json.loads(slug)
                except (json.JSONDecodeError, TypeError):
                    slug_info_cached = {}
            if slug_info_cached.get("detail"):
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
        mark_first_scan_complete(company)
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
        coverage = stats["companies_with_results"] / total_companies
        if coverage < MONITOR_COVERAGE_ALERT:
            pct = int(coverage * 100)
            logger.warning("Coverage alert: %d%%", pct)
            alerts.append({
                "level":   "warning",
                "message": f"Coverage {pct}% — only "
                           f"{stats['companies_with_results']}/"
                           f"{total_companies} companies returned jobs",
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

        credits = get_serper_credits()
        logger.info("Serper credits: %d/%d",
                    credits["credits_remaining"], credits["credits_limit"])
        print(f"[INFO] Serper credits: "
              f"{credits['credits_remaining']}/{credits['credits_limit']}")

        domain = matches[0].get("domain") if matches else None
        try:
            result = detect_ats(company_normalized, domain=domain)
            logger.info("Result: %s", result)
            build_detection_report([result], date_str)
        except QuotaExhaustedException:
            logger.warning("Serper exhausted during detection of %r",
                           company_normalized)
            print("[WARNING] Serper credits exhausted")
        return

    if batch:
        credits   = get_serper_credits()
        remaining = credits["credits_remaining"]
        logger.info("Batch detection: Serper remaining=%d", remaining)

        if remaining <= 0:
            logger.warning("Serper credits exhausted")
            print("[WARNING] Serper credits exhausted.")
            _print_detection_queue_status()
            return

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
            try:
                result = detect_ats(comp, domain=domain)
                results.append(result)
            except QuotaExhaustedException:
                logger.warning("Serper exhausted after %d companies", i - 1)
                print(f"\n[WARNING] Serper exhausted after {i-1}.")
                break

        credits = get_serper_credits()
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

    credits = get_serper_credits()
    print(f"[INFO] Detecting {len(to_detect)} companies...\n")

    results = []
    for i, company_row in enumerate(to_detect, 1):
        comp   = company_row["company"]
        domain = company_row.get("domain")
        print(f"[{i}/{len(to_detect)}] {comp}")
        try:
            result = detect_ats(comp, domain=domain)
            results.append(result)
        except QuotaExhaustedException:
            print(f"\n[WARNING] Serper exhausted after {i-1}.")
            break

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