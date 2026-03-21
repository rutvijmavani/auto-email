# jobs/job_monitor.py — Job monitoring pipeline orchestrator

import json
import time
import os
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
)
from jobs.ats_detector import (
    detect_ats, needs_redetection, override_ats,
    get_ats_module, QuotaExhaustedException
)
from jobs.ats.base import between_companies_delay
from db.serper_quota import get_serper_credits
from jobs.job_filter import filter_jobs, is_fresh, make_legacy_content_hash
from config import (
    JOB_MONITOR_REDETECT_DAYS,
    MONITOR_COVERAGE_ALERT,
    MONITOR_ATS_UNKNOWN_ALERT,
    MONITOR_RELIABILITY_ALERT,
    MONITOR_MATCH_RATE_LOW_ALERT,
    MONITOR_MATCH_RATE_HIGH_ALERT,
)

logger = get_logger(__name__)


def run():
    """
    Main entry point for --monitor-jobs.
    Scans all companies, finds new jobs, generates PDF digest.
    Returns stats dict.
    """
    init_logging("monitor")
    start_time = time.time()
    logger.info("════════════════════════════════════════")
    logger.info("--monitor-jobs starting")

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
    print(f"[INFO] Monitoring {len(companies)} companies")
    print(f"{'='*55}\n")

    # ── Stats tracking ──
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

    # ── Process each company ──
    for i, company_row in enumerate(companies, 1):
        company  = company_row["company"]
        platform = company_row.get("ats_platform", "unknown")
        slug     = company_row.get("ats_slug")

        logger.info("── [%d/%d] %r  platform=%s  slug=%s",
                    i, len(companies), company, platform, slug)
        print(f"[{i}/{len(companies)}] {company}")

        stats["companies_monitored"] += 1

        # ── ATS Detection ──
        if needs_redetection(company_row, JOB_MONITOR_REDETECT_DAYS):
            domain = company_row.get("domain")
            logger.info("Re-detection triggered for %r (domain=%s)", company, domain)
            result   = detect_ats(company, domain=domain)
            platform = result["ats_platform"]
            slug     = result["ats_slug"]
            logger.info("Re-detection result: %r → platform=%s slug=%s",
                        company, platform, slug)

        if platform == "unknown" or not slug:
            logger.warning("Skipping %r — ATS unknown or slug missing", company)
            stats["companies_unknown_ats"] += 1
            print("   [SKIP] Unknown ATS — skipping")
            continue

        # ── Fetch jobs ──
        ats_module = get_ats_module(platform)
        if not ats_module:
            logger.error("No ATS module for platform=%s (company=%r)", platform, company)
            stats["api_failures"] += 1
            stats["api_failure_list"].append(company)
            continue

        try:
            if platform == "workday":
                try:
                    slug_info = json.loads(slug)
                    if "path" not in slug_info:
                        slug_info["path"] = "careers"
                except (json.JSONDecodeError, TypeError):
                    slug_info = {"slug": slug, "wd": "wd5", "path": "careers"}
                logger.debug("Workday fetch: company=%r slug_info=%s", company, slug_info)
                raw_jobs = ats_module.fetch_jobs(slug_info, company)
            elif platform == "oracle_hcm":
                try:
                    slug_info = json.loads(slug)
                except (json.JSONDecodeError, TypeError):
                    slug_info = {"slug": slug, "site": ""}
                logger.debug("Oracle HCM fetch: company=%r slug_info=%s", company, slug_info)
                raw_jobs = ats_module.fetch_jobs(slug_info, company)
            else:
                logger.debug("%s fetch: company=%r slug=%s", platform, company, slug)
                raw_jobs = ats_module.fetch_jobs(slug, company)

        except Exception as e:
            logger.error("API fetch failed for %r (platform=%s): %s",
                         company, platform, e, exc_info=True)
            print(f"   [ERROR] API fetch failed: {e}")
            stats["api_failures"] += 1
            stats["api_failure_list"].append(company)
            continue

        logger.debug("Fetched %d raw jobs for %r", len(raw_jobs), company)
        stats["total_jobs_fetched"] += len(raw_jobs)

        between_companies_delay()

        if not raw_jobs:
            logger.info("No jobs returned for %r", company)
            update_company_check(company, found_jobs=False)
            print("   [INFO] No jobs returned")
            continue

        stats["companies_with_results"] += 1
        update_company_check(company, found_jobs=True)

        # ── Filter jobs ──
        matched = filter_jobs(raw_jobs)
        logger.debug("Filter: %d raw → %d matched for %r",
                     len(raw_jobs), len(matched), company)
        stats["jobs_matched_filters"] += len(matched)

        # ── First scan handling ──
        is_first_scan = company_row.get("first_scanned_at") is None
        if is_first_scan:
            logger.info("First scan for %r — all jobs marked pre_existing", company)

        # ── Freshness check + save ──
        new_count = 0
        for job in matched:
            if job_url_exists(job["job_url"]):
                logger.debug("Duplicate URL skipped: %s", job["job_url"])
                continue

            if job.get("content_hash") and \
               job_hash_exists(job["content_hash"],
                               job.get("content_hash_legacy")):
                logger.debug("Duplicate content_hash skipped for %r", company)
                continue

            if platform != "greenhouse" and not is_fresh(job, platform):
                logger.debug("Pre-existing (stale): %r title=%s posted=%s",
                             company, job.get("title"), job.get("posted_at"))
                save_job_posting(job, status="pre_existing")
                continue

            if is_first_scan:
                save_job_posting(job, status="pre_existing")
                continue

            if platform == "icims" and job.get("_base_url"):
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error(
                        "iCIMS fetch_job_detail failed for %s/%s: %s",
                        company, job.get("job_id"), e, exc_info=True
                    )

            if save_job_posting(job, status="new"):
                new_count += 1
                logger.info("NEW JOB: %r | %s | %s",
                            company, job.get("title"), job.get("location"))

        fetched_urls = {job["job_url"] for job in matched}
        tracked      = get_tracked_urls_for_company(company)

        present_ids = [
            tracked[url] for url in fetched_urls
            if url in tracked
        ]
        missing_ids = [
            tracked[url] for url in tracked
            if url not in fetched_urls
        ]

        if present_ids:
            reset_missing_days(present_ids)
        if missing_ids:
            increment_missing_days(missing_ids)
            logger.debug("Missing from scan: %d jobs for %r",
                         len(missing_ids), company)

        logger.info("Done %r: fetched=%d matched=%d new=%d",
                    company, len(raw_jobs), len(matched), new_count)
        stats["new_jobs_found"] += new_count
        print(f"   [OK] {len(raw_jobs)} fetched -> "
              f"{len(matched)} matched -> {new_count} new")

        if is_first_scan:
            mark_first_scan_complete(company)
            logger.info("First scan complete for %r", company)
            print(f"   [INFO] First scan complete — "
                  f"existing jobs marked as pre_existing")

    # ── Generate PDF digest ──
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
    print(f"[INFO] Run complete in {duration}s")
    print(f"[INFO] Companies: {stats['companies_monitored']} monitored | "
          f"{stats['companies_unknown_ats']} unknown ATS | "
          f"{stats['api_failures']} failures")
    print(f"[INFO] Jobs: {stats['total_jobs_fetched']} fetched | "
          f"{stats['jobs_matched_filters']} matched | "
          f"{stats['new_jobs_found']} new")

    if new_postings:
        logger.info("Generating PDF digest (%d new jobs)", len(new_postings))
        print(f"\n[INFO] Generating PDF digest "
              f"({len(new_postings)} new jobs)...")
        try:
            from outreach.report_templates.monitor_report import (
                build_monitor_report
            )
            alerts        = _build_alerts(stats, len(companies))
            result        = build_monitor_report(new_postings, stats, alerts)
            pdf_generated = result.get("pdf_generated", False)
            email_sent    = result.get("email_sent", False)
            logger.info("PDF digest sent: pdf=%s email=%s",
                        pdf_generated, email_sent)
            # Mark as digested only after confirmed email sent
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
                logger.info("Marked %d posting(s) as digested (text fallback)",
                            len(new_postings))
    else:
        logger.info("No new jobs — sending no-jobs email")
        print(f"\n[INFO] No new matching jobs today.")
        alerts     = _build_alerts(stats, len(companies))
        email_sent = _send_no_jobs_email(alerts=alerts)

    # ── Save stats ──
    final_stats = {
        **stats,
        "run_duration_seconds": duration,
        "pdf_generated":        int(pdf_generated),
        "email_sent":           int(email_sent),
    }
    save_monitor_stats(final_stats)

    # ── Print metric alerts ──
    _print_metric_alerts(stats, len(companies))

    logger.info("════ --monitor-jobs finished ════")
    return final_stats


def _build_alerts(stats, total_companies):
    """Build list of alert messages based on metric thresholds."""
    alerts = []

    if total_companies > 0:
        coverage = stats["companies_with_results"] / total_companies
        if coverage < MONITOR_COVERAGE_ALERT:
            pct = int(coverage * 100)
            logger.warning("Coverage alert: %d%% (threshold %d%%)",
                           pct, int(MONITOR_COVERAGE_ALERT * 100))
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
    """Send brief email when no new jobs found today. Returns bool."""
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
    Run ATS detection using 4-phase approach.
    No browser needed for most companies.

    Modes:
      --detect-ats                    detect all pending
      --detect-ats --batch            detect next batch
      --detect-ats "Company"          detect single company
      --detect-ats "Co" --override p s manually set ATS
    """
    init_logging("detect")
    logger.info("════════════════════════════════════════")
    logger.info("--detect-ats starting: company=%r batch=%s override=%s/%s",
                company, batch, override_platform, override_slug)

    from datetime import datetime
    from outreach.report_templates.detection_report import build_detection_report
    from config import DETECT_ATS_BATCH_SIZE

    init_db()
    companies = get_all_monitored_companies()

    if not companies:
        logger.warning("No companies in DB — run --import-prospects first")
        print("[INFO] No companies found. Run --import-prospects first.")
        return

    # ── Manual override ──
    if company and override_platform and override_slug:
        override_ats(company.strip(), override_platform, override_slug)
        return

    try:
        date_str = datetime.now().strftime("%B %d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    # ── Single company detection ──
    if company:
        company_normalized = company.strip()
        matches = [c for c in companies
                   if c["company"] == company_normalized]
        if not matches:
            logger.error("Company not found in DB: %r", company_normalized)
            print(f"[ERROR] '{company}' not found.")
            return

        credits = get_serper_credits()
        logger.info("Serper credits: %d/%d remaining",
                    credits["credits_remaining"], credits["credits_limit"])
        print(f"[INFO] Serper credits: "
              f"{credits['credits_remaining']}/{credits['credits_limit']} "
              f"remaining")

        domain = matches[0].get("domain") if matches else None
        logger.info("Single detection: %r domain=%s", company_normalized, domain)
        try:
            result = detect_ats(company_normalized, domain=domain)
            logger.info("Result: %s", result)
            build_detection_report([result], date_str)
        except QuotaExhaustedException:
            logger.warning("Serper exhausted during detection of %r",
                           company_normalized)
            print("[WARNING] Serper credits exhausted")
        return

    # ── Batch detection ──
    if batch:
        credits   = get_serper_credits()
        remaining = credits["credits_remaining"]
        logger.info("Batch detection: Serper remaining=%d", remaining)

        if remaining <= 0:
            logger.warning("Serper credits exhausted")
            print(f"[WARNING] Serper credits exhausted. "
                  f"Buy more at serper.dev")
            _print_detection_queue_status()
            return

        to_detect = get_detection_queue(batch_size=DETECT_ATS_BATCH_SIZE)
        if not to_detect:
            logger.info("Detection queue empty")
            print("[OK] No companies pending detection.")
            _print_detection_queue_status()
            return

        logger.info("Batch: %d companies queued", len(to_detect))
        print(f"[INFO] Detecting {len(to_detect)} companies "
              f"(Serper credits: {remaining} remaining)...\n")

        results = []
        total   = len(to_detect)

        for i, company_row in enumerate(to_detect, 1):
            comp   = company_row["company"]
            domain = company_row.get("domain")
            prio   = company_row.get("priority", "?")
            logger.info("[%d/%d] %r domain=%s priority=%s",
                        i, total, comp, domain, prio)
            print(f"[{i}/{total}] {comp} (priority {prio})")

            try:
                result = detect_ats(comp, domain=domain)
                results.append(result)
                logger.info("Detected %r → %s", comp, result.get("platform"))
            except QuotaExhaustedException:
                logger.warning("Serper exhausted after %d/%d companies",
                               i - 1, total)
                print(f"\n[WARNING] Serper credits exhausted after "
                      f"{i-1} companies.")
                break

        credits = get_serper_credits()
        logger.info("Batch complete: %d results, remaining=%d",
                    len(results), credits["credits_remaining"])
        print(f"\n[INFO] Batch complete.")
        print(f"[INFO] Serper credits: "
              f"{credits['credits_used']} used, "
              f"{credits['credits_remaining']} remaining")
        _print_detection_queue_status()

        if results:
            print(f"\n[INFO] Sending detection summary email...")
            build_detection_report(results, date_str)
        logger.info("════ --detect-ats (batch) finished ════")
        return

    # ── Full detection (no --batch flag) ──
    to_detect = [c for c in companies if needs_redetection(c)]
    logger.info("Full detection: %d/%d companies need detection",
                len(to_detect), len(companies))

    if not to_detect:
        logger.info("All companies detected — nothing to do")
        print("[OK] All companies have ATS detected.")
        _print_detection_queue_status()
        return

    credits = get_serper_credits()
    print(f"[INFO] Detecting {len(to_detect)} companies "
          f"(Serper credits: {credits['credits_remaining']} remaining)...\n")

    results = []
    for i, company_row in enumerate(to_detect, 1):
        comp   = company_row["company"]
        domain = company_row.get("domain")
        logger.info("[%d/%d] %r domain=%s", i, len(to_detect), comp, domain)
        print(f"[{i}/{len(to_detect)}] {comp}")
        try:
            result = detect_ats(comp, domain=domain)
            results.append(result)
        except QuotaExhaustedException:
            logger.warning("Serper exhausted after %d/%d companies",
                           i - 1, len(to_detect))
            print(f"\n[WARNING] Serper credits exhausted after "
                  f"{i-1} companies.")
            print("[INFO] Buy more credits at serper.dev then retry.")
            break

    credits = get_serper_credits()
    logger.info("Full detection complete: %d results, remaining=%d",
                len(results), credits["credits_remaining"])
    print(f"\n[INFO] Detection complete.")
    print(f"[INFO] Serper: {credits['credits_used']} used, "
          f"{credits['credits_remaining']} remaining")
    _print_detection_queue_status()

    if results:
        print(f"\n[INFO] Sending detection summary email...")
        build_detection_report(results, date_str)

    logger.info("════ --detect-ats finished ════")


def _print_detection_queue_status():
    """Print detection queue status summary."""
    try:
        from db.db import get_detection_queue_stats
        stats = get_detection_queue_stats()
        p1 = stats.get("priority1_new", 0) or 0
        p2 = stats.get("priority2_quiet", 0) or 0
        p3 = stats.get("priority3_unknown", 0) or 0
        p4 = stats.get("priority4_custom", 0) or 0
        total = p1 + p2 + p3 + p4

        logger.debug("Detection queue: total=%d p1=%d p2=%d p3=%d p4=%d",
                     total, p1, p2, p3, p4)

        if total > 0:
            print(f"\n[INFO] Detection queue ({total} companies pending):")
            if p1: print(f"  Priority 1 (new):          {p1}")
            if p2: print(f"  Priority 2 (14+ empty):    {p2}")
            if p3: print(f"  Priority 3 (unknown):      {p3}")
            if p4: print(f"  Priority 4 (custom/retry): {p4}")
            print(f"  Estimated days to complete: "
                  f"{max(1, total // 10)} days at 10/day")
        else:
            print("[OK] Detection queue empty — all companies detected")
    except Exception:
        pass


def run_monitor_status():
    """Show monitoring status summary. Called by --monitor-status."""
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

    logger.info("Monitor status: total=%d known=%d unknown=%d "
                "never_scanned=%d reliability=%.0f%%",
                total, known, unknown, never_scanned, reliability * 100)

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