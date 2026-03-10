# jobs/job_monitor.py — Job monitoring pipeline orchestrator

import json
import time
import os
from datetime import datetime

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
)
from jobs.ats_detector import (
    detect_ats, needs_redetection, override_ats,
    get_ats_module, QuotaExhaustedException
)
from db.serper_quota import get_serper_credits
from jobs.job_filter import filter_jobs, is_fresh
from config import (
    JOB_MONITOR_REDETECT_DAYS,
    MONITOR_COVERAGE_ALERT,
    MONITOR_ATS_UNKNOWN_ALERT,
    MONITOR_RELIABILITY_ALERT,
    MONITOR_MATCH_RATE_LOW_ALERT,
    MONITOR_MATCH_RATE_HIGH_ALERT,
)


def run():
    """
    Main entry point for --monitor-jobs.
    Scans all companies, finds new jobs, generates PDF digest.
    Returns stats dict.
    """
    start_time = time.time()
    init_db()

    companies = get_monitorable_companies()
    if not companies:
        print("[INFO] No monitorable companies found.")
        print("[INFO] Run: python pipeline.py --import-prospects prospects.txt")
        print("[INFO] Then: python pipeline.py --detect-ats")
        return {}

    all_companies = get_all_monitored_companies()
    skipped = len(all_companies) - len(companies)
    if skipped > 0:
        print(f"[INFO] Skipping {skipped} company/companies with "
              f"unknown/unverified ATS — run --detect-ats to fix")

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

        print(f"[{i}/{len(companies)}] {company}")

        stats["companies_monitored"] += 1

        # ── ATS Detection ──
        if needs_redetection(company_row, JOB_MONITOR_REDETECT_DAYS):
            result = detect_ats(company)
            platform = result["ats_platform"]
            slug     = result["ats_slug"]

        if platform == "unknown" or not slug:
            stats["companies_unknown_ats"] += 1
            print("   [SKIP] Unknown ATS — skipping")
            continue

        # ── Fetch jobs ──
        ats_module = get_ats_module(platform)
        if not ats_module:
            stats["api_failures"] += 1
            stats["api_failure_list"].append(company)
            continue

        try:
            # Parse slug_info for Workday
            if platform == "workday":
                try:
                    slug_info = json.loads(slug)
                    # Ensure path key exists — default to "careers"
                    if "path" not in slug_info:
                        slug_info["path"] = "careers"
                except (json.JSONDecodeError, TypeError):
                    slug_info = {"slug": slug, "wd": "wd5", "path": "careers"}
                raw_jobs = ats_module.fetch_jobs(slug_info, company)
            elif platform == "oracle_hcm":
                try:
                    slug_info = json.loads(slug)
                except (json.JSONDecodeError, TypeError):
                    slug_info = {"slug": slug, "site": ""}
                raw_jobs = ats_module.fetch_jobs(slug_info, company)
            else:
                raw_jobs = ats_module.fetch_jobs(slug, company)

        except Exception as e:
            print(f"   [ERROR] API fetch failed: {e}")
            stats["api_failures"] += 1
            stats["api_failure_list"].append(company)
            continue

        stats["total_jobs_fetched"] += len(raw_jobs)

        if not raw_jobs:
            update_company_check(company, found_jobs=False)
            print("   [INFO] No jobs returned")
            continue

        stats["companies_with_results"] += 1
        update_company_check(company, found_jobs=True)

        # ── Filter jobs ──
        matched = filter_jobs(raw_jobs)
        stats["jobs_matched_filters"] += len(matched)

        # ── First scan handling ──
        is_first_scan = company_row.get("first_scanned_at") is None

        # ── Freshness check + save ──
        new_count = 0
        for job in matched:
            # Layer 1: URL deduplication
            if job_url_exists(job["job_url"]):
                continue

            # Layer 2: Content hash deduplication
            if job.get("content_hash") and \
               job_hash_exists(job["content_hash"]):
                continue

            # Layer 3: Date-based freshness
            # (only for reliable ATS — not Greenhouse)
            if platform != "greenhouse" and not is_fresh(job, platform):
                save_job_posting(job, status="pre_existing")
                continue

            # First scan: all jobs are pre_existing
            if is_first_scan:
                save_job_posting(job, status="pre_existing")
                continue

            # iCIMS Option C: fetch detail page for new jobs only
            if platform == "icims" and job.get("_base_url"):
                try:
                    job = ats_module.fetch_job_detail(job)
                except Exception as e:
                    logger.error(
                        "iCIMS fetch_job_detail failed for %s/%s: %s",
                        company, job.get("job_id"), e, exc_info=True
                    )  # save with partial data

            # Genuinely new job
            if save_job_posting(job, status="new"):
                new_count += 1

        stats["new_jobs_found"] += new_count
        print(f"   [OK] {len(raw_jobs)} fetched -> "
              f"{len(matched)} matched -> {new_count} new")

        # Mark first scan complete
        if is_first_scan:
            mark_first_scan_complete(company)
            print(f"   [INFO] First scan complete — "
                  f"existing jobs marked as pre_existing")

    # ── Generate PDF digest ──
    new_postings = get_new_postings_for_digest()
    pdf_generated = False
    email_sent    = False

    duration = int(time.time() - start_time)

    print(f"\n{'='*55}")
    print(f"[INFO] Run complete in {duration}s")
    print(f"[INFO] Companies: {stats['companies_monitored']} monitored | "
          f"{stats['companies_unknown_ats']} unknown ATS | "
          f"{stats['api_failures']} failures")
    print(f"[INFO] Jobs: {stats['total_jobs_fetched']} fetched | "
          f"{stats['jobs_matched_filters']} matched | "
          f"{stats['new_jobs_found']} new")

    if new_postings:
        print(f"\n[INFO] Generating PDF digest "
              f"({len(new_postings)} new jobs)...")
        try:
            from outreach.report_templates.monitor_report import (
                build_monitor_report
            )
            alerts = _build_alerts(stats, len(companies))
            result = build_monitor_report(new_postings, stats, alerts)
            pdf_generated = result.get("pdf_generated", False)
            email_sent    = result.get("email_sent", False)
        except Exception as e:
            print(f"[ERROR] PDF generation failed: {e}")
            print("[INFO] Sending plain text digest instead...")
            email_sent = _send_text_fallback(new_postings)
    else:
        print(f"\n[INFO] No new matching jobs today.")
        email_sent = _send_no_jobs_email()

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

    return final_stats


def _build_alerts(stats, total_companies):
    """Build list of alert messages based on metric thresholds."""
    alerts = []

    if total_companies > 0:
        coverage = stats["companies_with_results"] / total_companies
        if coverage < MONITOR_COVERAGE_ALERT:
            pct = int(coverage * 100)
            alerts.append({
                "level":   "warning",
                "message": f"Coverage {pct}% — only "
                           f"{stats['companies_with_results']}/"
                           f"{total_companies} companies returned jobs",
            })

        unknown_rate = stats["companies_unknown_ats"] / total_companies
        if unknown_rate > MONITOR_ATS_UNKNOWN_ALERT:
            alerts.append({
                "level":   "warning",
                "message": f"{stats['companies_unknown_ats']} companies "
                           f"have unknown ATS — run --detect-ats",
            })

    if stats["api_failures"] > 0:
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
            alerts.append({
                "level":   "warning",
                "message": f"Filter match rate {int(match_rate*100)}% "
                           f"— filters may be too strict",
            })
        elif match_rate > MONITOR_MATCH_RATE_HIGH_ALERT:
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


def _send_no_jobs_email():
    """Send brief email when no new jobs found today. Returns bool."""
    from outreach.report_templates.base import send_report_email
    try:
        date_str = datetime.now().strftime("%B %-d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    subject = f"[Digest] Job Digest · {date_str} · No new jobs today"
    html = f"""
    <html><body style="font-family:sans-serif;padding:24px;">
      <h2>No New Jobs Today</h2>
      <p style="color:#64748b;">
        No new job postings matched your profile on {date_str}.
      </p>
      <p style="color:#64748b;">
        This is normal — check again tomorrow.
      </p>
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
        # Only allow safe URL schemes
        from urllib.parse import urlparse
        parsed = urlparse(raw_url)
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
    from datetime import datetime
    from outreach.report_templates.detection_report import build_detection_report
    from config import DETECT_ATS_BATCH_SIZE

    init_db()
    companies = get_all_monitored_companies()

    if not companies:
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
            print(f"[ERROR] '{company}' not found.")
            return

        credits = get_serper_credits()
        print(f"[INFO] Serper credits: "
              f"{credits['credits_remaining']}/{credits['credits_limit']} "
              f"remaining")

        domain = matches[0].get("domain") if matches else None
        try:
            result = detect_ats(company_normalized, domain=domain)
            build_detection_report([result], date_str)
        except QuotaExhaustedException:
            print("[WARNING] Serper credits exhausted")
        return

    # ── Batch detection ──
    if batch:
        credits = get_serper_credits()
        remaining = credits["credits_remaining"]

        if remaining <= 0:
            print(f"[WARNING] Serper credits exhausted. "
                  f"Buy more at serper.dev")
            _print_detection_queue_status()
            return

        to_detect = get_detection_queue(batch_size=DETECT_ATS_BATCH_SIZE)
        if not to_detect:
            print("[OK] No companies pending detection.")
            _print_detection_queue_status()
            return

        print(f"[INFO] Detecting {len(to_detect)} companies "
              f"(Serper credits: {remaining} remaining)...\n")

        results = []
        total   = len(to_detect)

        for i, company_row in enumerate(to_detect, 1):
            comp   = company_row["company"]
            domain = company_row.get("domain")
            prio   = company_row.get("priority", "?")
            print(f"[{i}/{total}] {comp} (priority {prio})")

            try:
                result = detect_ats(comp, domain=domain)
                results.append(result)
            except QuotaExhaustedException:
                print(f"\n[WARNING] Serper credits exhausted after "
                      f"{i-1} companies.")
                break

        credits = get_serper_credits()
        print(f"\n[INFO] Batch complete.")
        print(f"[INFO] Serper credits: "
              f"{credits['credits_used']} used, "
              f"{credits['credits_remaining']} remaining")
        _print_detection_queue_status()

        if results:
            print(f"\n[INFO] Sending detection summary email...")
            build_detection_report(results, date_str)
        return

    # ── Full detection (no --batch flag) ──
    to_detect = [c for c in companies if needs_redetection(c)]

    if not to_detect:
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
        print(f"[{i}/{len(to_detect)}] {comp}")
        try:
            result = detect_ats(comp, domain=domain)
            results.append(result)
        except QuotaExhaustedException:
            print(f"\n[WARNING] Serper credits exhausted after "
                  f"{i-1} companies.")
            print("[INFO] Buy more credits at serper.dev then retry.")
            break

    credits = get_serper_credits()
    print(f"\n[INFO] Detection complete.")
    print(f"[INFO] Serper: {credits['credits_used']} used, "
          f"{credits['credits_remaining']} remaining")
    _print_detection_queue_status()

    if results:
        print(f"\n[INFO] Sending detection summary email...")
        build_detection_report(results, date_str)



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
        pass  # non-critical display function



def run_monitor_status():
    """Show monitoring status summary. Called by --monitor-status."""
    init_db()
    companies = get_all_monitored_companies()
    stats_history = get_monitor_stats(7)
    reliability = get_pipeline_reliability(7)

    print(f"\n{'='*55}")
    print("[INFO] Job Monitor Status")
    print(f"{'='*55}")

    # ATS detection summary
    total     = len(companies)
    known     = sum(1 for c in companies
                    if c.get("ats_platform") not in ("unknown", None))
    unknown   = total - known
    never_scanned = sum(1 for c in companies
                        if not c.get("first_scanned_at"))

    print(f"\nCompanies:     {total}")
    print(f"ATS detected:  {known} ({int(known/total*100) if total else 0}%)")
    print(f"ATS unknown:   {unknown}")
    print(f"Never scanned: {never_scanned}")

    # Recent run stats
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