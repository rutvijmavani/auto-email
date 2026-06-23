# outreach/report_templates/monitor_report.py
# PDF digest generation + email for --monitor-jobs

import os
import html as html_lib
from datetime import datetime
from collections import defaultdict

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, HRFlowable, PageBreak,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

from outreach.report_templates.base import send_report_email
from config import JOB_MONITOR_PDF_RETENTION

# ── Colors ──
NAVY    = colors.HexColor("#0f172a")
ACCENT  = colors.HexColor("#3b82f6")
SUCCESS = colors.HexColor("#22c55e")
WARNING = colors.HexColor("#f59e0b")
DANGER  = colors.HexColor("#ef4444")
MUTED   = colors.HexColor("#64748b")
LIGHT   = colors.HexColor("#f0f2f5")
WHITE   = colors.white
BLACK   = colors.HexColor("#1e293b")

DIGESTS_DIR = "data/digests"


def _get_styles():
    """Build custom paragraph styles."""
    styles = {}

    styles["title"] = ParagraphStyle(
        "title",
        fontName="Helvetica-Bold",
        fontSize=20,
        textColor=WHITE,
        spaceAfter=4,
    )
    styles["subtitle"] = ParagraphStyle(
        "subtitle",
        fontName="Helvetica",
        fontSize=11,
        textColor=colors.HexColor("#94a3b8"),
        spaceAfter=2,
    )
    styles["section"] = ParagraphStyle(
        "section",
        fontName="Helvetica-Bold",
        fontSize=9,
        textColor=MUTED,
        spaceBefore=16,
        spaceAfter=6,
        textTransform="uppercase",
        letterSpacing=1,
    )
    styles["company"] = ParagraphStyle(
        "company",
        fontName="Helvetica-Bold",
        fontSize=13,
        textColor=NAVY,
        spaceBefore=12,
        spaceAfter=4,
    )
    styles["job_title"] = ParagraphStyle(
        "job_title",
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=BLACK,
        spaceAfter=2,
    )
    styles["job_meta"] = ParagraphStyle(
        "job_meta",
        fontName="Helvetica",
        fontSize=9,
        textColor=MUTED,
        spaceAfter=2,
    )
    styles["job_url"] = ParagraphStyle(
        "job_url",
        fontName="Helvetica",
        fontSize=8,
        textColor=ACCENT,
        spaceAfter=8,
    )
    styles["alert_warning"] = ParagraphStyle(
        "alert_warning",
        fontName="Helvetica",
        fontSize=9,
        textColor=colors.HexColor("#92400e"),
        backColor=colors.HexColor("#fef3c7"),
        leftIndent=8,
        rightIndent=8,
        spaceBefore=4,
        spaceAfter=4,
        borderPad=6,
    )
    styles["alert_error"] = ParagraphStyle(
        "alert_error",
        fontName="Helvetica",
        fontSize=9,
        textColor=colors.HexColor("#991b1b"),
        backColor=colors.HexColor("#fee2e2"),
        leftIndent=8,
        rightIndent=8,
        spaceBefore=4,
        spaceAfter=4,
    )
    styles["normal"] = ParagraphStyle(
        "normal",
        fontName="Helvetica",
        fontSize=10,
        textColor=BLACK,
        spaceAfter=4,
    )
    styles["small"] = ParagraphStyle(
        "small",
        fontName="Helvetica",
        fontSize=8,
        textColor=MUTED,
        spaceAfter=2,
    )

    return styles


def _safe_text(text):
    """Sanitize text for reportlab — escape XML special chars."""
    if not text:
        return ""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


def _format_date(posted_at):
    """Format posted_at for display."""
    if not posted_at:
        return "Unknown"
    try:
        if isinstance(posted_at, str):
            from datetime import timezone
            posted_at = datetime.fromisoformat(
                posted_at.replace("Z", "+00:00")
            )
        from datetime import timezone
        now = datetime.now(timezone.utc)
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=timezone.utc)
        days = (now - posted_at).days
        if days == 0:
            return "Today"
        elif days == 1:
            return "Yesterday"
        elif days <= 7:
            return f"{days} days ago"
        else:
            return posted_at.strftime("%b %d, %Y")
    except Exception:
        return "Unknown"


def _build_header_table(date_str, stats, styles):
    """Build dark navy header with summary stats."""
    title_para = Paragraph(
        f"Job Digest &mdash; {_safe_text(date_str)}", styles["title"]
    )
    subtitle_para = Paragraph(
        f"{stats.get('new_jobs_found', 0)} new jobs matching your profile",
        styles["subtitle"]
    )

    stats_data = [
        ["Companies", "Jobs Fetched", "Matched", "New Jobs"],
        [
            str(stats.get("companies_monitored", 0)),
            str(stats.get("total_jobs_fetched", 0)),
            str(stats.get("jobs_matched_filters", 0)),
            str(stats.get("new_jobs_found", 0)),
        ]
    ]

    stats_table = Table(stats_data, colWidths=[1.5*inch]*4)
    stats_table.setStyle(TableStyle([
        ("FONTNAME",   (0,0), (-1,0), "Helvetica"),
        ("FONTSIZE",   (0,0), (-1,0), 8),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.HexColor("#94a3b8")),
        ("FONTNAME",   (0,1), (-1,1), "Helvetica-Bold"),
        ("FONTSIZE",   (0,1), (-1,1), 18),
        ("TEXTCOLOR",  (0,1), (-1,1), WHITE),
        ("ALIGN",      (0,0), (-1,-1), "CENTER"),
        ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [NAVY, NAVY]),
        ("TOPPADDING", (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ]))

    header_content = [
        [title_para],
        [subtitle_para],
        [Spacer(1, 12)],
        [stats_table],
    ]

    header_table = Table([[item[0]] for item in header_content],
                          colWidths=[6.5*inch])
    header_table.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), NAVY),
        ("TOPPADDING",    (0,0), (-1,-1), 12),
        ("BOTTOMPADDING", (0,0), (-1,-1), 12),
        ("LEFTPADDING",   (0,0), (-1,-1), 20),
        ("RIGHTPADDING",  (0,0), (-1,-1), 20),
        ("ROWBACKGROUNDS",(0,0), (-1,-1), [NAVY, NAVY,
                                           NAVY, NAVY]),
    ]))
    return header_table


def _build_health_section(stats, alerts, styles):
    """Build pipeline health section."""
    elements = []
    elements.append(Paragraph("Pipeline Health", styles["section"]))
    elements.append(HRFlowable(width="100%", thickness=1,
                               color=colors.HexColor("#e2e8f0"),
                               spaceAfter=8))

    total = stats.get("companies_monitored", 0)
    known_ats = total - stats.get("companies_unknown_ats", 0)
    # covered_by_workers = companies whose full scan completed before job_monitor ran.
    # fallback_scanned   = missed companies whose fallback ATS fetch succeeded
    #                      (0 or more jobs).  Use this for coverage so zero-job
    #                      scans are not wrongly excluded.
    #                      Falls back to companies_with_results for old stat rows.
    worker_covered  = stats.get("covered_by_workers", 0)
    fallback_hits   = stats.get("fallback_scanned", stats.get("companies_with_results", 0))
    in_flight       = stats.get("in_flight", 0)
    # Include in-flight scans optimistically so they don't vanish from coverage
    # while actively running (consistent with _build_alerts in job_monitor.py).
    total_covered   = worker_covered + fallback_hits + in_flight
    coverage_pct = int(total_covered / total * 100) if total else 0
    ats_pct = int(known_ats / total * 100) if total else 0

    # Coverage value string:
    #   "111/139 (80%)  [111 by workers + 9 by fallback (6 with jobs, 3 empty)]"
    # The "(X empty)" sub-count is an early warning: if it's suddenly large,
    # some ATS integrations may have gone stale (returning HTTP 200 but 0 jobs).
    coverage_detail = f"{worker_covered} by workers"
    if fallback_hits:
        fallback_with_jobs = stats.get("companies_with_results", 0)
        fallback_empty     = fallback_hits - fallback_with_jobs
        breakdown = f"{fallback_with_jobs} with jobs"
        if fallback_empty:
            breakdown += f", {fallback_empty} empty"
        coverage_detail += f" + {fallback_hits} by job monitor ({breakdown})"
    coverage_val = f"{total_covered}/{total} ({coverage_pct}%)  [{coverage_detail}]"

    health_data = [
        ["Metric",          "Value",          "Status"],
        ["Coverage",
         coverage_val,
         "OK" if coverage_pct >= 70 else "WARNING"],
        ["ATS Detected",
         f"{known_ats}/{total} ({ats_pct}%)",
         "OK" if ats_pct >= 80 else "WARNING"],
        ["API Failures",
         str(stats.get("api_failures", 0)),
         "OK" if stats.get("api_failures", 0) == 0 else "WARNING"],
        ["Run Duration",
         f"{stats.get('run_duration_seconds', 0)}s",
         "OK"],
    ]

    health_table = Table(health_data,
                         colWidths=[2.5*inch, 2.5*inch, 1.5*inch])
    health_table.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,0), colors.HexColor("#f8fafc")),
        ("FONTNAME",     (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE",     (0,0), (-1,-1), 9),
        ("TEXTCOLOR",    (0,0), (-1,0), MUTED),
        ("TEXTCOLOR",    (0,1), (-1,-1), BLACK),
        ("ALIGN",        (2,0), (2,-1), "CENTER"),
        ("GRID",         (0,0), (-1,-1), 0.5,
         colors.HexColor("#e2e8f0")),
        ("ROWBACKGROUNDS",(0,1), (-1,-1),
         [WHITE, colors.HexColor("#f8fafc")]),
        ("TOPPADDING",   (0,0), (-1,-1), 6),
        ("BOTTOMPADDING",(0,0), (-1,-1), 6),
        ("LEFTPADDING",  (0,0), (-1,-1), 8),
    ]))
    elements.append(health_table)

    # Alerts
    for alert in alerts:
        level = alert.get("level", "info")
        msg   = _safe_text(alert.get("message", ""))
        style = (styles["alert_error"] if level == "error"
                 else styles["alert_warning"])
        prefix = "ERROR: " if level == "error" else "WARNING: "
        elements.append(Paragraph(prefix + msg, style))

    return elements


def _build_top_matches(jobs, styles, count=5):
    """Build top N matches summary."""
    elements = []
    elements.append(Paragraph("Top Matches Today", styles["section"]))
    elements.append(HRFlowable(width="100%", thickness=1,
                               color=colors.HexColor("#e2e8f0"),
                               spaceAfter=8))

    top = sorted(jobs, key=lambda j: j.get("skill_score", 0),
                 reverse=True)[:count]

    for i, job in enumerate(top, 1):
        title    = _safe_text(job.get("title", ""))
        company  = _safe_text(job.get("company", ""))
        location = _safe_text(job.get("location", ""))
        elements.append(Paragraph(
            f"{i}. <b>{title}</b> — {company} ({location})",
            styles["normal"]
        ))

    return elements


def _build_job_listings(jobs_by_company, styles):
    """Build grouped job listings — one section per company."""
    elements = []

    for company, jobs in sorted(jobs_by_company.items()):
        # Company header
        elements.append(Paragraph(
            f"{_safe_text(company)}  &bull;  {len(jobs)} new job(s)",
            styles["company"]
        ))
        elements.append(HRFlowable(
            width="100%", thickness=0.5,
            color=colors.HexColor("#e2e8f0"), spaceAfter=6
        ))

        for job in jobs:
            title    = _safe_text(job.get("title", ""))
            location = _safe_text(job.get("location", "Remote"))
            date_str = _format_date(job.get("posted_at"))
            job_url  = job.get("job_url", "")
            score    = job.get("skill_score", 0)
            dots     = min(5, max(1, score // 4))
            score_str = "●" * dots + "○" * (5 - dots)

            elements.append(Paragraph(
                f"<b>{title}</b>  <font color='#64748b'>{score_str}</font>",
                styles["job_title"]
            ))
            elements.append(Paragraph(
                f"{location}  &bull;  Posted: {date_str}",
                styles["job_meta"]
            ))
            if job_url:
                # Validate URL scheme before embedding in PDF href
                from urllib.parse import urlparse
                _parsed = urlparse(job_url)
                if _parsed.scheme in ("http", "https"):
                    _safe_href = _safe_text(job_url)
                    elements.append(Paragraph(
                        f'<a href="{_safe_href}" color="#3b82f6">{_safe_text(job_url)}</a>',
                        styles["job_url"]
                    ))
                else:
                    elements.append(Paragraph(
                        _safe_text(job_url), styles["job_url"]
                    ))

        elements.append(Spacer(1, 8))

    return elements


def _build_api_warning_section():
    """
    Build warning section for daily digest email.
    Shows Tier 2 (warning-level) pipeline alerts.
    Returns HTML string — empty if no warnings.
    """
    try:
        from db.pipeline_alerts import get_pending_warnings, mark_warnings_sent
        from config import (
            RATE_LIMIT_WARNING_THRESHOLD,
            SERPER_WARNING_THRESHOLD,
        )
        warnings = get_pending_warnings()
        if not warnings:
            return ""

        items = []
        for w in warnings:
            platform = w.get("platform", "")
            msg      = w.get("message", "")
            val      = w.get("value", 0)
            typ      = w.get("alert_type", "")

            if typ == "rate_limit":
                items.append(
                    f"⚠ {platform.title()} rate limit warning: "
                    f"{val}% of requests returned 429. "
                    f"Monitor api_health table."
                )
            elif typ == "slow_response":
                items.append(
                    f"⚠ {platform.title()} responses slow: "
                    f"avg {int(val)}ms. May indicate throttling."
                )
            elif typ == "serper_low":
                items.append(
                    f"⚠ Serper credits low: {int(val)} remaining. "
                    f"Consider purchasing more at serper.dev."
                )
            elif msg:
                items.append(f"⚠ {msg}")

        if not items:
            return ""

        # Mark as sent
        mark_warnings_sent([w["id"] for w in warnings])

        rows = "".join(
            f'<p style="color:#92400e;background:#fffbeb;'
            f'padding:8px;border-radius:4px;'
            f'font-size:12px;margin:4px 0;">{item}</p>'
            for item in items
        )
        return (
            f'<hr style="border:none;border-top:1px solid #e2e8f0;'
            f'margin:16px 0;">'
            f'<p style="font-weight:600;color:#374151;'
            f'font-size:13px;margin-bottom:8px;">'
            f'⚠ Pipeline Warnings</p>'
            f'{rows}'
        )
    except Exception:
        return ""


def _build_queue_health_section() -> str:
    """
    Build a queue-depth health HTML section for the daily digest email.

    Checks Redis detail queues and poll queues so a backlog is visible in the
    daily email even if the watchdog alert email was missed.

    Thresholds:
        detail queues combined > 100  → WARNING
        detail queues combined > 500  → ERROR
        poll:adaptive empty           → ERROR
        poll:fullscan empty           → WARNING

    Returns an HTML string.  Empty string on any failure so a Redis hiccup
    never blocks the digest from sending.
    """
    try:
        import redis as _redis_lib
        from config import (
            REDIS_URL,
            REDIS_POLL_ADAPTIVE,
            REDIS_POLL_FULLSCAN,
            REDIS_DETAIL_ADAPTIVE,
            REDIS_DETAIL_FULLSCAN,
        )
        import time as _time

        # Use a local client with a short socket timeout so a Redis hang never
        # blocks digest email generation indefinitely.  The shared get_redis()
        # client has no socket timeout configured.
        r = _redis_lib.from_url(
            REDIS_URL,
            socket_timeout=5,
            socket_connect_timeout=5,
        )

        # ── Gather metrics ────────────────────────────────────────────────────
        detail_adp   = r.llen(REDIS_DETAIL_ADAPTIVE)
        detail_fs    = r.llen(REDIS_DETAIL_FULLSCAN)
        detail_total = detail_adp + detail_fs

        poll_adp_total   = r.zcard(REDIS_POLL_ADAPTIVE)
        poll_adp_overdue = r.zcount(REDIS_POLL_ADAPTIVE, "-inf", _time.time() - 1800)
        poll_fs_total    = r.zcard(REDIS_POLL_FULLSCAN)
        poll_fs_overdue  = r.zcount(REDIS_POLL_FULLSCAN, "-inf", _time.time() - 7200)

        # ── Determine overall health ──────────────────────────────────────────
        issues = []

        if detail_total > 500:
            issues.append({
                "level": "error",
                "msg": (
                    f"Detail queue backlog CRITICAL: {detail_total:,} jobs pending "
                    f"({detail_adp:,} adaptive + {detail_fs:,} fullscan). "
                    "detail_worker may be dead or severely lagging."
                ),
            })
        elif detail_total > 100:
            issues.append({
                "level": "warning",
                "msg": (
                    f"Detail queue elevated: {detail_total:,} jobs pending "
                    f"({detail_adp:,} adaptive + {detail_fs:,} fullscan). "
                    "Monitor — may indicate detail_worker slowdown."
                ),
            })

        if poll_adp_total == 0:
            issues.append({
                "level": "error",
                "msg": (
                    "poll:adaptive queue is EMPTY — no companies scheduled. "
                    "Run: python pipeline.py --rebuild"
                ),
            })
        elif poll_adp_overdue > 10:
            issues.append({
                "level": "warning",
                "msg": (
                    f"{poll_adp_overdue}/{poll_adp_total} companies overdue >30 min "
                    "in poll:adaptive — scan_worker may be lagging."
                ),
            })

        if poll_fs_total == 0:
            issues.append({
                "level": "warning",
                "msg": (
                    "poll:fullscan queue is EMPTY — fullscans may not be scheduled. "
                    "Run: python pipeline.py --rebuild"
                ),
            })
        elif poll_fs_overdue > 5:
            issues.append({
                "level": "warning",
                "msg": (
                    f"{poll_fs_overdue}/{poll_fs_total} fullscans overdue >2h — "
                    "fullscan_worker may be lagging."
                ),
            })

        # ── Build HTML ────────────────────────────────────────────────────────
        # Determine status colour for the section header dot
        if any(i["level"] == "error" for i in issues):
            header_color  = "#ef4444"
            header_symbol = "✗"
        elif issues:
            header_color  = "#f59e0b"
            header_symbol = "⚠"
        else:
            header_color  = "#22c55e"
            header_symbol = "✓"

        # Summary row values
        def _depth_cell(val, warn, crit):
            if val > crit:
                return f'<span style="color:#ef4444;font-weight:700;">{val:,} ✗</span>'
            elif val > warn:
                return f'<span style="color:#f59e0b;font-weight:700;">{val:,} ⚠</span>'
            return f'<span style="color:#22c55e;">{val:,} ✓</span>'

        def _status_cell(val, warn, crit):
            """Status label that matches _depth_cell severity — no contradictions."""
            if val > crit:
                return "✗ CRITICAL"
            elif val > warn:
                return "⚠ backlog"
            return "OK"

        th = (
            "padding:5px 10px;font-size:11px;font-weight:600;color:#64748b;"
            "background:#f1f5f9;border-bottom:1px solid #e2e8f0;text-align:left;"
        )
        td = (
            "padding:5px 10px;font-size:12px;color:#1e293b;"
            "border-bottom:1px solid #f1f5f9;"
        )

        table_html = (
            f'<table width="100%" cellpadding="0" cellspacing="0" '
            f'style="border-collapse:collapse;font-size:12px;">'
            f'<tr>'
            f'<th style="{th}">Queue</th>'
            f'<th style="{th}">Depth</th>'
            f'<th style="{th}">Overdue</th>'
            f'<th style="{th}">Status</th>'
            f'</tr>'
            f'<tr style="background:#ffffff;">'
            f'<td style="{td}">detail:adaptive</td>'
            f'<td style="{td}">{_depth_cell(detail_adp, 100, 500)}</td>'
            f'<td style="{td}">—</td>'
            f'<td style="{td}">{_status_cell(detail_adp, 100, 500)}</td>'
            f'</tr>'
            f'<tr style="background:#f8fafc;">'
            f'<td style="{td}">detail:fullscan</td>'
            f'<td style="{td}">{_depth_cell(detail_fs, 100, 500)}</td>'
            f'<td style="{td}">—</td>'
            f'<td style="{td}">{_status_cell(detail_fs, 100, 500)}</td>'
            f'</tr>'
            f'<tr style="background:#ffffff;">'
            f'<td style="{td}">poll:adaptive</td>'
            f'<td style="{td}">{poll_adp_total:,} scheduled</td>'
            f'<td style="{td}">'
            f'{"<span style=\'color:#f59e0b;font-weight:600;\'>" + str(poll_adp_overdue) + " ⚠</span>" if poll_adp_overdue > 10 else str(poll_adp_overdue)}'
            f'</td>'
            f'<td style="{td}">{"⚠ overdue" if poll_adp_overdue > 10 else ("✗ EMPTY" if poll_adp_total == 0 else "OK")}</td>'
            f'</tr>'
            f'<tr style="background:#f8fafc;">'
            f'<td style="{td}">poll:fullscan</td>'
            f'<td style="{td}">{poll_fs_total:,} scheduled</td>'
            f'<td style="{td}">'
            f'{"<span style=\'color:#f59e0b;font-weight:600;\'>" + str(poll_fs_overdue) + " ⚠</span>" if poll_fs_overdue > 5 else str(poll_fs_overdue)}'
            f'</td>'
            f'<td style="{td}">{"⚠ overdue" if poll_fs_overdue > 5 else ("⚠ empty" if poll_fs_total == 0 else "OK")}</td>'
            f'</tr>'
            f'</table>'
        )

        issue_html = "".join(
            f'<p style="color:{"#991b1b" if i["level"] == "error" else "#92400e"};'
            f'background:{"#fee2e2" if i["level"] == "error" else "#fffbeb"};'
            f'padding:8px 10px;border-radius:4px;font-size:12px;margin:4px 0;">'
            f'{"✗" if i["level"] == "error" else "⚠"} {i["msg"]}</p>'
            for i in issues
        )

        return (
            f'<hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">'
            f'<p style="font-weight:700;font-size:13px;color:#0f172a;margin-bottom:6px;">'
            f'<span style="color:{header_color};">{header_symbol}</span>'
            f'&nbsp;Queue Health</p>'
            f'{table_html}'
            f'{issue_html}'
        )

    except Exception:
        return ""


def _build_adaptive_health_section() -> str:
    """
    Build the weekly adaptive polling health HTML section for the digest email.

    Reads from db.adaptive_health (last 7 days, score oscillation 14 days):
        • Miss rate per platform      — did adaptive catch jobs before full scan?
        • API error rates             — normal-context only (backoff/canary excluded)
        • Detection age distribution  — portfolio-wide ≤1h / ≤4h / ≤24h / >24h share
        • Wasted poll rate            — polls returning no new jobs per platform
        • Score stability             — stddev of daily poll score (14-day window)
        • Worker scaling events       — add/remove/outage/canary summary

    Returns an HTML string.  Empty string on any failure or missing data so
    a DB issue never prevents the digest email from sending.

    Called from _send_digest_email() on Mondays only.
    """
    try:
        from db.adaptive_health import (
            build_weekly_health_data,
            MISS_RATE_WARN, MISS_RATE_CRIT,
            ERROR_RATE_WARN, ERROR_RATE_CRIT,
        )
        data = build_weekly_health_data(days=7)
    except Exception:
        return ""

    # Wasted-poll thresholds (inline — no config constant needed)
    WASTED_RATE_WARN = 60.0   # %  — amber above this
    WASTED_RATE_CRIT = 85.0   # %  — red above this
    # Score oscillation thresholds (stddev units)
    OSCILLATION_WARN = 0.15
    OSCILLATION_CRIT = 0.30

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _pct_color(pct, warn, crit):
        """Return inline CSS color for a percentage value."""
        if pct is None:
            return "#64748b"
        if pct >= crit:
            return "#ef4444"   # red
        if pct >= warn:
            return "#f59e0b"   # amber
        return "#22c55e"       # green

    def _badge(pct, warn, crit):
        """Coloured percentage string."""
        if pct is None:
            return '<span style="color:#94a3b8;">—</span>'
        color = _pct_color(pct, warn, crit)
        symbol = "✓" if pct < warn else ("⚠" if pct < crit else "✗")
        return (
            f'<span style="color:{color};font-weight:600;">'
            f'{symbol} {pct:.1f}%</span>'
        )

    row_bg = ["#ffffff", "#f8fafc"]
    th_style = (
        "padding:6px 10px;font-size:11px;font-weight:600;"
        "color:#64748b;background:#f1f5f9;"
        "border-bottom:1px solid #e2e8f0;text-align:left;"
    )
    td_style = (
        "padding:5px 10px;font-size:12px;color:#1e293b;"
        "border-bottom:1px solid #f1f5f9;"
    )

    sections = []

    # ── Section header ────────────────────────────────────────────────────────
    sections.append(
        '<hr style="border:none;border-top:1px solid #e2e8f0;margin:20px 0;">'
        '<p style="font-weight:700;font-size:14px;color:#0f172a;margin-bottom:4px;">'
        '📊 Adaptive Polling Health — last 7 days</p>'
    )

    if not data["has_data"]:
        sections.append(
            '<p style="font-size:12px;color:#64748b;'
            'background:#f8fafc;padding:10px;border-radius:6px;">'
            '⏳ Insufficient data — adaptive polling metrics will appear here '
            'once the system has been running for at least one full day.</p>'
        )
        return "".join(sections)

    # ── Miss rate table ───────────────────────────────────────────────────────
    if data["miss_rates"]:
        sections.append(
            '<p style="font-size:11px;font-weight:600;color:#64748b;'
            'text-transform:uppercase;letter-spacing:0.5px;margin:12px 0 4px;">'
            'Miss Rate &nbsp;<span style="font-weight:400;font-size:10px;">'
            '(jobs first found by full scan — lower is better)</span></p>'
        )
        sections.append(
            '<table width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12px;">'
            f'<tr><th style="{th_style}">Platform</th>'
            f'<th style="{th_style}">Total New Jobs</th>'
            f'<th style="{th_style}">Caught by Adaptive</th>'
            f'<th style="{th_style}">Full Scan Only</th>'
            f'<th style="{th_style}">Miss Rate</th></tr>'
        )
        for i, row in enumerate(data["miss_rates"]):
            bg     = row_bg[i % 2]
            pct    = row.get("miss_rate_pct")
            badge  = _badge(pct, MISS_RATE_WARN, MISS_RATE_CRIT)
            plat   = html_lib.escape(str(row["platform"]).title())
            sections.append(
                f'<tr style="background:{bg};">'
                f'<td style="{td_style}">{plat}</td>'
                f'<td style="{td_style}">{int(row["total_new_jobs"] or 0)}</td>'
                f'<td style="{td_style}">{int(row["tier1_new_jobs"] or 0)}</td>'
                f'<td style="{td_style}">{int(row["tier2_new_jobs"] or 0)}</td>'
                f'<td style="{td_style}">{badge}</td>'
                f'</tr>'
            )
        sections.append('</table>')
    else:
        sections.append(
            '<p style="font-size:12px;color:#94a3b8;">'
            'Miss rate: no job activity recorded this week.</p>'
        )

    # ── API error rate table ──────────────────────────────────────────────────
    if data["error_rates"]:
        sections.append(
            '<p style="font-size:11px;font-weight:600;color:#64748b;'
            'text-transform:uppercase;letter-spacing:0.5px;margin:14px 0 4px;">'
            'API Error Rates &nbsp;<span style="font-weight:400;font-size:10px;">'
            '(normal context only — backoff &amp; canary excluded)</span></p>'
        )
        sections.append(
            '<table width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12px;">'
            f'<tr><th style="{th_style}">Platform</th>'
            f'<th style="{th_style}">Requests</th>'
            f'<th style="{th_style}">Errors</th>'
            f'<th style="{th_style}">Error Rate</th>'
            f'<th style="{th_style}">Avg Response</th></tr>'
        )
        for i, row in enumerate(data["error_rates"]):
            bg    = row_bg[i % 2]
            pct   = float(row.get("error_rate_pct") or 0)
            badge = _badge(pct, ERROR_RATE_WARN, ERROR_RATE_CRIT)
            plat  = html_lib.escape(str(row["platform"]).title())
            avg_ms = int(row.get("avg_response_ms") or 0)
            sections.append(
                f'<tr style="background:{bg};">'
                f'<td style="{td_style}">{plat}</td>'
                f'<td style="{td_style}">{int(row["requests_made"] or 0):,}</td>'
                f'<td style="{td_style}">{int(row["total_errors"] or 0):,}</td>'
                f'<td style="{td_style}">{badge}</td>'
                f'<td style="{td_style}">{avg_ms} ms</td>'
                f'</tr>'
            )
        sections.append('</table>')

    # ── Detection age distribution ────────────────────────────────────────────
    age = data.get("detection_age", {})
    if age.get("total_new_jobs", 0) > 0:
        sections.append(
            '<p style="font-size:11px;font-weight:600;color:#64748b;'
            'text-transform:uppercase;letter-spacing:0.5px;margin:14px 0 4px;">'
            'Detection Age Distribution &nbsp;'
            '<span style="font-weight:400;font-size:10px;">'
            '(how quickly adaptive polling finds new jobs)</span></p>'
        )

        def _age_row(label, count, pct, color):
            if pct is None:
                pct = 0.0
            bar_width = max(2, int(pct * 1.4))
            return (
                f'<tr>'
                f'<td style="width:120px;font-size:11px;color:#374151;'
                f'padding:3px 8px 3px 0;">{label}</td>'
                f'<td style="padding:3px 0;">'
                f'<div style="display:inline-block;width:{bar_width}px;height:14px;'
                f'background:{color};border-radius:3px;vertical-align:middle;">'
                f'</div>'
                f'&nbsp;<span style="font-size:11px;color:#1e293b;font-weight:600;">'
                f'{pct:.1f}%</span>'
                f'&nbsp;<span style="font-size:10px;color:#94a3b8;">({count:,})</span>'
                f'</td>'
                f'</tr>'
            )

        total_jobs = age["total_new_jobs"]
        sections.append(
            '<table cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;margin-left:4px;">'
            + _age_row("≤ 1 hour",  age.get("within_1hr",  0),
                       age.get("within_1hr_pct"),  "#22c55e")
            + _age_row("≤ 4 hours", age.get("within_4hr",  0),
                       age.get("within_4hr_pct"),  "#3b82f6")
            + _age_row("≤ 24 hours", age.get("within_24hr", 0),
                       age.get("within_24hr_pct"), "#f59e0b")
            + _age_row("> 24 hours", age.get("after_24hr",  0),
                       age.get("after_24hr_pct"),  "#ef4444")
            + f'<tr><td colspan="2" style="font-size:10px;color:#94a3b8;'
              f'padding-top:4px;">Total: {total_jobs:,} new jobs across all platforms</td></tr>'
            + '</table>'
        )

    # ── Wasted poll rate table ────────────────────────────────────────────────
    wasted = data.get("wasted_poll_rates", [])
    if wasted:
        sections.append(
            '<p style="font-size:11px;font-weight:600;color:#64748b;'
            'text-transform:uppercase;letter-spacing:0.5px;margin:14px 0 4px;">'
            'Wasted Poll Rate &nbsp;'
            '<span style="font-weight:400;font-size:10px;">'
            '(polls returning no new jobs — lower means better score tuning)</span></p>'
        )
        sections.append(
            '<table width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12px;">'
            f'<tr>'
            f'<th style="{th_style}">Platform</th>'
            f'<th style="{th_style}">Total Polls</th>'
            f'<th style="{th_style}">Wasted Polls</th>'
            f'<th style="{th_style}">Waste Rate</th>'
            f'<th style="{th_style}">Days</th>'
            f'</tr>'
        )
        for i, row in enumerate(wasted):
            bg   = row_bg[i % 2]
            pct  = row.get("wasted_rate_pct")
            badge = _badge(pct, WASTED_RATE_WARN, WASTED_RATE_CRIT)
            plat  = html_lib.escape(str(row["platform"]).title())
            sections.append(
                f'<tr style="background:{bg};">'
                f'<td style="{td_style}">{plat}</td>'
                f'<td style="{td_style}">{int(row["total_polls"] or 0):,}</td>'
                f'<td style="{td_style}">{int(row["wasted_polls"] or 0):,}</td>'
                f'<td style="{td_style}">{badge}</td>'
                f'<td style="{td_style}">{int(row["days_with_data"] or 0)}</td>'
                f'</tr>'
            )
        sections.append('</table>')

    # ── Score oscillation table ───────────────────────────────────────────────
    osc = data.get("score_oscillation", [])
    if osc:
        sections.append(
            '<p style="font-size:11px;font-weight:600;color:#64748b;'
            'text-transform:uppercase;letter-spacing:0.5px;margin:14px 0 4px;">'
            'Score Stability (14-day) &nbsp;'
            '<span style="font-weight:400;font-size:10px;">'
            '(stddev of daily poll score — high variance = score still converging)</span></p>'
        )
        sections.append(
            '<table width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12px;">'
            f'<tr>'
            f'<th style="{th_style}">Platform</th>'
            f'<th style="{th_style}">Avg Score</th>'
            f'<th style="{th_style}">Min</th>'
            f'<th style="{th_style}">Max</th>'
            f'<th style="{th_style}">Std Dev</th>'
            f'<th style="{th_style}">Days</th>'
            f'</tr>'
        )
        for i, row in enumerate(osc):
            bg      = row_bg[i % 2]
            stddev  = float(row.get("score_stddev") or 0)
            plat    = html_lib.escape(str(row["platform"]).title())
            # Colour the stddev cell by oscillation severity
            if stddev >= OSCILLATION_CRIT:
                stddev_color = "#ef4444"
                stddev_sym   = "✗"
            elif stddev >= OSCILLATION_WARN:
                stddev_color = "#f59e0b"
                stddev_sym   = "⚠"
            else:
                stddev_color = "#22c55e"
                stddev_sym   = "✓"
            stddev_cell = (
                f'<span style="color:{stddev_color};font-weight:600;">'
                f'{stddev_sym} {stddev:.3f}</span>'
            )
            sections.append(
                f'<tr style="background:{bg};">'
                f'<td style="{td_style}">{plat}</td>'
                f'<td style="{td_style}">{float(row.get("avg_score") or 0):.2f}</td>'
                f'<td style="{td_style}">{float(row.get("min_score") or 0):.2f}</td>'
                f'<td style="{td_style}">{float(row.get("max_score") or 0):.2f}</td>'
                f'<td style="{td_style}">{stddev_cell}</td>'
                f'<td style="{td_style}">{int(row.get("days_with_data") or 0)}</td>'
                f'</tr>'
            )
        sections.append('</table>')

    # ── Worker scaling summary ────────────────────────────────────────────────
    ev = data["scaling_events"]
    if ev["total_events"] > 0:
        sections.append(
            '<p style="font-size:11px;font-weight:600;color:#64748b;'
            'text-transform:uppercase;letter-spacing:0.5px;margin:14px 0 4px;">'
            'Worker Scaling Events</p>'
        )

        def _ev_cell(label, count, color="#1e293b"):
            return (
                f'<td style="padding:8px 12px;text-align:center;">'
                f'<div style="font-size:18px;font-weight:700;color:{color};">'
                f'{count}</div>'
                f'<div style="font-size:10px;color:#64748b;">{label}</div>'
                f'</td>'
            )

        outage_color  = "#ef4444" if ev["outage_start"] > 0 else "#22c55e"
        remove_color  = "#f59e0b" if ev["worker_remove"] > 0 else "#1e293b"

        sections.append(
            '<table width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;background:#f8fafc;'
            'border-radius:8px;border:1px solid #e2e8f0;">'
            '<tr>'
            + _ev_cell("Workers Added",   ev["worker_add"],      "#22c55e")
            + _ev_cell("Workers Removed", ev["worker_remove"],   remove_color)
            + _ev_cell("Outages Started", ev["outage_start"],    outage_color)
            + _ev_cell("Outages Ended",   ev["outage_end"],      "#22c55e")
            + _ev_cell("Canary Probes",   ev["canary_probe"],    "#3b82f6")
            + _ev_cell("Ceil. Learned",   ev["ceiling_learned"], "#8b5cf6")
            + '</tr></table>'
        )
    else:
        sections.append(
            '<p style="font-size:12px;color:#22c55e;margin-top:12px;">'
            '✓ No worker scaling events this week — system stable.</p>'
        )

    # ── Early-exit validation ─────────────────────────────────────────────────
    early = data.get("early_exit_stats", [])
    if early:
        # Only show if at least one platform has actual misses — zero-missed
        # platforms are uninteresting noise in the weekly report.
        has_misses = any(int(r.get("total_missed") or 0) > 0 for r in early)
        if has_misses:
            # Threshold: flag when early-exit misses account for > 5% of new jobs
            EARLY_EXIT_WARN = 5.0
            EARLY_EXIT_CRIT = 15.0
            sections.append(
                '<p style="font-size:11px;font-weight:600;color:#64748b;'
                'text-transform:uppercase;letter-spacing:0.5px;margin:14px 0 4px;">'
                'Early-Exit Validation &nbsp;'
                '<span style="font-weight:400;font-size:10px;">'
                '(jobs skipped by paginator early exit — should be near zero)</span></p>'
            )
            sections.append(
                '<table width="100%" cellpadding="0" cellspacing="0" '
                'style="border-collapse:collapse;font-size:12px;">'
                f'<tr>'
                f'<th style="{th_style}">Platform</th>'
                f'<th style="{th_style}">Missed by Early Exit</th>'
                f'<th style="{th_style}">Total New Jobs</th>'
                f'<th style="{th_style}">Missed Rate</th>'
                f'<th style="{th_style}">Days</th>'
                f'</tr>'
            )
            for i, row in enumerate(early):
                missed = int(row.get("total_missed") or 0)
                if missed == 0:
                    continue   # skip clean platforms
                bg    = row_bg[i % 2]
                pct   = row.get("missed_rate_pct")
                badge = _badge(pct, EARLY_EXIT_WARN, EARLY_EXIT_CRIT)
                plat  = html_lib.escape(str(row["platform"]).title())
                sections.append(
                    f'<tr style="background:{bg};">'
                    f'<td style="{td_style}">{plat}</td>'
                    f'<td style="{td_style};color:#ef4444;font-weight:600;">{missed:,}</td>'
                    f'<td style="{td_style}">{int(row.get("total_new_jobs") or 0):,}</td>'
                    f'<td style="{td_style}">{badge}</td>'
                    f'<td style="{td_style}">{int(row.get("days_with_data") or 0)}</td>'
                    f'</tr>'
                )
            sections.append('</table>')

    # ── Scaling effectiveness table ───────────────────────────────────────────
    eff = data.get("scaling_effectiveness", [])
    if eff:
        sections.append(
            '<p style="font-size:11px;font-weight:600;color:#64748b;'
            'text-transform:uppercase;letter-spacing:0.5px;margin:14px 0 4px;">'
            'Worker Reduction Effectiveness &nbsp;'
            '<span style="font-weight:400;font-size:10px;">'
            '(did error-triggered reductions actually fix the problem?)</span></p>'
        )
        sections.append(
            '<table width="100%" cellpadding="0" cellspacing="0" '
            'style="border-collapse:collapse;font-size:12px;">'
            f'<tr>'
            f'<th style="{th_style}">Platform</th>'
            f'<th style="{th_style}">Reductions</th>'
            f'<th style="{th_style}">Effective</th>'
            f'<th style="{th_style}">→ Outage</th>'
            f'<th style="{th_style}">Outages Resolved</th>'
            f'<th style="{th_style}">Effectiveness</th>'
            f'<th style="{th_style}">Err% at Reduce</th>'
            f'</tr>'
        )
        for i, row in enumerate(eff):
            bg     = row_bg[i % 2]
            plat   = html_lib.escape(str(row["platform"]).title())
            eff_pct = row.get("effectiveness_pct")
            reductions = int(row.get("reductions") or 0)
            effective  = int(row.get("effective_reductions") or 0)
            outages    = int(row.get("escalated_to_outage") or 0)
            resolved   = int(row.get("outages_resolved") or 0)
            err_reduce = row.get("avg_error_at_reduce")

            # Colour effectiveness: green ≥ 80%, amber 50-80%, red < 50%
            if eff_pct is None:
                eff_cell = '<span style="color:#94a3b8;">—</span>'
            elif eff_pct >= 80:
                eff_cell = (f'<span style="color:#22c55e;font-weight:600;">'
                            f'✓ {eff_pct:.0f}%</span>')
            elif eff_pct >= 50:
                eff_cell = (f'<span style="color:#f59e0b;font-weight:600;">'
                            f'⚠ {eff_pct:.0f}%</span>')
            else:
                eff_cell = (f'<span style="color:#ef4444;font-weight:600;">'
                            f'✗ {eff_pct:.0f}%</span>')

            outage_cell = (
                f'<span style="color:#ef4444;font-weight:600;">{outages}</span>'
                if outages > 0 else str(outages)
            )
            err_str = (
                f'{float(err_reduce):.1f}%' if err_reduce is not None else '—'
            )
            sections.append(
                f'<tr style="background:{bg};">'
                f'<td style="{td_style}">{plat}</td>'
                f'<td style="{td_style}">{reductions}</td>'
                f'<td style="{td_style};color:#22c55e;">{effective}</td>'
                f'<td style="{td_style}">{outage_cell}</td>'
                f'<td style="{td_style}">{resolved}</td>'
                f'<td style="{td_style}">{eff_cell}</td>'
                f'<td style="{td_style}">{err_str}</td>'
                f'</tr>'
            )
        sections.append('</table>')

    # ── Footer note ───────────────────────────────────────────────────────────
    sections.append(
        f'<p style="font-size:10px;color:#94a3b8;margin-top:8px;">'
        f'Data from {data["since"]} → today &nbsp;·&nbsp; '
        f'Miss rate: warn ≥{MISS_RATE_WARN:.0f}% crit ≥{MISS_RATE_CRIT:.0f}% &nbsp;·&nbsp; '
        f'Waste rate: warn ≥{WASTED_RATE_WARN:.0f}% crit ≥{WASTED_RATE_CRIT:.0f}% &nbsp;·&nbsp; '
        f'Score oscillation (14-day stddev): warn ≥{OSCILLATION_WARN} crit ≥{OSCILLATION_CRIT}'
        f'</p>'
    )

    return "".join(sections)


def _purge_old_digests():
    """
    Delete PDF digest files older than JOB_MONITOR_PDF_RETENTION days.
    Runs once per digest cycle, just before the new PDF is written.
    Silently skips missing files or permission errors so a cleanup failure
    never blocks the digest from being sent.
    """
    import glob as _glob
    from datetime import timedelta

    cutoff = datetime.now() - timedelta(days=JOB_MONITOR_PDF_RETENTION)
    pattern = os.path.join(DIGESTS_DIR, "jobs_digest_*.pdf")
    deleted = 0

    for pdf_file in _glob.glob(pattern):
        basename = os.path.basename(pdf_file)
        # Extract date from filename: jobs_digest_YYYY-MM-DD.pdf
        try:
            date_part = basename[len("jobs_digest_"):-len(".pdf")]
            file_date = datetime.strptime(date_part, "%Y-%m-%d")
        except ValueError:
            continue   # filename doesn't match expected pattern — leave it

        if file_date < cutoff:
            try:
                os.remove(pdf_file)
                deleted += 1
            except OSError as exc:
                print(f"[WARN] Could not delete old digest {basename}: {exc}")

    if deleted:
        print(f"[OK] Purged {deleted} digest PDF(s) older than "
              f"{JOB_MONITOR_PDF_RETENTION} days")


def build_monitor_report(new_postings, stats, alerts):
    """
    Build PDF digest and send as email attachment.

    new_postings: list of job dicts from job_postings table
    stats:        dict from job_monitor.run()
    alerts:       list of alert dicts from _build_alerts()

    Returns: {"pdf_generated": bool, "email_sent": bool}
    """
    try:
        date_str = datetime.now().strftime("%B %-d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    os.makedirs(DIGESTS_DIR, exist_ok=True)
    pdf_path = os.path.join(
        DIGESTS_DIR,
        f"jobs_digest_{datetime.now().strftime('%Y-%m-%d')}.pdf"
    )

    # ── Purge old digest PDFs (retention policy) ──────────────────────────────
    _purge_old_digests()

    # ── Align stats so PDF header matches email body ───────────────────────────
    # stats["new_jobs_found"] counts only jobs discovered in the foreground
    # --monitor-jobs fallback scan.  With the background scheduler running,
    # most jobs arrive via scan_worker/detail_worker before --monitor-jobs
    # runs, so new_jobs_found << len(new_postings).  Override it here so the
    # PDF header and the email subject/body all show the same number.
    stats = dict(stats)   # don't mutate caller's dict
    stats["new_jobs_found"] = len(new_postings)

    # ── Group jobs by company ──
    jobs_by_company = defaultdict(list)
    for job in new_postings:
        jobs_by_company[job["company"]].append(job)
    # Sort jobs within each company by score DESC
    for company in jobs_by_company:
        jobs_by_company[company].sort(
            key=lambda j: j.get("skill_score", 0), reverse=True
        )

    styles = _get_styles()
    story  = []

    # ── Page 1: Header + Health + Top Matches ──
    story.append(_build_header_table(date_str, stats, styles))
    story.append(Spacer(1, 16))

    # Pipeline health
    for el in _build_health_section(stats, alerts, styles):
        story.append(el)

    story.append(Spacer(1, 16))

    # Top matches
    for el in _build_top_matches(new_postings, styles):
        story.append(el)

    story.append(PageBreak())

    # ── Page 2+: Job listings grouped by company ──
    story.append(Paragraph(
        f"All New Jobs — {date_str}", styles["section"]
    ))
    story.append(HRFlowable(
        width="100%", thickness=1,
        color=colors.HexColor("#e2e8f0"), spaceAfter=12
    ))

    for el in _build_job_listings(jobs_by_company, styles):
        story.append(el)

    # ── Build PDF ──
    try:
        doc = SimpleDocTemplate(
            pdf_path,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch,
        )
        doc.build(story)
        print(f"[OK] PDF saved: {pdf_path}")
        pdf_generated = True
    except Exception as e:
        print(f"[ERROR] PDF build failed: {e}")
        return {"pdf_generated": False, "email_sent": False}

    # ── Send email with PDF attachment ──
    email_sent = _send_digest_email(
        pdf_path, date_str, len(new_postings), alerts, stats
    )

    return {"pdf_generated": pdf_generated, "email_sent": email_sent}


def _send_digest_email(pdf_path, date_str, job_count, alerts, stats):
    """Send PDF digest as email attachment."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    from config import EMAIL, APP_PASSWORD

    if not EMAIL or not APP_PASSWORD:
        print("[WARNING] Email not configured — PDF saved locally only.")
        return False

    # Build subject
    alert_flag = " ⚠" if any(
        a["level"] in ("warning", "error") for a in alerts
    ) else ""
    subject = (
        f"[Digest] Job Digest · {date_str} · "
        f"{job_count} new jobs{alert_flag}"
    )

    # Brief HTML body
    _worker_covered = stats.get("covered_by_workers", 0)
    _fallback_hits  = stats.get("fallback_scanned", stats.get("companies_with_results", 0))
    _in_flight      = stats.get("in_flight", 0)
    _total_covered  = _worker_covered + _fallback_hits + _in_flight
    _total          = stats.get("companies_monitored", 0)
    _cov_pct        = int(_total_covered / _total * 100) if _total else 0
    _cov_detail = f"{_worker_covered} by workers"
    if _fallback_hits:
        _fb_with_jobs = stats.get("companies_with_results", 0)
        _fb_empty     = _fallback_hits - _fb_with_jobs
        _breakdown    = f"{_fb_with_jobs} with jobs"
        if _fb_empty:
            _breakdown += f", {_fb_empty} empty"
        _cov_detail += f", {_fallback_hits} by fallback ({_breakdown})"
    coverage = f"{_total_covered}/{_total} ({_cov_pct}%)"

    body_html = f"""
    <html><body style="font-family:sans-serif;padding:24px;
                       color:#1e293b;">
      <h2 style="color:#0f172a;">Job Digest — {date_str}</h2>
      <p><strong>{job_count}</strong> new jobs matching your profile.</p>
      <p style="color:#64748b;font-size:13px;">
        Coverage: {coverage} &mdash; {_cov_detail} &nbsp;&bull;&nbsp;
        See attached PDF for full details.
      </p>
      {"".join(
        f'<p style="color:#b45309;background:#fef3c7;'
        f'padding:8px;border-radius:4px;font-size:12px;">'
        f'⚠ {html_lib.escape(str(a["message"]))}</p>'
        for a in alerts if a["level"] in ("warning","error")
      )}
      {_build_queue_health_section()}
      {_build_api_warning_section()}
      {_build_adaptive_health_section() if datetime.now().weekday() == 0 else ""}
    </body></html>
    """

    try:
        from email.header import Header
        msg = MIMEMultipart("mixed")
        msg["From"]    = EMAIL
        msg["To"]      = EMAIL
        msg["Subject"] = Header(subject, "utf-8")
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        # Attach PDF
        with open(pdf_path, "rb") as f:
            pdf_part = MIMEApplication(f.read(), _subtype="pdf")
            pdf_part.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(pdf_path)
            )
            msg.attach(pdf_part)

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(EMAIL, APP_PASSWORD)
            server.send_message(msg)

        print(f"[OK] Digest email sent: {subject}")
        return True

    except Exception as e:
        print(f"[WARNING] Email failed: {e}")
        return False