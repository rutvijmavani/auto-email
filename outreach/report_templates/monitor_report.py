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
    coverage_pct = (
        int(stats.get("companies_with_results", 0) / total * 100)
        if total else 0
    )
    ats_pct = int(known_ats / total * 100) if total else 0

    health_data = [
        ["Metric",          "Value",          "Status"],
        ["Coverage",
         f"{stats.get('companies_with_results',0)}/{total} ({coverage_pct}%)",
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
        f"📋 Job Digest · {date_str} · "
        f"{job_count} new jobs{alert_flag}"
    )

    # Brief HTML body
    coverage = (
        f"{stats.get('companies_with_results',0)}/"
        f"{stats.get('companies_monitored',0)}"
    )
    body_html = f"""
    <html><body style="font-family:sans-serif;padding:24px;
                       color:#1e293b;">
      <h2 style="color:#0f172a;">Job Digest — {date_str}</h2>
      <p><strong>{job_count}</strong> new jobs matching your profile.</p>
      <p style="color:#64748b;font-size:13px;">
        Coverage: {coverage} companies &nbsp;&bull;&nbsp;
        See attached PDF for full details.
      </p>
      {"".join(
        f'<p style="color:#b45309;background:#fef3c7;'
        f'padding:8px;border-radius:4px;font-size:12px;">'
        f'⚠ {html_lib.escape(str(a["message"]))}</p>'
        for a in alerts if a["level"] in ("warning","error")
      )}
    </body></html>
    """

    try:
        msg = MIMEMultipart("mixed")
        msg["From"]    = EMAIL
        msg["To"]      = EMAIL
        msg["Subject"] = subject
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