# outreach/report_templates/outreach_report.py
# HTML email report for --outreach-only pipeline run

from outreach.report_templates.base import (
    COLORS, _base, stat_card, stat_row, section_header,
    badge, table_row, table_header_row, alert_box, info_box,
    send_report_email,
)


def build_outreach_report(stats):
    """
    Build and send HTML outreach report email.

    stats dict:
      date               str   — e.g. "March 4, 2026"
      sent               int
      failed             int
      bounced            int
      skipped            int
      emails             list  — [{name, company, stage, status}]
      active_sequences   int
      completed_sequences int
      pending_reply      int
    """
    date_str = stats.get("date", "")
    sent     = stats.get("sent", 0)
    failed   = stats.get("failed", 0)
    bounced  = stats.get("bounced", 0)
    skipped  = stats.get("skipped", 0)
    emails   = stats.get("emails", [])

    active_seq    = stats.get("active_sequences", 0)
    completed_seq = stats.get("completed_sequences", 0)
    pending_reply = stats.get("pending_reply", 0)

    # ── Subject ──
    issues = []
    if bounced:
        issues.append(f"⚠ {bounced} bounced")
    if failed:
        issues.append(f"⚠ {failed} failed")
    issue_str = " | " + " | ".join(issues) if issues else ""
    subject = f"📧 Outreach Report · {date_str} · Sent: {sent}{issue_str}"

    # ── Stat cards ──
    cards = stat_row([
        stat_card(sent,    "Sent",    COLORS["success"]),
        stat_card(failed,  "Failed",  COLORS["danger"] if failed else COLORS["neutral"]),
        stat_card(bounced, "Bounced", COLORS["danger"] if bounced else COLORS["neutral"]),
        stat_card(skipped, "Skipped", COLORS["warning"] if skipped else COLORS["neutral"]),
    ])

    # ── Emails table ──
    stage_colors = {
        "initial":  COLORS["accent"],
        "followup1": COLORS["warning"],
        "followup2": COLORS["neutral"],
    }
    status_colors = {
        "sent":    COLORS["success"],
        "failed":  COLORS["danger"],
        "bounced": COLORS["danger"],
        "skipped": COLORS["neutral"],
    }

    email_rows = ""
    if emails:
        email_rows = f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-radius:8px;overflow:hidden;
                      border:1px solid {COLORS['border']};">
          {table_header_row(["Recruiter", "Company", "Stage", "Status"])}
          {"".join(
            table_row([
                e.get("name", "—"),
                e.get("company", "—"),
                badge(e.get("stage", "—"), stage_colors.get(e.get("stage",""), COLORS["neutral"])),
                badge(e.get("status", "—"), status_colors.get(e.get("status",""), COLORS["neutral"])),
            ], bg=COLORS["row_alt"] if i % 2 else COLORS["card"])
            for i, e in enumerate(emails)
          )}
        </table>"""
    else:
        email_rows = info_box("No emails sent in this run.")

    # ── Sequence status ──
    seq_html = f"""
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="margin-top:8px;">
      <tr>
        <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                   border:1px solid {COLORS['border']};text-align:center;">
          <div style="font-size:22px;font-weight:800;color:{COLORS['accent']};">
            {active_seq}
          </div>
          <div style="font-size:11px;color:{COLORS['subtext']};
                      text-transform:uppercase;letter-spacing:0.5px;">
            Active
          </div>
        </td>
        <td style="width:12px;"></td>
        <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                   border:1px solid {COLORS['border']};text-align:center;">
          <div style="font-size:22px;font-weight:800;color:{COLORS['success']};">
            {completed_seq}
          </div>
          <div style="font-size:11px;color:{COLORS['subtext']};
                      text-transform:uppercase;letter-spacing:0.5px;">
            Completed
          </div>
        </td>
        <td style="width:12px;"></td>
        <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                   border:1px solid {COLORS['border']};text-align:center;">
          <div style="font-size:22px;font-weight:800;color:{COLORS['warning']};">
            {pending_reply}
          </div>
          <div style="font-size:11px;color:{COLORS['subtext']};
                      text-transform:uppercase;letter-spacing:0.5px;">
            Awaiting Reply
          </div>
        </td>
      </tr>
    </table>"""

    # ── Alerts ──
    alerts = ""
    if bounced:
        alerts += alert_box(
            f"⚠ {bounced} hard bounce(s) detected — recruiters marked inactive.",
            COLORS["danger"]
        )
    if failed:
        alerts += alert_box(
            f"⚠ {failed} email(s) failed to send — check SMTP settings.",
            COLORS["warning"]
        )

    body = f"""
    {alerts}
    {cards}
    {section_header("Emails Sent This Run")}
    {email_rows}
    {section_header("Sequence Status")}
    {seq_html}
    """

    html = _base(
        title="Outreach Report",
        icon="📧",
        subject_line=subject,
        body_html=body,
        date_str=date_str,
    )

    send_report_email(subject, html)
    return stats