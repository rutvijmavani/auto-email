# outreach/report_templates/verify_report.py
# HTML email report for --verify-only pipeline run

from outreach.report_templates.base import (
    COLORS, _base, stat_card, stat_row, section_header,
    badge, table_row, table_header_row, alert_box, info_box,
    send_report_email,
)


def build_verify_report(stats):
    """
    Build and send HTML verification report email.

    stats dict:
      date             str
      tier1_count      int  — skipped (trusted)
      tier2_count      int  — lightweight checked
      tier2_verified   int  — passed Tier 2
      tier3_count      int  — full profile visited
      tier3_verified   int  — still active
      tier3_inactive   int  — marked inactive
      changes          list — [{name, company, action}]
      under_stocked    list — [{company, active_count, needed}]
    """
    date_str      = stats.get("date", "")
    tier1         = stats.get("tier1_count", 0)
    tier2         = stats.get("tier2_count", 0)
    tier2_ok      = stats.get("tier2_verified", 0)
    tier3         = stats.get("tier3_count", 0)
    tier3_ok      = stats.get("tier3_verified", 0)
    tier3_inactive= stats.get("tier3_inactive", 0)
    changes       = stats.get("changes", [])
    under_stocked = stats.get("under_stocked", [])

    total_checked = tier2 + tier3
    total_inactive = tier3_inactive

    # ── Subject ──
    issues = []
    if total_inactive:
        issues.append(f"⚠ {total_inactive} inactive")
    if under_stocked:
        issues.append(f"⚠ {len(under_stocked)} under-stocked")
    issue_str = " | " + " | ".join(issues) if issues else ""
    subject = (
        f"🔄 Verify Report · {date_str} · "
        f"Checked: {total_checked}{issue_str}"
    )

    # ── Tier summary cards ──
    cards = stat_row([
        stat_card(tier1, "Tier 1\n(Trusted)",   COLORS["success"]),
        stat_card(tier2, "Tier 2\n(Checked)",   COLORS["accent"]),
        stat_card(tier3, "Tier 3\n(Visited)",   COLORS["warning"]),
        stat_card(
            total_inactive,
            "Inactive",
            COLORS["danger"] if total_inactive else COLORS["neutral"]
        ),
    ])

    # ── Tier breakdown ──
    tier_html = f"""
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="border-radius:8px;overflow:hidden;
                  border:1px solid {COLORS['border']};">
      {table_header_row(["Tier", "Count", "Result"])}
      {table_row([
          "Tier 1 — Trusted (&lt;30 days)",
          str(tier1),
          badge("Skipped", COLORS["neutral"]),
      ], bg=COLORS["row_alt"])}
      {table_row([
          "Tier 2 — Lightweight check (30-60 days)",
          str(tier2),
          badge(f"{tier2_ok} verified", COLORS["success"]) if tier2_ok
          else badge("0 verified", COLORS["neutral"]),
      ], bg=COLORS["card"])}
      {table_row([
          "Tier 3 — Full profile visit (&gt;60 days)",
          str(tier3),
          (badge(f"{tier3_ok} active", COLORS["success"]) +
           ("&nbsp;" + badge(f"{tier3_inactive} inactive", COLORS["danger"])
            if tier3_inactive else ""))
          if tier3 else badge("None", COLORS["neutral"]),
      ], bg=COLORS["row_alt"])}
    </table>"""

    # ── Changes table ──
    changes_html = ""
    if changes:
        changes_html = f"""
        {section_header("Recruiter Changes")}
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-radius:8px;overflow:hidden;
                      border:1px solid {COLORS['border']};">
          {table_header_row(["Recruiter", "Company", "Action"])}
          {"".join(
            table_row([
                c.get("name", "—"),
                c.get("company", "—"),
                badge(c.get("action", "—"), COLORS["danger"]),
            ], bg=COLORS["row_alt"] if i % 2 else COLORS["card"])
            for i, c in enumerate(changes)
          )}
        </table>"""

    # ── Under-stocked section ──
    under_html = ""
    if under_stocked:
        under_html = f"""
        {section_header("Under-stocked Companies")}
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-radius:8px;overflow:hidden;
                      border:1px solid {COLORS['border']};">
          {table_header_row(["Company", "Active Recruiters", "Needs"])}
          {"".join(
            table_row([
                f'<strong>{u.get("company","—")}</strong>',
                str(u.get("active_count", 0)),
                f'+{u.get("needed", 0)} more',
            ], bg=COLORS["row_alt"] if i % 2 else COLORS["card"])
            for i, u in enumerate(under_stocked)
          )}
        </table>
        {info_box("Run <strong>--find-only</strong> to top up under-stocked companies.")}"""

    # ── Alerts ──
    alerts = ""
    if total_inactive:
        alerts += alert_box(
            f"⚠ {total_inactive} recruiter(s) marked inactive — "
            f"their pending outreach has been cancelled.",
            COLORS["danger"]
        )
    if under_stocked:
        alerts += alert_box(
            f"⚠ {len(under_stocked)} company/companies are under-stocked. "
            f"Run --find-only to top them up.",
            COLORS["warning"]
        )

    body = f"""
    {alerts}
    {cards}
    {section_header("Verification Tier Breakdown")}
    {tier_html}
    {changes_html}
    {under_html}
    """

    html = _base(
        title="Verification Report",
        icon="🔄",
        subject_line=subject,
        body_html=body,
        date_str=date_str,
    )

    send_report_email(subject, html)
    return stats