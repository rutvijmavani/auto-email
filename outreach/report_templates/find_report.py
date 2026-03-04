# outreach/report_templates/find_report.py
# HTML email report for --find-only pipeline run

from outreach.report_templates.base import (
    COLORS, _base, stat_card, stat_row, section_header,
    badge, table_row, table_header_row, alert_box, info_box,
    send_report_email,
)


def build_find_report(stats):
    """
    Build and send HTML find report email.

    stats dict:
      date                 str
      quota_used           int
      quota_total          int
      companies            list — [{name, status, count, domain}]
      prospective_scraped  int
      prospective_exhausted int
      ai_generated         int
      ai_cached            int
      ai_failed            int
    """
    date_str  = stats.get("date", "")
    used      = stats.get("quota_used", 0)
    total     = stats.get("quota_total", 50)
    remaining = total - used
    companies = stats.get("companies", [])

    found     = sum(1 for c in companies if c.get("status") == "found")
    exhausted = sum(1 for c in companies if c.get("status") == "exhausted")
    skipped   = sum(1 for c in companies if c.get("status") == "skipped")

    p_scraped   = stats.get("prospective_scraped", 0)
    p_exhausted = stats.get("prospective_exhausted", 0)
    ai_gen      = stats.get("ai_generated", 0)
    ai_cached   = stats.get("ai_cached", 0)
    ai_failed   = stats.get("ai_failed", 0)

    # ── Subject ──
    issues = []
    if ai_failed:
        issues.append(f"⚠ {ai_failed} AI failed")
    if exhausted:
        issues.append(f"⚠ {exhausted} exhausted")
    issue_str = " | " + " | ".join(issues) if issues else ""
    subject = (
        f"🔍 Find Report · {date_str} · "
        f"Found: {found} | Quota: {used}/{total}{issue_str}"
    )

    # ── Quota bar ──
    pct = int((used / total) * 100) if total else 0
    bar_color = (
        COLORS["danger"]  if pct >= 90 else
        COLORS["warning"] if pct >= 60 else
        COLORS["success"]
    )
    quota_bar = f"""
    <div style="margin-bottom:24px;">
      <div style="display:flex;justify-content:space-between;
                  margin-bottom:6px;">
        <span style="font-size:13px;font-weight:600;
                     color:{COLORS['text']};">
          CareerShift Quota
        </span>
        <span style="font-size:13px;color:{COLORS['subtext']};">
          {used} used · {remaining} remaining
        </span>
      </div>
      <div style="background:{COLORS['border']};border-radius:999px;
                  height:10px;overflow:hidden;">
        <div style="background:{bar_color};width:{pct}%;height:100%;
                    border-radius:999px;transition:width 0.3s;">
        </div>
      </div>
    </div>"""

    # ── Stat cards ──
    cards = stat_row([
        stat_card(found,     "Found",     COLORS["success"]),
        stat_card(exhausted, "Exhausted", COLORS["danger"] if exhausted else COLORS["neutral"]),
        stat_card(skipped,   "Skipped",   COLORS["warning"] if skipped else COLORS["neutral"]),
        stat_card(used,      "Quota Used",COLORS["accent"]),
    ])

    # ── Companies table ──
    status_colors = {
        "found":     COLORS["success"],
        "exhausted": COLORS["danger"],
        "skipped":   COLORS["warning"],
    }
    status_labels = {
        "found":     "Found",
        "exhausted": "Exhausted",
        "skipped":   "Skipped",
    }

    if companies:
        company_rows = f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-radius:8px;overflow:hidden;
                      border:1px solid {COLORS['border']};">
          {table_header_row(["Company", "Status", "Recruiters Found"])}
          {"".join(
            table_row([
                f'<strong>{c.get("name","—")}</strong>',
                badge(
                    status_labels.get(c.get("status",""), c.get("status","—")),
                    status_colors.get(c.get("status",""), COLORS["neutral"])
                ),
                str(c.get("count", 0)),
            ], bg=COLORS["row_alt"] if i % 2 else COLORS["card"])
            for i, c in enumerate(companies)
          )}
        </table>"""
    else:
        company_rows = info_box("No companies scraped in this run.")

    # ── Prospective section ──
    prospective_html = ""
    if p_scraped or p_exhausted:
        prospective_html = f"""
        {section_header("Prospective Companies")}
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                       border:1px solid {COLORS['border']};text-align:center;">
              <div style="font-size:22px;font-weight:800;
                          color:{COLORS['success']};">{p_scraped}</div>
              <div style="font-size:11px;color:{COLORS['subtext']};
                          text-transform:uppercase;letter-spacing:0.5px;">
                Pre-scraped
              </div>
            </td>
            <td style="width:12px;"></td>
            <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                       border:1px solid {COLORS['border']};text-align:center;">
              <div style="font-size:22px;font-weight:800;
                          color:{COLORS['danger']};">{p_exhausted}</div>
              <div style="font-size:11px;color:{COLORS['subtext']};
                          text-transform:uppercase;letter-spacing:0.5px;">
                Exhausted
              </div>
            </td>
          </tr>
        </table>"""

    # ── AI content section ──
    ai_html = f"""
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="margin-top:8px;">
      <tr>
        {stat_card(ai_gen,    "Generated", COLORS["success"])}
        {stat_card(ai_cached, "Cached",    COLORS["accent"])}
        {stat_card(ai_failed, "Failed",    COLORS["danger"] if ai_failed else COLORS["neutral"])}
      </tr>
    </table>"""

    # ── Alerts ──
    alerts = ""
    if ai_failed:
        alerts += alert_box(
            f"⚠ {ai_failed} AI generation(s) failed — quota may be exhausted.",
            COLORS["warning"]
        )

    body = f"""
    {alerts}
    {quota_bar}
    {cards}
    {section_header("Companies Scraped")}
    {company_rows}
    {prospective_html}
    {section_header("AI Email Content")}
    {ai_html}
    """

    html = _base(
        title="Find Report",
        icon="🔍",
        subject_line=subject,
        body_html=body,
        date_str=date_str,
    )

    send_report_email(subject, html)
    return stats