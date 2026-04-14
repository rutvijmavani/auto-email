# outreach/report_templates/weekly_summary.py
# Monday 9 AM weekly summary email
# Combines: find + verify + outreach + api_health + enrichment progress

from datetime import datetime, date, timedelta
from outreach.report_templates.base import (
    COLORS, FONT, _base, stat_card, stat_row,
    section_header, badge, table_row, table_header_row,
    alert_box, info_box, send_report_email,
)


def build_weekly_summary():
    """
    Build and send Monday weekly summary email.
    Pulls last 7 days of data from DB.
    Replaces separate find/verify/outreach emails.
    """
    today    = date.today()
    week_ago = today - timedelta(days=7)
    date_str = today.strftime("%B %d, %Y")
    period   = (
        f"{week_ago.strftime('%b %d')} - "
        f"{today.strftime('%b %d, %Y')}"
    )

    # ── Pull all data ──────────────────────────────────
    find_stats     = _get_find_stats(week_ago)
    verify_stats   = _get_verify_stats(week_ago)
    outreach_stats = _get_outreach_stats(week_ago)
    health_summary = _get_health_summary()
    enrich_stats   = _get_enrich_stats()
    detect_stats   = _get_detect_stats()

    # ── Subject ───────────────────────────────────────
    issues = []
    if health_summary.get("platforms_with_429"):
        issues.append(
            f"⚠ {health_summary['platforms_with_429']} "
            f"platform(s) rate limited"
        )
    if verify_stats.get("inactive", 0):
        issues.append(
            f"⚠ {verify_stats['inactive']} inactive recruiters"
        )
    issue_str = " | " + " | ".join(issues) if issues else ""
    subject = (
        f"📊 Weekly Summary · {period}"
        f"{issue_str}"
    )

    # ── Section: Recruiter Finding ────────────────────
    find_html = _build_find_section(find_stats)

    # ── Section: Verification ─────────────────────────
    verify_html = _build_verify_section(verify_stats)

    # ── Section: Outreach ─────────────────────────────
    outreach_html = _build_outreach_section(outreach_stats)

    # ── Section: API Health ───────────────────────────
    health_html = _build_health_section(health_summary)

    # ── Section: Enrichment ───────────────────────────
    enrich_html = _build_enrich_section(enrich_stats)

    # ── Section: Detection Queue ──────────────────────
    detect_html = _build_detect_section(detect_stats)

    # ── Alerts ────────────────────────────────────────
    alerts = ""
    if health_summary.get("platforms_with_429"):
        alerts += alert_box(
            f"⚠ {health_summary['platforms_with_429']} "
            f"platform(s) experienced rate limiting this week. "
            f"Check API Health section below.",
            COLORS["warning"]
        )
    if verify_stats.get("inactive", 0):
        alerts += alert_box(
            f"⚠ {verify_stats['inactive']} recruiter(s) went "
            f"inactive this week — outreach cancelled.",
            COLORS["danger"]
        )

    body = f"""
    {alerts}

    <!-- Period header -->
    <tr><td style="padding:0 32px 8px;">
      <div style="font-size:13px;color:{COLORS['subtext']};
                  text-align:center;">
        Week of {period}
      </div>
    </td></tr>

    {find_html}
    {verify_html}
    {outreach_html}
    {health_html}
    {enrich_html}
    {detect_html}
    """

    html = _base(
        title="Weekly Summary",
        icon="📊",
        subject_line=subject,
        body_html=body,
        date_str=date_str,
    )
    send_report_email(subject, html)
    print(f"[OK] Weekly summary sent for {period}")


# ─────────────────────────────────────────
# DATA FETCHERS
# ─────────────────────────────────────────

def _get_find_stats(since):
    """Aggregate find stats from last 7 days."""
    try:
        from db.connection import get_conn
        conn = get_conn()
        try:
            # Total new recruiters found this week
            row = conn.execute("""
                SELECT
                    COUNT(*) AS total_found,
                    COUNT(DISTINCT company) AS companies_found
                FROM recruiters
                WHERE created_at >= ?
                AND recruiter_status = 'active'
            """, (since.isoformat(),)).fetchone()
            return {
                "total_found":     row["total_found"]     if row else 0,
                "companies_found": row["companies_found"] if row else 0,
            }
        finally:
            conn.close()
    except Exception:
        return {"total_found": 0, "companies_found": 0}


def _get_verify_stats(since):
    """Aggregate verification stats from last 7 days."""
    try:
        from db.connection import get_conn
        conn = get_conn()
        try:
            # Recruiters verified this week
            verified = conn.execute("""
                SELECT COUNT(*) FROM recruiters
                WHERE verified_at >= ?
            """, (since.isoformat(),)).fetchone()[0]

            # Recruiters gone inactive this week
            inactive = conn.execute("""
                SELECT COUNT(*) FROM recruiters
                WHERE recruiter_status = 'inactive'
                AND verified_at >= ?
            """, (since.isoformat(),)).fetchone()[0]

            return {
                "verified": verified or 0,
                "inactive": inactive or 0,
            }
        finally:
            conn.close()
    except Exception:
        return {"verified": 0, "inactive": 0}


def _get_outreach_stats(since):
    """Aggregate outreach stats from last 7 days."""
    try:
        from db.connection import get_conn
        conn = get_conn()
        try:
            row = conn.execute("""
                SELECT
                    SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END)
                        AS sent,
                    SUM(CASE WHEN status='bounced' THEN 1 ELSE 0 END)
                        AS bounced,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END)
                        AS failed,
                    SUM(CASE WHEN replied=1 THEN 1 ELSE 0 END)
                        AS replies,
                    COUNT(DISTINCT CASE
                        WHEN stage='initial' AND status='sent'
                        THEN recruiter_id END)
                        AS new_sequences
                FROM outreach
                WHERE sent_at >= ?
            """, (since.isoformat(),)).fetchone()

            return {
                "sent":          row["sent"]          or 0,
                "bounced":       row["bounced"]        or 0,
                "failed":        row["failed"]         or 0,
                "replies":       row["replies"]        or 0,
                "new_sequences": row["new_sequences"]  or 0,
            }
        finally:
            conn.close()
    except Exception:
        return {
            "sent": 0, "bounced": 0, "failed": 0,
            "replies": 0, "new_sequences": 0
        }


def _get_health_summary():
    """Get 7-day API health summary."""
    try:
        from db.api_health import get_health_summary
        rows = get_health_summary(days=7)
        platforms_with_429 = sum(
            1 for r in rows if r.get("rate_429_pct", 0) > 0
        )
        return {
            "rows":               rows,
            "platforms_with_429": platforms_with_429,
        }
    except Exception:
        return {"rows": [], "platforms_with_429": 0}


def _get_enrich_stats():
    """Get enrichment progress stats."""
    try:
        from db.ats_companies import get_discovery_conn
        conn = get_discovery_conn()
        try:
            row = conn.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN is_enriched=1
                        THEN 1 ELSE 0 END) AS enriched,
                    SUM(CASE WHEN is_enriched=0
                        THEN 1 ELSE 0 END) AS pending
                FROM ats_companies
                WHERE is_active = 1
            """).fetchone()
            total    = row["total"]    or 0
            enriched = row["enriched"] or 0
            pct      = round(
                100.0 * enriched / total, 1
            ) if total > 0 else 0
            return {
                "total":    total,
                "enriched": enriched,
                "pending":  row["pending"] or 0,
                "pct":      pct,
            }
        finally:
            conn.close()
    except Exception:
        return {
            "total": 0, "enriched": 0,
            "pending": 0, "pct": 0
        }


def _get_detect_stats():
    """Get detection queue stats."""
    try:
        from db.job_monitor import get_detection_queue_stats
        return get_detection_queue_stats()
    except Exception:
        return {}


# ─────────────────────────────────────────
# SECTION BUILDERS
# ─────────────────────────────────────────

def _build_find_section(stats):
    found    = stats.get("total_found", 0)
    companies = stats.get("companies_found", 0)
    return f"""
    {section_header("🔍 Recruiter Finding (7 days)")}
    <tr><td style="padding:0 32px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          {stat_card(found, "New Recruiters", COLORS["success"])}
          {stat_card(companies, "Companies Found", COLORS["accent"])}
        </tr>
      </table>
    </td></tr>
    """


def _build_verify_section(stats):
    verified = stats.get("verified", 0)
    inactive = stats.get("inactive", 0)
    return f"""
    {section_header("🔄 Recruiter Verification (7 days)")}
    <tr><td style="padding:0 32px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          {stat_card(verified, "Verified", COLORS["success"])}
          {stat_card(
              inactive, "Gone Inactive",
              COLORS["danger"] if inactive else COLORS["neutral"]
          )}
        </tr>
      </table>
    </td></tr>
    """


def _build_outreach_section(stats):
    sent          = stats.get("sent", 0)
    replies       = stats.get("replies", 0)
    bounced       = stats.get("bounced", 0)
    new_sequences = stats.get("new_sequences", 0)
    reply_rate    = (
        round(100.0 * replies / sent, 1) if sent > 0 else 0
    )
    return f"""
    {section_header("📧 Outreach (7 days)")}
    <tr><td style="padding:0 32px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          {stat_card(sent, "Emails Sent", COLORS["success"])}
          {stat_card(replies, f"Replies ({reply_rate}%)",
                     COLORS["accent"])}
          {stat_card(new_sequences, "New Sequences",
                     COLORS["warning"])}
          {stat_card(
              bounced, "Bounced",
              COLORS["danger"] if bounced else COLORS["neutral"]
          )}
        </tr>
      </table>
    </td></tr>
    """


def _build_health_section(summary):
    rows = summary.get("rows", [])
    if not rows:
        return f"""
        {section_header("🏥 API Health (7 days)")}
        <tr><td style="padding:0 32px 16px;">
          {info_box("No API health data yet — "
                    "will populate after first monitoring run.")}
        </td></tr>
        """

    # Status indicator
    def _status(rate_429, error_pct, avg_ms):
        if rate_429 >= 10 or error_pct >= 20:
            return f'<span style="color:{COLORS["danger"]};">🔴</span>'
        if rate_429 >= 2 or error_pct >= 5 or avg_ms > 3000:
            return f'<span style="color:{COLORS["warning"]};">🟡</span>'
        return f'<span style="color:{COLORS["success"]};">🟢</span>'

    table_rows = "".join(
        table_row([
            f'<strong>{r["platform"]}</strong>',
            f'{r["total_requests"]:,}',
            (f'<span style="color:{COLORS["danger"]};'
             f'font-weight:600;">{r["rate_429_pct"]}%</span>'
             if r["rate_429_pct"] > 0
             else "0%"),
            (f'<span style="color:{COLORS["warning"]};">'
             f'{r["error_pct"]}%</span>'
             if r["error_pct"] > 0 else "0%"),
            f'{r["avg_response_ms"]}ms',
            _status(
                r["rate_429_pct"],
                r["error_pct"],
                r["avg_response_ms"]
            ),
        ], bg=COLORS["row_alt"] if i % 2 else COLORS["card"])
        for i, r in enumerate(rows)
    )

    return f"""
    {section_header("🏥 API Health (7 days)")}
    <tr><td style="padding:0 32px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border-radius:8px;overflow:hidden;
                    border:1px solid {COLORS['border']};">
        {table_header_row([
            "Platform", "Requests", "429 Rate",
            "Error Rate", "Avg Response", "Status"
        ])}
        {table_rows}
      </table>
      <div style="font-size:11px;color:{COLORS['subtext']};
                  margin-top:8px;">
        🟢 Healthy &nbsp;&nbsp;
        🟡 Watch &nbsp;&nbsp;
        🔴 Action needed
      </div>
    </td></tr>
    """


def _build_enrich_section(stats):
    total    = stats.get("total", 0)
    enriched = stats.get("enriched", 0)
    pending  = stats.get("pending", 0)
    pct      = stats.get("pct", 0)

    bar_color = (
        COLORS["success"] if pct >= 75 else
        COLORS["warning"] if pct >= 25 else
        COLORS["accent"]
    )

    return f"""
    {section_header("🗄️ ATS Slug Enrichment")}
    <tr><td style="padding:0 32px 16px;">
      <div style="margin-bottom:12px;">
        <div style="display:flex;justify-content:space-between;
                    margin-bottom:6px;">
          <span style="font-size:13px;font-weight:600;
                       color:{COLORS['text']};">
            Enrichment Progress
          </span>
          <span style="font-size:13px;color:{COLORS['subtext']};">
            {enriched:,} / {total:,} ({pct}%)
          </span>
        </div>
        <div style="background:{COLORS['border']};
                    border-radius:999px;height:10px;
                    overflow:hidden;">
          <div style="background:{bar_color};width:{pct}%;
                      height:100%;border-radius:999px;">
          </div>
        </div>
      </div>
      <div style="font-size:12px;color:{COLORS['subtext']};">
        {pending:,} slugs pending enrichment
      </div>
    </td></tr>
    """


def _build_detect_section(stats):
    p1 = stats.get("priority1_new", 0) or 0
    p2 = stats.get("priority2_quiet", 0) or 0
    p3 = stats.get("priority3_unknown", 0) or 0
    p4 = stats.get("priority4_custom_nocurl", 0) or 0
    total = p1 + p2 + p3 + p4

    if total == 0:
        return f"""
        {section_header("🎯 Detection Queue")}
        <tr><td style="padding:0 32px 16px;">
          {info_box("✅ Detection queue empty — "
                    "all companies detected.")}
        </td></tr>
        """

    days_est = max(1, total // 10)
    return f"""
    {section_header("🎯 Detection Queue")}
    <tr><td style="padding:0 32px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          {stat_card(p1, "New", COLORS["accent"])}
          {stat_card(p2, "Gone Quiet", COLORS["warning"])}
          {stat_card(p3, "Unknown", COLORS["neutral"])}
          {stat_card(total, "Total", COLORS["text"])}
        </tr>
      </table>
      <div style="font-size:12px;color:{COLORS['subtext']};
                  margin-top:8px;">
        Est. {days_est} day(s) to clear at 10/day
      </div>
    </td></tr>
    """