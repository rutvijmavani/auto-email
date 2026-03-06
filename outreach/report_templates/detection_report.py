# outreach/report_templates/detection_report.py
# HTML email report for --detect-ats results
# Batches all close calls + unknowns into ONE summary email

import html as html_lib
from outreach.report_templates.base import (
    COLORS, _base, section_header, badge,
    table_row, table_header_row, alert_box, info_box,
    send_report_email,
)
from config import ATS_STATUS_CLOSE_CALL, ATS_STATUS_UNKNOWN, ATS_STATUS_CUSTOM


def build_detection_report(results, date_str):
    """
    Send ONE summary email after --detect-ats completes.

    results: list of dicts from detect_ats() per company
    {company, status, platform, slug, confidence, job_count,
     runner_up, best_attempt}

    Only sends email if there are close calls or unknowns.
    Silently skips if everything was cleanly detected.
    """
    close_calls = [r for r in results if r["status"] == ATS_STATUS_CLOSE_CALL]
    unknowns    = [r for r in results
                   if r["status"] in (ATS_STATUS_UNKNOWN, ATS_STATUS_CUSTOM)]
    detected    = [r for r in results
                   if r["status"] not in (ATS_STATUS_CLOSE_CALL,
                                          ATS_STATUS_UNKNOWN,
                                          ATS_STATUS_CUSTOM)]

    # Only send email if there's something to review
    if not close_calls and not unknowns:
        print(f"[OK] All {len(detected)} companies detected with high confidence — no email needed.")
        return False

    # ── Subject ──
    parts = [f"✅ {len(detected)} auto-detected"]
    if close_calls:
        parts.append(f"⚠ {len(close_calls)} close calls")
    if unknowns:
        parts.append(f"❌ {len(unknowns)} unknown")

    subject = f"🔍 ATS Detection Complete · {date_str} · {' | '.join(parts)}"

    # ── Summary stat cards ──
    summary_html = f"""
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="margin-bottom:24px;">
      <tr>
        <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                   border:1px solid {COLORS['border']};text-align:center;">
          <div style="font-size:28px;font-weight:800;
                      color:{COLORS['success']};">{len(detected)}</div>
          <div style="font-size:11px;color:{COLORS['subtext']};
                      text-transform:uppercase;letter-spacing:0.5px;">
            Auto-Detected
          </div>
        </td>
        <td style="width:12px;"></td>
        <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                   border:1px solid {COLORS['border']};text-align:center;">
          <div style="font-size:28px;font-weight:800;
                      color:{COLORS['warning']};">{len(close_calls)}</div>
          <div style="font-size:11px;color:{COLORS['subtext']};
                      text-transform:uppercase;letter-spacing:0.5px;">
            Close Calls
          </div>
        </td>
        <td style="width:12px;"></td>
        <td style="padding:12px;background:{COLORS['bg']};border-radius:8px;
                   border:1px solid {COLORS['border']};text-align:center;">
          <div style="font-size:28px;font-weight:800;
                      color:{COLORS['danger']};">{len(unknowns)}</div>
          <div style="font-size:11px;color:{COLORS['subtext']};
                      text-transform:uppercase;letter-spacing:0.5px;">
            Needs Review
          </div>
        </td>
      </tr>
    </table>"""

    # ── Close calls section ──
    close_calls_html = ""
    if close_calls:
        rows = ""
        for i, r in enumerate(close_calls):
            company   = html_lib.escape(r["company"])
            platform  = html_lib.escape(r["platform"] or "")
            slug      = html_lib.escape(r["slug"] or "")
            conf      = r["confidence"]
            jobs      = r["job_count"]
            runner_up = r.get("runner_up")

            # Build verify URLs
            verify_url  = _build_verify_url(r["platform"], r["slug"])
            runner_url  = _build_verify_url(
                runner_up["platform"], runner_up["slug"]
            ) if runner_up else ""

            override_cmd = (
                f'python pipeline.py --detect-ats "{r["company"]}" '
                f'--override {r["platform"] or ""} {r["slug"] or ""}'
            )

            runner_html = ""
            if runner_up:
                ru_url = html_lib.escape(runner_url)
                runner_html = f"""
                <tr style="background:{COLORS['row_alt']};">
                  <td colspan="2" style="padding:8px 12px;font-size:12px;
                                         color:{COLORS['subtext']};">
                    Runner-up: <strong>{html_lib.escape(runner_up['platform'])}</strong>
                    ({runner_up['confidence']}% conf, {runner_up['job_count']} jobs)
                    &nbsp;|&nbsp;
                    <a href="{ru_url}" style="color:{COLORS['accent']};">
                      Verify runner-up
                    </a>
                  </td>
                  <td colspan="2" style="padding:8px 12px;font-size:11px;
                                         color:{COLORS['subtext']};
                                         font-family:monospace;">
                    Override: {html_lib.escape(override_cmd)}
                  </td>
                </tr>"""

            safe_url = html_lib.escape(verify_url)
            bg = COLORS["row_alt"] if i % 2 else COLORS["card"]
            rows += f"""
            <tr style="background:{bg};">
              <td style="padding:10px 12px;font-size:13px;
                         font-weight:600;color:{COLORS['text']};
                         border-bottom:1px solid {COLORS['border']};">
                {company}
              </td>
              <td style="padding:10px 12px;font-size:12px;
                         color:{COLORS['text']};
                         border-bottom:1px solid {COLORS['border']};">
                {badge(platform, COLORS['accent'])}
                <span style="color:{COLORS['subtext']};margin-left:4px;">
                  {slug}
                </span>
              </td>
              <td style="padding:10px 12px;font-size:12px;
                         color:{COLORS['text']};
                         border-bottom:1px solid {COLORS['border']};">
                {conf}% conf &nbsp;|&nbsp; {jobs} jobs
              </td>
              <td style="padding:10px 12px;font-size:12px;
                         border-bottom:1px solid {COLORS['border']};">
                <a href="{safe_url}" style="color:{COLORS['accent']};">
                  Verify selected
                </a>
              </td>
            </tr>
            {runner_html}"""

        close_calls_html = f"""
        {section_header("Close Calls — Auto-Selected, Please Verify")}
        {alert_box(
            f"<strong>{len(close_calls)} company/companies</strong> had two "
            f"platforms with similar confidence scores. We auto-selected the "
            f"best one but please verify the links below.",
            COLORS["warning"]
        )}
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-radius:8px;overflow:hidden;
                      border:1px solid {COLORS['border']};">
          {table_header_row(["Company", "Selected ATS", "Score", "Action"])}
          {rows}
        </table>"""

    # ── Unknowns section ──
    unknowns_html = ""
    if unknowns:
        rows = ""
        for i, r in enumerate(unknowns):
            company = html_lib.escape(r["company"])
            best    = r.get("best_attempt")
            bg      = COLORS["row_alt"] if i % 2 else COLORS["card"]

            if best:
                best_platform = html_lib.escape(best.get("platform", ""))
                best_slug     = html_lib.escape(str(best.get("slug", "")))
                best_conf     = best.get("confidence", 0)
                best_jobs     = best.get("job_count", 0)
                attempt_html  = (
                    f"{best_platform} / {best_slug} "
                    f"({best_conf}% conf, {best_jobs} jobs)"
                )
            else:
                attempt_html = "No match found"

            override_cmd = (
                f'python pipeline.py --detect-ats "{r["company"]}" '
                f'--override &lt;ats&gt; &lt;slug&gt;'
            )

            rows += f"""
            <tr style="background:{bg};">
              <td style="padding:10px 12px;font-size:13px;font-weight:600;
                         color:{COLORS['text']};
                         border-bottom:1px solid {COLORS['border']};">
                {company}
              </td>
              <td style="padding:10px 12px;font-size:12px;
                         color:{COLORS['subtext']};
                         border-bottom:1px solid {COLORS['border']};">
                {attempt_html}
              </td>
              <td style="padding:10px 12px;font-size:11px;
                         color:{COLORS['subtext']};font-family:monospace;
                         border-bottom:1px solid {COLORS['border']};">
                {override_cmd}
              </td>
            </tr>"""

        unknowns_html = f"""
        {section_header("Needs Manual Review")}
        {alert_box(
            f"<strong>{len(unknowns)} company/companies</strong> use a "
            f"custom or unsupported ATS. Check their careers pages manually "
            f"and use the override command if a supported ATS exists.",
            COLORS["danger"]
        )}
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-radius:8px;overflow:hidden;
                      border:1px solid {COLORS['border']};">
          {table_header_row(["Company", "Best Attempt", "Override Command"])}
          {rows}
        </table>"""

    # ── Full body ──
    body = f"""
    {summary_html}
    {close_calls_html}
    {unknowns_html}
    {info_box(
        "To override a detection: "
        "<code>python pipeline.py --detect-ats \"Company\" "
        "--override &lt;platform&gt; &lt;slug&gt;</code>"
    )}
    """

    html = _base(
        title="ATS Detection Report",
        icon="🔍",
        subject_line=subject,
        body_html=body,
        date_str=date_str,
    )

    result = send_report_email(subject, html)
    if result:
        print(f"[OK] Detection report sent: {len(close_calls)} close calls, "
              f"{len(unknowns)} unknowns")
    return result


def _build_verify_url(platform, slug):
    """Build a human-readable URL to verify the detected ATS."""
    if not platform or not slug:
        return "#"

    import json
    # Handle Workday JSON slug
    if platform == "workday":
        try:
            info = json.loads(slug)
            s    = info.get("slug", "")
            wd   = info.get("wd", "wd5")
            return f"https://{s}.{wd}.myworkdayjobs.com/careers"
        except (json.JSONDecodeError, TypeError):
            return "#"

    urls = {
        "greenhouse":      f"https://boards.greenhouse.io/{slug}",
        "lever":           f"https://jobs.lever.co/{slug}",
        "ashby":           f"https://jobs.ashbyhq.com/{slug}",
        "smartrecruiters": f"https://jobs.smartrecruiters.com/{slug}",
    }
    return urls.get(platform, "#")