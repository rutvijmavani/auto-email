# outreach/report_templates/base.py — Shared HTML email scaffold

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from config import EMAIL, APP_PASSWORD

COLORS = {
    "bg":        "#f0f2f5",
    "card":      "#ffffff",
    "header":    "#0f172a",
    "accent":    "#3b82f6",
    "success":   "#22c55e",
    "danger":    "#ef4444",
    "warning":   "#f59e0b",
    "neutral":   "#64748b",
    "text":      "#1e293b",
    "subtext":   "#64748b",
    "border":    "#e2e8f0",
    "row_alt":   "#f8fafc",
}

FONT = (
    "-apple-system, BlinkMacSystemFont, 'Segoe UI', "
    "Roboto, Helvetica, Arial, sans-serif"
)


def _base(title, icon, subject_line, body_html, date_str):
    """Wrap body_html in the full email scaffold."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
</head>
<body style="margin:0;padding:0;background:{COLORS['bg']};font-family:{FONT};">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background:{COLORS['bg']};padding:32px 0;">
    <tr><td align="center">
      <table width="620" cellpadding="0" cellspacing="0" border="0"
             style="max-width:620px;width:100%;">

        <!-- HEADER -->
        <tr>
          <td style="background:{COLORS['header']};border-radius:12px 12px 0 0;
                     padding:28px 32px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td>
                  <span style="font-size:24px;">{icon}</span>
                  <span style="color:#ffffff;font-size:18px;font-weight:700;
                               margin-left:10px;vertical-align:middle;">
                    {title}
                  </span>
                </td>
                <td align="right">
                  <span style="color:#94a3b8;font-size:13px;">{date_str}</span>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- BODY -->
        <tr>
          <td style="background:{COLORS['card']};padding:28px 32px;
                     border-left:1px solid {COLORS['border']};
                     border-right:1px solid {COLORS['border']};">
            {body_html}
          </td>
        </tr>

        <!-- FOOTER -->
        <tr>
          <td style="background:{COLORS['row_alt']};border-radius:0 0 12px 12px;
                     border:1px solid {COLORS['border']};border-top:none;
                     padding:16px 32px;text-align:center;">
            <span style="color:{COLORS['subtext']};font-size:12px;">
              Recruiter Outreach Pipeline &nbsp;·&nbsp; {date_str}
            </span>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def stat_card(value, label, color=None):
    """Single stat bubble — value + label."""
    color = color or COLORS["accent"]
    return f"""
    <td align="center" style="padding:0 8px;">
      <div style="background:{COLORS['bg']};border-radius:10px;
                  padding:16px 20px;min-width:100px;
                  border:1px solid {COLORS['border']};">
        <div style="font-size:28px;font-weight:800;color:{color};
                    line-height:1;">{value}</div>
        <div style="font-size:12px;color:{COLORS['subtext']};
                    margin-top:4px;text-transform:uppercase;
                    letter-spacing:0.5px;">{label}</div>
      </div>
    </td>"""


def stat_row(stats):
    """Row of stat_card cells."""
    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="margin-bottom:24px;">
      <tr>{''.join(stats)}</tr>
    </table>"""


def section_header(title):
    return f"""
    <div style="font-size:11px;font-weight:700;color:{COLORS['subtext']};
                text-transform:uppercase;letter-spacing:1px;
                border-bottom:2px solid {COLORS['border']};
                padding-bottom:8px;margin:24px 0 12px;">
      {title}
    </div>"""


def badge(text, color):
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:20px;'
        f'font-size:11px;font-weight:600;background:{color}20;color:{color};">'
        f'{text}</span>'
    )


def table_row(cells, bg=None):
    bg = bg or COLORS["card"]
    cells_html = "".join(
        f'<td style="padding:10px 12px;font-size:13px;'
        f'color:{COLORS["text"]};border-bottom:1px solid {COLORS["border"]};">'
        f'{c}</td>'
        for c in cells
    )
    return f'<tr style="background:{bg};">{cells_html}</tr>'


def table_header_row(headers):
    cells_html = "".join(
        f'<td style="padding:8px 12px;font-size:11px;font-weight:700;'
        f'color:{COLORS["subtext"]};text-transform:uppercase;letter-spacing:0.5px;'
        f'background:{COLORS["row_alt"]};border-bottom:2px solid {COLORS["border"]};">'
        f'{h}</td>'
        for h in headers
    )
    return f"<tr>{cells_html}</tr>"


def alert_box(text, color=None):
    color = color or COLORS["warning"]
    return f"""
    <div style="background:{color}15;border-left:4px solid {color};
                border-radius:0 8px 8px 0;padding:12px 16px;
                margin:16px 0;font-size:13px;color:{COLORS['text']};">
      {text}
    </div>"""


def info_box(text):
    return alert_box(text, COLORS["accent"])


def send_report_email(subject, html_body):
    """
    Send HTML report email to the configured Gmail address.
    Uses the same credentials as outreach emails.
    """
    if not EMAIL or not APP_PASSWORD:
        print("[WARNING] Report email skipped — GMAIL_EMAIL or GMAIL_APP_PASSWORD not set.")
        return False

    try:
        from email.header import Header
        msg = MIMEMultipart("alternative")
        msg["From"]    = EMAIL
        msg["To"]      = EMAIL  # send to yourself
        msg["Subject"] = Header(subject, "utf-8")
        # utf-8 charset handles emoji in subject on all platforms
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(EMAIL, APP_PASSWORD)
            server.send_message(msg)

        print(f"[OK] Report email sent: {subject}")
        return True

    except Exception as e:
        print(f"[WARNING] Report email failed: {e}")
        return False