# outreach/report_templates/api_health_report.py
# Critical alert emails for rate limiting + API health issues

from datetime import datetime
from outreach.report_templates.base import (
    _base, send_report_email, COLORS, FONT
)
from config import PLATFORM_DELAYS, ENRICH_DAILY_LIMITS


def build_critical_rate_limit_alert(platform, rate, threshold):
    """
    Send immediate critical email when 429 rate > threshold.
    Called as soon as threshold is exceeded — any time of day.
    """
    date_str = datetime.now().strftime("%B %d, %Y %H:%M")
    subject  = (
        f"🚨 [CRITICAL] {platform.title()} rate limited "
        f"({rate}%)"
    )

    # Suggested actions
    current_delay  = PLATFORM_DELAYS.get(
        platform, {}
    ).get("base", 0.3)
    suggested_delay = round(current_delay * 2, 1)
    current_limit   = ENRICH_DAILY_LIMITS.get(platform, 0)
    suggested_limit = max(10, current_limit // 2)

    body = f"""
    <!-- Alert Card -->
    <tr><td style="padding:0 32px 24px;">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="background:#fef2f2;border:2px solid #ef4444;
                    border-radius:12px;padding:24px;">
        <tr><td>
          <div style="font-size:32px;margin-bottom:8px;">🚨</div>
          <div style="font-size:20px;font-weight:700;
                      color:#991b1b;margin-bottom:4px;">
            Rate Limit Detected
          </div>
          <div style="font-size:14px;color:#7f1d1d;">
            Immediate action recommended
          </div>
        </td></tr>
      </table>
    </td></tr>

    <!-- Stats -->
    <tr><td style="padding:0 32px 24px;">
      <table width="100%" cellpadding="16" cellspacing="0"
             style="background:#ffffff;border:1px solid #e2e8f0;
                    border-radius:12px;">
        <tr style="background:#f8fafc;">
          <td style="font-weight:600;color:#374151;
                     border-bottom:1px solid #e2e8f0;">
            Detail
          </td>
          <td style="font-weight:600;color:#374151;
                     border-bottom:1px solid #e2e8f0;">
            Value
          </td>
        </tr>
        <tr>
          <td style="color:#6b7280;border-bottom:1px solid #f1f5f9;">
            Platform
          </td>
          <td style="font-weight:600;color:#1e293b;
                     border-bottom:1px solid #f1f5f9;">
            {platform.title()}
          </td>
        </tr>
        <tr style="background:#fafafa;">
          <td style="color:#6b7280;border-bottom:1px solid #f1f5f9;">
            429 Rate
          </td>
          <td style="font-weight:700;color:#ef4444;
                     border-bottom:1px solid #f1f5f9;">
            {rate}%
          </td>
        </tr>
        <tr>
          <td style="color:#6b7280;border-bottom:1px solid #f1f5f9;">
            Threshold
          </td>
          <td style="color:#1e293b;
                     border-bottom:1px solid #f1f5f9;">
            {threshold}%
          </td>
        </tr>
        <tr style="background:#fafafa;">
          <td style="color:#6b7280;">
            Detected at
          </td>
          <td style="color:#1e293b;">
            {date_str}
          </td>
        </tr>
      </table>
    </td></tr>

    <!-- Actions -->
    <tr><td style="padding:0 32px 24px;">
      <div style="font-size:15px;font-weight:700;
                  color:#1e293b;margin-bottom:12px;">
        Recommended Actions
      </div>
      <table width="100%" cellpadding="12" cellspacing="0"
             style="background:#fffbeb;border:1px solid #fcd34d;
                    border-radius:8px;">
        <tr><td>
          <div style="font-size:13px;color:#92400e;
                      line-height:1.8;">
            <b>1. Increase platform delay in config.py:</b><br>
            &nbsp;&nbsp;Current:   {current_delay}s<br>
            &nbsp;&nbsp;Suggested: {suggested_delay}s<br>
            <br>
            <b>2. Reduce daily enrichment limit:</b><br>
            &nbsp;&nbsp;Current:   {current_limit}/day<br>
            &nbsp;&nbsp;Suggested: {suggested_limit}/day<br>
            <br>
            <b>3. Check api_health table for trend:</b><br>
            &nbsp;&nbsp;python pipeline.py --monitor-status
          </div>
        </td></tr>
      </table>
    </td></tr>

    <!-- Note -->
    <tr><td style="padding:0 32px 32px;">
      <div style="font-size:12px;color:#94a3b8;text-align:center;">
        This alert will not repeat for 24 hours for this platform.
        Adjust thresholds in config.py:
        RATE_LIMIT_CRITICAL_THRESHOLD
      </div>
    </td></tr>
    """

    html = _base(
        title="Critical: Rate Limit Alert",
        icon="🚨",
        subject_line=subject,
        body_html=body,
        date_str=date_str,
    )
    send_report_email(subject, html)


def build_unreachable_alert(platform, error_rate, date_str):
    """
    Send alert when platform is completely unreachable.
    100% error rate = API down or IP blocked.
    """
    subject = (
        f"🚨 [CRITICAL] {platform.title()} unreachable "
        f"({error_rate}% errors)"
    )

    body = f"""
    <tr><td style="padding:0 32px 24px;">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="background:#fef2f2;border:2px solid #ef4444;
                    border-radius:12px;padding:24px;">
        <tr><td>
          <div style="font-size:32px;margin-bottom:8px;">❌</div>
          <div style="font-size:20px;font-weight:700;
                      color:#991b1b;margin-bottom:4px;">
            Platform Unreachable
          </div>
          <div style="font-size:14px;color:#7f1d1d;">
            {platform.title()} returned {error_rate}% errors
          </div>
        </td></tr>
      </table>
    </td></tr>

    <tr><td style="padding:0 32px 32px;">
      <div style="font-size:13px;color:#374151;line-height:1.8;">
        <b>Possible causes:</b><br>
        • IP address blocked by {platform.title()}<br>
        • {platform.title()} API is down<br>
        • Network issue on Oracle VM<br>
        <br>
        <b>Check:</b><br>
        • ssh into VM and manually test the API<br>
        • Check api_health table for when errors started<br>
        • python pipeline.py --monitor-status
      </div>
    </td></tr>
    """

    html = _base(
        title="Critical: Platform Unreachable",
        icon="❌",
        subject_line=subject,
        body_html=body,
        date_str=date_str,
    )
    send_report_email(subject, html)