"""
Clinic Notification System
===========================
Sends weekly recovery digest emails to clinic owners.

Supports two sending backends (configure via environment):

  SMTP (default):
    SMTP_HOST       — mail server hostname
    SMTP_PORT       — port (default 587)
    SMTP_USER       — login username
    SMTP_PASSWORD   — login password
    SMTP_USE_TLS    — "true" / "false" (default true)

  SendGrid:
    SENDGRID_API_KEY — if set, SMTP settings are ignored

  Both:
    NOTIFICATION_FROM   — "From" address, e.g. "CodeMed <noreply@codemed.io>"
    NOTIFICATION_BCC    — optional BCC for your own records

All sends are logged.  If no backend is configured (dev environment),
the digest is logged at INFO level instead of sent.
"""

import logging
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

NOTIFICATION_FROM = os.getenv("NOTIFICATION_FROM", "CodeMed <noreply@codemed.io>")
NOTIFICATION_BCC  = os.getenv("NOTIFICATION_BCC", "")


# ---------------------------------------------------------------------------
# Digest builder
# ---------------------------------------------------------------------------

def _build_digest_body(clinic_name: str, summary: dict) -> tuple[str, str]:
    """
    Build (plain_text, html) body for the weekly recovery digest.
    `summary` is the dict returned by RevenueTracker.get_clinic_summary().
    """
    s          = summary.get("summary", {})
    by_code    = summary.get("by_code", {})
    pipeline   = summary.get("pipeline", {})

    recovered  = s.get("total_recovered", 0)
    fees_earned = s.get("fees_earned", 0)
    clinic_net  = s.get("clinic_net", 0)
    potential   = s.get("total_potential", 0)

    co16 = by_code.get("CO-16", {})
    co50 = by_code.get("CO-50", {})
    co97 = by_code.get("CO-97", {})

    # Plain text
    plain = f"""Hi {clinic_name},

Here's your CodeMed denial recovery update:

RECOVERED THIS PERIOD
  Total recovered:   ${recovered:,.2f}
  Your net (80%):    ${clinic_net:,.2f}
  CodeMed fee (20%): ${fees_earned:,.2f}

BREAKDOWN BY CODE
  CO-16 (missing info):     {co16.get('count', 0)} claims, ${co16.get('value', 0):,.2f} identified
  CO-50 (medical necessity): {co50.get('count', 0)} claims, ${co50.get('value', 0):,.2f} identified
  CO-97 (bundling):          {co97.get('count', 0)} claims, ${co97.get('value', 0):,.2f} identified

PIPELINE STATUS
  Detected:  {pipeline.get('detected', 0)}
  Fixed:     {pipeline.get('fixed', 0)}
  Submitted: {pipeline.get('submitted', 0)}
  Paid:      {pipeline.get('paid', 0)}

REMAINING OPPORTUNITY
  ${potential:,.2f} in identified denials still in pipeline

Questions? Reply to this email.

— CodeMed
"""

    # HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         color: #1a1a1a; margin: 0; padding: 0; background: #f5f5f5; }}
  .container {{ max-width: 600px; margin: 32px auto; background: #fff;
               border-radius: 8px; overflow: hidden;
               box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .header {{ background: #1a56db; padding: 24px 32px; color: white; }}
  .header h1 {{ margin: 0; font-size: 20px; font-weight: 600; }}
  .header p {{ margin: 4px 0 0; font-size: 14px; opacity: 0.85; }}
  .body {{ padding: 32px; }}
  .metric-row {{ display: flex; gap: 16px; margin-bottom: 24px; }}
  .metric {{ flex: 1; background: #f8fafc; border-radius: 6px;
             padding: 16px; text-align: center; }}
  .metric .value {{ font-size: 28px; font-weight: 700; color: #1a56db; }}
  .metric .label {{ font-size: 12px; color: #6b7280; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 16px 0; }}
  th {{ text-align: left; font-size: 12px; color: #6b7280;
        text-transform: uppercase; letter-spacing: 0.05em;
        padding: 8px 0; border-bottom: 1px solid #e5e7eb; }}
  td {{ padding: 10px 0; border-bottom: 1px solid #f3f4f6;
        font-size: 14px; }}
  .footer {{ padding: 16px 32px; background: #f8fafc;
             font-size: 12px; color: #9ca3af; text-align: center; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Recovery Update — {clinic_name}</h1>
    <p>Your weekly denial recovery summary from CodeMed</p>
  </div>
  <div class="body">

    <div class="metric-row">
      <div class="metric">
        <div class="value">${clinic_net:,.0f}</div>
        <div class="label">Your net recovery</div>
      </div>
      <div class="metric">
        <div class="value">${recovered:,.0f}</div>
        <div class="label">Total recovered</div>
      </div>
      <div class="metric">
        <div class="value">${potential:,.0f}</div>
        <div class="label">Still in pipeline</div>
      </div>
    </div>

    <table>
      <tr>
        <th>Code</th><th>Type</th>
        <th style="text-align:right">Claims</th>
        <th style="text-align:right">Value</th>
      </tr>
      <tr>
        <td><strong>CO-16</strong></td><td>Missing information</td>
        <td style="text-align:right">{co16.get('count', 0)}</td>
        <td style="text-align:right">${co16.get('value', 0):,.2f}</td>
      </tr>
      <tr>
        <td><strong>CO-50</strong></td><td>Medical necessity</td>
        <td style="text-align:right">{co50.get('count', 0)}</td>
        <td style="text-align:right">${co50.get('value', 0):,.2f}</td>
      </tr>
      <tr>
        <td><strong>CO-97</strong></td><td>Bundling / modifier</td>
        <td style="text-align:right">{co97.get('count', 0)}</td>
        <td style="text-align:right">${co97.get('value', 0):,.2f}</td>
      </tr>
    </table>

    <table>
      <tr><th>Pipeline stage</th><th style="text-align:right">Count</th></tr>
      <tr><td>Detected</td>
          <td style="text-align:right">{pipeline.get('detected', 0)}</td></tr>
      <tr><td>Fixed &amp; ready to submit</td>
          <td style="text-align:right">{pipeline.get('fixed', 0)}</td></tr>
      <tr><td>Submitted to payer</td>
          <td style="text-align:right">{pipeline.get('submitted', 0)}</td></tr>
      <tr><td>Paid</td>
          <td style="text-align:right">{pipeline.get('paid', 0)}</td></tr>
    </table>

  </div>
  <div class="footer">
    Questions? Reply to this email. &nbsp;|&nbsp;
    CodeMed — Performance-based denial recovery
  </div>
</div>
</body>
</html>"""

    return plain, html


# ---------------------------------------------------------------------------
# Send backends
# ---------------------------------------------------------------------------

def _send_smtp(to_email: str, subject: str, plain: str, html: str) -> None:
    host     = os.getenv("SMTP_HOST", "")
    port     = int(os.getenv("SMTP_PORT", "587"))
    user     = os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASSWORD", "")
    use_tls  = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

    if not host:
        raise RuntimeError("SMTP_HOST is not configured")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = NOTIFICATION_FROM
    msg["To"]      = to_email
    if NOTIFICATION_BCC:
        msg["Bcc"] = NOTIFICATION_BCC

    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html,  "html"))

    recipients = [to_email] + ([NOTIFICATION_BCC] if NOTIFICATION_BCC else [])

    if use_tls:
        context = ssl.create_default_context()
        with smtplib.SMTP(host, port) as server:
            server.starttls(context=context)
            if user:
                server.login(user, password)
            server.sendmail(NOTIFICATION_FROM, recipients, msg.as_string())
    else:
        with smtplib.SMTP(host, port) as server:
            if user:
                server.login(user, password)
            server.sendmail(NOTIFICATION_FROM, recipients, msg.as_string())


def _send_sendgrid(to_email: str, subject: str, plain: str, html: str) -> None:
    import requests
    api_key = os.environ["SENDGRID_API_KEY"]
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "personalizations": [{"to": [{"email": to_email}]}],
            "from":    {"email": NOTIFICATION_FROM},
            "subject": subject,
            "content": [
                {"type": "text/plain", "value": plain},
                {"type": "text/html",  "value": html},
            ],
        },
        timeout=15,
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _build_approval_body(
    clinic_name: str,
    denials: list[dict],
    base_url: str,
) -> tuple[str, str]:
    """
    Build (plain_text, html) for an approval-request email.
    Each item in `denials` must have: denial_id, denial_code, fix_notes,
    estimated_recovery, fixes_applied, approve_url, reject_url.
    """
    total = sum(d.get("estimated_recovery", 0) for d in denials)

    # ---- plain text ----
    lines = [
        f"Hi {clinic_name},",
        "",
        f"CodeMed has fixed {len(denials)} denial(s) worth ${total:,.2f} in estimated recovery.",
        "Please review and approve each fix so we can resubmit to the payer.",
        "",
    ]
    for i, d in enumerate(denials, 1):
        lines += [
            f"Denial #{i}  [{d['denial_code']}]  ${d.get('estimated_recovery', 0):,.2f}",
            f"  Fix: {d.get('fix_notes', 'See fix details')}",
            f"  APPROVE: {d['approve_url']}",
            f"  REJECT:  {d['reject_url']}",
            "",
        ]
    lines += ["Questions? Reply to this email.", "", "— CodeMed"]
    plain = "\n".join(lines)

    # ---- html ----
    denial_rows = ""
    for d in denials:
        code_badge_color = {"CO-16": "#2563eb", "CO-50": "#d97706", "CO-97": "#7c3aed"}.get(
            d["denial_code"], "#6b7280"
        )
        denial_rows += f"""
    <div style="border:1px solid #e5e7eb;border-radius:8px;padding:20px;margin-bottom:16px;">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
        <span style="background:{code_badge_color};color:#fff;font-size:12px;font-weight:700;
                     padding:3px 10px;border-radius:99px;">{d['denial_code']}</span>
        <span style="font-size:18px;font-weight:700;color:#1a56db;">
          ${d.get('estimated_recovery', 0):,.2f}
        </span>
        <span style="font-size:12px;color:#6b7280;">estimated recovery</span>
      </div>
      <p style="margin:0 0 16px;font-size:14px;color:#374151;line-height:1.5;">
        {d.get('fix_notes', 'Fix applied — see details.')}
      </p>
      <div style="display:flex;gap:12px;">
        <a href="{d['approve_url']}"
           style="background:#16a34a;color:#fff;text-decoration:none;padding:10px 24px;
                  border-radius:6px;font-size:14px;font-weight:600;">
          ✓ Approve &amp; Submit
        </a>
        <a href="{d['reject_url']}"
           style="background:#fff;color:#dc2626;text-decoration:none;padding:10px 24px;
                  border-radius:6px;font-size:14px;font-weight:600;
                  border:1px solid #dc2626;">
          ✗ Reject
        </a>
      </div>
    </div>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
  body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         color:#1a1a1a;margin:0;padding:0;background:#f5f5f5; }}
  .wrap {{ max-width:600px;margin:32px auto;background:#fff;border-radius:8px;
           overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.1); }}
  .hdr  {{ background:#1a56db;padding:24px 32px;color:#fff; }}
  .hdr h1 {{ margin:0;font-size:20px;font-weight:600; }}
  .hdr p  {{ margin:4px 0 0;font-size:14px;opacity:.85; }}
  .body {{ padding:32px; }}
  .summary {{ background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;
              padding:16px 20px;margin-bottom:24px; }}
  .summary strong {{ color:#16a34a; }}
  .footer {{ padding:16px 32px;background:#f8fafc;font-size:12px;
             color:#9ca3af;text-align:center; }}
</style>
</head>
<body>
<div class="wrap">
  <div class="hdr">
    <h1>Action Required — {clinic_name}</h1>
    <p>CodeMed has fixes ready to submit. Your approval is needed.</p>
  </div>
  <div class="body">
    <div class="summary">
      <strong>{len(denials)} denial(s)</strong> fixed &mdash;
      <strong>${total:,.2f}</strong> in estimated recovery awaiting your approval.
    </div>
    {denial_rows}
  </div>
  <div class="footer">
    Questions? Reply to this email. &nbsp;|&nbsp;
    CodeMed — Performance-based denial recovery
  </div>
</div>
</body>
</html>"""

    return plain, html


def send_approval_request(
    clinic_id: str,
    clinic_name: str,
    to_email: str,
    denials: list[dict],
    base_url: str,
) -> None:
    """
    Send an approval-request email listing all pending_approval denials.

    Each item in `denials` must have: denial_id, denial_code, fix_notes,
    estimated_recovery, approve_token, reject_token.

    `base_url` is the public root of the app, e.g. "https://app.codemed.io".
    Approve/reject links are built as:
        {base_url}/api/recovery/approve/<token>
        {base_url}/api/recovery/reject/<token>
    """
    if not denials:
        return

    enriched = []
    for d in denials:
        tok = d.get("token", "")
        enriched.append({
            **d,
            "approve_url": f"{base_url.rstrip('/')}/api/recovery/approve/{tok}",
            "reject_url":  f"{base_url.rstrip('/')}/api/recovery/reject/{tok}",
        })

    total = sum(d.get("estimated_recovery", 0) for d in enriched)
    subject = (
        f"{clinic_name}: ${total:,.0f} in fixes ready — approval needed"
    )

    plain, html = _build_approval_body(clinic_name, enriched, base_url)

    if os.getenv("SENDGRID_API_KEY"):
        _send_sendgrid(to_email, subject, plain, html)
        logger.info("Approval request sent via SendGrid to %s (clinic %s)", to_email, clinic_id)
    elif os.getenv("SMTP_HOST"):
        _send_smtp(to_email, subject, plain, html)
        logger.info("Approval request sent via SMTP to %s (clinic %s)", to_email, clinic_id)
    else:
        logger.info(
            "Approval request (no email backend) for clinic %s:\n%s", clinic_id, plain
        )


def send_weekly_digest(
    clinic_id: str,
    clinic_name: str,
    to_email: str,
    summary: dict,
) -> None:
    """
    Send the weekly recovery digest email to a clinic.
    Raises on delivery failure so the scheduler can log it.
    """
    recovered = summary.get("summary", {}).get("total_recovered", 0)
    subject   = (
        f"{clinic_name}: ${recovered:,.0f} recovered — your CodeMed weekly update"
        if recovered > 0
        else f"{clinic_name}: Your CodeMed denial recovery update"
    )

    plain, html = _build_digest_body(clinic_name, summary)

    # Choose backend
    if os.getenv("SENDGRID_API_KEY"):
        _send_sendgrid(to_email, subject, plain, html)
        logger.info("Weekly digest sent via SendGrid to %s (clinic %s)", to_email, clinic_id)
    elif os.getenv("SMTP_HOST"):
        _send_smtp(to_email, subject, plain, html)
        logger.info("Weekly digest sent via SMTP to %s (clinic %s)", to_email, clinic_id)
    else:
        # Dev / no backend configured — log the full digest
        logger.info(
            "Weekly digest (no email backend configured) for clinic %s:\n%s",
            clinic_id, plain,
        )
