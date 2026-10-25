"""Send pipeline alerts by email (Gmail SMTP).

Configured through environment variables (see .env.example); if they are unset
the alert is skipped. Alert delivery never raises, so a broken alert can't hide
the failure that triggered it.
"""

import logging
import os
import smtplib
from email.message import EmailMessage

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465


def send_email(subject, body):
    user = os.environ.get("GMAIL_USERNAME")
    password = os.environ.get("GMAIL_APP_PASSWORD")
    recipients = [
        r.strip()
        for r in os.environ.get("ALERT_RECIPIENTS", "").split(",")
        if r.strip()
    ]
    if not (user and password and recipients):
        return False

    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, user, ", ".join(recipients)
    msg.set_content(body)
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.login(user, password)
        smtp.send_message(msg)
    return True


def send_alert(subject, body):
    """Email the alert; returns True if sent. Never raises, so a broken alert can't hide the real error."""
    try:
        sent = send_email(subject, body)
    except Exception as exc:
        log.error("email alert failed: %s", exc)
        return False
    if sent:
        log.info("alert emailed: %s", subject)
    else:
        log.warning("email alerts not configured (see .env): %s", subject)
    return sent
