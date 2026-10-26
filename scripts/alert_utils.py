"""Send pipeline alerts by email (Gmail SMTP).

Configured through environment variables (see .env.example); if they are unset
the alert is skipped. Alert delivery never raises, so a broken alert can't hide
the failure that triggered it.
"""

import json
import logging
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

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
    msg["Subject"] = subject
    msg["From"] = f"FinMLOps Airflow <{user}>"
    msg["To"] = ", ".join(recipients)
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


def task_output(ti, lines=15):
    """Last lines the task's command printed, read from its Airflow log file."""
    path = Path(
        os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
        "logs",
        f"dag_id={ti.dag_id}",
        f"run_id={ti.run_id}",
        f"task_id={ti.task_id}",
        f"attempt={ti.try_number}.log",
    )
    try:
        events = [json.loads(line) for line in path.read_text().splitlines()]
    except (OSError, ValueError):
        return "(task log not found)"
    output = [e["event"] for e in events if "SubprocessHook" in e.get("logger", "")]
    return "\n".join(output[-lines:])


def airflow_failure_callback(context):
    """Airflow on_failure_callback: email which task failed, why, and the end of its output."""
    ti = context["ti"]
    send_alert(
        f"[FinMLOps] {ti.dag_id}.{ti.task_id} failed",
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"Run: {context['run_id']}\n"
        f"Data window: {context.get('data_interval_start')} to {context.get('data_interval_end')}\n"
        f"Attempt: {ti.try_number}\n"
        f"Error: {context.get('exception')}\n\n"
        f"Last output from the task:\n{task_output(ti)}\n\n"
        f"Full log (only on the machine running Airflow): {ti.log_url}\n",
    )
