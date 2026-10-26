"""Unit tests for email alerting."""

from __future__ import annotations

import pytest

from scripts import alert_utils as alerts


class FakeSMTP:
    sent = []

    def __init__(self, host, port, timeout):
        self.host, self.port = host, port

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def login(self, user, password):
        if password == "wrong":
            raise OSError("auth failed")

    def send_message(self, msg):
        FakeSMTP.sent.append(msg)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in ("GMAIL_USERNAME", "GMAIL_APP_PASSWORD", "ALERT_RECIPIENTS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(alerts.smtplib, "SMTP_SSL", FakeSMTP)
    FakeSMTP.sent = []


def configure_email(monkeypatch, password="app-pass"):
    monkeypatch.setenv("GMAIL_USERNAME", "bot@gmail.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", password)
    monkeypatch.setenv("ALERT_RECIPIENTS", "a@x.com, b@x.com")


def test_not_configured_sends_nothing():
    assert alerts.send_alert("subject", "body") is False
    assert FakeSMTP.sent == []


def test_email_alert(monkeypatch):
    configure_email(monkeypatch)
    assert alerts.send_alert("Data anomalies", "title_len dropped") is True
    (msg,) = FakeSMTP.sent
    assert msg["Subject"] == "Data anomalies"
    assert msg["To"] == "a@x.com, b@x.com"
    assert "title_len dropped" in msg.get_content()


def test_failing_email_does_not_raise(monkeypatch):
    configure_email(monkeypatch, password="wrong")
    assert alerts.send_alert("s", "b") is False  # logged, not raised


class FakeTI:
    dag_id, task_id, try_number = "process_news", "tfdv_validate", 1


def test_airflow_failure_callback_emails_task_details(monkeypatch):
    configure_email(monkeypatch)
    context = {"ti": FakeTI(), "run_id": "scheduled__2026-10-25", "exception": RuntimeError("boom")}

    alerts.airflow_failure_callback(context)

    (msg,) = FakeSMTP.sent
    assert msg["Subject"] == "[FinMLOps] process_news.tfdv_validate failed"
    assert "scheduled__2026-10-25" in msg.get_content()
    assert "boom" in msg.get_content()


def test_deliberate_ci_failure():
    assert False, "deliberate failure to screenshot the CI alert email"
    
