"""Unit tests for email alerting."""

from __future__ import annotations

import json

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
    assert msg["From"] == "FinMLOps Airflow <bot@gmail.com>"
    assert msg["To"] == "a@x.com, b@x.com"
    assert "title_len dropped" in msg.get_content()


def test_failing_email_does_not_raise(monkeypatch):
    configure_email(monkeypatch, password="wrong")
    assert alerts.send_alert("s", "b") is False  # logged, not raised


class FakeTI:
    dag_id, task_id, try_number = "process_news", "tfdv_validate", 1
    run_id = "scheduled__2026-10-25"
    log_url = "http://localhost:8080/dags/process_news"


def write_task_log(root, events):
    path = root / "logs/dag_id=process_news/run_id=scheduled__2026-10-25/task_id=tfdv_validate/attempt=1.log"
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(json.dumps(e) for e in events))


def test_airflow_failure_callback_emails_task_details(monkeypatch, tmp_path):
    configure_email(monkeypatch)
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    hook = "airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook"
    write_task_log(tmp_path, [
        {"event": "Task started", "logger": "task"},
        {"event": "ValueError: no rows for 2026-10-25", "logger": hook},
    ])
    context = {"ti": FakeTI(), "run_id": "scheduled__2026-10-25", "exception": RuntimeError("boom")}

    alerts.airflow_failure_callback(context)

    (msg,) = FakeSMTP.sent
    body = msg.get_content()
    assert msg["Subject"] == "[FinMLOps] process_news.tfdv_validate failed"
    assert "scheduled__2026-10-25" in body
    assert "boom" in body
    assert "ValueError: no rows for 2026-10-25" in body
    assert "Task started" not in body  # only the command's own output
    assert FakeTI.log_url in body


def test_failure_callback_without_log_file(monkeypatch, tmp_path):
    configure_email(monkeypatch)
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))

    alerts.airflow_failure_callback({"ti": FakeTI(), "run_id": "r", "exception": None})

    assert "(task log not found)" in FakeSMTP.sent[0].get_content()
