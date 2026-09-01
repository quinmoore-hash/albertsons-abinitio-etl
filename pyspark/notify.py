"""PySpark/Airflow port of ``run/notify.ksh`` (plan method ``alert_store_data_ops``).

Sends the nightly-batch failure alert by email and to Slack, degrading to a
log line when neither transport is configured (as the legacy demo script did).
"""

from __future__ import annotations

import json
import os
import smtplib
import urllib.request
from datetime import datetime
from email.message import EmailMessage

DEFAULT_EMAIL = os.environ.get("ETL_ALERT_EMAIL", "store-data-ops@albertsons.com")
DEFAULT_CHANNEL = os.environ.get("ETL_ALERT_SLACK_CHANNEL", "#store-data-ops")
SUBJECT = "[Albertsons ETL] nightly_batch task failure"


def build_body(task_id: str | None = None) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    task = f" (task {task_id})" if task_id else ""
    log_dir = os.environ.get("PROJECT_DIR", ".") + "/logs"
    return f"A task in nightly_batch failed{task} at {timestamp}. Check logs under {log_dir}."


def send_email(body: str, email: str = DEFAULT_EMAIL) -> None:
    host = os.environ.get("SMTP_HOST")
    if not host:
        print(f"[notify] (demo) would email {email}: {SUBJECT}")
        return
    message = EmailMessage()
    message["Subject"] = SUBJECT
    message["From"] = os.environ.get("SMTP_FROM", "etl-noreply@albertsons.com")
    message["To"] = email
    message.set_content(body)
    with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", "25"))) as smtp:
        smtp.send_message(message)


def send_slack(body: str, channel: str = DEFAULT_CHANNEL) -> None:
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print(f"[notify] (demo) would post to {channel}: {SUBJECT}")
        return
    payload = json.dumps({"channel": channel, "text": f"{SUBJECT} - {body}"}).encode()
    request = urllib.request.Request(
        webhook, data=payload, headers={"Content-type": "application/json"}
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:  # notification failures never fail the batch
        print(f"[notify] slack post failed: {exc}")


def alert_store_data_ops(context: dict | None = None) -> None:
    """Airflow ``on_failure_callback`` equivalent of the plan's failure method."""
    task_id = None
    if context:
        task_instance = context.get("task_instance")
        task_id = getattr(task_instance, "task_id", None)
    body = build_body(task_id)
    send_email(body)
    send_slack(body)
