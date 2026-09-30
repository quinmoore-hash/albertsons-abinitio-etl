"""Failure notification, equivalent to ``run/notify.ksh``.

Sends an email through ``mailx`` and posts to Slack via ``SLACK_WEBHOOK_URL``;
either channel falls back to a ``(demo)`` log line when unavailable.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import urllib.request
from datetime import datetime

DEFAULT_EMAIL = "store-data-ops@albertsons.com"
DEFAULT_CHANNEL = "#store-data-ops"
SUBJECT = "[Albertsons ETL] nightly_batch task failure"


def build_body(detail: str | None = None, project_dir: str | None = None) -> str:
    when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logs = f"{project_dir or os.environ.get('PROJECT_DIR', '.')}/logs"
    body = f"A task in nightly_batch failed at {when}. Check logs under {logs}."
    return f"{body} {detail}" if detail else body


def send_email(email: str, subject: str, body: str) -> bool:
    mailx = shutil.which("mailx")
    if mailx:
        result = subprocess.run(
            [mailx, "-s", subject, email], input=body, text=True, capture_output=True, check=False
        )
        if result.returncode == 0:
            return True
    print(f"[notify] (demo) would email {email}: {subject}")
    return False


def post_slack(channel: str, text: str, webhook_url: str | None = None) -> bool:
    url = webhook_url if webhook_url is not None else os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        print(f"[notify] (demo) would post to {channel}: {SUBJECT}")
        return False
    payload = json.dumps({"channel": channel, "text": text}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            return True
    except OSError as exc:
        print(f"[notify] slack post failed: {exc}", file=sys.stderr)
        return False


def notify_failure(
    email: str = DEFAULT_EMAIL,
    channel: str = DEFAULT_CHANNEL,
    detail: str | None = None,
    project_dir: str | None = None,
) -> None:
    body = build_body(detail, project_dir)
    send_email(email, SUBJECT, body)
    post_slack(channel, f"{SUBJECT} - {body}")


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if not args:
        print("usage: notify <email> [slack-channel] [detail]", file=sys.stderr)
        return 1
    notify_failure(
        email=args[0],
        channel=args[1] if len(args) > 1 else DEFAULT_CHANNEL,
        detail=args[2] if len(args) > 2 else None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
