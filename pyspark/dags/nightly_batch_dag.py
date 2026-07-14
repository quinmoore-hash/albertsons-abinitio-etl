"""Airflow DAG replicating ``plan/nightly_batch.plan`` (Conduct>It).

Dependency chain (matching the plan):

    daily_pos_sales >> inventory_snapshot >> dq_check

* Cron schedule ``30 2 * * *`` (02:30 local, after store-close feeds land).
* ``BUSINESS_DATE`` is parameterised (``dag_run.conf['business_date']`` or a
  ``Param`` default); it defaults to the run's *previous* day in ``YYYYMMDD``
  form, matching the plan's "defaults to yesterday" comment.
* Every task has an on-failure notification callback equivalent to
  ``run/notify.ksh`` (email + Slack webhook to ``#store-data-ops``).
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# --- Paths / environment ---------------------------------------------------
# PROJECT_DIR is the repository root; PYSPARK_DIR holds the migrated jobs.
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/albertsons-abinitio-etl")
PYSPARK_DIR = os.path.join(PROJECT_DIR, "pyspark")
DATA_DIR = os.environ.get("AB_DATA_DIR", os.path.join(PROJECT_DIR, "data"))
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")

# Notification targets (mirror the plan's alert_store_data_ops method).
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "store-data-ops@albertsons.com")
ALERT_CHANNEL = os.environ.get("ALERT_SLACK_CHANNEL", "#store-data-ops")

# BUSINESS_DATE rendered per run. We read from ``params.business_date`` (which is
# schema-validated against BUSINESS_DATE_PATTERN, so a triggering user cannot
# inject shell metacharacters into the BashOperator commands below) and fall
# back to the run's logical date. For a daily ``30 2 * * *`` schedule Airflow's
# ``ds`` is the data_interval_start, which already equals "yesterday" relative to
# the 02:30 execution time -- matching the plan's "defaults to yesterday" -- so
# no extra day offset is applied.
BUSINESS_DATE_PATTERN = r"^\d{8}$"
BUSINESS_DATE = (
    "{{ params.business_date if params.business_date "
    "else macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"
)


def notify_failure(context: dict) -> None:
    """Failure hook equivalent to ``run/notify.ksh`` (email + Slack webhook)."""
    ti = context.get("task_instance")
    task_id = getattr(ti, "task_id", "unknown")
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    subject = "[Albertsons ETL] nightly_batch task failure"
    body = (
        f"Task {task_id} in nightly_batch failed at {ts}. "
        f"Check logs under {PROJECT_DIR}/logs."
    )

    # Email (best-effort; uses Airflow's configured SMTP when available).
    try:
        from airflow.utils.email import send_email

        send_email(to=ALERT_EMAIL, subject=subject, html_content=body)
    except Exception as exc:  # pragma: no cover - depends on SMTP config
        print(f"[notify] (demo) would email {ALERT_EMAIL}: {subject} ({exc})")

    # Slack webhook (best-effort).
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook:
        payload = json.dumps(
            {"channel": ALERT_CHANNEL, "text": f"{subject} - {body}"}
        ).encode("utf-8")
        req = urllib.request.Request(
            webhook, data=payload, headers={"Content-type": "application/json"}
        )
        try:
            urllib.request.urlopen(req, timeout=10)  # noqa: S310
        except Exception as exc:  # pragma: no cover - network dependent
            print(f"[notify] slack post failed: {exc}")
    else:
        print(f"[notify] (demo) would post to {ALERT_CHANNEL}: {subject}")


default_args = {
    "owner": "store-data-ops",
    "retries": 0,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="nightly_batch",
    description="Albertsons nightly retail batch (PySpark migration of nightly_batch.plan)",
    schedule="30 2 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "business_date": Param(
            default=None,
            type=["null", "string"],
            pattern=BUSINESS_DATE_PATTERN,
            description="BUSINESS_DATE in YYYYMMDD; defaults to the run's logical date.",
        ),
    },
    tags=["albertsons", "etl", "migration"],
) as dag:

    daily_pos_sales = BashOperator(
        task_id="daily_pos_sales",
        bash_command=(
            f"{SPARK_SUBMIT} {PYSPARK_DIR}/jobs/daily_pos_sales.py "
            f"--business-date {BUSINESS_DATE} --data-dir {DATA_DIR}"
        ),
    )

    inventory_snapshot = BashOperator(
        task_id="inventory_snapshot",
        bash_command=(
            f"{SPARK_SUBMIT} {PYSPARK_DIR}/jobs/inventory_snapshot.py "
            f"--business-date {BUSINESS_DATE} --data-dir {DATA_DIR}"
        ),
    )

    dq_check = BashOperator(
        task_id="dq_check",
        bash_command=(
            f"python {PYSPARK_DIR}/dq/dq_check.py "
            f"--business-date {BUSINESS_DATE} --data-dir {DATA_DIR}"
        ),
    )

    daily_pos_sales >> inventory_snapshot >> dq_check
