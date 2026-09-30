"""Airflow DAG replacing ``plan/nightly_batch.plan`` (Conduct>It).

    START -> daily_pos_sales -> inventory_snapshot -> dq_check -> END

Cron ``30 2 * * *`` in ``NIGHTLY_BATCH_TZ``; ``BUSINESS_DATE`` defaults to the
day before the scheduled run (plan default: yesterday) and can be overridden
with the ``business_date`` param on a manual trigger. Every task's failure runs
the ``alert_store_data_ops`` method equivalent (``spark.common.notify``).

Environment read at parse time:

``PROJECT_DIR``          repo checkout on the worker (default: parent of this dags/ dir)
``SPARK_SUBMIT``         spark-submit executable (default: ``spark-submit``)
``SPARK_SUBMIT_ARGS``    extra spark-submit args, e.g. ``--master yarn --packages ...``
``NIGHTLY_BATCH_TZ``     schedule timezone (default: ``America/Boise``)
"""

from __future__ import annotations

import os
import re
import shlex
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator

PROJECT_DIR = os.environ.get("PROJECT_DIR", str(Path(__file__).resolve().parents[1]))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from spark.common.notify import notify_failure  # noqa: E402

SPARK_SUBMIT = shlex.quote(os.environ.get("SPARK_SUBMIT", "spark-submit"))
SPARK_SUBMIT_ARGS = shlex.join(shlex.split(os.environ.get("SPARK_SUBMIT_ARGS", "")))
TIMEZONE = pendulum.timezone(os.environ.get("NIGHTLY_BATCH_TZ", "America/Boise"))

ALERT_EMAIL = "store-data-ops@albertsons.com"
ALERT_CHANNEL = "#store-data-ops"


def business_date(params: dict, logical_date: datetime | None, dag_run=None) -> str:
    """Explicit ``business_date`` param (``YYYYMMDD``), else the local day before the run."""
    if params.get("business_date"):
        value = str(params["business_date"])
        if not re.fullmatch(r"\d{8}", value):
            raise ValueError(f"business_date must be YYYYMMDD, got {value!r}")
        return value
    run_at = logical_date or getattr(dag_run, "run_after", None) or pendulum.now(TIMEZONE)
    local = pendulum.instance(run_at).in_timezone(TIMEZONE)
    return (local - timedelta(days=1)).strftime("%Y%m%d")


def alert_store_data_ops(context: dict) -> None:
    """``method alert_store_data_ops`` -> ``run/notify.ksh store-data-ops@... #store-data-ops``."""
    ti = context.get("task_instance")
    detail = (
        f"task={ti.task_id if ti else '?'} run_id={context.get('run_id', '?')} "
        f"exception={context.get('exception')}"
    )
    notify_failure(ALERT_EMAIL, ALERT_CHANNEL, detail=detail, project_dir=PROJECT_DIR)


def spark_job(module: str, *extra: str) -> str:
    script = shlex.quote(f"{PROJECT_DIR}/spark/jobs/{module}.py")
    args = " ".join(extra)
    root = shlex.quote(PROJECT_DIR)
    return (
        f"cd {root} && PYTHONPATH={root}${{PYTHONPATH:+:$PYTHONPATH}} "
        f"{SPARK_SUBMIT} {SPARK_SUBMIT_ARGS} {script} "
        "{{ business_date(params, logical_date, dag_run) }} "
        f"--project-dir {root} {args}"
    ).strip()


SKIP_LOAD = "{{ '--skip-load' if params.skip_load else '' }}"

with DAG(
    dag_id="nightly_batch",
    description="Albertsons nightly retail batch (port of plan/nightly_batch.plan)",
    schedule=CronTriggerTimetable("30 2 * * *", timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 7, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    params={"business_date": "", "skip_load": False},
    user_defined_macros={"business_date": business_date},
    default_args={"retries": 0, "on_failure_callback": alert_store_data_ops},
    tags=["albertsons", "edw", "pyspark"],
) as dag:
    start = EmptyOperator(task_id="START")
    daily_pos_sales = BashOperator(
        task_id="daily_pos_sales", bash_command=spark_job("daily_pos_sales", SKIP_LOAD)
    )
    inventory_snapshot = BashOperator(
        task_id="inventory_snapshot", bash_command=spark_job("inventory_snapshot", SKIP_LOAD)
    )
    dq_check = BashOperator(task_id="dq_check", bash_command=spark_job("dq_check"))
    end = EmptyOperator(task_id="END")

    start >> daily_pos_sales >> inventory_snapshot >> dq_check >> end
