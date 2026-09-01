"""Airflow DAG replacing ``plan/nightly_batch.plan`` (Conduct>It).

    daily_pos_sales >> inventory_snapshot >> dq_check

Every task alerts through ``alert_store_data_ops`` (``run/notify.ksh``) on
failure. ``BUSINESS_DATE`` is a DAG param defaulting to the logical date
(yesterday for the 02:30 nightly run), overridable at trigger time.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get(
    "PROJECT_DIR", os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
PYSPARK_DIR = os.path.join(PROJECT_DIR, "pyspark")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")
SPARK_SUBMIT_OPTS = os.environ.get("SPARK_SUBMIT_OPTS", "--master local[*]")
# Oracle JDBC driver jar, e.g. --jars /opt/jdbc/ojdbc8.jar
ORACLE_JDBC_JAR = os.environ.get("ORACLE_JDBC_JAR", "")

sys.path.insert(0, PYSPARK_DIR)
from notify import alert_store_data_ops  # noqa: E402

# Passed to the tasks as an environment variable rather than interpolated into
# the command, so a crafted trigger conf cannot inject shell.
BUSINESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('BUSINESS_DATE', params.BUSINESS_DATE)"
    " or macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"
)
TASK_ENV = {"BUSINESS_DATE": BUSINESS_DATE_TEMPLATE}
# Reject anything that is not a literal YYYYMMDD date before it reaches a job.
VALIDATE_BUSINESS_DATE = (
    'case "$BUSINESS_DATE" in '
    '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;; '
    '*) echo "invalid BUSINESS_DATE" >&2; exit 64 ;; esac'
)

default_args = {
    "owner": "store-data-ops",
    "retries": 0,
    "on_failure_callback": alert_store_data_ops,
    "execution_timeout": timedelta(hours=3),
}


def _spark_submit(script: str) -> str:
    jars = f" --jars {ORACLE_JDBC_JAR}" if ORACLE_JDBC_JAR else ""
    return (
        f"{VALIDATE_BUSINESS_DATE} && cd {PYSPARK_DIR} && "
        f'{SPARK_SUBMIT} {SPARK_SUBMIT_OPTS}{jars} {script} "$BUSINESS_DATE"'
    )


with DAG(
    dag_id="nightly_batch",
    description="Albertsons nightly retail batch (PySpark port of nightly_batch.plan)",
    # Cron from the plan: 30 2 * * * (after store close feeds land).
    schedule="30 2 * * *",
    start_date=datetime(2026, 7, 13),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"BUSINESS_DATE": ""},
    tags=["albertsons", "edw", "abinitio-migration"],
) as dag:
    daily_pos_sales = BashOperator(
        task_id="daily_pos_sales",
        bash_command=_spark_submit("daily_pos_sales.py"),
        env=TASK_ENV,
        append_env=True,
    )

    inventory_snapshot = BashOperator(
        task_id="inventory_snapshot",
        bash_command=_spark_submit("inventory_snapshot.py"),
        env=TASK_ENV,
        append_env=True,
    )

    dq_check = BashOperator(
        task_id="dq_check",
        bash_command=(
            f"{VALIDATE_BUSINESS_DATE} && cd {PYSPARK_DIR} && "
            'python dq_check.py "$BUSINESS_DATE"'
        ),
        env=TASK_ENV,
        append_env=True,
    )

    daily_pos_sales >> inventory_snapshot >> dq_check
