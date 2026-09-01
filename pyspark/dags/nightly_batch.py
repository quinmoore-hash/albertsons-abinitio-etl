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

BUSINESS_DATE = "{{ dag_run.conf.get('BUSINESS_DATE', params.BUSINESS_DATE) or macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"

default_args = {
    "owner": "store-data-ops",
    "retries": 0,
    "on_failure_callback": alert_store_data_ops,
    "execution_timeout": timedelta(hours=3),
}


def _spark_submit(script: str) -> str:
    jars = f" --jars {ORACLE_JDBC_JAR}" if ORACLE_JDBC_JAR else ""
    return (
        f"cd {PYSPARK_DIR} && {SPARK_SUBMIT} {SPARK_SUBMIT_OPTS}{jars} "
        f"{script} {BUSINESS_DATE}"
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
    )

    inventory_snapshot = BashOperator(
        task_id="inventory_snapshot",
        bash_command=_spark_submit("inventory_snapshot.py"),
    )

    dq_check = BashOperator(
        task_id="dq_check",
        bash_command=f"cd {PYSPARK_DIR} && python dq_check.py {BUSINESS_DATE}",
    )

    daily_pos_sales >> inventory_snapshot >> dq_check
