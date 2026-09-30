"""Opt-in round trip against a real Oracle EDW.

Set ``EDW_IT_JDBC_URL``, ``EDW_IT_USER``, ``EDW_IT_PASSWORD`` and put the Oracle
driver on the classpath with ``EDW_IT_JDBC_JAR=/path/to/ojdbc11.jar`` (added to the
shared test session in ``conftest.py``).
The schema of ``EDW_IT_USER`` must already contain ``F_DAILY_SALES_SUMMARY`` and
``F_INVENTORY_VALUE``.
"""

import os
from decimal import Decimal

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("EDW_IT_JDBC_URL"), reason="EDW_IT_JDBC_URL not set"
)


def test_jobs_load_edw(spark, batch_cfg):
    from spark.common.jdbc import EdwJdbc
    from spark.jobs import daily_pos_sales, inventory_snapshot

    schema = os.environ["EDW_IT_USER"].upper()
    edw = EdwJdbc(
        url=os.environ["EDW_IT_JDBC_URL"],
        user=os.environ["EDW_IT_USER"],
        password=os.environ["EDW_IT_PASSWORD"],
        default_schema=schema,
    )
    summary_tbl, inv_tbl = f"{schema}.F_DAILY_SALES_SUMMARY", f"{schema}.F_INVENTORY_VALUE"
    date_filter = "WHERE BUSINESS_DATE = DATE '2026-07-13'"
    before = edw.read(spark, query=f"SELECT COUNT(*) N FROM {summary_tbl} {date_filter}")
    before_n = int(before.first().N)

    daily_pos_sales.run(
        spark, batch_cfg, loader=lambda df, cfg: edw.write(df, summary_tbl, mode="append")
    )
    inventory_snapshot.run(
        spark, batch_cfg, loader=lambda df, cfg: edw.write(df, inv_tbl, mode="truncate")
    )
    inventory_snapshot.run(
        spark, batch_cfg, loader=lambda df, cfg: edw.write(df, inv_tbl, mode="truncate")
    )

    after = edw.read(spark, query=f"SELECT COUNT(*) N FROM {summary_tbl} {date_filter}")
    assert int(after.first().N) == before_n + 9
    inv = edw.read(spark, table=inv_tbl)
    assert inv.count() == 7  # truncate mode replaces, does not append
    total = edw.read(spark, query=f"SELECT SUM(RETAIL_VALUE) S FROM {inv_tbl}").first().S
    assert Decimal(total) == Decimal("4174.12")
