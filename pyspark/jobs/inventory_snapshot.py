"""PySpark port of ``mp/inventory_snapshot.mp`` and ``xfr/inventory_rollup.xfr``.

Pipeline (mirrors the Ab Initio graph):

    INPUT (inventory) -> FILTER_BY_EXPRESSION (active) -> SORT -> ROLLUP -> OUTPUT
                                                                              |
                                          + m_db load EDW.F_INVENTORY_VALUE (truncate)
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from common.io import load_to_edw, read_delimited, write_delimited
from common.schemas import INVENTORY_SCHEMA, INVENTORY_VALUE_COLUMNS
from common.spark_session import build_spark

MONEY = DecimalType(18, 2)

EDW_TABLE = "EDW.F_INVENTORY_VALUE"


# ---------------------------------------------------------------------------
# FILTER_BY_EXPRESSION active_only
# ---------------------------------------------------------------------------
def filter_active(inventory: DataFrame) -> DataFrame:
    """Port of ``FILTER_BY_EXPRESSION``: ``on_hand_qty >= 0 AND store_status == 'OPEN'``."""
    return inventory.filter(
        (F.col("on_hand_qty") >= F.lit(0))
        & (F.col("store_status") == F.lit("OPEN"))
    )


# ---------------------------------------------------------------------------
# ROLLUP inv_value  (port of xfr/inventory_rollup.xfr)
# ---------------------------------------------------------------------------
def rollup_inventory(active: DataFrame) -> DataFrame:
    """Aggregate active inventory to the inventory_value grain.

    Grain: ``store_id, department`` (``snapshot_date`` is carried through).
    """
    grouped = (
        active
        .groupBy("store_id", "department", "snapshot_date")
        .agg(
            F.count(F.lit(1)).cast(DecimalType(18, 0)).alias("sku_count"),
            F.sum("on_hand_qty").cast(DecimalType(18, 0)).alias("on_hand_units"),
            F.round(F.sum(F.col("on_hand_qty") * F.col("avg_cost")), 2)
            .cast(MONEY).alias("cost_value"),
            F.round(F.sum(F.col("on_hand_qty") * F.col("retail_price")), 2)
            .cast(MONEY).alias("retail_value"),
        )
    )
    return grouped.select(*INVENTORY_VALUE_COLUMNS)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run(
    spark: SparkSession,
    business_date: str,
    *,
    data_dir: str,
    load_db: bool = True,
) -> str:
    """Execute the inventory snapshot pipeline; returns the output path."""
    in_dir = os.path.join(data_dir, "in")
    out_dir = os.path.join(data_dir, "out")
    os.makedirs(out_dir, exist_ok=True)

    inv_path = os.path.join(in_dir, f"inventory_{business_date}.dat")
    out_path = os.path.join(out_dir, f"inventory_value_{business_date}.dat")

    inventory = read_delimited(spark, inv_path, INVENTORY_SCHEMA, "|")

    active = filter_active(inventory)
    value = rollup_inventory(active)

    value_ordered = value.orderBy("store_id", "department")
    write_delimited(value_ordered, out_path, "|", INVENTORY_VALUE_COLUMNS)

    if load_db:
        # TRUNCATE/overwrite mode, matching run/inventory_snapshot.ksh.
        loaded = load_to_edw(value, EDW_TABLE, "truncate", INVENTORY_VALUE_COLUMNS)
        if not loaded:
            print(
                f"[inventory_snapshot] EDW load skipped (no credentials); "
                f"value written to {out_path}"
            )

    return out_path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inventory value snapshot (PySpark).")
    parser.add_argument("--business-date", required=True, help="BUSINESS_DATE (YYYYMMDD)")
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("AB_DATA_DIR", "data"),
        help="Root data directory containing in/, out/ (default: ./data).",
    )
    parser.add_argument(
        "--no-load-db",
        action="store_true",
        help="Skip the EDW JDBC load even if credentials are present.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    spark = build_spark("inventory_snapshot")
    try:
        out_path = run(
            spark,
            args.business_date,
            data_dir=args.data_dir,
            load_db=not args.no_load_db,
        )
        print(f"[inventory_snapshot] inventory_value -> {out_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
