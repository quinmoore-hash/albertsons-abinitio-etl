"""PySpark port of ``mp/inventory_snapshot.mp``.

    INPUT FILE (inventory) -> FILTER_BY_EXPRESSION (active stores) -> SORT
                           -> ROLLUP (inventory_rollup.xfr)
                           -> OUTPUT FILE + m_db load EDW.F_INVENTORY_VALUE (truncate)

Usage: ``spark-submit pyspark/inventory_snapshot.py <BUSINESS_DATE:YYYYMMDD>``
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import config
import schemas
from io_utils import get_spark, load_to_edw, read_delimited, write_serial_file

ROLLUP_KEY = ["store_id", "department"]


def active_only(inventory: DataFrame) -> DataFrame:
    """FILTER_BY_EXPRESSION ``active_only``."""
    return inventory.filter(
        (F.col("on_hand_qty") >= F.lit(0)) & (F.col("store_status") == F.lit("OPEN"))
    )


def rollup(active: DataFrame) -> DataFrame:
    """ROLLUP ``inv_value`` to store x department."""
    aggregated = active.groupBy(*ROLLUP_KEY).agg(
        F.count(F.lit(1)).alias("sku_count"),
        F.sum("on_hand_qty").alias("on_hand_units"),
        F.round(F.sum(F.col("on_hand_qty") * F.col("avg_cost")), 2).alias("cost_value"),
        F.round(F.sum(F.col("on_hand_qty") * F.col("retail_price")), 2).alias("retail_value"),
        F.first("snapshot_date").alias("snapshot_date"),
    )

    return aggregated.select(
        *[F.col(field.name).cast(field.dataType) for field in schemas.inventory_value.fields]
    ).orderBy(*ROLLUP_KEY)


def run(spark: SparkSession, business_date: str) -> DataFrame:
    inventory = read_delimited(
        spark,
        config.inventory_input_file(business_date),
        schemas.inventory,
        config.INVENTORY_DELIMITER,
    )

    inventory_value = rollup(active_only(inventory)).cache()

    write_serial_file(
        inventory_value, config.inventory_output_file(business_date), config.OUTPUT_DELIMITER
    )
    load_to_edw(inventory_value, config.INVENTORY_VALUE_TABLE, mode="overwrite")

    print(
        f"[inventory_snapshot] business_date={business_date} rows={inventory_value.count()}"
    )
    return inventory_value


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: inventory_snapshot.py <BUSINESS_DATE:YYYYMMDD>", file=sys.stderr)
        return 1
    spark = get_spark("inventory_snapshot")
    try:
        run(spark, argv[1])
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
