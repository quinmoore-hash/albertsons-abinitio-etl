"""``inventory_snapshot.mp``: FILTER_BY_EXPRESSION ``active_only`` + ROLLUP ``inv_value``
(``xfr/inventory_rollup.xfr``)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark.common.io import conform
from spark.schemas import INVENTORY_VALUE

ROLLUP_KEY = ("snapshot_date", "store_id", "department")


def active_only() -> Column:
    """select_expr: ``in.on_hand_qty >= 0 && in.store_status == "OPEN"``."""
    return (F.col("on_hand_qty") >= F.lit(0)) & (F.col("store_status") == F.lit("OPEN"))


def filter_active(inventory: DataFrame) -> DataFrame:
    return inventory.filter(active_only())


def rollup_inventory(active: DataFrame) -> DataFrame:
    """``snapshot_date`` is part of the key; the graph keyed on store/department and
    took ``in.snapshot_date`` in finalize, which is identical for a single-date feed."""
    agg = active.groupBy(*ROLLUP_KEY).agg(
        F.count(F.lit(1)).alias("sku_count"),
        F.sum("on_hand_qty").alias("on_hand_units"),
        F.round(F.sum(F.col("on_hand_qty") * F.col("avg_cost")), 2).alias("cost_value"),
        F.round(F.sum(F.col("on_hand_qty") * F.col("retail_price")), 2).alias("retail_value"),
    )
    return conform(agg, INVENTORY_VALUE)
