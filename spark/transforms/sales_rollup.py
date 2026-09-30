"""ROLLUP ``sales_rollup`` from ``daily_pos_sales.mp`` (``xfr/sales_rollup.xfr``).

Accumulations match the XFR exactly, including its documented line-grain
approximation: ``txn_count`` and ``loyalty_txn_count`` count joined *lines*,
not distinct ``transaction_id`` values.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.io import conform
from spark.schemas import DAILY_SALES_SUMMARY, MONEY_AGG

ROLLUP_KEY = ("business_date", "store_id", "department", "category")


def _is_blank(col_name: str):
    return F.coalesce(F.trim(F.col(col_name)), F.lit("")) == F.lit("")


def rollup_sales(joined: DataFrame) -> DataFrame:
    zero = F.lit(0).cast(MONEY_AGG)
    agg = joined.groupBy(*ROLLUP_KEY).agg(
        # banner/region are functionally dependent on store_id (finalize reads in.*)
        F.max("banner").alias("banner"),
        F.max("region").alias("region"),
        F.count(F.lit(1)).alias("txn_count"),
        F.sum("qty").alias("unit_qty"),
        F.sum("ext_price").alias("_gross"),
        F.sum("discount_amt").alias("_discount"),
        F.sum(
            F.when(
                F.col("private_label_flag") == F.lit("Y"),
                F.col("ext_price") - F.col("discount_amt"),
            ).otherwise(zero)
        ).alias("_private_label"),
        F.sum(F.when(_is_blank("loyalty_id"), F.lit(0)).otherwise(F.lit(1))).alias(
            "loyalty_txn_count"
        ),
    )
    out = agg.withColumns(
        {
            "gross_sales": F.round(F.col("_gross"), 2),
            "discount_total": F.round(F.col("_discount"), 2),
            "net_sales": F.round(F.col("_gross") - F.col("_discount"), 2),
            "private_label_sales": F.round(F.col("_private_label"), 2),
        }
    )
    return conform(out, DAILY_SALES_SUMMARY)
