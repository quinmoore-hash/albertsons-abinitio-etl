"""REFORMAT ``cleanse`` from ``daily_pos_sales.mp`` (``xfr/pos_sales_cleanse.xfr``)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark.schemas import MONEY, POS_SALES

TRIMMED_FIELDS = ("transaction_id", "transaction_ts", "product_desc", "loyalty_id", "void_flag")


def is_valid_line() -> Column:
    """select_expr ``is_valid_line``: ``void_flag != "Y" && qty != 0``."""
    return (F.coalesce(F.trim(F.col("void_flag")), F.lit("")) != F.lit("Y")) & (
        F.coalesce(F.col("qty"), F.lit(0)) != F.lit(0)
    )


def normalize_tender(col: Column) -> Column:
    tender = F.upper(F.trim(col))
    return F.when(tender == F.lit("CC"), F.lit("CREDIT")).otherwise(tender)


def reconcile_ext_price(qty: Column, unit_price: Column, ext_price: Column) -> Column:
    """Keep the register ``ext_price`` only when it equals ``round(qty * unit_price, 2)``."""
    expected = F.round(qty * unit_price, 2).cast(MONEY)
    return F.when(ext_price.isNotNull() & (expected == ext_price), ext_price).otherwise(expected)


def cleanse(pos: DataFrame) -> DataFrame:
    """Filter to valid sale lines and apply the ``reformat`` rules.

    Output keeps the ``pos_sales`` layout so rows routed to the JOIN unused
    port can be written with ``dml/pos_sales.dml``.
    """
    unit_price = F.coalesce(F.col("unit_price"), F.lit(0).cast(MONEY))
    df = pos.filter(is_valid_line())
    df = df.withColumns(
        {
            **{c: F.trim(F.col(c)) for c in TRIMMED_FIELDS},
            "unit_price": unit_price,
            "ext_price": reconcile_ext_price(F.col("qty"), unit_price, F.col("ext_price")),
            "discount_amt": F.coalesce(F.col("discount_amt"), F.lit(0).cast(MONEY)),
            "tender_type": normalize_tender(F.col("tender_type")),
        }
    )
    return df.select(*POS_SALES.field_names)
