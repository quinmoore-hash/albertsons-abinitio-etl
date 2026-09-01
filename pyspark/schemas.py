"""Explicit Spark schemas mirroring the Ab Initio DML record formats in ``dml/``.

Field order and names are identical to the ``.dml`` files. Ab Initio
``decimal("")`` is an exact decimal type, so it maps to ``DecimalType``
(never to a float) to preserve the legacy rounding behaviour;
``date("YYYY-MM-DD")`` maps to ``DateType`` and ``string`` to ``StringType``.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

# Identifiers: whole numbers (store/register/upc).
ID_TYPE = DecimalType(18, 0)
# Counts produced by rollups.
COUNT_TYPE = DecimalType(18, 0)
# Quantities: the register can emit fractional units for weighted items.
QTY_TYPE = DecimalType(18, 3)
# Currency amounts as they appear in the feeds (cents). Spark widens the scale
# automatically for products such as qty * unit_price, so the explicit
# decimal_round(x, 2) calls in the transforms stay exact.
AMOUNT_TYPE = DecimalType(18, 2)
# Rounded currency amounts written to the targets.
MONEY_TYPE = DecimalType(18, 2)

DATE_FORMAT = "yyyy-MM-dd"

# --- dml/pos_sales.dml ------------------------------------------------------
pos_sales = StructType(
    [
        StructField("store_id", ID_TYPE, True),
        StructField("register_id", ID_TYPE, True),
        StructField("transaction_id", StringType(), True),
        StructField("business_date", DateType(), True),
        StructField("transaction_ts", StringType(), True),
        StructField("upc", ID_TYPE, True),
        StructField("product_desc", StringType(), True),
        StructField("qty", QTY_TYPE, True),
        StructField("unit_price", AMOUNT_TYPE, True),
        StructField("ext_price", AMOUNT_TYPE, True),
        StructField("discount_amt", AMOUNT_TYPE, True),
        StructField("loyalty_id", StringType(), True),
        StructField("tender_type", StringType(), True),
        StructField("void_flag", StringType(), True),
    ]
)

# --- dml/product_dim.dml ----------------------------------------------------
product_dim = StructType(
    [
        StructField("upc", ID_TYPE, True),
        StructField("product_desc", StringType(), True),
        StructField("department", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("private_label_flag", StringType(), True),
        StructField("case_pack", ID_TYPE, True),
        StructField("avg_cost", AMOUNT_TYPE, True),
    ]
)

# --- dml/store_dim.dml ------------------------------------------------------
store_dim = StructType(
    [
        StructField("store_id", ID_TYPE, True),
        StructField("store_name", StringType(), True),
        StructField("banner", StringType(), True),
        StructField("region", StringType(), True),
        StructField("state", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

# --- dml/inventory.dml ------------------------------------------------------
inventory = StructType(
    [
        StructField("store_id", ID_TYPE, True),
        StructField("store_status", StringType(), True),
        StructField("upc", ID_TYPE, True),
        StructField("department", StringType(), True),
        StructField("on_hand_qty", QTY_TYPE, True),
        StructField("avg_cost", AMOUNT_TYPE, True),
        StructField("retail_price", AMOUNT_TYPE, True),
        StructField("snapshot_date", DateType(), True),
    ]
)

# --- dml/daily_sales_summary.dml -------------------------------------------
daily_sales_summary = StructType(
    [
        StructField("business_date", DateType(), True),
        StructField("store_id", ID_TYPE, True),
        StructField("banner", StringType(), True),
        StructField("region", StringType(), True),
        StructField("department", StringType(), True),
        StructField("category", StringType(), True),
        StructField("txn_count", COUNT_TYPE, True),
        StructField("unit_qty", QTY_TYPE, True),
        StructField("gross_sales", MONEY_TYPE, True),
        StructField("discount_total", MONEY_TYPE, True),
        StructField("net_sales", MONEY_TYPE, True),
        StructField("private_label_sales", MONEY_TYPE, True),
        StructField("loyalty_txn_count", COUNT_TYPE, True),
    ]
)

# --- dml/inventory_value.dml ------------------------------------------------
inventory_value = StructType(
    [
        StructField("snapshot_date", DateType(), True),
        StructField("store_id", ID_TYPE, True),
        StructField("department", StringType(), True),
        StructField("sku_count", COUNT_TYPE, True),
        StructField("on_hand_units", QTY_TYPE, True),
        StructField("cost_value", MONEY_TYPE, True),
        StructField("retail_value", MONEY_TYPE, True),
    ]
)
