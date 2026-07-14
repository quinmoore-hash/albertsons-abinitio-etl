"""Explicit Spark schemas mirroring the Ab Initio ``dml/*.dml`` record formats.

Each ``StructType`` below preserves the exact field order and types of the
corresponding ``.dml`` file so the PySpark jobs read the flat feeds identically
to the legacy Co>Operating System graphs.

Delimiter reference (from the ``.dml`` declarations):

* ``pos_sales.dml``          -> COMMA delimited
* ``product_dim.dml``        -> COMMA delimited
* ``store_dim.dml``          -> COMMA delimited
* ``inventory.dml``          -> PIPE delimited
* ``daily_sales_summary.dml``-> PIPE delimited
* ``inventory_value.dml``    -> PIPE delimited

Dates use the format ``yyyy-MM-dd``.

Ab Initio ``decimal("")`` maps to Spark ``DecimalType``:

* money / currency values      -> ``DecimalType(18, 2)``
* counts, quantities and ids   -> ``DecimalType(18, 0)`` (kept as they appear
  in the feed rather than widened to floating point)
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

# Shared date format for every date column across the feeds.
DATE_FORMAT = "yyyy-MM-dd"

# Precision/scale helpers so intent is explicit at every field.
MONEY = DecimalType(18, 2)   # currency amounts, scale 2
COUNT = DecimalType(18, 0)   # counts / quantities / ids, scale 0

# ---------------------------------------------------------------------------
# Input feeds
# ---------------------------------------------------------------------------

# dml/pos_sales.dml  (COMMA delimited)
POS_SALES_SCHEMA = StructType([
    StructField("store_id", COUNT, True),
    StructField("register_id", COUNT, True),
    StructField("transaction_id", StringType(), True),
    StructField("business_date", DateType(), True),
    StructField("transaction_ts", StringType(), True),
    StructField("upc", COUNT, True),
    StructField("product_desc", StringType(), True),
    StructField("qty", COUNT, True),
    StructField("unit_price", MONEY, True),
    StructField("ext_price", MONEY, True),
    StructField("discount_amt", MONEY, True),
    StructField("loyalty_id", StringType(), True),
    StructField("tender_type", StringType(), True),
    StructField("void_flag", StringType(), True),
])

# dml/product_dim.dml  (COMMA delimited)
PRODUCT_DIM_SCHEMA = StructType([
    StructField("upc", COUNT, True),
    StructField("product_desc", StringType(), True),
    StructField("department", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("private_label_flag", StringType(), True),
    StructField("case_pack", COUNT, True),
    StructField("avg_cost", MONEY, True),
])

# dml/store_dim.dml  (COMMA delimited)
STORE_DIM_SCHEMA = StructType([
    StructField("store_id", COUNT, True),
    StructField("store_name", StringType(), True),
    StructField("banner", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("status", StringType(), True),
])

# dml/inventory.dml  (PIPE delimited)
INVENTORY_SCHEMA = StructType([
    StructField("store_id", COUNT, True),
    StructField("store_status", StringType(), True),
    StructField("upc", COUNT, True),
    StructField("department", StringType(), True),
    StructField("on_hand_qty", COUNT, True),
    StructField("avg_cost", MONEY, True),
    StructField("retail_price", MONEY, True),
    StructField("snapshot_date", DateType(), True),
])

# ---------------------------------------------------------------------------
# Output / target record formats
# ---------------------------------------------------------------------------

# dml/daily_sales_summary.dml  (PIPE delimited)
DAILY_SALES_SUMMARY_SCHEMA = StructType([
    StructField("business_date", DateType(), True),
    StructField("store_id", COUNT, True),
    StructField("banner", StringType(), True),
    StructField("region", StringType(), True),
    StructField("department", StringType(), True),
    StructField("category", StringType(), True),
    StructField("txn_count", COUNT, True),
    StructField("unit_qty", COUNT, True),
    StructField("gross_sales", MONEY, True),
    StructField("discount_total", MONEY, True),
    StructField("net_sales", MONEY, True),
    StructField("private_label_sales", MONEY, True),
    StructField("loyalty_txn_count", COUNT, True),
])

# dml/inventory_value.dml  (PIPE delimited)
INVENTORY_VALUE_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), True),
    StructField("store_id", COUNT, True),
    StructField("department", StringType(), True),
    StructField("sku_count", COUNT, True),
    StructField("on_hand_units", COUNT, True),
    StructField("cost_value", MONEY, True),
    StructField("retail_value", MONEY, True),
])

# Convenience: column-name lists in DML order for writers.
POS_SALES_COLUMNS = [f.name for f in POS_SALES_SCHEMA.fields]
DAILY_SALES_SUMMARY_COLUMNS = [f.name for f in DAILY_SALES_SUMMARY_SCHEMA.fields]
INVENTORY_VALUE_COLUMNS = [f.name for f in INVENTORY_VALUE_SCHEMA.fields]
