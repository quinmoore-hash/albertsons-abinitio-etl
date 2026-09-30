"""Explicit Spark schemas for every Ab Initio record format under ``dml/``.

Ab Initio delimited ``decimal("<delim>")`` fields carry no declared size, so each
field is mapped to a fixed ``DecimalType`` wide enough for the domain:

========================  ==================  =========================================
Domain                    Spark type          Used for
========================  ==================  =========================================
identifier                ``decimal(10,0)``   store_id, register_id, case_pack
UPC / GTIN-14             ``decimal(14,0)``   upc
quantity                  ``decimal(12,3)``   qty, on_hand_qty (weighed items allowed)
money                     ``decimal(12,2)``   unit_price, ext_price, discount_amt, retail
unit cost                 ``decimal(12,4)``   avg_cost
count (aggregate)         ``decimal(18,0)``   txn_count, sku_count, loyalty_txn_count
quantity (aggregate)      ``decimal(18,3)``   unit_qty, on_hand_units
money (aggregate)         ``decimal(18,2)``   gross/net/discount/PL sales, cost/retail value
========================  ==================  =========================================

Ab Initio fields are non-nullable unless the transforms treat them as optional.
Fields marked ``nullable=True`` below are the ones the XFRs null-check
(``is_null``/``is_blank``) or that the DML documents as nullable.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    DataType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

ID = DecimalType(10, 0)
UPC = DecimalType(14, 0)
QTY = DecimalType(12, 3)
MONEY = DecimalType(12, 2)
COST = DecimalType(12, 4)
COUNT_AGG = DecimalType(18, 0)
QTY_AGG = DecimalType(18, 3)
MONEY_AGG = DecimalType(18, 2)

# Quantities are written without trailing zeros, the way the scale-less
# ``decimal("<delim>")`` feeds carry them (``6``, ``1.235``).
VARIABLE_SCALE_TYPES = (QTY, QTY_AGG)

DATE_FORMAT = "yyyy-MM-dd"


def _f(name: str, data_type: DataType, nullable: bool = False) -> StructField:
    return StructField(name, data_type, nullable)


@dataclass(frozen=True)
class RecordFormat:
    """A DML record: Spark schema plus the delimited-file layout it was declared with."""

    name: str
    schema: StructType
    delimiter: str
    date_format: str = DATE_FORMAT

    @property
    def field_names(self) -> list[str]:
        return self.schema.fieldNames()

    def csv_options(self) -> dict[str, str]:
        # Ab Initio delimited records have no header and no quoting.
        return {
            "sep": self.delimiter,
            "header": "false",
            "quote": "",
            "dateFormat": self.date_format,
        }


# dml/pos_sales.dml
POS_SALES = RecordFormat(
    name="pos_sales",
    delimiter=",",
    schema=StructType(
        [
            _f("store_id", ID),
            _f("register_id", ID),
            _f("transaction_id", StringType()),
            _f("business_date", DateType()),
            _f("transaction_ts", StringType()),
            _f("upc", UPC),
            _f("product_desc", StringType()),
            _f("qty", QTY),
            _f("unit_price", MONEY, nullable=True),
            _f("ext_price", MONEY),
            _f("discount_amt", MONEY, nullable=True),
            _f("loyalty_id", StringType(), nullable=True),
            _f("tender_type", StringType()),
            _f("void_flag", StringType()),
        ]
    ),
)

# dml/product_dim.dml
PRODUCT_DIM = RecordFormat(
    name="product_dim",
    delimiter=",",
    schema=StructType(
        [
            _f("upc", UPC),
            _f("product_desc", StringType()),
            _f("department", StringType()),
            _f("category", StringType()),
            _f("brand", StringType(), nullable=True),
            _f("private_label_flag", StringType()),
            _f("case_pack", ID),
            _f("avg_cost", COST),
        ]
    ),
)

# dml/store_dim.dml
STORE_DIM = RecordFormat(
    name="store_dim",
    delimiter=",",
    schema=StructType(
        [
            _f("store_id", ID),
            _f("store_name", StringType()),
            _f("banner", StringType()),
            _f("region", StringType()),
            _f("state", StringType()),
            _f("timezone", StringType()),
            _f("status", StringType()),
        ]
    ),
)

# dml/daily_sales_summary.dml  ->  EDW.F_DAILY_SALES_SUMMARY
DAILY_SALES_SUMMARY = RecordFormat(
    name="daily_sales_summary",
    delimiter="|",
    schema=StructType(
        [
            _f("business_date", DateType()),
            _f("store_id", ID),
            _f("banner", StringType()),
            _f("region", StringType()),
            _f("department", StringType()),
            _f("category", StringType()),
            _f("txn_count", COUNT_AGG),
            _f("unit_qty", QTY_AGG),
            _f("gross_sales", MONEY_AGG),
            _f("discount_total", MONEY_AGG),
            _f("net_sales", MONEY_AGG),
            _f("private_label_sales", MONEY_AGG),
            _f("loyalty_txn_count", COUNT_AGG),
        ]
    ),
)

# dml/inventory.dml
INVENTORY = RecordFormat(
    name="inventory",
    delimiter="|",
    schema=StructType(
        [
            _f("store_id", ID),
            _f("store_status", StringType()),
            _f("upc", UPC),
            _f("department", StringType()),
            _f("on_hand_qty", QTY),
            _f("avg_cost", COST),
            _f("retail_price", MONEY),
            _f("snapshot_date", DateType()),
        ]
    ),
)

# dml/inventory_value.dml  ->  EDW.F_INVENTORY_VALUE
INVENTORY_VALUE = RecordFormat(
    name="inventory_value",
    delimiter="|",
    schema=StructType(
        [
            _f("snapshot_date", DateType()),
            _f("store_id", ID),
            _f("department", StringType()),
            _f("sku_count", COUNT_AGG),
            _f("on_hand_units", QTY_AGG),
            _f("cost_value", MONEY_AGG),
            _f("retail_value", MONEY_AGG),
        ]
    ),
)

RECORD_FORMATS: dict[str, RecordFormat] = {
    rf.name: rf
    for rf in (
        POS_SALES,
        PRODUCT_DIM,
        STORE_DIM,
        DAILY_SALES_SUMMARY,
        INVENTORY,
        INVENTORY_VALUE,
    )
}
