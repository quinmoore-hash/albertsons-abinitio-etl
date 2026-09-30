from datetime import date
from decimal import Decimal

from spark.schemas import INVENTORY, POS_SALES, PRODUCT_DIM, STORE_DIM

BIZ_DATE = date(2026, 7, 13)


def D(value) -> Decimal:
    return Decimal(str(value))


def pos_row(**kw):
    row = {
        "store_id": D(1001),
        "register_id": D(1),
        "transaction_id": "T1",
        "business_date": BIZ_DATE,
        "transaction_ts": "08:00:00",
        "upc": D(4011),
        "product_desc": "BANANAS",
        "qty": D(1),
        "unit_price": D("0.29"),
        "ext_price": D("0.29"),
        "discount_amt": D("0.00"),
        "loyalty_id": None,
        "tender_type": "CASH",
        "void_flag": "N",
    }
    row.update({k: D(v) if isinstance(v, int | float) else v for k, v in kw.items()})
    return tuple(row[f] for f in POS_SALES.field_names)


def pos_df(spark, *rows):
    return spark.createDataFrame(list(rows), POS_SALES.schema)


def product_df(spark, *rows):
    default = [(D(4011), "BANANAS", "PRODUCE", "FRESH FRUIT", "DOLE", "N", D(150), D("0.19"))]
    return spark.createDataFrame(list(rows) or default, PRODUCT_DIM.schema)


def store_df(spark, *rows):
    default = [(D(1001), "Boise", "ALBERTSONS", "INTERMOUNTAIN", "ID", "America/Boise", "OPEN")]
    return spark.createDataFrame(list(rows) or default, STORE_DIM.schema)


def inv_row(
    store_id=1001, status="OPEN", upc=4011, dept="PRODUCE", qty=1, cost="0.19", retail="0.29"
):
    return (D(store_id), status, D(upc), dept, D(qty), D(cost), D(retail), BIZ_DATE)


def inv_df(spark, *rows):
    return spark.createDataFrame(list(rows), INVENTORY.schema)
