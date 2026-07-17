"""Join (reject path) and rollup tests for the daily POS sales pipeline."""

from __future__ import annotations

from decimal import Decimal

from common.schemas import POS_SALES_SCHEMA, PRODUCT_DIM_SCHEMA, STORE_DIM_SCHEMA
from jobs.daily_pos_sales import join_dims, rollup_sales
from tests.helpers import date

D = Decimal

_POS_ORDER = [f.name for f in POS_SALES_SCHEMA.fields]

_POS_DEFAULTS = {
    "store_id": D("1001"),
    "register_id": D("1"),
    "transaction_id": "T1",
    "business_date": date("2026-07-13"),
    "transaction_ts": "08:00:00",
    "upc": D("4011"),
    "product_desc": "BANANAS",
    "qty": D("1"),
    "unit_price": D("1.00"),
    "ext_price": D("1.00"),
    "discount_amt": D("0.00"),
    "loyalty_id": None,
    "tender_type": "CASH",
    "void_flag": "N",
}


def _pos(**overrides):
    values = {**_POS_DEFAULTS, **overrides}
    return tuple(values[name] for name in _POS_ORDER)


def _product(upc, department, category, pl_flag="N", brand="B"):
    # upc, product_desc, department, category, brand, private_label_flag, case_pack, avg_cost
    return (D(str(upc)), "DESC", department, category, brand, pl_flag, D("1"), D("1.00"))


def _store(store_id, banner, region):
    # store_id, store_name, banner, region, state, timezone, status
    return (D(str(store_id)), "NAME", banner, region, "ST", "TZ", "OPEN")


def _frames(spark, pos_rows, products, stores):
    cleansed = spark.createDataFrame(pos_rows, schema=POS_SALES_SCHEMA)
    product_dim = spark.createDataFrame(products, schema=PRODUCT_DIM_SCHEMA)
    store_dim = spark.createDataFrame(stores, schema=STORE_DIM_SCHEMA)
    return cleansed, product_dim, store_dim


def test_unknown_upc_goes_to_reject(spark):
    cleansed, product_dim, store_dim = _frames(
        spark,
        [
            _pos(transaction_id="OK", upc=D("4011")),
            _pos(transaction_id="BAD", upc=D("9999999999")),
        ],
        [_product(4011, "PRODUCE", "FRESH FRUIT")],
        [_store(1001, "ALBERTSONS", "INTERMOUNTAIN")],
    )
    matched, rejects = join_dims(cleansed, product_dim, store_dim)

    reject_ids = [r["transaction_id"] for r in rejects.collect()]
    matched_ids = [r["transaction_id"] for r in matched.collect()]
    assert reject_ids == ["BAD"]
    assert matched_ids == ["OK"]
    # reject carries the full pos_sales schema/format
    assert [f.name for f in rejects.schema.fields] == _POS_ORDER


def test_unknown_store_goes_to_reject(spark):
    cleansed, product_dim, store_dim = _frames(
        spark,
        [_pos(transaction_id="BADSTORE", store_id=D("7777"), upc=D("4011"))],
        [_product(4011, "PRODUCE", "FRESH FRUIT")],
        [_store(1001, "ALBERTSONS", "INTERMOUNTAIN")],
    )
    matched, rejects = join_dims(cleansed, product_dim, store_dim)
    assert matched.count() == 0
    assert [r["transaction_id"] for r in rejects.collect()] == ["BADSTORE"]


def test_rollup_aggregates(spark):
    # Two lines in the same grain (store 1001 / PRODUCE / FRESH FRUIT) plus one
    # private-label line with a loyalty id and a discount.
    cleansed, product_dim, store_dim = _frames(
        spark,
        [
            _pos(transaction_id="A", upc=D("4011"), qty=D("6"),
                 ext_price=D("1.74"), discount_amt=D("0.00"), loyalty_id=None),
            _pos(transaction_id="B", upc=D("4011"), qty=D("4"),
                 ext_price=D("3.96"), discount_amt=D("0.00"), loyalty_id="JU1"),
            _pos(transaction_id="C", upc=D("4053"), qty=D("2"),
                 ext_price=D("5.00"), discount_amt=D("0.50"), loyalty_id="JU2"),
        ],
        [
            _product(4011, "PRODUCE", "FRESH FRUIT", pl_flag="N"),
            _product(4053, "PRODUCE", "FRESH FRUIT", pl_flag="Y"),
        ],
        [_store(1001, "ALBERTSONS", "INTERMOUNTAIN")],
    )
    matched, _ = join_dims(cleansed, product_dim, store_dim)
    row = rollup_sales(matched).collect()[0]

    assert row["banner"] == "ALBERTSONS"
    assert row["region"] == "INTERMOUNTAIN"
    assert row["txn_count"] == D("3")          # counts lines
    assert row["unit_qty"] == D("12")          # 6 + 4 + 2
    assert row["gross_sales"] == D("10.70")    # 1.74 + 3.96 + 5.00
    assert row["discount_total"] == D("0.50")
    assert row["net_sales"] == D("10.20")      # 10.70 - 0.50
    assert row["private_label_sales"] == D("4.50")  # line C only: 5.00 - 0.50
    assert row["loyalty_txn_count"] == D("2")  # B and C
