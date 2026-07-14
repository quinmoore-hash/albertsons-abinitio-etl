"""Unit tests for the cleanse transform (port of pos_sales_cleanse.xfr)."""

from __future__ import annotations

from decimal import Decimal

from common.schemas import POS_SALES_SCHEMA
from jobs.daily_pos_sales import cleanse
from tests.helpers import date

D = Decimal

_DEFAULTS = {
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

_ORDER = [f.name for f in POS_SALES_SCHEMA.fields]


def _row(**overrides):
    values = {**_DEFAULTS, **overrides}
    return tuple(values[name] for name in _ORDER)


def _cleanse(spark, rows):
    df = spark.createDataFrame(rows, schema=POS_SALES_SCHEMA)
    return {r["transaction_id"]: r for r in cleanse(df).collect()}


def test_void_and_zero_qty_dropped(spark):
    rows = [
        _row(transaction_id="KEEP"),
        _row(transaction_id="VOID", void_flag="Y"),
        _row(transaction_id="ZERO", qty=D("0")),
    ]
    out = _cleanse(spark, rows)
    assert set(out) == {"KEEP"}


def test_tender_cc_normalized_to_credit(spark):
    rows = [
        _row(transaction_id="A", tender_type=" cc "),
        _row(transaction_id="B", tender_type="credit"),
        _row(transaction_id="C", tender_type="debit"),
    ]
    out = _cleanse(spark, rows)
    assert out["A"]["tender_type"] == "CREDIT"
    assert out["B"]["tender_type"] == "CREDIT"
    assert out["C"]["tender_type"] == "DEBIT"


def test_null_unit_price_and_discount_filled_zero(spark):
    rows = [_row(transaction_id="A", unit_price=None, discount_amt=None, ext_price=None)]
    out = _cleanse(spark, rows)
    assert out["A"]["unit_price"] == D("0.00")
    assert out["A"]["discount_amt"] == D("0.00")


def test_ext_price_recomputed_when_not_reconciling(spark):
    # feed ext_price is wrong -> recompute round(qty*unit_price, 2)
    rows = [_row(transaction_id="A", qty=D("3"), unit_price=D("1.00"), ext_price=D("9.99"))]
    out = _cleanse(spark, rows)
    assert out["A"]["ext_price"] == D("3.00")


def test_ext_price_kept_when_reconciling(spark):
    rows = [_row(transaction_id="A", qty=D("2"), unit_price=D("5.99"), ext_price=D("11.98"))]
    out = _cleanse(spark, rows)
    assert out["A"]["ext_price"] == D("11.98")


def test_string_fields_trimmed(spark):
    rows = [_row(
        transaction_id="  T1  ",
        loyalty_id="  JU1  ",
        product_desc="  BANANAS  ",
    )]
    df = spark.createDataFrame(rows, schema=POS_SALES_SCHEMA)
    r = cleanse(df).collect()[0]
    assert r["transaction_id"] == "T1"
    assert r["loyalty_id"] == "JU1"
    assert r["product_desc"] == "BANANAS"
