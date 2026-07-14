"""Tests for the inventory snapshot pipeline (filter + value rollup)."""

from __future__ import annotations

from decimal import Decimal

from common.schemas import INVENTORY_SCHEMA
from jobs.inventory_snapshot import filter_active, rollup_inventory
from tests.helpers import date

D = Decimal

_ORDER = [f.name for f in INVENTORY_SCHEMA.fields]

_DEFAULTS = {
    "store_id": D("1001"),
    "store_status": "OPEN",
    "upc": D("4011"),
    "department": "PRODUCE",
    "on_hand_qty": D("10"),
    "avg_cost": D("1.00"),
    "retail_price": D("2.00"),
    "snapshot_date": date("2026-07-13"),
}


def _inv(**overrides):
    values = {**_DEFAULTS, **overrides}
    return tuple(values[name] for name in _ORDER)


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=INVENTORY_SCHEMA)


def test_filter_excludes_closed_and_negative(spark):
    rows = [
        _inv(store_id=D("1001"), store_status="OPEN", on_hand_qty=D("5")),
        _inv(store_id=D("1002"), store_status="REMODEL", on_hand_qty=D("5")),
        _inv(store_id=D("1003"), store_status="CLOSED", on_hand_qty=D("5")),
        _inv(store_id=D("1004"), store_status="OPEN", on_hand_qty=D("-3")),
    ]
    kept = {r["store_id"] for r in filter_active(_frame(spark, rows)).collect()}
    assert kept == {D("1001")}


def test_inventory_value_calculation(spark):
    # Two SKUs in store 1001 / DAIRY
    rows = [
        _inv(store_id=D("1001"), department="DAIRY", upc=D("1"),
             on_hand_qty=D("85"), avg_cost=D("1.35"), retail_price=D("2.49")),
        _inv(store_id=D("1001"), department="DAIRY", upc=D("2"),
             on_hand_qty=D("60"), avg_cost=D("3.10"), retail_price=D("4.29")),
    ]
    active = filter_active(_frame(spark, rows))
    row = rollup_inventory(active).collect()[0]

    assert row["snapshot_date"] == date("2026-07-13")
    assert row["sku_count"] == D("2")
    assert row["on_hand_units"] == D("145")            # 85 + 60
    assert row["cost_value"] == D("300.75")            # 85*1.35 + 60*3.10
    assert row["retail_value"] == D("469.05")          # 85*2.49 + 60*4.29
