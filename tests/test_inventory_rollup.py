from helpers import D, inv_df, inv_row

from spark.schemas import INVENTORY_VALUE
from spark.transforms.inventory_rollup import filter_active, rollup_inventory


def test_filter_active_only(spark):
    df = inv_df(
        spark,
        inv_row(upc=1),
        inv_row(upc=2, status="REMODEL"),
        inv_row(upc=3, status="CLOSED"),
        inv_row(upc=4, qty=-5),
        inv_row(upc=5, qty=0),
    )
    assert sorted(int(r.upc) for r in filter_active(df).collect()) == [1, 5]


def test_rollup_values(spark):
    df = inv_df(
        spark,
        inv_row(upc=1, qty=3, cost="0.3333", retail="0.99"),
        inv_row(upc=2, qty=2, cost="1.005", retail="2.49"),
        inv_row(upc=3, dept="DAIRY", qty=10, cost="1.00", retail="2.00"),
    )
    out = {r.department: r for r in rollup_inventory(filter_active(df)).collect()}
    produce = out["PRODUCE"]
    assert produce.sku_count == 2
    assert produce.on_hand_units == D(5)
    assert produce.cost_value == D("3.01")  # 0.9999 + 2.01 = 3.0099 -> 3.01
    assert produce.retail_value == D("7.95")
    assert out["DAIRY"].cost_value == D("10.00")


def test_output_schema_matches_dml(spark):
    out = rollup_inventory(inv_df(spark, inv_row()))
    assert [(f.name, f.dataType) for f in out.schema.fields] == [
        (f.name, f.dataType) for f in INVENTORY_VALUE.schema.fields
    ]
