from datetime import date

from helpers import D
from pyspark.sql.types import StructField, StructType

from spark.schemas import DAILY_SALES_SUMMARY, POS_SALES, PRODUCT_DIM, STORE_DIM
from spark.transforms.dim_join import JOINED_FIELDS
from spark.transforms.sales_rollup import rollup_sales


def joined_row(**kw):
    row = {
        "business_date": date(2026, 7, 13),
        "store_id": D(1001),
        "transaction_id": "T1",
        "qty": D(1),
        "ext_price": D("1.00"),
        "discount_amt": D("0.00"),
        "loyalty_id": None,
        "upc": D(1),
        "department": "GROCERY",
        "category": "CEREAL",
        "brand": "B",
        "private_label_flag": "N",
        "banner": "SAFEWAY",
        "region": "DENVER",
    }
    row.update(kw)
    return tuple(row[f] for f in JOINED_FIELDS)


def _schema():
    by_name = {f.name: f for rf in (POS_SALES, PRODUCT_DIM, STORE_DIM) for f in rf.schema.fields}
    return StructType([StructField(n, by_name[n].dataType, True) for n in JOINED_FIELDS])


def test_accumulations(spark):
    df = spark.createDataFrame(
        [
            joined_row(
                qty=D(2),
                ext_price=D("5.00"),
                discount_amt=D("0.50"),
                loyalty_id="JU1",
                private_label_flag="Y",
            ),
            joined_row(
                transaction_id="T2",
                qty=D(1),
                ext_price=D("3.33"),
                discount_amt=D("0.00"),
                loyalty_id="  ",
            ),
            joined_row(
                transaction_id="T3", qty=D(-1), ext_price=D("-1.00"), private_label_flag="Y"
            ),
            joined_row(category="MILK", ext_price=D("9.99")),
        ],
        _schema(),
    )
    out = {r.category: r for r in rollup_sales(df).collect()}
    cereal = out["CEREAL"]
    assert cereal.txn_count == 3
    assert cereal.unit_qty == D(2)
    assert cereal.gross_sales == D("7.33")
    assert cereal.discount_total == D("0.50")
    assert cereal.net_sales == D("6.83")
    assert cereal.private_label_sales == D("3.50")  # (5.00 - 0.50) + (-1.00 - 0)
    assert cereal.loyalty_txn_count == 1  # blank loyalty ids do not count
    assert (cereal.banner, cereal.region) == ("SAFEWAY", "DENVER")
    assert out["MILK"].txn_count == 1 and out["MILK"].gross_sales == D("9.99")


def test_output_schema_matches_dml(spark):
    df = spark.createDataFrame([joined_row()], _schema())
    assert rollup_sales(df).schema.fieldNames() == DAILY_SALES_SUMMARY.field_names
    assert [f.dataType for f in rollup_sales(df).schema.fields] == [
        f.dataType for f in DAILY_SALES_SUMMARY.schema.fields
    ]
