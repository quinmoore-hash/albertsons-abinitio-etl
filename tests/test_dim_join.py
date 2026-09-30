from helpers import D, pos_df, pos_row, product_df, store_df

from spark.schemas import POS_SALES
from spark.transforms.dim_join import JOINED_FIELDS, join_dimensions


def test_matched_rows_carry_dim_attributes(spark):
    res = join_dimensions(pos_df(spark, pos_row()), product_df(spark), store_df(spark))
    [row] = res.joined.collect()
    assert res.joined.columns == list(JOINED_FIELDS)
    assert (row.department, row.category, row.brand, row.private_label_flag) == (
        "PRODUCE",
        "FRESH FRUIT",
        "DOLE",
        "N",
    )
    assert (row.banner, row.region) == ("ALBERTSONS", "INTERMOUNTAIN")
    assert res.rejects.count() == 0


def test_unknown_upc_goes_to_reject_port(spark):
    sales = pos_df(spark, pos_row(), pos_row(transaction_id="X", upc=9999999999))
    res = join_dimensions(sales, product_df(spark), store_df(spark))
    assert res.joined.count() == 1
    [rej] = res.rejects.collect()
    assert rej.upc == D(9999999999) and rej.transaction_id == "X"
    assert res.rejects.columns == POS_SALES.field_names


def test_unknown_store_goes_to_reject_port(spark):
    sales = pos_df(spark, pos_row(store_id=7777))
    res = join_dimensions(sales, product_df(spark), store_df(spark))
    assert res.joined.count() == 0
    assert [r.store_id for r in res.rejects.collect()] == [D(7777)]


def test_null_brand_does_not_reject(spark):
    product = product_df(
        spark, (D(4053), "AVOCADO", "PRODUCE", "FRESH FRUIT", None, "N", D(48), D("0.55"))
    )
    res = join_dimensions(pos_df(spark, pos_row(upc=4053)), product, store_df(spark))
    assert res.joined.count() == 1 and res.rejects.count() == 0
