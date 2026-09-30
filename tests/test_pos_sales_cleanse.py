from helpers import D, pos_df, pos_row

from spark.schemas import POS_SALES
from spark.transforms.pos_sales_cleanse import cleanse


def _one(spark, **kw):
    rows = cleanse(pos_df(spark, pos_row(**kw))).collect()
    assert len(rows) == 1
    return rows[0]


def test_drops_voided_and_zero_qty_lines(spark):
    df = pos_df(
        spark,
        pos_row(transaction_id="KEEP"),
        pos_row(transaction_id="VOID", void_flag="Y"),
        pos_row(transaction_id="VOID_PADDED", void_flag=" Y "),
        pos_row(transaction_id="ZERO", qty=0, ext_price=0),
        pos_row(transaction_id="NEG", qty=-1, ext_price=-0.29),
    )
    assert sorted(r.transaction_id for r in cleanse(df).collect()) == ["KEEP", "NEG"]


def test_blank_void_flag_is_kept(spark):
    assert _one(spark, void_flag="").transaction_id == "T1"


def test_null_fill_unit_price_and_discount(spark):
    row = _one(spark, unit_price=None, discount_amt=None, ext_price=1.00)
    assert row.unit_price == D("0.00")
    assert row.discount_amt == D("0.00")
    assert row.ext_price == D("0.00")  # recomputed as qty * 0


def test_ext_price_kept_when_reconciled(spark):
    assert _one(spark, qty=6, unit_price=0.29, ext_price=1.74).ext_price == D("1.74")


def test_ext_price_recomputed_when_not_reconciled(spark):
    assert _one(spark, qty=3, unit_price=0.29, ext_price=9.99).ext_price == D("0.87")
    # rounded to 2 decimals (half up): 1.5 * 0.33 = 0.495 -> 0.50
    assert _one(spark, qty=1.5, unit_price=0.33, ext_price=0.49).ext_price == D("0.50")


def test_tender_normalization(spark):
    assert _one(spark, tender_type="CC").tender_type == "CREDIT"
    assert _one(spark, tender_type=" cc ").tender_type == "CREDIT"
    assert _one(spark, tender_type="debit ").tender_type == "DEBIT"
    assert _one(spark, tender_type="CREDIT").tender_type == "CREDIT"


def test_trims_string_fields(spark):
    row = _one(spark, transaction_id="  T9 ", product_desc=" EGGS  ", loyalty_id=" JU1 ")
    assert (row.transaction_id, row.product_desc, row.loyalty_id) == ("T9", "EGGS", "JU1")


def test_output_keeps_pos_sales_layout(spark):
    assert cleanse(pos_df(spark, pos_row())).columns == POS_SALES.field_names
