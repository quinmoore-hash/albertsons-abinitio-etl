"""Parity tests for the daily_pos_sales port against the sample feed."""

from __future__ import annotations

from decimal import Decimal

import config
import daily_pos_sales

# business_date | store_id | banner | region | department | category |
# txn_count | unit_qty | gross | discount | net | private_label | loyalty
EXPECTED_SUMMARY = [
    ("2026-07-13", 1001, "ALBERTSONS", "INTERMOUNTAIN", "DAIRY", "EGGS", 1, 1, "2.49", "0.30", "2.19", "2.19", 1),
    ("2026-07-13", 1001, "ALBERTSONS", "INTERMOUNTAIN", "GROCERY", "BEVERAGES", 1, 2, "11.98", "1.00", "10.98", "0.00", 1),
    ("2026-07-13", 1001, "ALBERTSONS", "INTERMOUNTAIN", "PRODUCE", "FRESH FRUIT", 1, 6, "1.74", "0.00", "1.74", "0.00", 0),
    ("2026-07-13", 1042, "SAFEWAY", "DENVER", "DAIRY", "MILK", 1, 1, "4.29", "0.00", "4.29", "4.29", 0),
    ("2026-07-13", 1042, "SAFEWAY", "DENVER", "GROCERY", "CEREAL", 1, 1, "3.99", "0.50", "3.49", "0.00", 1),
    ("2026-07-13", 1042, "SAFEWAY", "DENVER", "PRODUCE", "FRESH FRUIT", 1, 4, "3.96", "0.00", "3.96", "0.00", 0),
    ("2026-07-13", 2255, "VONS", "SOCAL", "GROCERY", "BEVERAGES", 1, 3, "10.47", "0.00", "10.47", "10.47", 1),
    ("2026-07-13", 2255, "VONS", "SOCAL", "MEAT", "POULTRY", 1, 2, "7.98", "0.00", "7.98", "7.98", 1),
    ("2026-07-13", 3300, "JEWEL-OSCO", "MIDWEST", "GROCERY", "CEREAL", 1, 2, "7.98", "0.00", "7.98", "0.00", 0),
]


def _run(spark, business_date):
    if not hasattr(_run, "cache"):
        _run.cache = daily_pos_sales.run(spark, business_date)
    return _run.cache


def test_summary_matches_legacy_expected_results(spark, business_date):
    summary, _ = _run(spark, business_date)
    rows = [
        (
            row.business_date.strftime("%Y-%m-%d"),
            int(row.store_id),
            row.banner,
            row.region,
            row.department,
            row.category,
            int(row.txn_count),
            int(row.unit_qty),
            str(row.gross_sales),
            str(row.discount_total),
            str(row.net_sales),
            str(row.private_label_sales),
            int(row.loyalty_txn_count),
        )
        for row in summary.collect()
    ]
    assert rows == EXPECTED_SUMMARY


def test_voided_line_is_dropped(spark, business_date):
    """T0501 is voided, so store 2255 PRODUCE never reaches the summary."""
    summary, rejected = _run(spark, business_date)
    assert summary.filter("store_id = 2255 and department = 'PRODUCE'").count() == 0
    assert rejected.filter("transaction_id = 'T0501'").count() == 0


def test_unknown_upc_is_rejected(spark, business_date):
    summary, rejected = _run(spark, business_date)
    reject_rows = rejected.collect()
    assert len(reject_rows) == 1
    assert int(reject_rows[0].upc) == 9999999999
    assert reject_rows[0].transaction_id == "T0901"
    assert summary.filter("store_id = 3300").count() == 1


def test_dimension_row_with_null_attributes_still_matches(spark, business_date):
    """Reject routing keys off the join key, not off nullable attributes."""
    import schemas
    from io_utils import read_delimited

    pos = read_delimited(
        spark, config.pos_input_file(business_date), schemas.pos_sales, config.POS_DELIMITER
    ).filter("upc = 4011 and void_flag = 'N'")
    product_dim = read_delimited(
        spark, config.product_dim_file(), schemas.product_dim, config.DIM_DELIMITER
    ).withColumn("department", daily_pos_sales.F.lit(None).cast("string"))
    store_dim = read_delimited(
        spark, config.store_dim_file(), schemas.store_dim, config.DIM_DELIMITER
    )

    matched, rejected = daily_pos_sales.join_dimensions(
        daily_pos_sales.cleanse(pos), product_dim, store_dim
    )
    assert rejected.count() == 0
    assert matched.count() == 1


def test_tender_normalization_maps_cc_to_credit(spark, business_date):
    pos = daily_pos_sales.read_delimited(
        spark, config.pos_input_file(business_date), daily_pos_sales.schemas.pos_sales, config.POS_DELIMITER
    )
    tenders = {row.tender_type for row in daily_pos_sales.cleanse(pos).select("tender_type").collect()}
    assert tenders == {"CASH", "CREDIT", "DEBIT", "EBT"}


def test_ext_price_is_recomputed_when_it_does_not_reconcile(spark, business_date):
    pos = daily_pos_sales.read_delimited(
        spark, config.pos_input_file(business_date), daily_pos_sales.schemas.pos_sales, config.POS_DELIMITER
    )
    unreconciled = pos.filter("transaction_id = 'T0001' and upc = 4011").withColumn(
        "ext_price", daily_pos_sales.F.lit(Decimal("99.99")).cast("decimal(18,2)")
    )
    (row,) = daily_pos_sales.cleanse(unreconciled).collect()
    assert row.ext_price == Decimal("1.74")


def test_output_files_are_written(spark, business_date):
    _run(spark, business_date)
    summary_path = config.summary_output_file(business_date)
    reject_path = config.reject_output_file(business_date)

    summary_lines = summary_path.read_text().splitlines()
    assert len(summary_lines) == 9
    assert summary_lines[0] == (
        "2026-07-13|1001|ALBERTSONS|INTERMOUNTAIN|DAIRY|EGGS|1|1|2.49|0.30|2.19|2.19|1"
    )

    reject_lines = reject_path.read_text().splitlines()
    assert reject_lines == [
        "3300,7,T0901,2026-07-13,11:22:41,9999999999,UNKNOWN ITEM,1,1.00,1.00,0.00,,CASH,N"
    ]
