"""PySpark port of ``mp/daily_pos_sales.mp``.

    INPUT FILE (pos) -> REFORMAT (pos_sales_cleanse.xfr) -> SORT
                     -> JOIN (dim_join.xfr, unused -> reject file)
                     -> ROLLUP (sales_rollup.xfr)
                     -> OUTPUT FILE + m_db load EDW.F_DAILY_SALES_SUMMARY (append)

Usage: ``spark-submit pyspark/daily_pos_sales.py <BUSINESS_DATE:YYYYMMDD>``
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import config
import schemas
from io_utils import get_spark, load_to_edw, read_delimited, write_serial_file

ROLLUP_KEY = ["business_date", "store_id", "department", "category"]


def cleanse(pos: DataFrame) -> DataFrame:
    """REFORMAT ``cleanse`` with ``select = is_valid_line``.

    Raw feed columns are carried through so unmatched rows can be written to
    the reject file in the ``dml/pos_sales.dml`` layout.
    """
    valid = pos.filter((F.col("void_flag") != F.lit("Y")) & (F.col("qty") != F.lit(0)))

    unit_price = F.coalesce(F.col("unit_price"), F.lit(0).cast(schemas.AMOUNT_TYPE))
    # Trust the register ext_price only when it reconciles, else recompute.
    recomputed = F.round(F.col("qty") * unit_price, 2)
    ext_price = F.when(
        F.round(F.col("qty") * F.col("unit_price"), 2) == F.col("ext_price"),
        F.col("ext_price"),
    ).otherwise(recomputed)

    tender = F.upper(F.trim(F.col("tender_type")))

    return valid.select(
        F.col("store_id"),
        F.col("register_id"),
        F.trim(F.col("transaction_id")).alias("transaction_id"),
        F.col("business_date"),
        F.col("transaction_ts"),
        unit_price.cast(schemas.AMOUNT_TYPE).alias("unit_price"),
        F.col("qty"),
        ext_price.cast(schemas.AMOUNT_TYPE).alias("ext_price"),
        F.coalesce(F.col("discount_amt"), F.lit(0).cast(schemas.AMOUNT_TYPE))
        .cast(schemas.AMOUNT_TYPE)
        .alias("discount_amt"),
        F.when(tender == F.lit("CC"), F.lit("CREDIT")).otherwise(tender).alias("tender_type"),
        F.trim(F.col("loyalty_id")).alias("loyalty_id"),
        F.col("upc"),
        F.trim(F.col("product_desc")).alias("product_desc"),
        F.col("void_flag"),
    )


def join_dimensions(
    cleansed: DataFrame, product_dim: DataFrame, store_dim: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """JOIN ``dim_join`` — returns ``(matched, rejected)``.

    The Ab Initio JOIN is an inner join with the unused port enabled: lines
    without a product or store dimension row fall out to the reject file.
    """
    joined = cleansed.join(
        product_dim.select(
            F.col("upc"),
            F.col("department"),
            F.col("category"),
            F.col("brand"),
            F.col("private_label_flag"),
            F.lit(True).alias("_product_matched"),
        ),
        on="upc",
        how="left",
    ).join(
        store_dim.select(
            F.col("store_id"),
            F.col("banner"),
            F.col("region"),
            F.lit(True).alias("_store_matched"),
        ),
        on="store_id",
        how="left",
    )

    # Key presence, not attribute nullability, decides the reject routing.
    unmatched = F.col("_product_matched").isNull() | F.col("_store_matched").isNull()

    rejected = joined.filter(unmatched).select(
        *[F.col(field.name) for field in schemas.pos_sales.fields]
    )

    matched = joined.filter(~unmatched).select(
        F.col("business_date"),
        F.col("store_id"),
        F.col("transaction_id"),
        F.col("qty"),
        F.col("ext_price"),
        F.col("discount_amt"),
        F.col("loyalty_id"),
        F.col("upc"),
        F.col("department"),
        F.col("category"),
        F.col("brand"),
        F.col("private_label_flag"),
        F.col("banner"),
        F.col("region"),
    )
    return matched, rejected


def rollup(matched: DataFrame) -> DataFrame:
    """ROLLUP ``sales_rollup`` to business_date x store x department x category."""
    private_label = F.when(
        F.col("private_label_flag") == F.lit("Y"),
        F.col("ext_price") - F.col("discount_amt"),
    ).otherwise(F.lit(0).cast(schemas.AMOUNT_TYPE))

    loyalty = F.when(
        F.col("loyalty_id").isNull() | (F.length(F.trim(F.col("loyalty_id"))) == 0), F.lit(0)
    ).otherwise(F.lit(1))

    gross_sales = F.round(F.sum("ext_price"), 2)
    discount_total = F.round(F.sum("discount_amt"), 2)

    aggregated = matched.groupBy(*ROLLUP_KEY).agg(
        # count_distinct(transaction_id) is approximated at line grain, as in
        # the legacy transform.
        F.count(F.lit(1)).alias("txn_count"),
        F.sum("qty").alias("unit_qty"),
        gross_sales.alias("gross_sales"),
        discount_total.alias("discount_total"),
        F.round(F.sum("ext_price") - F.sum("discount_amt"), 2).alias("net_sales"),
        F.round(F.sum(private_label), 2).alias("private_label_sales"),
        F.sum(loyalty).alias("loyalty_txn_count"),
        F.first("banner").alias("banner"),
        F.first("region").alias("region"),
    )

    return aggregated.select(
        *[F.col(field.name).cast(field.dataType) for field in schemas.daily_sales_summary.fields]
    ).orderBy(*ROLLUP_KEY)


def run(spark: SparkSession, business_date: str) -> tuple[DataFrame, DataFrame]:
    pos = read_delimited(
        spark, config.pos_input_file(business_date), schemas.pos_sales, config.POS_DELIMITER
    )
    product_dim = read_delimited(
        spark, config.product_dim_file(), schemas.product_dim, config.DIM_DELIMITER
    )
    store_dim = read_delimited(
        spark, config.store_dim_file(), schemas.store_dim, config.DIM_DELIMITER
    )

    matched, rejected = join_dimensions(cleanse(pos), product_dim, store_dim)
    summary = rollup(matched).cache()
    rejected = rejected.cache()

    write_serial_file(rejected, config.reject_output_file(business_date), config.POS_DELIMITER)
    write_serial_file(summary, config.summary_output_file(business_date), config.OUTPUT_DELIMITER)

    load_to_edw(summary, config.DAILY_SALES_SUMMARY_TABLE, mode="append")

    print(
        f"[daily_pos_sales] business_date={business_date} "
        f"summary_rows={summary.count()} reject_rows={rejected.count()}"
    )
    return summary, rejected


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: daily_pos_sales.py <BUSINESS_DATE:YYYYMMDD>", file=sys.stderr)
        return 1
    spark = get_spark("daily_pos_sales")
    try:
        run(spark, argv[1])
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
