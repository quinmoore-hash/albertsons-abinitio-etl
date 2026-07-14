"""PySpark port of ``mp/daily_pos_sales.mp`` and its XFRs.

Pipeline (mirrors the Ab Initio graph):

    INPUT (pos)  -> REFORMAT (pos_sales_cleanse.xfr) -> SORT (upc)
                                                          |
    INPUT (product_dim) --> [lookup in1] --------------->|
    INPUT (store_dim)   --> [lookup in2] --------------> JOIN (dim_join.xfr)
                                                            |
                          unused0 --> OUTPUT FILE (reject_<date>.dat)
                                                            |
                                                    SORT (rollup key)
                                                            |
                                            ROLLUP (sales_rollup.xfr)
                                                            |
                              OUTPUT FILE (daily_sales_summary_<date>.dat)
                                          + m_db load EDW.F_DAILY_SALES_SUMMARY (append)

The transform functions are pure (DataFrame in / DataFrame out) so they can be
unit-tested against hand-computed expected values from the sample feeds.
"""

from __future__ import annotations

import argparse
import os
import sys

# Make ``pyspark/`` (this file's grandparent) importable as the source root so
# ``common`` resolves without shadowing the real ``pyspark`` library.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from common.io import load_to_edw, read_delimited, write_delimited
from common.schemas import (
    DAILY_SALES_SUMMARY_COLUMNS,
    POS_SALES_COLUMNS,
    POS_SALES_SCHEMA,
    PRODUCT_DIM_SCHEMA,
    STORE_DIM_SCHEMA,
)
from common.spark_session import build_spark

MONEY = DecimalType(18, 2)

EDW_TABLE = "EDW.F_DAILY_SALES_SUMMARY"


# ---------------------------------------------------------------------------
# REFORMAT cleanse  (port of xfr/pos_sales_cleanse.xfr)
# ---------------------------------------------------------------------------
def cleanse(pos: DataFrame) -> DataFrame:
    """Port of ``pos_sales_cleanse.xfr`` (REFORMAT + ``is_valid_line`` select).

    * select: keep only ``void_flag != 'Y' AND qty != 0``
    * trim ``transaction_id``, ``loyalty_id``, ``product_desc``
    * ``unit_price`` null -> 0, ``discount_amt`` null -> 0
    * ``ext_price``: trust the feed value only when it reconciles with
      ``round(qty * unit_price, 2)``, otherwise recompute
    * ``tender_type``: upper(trim); map ``'CC'`` -> ``'CREDIT'``

    All original ``pos_sales`` columns are preserved (in DML order) so unmatched
    rows can be written to the reject file in the ``pos_sales`` format.
    """
    unit_price = F.coalesce(F.col("unit_price"), F.lit(0).cast(MONEY))
    discount_amt = F.coalesce(F.col("discount_amt"), F.lit(0).cast(MONEY))
    recomputed = F.round(F.col("qty") * unit_price, 2).cast(MONEY)
    tender_norm = F.upper(F.trim(F.col("tender_type")))

    cleansed = (
        pos
        .filter((F.col("void_flag") != F.lit("Y")) & (F.col("qty") != F.lit(0)))
        .withColumn("transaction_id", F.trim(F.col("transaction_id")))
        .withColumn("unit_price", unit_price)
        .withColumn(
            "ext_price",
            F.when(recomputed == F.col("ext_price"), F.col("ext_price"))
            .otherwise(recomputed),
        )
        .withColumn("discount_amt", discount_amt)
        .withColumn(
            "tender_type",
            F.when(tender_norm == F.lit("CC"), F.lit("CREDIT")).otherwise(tender_norm),
        )
        .withColumn("loyalty_id", F.trim(F.col("loyalty_id")))
        .withColumn("product_desc", F.trim(F.col("product_desc")))
    )
    # Preserve pos_sales DML column order.
    return cleansed.select(*POS_SALES_COLUMNS)


# ---------------------------------------------------------------------------
# JOIN dim_join  (port of xfr/dim_join.xfr + unused-port reject handling)
# ---------------------------------------------------------------------------
def join_dims(
    cleansed: DataFrame,
    product_dim: DataFrame,
    store_dim: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Left-join cleansed sales to product_dim (upc) then store_dim (store_id).

    Returns ``(matched, rejects)`` where ``rejects`` are the cleansed rows whose
    product OR store dimension lookup missed (the JOIN ``unused-port``), carried
    in the ``pos_sales`` format. Matched rows gain the product and store
    attributes required by the rollup.
    """
    prod = product_dim.select(
        F.col("upc").alias("_p_upc"),
        F.col("department"),
        F.col("category"),
        F.col("brand"),
        F.col("private_label_flag"),
    )
    store = store_dim.select(
        F.col("store_id").alias("_s_store_id"),
        F.col("banner"),
        F.col("region"),
    )

    joined = (
        cleansed
        .join(prod, cleansed["upc"] == prod["_p_upc"], "left")
        .join(store, cleansed["store_id"] == store["_s_store_id"], "left")
    )

    # A reject is any row where either dimension side failed to match.
    reject_cond = F.col("_p_upc").isNull() | F.col("_s_store_id").isNull()

    rejects = joined.filter(reject_cond).select(*POS_SALES_COLUMNS)

    matched = joined.filter(~reject_cond).select(
        cleansed["business_date"],
        cleansed["store_id"],
        cleansed["transaction_id"],
        cleansed["qty"],
        cleansed["ext_price"],
        cleansed["discount_amt"],
        cleansed["loyalty_id"],
        cleansed["upc"],
        F.col("department"),
        F.col("category"),
        F.col("brand"),
        F.col("private_label_flag"),
        F.col("banner"),
        F.col("region"),
    )
    return matched, rejects


# ---------------------------------------------------------------------------
# ROLLUP sales_rollup  (port of xfr/sales_rollup.xfr)
# ---------------------------------------------------------------------------
def rollup_sales(matched: DataFrame) -> DataFrame:
    """Aggregate joined sale lines to the daily_sales_summary grain.

    Grain: ``business_date, store_id, department, category`` (``banner`` and
    ``region`` are carried through -- functionally dependent on ``store_id``).

    Note: ``txn_count`` deliberately counts *lines* (``count(*)``), matching the
    legacy XFR comment that the register feed emits one line per scanned item.
    """
    pl_sales = F.when(
        F.col("private_label_flag") == F.lit("Y"),
        F.col("ext_price") - F.col("discount_amt"),
    ).otherwise(F.lit(0).cast(MONEY))

    loyalty_flag = F.when(
        F.col("loyalty_id").isNull() | (F.trim(F.col("loyalty_id")) == F.lit("")),
        F.lit(0),
    ).otherwise(F.lit(1))

    grouped = (
        matched
        .groupBy(
            "business_date", "store_id", "department", "category", "banner", "region",
        )
        .agg(
            F.count(F.lit(1)).cast(DecimalType(18, 0)).alias("txn_count"),
            F.sum("qty").cast(DecimalType(18, 0)).alias("unit_qty"),
            F.round(F.sum("ext_price"), 2).cast(MONEY).alias("gross_sales"),
            F.round(F.sum("discount_amt"), 2).cast(MONEY).alias("discount_total"),
            F.round(F.sum(pl_sales), 2).cast(MONEY).alias("private_label_sales"),
            F.sum(loyalty_flag).cast(DecimalType(18, 0)).alias("loyalty_txn_count"),
        )
        .withColumn(
            "net_sales",
            F.round(F.col("gross_sales") - F.col("discount_total"), 2).cast(MONEY),
        )
    )
    return grouped.select(*DAILY_SALES_SUMMARY_COLUMNS)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run(
    spark: SparkSession,
    business_date: str,
    *,
    data_dir: str,
    load_db: bool = True,
) -> tuple[str, str]:
    """Execute the full daily POS sales pipeline for ``business_date``.

    Returns the ``(summary_path, reject_path)`` written.
    """
    serial_dir = os.path.join(data_dir, "serial")
    in_dir = os.path.join(data_dir, "in")
    out_dir = os.path.join(data_dir, "out")
    os.makedirs(out_dir, exist_ok=True)

    pos_path = os.path.join(in_dir, f"pos_sales_{business_date}.dat")
    product_path = os.path.join(serial_dir, "product_dim.dat")
    store_path = os.path.join(serial_dir, "store_dim.dat")
    summary_path = os.path.join(out_dir, f"daily_sales_summary_{business_date}.dat")
    reject_path = os.path.join(out_dir, f"reject_{business_date}.dat")

    pos = read_delimited(spark, pos_path, POS_SALES_SCHEMA, ",")
    product_dim = read_delimited(spark, product_path, PRODUCT_DIM_SCHEMA, ",")
    store_dim = read_delimited(spark, store_path, STORE_DIM_SCHEMA, ",")

    cleansed = cleanse(pos)
    matched, rejects = join_dims(cleansed, product_dim, store_dim)
    summary = rollup_sales(matched)

    # Deterministic ordering for byte-identical output vs. the sorted Ab Initio
    # graph (SORT before ROLLUP / the unused port emits in upc order).
    summary_ordered = summary.orderBy(
        "business_date", "store_id", "department", "category",
    )
    rejects_ordered = rejects.orderBy("upc", "store_id", "transaction_id")

    write_delimited(summary_ordered, summary_path, "|", DAILY_SALES_SUMMARY_COLUMNS)
    write_delimited(rejects_ordered, reject_path, ",", POS_SALES_COLUMNS)

    if load_db:
        loaded = load_to_edw(summary, EDW_TABLE, "append", DAILY_SALES_SUMMARY_COLUMNS)
        if not loaded:
            print(
                f"[daily_pos_sales] EDW load skipped (no credentials); "
                f"summary written to {summary_path}"
            )

    return summary_path, reject_path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily POS sales aggregation (PySpark).")
    parser.add_argument("--business-date", required=True, help="BUSINESS_DATE (YYYYMMDD)")
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("AB_DATA_DIR", "data"),
        help="Root data directory containing in/, serial/, out/ (default: ./data).",
    )
    parser.add_argument(
        "--no-load-db",
        action="store_true",
        help="Skip the EDW JDBC load even if credentials are present.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    spark = build_spark("daily_pos_sales")
    try:
        summary_path, reject_path = run(
            spark,
            args.business_date,
            data_dir=args.data_dir,
            load_db=not args.no_load_db,
        )
        print(f"[daily_pos_sales] summary -> {summary_path}")
        print(f"[daily_pos_sales] reject  -> {reject_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
