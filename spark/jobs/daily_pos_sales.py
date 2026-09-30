"""``daily_pos_sales`` job (port of ``mp/daily_pos_sales.mp`` / ``run/daily_pos_sales.ksh``).

python -m spark.jobs.daily_pos_sales 20260713 [--skip-load]
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from spark.common.config import BatchConfig, build_parser
from spark.common.io import read_delimited, write_delimited
from spark.common.jdbc import edw_from_project
from spark.common.session import create_spark_session
from spark.schemas import DAILY_SALES_SUMMARY, POS_SALES, PRODUCT_DIM, STORE_DIM
from spark.transforms.dim_join import join_dimensions
from spark.transforms.pos_sales_cleanse import cleanse
from spark.transforms.sales_rollup import ROLLUP_KEY, rollup_sales

log = logging.getLogger("daily_pos_sales")

TARGET_TABLE = "EDW.F_DAILY_SALES_SUMMARY"

Loader = Callable[[DataFrame, BatchConfig], None]


@dataclass(frozen=True)
class DailyPosSalesResult:
    summary: DataFrame
    rejects: DataFrame
    summary_rows: int
    reject_rows: int


def build(spark: SparkSession, cfg: BatchConfig) -> tuple[DataFrame, DataFrame]:
    """Return ``(summary, rejects)`` DataFrames for ``cfg.business_date``."""
    pos = read_delimited(spark, cfg.pos_input_path, POS_SALES)
    product_dim = read_delimited(spark, cfg.product_dim_path, PRODUCT_DIM)
    store_dim = read_delimited(spark, cfg.store_dim_path, STORE_DIM)

    result = join_dimensions(cleanse(pos), product_dim, store_dim)
    summary = rollup_sales(result.joined).orderBy(*ROLLUP_KEY)
    return summary, result.rejects


def load_to_edw(summary: DataFrame, cfg: BatchConfig) -> None:
    """``m_db load ... -table EDW.F_DAILY_SALES_SUMMARY -mode append``."""
    edw_from_project(cfg.project_dir).write(summary, TARGET_TABLE, mode="append")


def run(
    spark: SparkSession, cfg: BatchConfig, loader: Loader | None = load_to_edw
) -> DailyPosSalesResult:
    log.info("START daily_pos_sales BUSINESS_DATE=%s", cfg.business_date)
    summary, rejects = build(spark, cfg)
    summary = summary.persist(StorageLevel.MEMORY_AND_DISK)
    rejects = rejects.persist(StorageLevel.MEMORY_AND_DISK)

    write_delimited(summary, cfg.summary_out_path, DAILY_SALES_SUMMARY)
    write_delimited(rejects, cfg.reject_out_path, POS_SALES)
    if loader is not None:
        loader(summary, cfg)

    result = DailyPosSalesResult(summary, rejects, summary.count(), rejects.count())
    log.info("rejected (unmatched UPC) rows: %d", result.reject_rows)
    log.info("END   daily_pos_sales BUSINESS_DATE=%s", cfg.business_date)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = build_parser("daily_pos_sales", __doc__)
    parser.add_argument("--skip-load", action="store_true", help="write files only; no EDW load")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s %(message)s")
    cfg = BatchConfig.from_args(args)
    spark = create_spark_session("daily_pos_sales", cfg)
    try:
        run(spark, cfg, loader=None if args.skip_load else load_to_edw)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
