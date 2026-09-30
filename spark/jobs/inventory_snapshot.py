"""``inventory_snapshot`` job.

Port of ``mp/inventory_snapshot.mp`` / ``run/inventory_snapshot.ksh``.

python -m spark.jobs.inventory_snapshot 20260713 [--skip-load]
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from spark.common.config import BatchConfig, build_parser
from spark.common.io import read_delimited, write_delimited
from spark.common.jdbc import edw_from_project
from spark.common.session import create_spark_session
from spark.schemas import INVENTORY, INVENTORY_VALUE
from spark.transforms.inventory_rollup import ROLLUP_KEY, filter_active, rollup_inventory

log = logging.getLogger("inventory_snapshot")

TARGET_TABLE = "EDW.F_INVENTORY_VALUE"

Loader = Callable[[DataFrame, BatchConfig], None]


def build(spark: SparkSession, cfg: BatchConfig) -> DataFrame:
    inventory = read_delimited(spark, cfg.inventory_input_path, INVENTORY)
    return rollup_inventory(filter_active(inventory)).orderBy(*ROLLUP_KEY)


def load_to_edw(inventory_value: DataFrame, cfg: BatchConfig) -> None:
    """``m_db load ... -table EDW.F_INVENTORY_VALUE -mode truncate``."""
    edw_from_project(cfg.project_dir).write(inventory_value, TARGET_TABLE, mode="truncate")


def run(spark: SparkSession, cfg: BatchConfig, loader: Loader | None = load_to_edw) -> DataFrame:
    log.info("START inventory_snapshot BUSINESS_DATE=%s", cfg.business_date)
    inventory_value = build(spark, cfg).cache()
    write_delimited(inventory_value, cfg.inventory_out_path, INVENTORY_VALUE)
    if loader is not None:
        loader(inventory_value, cfg)
    log.info("END   inventory_snapshot BUSINESS_DATE=%s", cfg.business_date)
    return inventory_value


def main(argv: list[str] | None = None) -> int:
    parser = build_parser("inventory_snapshot", __doc__)
    parser.add_argument("--skip-load", action="store_true", help="write files only; no EDW load")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s %(message)s")
    cfg = BatchConfig.from_args(args)
    spark = create_spark_session("inventory_snapshot", cfg)
    try:
        run(spark, cfg, loader=None if args.skip_load else load_to_edw)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
