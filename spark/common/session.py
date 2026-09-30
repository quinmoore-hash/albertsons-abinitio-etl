"""SparkSession factory shared by the batch jobs."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from spark.common.config import BatchConfig


def create_spark_session(app_name: str, cfg: BatchConfig | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(f"albertsons-{app_name}")
    packages = os.environ.get("EDW_JDBC_PACKAGES")
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    for key, value in (cfg.to_spark_conf() if cfg else {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
