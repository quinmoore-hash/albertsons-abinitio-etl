"""SparkSession builder shared by the migration jobs and the test suite.

Keeps configuration in one place so the jobs, the DQ gate and pytest all use a
consistent, deterministic session (single shuffle partition locally, legacy
date parsing disabled, decimals preserved).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# Default local shuffle partition count. The nightly volumes are modest and a
# single partition keeps output deterministic (stable ordering) for the
# byte-identical parity comparison against the Ab Initio graphs.
DEFAULT_SHUFFLE_PARTITIONS = "1"


def build_spark(
    app_name: str = "albertsons-etl",
    *,
    master: str | None = None,
    shuffle_partitions: str = DEFAULT_SHUFFLE_PARTITIONS,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Build (or fetch) a configured :class:`SparkSession`.

    Parameters
    ----------
    app_name:
        Spark application name.
    master:
        Optional master URL. When ``None`` (the default) Spark resolves the
        master from ``spark-submit`` / the environment, which is what the
        production submit command relies on. Tests pass ``local[1]``.
    shuffle_partitions:
        ``spark.sql.shuffle.partitions`` value.
    extra_conf:
        Additional Spark configuration entries.
    """
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    builder = (
        builder
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        # Match the exact yyyy-MM-dd parsing/formatting of the feeds.
        .config("spark.sql.session.timeZone", "UTC")
        # CORRECTED parser so out-of-range dates fail loudly rather than silently
        # rolling over (mirrors Ab Initio's strict date handling).
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
