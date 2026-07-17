"""SparkSession builder shared by the migration jobs and the test suite.

Keeps configuration in one place so the jobs, the DQ gate and pytest all use a
consistent session (legacy date parsing disabled, UTC time zone, decimals
preserved).

Note on parallelism: production jobs do NOT pin ``spark.sql.shuffle.partitions``
so they inherit the cluster / ``spark-submit --conf`` setting and can scale to
real nightly volumes. Byte-identical output is guaranteed by the explicit
``orderBy`` in each job plus the ``coalesce(1)`` in ``write_delimited`` -- not by
running single-threaded. The test suite passes ``shuffle_partitions="1"`` for
deterministic, fast local runs.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(
    app_name: str = "albertsons-etl",
    *,
    master: str | None = None,
    shuffle_partitions: str | None = None,
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
        Optional ``spark.sql.shuffle.partitions`` override. When ``None`` (the
        production default) the value is left unset so Spark follows the
        cluster / ``spark-submit`` configuration. Tests pass ``"1"``.
    extra_conf:
        Additional Spark configuration entries.
    """
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", shuffle_partitions)

    builder = (
        builder
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
