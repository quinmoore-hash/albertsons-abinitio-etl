"""Shared I/O helpers: serial file reads/writes and the EDW JDBC load.

Ab Initio equivalents:

* ``INPUT FILE`` + DML  -> :func:`read_delimited`
* ``OUTPUT FILE`` (serial, single file) -> :func:`write_serial_file`
* ``m_db load -mode ...`` -> :func:`load_to_edw`
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StructType

import config
from schemas import DATE_FORMAT


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_delimited(spark: SparkSession, path: str | Path, schema: StructType, delimiter: str) -> DataFrame:
    """Read a headerless delimited serial file with an explicit DML schema."""
    return (
        spark.read.option("sep", delimiter)
        .option("header", "false")
        .option("dateFormat", DATE_FORMAT)
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(str(path))
    )


def _output_column(name: str, dtype) -> Column:
    col = F.col(name)
    if isinstance(dtype, DateType):
        return F.date_format(col, DATE_FORMAT).alias(name)
    if isinstance(dtype, DecimalType) and dtype.scale >= 3:
        # Ab Initio decimal("") is variable length: drop insignificant zeros
        # (e.g. a unit_qty of 6.000 is written as "6").
        as_string = col.cast("string")
        trimmed = F.regexp_replace(as_string, r"(\.\d*?)0+$", r"$1")
        return F.regexp_replace(trimmed, r"\.$", "").alias(name)
    return col.cast("string").alias(name)


def write_serial_file(df: DataFrame, path: str | Path, delimiter: str) -> None:
    """Write ``df`` as a single headerless delimited file at ``path``.

    Mirrors an Ab Initio serial ``OUTPUT FILE``: one flat file, no header, no
    part directory.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    formatted = df.select(*[_output_column(f.name, f.dataType) for f in df.schema.fields])

    staging = tempfile.mkdtemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        (
            formatted.coalesce(1)
            .write.mode("overwrite")
            .option("sep", delimiter)
            .option("header", "false")
            .option("emptyValue", "")
            .option("nullValue", "")
            .csv(staging)
        )
        parts = sorted(glob.glob(os.path.join(staging, "part-*.csv")))
        with open(path, "wb") as out:
            for part in parts:
                with open(part, "rb") as src:
                    shutil.copyfileobj(src, out)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def load_to_edw(df: DataFrame, table: str, mode: str) -> None:
    """Load ``df`` into an Oracle EDW table.

    ``mode="append"`` mirrors ``m_db load -mode append`` and
    ``mode="overwrite"`` mirrors ``m_db load -mode truncate`` (the JDBC
    ``truncate`` option keeps the existing table definition).
    """
    if mode not in ("append", "overwrite"):
        raise ValueError(f"unsupported load mode: {mode}")

    if config.skip_db_load():
        print(f"[load] SKIP_DB_LOAD=1, skipping {mode} load into {table}")
        return

    jdbc = config.jdbc_config()
    writer = df.write.mode(mode).format("jdbc").option("url", jdbc.url).option("dbtable", table)
    for key, value in jdbc.properties().items():
        writer = writer.option(key, value)
    if mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.save()
    print(f"[load] {mode} load into {table} complete")
