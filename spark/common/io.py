"""Delimited-file INPUT FILE / OUTPUT FILE equivalents driven by a :class:`RecordFormat`."""

from __future__ import annotations

from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark.schemas import VARIABLE_SCALE_TYPES, RecordFormat


class RecordFormatError(ValueError):
    """Raised when input data violates the non-null contract of its DML."""


def read_delimited(
    spark: SparkSession, path: str, record_format: RecordFormat, *, strict: bool = True
) -> DataFrame:
    """Read a delimited feed with an explicit schema (Ab Initio INPUT FILE + DML).

    Spark reads empty delimited values as null; non-nullable string fields are
    coerced back to ``""`` (Ab Initio semantics) and, when ``strict``, a null in
    any other non-nullable field aborts the read the way a DML parse error would.
    """
    df = (
        spark.read.options(**record_format.csv_options())
        .option("mode", "FAILFAST")
        .schema(record_format.schema)
        .csv(path)
    )
    for field in record_format.schema.fields:
        if not field.nullable and isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit("")))

    if strict:
        required = [
            f.name
            for f in record_format.schema.fields
            if not f.nullable and not isinstance(f.dataType, StringType)
        ]
        if required:
            any_null = reduce(or_, [F.col(c).isNull() for c in required])
            violations = df.filter(any_null).limit(1).collect()
            if violations:
                bad = [c for c in required if violations[0][c] is None]
                raise RecordFormatError(
                    f"{path}: null in non-nullable {record_format.name} field(s) {bad}"
                )
    return df


def conform(df: DataFrame, record_format: RecordFormat) -> DataFrame:
    """Project and cast ``df`` to exactly the columns/types of ``record_format``."""
    return df.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in record_format.schema.fields]
    )


def write_delimited(df: DataFrame, path: str, record_format: RecordFormat) -> None:
    """Write ``df`` as a single delimited part file (Ab Initio serial OUTPUT FILE)."""
    out = conform(df, record_format)
    trimmed = {
        f.name: F.regexp_replace(F.col(f.name).cast("string"), r"\.?0+$", "")
        for f in record_format.schema.fields
        if f.dataType in VARIABLE_SCALE_TYPES
    }
    (
        out.withColumns(trimmed)
        .coalesce(1)
        .write.mode("overwrite")
        .options(**record_format.csv_options())
        .option("emptyValue", "")
        .csv(path)
    )
