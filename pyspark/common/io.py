"""Read / write helpers for the delimited flat feeds.

These mirror the Ab Initio ``INPUT FILE`` / ``OUTPUT FILE`` components:

* No header row.
* No field quoting (Co>Operating System serial files are raw delimited).
* Empty field  <-> NULL.
* Dates formatted / parsed as ``yyyy-MM-dd``.
* The record delimiter is a newline; the final field's delimiter in the DML is
  ``"\\n"`` which simply means fields are joined by the field delimiter and each
  record ends with a newline -- exactly what a single-character CSV writer does.

``write_delimited`` produces a single, deterministically named output file
(e.g. ``daily_sales_summary_20260713.dat``) rather than a Spark part-file
directory, so the result can be diffed byte-for-byte against the legacy output.
"""

from __future__ import annotations

import glob
import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

# Sentinel char used to effectively disable CSV quoting/escaping so output
# matches Ab Initio's un-quoted serial format.
_NO_QUOTE = "\u0000"


def read_delimited(
    spark: SparkSession,
    path: str,
    schema: StructType,
    delimiter: str,
) -> DataFrame:
    """Read a raw delimited feed using an explicit schema (no header)."""
    return (
        spark.read
        .option("sep", delimiter)
        .option("header", "false")
        .option("quote", _NO_QUOTE)
        .option("escape", _NO_QUOTE)
        .option("nullValue", "")
        .option("dateFormat", "yyyy-MM-dd")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(path)
    )


def write_delimited(
    df: DataFrame,
    path: str,
    delimiter: str,
    columns: list[str],
) -> None:
    """Write ``df`` to a single delimited file at ``path`` (DML column order).

    The DataFrame is coalesced to one partition (the nightly volumes are small
    and this guarantees a stable single-file output). The caller is responsible
    for ordering rows deterministically before calling this helper.
    """
    ordered = df.select(*columns).coalesce(1)

    tmp_dir = path + ".__spark_tmp__"
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)

    (
        ordered.write
        .option("sep", delimiter)
        .option("header", "false")
        .option("quote", _NO_QUOTE)
        .option("escape", _NO_QUOTE)
        .option("emptyValue", "")
        .option("nullValue", "")
        .option("dateFormat", "yyyy-MM-dd")
        .mode("overwrite")
        .csv(tmp_dir)
    )

    part_files = sorted(glob.glob(os.path.join(tmp_dir, "part-*")))
    if not part_files:
        # No rows written: Spark may still emit an empty part file, but guard
        # against a completely empty directory by creating an empty target.
        open(path, "w").close()
    else:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # Concatenate the (single, since coalesced) part file(s) to the target.
        with open(path, "wb") as out:
            for pf in part_files:
                with open(pf, "rb") as src:
                    shutil.copyfileobj(src, out)

    shutil.rmtree(tmp_dir)


def load_to_edw(
    df: DataFrame,
    table: str,
    mode: str,
    columns: list[str],
) -> bool:
    """Optionally load ``df`` into the EDW via JDBC.

    Mirrors ``m_db load ... -mode {append|truncate}`` from the ``.ksh`` scripts.
    Connection details come from environment variables analogous to
    ``dbc/edw.dbc`` (the target DDL and real credentials are intentionally not
    in the repo):

    * ``EDW_DB_HOST``     (default ``edw-scan.albertsons.internal``)
    * ``EDW_DB_PORT``     (default ``1521``)
    * ``EDW_DB_SERVICE``  (default ``EDWPRD.albertsons.internal``)
    * ``EDW_DB_SCHEMA``   (default ``EDW``)
    * ``EDW_DB_USER``
    * ``EDW_DB_PASSWORD``

    The load is a no-op (returns ``False``) unless ``EDW_DB_USER`` and
    ``EDW_DB_PASSWORD`` are both set, so local/demo runs succeed without a
    warehouse. ``mode`` maps to Spark write modes:

    * ``append``   -> ``"append"``
    * ``truncate`` -> ``"overwrite"`` with ``truncate=true`` (preserves the
      table DDL, matching ``m_db load -mode truncate``).
    """
    user = os.environ.get("EDW_DB_USER")
    password = os.environ.get("EDW_DB_PASSWORD")
    if not user or not password or password == "__SET_IN_VAULT__":
        return False

    host = os.environ.get("EDW_DB_HOST", "edw-scan.albertsons.internal")
    port = os.environ.get("EDW_DB_PORT", "1521")
    service = os.environ.get("EDW_DB_SERVICE", "EDWPRD.albertsons.internal")
    schema = os.environ.get("EDW_DB_SCHEMA", "EDW")

    # Oracle service-name JDBC URL.
    url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"

    # ``table`` may already be schema-qualified (e.g. EDW.F_...); if not, qualify
    # it with the default schema.
    dbtable = table if "." in table else f"{schema}.{table}"

    writer = (
        df.select(*columns).write.format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", user)
        .option("password", password)
        .option("driver", "oracle.jdbc.OracleDriver")
    )

    if mode == "append":
        writer = writer.mode("append")
    elif mode == "truncate":
        writer = writer.option("truncate", "true").mode("overwrite")
    else:  # pragma: no cover - defensive
        raise ValueError(f"unsupported load mode: {mode!r}")

    writer.save()
    return True
