"""Oracle EDW JDBC read/write helper replacing ``dbc/edw.dbc`` + ``m_db``.

The ``.dbc`` profile is parsed as-is; ``${VAR}`` references are resolved from
the environment after sourcing ``dbc/edw.dbc.env`` (which only supplies
defaults, so vault/scheduler-injected ``EDW_DB_USER`` / ``EDW_DB_PASSWORD``
take precedence).

``m_db load -mode append``   -> :meth:`EdwJdbc.write` with ``mode="append"``
``m_db load -mode truncate`` -> :meth:`EdwJdbc.write` with ``mode="truncate"``
(``overwrite`` + ``truncate=true`` so the EDW table DDL/grants are preserved).

The Oracle driver jar must be on the Spark classpath, e.g.
``spark-submit --packages com.oracle.database.jdbc:ojdbc11:<version>`` or
``EDW_JDBC_PACKAGES`` (see :mod:`spark.common.session`).
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from spark.common import pset

ORACLE_DRIVER = "oracle.jdbc.OracleDriver"
VAULT_PLACEHOLDER = "__SET_IN_VAULT__"
_DBC_LINE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.*?)\s*$")


class EdwConfigError(RuntimeError):
    pass


def parse_dbc(path: str | Path, env: Mapping[str, str]) -> dict[str, str]:
    """Parse an Ab Initio ``.dbc`` file (``key: value``, ``;`` comments)."""
    values: dict[str, str] = {}
    for raw in Path(path).read_text().splitlines():
        line = raw.split(";", 1)[0]
        match = _DBC_LINE.match(line)
        if match:
            key, value = match.groups()
            values[key] = pset.expand(value, dict(env))
    return values


@dataclass(frozen=True)
class EdwJdbc:
    url: str
    user: str
    password: str = ""
    default_schema: str = "EDW"
    batch_size: int = 5000
    fetch_size: int = 5000
    driver: str = ORACLE_DRIVER

    @classmethod
    def from_dbc(
        cls,
        dbc_path: str | Path,
        env_path: str | Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> EdwJdbc:
        env: MutableMapping[str, str] = dict(os.environ if environ is None else environ)
        if env_path is not None and Path(env_path).exists():
            pset.source(env_path, env)
        dbc = parse_dbc(dbc_path, env)
        if dbc.get("dbms", "").lower() != "oracle":
            raise EdwConfigError(
                f"{dbc_path}: only dbms=oracle is supported, got {dbc.get('dbms')!r}"
            )
        url = env.get("EDW_JDBC_URL") or (
            f"jdbc:oracle:thin:@//{dbc['db_nodes']}:{dbc.get('db_port', '1521')}/"
            f"{dbc.get('db_service') or dbc['db_name']}"
        )
        array_size = int(dbc.get("db_array_size", "5000"))
        return cls(
            url=url,
            user=dbc.get("db_user", ""),
            password=dbc.get("db_password", ""),
            default_schema=dbc.get("db_default_schema", "EDW"),
            batch_size=array_size,
            fetch_size=array_size,
        )

    def _require_credentials(self) -> None:
        if not self.user or not self.password or self.password == VAULT_PLACEHOLDER:
            raise EdwConfigError(
                "EDW credentials are not set: export EDW_DB_USER / EDW_DB_PASSWORD "
                "(injected from the vault in scheduled runs)"
            )

    def qualify(self, table: str) -> str:
        return table if "." in table else f"{self.default_schema}.{table}"

    def options(self) -> dict[str, str]:
        return {
            "url": self.url,
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }

    def read(
        self,
        spark: SparkSession,
        table: str | None = None,
        query: str | None = None,
        **extra: str,
    ) -> DataFrame:
        if (table is None) == (query is None):
            raise ValueError("pass exactly one of table= or query=")
        self._require_credentials()
        reader = (
            spark.read.format("jdbc")
            .options(**self.options())
            .option("fetchsize", str(self.fetch_size))
            .options(**extra)
        )
        if table is not None:
            reader = reader.option("dbtable", self.qualify(table))
        else:
            reader = reader.option("query", query)
        return reader.load()

    def write(self, df: DataFrame, table: str, mode: str = "append", **extra: str) -> None:
        """Load ``df`` into ``table``; ``mode`` is ``append`` or ``truncate``."""
        if mode not in ("append", "truncate"):
            raise ValueError(f"mode must be 'append' or 'truncate', got {mode!r}")
        self._require_credentials()
        # Oracle folds unquoted identifiers to upper case.
        df = df.toDF(*[c.upper() for c in df.columns])
        writer = (
            df.write.format("jdbc")
            .options(**self.options())
            .option("dbtable", self.qualify(table))
            .option("batchsize", str(self.batch_size))
            .options(**extra)
        )
        if mode == "truncate":
            writer = writer.mode("overwrite").option("truncate", "true")
        else:
            writer = writer.mode("append")
        writer.save()


def edw_from_dbc_dir(dbc_dir: str | Path) -> EdwJdbc:
    """``$AI_DBC/edw.dbc`` + ``$AI_DBC/edw.dbc.env``."""
    root = Path(dbc_dir)
    return EdwJdbc.from_dbc(root / "edw.dbc", root / "edw.dbc.env")
