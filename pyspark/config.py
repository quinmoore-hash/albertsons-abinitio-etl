"""Central configuration for the PySpark port.

PySpark equivalent of ``sand/project.pset`` + ``sand/sandbox.pset`` (paths and
DQ thresholds) and ``dbc/edw.dbc`` + ``dbc/edw.dbc.env`` (Oracle connection).

Every value can be overridden through the environment so the same code runs in
the sandbox, in CI and on the cluster.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parent.parent))

# ---------------------------------------------------------------------------
# Paths (sand/project.pset + sand/sandbox.pset)
# ---------------------------------------------------------------------------
AB_DATA_DIR = Path(os.environ.get("AB_DATA_DIR", PROJECT_DIR / "data"))
AI_IN = Path(os.environ.get("AI_IN", AB_DATA_DIR / "in"))
AI_SERIAL = Path(os.environ.get("AI_SERIAL", AB_DATA_DIR / "serial"))
AI_OUT = Path(os.environ.get("AI_OUT", AB_DATA_DIR / "out"))

# Delimiters. The DML header comments claim the POS feed is pipe delimited but
# both the specs (``decimal(",")``) and the sample data are comma delimited;
# the inventory feed and every target file are pipe delimited.
POS_DELIMITER = ","
DIM_DELIMITER = ","
INVENTORY_DELIMITER = "|"
OUTPUT_DELIMITER = "|"

# ---------------------------------------------------------------------------
# Data-quality thresholds (sand/sandbox.pset)
# ---------------------------------------------------------------------------
DQ_MIN_ROWCOUNT = int(os.environ.get("DQ_MIN_ROWCOUNT", "1000"))
DQ_MAX_REJECT_PCT = float(os.environ.get("DQ_MAX_REJECT_PCT", "2.0"))
ROWCOUNT_DEVIATION_PCT = float(os.environ.get("ROWCOUNT_DEVIATION_PCT", "25.0"))

# ---------------------------------------------------------------------------
# Target tables
# ---------------------------------------------------------------------------
DAILY_SALES_SUMMARY_TABLE = os.environ.get(
    "DAILY_SALES_SUMMARY_TABLE", "EDW.F_DAILY_SALES_SUMMARY"
)
INVENTORY_VALUE_TABLE = os.environ.get("INVENTORY_VALUE_TABLE", "EDW.F_INVENTORY_VALUE")


def pos_input_file(business_date: str) -> Path:
    return AI_IN / f"pos_sales_{business_date}.dat"


def inventory_input_file(business_date: str) -> Path:
    return AI_IN / f"inventory_{business_date}.dat"


def product_dim_file() -> Path:
    return AI_SERIAL / "product_dim.dat"


def store_dim_file() -> Path:
    return AI_SERIAL / "store_dim.dat"


def summary_output_file(business_date: str) -> Path:
    return AI_OUT / f"daily_sales_summary_{business_date}.dat"


def reject_output_file(business_date: str) -> Path:
    return AI_OUT / f"reject_{business_date}.dat"


def inventory_output_file(business_date: str) -> Path:
    return AI_OUT / f"inventory_value_{business_date}.dat"


@dataclass(frozen=True)
class JdbcConfig:
    """Oracle EDW connection profile (``dbc/edw.dbc``)."""

    host: str = os.environ.get("EDW_DB_HOST", "edw-scan.albertsons.internal")
    port: int = int(os.environ.get("EDW_DB_PORT", "1521"))
    service: str = os.environ.get("EDW_DB_SERVICE", "EDWPRD.albertsons.internal")
    schema: str = os.environ.get("EDW_DB_SCHEMA", "EDW")
    driver: str = os.environ.get("EDW_JDBC_DRIVER", "oracle.jdbc.OracleDriver")
    # db_array_size / db_commit_interval from edw.dbc
    fetch_size: int = int(os.environ.get("EDW_DB_ARRAY_SIZE", "5000"))
    batch_size: int = int(os.environ.get("EDW_DB_COMMIT_INTERVAL", "50000"))
    user: str = field(default_factory=lambda: os.environ.get("EDW_DB_USER", ""))
    password: str = field(default_factory=lambda: os.environ.get("EDW_DB_PASSWORD", ""))

    @property
    def url(self) -> str:
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service}"

    def properties(self) -> dict[str, str]:
        if not self.user or not self.password:
            raise RuntimeError(
                "EDW_DB_USER / EDW_DB_PASSWORD must be set (injected from the vault "
                "by the scheduler, see dbc/edw.dbc.env)"
            )
        return {
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
            "fetchsize": str(self.fetch_size),
            "batchsize": str(self.batch_size),
        }


def jdbc_config() -> JdbcConfig:
    return JdbcConfig()


# Set to "1" for local/demo runs to skip the JDBC load steps.
def skip_db_load() -> bool:
    return os.environ.get("SKIP_DB_LOAD", "0") == "1"
