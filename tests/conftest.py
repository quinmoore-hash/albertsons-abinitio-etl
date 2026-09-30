from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_DIR = Path(__file__).resolve().parent / "expected"
BUSINESS_DATE = "20260713"

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    builder = pyspark_sql.SparkSession.builder
    if os.environ.get("EDW_IT_JDBC_JAR"):
        builder = builder.config("spark.jars", os.environ["EDW_IT_JDBC_JAR"])
    session = (
        builder.master("local[1]")
        .appName("albertsons-etl-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def batch_cfg(tmp_path):
    from spark.common.config import BatchConfig

    return BatchConfig.load(
        business_date=BUSINESS_DATE,
        project_dir=REPO_ROOT,
        overrides={"AI_OUT": str(tmp_path / "out")},
        environ={},
    )
