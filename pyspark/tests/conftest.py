from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

PYSPARK_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = PYSPARK_DIR.parent
# The migration package lives in a directory called "pyspark/"; add that
# directory to the path directly (never the repo root, which would shadow the
# pyspark library).
sys.path.insert(0, str(PYSPARK_DIR))

BUSINESS_DATE = "20260713"

# Must happen before `config` is imported: it snapshots the environment.
os.environ["PROJECT_DIR"] = str(PROJECT_DIR)
os.environ.setdefault("AI_OUT", tempfile.mkdtemp(prefix="abinitio-migration-out-"))
os.environ["SKIP_DB_LOAD"] = "1"


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("abinitio-migration-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def business_date() -> str:
    return BUSINESS_DATE
