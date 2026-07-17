"""Pytest configuration + shared fixtures for the PySpark migration tests.

Placed at the ``pyspark/`` source root (which pytest adds to ``sys.path``) so
tests can ``import`` the ``common``, ``jobs`` and ``dq`` packages without the
top-level directory shadowing the real ``pyspark`` library.
"""

from __future__ import annotations

import os
import shutil
import sys

import pytest

# Ensure the pyspark/ source root is importable (jobs/, common/, dq/).
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Repository root and its shipped sample data (business date 20260713).
_REPO_ROOT = os.path.dirname(_ROOT)
_REPO_DATA = os.path.join(_REPO_ROOT, "data")

BUSINESS_DATE = "20260713"

from common.spark_session import build_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for the whole test run."""
    # Pin a single shuffle partition for deterministic, fast local test runs.
    session = build_spark(
        "albertsons-etl-tests", master="local[1]", shuffle_partitions="1"
    )
    yield session
    session.stop()


@pytest.fixture()
def data_dir(tmp_path):
    """A temporary data dir seeded with the repo's sample in/ and serial/ feeds.

    Outputs land under this tmp dir's ``out/`` so tests never touch the repo's
    working tree.
    """
    root = tmp_path / "data"
    (root / "out").mkdir(parents=True)
    for sub in ("in", "serial"):
        shutil.copytree(os.path.join(_REPO_DATA, sub), root / sub)
    return str(root)
