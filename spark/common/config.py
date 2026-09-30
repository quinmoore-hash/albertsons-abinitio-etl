"""Batch configuration: ``sand/*.pset`` parameters mapped to Spark job args/config.

Resolution order mirrors the deployed ``run/*.ksh`` scripts:

1. process environment (so scheduler/vault-injected values win for ``${X:-default}``)
2. ``PROJECT_DIR`` (``--project-dir`` or repo root)
3. ``sand/project.pset`` then ``sand/sandbox.pset``, then any ``--pset`` files
   (sourced in order)
4. ``--param NAME=VALUE`` overrides (Ab Initio parameter overrides); these are
   pinned before sourcing, so parameters derived from them (e.g. ``AI_SERIAL``
   from ``PROJECT_DIR``) pick them up too

Paths follow the deployed scripts (``data/in`` feeds, ``$AI_SERIAL`` dims,
``data/out`` results) rather than the defaults embedded in the ``.mp`` files.
"""

from __future__ import annotations

import argparse
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from spark.common import pset

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PSETS = ("sand/project.pset", "sand/sandbox.pset")
SPARK_CONF_PREFIX = "spark.albertsons."


_YYYYMMDD = re.compile(r"\d{8}")


def parse_business_date(value: str) -> date:
    if not _YYYYMMDD.fullmatch(value):
        raise ValueError(f"BUSINESS_DATE must be YYYYMMDD, got {value!r}")
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"BUSINESS_DATE must be YYYYMMDD, got {value!r}") from exc


def default_business_date(today: date | None = None) -> str:
    """``nightly_batch.plan``: BUSINESS_DATE defaults to yesterday."""
    return ((today or date.today()) - timedelta(days=1)).strftime("%Y%m%d")


@dataclass(frozen=True)
class BatchConfig:
    business_date: str
    project_dir: Path
    params: Mapping[str, str] = field(repr=False)

    @property
    def business_date_value(self) -> date:
        return parse_business_date(self.business_date)

    @property
    def data_dir(self) -> Path:
        return Path(self.params.get("AB_DATA_DIR") or self.project_dir / "data")

    @property
    def in_dir(self) -> Path:
        return Path(self.params.get("AI_IN") or self.data_dir / "in")

    @property
    def serial_dir(self) -> Path:
        return Path(self.params.get("AI_SERIAL") or self.data_dir / "serial")

    @property
    def out_dir(self) -> Path:
        return Path(self.params.get("AI_OUT") or self.data_dir / "out")

    @property
    def dbc_dir(self) -> Path:
        return Path(self.params.get("AI_DBC") or self.project_dir / "dbc")

    # daily_pos_sales.ksh
    @property
    def pos_input_path(self) -> str:
        return str(self.in_dir / f"pos_sales_{self.business_date}.dat")

    @property
    def product_dim_path(self) -> str:
        return str(self.serial_dir / "product_dim.dat")

    @property
    def store_dim_path(self) -> str:
        return str(self.serial_dir / "store_dim.dat")

    @property
    def summary_out_path(self) -> str:
        return str(self.out_dir / f"daily_sales_summary_{self.business_date}")

    @property
    def reject_out_path(self) -> str:
        return str(self.out_dir / f"reject_{self.business_date}")

    # inventory_snapshot.ksh
    @property
    def inventory_input_path(self) -> str:
        return str(self.in_dir / f"inventory_{self.business_date}.dat")

    @property
    def inventory_out_path(self) -> str:
        return str(self.out_dir / f"inventory_value_{self.business_date}")

    # sandbox.pset DQ thresholds
    @property
    def dq_max_reject_pct(self) -> float:
        return float(self.params.get("DQ_MAX_REJECT_PCT", "2.0"))

    @property
    def dq_min_rowcount(self) -> int:
        return int(self.params.get("DQ_MIN_ROWCOUNT", "1000"))

    @property
    def rowcount_deviation_pct(self) -> float:
        return float(self.params.get("ROWCOUNT_DEVIATION_PCT", "25.0"))

    def to_spark_conf(self) -> dict[str, str]:
        """Job parameters exposed as ``spark.albertsons.*`` Spark conf entries."""
        conf = {
            "business_date": self.business_date,
            "project_dir": str(self.project_dir),
            "pos_input_path": self.pos_input_path,
            "product_dim_path": self.product_dim_path,
            "store_dim_path": self.store_dim_path,
            "summary_out_path": self.summary_out_path,
            "reject_out_path": self.reject_out_path,
            "inventory_input_path": self.inventory_input_path,
            "inventory_out_path": self.inventory_out_path,
            "dq_max_reject_pct": str(self.dq_max_reject_pct),
            "dq_min_rowcount": str(self.dq_min_rowcount),
            "rowcount_deviation_pct": str(self.rowcount_deviation_pct),
        }
        return {SPARK_CONF_PREFIX + k: v for k, v in conf.items()}

    @classmethod
    def load(
        cls,
        business_date: str,
        project_dir: str | Path | None = None,
        psets: Sequence[str | Path] = DEFAULT_PSETS,
        overrides: Mapping[str, str] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> BatchConfig:
        parse_business_date(business_date)
        env = dict(os.environ if environ is None else environ)
        root = Path(project_dir or env.get("PROJECT_DIR") or REPO_ROOT).resolve()
        env["PROJECT_DIR"] = str(root)
        env["BUSINESS_DATE"] = business_date
        pinned = dict(overrides or {})
        env.update(pinned)
        for p in psets:
            path = Path(p)
            pset.source(path if path.is_absolute() else root / path, env, pinned=pinned)
        return cls(business_date=business_date, project_dir=root, params=env)

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> BatchConfig:
        return cls.load(
            business_date=args.business_date,
            project_dir=args.project_dir,
            psets=(*DEFAULT_PSETS, *(args.pset or ())),
            overrides=dict(args.param or []),
        )


def _param(value: str) -> tuple[str, str]:
    name, sep, val = value.partition("=")
    if not sep or not name:
        raise argparse.ArgumentTypeError(f"--param expects NAME=VALUE, got {value!r}")
    return name, val


def build_parser(prog: str, description: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument(
        "business_date",
        nargs="?",
        default=default_business_date(),
        help="BUSINESS_DATE as YYYYMMDD (default: yesterday)",
    )
    parser.add_argument("--project-dir", help="PROJECT_DIR (default: $PROJECT_DIR or repo root)")
    parser.add_argument(
        "--pset",
        action="append",
        help="extra parameter set sourced after "
        f"{' '.join(DEFAULT_PSETS)}, relative to PROJECT_DIR (repeatable)",
    )
    parser.add_argument(
        "--param",
        action="append",
        type=_param,
        metavar="NAME=VALUE",
        help="override a pset parameter, e.g. --param DQ_MIN_ROWCOUNT=5",
    )
    return parser
