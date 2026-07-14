"""Post-load data-quality gate -- port of ``run/dq_check.ksh``.

Validates the daily sales summary output against the sandbox thresholds
(mirroring ``sand/sandbox.pset``):

* summary row count must be ``>= DQ_MIN_ROWCOUNT`` (default 1000)
* reject percentage ``reject / (summary + reject) * 100`` must be
  ``<= DQ_MAX_REJECT_PCT`` (default 2.0)

Exit codes match the legacy script:

* ``0`` -> PASS
* ``2`` -> summary rows below minimum
* ``3`` -> reject pct exceeds maximum
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

# Defaults mirror sand/sandbox.pset.
DEFAULT_MIN_ROWCOUNT = 1000
DEFAULT_MAX_REJECT_PCT = 2.0


@dataclass
class DQResult:
    passed: bool
    exit_code: int
    rows: int
    rejects: int
    reject_pct: float
    message: str


def _count_lines(path: str) -> int:
    """Count records in a delimited output file (0 if it does not exist)."""
    if not path or not os.path.isfile(path):
        return 0
    count = 0
    with open(path, "rb") as fh:
        for _ in fh:
            count += 1
    return count


def check_dq(
    summary_path: str,
    reject_path: str,
    *,
    min_rowcount: int = DEFAULT_MIN_ROWCOUNT,
    max_reject_pct: float = DEFAULT_MAX_REJECT_PCT,
) -> DQResult:
    """Evaluate the DQ gate; returns a :class:`DQResult` (no process exit)."""
    rows = _count_lines(summary_path)
    rejects = _count_lines(reject_path)

    total = rows + rejects
    reject_pct = round(rejects * 100 / total, 2) if total > 0 else 0.0

    if rows < min_rowcount:
        return DQResult(
            passed=False,
            exit_code=2,
            rows=rows,
            rejects=rejects,
            reject_pct=reject_pct,
            message=f"[DQ][FAIL] summary rows {rows} below minimum {min_rowcount}",
        )

    if reject_pct > max_reject_pct:
        return DQResult(
            passed=False,
            exit_code=3,
            rows=rows,
            rejects=rejects,
            reject_pct=reject_pct,
            message=(
                f"[DQ][FAIL] reject pct {reject_pct}% exceeds max {max_reject_pct}%"
            ),
        )

    return DQResult(
        passed=True,
        exit_code=0,
        rows=rows,
        rejects=rejects,
        reject_pct=reject_pct,
        message="[DQ][PASS]",
    )


def _thresholds_from_env() -> tuple[int, float]:
    min_rowcount = int(os.environ.get("DQ_MIN_ROWCOUNT", DEFAULT_MIN_ROWCOUNT))
    max_reject_pct = float(os.environ.get("DQ_MAX_REJECT_PCT", DEFAULT_MAX_REJECT_PCT))
    return min_rowcount, max_reject_pct


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nightly batch data-quality gate.")
    parser.add_argument("--business-date", required=True, help="BUSINESS_DATE (YYYYMMDD)")
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("AB_DATA_DIR", "data"),
        help="Root data directory containing out/ (default: ./data).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    out_dir = os.path.join(args.data_dir, "out")
    summary_path = os.path.join(out_dir, f"daily_sales_summary_{args.business_date}.dat")
    reject_path = os.path.join(out_dir, f"reject_{args.business_date}.dat")

    min_rowcount, max_reject_pct = _thresholds_from_env()
    result = check_dq(
        summary_path,
        reject_path,
        min_rowcount=min_rowcount,
        max_reject_pct=max_reject_pct,
    )

    print(
        f"[DQ] business_date={args.business_date} "
        f"summary_rows={result.rows} reject_rows={result.rejects} "
        f"reject_pct={result.reject_pct}%"
    )
    if result.passed:
        print(f"[DQ][PASS] business_date={args.business_date}")
    else:
        print(result.message, file=sys.stderr)

    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
