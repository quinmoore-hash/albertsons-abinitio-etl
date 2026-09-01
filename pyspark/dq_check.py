"""PySpark port of ``run/dq_check.ksh`` — post-load data-quality gate.

Counts the summary and reject rows for a business date and fails (non-zero
exit) when the summary row count is below ``DQ_MIN_ROWCOUNT`` or the reject
percentage exceeds ``DQ_MAX_REJECT_PCT``. Thresholds come from the environment
(``sand/sandbox.pset`` equivalent, see :mod:`config`).

Usage: ``python pyspark/dq_check.py <BUSINESS_DATE:YYYYMMDD>``
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import config

EXIT_OK = 0
EXIT_LOW_ROWCOUNT = 2
EXIT_HIGH_REJECT_PCT = 3


def count_lines(path: str | Path) -> int:
    path = Path(path)
    if not path.exists():
        return 0
    with open(path, "rb") as handle:
        return sum(1 for line in handle if line.strip())


@dataclass(frozen=True)
class DqResult:
    business_date: str
    summary_rows: int
    reject_rows: int
    reject_pct: float
    exit_code: int
    message: str


def evaluate(
    business_date: str,
    summary_rows: int,
    reject_rows: int,
    min_rowcount: int,
    max_reject_pct: float,
) -> DqResult:
    total = summary_rows + reject_rows
    # bc "scale=2" truncates, as in the legacy script.
    reject_pct = int(reject_rows * 100 * 100 / total) / 100 if total > 0 else 0.0

    if summary_rows < min_rowcount:
        return DqResult(
            business_date,
            summary_rows,
            reject_rows,
            reject_pct,
            EXIT_LOW_ROWCOUNT,
            f"[DQ][FAIL] summary rows {summary_rows} below minimum {min_rowcount}",
        )

    if total > 0 and reject_pct > max_reject_pct:
        return DqResult(
            business_date,
            summary_rows,
            reject_rows,
            reject_pct,
            EXIT_HIGH_REJECT_PCT,
            f"[DQ][FAIL] reject pct {reject_pct}% exceeds max {max_reject_pct}%",
        )

    return DqResult(
        business_date,
        summary_rows,
        reject_rows,
        reject_pct,
        EXIT_OK,
        f"[DQ][PASS] business_date={business_date}",
    )


def run(business_date: str) -> DqResult:
    summary_rows = count_lines(config.summary_output_file(business_date))
    reject_rows = count_lines(config.reject_output_file(business_date))
    print(
        f"[DQ] business_date={business_date} summary_rows={summary_rows} "
        f"reject_rows={reject_rows}"
    )
    result = evaluate(
        business_date,
        summary_rows,
        reject_rows,
        config.DQ_MIN_ROWCOUNT,
        config.DQ_MAX_REJECT_PCT,
    )
    print(result.message, file=sys.stderr if result.exit_code else sys.stdout)
    return result


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: dq_check.py <BUSINESS_DATE:YYYYMMDD>", file=sys.stderr)
        return 1
    return run(argv[1]).exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
