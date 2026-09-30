"""``dq_check`` job (port of ``run/dq_check.ksh``): post-load data-quality gate.

    python -m spark.jobs.dq_check 20260713 [--notify]

Exit codes match the ksh gate: ``0`` pass, ``2`` summary rows below
``DQ_MIN_ROWCOUNT``, ``3`` reject pct above ``DQ_MAX_REJECT_PCT``.
The reject percentage is ``reject_rows * 100 / (summary_rows + reject_rows)``
truncated to 2 decimals, exactly as the ksh ``bc scale=2`` computation.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from spark.common.config import BatchConfig, build_parser
from spark.common.io import read_delimited
from spark.common.notify import DEFAULT_CHANNEL, DEFAULT_EMAIL, notify_failure
from spark.common.session import create_spark_session
from spark.schemas import DAILY_SALES_SUMMARY, POS_SALES, RecordFormat

log = logging.getLogger("dq_check")

EXIT_PASS = 0
EXIT_MIN_ROWCOUNT = 2
EXIT_REJECT_PCT = 3


class DQFailure(RuntimeError):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True)
class DQResult:
    business_date: str
    summary_rows: int
    reject_rows: int
    reject_pct: Decimal | None


def reject_pct(summary_rows: int, reject_rows: int) -> Decimal | None:
    total = summary_rows + reject_rows
    if total <= 0:
        return None
    return (Decimal(reject_rows) * 100 / Decimal(total)).quantize(Decimal("0.01"), ROUND_DOWN)


def evaluate(
    business_date: str,
    summary_rows: int,
    reject_rows: int,
    min_rowcount: int,
    max_reject_pct: float,
) -> DQResult:
    """Apply the ksh gate to row counts; raise :class:`DQFailure` when it fails."""
    pct = reject_pct(summary_rows, reject_rows)
    result = DQResult(business_date, summary_rows, reject_rows, pct)
    if summary_rows < min_rowcount:
        raise DQFailure(
            f"summary rows {summary_rows} below minimum {min_rowcount}", EXIT_MIN_ROWCOUNT
        )
    if pct is not None and pct > Decimal(str(max_reject_pct)):
        raise DQFailure(f"reject pct {pct}% exceeds max {max_reject_pct}%", EXIT_REJECT_PCT)
    return result


def _count(spark: SparkSession, path: str, record_format: RecordFormat) -> int:
    # m_wc -l on a missing file yields 0 in the ksh gate.
    if not Path(path).exists():
        return 0
    return read_delimited(spark, path, record_format, strict=False).count()


def run(spark: SparkSession, cfg: BatchConfig) -> DQResult:
    summary_rows = _count(spark, cfg.summary_out_path, DAILY_SALES_SUMMARY)
    reject_rows = _count(spark, cfg.reject_out_path, POS_SALES)
    log.info(
        "[DQ] business_date=%s summary_rows=%d reject_rows=%d",
        cfg.business_date,
        summary_rows,
        reject_rows,
    )
    result = evaluate(
        cfg.business_date, summary_rows, reject_rows, cfg.dq_min_rowcount, cfg.dq_max_reject_pct
    )
    log.info("[DQ][PASS] business_date=%s", cfg.business_date)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = build_parser("dq_check", __doc__)
    parser.add_argument(
        "--notify",
        action="store_true",
        help="send the notify.ksh-equivalent alert on failure (the Airflow DAG does this itself)",
    )
    parser.add_argument("--notify-email", default=DEFAULT_EMAIL)
    parser.add_argument("--notify-channel", default=DEFAULT_CHANNEL)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s %(message)s")
    cfg = BatchConfig.from_args(args)
    spark = create_spark_session("dq_check", cfg)
    try:
        run(spark, cfg)
    except DQFailure as exc:
        log.error("[DQ][FAIL] %s", exc)
        if args.notify:
            notify_failure(
                args.notify_email,
                args.notify_channel,
                detail=f"dq_check {cfg.business_date}: {exc}",
                project_dir=str(cfg.project_dir),
            )
        return exc.exit_code
    finally:
        spark.stop()
    return EXIT_PASS


if __name__ == "__main__":
    sys.exit(main())
