from decimal import Decimal

import pytest

from spark.jobs import dq_check
from spark.jobs.dq_check import DQFailure, evaluate, reject_pct


def test_reject_pct_truncates_like_bc():
    assert reject_pct(9, 1) == Decimal("10.00")
    assert reject_pct(2, 1) == Decimal("33.33")
    assert reject_pct(1, 2) == Decimal("66.66")
    assert reject_pct(0, 0) is None


def test_min_rowcount_fails_with_exit_2():
    with pytest.raises(DQFailure) as exc:
        evaluate("20260713", 999, 0, min_rowcount=1000, max_reject_pct=2.0)
    assert exc.value.exit_code == 2


def test_reject_pct_fails_with_exit_3():
    with pytest.raises(DQFailure) as exc:
        evaluate("20260713", 1000, 21, min_rowcount=1000, max_reject_pct=2.0)
    assert exc.value.exit_code == 3


def test_reject_pct_at_threshold_passes():
    # 20 * 100 / (980 + 20) = 2.00%; the gate fails only when strictly greater
    result = evaluate("20260713", 980, 20, min_rowcount=900, max_reject_pct=2.0)
    assert result.reject_pct == Decimal("2.00")


def test_missing_files_count_as_zero(spark, batch_cfg):
    with pytest.raises(DQFailure) as exc:
        dq_check.run(spark, batch_cfg)
    assert exc.value.exit_code == 2
