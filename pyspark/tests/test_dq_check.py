"""Tests for the data-quality gate (port of run/dq_check.ksh)."""

from __future__ import annotations

from dq.dq_check import check_dq


def _write_lines(path, n):
    with open(path, "w") as fh:
        for i in range(n):
            fh.write(f"line{i}\n")


def test_pass_within_thresholds(tmp_path):
    summary = tmp_path / "summary.dat"
    reject = tmp_path / "reject.dat"
    _write_lines(summary, 1000)
    _write_lines(reject, 5)  # 5 / 1005 = 0.49% < 2.0
    res = check_dq(str(summary), str(reject), min_rowcount=1000, max_reject_pct=2.0)
    assert res.passed
    assert res.exit_code == 0
    assert res.rows == 1000
    assert res.rejects == 5


def test_fail_below_min_rowcount(tmp_path):
    summary = tmp_path / "summary.dat"
    reject = tmp_path / "reject.dat"
    _write_lines(summary, 9)
    _write_lines(reject, 1)
    res = check_dq(str(summary), str(reject), min_rowcount=1000, max_reject_pct=2.0)
    assert not res.passed
    assert res.exit_code == 2


def test_fail_reject_pct_exceeded(tmp_path):
    summary = tmp_path / "summary.dat"
    reject = tmp_path / "reject.dat"
    _write_lines(summary, 1000)
    _write_lines(reject, 100)  # 100 / 1100 = 9.09% > 2.0
    res = check_dq(str(summary), str(reject), min_rowcount=1000, max_reject_pct=2.0)
    assert not res.passed
    assert res.exit_code == 3


def test_missing_files_treated_as_zero(tmp_path):
    res = check_dq(
        str(tmp_path / "nope.dat"),
        str(tmp_path / "none.dat"),
        min_rowcount=1000,
        max_reject_pct=2.0,
    )
    assert res.rows == 0
    assert res.exit_code == 2  # below minimum
