"""Tests for the data-quality gate ported from run/dq_check.ksh."""

from __future__ import annotations

import dq_check


def test_pass_within_thresholds():
    result = dq_check.evaluate("20260713", summary_rows=9, reject_rows=0, min_rowcount=1, max_reject_pct=2.0)
    assert result.exit_code == dq_check.EXIT_OK


def test_fail_below_min_rowcount():
    result = dq_check.evaluate("20260713", summary_rows=9, reject_rows=1, min_rowcount=1000, max_reject_pct=2.0)
    assert result.exit_code == dq_check.EXIT_LOW_ROWCOUNT
    assert "below minimum" in result.message


def test_fail_above_max_reject_pct():
    result = dq_check.evaluate("20260713", summary_rows=9, reject_rows=1, min_rowcount=1, max_reject_pct=2.0)
    assert result.exit_code == dq_check.EXIT_HIGH_REJECT_PCT
    assert result.reject_pct == 10.0


def test_reject_pct_truncates_like_bc_scale_2():
    result = dq_check.evaluate("20260713", summary_rows=3000, reject_rows=1, min_rowcount=1, max_reject_pct=2.0)
    assert result.reject_pct == 0.03
    assert result.exit_code == dq_check.EXIT_OK


def test_counts_sample_outputs(spark, business_date):
    """The sample date produces 9 summary rows and 1 reject row on disk."""
    import config
    import daily_pos_sales

    daily_pos_sales.run(spark, business_date)
    assert dq_check.count_lines(config.summary_output_file(business_date)) == 9
    assert dq_check.count_lines(config.reject_output_file(business_date)) == 1
