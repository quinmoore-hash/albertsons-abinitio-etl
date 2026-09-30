"""End-to-end parity on the checked-in 20260713 sample against graph semantics."""

from pathlib import Path

import pytest
from conftest import BUSINESS_DATE, EXPECTED_DIR, REPO_ROOT
from helpers import D

from spark.common.io import read_delimited
from spark.jobs import daily_pos_sales, dq_check, inventory_snapshot
from spark.jobs.dq_check import DQFailure
from spark.schemas import DAILY_SALES_SUMMARY, INVENTORY_VALUE, POS_SALES


def _rows(spark, path, record_format):
    return sorted(tuple(r) for r in read_delimited(spark, str(path), record_format).collect())


@pytest.fixture
def loaded(spark, batch_cfg):
    captured = {}

    def capture(name):
        def _loader(df, cfg):
            captured[name] = [tuple(r) for r in df.collect()]

        return _loader

    sales = daily_pos_sales.run(spark, batch_cfg, loader=capture("summary"))
    inventory = inventory_snapshot.run(spark, batch_cfg, loader=capture("inventory"))
    return sales, inventory, captured


def test_daily_sales_summary_matches_expected(spark, batch_cfg, loaded):
    sales, _, captured = loaded
    expected = _rows(
        spark, EXPECTED_DIR / f"daily_sales_summary_{BUSINESS_DATE}.dat", DAILY_SALES_SUMMARY
    )
    assert _rows(spark, batch_cfg.summary_out_path, DAILY_SALES_SUMMARY) == expected
    assert sorted(captured["summary"]) == expected
    assert sales.summary_rows == 9


def test_reject_file_holds_only_unknown_upc(spark, batch_cfg, loaded):
    sales, _, _ = loaded
    expected = _rows(spark, EXPECTED_DIR / f"reject_{BUSINESS_DATE}.dat", POS_SALES)
    rejects = _rows(spark, batch_cfg.reject_out_path, POS_SALES)
    assert rejects == expected
    assert [r[5] for r in rejects] == [D(9999999999)]
    assert sales.reject_rows == 1


def test_voided_line_is_dropped_not_rejected(spark, batch_cfg, loaded):
    sales, _, _ = loaded
    assert sales.rejects.filter("transaction_id = 'T0501'").count() == 0
    vons_produce = sales.summary.filter("store_id = 2255 AND department = 'PRODUCE'")
    assert vons_produce.count() == 0


def test_cc_tender_normalized_and_loyalty_counted(spark, batch_cfg, loaded):
    sales, _, _ = loaded
    loyalty = {
        (int(r.store_id), r.category): int(r.loyalty_txn_count) for r in sales.summary.collect()
    }
    assert loyalty[(2255, "BEVERAGES")] == 1 and loyalty[(1001, "FRESH FRUIT")] == 0


def test_inventory_value_matches_expected(spark, batch_cfg, loaded):
    _, inventory, captured = loaded
    expected = _rows(spark, EXPECTED_DIR / f"inventory_value_{BUSINESS_DATE}.dat", INVENTORY_VALUE)
    assert _rows(spark, batch_cfg.inventory_out_path, INVENTORY_VALUE) == expected
    assert sorted(captured["inventory"]) == expected
    assert inventory.filter("store_id = 4120").count() == 0  # REMODEL store filtered


def test_dq_gate_on_sample(spark, batch_cfg, loaded):
    from spark.common.config import BatchConfig

    # default thresholds (DQ_MIN_ROWCOUNT=1000) fail the 9-row sample, like the ksh gate
    with pytest.raises(DQFailure) as exc:
        dq_check.run(spark, batch_cfg)
    assert exc.value.exit_code == dq_check.EXIT_MIN_ROWCOUNT

    def cfg_with(**params):
        return BatchConfig.load(
            BUSINESS_DATE,
            project_dir=REPO_ROOT,
            overrides={"AI_OUT": str(batch_cfg.out_dir), **params},
            environ={},
        )

    # 1 reject / (9 + 1) = 10.00% > 2.0%
    with pytest.raises(DQFailure) as exc:
        dq_check.run(spark, cfg_with(DQ_MIN_ROWCOUNT="9"))
    assert exc.value.exit_code == dq_check.EXIT_REJECT_PCT

    result = dq_check.run(spark, cfg_with(DQ_MIN_ROWCOUNT="9", DQ_MAX_REJECT_PCT="10.0"))
    assert (result.summary_rows, result.reject_rows, str(result.reject_pct)) == (9, 1, "10.00")


def test_outputs_are_single_part_files_matching_expected_bytes(batch_cfg, loaded):
    for path, expected in (
        (batch_cfg.summary_out_path, f"daily_sales_summary_{BUSINESS_DATE}.dat"),
        (batch_cfg.reject_out_path, f"reject_{BUSINESS_DATE}.dat"),
        (batch_cfg.inventory_out_path, f"inventory_value_{BUSINESS_DATE}.dat"),
    ):
        parts = list(Path(path).glob("part-*"))
        assert len(parts) == 1
        assert parts[0].read_bytes() == (EXPECTED_DIR / expected).read_bytes()
