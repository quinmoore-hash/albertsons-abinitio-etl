"""End-to-end parity tests against the shipped sample data (business 20260713).

These assert the PySpark jobs reproduce hand-computed expected output for the
sample feeds -- including the reject path, the dropped voided line, and the
active-store inventory filter. If an Ab Initio environment is available, the
same ``data/out/*.dat`` files can be diffed byte-for-byte against these.
"""

from __future__ import annotations

import os

from dq.dq_check import check_dq
from jobs.daily_pos_sales import run as run_pos
from jobs.inventory_snapshot import run as run_inventory

BUSINESS_DATE = "20260713"

EXPECTED_SUMMARY = [
    "2026-07-13|1001|ALBERTSONS|INTERMOUNTAIN|DAIRY|EGGS|1|1|2.49|0.30|2.19|2.19|1",
    "2026-07-13|1001|ALBERTSONS|INTERMOUNTAIN|GROCERY|BEVERAGES|1|2|11.98|1.00|10.98|0.00|1",
    "2026-07-13|1001|ALBERTSONS|INTERMOUNTAIN|PRODUCE|FRESH FRUIT|1|6|1.74|0.00|1.74|0.00|0",
    "2026-07-13|1042|SAFEWAY|DENVER|DAIRY|MILK|1|1|4.29|0.00|4.29|4.29|0",
    "2026-07-13|1042|SAFEWAY|DENVER|GROCERY|CEREAL|1|1|3.99|0.50|3.49|0.00|1",
    "2026-07-13|1042|SAFEWAY|DENVER|PRODUCE|FRESH FRUIT|1|4|3.96|0.00|3.96|0.00|0",
    "2026-07-13|2255|VONS|SOCAL|GROCERY|BEVERAGES|1|3|10.47|0.00|10.47|10.47|1",
    "2026-07-13|2255|VONS|SOCAL|MEAT|POULTRY|1|2|7.98|0.00|7.98|7.98|1",
    "2026-07-13|3300|JEWEL-OSCO|MIDWEST|GROCERY|CEREAL|1|2|7.98|0.00|7.98|0.00|0",
]

EXPECTED_REJECT = [
    "3300,7,T0901,2026-07-13,11:22:41,9999999999,UNKNOWN ITEM,1,1.00,1.00,0.00,,CASH,N",
]

EXPECTED_INVENTORY = [
    "2026-07-13|1001|DAIRY|1|85|114.75|211.65",
    "2026-07-13|1001|PRODUCE|1|320|60.80|92.80",
    "2026-07-13|1042|DAIRY|1|60|186.00|257.40",
    "2026-07-13|1042|GROCERY|1|210|577.50|837.90",
    "2026-07-13|2255|GROCERY|1|540|1323.00|1884.60",
    "2026-07-13|2255|MEAT|1|48|105.60|191.52",
    "2026-07-13|3300|GROCERY|1|175|481.25|698.25",
]


def _lines(path):
    with open(path) as fh:
        return [ln.rstrip("\n") for ln in fh if ln.strip() != ""]


def test_daily_pos_sales_matches_expected(spark, data_dir):
    summary_path, reject_path = run_pos(
        spark, BUSINESS_DATE, data_dir=data_dir, load_db=False
    )
    summary = _lines(summary_path)
    reject = _lines(reject_path)

    assert summary == EXPECTED_SUMMARY
    assert reject == EXPECTED_REJECT

    # voided line T0501 must not appear anywhere in the output
    assert not any("T0501" in ln for ln in summary + reject)
    # unknown UPC only in the reject file
    assert not any("9999999999" in ln for ln in summary)
    assert all("9999999999" in ln for ln in reject)


def test_inventory_snapshot_matches_expected(spark, data_dir):
    out_path = run_inventory(spark, BUSINESS_DATE, data_dir=data_dir, load_db=False)
    assert _lines(out_path) == EXPECTED_INVENTORY

    # store 4120 (REMODEL, 0 on hand) must be filtered out
    assert not any("|4120|" in ln for ln in _lines(out_path))


def test_dq_gate_on_sample(spark, data_dir):
    run_pos(spark, BUSINESS_DATE, data_dir=data_dir, load_db=False)
    out_dir = os.path.join(data_dir, "out")
    summary_path = os.path.join(out_dir, f"daily_sales_summary_{BUSINESS_DATE}.dat")
    reject_path = os.path.join(out_dir, f"reject_{BUSINESS_DATE}.dat")

    # Production thresholds fail on the tiny sample (9 rows < 1000).
    prod = check_dq(summary_path, reject_path, min_rowcount=1000, max_reject_pct=2.0)
    assert not prod.passed and prod.exit_code == 2

    # Demo thresholds: 9 summary rows, 1 reject -> 10% reject pct.
    lenient = check_dq(summary_path, reject_path, min_rowcount=5, max_reject_pct=20.0)
    assert lenient.passed
    assert lenient.rows == 9 and lenient.rejects == 1
    assert lenient.reject_pct == 10.0
