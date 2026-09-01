"""Parity tests for the inventory_snapshot port against the sample feed."""

from __future__ import annotations

import config
import inventory_snapshot

# snapshot_date | store_id | department | sku_count | on_hand_units | cost | retail
EXPECTED_INVENTORY_VALUE = [
    ("2026-07-13", 1001, "DAIRY", 1, 85, "114.75", "211.65"),
    ("2026-07-13", 1001, "PRODUCE", 1, 320, "60.80", "92.80"),
    ("2026-07-13", 1042, "DAIRY", 1, 60, "186.00", "257.40"),
    ("2026-07-13", 1042, "GROCERY", 1, 210, "577.50", "837.90"),
    ("2026-07-13", 2255, "GROCERY", 1, 540, "1323.00", "1884.60"),
    ("2026-07-13", 2255, "MEAT", 1, 48, "105.60", "191.52"),
    ("2026-07-13", 3300, "GROCERY", 1, 175, "481.25", "698.25"),
]


def _run(spark, business_date):
    if not hasattr(_run, "cache"):
        _run.cache = inventory_snapshot.run(spark, business_date)
    return _run.cache


def test_inventory_value_matches_legacy_expected_results(spark, business_date):
    rows = [
        (
            row.snapshot_date.strftime("%Y-%m-%d"),
            int(row.store_id),
            row.department,
            int(row.sku_count),
            int(row.on_hand_units),
            str(row.cost_value),
            str(row.retail_value),
        )
        for row in _run(spark, business_date).collect()
    ]
    assert rows == EXPECTED_INVENTORY_VALUE


def test_remodel_store_is_filtered_out(spark, business_date):
    assert _run(spark, business_date).filter("store_id = 4120").count() == 0


def test_output_file_is_written(spark, business_date):
    _run(spark, business_date)
    lines = config.inventory_output_file(business_date).read_text().splitlines()
    assert len(lines) == 7
    assert lines[0] == "2026-07-13|1001|DAIRY|1|85|114.75|211.65"
