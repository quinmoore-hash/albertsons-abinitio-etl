import re
from pathlib import Path

import pytest

from spark import schemas
from spark.schemas import RECORD_FORMATS

DML_DIR = Path(__file__).resolve().parents[1] / "dml"
_FIELD = re.compile(r"^\s*(decimal|string|date)\b.*?\)\s*(\w+)\s*;", re.MULTILINE)
_DELIM = re.compile(r'^\s*\w+\("([^"]*)"\)(?:\("([^"]*)"\))?\s*\w+\s*;', re.MULTILINE)


def _strip_comments(text: str) -> str:
    return re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)


def parse_dml(path: Path) -> list[tuple[str, str]]:
    return [(m.group(2), m.group(1)) for m in _FIELD.finditer(_strip_comments(path.read_text()))]


def test_every_dml_has_a_schema():
    assert {p.stem for p in DML_DIR.glob("*.dml")} == set(RECORD_FORMATS)


@pytest.mark.parametrize("name", sorted(RECORD_FORMATS))
def test_schema_matches_dml_field_order_and_types(name):
    dml_fields = parse_dml(DML_DIR / f"{name}.dml")
    spark_fields = RECORD_FORMATS[name].schema.fields
    assert [n for n, _ in dml_fields] == [f.name for f in spark_fields]
    for (field, dml_type), sf in zip(dml_fields, spark_fields, strict=True):
        expected = {"decimal": "DecimalType", "string": "StringType", "date": "DateType"}[dml_type]
        assert type(sf.dataType).__name__ == expected, field


@pytest.mark.parametrize("name", sorted(RECORD_FORMATS))
def test_delimiter_matches_dml(name):
    text = _strip_comments((DML_DIR / f"{name}.dml").read_text())
    delims = {d for m in _DELIM.finditer(text) for d in m.groups() if d and d != "\\n"}
    delims.discard("YYYY-MM-DD")
    assert delims == {RECORD_FORMATS[name].delimiter}


def test_decimal_precision():
    pos = schemas.POS_SALES.schema
    assert pos["upc"].dataType == schemas.UPC
    assert (pos["unit_price"].dataType.precision, pos["unit_price"].dataType.scale) == (12, 2)
    assert pos["qty"].dataType.scale == 3
    assert schemas.PRODUCT_DIM.schema["avg_cost"].dataType.scale == 4
    summary = schemas.DAILY_SALES_SUMMARY.schema
    for money in ("gross_sales", "discount_total", "net_sales", "private_label_sales"):
        assert summary[money].dataType == schemas.MONEY_AGG
    assert summary["txn_count"].dataType.scale == 0


def test_nullability_follows_xfr_null_handling():
    nullable = {
        rf.name: {f.name for f in rf.schema.fields if f.nullable} for rf in RECORD_FORMATS.values()
    }
    assert nullable == {
        "pos_sales": {"unit_price", "discount_amt", "loyalty_id"},
        "product_dim": {"brand"},
        "store_dim": set(),
        "daily_sales_summary": set(),
        "inventory": set(),
        "inventory_value": set(),
    }
