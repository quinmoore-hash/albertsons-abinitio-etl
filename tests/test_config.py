from pathlib import Path

import pytest

from spark.common import pset
from spark.common.config import BatchConfig, build_parser, default_business_date

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_expand_default_and_nested_braces():
    env = {"A": "x"}
    assert pset.expand("${A:-y}/$A/${B:-z}", env) == "x/x/z"
    # nested default is never evaluated when the variable is already set
    env["PROJECT_DIR"] = "/p"
    assert (
        pset.expand('${PROJECT_DIR:-$(cd "$(dirname "${.sh.file:-$0}")/.." && pwd)}', env) == "/p"
    )


def test_command_substitution_rejected_when_needed():
    with pytest.raises(pset.PsetError):
        pset.expand("${MISSING:-$(pwd)}", {})


def test_source_sandbox_pset(tmp_path):
    env = pset.source(REPO_ROOT / "sand" / "sandbox.pset", {"PROJECT_DIR": "/proj"})
    assert env["AI_SERIAL"] == "/proj/data/serial"
    assert env["DQ_MAX_REJECT_PCT"] == "2.0"
    assert env["DQ_MIN_ROWCOUNT"] == "1000"
    assert env["ROWCOUNT_DEVIATION_PCT"] == "25.0"


def test_source_quotes_and_comments(tmp_path):
    f = tmp_path / "x.pset"
    f.write_text("# c\nexport A=\"$B/1\"  # trailing\nC='$B'\nnot an assignment\n")
    env = pset.source(f, {"B": "b"})
    assert env["A"] == "b/1" and env["C"] == "$B"


def test_batch_config_paths_and_thresholds():
    cfg = BatchConfig.load("20260713", project_dir=REPO_ROOT, environ={})
    assert cfg.pos_input_path == str(REPO_ROOT / "data/in/pos_sales_20260713.dat")
    assert cfg.product_dim_path == str(REPO_ROOT / "data/serial/product_dim.dat")
    assert cfg.store_dim_path == str(REPO_ROOT / "data/serial/store_dim.dat")
    assert cfg.summary_out_path == str(REPO_ROOT / "data/out/daily_sales_summary_20260713")
    assert cfg.reject_out_path == str(REPO_ROOT / "data/out/reject_20260713")
    assert cfg.inventory_input_path == str(REPO_ROOT / "data/in/inventory_20260713.dat")
    assert cfg.dq_max_reject_pct == 2.0
    assert cfg.dq_min_rowcount == 1000
    assert cfg.params["AB_HOME"] == "/opt/abinitio/abinitio-V4"


def test_environment_wins_for_defaulted_params():
    cfg = BatchConfig.load("20260713", project_dir=REPO_ROOT, environ={"AB_HOME": "/custom"})
    assert cfg.params["AB_HOME"] == "/custom"


def test_cli_overrides_and_spark_conf():
    args = build_parser("t").parse_args(
        ["20260713", "--project-dir", str(REPO_ROOT), "--param", "DQ_MIN_ROWCOUNT=5"]
    )
    cfg = BatchConfig.from_args(args)
    assert cfg.dq_min_rowcount == 5
    conf = cfg.to_spark_conf()
    assert conf["spark.albertsons.business_date"] == "20260713"
    assert conf["spark.albertsons.dq_min_rowcount"] == "5"
    assert conf["spark.albertsons.pos_input_path"].endswith("pos_sales_20260713.dat")


def test_invalid_business_date():
    with pytest.raises(ValueError):
        BatchConfig.load("2026-07-13", project_dir=REPO_ROOT, environ={})


def test_default_business_date_is_yesterday():
    from datetime import date

    assert default_business_date(date(2026, 7, 14)) == "20260713"
