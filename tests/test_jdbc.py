from pathlib import Path

import pytest

from spark.common.jdbc import EdwConfigError, EdwJdbc, edw_from_dbc_dir, parse_dbc

REPO_ROOT = Path(__file__).resolve().parents[1]
DBC = REPO_ROOT / "dbc" / "edw.dbc"
DBC_ENV = REPO_ROOT / "dbc" / "edw.dbc.env"


def test_parse_dbc_expands_env():
    values = parse_dbc(DBC, {"EDW_DB_USER": "u", "EDW_DB_PASSWORD": "p", "ORACLE_HOME": "/o"})
    assert values["dbms"] == "oracle"
    assert values["db_user"] == "u"
    assert values["db_home"] == "/o"
    assert values["db_array_size"] == "5000"


def test_from_dbc_builds_oracle_thin_url():
    edw = EdwJdbc.from_dbc(DBC, DBC_ENV, environ={"EDW_DB_PASSWORD": "secret"})
    assert (
        edw.url
        == "jdbc:oracle:thin:@//edw-scan.albertsons.internal:1521/EDWPRD.albertsons.internal"
    )
    assert edw.user == "edw_etl_svc"
    assert edw.password == "secret"
    assert edw.default_schema == "EDW"
    assert edw.batch_size == 5000
    assert edw.options()["driver"] == "oracle.jdbc.OracleDriver"


def test_url_override():
    edw = EdwJdbc.from_dbc(DBC, DBC_ENV, environ={"EDW_JDBC_URL": "jdbc:oracle:thin:@//h:1/s"})
    assert edw.url == "jdbc:oracle:thin:@//h:1/s"


def test_vault_placeholder_rejected():
    edw = EdwJdbc.from_dbc(DBC, DBC_ENV, environ={})
    assert edw.password == "__SET_IN_VAULT__"
    with pytest.raises(EdwConfigError):
        edw._require_credentials()


def test_qualify_and_modes(spark):
    edw = EdwJdbc(url="jdbc:oracle:thin:@//h:1/s", user="u", password="p")
    assert edw.qualify("F_X") == "EDW.F_X"
    assert edw.qualify("OTHER.F_X") == "OTHER.F_X"
    with pytest.raises(ValueError):
        edw.write(spark.range(1), "F_X", mode="overwrite")
    with pytest.raises(ValueError):
        edw.read(spark)


def test_edw_from_dbc_dir(monkeypatch):
    monkeypatch.setenv("EDW_DB_PASSWORD", "pw")
    assert edw_from_dbc_dir(REPO_ROOT / "dbc").password == "pw"
