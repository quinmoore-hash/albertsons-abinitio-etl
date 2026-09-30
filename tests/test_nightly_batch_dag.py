import sys
from datetime import datetime, timezone

import pytest

pytest.importorskip("airflow")

from conftest import REPO_ROOT  # noqa: E402

sys.path.insert(0, str(REPO_ROOT / "dags"))


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(REPO_ROOT / "dags"), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("nightly_batch")


def test_task_chain_mirrors_plan(dag):
    order = ["START", "daily_pos_sales", "inventory_snapshot", "dq_check", "END"]
    assert set(dag.task_ids) == set(order)
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_schedule_and_failure_callback(dag):
    assert dag.timetable.summary == "30 2 * * *"
    for task in dag.tasks:
        callbacks = task.on_failure_callback
        callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
        assert [cb.__name__ for cb in callbacks] == ["alert_store_data_ops"]


def test_business_date_defaults_to_local_yesterday():
    import nightly_batch

    run = datetime(2026, 7, 14, 8, 30, tzinfo=timezone.utc)  # 02:30 America/Boise
    assert nightly_batch.business_date({}, run) == "20260713"
    assert nightly_batch.business_date({"business_date": "20260101"}, run) == "20260101"
    with pytest.raises(ValueError):
        nightly_batch.business_date({"business_date": "20260101; rm -rf /"}, run)


def test_spark_commands(dag):
    cmd = dag.get_task("dq_check").bash_command
    assert "spark/jobs/dq_check.py" in cmd
    assert "business_date(params, logical_date, dag_run)" in cmd
    assert "--skip-load" in dag.get_task("daily_pos_sales").bash_command


def test_alert_calls_notify(monkeypatch):
    import nightly_batch

    calls = []
    monkeypatch.setattr(nightly_batch, "notify_failure", lambda *a, **k: calls.append((a, k)))
    nightly_batch.alert_store_data_ops({"run_id": "r1", "exception": "boom"})
    ((args, kwargs),) = calls
    assert args == ("store-data-ops@albertsons.com", "#store-data-ops")
    assert "run_id=r1" in kwargs["detail"] and "boom" in kwargs["detail"]
