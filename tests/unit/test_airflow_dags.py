from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[2] / "orchestration" / "dags"


def test_dags_import_and_have_expected_shape():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert not bag.import_errors
    daily = bag.get_dag("settlelake_daily_batch")
    assert daily.catchup is True and daily.max_active_runs == 1
    assert [t.task_id for t in daily.topological_sort()] == [
        "ingest_statements",
        "build_silver_reconcile_gold",
        "data_quality",
    ]
    replay = bag.get_dag("settlelake_event_replay")
    assert replay.schedule_interval is None and "checkpoint_suffix" in replay.params
