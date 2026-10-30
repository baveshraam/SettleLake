"""Airflow DAGs orchestrating the SettleLake Spark/Iceberg jobs.

settlelake_daily_batch   daily partner-statement load -> silver/reconcile/gold -> quality.
                         catchup=True, so `airflow dags backfill` replays any date range; each run
                         atomically replaces that day's Bronze statements partition (idempotent).
settlelake_event_replay  manual: re-read the CDC topic from the beginning into a fresh checkpoint.
                         Bronze MERGE is keyed by CDC sequence, so replay never duplicates rows.

Jobs run `python -m settlelake.spark ...` inside this image (Spark in local mode); catalog and
object-store settings come from the container environment.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

LANDING = "/data/landing"
SPARK = "python -m settlelake.spark"
DEFAULTS = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="settlelake_daily_batch",
    description="Daily partner statements -> Iceberg bronze -> silver -> reconciliation -> gold",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["settlelake", "batch"],
) as daily:
    ingest = BashOperator(
        task_id="ingest_statements",
        # exit 99 = no landing partition for this date: skip rather than fail
        bash_command=(
            f"test -d {LANDING}/dt={{{{ ds }}}} || exit 99; "
            f"{SPARK} ingest --input {LANDING}/dt={{{{ ds }}}} "
            "--landing-date {{ ds }} --types statement"
        ),
        skip_on_exit_code=99,
    )
    build = BashOperator(
        task_id="build_silver_reconcile_gold",
        bash_command=f"{SPARK} stage --name build",
        trigger_rule="none_failed",
    )
    quality = BashOperator(
        task_id="data_quality",
        bash_command=f"{SPARK} stage --name quality --run-id airflow_{{{{ run_id | replace(':', '_') | replace('+', '_') }}}}",
    )
    ingest >> build >> quality

with DAG(
    dag_id="settlelake_event_replay",
    description="Replay the CDC topic into Bronze using a fresh checkpoint (idempotent)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"checkpoint_suffix": Param("replay", type="string", minLength=1, pattern="^[A-Za-z0-9_-]+$")},
    default_args=DEFAULTS,
    tags=["settlelake", "replay"],
) as replay:
    BashOperator(
        task_id="replay_cdc_topic",
        bash_command=(
            "CHECKPOINT_DIR=/home/airflow/checkpoints/{{ params.checkpoint_suffix }}-{{ ts_nodash }} "
            f"{SPARK} stream --once"
        ),
    )
