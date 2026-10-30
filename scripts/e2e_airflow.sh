#!/usr/bin/env bash
# Airflow backfill proof: partner statements are loaded per day by `airflow dags backfill`, then the
# same backfill is re-run (reset) and must leave Iceberg tables unchanged.
set -euo pipefail
cd "$(dirname "$0")/.."
C="docker compose --profile airflow"
RUN="$C run --rm -T spark"
AF="$C run --rm -T airflow"
Q() { $RUN python -m settlelake.spark sql "$1" | tr -d '\r'; }
step() { echo; echo "=== $* ==="; }

step "infrastructure"
$C up -d --build --wait postgres s3
$C run --rm -T s3-init
$C build spark airflow

step "dataset: events via file ingest, statements split into daily landing partitions"
$RUN settlelake generate --payments 1000 --seed 42 --anomalies low
$RUN python -m settlelake.spark split-landing --raw /data/raw --landing /data/landing
$RUN python -m settlelake.spark ingest --input /data/raw --types event >/dev/null

step "DAGs import cleanly and have the expected tasks"
$AF python -c "
from airflow.models import DagBag
b = DagBag('/opt/airflow/dags', include_examples=False)
assert not b.import_errors, b.import_errors
d = b.get_dag('settlelake_daily_batch')
assert d.catchup and [t.task_id for t in d.topological_sort()] == ['ingest_statements', 'build_silver_reconcile_gold', 'data_quality']
assert b.get_dag('settlelake_event_replay').schedule_interval is None
print('dags ok')
"

step "airflow backfill 2026-01-02 .. 2026-01-04"
$AF bash -c "airflow db migrate >/dev/null 2>&1 && airflow dags backfill -s 2026-01-02 -e 2026-01-04 settlelake_daily_batch"
DAYS=$(Q "SELECT count(DISTINCT landing_date) AS n FROM lake.bronze.statements")
ROWS=$(Q "SELECT count(*) AS n FROM lake.bronze.statements")
REC=$(Q "SELECT count(*) AS n FROM lake.gold.reconciliation")
echo "after backfill: days=$DAYS rows=$ROWS reconciliation=$REC"
echo "$DAYS" | grep -q '"n": 3'

step "re-run the same backfill (reset): tables must be unchanged (idempotent)"
$AF bash -c "airflow db migrate >/dev/null 2>&1 && airflow dags backfill --reset-dagruns -y -s 2026-01-02 -e 2026-01-04 settlelake_daily_batch"
[ "$(Q "SELECT count(DISTINCT landing_date) AS n FROM lake.bronze.statements")" = "$DAYS" ]
[ "$(Q "SELECT count(*) AS n FROM lake.bronze.statements")" = "$ROWS" ]
[ "$(Q "SELECT count(*) AS n FROM lake.gold.reconciliation")" = "$REC" ]

step "quality: no ERROR-severity failures recorded"
FAILS=$(Q "SELECT count(*) AS n FROM lake.ops.quality_results WHERE status = 'FAIL'")
echo "failed error-checks: $FAILS"
echo "$FAILS" | grep -q '"n": 0'

echo; echo "AIRFLOW BACKFILL: PASSED"
