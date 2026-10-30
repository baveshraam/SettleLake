#!/usr/bin/env bash
# End-to-end proof of the streaming architecture on Docker Compose:
#   Postgres -> Debezium -> Redpanda -> Spark Structured Streaming -> Iceberg (S3-compatible store)
# then Spark batch silver/reconcile/gold and an exact-parity check against the DuckDB engine.
set -euo pipefail
cd "$(dirname "$0")/.."
C="docker compose --profile stream"
RUN="$C run --rm -T spark"
PAYMENTS="${PAYMENTS:-1000}"
TOPIC=payments.public.payment_events

landing_days() {  # one word per daily partition, tolerant of CR/LF differences
  $RUN sh -c 'ls /data/landing' | tr -s '[:space:]' ' ' | sed 's/dt=//g'
}
step() { echo; echo "=== $* ==="; }
topic_hw() {  # high watermark of the CDC topic
  $C exec -T redpanda rpk topic describe "$TOPIC" -p 2>/dev/null | awk 'NR>1 {s+=$6} END {print s+0}'
}
wait_topic() {
  for _ in $(seq 1 60); do
    [ "$(topic_hw)" -ge "$1" ] && return 0
    sleep 2
  done
  echo "CDC topic never reached $1 records (got $(topic_hw))" >&2; return 1
}

step "start infrastructure"
$C up -d --build --wait postgres redpanda s3 connect
$C build spark
$C run --rm -T s3-init
$C run --rm -T connector-setup

step "generate dataset and run the DuckDB reference pipeline"
$RUN settlelake generate --payments "$PAYMENTS" --seed 42 --anomalies low
$RUN settlelake run
TOTAL=$($RUN sh -c 'grep -c . /data/raw/payment_events.jsonl' | tr -d '\r')
HALF=$((TOTAL / 2))
echo "events: $TOTAL (wave 1: $HALF)"

step "wave 1: load events into Postgres, Debezium captures them"
$RUN python -m settlelake.spark load --input /data/raw/payment_events.jsonl --start 0 --end "$HALF"
wait_topic "$HALF"
$RUN python -m settlelake.spark stream --once
$RUN python -m settlelake.spark assert-bronze --expected "$HALF"

step "wave 2 with injected crash: MERGE commits, then the job dies before checkpointing offsets"
$RUN python -m settlelake.spark load --input /data/raw/payment_events.jsonl --start "$HALF"
wait_topic "$TOTAL"
if $RUN python -m settlelake.spark stream --once --crash-once-flag /data/checkpoints/crash.flag; then
  echo "expected the injected crash" >&2; exit 1
fi
echo "job crashed as injected; restarting from the checkpoint"
$RUN python -m settlelake.spark stream --once
$RUN python -m settlelake.spark assert-bronze --expected "$TOTAL"

step "partner statements: split into daily landing partitions, load each day (backfill-style)"
$RUN python -m settlelake.spark split-landing --raw /data/raw --landing /data/landing
for d in $(landing_days); do
  $RUN python -m settlelake.spark ingest --input "/data/landing/dt=$d" --landing-date "$d" --types statement >/dev/null
done

step "Spark batch: silver -> reconcile -> gold -> quality on Iceberg"
$RUN python -m settlelake.spark batch --no-ingest

step "parity: Iceberg gold from the streaming path equals the DuckDB reference"
$RUN python -m settlelake.spark verify --home /data

step "idempotency: re-ingesting every landing day and rebuilding changes nothing"
for d in $(landing_days); do
  $RUN python -m settlelake.spark ingest --input "/data/landing/dt=$d" --landing-date "$d" --types statement >/dev/null
done
$RUN python -m settlelake.spark stream --once
$RUN python -m settlelake.spark assert-bronze --expected "$TOTAL"
$RUN python -m settlelake.spark batch --no-ingest
$RUN python -m settlelake.spark verify --home /data

echo; echo "STREAMING END-TO-END: PASSED"
