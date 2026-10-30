"""Structured Streaming: Debezium CDC events (Kafka/Redpanda) -> Iceberg Bronze.

Delivery is at-least-once (Kafka offsets are checkpointed after each micro-batch commits); the sink
is an Iceberg MERGE keyed by the CDC row sequence, so replaying a micro-batch after a crash is a
no-op. Net effect is effectively-once Bronze, which is what the recovery test demonstrates.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from settlelake.spark.engine import BRONZE_EVENTS, OPS_RUNS, init_tables, merge_bronze

log = logging.getLogger("settlelake.stream")

# Debezium envelope (JSON converter, schemas disabled) for public.payment_events
ENVELOPE = T.StructType(
    [
        T.StructField("op", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
        T.StructField(
            "after",
            T.StructType(
                [
                    T.StructField("seq", T.LongType()),
                    T.StructField("event_id", T.StringType()),
                    T.StructField("payload", T.StringType()),
                ]
            ),
        ),
    ]
)


class SimulatedCrash(RuntimeError):
    pass


def to_bronze_rows(batch_df):
    """Kafka records -> Bronze rows. Snapshot (`r`) and insert (`c`) ops carry events; the rest
    (updates/deletes/tombstones) are ignored because payment events are immutable."""
    m = F.from_json(F.col("value").cast("string"), ENVELOPE)
    return (
        batch_df.select(m.alias("m"), "topic")
        .where(F.col("m.op").isin("c", "r") & F.col("m.after.seq").isNotNull())
        .select(
            F.concat(F.lit("cdc:"), F.col("m.after.seq").cast("string")).alias("bronze_record_id"),
            F.lit("event").alias("record_type"),
            F.lit("cdc").alias("source"),
            F.concat(F.lit("cdc:"), F.col("topic")).alias("source_file"),
            F.lit("jsonl").alias("source_format"),
            F.lit(None).cast("string").alias("file_fingerprint"),
            F.col("m.after.seq").alias("line_no"),
            F.col("m.after.payload").alias("raw"),
            F.current_timestamp().cast("timestamp_ntz").alias("ingested_at"),
            F.to_date(F.timestamp_seconds(F.col("m.ts_ms") / 1000)).alias("landing_date"),
        )
    )


def start_stream(
    spark: SparkSession,
    bootstrap: str,
    topic: str,
    checkpoint: str,
    once: bool = False,
    interval: str = "5 seconds",
    crash_after_merge_flag: Path | None = None,
):
    """Start the CDC -> Bronze query. `crash_after_merge_flag`: if set and the file does not exist,
    create it and raise right after the first MERGE commits (before the offset checkpoint)."""
    init_tables(spark)

    def process(batch_df, batch_id: int) -> None:
        t0 = time.perf_counter()
        rows = to_bronze_rows(batch_df)
        before = spark.table(BRONZE_EVENTS).count()
        merge_bronze(batch_df.sparkSession, rows)
        added = spark.table(BRONZE_EVENTS).count() - before
        n_in = batch_df.count()
        detail = json.dumps({"batch_id": batch_id, "kafka_records": n_in, "new_bronze_rows": added})
        spark.createDataFrame(
            [("stream", "cdc_to_bronze", "COMPLETED", datetime.now(UTC).replace(tzinfo=None),
              time.perf_counter() - t0, detail)],
            spark.table(OPS_RUNS).schema,
        ).writeTo(OPS_RUNS).append()  # fmt: skip
        log.info(
            "micro-batch committed", extra={"batch_id": batch_id, "records": n_in, "new": added}
        )
        if crash_after_merge_flag is not None and not crash_after_merge_flag.exists() and n_in:
            crash_after_merge_flag.write_text("crashed once")
            raise SimulatedCrash(f"simulated crash after MERGE of batch {batch_id}")

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    writer = (
        reader.writeStream.foreachBatch(process)
        .option("checkpointLocation", checkpoint)
        .queryName("cdc_to_bronze")
    )
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=interval)
    return writer.start()
