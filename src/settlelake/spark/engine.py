"""Spark/Iceberg engine: Bronze ingest, Silver, reconciliation, Gold and quality on Iceberg tables.

Mirrors the DuckDB pipeline (same contracts via `parse_record`, same reconciliation rules in
`reconcile_spark.sql`); tests assert exact output parity. Silver/Gold are rebuilt with
`createOrReplace`, which is an atomic Iceberg snapshot commit, so readers never see partial output.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from settlelake.config import CURRENCIES, DELAY_SLA_HOURS, PRECEDENCE
from settlelake.ingestion.bronze import _records, discover_inputs
from settlelake.ingestion.parse import FIELDS, parse_record
from settlelake.observability import lineage, metrics
from settlelake.quality.checks import evaluate_checks
from settlelake.reconciliation.engine import load_sql
from settlelake.spark.session import CATALOG

log = logging.getLogger("settlelake.spark")
BRONZE_EVENTS = f"{CATALOG}.bronze.payment_events"  # CDC stream and event files
BRONZE_STATEMENTS = f"{CATALOG}.bronze.statements"  # partner statements, by landing_date
OPS_QUALITY = f"{CATALOG}.ops.quality_results"
OPS_RUNS = f"{CATALOG}.ops.job_runs"

_STR, _LONG, _TS = T.StringType(), T.LongType(), T.TimestampNTZType()
_FIELD_TYPES = {
    "event_time": _TS, "ingested_at": _TS, "settled_at": _TS,
    "amount_minor": _LONG, "dest_amount_minor": _LONG,
}  # fmt: skip
PARSED_SCHEMA = T.StructType(
    [
        T.StructField("bronze_record_id", _STR),
        T.StructField("record_type", _STR),
        T.StructField("source_file", _STR),
        T.StructField("line_no", _LONG),
        T.StructField("raw", _STR),
        *[T.StructField(f, _FIELD_TYPES.get(f, _STR)) for f in FIELDS],
    ]
)


def t(layer: str, name: str) -> str:
    return f"{CATALOG}.{layer}.{name}"


def init_tables(spark: SparkSession) -> None:
    for ns in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    for tbl in (BRONZE_EVENTS, BRONZE_STATEMENTS):
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {tbl} (
            bronze_record_id STRING, record_type STRING, source STRING, source_file STRING,
            source_format STRING, file_fingerprint STRING, line_no BIGINT, raw STRING,
            ingested_at TIMESTAMP_NTZ, landing_date DATE)
            USING iceberg PARTITIONED BY (landing_date)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS_QUALITY} (
        run_id STRING, check_name STRING, layer STRING, severity STRING, status STRING,
        evaluated_count BIGINT, failed_count BIGINT, duration_ms DOUBLE, explanation STRING,
        executed_at TIMESTAMP_NTZ) USING iceberg""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS_RUNS} (
        run_id STRING, job STRING, status STRING, started_at TIMESTAMP_NTZ,
        duration_s DOUBLE, detail STRING) USING iceberg""")


def merge_bronze(spark: SparkSession, df, table: str = BRONZE_EVENTS) -> None:
    """Idempotent append: rows whose bronze_record_id already exists are ignored."""
    df.createOrReplaceTempView("_bronze_incoming")
    spark.sql(f"""MERGE INTO {table} t USING (
        SELECT DISTINCT * FROM _bronze_incoming) s ON t.bronze_record_id = s.bronze_record_id
        WHEN NOT MATCHED THEN INSERT *""")


def ingest_files(
    spark: SparkSession,
    input_dir: Path,
    landing_date: date | None = None,
    record_types: tuple[str, ...] = ("event", "statement"),
) -> dict:
    """Land JSONL/CSV/JSON files into Bronze (driver-side read).

    Statements replace their landing_date partition atomically (safe, idempotent backfill:
    re-running a date with corrected files swaps that day's data in one Iceberg commit).
    Events are merged by record id (repeated files are no-ops).
    """
    landing = landing_date or datetime.now(UTC).date()
    now = datetime.now(UTC).replace(tzinfo=None)
    by_type: dict[str, list] = {"event": [], "statement": []}
    for f in discover_inputs(input_dir):
        if f.record_type not in record_types:
            continue
        for n, raw in _records(f):
            by_type[f.record_type].append(
                (f"{f.fingerprint[:16]}:{n}", f.record_type, "file", f.path.name, f.fmt,
                 f.fingerprint, n, raw, now, landing)
            )  # fmt: skip
    stats = {}
    schema = spark.table(BRONZE_EVENTS).schema
    if by_type["event"]:
        before = spark.table(BRONZE_EVENTS).count()
        merge_bronze(spark, spark.createDataFrame(by_type["event"], schema))
        stats["events_new"] = spark.table(BRONZE_EVENTS).count() - before
    if by_type["statement"]:
        before = spark.table(BRONZE_STATEMENTS).count()
        df = spark.createDataFrame(by_type["statement"], schema).dropDuplicates(
            ["bronze_record_id"]
        )
        df.writeTo(BRONZE_STATEMENTS).overwritePartitions()
        stats["statements_new"] = spark.table(BRONZE_STATEMENTS).count() - before
    stats["records_seen"] = len(by_type["event"]) + len(by_type["statement"])
    stats["records_new"] = stats.get("events_new", 0) + stats.get("statements_new", 0)
    return stats


def _parse_partition(rows):
    for r in rows:
        p = parse_record(r["record_type"], r["source_format"], r["raw"])
        yield (r["bronze_record_id"], r["record_type"], r["source_file"], r["line_no"], r["raw"],
               *[p[f] for f in FIELDS])  # fmt: skip


def build_silver(spark: SparkSession) -> dict:
    src = spark.table(BRONZE_EVENTS).unionByName(spark.table(BRONZE_STATEMENTS)).select(
        "bronze_record_id", "record_type", "source_format",
        F.coalesce("source_file", F.lit("cdc")).alias("source_file"), "line_no", "raw",
    )  # fmt: skip
    parsed = spark.createDataFrame(src.rdd.mapPartitions(_parse_partition), PARSED_SCHEMA)
    parsed.createOrReplaceTempView("_parsed")
    w = "PARTITION BY event_id ORDER BY source_file, line_no"
    spark.sql(f"""
        SELECT *, row_number() OVER ({w}) AS rn, count(*) OVER (PARTITION BY event_id) AS nver,
               size(collect_set(sig) OVER (PARTITION BY event_id)) AS nsig,
               first_value(bronze_record_id) OVER ({w}) AS keep_rid
        FROM _parsed WHERE outcome = 'event'""").createOrReplaceTempView("_ev")
    spark.sql("""
        SELECT *, row_number() OVER (PARTITION BY sig ORDER BY source_file, line_no) AS rn,
               first_value(bronze_record_id) OVER (
                   PARTITION BY sig ORDER BY source_file, line_no) AS keep_rid
        FROM _parsed WHERE outcome = 'statement'""").createOrReplaceTempView("_st")

    out = {
        "events": """SELECT event_id, event_type, payment_id, event_time, ingested_at, provider,
                provider_ref, match_key, amount_minor, currency, corridor, dest_currency,
                dest_amount_minor, schema_version, source, channel, bronze_record_id
            FROM _ev WHERE nsig = 1 AND rn = 1""",
        "statements": """SELECT statement_row_id, provider, external_ref, match_key, amount_minor,
                currency, status, settled_at, bronze_record_id FROM _st WHERE rn = 1""",
        "duplicates": """
            SELECT 'EVENT' AS obs_type, event_id AS natural_id, match_key,
                   bronze_record_id, keep_rid AS duplicate_of FROM _ev WHERE nsig = 1 AND rn > 1
            UNION ALL
            SELECT 'STATEMENT', statement_row_id, match_key, bronze_record_id, keep_rid
            FROM _st WHERE rn > 1""",
        "rejected": """
            SELECT bronze_record_id, record_type, reason, detail, match_key, side, raw
            FROM _parsed WHERE outcome = 'rejected'
            UNION ALL
            SELECT bronze_record_id, 'event', 'CONFLICTING_EVENT_ID',
                   concat('event_id ', event_id, ' seen with ', CAST(nver AS STRING),
                          ' differing versions'),
                   match_key, CASE WHEN match_key IS NOT NULL THEN 'INTERNAL' END, raw
            FROM _ev WHERE nsig > 1""",
        "quarantine": """SELECT bronze_record_id, record_type, reason, detail, raw
            FROM _parsed WHERE outcome = 'quarantine'""",
    }
    counts = {}
    for name, sql in out.items():
        df = spark.sql(sql)
        df.writeTo(t("silver", name)).createOrReplace()
        counts[name] = spark.table(t("silver", name)).count()
    counts["bronze_records"] = (
        spark.table(BRONZE_EVENTS).count() + spark.table(BRONZE_STATEMENTS).count()
    )
    return counts


def reconcile(spark: SparkSession, sla_hours: int = DELAY_SLA_HOURS) -> dict:
    sql = load_sql("reconcile_spark.sql").format(
        events=t("silver", "events"), statements=t("silver", "statements"),
        rejected=t("silver", "rejected"), duplicates=t("silver", "duplicates"),
        sla_seconds=sla_hours * 3600, precedence=", ".join(f"'{c}'" for c in PRECEDENCE),
    )  # fmt: skip
    spark.sql(sql).writeTo(t("gold", "reconciliation")).createOrReplace()
    r = spark.table(t("gold", "reconciliation"))
    return {"reconciled_keys": r.count(), "matched": r.filter("classification = 'MATCHED'").count()}


def build_gold(spark: SparkSession) -> dict:
    ev, rec = t("silver", "events"), t("gold", "reconciliation")
    fx = ", ".join(f"('{c}', {e})" for c, e in sorted(CURRENCIES.items()))
    queries = {
        "fact_payments": f"""
            SELECT payment_id, provider, corridor, source_currency, source_amount_minor,
                   dest_currency, dest_amount_minor, created_at, cleared_at, settled_at,
                   delivered_at, CAST(created_at AS DATE) AS payment_date,
                   CASE WHEN delivered_at IS NOT NULL THEN 'DELIVERED'
                        WHEN settled_at IS NOT NULL THEN 'SETTLED'
                        WHEN cleared_at IS NOT NULL THEN 'COMPLIANCE_CLEARED'
                        ELSE 'CREATED' END AS status
            FROM (SELECT payment_id, min(provider) AS provider, min(corridor) AS corridor,
                   min(amount_minor) FILTER (WHERE event_type = 'payment.created') AS source_amount_minor,
                   min(currency) FILTER (WHERE event_type = 'payment.created') AS source_currency,
                   min(dest_currency) FILTER (WHERE event_type = 'payment.created') AS dest_currency,
                   min(dest_amount_minor) FILTER (WHERE event_type = 'payment.created') AS dest_amount_minor,
                   min(event_time) FILTER (WHERE event_type = 'payment.created') AS created_at,
                   min(event_time) FILTER (WHERE event_type = 'compliance.cleared') AS cleared_at,
                   min(event_time) FILTER (WHERE event_type = 'funds.settled') AS settled_at,
                   min(event_time) FILTER (WHERE event_type = 'payout.delivered') AS delivered_at
                  FROM {ev} GROUP BY payment_id)
            WHERE created_at IS NOT NULL""",
        "fact_settlements": f"""
            SELECT e.event_id AS settlement_event_id, e.payment_id, e.provider, e.corridor,
                   e.currency, e.amount_minor, e.event_time AS settled_at, e.ingested_at,
                   timestampdiff(SECOND, e.event_time, e.ingested_at) AS ingestion_lag_seconds,
                   e.match_key, r.classification, r.reconciliation_id
            FROM {ev} e LEFT JOIN {rec} r ON e.match_key = r.match_key
            WHERE e.event_type = 'funds.settled'""",
        "exceptions": f"SELECT * FROM {rec} WHERE classification <> 'MATCHED'",
        "dim_provider": f"""
            SELECT provider, count(*) AS statement_rows_seen FROM
              (SELECT provider FROM {t("silver", "statements")} UNION ALL SELECT provider FROM {ev})
            GROUP BY provider""",
        "dim_corridor": f"""
            SELECT DISTINCT corridor, split_part(corridor, '-', 1) AS source_currency,
                   split_part(corridor, '-', 2) AS dest_currency FROM {ev}""",
        "dim_currency": f"SELECT * FROM (VALUES {fx}) AS c(currency, minor_unit_exponent)",
        "dim_date": f"""
            SELECT d AS date, year(d) AS year, month(d) AS month, dayofweek(d) AS dow FROM (
              SELECT explode(sequence(CAST(min(event_time) AS DATE), CAST(max(event_time) AS DATE),
                                      interval 1 day)) AS d FROM {ev})""",
    }
    counts = {}
    for name, sql in queries.items():
        spark.sql(sql).writeTo(t("gold", name)).createOrReplace()
        counts[name] = spark.table(t("gold", name)).count()
    return counts


def run_quality(spark: SparkSession, run_id: str) -> list[dict]:
    tables = {
        "events": t("silver", "events"), "statements": t("silver", "statements"),
        "rej": t("silver", "rejected"), "qua": t("silver", "quarantine"),
        "dup": t("silver", "duplicates"), "rec": t("gold", "reconciliation"),
        "fs": t("gold", "fact_settlements"), "fp": t("gold", "fact_payments"),
        "bronze": f"(SELECT 1 FROM {BRONZE_EVENTS} UNION ALL SELECT 1 FROM {BRONZE_STATEMENTS})",
    }  # fmt: skip
    results = evaluate_checks(lambda sql: tuple(spark.sql(sql).collect()[0]), tables)
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (run_id, r.check_name, r.layer, r.severity, r.status, r.evaluated_count, r.failed_count,
         float(r.duration_ms), r.explanation, now)
        for r in results
    ]  # fmt: skip
    schema = spark.table(OPS_QUALITY).schema
    spark.createDataFrame(rows, schema).writeTo(OPS_QUALITY).append()
    return [r.as_dict() for r in results]


def run_batch(
    spark: SparkSession,
    input_dir: Path | None,
    landing_date: date | None = None,
    run_id: str | None = None,
    record_types: tuple[str, ...] = ("event", "statement"),
) -> dict:
    """Ingest (optional) -> silver -> reconcile -> gold -> quality. Returns timings and counts."""
    run_id = run_id or f"spark_{datetime.now(UTC):%Y%m%dT%H%M%S}"
    init_tables(spark)
    timings, detail = {}, {}

    samples: list = []

    def step(name, fn):
        ins, outs = lineage.STAGE_DATASETS[name]
        lineage.emit("START", f"spark.{name}", run_id, ins, outs)
        t0 = time.perf_counter()
        try:
            detail[name] = fn()
        except Exception:
            lineage.emit("FAIL", f"spark.{name}", run_id, ins, outs)
            log.exception("spark step failed", extra={"run_id": run_id, "stage": name})
            raise
        timings[name] = round(time.perf_counter() - t0, 3)
        lineage.emit("COMPLETE", f"spark.{name}", run_id, ins, outs)
        log.info("spark step completed", extra={"run_id": run_id, "stage": name,
                                                "seconds": timings[name]})  # fmt: skip
        samples.append(("settlelake_stage_duration_seconds",
                        {"engine": "spark", "stage": name}, timings[name]))  # fmt: skip

    if input_dir:
        step("ingest", lambda: ingest_files(spark, input_dir, landing_date, record_types))
    step("silver", lambda: build_silver(spark))
    step("reconcile", lambda: reconcile(spark))
    step("gold", lambda: build_gold(spark))
    step("quality", lambda: run_quality(spark, run_id))
    errors = [q for q in detail["quality"] if q["status"] == "FAIL"]
    rec = detail["reconcile"]
    samples += [
        ("settlelake_reconciliation_match_rate", {"engine": "spark"},
         rec["matched"] / rec["reconciled_keys"] if rec["reconciled_keys"] else 0.0),
        ("settlelake_quality_error_checks_failed", {"engine": "spark"}, len(errors)),
    ]  # fmt: skip
    metrics.write_metrics("spark_batch", samples)
    return {"run_id": run_id, "timings": timings, "detail": detail, "quality_errors": errors}
