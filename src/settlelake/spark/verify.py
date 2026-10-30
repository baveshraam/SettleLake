"""Cross-engine verification: Iceberg gold produced by Spark must equal the DuckDB reference."""

from __future__ import annotations

from pathlib import Path

from settlelake.config import get_paths
from settlelake.lakehouse.store import connect
from settlelake.spark.engine import t
from settlelake.spark.session import build_spark

COLS = (
    "match_key, classification, reasons, internal_amount_minor, internal_currency, "
    "partner_amount_minor, partner_currency, partner_status, settlement_lag_seconds, "
    "ingestion_lag_seconds, explanation, payment_id, corridor, internal_refs, partner_refs, "
    "match_method, fallback_candidates, review_candidate_key"
)


def _norm(rows):
    return [tuple(list(v) if isinstance(v, (list, tuple)) else v for v in r) for r in rows]


def compare_with_duckdb(spark, duck_gold: Path, table: str | None = None) -> dict:
    """Compare Iceberg gold.reconciliation (all COLS) with the DuckDB engine's Parquet output."""
    table = table or t("gold", "reconciliation")
    got = _norm(
        tuple(r) for r in spark.sql(f"SELECT {COLS} FROM {table} ORDER BY match_key").collect()
    )
    con = connect()
    ref = _norm(
        con.execute(
            f"SELECT {COLS} FROM read_parquet('{(duck_gold / 'reconciliation.parquet').as_posix()}') "
            "ORDER BY match_key"
        ).fetchall()
    )
    con.close()
    diff = [(a[0], b[0]) for a, b in zip(got, ref, strict=False) if a != b]
    return {
        "spark_rows": len(got),
        "duckdb_rows": len(ref),
        "differing_keys": diff[:10],
        "identical": got == ref and len(ref) > 0,
    }


def verify_parity(home: Path) -> dict:
    return compare_with_duckdb(build_spark("settlelake-verify"), get_paths(home).gold)


def bronze_stats(expected_events: int) -> dict:
    """CDC Bronze must hold every loaded event exactly once (no loss, no duplicate rows)."""
    from settlelake.spark.engine import BRONZE_EVENTS

    spark = build_spark("settlelake-verify")
    rows, distinct = spark.sql(
        f"SELECT count(*), count(DISTINCT bronze_record_id) FROM {BRONZE_EVENTS} WHERE source = 'cdc'"
    ).collect()[0]
    return {
        "expected": expected_events,
        "bronze_rows": rows,
        "distinct_ids": distinct,
        "ok": rows == distinct == expected_events,
    }
