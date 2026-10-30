"""Command line entry points for the Spark/Iceberg side of SettleLake.

python -m settlelake.spark batch   --input DIR [--landing-date YYYY-MM-DD] [--types event,statement]
python -m settlelake.spark stream  [--once] [--crash-once-flag PATH]
python -m settlelake.spark load    --input FILE [--start N] [--end N]      (events -> Postgres)
python -m settlelake.spark split-landing --raw DIR --landing DIR           (statements by day)
python -m settlelake.spark verify  --home DIR      (Spark gold == DuckDB gold on the same inputs)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

from settlelake.observability.logs import setup_logging


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise SystemExit(f"error: environment variable {name} is required")
    return v


def cmd_batch(a) -> int:
    from settlelake.spark.engine import run_batch
    from settlelake.spark.session import build_spark

    spark = build_spark("settlelake-batch")
    types = tuple(x for x in a.types.split(",") if x)
    res = run_batch(
        spark,
        None if a.no_ingest else Path(a.input),
        date.fromisoformat(a.landing_date) if a.landing_date else None,
        a.run_id,
        types,
    )
    print(json.dumps({k: v for k, v in res.items() if k != "detail"}, default=str))
    return 3 if res["quality_errors"] else 0


def cmd_ingest(a) -> int:
    from settlelake.spark.engine import ingest_files, init_tables
    from settlelake.spark.session import build_spark

    spark = build_spark("settlelake-ingest")
    init_tables(spark)
    types = tuple(x for x in a.types.split(",") if x)
    landing = date.fromisoformat(a.landing_date) if a.landing_date else None
    print(json.dumps(ingest_files(spark, Path(a.input), landing, types)))
    return 0


def cmd_stage(a) -> int:
    """Run one orchestrated step: `build` (silver+reconcile+gold) or `quality`."""
    from datetime import UTC, datetime

    from settlelake.spark.engine import (
        build_gold,
        build_silver,
        init_tables,
        reconcile,
        run_quality,
    )
    from settlelake.spark.session import build_spark

    spark = build_spark(f"settlelake-{a.name}")
    init_tables(spark)
    run_id = a.run_id or f"spark_{datetime.now(UTC):%Y%m%dT%H%M%S}"
    if a.name == "build":
        out = {
            "silver": build_silver(spark),
            "reconcile": reconcile(spark),
            "gold": build_gold(spark),
        }
        print(json.dumps(out))
        return 0
    results = run_quality(spark, run_id)
    errors = [r for r in results if r["status"] == "FAIL"]
    print(
        json.dumps(
            {"run_id": run_id, "checks": len(results), "errors": [e["check_name"] for e in errors]}
        )
    )
    return 3 if errors else 0


def cmd_sql(a) -> int:
    """Run an ad-hoc read-only query against the Iceberg catalog; prints one JSON row per line."""
    from settlelake.spark.session import build_spark

    if not a.query.lstrip().lower().startswith(("select", "show", "describe", "with")):
        raise SystemExit("error: only SELECT/SHOW/DESCRIBE/WITH queries are allowed")
    for row in build_spark("settlelake-sql").sql(a.query).collect():
        print(json.dumps(row.asDict(), default=str))
    return 0


def cmd_stream(a) -> int:
    from settlelake.spark.session import build_spark
    from settlelake.spark.stream import start_stream

    spark = build_spark("settlelake-stream")
    q = start_stream(
        spark,
        _env("KAFKA_BOOTSTRAP", "redpanda:9092"),
        _env("CDC_TOPIC", "payments.public.payment_events"),
        _env("CHECKPOINT_DIR", "/data/checkpoints/cdc_to_bronze"),
        once=a.once,
        interval=a.interval,
        crash_after_merge_flag=Path(a.crash_once_flag) if a.crash_once_flag else None,
    )
    q.awaitTermination()
    return 0


def cmd_load(a) -> int:
    import psycopg

    lines = [x for x in Path(a.input).read_text(encoding="utf-8").splitlines() if x.strip()]
    chunk = lines[a.start : a.end]
    dsn = _env("POSTGRES_DSN", "postgresql://settlelake:settlelake@postgres:5432/payments")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO payment_events (event_id, payload) VALUES (%s, %s)",
            [(_event_id(x), x) for x in chunk],
        )
    print(json.dumps({"loaded": len(chunk), "start": a.start, "end": a.end}))
    return 0


def _event_id(line: str) -> str | None:
    try:
        v = json.loads(line).get("event_id")
        return v if isinstance(v, str) else None
    except (ValueError, AttributeError):
        return None  # malformed lines are loaded verbatim; Silver quarantines them


def cmd_split_landing(a) -> int:
    from settlelake.spark.landing import split_statements

    print(json.dumps(split_statements(Path(a.raw), Path(a.landing))))
    return 0


def cmd_verify(a) -> int:
    from settlelake.spark.verify import verify_parity

    res = verify_parity(Path(a.home))
    print(json.dumps(res, default=str))
    return 0 if res["identical"] else 4


def cmd_assert_bronze(a) -> int:
    from settlelake.spark.verify import bronze_stats

    res = bronze_stats(a.expected)
    print(json.dumps(res))
    return 0 if res["ok"] else 4


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    ap = argparse.ArgumentParser(prog="settlelake-spark")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("batch")
    b.add_argument("--input")
    b.add_argument("--no-ingest", action="store_true")
    b.add_argument("--landing-date")
    b.add_argument("--types", default="event,statement")
    b.add_argument("--run-id")
    b.set_defaults(fn=cmd_batch)
    ig = sub.add_parser("ingest", help="land files into Bronze only (no downstream stages)")
    ig.add_argument("--input", required=True)
    ig.add_argument("--landing-date")
    ig.add_argument("--types", default="event,statement")
    ig.set_defaults(fn=cmd_ingest)
    st = sub.add_parser("stage", help="one orchestrated step for Airflow")
    st.add_argument("--name", choices=["build", "quality"], required=True)
    st.add_argument("--run-id")
    st.set_defaults(fn=cmd_stage)
    q = sub.add_parser(
        "sql", help="ad-hoc read-only query, e.g. SELECT count(*) FROM lake.gold.reconciliation"
    )
    q.add_argument("query")
    q.set_defaults(fn=cmd_sql)
    s = sub.add_parser("stream")
    s.add_argument("--once", action="store_true", help="process available data then stop")
    s.add_argument("--interval", default="5 seconds")
    s.add_argument("--crash-once-flag", help="failure injection: crash once after the first MERGE")
    s.set_defaults(fn=cmd_stream)
    ld = sub.add_parser("load")
    ld.add_argument("--input", required=True)
    ld.add_argument("--start", type=int, default=0)
    ld.add_argument("--end", type=int, default=None)
    ld.set_defaults(fn=cmd_load)
    sp = sub.add_parser("split-landing")
    sp.add_argument("--raw", required=True)
    sp.add_argument("--landing", required=True)
    sp.set_defaults(fn=cmd_split_landing)
    v = sub.add_parser("verify")
    v.add_argument("--home", required=True)
    v.set_defaults(fn=cmd_verify)
    ab = sub.add_parser("assert-bronze")
    ab.add_argument("--expected", type=int, required=True)
    ab.set_defaults(fn=cmd_assert_bronze)
    args = ap.parse_args(argv)
    if args.cmd == "batch" and not (args.input or args.no_ingest):
        ap.error("batch requires --input or --no-ingest")
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
