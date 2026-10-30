"""Open a DuckDB connection exposing Gold tables, pipeline metadata and analytical views."""

from __future__ import annotations

import duckdb

from settlelake.config import Paths
from settlelake.lakehouse.gold import GOLD_TABLES
from settlelake.lakehouse.store import connect
from settlelake.observability.tracking import DDL
from settlelake.reconciliation.engine import load_sql


def open_analytics(paths: Paths) -> duckdb.DuckDBPyConnection:
    """Return an in-memory connection with Gold parquet files as views, `meta` attached
    read-only (or an empty in-memory `meta` before the first run) and the v_* views created."""
    con = connect()
    for name in GOLD_TABLES:
        f = paths.gold / f"{name}.parquet"
        if f.exists():
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{f.as_posix()}')")
    if paths.meta_db.exists():
        con.execute(f"ATTACH '{paths.meta_db.as_posix()}' AS meta (READ_ONLY)")
    else:
        con.execute("ATTACH ':memory:' AS meta")
        con.execute("USE meta")
        con.execute(DDL)
        con.execute("USE memory")
    have_gold = (paths.gold / "reconciliation.parquet").exists() and (
        paths.gold / "fact_payments.parquet"
    ).exists()
    if have_gold:
        con.execute(load_sql("analytics.sql"))
    return con
