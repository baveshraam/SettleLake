"""Parquet/DuckDB helpers: atomic writes, content hashing, connections."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


def _tmp(path: Path) -> Path:
    return path.with_name(path.name + ".tmp")


def write_table(path: Path, table: pa.Table) -> None:
    """Write a Parquet file atomically (temp file + rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp(path)
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def write_query(con: duckdb.DuckDBPyConnection, path: Path, sql: str) -> None:
    """Materialize a SQL query to Parquet atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp(path)
    con.execute(f"COPY ({sql}) TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    os.replace(tmp, path)


def content_hash(con: duckdb.DuckDBPyConnection, path: Path) -> str:
    """Order-independent content hash of a Parquet file: row count + sum of per-row hashes."""
    n, h = con.execute(
        f"SELECT count(*), coalesce(sum(hash(t)::HUGEINT), 0) FROM read_parquet('{path.as_posix()}') t"
    ).fetchone()
    return f"{n}:{h}"


def quote_list(paths: list[Path]) -> str:
    return "[" + ", ".join(f"'{p.as_posix()}'" for p in paths) + "]"
