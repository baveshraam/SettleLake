"""Reconciliation stage: runs the deterministic matching SQL over Silver and writes Gold."""

from __future__ import annotations

from functools import cache
from importlib import resources
from pathlib import Path

from settlelake.config import (
    DELAY_SLA_HOURS,
    FALLBACK_AUTO,
    FALLBACK_MAX_EDIT,
    FALLBACK_WINDOW_HOURS,
    PRECEDENCE,
)
from settlelake.lakehouse.store import connect, write_query


@cache
def load_sql(name: str) -> str:
    return resources.files("settlelake").joinpath("sql", name).read_text(encoding="utf-8")


def reconcile(
    silver_dir: Path,
    gold_dir: Path,
    sla_hours: int = DELAY_SLA_HOURS,
    fb_max_edit: int = FALLBACK_MAX_EDIT,
    fb_window_hours: int = FALLBACK_WINDOW_HOURS,
    fb_auto: int = FALLBACK_AUTO,
) -> dict:
    """Write gold/reconciliation.parquet. Pure function of the Silver files."""
    sql = load_sql("reconcile.sql").format(
        silver=silver_dir.as_posix(),
        sla_seconds=sla_hours * 3600,
        fb_max_edit=fb_max_edit,
        fb_auto=fb_auto,
        fb_window_seconds=fb_window_hours * 3600,
        precedence="[" + ", ".join(f"'{c}'" for c in PRECEDENCE) + "]",
    )
    con = connect()
    target = gold_dir / "reconciliation.parquet"
    write_query(con, target, sql)
    rows, matched = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE classification = 'MATCHED') "
        f"FROM read_parquet('{target.as_posix()}')"
    ).fetchone()
    con.close()
    return {"reconciled_keys": rows, "matched": matched}
