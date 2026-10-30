"""Run, stage and quality-result tracking in a small DuckDB metadata database."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb

DDL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY, kind VARCHAR, parent_run_id VARCHAR, status VARCHAR,
    started_at TIMESTAMP, finished_at TIMESTAMP, duration_s DOUBLE,
    input_fingerprint VARCHAR, inputs_json VARCHAR, error VARCHAR, replay_result VARCHAR);
CREATE TABLE IF NOT EXISTS stage_runs (
    run_id VARCHAR, stage VARCHAR, status VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
    duration_s DOUBLE, rows_in BIGINT, rows_out BIGINT, detail_json VARCHAR,
    outputs_json VARCHAR, error VARCHAR, PRIMARY KEY (run_id, stage));
CREATE TABLE IF NOT EXISTS quality_results (
    run_id VARCHAR, check_name VARCHAR, layer VARCHAR, severity VARCHAR, status VARCHAR,
    evaluated_count BIGINT, failed_count BIGINT, duration_ms DOUBLE, explanation VARCHAR,
    executed_at TIMESTAMP);
"""


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def new_run_id() -> str:
    return f"run_{_now():%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


class Tracker:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path))
        self.con.execute(DDL)

    def close(self) -> None:
        self.con.close()

    def start_run(self, run_id: str, kind: str, inputs: list[dict], fingerprint: str,
                  parent: str | None = None) -> None:  # fmt: skip
        self.con.execute(
            "INSERT INTO runs VALUES (?, ?, ?, 'RUNNING', ?, NULL, NULL, ?, ?, NULL, NULL)",
            [run_id, kind, parent, _now(), fingerprint, json.dumps(inputs)],
        )

    def finish_run(self, run_id: str, status: str, error: str | None = None,
                   replay_result: str | None = None) -> None:  # fmt: skip
        self.con.execute(
            "UPDATE runs SET status=?, finished_at=?, error=?, replay_result=?, "
            "duration_s = epoch(? - started_at) WHERE run_id=?",
            [status, _now(), error, replay_result, _now(), run_id],
        )

    def get_run(self, run_id: str) -> dict | None:
        cur = self.con.execute("SELECT * FROM runs WHERE run_id=?", [run_id])
        row = cur.fetchone()
        if row is None:
            return None
        run = dict(zip([d[0] for d in cur.description], row, strict=True))
        run["inputs"] = json.loads(run.pop("inputs_json"))
        return run

    def latest_run(self, kind: str = "pipeline", status: str | None = None) -> dict | None:
        q = "SELECT run_id FROM runs WHERE kind=?" + (" AND status=?" if status else "")
        q += " ORDER BY started_at DESC, run_id DESC LIMIT 1"
        row = self.con.execute(q, [kind, status] if status else [kind]).fetchone()
        return self.get_run(row[0]) if row else None

    def start_stage(self, run_id: str, stage: str) -> None:
        self.con.execute("DELETE FROM stage_runs WHERE run_id=? AND stage=?", [run_id, stage])
        self.con.execute(
            "INSERT INTO stage_runs VALUES (?, ?, 'RUNNING', ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            [run_id, stage, _now()],
        )

    def finish_stage(self, run_id: str, stage: str, status: str, duration_s: float,
                     rows_in: int | None = None, rows_out: int | None = None,
                     detail: dict | None = None, outputs: dict | None = None,
                     error: str | None = None) -> None:  # fmt: skip
        self.con.execute(
            "UPDATE stage_runs SET status=?, finished_at=?, duration_s=?, rows_in=?, rows_out=?, "
            "detail_json=?, outputs_json=?, error=? WHERE run_id=? AND stage=?",
            [status, _now(), duration_s, rows_in, rows_out, json.dumps(detail or {}),
             json.dumps(outputs or {}), error, run_id, stage],
        )  # fmt: skip

    def stages(self, run_id: str) -> dict[str, dict]:
        cur = self.con.execute(
            "SELECT * FROM stage_runs WHERE run_id=? ORDER BY started_at", [run_id]
        )
        cols = [d[0] for d in cur.description]
        out = {}
        for row in cur.fetchall():
            d = dict(zip(cols, row, strict=True))
            d["detail"] = json.loads(d.pop("detail_json") or "{}")
            d["outputs"] = json.loads(d.pop("outputs_json") or "{}")
            out[d["stage"]] = d
        return out

    def save_quality(self, run_id: str, results: list[dict]) -> None:
        self.con.execute("DELETE FROM quality_results WHERE run_id=?", [run_id])
        now = _now()
        self.con.executemany(
            "INSERT INTO quality_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [[run_id, r["check_name"], r["layer"], r["severity"], r["status"], r["evaluated_count"],
              r["failed_count"], r["duration_ms"], r["explanation"], now] for r in results],
        )  # fmt: skip

    def quality(self, run_id: str) -> list[dict]:
        cur = self.con.execute(
            "SELECT check_name, layer, severity, status, evaluated_count, failed_count, "
            "duration_ms, explanation FROM quality_results WHERE run_id=? ORDER BY check_name",
            [run_id],
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
