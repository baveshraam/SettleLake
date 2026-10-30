"""OpenLineage-style run events (START/COMPLETE/FAIL/ABORT) appended to a JSONL file.

Each pipeline stage and Spark job step records which datasets it read and wrote, so lineage can be
reconstructed from `<home>/lineage/events.jsonl` or shipped to any OpenLineage-compatible backend.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

PRODUCER = "https://github.com/baveshraam/SettleLake"
SCHEMA = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

# stage -> (input datasets, output datasets); identical for the DuckDB and Spark engines
STAGE_DATASETS = {
    "ingest": (["raw/files"], ["bronze/records"]),
    "silver": (
        ["bronze/records"],
        [
            "silver/events",
            "silver/statements",
            "silver/duplicates",
            "silver/rejected",
            "silver/quarantine",
        ],
    ),
    "reconcile": (
        ["silver/events", "silver/statements", "silver/rejected", "silver/duplicates"],
        ["gold/reconciliation"],
    ),
    "gold": (
        ["silver/events", "silver/statements", "gold/reconciliation"],
        ["gold/exceptions", "gold/fact_payments", "gold/fact_settlements", "gold/dim_*"],
    ),
    "quality": (["silver/*", "gold/*"], ["ops/quality_results"]),
}


def lineage_path(home: Path | str | None = None) -> Path:
    base = Path(home or os.environ.get("SETTLELAKE_HOME") or "data")
    return base / "lineage" / "events.jsonl"


def emit(
    event_type: str,
    job: str,
    run_id: str,
    inputs: list[str],
    outputs: list[str],
    namespace: str = "settlelake",
    home: Path | str | None = None,
) -> dict:
    ev = {
        "eventType": event_type,
        "eventTime": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "run": {"runId": run_id},
        "job": {"namespace": namespace, "name": job},
        "inputs": [{"namespace": namespace, "name": n} for n in inputs],
        "outputs": [{"namespace": namespace, "name": n} for n in outputs],
        "producer": PRODUCER,
        "schemaURL": SCHEMA,
    }
    p = lineage_path(home)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(ev) + "\n")
    return ev
