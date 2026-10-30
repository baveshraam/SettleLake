import json
from pathlib import Path

import duckdb
import pytest

from settlelake.config import get_paths
from settlelake.pipeline import run_pipeline
from settlelake.simulator.generator import GenConfig, generate


def settle_event(
    eid,
    ref,
    amount=10_000,
    cur="USD",
    provider="ATLAS",
    t="2026-01-05T10:00:00Z",
    ingested="2026-01-05T10:00:30Z",
    pid=None,
    **over,
):
    d = {
        "event_id": eid,
        "event_type": "funds.settled",
        "payment_id": pid or f"PAY-{eid}",
        "event_time": t,
        "ingested_at": ingested,
        "schema_version": "1.0",
        "source": "test",
        "provider": provider,
        "provider_ref": ref,
        "amount_minor": amount,
        "currency": cur,
        "corridor": "USD-INR",
        "dest_currency": None,
        "dest_amount_minor": None,
    }
    d.update(over)
    return d


def stmt_row(
    sid,
    ref,
    amount="100.00",
    cur="USD",
    status="SETTLED",
    provider="ATLAS",
    at="2026-01-05T12:00:00Z",
    **over,
):
    d = {
        "statement_row_id": sid,
        "provider": provider,
        "external_ref": ref,
        "amount": amount,
        "currency": cur,
        "status": status,
        "settled_at": at,
    }
    d.update(over)
    return d


def write_inputs(home: Path, events: list, stmts: list, raw_event_lines=()):
    raw = get_paths(home).raw
    raw.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(e) for e in events] + list(raw_event_lines)
    (raw / "payment_events.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    body = ",\n".join(json.dumps(r, sort_keys=True, separators=(",", ":")) for r in stmts)
    (raw / "statements_ATLAS.json").write_text(
        '{"provider":"ATLAS","rows":[\n' + body + "\n]}\n", encoding="utf-8"
    )


@pytest.fixture
def mini(tmp_path):
    """Run the pipeline on hand-written inputs; return ({match_key: (class, reasons)}, result)."""

    def _run(events, stmts, raw_event_lines=()):
        write_inputs(tmp_path, events, stmts, raw_event_lines)
        res = run_pipeline(tmp_path)
        p = get_paths(tmp_path)
        con = duckdb.connect()
        rows = con.execute(
            "SELECT match_key, classification, reasons FROM "
            f"read_parquet('{p.gold.as_posix()}/reconciliation.parquet')"
        ).fetchall()
        return {k: (c, list(r)) for k, c, r in rows}, res

    _run.home = tmp_path
    return _run


@pytest.fixture(scope="session")
def quick_env(tmp_path_factory):
    home = tmp_path_factory.mktemp("quick")
    paths = get_paths(home)
    manifest = generate(paths, GenConfig(1000, 42, "stress"))
    res = run_pipeline(home)
    return home, paths, manifest, res
