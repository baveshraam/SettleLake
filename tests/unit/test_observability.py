import json
import logging

from settlelake.config import get_paths
from settlelake.observability import lineage
from settlelake.observability.logs import JsonFormatter
from settlelake.observability.metrics import write_metrics
from settlelake.pipeline import run_pipeline
from settlelake.simulator.generator import GenConfig, generate
from settlelake.spark.landing import split_statements


def test_pipeline_emits_lineage_metrics_and_logs(tmp_path):
    generate(get_paths(tmp_path), GenConfig(200, 4, "low"))
    run_pipeline(tmp_path)
    events = [json.loads(x) for x in lineage.lineage_path(tmp_path).read_text().splitlines()]
    jobs = {(e["job"]["name"], e["eventType"]) for e in events}
    for stage in ("ingest", "silver", "reconcile", "gold", "quality"):
        assert (f"duckdb.{stage}", "START") in jobs and (f"duckdb.{stage}", "COMPLETE") in jobs
    silver = next(
        e for e in events if e["job"]["name"] == "duckdb.silver" and e["eventType"] == "START"
    )
    assert silver["inputs"][0]["name"] == "bronze/records" and len(silver["outputs"]) == 5
    assert silver["run"]["runId"].startswith("run_") and silver["schemaURL"].startswith(
        "https://openlineage"
    )
    prom = (tmp_path / "metrics" / "duckdb_pipeline.prom").read_text()
    assert 'settlelake_stage_duration_seconds{engine="duckdb",stage="quality"}' in prom
    assert "settlelake_last_update_timestamp_seconds" in prom


def test_failed_stage_is_recorded_in_lineage(tmp_path):
    import pytest

    from settlelake.pipeline import SimulatedInterruption

    generate(get_paths(tmp_path), GenConfig(100, 4, "clean"))
    with pytest.raises(SimulatedInterruption):
        run_pipeline(tmp_path, simulate_failure="gold")
    events = [json.loads(x) for x in lineage.lineage_path(tmp_path).read_text().splitlines()]
    assert ("duckdb.gold", "ABORT") in {(e["job"]["name"], e["eventType"]) for e in events}


def test_metrics_file_is_valid_prometheus_text(tmp_path):
    p = write_metrics("t", [("m_total", {"a": 'x"y'}, 3)], home=tmp_path)
    lines = p.read_text().splitlines()
    assert lines[0] == "# TYPE m_total gauge" and lines[1] == 'm_total{a="x\\"y"} 3'


def test_json_log_formatter_includes_extras():
    rec = logging.LogRecord("settlelake.x", logging.INFO, "f", 1, "hello %s", ("w",), None)
    rec.run_id = "r1"
    out = json.loads(JsonFormatter().format(rec))
    assert out["msg"] == "hello w" and out["run_id"] == "r1" and out["level"] == "INFO"


def test_landing_split_preserves_every_statement_row(tmp_path):
    p = get_paths(tmp_path)
    m = generate(p, GenConfig(300, 5, "stress"))
    counts = split_statements(p.raw, tmp_path / "landing")
    # every row (including malformed CSV lines) lands in exactly one daily partition
    assert (
        sum(counts.values())
        == sum(m["statement_rows"].values()) + m["malformed_lines"]["statements"]
    )
    assert all(k.startswith("2026-") for k in counts)
    assert len(list((tmp_path / "landing").glob("dt=*/statements_*"))) >= len(counts)
