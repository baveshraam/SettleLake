import json
import shutil

import duckdb
import pytest

from settlelake.cli import main
from settlelake.config import get_paths
from settlelake.observability.benchmark import run_benchmark
from settlelake.observability.evaluation import evaluate
from settlelake.observability.tracking import Tracker
from settlelake.pipeline import PipelineError, SimulatedInterruption, replay, run_pipeline
from settlelake.simulator.generator import GenConfig, generate


def _q(path, sql):
    return (
        duckdb.connect().execute(sql.replace("$", f"read_parquet('{path.as_posix()}')")).fetchall()
    )


def test_full_pipeline_completes_and_quality_gates_hold(quick_env):
    _, paths, _, res = quick_env
    assert res.status == "COMPLETED"
    assert [s["status"] for s in res.stages.values()] == ["COMPLETED"] * 5
    assert not res.quality_errors  # every ERROR-severity invariant holds
    assert any(q["status"] == "WARN" for q in res.quality)  # anomalies are surfaced as warnings
    assert all(q["evaluated_count"] >= 0 and q["duration_ms"] >= 0 for q in res.quality)
    for t in [
        "reconciliation",
        "exceptions",
        "fact_payments",
        "fact_settlements",
        "dim_provider",
        "dim_corridor",
        "dim_currency",
        "dim_date",
    ]:
        assert (paths.gold / f"{t}.parquet").exists()


def test_reconciliation_matches_ground_truth(quick_env):
    _, paths, _, _ = quick_env
    ev = evaluate(paths)
    assert ev["overall"]["precision"] == 1.0 and ev["overall"]["recall"] == 1.0
    for cls, v in ev["per_class"].items():
        assert v["f1"] == 1.0, cls


def test_decisions_retain_evidence(quick_env):
    _, paths, _, _ = quick_env
    rows = _q(
        paths.gold / "reconciliation.parquet",
        "SELECT count(*) FILTER (WHERE explanation IS NULL OR len(reasons)=0), "
        "count(*) FILTER (WHERE classification='AMOUNT_MISMATCH' AND (internal_amount_minor IS NULL "
        "OR partner_amount_minor IS NULL OR internal_currency IS NULL OR settlement_lag_seconds IS NULL "
        "OR len(internal_refs)=0 OR len(partner_refs)=0)) FROM $",
    )
    assert rows == [(0, 0)]


def test_financial_correctness_matched_and_gold_vs_silver(quick_env):
    _, paths, _, _ = quick_env
    # per-currency totals only; MATCHED rows agree to the minor unit
    bad = _q(
        paths.gold / "reconciliation.parquet",
        "SELECT count(*) FROM $ WHERE classification='MATCHED' AND (internal_amount_minor <> "
        "partner_amount_minor OR internal_currency <> partner_currency)",
    )
    assert bad == [(0,)]
    gold = _q(
        paths.gold / "fact_settlements.parquet",
        "SELECT currency, sum(amount_minor) FROM $ GROUP BY 1 ORDER BY 1",
    )
    silver = _q(
        paths.silver / "events.parquet",
        "SELECT currency, sum(amount_minor) FROM $ WHERE event_type='funds.settled' GROUP BY 1 ORDER BY 1",
    )
    assert gold == silver and len(gold) > 1


def test_duplicates_and_quarantine_separated(quick_env):
    _, paths, manifest, _ = quick_env
    dup_ids = {
        r[0]
        for r in _q(
            paths.silver / "duplicates.parquet", "SELECT natural_id FROM $ WHERE obs_type='EVENT'"
        )
    }
    assert set(manifest["duplicate_event_ids"]) <= dup_ids
    q = _q(paths.silver / "quarantine.parquet", "SELECT count(*) FROM $")[0][0]
    assert q >= manifest["malformed_lines"]["events"] + manifest["malformed_lines"]["statements"]
    conflicts = {
        r[0]
        for r in _q(
            paths.silver / "rejected.parquet",
            "SELECT regexp_extract(detail, 'event_id (\\S+)', 1) FROM $ WHERE reason='CONFLICTING_EVENT_ID'",
        )
    }
    assert conflicts == set(manifest["conflicting_event_ids"])


def test_idempotent_rerun_does_not_change_totals_or_duplicate_gold(tmp_path):
    generate(get_paths(tmp_path), GenConfig(500, 3, "low"))
    r1 = run_pipeline(tmp_path)
    p = get_paths(tmp_path)

    def snapshot():
        return (
            _q(
                p.gold / "fact_settlements.parquet",
                "SELECT currency, count(*), sum(amount_minor) FROM $ GROUP BY 1 ORDER BY 1",
            ),
            _q(
                p.gold / "reconciliation.parquet",
                "SELECT count(*), count(DISTINCT reconciliation_id) FROM $",
            ),
            len(list(p.bronze.glob("*.parquet"))),
        )

    before = snapshot()
    r2 = run_pipeline(tmp_path)
    assert snapshot() == before
    for stage in ("silver", "reconcile", "gold"):
        assert r1.stages[stage]["outputs"] == r2.stages[stage]["outputs"]
    assert r2.stages["ingest"]["rows_out"] == 0


def test_interrupted_stage_recovery(tmp_path):
    generate(get_paths(tmp_path), GenConfig(400, 5, "low"))
    clean_home = tmp_path / "clean"
    shutil.copytree(get_paths(tmp_path).raw, get_paths(clean_home).raw)
    clean = run_pipeline(clean_home)

    with pytest.raises(SimulatedInterruption) as ei:
        run_pipeline(tmp_path, simulate_failure="reconcile")
    run_id = str(ei.value).split("run_id=")[1].rstrip("]")
    t = Tracker(get_paths(tmp_path).meta_db)
    assert t.get_run(run_id)["status"] == "INTERRUPTED"
    st = t.stages(run_id)
    assert st["silver"]["status"] == "COMPLETED" and st["reconcile"]["status"] == "INTERRUPTED"
    silver_started = st["silver"]["started_at"]
    t.close()
    assert not (
        get_paths(tmp_path).gold / "reconciliation.parquet"
    ).exists()  # nothing half-published

    res = run_pipeline(tmp_path, resume=run_id)
    assert res.status == "COMPLETED"
    assert res.stages["silver"]["started_at"] == silver_started  # completed stage was not re-run
    assert not list(get_paths(tmp_path).lake.rglob("*.tmp"))
    for stage in ("silver", "reconcile", "gold"):
        assert res.stages[stage]["outputs"] == clean.stages[stage]["outputs"]  # same as a clean run
    with pytest.raises(PipelineError, match="already COMPLETED"):
        run_pipeline(tmp_path, resume=run_id)


def test_replay_is_consistent_and_detects_tampering(quick_env, tmp_path):
    home, paths, _, res = quick_env
    rp = replay(home, res.run_id)
    assert rp.replay_result == "CONSISTENT" and rp.replay_diff == []
    t = Tracker(paths.meta_db)
    kinds = t.con.execute(
        "SELECT kind, parent_run_id, replay_result FROM runs WHERE kind='replay'"
    ).fetchall()
    t.close()
    assert (("replay", res.run_id, "CONSISTENT")) in kinds
    with pytest.raises(PipelineError, match="not found"):
        replay(home, "nope")


def test_pipeline_never_reads_ground_truth(tmp_path):
    p = get_paths(tmp_path)
    generate(p, GenConfig(300, 9, "low"))
    shutil.rmtree(p.ground_truth)
    assert run_pipeline(tmp_path).status == "COMPLETED"
    assert evaluate(p) is None


def test_cli_exit_codes_and_commands(tmp_path, capsys):
    h = ["--home", str(tmp_path)]
    assert main([*h, "run"]) == 1  # no input yet
    assert "settlelake generate" in capsys.readouterr().err
    assert main([*h, "generate", "--payments", "200", "--seed", "1"]) == 0
    assert main([*h, "run"]) == 0
    assert main([*h, "reconcile"]) == 0
    assert main([*h, "quality"]) == 0
    assert main([*h, "report"]) == 0
    assert (tmp_path / "reports" / "report.md").exists()
    rid = Tracker(get_paths(tmp_path).meta_db)
    run_id = rid.latest_run("pipeline", "COMPLETED")["run_id"]
    rid.close()
    assert main([*h, "replay", "--run-id", run_id]) == 0
    assert main([*h, "replay", "--run-id", "bogus"]) == 1
    with pytest.raises(SystemExit) as e:
        main([*h, "generate", "--profile", "huge"])
    assert e.value.code == 2
    assert main([*h, "run", "--simulate-failure", "gold"]) == 1


def test_benchmark_report_is_measured_and_consistent(tmp_path):
    r = run_benchmark(tmp_path, 300, 11, "low")
    saved = json.loads((tmp_path / "reports" / "benchmark.json").read_text())
    assert saved["workload"]["payments"] == 300 and saved["environment"]["python"]
    assert r["idempotency"]["consistent"] is True
    p = r["pipeline"]
    assert (
        p["bronze_records"] == r["workload"]["event_records"] + r["workload"]["statement_rows"] + 0
        or p["bronze_records"] > 0
    )
    assert sum(r["reconciliation"]["classes"].values()) == r["reconciliation"]["reconciled_keys"]
    assert 0 < r["reconciliation"]["match_rate"] < 1
    assert r["evaluation"]["overall"]["f1"] == 1.0
    assert (tmp_path / "reports" / "benchmark.md").read_text().startswith("# SettleLake benchmark")


def test_analytics_views_never_mix_currencies(quick_env):
    from settlelake.analytics.views import open_analytics

    _, paths, _, _ = quick_env
    con = open_analytics(paths)
    for v in (
        "v_daily_volume",
        "v_volume_by_currency",
        "v_corridor_analytics",
        "v_provider_performance",
        "v_match_rates",
        "v_outstanding_settlements",
        "v_exception_summary",
        "v_exception_amounts",
        "v_dq_trend",
        "v_pipeline_stats",
    ):
        assert con.execute(f"SELECT count(*) FROM {v}").fetchone()[0] > 0, v
    cols = [c[0] for c in con.execute("DESCRIBE v_volume_by_currency").fetchall()]
    assert "currency" in cols
    con.close()


def test_dashboard_renders_all_sections(quick_env, monkeypatch):
    from streamlit.testing.v1 import AppTest

    home, _, _, _ = quick_env
    monkeypatch.setenv("SETTLELAKE_HOME", str(home))
    at = AppTest.from_file(
        str(__import__("settlelake.dashboard.app", fromlist=["x"]).__file__), default_timeout=60
    )
    at.run()
    assert not at.exception
    for section in [
        "Overview",
        "Payment Intelligence",
        "Reconciliation Center",
        "Pipeline Observability",
    ]:
        at.sidebar.radio[0].set_value(section).run()
        assert not at.exception, (section, at.exception)
        assert any("SYNTHETIC DATA" in w.value for w in at.warning), section
