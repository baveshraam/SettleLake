import pytest

from settlelake.observability.reliability import run_reliability
from settlelake.observability.suite import run_suite
from settlelake.pipeline import STAGES


@pytest.fixture(scope="module")
def rel(tmp_path_factory):
    return run_reliability(tmp_path_factory.mktemp("rel"), payments=600, seed=3)


def test_every_stage_can_be_crashed_and_resumed_to_identical_outputs(rel):
    got = {x["interrupted_stage"]: x for x in rel["failure_injection_recovery"]}
    assert list(got) == STAGES
    for stage, x in got.items():
        assert x["status"] == "COMPLETED" and x["outputs_identical_to_clean_run"], stage
        assert x["quality_errors"] == 0 and x["recovery_seconds"] > 0


def test_reliability_metrics_are_consistent_with_each_other(rel):
    c = rel["source_record_completeness"]
    assert c["bronze_records"] == c["source_records"] and c["bronze_completeness"] == 1.0
    d = rel["duplicate_delivery"]
    assert d["suppressed_in_silver"] == d["injected_duplicate_deliveries"]
    assert d["duplicate_event_ids_remaining_in_silver"] == 0
    assert rel["late_events"]["incorporation_rate"] == 1.0
    assert rel["replay"]["result"] == "CONSISTENT"
    assert rel["financial_aggregate_consistency"]["quality_error_checks_failed"] == 0
    assert all(v == "COMPLETED" for v in rel["pipeline_stage_status"].values())
    for cur, v in rel["unresolved_exposure_by_currency_minor_units"].items():
        assert len(cur) == 3 and v["amount_minor"] >= 0  # per-currency, integer


def test_suite_uses_isolated_directories_and_reports_medians(tmp_path):
    a = run_suite(tmp_path, [400], {400: 2}, {400: 0}, seed=1)
    b = run_suite(tmp_path, [400], {400: 2}, {400: 0}, seed=1)  # same home, same parameters
    assert a["run_id"] != b["run_id"]
    d = a["sizes"]["400"]["duckdb"]
    assert d["wall_seconds"]["n"] == 2 and d["wall_seconds"]["median"] > 0
    assert (
        d["payments_per_second"] > 0 and d["ingestion_lag"]["p99_s"] >= d["ingestion_lag"]["p50_s"]
    )
    assert (
        d["reconciliation_rows"] == b["sizes"]["400"]["duckdb"]["reconciliation_rows"]
    )  # deterministic
    assert (tmp_path / "reports" / f"{a['run_id']}.md").exists()
    assert not list((tmp_path / "benchmarks").glob("suite-*"))  # scratch data cleaned up
