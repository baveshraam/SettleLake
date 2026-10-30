import json

from settlelake.config import get_paths
from settlelake.simulator.generator import GenConfig, generate


def test_same_seed_same_bytes(tmp_path):
    a = generate(get_paths(tmp_path / "a"), GenConfig(300, 7, "stress"))
    b = generate(get_paths(tmp_path / "b"), GenConfig(300, 7, "stress"))
    assert a["files"] == b["files"]
    gt = "ground_truth/ground_truth.jsonl"
    assert (tmp_path / "a" / gt).read_bytes() == (tmp_path / "b" / gt).read_bytes()


def test_different_seed_differs(tmp_path):
    a = generate(get_paths(tmp_path / "a"), GenConfig(300, 1, "low"))
    b = generate(get_paths(tmp_path / "b"), GenConfig(300, 2, "low"))
    assert a["files"] != b["files"]


def test_clean_has_no_anomalies(tmp_path):
    m = generate(get_paths(tmp_path), GenConfig(300, 1, "clean"))
    assert set(m["scenario_counts"]) <= {"CLEAN"}
    assert m["malformed_lines"] == {"events": 0, "statements": 0}
    lines = (tmp_path / "ground_truth/ground_truth.jsonl").read_text().splitlines()
    assert all(json.loads(x)["expected_classes"] == ["MATCHED"] for x in lines)


def test_stress_covers_all_scenarios(quick_env):
    _, _, m, _ = quick_env
    for s in [
        "AMOUNT_MISMATCH",
        "CURRENCY_MISMATCH",
        "STATUS_MISMATCH",
        "DELAYED",
        "MISSING_PARTNER",
        "DUP_STATEMENT",
        "DUP_EVENT",
        "INVALID_EVENT",
        "INVALID_STATEMENT",
        "CONFLICT_EVENT",
        "AMBIGUOUS",
        "LATE_EVENT",
        "GHOST",
        "AMOUNT_AND_DELAYED",
    ]:
        assert m["scenario_counts"].get(s, 0) > 0, s
    assert m["malformed_lines"]["events"] > 0 and m["out_of_order_swaps"] > 0


def test_ground_truth_outside_raw(tmp_path):
    p = get_paths(tmp_path)
    generate(p, GenConfig(100, 1, "low"))
    assert not any("truth" in f.name for f in p.raw.iterdir())
