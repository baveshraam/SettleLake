import shutil

import duckdb
import pytest
from helpers import settle_event as se
from helpers import stmt_row as sr

from settlelake.config import get_paths
from settlelake.observability import reconcile_eval as E
from settlelake.reconciliation.engine import reconcile


def _recon(home, **params):
    p = get_paths(home)
    out = home / "gold_x"
    reconcile(p.silver, out, **params)
    rows = (
        duckdb.connect()
        .execute(
            "SELECT match_key, classification, match_method, internal_refs, partner_refs, "
            f"fallback_candidates FROM read_parquet('{out.as_posix()}/reconciliation.parquet') ORDER BY 1"
        )
        .fetchall()
    )
    return {r[0]: r[1:] for r in rows}


def test_unique_fuzzy_candidate_needs_review_and_is_never_paired_by_default(mini):
    mini([se("e1", "ATL-REF-100")], [sr("s1", "ATL-REF-101")])
    out = _recon(mini.home)  # frozen policy: suggest only
    a, b = out["ATLAS|ATLREF100"], out["ATLAS|ATLREF101"]
    assert a[0] == b[0] == "REVIEW_REQUIRED"  # one unique candidate: review, NOT ambiguous
    assert a[2] == ["e1"] and a[3] == [] and b[2] == [] and b[3] == ["s1"]  # records stay unpaired


def test_fallback_auto_mode_matches_unique_pair_and_labels_method(mini):
    mini([se("e1", "ATL-REF-100")], [sr("s1", "ATL-REF-101")])
    out = _recon(mini.home, fb_auto=1)
    cls, method, irefs, prefs, _ = out["ATLAS|ATLREF100"]
    assert (cls, method, irefs, prefs) == ("MATCHED", "FALLBACK", ["e1"], ["s1"])
    assert "ATLAS|ATLREF101" not in out  # the partner row was re-keyed onto the internal key


def test_fallback_never_forces_non_unique_candidates(mini):
    mini(
        [se("e1", "ATL-REF-100"), se("e2", "ATL-REF-102")],
        [sr("s1", "ATL-REF-101"), sr("s2", "ATL-REF-103")],
    )
    for auto in (0, 1):
        out = _recon(mini.home, fb_auto=auto)
        assert {v[0] for v in out.values()} == {"AMBIGUOUS_MATCH"}  # competing candidates
        assert all(v[1] == "KEY" for v in out.values())


@pytest.mark.parametrize(
    "kw",
    [{"amount": "100.01"}, {"cur": "EUR"}, {"provider": "NORDPAY"}, {"at": "2026-01-09T10:00:00Z"}],
)
def test_fallback_requires_equal_provider_currency_amount_and_time_window(mini, kw):
    mini([se("e1", "ATL-REF-100")], [sr("s1", "ATL-REF-101", **kw)])
    out = _recon(mini.home, fb_auto=1)
    assert {v[0] for v in out.values()} == {"MISSING_PARTNER_RECORD", "MISSING_INTERNAL_RECORD"}


def test_two_edit_distance_is_not_matched(mini):
    mini([se("e1", "ATL-REF-100")], [sr("s1", "ATL-REF-111")])
    assert {v[0] for v in _recon(mini.home, fb_auto=1).values()} == {
        "MISSING_PARTNER_RECORD",
        "MISSING_INTERNAL_RECORD",
    }


def test_fallback_disabled_with_edit_zero(mini):
    mini([se("e1", "ATL-REF-100")], [sr("s1", "ATL-REF-101")])
    out = _recon(mini.home, fb_max_edit=0, fb_auto=1)
    assert {v[0] for v in out.values()} == {"MISSING_PARTNER_RECORD", "MISSING_INTERNAL_RECORD"}


@pytest.fixture(scope="module")
def prepared(tmp_path_factory):
    home = tmp_path_factory.mktemp("evalhome")
    return E.prepare(home, "dev", [E.DEV_SEEDS[0]], 1500)


def test_eval_is_independent_of_pipeline_and_abstains_on_collisions(prepared):
    res = E.evaluate_params(prepared)
    a, amb = res["automatic_matching"], res["ambiguity"]
    assert amb["correct_abstention_rate"] == 1.0 and amb["wrongly_auto_matched"] == 0
    assert (
        res["per_class"]["AMBIGUOUS_MATCH"]["precision"] == 1.0
    )  # review no longer pollutes ambiguity
    # the only false automatic matches come from the two undetectable-by-design scenarios
    bad = {k for k, v in res["per_scenario"].items() if v.get("false_auto_matches")}
    assert bad <= {"WRONG_TARGET"}
    assert a["precision"] > 0.95 and res["duplicates"]["duplicate_event_recall"] == 1.0
    for cur, e in res["exposure_by_currency"].items():  # currencies are never merged
        assert len(cur) == 3 and e["truth_unresolved"] >= e["truth_unresolved_flagged"]


def test_auto_mode_trades_precision_for_coverage_on_dev_data(prepared):
    off = E.evaluate_params(prepared, {"fb_max_edit": 0, "fb_window_hours": 24, "fb_auto": 1})
    on = E.evaluate_params(prepared, {"fb_max_edit": 1, "fb_window_hours": 24, "fb_auto": 1})
    assert (
        on["automatic_matching"]["recall_of_all_true_pairs"]
        > off["automatic_matching"]["recall_of_all_true_pairs"]
    )
    assert (
        on["automatic_matching"]["false_automatic_matches"]
        > off["automatic_matching"]["false_automatic_matches"]
    )


def test_pipeline_result_does_not_depend_on_ground_truth_files(prepared):
    p = prepared[0]["paths"]
    before = E.load_rows(p.gold)
    shutil.copytree(p.ground_truth, p.home / "truth_backup")
    shutil.rmtree(p.ground_truth)
    reconcile(p.silver, p.home / "gold_nogt")
    after = E.load_rows(p.home / "gold_nogt")
    assert sorted(map(str, before)) == sorted(map(str, after))
