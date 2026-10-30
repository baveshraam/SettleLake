"""Every number in an evaluation report must be recomputable from the report's own confusion matrix."""

import re

import pytest

from settlelake.observability import reconcile_eval as E


@pytest.fixture(scope="module")
def res(tmp_path_factory):
    prepared = E.prepare(tmp_path_factory.mktemp("cons"), "dev", [E.DEV_SEEDS[1]], 1500)
    return E.evaluate_params(prepared)


def _conf(res):
    return res["confusion_matrix"]


def test_per_class_table_is_exactly_the_confusion_matrix(res):
    conf = _conf(res)
    for cls, v in res["per_class"].items():
        row = conf.get(cls, {})
        assert v["support"] == sum(row.values())
        assert v["tp"] == row.get(cls, 0)
        col = sum(r.get(cls, 0) for r in conf.values())
        if cls in E.SAFE_FOR_UNRESOLVED_PAIR:  # documented exclusion of UNRESOLVED_PAIR units
            col -= conf.get("UNRESOLVED_PAIR", {}).get(cls, 0)
        assert v["predicted"] == col
        assert v["recall"] == (round(v["tp"] / v["support"], 4) if v["support"] else None)
        assert v["precision"] == (round(v["tp"] / col, 4) if col else None)


def test_missing_record_metrics_use_the_same_denominators_as_the_matrix(res):
    conf, m = _conf(res), res["missing_records"]
    for key, label in (
        ("missing_partner", "MISSING_PARTNER_RECORD"),
        ("missing_internal", "MISSING_INTERNAL_RECORD"),
    ):
        row = conf[label]
        d = m[key]
        assert d["units"] == sum(row.values())
        assert d["exact_class"] == row.get(label, 0)
        assert d["flagged_for_attention"] == sum(v for k, v in row.items() if k in E.ATTENTION)
        assert d["flagged_for_attention"] + d["wrongly_paired_or_cleared"] == d["units"]
        assert d["exact_class_recall"] == d["exact_class_recall"] <= d["attention_rate"] <= 1
    # the per-class recall IS the exact-class recall (the two numbers can no longer disagree)
    assert (
        res["per_class"]["MISSING_PARTNER_RECORD"]["recall"]
        == m["missing_partner"]["exact_class_recall"]
    )
    assert (
        res["per_class"]["MISSING_INTERNAL_RECORD"]["recall"]
        == m["missing_internal"]["exact_class_recall"]
    )


def test_every_unit_appears_exactly_once_in_the_confusion_matrix(res):
    total = sum(sum(r.values()) for r in _conf(res).values())
    assert total == sum(v.get("units", 0) for v in res["per_scenario"].values())


def test_pair_counts_are_consistent(res):
    a, c = res["automatic_matching"], res["counts"]
    assert (
        a["correct_pairs"] + a["false_automatic_matches"] == a["predicted_pairs"] == c["pred_pairs"]
    )
    assert a["precision"] == round(a["correct_pairs"] / a["predicted_pairs"], 4)
    assert a["false_automatic_match_rate"] == round(
        a["false_automatic_matches"] / a["predicted_pairs"], 4
    )
    assert (
        sum(v.get("false_auto_matches", 0) for v in res["per_scenario"].values())
        == a["false_automatic_matches"]
    )
    assert sum(v.get("pairs_found", 0) for v in res["per_scenario"].values()) == a["correct_pairs"]


def test_exposure_identity_per_currency(res):
    for cur, e in res["exposure_by_currency"].items():
        assert e["truth_unresolved"] == e.get("truth_unresolved_flagged", 0) + e.get(
            "falsely_cleared", 0
        ), cur


def test_markdown_tables_match_the_numbers(res):
    md = E.to_markdown("t", res)
    # confusion table cells equal the matrix
    header = next(ln for ln in md.splitlines() if ln.startswith("| true / predicted"))
    cols = [c.strip() for c in header.strip("|").split("|")][1:]
    for line in md.splitlines():
        m = re.match(r"\| (\S+) \| (\d+(?: \| \d+)+) \|$", line)
        if m and m.group(1) in _conf(res) and len(m.group(2).split(" | ")) == len(cols):
            cells = dict(zip(cols, map(int, m.group(2).split(" | ")), strict=True))
            assert {k: v for k, v in cells.items() if v} == _conf(res)[m.group(1)]
    # per-class table support/predicted
    for cls, v in res["per_class"].items():
        assert re.search(rf"\| {cls} \| {v['support']} \| {v['predicted']} \|", md)
    # headline false-match line
    a = res["automatic_matching"]
    assert f"**{a['false_automatic_matches']}**" in md
