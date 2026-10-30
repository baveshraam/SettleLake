"""Record-level evaluation of reconciliation against the adversarial evaluation dataset.

Truth comes from `simulator/evaluation.py` (written to a separate directory). The reconciliation
engine never reads it. Every result is measured against controlled synthetic ground truth and says
nothing about real-world accuracy.
"""

from __future__ import annotations

import json
import shutil
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from settlelake.config import (
    ALL_CLASSES,
    FALLBACK_AUTO,
    FALLBACK_MAX_EDIT,
    FALLBACK_WINDOW_HOURS,
    get_paths,
)
from settlelake.lakehouse.store import connect
from settlelake.pipeline import run_pipeline
from settlelake.reconciliation.engine import reconcile
from settlelake.simulator.evaluation import UNRESOLVED, EvalConfig, generate_eval

DEV_SEEDS = [11, 12, 13]
HELDOUT_SEEDS = [901, 902, 903]
NOT_AUTO = {"AMBIGUOUS_MATCH", "INVALID_RECORD"}
LABELS = [*ALL_CLASSES, "UNRESOLVED_PAIR", "NO_ROW"]
SAFE_FOR_UNRESOLVED_PAIR = {
    "MISSING_PARTNER_RECORD", "MISSING_INTERNAL_RECORD", "AMBIGUOUS_MATCH", "INVALID_RECORD",
}  # fmt: skip


def load_rows(gold_dir: Path) -> list[dict]:
    con = connect()
    cur = con.execute(
        "SELECT match_key, classification, reasons, internal_refs, partner_refs, "
        "internal_amount_minor, internal_currency, partner_amount_minor, partner_currency, "
        f"match_method FROM read_parquet('{(gold_dir / 'reconciliation.parquet').as_posix()}')"
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    con.close()
    return rows


def evaluate_run(truth: dict, rows: list[dict]) -> dict:
    """Return additive raw counters so several seeds can be pooled before computing rates."""
    by_event, by_stmt = {}, {}
    for r in rows:
        for e in r["internal_refs"] or []:
            by_event[e] = r
        for s in r["partner_refs"] or []:
            by_stmt[s] = r
    by_key = {r["match_key"]: r for r in rows}
    pred_pairs = set()
    for r in rows:
        if (
            len(r["internal_refs"] or []) == 1
            and len(r["partner_refs"] or []) == 1
            and r["classification"] not in NOT_AUTO
        ):
            pred_pairs.add((r["internal_refs"][0], r["partner_refs"][0]))
    true_pairs = {(p["event_id"], p["statement_row_id"]): p for p in truth["pairs"]}
    tp = pred_pairs & true_pairs.keys()
    resolvable = {k for k, p in true_pairs.items() if p["resolvable"]}
    c: Counter = Counter()
    c["pred_pairs"], c["true_pairs"] = len(pred_pairs), len(true_pairs)
    c["tp_pairs"], c["false_pairs"] = len(tp), len(pred_pairs - set(true_pairs))
    c["resolvable_pairs"], c["resolvable_found"] = len(resolvable), len(pred_pairs & resolvable)
    c["internal_units"] = sum(u["kind"] == "internal" for u in truth["units"])
    scen: dict[str, Counter] = defaultdict(Counter)
    for (e, s), p in true_pairs.items():
        scen[p["scenario"]]["true_pairs"] += 1
        scen[p["scenario"]]["pairs_found"] += (e, s) in pred_pairs
    unit_by_id = {u["id"]: u for u in truth["units"]}
    for e, _s in pred_pairs - set(true_pairs):
        u = unit_by_id.get(e)
        scen[u["scenario"] if u else "?"]["false_auto_matches"] += 1
    paired_ids = {a for a, _ in pred_pairs} | {b for _, b in pred_pairs}
    confusion: dict[str, Counter] = defaultdict(Counter)
    exposure: dict[str, Counter] = defaultdict(Counter)
    for u in truth["units"]:
        if u["kind"] == "internal":
            row = by_event.get(u["id"]) or by_key.get(u.get("match_key"))
        else:
            row = by_stmt.get(u["id"])
        pred = row["classification"] if row else "NO_ROW"
        reasons = set(row["reasons"]) if row else set()
        true_label = u["expected_class"]
        confusion[true_label][pred] += 1
        o = u["outcome"]
        scen[u["scenario"]]["units"] += 1
        auto = row is not None and u["id"] in paired_ids
        if o == "ABSTAIN":
            c["abstain_units"] += 1
            c["abstain_explicit"] += pred == "AMBIGUOUS_MATCH"
            c["abstain_wrongly_auto"] += auto
        if o == "MISSING_PARTNER":
            c["missing_partner_units"] += 1
            c["missing_partner_found"] += ("MISSING_PARTNER_RECORD" in reasons) and not auto
        if o == "MISSING_INTERNAL":
            c["missing_internal_units"] += 1
            c["missing_internal_found"] += ("MISSING_INTERNAL_RECORD" in reasons) and not auto
        if true_label == "UNRESOLVED_PAIR":
            c["unresolved_pair_units"] += 1
            c["unresolved_pair_safe"] += (pred in SAFE_FOR_UNRESOLVED_PAIR) and not auto
        if true_label == "DUPLICATE_EVENT":
            c["dup_event_units"] += 1
            c["dup_event_found"] += "DUPLICATE_EVENT" in reasons
        if true_label == "DUPLICATE_STATEMENT":
            c["dup_stmt_units"] += 1
            c["dup_stmt_found"] += "DUPLICATE_STATEMENT" in reasons
        if u["kind"] == "internal":
            c["dup_event_flagged"] += "DUPLICATE_EVENT" in reasons
            c["dup_stmt_flagged"] += "DUPLICATE_STATEMENT" in reasons
        flagged = pred in UNRESOLVED
        cur = u["currency"]
        if u["unresolved"]:
            exposure[cur]["truth_unresolved"] += u["amount_minor"]
            if flagged or (true_label == "UNRESOLVED_PAIR" and pred in SAFE_FOR_UNRESOLVED_PAIR):
                exposure[cur]["truth_unresolved_flagged"] += u["amount_minor"]
            else:
                exposure[cur]["falsely_cleared"] += u["amount_minor"]
        elif flagged:
            exposure[cur]["false_alarm"] += u["amount_minor"]
        if flagged or pred in SAFE_FOR_UNRESOLVED_PAIR:
            exposure[cur]["engine_reported_unresolved"] += u["amount_minor"]
    return {
        "counts": dict(c),
        "scenarios": {k: dict(v) for k, v in scen.items()},
        "confusion": {k: dict(v) for k, v in confusion.items()},
        "exposure": {k: dict(v) for k, v in exposure.items()},
    }


def pool(results: list[dict]) -> dict:
    out: dict = {"counts": Counter(), "scenarios": defaultdict(Counter), "confusion": defaultdict(Counter),
                 "exposure": defaultdict(Counter)}  # fmt: skip
    for r in results:
        out["counts"].update(r["counts"])
        for k in ("scenarios", "confusion", "exposure"):
            for a, b in r[k].items():
                out[k][a].update(b)
    return out


def _r(n, d):
    return None if not d else round(n / d, 4)


def summarize(p: dict) -> dict:
    c = p["counts"]
    cls_metrics = {}
    for cls in ALL_CLASSES:
        tp = p["confusion"].get(cls, {}).get(cls, 0)
        support = sum(p["confusion"].get(cls, {}).values())
        # Flagging a true-but-unresolvable pair as missing/ambiguous is the *safe* outcome, so those
        # units are not counted as false positives of the class (reported separately in missing_records).
        predicted = sum(
            row.get(cls, 0)
            for true_label, row in p["confusion"].items()
            if not (true_label == "UNRESOLVED_PAIR" and cls in SAFE_FOR_UNRESOLVED_PAIR)
        )
        if support or predicted:
            pr, rc = _r(tp, predicted), _r(tp, support)
            f1 = round(2 * pr * rc / (pr + rc), 4) if pr and rc else None
            cls_metrics[cls] = dict(
                support=support, predicted=predicted, tp=tp, precision=pr, recall=rc, f1=f1
            )
    expo = {}
    for cur, e in sorted(p["exposure"].items()):
        expo[cur] = dict(e) | {
            "exposure_recall": _r(
                e.get("truth_unresolved_flagged", 0), e.get("truth_unresolved", 0)
            )
        }
    return {
        "automatic_matching": {
            "predicted_pairs": c["pred_pairs"],
            "correct_pairs": c["tp_pairs"],
            "false_automatic_matches": c["false_pairs"],
            "precision": _r(c["tp_pairs"], c["pred_pairs"]),
            "false_automatic_match_rate": _r(c["false_pairs"], c["pred_pairs"]),
            "coverage_of_resolvable_pairs": _r(c["resolvable_found"], c["resolvable_pairs"]),
            "recall_of_all_true_pairs": _r(c["tp_pairs"], c["true_pairs"]),
            "automation_rate_of_internal_records": _r(c["pred_pairs"], c["internal_units"]),
        },
        "missing_records": {
            "missing_partner_recall": _r(c["missing_partner_found"], c["missing_partner_units"]),
            "missing_internal_recall": _r(c["missing_internal_found"], c["missing_internal_units"]),
            "unresolvable_pairs_flagged_safely": _r(
                c["unresolved_pair_safe"], c["unresolved_pair_units"]
            ),
        },
        "duplicates": {
            "duplicate_event_recall": _r(c["dup_event_found"], c["dup_event_units"]),
            "duplicate_statement_recall": _r(c["dup_stmt_found"], c["dup_stmt_units"]),
            "duplicate_event_precision": _r(c["dup_event_found"], c["dup_event_flagged"]),
            "duplicate_statement_precision": _r(c["dup_stmt_found"], c["dup_stmt_flagged"]),
        },
        "ambiguity": {
            "should_abstain_units": c["abstain_units"],
            "correct_abstention_rate": _r(c["abstain_explicit"], c["abstain_units"]),
            "wrongly_auto_matched": c["abstain_wrongly_auto"],
        },
        "per_class": cls_metrics,
        "per_scenario": {k: dict(v) for k, v in sorted(p["scenarios"].items())},
        "confusion_matrix": {k: dict(v) for k, v in p["confusion"].items()},
        "exposure_by_currency": expo,
        "counts": dict(c),
    }


def prepare(home, split: str, seeds: list[int], payments: int) -> list[dict]:
    """Generate + ingest + silver for each seed in an isolated, uniquely named directory."""
    base = get_paths(home).home / "evaluation"
    prepared = []
    for seed in seeds:
        work = base / f"{split}-{seed}-{uuid.uuid4().hex[:8]}"
        paths = get_paths(work)
        generate_eval(paths, EvalConfig(payments, seed))
        run_pipeline(work, stages=None)
        truth = json.loads((paths.ground_truth / "eval_truth.json").read_text(encoding="utf-8"))
        prepared.append({"seed": seed, "paths": paths, "truth": truth})
    return prepared


def evaluate_params(prepared: list[dict], params: dict | None = None) -> dict:
    per_seed, results = [], []
    for item in prepared:
        paths = item["paths"]
        gold = paths.gold
        if params is not None:
            gold = paths.home / f"gold_{uuid.uuid4().hex[:6]}"
            reconcile(paths.silver, gold, **params)
        res = evaluate_run(item["truth"], load_rows(gold))
        results.append(res)
        s = summarize(pool([res]))
        per_seed.append({"seed": item["seed"], **{k: s["automatic_matching"][k] for k in
                         ("precision", "coverage_of_resolvable_pairs", "false_automatic_matches")}})  # fmt: skip
    out = summarize(pool(results))
    out["per_seed"] = per_seed
    out["params"] = params or {
        "fb_max_edit": FALLBACK_MAX_EDIT, "fb_window_hours": FALLBACK_WINDOW_HOURS, "fb_auto": FALLBACK_AUTO,
    }  # fmt: skip
    return out


def cleanup(prepared: list[dict]) -> None:
    for item in prepared:
        shutil.rmtree(item["paths"].home, ignore_errors=True)


# ------------------------------------------------------------------ sweep and reporting
# Pre-registered selection rule (fixed before looking at any held-out data): among fallback policies,
# maximise recall of all true pairs on the DEV seeds subject to the false-automatic-match rate exceeding
# the exact-key-only baseline (fallback disabled) by at most 0.5 percentage points. The baseline is not
# zero: the dataset contains clerical reference errors (WRONG_TARGET) that exact matching cannot detect.
MAX_EXTRA_FALSE_AUTO_RATE = 0.005
GRID = [
    {"fb_max_edit": e, "fb_window_hours": w, "fb_auto": a}
    for a in (1, 0)
    for e in (0, 1, 2)
    for w in (6, 24, 48)
    if not (e == 0 and (w != 6 or a == 0))  # edit 0 disables fallback: one config is enough
]


def run_sweep(prepared: list[dict]) -> dict:
    rows = []
    for params in GRID:
        r = evaluate_params(prepared, params)
        a = r["automatic_matching"]
        rows.append({**params, "precision": a["precision"], "false_automatic_matches": a["false_automatic_matches"],
                     "false_automatic_match_rate": a["false_automatic_match_rate"] or 0.0,
                     "recall_of_all_true_pairs": a["recall_of_all_true_pairs"],
                     "automation_rate": a["automation_rate_of_internal_records"]})  # fmt: skip
    baseline = next(r for r in rows if r["fb_max_edit"] == 0)["false_automatic_match_rate"]
    ok = [
        r for r in rows if r["false_automatic_match_rate"] <= baseline + MAX_EXTRA_FALSE_AUTO_RATE
    ]
    top = max(r["recall_of_all_true_pairs"] for r in ok)
    ties = [r for r in ok if r["recall_of_all_true_pairs"] == top]
    # Pre-registered tie-break: at equal recall prefer *suggesting* fuzzy candidates (fb_auto=0, edit 1,
    # 24h) over doing nothing, because it surfaces probable matches for review at no precision cost.
    best = next(
        (r for r in ties if (r["fb_max_edit"], r["fb_window_hours"], r["fb_auto"]) == (1, 24, 0)),
        ties[0],
    )
    return {"rule": "max recall_of_all_true_pairs s.t. false_automatic_match_rate <= exact-key baseline "
            f"({_pct(baseline)}) + {_pct(MAX_EXTRA_FALSE_AUTO_RATE)}",
            "grid": rows, "selected": {k: best[k] for k in ("fb_max_edit", "fb_window_hours", "fb_auto")}}  # fmt: skip


def _pct(v):
    return "n/a" if v is None else f"{100 * v:.2f}%"


def to_markdown(title: str, res: dict, sweep: dict | None = None, meta: dict | None = None) -> str:
    a, m, d, amb = (
        res["automatic_matching"],
        res["missing_records"],
        res["duplicates"],
        res["ambiguity"],
    )
    L = [f"# {title}", "",
         "> All figures are measured against **controlled synthetic ground truth** from an adversarial "
         "generator (`simulator/evaluation.py`). They do not estimate real-world accuracy.", ""]  # fmt: skip
    if meta:
        L += [*(f"- {k}: {v}" for k, v in meta.items()), ""]
    L += [f"- engine parameters: `{res['params']}`", "", "## Automatic matching", "",
          "| metric | value |", "|---|---|",
          f"| automatic pairs decided | {a['predicted_pairs']} |",
          f"| correct pairs | {a['correct_pairs']} |",
          f"| **false automatic matches** | **{a['false_automatic_matches']}** ({_pct(a['false_automatic_match_rate'])} of automatic pairs) |",
          f"| **precision** | **{_pct(a['precision'])}** |",
          f"| coverage of resolvable pairs | {_pct(a['coverage_of_resolvable_pairs'])} |",
          f"| recall of all true pairs | {_pct(a['recall_of_all_true_pairs'])} |",
          f"| automation rate (internal records auto-paired) | {_pct(a['automation_rate_of_internal_records'])} |", "",
          "## Missing / duplicate / ambiguous", "", "| metric | value |", "|---|---|",
          f"| missing-partner records found | {_pct(m['missing_partner_recall'])} |",
          f"| missing-internal (partner-only) records found | {_pct(m['missing_internal_recall'])} |",
          f"| unresolvable true pairs flagged instead of mis-matched | {_pct(m['unresolvable_pairs_flagged_safely'])} |",
          f"| duplicate events: recall / precision | {_pct(d['duplicate_event_recall'])} / {_pct(d['duplicate_event_precision'])} |",
          f"| duplicate statements: recall / precision | {_pct(d['duplicate_statement_recall'])} / {_pct(d['duplicate_statement_precision'])} |",
          f"| correct ambiguity abstention (n={amb['should_abstain_units']}) | {_pct(amb['correct_abstention_rate'])}; wrongly auto-matched: {amb['wrongly_auto_matched']} |", "",
          "## Per-category detection (record level)", "",
          "| class | support | predicted | precision | recall | F1 |", "|---|---|---|---|---|---|"]  # fmt: skip
    for k, v in res["per_class"].items():
        L.append(
            f"| {k} | {v['support']} | {v['predicted']} | {_pct(v['precision'])} | {_pct(v['recall'])} | {_pct(v['f1'])} |"
        )
    L += ["", "## Per-scenario outcome (true pairs found / false automatic matches)", "",
          "| scenario | units | true pairs | pairs found | false automatic matches |", "|---|---|---|---|---|"]  # fmt: skip
    for k, v in res["per_scenario"].items():
        L.append(
            f"| {k} | {v.get('units', 0)} | {v.get('true_pairs', 0)} | {v.get('pairs_found', 0)} | {v.get('false_auto_matches', 0)} |"
        )
    labels = [
        x
        for x in LABELS
        if x in res["confusion_matrix"] or any(x in r for r in res["confusion_matrix"].values())
    ]
    L += ["", "## Confusion matrix (rows: true category, columns: engine classification)", "",
          "| true / predicted | " + " | ".join(labels) + " |", "|---|" + "---|" * len(labels)]  # fmt: skip
    for t in labels:
        row = res["confusion_matrix"].get(t)
        if row:
            L.append(f"| {t} | " + " | ".join(str(row.get(p, 0)) for p in labels) + " |")
    L += ["", "UNRESOLVED_PAIR: a true pair the policy cannot safely resolve (2-edit typo, typo+amount difference); "
          "the safe outcome is a missing/ambiguous flag, not a match.", "",
          "## Unresolved financial exposure (integer minor units, per currency, never summed across currencies)", "",
          "| currency | truth unresolved | flagged by engine | falsely cleared | false alarm | exposure recall |", "|---|---|---|---|---|---|"]  # fmt: skip
    for cur, e in res["exposure_by_currency"].items():
        L.append(f"| {cur} | {e.get('truth_unresolved', 0)} | {e.get('truth_unresolved_flagged', 0)} | "
                 f"{e.get('falsely_cleared', 0)} | {e.get('false_alarm', 0)} | {_pct(e.get('exposure_recall'))} |")  # fmt: skip
    L += [
        "",
        "## Per-seed stability",
        "",
        "| seed | precision | coverage of resolvable | false automatic matches |",
        "|---|---|---|---|",
    ]
    for s in res["per_seed"]:
        L.append(
            f"| {s['seed']} | {_pct(s['precision'])} | {_pct(s['coverage_of_resolvable_pairs'])} | {s['false_automatic_matches']} |"
        )
    if sweep:
        L += ["", "## Development-seed policy sweep", "", f"Selection rule (fixed in advance): {sweep['rule']}.", "",
              "| max edit | window h | auto | precision | false auto matches | false-auto rate | recall of true pairs | automation |",
              "|---|---|---|---|---|---|---|---|"]  # fmt: skip
        for r in sweep["grid"]:
            mark = (
                " **(selected)**"
                if all(r[k] == sweep["selected"][k] for k in sweep["selected"])
                else ""
            )
            L.append(f"| {r['fb_max_edit']} | {r['fb_window_hours']} | {r['fb_auto']} | {_pct(r['precision'])} | "
                     f"{r['false_automatic_matches']} | {_pct(r['false_automatic_match_rate'])} | "
                     f"{_pct(r['recall_of_all_true_pairs'])} | {_pct(r['automation_rate'])} |{mark}")  # fmt: skip
    return "\n".join(L) + "\n"
