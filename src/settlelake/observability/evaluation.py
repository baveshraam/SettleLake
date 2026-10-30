"""Evaluate reconciliation output against separately stored ground-truth labels."""

from __future__ import annotations

import json
from pathlib import Path

from settlelake.config import ALL_CLASSES, Paths
from settlelake.lakehouse.store import connect


def _prf(tp: int, fp: int, fn: int) -> dict:
    p = tp / (tp + fp) if tp + fp else None
    r = tp / (tp + fn) if tp + fn else None
    f1 = 2 * p * r / (p + r) if p and r else (0.0 if p is not None and r is not None else None)
    return {"tp": tp, "fp": fp, "fn": fn, "precision": p, "recall": r, "f1": f1}


def load_truth(path: Path) -> set[tuple[str, str]]:
    pairs = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        d = json.loads(line)
        pairs |= {(d["match_key"], c) for c in d["expected_classes"]}
    return pairs


def evaluate(paths: Paths, gold_dir: Path | None = None) -> dict | None:
    """Compare (match_key, class) pairs from gold reconciliation with ground truth.
    Returns None when no ground truth exists (e.g. externally supplied inputs)."""
    truth_file = paths.ground_truth / "ground_truth.jsonl"
    if not truth_file.exists():
        return None
    gold = gold_dir or paths.gold
    con = connect()
    pred = {
        (k, r)
        for k, r in con.execute(
            "SELECT match_key, unnest(reasons) FROM "
            f"read_parquet('{(gold / 'reconciliation.parquet').as_posix()}')"
        ).fetchall()
    }
    con.close()
    truth = load_truth(truth_file)
    per_class = {}
    for cls in ALL_CLASSES:
        t = {x for x in truth if x[1] == cls}
        p = {x for x in pred if x[1] == cls}
        if t or p:
            per_class[cls] = _prf(len(t & p), len(p - t), len(t - p))
    exc_t = {x for x in truth if x[1] != "MATCHED"}
    exc_p = {x for x in pred if x[1] != "MATCHED"}
    return {
        "truth_pairs": len(truth),
        "predicted_pairs": len(pred),
        "per_class": per_class,
        "overall": _prf(len(truth & pred), len(pred - truth), len(truth - pred)),
        "exceptions_only": _prf(len(exc_t & exc_p), len(exc_p - exc_t), len(exc_t - exc_p)),
        "exact_key_agreement": len({k for k, _ in truth} - {k for k, _ in pred}) == 0,
    }
