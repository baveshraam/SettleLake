"""Markdown/JSON run report assembled from the Gold analytics views and run metadata."""

from __future__ import annotations

import json

from settlelake.analytics.views import open_analytics
from settlelake.config import Paths
from settlelake.observability.evaluation import evaluate
from settlelake.observability.tracking import Tracker


def _table(headers: list[str], rows: list[tuple]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out)


def build_report(paths: Paths) -> tuple[dict, str]:
    tracker = Tracker(paths.meta_db)
    run = tracker.latest_run("pipeline", "COMPLETED")
    if run is None:
        tracker.close()
        raise LookupError("no completed pipeline run found; run `settlelake run` first")
    stages, quality = tracker.stages(run["run_id"]), tracker.quality(run["run_id"])
    tracker.close()
    con = open_analytics(paths)
    classes = con.execute(
        "SELECT classification, count(*) FROM reconciliation GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    total = sum(n for _, n in classes)
    matched = dict(classes).get("MATCHED", 0)
    volume = con.execute(
        "SELECT currency, payments, total_minor FROM v_volume_by_currency ORDER BY currency"
    ).fetchall()
    providers = con.execute(
        "SELECT provider, reconciled_keys, matched, exceptions, match_rate_pct "
        "FROM v_provider_performance"
    ).fetchall()
    con.close()
    ev = evaluate(paths)
    data = {
        "run_id": run["run_id"],
        "started_at": str(run["started_at"]),
        "duration_s": run["duration_s"],
        "reconciled_keys": total,
        "match_rate": matched / total if total else None,
        "classes": dict(classes),
        "stages": {
            k: {"seconds": v["duration_s"], "rows_in": v["rows_in"], "rows_out": v["rows_out"]}
            for k, v in stages.items()
        },  # fmt: skip
        "quality": quality,
        "evaluation": ev,
    }
    md = [
        "# SettleLake run report",
        "",
        "> Synthetic dataset. No real payments or customer data.",
        "",
        f"- Run: `{run['run_id']}` started {run['started_at']} UTC, "
        f"duration {run['duration_s']:.2f}s",
        f"- Reconciled keys: {total}; match rate: {(100 * matched / total):.2f}%"
        if total
        else "- No reconciled keys",
        "",
        "## Stages",
        _table(
            ["stage", "status", "seconds", "rows in", "rows out"],
            [
                (k, v["status"], f"{v['duration_s']:.3f}", v["rows_in"], v["rows_out"])
                for k, v in stages.items()
            ],
        ),  # fmt: skip
        "",
        "## Reconciliation classes",
        _table(["classification", "keys"], classes),
        "",
        "## Provider performance",
        _table(["provider", "keys", "matched", "exceptions", "match rate %"], providers),
        "",
        "## Payment volume by currency (never mixed across currencies; integer minor units)",
        _table(["currency", "payments", "total (minor units)"], volume),
        "",
        "## Data quality",
        _table(
            ["check", "severity", "status", "evaluated", "failed"],
            [
                (
                    q["check_name"],
                    q["severity"],
                    q["status"],
                    q["evaluated_count"],
                    q["failed_count"],
                )
                for q in quality
            ],
        ),  # fmt: skip
    ]
    if ev:
        md += [
            "",
            "## Ground-truth evaluation",
            f"Overall (match_key, class) pairs: precision {ev['overall']['precision']:.4f}, "
            f"recall {ev['overall']['recall']:.4f}, F1 {ev['overall']['f1']:.4f}",
            "",
            _table(
                ["class", "tp", "fp", "fn", "precision", "recall", "f1"],
                [
                    (c, v["tp"], v["fp"], v["fn"], v["precision"], v["recall"], v["f1"])
                    for c, v in ev["per_class"].items()
                ],
            ),  # fmt: skip
        ]
    text = "\n".join(md) + "\n"
    paths.reports.mkdir(parents=True, exist_ok=True)
    (paths.reports / "report.md").write_text(text, encoding="utf-8", newline="\n")
    (paths.reports / "report.json").write_text(
        json.dumps(data, indent=2, default=str), encoding="utf-8", newline="\n"
    )
    return data, text
