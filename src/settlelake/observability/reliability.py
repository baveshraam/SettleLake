"""Operational reliability metrics, each measured by an executed scenario (nothing hard-coded).

Scenarios (all on one generated dataset, unique scratch directory):
  clean run                -> completeness, quarantine/rejection rates, duplicate suppression,
                              late-event incorporation, reconciliation coverage, exposure, stage status
  interrupt-and-resume     -> for each stage: crash it, resume, compare output hashes with the clean
                              run, measure recovery time
  replay                   -> output-hash consistency
"""

from __future__ import annotations

import json
import shutil
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from settlelake.config import STMT_COLS, get_paths
from settlelake.lakehouse.store import connect
from settlelake.observability.benchmark import environment
from settlelake.pipeline import STAGES, SimulatedInterruption, replay, run_pipeline
from settlelake.simulator.evaluation import UNRESOLVED
from settlelake.simulator.generator import GenConfig, generate


def _input_records(raw: Path) -> int:
    """Independent count of source records in the raw files (not taken from the pipeline)."""
    n = 0
    for f in raw.glob("payment_events*.jsonl"):
        n += sum(1 for line in f.read_text(encoding="utf-8").splitlines() if line.strip())
    for f in raw.glob("statements_*.csv"):
        lines = [x for x in f.read_text(encoding="utf-8").splitlines() if x.strip()]
        assert lines[0] == ",".join(STMT_COLS)
        n += len(lines) - 1
    for f in raw.glob("statements_*.json"):
        n += len(json.loads(f.read_text(encoding="utf-8"))["rows"])
    return n


def _q(con, sql):
    return con.execute(sql).fetchall()


def _outputs(res) -> dict:
    return {
        k: v for s in ("silver", "reconcile", "gold") for k, v in res.stages[s]["outputs"].items()
    }


def run_reliability(home, payments: int = 10000, seed: int = 42, anomalies: str = "low") -> dict:
    base = get_paths(home)
    run_id = f"reliability-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    work = base.home / "benchmarks" / run_id
    work.mkdir(parents=True, exist_ok=False)
    try:
        clean = get_paths(work / "clean")
        manifest = generate(clean, GenConfig(payments, seed, anomalies))
        t0 = time.perf_counter()
        res = run_pipeline(clean.home)
        clean_s = time.perf_counter() - t0
        con = connect()
        g, s = clean.gold.as_posix(), clean.silver.as_posix()

        # ---- completeness / rejection / quarantine / duplicates
        src = _input_records(clean.raw)
        bronze = res.stages["silver"]["rows_in"]
        counts = {n: _q(con, f"SELECT count(*) FROM read_parquet('{s}/{n}.parquet')")[0][0]
                  for n in ("events", "statements", "duplicates", "rejected", "quarantine")}  # fmt: skip
        accounted = sum(counts.values())
        inj_dups = len(manifest["duplicate_event_ids"]) + manifest["scenario_counts"].get(
            "DUP_STATEMENT", 0
        )
        conflicts = len(manifest["conflicting_event_ids"])
        dups_found = _q(con, f"SELECT count(*) FROM read_parquet('{s}/duplicates.parquet')")[0][0]
        uniq_events = _q(
            con,
            f"SELECT count(*) - count(DISTINCT event_id) FROM read_parquet('{s}/events.parquet')",
        )[0][0]

        # ---- late events: keys the generator labelled LATE_EVENT must still reconcile as MATCHED
        truth = [
            json.loads(x)
            for x in (clean.ground_truth / "ground_truth.jsonl").read_text().splitlines()
        ]
        late_keys = {t["match_key"] for t in truth if t["scenario"] == "LATE_EVENT"}
        rec = {
            k: c
            for k, c in _q(
                con,
                f"SELECT match_key, classification FROM read_parquet('{g}/reconciliation.parquet')",
            )
        }
        late_ok = sum(rec.get(k) == "MATCHED" for k in late_keys)

        # ---- coverage and exposure
        total_keys = len(rec)
        unresolved_rows = _q(
            con,
            "SELECT coalesce(internal_currency, partner_currency) AS cur, count(*), "
            "sum(coalesce(internal_amount_minor, partner_amount_minor)) "
            f"FROM read_parquet('{g}/reconciliation.parquet') "
            f"WHERE classification IN ({', '.join(repr(c) for c in sorted(UNRESOLVED - {'UNRESOLVED_PAIR'}))}) "
            "GROUP BY 1 ORDER BY 1",
        )
        ambiguous = sum(1 for c in rec.values() if c in ("AMBIGUOUS_MATCH", "INVALID_RECORD"))
        stage_status = {k: v["status"] for k, v in res.stages.items()}

        # ---- interrupt every stage, resume, compare with the clean outputs
        clean_out = _outputs(res)
        recoveries = []
        for stage in STAGES:
            hm = work / f"crash-{stage}"
            shutil.copytree(clean.raw, get_paths(hm).raw)
            try:
                run_pipeline(hm, simulate_failure=stage)
                raise AssertionError("injected failure did not fire")
            except SimulatedInterruption as exc:
                rid = str(exc).split("run_id=")[1].rstrip("]")
            t1 = time.perf_counter()
            r2 = run_pipeline(hm, resume=rid)
            rec_s = time.perf_counter() - t1
            recoveries.append({"interrupted_stage": stage, "recovery_seconds": round(rec_s, 3),
                               "status": r2.status, "outputs_identical_to_clean_run": _outputs(r2) == clean_out,
                               "quality_errors": len(r2.quality_errors)})  # fmt: skip

        rp = replay(clean.home, res.run_id)
        con.close()
        out = {
            "run_id": run_id,
            "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "environment": environment(),
            "workload": {
                "payments": payments,
                "seed": seed,
                "anomalies": anomalies,
                "source_records": src,
                "clean_pipeline_seconds": round(clean_s, 3),
            },  # fmt: skip
            "source_record_completeness": {
                "source_records": src,
                "bronze_records": bronze,
                "bronze_completeness": round(bronze / src, 6),
                "silver_accounted_records": accounted,
                "note": "accepted + duplicates + rejected + quarantined; conflicting event ids count once per version",
            },  # fmt: skip
            "rejection": {
                "rejected": counts["rejected"],
                "quarantined": counts["quarantine"],
                "rejected_rate": round(counts["rejected"] / src, 6),
                "quarantine_rate": round(counts["quarantine"] / src, 6),
                "injected_malformed_lines": sum(manifest["malformed_lines"].values()),
                "injected_conflicting_event_ids": conflicts,
            },  # fmt: skip
            "duplicate_delivery": {
                "injected_duplicate_deliveries": inj_dups,
                "suppressed_in_silver": dups_found,
                "suppression_rate": round(dups_found / inj_dups, 4) if inj_dups else None,
                "duplicate_event_ids_remaining_in_silver": uniq_events,
            },  # fmt: skip
            "late_events": {
                "late_arriving_keys": len(late_keys),
                "reconciled_as_matched": late_ok,
                "incorporation_rate": round(late_ok / len(late_keys), 4) if late_keys else None,
            },  # fmt: skip
            "reconciliation_coverage": {
                "keys": total_keys,
                "resolved_without_abstention": total_keys - ambiguous,
                "coverage": round((total_keys - ambiguous) / total_keys, 4),
                "matched": sum(c == "MATCHED" for c in rec.values()),
            },  # fmt: skip
            "unresolved_exposure_by_currency_minor_units": {
                cur: {"keys": n, "amount_minor": int(amt or 0)} for cur, n, amt in unresolved_rows
            },
            "pipeline_stage_status": stage_status,
            "financial_aggregate_consistency": {
                "quality_error_checks_failed": len(res.quality_errors),
                "checks_run": len(res.quality),
                "checks": {
                    q["check_name"]: q["status"] for q in res.quality if q["severity"] == "ERROR"
                },
            },  # fmt: skip
            "failure_injection_recovery": recoveries,
            "replay": {"result": rp.replay_result, "differing_outputs": rp.replay_diff},
        }
    finally:
        pass
    base.reports.mkdir(parents=True, exist_ok=True)
    (base.reports / "reliability.json").write_text(
        json.dumps(out, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (base.reports / "reliability.md").write_text(to_markdown(out), encoding="utf-8")
    shutil.rmtree(work, ignore_errors=True)
    return out


def to_markdown(r: dict) -> str:
    c, rj, d = r["source_record_completeness"], r["rejection"], r["duplicate_delivery"]
    L = ["# Reliability scenarios", "",
         "> Synthetic data; each number comes from a scenario executed by `settlelake reliability`.", "",
         f"Workload: {r['workload']['payments']} payments, {r['workload']['source_records']} source records, seed {r['workload']['seed']}, anomalies `{r['workload']['anomalies']}`.", "",
         "| property | measured |", "|---|---|",
         f"| source-record completeness (Bronze/source) | {c['bronze_records']}/{c['source_records']} = {100 * c['bronze_completeness']:.3f}% |",
         f"| rejected / quarantined records | {rj['rejected']} ({100 * rj['rejected_rate']:.2f}%) / {rj['quarantined']} ({100 * rj['quarantine_rate']:.2f}%) |",
         f"| duplicate deliveries suppressed | {d['suppressed_in_silver']}/{d['injected_duplicate_deliveries']}; duplicate event ids left in Silver: {d['duplicate_event_ids_remaining_in_silver']} |",
         f"| late-arriving events reconciled correctly | {r['late_events']['reconciled_as_matched']}/{r['late_events']['late_arriving_keys']} |",
         f"| reconciliation coverage (resolved without abstention) | {100 * r['reconciliation_coverage']['coverage']:.2f}% of {r['reconciliation_coverage']['keys']} keys |",
         f"| stage success | {', '.join(f'{k}={v}' for k, v in r['pipeline_stage_status'].items())} |",
         f"| financial-aggregate checks | {r['financial_aggregate_consistency']['checks_run'] - r['financial_aggregate_consistency']['quality_error_checks_failed']}/{r['financial_aggregate_consistency']['checks_run']} passing, ERROR failures: {r['financial_aggregate_consistency']['quality_error_checks_failed']} |",
         f"| replay output hashes | {r['replay']['result']} |", "",
         "## Unresolved exposure (engine view, integer minor units, per currency)", "",
         "| currency | keys | amount (minor units) |", "|---|---|---|"]  # fmt: skip
    L += [
        f"| {cur} | {v['keys']} | {v['amount_minor']} |"
        for cur, v in r["unresolved_exposure_by_currency_minor_units"].items()
    ]
    L += ["", "## Failure injection: crash a stage, resume, compare with a clean run", "",
          "| interrupted stage | recovery (s) | final status | outputs identical to clean run |", "|---|---|---|---|"]  # fmt: skip
    L += [
        f"| {x['interrupted_stage']} | {x['recovery_seconds']} | {x['status']} | {x['outputs_identical_to_clean_run']} |"
        for x in r["failure_injection_recovery"]
    ]
    L += [
        "",
        "Recovery here is stage-level resume on one machine (completed stages are skipped after output-hash verification); "
        "zero-loss/zero-duplicate results for the streaming path are asserted by `scripts/e2e_stream.sh`, not by this scenario.",
        "",
    ]
    return "\n".join(L)
