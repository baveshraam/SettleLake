"""Pipeline orchestration: staged execution, run tracking, stage-level resume and replay.

Recovery model (single machine, deliberately modest):
  * Every stage publishes its Parquet outputs atomically (temp file + rename).
  * A stage is only recorded COMPLETED, with content hashes of its outputs, after publishing.
  * `resume` re-executes a run from the first stage that is not COMPLETED (or whose recorded
    output hashes no longer match the files on disk). Stages are pure functions of their inputs,
    so re-execution is deterministic. There are no distributed checkpoints or streaming offsets.
"""

from __future__ import annotations

import hashlib
import shutil
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from settlelake.config import Paths, get_paths
from settlelake.ingestion.bronze import (
    IngestionError,
    InputFile,
    bronze_path,
    discover_inputs,
    ingest_file,
)
from settlelake.ingestion.silver import SILVER_FILES, build_silver
from settlelake.lakehouse.gold import GOLD_TABLES, build_gold
from settlelake.lakehouse.store import connect, content_hash
from settlelake.observability.tracking import Tracker, new_run_id
from settlelake.quality.checks import run_checks
from settlelake.reconciliation.engine import reconcile

STAGES = ["ingest", "silver", "reconcile", "gold", "quality"]


class PipelineError(Exception):
    """A pipeline run failed for a reportable reason."""


class SimulatedInterruption(PipelineError):
    """Raised by the failure-injection hook to emulate a crash mid-stage."""


@dataclass
class Ctx:
    run_id: str
    inputs: list[dict]
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    tracker: Tracker
    input_dir: Path | None = None
    simulate_failure: str | None = None
    lake_root: Path = field(default_factory=Path)

    @property
    def bronze_files(self) -> list[Path]:
        return [bronze_path(self.bronze_dir, i["fingerprint"]) for i in self.inputs]


@dataclass
class RunResult:
    run_id: str
    status: str
    stages: dict[str, dict]
    quality: list[dict]
    replay_result: str | None = None
    replay_diff: list[str] = field(default_factory=list)

    @property
    def quality_errors(self) -> list[dict]:
        return [q for q in self.quality if q["status"] == "FAIL"]


def fingerprint_of(inputs: list[dict]) -> str:
    joined = "|".join(sorted(i["fingerprint"] for i in inputs))
    return hashlib.sha256(joined.encode()).hexdigest()[:16]


def _hash_files(ctx: Ctx, files: list[Path]) -> dict[str, str]:
    con = connect()
    out = {p.relative_to(ctx.lake_root).as_posix(): content_hash(con, p) for p in files}
    con.close()
    return out


# ---------------------------------------------------------------- stages
def stage_ingest(ctx: Ctx) -> tuple[int, int, dict, dict]:
    if ctx.input_dir is None:
        raise PipelineError("ingest stage requires an input directory")
    files = [InputFile(ctx.input_dir / i["name"], i["record_type"], i["format"], i["fingerprint"])
             for i in ctx.inputs]  # fmt: skip
    new_records = skipped = 0
    for f in files:
        n = ingest_file(f, ctx.bronze_dir, ctx.run_id)
        if n is None:
            skipped += 1
        else:
            new_records += n
    detail = {
        "input_files": len(files),
        "new_files": len(files) - skipped,
        "repeated_inputs": skipped,
    }
    outputs = _hash_files(ctx, ctx.bronze_files)
    return len(files), new_records, detail, outputs


def stage_silver(ctx: Ctx) -> tuple[int, int, dict, dict]:
    counts = build_silver(ctx.bronze_files, ctx.silver_dir)
    outputs = _hash_files(ctx, [ctx.silver_dir / f"{n}.parquet" for n in SILVER_FILES])
    return counts["bronze_records"], counts["events"] + counts["statements"], counts, outputs


def stage_reconcile(ctx: Ctx) -> tuple[int, int, dict, dict]:
    res = reconcile(ctx.silver_dir, ctx.gold_dir)
    outputs = _hash_files(ctx, [ctx.gold_dir / "reconciliation.parquet"])
    return res["reconciled_keys"], res["reconciled_keys"], res, outputs


def stage_gold(ctx: Ctx) -> tuple[int, int, dict, dict]:
    counts = build_gold(ctx.silver_dir, ctx.gold_dir)
    others = [n for n in GOLD_TABLES if n != "reconciliation"]
    outputs = _hash_files(ctx, [ctx.gold_dir / f"{n}.parquet" for n in others])
    return counts["fact_payments"], sum(counts.values()), counts, outputs


def stage_quality(ctx: Ctx) -> tuple[int, int, dict, dict]:
    results = [r.as_dict() for r in run_checks(ctx.bronze_files, ctx.silver_dir, ctx.gold_dir)]
    ctx.tracker.save_quality(ctx.run_id, results)
    failed = sum(r["status"] == "FAIL" for r in results)
    detail = {"checks": len(results), "failed": failed,
              "warnings": sum(r["status"] == "WARN" for r in results)}  # fmt: skip
    # counts (not durations) are what replay compares
    outputs = {f"quality:{r['check_name']}": f"{r['evaluated_count']}:{r['failed_count']}"
               for r in results}  # fmt: skip
    return len(results), len(results), detail, outputs


STAGE_FUNCS: dict[str, Callable[[Ctx], tuple[int, int, dict, dict]]] = {
    "ingest": stage_ingest,
    "silver": stage_silver,
    "reconcile": stage_reconcile,
    "gold": stage_gold,
    "quality": stage_quality,
}
STAGE_DIR = {"ingest": "bronze", "silver": "silver", "reconcile": "gold", "gold": "gold"}


def _outputs_still_valid(ctx: Ctx, outputs: dict[str, str]) -> bool:
    con = connect()
    try:
        for rel, h in outputs.items():
            if rel.startswith("quality:"):
                continue
            p = ctx.lake_root / rel
            if not p.exists() or content_hash(con, p) != h:
                return False
        return True
    finally:
        con.close()


def _run_stages(ctx: Ctx, stages: list[str], completed: dict[str, dict] | None = None) -> None:
    done = completed or {}
    for stage in stages:
        prev = done.get(stage)
        if prev and prev["status"] == "COMPLETED" and _outputs_still_valid(ctx, prev["outputs"]):
            continue  # resume: stage already published and verified
        ctx.tracker.start_stage(ctx.run_id, stage)
        t0 = time.perf_counter()
        try:
            if ctx.simulate_failure == stage:
                # emulate a crash after a partial temp write, before atomic publish
                d = {"ingest": ctx.bronze_dir, "silver": ctx.silver_dir}.get(stage, ctx.gold_dir)
                d.mkdir(parents=True, exist_ok=True)
                (d / f"{stage}.partial.tmp").write_text("partial")
                raise SimulatedInterruption(f"simulated interruption during stage '{stage}'")
            rows_in, rows_out, detail, outputs = STAGE_FUNCS[stage](ctx)
        except SimulatedInterruption as exc:
            ctx.tracker.finish_stage(ctx.run_id, stage, "INTERRUPTED", time.perf_counter() - t0,
                                     error=str(exc))  # fmt: skip
            raise
        except Exception as exc:
            ctx.tracker.finish_stage(ctx.run_id, stage, "FAILED", time.perf_counter() - t0,
                                     error=f"{type(exc).__name__}: {exc}")  # fmt: skip
            raise
        ctx.tracker.finish_stage(ctx.run_id, stage, "COMPLETED", time.perf_counter() - t0,
                                 rows_in, rows_out, detail, outputs)  # fmt: skip
        for stale in (ctx.bronze_dir, ctx.silver_dir, ctx.gold_dir):
            for tmp in stale.glob("*.partial.tmp"):
                tmp.unlink()


def run_pipeline(
    home: str | Path | None = None,
    *,
    input_dir: str | Path | None = None,
    stages: list[str] | None = None,
    resume: str | None = None,
    simulate_failure: str | None = None,
) -> RunResult:
    paths = get_paths(home)
    stages = stages or STAGES
    bad = [s for s in stages if s not in STAGES]
    if bad:
        raise PipelineError(f"unknown stage(s): {', '.join(bad)}; valid: {', '.join(STAGES)}")
    in_dir = Path(input_dir).resolve() if input_dir else paths.raw
    tracker = Tracker(paths.meta_db)
    try:
        completed: dict[str, dict] = {}
        if resume:
            run = tracker.get_run(resume)
            if run is None:
                raise PipelineError(f"run {resume} not found")
            if run["status"] == "COMPLETED":
                raise PipelineError(f"run {resume} is already COMPLETED; nothing to resume")
            inputs = run["inputs"]
            fps = {i.fingerprint for i in discover_inputs(in_dir)}
            if fps != {i["fingerprint"] for i in inputs}:
                raise PipelineError("input files changed since the run started; cannot resume")
            completed = tracker.stages(resume)
            run_id = resume
            tracker.con.execute(
                "UPDATE runs SET status='RUNNING', error=NULL WHERE run_id=?", [run_id]
            )
        elif set(stages) == set(STAGES):
            inputs = [i.as_dict() for i in discover_inputs(in_dir)]
            run_id = new_run_id()
            tracker.start_run(run_id, "pipeline", inputs, fingerprint_of(inputs))
        else:
            last = tracker.latest_run("pipeline", "COMPLETED")
            if last is None:
                raise PipelineError("no completed pipeline run found; run `settlelake run` first")
            inputs, run_id = last["inputs"], new_run_id()
            tracker.start_run(run_id, "pipeline", inputs, fingerprint_of(inputs))
        ctx = Ctx(run_id, inputs, paths.bronze, paths.silver, paths.gold, tracker, in_dir,
                  simulate_failure, paths.lake)  # fmt: skip
        try:
            _run_stages(ctx, stages, completed)
        except SimulatedInterruption as exc:
            tracker.finish_run(run_id, "INTERRUPTED", str(exc))
            raise SimulatedInterruption(f"{exc} [run_id={run_id}]") from exc
        except (IngestionError, PipelineError) as exc:
            tracker.finish_run(run_id, "FAILED", str(exc))
            raise PipelineError(str(exc)) from exc
        except Exception as exc:
            tracker.finish_run(run_id, "FAILED", traceback.format_exc(limit=4))
            raise PipelineError(f"{type(exc).__name__}: {exc}") from exc
        tracker.finish_run(run_id, "COMPLETED")
        return RunResult(run_id, "COMPLETED", tracker.stages(run_id), tracker.quality(run_id))
    finally:
        tracker.close()


def replay(home: str | Path | None, run_id: str) -> RunResult:
    """Re-execute silver..quality for a past run from its preserved Bronze inputs into a scratch
    lake, then compare output content hashes with those recorded by the original run."""
    paths: Paths = get_paths(home)
    tracker = Tracker(paths.meta_db)
    try:
        orig = tracker.get_run(run_id)
        if orig is None:
            raise PipelineError(f"run {run_id} not found")
        orig_stages = tracker.stages(run_id)
        if orig["kind"] != "pipeline" or not orig_stages:
            raise PipelineError(f"run {run_id} is not a replayable pipeline run")
        orig_outputs = {k: v for s in ("silver", "reconcile", "gold", "quality")
                        for k, v in orig_stages.get(s, {}).get("outputs", {}).items()}  # fmt: skip
        if not orig_outputs:
            raise PipelineError(f"run {run_id} has no completed outputs to compare against")
        missing = [i["name"] for i in orig["inputs"]
                   if not bronze_path(paths.bronze, i["fingerprint"]).exists()]  # fmt: skip
        if missing:
            raise PipelineError(f"Bronze data for run {run_id} is missing: {', '.join(missing)}")

        rid = new_run_id()
        scratch = paths.replays / rid / "lake"
        shutil.rmtree(scratch.parent, ignore_errors=True)
        tracker.start_run(rid, "replay", orig["inputs"], orig["input_fingerprint"], parent=run_id)
        ctx = Ctx(rid, orig["inputs"], paths.bronze, scratch / "silver", scratch / "gold", tracker,
                  None, None, scratch)  # fmt: skip
        try:
            _run_stages(ctx, ["silver", "reconcile", "gold", "quality"])
        except Exception as exc:
            tracker.finish_run(rid, "FAILED", f"{type(exc).__name__}: {exc}")
            raise PipelineError(f"replay failed: {exc}") from exc
        new_outputs = {k: v for s in tracker.stages(rid).values() for k, v in s["outputs"].items()}
        # compare file hashes by name relative to the lake root (identical layout)
        diff = sorted(k for k, v in orig_outputs.items() if new_outputs.get(k) != v)
        result = "CONSISTENT" if not diff else "INCONSISTENT"
        tracker.finish_run(rid, "COMPLETED", replay_result=result)
        shutil.rmtree(scratch.parent, ignore_errors=True)
        return RunResult(rid, "COMPLETED", tracker.stages(rid), tracker.quality(rid), result, diff)
    finally:
        tracker.close()
