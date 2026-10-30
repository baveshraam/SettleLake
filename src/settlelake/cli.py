"""SettleLake command line interface.

Exit codes: 0 success, 1 runtime/pipeline failure, 2 usage error, 3 quality ERROR check failed,
4 replay output differs from the original run.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from settlelake.config import ANOMALY_LEVELS, PROFILES, get_paths
from settlelake.ingestion.bronze import IngestionError
from settlelake.pipeline import STAGES, PipelineError, SimulatedInterruption, replay, run_pipeline


def _print_run(res) -> None:
    print(f"run {res.run_id}: {res.status}")
    for name, s in res.stages.items():
        print(f"  {name:<10} {s['status']:<10} {s['duration_s'] or 0:7.3f}s  "
              f"in={s['rows_in']} out={s['rows_out']}")  # fmt: skip


def _quality_exit(res) -> int:
    fails = res.quality_errors
    if fails:
        print(
            "quality ERROR checks failed:",
            ", ".join(q["check_name"] for q in fails),
            file=sys.stderr,
        )
        return 3
    return 0


def cmd_generate(a) -> int:
    from settlelake.simulator.generator import GenConfig, generate

    n = a.payments or PROFILES[a.profile]
    paths = get_paths(a.home)
    m = generate(paths, GenConfig(n, a.seed, a.anomalies))
    print(f"generated {n} payments (seed={a.seed}, anomalies={a.anomalies}) into {paths.raw}")
    print(f"  events={m['event_records']} statements={sum(m['statement_rows'].values())}")
    print(f"  ground truth written separately to {paths.ground_truth}")
    return 0


def cmd_run(a) -> int:
    res = run_pipeline(a.home, input_dir=a.input, resume=a.resume,
                       simulate_failure=a.simulate_failure)  # fmt: skip
    _print_run(res)
    return _quality_exit(res)


def cmd_reconcile(a) -> int:
    res = run_pipeline(a.home, stages=["reconcile", "gold"])
    _print_run(res)
    return 0


def cmd_quality(a) -> int:
    res = run_pipeline(a.home, stages=["quality"])
    for q in res.quality:
        print(f"  {q['status']:<5} {q['severity']:<5} {q['check_name']:<36} "
              f"evaluated={q['evaluated_count']} failed={q['failed_count']}")  # fmt: skip
    return _quality_exit(res)


def cmd_replay(a) -> int:
    res = replay(a.home, a.run_id)
    print(f"replay {res.run_id} of {a.run_id}: {res.replay_result}")
    if res.replay_diff:
        print("  differing outputs:", ", ".join(res.replay_diff), file=sys.stderr)
        return 4
    return 0


def cmd_report(a) -> int:
    from settlelake.observability.report import build_report

    try:
        _, text = build_report(get_paths(a.home))
    except LookupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(text)
    print(f"(written to {get_paths(a.home).reports / 'report.md'})")
    return 0


def cmd_benchmark(a) -> int:
    from settlelake.observability.benchmark import run_benchmark

    r = run_benchmark(a.home, a.payments, a.seed, a.anomalies)
    p = r["pipeline"]
    print(f"benchmark: {a.payments} payments, pipeline {p['total_seconds']}s, "
          f"{p['end_to_end_records_per_second']} records/s")  # fmt: skip
    print(
        f"match rate {r['reconciliation']['match_rate']}, "
        f"F1 {r['evaluation']['overall']['f1']}, idempotent={r['idempotency']['consistent']}"
    )
    print(f"reports: {get_paths(a.home).reports}/benchmark.(json|md)")
    return 0 if r["idempotency"]["consistent"] else 4


def cmd_dashboard(a) -> int:
    app = Path(__file__).parent / "dashboard" / "app.py"
    env_home = str(get_paths(a.home).home)
    import os

    os.environ["SETTLELAKE_HOME"] = env_home
    return subprocess.call([sys.executable, "-m", "streamlit", "run", str(app),
                            "--server.port", str(a.port)])  # fmt: skip


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="settlelake", description="Synthetic cross-border payments data platform."
    )
    ap.add_argument("--home", help="data directory (default: $SETTLELAKE_HOME or ./data)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="generate a deterministic synthetic dataset")
    g.add_argument("--profile", choices=sorted(PROFILES), default="quick")
    g.add_argument("--payments", type=int, help="override the profile's payment count")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--anomalies", choices=ANOMALY_LEVELS, default="low")
    g.set_defaults(fn=cmd_generate)

    r = sub.add_parser("run", help=f"run the full pipeline ({' > '.join(STAGES)})")
    r.add_argument("--input", help="input directory (default: <home>/raw)")
    r.add_argument("--resume", metavar="RUN_ID", help="resume an interrupted/failed run")
    r.add_argument("--simulate-failure", choices=STAGES, help="inject a crash in a stage (testing)")
    r.set_defaults(fn=cmd_run)

    sub.add_parser("reconcile", help="re-run reconciliation + gold on current Silver").set_defaults(
        fn=cmd_reconcile
    )
    sub.add_parser("quality", help="run data quality checks").set_defaults(fn=cmd_quality)

    rp = sub.add_parser(
        "replay", help="deterministically re-execute a past run and compare outputs"
    )
    rp.add_argument("--run-id", required=True)
    rp.set_defaults(fn=cmd_replay)

    sub.add_parser(
        "report", help="print and save a Markdown report of the latest run"
    ).set_defaults(fn=cmd_report)

    b = sub.add_parser("benchmark", help="reproducible end-to-end benchmark")
    b.add_argument("--payments", type=int, default=PROFILES["benchmark"])
    b.add_argument("--seed", type=int, default=42)
    b.add_argument("--anomalies", choices=ANOMALY_LEVELS, default="low")
    b.set_defaults(fn=cmd_benchmark)

    d = sub.add_parser("dashboard", help="launch the Streamlit dashboard")
    d.add_argument("--port", type=int, default=8501)
    d.set_defaults(fn=cmd_dashboard)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except SimulatedInterruption as exc:
        print(f"interrupted: {exc}. Resume with `settlelake run --resume <RUN_ID>`.", file=sys.stderr)  # fmt: skip
        return 1
    except (PipelineError, IngestionError, ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
