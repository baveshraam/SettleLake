"""Reproducible benchmark: generate -> pipeline -> evaluate -> re-run -> replay, with timings.

Everything reported is measured by this process on this machine; nothing is hard-coded.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
import time
import uuid
from datetime import UTC, datetime

import duckdb
import pyarrow
import pydantic

from settlelake import __version__
from settlelake.config import ALL_CLASSES, Paths, get_paths
from settlelake.lakehouse.store import connect
from settlelake.observability.evaluation import evaluate
from settlelake.pipeline import STAGES, replay, run_pipeline
from settlelake.simulator.generator import GenConfig, generate


def peak_rss_mb() -> float | None:
    """Peak resident set size of this process in MiB (None if unavailable)."""
    try:
        import resource

        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(kb / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)
    except ImportError:
        pass
    try:
        import ctypes
        from ctypes import wintypes

        class PMC(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        k32, psapi = ctypes.windll.kernel32, ctypes.windll.psapi
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD]
        c = PMC()
        c.cb = ctypes.sizeof(c)
        if not psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(c), c.cb):
            return None
        return round(c.PeakWorkingSetSize / (1024 * 1024), 1)
    except Exception:
        return None


def total_ram_gb() -> float | None:
    try:
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024**3, 1)
    except (AttributeError, ValueError, OSError):
        pass
    try:
        import ctypes

        class MS(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        m = MS()
        m.dwLength = ctypes.sizeof(m)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
        return round(m.ullTotalPhys / 1024**3, 1)
    except Exception:
        return None


def environment() -> dict:
    return {
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "logical_cpus": os.cpu_count(),
        "total_ram_gb": total_ram_gb(),
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "pyarrow": pyarrow.__version__,
        "pydantic": pydantic.__version__,
        "settlelake": __version__,
    }


def _settlement_totals(paths: Paths) -> dict[str, tuple[int, int]]:
    con = connect()
    rows = con.execute(
        "SELECT currency, count(*), sum(amount_minor) FROM "
        f"read_parquet('{(paths.gold / 'fact_settlements.parquet').as_posix()}') GROUP BY 1 ORDER BY 1"
    ).fetchall()
    con.close()
    return {c: (int(n), int(t)) for c, n, t in rows}


def run_benchmark(home, payments: int, seed: int = 42, anomalies: str = "low") -> dict:
    base = get_paths(home)
    # unique directory per run: a previous (or interrupted) run can never collide with this one
    work = Paths(
        base.home / "benchmarks" / f"n{payments}_s{seed}_{anomalies}_{uuid.uuid4().hex[:8]}"
    )
    work.home.mkdir(parents=True, exist_ok=False)

    t = time.perf_counter()
    manifest = generate(work, GenConfig(payments, seed, anomalies))
    gen_s = time.perf_counter() - t
    mem_after_gen = peak_rss_mb()

    t = time.perf_counter()
    run1 = run_pipeline(work.home)
    pipe_s = time.perf_counter() - t
    mem_after_pipeline = peak_rss_mb()
    totals1 = _settlement_totals(work)

    con = connect()
    silver = {
        n: con.execute(f"SELECT count(*) FROM read_parquet('{(work.silver / (n + '.parquet')).as_posix()}')").fetchone()[0]
        for n in ("events", "statements", "duplicates", "rejected", "quarantine")
    }  # fmt: skip
    classes = dict(
        con.execute(
            f"SELECT classification, count(*) FROM read_parquet('{(work.gold / 'reconciliation.parquet').as_posix()}') GROUP BY 1"
        ).fetchall()
    )
    con.close()
    keys = sum(classes.values())
    bronze_records = run1.stages["silver"]["rows_in"]
    evaluation = evaluate(work)

    # idempotent re-run on identical inputs: Bronze is not duplicated, outputs identical
    t = time.perf_counter()
    run2 = run_pipeline(work.home)
    rerun_s = time.perf_counter() - t
    totals2 = _settlement_totals(work)
    out1 = {k: v for s in run1.stages.values() for k, v in s["outputs"].items() if not k.startswith(("quality:", "bronze/"))}  # fmt: skip
    out2 = {k: v for s in run2.stages.values() for k, v in s["outputs"].items() if not k.startswith(("quality:", "bronze/"))}  # fmt: skip
    rp = replay(work.home, run1.run_id)

    result = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": environment(),
        "workload": {
            "payments": payments,
            "seed": seed,
            "anomalies": anomalies,
            "event_records": manifest["event_records"],
            "statement_rows": sum(manifest["statement_rows"].values()),
            "input_files": len(manifest["files"]),
        },  # fmt: skip
        "generation_seconds": round(gen_s, 3),
        "pipeline": {
            "total_seconds": round(pipe_s, 3),
            "bronze_records": bronze_records,
            "end_to_end_records_per_second": round(bronze_records / pipe_s),
            "accepted_records": silver["events"] + silver["statements"],
            "duplicates_removed": silver["duplicates"],
            "rejected_records": silver["rejected"],
            "quarantined_records": silver["quarantine"],
            "stages": {
                name: {
                    "seconds": round(run1.stages[name]["duration_s"], 3),
                    "rows_in": run1.stages[name]["rows_in"],
                    "rows_out": run1.stages[name]["rows_out"],
                    # ingest "rows in" counts files, so use records written for its throughput
                    "records_per_second": round(
                        run1.stages[name]["rows_out" if name == "ingest" else "rows_in"]
                        / max(run1.stages[name]["duration_s"], 1e-9)
                    ),
                }
                for name in STAGES
            },
        },
        "reconciliation": {
            "reconciled_keys": keys,
            "match_rate": round(classes.get("MATCHED", 0) / keys, 6) if keys else None,
            "exception_rate": round(1 - classes.get("MATCHED", 0) / keys, 6) if keys else None,
            "classes": {c: classes[c] for c in ALL_CLASSES if c in classes},
        },
        "evaluation": evaluation,
        "idempotency": {
            "rerun_seconds": round(rerun_s, 3),
            "bronze_files_unchanged": run2.stages["ingest"]["detail"]["repeated_inputs"]
            == run2.stages["ingest"]["detail"]["input_files"],
            "settlement_totals_equal": totals1 == totals2,
            "output_hashes_equal": out1 == out2,
            "replay_result": rp.replay_result,
            "replay_differences": rp.replay_diff,
            "consistent": totals1 == totals2 and out1 == out2 and rp.replay_result == "CONSISTENT",
        },
        "memory_peak_rss_mb": {
            "after_generation": mem_after_gen,
            "after_pipeline": mem_after_pipeline,
            "end": peak_rss_mb(),
        },  # fmt: skip
        "notes": [
            "Peak RSS is for the whole benchmark process (generation + pipeline), not per stage.",
            "Single machine, single process; DuckDB parallelism is not tuned.",
        ],
    }
    base.reports.mkdir(parents=True, exist_ok=True)
    (base.reports / "benchmark.json").write_text(
        json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8", newline="\n"
    )
    (base.reports / "benchmark.md").write_text(to_markdown(result), encoding="utf-8", newline="\n")
    shutil.rmtree(work.home, ignore_errors=True)  # scratch data only; reports are kept
    return result


def _fmt(v) -> str:
    return "n/a" if v is None else (f"{v:.4f}" if isinstance(v, float) else str(v))


def to_markdown(r: dict) -> str:
    e, w, p = r["environment"], r["workload"], r["pipeline"]
    lines = [
        "# SettleLake benchmark report",
        "",
        "> Synthetic dataset, local single-process execution. All numbers were measured by this run.",
        "",
        f"Generated: {r['generated_at']}",
        "",
        "## Environment",
        *[f"- {k}: {v}" for k, v in e.items()],
        "",
        "## Workload",
        *[f"- {k}: {v}" for k, v in w.items()],
        f"- generation time: {r['generation_seconds']}s",
        "",
        "## Pipeline",
        f"- total: {p['total_seconds']}s, end-to-end {p['end_to_end_records_per_second']} bronze records/s",
        f"- bronze records: {p['bronze_records']}; accepted (silver events+statements): "
        f"{p['accepted_records']}; duplicates removed: {p['duplicates_removed']}; "
        f"rejected: {p['rejected_records']}; quarantined (malformed): {p['quarantined_records']}",
        "",
        "| stage | seconds | rows in | rows out | records / s |",
        "|---|---|---|---|---|",
        *[
            f"| {n} | {s['seconds']} | {s['rows_in']} | {s['rows_out']} | {s['records_per_second']} |"
            for n, s in p["stages"].items()
        ],  # fmt: skip
        "",
        "## Reconciliation",
        f"- keys: {r['reconciliation']['reconciled_keys']}; match rate: "
        f"{_fmt(r['reconciliation']['match_rate'])}; exception rate: "
        f"{_fmt(r['reconciliation']['exception_rate'])}",
        "",
        "| class | keys |",
        "|---|---|",
        *[f"| {c} | {n} |" for c, n in r["reconciliation"]["classes"].items()],
    ]
    ev = r["evaluation"]
    if ev:
        lines += [
            "",
            "## Classification quality vs. ground truth",
            f"- overall: precision {_fmt(ev['overall']['precision'])}, recall "
            f"{_fmt(ev['overall']['recall'])}, F1 {_fmt(ev['overall']['f1'])}",
            f"- exceptions only: precision {_fmt(ev['exceptions_only']['precision'])}, recall "
            f"{_fmt(ev['exceptions_only']['recall'])}, F1 {_fmt(ev['exceptions_only']['f1'])}",
            "",
            "| class | tp | fp | fn | precision | recall | F1 |",
            "|---|---|---|---|---|---|---|",
            *[
                f"| {c} | {v['tp']} | {v['fp']} | {v['fn']} | {_fmt(v['precision'])} | "
                f"{_fmt(v['recall'])} | {_fmt(v['f1'])} |"
                for c, v in ev["per_class"].items()
            ],  # fmt: skip
        ]
    i = r["idempotency"]
    lines += [
        "",
        "## Idempotency and replay",
        *[f"- {k}: {v}" for k, v in i.items()],
        "",
        "## Memory",
        *[f"- peak RSS {k}: {_fmt(v)} MiB" for k, v in r["memory_peak_rss_mb"].items()],
        "",
        "## Notes",
        *[f"- {n}" for n in r["notes"]],
        "",
    ]
    return "\n".join(lines)
