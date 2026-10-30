"""Engine comparison benchmark: DuckDB pipeline vs Spark (local mode) on the same generated dataset.

Both engines run on one machine; Spark runs in local[*] mode, so this measures engine overhead and
single-node throughput, NOT distributed scale-out. Output parity is checked on every run.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

from settlelake.config import Paths, get_paths
from settlelake.observability.benchmark import environment, peak_rss_mb
from settlelake.pipeline import run_pipeline
from settlelake.simulator.generator import GenConfig, generate


def run_engine_benchmark(home: str | Path | None, payments: int, seed: int = 42,
                         anomalies: str = "low") -> dict:  # fmt: skip
    base = get_paths(home)
    work = Paths(base.home / "benchmarks" / f"engines_n{payments}_s{seed}_{anomalies}")
    shutil.rmtree(work.home, ignore_errors=True)
    manifest = generate(work, GenConfig(payments, seed, anomalies))

    t = time.perf_counter()
    duck = run_pipeline(work.home)
    duck_total = time.perf_counter() - t

    os.environ["SETTLELAKE_CATALOG"] = os.environ.get("SETTLELAKE_CATALOG", "local")
    os.environ.setdefault("SETTLELAKE_WAREHOUSE", str(work.home / "iceberg-warehouse"))
    from settlelake.spark.engine import run_batch
    from settlelake.spark.session import build_spark
    from settlelake.spark.verify import verify_parity

    spark = build_spark("settlelake-bench")
    t = time.perf_counter()
    res = run_batch(spark, work.raw, run_id="bench")
    spark_total = time.perf_counter() - t
    parity = verify_parity(work.home)

    out = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": environment(),
        "workload": {
            "payments": payments, "seed": seed, "anomalies": anomalies,
            "event_records": manifest["event_records"],
            "statement_rows": sum(manifest["statement_rows"].values()),
        },
        "duckdb": {
            "total_seconds": round(duck_total, 3),
            "stages": {k: round(v["duration_s"], 3) for k, v in duck.stages.items()},
        },
        "spark_local": {
            "total_seconds": round(spark_total, 3),
            "stages": res["timings"],
            "shuffle_partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"),
        },
        "output_parity": parity,
        "peak_rss_mb_python_process": peak_rss_mb(),
        "notes": [
            "Spark runs in local[*] mode inside one process/container; JVM memory is not in peak RSS of the Python process.",
            "Single run, no warm-up; Spark time includes Iceberg commits and JVM startup effects.",
            "File ingest is driver-side in the Spark engine; the streaming path is the distributed one.",
        ],
    }  # fmt: skip
    base.reports.mkdir(parents=True, exist_ok=True)
    (base.reports / "engine_benchmark.json").write_text(
        json.dumps(out, indent=2, default=str) + "\n", encoding="utf-8", newline="\n"
    )
    return out
