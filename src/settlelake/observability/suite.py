"""Reproducible multi-size, multi-trial benchmark suite for the DuckDB and Spark engines.

* Every run uses a fresh, uniquely named working directory; nothing is ever reused or deleted by path
  reuse, so earlier runs cannot contaminate later measurements.
* Dataset generation is timed separately from processing.
* Both engines read the *same* generated files; Spark output is compared with DuckDB output.
* Nothing is hard-coded: all figures come from this process's measurements.
"""

from __future__ import annotations

import json
import os
import shutil
import statistics
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from settlelake.config import Paths, get_paths
from settlelake.lakehouse.store import connect
from settlelake.observability.benchmark import environment, peak_rss_mb
from settlelake.pipeline import STAGES, run_pipeline
from settlelake.simulator.generator import GenConfig, generate


def _stats(xs: list[float]) -> dict:
    return {
        "n": len(xs),
        "median": round(statistics.median(xs), 3),
        "min": round(min(xs), 3),
        "max": round(max(xs), 3),
        "stdev": round(statistics.stdev(xs), 3) if len(xs) > 1 else None,
        "values": [round(x, 3) for x in xs],
    }


def _lag_percentiles(silver: Path) -> dict:
    """Event ingestion lag (ingested_at - event_time, seconds) over all accepted events."""
    con = connect()
    p50, p95, p99, mx, n = con.execute(
        "SELECT quantile_cont(l, 0.5), quantile_cont(l, 0.95), quantile_cont(l, 0.99), max(l), count(*) FROM ("
        "SELECT date_diff('second', event_time, ingested_at) AS l "
        f"FROM read_parquet('{(silver / 'events.parquet').as_posix()}'))"
    ).fetchone()
    con.close()
    return {"events": n, "p50_s": p50, "p95_s": p95, "p99_s": p99, "max_s": mx}


def _silver_counts(silver: Path) -> dict:
    con = connect()
    out = {
        n: con.execute(f"SELECT count(*) FROM read_parquet('{(silver / (n + '.parquet')).as_posix()}')").fetchone()[0]
        for n in ("events", "statements", "duplicates", "rejected", "quarantine")
    }  # fmt: skip
    con.close()
    return out


def spark_available() -> bool:
    try:
        import pyspark  # noqa: F401

        return shutil.which("java") is not None
    except ImportError:
        return False


def _run_spark_trial(raw: Path, warehouse: Path, duck_gold: Path | None, run_id: str) -> dict:
    """One isolated Spark trial: new session, new catalog warehouse."""
    os.environ["SETTLELAKE_CATALOG"] = "local"
    os.environ["SETTLELAKE_WAREHOUSE"] = str(warehouse)
    from settlelake.spark.engine import run_batch
    from settlelake.spark.session import build_spark
    from settlelake.spark.verify import compare_with_duckdb

    spark = build_spark(f"bench-{run_id}")
    try:
        t0 = time.perf_counter()
        res = run_batch(spark, raw, run_id=run_id)
        total = time.perf_counter() - t0
        parity = compare_with_duckdb(spark, duck_gold) if duck_gold else None
    finally:
        spark.stop()
    return {"total_s": total, "stages": res["timings"], "quality_errors": len(res["quality_errors"]),
            "parity": parity}  # fmt: skip


def run_suite(
    home,
    sizes: list[int],
    duck_trials: dict[int, int],
    spark_trials: dict[int, int],
    seed: int = 42,
    anomalies: str = "low",
    keep: bool = False,
) -> dict:
    base = get_paths(home)
    run_id = f"suite-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    run_dir = base.home / "benchmarks" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)  # refuses to reuse an existing directory
    do_spark = any(spark_trials.get(s, 0) for s in sizes) and spark_available()
    results = {"run_id": run_id, "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "environment": environment(), "seed": seed, "anomalies": anomalies,
               "spark_executed": do_spark, "sizes": {}}  # fmt: skip
    for size in sizes:
        sdir = run_dir / f"n{size}"
        data = Paths(sdir / "data")
        t0 = time.perf_counter()
        manifest = generate(data, GenConfig(size, seed, anomalies))
        gen_s = time.perf_counter() - t0
        records = manifest["event_records"] + sum(manifest["statement_rows"].values()) + sum(
            manifest["malformed_lines"].values()
        )  # fmt: skip
        entry: dict = {"payments": size, "input_records": records, "generation_seconds": round(gen_s, 3),
                       "duckdb": {}, "spark": {}}  # fmt: skip
        walls, stage_runs, first_home = [], [], None
        for k in range(duck_trials.get(size, 0)):
            home_k = sdir / f"duckdb-{k}"
            t0 = time.perf_counter()
            res = run_pipeline(home_k, input_dir=data.raw)
            walls.append(time.perf_counter() - t0)
            stage_runs.append({s: v["duration_s"] for s, v in res.stages.items()})
            first_home = first_home or get_paths(home_k)
            if any(q["status"] == "FAIL" for q in res.quality):
                raise RuntimeError("quality ERROR check failed during benchmark")
        if walls:
            med = statistics.median(walls)
            entry["duckdb"] = {
                "wall_seconds": _stats(walls),
                "stage_median_seconds": {s: round(statistics.median(r[s] for r in stage_runs), 3) for s in STAGES},
                "payments_per_second": round(size / med),
                "records_per_second": round(records / med),
                "silver_counts": _silver_counts(first_home.silver),
                "ingestion_lag": _lag_percentiles(first_home.silver),
                "reconciliation_rows": connect().execute(
                    f"SELECT count(*) FROM read_parquet('{(first_home.gold / 'reconciliation.parquet').as_posix()}')"
                ).fetchone()[0],
            }  # fmt: skip
            entry["duckdb"]["peak_rss_mb_so_far"] = peak_rss_mb()
        n_spark = spark_trials.get(size, 0) if do_spark else 0
        if n_spark:
            trials = [
                _run_spark_trial(data.raw, sdir / f"spark-warehouse-{k}", first_home.gold if first_home else None, f"s{size}k{k}")
                for k in range(n_spark)
            ]  # fmt: skip
            tot = [t["total_s"] for t in trials]
            med = statistics.median(tot)
            entry["spark"] = {
                "mode": "local[*] single node (not a cluster)",
                "wall_seconds": _stats(tot),
                "stage_median_seconds": {s: round(statistics.median(t["stages"][s] for t in trials), 3) for s in STAGES},
                "payments_per_second": round(size / med),
                "records_per_second": round(records / med),
                "parity_identical_all_trials": all(t["parity"] and t["parity"]["identical"] for t in trials),
                "parity_rows": trials[0]["parity"]["spark_rows"] if trials[0]["parity"] else None,
                "quality_errors": sum(t["quality_errors"] for t in trials),
            }  # fmt: skip
        results["sizes"][str(size)] = entry
        if not keep:
            shutil.rmtree(sdir, ignore_errors=True)
    results["peak_rss_mb_python_process"] = peak_rss_mb()
    base.reports.mkdir(parents=True, exist_ok=True)
    (base.reports / f"{run_id}.json").write_text(
        json.dumps(results, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (base.reports / f"{run_id}.md").write_text(to_markdown(results), encoding="utf-8")
    if not keep:
        shutil.rmtree(run_dir, ignore_errors=True)
    return results


def to_markdown(r: dict) -> str:
    env = r["environment"]
    L = [f"# Benchmark suite `{r['run_id']}`", "",
         "> Synthetic data; single machine. Spark runs in `local[*]` mode, so Spark numbers describe a single-node run, "
         "not distributed scale-out. Dataset generation time is reported separately and excluded from processing time.", "",
         f"- seed {r['seed']}, anomaly level `{r['anomalies']}`, Spark executed: {r['spark_executed']}",
         f"- CPU: {env['processor']} ({env['logical_cpus']} logical), RAM {env['total_ram_gb']} GB, {env['platform']}",
         f"- Python {env['python']}, DuckDB {env['duckdb']}, PyArrow {env['pyarrow']}, Pydantic {env['pydantic']}", ""]  # fmt: skip
    L += ["## Throughput and latency", "",
          "| payments | input records | engine | trials | median wall (s) | min-max (s) | stdev | payments/s | records/s |",
          "|---|---|---|---|---|---|---|---|---|"]  # fmt: skip
    for size, e in r["sizes"].items():
        for eng in ("duckdb", "spark"):
            d = e.get(eng)
            if d:
                w = d["wall_seconds"]
                L.append(
                    f"| {size} | {e['input_records']} | {eng} | {w['n']} | {w['median']} | {w['min']}-{w['max']} | {w['stdev']} | {d['payments_per_second']} | {d['records_per_second']} |"
                )
    L += [
        "",
        "## Median stage durations (s)",
        "",
        "| payments | engine | " + " | ".join(STAGES) + " |",
        "|---|---|" + "---|" * len(STAGES),
    ]
    for size, e in r["sizes"].items():
        for eng in ("duckdb", "spark"):
            d = e.get(eng)
            if d:
                L.append(
                    f"| {size} | {eng} | "
                    + " | ".join(str(d["stage_median_seconds"][s]) for s in STAGES)
                    + " |"
                )
    L += ["", "## Event-ingestion lag (ingested_at - event_time, seconds, accepted events)", "",
          "| payments | events | p50 | p95 | p99 | max |", "|---|---|---|---|---|---|"]  # fmt: skip
    for size, e in r["sizes"].items():
        g = e.get("duckdb", {}).get("ingestion_lag")
        if g:
            L.append(
                f"| {size} | {g['events']} | {g['p50_s']:.1f} | {g['p95_s']:.1f} | {g['p99_s']:.1f} | {g['max_s']} |"
            )
    L += ["", "## Engine parity and outputs", "", "| payments | reconciliation rows | accepted events | duplicates | rejected | quarantined | Spark == DuckDB |",
          "|---|---|---|---|---|---|---|"]  # fmt: skip
    for size, e in r["sizes"].items():
        d, s = e.get("duckdb"), e.get("spark")
        if d:
            c = d["silver_counts"]
            par = (
                "n/a" if not s else f"{s['parity_identical_all_trials']} ({s['parity_rows']} rows)"
            )
            L.append(
                f"| {size} | {d['reconciliation_rows']} | {c['events']} | {c['duplicates']} | {c['rejected']} | {c['quarantine']} | {par} |"
            )
    L += [
        "",
        "## Generation time (excluded from the figures above)",
        "",
        "| payments | seconds |",
        "|---|---|",
    ]
    L += [f"| {size} | {e['generation_seconds']} |" for size, e in r["sizes"].items()]
    L += [
        "",
        f"Peak RSS of the benchmark Python process: {r['peak_rss_mb_python_process']} MiB (JVM memory excluded).",
        "",
    ]
    return "\n".join(L)
