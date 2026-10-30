# SettleLake — Cross-Border Payments Data Platform

A reproducible financial data-engineering project. It generates **synthetic** cross-border payment
events and partner bank settlement statements, ingests them into a Bronze → Silver → Gold lakehouse,
reconciles internal settlements against partner statements with deterministic rules, enforces data
quality, and serves an operations dashboard.

> **Synthetic data only.** SettleLake does not execute, route or settle real payments and contains no
> real customer or banking data.

Two engines implement the same business rules and are tested for **exact output parity**:

| | Local engine (default) | Lakehouse engine |
|---|---|---|
| compute / storage | Python + DuckDB, Parquet files | PySpark 3.5 + Apache Iceberg |
| ingest | JSONL / CSV / JSON files | files **and** Postgres → Debezium → Redpanda → Spark Structured Streaming |
| catalog / object store | paths on disk | Hadoop (tests), JDBC + S3-compatible store (Compose), **AWS Glue + S3** |
| orchestration | CLI | CLI and Apache Airflow |

See [docs/architecture.md](docs/architecture.md) for the data-flow diagram and design decisions.

## Results at a glance

All figures are synthetic-data measurements; each links to its method. Spark runs in `local[*]` on one node (no cluster results).

| area | verified result | source |
|---|---|---|
| Throughput (DuckDB, 100k payments / 489,572 records, Linux container, median of 3) | 20.5 s, 4,877 payments/s, 23,874 records/s | [benchmark-results §1](docs/benchmark-results.md) |
| Throughput (DuckDB, Windows host, 500k payments, 1 trial) | 241 s, 2,075 payments/s, 10,153 records/s | [§2](docs/benchmark-results.md) |
| DuckDB vs Spark(local), 100k | DuckDB 20.5 s vs Spark 108.6 s (1 Spark trial); Spark was slower at every size | [§1](docs/benchmark-results.md) |
| Engine parity | Spark and DuckDB reconciliation tables identical (9,663 / 96,291 rows) | [§1](docs/benchmark-results.md), `tests/spark` |
| Event-ingestion lag (generator property) | p50 31 s, p95 58 s, p99 60 s | [§3](docs/benchmark-results.md) |
| Auto-matching precision (adversarial held-out seeds) | 98.35%; 182 false automatic matches of 11,024 (1.65%), all from the undetectable `WRONG_TARGET` scenario | [evaluation](docs/evaluation.md) |
| Auto-matching coverage / automation (held-out) | 92.60% of resolvable pairs; 75.94% of internal records auto-paired | [evaluation](docs/evaluation.md) |
| Ambiguity handling (held-out) | 676/676 competing-candidate records abstained (AMBIGUOUS_MATCH); fuzzy auto-matching disabled | [evaluation](docs/evaluation.md) |
| Unresolved exposure (held-out, per currency) | exposure recall 94.8-97.1%; falsely cleared e.g. USD 3,403,427.31 (all from `WRONG_TARGET`); every currency reported separately | [evaluation](docs/evaluation.md) |
| Failure recovery | crash at each of 5 stages then resume: 5/5 outputs identical to a clean run (0.9-4.8 s); replay hashes consistent; streaming crash-after-MERGE: 3,937/3,937 events exactly once (CI) | [reliability](docs/results/reliability.md), `scripts/e2e_stream.sh` |
| Tests | 98 pytest tests pass (non-Spark line coverage 92.5%) + 15 Spark parity tests + streaming and Airflow e2e jobs in CI | [BUILD_STATUS](docs/BUILD_STATUS.md) |

The first generator's "F1 = 1.0" agreement is a regression test, not a quality claim; see [evaluation](docs/evaluation.md).

## Quick start (local engine, no Docker)

Requires Python 3.11+.

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

settlelake generate --profile quick --seed 42          # 1,000 payments (demo: 10k, benchmark: 100k)
settlelake run                                         # ingest > silver > reconcile > gold > quality
settlelake report                                      # Markdown summary of the latest run
settlelake dashboard                                   # http://localhost:8501
```

Data lives under `./data` (override with `--home` or `SETTLELAKE_HOME`); it is git-ignored.

| command | purpose |
|---|---|
| `generate --profile quick\|demo\|benchmark --seed N [--anomalies clean\|low\|stress] [--payments N]` | deterministic dataset + separate ground truth |
| `run [--input DIR] [--resume RUN_ID] [--simulate-failure STAGE]` | full pipeline with run/stage tracking |
| `reconcile` / `quality` | re-run reconciliation+gold / data-quality checks |
| `replay --run-id RUN_ID` | re-execute a past run from Bronze into a scratch lake and compare output hashes |
| `report` / `benchmark --payments N` / `dashboard` | report, measured benchmark, Streamlit UI |

Exit codes: `0` ok, `1` runtime error, `2` usage error, `3` a quality ERROR check failed, `4` replay/parity mismatch.

## Docker

```bash
docker compose up --build dashboard              # MVP: QUICK pipeline into a volume, dashboard on :8501
bash scripts/e2e_stream.sh                       # streaming stack end-to-end proof (see below)
bash scripts/e2e_airflow.sh                      # Airflow backfill end-to-end proof
```

Compose profiles: default = MVP; `stream` = Postgres, Debezium/Kafka Connect, Redpanda, SeaweedFS (S3 API),
Spark; `airflow` = Airflow (dev standalone mode) plus the storage services. Credentials in
`docker-compose.yml` and `docker/seaweed/s3.json` are throwaway local-development values.
On Windows run the scripts from Git Bash with `MSYS_NO_PATHCONV=1`.

**`scripts/e2e_stream.sh`** (also a CI job) loads events into Postgres, lets Debezium capture them, streams them
into Iceberg Bronze, **injects a crash after the MERGE commits but before offsets are checkpointed**, restarts,
and asserts exactly one Bronze row per event. It then loads daily partner-statement partitions, builds
Silver/Gold on Iceberg, asserts the reconciliation is **identical to the DuckDB engine's**, and asserts nothing
changes after re-ingesting everything.

**`scripts/e2e_airflow.sh`** (also a CI job) imports the DAGs, runs `airflow dags backfill` over three days,
re-runs the backfill with reset, and asserts the Iceberg tables are unchanged and no quality ERROR check failed.

## AWS

`infra/terraform` defines an S3 lake bucket (versioned, encrypted, private), Glue databases
(`bronze|silver|gold|ops`), an Athena workgroup with saved queries, and a least-privilege IAM role for Spark jobs.
`terraform fmt`/`validate` run in CI. **It has never been applied** (no AWS account or credentials were available),
so Glue + Athena operation is unverified. See [docs/runbook.md](docs/runbook.md).

## What is verified

See [docs/BUILD_STATUS.md](docs/BUILD_STATUS.md). In short: local-engine tests, Spark/Iceberg parity tests, the
streaming and Airflow end-to-end scripts, the Docker dashboard smoke test and Terraform validation run in GitHub
Actions on every push. Benchmarks and caveats: [docs/benchmark-results.md](docs/benchmark-results.md),
[docs/benchmark-methodology.md](docs/benchmark-methodology.md).

## Guarantees and limits (read before relying on anything)

- **Idempotency:** Bronze ids are stable (`fingerprint:line`, `cdc:<seq>`); Silver/Gold are pure functions of
  Bronze, published atomically (Parquet rename / Iceberg snapshot commit). Re-runs and replays are tested to
  produce identical content.
- **Delivery:** the CDC path is at-least-once with an idempotent MERGE sink, which yields effectively-once
  Bronze. This is not a general exactly-once claim.
- **Recovery:** the local engine resumes at stage granularity; Spark streaming resumes from its checkpoint.
  Single-node Compose proves semantics, not horizontal scale-out; Spark runs in `local[*]` here.
- **Synthetic evaluation only.** Reconciliation precision/coverage are measured against generated labels. The reconciliation
  rule is blind to a partner booking a payment under another payment's reference with an identical amount (182 false matches
  in the held-out set); real accuracy is unknown.
- Terraform does not provision Kafka/Redpanda, Debezium or Postgres. Streaming checkpoints need durable storage on
  AWS (not configured). No Prometheus/Grafana server is bundled; metrics are written in Prometheus text format.

## Testing

```bash
ruff check src tests && ruff format --check src tests
pytest --cov=settlelake        # local engine; Spark tests skip without pyspark + Java
pip install -e ".[stream]" && bash scripts/install_spark_jars.sh && pytest tests/spark   # needs Java 17
```

## Docs

[architecture](docs/architecture.md) · [data contracts](docs/data-contracts.md) ·
[reconciliation rules](docs/reconciliation-rules.md) · [failure matrix](docs/failure-matrix.md) ·
[benchmark methodology](docs/benchmark-methodology.md) · [runbook](docs/runbook.md) ·
[build status](docs/BUILD_STATUS.md)
