# SettleLake — Cross-Border Payments Data Platform

A local, reproducible financial data-engineering project. It generates **synthetic** cross-border
payment events and partner bank settlement statements, ingests them into a Parquet medallion
lakehouse (Bronze → Silver → Gold) queried with DuckDB, reconciles internal settlements against
partner statements with deterministic rules, enforces data quality, and exposes a Streamlit
operations dashboard.

> **Synthetic data only.** SettleLake does not execute, route or settle real payments, and contains no
> real customer or banking data. It is a single-machine, single-process demonstration; it is not a
> distributed system and makes no streaming/exactly-once claims.

```
Synthetic data → Ingestion → Bronze → Silver → Reconciliation → Gold → Analytics / Dashboard
```

## Quick start

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

## Docker

```bash
docker compose up --build dashboard   # runs the QUICK pipeline into a volume, then serves http://localhost:8501
```

The image runs as a non-root user with data in the `/data` volume. This flow is smoke-tested in CI (`docker` job).

## Commands

| command | purpose |
|---|---|
| `generate --profile quick\|demo\|benchmark --seed N [--anomalies clean\|low\|stress] [--payments N]` | deterministic dataset + separate ground truth |
| `run [--input DIR] [--resume RUN_ID] [--simulate-failure STAGE]` | full pipeline with run/stage tracking |
| `reconcile` / `quality` | re-run reconciliation+gold / data-quality checks on existing data |
| `replay --run-id RUN_ID` | re-execute a past run from Bronze into a scratch lake and compare output hashes |
| `report` | write and print `reports/report.md` |
| `benchmark --payments N --seed N` | measured end-to-end benchmark → `reports/benchmark.{json,md}` |
| `dashboard` | launch Streamlit |

Exit codes: `0` ok, `1` runtime/pipeline error, `2` usage error, `3` a quality ERROR check failed,
`4` replay output differs.

## What is implemented

- **Generator** (`simulator/`): 8 currencies, 10 corridors, 4 partners (2 send CSV, 2 send JSON),
  4-stage payment lifecycle, integer minor units everywhere, seeded and byte-reproducible.
  Failure injection with ground-truth labels: duplicate events/statement rows, missing partner records,
  partner-only records, amount/currency/status mismatches, invalid schemas, malformed lines, late-arriving
  and out-of-order events, conflicting duplicate event IDs, delayed partner settlement, ambiguous
  reference collisions.
- **Ingestion** (`ingestion/`): Pydantic contracts, record-level provenance, SHA-256 input
  fingerprints, repeated-input detection, quarantine of malformed records.
- **Lakehouse** (`lakehouse/`, `pipeline.py`): Parquet Bronze/Silver/Gold, atomic publishes,
  run + stage tracking in a DuckDB metadata database, stage-level resume, replay verification.
- **Reconciliation** (`sql/reconcile.sql`): one set-based SQL statement; documented precedence;
  all discrepancy reasons retained; ambiguous matches are never forced.
- **Quality** (`quality/`): 14 persisted checks: 10 ERROR-severity invariants that gate the exit code, 4 WARN source-health monitors.
- **Analytics** (`sql/analytics.sql`): views for volume, corridors, provider performance, match rates,
  outstanding settlements, exceptions, DQ trend, pipeline stats. Monetary totals are always per currency.
- **Dashboard** (`dashboard/app.py`): Overview, Payment Intelligence, Reconciliation Center, Pipeline Observability.

## Idempotency and recovery (what is actually guaranteed)

- Inputs are fingerprinted (SHA-256 of bytes). Re-ingesting identical bytes is detected and skipped;
  Bronze record IDs are `fingerprint:line`, so they are stable.
- Silver/Gold are pure functions of the run's Bronze files, rebuilt in full and published atomically
  (temp file + rename). Running twice yields identical content hashes, row counts and totals (tested).
- A stage is recorded `COMPLETED` only after publishing; `run --resume RUN_ID` skips completed stages
  whose recorded output hashes still verify and re-executes the rest. `--simulate-failure STAGE`
  emulates a crash after a partial temp write.
- This is **stage-level recovery on one machine**. There are no distributed checkpoints, offsets,
  or exactly-once streaming semantics.

## Testing

```bash
ruff check src tests && ruff format --check src tests
pytest --cov=settlelake
```

Tests cover money arithmetic, contracts, generator determinism, every reconciliation class, quarantine,
repeated inputs, idempotent re-runs, interrupted-stage recovery, replay, ground-truth evaluation,
benchmark output, CLI exit codes, and a headless render of every dashboard section.

## Benchmarks

`settlelake benchmark` generates data, runs the pipeline, scores classifications against ground truth,
re-runs for idempotency and replays. A recorded run (with hardware and versions) is in
[docs/benchmark-results.md](docs/benchmark-results.md); methodology and caveats in
[docs/benchmark-methodology.md](docs/benchmark-methodology.md). Precision/recall of 1.0 on the synthetic
data mean the engine's rules agree with the generator's labels; they are not a claim about real-world accuracy.

## Docs

[architecture](docs/architecture.md) · [data contracts](docs/data-contracts.md) ·
[reconciliation rules](docs/reconciliation-rules.md) · [failure matrix](docs/failure-matrix.md) ·
[benchmark methodology](docs/benchmark-methodology.md) · [runbook](docs/runbook.md) ·
[build status](docs/BUILD_STATUS.md)

## Not in this MVP

Kafka/Redpanda, Spark, Airflow, Debezium, S3/Glue/Athena, Iceberg, Terraform and distributed tracing are
future extensions only; nothing here integrates with them. The stages are plain functions over Parquet
paths, so they can be ported later.
