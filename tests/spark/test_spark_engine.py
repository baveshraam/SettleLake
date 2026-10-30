import duckdb
import pytest

from settlelake.spark import engine
from settlelake.spark.engine import t

SILVER = ["events", "statements", "duplicates", "rejected", "quarantine"]


@pytest.fixture(scope="module")
def batch(spark, dataset):
    paths, _ = dataset
    return engine.run_batch(spark, paths.raw, run_id="test-run")


def _duck(paths, sql):
    return duckdb.connect().execute(sql).fetchall()


def test_quality_gates_hold(batch):
    assert batch["quality_errors"] == []
    assert len(batch["detail"]["quality"]) == 14


@pytest.mark.parametrize("name", SILVER)
def test_silver_parity_with_duckdb_engine(spark, dataset, batch, name):
    paths, _ = dataset
    local = _duck(
        paths, f"SELECT count(*) FROM read_parquet('{paths.silver.as_posix()}/{name}.parquet')"
    )[0][0]
    assert spark.table(t("silver", name)).count() == local


def test_reconciliation_parity_with_duckdb_engine(spark, dataset, batch):
    paths, _ = dataset
    cols = (
        "match_key, classification, reasons, internal_amount_minor, internal_currency, "
        "partner_amount_minor, partner_currency, settlement_lag_seconds, ingestion_lag_seconds, "
        "explanation, reconciliation_id, provider, payment_id, corridor, internal_refs, partner_refs"
    )
    local = _duck(
        paths,
        f"SELECT {cols} FROM read_parquet('{paths.gold.as_posix()}/reconciliation.parquet') ORDER BY match_key",
    )
    got = spark.sql(
        f"SELECT {cols} FROM {t('gold', 'reconciliation')} ORDER BY match_key"
    ).collect()
    assert len(got) == len(local) > 900
    norm = lambda r: tuple(list(v) if isinstance(v, (list, tuple)) else v for v in r)  # noqa: E731
    assert [norm(r) for r in got] == [norm(r) for r in local]


def test_gold_totals_per_currency_match(spark, dataset, batch):
    paths, _ = dataset
    local = _duck(
        paths,
        f"SELECT currency, sum(amount_minor) FROM read_parquet('{paths.gold.as_posix()}/fact_settlements.parquet') GROUP BY 1 ORDER BY 1",
    )
    got = spark.sql(
        f"SELECT currency, sum(amount_minor) FROM {t('gold', 'fact_settlements')} GROUP BY 1 ORDER BY 1"
    ).collect()
    assert [tuple(r) for r in got] == local and len(local) > 1
    assert (
        spark.table(t("gold", "fact_payments")).count()
        == _duck(
            paths,
            f"SELECT count(*) FROM read_parquet('{paths.gold.as_posix()}/fact_payments.parquet')",
        )[0][0]
    )


def test_repeated_ingest_and_rebuild_are_idempotent(spark, dataset, batch):
    paths, _ = dataset
    before = spark.table(t("gold", "reconciliation")).orderBy("match_key").collect()
    again = engine.run_batch(spark, paths.raw, run_id="test-run-2")
    assert again["detail"]["ingest"]["records_new"] == 0
    assert spark.table(t("gold", "reconciliation")).orderBy("match_key").collect() == before


def test_iceberg_schema_evolution_is_backward_compatible(spark, batch):
    tbl = t("silver", "events")
    n = spark.table(tbl).count()
    spark.sql(f"ALTER TABLE {tbl} ADD COLUMN region STRING")
    assert "region" in spark.table(tbl).columns
    assert spark.table(tbl).count() == n  # old rows readable, new column NULL
    assert spark.table(tbl).filter("region IS NOT NULL").count() == 0
    spark.sql(f"ALTER TABLE {tbl} DROP COLUMN region")


def test_contract_1_1_channel_flows_to_silver(spark, batch):
    rows = {
        r[0]: r[1]
        for r in spark.sql(
            f"SELECT schema_version, count(channel) FROM {t('silver', 'events')} GROUP BY 1"
        ).collect()
    }
    assert rows["1.0"] == 0 and rows["1.1"] > 0
