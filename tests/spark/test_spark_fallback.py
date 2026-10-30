"""Spark/DuckDB parity for fuzzy-reference fallback matching, on the adversarial evaluation data."""

import pytest

from settlelake.config import get_paths
from settlelake.lakehouse.store import connect
from settlelake.pipeline import run_pipeline
from settlelake.reconciliation.engine import reconcile as duck_reconcile
from settlelake.simulator.evaluation import EvalConfig, generate_eval
from settlelake.spark import engine
from settlelake.spark.verify import COLS, _norm

TABLES = ["events", "statements", "duplicates", "rejected"]


@pytest.fixture(scope="module")
def eval_silver(spark, tmp_path_factory):
    home = tmp_path_factory.mktemp("evalparity")
    paths = get_paths(home)
    generate_eval(paths, EvalConfig(1500, 5))
    run_pipeline(home)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.evalsilver")
    for t in TABLES:
        df = spark.read.parquet((paths.silver / f"{t}.parquet").as_posix())
        df.writeTo(f"lake.evalsilver.{t}").createOrReplace()
    return paths


@pytest.mark.parametrize(
    "params",
    [
        {"fb_max_edit": 1, "fb_window_hours": 24, "fb_auto": 0},
        {"fb_max_edit": 1, "fb_window_hours": 24, "fb_auto": 1},
        {"fb_max_edit": 2, "fb_window_hours": 6, "fb_auto": 1},
        {"fb_max_edit": 0, "fb_window_hours": 24, "fb_auto": 1},
    ],
)
def test_fallback_reconciliation_identical_across_engines(spark, eval_silver, params):
    paths = eval_silver
    out = paths.home / f"g_{params['fb_max_edit']}_{params['fb_window_hours']}_{params['fb_auto']}"
    duck_reconcile(paths.silver, out, **params)
    con = connect()
    ref = _norm(
        con.execute(
            f"SELECT {COLS} FROM read_parquet('{(out / 'reconciliation.parquet').as_posix()}') ORDER BY match_key"
        ).fetchall()
    )
    engine.reconcile(
        spark, silver_ns="evalsilver", out_table="lake.gold.eval_reconciliation", **params
    )
    got = _norm(
        tuple(r)
        for r in spark.sql(
            f"SELECT {COLS} FROM lake.gold.eval_reconciliation ORDER BY match_key"
        ).collect()
    )
    assert len(ref) > 1000 and got == ref
    methods = {r[-3] for r in ref}
    if params["fb_auto"] == 1 and params["fb_max_edit"] > 0:
        assert "FALLBACK" in methods  # the fallback path was really exercised
