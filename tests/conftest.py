import duckdb
import pytest
from helpers import write_inputs

from settlelake.config import get_paths
from settlelake.pipeline import run_pipeline
from settlelake.simulator.generator import GenConfig, generate


@pytest.fixture
def mini(tmp_path):
    """Run the pipeline on hand-written inputs; return ({match_key: (class, reasons)}, result)."""

    def _run(events, stmts, raw_event_lines=()):
        write_inputs(tmp_path, events, stmts, raw_event_lines)
        res = run_pipeline(tmp_path)
        p = get_paths(tmp_path)
        con = duckdb.connect()
        rows = con.execute(
            "SELECT match_key, classification, reasons FROM "
            f"read_parquet('{p.gold.as_posix()}/reconciliation.parquet')"
        ).fetchall()
        return {k: (c, list(r)) for k, c, r in rows}, res

    _run.home = tmp_path
    return _run


@pytest.fixture(scope="session")
def quick_env(tmp_path_factory):
    home = tmp_path_factory.mktemp("quick")
    paths = get_paths(home)
    manifest = generate(paths, GenConfig(1000, 42, "stress"))
    res = run_pipeline(home)
    return home, paths, manifest, res
