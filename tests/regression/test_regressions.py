import duckdb

from settlelake.config import get_paths
from settlelake.simulator.generator import GenConfig, generate


def test_event_ids_unique_per_lifecycle_stage(tmp_path):
    """Regression: payment.created and payout.delivered once shared an id suffix and collided."""
    p = get_paths(tmp_path)
    generate(p, GenConfig(200, 1, "clean"))
    n, d = (
        duckdb.connect()
        .execute(
            "SELECT count(*), count(DISTINCT j->>'event_id') FROM (SELECT CAST(json AS JSON) j FROM "
            f"read_json_objects('{(p.raw / 'payment_events.jsonl').as_posix()}'))"
        )
        .fetchone()
    )
    assert n == d


def test_statement_files_use_both_formats(tmp_path):
    p = get_paths(tmp_path)
    generate(p, GenConfig(200, 1, "clean"))
    assert {f.suffix for f in p.raw.glob("statements_*")} == {".csv", ".json"}
