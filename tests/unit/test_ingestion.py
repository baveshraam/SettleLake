import duckdb
import pytest
from helpers import settle_event as se
from helpers import stmt_row as sr
from helpers import write_inputs

from settlelake.config import get_paths
from settlelake.ingestion.bronze import IngestionError, discover_inputs
from settlelake.pipeline import PipelineError, run_pipeline


def test_malformed_lines_quarantined_and_originals_preserved(mini):
    bad = ['{"event_id": "TRUNC', "garbage"]
    out, _ = mini([se("e1", "R1")], [sr("s1", "R1")], raw_event_lines=bad)
    p = get_paths(mini.home)
    con = duckdb.connect()
    q = con.execute(
        f"SELECT raw, reason FROM read_parquet('{p.silver.as_posix()}/quarantine.parquet') ORDER BY raw"
    ).fetchall()
    assert [r[0] for r in q] == sorted(bad) and all(r[1] == "MALFORMED_JSON" for r in q)
    raws = {
        r[0]
        for r in con.execute(
            f"SELECT raw FROM read_parquet('{p.bronze.as_posix()}/*.parquet')"
        ).fetchall()
    }
    assert set(bad) <= raws  # Bronze keeps every original record, including malformed ones
    assert out["ATLAS|R1"][0] == "MATCHED"


def test_repeated_input_detected(mini):
    mini([se("e1", "R1")], [sr("s1", "R1")])
    res2 = run_pipeline(mini.home)
    d = res2.stages["ingest"]["detail"]
    assert d["repeated_inputs"] == d["input_files"] == 2
    assert res2.stages["ingest"]["rows_out"] == 0
    assert len(list(get_paths(mini.home).bronze.glob("*.parquet"))) == 2


def test_bad_csv_header_gives_useful_error(tmp_path):
    raw = get_paths(tmp_path).raw
    raw.mkdir(parents=True)
    (raw / "statements_X.csv").write_text("a,b\n1,2\n")
    with pytest.raises(PipelineError, match="unexpected CSV header"):
        run_pipeline(tmp_path)


def test_empty_input_dir_errors_with_hint(tmp_path):
    with pytest.raises(IngestionError, match="settlelake generate"):
        discover_inputs(tmp_path)


def test_csv_ingestion_with_malformed_row(tmp_path):
    write_inputs(tmp_path, [se("e1", "R1")], [])
    raw = get_paths(tmp_path).raw
    (raw / "statements_ATLAS.json").unlink()
    (raw / "statements_SAKURA.csv").write_text(
        "statement_row_id,provider,external_ref,amount,currency,status,settled_at\n"
        "s1,ATLAS,R1,100.00,USD,SETTLED,2026-01-05T12:00:00Z\nonly,three,cols\n"
    )
    res = run_pipeline(tmp_path)
    p = get_paths(tmp_path)
    got = (
        duckdb.connect()
        .execute(f"SELECT reason FROM read_parquet('{p.silver.as_posix()}/quarantine.parquet')")
        .fetchall()
    )
    assert got == [("MALFORMED_CSV",)] and res.status == "COMPLETED"
