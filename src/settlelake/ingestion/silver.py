"""Silver (DuckDB engine): parse, validate, normalize and deduplicate Bronze records.

Outputs (all Parquet):
  events, statements   validated, deduplicated business records
  duplicates           exact duplicate observations (kept record is referenced)
  rejected             parsed but invalid (schema/business rule/conflicting id); key kept if known
  quarantine           unparseable records (malformed JSON/CSV)
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow as pa

from settlelake.ingestion.parse import parse_record
from settlelake.lakehouse.store import connect, quote_list, write_table

TS = pa.timestamp("us")
EVENTS_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("payment_id", pa.string()),
        ("event_time", TS),
        ("ingested_at", TS),
        ("provider", pa.string()),
        ("provider_ref", pa.string()),
        ("match_key", pa.string()),
        ("amount_minor", pa.int64()),
        ("currency", pa.string()),
        ("corridor", pa.string()),
        ("dest_currency", pa.string()),
        ("dest_amount_minor", pa.int64()),
        ("schema_version", pa.string()),
        ("source", pa.string()),
        ("channel", pa.string()),
        ("bronze_record_id", pa.string()),
    ]
)
STATEMENTS_SCHEMA = pa.schema(
    [
        ("statement_row_id", pa.string()),
        ("provider", pa.string()),
        ("external_ref", pa.string()),
        ("match_key", pa.string()),
        ("amount_minor", pa.int64()),
        ("currency", pa.string()),
        ("status", pa.string()),
        ("settled_at", TS),
        ("bronze_record_id", pa.string()),
    ]
)
DUPLICATES_SCHEMA = pa.schema(
    [
        ("obs_type", pa.string()),  # EVENT | STATEMENT
        ("natural_id", pa.string()),
        ("match_key", pa.string()),
        ("bronze_record_id", pa.string()),
        ("duplicate_of", pa.string()),
    ]
)
REJECTED_SCHEMA = pa.schema(
    [
        ("bronze_record_id", pa.string()),
        ("record_type", pa.string()),
        ("reason", pa.string()),
        ("detail", pa.string()),
        ("match_key", pa.string()),
        ("side", pa.string()),  # INTERNAL | PARTNER when the key could be recovered
        ("raw", pa.string()),
    ]
)
QUARANTINE_SCHEMA = pa.schema(
    [
        ("bronze_record_id", pa.string()),
        ("record_type", pa.string()),
        ("reason", pa.string()),
        ("detail", pa.string()),
        ("raw", pa.string()),
    ]
)

SILVER_FILES = ["events", "statements", "duplicates", "rejected", "quarantine"]
EVENT_COLS = [f.name for f in EVENTS_SCHEMA if f.name != "bronze_record_id"]
STMT_OUT_COLS = [f.name for f in STATEMENTS_SCHEMA if f.name != "bronze_record_id"]


def build_silver(bronze_files: list[Path], silver_dir: Path) -> dict:
    """Build all Silver tables from the given Bronze files. Returns row counts."""
    con = connect()
    rows = con.execute(
        f"SELECT bronze_record_id, record_type, source_format, raw FROM "
        f"read_parquet({quote_list(bronze_files)}) ORDER BY source_file, line_no"
    ).fetchall()
    con.close()

    events: dict[str, list[tuple[dict, str, str]]] = defaultdict(list)
    statements: list[tuple[dict, str, str]] = []
    rejected: list[dict] = []
    quarantine: list[dict] = []

    for rid, rtype, fmt, raw in rows:
        p = parse_record(rtype, fmt, raw)
        if p["outcome"] == "event":
            events[p["event_id"]].append((p, rid, raw))
        elif p["outcome"] == "statement":
            statements.append((p, rid, raw))
        elif p["outcome"] == "rejected":
            rejected.append(
                dict(
                    bronze_record_id=rid,
                    record_type=rtype,
                    reason=p["reason"],
                    detail=p["detail"],
                    match_key=p["match_key"],
                    side=p["side"],
                    raw=raw,
                )  # fmt: skip
            )
        else:
            quarantine.append(
                dict(
                    bronze_record_id=rid,
                    record_type=rtype,
                    reason=p["reason"],
                    detail=p["detail"],
                    raw=raw,
                )  # fmt: skip
            )

    # ---- events: dedupe identical content, reject conflicting duplicates of an event_id ----
    ev_rows: list[dict] = []
    dups: list[dict] = []
    for eid in sorted(events):
        versions = events[eid]
        if len({v[0]["sig"] for v in versions}) > 1:
            for p, rid, raw in versions:
                key = p["match_key"]
                rejected.append(
                    dict(
                        bronze_record_id=rid,
                        record_type="event",
                        reason="CONFLICTING_EVENT_ID",
                        detail=f"event_id {eid} seen with {len(versions)} differing versions",
                        match_key=key,
                        side="INTERNAL" if key else None,
                        raw=raw,
                    )  # fmt: skip
                )
            continue
        keep, keep_rid, _ = versions[0]
        for _, rid, _ in versions[1:]:
            dups.append(
                dict(
                    obs_type="EVENT",
                    natural_id=eid,
                    match_key=keep["match_key"],
                    bronze_record_id=rid,
                    duplicate_of=keep_rid,
                )  # fmt: skip
            )
        ev_rows.append({**{c: keep[c] for c in EVENT_COLS}, "bronze_record_id": keep_rid})

    # ---- statements: dedupe rows with identical business content ----
    st_rows: list[dict] = []
    seen: dict[str, str] = {}
    for p, rid, _ in statements:
        if p["sig"] in seen:
            dups.append(
                dict(
                    obs_type="STATEMENT",
                    natural_id=p["statement_row_id"],
                    match_key=p["match_key"],
                    bronze_record_id=rid,
                    duplicate_of=seen[p["sig"]],
                )  # fmt: skip
            )
            continue
        seen[p["sig"]] = rid
        st_rows.append({**{c: p[c] for c in STMT_OUT_COLS}, "bronze_record_id": rid})

    ev_rows.sort(key=lambda r: (r["event_time"], r["event_id"]))
    st_rows.sort(key=lambda r: (r["settled_at"], r["statement_row_id"], r["bronze_record_id"]))
    tables = {
        "events": pa.Table.from_pylist(ev_rows, schema=EVENTS_SCHEMA),
        "statements": pa.Table.from_pylist(st_rows, schema=STATEMENTS_SCHEMA),
        "duplicates": pa.Table.from_pylist(dups, schema=DUPLICATES_SCHEMA),
        "rejected": pa.Table.from_pylist(rejected, schema=REJECTED_SCHEMA),
        "quarantine": pa.Table.from_pylist(quarantine, schema=QUARANTINE_SCHEMA),
    }
    for name, t in tables.items():
        write_table(silver_dir / f"{name}.parquet", t)
    return {"bronze_records": len(rows), **{name: t.num_rows for name, t in tables.items()}}
