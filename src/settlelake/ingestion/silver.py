"""Silver: parse, validate, normalize and deduplicate Bronze records.

Outputs (all Parquet):
  events, statements   validated, deduplicated business records
  duplicates           exact duplicate observations (kept record is referenced)
  rejected             parsed but invalid (schema/business rule/conflicting id); key kept if known
  quarantine           unparseable records (malformed JSON/CSV)
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
from pydantic import ValidationError

from settlelake.config import STMT_COLS
from settlelake.lakehouse.store import connect, quote_list, write_table
from settlelake.schemas.models import PaymentEvent, StatementRow, make_match_key, to_naive_utc

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


def _best_effort_key(raw_obj: object, record_type: str) -> str | None:
    """Recover a match key from an invalid-but-parseable record so it can be flagged later."""
    if not isinstance(raw_obj, dict):
        return None
    provider = raw_obj.get("provider")
    if record_type == "event":
        ref = raw_obj.get("provider_ref")
        if raw_obj.get("event_type") != "funds.settled":
            return None
    else:
        ref = raw_obj.get("external_ref")
    if isinstance(provider, str) and isinstance(ref, str) and provider.strip() and ref.strip():
        key = make_match_key(provider, ref)
        return key if not key.endswith("|") else None
    return None


def _err(exc: ValidationError | ValueError) -> str:
    if isinstance(exc, ValidationError):
        return "; ".join(
            f"{'.'.join(map(str, e['loc'])) or 'record'}: {e['msg']}" for e in exc.errors()[:3]
        )
    return str(exc)


def _is_malformed(exc: ValidationError) -> bool:
    return any(e["type"] == "json_invalid" for e in exc.errors())


def build_silver(bronze_files: list[Path], silver_dir: Path) -> dict:
    """Build all Silver tables from the given Bronze files. Returns row counts."""
    con = connect()
    rows = con.execute(
        f"SELECT bronze_record_id, record_type, source_format, raw FROM "
        f"read_parquet({quote_list(bronze_files)}) ORDER BY source_file, line_no"
    ).fetchall()
    con.close()

    events: dict[str, list[tuple[PaymentEvent, str]]] = defaultdict(list)
    statements: list[tuple[StatementRow, str]] = []
    rejected: list[dict] = []
    quarantine: list[dict] = []

    def reject(rid, rtype, reason, detail, raw, key=None):
        side = ("INTERNAL" if rtype == "event" else "PARTNER") if key else None
        rejected.append(
            dict(
                bronze_record_id=rid,
                record_type=rtype,
                reason=reason,
                detail=detail[:500],
                match_key=key,
                side=side,
                raw=raw,
            )
        )

    def quarantine_(rid, rtype, reason, detail, raw):
        quarantine.append(
            dict(
                bronze_record_id=rid, record_type=rtype, reason=reason, detail=detail[:500], raw=raw
            )
        )

    for rid, rtype, fmt, raw in rows:
        if rtype == "event":
            try:
                ev = PaymentEvent.model_validate_json(raw)
                events[ev.event_id].append((ev, rid))
            except ValidationError as exc:
                if _is_malformed(exc):
                    quarantine_(rid, rtype, "MALFORMED_JSON", _err(exc), raw)
                else:
                    try:
                        key = _best_effort_key(json.loads(raw), rtype)
                    except ValueError:
                        key = None
                    reject(rid, rtype, "SCHEMA_VALIDATION", _err(exc), raw, key)
            continue

        # statements: csv line or canonical json row
        try:
            if fmt == "csv":
                cells = next(csv.reader([raw]))
                if len(cells) != len(STMT_COLS):
                    quarantine_(
                        rid,
                        rtype,
                        "MALFORMED_CSV",
                        f"{len(cells)} columns, expected {len(STMT_COLS)}",
                        raw,
                    )
                    continue
                obj = dict(zip(STMT_COLS, cells, strict=True))
            else:
                obj = json.loads(raw)
                if not isinstance(obj, dict):
                    quarantine_(rid, rtype, "MALFORMED_JSON", "row is not an object", raw)
                    continue
        except (ValueError, csv.Error, StopIteration) as exc:
            quarantine_(rid, rtype, "MALFORMED_" + fmt.upper(), str(exc), raw)
            continue
        try:
            statements.append((StatementRow.model_validate(obj), rid))
        except ValidationError as exc:
            reject(rid, rtype, "SCHEMA_VALIDATION", _err(exc), raw, _best_effort_key(obj, rtype))

    # ---- events: dedupe identical content, reject conflicting duplicates of an event_id ----
    ev_rows: list[dict] = []
    dups: list[dict] = []
    for eid in sorted(events):
        versions = events[eid]
        if len({e.content_signature() for e, _ in versions}) > 1:
            for e, rid in versions:
                reject(
                    rid,
                    "event",
                    "CONFLICTING_EVENT_ID",
                    f"event_id {eid} seen with {len(versions)} differing versions",
                    e.model_dump_json(),
                    e.match_key,
                )
            continue
        keep, keep_rid = versions[0]
        for _, rid in versions[1:]:
            dups.append(
                dict(
                    obs_type="EVENT",
                    natural_id=eid,
                    match_key=keep.match_key,
                    bronze_record_id=rid,
                    duplicate_of=keep_rid,
                )
            )
        ev_rows.append(
            dict(
                event_id=keep.event_id,
                event_type=keep.event_type,
                payment_id=keep.payment_id,
                event_time=to_naive_utc(keep.event_time),
                ingested_at=to_naive_utc(keep.ingested_at),
                provider=keep.provider,
                provider_ref=keep.provider_ref,
                match_key=keep.match_key,
                amount_minor=keep.amount_minor,
                currency=keep.currency,
                corridor=keep.corridor,
                dest_currency=keep.dest_currency,
                dest_amount_minor=keep.dest_amount_minor,
                schema_version=keep.schema_version,
                source=keep.source,
                bronze_record_id=keep_rid,
            )
        )

    # ---- statements: dedupe rows with identical business content ----
    st_rows: list[dict] = []
    seen: dict[tuple, str] = {}
    for s, rid in statements:
        sig = (
            s.statement_row_id,
            s.match_key,
            s.amount_minor,
            s.currency,
            s.status,
            to_naive_utc(s.settled_at),
        )
        if sig in seen:
            dups.append(
                dict(
                    obs_type="STATEMENT",
                    natural_id=s.statement_row_id,
                    match_key=s.match_key,
                    bronze_record_id=rid,
                    duplicate_of=seen[sig],
                )
            )
            continue
        seen[sig] = rid
        st_rows.append(
            dict(
                statement_row_id=s.statement_row_id,
                provider=s.provider.strip().upper(),
                external_ref=s.external_ref,
                match_key=s.match_key,
                amount_minor=s.amount_minor,
                currency=s.currency,
                status=s.status,
                settled_at=to_naive_utc(s.settled_at),
                bronze_record_id=rid,
            )
        )

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
    return {
        "bronze_records": len(rows),
        **{name: t.num_rows for name, t in tables.items()},
    }
