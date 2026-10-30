"""Record-level parsing and validation shared by the DuckDB and Spark engines.

`parse_record` is a pure function of (record_type, source_format, raw) so both engines apply the
exact same data contract and produce the same outcomes.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime

from pydantic import ValidationError

from settlelake.config import STMT_COLS
from settlelake.schemas.models import PaymentEvent, StatementRow, make_match_key, to_naive_utc

# outcome: event | statement | rejected | quarantine
FIELDS = [
    "outcome", "reason", "detail", "match_key", "side",
    "event_id", "event_type", "payment_id", "event_time", "ingested_at", "provider",
    "provider_ref", "amount_minor", "currency", "corridor", "dest_currency",
    "dest_amount_minor", "schema_version", "source", "channel",
    "statement_row_id", "external_ref", "status", "settled_at", "sig",
]  # fmt: skip


def _blank() -> dict:
    return dict.fromkeys(FIELDS)


def best_effort_key(obj: object, record_type: str) -> str | None:
    """Recover a match key from an invalid-but-parseable record so it can be flagged later."""
    if not isinstance(obj, dict):
        return None
    provider = obj.get("provider")
    if record_type == "event":
        ref = obj.get("provider_ref")
        if obj.get("event_type") != "funds.settled":
            return None
    else:
        ref = obj.get("external_ref")
    if isinstance(provider, str) and isinstance(ref, str) and provider.strip() and ref.strip():
        key = make_match_key(provider, ref)
        return None if key.endswith("|") else key
    return None


def _err(exc: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(map(str, e['loc'])) or 'record'}: {e['msg']}" for e in exc.errors()[:3]
    )


def _out(outcome: str, reason=None, detail=None, key=None, rtype="event") -> dict:
    d = _blank()
    d["outcome"], d["reason"], d["detail"] = outcome, reason, (detail or "")[:500] or None
    d["match_key"] = key
    d["side"] = ("INTERNAL" if rtype == "event" else "PARTNER") if key else None
    return d


def parse_record(record_type: str, fmt: str, raw: str) -> dict:
    """Validate one Bronze record. Returns a flat dict with every key in FIELDS."""
    if record_type == "event":
        try:
            ev = PaymentEvent.model_validate_json(raw)
        except ValidationError as exc:
            if any(e["type"] == "json_invalid" for e in exc.errors()):
                return _out("quarantine", "MALFORMED_JSON", _err(exc))
            try:
                key = best_effort_key(json.loads(raw), record_type)
            except ValueError:
                key = None
            return _out("rejected", "SCHEMA_VALIDATION", _err(exc), key)
        d = _blank()
        d.update(
            outcome="event", event_id=ev.event_id, event_type=ev.event_type,
            payment_id=ev.payment_id, event_time=to_naive_utc(ev.event_time),
            ingested_at=to_naive_utc(ev.ingested_at), provider=ev.provider,
            provider_ref=ev.provider_ref, match_key=ev.match_key, amount_minor=ev.amount_minor,
            currency=ev.currency, corridor=ev.corridor, dest_currency=ev.dest_currency,
            dest_amount_minor=ev.dest_amount_minor, schema_version=ev.schema_version,
            source=ev.source, channel=ev.channel, sig=repr(ev.content_signature()),
        )  # fmt: skip
        return d

    try:
        if fmt == "csv":
            cells = next(csv.reader([raw]))
            if len(cells) != len(STMT_COLS):
                return _out(
                    "quarantine", "MALFORMED_CSV",
                    f"{len(cells)} columns, expected {len(STMT_COLS)}", rtype=record_type,
                )  # fmt: skip
            obj = dict(zip(STMT_COLS, cells, strict=True))
        else:
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                return _out("quarantine", "MALFORMED_JSON", "row is not an object")
    except (ValueError, csv.Error, StopIteration) as exc:
        return _out("quarantine", "MALFORMED_" + fmt.upper(), str(exc))
    try:
        s = StatementRow.model_validate(obj)
    except ValidationError as exc:
        return _out(
            "rejected", "SCHEMA_VALIDATION", _err(exc), best_effort_key(obj, record_type), "statement"
        )  # fmt: skip
    settled: datetime = to_naive_utc(s.settled_at)
    d = _blank()
    d.update(
        outcome="statement", statement_row_id=s.statement_row_id,
        provider=s.provider.strip().upper(), external_ref=s.external_ref, match_key=s.match_key,
        amount_minor=s.amount_minor, currency=s.currency, status=s.status, settled_at=settled,
        sig=repr((s.statement_row_id, s.match_key, s.amount_minor, s.currency, s.status, settled)),
    )  # fmt: skip
    return d
