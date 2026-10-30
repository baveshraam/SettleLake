"""Bronze ingestion: land raw input records unmodified with provenance and fingerprints."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from settlelake.config import STMT_COLS
from settlelake.lakehouse.store import write_table

BRONZE_SCHEMA = pa.schema(
    [
        ("bronze_record_id", pa.string()),
        ("batch_id", pa.string()),
        ("record_type", pa.string()),  # event | statement
        ("source_file", pa.string()),
        ("source_format", pa.string()),  # jsonl | csv | json
        ("file_fingerprint", pa.string()),
        ("line_no", pa.int64()),
        ("raw", pa.string()),
        ("ingested_at", pa.timestamp("us")),
    ]
)


class IngestionError(Exception):
    pass


@dataclass(frozen=True)
class InputFile:
    path: Path
    record_type: str
    fmt: str
    fingerprint: str

    def as_dict(self) -> dict:
        return {
            "name": self.path.name,
            "record_type": self.record_type,
            "format": self.fmt,
            "fingerprint": self.fingerprint,
        }


def discover_inputs(input_dir: Path) -> list[InputFile]:
    """Find recognised input files in a directory, fingerprinted by SHA-256 of their bytes."""
    if not input_dir.is_dir():
        raise IngestionError(
            f"input directory {input_dir} does not exist. Run `settlelake generate` first."
        )
    found: list[InputFile] = []
    patterns = [
        ("payment_events*.jsonl", "event", "jsonl"),
        ("statements_*.csv", "statement", "csv"),
        ("statements_*.json", "statement", "json"),
    ]
    for pattern, rtype, fmt in patterns:
        for p in sorted(input_dir.glob(pattern)):
            found.append(InputFile(p, rtype, fmt, hashlib.sha256(p.read_bytes()).hexdigest()))
    if not found:
        raise IngestionError(
            f"no input files found in {input_dir} (expected payment_events*.jsonl, "
            "statements_*.csv, statements_*.json). Run `settlelake generate` first."
        )
    return found


def bronze_path(bronze_dir: Path, fingerprint: str) -> Path:
    return bronze_dir / f"{fingerprint}.parquet"


def _records(f: InputFile) -> list[tuple[int, str]]:
    """(line_no, raw) pairs. Line numbers are 1-based; for JSON documents, the row index + 1."""
    text = f.path.read_text(encoding="utf-8", errors="replace")
    if f.fmt == "jsonl":
        return [(n, line) for n, line in enumerate(text.splitlines(), 1) if line.strip()]
    if f.fmt == "csv":
        lines = text.splitlines()
        if not lines or lines[0].strip() != ",".join(STMT_COLS):
            raise IngestionError(
                f"{f.path.name}: unexpected CSV header, expected {','.join(STMT_COLS)}"
            )
        return [(n, line) for n, line in enumerate(lines[1:], 2) if line.strip()]
    try:
        doc = json.loads(text)
        rows = doc["rows"]
        if not isinstance(rows, list):
            raise TypeError("'rows' is not a list")
    except (ValueError, KeyError, TypeError):
        # Unreadable document: keep it whole as one record so Silver quarantines it.
        return [(1, text)]
    return [
        (n, json.dumps(r, sort_keys=True, separators=(",", ":"))) for n, r in enumerate(rows, 1)
    ]


def ingest_file(f: InputFile, bronze_dir: Path, batch_id: str) -> int | None:
    """Write one input file to Bronze. Returns record count, or None if already ingested."""
    target = bronze_path(bronze_dir, f.fingerprint)
    if target.exists():
        return None  # repeated input detection: same bytes => same fingerprint => skip
    now = datetime.now(UTC).replace(tzinfo=None)
    recs = _records(f)
    table = pa.table(
        {
            "bronze_record_id": [f"{f.fingerprint[:16]}:{n}" for n, _ in recs],
            "batch_id": [batch_id] * len(recs),
            "record_type": [f.record_type] * len(recs),
            "source_file": [f.path.name] * len(recs),
            "source_format": [f.fmt] * len(recs),
            "file_fingerprint": [f.fingerprint] * len(recs),
            "line_no": [n for n, _ in recs],
            "raw": [r for _, r in recs],
            "ingested_at": [now] * len(recs),
        },
        schema=BRONZE_SCHEMA,
    )
    write_table(target, table)
    return len(recs)
