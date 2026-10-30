"""Split partner statement files into per-day landing partitions (dt=YYYY-MM-DD).

Gives Airflow date-partitioned inputs so a backfill can (re)load exactly one day at a time. Rows are
copied verbatim (CSV lines and canonical JSON rows); unparseable rows go to the `dt=unknown`
partition so nothing is silently dropped.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

from settlelake.config import STMT_COLS


def _day(settled_at: object) -> str:
    s = str(settled_at or "")
    return s[:10] if len(s) >= 10 and s[4] == "-" and s[7] == "-" else "unknown"


def split_statements(raw: Path, landing: Path) -> dict:
    counts: dict[str, int] = defaultdict(int)
    for f in sorted([*raw.glob("statements_*.csv"), *raw.glob("statements_*.json")]):
        by_day: dict[str, list[str]] = defaultdict(list)
        text = f.read_text(encoding="utf-8")
        if f.suffix == ".csv":
            last = None
            for line in text.splitlines()[1:]:
                if not line.strip():
                    continue
                cells = next(csv.reader([line]), [])
                if len(cells) == len(STMT_COLS):
                    last = _day(cells[STMT_COLS.index("settled_at")])
                # malformed rows stay with their neighbours' day so they still reach Bronze
                by_day[last or "unknown"].append(line)
        else:
            for row in json.loads(text)["rows"]:
                by_day[_day(row.get("settled_at") if isinstance(row, dict) else None)].append(
                    json.dumps(row, sort_keys=True, separators=(",", ":"))
                )
        for day, rows in by_day.items():
            d = landing / f"dt={day}"
            d.mkdir(parents=True, exist_ok=True)
            if f.suffix == ".csv":
                body = ",".join(STMT_COLS) + "\n" + "\n".join(rows) + "\n"
            else:
                body = '{"rows":[\n' + ",\n".join(rows) + "\n]}\n"
            (d / f.name).write_text(body, encoding="utf-8", newline="\n")
            counts[day] += len(rows)
    return dict(sorted(counts.items()))
