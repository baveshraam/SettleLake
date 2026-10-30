"""Structured JSON logging shared by the CLI, Spark jobs and the streaming job."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

_STD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": datetime.fromtimestamp(record.created, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        out.update({k: v for k, v in record.__dict__.items() if k not in _STD})
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def setup_logging(level: int = logging.INFO) -> None:
    """Idempotently route the `settlelake` logger tree to stderr, one JSON object per line."""
    log = logging.getLogger("settlelake")
    if any(getattr(h, "_settlelake", False) for h in log.handlers):
        return
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(JsonFormatter())
    h._settlelake = True  # type: ignore[attr-defined]
    log.addHandler(h)
    log.setLevel(level)
    log.propagate = False
