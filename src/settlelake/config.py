"""Static reference data, profiles and filesystem layout."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SCHEMA_VERSION = "1.0"

# ISO-4217 minor-unit exponents for the supported currencies.
CURRENCIES: dict[str, int] = {
    "USD": 2,
    "EUR": 2,
    "GBP": 2,
    "INR": 2,
    "AED": 2,
    "SGD": 2,
    "PHP": 2,
    "JPY": 0,
}
# Fixed synthetic reference rates (units of currency per 1 USD) used only to derive
# destination amounts of synthetic payments. Never used to aggregate across currencies.
USD_RATES: dict[str, str] = {
    "USD": "1",
    "EUR": "0.92",
    "GBP": "0.79",
    "INR": "83.2",
    "AED": "3.6725",
    "SGD": "1.35",
    "PHP": "56.4",
    "JPY": "149.5",
}
CORRIDORS: list[tuple[str, str]] = [
    ("USD", "INR"),
    ("USD", "PHP"),
    ("GBP", "INR"),
    ("EUR", "INR"),
    ("AED", "INR"),
    ("SGD", "PHP"),
    ("GBP", "EUR"),
    ("EUR", "PHP"),
    ("USD", "JPY"),
    ("USD", "EUR"),
]
# provider -> statement file format
PROVIDERS: dict[str, str] = {
    "ATLAS": "csv",
    "NORDPAY": "json",
    "SAKURA": "csv",
    "GULFCLR": "json",
}
EVENT_TYPES = ["payment.created", "compliance.cleared", "funds.settled", "payout.delivered"]
STMT_COLS = [
    "statement_row_id",
    "provider",
    "external_ref",
    "amount",
    "currency",
    "status",
    "settled_at",
]
STATEMENT_STATUSES = ["SETTLED", "REJECTED", "RETURNED", "PENDING"]

PROFILES: dict[str, int] = {"quick": 1_000, "demo": 10_000, "benchmark": 100_000}
ANOMALY_LEVELS = ["clean", "low", "stress"]

# A partner settlement later than this after the internal settlement is DELAYED_SETTLEMENT.
DELAY_SLA_HOURS = 48

# Fallback matching of rows whose references differ: max Levenshtein distance between normalised
# references (0 disables it) and max |partner settled_at - internal event_time|. Values chosen on the
# development seeds only (see docs/evaluation.md).
FALLBACK_MAX_EDIT = 1
FALLBACK_WINDOW_HOURS = 24
# 1 = a unique mutual fuzzy candidate is matched automatically; 0 = it is only suggested (AMBIGUOUS_MATCH).
FALLBACK_AUTO = 0

# Reconciliation classification precedence (first match becomes the primary class).
PRECEDENCE = [
    "INVALID_RECORD",
    "AMBIGUOUS_MATCH",
    "MISSING_INTERNAL_RECORD",
    "MISSING_PARTNER_RECORD",
    "CURRENCY_MISMATCH",
    "AMOUNT_MISMATCH",
    "STATUS_MISMATCH",
    "DELAYED_SETTLEMENT",
    "DUPLICATE_EVENT",
    "DUPLICATE_STATEMENT",
]
ALL_CLASSES = [*PRECEDENCE, "MATCHED"]


@dataclass(frozen=True)
class Paths:
    home: Path

    @property
    def raw(self) -> Path:
        return self.home / "raw"

    @property
    def ground_truth(self) -> Path:
        # Deliberately outside raw/: the pipeline never reads this directory.
        return self.home / "ground_truth"

    @property
    def lake(self) -> Path:
        return self.home / "lake"

    @property
    def bronze(self) -> Path:
        return self.lake / "bronze"

    @property
    def silver(self) -> Path:
        return self.lake / "silver"

    @property
    def gold(self) -> Path:
        return self.lake / "gold"

    @property
    def meta_db(self) -> Path:
        return self.home / "meta.duckdb"

    @property
    def reports(self) -> Path:
        return self.home / "reports"

    @property
    def replays(self) -> Path:
        return self.home / "replays"


def get_home(home: str | Path | None = None) -> Path:
    return Path(home or os.environ.get("SETTLELAKE_HOME") or "data").resolve()


def get_paths(home: str | Path | None = None) -> Paths:
    return Paths(get_home(home))
