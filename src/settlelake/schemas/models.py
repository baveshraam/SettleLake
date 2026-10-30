"""Pydantic data contracts for inbound payment events and partner statement rows."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    StrictInt,
    field_validator,
    model_validator,
)

from settlelake.config import CURRENCIES, STATEMENT_STATUSES
from settlelake.schemas.money import to_minor

_NON_ALNUM = re.compile(r"[^A-Z0-9]")


def normalize_ref(ref: str) -> str:
    """Normalize a provider/external reference: upper-case, alphanumerics only."""
    return _NON_ALNUM.sub("", ref.upper())


def make_match_key(provider: str, ref: str) -> str:
    return f"{provider.strip().upper()}|{normalize_ref(ref)}"


def to_naive_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None)


def _no_pipe(v: str) -> str:
    if not v.strip() or "|" in v:
        raise ValueError("must be non-empty and must not contain '|'")
    return v


class PaymentEvent(BaseModel):
    """Payment lifecycle event (JSONL). amount/currency are those relevant to the event type:
    created/cleared/settled use the source (settlement) currency, payout uses the destination."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: Literal[
        "payment.created", "compliance.cleared", "funds.settled", "payout.delivered"
    ]
    payment_id: str
    event_time: AwareDatetime
    ingested_at: AwareDatetime
    schema_version: Literal["1.0"]
    source: str
    provider: str
    provider_ref: str | None = None
    amount_minor: StrictInt
    currency: str
    corridor: str
    dest_currency: str | None = None
    dest_amount_minor: StrictInt | None = None

    @field_validator("event_id", "payment_id", "source", "provider", "corridor")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        return _no_pipe(v)

    @field_validator("currency", "dest_currency")
    @classmethod
    def _currency(cls, v: str | None) -> str | None:
        if v is not None and v not in CURRENCIES:
            raise ValueError(f"unsupported currency {v!r}")
        return v

    @model_validator(mode="after")
    def _consistency(self) -> PaymentEvent:
        if self.amount_minor <= 0:
            raise ValueError("amount_minor must be positive")
        if self.ingested_at < self.event_time:
            raise ValueError("ingested_at precedes event_time")
        if self.event_type == "funds.settled" and not (
            self.provider_ref and normalize_ref(self.provider_ref)
        ):
            raise ValueError("funds.settled requires provider_ref")
        if (self.dest_currency is None) != (self.dest_amount_minor is None):
            raise ValueError("dest_currency and dest_amount_minor must be set together")
        return self

    @property
    def match_key(self) -> str | None:
        if self.event_type == "funds.settled" and self.provider_ref:
            return make_match_key(self.provider, self.provider_ref)
        return None

    def content_signature(self) -> tuple:
        """Business content (excludes ingested_at, so a retried delivery is a duplicate)."""
        d = self.model_dump(mode="json")
        d.pop("ingested_at")
        return tuple(sorted(d.items()))


class StatementRow(BaseModel):
    """Partner settlement statement row. `amount` is a decimal string in major units."""

    model_config = ConfigDict(extra="forbid")

    statement_row_id: str
    provider: str
    external_ref: str
    amount: str
    currency: str
    status: str
    settled_at: AwareDatetime

    @field_validator("statement_row_id", "provider", "external_ref")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        return _no_pipe(v)

    @field_validator("status")
    @classmethod
    def _status(cls, v: str) -> str:
        if v not in STATEMENT_STATUSES:
            raise ValueError(f"unsupported status {v!r}")
        return v

    @model_validator(mode="after")
    def _amount(self) -> StatementRow:
        if not normalize_ref(self.external_ref):
            raise ValueError("external_ref has no alphanumeric characters")
        to_minor(self.amount, self.currency)  # raises ValueError on bad amount/currency
        return self

    @property
    def amount_minor(self) -> int:
        return to_minor(self.amount, self.currency)

    @property
    def match_key(self) -> str:
        return make_match_key(self.provider, self.external_ref)
