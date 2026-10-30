"""Integer minor-unit money helpers. Floating point is never used for amounts."""

from __future__ import annotations

import re
from decimal import ROUND_HALF_EVEN, Decimal

from settlelake.config import CURRENCIES, USD_RATES

_AMOUNT_RE = re.compile(r"^\d+(\.\d+)?$")


def to_minor(amount: str, currency: str) -> int:
    """Parse a positive decimal string such as '1234.50' into integer minor units.

    Rejects scientific notation, negatives, zero, and more decimals than the currency allows.
    """
    if currency not in CURRENCIES:
        raise ValueError(f"unsupported currency {currency!r}")
    if not isinstance(amount, str) or not _AMOUNT_RE.match(amount.strip()):
        raise ValueError(f"invalid amount {amount!r}")
    exp = CURRENCIES[currency]
    scaled = Decimal(amount.strip()).scaleb(exp)
    if scaled != scaled.to_integral_value():
        raise ValueError(f"amount {amount!r} has more than {exp} decimals for {currency}")
    minor = int(scaled)
    if minor <= 0:
        raise ValueError("amount must be positive")
    return minor


def format_minor(minor: int, currency: str) -> str:
    """Render integer minor units as a decimal string (inverse of to_minor)."""
    exp = CURRENCIES[currency]
    if exp == 0:
        return str(minor)
    sign = "-" if minor < 0 else ""
    whole, frac = divmod(abs(minor), 10**exp)
    return f"{sign}{whole}.{frac:0{exp}d}"


def convert_minor(minor: int, src: str, dst: str) -> int:
    """Convert using the fixed synthetic rate table, banker's rounding, pure Decimal."""
    rate = Decimal(USD_RATES[dst]) / Decimal(USD_RATES[src])
    major = Decimal(minor).scaleb(-CURRENCIES[src]) * rate
    return int(major.scaleb(CURRENCIES[dst]).quantize(Decimal(1), rounding=ROUND_HALF_EVEN))
