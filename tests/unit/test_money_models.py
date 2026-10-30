import pytest
from helpers import settle_event, stmt_row
from pydantic import ValidationError

from settlelake.schemas.models import PaymentEvent, StatementRow, make_match_key, normalize_ref
from settlelake.schemas.money import convert_minor, format_minor, to_minor


@pytest.mark.parametrize(
    "s,cur,minor",
    [
        ("12.34", "USD", 1234),
        ("0.01", "EUR", 1),
        ("500", "JPY", 500),
        ("1000000.00", "INR", 100_000_000),
    ],
)
def test_roundtrip(s, cur, minor):
    assert to_minor(s, cur) == minor
    assert to_minor(format_minor(minor, cur), cur) == minor


@pytest.mark.parametrize(
    "s,cur",
    [
        ("1.234", "USD"),
        ("1.5", "JPY"),
        ("-1.00", "USD"),
        ("0", "USD"),
        ("1e3", "USD"),
        ("abc", "USD"),
        ("1.00", "XXX"),
        ("", "USD"),
    ],
)
def test_money_rejects(s, cur):
    with pytest.raises(ValueError):
        to_minor(s, cur)


def test_no_float_drift():
    assert sum(to_minor("0.10", "USD") for _ in range(3)) == 30


def test_convert_is_integer_and_deterministic():
    a = convert_minor(123_456, "USD", "INR")
    assert isinstance(a, int) and a == convert_minor(123_456, "USD", "INR")
    assert convert_minor(100, "USD", "USD") == 100


def test_normalize_ref_variants_share_key():
    keys = {
        make_match_key("atlas", r) for r in ["ATL-REF-1", "atl-ref-1", " ATL-REF-1 ", "ATLREF1"]
    }
    assert keys == {"ATLAS|ATLREF1"}


@pytest.mark.parametrize(
    "over",
    [
        {"currency": "ZZZ"},
        {"amount_minor": -1},
        {"amount_minor": "x"},
        {"amount_minor": 1.5},
        {"provider_ref": None},
        {"ingested_at": "2026-01-05T09:00:00Z"},
        {"extra": 1},
        {"event_time": "2026-01-05T10:00:00"},
    ],
)
def test_event_rejects(over):
    with pytest.raises(ValidationError):
        PaymentEvent.model_validate(settle_event("e1", "R1", **over))


def test_event_valid_and_key():
    e = PaymentEvent.model_validate(settle_event("e1", "r-1"))
    assert e.match_key == "ATLAS|R1" and normalize_ref("r-1") == "R1"


@pytest.mark.parametrize(
    "over",
    [
        {"amount": "1.234"},
        {"currency": "ZZZ"},
        {"status": "WEIRD"},
        {"amount": "-5.00"},
        {"external_ref": "---"},
    ],
)
def test_statement_rejects(over):
    with pytest.raises(ValidationError):
        StatementRow.model_validate(stmt_row("s1", "R1", **over))


def test_statement_amount_minor():
    assert StatementRow.model_validate(stmt_row("s1", "R1", amount="12.50")).amount_minor == 1250


def test_contract_v1_1_adds_optional_channel():
    ok = PaymentEvent.model_validate(settle_event("e1", "R1", schema_version="1.1", channel="api"))
    assert ok.channel == "api"
    assert (
        PaymentEvent.model_validate(settle_event("e1", "R1", schema_version="1.1")).channel is None
    )
    with pytest.raises(ValidationError):
        PaymentEvent.model_validate(settle_event("e1", "R1", channel="api"))  # 1.0 has no channel
    with pytest.raises(ValidationError):
        PaymentEvent.model_validate(settle_event("e1", "R1", schema_version="2.0"))
