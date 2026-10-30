from conftest import settle_event as se
from conftest import stmt_row as sr


def test_matched_with_noisy_reference_formats(mini):
    out, _ = mini([se("e1", "ATL-REF-1", 10_000)], [sr("s1", " atl-ref-1 ", "100.00")])
    assert out["ATLAS|ATLREF1"] == ("MATCHED", ["MATCHED"])


def test_amount_mismatch(mini):
    out, _ = mini([se("e1", "R1", 10_000)], [sr("s1", "R1", "100.01")])
    assert out["ATLAS|R1"] == ("AMOUNT_MISMATCH", ["AMOUNT_MISMATCH"])


def test_currency_mismatch_suppresses_amount_comparison(mini):
    out, _ = mini([se("e1", "R1", 10_000, "USD")], [sr("s1", "R1", "5.00", "EUR")])
    assert out["ATLAS|R1"] == ("CURRENCY_MISMATCH", ["CURRENCY_MISMATCH"])


def test_status_mismatch(mini):
    out, _ = mini([se("e1", "R1")], [sr("s1", "R1", status="REJECTED")])
    assert out["ATLAS|R1"][0] == "STATUS_MISMATCH"


def test_missing_partner_and_missing_internal(mini):
    out, _ = mini([se("e1", "R1")], [sr("s9", "GHOST1")])
    assert out["ATLAS|R1"][0] == "MISSING_PARTNER_RECORD"
    assert out["ATLAS|GHOST1"][0] == "MISSING_INTERNAL_RECORD"


def test_delayed_settlement_uses_event_time(mini):
    out, _ = mini([se("e1", "R1")], [sr("s1", "R1", at="2026-01-09T10:00:00Z")])
    assert out["ATLAS|R1"] == ("DELAYED_SETTLEMENT", ["DELAYED_SETTLEMENT"])
    out2, _ = mini([se("e1", "R1")], [sr("s1", "R1", at="2026-01-06T10:00:00Z")])
    assert out2["ATLAS|R1"][0] == "MATCHED"


def test_multiple_reasons_preserved_in_precedence_order(mini):
    out, _ = mini([se("e1", "R1", 10_000)], [sr("s1", "R1", "100.50", at="2026-01-12T10:00:00Z")])
    assert out["ATLAS|R1"] == ("AMOUNT_MISMATCH", ["AMOUNT_MISMATCH", "DELAYED_SETTLEMENT"])


def test_duplicate_statement_is_deduped_but_flagged(mini):
    row = sr("s1", "R1")
    out, _ = mini([se("e1", "R1")], [row, dict(row)])
    assert out["ATLAS|R1"] == ("DUPLICATE_STATEMENT", ["DUPLICATE_STATEMENT"])


def test_duplicate_event_is_deduped_but_flagged(mini):
    e = se("e1", "R1")
    out, _ = mini([e, dict(e), dict(e, ingested_at="2026-01-05T11:00:00Z")], [sr("s1", "R1")])
    assert out["ATLAS|R1"] == ("DUPLICATE_EVENT", ["DUPLICATE_EVENT"])


def test_ambiguous_not_forced(mini):
    out, _ = mini(
        [se("e1", "R1", 10_000, pid="P1"), se("e2", "R1", 10_000, pid="P2")], [sr("s1", "R1")]
    )
    assert out["ATLAS|R1"] == ("AMBIGUOUS_MATCH", ["AMBIGUOUS_MATCH"])


def test_conflicting_statement_rows_are_ambiguous(mini):
    out, _ = mini([se("e1", "R1")], [sr("s1", "R1"), sr("s2", "R1")])
    assert out["ATLAS|R1"][0] == "AMBIGUOUS_MATCH"


def test_invalid_internal_and_partner_records(mini):
    out, _ = mini(
        [se("e1", "R1", currency="ZZZ"), se("e2", "R2")],
        [sr("s1", "R1"), sr("s2", "R2", cur="ZZZ")],
    )
    assert out["ATLAS|R1"] == ("INVALID_RECORD", ["INVALID_RECORD"])
    assert out["ATLAS|R2"] == ("INVALID_RECORD", ["INVALID_RECORD"])


def test_conflicting_event_id_is_invalid(mini):
    out, _ = mini([se("e1", "R1", 10_000), se("e1", "R1", 10_001)], [sr("s1", "R1")])
    assert out["ATLAS|R1"][0] == "INVALID_RECORD"


def test_late_arriving_event_still_matches_on_event_time(mini):
    out, _ = mini([se("e1", "R1", ingested="2026-01-12T10:00:00Z")], [sr("s1", "R1")])
    assert out["ATLAS|R1"][0] == "MATCHED"


def test_out_of_order_input_gives_identical_result(mini):
    evs = [se(f"e{i}", f"R{i}") for i in range(5)]
    sts = [sr(f"s{i}", f"R{i}") for i in range(5)]
    out1, _ = mini(evs, sts)
    assert {c for c, _ in out1.values()} == {"MATCHED"}
    out2, _ = mini(evs[::-1], sts[::-1])
    assert out1 == out2


def test_currencies_never_mixed(mini):
    out, _ = mini(
        [se("e1", "R1", 100, "USD"), se("e2", "R2", 100, "JPY")],
        [sr("s1", "R1", "1.00", "USD"), sr("s2", "R2", "100", "JPY")],
    )
    assert {c for c, _ in out.values()} == {"MATCHED"}
