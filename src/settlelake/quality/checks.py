"""Data quality checks over Bronze/Silver/Gold. Every check is a SQL query returning
(evaluated_count, failed_count). ERROR checks are invariants that must hold for any input
(failure means a pipeline bug); WARN checks monitor source-data health and are expected to
fire when anomalies are present."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from pathlib import Path

from settlelake.config import CURRENCIES, STATEMENT_STATUSES
from settlelake.lakehouse.store import connect, quote_list


@dataclass
class CheckResult:
    check_name: str
    layer: str
    severity: str  # ERROR | WARN
    status: str  # PASS | FAIL | WARN
    evaluated_count: int
    failed_count: int
    duration_ms: float
    explanation: str

    def as_dict(self) -> dict:
        return asdict(self)


def _checks(s: str, g: str, bronze: str) -> list[tuple[str, str, str, str, str]]:
    """(name, layer, severity, explanation, sql)."""
    ev, st = f"read_parquet('{s}/events.parquet')", f"read_parquet('{s}/statements.parquet')"
    rec = f"read_parquet('{g}/reconciliation.parquet')"
    cur = ", ".join(f"'{c}'" for c in CURRENCIES)
    stat = ", ".join(f"'{c}'" for c in STATEMENT_STATUSES)
    return [
        (
            "silver_events_mandatory_fields",
            "silver",
            "ERROR",
            "event_id, event_type, payment_id, event_time, ingested_at, provider, amount_minor, "
            "currency, corridor must be non-null",
            f"""SELECT count(*), count(*) FILTER (WHERE event_id IS NULL OR event_type IS NULL
             OR payment_id IS NULL OR event_time IS NULL OR ingested_at IS NULL OR provider IS NULL
             OR amount_minor IS NULL OR currency IS NULL OR corridor IS NULL) FROM {ev}""",
        ),
        (
            "silver_events_unique_event_id",
            "silver",
            "ERROR",
            "event_id must be unique after deduplication",
            f"SELECT count(*), count(*) - count(DISTINCT event_id) FROM {ev}",
        ),
        (
            "silver_events_valid_currency",
            "silver",
            "ERROR",
            "currency and dest_currency must be supported ISO codes",
            f"""SELECT count(*), count(*) FILTER (WHERE currency NOT IN ({cur})
             OR (dest_currency IS NOT NULL AND dest_currency NOT IN ({cur}))) FROM {ev}""",
        ),
        (
            "silver_events_timestamp_validity",
            "silver",
            "ERROR",
            "ingested_at must not precede event_time; amounts must be positive",
            f"SELECT count(*), count(*) FILTER (WHERE ingested_at < event_time OR amount_minor <= 0) FROM {ev}",
        ),
        (
            "silver_statements_valid",
            "silver",
            "ERROR",
            "statement rows need supported currency/status, positive amount and a unique row id",
            f"""SELECT count(*), count(*) FILTER (WHERE currency NOT IN ({cur}) OR status NOT IN ({stat})
             OR amount_minor <= 0) + (count(*) - count(DISTINCT statement_row_id)) FROM {st}""",
        ),
        (
            "gold_reconciliation_unique_key",
            "gold",
            "ERROR",
            "exactly one reconciliation row per match key",
            f"SELECT count(*), count(*) - count(DISTINCT match_key) FROM {rec}",
        ),
        (
            "gold_reconciliation_covers_silver",
            "gold",
            "ERROR",
            "every internal settlement and partner statement key appears in reconciliation",
            f"""SELECT count(*), count(*) FILTER (WHERE match_key NOT IN (SELECT match_key FROM {rec}))
             FROM (SELECT match_key FROM {ev} WHERE event_type = 'funds.settled'
                   UNION SELECT match_key FROM {st})""",
        ),
        (
            "gold_matched_totals_tie_out",
            "gold",
            "ERROR",
            "per currency, internal and partner totals of MATCHED rows must be identical",
            f"""SELECT count(*), count(*) FILTER (WHERE i <> p) FROM (
             SELECT internal_currency, sum(internal_amount_minor) i, sum(partner_amount_minor) p
             FROM {rec} WHERE classification = 'MATCHED' GROUP BY internal_currency)""",
        ),
        (
            "gold_settlements_tie_to_silver",
            "gold",
            "ERROR",
            "per currency, fact_settlements totals equal Silver funds.settled totals",
            f"""SELECT count(*), count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
             SELECT s.currency, s.t a, g.t b FROM
               (SELECT currency, sum(amount_minor) t FROM {ev} WHERE event_type = 'funds.settled'
                GROUP BY currency) s
               FULL JOIN (SELECT currency, sum(amount_minor) t
                          FROM read_parquet('{g}/fact_settlements.parquet') GROUP BY currency) g
               USING (currency))""",
        ),
        (
            "gold_referential_integrity",
            "gold",
            "ERROR",
            "every fact_settlements payment_id exists in fact_payments",
            f"""SELECT count(*), count(*) FILTER (WHERE payment_id NOT IN
             (SELECT payment_id FROM read_parquet('{g}/fact_payments.parquet')))
             FROM read_parquet('{g}/fact_settlements.parquet')""",
        ),
        (
            "source_schema_compliance",
            "bronze",
            "WARN",
            "bronze records rejected (schema/conflict) or quarantined (malformed) in Silver",
            f"""SELECT (SELECT count(*) FROM read_parquet({bronze})),
             (SELECT count(*) FROM read_parquet('{s}/rejected.parquet'))
             + (SELECT count(*) FROM read_parquet('{s}/quarantine.parquet'))""",
        ),
        (
            "source_duplicate_records",
            "silver",
            "WARN",
            "duplicate event/statement observations removed during deduplication",
            f"""SELECT (SELECT count(*) FROM read_parquet({bronze})),
             (SELECT count(*) FROM read_parquet('{s}/duplicates.parquet'))""",
        ),
        (
            "source_referential_integrity",
            "silver",
            "WARN",
            "events whose payment has no payment.created event",
            f"""SELECT count(*), count(*) FILTER (WHERE payment_id NOT IN
             (SELECT payment_id FROM {ev} WHERE event_type = 'payment.created')) FROM {ev}""",
        ),
        (
            "source_lifecycle_consistency",
            "silver",
            "WARN",
            "payments whose event_time order violates created < cleared < settled < delivered, or "
            "skip a stage",
            f"""SELECT count(*), count(*) FILTER (WHERE
               (t2 IS NOT NULL AND (t1 IS NULL OR t2 < t1))
            OR (t3 IS NOT NULL AND (t2 IS NULL OR t3 < t2))
            OR (t4 IS NOT NULL AND (t3 IS NULL OR t4 < t3))) FROM (
             SELECT payment_id,
               min(event_time) FILTER (WHERE event_type = 'payment.created') t1,
               min(event_time) FILTER (WHERE event_type = 'compliance.cleared') t2,
               min(event_time) FILTER (WHERE event_type = 'funds.settled') t3,
               min(event_time) FILTER (WHERE event_type = 'payout.delivered') t4
             FROM {ev} GROUP BY payment_id)""",
        ),
    ]


def run_checks(bronze_files: list[Path], silver_dir: Path, gold_dir: Path) -> list[CheckResult]:
    con = connect()
    out: list[CheckResult] = []
    checks = _checks(silver_dir.as_posix(), gold_dir.as_posix(), quote_list(bronze_files))
    for name, layer, severity, explanation, sql in checks:
        t0 = time.perf_counter()
        evaluated, failed = con.execute(sql).fetchone()
        ms = (time.perf_counter() - t0) * 1000
        status = "PASS" if failed == 0 else ("FAIL" if severity == "ERROR" else "WARN")
        out.append(
            CheckResult(name, layer, severity, status, int(evaluated), int(failed), ms, explanation)
        )
    con.close()
    return out
