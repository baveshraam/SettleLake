"""Gold layer: facts, dimensions and the exception queue, built from Silver + reconciliation."""

from __future__ import annotations

from pathlib import Path

from settlelake.config import CURRENCIES
from settlelake.lakehouse.store import connect, write_query

GOLD_TABLES = [
    "reconciliation",  # written by the reconcile stage
    "exceptions",
    "fact_payments",
    "fact_settlements",
    "dim_provider",
    "dim_corridor",
    "dim_currency",
    "dim_date",
]


def _queries(silver: str, gold: str) -> dict[str, str]:
    ev = f"read_parquet('{silver}/events.parquet')"
    st = f"read_parquet('{silver}/statements.parquet')"
    rec = f"read_parquet('{gold}/reconciliation.parquet')"
    fx = ", ".join(f"('{c}', {e})" for c, e in sorted(CURRENCIES.items()))
    return {
        "fact_payments": f"""
            SELECT payment_id, provider, corridor, source_currency, source_amount_minor,
                   dest_currency, dest_amount_minor, created_at, cleared_at, settled_at,
                   delivered_at, CAST(created_at AS DATE) AS payment_date,
                   CASE WHEN delivered_at IS NOT NULL THEN 'DELIVERED'
                        WHEN settled_at IS NOT NULL THEN 'SETTLED'
                        WHEN cleared_at IS NOT NULL THEN 'COMPLIANCE_CLEARED'
                        ELSE 'CREATED' END AS status
            FROM (
                SELECT payment_id,
                       min(provider) AS provider, min(corridor) AS corridor,
                       min(amount_minor) FILTER (WHERE event_type = 'payment.created') AS source_amount_minor,
                       min(currency) FILTER (WHERE event_type = 'payment.created') AS source_currency,
                       min(dest_currency) FILTER (WHERE event_type = 'payment.created') AS dest_currency,
                       min(dest_amount_minor) FILTER (WHERE event_type = 'payment.created') AS dest_amount_minor,
                       min(event_time) FILTER (WHERE event_type = 'payment.created') AS created_at,
                       min(event_time) FILTER (WHERE event_type = 'compliance.cleared') AS cleared_at,
                       min(event_time) FILTER (WHERE event_type = 'funds.settled') AS settled_at,
                       min(event_time) FILTER (WHERE event_type = 'payout.delivered') AS delivered_at
                FROM {ev} GROUP BY payment_id)
            WHERE created_at IS NOT NULL
            ORDER BY payment_id""",
        "fact_settlements": f"""
            SELECT e.event_id AS settlement_event_id, e.payment_id, e.provider, e.corridor,
                   e.currency, e.amount_minor, e.event_time AS settled_at,
                   e.ingested_at, date_diff('second', e.event_time, e.ingested_at) AS ingestion_lag_seconds,
                   e.match_key, r.classification, r.reconciliation_id
            FROM {ev} e LEFT JOIN {rec} r USING (match_key)
            WHERE e.event_type = 'funds.settled'
            ORDER BY e.event_id""",
        "exceptions": f"SELECT * FROM {rec} WHERE classification <> 'MATCHED' ORDER BY match_key",
        "dim_provider": f"""
            SELECT provider, count(*) AS statement_rows_seen FROM
              (SELECT provider FROM {st} UNION ALL SELECT provider FROM {ev}) GROUP BY provider
            ORDER BY provider""",
        "dim_corridor": f"""
            SELECT DISTINCT corridor, split_part(corridor, '-', 1) AS source_currency,
                   split_part(corridor, '-', 2) AS dest_currency
            FROM {ev} ORDER BY corridor""",
        "dim_currency": f"SELECT * FROM (VALUES {fx}) t(currency, minor_unit_exponent) ORDER BY currency",
        "dim_date": f"""
            SELECT CAST(d AS DATE) AS date, year(d) AS year, month(d) AS month, dayofweek(d) AS dow
            FROM generate_series(
                (SELECT CAST(min(event_time) AS DATE) FROM {ev}),
                (SELECT CAST(max(event_time) AS DATE) FROM {ev}), INTERVAL 1 DAY) g(d)
            ORDER BY 1""",
    }


def build_gold(silver_dir: Path, gold_dir: Path) -> dict:
    """Build every Gold table except reconciliation (which must already exist)."""
    con = connect()
    counts = {}
    for name, sql in _queries(silver_dir.as_posix(), gold_dir.as_posix()).items():
        target = gold_dir / f"{name}.parquet"
        write_query(con, target, sql)
        counts[name] = con.execute(
            f"SELECT count(*) FROM read_parquet('{target.as_posix()}')"
        ).fetchone()[0]
    con.close()
    return counts
