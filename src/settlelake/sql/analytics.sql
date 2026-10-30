-- Analytical views over Gold tables (registered as views of the same name) and the pipeline
-- metadata database (schema `meta`). Monetary totals are ALWAYS grouped by currency; amounts in
-- different currencies are never summed. Amounts are integer minor units.

CREATE OR REPLACE VIEW v_daily_volume AS
SELECT payment_date, source_currency AS currency, count(*) AS payments,
       sum(source_amount_minor) AS total_minor
FROM fact_payments GROUP BY ALL ORDER BY payment_date, currency;

CREATE OR REPLACE VIEW v_volume_by_currency AS
SELECT source_currency AS currency, count(*) AS payments, sum(source_amount_minor) AS total_minor,
       count(settled_at) AS settled_payments
FROM fact_payments GROUP BY ALL ORDER BY payments DESC;

CREATE OR REPLACE VIEW v_corridor_analytics AS
SELECT corridor, source_currency, dest_currency, count(*) AS payments,
       sum(source_amount_minor) AS total_source_minor, sum(dest_amount_minor) AS total_dest_minor,
       count(settled_at) AS settled_payments, count(delivered_at) AS delivered_payments,
       round(avg(date_diff('minute', created_at, delivered_at)) / 60.0, 2) AS avg_hours_to_deliver
FROM fact_payments GROUP BY ALL ORDER BY payments DESC;

CREATE OR REPLACE VIEW v_provider_performance AS
SELECT provider, count(*) AS reconciled_keys,
       count(*) FILTER (WHERE classification = 'MATCHED') AS matched,
       count(*) FILTER (WHERE classification <> 'MATCHED') AS exceptions,
       round(100.0 * count(*) FILTER (WHERE classification = 'MATCHED') / count(*), 2) AS match_rate_pct,
       count(*) FILTER (WHERE list_contains(reasons, 'DELAYED_SETTLEMENT')) AS delayed,
       round(avg(settlement_lag_seconds) / 3600.0, 2) AS avg_settlement_lag_hours
FROM reconciliation GROUP BY provider ORDER BY provider;

CREATE OR REPLACE VIEW v_match_rates AS
SELECT CAST(coalesce(internal_event_time, partner_settled_at) AS DATE) AS settlement_date, provider,
       count(*) AS reconciled_keys,
       count(*) FILTER (WHERE classification = 'MATCHED') AS matched,
       round(100.0 * count(*) FILTER (WHERE classification = 'MATCHED') / count(*), 2) AS match_rate_pct
FROM reconciliation GROUP BY ALL ORDER BY settlement_date, provider;

-- Payments with no funds.settled event, aged relative to the newest event in the dataset.
CREATE OR REPLACE VIEW v_outstanding_settlements AS
SELECT payment_id, provider, corridor, source_currency AS currency, source_amount_minor, status,
       created_at,
       date_diff('hour', created_at, (SELECT max(created_at) FROM fact_payments)) AS age_hours
FROM fact_payments WHERE settled_at IS NULL ORDER BY created_at;

CREATE OR REPLACE VIEW v_exception_summary AS
SELECT classification, provider, count(*) AS records
FROM reconciliation GROUP BY ALL ORDER BY records DESC;

-- Value at risk per classification and currency (internal side; NULL for partner-only rows).
CREATE OR REPLACE VIEW v_exception_amounts AS
SELECT classification, internal_currency AS currency, count(*) AS records,
       sum(internal_amount_minor) AS internal_total_minor
FROM reconciliation WHERE classification <> 'MATCHED' AND internal_currency IS NOT NULL
GROUP BY ALL ORDER BY classification, currency;

CREATE OR REPLACE VIEW v_dq_trend AS
SELECT r.run_id, r.started_at, count(*) AS checks,
       count(*) FILTER (WHERE q.status = 'FAIL') AS failed_checks,
       count(*) FILTER (WHERE q.status = 'WARN') AS warned_checks,
       sum(q.failed_count) AS failed_records
FROM meta.quality_results q JOIN meta.runs r USING (run_id)
GROUP BY r.run_id, r.started_at ORDER BY r.started_at;

CREATE OR REPLACE VIEW v_pipeline_stats AS
SELECT r.run_id, r.kind, r.status AS run_status, r.started_at, r.duration_s AS run_seconds,
       s.stage, s.status AS stage_status, s.duration_s AS stage_seconds, s.rows_in, s.rows_out,
       round(s.rows_in / nullif(s.duration_s, 0), 0) AS rows_per_second
FROM meta.runs r JOIN meta.stage_runs s USING (run_id) ORDER BY r.started_at, s.started_at;
