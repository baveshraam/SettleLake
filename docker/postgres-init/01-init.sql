-- Source "payments" database (POSTGRES_DB) plus a separate database for the Iceberg JDBC catalog.
CREATE DATABASE iceberg_catalog;

-- Outbox-style event table captured by Debezium. `payload` keeps the original JSON line verbatim
-- (including malformed ones) so Bronze preserves source records; event_id is deliberately NOT unique
-- because upstream retries produce duplicates that Silver must detect.
CREATE TABLE payment_events (
    seq        BIGSERIAL PRIMARY KEY,
    event_id   TEXT,
    payload    TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE payment_events REPLICA IDENTITY FULL;
