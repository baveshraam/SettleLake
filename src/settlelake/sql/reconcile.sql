-- Deterministic reconciliation of internal settlements (funds.settled events) against
-- partner statement rows. One output row per (provider, normalized reference) match key.
-- Parameters: {silver}, {sla_seconds}, {precedence} (SQL array literal of classes).
-- Ground truth is never referenced here.
WITH i AS (
    SELECT match_key, event_id, payment_id, corridor, amount_minor, currency, event_time, ingested_at
    FROM read_parquet('{silver}/events.parquet')
    WHERE event_type = 'funds.settled'
),
p AS (
    SELECT match_key, statement_row_id, amount_minor, currency, status, settled_at
    FROM read_parquet('{silver}/statements.parquet')
),
ia AS (
    SELECT match_key, count(*) AS n_i, list(event_id ORDER BY event_id) AS i_refs,
           min(payment_id) AS payment_id, min(corridor) AS corridor,
           min(amount_minor) AS i_amt, min(currency) AS i_cur,
           min(event_time) AS i_time, min(ingested_at) AS i_ingested
    FROM i GROUP BY match_key
),
pa AS (
    SELECT match_key, count(*) AS n_p, list(statement_row_id ORDER BY statement_row_id) AS p_refs,
           min(amount_minor) AS p_amt, min(currency) AS p_cur, min(status) AS p_status,
           min(settled_at) AS p_time
    FROM p GROUP BY match_key
),
inv AS (
    SELECT match_key,
           coalesce(bool_or(side = 'INTERNAL'), false) AS inv_i,
           coalesce(bool_or(side = 'PARTNER'), false) AS inv_p
    FROM read_parquet('{silver}/rejected.parquet')
    WHERE match_key IS NOT NULL
    GROUP BY match_key
),
dup AS (
    SELECT match_key,
           count(*) FILTER (WHERE obs_type = 'EVENT') AS d_e,
           count(*) FILTER (WHERE obs_type = 'STATEMENT') AS d_s
    FROM read_parquet('{silver}/duplicates.parquet')
    WHERE match_key IS NOT NULL
    GROUP BY match_key
),
keys AS (
    SELECT match_key FROM ia UNION SELECT match_key FROM pa UNION SELECT match_key FROM inv
),
base AS (
    SELECT k.match_key,
           coalesce(ia.n_i, 0) AS n_i, coalesce(pa.n_p, 0) AS n_p,
           coalesce(inv.inv_i, false) AS inv_i, coalesce(inv.inv_p, false) AS inv_p,
           coalesce(dup.d_e, 0) AS d_e, coalesce(dup.d_s, 0) AS d_s,
           coalesce(ia.i_refs, []::VARCHAR[]) AS i_refs, coalesce(pa.p_refs, []::VARCHAR[]) AS p_refs,
           ia.payment_id, ia.corridor, ia.i_amt, ia.i_cur, ia.i_time, ia.i_ingested,
           pa.p_amt, pa.p_cur, pa.p_status, pa.p_time
    FROM keys k
    LEFT JOIN ia USING (match_key) LEFT JOIN pa USING (match_key)
    LEFT JOIN inv USING (match_key) LEFT JOIN dup USING (match_key)
),
flagged AS (
    SELECT *, (n_i = 1 AND n_p = 1) AS one_to_one FROM base
),
found AS (
    SELECT *, list_filter([
        CASE WHEN inv_i OR inv_p THEN 'INVALID_RECORD' END,
        CASE WHEN n_i > 1 OR n_p > 1 THEN 'AMBIGUOUS_MATCH' END,
        CASE WHEN n_i = 0 AND NOT inv_i THEN 'MISSING_INTERNAL_RECORD' END,
        CASE WHEN n_p = 0 AND NOT inv_p THEN 'MISSING_PARTNER_RECORD' END,
        CASE WHEN one_to_one AND i_cur <> p_cur THEN 'CURRENCY_MISMATCH' END,
        CASE WHEN one_to_one AND i_cur = p_cur AND i_amt <> p_amt THEN 'AMOUNT_MISMATCH' END,
        CASE WHEN one_to_one AND p_status <> 'SETTLED' THEN 'STATUS_MISMATCH' END,
        CASE WHEN one_to_one AND date_diff('second', i_time, p_time) > {sla_seconds}
             THEN 'DELAYED_SETTLEMENT' END,
        CASE WHEN d_e > 0 THEN 'DUPLICATE_EVENT' END,
        CASE WHEN d_s > 0 THEN 'DUPLICATE_STATEMENT' END
    ], lambda x: x IS NOT NULL) AS found_reasons
    FROM flagged
),
ordered AS (
    -- order reasons by documented precedence; the first one is the primary classification
    SELECT *, list_filter({precedence}, lambda c: list_contains(found_reasons, c)) AS reasons FROM found
)
SELECT
    'rec_' || substr(md5(match_key), 1, 16) AS reconciliation_id,
    match_key,
    split_part(match_key, '|', 1) AS provider,
    payment_id,
    corridor,
    CASE WHEN len(reasons) = 0 THEN 'MATCHED' ELSE reasons[1] END AS classification,
    CASE WHEN len(reasons) = 0 THEN ['MATCHED'] ELSE reasons END AS reasons,
    i_refs AS internal_refs,
    p_refs AS partner_refs,
    CASE WHEN n_i = 1 THEN i_amt END AS internal_amount_minor,
    CASE WHEN n_i = 1 THEN i_cur END AS internal_currency,
    CASE WHEN n_p = 1 THEN p_amt END AS partner_amount_minor,
    CASE WHEN n_p = 1 THEN p_cur END AS partner_currency,
    CASE WHEN n_p = 1 THEN p_status END AS partner_status,
    CASE WHEN n_i = 1 THEN i_time END AS internal_event_time,
    CASE WHEN n_i = 1 THEN i_ingested END AS internal_ingested_at,
    CASE WHEN n_p = 1 THEN p_time END AS partner_settled_at,
    CASE WHEN one_to_one THEN date_diff('second', i_time, p_time) END AS settlement_lag_seconds,
    CASE WHEN n_i = 1 THEN date_diff('second', i_time, i_ingested) END AS ingestion_lag_seconds,
    CASE WHEN len(reasons) = 0
         THEN 'Internal settlement and partner statement agree on amount, currency and status.'
         ELSE concat_ws('; ',
            CASE WHEN list_contains(reasons, 'INVALID_RECORD') THEN
                'invalid source record(s) quarantined for this key (internal='
                || inv_i::VARCHAR || ', partner=' || inv_p::VARCHAR || ')' END,
            CASE WHEN list_contains(reasons, 'AMBIGUOUS_MATCH') THEN
                n_i::VARCHAR || ' internal and ' || n_p::VARCHAR
                || ' partner records share this key; not forced into a match' END,
            CASE WHEN list_contains(reasons, 'MISSING_INTERNAL_RECORD') THEN
                'partner statement has no internal settlement for this reference' END,
            CASE WHEN list_contains(reasons, 'MISSING_PARTNER_RECORD') THEN
                'internal settlement has no partner statement row' END,
            CASE WHEN list_contains(reasons, 'CURRENCY_MISMATCH') THEN
                'currency differs: internal ' || i_cur || ' vs partner ' || p_cur
                || ' (amounts not compared)' END,
            CASE WHEN list_contains(reasons, 'AMOUNT_MISMATCH') THEN
                'amount differs: internal ' || i_amt::VARCHAR || ' vs partner ' || p_amt::VARCHAR
                || ' ' || i_cur || ' minor units (diff ' || (p_amt - i_amt)::VARCHAR || ')' END,
            CASE WHEN list_contains(reasons, 'STATUS_MISMATCH') THEN
                'partner status is ' || p_status || ', expected SETTLED' END,
            CASE WHEN list_contains(reasons, 'DELAYED_SETTLEMENT') THEN
                'partner settled ' || (date_diff('second', i_time, p_time) // 3600)::VARCHAR
                || 'h after internal settlement (SLA {sla_seconds}s)' END,
            CASE WHEN list_contains(reasons, 'DUPLICATE_EVENT') THEN
                d_e::VARCHAR || ' duplicate internal event(s) removed' END,
            CASE WHEN list_contains(reasons, 'DUPLICATE_STATEMENT') THEN
                d_s::VARCHAR || ' duplicate statement row(s) removed' END)
    END AS explanation
FROM ordered
ORDER BY match_key
