-- Spark SQL port of reconcile.sql. Same rules, same precedence; verified for exact parity with
-- the DuckDB engine in tests/spark. Parameters: {events}, {statements}, {rejected}, {duplicates},
-- {sla_seconds}, {precedence}.
WITH i AS (
    SELECT match_key, event_id, payment_id, corridor, amount_minor, currency, event_time, ingested_at
    FROM {events} WHERE event_type = 'funds.settled'
),
p AS (
    SELECT match_key, statement_row_id, amount_minor, currency, status, settled_at FROM {statements}
),
ia AS (
    SELECT match_key, count(*) AS n_i, sort_array(collect_list(event_id)) AS i_refs,
           min(payment_id) AS payment_id, min(corridor) AS corridor,
           min(amount_minor) AS i_amt, min(currency) AS i_cur,
           min(event_time) AS i_time, min(ingested_at) AS i_ingested
    FROM i GROUP BY match_key
),
pa AS (
    SELECT match_key, count(*) AS n_p, sort_array(collect_list(statement_row_id)) AS p_refs,
           min(amount_minor) AS p_amt, min(currency) AS p_cur, min(status) AS p_status,
           min(settled_at) AS p_time
    FROM p GROUP BY match_key
),
inv AS (
    SELECT match_key,
           coalesce(bool_or(side = 'INTERNAL'), false) AS inv_i,
           coalesce(bool_or(side = 'PARTNER'), false) AS inv_p
    FROM {rejected} WHERE match_key IS NOT NULL GROUP BY match_key
),
dup AS (
    SELECT match_key,
           count(*) FILTER (WHERE obs_type = 'EVENT') AS d_e,
           count(*) FILTER (WHERE obs_type = 'STATEMENT') AS d_s
    FROM {duplicates} WHERE match_key IS NOT NULL GROUP BY match_key
),
keys AS (
    SELECT match_key FROM ia UNION SELECT match_key FROM pa UNION SELECT match_key FROM inv
),
base AS (
    SELECT k.match_key,
           coalesce(ia.n_i, 0) AS n_i, coalesce(pa.n_p, 0) AS n_p,
           coalesce(inv.inv_i, false) AS inv_i, coalesce(inv.inv_p, false) AS inv_p,
           coalesce(dup.d_e, 0) AS d_e, coalesce(dup.d_s, 0) AS d_s,
           coalesce(ia.i_refs, CAST(array() AS ARRAY<STRING>)) AS i_refs,
           coalesce(pa.p_refs, CAST(array() AS ARRAY<STRING>)) AS p_refs,
           ia.payment_id, ia.corridor, ia.i_amt, ia.i_cur, ia.i_time, ia.i_ingested,
           pa.p_amt, pa.p_cur, pa.p_status, pa.p_time
    FROM keys k
    LEFT JOIN ia ON ia.match_key = k.match_key LEFT JOIN pa ON pa.match_key = k.match_key
    LEFT JOIN inv ON inv.match_key = k.match_key LEFT JOIN dup ON dup.match_key = k.match_key
),
flagged AS (
    SELECT *, (n_i = 1 AND n_p = 1) AS one_to_one FROM base
),
found AS (
    SELECT *, filter(array(
        CASE WHEN inv_i OR inv_p THEN 'INVALID_RECORD' END,
        CASE WHEN n_i > 1 OR n_p > 1 THEN 'AMBIGUOUS_MATCH' END,
        CASE WHEN n_i = 0 AND NOT inv_i THEN 'MISSING_INTERNAL_RECORD' END,
        CASE WHEN n_p = 0 AND NOT inv_p THEN 'MISSING_PARTNER_RECORD' END,
        CASE WHEN one_to_one AND i_cur <> p_cur THEN 'CURRENCY_MISMATCH' END,
        CASE WHEN one_to_one AND i_cur = p_cur AND i_amt <> p_amt THEN 'AMOUNT_MISMATCH' END,
        CASE WHEN one_to_one AND p_status <> 'SETTLED' THEN 'STATUS_MISMATCH' END,
        CASE WHEN one_to_one AND timestampdiff(SECOND, i_time, p_time) > {sla_seconds}
             THEN 'DELAYED_SETTLEMENT' END,
        CASE WHEN d_e > 0 THEN 'DUPLICATE_EVENT' END,
        CASE WHEN d_s > 0 THEN 'DUPLICATE_STATEMENT' END
    ), x -> x IS NOT NULL) AS found_reasons
    FROM flagged
),
ordered AS (
    SELECT *, filter(array({precedence}), c -> array_contains(found_reasons, c)) AS reasons FROM found
)
SELECT
    concat('rec_', substr(md5(match_key), 1, 16)) AS reconciliation_id,
    match_key,
    split_part(match_key, '|', 1) AS provider,
    payment_id,
    corridor,
    CASE WHEN size(reasons) = 0 THEN 'MATCHED' ELSE reasons[0] END AS classification,
    CASE WHEN size(reasons) = 0 THEN array('MATCHED') ELSE reasons END AS reasons,
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
    CASE WHEN one_to_one THEN timestampdiff(SECOND, i_time, p_time) END AS settlement_lag_seconds,
    CASE WHEN n_i = 1 THEN timestampdiff(SECOND, i_time, i_ingested) END AS ingestion_lag_seconds,
    CASE WHEN size(reasons) = 0
         THEN 'Internal settlement and partner statement agree on amount, currency and status.'
         ELSE concat_ws('; ',
            CASE WHEN array_contains(reasons, 'INVALID_RECORD') THEN
                concat('invalid source record(s) quarantined for this key (internal=',
                       CAST(inv_i AS STRING), ', partner=', CAST(inv_p AS STRING), ')') END,
            CASE WHEN array_contains(reasons, 'AMBIGUOUS_MATCH') THEN
                concat(CAST(n_i AS STRING), ' internal and ', CAST(n_p AS STRING),
                       ' partner records share this key; not forced into a match') END,
            CASE WHEN array_contains(reasons, 'MISSING_INTERNAL_RECORD') THEN
                'partner statement has no internal settlement for this reference' END,
            CASE WHEN array_contains(reasons, 'MISSING_PARTNER_RECORD') THEN
                'internal settlement has no partner statement row' END,
            CASE WHEN array_contains(reasons, 'CURRENCY_MISMATCH') THEN
                concat('currency differs: internal ', i_cur, ' vs partner ', p_cur,
                       ' (amounts not compared)') END,
            CASE WHEN array_contains(reasons, 'AMOUNT_MISMATCH') THEN
                concat('amount differs: internal ', CAST(i_amt AS STRING), ' vs partner ',
                       CAST(p_amt AS STRING), ' ', i_cur, ' minor units (diff ',
                       CAST(p_amt - i_amt AS STRING), ')') END,
            CASE WHEN array_contains(reasons, 'STATUS_MISMATCH') THEN
                concat('partner status is ', p_status, ', expected SETTLED') END,
            CASE WHEN array_contains(reasons, 'DELAYED_SETTLEMENT') THEN
                concat('partner settled ', CAST(timestampdiff(SECOND, i_time, p_time) div 3600 AS STRING),
                       'h after internal settlement (SLA {sla_seconds}s)') END,
            CASE WHEN array_contains(reasons, 'DUPLICATE_EVENT') THEN
                concat(CAST(d_e AS STRING), ' duplicate internal event(s) removed') END,
            CASE WHEN array_contains(reasons, 'DUPLICATE_STATEMENT') THEN
                concat(CAST(d_s AS STRING), ' duplicate statement row(s) removed') END)
    END AS explanation
FROM ordered
ORDER BY match_key
