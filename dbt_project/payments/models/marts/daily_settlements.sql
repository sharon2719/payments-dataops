WITH base AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    transaction_date,
    country,
    card_type,
    COUNT(*)                                    AS total_transactions,
    COUNT(*) FILTER (WHERE is_settled)          AS settled_transactions,
    COUNT(*) FILTER (WHERE is_fraud)            AS fraud_transactions,
    COUNT(*) FILTER (WHERE status = 'declined') AS declined_transactions,
    ROUND(SUM(amount), 2)                       AS total_volume,
    ROUND(AVG(amount), 2)                       AS avg_transaction_value,
    ROUND(COUNT(*) FILTER (WHERE is_fraud)
        * 100.0 / COUNT(*), 4)                  AS fraud_rate_pct
FROM base
GROUP BY transaction_date, country, card_type
ORDER BY transaction_date