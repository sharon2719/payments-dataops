WITH base AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    country,
    card_type,
    merchant_category,
    COUNT(*)                                    AS total_transactions,
    COUNT(*) FILTER (WHERE is_fraud)            AS fraud_count,
    ROUND(SUM(amount) FILTER (WHERE is_fraud), 2) AS fraud_volume,
    ROUND(COUNT(*) FILTER (WHERE is_fraud)
        * 100.0 / COUNT(*), 4)                  AS fraud_rate_pct,
    ROUND(AVG(amount) FILTER (WHERE is_fraud), 2) AS avg_fraud_amount
FROM base
GROUP BY country, card_type, merchant_category
ORDER BY fraud_rate_pct DESC