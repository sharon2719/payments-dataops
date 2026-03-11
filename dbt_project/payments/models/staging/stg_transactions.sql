WITH source AS (
    SELECT * FROM raw_transactions
),

cleaned AS (
    SELECT
        transaction_id,
        transaction_date::DATE         AS transaction_date,
        card_type,
        ROUND(amount, 2)               AS amount,
        currency,
        country,
        merchant_category,
        status,
        is_fraud::BOOLEAN              AS is_fraud,
        cardholder_id,
        merchant_id,
        CASE
            WHEN status = 'approved'  THEN true
            ELSE false
        END                            AS is_settled,
        CASE
            WHEN amount < 10          THEN 'micro'
            WHEN amount < 100         THEN 'small'
            WHEN amount < 1000        THEN 'medium'
            ELSE 'large'
        END                            AS amount_bucket
    FROM source
    WHERE transaction_id IS NOT NULL
      AND amount > 0
)

SELECT * FROM cleaned