-- Validate standardization only; do not claim to fully validate email delivery or RFC compliance.
SELECT c.CUSTOMER_ID
FROM {{ ref('stg_customers') }} AS c
WHERE c.EMAIL IS NULL OR c.EMAIL <> LOWER(TRIM(c.EMAIL))
