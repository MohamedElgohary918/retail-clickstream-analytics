-- FAIL only for invalid numeric values; zero price/cost is allowed, flagged separately.
SELECT p.PRODUCT_ID
FROM {{ ref('stg_products') }} AS p
WHERE p.PRICE IS NULL OR p.COST IS NULL OR p.IS_NEGATIVE_PRICE OR p.IS_NEGATIVE_COST
