{{ config(materialized='view') }}

-- Same source grain (one row per PRODUCT_ID); do not remove unknown or zero-price products.
WITH source_data AS (
    SELECT
        p.PRODUCT_ID,
        p.CATEGORY_ID,
        NULLIF(TRIM(p.CATEGORY_CODE), '') AS CATEGORY_CODE,
        NULLIF(TRIM(p.BRAND), '') AS BRAND,
        p.PRICE,
        p.COST
    FROM {{ source('retail_raw', 'products') }} AS p
)
SELECT
    s.PRODUCT_ID,
    s.CATEGORY_ID,
    s.CATEGORY_CODE,
    COALESCE(s.CATEGORY_CODE, 'Unknown') AS CATEGORY_CODE_LABEL,
    s.BRAND,
    COALESCE(s.BRAND, 'Unknown') AS BRAND_LABEL,
    s.PRICE,
    s.COST,
    (s.PRICE = 0) AS IS_ZERO_PRICE,
    (s.COST = 0) AS IS_ZERO_COST,
    (s.CATEGORY_CODE IS NULL) AS IS_MISSING_CATEGORY_CODE,
    (s.BRAND IS NULL) AS IS_MISSING_BRAND,
    (s.PRICE < 0) AS IS_NEGATIVE_PRICE,
    (s.COST < 0) AS IS_NEGATIVE_COST
FROM source_data AS s
