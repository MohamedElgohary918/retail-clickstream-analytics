{{ config(materialized='view') }}


WITH source_data AS (
    SELECT
        e.EVENT_ID,
        e.EVENT_TIME,
        LOWER(TRIM(e.EVENT_TYPE)) AS EVENT_TYPE,
        e.PRODUCT_ID,
        e.CATEGORY_ID,
        NULLIF(TRIM(e.CATEGORY_CODE), '') AS CATEGORY_CODE,
        NULLIF(TRIM(e.BRAND), '') AS BRAND,
        e.PRICE,
        e.USER_ID,
        NULLIF(TRIM(e.USER_SESSION), '') AS USER_SESSION,
        e.SIMULATED_AT
    FROM {{ source('retail_raw', 'clickstream_events') }} AS e
)

SELECT
    s.EVENT_ID,
    s.EVENT_TIME,
    s.EVENT_TYPE,
    s.PRODUCT_ID,
    s.CATEGORY_ID,
    s.CATEGORY_CODE,
    COALESCE(s.CATEGORY_CODE, 'Unknown') AS CATEGORY_CODE_LABEL,
    s.BRAND,
    COALESCE(s.BRAND, 'Unknown') AS BRAND_LABEL,
    s.PRICE,
    s.USER_ID,
    s.USER_SESSION,
    s.SIMULATED_AT,

    CASE
        WHEN s.USER_ID IS NOT NULL
             AND s.USER_SESSION IS NOT NULL
            THEN SHA2(
                CONCAT(TO_VARCHAR(s.USER_ID), '|', s.USER_SESSION),
                256
            )
        ELSE NULL
    END AS SESSION_KEY,

    (s.PRICE = 0) AS IS_ZERO_PRICE,
    (s.CATEGORY_CODE IS NULL) AS IS_MISSING_CATEGORY_CODE,
    (s.BRAND IS NULL) AS IS_MISSING_BRAND,

    (s.PRICE IS NULL OR s.PRICE < 0) AS HAS_INVALID_PRICE,

    (
        s.USER_ID IS NULL
        OR s.USER_SESSION IS NULL
    ) AS HAS_INVALID_SESSION_ID,

    (
        s.EVENT_TIME IS NULL
        OR s.SIMULATED_AT IS NULL
    ) AS HAS_MISSING_TIMESTAMPS

FROM source_data AS s
