{{ config(materialized='view') }}

-- Preserve the business status as received; add standard value and technical quality flags.
SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.ORDER_TIMESTAMP,
    o.ORDER_TOTAL,
    o.ORDER_STATUS AS ORDER_STATUS_RAW,
    UPPER(TRIM(o.ORDER_STATUS)) AS ORDER_STATUS,
    (o.ORDER_TOTAL IS NULL OR o.ORDER_TOTAL <= 0) AS HAS_INVALID_ORDER_TOTAL,
    (o.ORDER_TIMESTAMP IS NULL) AS IS_MISSING_ORDER_TIMESTAMP
FROM {{ source('retail_raw', 'orders') }} AS o
