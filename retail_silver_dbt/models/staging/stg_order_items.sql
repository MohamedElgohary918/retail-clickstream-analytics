{{ config(materialized='view') }}

-- Preserve one row per ORDER_ITEM_ID and the source financial values.
SELECT
    i.ORDER_ITEM_ID,
    i.ORDER_ID,
    i.PRODUCT_ID,
    i.QUANTITY,
    i.UNIT_PRICE,
    i.LINE_TOTAL,
    i.FIRST_PURCHASE_TIME,
    (i.LINE_TOTAL IS NULL OR i.QUANTITY IS NULL OR i.UNIT_PRICE IS NULL
        OR ABS(i.LINE_TOTAL - (i.QUANTITY * i.UNIT_PRICE)) > 0.01)
        AS HAS_LINE_TOTAL_MISMATCH,
    (i.QUANTITY IS NULL OR i.QUANTITY <= 0) AS HAS_INVALID_QUANTITY,
    (i.UNIT_PRICE IS NULL OR i.UNIT_PRICE <= 0) AS HAS_INVALID_UNIT_PRICE,
    (i.LINE_TOTAL IS NULL OR i.LINE_TOTAL <= 0) AS HAS_INVALID_LINE_TOTAL
FROM {{ source('retail_raw', 'order_items') }} AS i
