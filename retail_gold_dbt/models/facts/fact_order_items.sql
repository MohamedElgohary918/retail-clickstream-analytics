
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['ORDER_ID', 'PRODUCT_ID']
    )
}}

SELECT
    oi.ORDER_ID,
    oi.PRODUCT_ID,
    o.CUSTOMER_ID,
    o.ORDER_TIMESTAMP,
    o.ORDER_STATUS,
    oi.QUANTITY,
    oi.UNIT_PRICE,
    oi.LINE_TOTAL,
    oi.FIRST_PURCHASE_TIME,
    o.ORDER_TOTAL
FROM {{ source('retail_staging', 'STG_ORDER_ITEMS') }} AS oi
INNER JOIN {{ source('retail_staging', 'STG_ORDERS') }} AS o
    ON oi.ORDER_ID = o.ORDER_ID
