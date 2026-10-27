
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='EVENT_ID'
    )
}}

SELECT
    EVENT_ID,
    EVENT_TIME,
    EVENT_TYPE,
    PRODUCT_ID,
    CATEGORY_ID,
    BRAND,
    USER_ID,
    USER_SESSION,
    SESSION_KEY,
    IS_ZERO_PRICE,
    IS_MISSING_CATEGORY_CODE,
    HAS_INVALID_PRICE,
    HAS_INVALID_SESSION_ID,
    HAS_MISSING_TIMESTAMPS
FROM {{ source('retail_staging', 'STG_CLICKSTREAM_EVENTS') }}
