
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='PRODUCT_ID'
    )
}}

SELECT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE_LABEL,
    BRAND_LABEL,
    PRICE,
    COST
FROM {{ source('retail_staging', 'STG_PRODUCTS') }}
