
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='CUSTOMER_ID'
    )
}}

SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    COUNTRY,
    REGISTRATION_DATE
FROM {{ source('retail_staging', 'STG_CUSTOMERS') }}
