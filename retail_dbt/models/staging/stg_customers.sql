
{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.EMAIL,
        c.COUNTRY,
        c.REGISTRATION_DATE
    FROM {{ source('retail_raw', 'customers') }} AS c
)

SELECT
    s.CUSTOMER_ID AS CUSTOMER_ID,
    TRIM(s.FIRST_NAME) AS FIRST_NAME,
    TRIM(s.LAST_NAME) AS LAST_NAME,
    LOWER(TRIM(s.EMAIL)) AS EMAIL,
    TRIM(s.COUNTRY) AS COUNTRY,
    s.REGISTRATION_DATE AS REGISTRATION_DATE
FROM source_data AS s
