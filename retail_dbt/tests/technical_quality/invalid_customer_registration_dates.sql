-- Source's registration timestamp is required, but historical values are accepted.
SELECT c.CUSTOMER_ID
FROM {{ ref('stg_customers') }} AS c
WHERE c.REGISTRATION_DATE IS NULL
