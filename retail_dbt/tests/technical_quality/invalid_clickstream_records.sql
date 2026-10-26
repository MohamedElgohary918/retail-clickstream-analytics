-- No event deduplication; reject only missing essential data or negative price.
SELECT e.USER_ID, e.EVENT_TIME, e.PRODUCT_ID
FROM {{ ref('stg_clickstream_events') }} AS e
WHERE e.HAS_INVALID_PRICE
   OR e.HAS_INVALID_SESSION_ID
   OR e.HAS_MISSING_TIMESTAMPS
   OR e.SESSION_KEY IS NULL
