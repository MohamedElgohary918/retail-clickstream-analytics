-- MONITOR 05: EVENT_ID integrity (current preferred event identity check).
-- The staging view preserves EVENT_ID from RAW; this query is read-only.
-- NOTE: A unique EVENT_ID only supports retry detection when it is STABLE across retries.
SELECT
    CURRENT_TIMESTAMP() AS CHECKED_AT,
    COUNT(*) AS TOTAL_EVENTS,
    COUNT_IF(e.EVENT_ID IS NULL) AS MISSING_EVENT_IDS,
    COUNT(DISTINCT e.EVENT_ID) AS DISTINCT_EVENT_IDS,
    COUNT(*) - COUNT_IF(e.EVENT_ID IS NULL) - COUNT(DISTINCT e.EVENT_ID)
        AS DUPLICATE_EVENT_ID_ROWS,
    IFF(
        COUNT_IF(e.EVENT_ID IS NULL) = 0
        AND COUNT(*) = COUNT(DISTINCT e.EVENT_ID),
        'PASS',
        'INVESTIGATE'
    ) AS EVENT_ID_STATUS
FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_CLICKSTREAM_EVENTS AS e;
