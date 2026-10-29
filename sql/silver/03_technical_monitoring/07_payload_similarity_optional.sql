-- OPTIONAL DIAGNOSTIC ONLY: Identical event payload does NOT prove identical EVENT_ID or retry.
-- This query intentionally groups by payload (not EVENT_ID) to examine content similarity.

-- MONITOR 05: Exact repeated clickstream payload patterns.
-- Same payload ≠ proven Kafka retry. DO NOT deduplicate based on this audit.
WITH payload_groups AS (
    SELECT COUNT(*) AS REPEAT_COUNT
    FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS AS e
    GROUP BY
        e.EVENT_TIME, e.EVENT_TYPE, e.PRODUCT_ID, e.CATEGORY_ID,
        e.CATEGORY_CODE, e.BRAND, e.PRICE, e.USER_ID,
        e.USER_SESSION, e.SIMULATED_AT
    HAVING COUNT(*) > 1
)
SELECT
    COUNT(*) AS REPEATED_PAYLOAD_GROUPS,
    COALESCE(SUM(p.REPEAT_COUNT), 0) AS TOTAL_ROWS_IN_REPEATED_GROUPS,
    COALESCE(SUM(p.REPEAT_COUNT - 1), 0) AS EXTRA_IDENTICAL_PAYLOAD_ROWS
FROM payload_groups AS p;
