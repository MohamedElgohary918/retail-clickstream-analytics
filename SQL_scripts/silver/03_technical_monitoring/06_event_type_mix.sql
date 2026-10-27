-- MONITOR 06: Event type distribution for a Snowsight bar chart.
-- A replay of the first historical hours is NOT a representative full funnel.
SELECT
    e.EVENT_TYPE,
    COUNT(*) AS EVENT_COUNT
FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_CLICKSTREAM_EVENTS AS e
GROUP BY e.EVENT_TYPE
ORDER BY EVENT_COUNT DESC, e.EVENT_TYPE;
