-- EVENT_ID completeness + uniqueness (source baseline). Read-only.
-- EVENT_ID is NUMBER(38,0), supplied by the upstream Kafka producer/consumer.
-- A stable ID must remain the same when the SAME event is retried or replayed.
SELECT
    COUNT(*) AS TOTAL_EVENTS,
    COUNT_IF(e.EVENT_ID IS NULL) AS NULL_EVENT_IDS,
    COUNT(DISTINCT e.EVENT_ID) AS DISTINCT_NON_NULL_EVENT_IDS,
    COUNT(*) - COUNT_IF(e.EVENT_ID IS NULL) - COUNT(DISTINCT e.EVENT_ID)
        AS DUPLICATE_EVENT_ID_ROWS
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS AS e;
