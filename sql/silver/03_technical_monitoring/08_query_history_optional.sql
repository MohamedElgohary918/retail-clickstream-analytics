-- OPTIONAL: Your recent Snowflake queries and failures (last 7 days).
-- Snowflake Information Schema exposes history; Snowflake GUI Monitoring > Query History is easier.
-- Requires visibility into the queried user's activity; default is CURRENT_USER().
SELECT
    q.START_TIME,
    q.QUERY_ID,
    q.QUERY_TYPE,
    q.EXECUTION_STATUS,
    q.WAREHOUSE_NAME,
    ROUND(q.TOTAL_ELAPSED_TIME / 1000.0, 2) AS DURATION_SECONDS,
    q.ERROR_MESSAGE
FROM TABLE(
    RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(
        END_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 1000
    )
) AS q
WHERE q.QUERY_TEXT ILIKE '%STG_%'
   OR q.QUERY_TEXT ILIKE '%RAW_CLICKSTREAM_EVENTS%'
ORDER BY q.START_TIME DESC
LIMIT 50;
