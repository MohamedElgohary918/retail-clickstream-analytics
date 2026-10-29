-- ============================================================================
-- 06_orchestration / 06_test_and_scd2_proof.sql
-- STEP 6 (Part A): Resume child tasks and run ONE clean end-to-end graph test.
-- ============================================================================
USE ROLE DBT_SILVER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE RETAIL_CLICKSTREAM_DB;
USE SCHEMA MARTS;


-- ============================================================================
-- A1. ENABLE ALL CHILD TASKS (keep root TSK_GATE suspended for controlled testing)
-- SYSTEM$TASK_DEPENDENTS_ENABLE resumes every child/finalizer recursively
-- without starting TSK_GATE's 15-minute schedule yet.
-- ============================================================================
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE');

-- Verify: TSK_GATE should be 'suspended', and all 9 other tasks should be 'started'
SHOW TASKS IN SCHEMA RETAIL_CLICKSTREAM_DB.MARTS;
SELECT "name", "state", "schedule", "predecessors"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";


-- ============================================================================
-- A2. TRIGGER ONE MANUAL END-TO-END RUN OF THE GRAPH
-- ============================================================================
EXECUTE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE;


-- ============================================================================
-- A3. WATCH THE GRAPH RUN & VERIFY LOGS
-- Wait ~30-60 seconds after A2, then run A3a and A3b below.
-- If some tasks still show 'SCHEDULED' or 'EXECUTING', wait 15 seconds and re-run A3a.
-- ============================================================================

-- A3a. Task History for this graph run (expect all 10 tasks to reach 'SUCCEEDED')
SELECT
    NAME AS TASK_NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DURATION_S,
    RETURN_VALUE,
    ERROR_MESSAGE,
    GRAPH_RUN_GROUP_ID
FROM TABLE(RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('minute', -15, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30
))
WHERE DATABASE_NAME = 'RETAIL_CLICKSTREAM_DB'
ORDER BY SCHEDULED_TIME DESC, COMPLETED_TIME DESC NULLS FIRST;

-- A3b. Check PIPELINE_RUN_LOG (expect 3 rows sharing the same RUN_ID:
--      1. SILVER_GATE     -> READY
--      2. GOLD_VALIDATION -> PASS
--      3. PIPELINE        -> SUCCESS)
SELECT LOG_ID, RUN_ID, LOGGED_AT, STEP_NAME, STATUS, MESSAGE, METRICS
FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
ORDER BY LOG_ID DESC;