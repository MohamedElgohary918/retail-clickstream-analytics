-- ============================================================================
-- 06_orchestration / 07_fix_validation_task.sql
-- PROBLEM: TSK_GOLD_VALIDATION never runs, so TSK_FINALIZE logs every run as
--          FAILED ("gate=READY, validation=NULL").
-- Run top to bottom. Send me the output of sections A, B, C and F.
-- ============================================================================
USE ROLE DBT_SILVER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE RETAIL_CLICKSTREAM_DB;
USE SCHEMA MARTS;

-- 0. Stop the 15-minute schedule while we fix this (no more false FAILED rows)
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE SUSPEND;


-- ============================================================================
-- A. DOES THE TASK EXIST, IS IT ENABLED, AND DOES IT HAVE ITS 6 PREDECESSORS?
-- Expect 10 tasks. For TSK_GOLD_VALIDATION look for: missing row, state
-- 'suspended', or a "predecessors" list shorter than 6 tasks.
-- ============================================================================
SHOW TASKS IN SCHEMA RETAIL_CLICKSTREAM_DB.MARTS;
SELECT "name", "state", "predecessors"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";


-- ============================================================================
-- B. DID IT EVER RUN OR FAIL IN THE LAST DAY?  (0 rows = it was never scheduled)
-- ============================================================================
SELECT NAME, STATE, SCHEDULED_TIME, ERROR_CODE, ERROR_MESSAGE
FROM TABLE(RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TSK_GOLD_VALIDATION',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20))
ORDER BY SCHEDULED_TIME DESC;


-- ============================================================================
-- C. DOES THE VALIDATION LOGIC ITSELF WORK WHEN CALLED BY HAND?
-- Expect: PASS: All Gold validation checks passed
-- PASS here + nothing in B  => the problem is graph wiring, not the SQL.
-- ============================================================================
CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_GOLD_VALIDATION();


-- ============================================================================
-- D. FIX: re-create the validation task with all 6 predecessors
-- (safe to run even if it already exists; the root is suspended)
-- ============================================================================
CREATE OR REPLACE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION
    WAREHOUSE = RETAIL_WH
    USER_TASK_TIMEOUT_MS = 600000
    AFTER RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_CUSTOMER,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_DATE,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_PRODUCT,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_FACT_ORDER_ITEMS,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_FACT_CLICKSTREAM_EVENTS,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_INVENTORY
AS
    CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_GOLD_VALIDATION();


-- ============================================================================
-- E. ENABLE EVERY TASK IN THE GRAPH, THEN CHECK
-- Expect: TSK_GATE 'suspended'; the other 9 tasks 'started'
-- ============================================================================
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE');

SHOW TASKS IN SCHEMA RETAIL_CLICKSTREAM_DB.MARTS;
SELECT "name", "state", "predecessors"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";


-- ============================================================================
-- F. ONE CLEAN END-TO-END TEST  (run F1, wait ~45 seconds, then F2 and F3)
-- ============================================================================
-- F1
EXECUTE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE;

-- F2. Expect 10 tasks from this run, ALL 'SUCCEEDED', including TSK_GOLD_VALIDATION
SELECT NAME AS TASK_NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME,
       DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DURATION_S,
       RETURN_VALUE, ERROR_MESSAGE, GRAPH_RUN_GROUP_ID
FROM TABLE(RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('minute', -5, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30))
WHERE DATABASE_NAME = 'RETAIL_CLICKSTREAM_DB'
ORDER BY SCHEDULED_TIME DESC, COMPLETED_TIME DESC NULLS FIRST;

-- F3. Newest rows FIRST BY TIME (not by LOG_ID, which is not chronological).
-- Expect 3 rows with the same RUN_ID:
--   SILVER_GATE READY  /  GOLD_VALIDATION PASS  /  PIPELINE SUCCESS
SELECT LOG_ID, RUN_ID, LOGGED_AT, STEP_NAME, STATUS, MESSAGE, METRICS
FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT DESC
LIMIT 10;


-- ============================================================================
-- G. ONLY AFTER F SHOWS SUCCESS: turn the 15-minute schedule back on
-- ============================================================================
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE RESUME;


-- ============================================================================
-- H. OPTIONAL: remove the false FAILED rows written while validation was missing
-- ============================================================================
-- DELETE FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
-- WHERE STEP_NAME = 'PIPELINE' AND STATUS = 'FAILED'
--   AND MESSAGE LIKE 'Pipeline ended in an incomplete state%';