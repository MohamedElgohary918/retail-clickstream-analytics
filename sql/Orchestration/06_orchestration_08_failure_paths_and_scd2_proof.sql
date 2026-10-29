-- ============================================================================
-- 06_orchestration / 08_failure_paths_and_scd2_proof.sql
-- STEP 6 (Part B): prove the pipeline behaves correctly when things go wrong,
--                  and prove SCD2 history tracking really works.
--
--   A. BLOCKED path : gate fails  -> Gold tasks skipped -> run logged BLOCKED
--   B. FAILED path  : validation fails                  -> run logged FAILED
--   C. SCD2 proof   : one inventory change -> two versions (on SCRATCH copies,
--                     so no teammate data and no real table is touched)
--   D. Restore + final clean run + turn the 15-minute schedule back on
--
-- Run each numbered statement ONE AT A TIME, in order. After every EXECUTE TASK
-- wait ~45 seconds before running the check queries.
-- Parts A and B temporarily change a task body; section D verifies both were
-- restored. Do not skip D.
-- ============================================================================
USE ROLE DBT_SILVER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE RETAIL_CLICKSTREAM_DB;
USE SCHEMA MARTS;

-- Keep the 15-minute schedule OFF during all tests
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE SUSPEND;


-- ############################################################################
-- A. BLOCKED PATH  (gate forced to fail)
-- Expected: gate returns INVESTIGATE, the 7 Gold-refresh tasks are SKIPPED,
-- the finalizer logs PIPELINE = BLOCKED.
-- ############################################################################

-- A1. Temporarily make the gate fail on purpose (forced test mode)
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE MODIFY AS
EXECUTE IMMEDIATE $$
DECLARE
    v_result VARCHAR;
    v_status VARCHAR;
BEGIN
    CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_SILVER_GATE(TRUE) INTO :v_result;
    v_status := SPLIT_PART(:v_result, ':', 1);
    CALL SYSTEM$SET_RETURN_VALUE(:v_status);
END;
$$;

-- A2. Run the graph, then wait ~45 seconds
EXECUTE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE;

-- A3. Task states. Expect: TSK_GATE SUCCEEDED with RETURN_VALUE = INVESTIGATE;
--     the Gold-refresh tasks SKIPPED (none SUCCEEDED); TSK_FINALIZE SUCCEEDED.
--     Tell me what TSK_GOLD_VALIDATION shows (SKIPPED, or SUCCEEDED as a no-op).
SELECT NAME AS TASK_NAME, STATE, RETURN_VALUE, SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('minute', -5, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30))
WHERE DATABASE_NAME = 'RETAIL_CLICKSTREAM_DB'
ORDER BY SCHEDULED_TIME DESC;

-- A4. Log. Expect the newest run: SILVER_GATE INVESTIGATE, PIPELINE BLOCKED
SELECT LOGGED_AT, RUN_ID, STEP_NAME, STATUS, MESSAGE
FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT DESC
LIMIT 6;

-- A5. RESTORE the real gate (do this before anything else)
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE MODIFY AS
EXECUTE IMMEDIATE $$
DECLARE
    v_result VARCHAR;
    v_status VARCHAR;
BEGIN
    CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_SILVER_GATE() INTO :v_result;
    v_status := SPLIT_PART(:v_result, ':', 1);
    CALL SYSTEM$SET_RETURN_VALUE(:v_status);
END;
$$;


-- ############################################################################
-- B. FAILED PATH  (validation forced to fail)
-- Expected: TSK_GOLD_VALIDATION state FAILED, log GOLD_VALIDATION = FAIL,
-- finalizer logs PIPELINE = FAILED naming TSK_GOLD_VALIDATION.
-- ############################################################################

-- B1. Temporarily make validation fail on purpose
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION SUSPEND;
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION MODIFY AS
    CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_GOLD_VALIDATION(TRUE);
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION RESUME;

-- B2. Run the graph, then wait ~45 seconds
EXECUTE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE;

-- B3. Task states. Expect: Gold-refresh tasks SUCCEEDED, TSK_GOLD_VALIDATION FAILED,
--     TSK_FINALIZE SUCCEEDED.
SELECT NAME AS TASK_NAME, STATE, RETURN_VALUE, SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('minute', -5, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30))
WHERE DATABASE_NAME = 'RETAIL_CLICKSTREAM_DB'
ORDER BY SCHEDULED_TIME DESC;

-- B4. Log. Expect: SILVER_GATE READY, GOLD_VALIDATION FAIL, PIPELINE FAILED
SELECT LOGGED_AT, RUN_ID, STEP_NAME, STATUS, MESSAGE
FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT DESC
LIMIT 6;

-- B5. RESTORE the real validation task (do this before anything else)
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION SUSPEND;
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION MODIFY AS
    CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_GOLD_VALIDATION();
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION RESUME;


-- ############################################################################
-- C. SCD2 PROOF  (scratch copies only - REAL tables are NOT modified)
-- Uses the SAME merge logic as TSK_INVENTORY_SNAPSHOT, pointed at:
--   ZZ_SCD2_TEST_SNAPSHOT = zero-copy clone of INVENTORY_SNAPSHOT
--   ZZ_SCD2_TEST_STG      = copy of STG_INVENTORY with ONE product's stock changed
-- ############################################################################

-- C1. Create the scratch copies
    
CREATE OR REPLACE TRANSIENT TABLE RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_STG AS
    SELECT * FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_INVENTORY;

CREATE OR REPLACE TRANSIENT TABLE RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT
    CLONE RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT;
    
-- C2. Pick a product and change ONLY its stock quantity (+100) in the scratch staging copy
SET test_pid = (SELECT MIN(PRODUCT_ID)
                FROM RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_STG
                WHERE STOCK_QUANTITY IS NOT NULL);
SELECT $test_pid AS TEST_PRODUCT_ID;

UPDATE RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_STG
SET STOCK_QUANTITY = STOCK_QUANTITY + 100
WHERE PRODUCT_ID = $test_pid;

-- C3. BEFORE: expect exactly 1 row for this product, VALID_TO = NULL
SELECT PRODUCT_ID, STOCK_QUANTITY, DBT_VALID_FROM AS VALID_FROM, DBT_VALID_TO AS VALID_TO,
       (DBT_VALID_TO IS NULL) AS IS_CURRENT
FROM RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT
WHERE PRODUCT_ID = $test_pid
ORDER BY DBT_VALID_FROM;

-- C4. Run the snapshot merge (identical logic to the real task).
--     Expect: number of rows inserted = 1, number of rows updated = 1
MERGE INTO RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT AS t
USING (
    WITH run_ts AS (
        SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS ts
    ),
    src AS (
        SELECT PRODUCT_ID, STOCK_QUANTITY, REORDER_LEVEL, INVENTORY_ORIGIN,
               IS_OUT_OF_STOCK, IS_AT_OR_BELOW_REORDER_LEVEL,
               HAS_INVALID_STOCK_QUANTITY, HAS_INVALID_REORDER_LEVEL,
               IS_MISSING_INVENTORY_ORIGIN
        FROM RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_STG
    ),
    cur AS (
        SELECT *
        FROM RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT
        WHERE DBT_VALID_TO IS NULL
    ),
    changes AS (
        SELECT src.*, (cur.PRODUCT_ID IS NULL) AS IS_NEW
        FROM src
        LEFT JOIN cur ON src.PRODUCT_ID = cur.PRODUCT_ID
        WHERE cur.PRODUCT_ID IS NULL
           OR src.STOCK_QUANTITY                IS DISTINCT FROM cur.STOCK_QUANTITY
           OR src.REORDER_LEVEL                 IS DISTINCT FROM cur.REORDER_LEVEL
           OR src.INVENTORY_ORIGIN              IS DISTINCT FROM cur.INVENTORY_ORIGIN
           OR src.IS_OUT_OF_STOCK               IS DISTINCT FROM cur.IS_OUT_OF_STOCK
           OR src.IS_AT_OR_BELOW_REORDER_LEVEL  IS DISTINCT FROM cur.IS_AT_OR_BELOW_REORDER_LEVEL
           OR src.HAS_INVALID_STOCK_QUANTITY    IS DISTINCT FROM cur.HAS_INVALID_STOCK_QUANTITY
           OR src.HAS_INVALID_REORDER_LEVEL     IS DISTINCT FROM cur.HAS_INVALID_REORDER_LEVEL
           OR src.IS_MISSING_INVENTORY_ORIGIN   IS DISTINCT FROM cur.IS_MISSING_INVENTORY_ORIGIN
    )
    SELECT c.PRODUCT_ID AS MERGE_KEY,
           c.PRODUCT_ID, c.STOCK_QUANTITY, c.REORDER_LEVEL, c.INVENTORY_ORIGIN,
           c.IS_OUT_OF_STOCK, c.IS_AT_OR_BELOW_REORDER_LEVEL,
           c.HAS_INVALID_STOCK_QUANTITY, c.HAS_INVALID_REORDER_LEVEL,
           c.IS_MISSING_INVENTORY_ORIGIN, r.ts AS RUN_TS
    FROM changes c CROSS JOIN run_ts r
    WHERE NOT c.IS_NEW
    UNION ALL
    SELECT NULL AS MERGE_KEY,
           c.PRODUCT_ID, c.STOCK_QUANTITY, c.REORDER_LEVEL, c.INVENTORY_ORIGIN,
           c.IS_OUT_OF_STOCK, c.IS_AT_OR_BELOW_REORDER_LEVEL,
           c.HAS_INVALID_STOCK_QUANTITY, c.HAS_INVALID_REORDER_LEVEL,
           c.IS_MISSING_INVENTORY_ORIGIN, r.ts AS RUN_TS
    FROM changes c CROSS JOIN run_ts r
) AS s
ON t.PRODUCT_ID = s.MERGE_KEY AND t.DBT_VALID_TO IS NULL
WHEN MATCHED THEN UPDATE SET
    t.DBT_VALID_TO = s.RUN_TS
WHEN NOT MATCHED THEN INSERT
    (PRODUCT_ID, STOCK_QUANTITY, REORDER_LEVEL, INVENTORY_ORIGIN,
     IS_OUT_OF_STOCK, IS_AT_OR_BELOW_REORDER_LEVEL,
     HAS_INVALID_STOCK_QUANTITY, HAS_INVALID_REORDER_LEVEL, IS_MISSING_INVENTORY_ORIGIN,
     DBT_SCD_ID, DBT_UPDATED_AT, DBT_VALID_FROM, DBT_VALID_TO)
VALUES
    (s.PRODUCT_ID, s.STOCK_QUANTITY, s.REORDER_LEVEL, s.INVENTORY_ORIGIN,
     s.IS_OUT_OF_STOCK, s.IS_AT_OR_BELOW_REORDER_LEVEL,
     s.HAS_INVALID_STOCK_QUANTITY, s.HAS_INVALID_REORDER_LEVEL, s.IS_MISSING_INVENTORY_ORIGIN,
     MD5(COALESCE(CAST(s.PRODUCT_ID AS VARCHAR), '') || '|' || COALESCE(CAST(s.RUN_TS AS VARCHAR), '')),
     s.RUN_TS, s.RUN_TS, NULL);

-- C5. AFTER: expect 2 rows for this product.
--     Row 1 (old stock): VALID_TO is now set, IS_CURRENT = FALSE.
--     Row 2 (stock + 100): VALID_FROM = row 1's VALID_TO, VALID_TO = NULL, IS_CURRENT = TRUE.
SELECT PRODUCT_ID, STOCK_QUANTITY, DBT_VALID_FROM AS VALID_FROM, DBT_VALID_TO AS VALID_TO,
       (DBT_VALID_TO IS NULL) AS IS_CURRENT
FROM RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT
WHERE PRODUCT_ID = $test_pid
ORDER BY DBT_VALID_FROM;

-- C6. Nothing else changed. Expect: total_rows = 166795 (166794 + 1 new version),
--     current_rows = 166794, products_with_multiple_current = 0
SELECT COUNT(*)                        AS total_rows,
       COUNT_IF(DBT_VALID_TO IS NULL)  AS current_rows,
       (SELECT COUNT(*) FROM (
            SELECT PRODUCT_ID FROM RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT
            WHERE DBT_VALID_TO IS NULL GROUP BY 1 HAVING COUNT(*) > 1)) AS products_with_multiple_current
FROM RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT;

-- C7. Re-run the exact MERGE from C4 once more (copy it again).
--     Expect: 0 rows inserted, 0 rows updated  (running twice never creates duplicate versions)

-- C8. CLEAN UP the scratch tables
DROP TABLE IF EXISTS RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_SNAPSHOT;
DROP TABLE IF EXISTS RETAIL_CLICKSTREAM_DB.MARTS.ZZ_SCD2_TEST_STG;


-- ############################################################################
-- D. RESTORE, FINAL CLEAN RUN, SCHEDULE BACK ON
-- ############################################################################

-- D1. Confirm both temporary changes were undone.
--     Expect: NO definition contains "(TRUE)".
SHOW TASKS IN SCHEMA RETAIL_CLICKSTREAM_DB.MARTS;
SELECT "name", "state", "definition"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('TSK_GATE', 'TSK_GOLD_VALIDATION');

-- D2. One clean run (also resets the consecutive-failure counter after test B), wait ~45 s
EXECUTE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE;

-- D3. Expect the newest run: SILVER_GATE READY, GOLD_VALIDATION PASS, PIPELINE SUCCESS
SELECT LOGGED_AT, RUN_ID, STEP_NAME, STATUS, MESSAGE
FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
ORDER BY LOGGED_AT DESC
LIMIT 4;

-- D4. ONLY if D1 is clean and D3 shows SUCCESS: turn the 15-minute schedule on
ALTER TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE RESUME;

SHOW TASKS LIKE 'TSK_GATE' IN SCHEMA RETAIL_CLICKSTREAM_DB.MARTS;
SELECT "name", "state", "schedule" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));