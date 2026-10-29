-- ============================================================================
-- 06_orchestration / 05_validation_and_finalizer.sql
-- STEP 5: (a) SP_GOLD_VALIDATION   - Post-load Gold checks (counts, keys, orphans, SCD2)
--         (b) SP_FINALIZE_PIPELINE - Determines overall run outcome (SUCCESS / FAILED / BLOCKED)
--         (c) TSK_GOLD_VALIDATION  - Leaf task after all 6 Gold refresh branches
--         (d) TSK_FINALIZE         - Finalizer task attached to TSK_GATE (FINALIZE = TSK_GATE)
--
-- HOW TO RUN:
--   1. Run Section A (creates both stored procedures)
--   2. Run Section B (tests by hand and cleans up the test log rows)
--   3. Run Section C (creates TSK_GOLD_VALIDATION and TSK_FINALIZE, both suspended)
--   4. Run Section D (verifies all 10 tasks exist and are suspended)
-- ============================================================================
USE ROLE DBT_SILVER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE RETAIL_CLICKSTREAM_DB;
USE SCHEMA MARTS;


-- ============================================================================
-- A1. STORED PROCEDURE: SP_GOLD_VALIDATION
-- P_FORCE_FAIL = TRUE lets us test the FAILED path by hand or in Step 6.
-- ============================================================================
CREATE OR REPLACE PROCEDURE RETAIL_CLICKSTREAM_DB.MARTS.SP_GOLD_VALIDATION(P_FORCE_FAIL BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_run_id                VARCHAR;
    v_gate_status           VARCHAR;
    -- Row count diffs (Gold rows minus Staging distinct keys)
    v_diff_cust             NUMBER;
    v_diff_date             NUMBER;
    v_diff_prod             NUMBER;
    v_diff_oi               NUMBER;
    v_diff_ev               NUMBER;
    v_diff_inv_cur          NUMBER;
    -- Duplicate or NULL keys in Gold
    v_bad_keys_cust         NUMBER;
    v_bad_keys_date         NUMBER;
    v_bad_keys_prod         NUMBER;
    v_bad_keys_oi           NUMBER;
    v_bad_keys_ev           NUMBER;
    v_bad_keys_inv          NUMBER;
    -- Orphan PRODUCT_IDs in Facts vs DIM_PRODUCT
    v_orphan_oi_prod        NUMBER;
    v_orphan_ev_prod        NUMBER;
    -- SCD2 integrity in DIM_INVENTORY
    v_inv_multi_current     NUMBER;
    v_issues                VARCHAR DEFAULT '';
    v_status                VARCHAR;
    v_message               VARCHAR;
    e_validation_failed     EXCEPTION (-20001, 'Gold validation failed - see PIPELINE_RUN_LOG for details.');
BEGIN
    -- 1. Identify current graph run (or fallback UUID when called by hand)
    BEGIN
        v_run_id := SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');
    EXCEPTION
        WHEN OTHER THEN v_run_id := NULL;
    END;
    v_run_id := COALESCE(v_run_id, UUID_STRING());

    -- 2. Safety guard: if SILVER_GATE blocked this run_id, no-op immediately
    SELECT MAX(STATUS)
    INTO :v_gate_status
    FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
    WHERE RUN_ID = :v_run_id
      AND STEP_NAME = 'SILVER_GATE';

    IF (v_gate_status = 'INVESTIGATE') THEN
        RETURN 'SKIPPED: Silver gate returned INVESTIGATE for this run.';
    END IF;

    -- 3. Compute all Gold quality metrics using CTEs + FROM ... CROSS JOIN
    WITH c_cust AS (
        SELECT
            (SELECT COUNT(*) FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_CUSTOMER)
                - (SELECT COUNT(DISTINCT CUSTOMER_ID) FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_CUSTOMERS) AS diff_cust,
            COUNT(*) - COUNT(DISTINCT CUSTOMER_ID) + COUNT_IF(CUSTOMER_ID IS NULL) AS bad_keys_cust
        FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_CUSTOMER
    ),
    c_date AS (
        SELECT
            (SELECT COUNT(*) FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_DATE)
                - (SELECT COUNT(DISTINCT ORDER_TIMESTAMP) FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_ORDERS WHERE ORDER_TIMESTAMP IS NOT NULL) AS diff_date,
            COUNT(*) - COUNT(DISTINCT ORDER_TIMESTAMP) + COUNT_IF(ORDER_TIMESTAMP IS NULL) AS bad_keys_date
        FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_DATE
    ),
    c_prod AS (
        SELECT
            (SELECT COUNT(*) FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_PRODUCT)
                - (SELECT COUNT(DISTINCT PRODUCT_ID) FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_PRODUCTS) AS diff_prod,
            COUNT(*) - COUNT(DISTINCT PRODUCT_ID) + COUNT_IF(PRODUCT_ID IS NULL) AS bad_keys_prod
        FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_PRODUCT
    ),
    c_oi AS (
        SELECT
            (SELECT COUNT(*) FROM RETAIL_CLICKSTREAM_DB.MARTS.FACT_ORDER_ITEMS)
                - (SELECT COUNT(*) FROM (
                       SELECT DISTINCT oi.ORDER_ID, oi.PRODUCT_ID
                       FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_ORDER_ITEMS oi
                       INNER JOIN RETAIL_CLICKSTREAM_DB.STAGING.STG_ORDERS o ON oi.ORDER_ID = o.ORDER_ID)) AS diff_oi,
            COUNT(*) - COUNT(DISTINCT ORDER_ID, PRODUCT_ID) + COUNT_IF(ORDER_ID IS NULL OR PRODUCT_ID IS NULL) AS bad_keys_oi
        FROM RETAIL_CLICKSTREAM_DB.MARTS.FACT_ORDER_ITEMS
    ),
    c_ev AS (
        SELECT
            IFF(COUNT(*) = 0, -1, 0) AS diff_ev,
            COUNT(*) - COUNT(DISTINCT EVENT_ID) + COUNT_IF(EVENT_ID IS NULL) AS bad_keys_ev
        FROM RETAIL_CLICKSTREAM_DB.MARTS.FACT_CLICKSTREAM_EVENTS
    ),
    c_inv AS (
        SELECT
            COUNT_IF(IS_CURRENT)
                - (SELECT COUNT(DISTINCT PRODUCT_ID) FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_INVENTORY) AS diff_inv_cur,
            COUNT(*) - COUNT(DISTINCT PRODUCT_ID, VALID_FROM) + COUNT_IF(PRODUCT_ID IS NULL OR VALID_FROM IS NULL) AS bad_keys_inv
        FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_INVENTORY
    ),
    c_orph_oi AS (
        SELECT COUNT(*) AS orphan_oi_prod
        FROM RETAIL_CLICKSTREAM_DB.MARTS.FACT_ORDER_ITEMS f
        LEFT JOIN RETAIL_CLICKSTREAM_DB.MARTS.DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
        WHERE f.PRODUCT_ID IS NOT NULL AND p.PRODUCT_ID IS NULL
    ),
    c_orph_ev AS (
        SELECT COUNT(*) AS orphan_ev_prod
        FROM RETAIL_CLICKSTREAM_DB.MARTS.FACT_CLICKSTREAM_EVENTS f
        LEFT JOIN RETAIL_CLICKSTREAM_DB.MARTS.DIM_PRODUCT p ON f.PRODUCT_ID = p.PRODUCT_ID
        WHERE f.PRODUCT_ID IS NOT NULL AND p.PRODUCT_ID IS NULL
    ),
    c_multi_cur AS (
        SELECT COUNT(*) AS inv_multi_current
        FROM (
            SELECT PRODUCT_ID
            FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_INVENTORY
            WHERE IS_CURRENT = TRUE
            GROUP BY PRODUCT_ID
            HAVING COUNT(*) > 1
        )
    )
    SELECT
        c_cust.diff_cust,
        c_date.diff_date,
        c_prod.diff_prod,
        c_oi.diff_oi,
        c_ev.diff_ev,
        c_inv.diff_inv_cur,
        c_cust.bad_keys_cust,
        c_date.bad_keys_date,
        c_prod.bad_keys_prod,
        c_oi.bad_keys_oi,
        c_ev.bad_keys_ev,
        c_inv.bad_keys_inv,
        c_orph_oi.orphan_oi_prod,
        c_orph_ev.orphan_ev_prod,
        c_multi_cur.inv_multi_current
    INTO
        :v_diff_cust, :v_diff_date, :v_diff_prod, :v_diff_oi, :v_diff_ev, :v_diff_inv_cur,
        :v_bad_keys_cust, :v_bad_keys_date, :v_bad_keys_prod, :v_bad_keys_oi, :v_bad_keys_ev, :v_bad_keys_inv,
        :v_orphan_oi_prod, :v_orphan_ev_prod,
        :v_inv_multi_current
    FROM c_cust
    CROSS JOIN c_date
    CROSS JOIN c_prod
    CROSS JOIN c_oi
    CROSS JOIN c_ev
    CROSS JOIN c_inv
    CROSS JOIN c_orph_oi
    CROSS JOIN c_orph_ev
    CROSS JOIN c_multi_cur;

    -- 4. Build human-readable failure list
    IF (v_diff_cust <> 0) THEN
        v_issues := v_issues || 'DIM_CUSTOMER count diff=' || v_diff_cust || '; ';
    END IF;
    IF (v_diff_date <> 0) THEN
        v_issues := v_issues || 'DIM_DATE count diff=' || v_diff_date || '; ';
    END IF;
    IF (v_diff_prod <> 0) THEN
        v_issues := v_issues || 'DIM_PRODUCT count diff=' || v_diff_prod || '; ';
    END IF;
    IF (v_diff_oi <> 0) THEN
        v_issues := v_issues || 'FACT_ORDER_ITEMS count diff=' || v_diff_oi || '; ';
    END IF;
    IF (v_diff_ev <> 0) THEN
        v_issues := v_issues || 'FACT_CLICKSTREAM_EVENTS is empty; ';
    END IF;
    IF (v_diff_inv_cur <> 0) THEN
        v_issues := v_issues || 'DIM_INVENTORY current rows diff=' || v_diff_inv_cur || '; ';
    END IF;
    IF (v_bad_keys_cust + v_bad_keys_date + v_bad_keys_prod + v_bad_keys_oi + v_bad_keys_ev + v_bad_keys_inv > 0) THEN
        v_issues := v_issues || 'Duplicate or NULL primary keys in Gold; ';
    END IF;
    IF (v_orphan_oi_prod > 0) THEN
        v_issues := v_issues || v_orphan_oi_prod || ' orphan PRODUCT_ID(s) in FACT_ORDER_ITEMS; ';
    END IF;
    IF (v_orphan_ev_prod > 0) THEN
        v_issues := v_issues || v_orphan_ev_prod || ' orphan PRODUCT_ID(s) in FACT_CLICKSTREAM_EVENTS; ';
    END IF;
    IF (v_inv_multi_current > 0) THEN
        v_issues := v_issues || v_inv_multi_current || ' product(s) with multiple IS_CURRENT=TRUE rows in DIM_INVENTORY; ';
    END IF;
    IF (P_FORCE_FAIL) THEN
        v_issues := v_issues || 'FORCED GOLD VALIDATION FAILURE (test mode); ';
    END IF;

    IF (v_issues = '') THEN
        v_status  := 'PASS';
        v_message := 'All Gold validation checks passed';
    ELSE
        v_status  := 'FAIL';
        v_message := v_issues;
    END IF;

    -- 5. Log result to PIPELINE_RUN_LOG
    INSERT INTO RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG (RUN_ID, STEP_NAME, STATUS, MESSAGE, METRICS)
    SELECT :v_run_id, 'GOLD_VALIDATION', :v_status, :v_message,
           OBJECT_CONSTRUCT(
               'diff_cust',          :v_diff_cust,
               'diff_date',          :v_diff_date,
               'diff_prod',          :v_diff_prod,
               'diff_order_items',   :v_diff_oi,
               'diff_inv_current',   :v_diff_inv_cur,
               'bad_keys_total',     :v_bad_keys_cust + :v_bad_keys_date + :v_bad_keys_prod + :v_bad_keys_oi + :v_bad_keys_ev + :v_bad_keys_inv,
               'orphan_oi_prod',     :v_orphan_oi_prod,
               'orphan_ev_prod',     :v_orphan_ev_prod,
               'inv_multi_current',  :v_inv_multi_current
           );

    BEGIN
        CALL SYSTEM$SET_RETURN_VALUE(:v_status);
    EXCEPTION
        WHEN OTHER THEN NULL;
    END;

    -- If validation failed inside a task run, raise exception so the task state becomes FAILED
    IF (v_status = 'FAIL') THEN
        RAISE e_validation_failed;
    END IF;

    RETURN v_status || ': ' || v_message;
END;
$$;


-- ============================================================================
-- A2. STORED PROCEDURE: SP_FINALIZE_PIPELINE
-- Called by TSK_FINALIZE (FINALIZE = TSK_GATE) at the end of every graph run.
-- Writes STEP_NAME = 'PIPELINE' with STATUS IN ('SUCCESS', 'FAILED', 'BLOCKED').
-- ============================================================================
CREATE OR REPLACE PROCEDURE RETAIL_CLICKSTREAM_DB.MARTS.SP_FINALIZE_PIPELINE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_run_id          VARCHAR;
    v_gate_status     VARCHAR;
    v_gate_msg        VARCHAR;
    v_val_status      VARCHAR;
    v_val_msg         VARCHAR;
    v_failed_tasks    NUMBER DEFAULT 0;
    v_failed_names    VARCHAR DEFAULT '';
    v_final_status    VARCHAR;
    v_final_message   VARCHAR;
BEGIN
    -- 1. Get the graph run group ID (or most recent SILVER_GATE run_id when testing by hand)
    BEGIN
        v_run_id := SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');
    EXCEPTION
        WHEN OTHER THEN v_run_id := NULL;
    END;

    IF (v_run_id IS NULL) THEN
        SELECT MAX(RUN_ID) INTO :v_run_id
        FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
        WHERE STEP_NAME = 'SILVER_GATE';
    END IF;
    v_run_id := COALESCE(v_run_id, UUID_STRING());

    -- 2. Read SILVER_GATE and GOLD_VALIDATION log entries for this RUN_ID
    SELECT MAX(STATUS), MAX(MESSAGE)
    INTO :v_gate_status, :v_gate_msg
    FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
    WHERE RUN_ID = :v_run_id AND STEP_NAME = 'SILVER_GATE';

    SELECT MAX(STATUS), MAX(MESSAGE)
    INTO :v_val_status, :v_val_msg
    FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
    WHERE RUN_ID = :v_run_id AND STEP_NAME = 'GOLD_VALIDATION';

    -- 3. Check INFORMATION_SCHEMA.TASK_HISTORY for any FAILED task in this graph run group
    BEGIN
        SELECT COUNT(*), COALESCE(LISTAGG(NAME, ', '), '')
        INTO :v_failed_tasks, :v_failed_names
        FROM TABLE(RETAIL_CLICKSTREAM_DB.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD('hour', -2, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 200
        ))
        WHERE GRAPH_RUN_GROUP_ID = :v_run_id
          AND STATE IN ('FAILED', 'CANCELLED');
    EXCEPTION
        WHEN OTHER THEN
            v_failed_tasks := 0;
            v_failed_names := '';
    END;

    -- 4. Decide overall pipeline status: BLOCKED vs FAILED vs SUCCESS
    IF (v_gate_status = 'INVESTIGATE') THEN
        v_final_status  := 'BLOCKED';
        v_final_message := 'Pipeline blocked by Silver readiness gate: ' || COALESCE(v_gate_msg, 'INVESTIGATE');
    ELSEIF (v_val_status = 'FAIL' OR v_failed_tasks > 0) THEN
        v_final_status  := 'FAILED';
        v_final_message := 'Pipeline failed. '
            || IFF(v_failed_tasks > 0, 'Failed task(s): ' || v_failed_names || '. ', '')
            || IFF(v_val_status = 'FAIL', 'Validation issues: ' || COALESCE(v_val_msg, ''), '');
    ELSEIF (v_gate_status = 'READY' AND v_val_status = 'PASS') THEN
        v_final_status  := 'SUCCESS';
        v_final_message := 'Pipeline completed and validated end-to-end.';
    ELSE
        v_final_status  := 'FAILED';
        v_final_message := 'Pipeline ended in an incomplete state (gate='
            || COALESCE(v_gate_status, 'NULL') || ', validation=' || COALESCE(v_val_status, 'NULL') || ').';
    END IF;

    INSERT INTO RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG (RUN_ID, STEP_NAME, STATUS, MESSAGE, METRICS)
    SELECT :v_run_id, 'PIPELINE', :v_final_status, :v_final_message,
           OBJECT_CONSTRUCT(
               'gate_status',       :v_gate_status,
               'validation_status', :v_val_status,
               'failed_task_count', :v_failed_tasks,
               'failed_tasks',      :v_failed_names
           );

    RETURN v_final_status || ': ' || v_final_message;
END;
$$;


-- ============================================================================
-- B. TEST BY HAND (before wiring the tasks)
-- ============================================================================
-- B1. Expect:  PASS: All Gold validation checks passed
CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_GOLD_VALIDATION();

-- B2. Inspect PIPELINE_RUN_LOG (should show the GOLD_VALIDATION PASS row)
SELECT LOG_ID, RUN_ID, LOGGED_AT, STEP_NAME, STATUS, MESSAGE, METRICS
FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
ORDER BY LOG_ID DESC;

-- B3. Clean up the by-hand test rows so the table is clean for Step 6
DELETE FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG;


-- ============================================================================
-- C. ADD TSK_GOLD_VALIDATION & TSK_FINALIZE TO THE TASK GRAPH (SUSPENDED)
-- ============================================================================
-- Root must be suspended when adding tasks to the graph
ALTER TASK IF EXISTS RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE SUSPEND;

-- C1. TSK_GOLD_VALIDATION: waits for all 6 leaf Gold tasks
CREATE OR REPLACE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_GOLD_VALIDATION
    WAREHOUSE = RETAIL_WH
    USER_TASK_TIMEOUT_MS = 600000
    AFTER RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_CUSTOMER,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_DATE,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_PRODUCT,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_FACT_ORDER_ITEMS,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_FACT_CLICKSTREAM_EVENTS,
          RETAIL_CLICKSTREAM_DB.MARTS.TSK_DIM_INVENTORY
    COMMENT = 'Validates Gold counts, keys, orphans, and SCD2 after all 6 Gold branches finish.'
AS
    CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_GOLD_VALIDATION();


-- C2. TSK_FINALIZE: Finalizer task attached to TSK_GATE (always runs at graph completion)
CREATE OR REPLACE TASK RETAIL_CLICKSTREAM_DB.MARTS.TSK_FINALIZE
    WAREHOUSE = RETAIL_WH
    USER_TASK_TIMEOUT_MS = 600000
    FINALIZE = RETAIL_CLICKSTREAM_DB.MARTS.TSK_GATE
AS
    CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_FINALIZE_PIPELINE();


-- ============================================================================
-- D. VERIFY ALL 10 TASKS IN THE GRAPH (still suspended)
-- ============================================================================
SHOW TASKS IN SCHEMA RETAIL_CLICKSTREAM_DB.MARTS;
SELECT "name", "state", "schedule", "predecessors", "condition"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";