-- ============================================================================
-- 06_orchestration / 03_log_and_gate.sql
-- STEP 3: (a) PIPELINE_RUN_LOG  - one row per pipeline step, per run
--         (b) SP_SILVER_GATE    - Silver readiness check; first task of the DAG
--
-- The gate extends 01_silver_readiness.sql: besides RAW-vs-STG counts and
-- NULL / duplicate EVENT_IDs, it also checks the keys the Gold MERGEs rely on
-- (a MERGE fails on duplicate source keys, and inserts duplicates on NULL keys).
--
-- If everything is clean  -> returns 'READY'        (Gold tasks will run)
-- If anything is wrong    -> returns 'INVESTIGATE'  (Gold tasks will be skipped)
-- ============================================================================
USE ROLE DBT_SILVER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE RETAIL_CLICKSTREAM_DB;
USE SCHEMA MARTS;


-- ============================================================================
-- A. RUN LOG
-- RUN_ID = the task-graph run id when called from a task (all tasks of one run
-- share it), or a random UUID when called by hand.
-- ============================================================================
CREATE TABLE IF NOT EXISTS RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG (
    LOG_ID     NUMBER AUTOINCREMENT,
    RUN_ID     VARCHAR,
    LOGGED_AT  TIMESTAMP_NTZ DEFAULT SYSDATE(),   -- SYSDATE() = UTC
    STEP_NAME  VARCHAR,                           -- e.g. SILVER_GATE, GOLD_VALIDATION, PIPELINE
    STATUS     VARCHAR,                           -- READY / INVESTIGATE / PASS / FAIL / SUCCESS / FAILED / BLOCKED
    MESSAGE    VARCHAR,
    METRICS    VARIANT
);


-- ============================================================================
-- B. SILVER READINESS GATE
-- P_FORCE_INVESTIGATE is for testing only: CALL SP_SILVER_GATE(TRUE) simulates a
-- failed gate so you can see the "blocked" path without damaging any data.
-- ============================================================================
CREATE OR REPLACE PROCEDURE RETAIL_CLICKSTREAM_DB.MARTS.SP_SILVER_GATE(P_FORCE_INVESTIGATE BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_run_id            VARCHAR;
    v_raw_rows          NUMBER;
    v_stg_rows          NUMBER;
    v_null_event_ids    NUMBER;
    v_dup_event_ids     NUMBER;
    v_dup_customers     NUMBER;
    v_dup_products      NUMBER;
    v_dup_inventory     NUMBER;
    v_dup_order_items   NUMBER;
    v_null_other_keys   NUMBER;
    v_issues            VARCHAR DEFAULT '';
    v_status            VARCHAR;
    v_message           VARCHAR;
BEGIN
    -- Run id shared by every task of this graph run (falls back to a UUID outside a task)
    BEGIN
        v_run_id := SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');
    EXCEPTION
        WHEN OTHER THEN v_run_id := NULL;
    END;
    v_run_id := COALESCE(v_run_id, UUID_STRING());

    -- All checks in ONE statement so they see the same consistent snapshot of the data
    WITH raw_c AS (
        SELECT COUNT(*) AS raw_rows
        FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS
    ),
    ev_c AS (
        SELECT COUNT(*)                                  AS stg_rows,
               COUNT_IF(EVENT_ID IS NULL)                AS null_event_ids,
               COUNT(EVENT_ID) - COUNT(DISTINCT EVENT_ID) AS dup_event_ids
        FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_CLICKSTREAM_EVENTS
    ),
    cust_c AS (
        SELECT COUNT(CUSTOMER_ID) - COUNT(DISTINCT CUSTOMER_ID) AS dup_customers,
               COUNT_IF(CUSTOMER_ID IS NULL)                    AS null_customers
        FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_CUSTOMERS
    ),
    prod_c AS (
        SELECT COUNT(PRODUCT_ID) - COUNT(DISTINCT PRODUCT_ID) AS dup_products,
               COUNT_IF(PRODUCT_ID IS NULL)                   AS null_products
        FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_PRODUCTS
    ),
    inv_c AS (
        SELECT COUNT(PRODUCT_ID) - COUNT(DISTINCT PRODUCT_ID) AS dup_inventory,
               COUNT_IF(PRODUCT_ID IS NULL)                   AS null_inventory
        FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_INVENTORY
    ),
    oi_c AS (
        -- same join FACT_ORDER_ITEMS uses
        SELECT COUNT_IF(oi.ORDER_ID IS NOT NULL AND oi.PRODUCT_ID IS NOT NULL)
                   - COUNT(DISTINCT oi.ORDER_ID, oi.PRODUCT_ID) AS dup_order_items,
               COUNT_IF(oi.ORDER_ID IS NULL OR oi.PRODUCT_ID IS NULL) AS null_order_item_keys
        FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_ORDER_ITEMS AS oi
        INNER JOIN RETAIL_CLICKSTREAM_DB.STAGING.STG_ORDERS AS ord
            ON oi.ORDER_ID = ord.ORDER_ID
    )
    SELECT raw_c.raw_rows,
           ev_c.stg_rows,
           ev_c.null_event_ids,
           ev_c.dup_event_ids,
           cust_c.dup_customers,
           prod_c.dup_products,
           inv_c.dup_inventory,
           oi_c.dup_order_items,
           cust_c.null_customers + prod_c.null_products + inv_c.null_inventory + oi_c.null_order_item_keys
    INTO :v_raw_rows, :v_stg_rows, :v_null_event_ids, :v_dup_event_ids,
         :v_dup_customers, :v_dup_products, :v_dup_inventory, :v_dup_order_items,
         :v_null_other_keys
    FROM raw_c CROSS JOIN ev_c CROSS JOIN cust_c CROSS JOIN prod_c CROSS JOIN inv_c CROSS JOIN oi_c;

    -- Build a human-readable list of anything that failed
    IF (v_raw_rows <> v_stg_rows) THEN
        v_issues := v_issues || 'RAW vs STG clickstream count mismatch (' || v_raw_rows || ' vs ' || v_stg_rows || '); ';
    END IF;
    IF (v_null_event_ids > 0) THEN
        v_issues := v_issues || v_null_event_ids || ' NULL EVENT_ID(s); ';
    END IF;
    IF (v_dup_event_ids > 0) THEN
        v_issues := v_issues || v_dup_event_ids || ' duplicate EVENT_ID row(s); ';
    END IF;
    IF (v_dup_customers > 0) THEN
        v_issues := v_issues || v_dup_customers || ' duplicate CUSTOMER_ID(s); ';
    END IF;
    IF (v_dup_products > 0) THEN
        v_issues := v_issues || v_dup_products || ' duplicate PRODUCT_ID(s) in products; ';
    END IF;
    IF (v_dup_inventory > 0) THEN
        v_issues := v_issues || v_dup_inventory || ' duplicate PRODUCT_ID(s) in inventory; ';
    END IF;
    IF (v_dup_order_items > 0) THEN
        v_issues := v_issues || v_dup_order_items || ' duplicate (ORDER_ID, PRODUCT_ID) pair(s); ';
    END IF;
    IF (v_null_other_keys > 0) THEN
        v_issues := v_issues || v_null_other_keys || ' NULL key(s) in customers/products/inventory/order items; ';
    END IF;
    IF (P_FORCE_INVESTIGATE) THEN
        v_issues := v_issues || 'FORCED FAILURE (test mode); ';
    END IF;

    IF (v_issues = '') THEN
        v_status  := 'READY';
        v_message := 'All Silver readiness checks passed';
    ELSE
        v_status  := 'INVESTIGATE';
        v_message := v_issues;
    END IF;

    INSERT INTO RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG (RUN_ID, STEP_NAME, STATUS, MESSAGE, METRICS)
    SELECT :v_run_id, 'SILVER_GATE', :v_status, :v_message,
           OBJECT_CONSTRUCT(
               'raw_rows',          :v_raw_rows,
               'stg_rows',          :v_stg_rows,
               'null_event_ids',    :v_null_event_ids,
               'dup_event_ids',     :v_dup_event_ids,
               'dup_customers',     :v_dup_customers,
               'dup_products',      :v_dup_products,
               'dup_inventory',     :v_dup_inventory,
               'dup_order_items',   :v_dup_order_items,
               'null_other_keys',   :v_null_other_keys
           );

    -- Tell the child tasks the result. Inside a task graph, children read it with
    -- SYSTEM$GET_PREDECESSOR_RETURN_VALUE('TSK_GATE'). Outside a task this call is
    -- not allowed, so ignore the error when testing by hand.
    BEGIN
        CALL SYSTEM$SET_RETURN_VALUE(:v_status);
    EXCEPTION
        WHEN OTHER THEN NULL;
    END;

    RETURN v_status || ': ' || v_message;
END;
$$;


-- ============================================================================
-- C. TEST BY HAND
-- ============================================================================
-- C1. Normal run - expect:  READY: All Silver readiness checks passed
CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_SILVER_GATE();

-- C2. Simulated failure - expect:  INVESTIGATE: FORCED FAILURE (test mode);
CALL RETAIL_CLICKSTREAM_DB.MARTS.SP_SILVER_GATE(TRUE);

-- C3. Both runs are logged (expect 2 rows; METRICS holds every counter)
SELECT LOG_ID, RUN_ID, LOGGED_AT, STEP_NAME, STATUS, MESSAGE, METRICS
FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG
ORDER BY LOG_ID DESC;

-- C4. Optional cleanup of the test rows before the real DAG goes live
DELETE FROM RETAIL_CLICKSTREAM_DB.MARTS.PIPELINE_RUN_LOG;