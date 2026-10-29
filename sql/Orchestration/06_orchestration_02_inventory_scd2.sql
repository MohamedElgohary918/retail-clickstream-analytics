-- ============================================================================
-- 06_orchestration / 02_inventory_scd2.sql
-- STEP 2: Plain-SQL replacement for
--     (a) dbt snapshot  inventory_snapshot   (check strategy, key PRODUCT_ID)
--     (b) dbt model     dim_inventory        (table rebuilt from the snapshot)
--
-- Run in this order:  A (checks)  ->  B (snapshot)  ->  C (dim_inventory)  ->  D (verify)
-- STOP after Section A if the snapshot table is NOT in MARTS - tell me first.
-- ============================================================================
USE ROLE DBT_SILVER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE RETAIL_CLICKSTREAM_DB;
USE SCHEMA MARTS;


-- ============================================================================
-- A. PRE-FLIGHT CHECKS
-- ============================================================================
-- A1. Where does the snapshot table actually live? (expect: schema MARTS, 1 row)
SHOW TABLES LIKE 'INVENTORY_SNAPSHOT' IN DATABASE RETAIL_CLICKSTREAM_DB;

-- A2. Does it have the columns this script writes? (expect the 9 inventory columns
--     + DBT_SCD_ID, DBT_UPDATED_AT, DBT_VALID_FROM, DBT_VALID_TO)
DESCRIBE TABLE RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT;

-- A3. Current state of the history table
SELECT COUNT(*)                          AS total_rows,
       COUNT_IF(DBT_VALID_TO IS NULL)    AS current_rows,
       MIN(DBT_VALID_FROM)               AS first_valid_from,
       MAX(DBT_VALID_FROM)               AS last_valid_from
FROM RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT;

-- A4. Staging must have exactly one row per product (snapshot MERGE assumes it)
SELECT COUNT(*)                     AS staging_rows,
       COUNT(DISTINCT PRODUCT_ID)   AS staging_products,
       COUNT_IF(PRODUCT_ID IS NULL) AS null_product_ids
FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_INVENTORY;


-- ============================================================================
-- B. SNAPSHOT REFRESH  (= one future task body: TSK_INVENTORY_SNAPSHOT)
-- Same logic as dbt's check strategy, in ONE atomic MERGE:
--   * product not in snapshot            -> insert first version
--   * any of the 8 check columns changed -> close current version (DBT_VALID_TO = now)
--                                           and insert a new version
--   * nothing changed                    -> nothing happens (0 inserted / 0 updated)
-- Products that disappear from staging are left untouched (dbt default).
-- The first source branch carries the real key (used to CLOSE old versions);
-- the second carries a NULL key so it never matches and is always INSERTED.
-- ============================================================================
MERGE INTO RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT AS t
USING (
    WITH run_ts AS (
        SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS ts
    ),
    src AS (
        SELECT PRODUCT_ID, STOCK_QUANTITY, REORDER_LEVEL, INVENTORY_ORIGIN,
               IS_OUT_OF_STOCK, IS_AT_OR_BELOW_REORDER_LEVEL,
               HAS_INVALID_STOCK_QUANTITY, HAS_INVALID_REORDER_LEVEL,
               IS_MISSING_INVENTORY_ORIGIN
        FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_INVENTORY
    ),
    cur AS (
        SELECT *
        FROM RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT
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
    -- Branch 1: changed products only -> match the current version so it can be closed
    SELECT c.PRODUCT_ID AS MERGE_KEY,
           c.PRODUCT_ID, c.STOCK_QUANTITY, c.REORDER_LEVEL, c.INVENTORY_ORIGIN,
           c.IS_OUT_OF_STOCK, c.IS_AT_OR_BELOW_REORDER_LEVEL,
           c.HAS_INVALID_STOCK_QUANTITY, c.HAS_INVALID_REORDER_LEVEL,
           c.IS_MISSING_INVENTORY_ORIGIN, r.ts AS RUN_TS
    FROM changes c CROSS JOIN run_ts r
    WHERE NOT c.IS_NEW
    UNION ALL
    -- Branch 2: new + changed products -> NULL key, never matches, always inserted
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


-- ============================================================================
-- C. DIM_INVENTORY REBUILD  (= one future task body: TSK_DIM_INVENTORY)
-- dbt materialized this model as a TABLE rebuilt from the snapshot on every run.
-- INSERT OVERWRITE does the same atomically WITHOUT dropping the table, so grants
-- and your VW_INVENTORY_SCD2_HEALTH view keep working.
-- ============================================================================
INSERT OVERWRITE INTO RETAIL_CLICKSTREAM_DB.MARTS.DIM_INVENTORY
    (PRODUCT_ID, STOCK_QUANTITY, REORDER_LEVEL, INVENTORY_ORIGIN,
     IS_OUT_OF_STOCK, IS_AT_OR_BELOW_REORDER_LEVEL,
     HAS_INVALID_STOCK_QUANTITY, HAS_INVALID_REORDER_LEVEL, IS_MISSING_INVENTORY_ORIGIN,
     VALID_FROM, VALID_TO, IS_CURRENT)
SELECT
    PRODUCT_ID, STOCK_QUANTITY, REORDER_LEVEL, INVENTORY_ORIGIN,
    IS_OUT_OF_STOCK, IS_AT_OR_BELOW_REORDER_LEVEL,
    HAS_INVALID_STOCK_QUANTITY, HAS_INVALID_REORDER_LEVEL, IS_MISSING_INVENTORY_ORIGIN,
    DBT_VALID_FROM AS VALID_FROM,
    DBT_VALID_TO   AS VALID_TO,
    CASE WHEN DBT_VALID_TO IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT
FROM RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT;


-- ============================================================================
-- D. VERIFICATION
-- Expected:
--   snapshot_current          = staging_products
--   products_multi_current    = 0   (never two "current" versions of one product)
--   dim_rows                  = snapshot_rows
--   dim_current               = snapshot_current
-- ============================================================================
SELECT
    (SELECT COUNT(*) FROM RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT)                         AS snapshot_rows,
    (SELECT COUNT(*) FROM RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT WHERE DBT_VALID_TO IS NULL) AS snapshot_current,
    (SELECT COUNT(DISTINCT PRODUCT_ID) FROM RETAIL_CLICKSTREAM_DB.STAGING.STG_INVENTORY)          AS staging_products,
    (SELECT COUNT(*) FROM (
         SELECT PRODUCT_ID FROM RETAIL_CLICKSTREAM_DB.MARTS.INVENTORY_SNAPSHOT
         WHERE DBT_VALID_TO IS NULL GROUP BY 1 HAVING COUNT(*) > 1))                              AS products_multi_current,
    (SELECT COUNT(*) FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_INVENTORY)                              AS dim_rows,
    (SELECT COUNT_IF(IS_CURRENT) FROM RETAIL_CLICKSTREAM_DB.MARTS.DIM_INVENTORY)                  AS dim_current;