-- =============================================================================
-- File: 01 RAW Tables.sql
-- Project: Real-Time Clickstream Ingestion & Omnichannel Data Warehouse
--
-- Purpose:
-- Create the RAW tables for clickstream events and OLTP data.
--
-- Data sources:
--   1. Kafka -> Kafka Connect -> RAW_CLICKSTREAM_EVENTS
--   2. OLTP CSV files -> RAW_CUSTOMERS, RAW_PRODUCTS, RAW_ORDERS,
--      RAW_ORDER_ITEMS, RAW_INVENTORY
--
-- Design principles:
--   - Preserve ingested data with minimal transformation.
--   - Do not define primary-key, unique, or foreign-key constraints.
--   - Perform data cleaning, deduplication, and validation in STAGING.
--   - Keep source data types appropriate for the incoming data.
--
-- Database: RETAIL_CLICKSTREAM_DB
-- Schema:   RAW
-- =============================================================================


-- =============================================================================
-- 1. RAW_CLICKSTREAM_EVENTS
-- Source: Kafka topic clickstream_events
-- Ingestion: Kafka Connect -> Snowflake
-- =============================================================================

CREATE TABLE IF NOT EXISTS RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS (
    EVENT_TIME     TIMESTAMP_NTZ,
    EVENT_TYPE     VARCHAR(50),
    PRODUCT_ID     NUMBER(38, 0),
    CATEGORY_ID    NUMBER(38, 0),
    CATEGORY_CODE  VARCHAR(500),
    BRAND          VARCHAR(255),
    PRICE          NUMBER(12, 2),
    USER_ID        NUMBER(38, 0),
    USER_SESSION   VARCHAR(255),
    SIMULATED_AT   TIMESTAMP_NTZ
);


-- =============================================================================
-- 2. RAW_CUSTOMERS
-- Source: OLTP customers data
-- =============================================================================

CREATE TABLE IF NOT EXISTS RETAIL_CLICKSTREAM_DB.RAW.RAW_CUSTOMERS (
    CUSTOMER_ID       NUMBER(38, 0),
    FIRST_NAME        VARCHAR(100),
    LAST_NAME         VARCHAR(100),
    EMAIL             VARCHAR(255),
    COUNTRY           VARCHAR(100),
    REGISTRATION_DATE TIMESTAMP_NTZ
);


-- =============================================================================
-- 3. RAW_PRODUCTS
-- Source: OLTP products data
-- =============================================================================

CREATE TABLE IF NOT EXISTS RETAIL_CLICKSTREAM_DB.RAW.RAW_PRODUCTS (
    PRODUCT_ID     NUMBER(38, 0),
    CATEGORY_ID    NUMBER(38, 0),
    CATEGORY_CODE  VARCHAR(500),
    BRAND          VARCHAR(255),
    PRICE          NUMBER(12, 2),
    COST           NUMBER(12, 2)
);


-- =============================================================================
-- 4. RAW_ORDERS
-- Source: OLTP orders data
-- =============================================================================

CREATE TABLE IF NOT EXISTS RETAIL_CLICKSTREAM_DB.RAW.RAW_ORDERS (
    ORDER_ID        VARCHAR(100),
    CUSTOMER_ID     NUMBER(38, 0),
    ORDER_TIMESTAMP TIMESTAMP_NTZ,
    ORDER_TOTAL     NUMBER(14, 2),
    ORDER_STATUS    VARCHAR(50)
);


-- =============================================================================
-- 5. RAW_ORDER_ITEMS
-- Source: OLTP order_items data
-- =============================================================================

CREATE TABLE IF NOT EXISTS RETAIL_CLICKSTREAM_DB.RAW.RAW_ORDER_ITEMS (
    ORDER_ITEM_ID       NUMBER(38, 0),
    ORDER_ID            VARCHAR(100),
    PRODUCT_ID          NUMBER(38, 0),
    QUANTITY             NUMBER(38, 0),
    UNIT_PRICE           NUMBER(12, 2),
    LINE_TOTAL           NUMBER(14, 2),
    FIRST_PURCHASE_TIME  TIMESTAMP_NTZ
);


-- =============================================================================
-- 6. RAW_INVENTORY
-- Source: OLTP inventory data
-- =============================================================================

CREATE TABLE IF NOT EXISTS RETAIL_CLICKSTREAM_DB.RAW.RAW_INVENTORY (
    PRODUCT_ID        NUMBER(38, 0),
    STOCK_QUANTITY    NUMBER(38, 0),
    REORDER_LEVEL     NUMBER(38, 0),
    INVENTORY_ORIGIN  VARCHAR(100)
);


-- =============================================================================
-- 7. Verify RAW tables
-- =============================================================================

SHOW TABLES IN SCHEMA RETAIL_CLICKSTREAM_DB.RAW;



select count(*) from RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS;RETAIL_CLICKSTREAM_DB.PUBLIC