-- =============================================================================
-- File: 07_kafka_snowflake_setup.sql
-- Project: Real-Time Clickstream Ingestion & Omnichannel Data Warehouse
--
-- Purpose:
-- Configure Snowflake to receive clickstream events from Apache Kafka through
-- Kafka Connect and the Snowflake Kafka Connector using RSA key-pair
-- authentication.
--
-- This script:
--   1. Creates a dedicated role for Kafka Connect.
--   2. Grants database, schema, and warehouse access.
--   3. Creates a service user for the connector.
--   4. Configures RSA public-key authentication.
--   5. Grants permission to insert records into the RAW clickstream table.
--   6. Provides verification queries for the configuration.
--
-- Prerequisites:
--   - RETAIL_CLICKSTREAM_DB and its RAW schema already exist.
--   - RAW_CLICKSTREAM_EVENTS has already been created.
--   - Run administrative statements with a role authorized to grant privileges.
--   - Register the public key corresponding to the connector's private key.
--
-- Security:
--   Never store the RSA private key or other credentials in this script or
--   commit them to GitHub. Only the public key belongs in Snowflake.
-- =============================================================================


-- =============================================================================
-- 1. Create a dedicated role for Kafka Connect
-- =============================================================================

CREATE ROLE IF NOT EXISTS RETAIL_KAFKA_CONNECT_ROLE;

-- Allow the role to access the project database.
GRANT USAGE
ON DATABASE RETAIL_CLICKSTREAM_DB
TO ROLE RETAIL_KAFKA_CONNECT_ROLE;

-- Allow the role to access the RAW schema.
GRANT USAGE
ON SCHEMA RETAIL_CLICKSTREAM_DB.RAW
TO ROLE RETAIL_KAFKA_CONNECT_ROLE;

-- Allow the connector to create tables or pipes in the RAW schema if needed.
GRANT CREATE TABLE, CREATE PIPE
ON SCHEMA RETAIL_CLICKSTREAM_DB.RAW
TO ROLE RETAIL_KAFKA_CONNECT_ROLE;

-- Allow the connector to use the warehouse for Snowflake operations.
GRANT USAGE
ON WAREHOUSE RETAIL_WH
TO ROLE RETAIL_KAFKA_CONNECT_ROLE;


-- =============================================================================
-- 2. Create the Kafka Connect service user
-- =============================================================================

CREATE USER IF NOT EXISTS RETAIL_KAFKA_CONNECT_USER
    TYPE = SERVICE
    DEFAULT_ROLE = RETAIL_KAFKA_CONNECT_ROLE
    DEFAULT_WAREHOUSE = RETAIL_WH
    DEFAULT_NAMESPACE = RETAIL_CLICKSTREAM_DB.RAW
    COMMENT = 'Service user for Kafka Connect streaming into Snowflake';

-- Assign the dedicated role to the service user.
GRANT ROLE RETAIL_KAFKA_CONNECT_ROLE
TO USER RETAIL_KAFKA_CONNECT_USER;


-- =============================================================================
-- 3. Configure RSA public-key authentication
-- =============================================================================

-- Register the public key corresponding to the private key configured in
-- Kafka Connect. Replace the placeholder below with your actual public key.
--
-- IMPORTANT:
-- Keep the private key outside the repository. Do not paste it into this file.

-- ALTER USER RETAIL_KAFKA_CONNECT_USER
-- SET RSA_PUBLIC_KEY = '<YOUR_RSA_PUBLIC_KEY>';


-- =============================================================================
-- 4. Grant access to the target clickstream table
-- =============================================================================


-- Allow the connector to insert incoming clickstream events into the RAW table.
GRANT INSERT
ON TABLE RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS
TO ROLE RETAIL_KAFKA_CONNECT_ROLE;


-- =============================================================================
-- 5. Verify the role, user, and authentication configuration
-- =============================================================================

-- Check privileges assigned to the Kafka Connect role.
SHOW GRANTS TO ROLE RETAIL_KAFKA_CONNECT_ROLE;

-- Verify that the service user exists.
SHOW USERS LIKE 'RETAIL_KAFKA_CONNECT_USER';

-- Check roles granted to the service user.
SHOW GRANTS TO USER RETAIL_KAFKA_CONNECT_USER;

-- Inspect the user's RSA public-key fingerprint.
DESC USER RETAIL_KAFKA_CONNECT_USER;

SELECT
    "property",
    "value"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "property" = 'RSA_PUBLIC_KEY_FP';


-- =============================================================================
-- 6. Verify the Snowflake account and region
-- =============================================================================

SELECT
    CURRENT_ORGANIZATION_NAME() AS ORGANIZATION_NAME,
    CURRENT_ACCOUNT_NAME()      AS ACCOUNT_NAME,
    CURRENT_REGION()            AS REGION;
