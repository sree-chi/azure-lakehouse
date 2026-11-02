-- Snowflake setup for the TRI lakehouse. Run once in a Snowsight worksheet as ACCOUNTADMIN.
-- Before running: generate the key pair with scripts/New-SnowflakeKeyPair.ps1 and paste
-- the public key body into RSA_PUBLIC_KEY below.

USE ROLE ACCOUNTADMIN;

-- Smallest warehouse, suspends after 60 s idle.
CREATE WAREHOUSE IF NOT EXISTS TRI_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Hard stop on spend: suspend the warehouse after 5 credits in a month.
CREATE RESOURCE MONITOR IF NOT EXISTS TRI_MONITOR
  WITH CREDIT_QUOTA = 5
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE TRI_WH SET RESOURCE_MONITOR = TRI_MONITOR;

CREATE DATABASE IF NOT EXISTS TRI_DB;
CREATE SCHEMA IF NOT EXISTS TRI_DB.GOLD;

-- Least-privilege loader role: can only write tables in TRI_DB.GOLD.
CREATE ROLE IF NOT EXISTS TRI_LOADER;
GRANT USAGE ON WAREHOUSE TRI_WH TO ROLE TRI_LOADER;
GRANT USAGE ON DATABASE TRI_DB TO ROLE TRI_LOADER;
GRANT USAGE, CREATE TABLE, CREATE STAGE ON SCHEMA TRI_DB.GOLD TO ROLE TRI_LOADER;
GRANT ROLE TRI_LOADER TO ROLE SYSADMIN;

-- Service user for Databricks: key-pair auth only (TYPE = SERVICE cannot use passwords).
CREATE USER IF NOT EXISTS TRI_DATABRICKS_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = TRI_LOADER
  DEFAULT_WAREHOUSE = TRI_WH
  RSA_PUBLIC_KEY = 'PASTE_PUBLIC_KEY_BODY_HERE'
  COMMENT = 'Databricks job writer for TRI gold tables';
GRANT ROLE TRI_LOADER TO USER TRI_DATABRICKS_SVC;

-- Your account URL for -SnowflakeAccountUrl (use the host part, without https://):
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() || '.snowflakecomputing.com' AS account_url;
