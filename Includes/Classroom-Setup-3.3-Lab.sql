-- Databricks notebook source
-- MAGIC %run ./Classroom-Setup-Common-SQL-WAREHOUSE

-- COMMAND ----------

-- Build CDC source table for Lab 8b
BEGIN
  -- Build customer_changes: baseline INSERTs from raw_customers + CDC change events
  EXECUTE IMMEDIATE CONCAT(
    'CREATE OR REPLACE TABLE ', my_catalog, '.bronze.customer_changes ',
    'AS ',
    'SELECT customer_id, first_name, last_name, email, phone, address, city, state, ',
    '  zip_code, country, customer_segment, loyalty_tier, ',
    '  \'INSERT\' AS operation, ',
    '  CAST(registration_date AS TIMESTAMP) AS updated_at ',
    'FROM ', my_catalog, '.data.raw_customers ',
    'UNION ALL ',
    'SELECT * FROM ', my_catalog, '.data.raw_customers_cdc'
  );

END;

-- COMMAND ----------

SELECT
  'Setup Complete' AS Status,
  my_catalog AS Catalog,
  'bronze' AS `CDC Source Schema`,
  'silver' AS `Target Schema`;
