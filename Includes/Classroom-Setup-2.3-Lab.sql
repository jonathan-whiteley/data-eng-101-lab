-- Databricks notebook source
-- MAGIC %run ./Classroom-Setup-Common-SQL-WAREHOUSE

-- COMMAND ----------

-- Enable row tracking and prepare environment for Lab 5b
BEGIN
  -- Enable row tracking on data tables so EXPLAIN doesn't flag it
  -- (Demo 5 already covers the ROW_TRACKING_NOT_ENABLED error)
  EXECUTE IMMEDIATE CONCAT('ALTER TABLE ' || my_catalog || '.data.customer_feedback SET TBLPROPERTIES (\'delta.enableRowTracking\' = \'true\')');
  EXECUTE IMMEDIATE CONCAT('ALTER TABLE ' || my_catalog || '.data.raw_orders SET TBLPROPERTIES (\'delta.enableRowTracking\' = \'true\')');
  EXECUTE IMMEDIATE CONCAT('ALTER TABLE ' || my_catalog || '.data.raw_products SET TBLPROPERTIES (\'delta.enableRowTracking\' = \'true\')');
  EXECUTE IMMEDIATE CONCAT('ALTER TABLE ' || my_catalog || '.data.raw_customers SET TBLPROPERTIES (\'delta.enableRowTracking\' = \'true\')');

END;

-- COMMAND ----------

SELECT
  'Setup Complete' AS Status,
  my_catalog AS Catalog,
  'bronze' AS `Bronze Schema`,
  'silver' AS `Silver Schema`,
  'gold' AS `Gold Schema`;
