-- Databricks notebook source
-- MAGIC %run ./Classroom-Setup-Common-SQL-WAREHOUSE

-- COMMAND ----------

-- Reset Volume to clean state and recreate source data files
BEGIN
  EXECUTE IMMEDIATE CONCAT('DROP VOLUME IF EXISTS ', my_catalog, '.data.course_data');
  EXECUTE IMMEDIATE CONCAT('CREATE VOLUME IF NOT EXISTS ', my_catalog, '.data.course_data');

  -- raw_orders CSV files (for Bronze ST ingestion)
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/raw_orders', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.raw_orders'
  );

  -- raw_products CSV files (for Bronze ST ingestion)
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/raw_products', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.raw_products'
  );

  -- raw_customers CSV (used by other demos/labs)
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/raw_customers', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.raw_customers'
  );

  -- customer_feedback CSV (used by other demos/labs)
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/customer_feedback', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.customer_feedback'
  );

  -- raw_customers_cdc initial load (for this demo's CDC pipeline)
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/raw_customers_cdc/initial', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT customer_id, first_name, last_name, email, phone, address, city, state, ',
    '  zip_code, country, customer_segment, loyalty_tier, ',
    CHAR(39), 'INSERT', CHAR(39), ' AS operation, ',
    '  CAST(registration_date AS TIMESTAMP) AS updated_at ',
    ' FROM ', my_catalog, '.data.raw_customers ',
    ' UNION ALL ',
    ' SELECT * FROM ', my_catalog, '.data.raw_customers_cdc'
  );
END;

-- COMMAND ----------

-- Drop any previously created pipeline tables (from prior demo runs)
-- These use the prefixed naming convention: bronze_raw_orders, silver_orders_enriched, etc.
BEGIN
  EXECUTE IMMEDIATE CONCAT('DROP TABLE IF EXISTS ', my_catalog, '.bronze.bronze_raw_orders');
  EXECUTE IMMEDIATE CONCAT('DROP TABLE IF EXISTS ', my_catalog, '.bronze.bronze_raw_products');
  EXECUTE IMMEDIATE CONCAT('DROP TABLE IF EXISTS ', my_catalog, '.silver.silver_dim_customers');
  EXECUTE IMMEDIATE CONCAT('DROP MATERIALIZED VIEW IF EXISTS ', my_catalog, '.silver.silver_orders_enriched');
  EXECUTE IMMEDIATE CONCAT('DROP MATERIALIZED VIEW IF EXISTS ', my_catalog, '.default.staging_orders_deduped');
  EXECUTE IMMEDIATE CONCAT('DROP MATERIALIZED VIEW IF EXISTS ', my_catalog, '.default.staging_orders_cleaned');
  EXECUTE IMMEDIATE CONCAT('DROP MATERIALIZED VIEW IF EXISTS ', my_catalog, '.gold.gold_revenue_by_category');
  EXECUTE IMMEDIATE CONCAT('DROP MATERIALIZED VIEW IF EXISTS ', my_catalog, '.gold.gold_daily_sales_summary');
END;

-- COMMAND ----------

-- Create schemas for the SDP pipeline output (if not already present)
EXECUTE IMMEDIATE CONCAT('CREATE SCHEMA IF NOT EXISTS ', my_catalog, '.bronze');
EXECUTE IMMEDIATE CONCAT('CREATE SCHEMA IF NOT EXISTS ', my_catalog, '.silver');
EXECUTE IMMEDIATE CONCAT('CREATE SCHEMA IF NOT EXISTS ', my_catalog, '.gold');

-- COMMAND ----------

SELECT
  'Setup Complete — Volume data ready for SDP pipeline' AS Status,
  my_catalog AS Catalog,
  'bronze, silver, gold schemas created' AS Schemas;
