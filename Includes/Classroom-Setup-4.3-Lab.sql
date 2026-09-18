-- Databricks notebook source
-- MAGIC %run ./Classroom-Setup-Common-SQL-WAREHOUSE

-- COMMAND ----------

-- BRONZE LAYER — Create streaming tables
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE STREAMING TABLE ' || my_catalog || '.bronze.raw_orders
   TBLPROPERTIES (
     \'delta.enableChangeDataFeed\' = \'true\',
     \'delta.enableRowTracking\' = \'true\',
     \'delta.enableDeletionVectors\' = \'true\'
   )
   AS SELECT * FROM STREAM read_files(
     \'/Volumes/' || my_catalog || '/data/course_data/raw_orders\',
     format => \'csv\',
     header => \'true\',
     inferColumnTypes => \'true\',
     schemaEvolutionMode => \'rescue\',
     partitionColumns => \'\'
   )'
  );
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE STREAMING TABLE ' || my_catalog || '.bronze.raw_products
   TBLPROPERTIES (
     \'delta.enableChangeDataFeed\' = \'true\',
     \'delta.enableRowTracking\' = \'true\',
     \'delta.enableDeletionVectors\' = \'true\'
   )
   AS SELECT * FROM STREAM read_files(
     \'/Volumes/' || my_catalog || '/data/course_data/raw_products\',
     format => \'csv\',
     header => \'true\',
     inferColumnTypes => \'true\',
     rescuedDataColumn => \'_rescued_data\',
     schemaEvolutionMode => \'rescue\',
     partitionColumns => \'\'
   )'
  );
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE STREAMING TABLE ' || my_catalog || '.bronze.customer_feedback
   TBLPROPERTIES (
     \'delta.enableChangeDataFeed\' = \'true\',
     \'delta.enableRowTracking\' = \'true\',
     \'delta.enableDeletionVectors\' = \'true\'
   )
   AS SELECT * FROM STREAM read_files(
     \'/Volumes/' || my_catalog || '/data/course_data/customer_feedback\',
     format => \'csv\',
     header => \'true\',
     inferColumnTypes => \'true\',
     rescuedDataColumn => \'_rescued_data\',
     schemaEvolutionMode => \'rescue\',
     partitionColumns => \'\'
   )'
  );

-- COMMAND ----------

-- CDC LAYER — Build customer_changes source and CDC streaming table
BEGIN
  EXECUTE IMMEDIATE
    CONCAT(
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
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REFRESH STREAMING TABLE ', my_catalog, '.demo_cdc.demo_cdc_customers_current ',
    'FLOW AUTO CDC ',
    'FROM STREAM ', my_catalog, '.bronze.customer_changes ',
    'KEYS (customer_id) ',
    'APPLY AS DELETE WHEN operation = \'DELETE\' ',
    'SEQUENCE BY updated_at ',
    'COLUMNS * EXCEPT (operation) ',
    'STORED AS SCD TYPE 1'
  );

-- COMMAND ----------

-- SILVER LAYER — Enriched feedback MV (joins bronze + CDC)
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE MATERIALIZED VIEW ', my_catalog, '.silver.customer_feedback_enriched ',
    'AS SELECT ',
    '  f.feedback_id, f.customer_id, f.order_id, f.feedback_date, f.rating, ',
    '  f.feedback_text, f.channel, ',
    '  COALESCE(c.first_name || \' \' || c.last_name, \'Unknown\') AS customer_name, ',
    '  c.customer_segment, ',
    '  o.order_date, o.order_status, ',
    '  CAST(o.quantity * o.unit_price AS DECIMAL(12,2)) AS order_value, ',
    '  p.product_name, p.category AS product_category, p.brand AS product_brand ',
    'FROM ', my_catalog, '.bronze.customer_feedback f ',
    'LEFT JOIN ', my_catalog, '.bronze.raw_orders o ON f.order_id = o.order_id ',
    'LEFT JOIN ', my_catalog, '.bronze.raw_products p ON o.product_id = p.product_id ',
    'LEFT JOIN ', my_catalog, '.demo_cdc.demo_cdc_customers_current c ON f.customer_id = c.customer_id'
  );

-- COMMAND ----------

-- GOLD LAYER — Aggregation MV + parameterized report table
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE MATERIALIZED VIEW ', my_catalog, '.gold.feedback_by_category ',
    'AS SELECT ',
    '  product_category, ',
    '  ROUND(AVG(rating), 2) AS avg_rating, ',
    '  COUNT(*) AS feedback_count, ',
    '  ROUND(AVG(order_value), 2) AS avg_order_value ',
    'FROM ', my_catalog, '.silver.customer_feedback_enriched ',
    'WHERE product_category IS NOT NULL ',
    'GROUP BY product_category'
  );
DROP TABLE IF EXISTS IDENTIFIER(my_catalog || '.gold.pipeline_report');
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE TABLE ', my_catalog, '.gold.pipeline_report (',
    '  dimension_value STRING,',
    '  dimension_name STRING,',
    '  feedback_count BIGINT,',
    '  avg_rating DECIMAL(12,2),',
    '  avg_order_value DECIMAL(12,2),',
    '  report_generated_at TIMESTAMP',
    ')'
  );

-- COMMAND ----------

SELECT
  'Setup Complete' AS Status,
  my_catalog AS Catalog,
  'bronze, demo_cdc, silver, gold' AS `Schemas`,
  'Ready for Lab 11b — build a Lakeflow Job' AS `Next Step`;
