-- Databricks notebook source
-- MAGIC %run ./Classroom-Setup-Common-SQL-WAREHOUSE

-- COMMAND ----------

-- Reset Volume to clean state (removes new batch files from previous demo runs)
BEGIN
  EXECUTE IMMEDIATE CONCAT('DROP VOLUME IF EXISTS ', my_catalog, '.data.course_data');
  EXECUTE IMMEDIATE CONCAT('CREATE VOLUME IF NOT EXISTS ', my_catalog, '.data.course_data');

  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/raw_customers', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.raw_customers'
  );
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/raw_products', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.raw_products'
  );
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/raw_orders', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.raw_orders'
  );
  EXECUTE IMMEDIATE CONCAT(
    'INSERT OVERWRITE DIRECTORY ', CHAR(39), '/Volumes/', my_catalog, '/data/course_data/customer_feedback', CHAR(39),
    ' USING CSV OPTIONS (', CHAR(39), 'header', CHAR(39), ' ', CHAR(39), 'true', CHAR(39), ')',
    ' SELECT * FROM ', my_catalog, '.data.customer_feedback'
  );
END;

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

-- COMMAND ----------

-- CDC LAYER — customer_changes source + CDC streaming tables
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
    'APPLY AS DELETE WHEN operation = \'DELETE\'
     SEQUENCE BY updated_at
     COLUMNS * EXCEPT (operation)
     STORED AS SCD TYPE 1'
  );
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REFRESH STREAMING TABLE ', my_catalog, '.demo_cdc.demo_cdc_customers_history ',
    'FLOW AUTO CDC ',
    'FROM STREAM ', my_catalog, '.bronze.customer_changes ',
    'KEYS (customer_id) ',
    'APPLY AS DELETE WHEN operation = \'DELETE\'
     SEQUENCE BY updated_at
     COLUMNS * EXCEPT (operation)
     STORED AS SCD TYPE 2'
  );

-- COMMAND ----------

-- SILVER LAYER — Enriched orders MV (joins bronze + CDC dimension)
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE MATERIALIZED VIEW ', my_catalog, '.silver.orders_enriched ',
    'AS SELECT ',
    '  o.order_id, ',
    '  o.customer_id, ',
    '  o.product_id, ',
    '  CAST(o.order_date AS DATE) AS order_date, ',
    '  CAST(o.quantity AS INT) AS quantity, ',
    '  CAST(o.unit_price AS DECIMAL(10,2)) AS unit_price, ',
    '  CAST(o.quantity * o.unit_price AS DECIMAL(12,2)) AS line_total, ',
    '  COALESCE(o.discount_pct, 0) AS discount_pct, ',
    '  COALESCE(o.shipping_cost, 0.00) AS shipping_cost, ',
    '  o.order_status, ',
    '  o.payment_method, ',
    '  COALESCE(c.first_name || \' \' || c.last_name, \'Unknown\') AS customer_name, ',
    '  c.customer_segment, ',
    '  c.city AS customer_city, ',
    '  c.state AS customer_state, ',
    '  COALESCE(p.product_name, \'Unknown Product\') AS product_name, ',
    '  p.category AS product_category, ',
    '  p.brand AS product_brand, ',
    '  CAST(p.list_price AS DECIMAL(10,2)) AS product_list_price ',
    'FROM ', my_catalog, '.bronze.raw_orders o ',
    'LEFT JOIN ', my_catalog, '.demo_cdc.demo_cdc_customers_current c ',
    '  ON o.customer_id = c.customer_id ',
    'LEFT JOIN ', my_catalog, '.bronze.raw_products p ',
    '  ON o.product_id = p.product_id'
  );

-- COMMAND ----------

-- GOLD LAYER — business-level aggregation views
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE MATERIALIZED VIEW ', my_catalog, '.gold.revenue_by_category ',
    'AS SELECT ',
    '  product_category, ',
    '  COUNT(*) AS total_orders, ',
    '  SUM(line_total) AS total_revenue, ',
    '  ROUND(AVG(line_total), 2) AS avg_order_value ',
    'FROM ', my_catalog, '.silver.orders_enriched ',
    'WHERE order_status != \'cancelled\'
     GROUP BY product_category'
  );
EXECUTE IMMEDIATE
  CONCAT(
    'CREATE OR REPLACE MATERIALIZED VIEW ', my_catalog, '.gold.daily_sales_summary ',
    'AS SELECT ',
    '  order_date, ',
    '  COUNT(*) AS total_orders, ',
    '  COUNT(DISTINCT customer_id) AS unique_customers, ',
    '  SUM(line_total) AS total_revenue, ',
    '  ROUND(AVG(line_total), 2) AS avg_order_value ',
    'FROM ', my_catalog, '.silver.orders_enriched ',
    'WHERE order_status != \'cancelled\'
     GROUP BY order_date'
  );

-- COMMAND ----------

-- REFRESH all objects in dependency order
REFRESH STREAMING TABLE IDENTIFIER(my_catalog || '.bronze.raw_orders');
REFRESH STREAMING TABLE IDENTIFIER(my_catalog || '.bronze.raw_products');
REFRESH STREAMING TABLE IDENTIFIER(my_catalog || '.demo_cdc.demo_cdc_customers_current');
REFRESH STREAMING TABLE IDENTIFIER(my_catalog || '.demo_cdc.demo_cdc_customers_history');
REFRESH MATERIALIZED VIEW IDENTIFIER(my_catalog || '.silver.orders_enriched');
REFRESH MATERIALIZED VIEW IDENTIFIER(my_catalog || '.gold.revenue_by_category');
REFRESH MATERIALIZED VIEW IDENTIFIER(my_catalog || '.gold.daily_sales_summary');

-- COMMAND ----------

SELECT
  'Setup Complete' AS Status,
  my_catalog AS Catalog,
  'bronze, silver, gold, demo_cdc' AS `Schemas`;
