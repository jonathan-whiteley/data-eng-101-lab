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

-- BRONZE LAYER — Create streaming tables (raw_products, customer_feedback)
-- bronze.raw_orders is created in the demo itself
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

ALTER TABLE data.raw_orders SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false',
  'delta.enableRowTracking' = 'false',
  'delta.enableDeletionVectors' = 'false'
);

ALTER TABLE data.raw_customers SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false',
  'delta.enableRowTracking' = 'false',
  'delta.enableDeletionVectors' = 'false'
);

ALTER TABLE data.raw_products SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false',
  'delta.enableRowTracking' = 'false',
  'delta.enableDeletionVectors' = 'false'
);

-- COMMAND ----------

USE SCHEMA bronze;

SELECT
  'Setup Complete' AS Status,
  my_catalog AS Catalog,
  'bronze' AS `Working Schema`,
  'data' AS `Data Schema`;
