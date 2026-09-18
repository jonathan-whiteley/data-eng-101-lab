-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](./Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1 - REQUIRED — Course Data Setup
-- MAGIC
-- MAGIC ## Run This Notebook Once Before Starting Any Demos or Labs
-- MAGIC
-- MAGIC This notebook creates the course dataset — tables and CSV files in your personal catalog that all demos and labs depend on. It only needs to be run **once** at the beginning of the course. Subsequent demos and labs will verify the data exists and warn you if it does not.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border: 3px solid #dc2626; background: linear-gradient(135deg, #fef2f2, #fee2e2); padding: 20px 24px; border-radius: 8px; margin: 16px 0; box-shadow: 0 2px 8px rgba(220,38,38,0.15);">
-- MAGIC   <div style="display:flex; align-items:center; gap:12px; margin-bottom:10px;">
-- MAGIC     <img src="./Includes/images/icons/warning-triangle.png" height="36" style="vertical-align:middle;">
-- MAGIC     <strong style="color:#991b1b; font-size:1.4em;">REQUIRED — Use the Shared SQL Warehouse</strong>
-- MAGIC   </div>
-- MAGIC   <div style="color:#333; font-size:1.05em; line-height:1.6;">
-- MAGIC     Before running any cells in this notebook, you <strong>must</strong> attach to the <strong>shared SQL Warehouse</strong> provided for this course.<br/><br/>
-- MAGIC     <strong>How to connect:</strong> Click the <strong>Connect</strong> drop-down in the top-right corner of this notebook and select the <strong>shared SQL Warehouse</strong> (not a personal cluster or serverless compute).<br/><br/>
-- MAGIC     <span style="color:#991b1b; font-weight:bold;">Do NOT use All Purpose Compute or personal clusters — they will cause errors and are not supported for this course.</span>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create the Course Dataset
-- MAGIC
-- MAGIC This creates 7 tables in your catalog's `data` schema and exports them as CSV files to a Unity Catalog Volume. This may take 2-3 minutes.
-- MAGIC
-- MAGIC | Table | Rows | Purpose |
-- MAGIC |---|---|---|
-- MAGIC | `raw_customers` | 2,500 | Customer dimension |
-- MAGIC | `raw_products` | 200 | Product dimension |
-- MAGIC | `raw_orders` | 15,000 | Order fact table |
-- MAGIC | `raw_orders_new_batch` | 2,000 | New orders for incremental demos |
-- MAGIC | `raw_customers_cdc` | 1,500 | CDC events for SCD demos |
-- MAGIC | `raw_customers_cdc_new_batch` | 200 | New CDC events for incremental demos |
-- MAGIC | `customer_feedback` | 500 | Customer feedback dimension |

-- COMMAND ----------

-- MAGIC %run ./Includes/data/dataset_etl_pipelines_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verification
-- MAGIC
-- MAGIC The query below confirms all tables were created with the expected row counts.

-- COMMAND ----------

SELECT 'raw_customers' AS table_name, COUNT(*) AS rows FROM IDENTIFIER(my_catalog || '.data.raw_customers')
  UNION ALL
SELECT 'raw_products', COUNT(*) FROM IDENTIFIER(my_catalog || '.data.raw_products')
  UNION ALL
SELECT 'raw_orders', COUNT(*) FROM IDENTIFIER(my_catalog || '.data.raw_orders')
  UNION ALL
SELECT 'raw_orders_new_batch', COUNT(*) FROM IDENTIFIER(my_catalog || '.data.raw_orders_new_batch')
  UNION ALL
SELECT 'raw_customers_cdc', COUNT(*) FROM IDENTIFIER(my_catalog || '.data.raw_customers_cdc')
  UNION ALL
SELECT 'raw_customers_cdc_new_batch', COUNT(*) FROM IDENTIFIER(my_catalog || '.data.raw_customers_cdc_new_batch')
  UNION ALL
SELECT 'customer_feedback', COUNT(*) FROM IDENTIFIER(my_catalog || '.data.customer_feedback')
  ORDER BY table_name;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">Setup Complete</strong>
-- MAGIC   <div style="color:#333;">Your course data is ready. You can now proceed to the demos and labs. You do not need to run this notebook again unless you reset your catalog.</div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC &copy; <span id="dbx-year">2026</span> Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> |
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> |
-- MAGIC <a href="https://help.databricks.com/">Support</a>
-- MAGIC
-- MAGIC <script>document.getElementById("dbx-year").textContent = new Date().getFullYear();</script>
