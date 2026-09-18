-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](../Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demo 2.2b — Building a Bronze-to-Silver Pipeline
-- MAGIC ## Overview
-- MAGIC In this demo, we build a two-layer incremental pipeline: a **Bronze Streaming Table** that ingests raw CSV files via Auto Loader, and a **Silver Materialized View** that joins and enriches the data. We then configure **REFRESH POLICY** and **TRIGGER ON UPDATE** so that refreshing the Streaming Table automatically cascades to the Materialized View — no manual MV refresh needed.
-- MAGIC
-- MAGIC Most pipeline objects are created in the **SQL Editor** (the natural tool for DDL). This notebook serves as the guide — it provides the SQL to copy, verification queries, and teaching callouts.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC - Use **EXPLAIN CREATE MATERIALIZED VIEW** to verify incrementalizability before building
-- MAGIC - Create a **Streaming Table** and **Materialized View** using the SQL Editor
-- MAGIC - Configure **REFRESH POLICY INCREMENTAL** on a Materialized View
-- MAGIC - Set **TRIGGER ON UPDATE** to automatically cascade refreshes from ST to MV
-- MAGIC - Demonstrate **incremental processing** triggered by new data arriving in the pipeline

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #f44336;
-- MAGIC   background: #ffebee;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select SQL Warehouse</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC Before starting this notebook, select the required compute environment listed below.
-- MAGIC
-- MAGIC - **SQL Warehouse**
-- MAGIC
-- MAGIC **NOTE:** This notebook was **developed and tested using a Serverless SQL Warehouse**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Classroom Setup

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.2-Demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## B. The Pipeline We Are Building
-- MAGIC
-- MAGIC This demo builds the **Bronze → Silver** segment of the course pipeline. Gold aggregations, AUTO CDC, and orchestration are covered in later demos.
-- MAGIC
-- MAGIC
-- MAGIC <span style="color:#991b1b; font-weight:bold; font-style:italic;">If the diagram below does not render, click into the cell and then click out — this triggers the Mermaid library to load.</span>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="max-width: 900px; margin: 0 auto;">
-- MAGIC <div>
-- MAGIC <div class="mermaid">
-- MAGIC flowchart TD
-- MAGIC     subgraph BRONZE["<b>BRONZE — Streaming Tables</b>"]
-- MAGIC         RO["raw_orders<br/><small>ST · Auto Loader · CSV from Volumes</small>"]
-- MAGIC         RP["raw_products<br/><small>ST · Auto Loader · CSV from Volumes</small>"]
-- MAGIC     end
-- MAGIC     RC["data.raw_customers<br/><small>Delta table · joined directly</small>"]
-- MAGIC     subgraph SILVER["<b>SILVER — Materialized View</b>"]
-- MAGIC         OE["orders_enriched<br/><small>MV · joins orders + products + customers</small>"]
-- MAGIC     end
-- MAGIC     subgraph LATER["<b>Built in Later Demos</b>"]
-- MAGIC         direction LR
-- MAGIC         CDC["AUTO CDC (Module 3)"]
-- MAGIC         GOLD["Gold MVs (Module 4)"]
-- MAGIC     end
-- MAGIC     RO -->|"TRIGGER ON UPDATE"| OE
-- MAGIC     RP --> OE
-- MAGIC     RC --> OE
-- MAGIC     OE -.-> GOLD
-- MAGIC     classDef bronze fill:#FFF3E0,stroke:#e65100,stroke-width:2px,color:#333
-- MAGIC     classDef silver fill:#E3F2FD,stroke:#1565c0,stroke-width:2px,color:#333
-- MAGIC     classDef source fill:#F3E5F5,stroke:#7b1fa2,stroke-width:2px,color:#333
-- MAGIC     classDef later fill:#F5F5F5,stroke:#9e9e9e,stroke-width:1px,stroke-dasharray:5 5,color:#999
-- MAGIC     class RO,RP bronze
-- MAGIC     class OE silver
-- MAGIC     class RC source
-- MAGIC     class CDC,GOLD later
-- MAGIC     style BRONZE fill:#FFF8E1,stroke:#e65100,stroke-width:2px
-- MAGIC     style SILVER fill:#E8EAF6,stroke:#1565c0,stroke-width:2px
-- MAGIC     style LATER fill:#FAFAFA,stroke:#bdbdbd,stroke-width:1px,stroke-dasharray:5 5
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <script type="module">
-- MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
-- MAGIC mermaid.initialize({
-- MAGIC   startOnLoad: true,
-- MAGIC   theme: "base",
-- MAGIC   themeVariables: {
-- MAGIC     background: "#FFFFFF",
-- MAGIC     lineColor: "#1B5162"
-- MAGIC   }
-- MAGIC });
-- MAGIC </script>
-- MAGIC
-- MAGIC **What we build in this demo:**
-- MAGIC - Two **Streaming Tables** (`bronze.raw_orders` and `bronze.raw_products`) that ingest raw CSV files incrementally via Auto Loader
-- MAGIC - A **Materialized View** (`silver.orders_enriched`) that joins orders with products and customers
-- MAGIC - **REFRESH POLICY INCREMENTAL** to enforce incremental refresh on the MV
-- MAGIC - **TRIGGER ON UPDATE** so refreshing the ST automatically cascades to the MV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. EXPLAIN Before You Build
-- MAGIC
-- MAGIC Before we create any pipeline objects, let's check whether the query we plan for the MV **can** be refreshed incrementally. `EXPLAIN CREATE MATERIALIZED VIEW` analyzes the query plan and reports whether incremental refresh is possible.
-- MAGIC
-- MAGIC We use the `data` schema tables (regular Delta tables pre-loaded by setup) to test our planned query. This lets us see what properties are needed and fix them before building the pipeline.
-- MAGIC
-- MAGIC **Note on schemas:** The `data` schema contains the original Delta tables loaded during setup. The `bronze` schema will hold the Streaming Tables we create later. EXPLAIN runs against `data` (to test the query before we build), while the actual MV will source from `bronze`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Run EXPLAIN — BEFORE Setting Properties
-- MAGIC
-- MAGIC Let's run `EXPLAIN CREATE MATERIALIZED VIEW` against the query we plan to use. EXPLAIN requires a view name in the syntax — we use a placeholder name since it does not actually create anything.
-- MAGIC
-- MAGIC **Note:** This is a **Beta** feature available in current Serverless SQL Warehouses.

-- COMMAND ----------

EXPLAIN CREATE MATERIALIZED VIEW IDENTIFIER(my_catalog || '.data._explain_check')
AS
SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  CAST(o.order_date AS DATE)                          AS order_date,
  CAST(o.quantity AS INT)                              AS quantity,
  CAST(o.unit_price AS DECIMAL(10,2))                  AS unit_price,
  CAST(o.quantity * o.unit_price AS DECIMAL(12,2))     AS line_total,
  COALESCE(o.discount_pct, 0)                          AS discount_pct,
  COALESCE(o.shipping_cost, 0.00)                      AS shipping_cost,
  o.order_status,
  o.payment_method,
  COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS customer_name,
  c.customer_segment,
  c.city                                                AS customer_city,
  c.state                                               AS customer_state,
  COALESCE(p.product_name, 'Unknown Product')           AS product_name,
  p.category                                            AS product_category,
  p.brand                                               AS product_brand,
  CAST(p.list_price AS DECIMAL(10,2))                   AS product_list_price
FROM IDENTIFIER(my_catalog || '.data.raw_orders') o
LEFT JOIN IDENTIFIER(my_catalog || '.data.raw_customers') c
  ON o.customer_id = c.customer_id
LEFT JOIN IDENTIFIER(my_catalog || '.data.raw_products') p
  ON o.product_id = p.product_id;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>The EXPLAIN output shows the view is <strong>not incrementalizable</strong></li>
-- MAGIC       <li>It reports error conditions such as <code>ROW_TRACKING_NOT_ENABLED</code>, <code>CDF_NOT_ENABLED</code>, and <code>DELETION_VECTORS_NOT_ENABLED</code>, listing the specific tables that need changes</li>
-- MAGIC       <li>It tells you exactly what to fix — no guesswork needed</li>
-- MAGIC     </ul>
-- MAGIC     <p style="margin-top:10px;"><strong>Key teaching point:</strong> Always run EXPLAIN before deploying a production MV. It is your pre-deployment diagnostic tool — it tells you exactly what needs to change to enable incremental refresh, saving you from full recomputation on every refresh.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Set Source Table Properties for Incremental Refresh
-- MAGIC
-- MAGIC The EXPLAIN output told us that the source tables need specific Delta features enabled:
-- MAGIC - **Change Data Feed**: tracks row-level changes
-- MAGIC - **Row Tracking**: enables precise change identification
-- MAGIC - **Deletion Vectors**: enables efficient deletes
-- MAGIC
-- MAGIC For **regular Delta tables** like these, we use `ALTER TABLE ... SET TBLPROPERTIES`. (For **Streaming Tables**, these properties must be set at creation time — they cannot be altered after the fact.)

-- COMMAND ----------

ALTER TABLE IDENTIFIER(my_catalog || '.data.raw_orders')
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

ALTER TABLE IDENTIFIER(my_catalog || '.data.raw_customers')
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

ALTER TABLE IDENTIFIER(my_catalog || '.data.raw_products')
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Each ALTER TABLE should complete successfully. These properties enable Delta Lake to track changes at the row level, which is what allows Materialized Views to refresh incrementally rather than recomputing from scratch.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Re-Run EXPLAIN — AFTER Setting Properties
-- MAGIC
-- MAGIC Now that the source table properties are set, let's run EXPLAIN again. This time, the output should confirm that the view **is incrementalizable**.

-- COMMAND ----------

EXPLAIN CREATE MATERIALIZED VIEW IDENTIFIER(my_catalog || '.data._explain_check')
AS
SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  CAST(o.order_date AS DATE)                          AS order_date,
  CAST(o.quantity AS INT)                              AS quantity,
  CAST(o.unit_price AS DECIMAL(10,2))                  AS unit_price,
  CAST(o.quantity * o.unit_price AS DECIMAL(12,2))     AS line_total,
  COALESCE(o.discount_pct, 0)                          AS discount_pct,
  COALESCE(o.shipping_cost, 0.00)                      AS shipping_cost,
  o.order_status,
  o.payment_method,
  COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS customer_name,
  c.customer_segment,
  c.city                                                AS customer_city,
  c.state                                               AS customer_state,
  COALESCE(p.product_name, 'Unknown Product')           AS product_name,
  p.category                                            AS product_category,
  p.brand                                               AS product_brand,
  CAST(p.list_price AS DECIMAL(10,2))                   AS product_list_price
FROM IDENTIFIER(my_catalog || '.data.raw_orders') o
LEFT JOIN IDENTIFIER(my_catalog || '.data.raw_customers') c
  ON o.customer_id = c.customer_id
LEFT JOIN IDENTIFIER(my_catalog || '.data.raw_products') p
  ON o.product_id = p.product_id;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>The EXPLAIN output should now report that the view <strong>is incrementalizable</strong></li>
-- MAGIC       <li>Compare this to the previous EXPLAIN output — the difference is entirely due to the table properties we just set</li>
-- MAGIC     </ul>
-- MAGIC     <p style="margin-top:10px;"><strong>Key teaching point:</strong> This is the "before and after" moment. The same query, the same tables — but now the engine knows it can process only changed rows instead of recomputing everything. We are confident our pipeline will support incremental refresh, so let's build it.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Create the Bronze Streaming Table
-- MAGIC
-- MAGIC Our source data lives as CSV files in a Volume directory (exported by the setup script from the `data.raw_orders` Delta table). We will create a **Streaming Table** that ingests these files incrementally using `read_files()` with Auto Loader.
-- MAGIC
-- MAGIC Streaming Tables are ideal for append-only ingestion patterns — they track which files have been processed and only pick up new ones on each refresh.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Preview the Source Data
-- MAGIC
-- MAGIC Before building the Streaming Table, let's preview the raw orders data to understand its structure.

-- COMMAND ----------

SELECT *
FROM IDENTIFIER(my_catalog || '.data.raw_orders')
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>The <code>raw_orders</code> table contains order-level transaction data with fields like <code>order_id</code>, <code>customer_id</code>, <code>product_id</code>, <code>order_date</code>, <code>quantity</code>, <code>unit_price</code>, and <code>order_status</code></li>
-- MAGIC       <li>Some fields are stored as strings that we will later cast to appropriate types in the Silver layer</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Your catalog name** — you will need this when creating objects in the SQL Editor:

-- COMMAND ----------

SELECT my_catalog AS your_catalog_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Create the Streaming Table in the SQL Editor
-- MAGIC
-- MAGIC We create the Streaming Table in the **SQL Editor** because it provides a clean environment for DDL — no `IDENTIFIER()` wrappers or `EXECUTE IMMEDIATE` needed.
-- MAGIC
-- MAGIC Notice the `TBLPROPERTIES` — we enable Change Data Feed, Row Tracking, and Deletion Vectors at creation time because Streaming Table properties **cannot be altered after the fact**.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fffbeb; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Copy to SQL Editor</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Open the <strong>SQL Editor</strong> (click <strong>SQL Editor</strong> in the left sidebar)</li>
-- MAGIC       <li><span style="background:#FEF3C7; padding:2px 6px; border-radius:3px;"><strong>Set the default catalog</strong> to your catalog (e.g., <code>labuser_XXXX</code>) using the catalog picker at the top</span></li>
-- MAGIC       <li>Copy the SQL below and run it:</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC <button onclick="copySTOrders()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-st-orders" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>DROP TABLE IF EXISTS bronze.raw_orders;
-- MAGIC
-- MAGIC CREATE OR REPLACE STREAMING TABLE bronze.raw_orders
-- MAGIC TBLPROPERTIES (
-- MAGIC   'delta.enableChangeDataFeed' = 'true',
-- MAGIC   'delta.enableRowTracking' = 'true',
-- MAGIC   'delta.enableDeletionVectors' = 'true'
-- MAGIC )
-- MAGIC AS SELECT * FROM STREAM read_files(
-- MAGIC   '/Volumes/<mark style="background:#FEF3C7;color:#000;padding:1px 3px;">ADD_YOUR_CATALOG_HERE</mark>/data/course_data/raw_orders',
-- MAGIC   format =&gt; 'csv',
-- MAGIC   header =&gt; 'true',
-- MAGIC   inferColumnTypes =&gt; 'true',
-- MAGIC   schemaEvolutionMode =&gt; 'rescue'
-- MAGIC );
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copySTOrders() {
-- MAGIC   var el = document.getElementById("copy-block-st-orders");
-- MAGIC   if (!el) return;
-- MAGIC   var text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text).then(function() { alert("Copied to clipboard"); }).catch(function() { fallbackCopyST(text); });
-- MAGIC   } else { fallbackCopyST(text); }
-- MAGIC }
-- MAGIC function fallbackCopyST(text) {
-- MAGIC   var textarea = document.createElement("textarea"); textarea.value = text; textarea.style.position = "fixed"; textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea); textarea.select();
-- MAGIC   try { document.execCommand("copy"); alert("Copied to clipboard"); } catch(e) { alert("Copy failed — select and copy manually."); }
-- MAGIC   finally { document.body.removeChild(textarea); }
-- MAGIC }
-- MAGIC </script>
-- MAGIC <p style="margin-top:8px; color:#555;"><strong>Replace <code><mark style="background:#FEF3C7;color:#000;padding:1px 3px;">ADD_YOUR_CATALOG_HERE</mark></code> with your catalog name from the cell output above (e.g., <code>labuser_1234</code>)</strong> with your actual catalog name. After the statement completes, return to this notebook and continue with D3.</p>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D3. Create the Products Streaming Table in the SQL Editor
-- MAGIC
-- MAGIC Now create a second Streaming Table for the product catalog. Same pattern — copy to the SQL Editor.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fffbeb; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Copy to SQL Editor</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Paste and run in the same SQL Editor tab (your catalog should still be set):</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC <button onclick="copySTProducts()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-st-products" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>DROP TABLE IF EXISTS bronze.raw_products;
-- MAGIC
-- MAGIC CREATE OR REPLACE STREAMING TABLE bronze.raw_products
-- MAGIC TBLPROPERTIES (
-- MAGIC   'delta.enableChangeDataFeed' = 'true',
-- MAGIC   'delta.enableRowTracking' = 'true',
-- MAGIC   'delta.enableDeletionVectors' = 'true'
-- MAGIC )
-- MAGIC AS SELECT * FROM STREAM read_files(
-- MAGIC   '/Volumes/<mark style="background:#FEF3C7;color:#000;padding:1px 3px;">ADD_YOUR_CATALOG_HERE</mark>/data/course_data/raw_products',
-- MAGIC   format =&gt; 'csv',
-- MAGIC   header =&gt; 'true',
-- MAGIC   inferColumnTypes =&gt; 'true',
-- MAGIC   schemaEvolutionMode =&gt; 'rescue'
-- MAGIC );
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copySTProducts() {
-- MAGIC   var el = document.getElementById("copy-block-st-products");
-- MAGIC   if (!el) return;
-- MAGIC   var text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text).then(function() { alert("Copied to clipboard"); }).catch(function() { fallbackCopyProd(text); });
-- MAGIC   } else { fallbackCopyProd(text); }
-- MAGIC }
-- MAGIC function fallbackCopyProd(text) {
-- MAGIC   var textarea = document.createElement("textarea"); textarea.value = text; textarea.style.position = "fixed"; textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea); textarea.select();
-- MAGIC   try { document.execCommand("copy"); alert("Copied to clipboard"); } catch(e) { alert("Copy failed — select and copy manually."); }
-- MAGIC   finally { document.body.removeChild(textarea); }
-- MAGIC }
-- MAGIC </script>
-- MAGIC <p style="margin-top:8px; color:#555;"><strong>Replace <code><mark style="background:#FEF3C7;color:#000;padding:1px 3px;">ADD_YOUR_CATALOG_HERE</mark></code> with your catalog name from the cell output above (e.g., <code>labuser_1234</code>)</strong> with your catalog name. After the statement completes, return to this notebook and continue with D4.</p>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D4. Verify the Bronze Tables
-- MAGIC
-- MAGIC Let's confirm the row counts for both Streaming Tables.

-- COMMAND ----------

SELECT 'bronze.raw_orders' AS table_name, COUNT(*) AS row_count
FROM IDENTIFIER(my_catalog || '.bronze.raw_orders')
UNION ALL
SELECT 'bronze.raw_products', COUNT(*)
FROM IDENTIFIER(my_catalog || '.bronze.raw_products');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>bronze.raw_orders</strong> should contain approximately <strong>15,000 rows</strong></li>
-- MAGIC       <li><strong>bronze.raw_products</strong> should contain approximately <strong>200 rows</strong> (the product catalog)</li>
-- MAGIC       <li>Both schemas were auto-inferred from CSV files by Auto Loader with <code>inferColumnTypes</code></li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Create the Silver Materialized View
-- MAGIC
-- MAGIC Now we build the Silver layer. The **Materialized View** joins orders with customers and products to create a denormalized, analysis-ready table. We apply type casting, null handling, and enrichment joins.
-- MAGIC
-- MAGIC **Why CAST when `inferColumnTypes` is enabled?** This is *defensive typing*: Auto Loader infers types from the first batch of files, but CSV inference is best-effort. Explicit CASTs in the MV guarantee the Silver layer has the exact types downstream consumers expect, regardless of how the source schema was inferred.
-- MAGIC
-- MAGIC **Note:** This MV joins `data.raw_customers` directly (a regular Delta table) as a simplification for this demo. In the full pipeline (Module 3+), this join will target the CDC-processed Silver customer table instead.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E1. Create the MV in the SQL Editor

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fffbeb; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Copy to SQL Editor</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In the <strong>SQL Editor</strong>:</p>
-- MAGIC     <ol>
-- MAGIC       <li>Confirm the <strong>default catalog</strong> is set to your <code>labuser_XXXX</code> catalog</li>
-- MAGIC       <li>Confirm the <strong>default schema</strong> is set to <code>default</code></li>
-- MAGIC       <li>Copy the SQL below and run it. Hover over each highlighted section to explore what it does.</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <style>
-- MAGIC .e1mv-rp { display:inline-block; width:100%; background:rgba(0,169,114,0.2); padding:0 4px; border-radius:3px; cursor:pointer; transition:background 0.2s; }
-- MAGIC .e1mv-rp:hover { background:rgba(0,169,114,0.4); }
-- MAGIC .e1mv-card { position:absolute; left:50%; top:80px; width:260px; background:#F9F7F4; border-radius:8px; border-left:5px solid #00A972; padding:12px 14px; font-size:0.75rem; line-height:1.5; color:#444; opacity:0; transition:opacity 0.3s; pointer-events:none; box-shadow:0 4px 16px rgba(0,0,0,0.3); font-family:system-ui,sans-serif; z-index:10; word-wrap:break-word; overflow-wrap:break-word; white-space:normal; }
-- MAGIC .e1mv-rp:hover + .e1mv-card, .e1mv-card:hover { opacity:1; }
-- MAGIC </style>
-- MAGIC <button onclick="e1CopyMVSQL()">Copy to clipboard</button>
-- MAGIC <div style="position:relative; display:inline-block; width:100%;">
-- MAGIC <pre id="copy-block-mv" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>DROP MATERIALIZED VIEW IF EXISTS silver.orders_enriched;
-- MAGIC
-- MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver.orders_enriched
-- MAGIC <span class="e1mv-rp">REFRESH POLICY INCREMENTAL</span><span class="e1mv-card"><strong style="color:#00A972;font-size:0.9rem;">REFRESH POLICY INCREMENTAL</strong><br/><br/>Prefers <strong>incremental refresh</strong> &mdash; processing only changed rows when feasible. Falls back to full recompute if incremental becomes unavailable.<br/><br/><strong>Fails at CREATE</strong> if the query cannot be incrementalized &mdash; which is why we verified with EXPLAIN first (Section C).<br/><br/>Must be set at <strong>creation time</strong> &mdash; cannot be added via ALTER.</span>
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   o.order_id,
-- MAGIC   o.customer_id,
-- MAGIC   o.product_id,
-- MAGIC   CAST(o.order_date AS DATE)                          AS order_date,
-- MAGIC   CAST(o.quantity AS INT)                              AS quantity,
-- MAGIC   CAST(o.unit_price AS DECIMAL(10,2))                  AS unit_price,
-- MAGIC   CAST(o.quantity * o.unit_price AS DECIMAL(12,2))     AS line_total,
-- MAGIC   COALESCE(o.discount_pct, 0)                          AS discount_pct,
-- MAGIC   COALESCE(o.shipping_cost, 0.00)                      AS shipping_cost,
-- MAGIC   o.order_status,
-- MAGIC   o.payment_method,
-- MAGIC   COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS customer_name,
-- MAGIC   c.customer_segment,
-- MAGIC   c.city                                                AS customer_city,
-- MAGIC   c.state                                               AS customer_state,
-- MAGIC   COALESCE(p.product_name, 'Unknown Product')           AS product_name,
-- MAGIC   p.category                                            AS product_category,
-- MAGIC   p.brand                                               AS product_brand,
-- MAGIC   CAST(p.list_price AS DECIMAL(10,2))                   AS product_list_price
-- MAGIC FROM bronze.raw_orders o
-- MAGIC LEFT JOIN data.raw_customers c
-- MAGIC   ON o.customer_id = c.customer_id
-- MAGIC LEFT JOIN bronze.raw_products p
-- MAGIC   ON o.product_id = p.product_id;
-- MAGIC </code></pre>
-- MAGIC </div>
-- MAGIC <textarea id="e1mv-sql-text" style="position:absolute;left:-9999px;">DROP MATERIALIZED VIEW IF EXISTS silver.orders_enriched;
-- MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver.orders_enriched
-- MAGIC REFRESH POLICY INCREMENTAL
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   o.order_id,
-- MAGIC   o.customer_id,
-- MAGIC   o.product_id,
-- MAGIC   CAST(o.order_date AS DATE)                          AS order_date,
-- MAGIC   CAST(o.quantity AS INT)                              AS quantity,
-- MAGIC   CAST(o.unit_price AS DECIMAL(10,2))                  AS unit_price,
-- MAGIC   CAST(o.quantity * o.unit_price AS DECIMAL(12,2))     AS line_total,
-- MAGIC   COALESCE(o.discount_pct, 0)                          AS discount_pct,
-- MAGIC   COALESCE(o.shipping_cost, 0.00)                      AS shipping_cost,
-- MAGIC   o.order_status,
-- MAGIC   o.payment_method,
-- MAGIC   COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS customer_name,
-- MAGIC   c.customer_segment,
-- MAGIC   c.city                                                AS customer_city,
-- MAGIC   c.state                                               AS customer_state,
-- MAGIC   COALESCE(p.product_name, 'Unknown Product')           AS product_name,
-- MAGIC   p.category                                            AS product_category,
-- MAGIC   p.brand                                               AS product_brand,
-- MAGIC   CAST(p.list_price AS DECIMAL(10,2))                   AS product_list_price
-- MAGIC FROM bronze.raw_orders o
-- MAGIC LEFT JOIN data.raw_customers c
-- MAGIC   ON o.customer_id = c.customer_id
-- MAGIC LEFT JOIN bronze.raw_products p
-- MAGIC   ON o.product_id = p.product_id;</textarea>
-- MAGIC <script>
-- MAGIC function e1CopyMVSQL() {
-- MAGIC   var el = document.getElementById('e1mv-sql-text');
-- MAGIC   if (!el) return;
-- MAGIC   el.style.position = 'fixed'; el.style.left = '0'; el.style.top = '0';
-- MAGIC   el.select();
-- MAGIC   try { document.execCommand('copy'); alert('Copied to clipboard'); } catch(e) { alert('Copy failed'); }
-- MAGIC   el.style.position = 'absolute'; el.style.left = '-9999px';
-- MAGIC }
-- MAGIC </script>
-- MAGIC <p style="margin-top:8px; color:#555;">After the statement completes in the SQL Editor, return to this notebook and continue with E2.</p>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E2. Verify the Materialized View
-- MAGIC
-- MAGIC Let's preview the enriched data and confirm the joins worked correctly.

-- COMMAND ----------

SELECT *
FROM IDENTIFIER(my_catalog || '.silver.orders_enriched')
LIMIT 20;

-- COMMAND ----------

SELECT COUNT(*) AS row_count
FROM IDENTIFIER(my_catalog || '.silver.orders_enriched');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>Each order now includes the <strong>customer name</strong>, <strong>segment</strong>, and <strong>location</strong> from the customers table</li>
-- MAGIC       <li>Each order includes the <strong>product name</strong>, <strong>category</strong>, and <strong>brand</strong> from the products table</li>
-- MAGIC       <li>The <code>line_total</code> column was computed as <code>quantity * unit_price</code></li>
-- MAGIC       <li><code>COALESCE</code> replaced any null customer names with <code>'Unknown'</code> and null product names with <code>'Unknown Product'</code></li>
-- MAGIC       <li>Dates and numeric fields are now properly typed</li>
-- MAGIC     </ul>
-- MAGIC     <p style="margin-top:10px;"><strong>Key teaching point:</strong> Materialized Views store the precomputed result. This query would be expensive to run repeatedly against raw data, but the MV makes it instant for downstream consumers.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Configure TRIGGER ON UPDATE
-- MAGIC
-- MAGIC We already set **REFRESH POLICY INCREMENTAL** at creation time (the `REFRESH POLICY INCREMENTAL` clause in the CREATE statement). This tells the engine *how* to refresh — process only changed rows.
-- MAGIC
-- MAGIC Now we configure *when* to refresh. Right now the MV has no automatic trigger — we would need to manually run `REFRESH MATERIALIZED VIEW` every time the source data changes. **TRIGGER ON UPDATE** fixes that by automatically refreshing the MV whenever its source table is updated.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### F1. Set TRIGGER ON UPDATE
-- MAGIC
-- MAGIC Now we connect the two pipeline objects: when `bronze.raw_orders` is refreshed, the MV should **automatically** refresh too. This is `TRIGGER ON UPDATE` — it creates a dependency chain that eliminates the need for manual MV refreshes.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fffbeb; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Copy to SQL Editor</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In the <strong>SQL Editor</strong>, run:</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC <button onclick="copyTrigger()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-trigger" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>ALTER STREAMING TABLE bronze.raw_orders
-- MAGIC ADD TRIGGER ON UPDATE;
-- MAGIC
-- MAGIC ALTER STREAMING TABLE bronze.raw_products
-- MAGIC ADD TRIGGER ON UPDATE;
-- MAGIC
-- MAGIC ALTER MATERIALIZED VIEW silver.orders_enriched
-- MAGIC ADD TRIGGER ON UPDATE;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyTrigger() {
-- MAGIC   var el = document.getElementById("copy-block-trigger");
-- MAGIC   if (!el) return;
-- MAGIC   var text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text).then(function() { alert("Copied to clipboard"); }).catch(function() { fallbackCopyTR(text); });
-- MAGIC   } else { fallbackCopyTR(text); }
-- MAGIC }
-- MAGIC function fallbackCopyTR(text) {
-- MAGIC   var textarea = document.createElement("textarea"); textarea.value = text; textarea.style.position = "fixed"; textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea); textarea.select();
-- MAGIC   try { document.execCommand("copy"); alert("Copied to clipboard"); } catch(e) { alert("Copy failed — select and copy manually."); }
-- MAGIC   finally { document.body.removeChild(textarea); }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>We have now configured a two-part refresh strategy:</p>
-- MAGIC     <ul>
-- MAGIC       <li><strong>REFRESH POLICY INCREMENTAL</strong> — the MV processes only changed rows (the <em>how</em>)</li>
-- MAGIC       <li><strong>TRIGGER ON UPDATE</strong> — the MV refreshes automatically when the ST is refreshed (the <em>when</em>)</li>
-- MAGIC     </ul>
-- MAGIC     <p style="margin-top:10px;"><strong>Key teaching point:</strong> Together, these two settings mean we only need to refresh the Streaming Table. The MV takes care of itself — it detects the upstream change and incrementally processes only the new data. This is declarative pipeline orchestration: you define the dependencies, and the engine handles the execution.</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Incremental Processing — Triggered by New Data
-- MAGIC
-- MAGIC Now we put it all together. We will:
-- MAGIC 1. Capture baseline counts
-- MAGIC 2. Add a new batch of order files to the Volume
-- MAGIC 3. Refresh **only** the Streaming Table
-- MAGIC 4. Verify that the MV **auto-refreshed** via TRIGGER ON UPDATE
-- MAGIC
-- MAGIC This is the key teaching moment: **one manual refresh triggers the entire pipeline**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G1. Capture Baseline Counts
-- MAGIC
-- MAGIC Before adding new data, record the current row counts for both objects.

-- COMMAND ----------

SELECT 'bronze.raw_orders (ST)' AS table_name, COUNT(*) AS row_count
FROM IDENTIFIER(my_catalog || '.bronze.raw_orders')
UNION ALL
SELECT 'bronze.raw_products (ST)', COUNT(*)
FROM IDENTIFIER(my_catalog || '.bronze.raw_products')
UNION ALL
SELECT 'silver.orders_enriched (MV)', COUNT(*)
FROM IDENTIFIER(my_catalog || '.silver.orders_enriched');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G2. Add New Order Files to the Volume
-- MAGIC
-- MAGIC In a real pipeline, new data arrives as files landing in cloud storage. We simulate this by writing a new CSV file into the same Volume directory that our Streaming Table monitors. The ST does not know about this file yet — it will only pick it up when we refresh.

-- COMMAND ----------

EXECUTE IMMEDIATE
  CONCAT(
    'INSERT OVERWRITE DIRECTORY \'/Volumes/' || my_catalog || '/data/course_data/raw_orders/raw_orders_new_batch\'
     USING CSV OPTIONS (\'header\' = \'true\')
     SELECT * FROM ' || my_catalog || '.data.raw_orders_new_batch'
  );

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     A new CSV file containing approximately <strong>2,000 new order records</strong> has been written to the Volume. The Streaming Table has not yet ingested it.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G3. Refresh the Streaming Table
-- MAGIC
-- MAGIC This is the **only manual step**. We refresh the Streaming Table — Auto Loader detects the new file and ingests only its contents. Because we configured `TRIGGER ON UPDATE`, this refresh will also **automatically trigger** a refresh of the `orders_enriched` Materialized View.

-- COMMAND ----------

REFRESH STREAMING TABLE IDENTIFIER(my_catalog || '.bronze.raw_orders');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     The REFRESH STREAMING TABLE command completed. Behind the scenes, two things happened:
-- MAGIC     <ol>
-- MAGIC       <li>Auto Loader ingested the new CSV file into <code>bronze.raw_orders</code></li>
-- MAGIC       <li>The TRIGGER ON UPDATE fired, automatically refreshing <code>silver.orders_enriched</code></li>
-- MAGIC     </ol>
-- MAGIC     <p>We did <strong>not</strong> run <code>REFRESH MATERIALIZED VIEW</code> — the trigger handled it.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G4. Verify the Auto-Refresh
-- MAGIC
-- MAGIC Let's confirm that **both** the Streaming Table and Materialized View now contain the new data — even though we only manually refreshed the ST.

-- COMMAND ----------

SELECT 'bronze.raw_orders (ST)' AS table_name, COUNT(*) AS row_count
FROM IDENTIFIER(my_catalog || '.bronze.raw_orders')
UNION ALL
SELECT 'bronze.raw_products (ST)', COUNT(*)
FROM IDENTIFIER(my_catalog || '.bronze.raw_products')
UNION ALL
SELECT 'silver.orders_enriched (MV)', COUNT(*)
FROM IDENTIFIER(my_catalog || '.silver.orders_enriched');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>Both the ST and MV row counts increased by approximately <strong>2,000 rows</strong></li>
-- MAGIC       <li>The totals should now be approximately <strong>17,000 rows</strong> each</li>
-- MAGIC       <li>We only ran <code>REFRESH STREAMING TABLE</code> — the MV refreshed <strong>automatically</strong> because of the <code>TRIGGER ON UPDATE</code> we configured in Section F</li>
-- MAGIC     </ul>
-- MAGIC     <p style="margin-top:10px;"><strong>Key teaching point:</strong> This is declarative pipeline orchestration. You define the dependency (<code>TRIGGER ON UPDATE</code>), and the engine handles the cascading refresh. In a production pipeline with dozens of downstream objects, this eliminates the need for external orchestration of individual refresh commands.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G5. Examine the Refresh History
-- MAGIC
-- MAGIC `DESCRIBE HISTORY` shows the Delta version history with operation metrics for each refresh — proof that only the new batch was processed, not the entire table.

-- COMMAND ----------

EXECUTE IMMEDIATE
  CONCAT('DESCRIBE HISTORY ' || my_catalog || '.bronze.raw_orders LIMIT 5');

-- COMMAND ----------

-- DESCRIBE HISTORY is not supported on Materialized Views
-- To check MV refresh history, use the pipeline UI (Section H) or query the event_log
SELECT 'Check the auto-created pipeline in the Pipelines UI for MV refresh history' AS note;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Look at the <code>operationMetrics</code> column in both histories:</p>
-- MAGIC     <ul>
-- MAGIC       <li><strong>Streaming Table:</strong> The most recent version shows ~2,000 rows written (the new batch). Earlier versions show the initial load of ~15,000 rows. Auto Loader remembered which files were already processed.</li>
-- MAGIC       <li><strong>Materialized View:</strong> The most recent version also shows the incremental refresh triggered by the ST update. Look at the timestamps — the MV refresh happened shortly after the ST refresh, confirming the TRIGGER ON UPDATE fired automatically.</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Explore the Auto-Created Pipeline
-- MAGIC
-- MAGIC Every Streaming Table and Materialized View is backed by a **Lakeflow Spark Declarative Pipeline (SDP)** that is auto-created and managed. You can explore these in the UI.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fffbeb; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Explore in the UI</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the left sidebar, click <strong>Jobs and Pipelines</strong></li>
-- MAGIC       <li>Click the <strong>Pipelines</strong> tab</li>
-- MAGIC       <li>Find the pipeline named <strong>ST-&lt;&lt;your_catalog&gt;&gt;.bronze.raw_orders</strong></li>
-- MAGIC       <li>Click the <strong>Performance</strong> tab and look at the most recent refresh entry</li>
-- MAGIC       <li>Check <strong>Rows Read / Rows Written</strong> — this confirms only ~2,000 rows were processed in the last refresh</li>
-- MAGIC       <li>Then find <strong>MV-&lt;&lt;your_catalog&gt;&gt;.silver.orders_enriched</strong> and check its <strong>Incrementalization</strong> field — it should show <strong>Incremental</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p style="margin-top:8px;">The Pipelines UI gives you full visibility into what happened under the hood — Rows Read/Written per refresh, query profiles, and incrementalization status.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## I. Key Summary and Takeaways
-- MAGIC
-- MAGIC In this demo, we built a complete Bronze-to-Silver incremental pipeline. Here are the key patterns:
-- MAGIC
-- MAGIC | Concept | What We Did | Why It Matters |
-- MAGIC |---------|-------------|----------------|
-- MAGIC | **EXPLAIN** | Ran EXPLAIN before and after setting properties | Pre-deployment diagnostic — tells you exactly what to fix for incremental refresh |
-- MAGIC | **Streaming Tables** | Created `bronze.raw_orders` and `bronze.raw_products` with Auto Loader | Append-only ingestion with automatic file tracking — only new files are processed |
-- MAGIC | **Materialized View** | Created `silver.orders_enriched` with joins | Precomputed, analysis-ready data — complex query runs once, not on every read |
-- MAGIC | **REFRESH POLICY INCREMENTAL** | Set on the MV | Processes only changed rows, not the full dataset (the *how*) |
-- MAGIC | **TRIGGER ON UPDATE** | Connected MV to ST | MV auto-refreshes when ST is refreshed — no manual step needed (the *when*) |
-- MAGIC | **Incremental processing** | Added new data, refreshed only the ST | One manual refresh cascaded through the entire pipeline — ~2,000 new rows processed, not 17,000 |
-- MAGIC
-- MAGIC **The key insight:** By combining `REFRESH POLICY INCREMENTAL` with `TRIGGER ON UPDATE`, you build pipelines where refreshing the first object in the chain is the *only* manual step. Everything downstream refreshes automatically and incrementally.

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
