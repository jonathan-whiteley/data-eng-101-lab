-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](../Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2.3 Lab - Building a Customer Feedback Pipeline
-- MAGIC
-- MAGIC #### Duration: ~15–20 Minutes
-- MAGIC
-- MAGIC ## Overview
-- MAGIC In this lab, you will build a medallion-architecture pipeline centered on customer feedback. You will create bronze streaming tables with Auto Loader, build a silver materialized view that enriches feedback with order, product, and customer context, and create a gold-layer aggregation.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you will be able to:
-- MAGIC - Use **EXPLAIN CREATE MATERIALIZED VIEW** to verify incrementalizability and understand its limitations with streaming tables
-- MAGIC - Create **Streaming Tables** from volume CSVs using `read_files()` with Auto Loader
-- MAGIC - Create a **Materialized View** with multi-table joins, type casting, and null handling
-- MAGIC - Build a **gold-layer aggregation** from the silver materialized view
-- MAGIC - Demonstrate **incremental processing** by inserting new data and verifying only deltas are processed

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

-- MAGIC %md-sandbox
-- MAGIC ## A. REQUIRED - Classroom Setup
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Run the Setup Cell</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Run the following cell to configure your working environment for this lab. The setup creates your schemas and drops any leftover objects from previous runs so you start fresh.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.3-Lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Pipeline Architecture
-- MAGIC
-- MAGIC Here is the full pipeline architecture you will build across this course. The objects highlighted below show what you will create in **this lab** — the remaining components are built in later demos and labs.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="max-width: 1100px; margin: 0 auto;">
-- MAGIC <div>
-- MAGIC <div class="mermaid">
-- MAGIC flowchart TD
-- MAGIC     subgraph BRONZE["<b>BRONZE — Streaming Tables</b>"]
-- MAGIC         direction LR
-- MAGIC         RO["raw_orders<br/><small>ST · Auto Loader</small>"]
-- MAGIC         RP["raw_products<br/><small>ST · Auto Loader</small>"]
-- MAGIC         CF["customer_feedback<br/><small>ST · Auto Loader</small>"]
-- MAGIC     end
-- MAGIC     subgraph CDC["<b>CDC — Streaming Tables</b>"]
-- MAGIC         CDC1["demo_cdc_customers_current<br/><small>ST · AUTO CDC · SCD Type 1</small>"]
-- MAGIC     end
-- MAGIC     subgraph SILVER["<b>SILVER — Materialized Views</b>"]
-- MAGIC         CFE["customer_feedback_enriched<br/><small>MV · joins + context</small>"]
-- MAGIC     end
-- MAGIC     subgraph GOLD["<b>GOLD — Business-Ready Aggregations</b>"]
-- MAGIC         FBC["feedback_by_category<br/><small>MV · aggregation</small>"]
-- MAGIC     end
-- MAGIC     CF --> CFE
-- MAGIC     RO --> CFE
-- MAGIC     RP --> CFE
-- MAGIC     CDC1 --> CFE
-- MAGIC     CFE --> FBC
-- MAGIC     classDef bronze fill:#FFF3E0,stroke:#e65100,stroke-width:2px,color:#333
-- MAGIC     classDef silver fill:#E3F2FD,stroke:#1565c0,stroke-width:2px,color:#333
-- MAGIC     classDef gold fill:#E8F5E9,stroke:#2e7d32,stroke-width:2px,color:#333
-- MAGIC     classDef cdc fill:#F3E5F5,stroke:#1976d2,stroke-width:2px,color:#333
-- MAGIC     class RO,RP,CF bronze
-- MAGIC     class CFE silver
-- MAGIC     class FBC gold
-- MAGIC     class CDC1 cdc
-- MAGIC     style BRONZE fill:#FFF8E1,stroke:#e65100,stroke-width:2px
-- MAGIC     style SILVER fill:#E8EAF6,stroke:#1565c0,stroke-width:2px
-- MAGIC     style GOLD fill:#E8F5E9,stroke:#2e7d32,stroke-width:2px
-- MAGIC     style CDC fill:#F3E5F5,stroke:#1976d2,stroke-width:2px
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. EXPLAIN CREATE MATERIALIZED VIEW
-- MAGIC
-- MAGIC Before building the pipeline, we use `EXPLAIN CREATE MATERIALIZED VIEW` to verify that our planned query can be **refreshed incrementally**. Incremental refresh means the engine processes only new/changed rows on each refresh — not the entire dataset. This is critical for production pipelines with millions of rows.
-- MAGIC
-- MAGIC In Demo 2.2, the EXPLAIN issue was about missing table properties on the source tables. Here, we explore a different problem: **query patterns that prevent incrementalizability** even when the tables are configured correctly.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### C1. A Non-Incrementalizable Query
-- MAGIC
-- MAGIC The query uses a **window function without `PARTITION BY`** — `ROW_NUMBER() OVER (ORDER BY f.rating DESC)`. This creates a single ranking across every row in the table.
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Run Both Cells Below</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     The first runs <code>EXPLAIN</code> to check incrementalizability. The second runs the actual query so you can see the output — a global ranking of all feedback by rating.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

EXPLAIN CREATE MATERIALIZED VIEW silver._explain_check
AS SELECT
  f.feedback_id,
  f.customer_id,
  f.rating,
  p.category AS product_category,
  ROW_NUMBER() OVER (ORDER BY f.rating DESC) AS rating_rank
FROM data.customer_feedback f
LEFT JOIN data.raw_orders o
  ON f.order_id = o.order_id
LEFT JOIN data.raw_products p
  ON o.product_id = p.product_id;

-- COMMAND ----------

-- See what the global ranking looks like
SELECT
  f.feedback_id,
  f.customer_id,
  f.rating,
  p.category AS product_category,
  ROW_NUMBER() OVER (ORDER BY f.rating DESC) AS rating_rank
FROM data.customer_feedback f
LEFT JOIN data.raw_orders o
  ON f.order_id = o.order_id
LEFT JOIN data.raw_products p
  ON o.product_id = p.product_id
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #dc2626; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#991b1b; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The EXPLAIN output reports error <code>WINDOW_WITHOUT_PARTITION_BY</code> — the query is <strong>NOT incrementalizable</strong>.</p>
-- MAGIC     <p>Look at the SELECT output above. Notice how <code>rating_rank</code> is a single sequence (1, 2, 3, ...) across ALL rows regardless of category. If a new row arrives with rating 5, every rank below it shifts — the engine would have to recompute the <strong>entire ranking</strong> on every refresh.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Fix the Query — Add PARTITION BY and Complete the JOINs
-- MAGIC
-- MAGIC The fix is not to remove the window function — it is to add `PARTITION BY`. But you also need to complete the JOIN conditions.
-- MAGIC
-- MAGIC **An easy way to think about it:**
-- MAGIC - Without `PARTITION BY` = **one leaderboard for everyone** — every new row reshuffles the entire ranking
-- MAGIC - With `PARTITION BY p.category` = **one leaderboard per category** — a new Electronics review only affects the Electronics leaderboard
-- MAGIC
-- MAGIC **Why that matters for incremental refresh:** The engine can update one partition's leaderboard without touching the others. A global leaderboard requires reprocessing everything.
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Your Task</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     In the cell below, replace each <code>&lt;FILL_IN&gt;</code>:
-- MAGIC     <ul>
-- MAGIC       <li>The first two <code>&lt;FILL_IN&gt;</code> are the <strong>JOIN ON conditions</strong> — match the correct keys between the tables</li>
-- MAGIC       <li>The third <code>&lt;FILL_IN&gt;</code> adds <code>PARTITION BY p.category</code> to the window function</li>
-- MAGIC     </ul>
-- MAGIC     Run the EXPLAIN to confirm it passes, then run the SELECT to compare the per-category rankings with the global rankings from C1.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- Fix the query: complete the JOIN conditions and add PARTITION BY
-- Hint: Match the keys between tables and partition the window function by category

EXPLAIN CREATE MATERIALIZED VIEW silver._explain_check
AS SELECT
  f.feedback_id,
  f.customer_id,
  f.rating,
  p.category AS product_category,
  ROW_NUMBER() OVER (<FILL_IN> ORDER BY f.rating DESC) AS rating_rank
FROM data.customer_feedback f
LEFT JOIN data.raw_orders o
  ON <FILL_IN>
LEFT JOIN data.raw_products p
  ON <FILL_IN>;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyExplainFix()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-explain" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>EXPLAIN CREATE MATERIALIZED VIEW silver._explain_check
-- MAGIC AS SELECT
-- MAGIC   f.feedback_id,
-- MAGIC   f.customer_id,
-- MAGIC   f.rating,
-- MAGIC   p.category AS product_category,
-- MAGIC   ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY f.rating DESC) AS rating_rank
-- MAGIC FROM data.customer_feedback f
-- MAGIC LEFT JOIN data.raw_orders o
-- MAGIC   ON f.order_id = o.order_id
-- MAGIC LEFT JOIN data.raw_products p
-- MAGIC   ON o.product_id = p.product_id;</code></pre>
-- MAGIC         <script>function copyExplainFix(){const e=document.getElementById("copy-block-explain");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbExpl(t)):fbExpl(t)}function fbExpl(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <style>
-- MAGIC details summary span:first-child {
-- MAGIC   transition: transform 0.2s ease;
-- MAGIC   display: inline-block;
-- MAGIC }
-- MAGIC details[open] summary span:first-child {
-- MAGIC   transform: rotate(90deg);
-- MAGIC }
-- MAGIC </style>

-- COMMAND ----------

-- Compare: see how the per-category ranking differs from the global ranking in C1
SELECT
  f.feedback_id,
  f.customer_id,
  f.rating,
  p.category AS product_category,
  ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY f.rating DESC) AS rating_rank
FROM data.customer_feedback f
LEFT JOIN data.raw_orders o
  ON f.order_id = o.order_id
LEFT JOIN data.raw_products p
  ON o.product_id = p.product_id
LIMIT 200;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>This time EXPLAIN reports the query <strong>is incrementalizable</strong>. The window function still works — but now it ranks within each product category instead of globally. The engine can maintain per-partition rankings incrementally because new rows only affect their own partition.</p>
-- MAGIC     <p><strong>The lesson:</strong> A non-incrementalizable query does not always mean you have to remove the offending pattern — sometimes you can <strong>rewrite it</strong> to achieve the same goal in an incrementalizable way. Always run EXPLAIN before building a materialized view. See the full list of non-incrementalizable error codes (<a href="https://docs.databricks.com/aws/en/error-messages/materialized-view-not-incrementalizable-error-class">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/error-messages/materialized-view-not-incrementalizable-error-class">Azure</a> | <a href="https://docs.databricks.com/gcp/en/error-messages/materialized-view-not-incrementalizable-error-class">GCP</a>).</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Create Bronze Streaming Tables
-- MAGIC
-- MAGIC The bronze layer ingests raw data from CSV files in a Unity Catalog Volume using `read_files()` with **Auto Loader**. Auto Loader tracks which files have been processed, so on subsequent refreshes only new files are ingested — this is the foundation of incremental processing.
-- MAGIC
-- MAGIC You will create three streaming tables. Each requires:
-- MAGIC - **`TBLPROPERTIES`** enabling Change Data Feed, Row Tracking, and Deletion Vectors (required for downstream incremental MVs)
-- MAGIC - **`read_files()`** pointing to the correct Volume path with CSV format options
-- MAGIC - **`partitionColumns => ''`** to prevent Auto Loader partition inference errors
-- MAGIC
-- MAGIC D1 is provided as a working example. Study the pattern, then replicate it for D2 and D3.
-- MAGIC
-- MAGIC **These CREATE statements must be run in the SQL Editor** — not in this notebook. Each section below provides a copy-to-clipboard block. After running the SQL in the SQL Editor, return here for verification.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### D0. Your Catalog Name
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Reference Query</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Run the cell below to display your catalog name. You will need to substitute it into the SQL Editor statements throughout sections D and E.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

SELECT my_catalog AS your_catalog_name;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### D1. Create raw_orders (example — study this pattern)
-- MAGIC
-- MAGIC This first streaming table is provided for you — no FILL_IN needed. **Copy it to the SQL Editor**, replace <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">ADD_YOUR_CATALOG_HERE</mark> with your catalog name, and run it. Study the pattern — you will replicate it for D2 and D3.
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #e65100; background: #FFF3E0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#bf360c; margin-bottom:6px; font-size: 1.1em;">Set the Default Catalog</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     In the SQL Editor, make sure your <strong>default catalog</strong> is set to your assigned catalog (shown above) before running the statement.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <button onclick="copyD1()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-d1" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
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
-- MAGIC   schemaEvolutionMode =&gt; 'rescue',
-- MAGIC   partitionColumns =&gt; ''
-- MAGIC );</code></pre>
-- MAGIC <script>function copyD1(){const e=document.getElementById("copy-block-d1");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbD1(t)):fbD1(t)}function fbD1(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC
-- MAGIC <p style="margin-top:12px; color:#555;"><strong>After replacing</strong> <code>ADD_YOUR_CATALOG_HERE</code> <strong>and running in the SQL Editor, return to this notebook.</strong></p>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Check the Pipeline</strong>
-- MAGIC   <div style="color:#333;">The <code>CREATE STREAMING TABLE</code> statement automatically triggers an initial refresh. In the left sidebar, open <strong>Jobs &amp; Pipelines</strong> to see it running. While it processes, continue creating the next table.</div>
-- MAGIC </div>

-- COMMAND ----------

-- Verify raw_orders was populated
SELECT COUNT(*) AS row_count, MIN(order_date) AS earliest, MAX(order_date) AS latest
FROM bronze.raw_orders;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     You should see approximately <strong>15,000 rows</strong> spanning the course's date range. This confirms Auto Loader ingested all CSV files from the volume on the initial refresh.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### D2. Create raw_products
-- MAGIC
-- MAGIC Using D1 as your template, create the `raw_products` streaming table. The only differences from D1 are the **table name**, **volume path** (`raw_products` instead of `raw_orders`), and the addition of `rescuedDataColumn => '_rescued_data'` (which captures any columns that don't match the inferred schema).
-- MAGIC
-- MAGIC Replace <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark> with the volume path: `/Volumes/ADD_YOUR_CATALOG_HERE/data/course_data/raw_products` — substitute your actual catalog name for `ADD_YOUR_CATALOG_HERE` (the same catalog shown by the query in D0).
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #e65100; background: #FFF3E0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#bf360c; margin-bottom:6px; font-size: 1.1em;">Set the Default Catalog</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     In the SQL Editor, make sure your <strong>default catalog</strong> is set to your assigned catalog before running the statement.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <button onclick="copyD2()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-d2" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>DROP TABLE IF EXISTS bronze.raw_products;
-- MAGIC
-- MAGIC CREATE OR REPLACE STREAMING TABLE bronze.raw_products
-- MAGIC TBLPROPERTIES (
-- MAGIC   'delta.enableChangeDataFeed' = 'true',
-- MAGIC   'delta.enableRowTracking' = 'true',
-- MAGIC   'delta.enableDeletionVectors' = 'true'
-- MAGIC )
-- MAGIC AS SELECT * FROM STREAM read_files(
-- MAGIC   '<mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark>',
-- MAGIC   format =&gt; 'csv',
-- MAGIC   header =&gt; 'true',
-- MAGIC   inferColumnTypes =&gt; 'true',
-- MAGIC   rescuedDataColumn =&gt; '_rescued_data',
-- MAGIC   schemaEvolutionMode =&gt; 'rescue',
-- MAGIC   partitionColumns =&gt; ''
-- MAGIC );</code></pre>
-- MAGIC <script>function copyD2(){const e=document.getElementById("copy-block-d2");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbD2(t)):fbD2(t)}function fbD2(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC
-- MAGIC <p style="margin-top:12px; color:#555;"><strong>After completing the FILL_IN and running in the SQL Editor, return to this notebook.</strong></p>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyD2Help()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-d2-help" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>DROP TABLE IF EXISTS bronze.raw_products;
-- MAGIC
-- MAGIC CREATE OR REPLACE STREAMING TABLE bronze.raw_products
-- MAGIC TBLPROPERTIES (
-- MAGIC   'delta.enableChangeDataFeed' = 'true',
-- MAGIC   'delta.enableRowTracking' = 'true',
-- MAGIC   'delta.enableDeletionVectors' = 'true'
-- MAGIC )
-- MAGIC AS SELECT * FROM STREAM read_files(
-- MAGIC   '/Volumes/&lt;YOUR_CATALOG&gt;/data/course_data/raw_products',
-- MAGIC   format =&gt; 'csv',
-- MAGIC   header =&gt; 'true',
-- MAGIC   inferColumnTypes =&gt; 'true',
-- MAGIC   rescuedDataColumn =&gt; '_rescued_data',
-- MAGIC   schemaEvolutionMode =&gt; 'rescue',
-- MAGIC   partitionColumns =&gt; ''
-- MAGIC );</code></pre>
-- MAGIC         <p style="margin-top:8px; color:#555;"><strong>Note:</strong> Replace <code>&lt;your_catalog&gt;</code> with your actual catalog name.</p>
-- MAGIC         <script>function copyD2Help(){const e=document.getElementById("copy-block-d2-help");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbD2H(t)):fbD2H(t)}function fbD2H(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <style>
-- MAGIC details summary span:first-child {
-- MAGIC   transition: transform 0.2s ease;
-- MAGIC   display: inline-block;
-- MAGIC }
-- MAGIC details[open] summary span:first-child {
-- MAGIC   transform: rotate(90deg);
-- MAGIC }
-- MAGIC </style>

-- COMMAND ----------

-- Verify raw_products was populated
SELECT COUNT(*) AS row_count FROM bronze.raw_products;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     You should see approximately <strong>200 rows</strong> — one for each product in the catalog. This confirms the raw_products streaming table was created and populated correctly.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### D3. Create customer_feedback
-- MAGIC
-- MAGIC This is the most challenging one — the entire `CREATE` statement is <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark>. Write it from scratch using the D1 pattern. The volume path is `/Volumes/<YOUR_CATALOG>/data/course_data/customer_feedback`. This table contains feedback ratings (1-5), free-text comments, and the channel (email, survey, social_media, support) for each feedback submission.
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #e65100; background: #FFF3E0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#bf360c; margin-bottom:6px; font-size: 1.1em;">Set the Default Catalog</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     In the SQL Editor, make sure your <strong>default catalog</strong> is set to your assigned catalog before running the statement.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <button onclick="copyD3()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-d3" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>DROP TABLE IF EXISTS bronze.customer_feedback;
-- MAGIC
-- MAGIC CREATE OR REPLACE STREAMING TABLE bronze.customer_feedback
-- MAGIC <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark>;</code></pre>
-- MAGIC <script>function copyD3(){const e=document.getElementById("copy-block-d3");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbD3(t)):fbD3(t)}function fbD3(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC
-- MAGIC <p style="margin-top:12px; color:#555;"><strong>After completing the FILL_IN and running in the SQL Editor, return to this notebook.</strong></p>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyD3Help()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-d3-help" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>DROP TABLE IF EXISTS bronze.customer_feedback;
-- MAGIC
-- MAGIC CREATE OR REPLACE STREAMING TABLE bronze.customer_feedback
-- MAGIC TBLPROPERTIES (
-- MAGIC   'delta.enableChangeDataFeed' = 'true',
-- MAGIC   'delta.enableRowTracking' = 'true',
-- MAGIC   'delta.enableDeletionVectors' = 'true'
-- MAGIC )
-- MAGIC AS SELECT * FROM STREAM read_files(
-- MAGIC   '/Volumes/&lt;YOUR_CATALOG&gt;/data/course_data/customer_feedback',
-- MAGIC   format =&gt; 'csv',
-- MAGIC   header =&gt; 'true',
-- MAGIC   inferColumnTypes =&gt; 'true',
-- MAGIC   rescuedDataColumn =&gt; '_rescued_data',
-- MAGIC   schemaEvolutionMode =&gt; 'rescue',
-- MAGIC   partitionColumns =&gt; ''
-- MAGIC );</code></pre>
-- MAGIC         <p style="margin-top:8px; color:#555;"><strong>Note:</strong> Replace <code>&lt;your_catalog&gt;</code> with your actual catalog name.</p>
-- MAGIC         <script>function copyD3Help(){const e=document.getElementById("copy-block-d3-help");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbD3H(t)):fbD3H(t)}function fbD3H(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <style>
-- MAGIC details summary span:first-child {
-- MAGIC   transition: transform 0.2s ease;
-- MAGIC   display: inline-block;
-- MAGIC }
-- MAGIC details[open] summary span:first-child {
-- MAGIC   transform: rotate(90deg);
-- MAGIC }
-- MAGIC </style>

-- COMMAND ----------

-- Verify customer_feedback was populated
SELECT COUNT(*) AS row_count, ROUND(AVG(rating), 2) AS avg_rating FROM bronze.customer_feedback;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     You should see approximately <strong>500 rows</strong> with an average rating around 3.9–4.0. This confirms all three bronze streaming tables are ready for the silver layer.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Create Silver Materialized View
-- MAGIC
-- MAGIC The silver layer transforms raw data into clean, enriched datasets by joining multiple sources. Unlike streaming tables (which just ingest), materialized views apply **business logic** — joins, type casting, null handling — and the engine maintains them incrementally.
-- MAGIC
-- MAGIC You will create one materialized view that enriches customer feedback by joining it with order details, product information, and customer demographics.
-- MAGIC
-- MAGIC **This CREATE statement must be run in the SQL Editor** — not in this notebook. Copy the SQL below, complete the FILL_IN placeholders, and run it in the SQL Editor.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### E1. Create customer_feedback_enriched
-- MAGIC
-- MAGIC This materialized view joins four tables to create a rich feedback record:
-- MAGIC
-- MAGIC | Source Table | Join Key | What It Adds |
-- MAGIC |---|---|---|
-- MAGIC | `bronze.customer_feedback` (f) | — | Feedback ID, rating, text, channel |
-- MAGIC | `bronze.raw_orders` (o) | `f.order_id = o.order_id` | Order date, status, value |
-- MAGIC | `bronze.raw_products` (p) | `o.product_id = p.product_id` | Product name, category, brand |
-- MAGIC | `data.raw_customers` (c) | `f.customer_id = c.customer_id` | Customer name, segment |
-- MAGIC
-- MAGIC The first three JOINs are provided. You need to fill in:
-- MAGIC 1. The `customer_name` expression — concatenate `first_name` and `last_name` with `COALESCE` for null handling
-- MAGIC 2. The `customer_segment` column
-- MAGIC 3. The fourth JOIN to `data.raw_customers`
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #e65100; background: #FFF3E0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#bf360c; margin-bottom:6px; font-size: 1.1em;">Set the Default Catalog</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     In the SQL Editor, make sure your <strong>default catalog</strong> is set to your assigned catalog before running the statement.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <button onclick="copyE1()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-e1" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>CREATE OR REFRESH MATERIALIZED VIEW silver.customer_feedback_enriched
-- MAGIC AS SELECT
-- MAGIC   f.feedback_id,
-- MAGIC   f.customer_id,
-- MAGIC   f.order_id,
-- MAGIC   f.feedback_date,
-- MAGIC   f.rating,
-- MAGIC   f.feedback_text,
-- MAGIC   f.channel,
-- MAGIC   <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark> AS customer_name,
-- MAGIC   <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark> AS customer_segment,
-- MAGIC   o.order_date,
-- MAGIC   o.order_status,
-- MAGIC   CAST(o.quantity * o.unit_price AS DECIMAL(12,2)) AS order_value,
-- MAGIC   p.product_name,
-- MAGIC   p.category AS product_category,
-- MAGIC   p.brand AS product_brand
-- MAGIC FROM bronze.customer_feedback f
-- MAGIC LEFT JOIN bronze.raw_orders o
-- MAGIC   ON f.order_id = o.order_id
-- MAGIC LEFT JOIN bronze.raw_products p
-- MAGIC   ON o.product_id = p.product_id
-- MAGIC LEFT JOIN <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark>
-- MAGIC   ON <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark>;</code></pre>
-- MAGIC <script>function copyE1(){const e=document.getElementById("copy-block-e1");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbE1(t)):fbE1(t)}function fbE1(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC
-- MAGIC <p style="margin-top:12px; color:#555;"><strong>After completing the FILL_IN placeholders and running in the SQL Editor, return to this notebook.</strong></p>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyE1Help()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-e1-help" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>CREATE OR REFRESH MATERIALIZED VIEW silver.customer_feedback_enriched
-- MAGIC AS SELECT
-- MAGIC   f.feedback_id,
-- MAGIC   f.customer_id,
-- MAGIC   f.order_id,
-- MAGIC   f.feedback_date,
-- MAGIC   f.rating,
-- MAGIC   f.feedback_text,
-- MAGIC   f.channel,
-- MAGIC   COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS customer_name,
-- MAGIC   c.customer_segment,
-- MAGIC   o.order_date,
-- MAGIC   o.order_status,
-- MAGIC   CAST(o.quantity * o.unit_price AS DECIMAL(12,2)) AS order_value,
-- MAGIC   p.product_name,
-- MAGIC   p.category AS product_category,
-- MAGIC   p.brand AS product_brand
-- MAGIC FROM bronze.customer_feedback f
-- MAGIC LEFT JOIN bronze.raw_orders o
-- MAGIC   ON f.order_id = o.order_id
-- MAGIC LEFT JOIN bronze.raw_products p
-- MAGIC   ON o.product_id = p.product_id
-- MAGIC LEFT JOIN data.raw_customers c
-- MAGIC   ON f.customer_id = c.customer_id;</code></pre>
-- MAGIC         <script>function copyE1Help(){const e=document.getElementById("copy-block-e1-help");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbE1H(t)):fbE1H(t)}function fbE1H(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <style>
-- MAGIC details summary span:first-child {
-- MAGIC   transition: transform 0.2s ease;
-- MAGIC   display: inline-block;
-- MAGIC }
-- MAGIC details[open] summary span:first-child {
-- MAGIC   transform: rotate(90deg);
-- MAGIC }
-- MAGIC </style>

-- COMMAND ----------

-- Verify customer_feedback_enriched — check the join brought in customer and product data
SELECT feedback_id, customer_name, customer_segment, product_category, rating, order_value
FROM silver.customer_feedback_enriched
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Verify that <strong>customer_name</strong> and <strong>customer_segment</strong> are populated (not NULL) — this confirms the join to raw_customers worked. Similarly, <strong>product_category</strong> should be filled in from the products join. Any NULLs would indicate a join key mismatch.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Create Gold Materialized View
-- MAGIC
-- MAGIC The gold layer produces **business-ready aggregations** that analysts and dashboards consume directly. Unlike silver (which cleans and enriches), gold **summarizes** — turning thousands of individual feedback records into actionable metrics.
-- MAGIC
-- MAGIC This view answers the question: *"Which product categories have the happiest (or unhappiest) customers?"* It reads from `silver.customer_feedback_enriched` (the MV you just created) and groups by `product_category`.
-- MAGIC
-- MAGIC You need to fill in three `<FILL_IN>` placeholders — the first and third are `f.product_category` (the SELECT column and the GROUP BY column). The second wraps the average rating: `ROUND(AVG(f.rating), 2)`.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fffbeb; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Copy to SQL Editor</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Complete the three <code>FILL_IN</code> placeholders below, then copy to the <strong>SQL Editor</strong> and run:</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #e65100; background: #FFF3E0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#bf360c; margin-bottom:6px; font-size: 1.1em;">Set the Default Catalog</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     In the SQL Editor, make sure your <strong>default catalog</strong> is set to your assigned catalog before running the statement.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <button onclick="copyF1Main()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-f1-main" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>CREATE OR REFRESH MATERIALIZED VIEW gold.feedback_by_category
-- MAGIC AS SELECT
-- MAGIC   <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark> AS product_category,
-- MAGIC   <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark> AS avg_rating,
-- MAGIC   COUNT(*) AS feedback_count,
-- MAGIC   ROUND(AVG(f.order_value), 2) AS avg_order_value
-- MAGIC FROM silver.customer_feedback_enriched f
-- MAGIC WHERE f.product_category IS NOT NULL
-- MAGIC GROUP BY <mark style="background:#FEF3C7;color:#000;padding:1px 3px;">FILL_IN</mark>;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyF1Main() {
-- MAGIC   var el = document.getElementById("copy-block-f1-main");
-- MAGIC   if (!el) return;
-- MAGIC   var text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text).then(function(){ alert("Copied to clipboard"); }).catch(function(){ alert("Copy failed"); });
-- MAGIC   } else { alert("Copy not supported — select and copy manually."); }
-- MAGIC }
-- MAGIC </script>
-- MAGIC <p style="margin-top:8px; color:#555;">After completing the FILL_IN placeholders and running in the SQL Editor, return to this notebook to verify.</p>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyF1()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-f1" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>CREATE OR REFRESH MATERIALIZED VIEW gold.feedback_by_category
-- MAGIC AS SELECT
-- MAGIC   f.product_category,
-- MAGIC   ROUND(AVG(f.rating), 2) AS avg_rating,
-- MAGIC   COUNT(*) AS feedback_count,
-- MAGIC   ROUND(AVG(f.order_value), 2) AS avg_order_value
-- MAGIC FROM silver.customer_feedback_enriched f
-- MAGIC WHERE f.product_category IS NOT NULL
-- MAGIC GROUP BY f.product_category</code></pre>
-- MAGIC         <script>function copyF1(){const e=document.getElementById("copy-block-f1");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbF1(t)):fbF1(t)}function fbF1(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <style>
-- MAGIC details summary span:first-child {
-- MAGIC   transition: transform 0.2s ease;
-- MAGIC   display: inline-block;
-- MAGIC }
-- MAGIC details[open] summary span:first-child {
-- MAGIC   transform: rotate(90deg);
-- MAGIC }
-- MAGIC </style>

-- COMMAND ----------

-- Verify the gold output
SELECT * FROM gold.feedback_by_category
ORDER BY feedback_count DESC;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     You should see one row per product category with average ratings, feedback counts, and average order values. This is the business-ready aggregation that dashboards and analysts consume directly.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Incremental Refresh
-- MAGIC
-- MAGIC The real power of streaming tables and materialized views is **incremental processing** — each refresh handles only the new/changed data, not the full dataset. Let's prove this works by inserting new rows and watching the counts change at each layer.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### G1. Check Current Row Counts
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Run and Record Values</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Run the cell below and <strong>note the numbers</strong> — you will compare them after inserting new data.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

SELECT 'customer_feedback (bronze)' AS layer, COUNT(*) AS rows FROM bronze.customer_feedback
UNION ALL
SELECT 'customer_feedback_enriched (silver)', COUNT(*) FROM silver.customer_feedback_enriched
UNION ALL
SELECT 'feedback_by_category (gold)', COUNT(*) FROM gold.feedback_by_category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G2. Insert New Feedback Data
-- MAGIC
-- MAGIC We insert 3 new feedback records directly into the bronze streaming table. These records will flow through the pipeline when we refresh in G3.

-- COMMAND ----------

-- Insert 3 new feedback records directly into the bronze streaming table
INSERT INTO bronze.customer_feedback (feedback_id, customer_id, order_id, feedback_date, rating, feedback_text, channel)
VALUES
  (9901, 1, 100, '2026-04-10', 5, 'Exceptional quality! Will buy again.', 'email'),
  (9902, 2, 200, '2026-04-10', 1, 'Product arrived damaged. Very disappointed.', 'support'),
  (9903, 3, 300, '2026-04-10', 4, 'Good value for the price.', 'survey');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G3. Refresh and Verify
-- MAGIC
-- MAGIC Refresh the silver and gold MVs to pick up the new bronze data. The bronze streaming table already has the 3 new rows (we inserted directly). The silver MV will detect the new rows and process only those 3. The gold MV will recompute its aggregations with the updated silver data.
-- MAGIC
-- MAGIC After the refresh, run the count query and compare with G1.

-- COMMAND ----------

REFRESH MATERIALIZED VIEW silver.customer_feedback_enriched;

-- COMMAND ----------

REFRESH MATERIALIZED VIEW gold.feedback_by_category;

-- COMMAND ----------

-- Compare these counts with G1 — bronze and silver should each have 3 more rows
SELECT 'customer_feedback (bronze)' AS layer, COUNT(*) AS rows FROM bronze.customer_feedback
UNION ALL
SELECT 'customer_feedback_enriched (silver)', COUNT(*) FROM silver.customer_feedback_enriched
UNION ALL
SELECT 'feedback_by_category (gold)', COUNT(*) FROM gold.feedback_by_category;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Bronze:</strong> Row count increased by 3 (the INSERT went directly into the streaming table)</li>
-- MAGIC       <li><strong>Silver:</strong> Row count increased by 3 — the MV processed only the new rows incrementally</li>
-- MAGIC       <li><strong>Gold:</strong> Row count stayed the same (it is an aggregation — the same categories, but with updated averages)</li>
-- MAGIC     </ul>
-- MAGIC     <p>This is incremental processing in action: each layer processes only the delta, not the full dataset. In production with millions of rows, this saves significant compute time and cost.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Key Summary and Takeaways
-- MAGIC
-- MAGIC In this lab, you built a complete **bronze → silver → gold** pipeline:
-- MAGIC
-- MAGIC | What You Did | Key Concept |
-- MAGIC |---|---|
-- MAGIC | Ran EXPLAIN with a non-incrementalizable window function, then fixed it with `PARTITION BY` | **Always run EXPLAIN before building** — certain SQL patterns silently prevent incremental refresh |
-- MAGIC | Created 3 bronze streaming tables with Auto Loader | **`read_files()` + `TBLPROPERTIES`** enable incremental ingestion with CDF, row tracking, and deletion vectors |
-- MAGIC | Built a silver MV joining feedback + orders + products + customers | **Materialized views apply business logic** (joins, casting, null handling) and refresh incrementally |
-- MAGIC | Built a gold MV aggregating by product category | **Gold summarizes silver** into metrics that dashboards and analysts consume directly |
-- MAGIC | Inserted new data, refreshed, and verified only the delta was processed | **Incremental refresh** at every layer — bronze ingests new files, silver processes new rows, gold recomputes affected aggregations |
-- MAGIC
-- MAGIC **What comes next:** In Demo 3.2, you will see how **AUTO CDC** replaces the static `data.raw_customers` table with an always-current customer dimension — built from a stream of INSERT, UPDATE, and DELETE events.

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
