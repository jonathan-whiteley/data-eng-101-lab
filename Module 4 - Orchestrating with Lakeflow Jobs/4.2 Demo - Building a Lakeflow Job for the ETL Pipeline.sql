-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](../Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4.2 Demo - Building a Lakeflow Job for the ETL Pipeline
-- MAGIC ## Overview
-- MAGIC In this demo, we will wire together the streaming tables, materialized views, and AUTO CDC tables built in previous demos into an end-to-end **Lakeflow Job**. You will create SQL files that refresh each pipeline layer, assemble those files into a diamond-pattern DAG with parallel branches, configure job parameters and scheduling, run the job, and use **Repair Run** to recover from failures efficiently.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC - Create SQL files that refresh bronze, silver, gold, and CDC pipeline layers
-- MAGIC - Build a Lakeflow Job with SQL File tasks wired in a diamond-pattern DAG
-- MAGIC - Configure job parameters (`{{catalog}}`) as dynamic value references
-- MAGIC - Run the job and interpret Graph, Timeline, and List views
-- MAGIC - Add a schedule with triggers
-- MAGIC - Use Repair Run to re-execute only the failed task after fixing an error

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
-- MAGIC     Run the following cell to configure your working environment for this demo.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.2-Demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Prepare SQL Files for the Pipeline
-- MAGIC
-- MAGIC A Lakeflow Job executes **SQL File tasks** — each task points to a `.sql` file in your workspace. We need four files that refresh the streaming tables and materialized views built in previous demos:
-- MAGIC
-- MAGIC | File | Layer | Objects | Source Demo |
-- MAGIC |------|-------|---------|-------------|
-- MAGIC | `demo_refresh_bronze.sql` | Bronze | `bronze.raw_orders`, `bronze.raw_products` | Demo 2.2 |
-- MAGIC | `demo_refresh_silver.sql` | Silver | `silver.orders_enriched` | Demo 2.2 |
-- MAGIC | `demo_refresh_gold.sql` | Gold | `gold.revenue_by_category`, `gold.daily_sales_summary` | Created in Section B3 below |
-- MAGIC | `demo_refresh_silver_cdc.sql` | CDC | `demo_cdc.demo_cdc_customers_current`, `demo_cdc.demo_cdc_customers_history` | Demo 3.2 |
-- MAGIC | `demo_audit_log.sql` | Audit | Logs run metadata and task output to an audit record | Pre-populated by setup |
-- MAGIC
-- MAGIC The `{{catalog}}` placeholder in each file is a **job parameter** — we will configure it when creating the job so it resolves to your catalog at runtime.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">Where to Create SQL Files</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     In the Databricks workspace sidebar, navigate to your project folder (e.g., <code>/Workspace/Users/your_email/Building-ETL-SQL/Module 4 - Orchestrating with Lakeflow Jobs/</code>). Open the <code>pipeline_sql</code> subfolder — the classroom setup created it with empty placeholder files:
-- MAGIC     <ul>
-- MAGIC       <li><code>pipeline_sql/demo_refresh_bronze.sql</code> — <strong>empty, you will fill it in B1</strong></li>
-- MAGIC       <li><code>pipeline_sql/demo_refresh_silver.sql</code> — pre-populated</li>
-- MAGIC       <li><code>pipeline_sql/demo_refresh_silver_cdc.sql</code> — pre-populated</li>
-- MAGIC       <li><code>pipeline_sql/demo_refresh_gold.sql</code> — <strong>does not exist yet, you will create it in B3</strong></li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Create demo_refresh_bronze.sql
-- MAGIC
-- MAGIC The bronze file is intentionally empty — let's fill it in. These are the two streaming tables created in Demo 2.2 using `read_files()` with Auto Loader: `raw_orders` and `raw_products` in the `bronze` schema.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">Code to Copy — demo_refresh_bronze.sql</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Open <code>pipeline_sql/demo_refresh_bronze.sql</code> and paste the following:</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC <button onclick="copyBronze()" style="background:#1B3139; color:#fff; border:none; padding:6px 14px; border-radius:6px; font-size:0.85em; cursor:pointer; margin-bottom:6px;">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-bronze" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code><span style="color:#6A8F9F">-- demo_refresh_bronze.sql</span>
-- MAGIC <span style="color:#6A8F9F">-- Refresh all bronze-layer streaming tables</span>
-- MAGIC <span style="color:#6A8F9F">-- These were created in Demo 2.2 using read_files() with Auto Loader</span>
-- MAGIC <span style="color:#56B6C2">REFRESH STREAMING TABLE</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.bronze.raw_orders'</span>);
-- MAGIC <span style="color:#56B6C2">REFRESH STREAMING TABLE</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.bronze.raw_products'</span>);
-- MAGIC <span style="color:#6A8F9F">-- Verify bronze refresh completed</span>
-- MAGIC <span style="color:#56B6C2">SELECT</span> <span style="color:#98C379">'Bronze refresh complete'</span> <span style="color:#56B6C2">AS</span> status,
-- MAGIC        <span style="color:#E5C07B">current_timestamp</span>() <span style="color:#56B6C2">AS</span> completed_at;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBronze() {
-- MAGIC   const el = document.getElementById("copy-block-bronze");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopyBronze(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopyBronze(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopyBronze(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Connection to Lecture 4.1 — Dynamic Value References</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Notice the <code>{{catalog}}</code> in every REFRESH statement. This is a <strong>dynamic value reference</strong> from Lecture 4.1 — specifically a <strong>job parameter</strong> (shorthand for <code>{{job.parameters.catalog}}</code>). When the job runs, it resolves to the actual catalog name you configured.</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Review the Silver Files
-- MAGIC
-- MAGIC The two silver-layer SQL files are already pre-populated. Let's review what they contain.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">demo_refresh_silver.sql — orders_enriched</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>This file refreshes the <code>orders_enriched</code> materialized view in the <code>silver</code> schema — the cleaned and enriched join of orders, customers, and products we built in Demo 2.2:</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC <pre id="preview-silver" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code><span style="color:#6A8F9F">-- demo_refresh_silver.sql</span>
-- MAGIC <span style="color:#6A8F9F">-- Refresh the silver-layer materialized view</span>
-- MAGIC <span style="color:#6A8F9F">-- orders_enriched joins orders, customers, and products (built in Demo 2.2)</span>
-- MAGIC <span style="color:#56B6C2">REFRESH MATERIALIZED VIEW</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.silver.orders_enriched'</span>);
-- MAGIC <span style="color:#6A8F9F">-- Verify silver refresh completed</span>
-- MAGIC <span style="color:#56B6C2">SELECT</span> <span style="color:#98C379">'Silver refresh complete'</span> <span style="color:#56B6C2">AS</span> status,
-- MAGIC        <span style="color:#E5C07B">current_timestamp</span>() <span style="color:#56B6C2">AS</span> completed_at;
-- MAGIC </code></pre>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">demo_refresh_silver_cdc.sql — AUTO CDC Dimensions</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>This file refreshes the AUTO CDC streaming tables from Demo 3.2 — the SCD Type 1 (current state) and SCD Type 2 (full history) dimension tables that track customer changes over time. Open <code>pipeline_sql/demo_refresh_silver_cdc.sql</code> in the workspace sidebar and review its contents:</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC <pre id="preview-cdc" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code><span style="color:#6A8F9F">-- demo_refresh_silver_cdc.sql</span>
-- MAGIC <span style="color:#6A8F9F">-- Refresh the AUTO CDC streaming tables from Demo 3.2</span>
-- MAGIC <span style="color:#6A8F9F">-- SCD Type 1 (current state) and SCD Type 2 (full history)</span>
-- MAGIC <span style="color:#56B6C2">REFRESH STREAMING TABLE</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.demo_cdc.demo_cdc_customers_current'</span>);
-- MAGIC <span style="color:#56B6C2">REFRESH STREAMING TABLE</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.demo_cdc.demo_cdc_customers_history'</span>);
-- MAGIC <span style="color:#6A8F9F">-- Verify CDC refresh completed</span>
-- MAGIC <span style="color:#56B6C2">SELECT</span> <span style="color:#98C379">'CDC refresh complete'</span> <span style="color:#56B6C2">AS</span> status,
-- MAGIC        <span style="color:#E5C07B">current_timestamp</span>() <span style="color:#56B6C2">AS</span> completed_at;
-- MAGIC </code></pre>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Create demo_refresh_gold.sql
-- MAGIC
-- MAGIC Notice that the `pipeline_sql` folder has no gold file — the bronze and silver layers are covered, but the final step that refreshes business-ready aggregations is missing. Let's create it.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Create the Gold SQL File</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the workspace sidebar, navigate to the <code>pipeline_sql</code> folder</li>
-- MAGIC       <li>Click the <strong>kebab menu</strong> (&#x22EE;) next to the folder name</li>
-- MAGIC       <li>Select <strong>Create &gt; SQL file</strong></li>
-- MAGIC       <li>Name the file: <strong><code>demo_refresh_gold.sql</code></strong></li>
-- MAGIC       <li>Copy the code below and paste it into the new file</li>
-- MAGIC       <li><strong>Save</strong> the file</li>
-- MAGIC     </ol>
-- MAGIC     <p>This file refreshes the two gold-layer materialized views — <code>revenue_by_category</code> and <code>daily_sales_summary</code> — that aggregate from <code>silver.orders_enriched</code>.</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC <button onclick="copyGold()" style="background:#1B3139; color:#fff; border:none; padding:6px 14px; border-radius:6px; font-size:0.85em; cursor:pointer; margin-bottom:6px;">Copy to clipboard</button>
-- MAGIC <pre id="copy-block-gold" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code><span style="color:#6A8F9F">-- demo_refresh_gold.sql</span>
-- MAGIC <span style="color:#6A8F9F">-- Refresh all gold-layer materialized views</span>
-- MAGIC <span style="color:#6A8F9F">-- These aggregate from silver.orders_enriched</span>
-- MAGIC <span style="color:#56B6C2">REFRESH MATERIALIZED VIEW</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.gold.revenue_by_category'</span>);
-- MAGIC <span style="color:#56B6C2">REFRESH MATERIALIZED VIEW</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.gold.daily_sales_summary'</span>);
-- MAGIC <span style="color:#6A8F9F">-- Verify gold refresh completed</span>
-- MAGIC <span style="color:#56B6C2">SELECT</span> <span style="color:#98C379">'Gold refresh complete'</span> <span style="color:#56B6C2">AS</span> status,
-- MAGIC        <span style="color:#E5C07B">current_timestamp</span>() <span style="color:#56B6C2">AS</span> completed_at;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyGold() {
-- MAGIC   const el = document.getElementById("copy-block-gold");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopyGold(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopyGold(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopyGold(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Create the Lakeflow Job (UI Walkthrough)
-- MAGIC
-- MAGIC Now we will create a **Lakeflow Job** that orchestrates these four SQL files as a DAG. Unlike a simple linear chain, this job has a **diamond pattern** — silver and CDC run in parallel after bronze completes, then reconverge at gold. This section is a UI walkthrough — follow the instructor's screen.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Navigate to Jobs & Pipelines

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">C1. Navigate to Jobs &amp; Pipelines</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the left sidebar, right-click <strong>Jobs &amp; Pipelines</strong> and open in a new tab</li>
-- MAGIC       <li>Click <strong>Create</strong>, then select <strong>Job</strong></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Name the Job

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">C2. Name the Job</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click the job name at the top (defaults to "New Job")</li>
-- MAGIC       <li>Rename it to: <strong>Sales ETL Pipeline</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p>This is your orchestration job — it will run all four pipeline tasks.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Add Task 1: refresh_bronze

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">C3. Add Task 1: refresh_bronze</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the <strong>Tasks</strong> tab, you will see <em>"Add your first task"</em> — the default task type is Notebook. Click the task type dropdown and select <strong>SQL File</strong> (not SQL Query)</li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_bronze</code></li>
-- MAGIC       <li>Click the <strong>Path</strong> field and navigate to <code>pipeline_sql/demo_refresh_bronze.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to your serverless warehouse</li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C4. Add Task 2: refresh_silver

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">C4. Add Task 2: refresh_silver (depends on refresh_bronze)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Add task</strong> below the existing task</li>
-- MAGIC       <li>Select <strong>SQL File</strong> (not SQL Query)</li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_silver</code></li>
-- MAGIC       <li>Click the <strong>Path</strong> field and navigate to <code>pipeline_sql/demo_refresh_silver.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same serverless warehouse</li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <code>refresh_bronze</code></li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p><strong>Key point:</strong> This "Depends on" setting creates the DAG edge. Silver will not start until bronze succeeds.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C5. Add Task 3: refresh_silver_cdc

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">C5. Add Task 3: refresh_silver_cdc (depends on refresh_bronze)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Add task</strong></li>
-- MAGIC       <li>Select <strong>SQL File</strong> (not SQL Query)</li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_silver_cdc</code></li>
-- MAGIC       <li>Click the <strong>Path</strong> field and navigate to <code>pipeline_sql/demo_refresh_silver_cdc.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same serverless warehouse</li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <code>refresh_bronze</code></li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p><strong>Key point:</strong> This task depends on bronze — not silver. That means <code>refresh_silver</code> and <code>refresh_silver_cdc</code> will run <strong>in parallel</strong> once bronze completes. This is our first branching DAG.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C6. Add Task 4: refresh_gold

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">C6. Add Task 4: refresh_gold (depends on refresh_silver AND refresh_silver_cdc)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Add task</strong></li>
-- MAGIC       <li>Select <strong>SQL File</strong> (not SQL Query)</li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_gold</code></li>
-- MAGIC       <li>Click the <strong>Path</strong> field and navigate to <code>pipeline_sql/demo_refresh_gold.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same serverless warehouse</li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <strong>both</strong> <code>refresh_silver</code> and <code>refresh_silver_cdc</code></li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p><strong>Key point:</strong> Gold depends on <strong>both</strong> silver tasks. It will not start until both <code>refresh_silver</code> and <code>refresh_silver_cdc</code> have completed successfully. This is a <strong>fan-in</strong> pattern — multiple parallel branches converge before the final layer runs.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">DAG Visualization</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     You should now see a four-node DAG in the <strong>Tasks</strong> tab with a diamond pattern:
-- MAGIC     <pre style="background: #fff; border: 1px solid #ccc; padding: 12px; border-radius: 4px; font-family: monospace; margin: 12px 0;">
-- MAGIC                     ┌──▶  refresh_silver      ──┐
-- MAGIC   refresh_bronze  ──┤                            ├──▶  refresh_gold
-- MAGIC                     └──▶  refresh_silver_cdc  ──┘
-- MAGIC     </pre>
-- MAGIC     This is the <strong>diamond pattern</strong> from Lecture 4.1 — bronze fans out to two parallel silver tasks, which both fan in to gold. In Timeline view, you will see the silver and CDC bars overlapping — that is real parallelism saving wall-clock time. Gold starts only after both complete.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C7. Add the Job Parameter

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">C7. Add the Job Parameter for {{catalog}}</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>All four SQL files use <code>{{catalog}}</code> as a placeholder. We need to define this as a <strong>job parameter</strong> so it resolves at runtime.</p>
-- MAGIC     <ol>
-- MAGIC       <li>On the right side of the job page, scroll down to <strong>Job parameters</strong></li>
-- MAGIC       <li>Click <strong>Edit parameters</strong></li>
-- MAGIC       <li>In the <strong>Key</strong> field, enter: <code>catalog</code></li>
-- MAGIC       <li>In the <strong>Value</strong> field, paste your catalog name — copy it from the cell below</li>
-- MAGIC       <li>Click <strong>Save</strong></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Your catalog name** — copy this value into the job parameter:

-- COMMAND ----------

SELECT my_catalog AS your_catalog_name;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <strong>Copy this catalog name exactly as shown</strong> — you will paste it into the job parameter value in the next step. A typo here will cause every task in the job to fail.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Run and Monitor
-- MAGIC
-- MAGIC With the job created, let's trigger a run and explore the monitoring views that Lakeflow Jobs provides.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Run the Job

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D1. Run the Job</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Run Now</strong></li>
-- MAGIC       <li>Click the <strong>Runs</strong> tab — this is where all job runs are listed as a matrix of historical runs. Select the current run to drill in.</li>
-- MAGIC       <li>You will see the <strong>Graph</strong> view by default — watch the nodes transition through states:
-- MAGIC         <ul>
-- MAGIC           <li><strong>Queued</strong> — Waiting to be scheduled (gray)</li>
-- MAGIC           <li><strong>Running</strong> — Actively executing (green spinner)</li>
-- MAGIC           <li><strong>Succeeded</strong> — Completed successfully (green checkmark)</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC     </ol>
-- MAGIC     <p>Graph view gives you a real-time picture of which tasks are running, which are waiting, and which have finished. Notice how refresh_silver and refresh_silver_cdc run in parallel once bronze completes — this is the diamond pattern in action.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Explore the Run Views

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D2. Explore the Run Views</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>While the run is active (or after it completes), click through the view tabs at the top:</p>
-- MAGIC     <ul>
-- MAGIC       <li><strong>Graph</strong> — DAG with real-time task status (your default view)</li>
-- MAGIC       <li><strong>Timeline</strong> — Horizontal bars showing each task's start time, duration, and parallelism. Look for the overlapping bars where silver and CDC ran simultaneously.</li>
-- MAGIC       <li><strong>List</strong> — Tabular view of all tasks with status, duration, and start/end times</li>
-- MAGIC     </ul>
-- MAGIC     <p>Timeline is your performance analysis tool. You can see exactly how long each task took and whether tasks ran in parallel. The overlapping bars for refresh_silver and refresh_silver_cdc confirm that our fan-out DAG is saving wall-clock time compared to a linear chain.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D3. Add a Schedule (then Pause)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D3. Add a Schedule (then Pause)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In production, pipelines run on a schedule. Let's configure one, then immediately pause it so it does not trigger during class.</p>
-- MAGIC     <ol>
-- MAGIC       <li>On the right side of the job page, find <strong>Schedules &amp; Triggers</strong></li>
-- MAGIC       <li>Click <strong>Add Trigger</strong></li>
-- MAGIC       <li>For <strong>Trigger type</strong>, select <strong>Scheduled</strong></li>
-- MAGIC       <li>Select <strong>Schedule</strong> (not Interval)</li>
-- MAGIC       <li>Set the schedule to <strong>Every Day</strong> at <strong>6:00 AM</strong>, timezone <strong>America/New_York</strong></li>
-- MAGIC       <li>Click <strong>Save</strong></li>
-- MAGIC       <li>You will now see <strong>Edit Trigger</strong>, <strong>Pause</strong>, and <strong>Delete</strong> options &mdash; click <strong>Pause</strong> so the schedule does not trigger during class</li>
-- MAGIC     </ol>
-- MAGIC     <p>In production, this job would run every morning before business hours, ensuring fresh data in the gold layer for dashboards and reports. Lakeflow Jobs also supports interval-based triggers, file arrival triggers, and table update triggers (covered in Lecture 4.1).</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Incremental Ingestion — Add New Data and Re-Run
-- MAGIC
-- MAGIC The first run processed all existing data. Now let's simulate what happens in production when **new data arrives**. We will add a new batch of order files to the volume, then re-run the job and verify that only the new rows flow through the pipeline — bronze ingests just the new files, silver recomputes only the affected rows, and gold updates its aggregations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E1. Capture Baseline Counts

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">E1. Capture Baseline Counts</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Before adding new data, let's record the current row counts so we can compare after the incremental run.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

SELECT 'bronze.raw_orders' AS layer, COUNT(*) AS row_count
FROM IDENTIFIER(my_catalog || '.bronze.raw_orders')
UNION ALL
SELECT 'silver.orders_enriched', COUNT(*)
FROM IDENTIFIER(my_catalog || '.silver.orders_enriched')
UNION ALL
SELECT 'gold.revenue_by_category', COUNT(*)
FROM IDENTIFIER(my_catalog || '.gold.revenue_by_category')
UNION ALL
SELECT 'gold.daily_sales_summary', COUNT(*)
FROM IDENTIFIER(my_catalog || '.gold.daily_sales_summary');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Note these numbers — <strong>bronze.raw_orders</strong> should have approximately <strong>15,000 rows</strong> and <strong>silver.orders_enriched</strong> should be approximately the same (the exact count may differ slightly depending on join results). You will compare these after the incremental run.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E2. Add a New Batch of Orders to the Volume

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">E2. Add a New Batch of Orders to the Volume</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In production, new files land in cloud storage continuously. We simulate this by writing a new CSV batch into the same volume directory that our bronze streaming table monitors.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

EXECUTE IMMEDIATE
  CONCAT(
    'INSERT OVERWRITE DIRECTORY \'/Volumes/' || my_catalog || '/data/course_data/raw_orders/raw_orders_new_batch\'
     USING CSV OPTIONS (\'header\' \'true\')
     SELECT * FROM ' || my_catalog || '.data.raw_orders_new_batch'
  );

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What happened</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     A new CSV file containing approximately <strong>2,000 new order records</strong> was written to the volume. The streaming table does not know about it yet — it will only pick it up when the job refreshes bronze.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E3. Re-Run the Job

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">E3. Re-Run the Job</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Go back to your <strong>Sales ETL Pipeline</strong> job</li>
-- MAGIC       <li>Click <strong>Run Now</strong></li>
-- MAGIC       <li>Watch the run complete — bronze picks up the new file, silver recomputes affected rows, gold updates its aggregations</li>
-- MAGIC     </ol>
-- MAGIC     <p>This is exactly how production pipelines work: new data lands → scheduled job triggers → each layer processes only the delta.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E4. Verify Incremental Processing

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">E4. Verify Incremental Processing</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Once the job completes, run the cells below to confirm the new data flowed through every layer.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

SELECT 'bronze.raw_orders' AS layer, COUNT(*) AS row_count
FROM IDENTIFIER(my_catalog || '.bronze.raw_orders')
UNION ALL
SELECT 'silver.orders_enriched', COUNT(*)
FROM IDENTIFIER(my_catalog || '.silver.orders_enriched')
UNION ALL
SELECT 'gold.revenue_by_category', COUNT(*)
FROM IDENTIFIER(my_catalog || '.gold.revenue_by_category')
UNION ALL
SELECT 'gold.daily_sales_summary', COUNT(*)
FROM IDENTIFIER(my_catalog || '.gold.daily_sales_summary');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>bronze.raw_orders</strong> increased by ~2,000 rows (from ~15,000 to ~17,000) — Auto Loader detected the new file and ingested only its contents</li>
-- MAGIC       <li><strong>silver.orders_enriched</strong> grew by the same amount — the incremental MV refresh processed only the new rows</li>
-- MAGIC       <li><strong>gold.revenue_by_category</strong> row count may stay the same (same categories) but the values changed — aggregations were recomputed with the new data</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E5. Examine Incremental Processing
-- MAGIC
-- MAGIC Let's verify that the pipeline processed only the new data — first with `DESCRIBE HISTORY` on the bronze streaming table, then by exploring the **Jobs and Pipelines** UI to see incremental refresh in action.

-- COMMAND ----------

DESCRIBE HISTORY bronze.raw_orders LIMIT 10;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Scroll through the version history. Each row represents a Delta operation. Look at the <code>operationMetrics</code> column for each version — it shows exactly how many rows were read and written in each operation. The most recent version should show the incremental batch (~2,000 rows), not a full reload of all 15,000+ rows. This is Auto Loader's file-tracking in action.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E6. Explore the Streaming Table Pipeline

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">E6. Explore the Streaming Table Pipeline</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Every streaming table is backed by a Lakeflow Spark Declarative Pipelines (SDP) pipeline. Let's explore it in the UI:</p>
-- MAGIC     <ol>
-- MAGIC       <li>In the left sidebar, click <strong>Jobs and Pipelines</strong></li>
-- MAGIC       <li>Click the <strong>Pipelines</strong> tab</li>
-- MAGIC       <li>Find the pipeline named <strong>ST-&lt;&lt;your_catalog&gt;&gt;.bronze.raw_orders</strong></li>
-- MAGIC       <li>Click the <strong>Performance</strong> tab</li>
-- MAGIC       <li>Click on the <strong>REFRESH STREAMING TABLE...</strong> entry for the most recent run</li>
-- MAGIC       <li>Look at <strong>Rows Read / Rows Written</strong> — this confirms the incremental batch size</li>
-- MAGIC       <li>Click <strong>See Query Profile</strong> to drill into the execution plan</li>
-- MAGIC     </ol>
-- MAGIC     <p>This is the same infrastructure that powers the <code>REFRESH STREAMING TABLE</code> statement — you can see exactly what happened under the hood.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E7. Verify Incremental Refresh on the Gold MV

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">E7. Verify Incremental Refresh on the Gold Materialized View</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Now let's confirm that the gold layer refreshed incrementally:</p>
-- MAGIC     <ol>
-- MAGIC       <li>Navigate back to <strong>Jobs and Pipelines &gt; Pipelines</strong></li>
-- MAGIC       <li>Find the pipeline named <strong>MV-&lt;&lt;your_catalog&gt;&gt;.gold.daily_sales_summary</strong></li>
-- MAGIC       <li>In the overview, look for the <strong>Incrementalization</strong> field — it should show <strong>Incremental</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p>This confirms the MV is using incremental refresh — it recomputes only the affected aggregation rows based on the new data that flowed through from bronze and silver, rather than scanning the entire dataset.</p>
-- MAGIC     <p style="margin-top:10px;"><strong>Key teaching point:</strong> This is the complete incremental pipeline in action through a Lakeflow Job — new files land in the volume → the job triggers → bronze ingests only new files → silver processes only new rows → gold recomputes affected aggregations. Each layer does the minimum work needed.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Add an Audit Task with Task Output Passing
-- MAGIC
-- MAGIC Let's add a 5th task to the job that demonstrates **task output passing** — reading one task's SELECT output from another task.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### F1. Add the audit_log Task

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">F1. Add the audit_log Task</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In your job, click <strong>Add task</strong></li>
-- MAGIC       <li>Select <strong>SQL File</strong></li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>audit_log</code></li>
-- MAGIC       <li>Click the <strong>Path</strong> field and navigate to <code>pipeline_sql/demo_audit_log.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same serverless warehouse</li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <code>refresh_gold</code></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### F2. Add Task Parameters — Task Output Passing

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border: 2px solid #1976d2; background: linear-gradient(135deg, #e3f2fd, #e8eaf6); padding: 18px 22px; border-radius: 8px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; font-size:1.1em; margin-bottom:10px;">F2. Add Two Task Parameters — This is Task Output Passing</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In the audit_log task config, scroll to <strong>Parameters</strong> and add these two:</p>
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:8px 0;">
-- MAGIC       <tr style="background:#0d47a1; color:#fff;"><th style="padding:8px 10px; text-align:left;">Parameter Name</th><th style="padding:8px 10px;">Value</th><th style="padding:8px 10px;">What It Does</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:8px 10px;"><code>run_id</code></td><td><code>{{job.run_id}}</code></td><td><strong>Job metadata</strong> — the engine provides the current run's unique ID</td></tr>
-- MAGIC       <tr><td style="padding:8px 10px;"><code>gold_status</code></td><td><code>{{tasks.refresh_gold.output.first_row.status}}</code></td><td><strong>Task output passing</strong> — reads the <code>status</code> column from refresh_gold's SELECT output</td></tr>
-- MAGIC     </table>
-- MAGIC     <p>Click <strong>Save task</strong>.</p>
-- MAGIC     <p style="margin-top:8px;">The SQL file uses <code>:run_id</code> and <code>:gold_status</code> to receive these resolved values. The <code>{{ }}</code> references live in the <strong>task config</strong> — the <code>:</code> prefix is how SQL files receive them. This two-step pattern (config → SQL) is how task output passing works.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### F3. Run the Job and Check the Audit Output

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">F3. Run the Job and Check the Audit Output</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Click <strong>Run Now</strong>. After the run completes, click into the <strong>audit_log</strong> task in the Runs tab to see its output:</p>
-- MAGIC     <ul>
-- MAGIC       <li><strong>run_id</strong> — the actual run ID (not literal <code>{{job.run_id}}</code>)</li>
-- MAGIC       <li><strong>gold_status</strong> — should say "Gold refresh complete" (passed from the gold task's SELECT via the <code>:gold_status</code> parameter)</li>
-- MAGIC     </ul>
-- MAGIC     <p>This proves that task output passing worked — data flowed from the gold task's SELECT output into the audit task's SQL via task parameters.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Demonstrate Repair Run
-- MAGIC
-- MAGIC One of the most powerful features of Lakeflow Jobs is **Repair Run** — the ability to re-execute only the failed tasks (and their downstream dependents) without re-running tasks that already succeeded. Let's see this in action.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G1. Intentionally Break demo_refresh_silver.sql

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">G1. Intentionally Break demo_refresh_silver.sql</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Open <code>pipeline_sql/demo_refresh_silver.sql</code> in the workspace editor</li>
-- MAGIC       <li><strong>Add</strong> the following line at the end of the file (after the existing REFRESH statement, before the verify SELECT):</li>
-- MAGIC     </ol>
-- MAGIC     <button onclick="copyError()" style="background:#1B3139; color:#fff; border:none; padding:6px 14px; border-radius:6px; font-size:0.85em; cursor:pointer; margin-bottom:6px;">Copy to clipboard</button>
-- MAGIC     <pre id="copy-block-error" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code><span style="color:#6A8F9F">-- INTENTIONAL ERROR: this view does not exist</span>
-- MAGIC <span style="color:#56B6C2">REFRESH MATERIALIZED VIEW</span> <span style="color:#E5C07B">IDENTIFIER</span>({{catalog}} || <span style="color:#98C379">'.silver.mv_this_does_not_exist'</span>);
-- MAGIC </code></pre>
-- MAGIC     <script>
-- MAGIC     function copyError() {
-- MAGIC       const el = document.getElementById("copy-block-error");
-- MAGIC       if (!el) return;
-- MAGIC       const text = el.innerText;
-- MAGIC       if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC         navigator.clipboard.writeText(text)
-- MAGIC           .then(() => alert("Copied to clipboard"))
-- MAGIC           .catch(err => { fallbackCopyError(text); });
-- MAGIC       } else { fallbackCopyError(text); }
-- MAGIC     }
-- MAGIC     function fallbackCopyError(text) {
-- MAGIC       const ta = document.createElement("textarea");
-- MAGIC       ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
-- MAGIC       document.body.appendChild(ta); ta.select();
-- MAGIC       try { document.execCommand("copy"); alert("Copied to clipboard"); }
-- MAGIC       catch(e) { alert("Copy failed. Please select and copy manually."); }
-- MAGIC       finally { document.body.removeChild(ta); }
-- MAGIC     }
-- MAGIC     </script>
-- MAGIC     <p>Save the file. The existing REFRESH statement is untouched — we just added a bad one. The error will occur approximately 2 minutes into the run (timing varies depending on warehouse load) after bronze and the valid silver refresh complete.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G2. Run the Job — Watch Silver Fail

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">G2. Run the Job — Watch Silver Fail</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Go back to the <strong>Sales ETL Pipeline</strong> job</li>
-- MAGIC       <li>Click <strong>Run Now</strong>, then go to the <strong>Runs</strong> tab and select the active run</li>
-- MAGIC       <li>Watch the Graph view:
-- MAGIC         <ul>
-- MAGIC           <li><code>refresh_bronze</code> — <strong style="color: green;">Succeeds</strong></li>
-- MAGIC           <li><code>refresh_silver_cdc</code> — <strong style="color: green;">Succeeds</strong></li>
-- MAGIC           <li><code>refresh_silver</code> — <strong style="color: red;">Fails</strong></li>
-- MAGIC           <li><code>refresh_gold</code> — <strong style="color: gray;">Skipped</strong> (dependency failed)</li>
-- MAGIC           <li><code>audit_log</code> — <strong style="color: gray;">Skipped</strong> (dependency failed — depends on refresh_gold)</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC     </ol>
-- MAGIC     <p>Bronze and silver_cdc ran and succeeded. Silver failed because it references a view that does not exist. Gold was <strong>skipped</strong> because its dependency failed — this is the <strong>All succeeded</strong> Run-If condition from Lecture 4.1 in action. Notice that silver_cdc still succeeded because it depends on bronze, not silver — the parallel branch was unaffected.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G3. Fix the SQL File

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">G3. Fix the SQL File</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Open <code>pipeline_sql/demo_refresh_silver.sql</code> again</li>
-- MAGIC       <li><strong>Delete</strong> the two lines you added (the comment and the bad REFRESH)</li>
-- MAGIC       <li>Save the file — it should look exactly as it did before G1</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G4. Repair Run — Only Silver and Gold Re-Execute

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">G4. Repair Run — Only Silver and Gold Re-Execute</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Go to the <strong>failed run</strong> in the Runs tab</li>
-- MAGIC       <li>Click <strong>Repair Run</strong></li>
-- MAGIC       <li>Observe: <code>refresh_silver</code> re-executes, then <code>refresh_gold</code> runs after it succeeds. <code>refresh_bronze</code> and <code>refresh_silver_cdc</code> show as skipped (previously succeeded).</li>
-- MAGIC       <li>The entire job now completes successfully</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Why Repair Run Matters</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p><strong>You did not re-run bronze or silver_cdc — Repair Run saves time and cost.</strong></p>
-- MAGIC     <p>In our demo, Repair Run re-executed <code>refresh_silver</code> (the failed task) and <code>refresh_gold</code> (its downstream dependent), while skipping <code>refresh_bronze</code> and <code>refresh_silver_cdc</code> which had already succeeded. In production pipelines:</p>
-- MAGIC     <ul>
-- MAGIC       <li>Bronze refresh might process millions of rows and take 30+ minutes</li>
-- MAGIC       <li>Parallel branches that succeeded (like our CDC refresh) are not re-run</li>
-- MAGIC       <li>Only the failed task <strong>and everything downstream of it</strong> re-executes — gold had to re-run because it depends on silver, even though gold itself did not fail</li>
-- MAGIC     </ul>
-- MAGIC     <p>Repair Run preserves the results of tasks that already succeeded and re-executes failed tasks along with their downstream dependents (documentation (<a href="https://docs.databricks.com/aws/en/jobs/repair-job-failures" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/repair-job-failures" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/repair-job-failures" target="_blank">GCP</a>)). This is one of the most important operational features of Lakeflow Jobs.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Key Summary and Takeaways
-- MAGIC
-- MAGIC In this demo, we orchestrated the full ETL pipeline using a **Lakeflow Job**:
-- MAGIC
-- MAGIC - **SQL Files as tasks:** Each pipeline layer (bronze, silver, gold, CDC) is a `.sql` file containing `REFRESH` commands. SQL File tasks execute these files on a SQL Warehouse.
-- MAGIC - **Diamond-pattern DAG:** The "Depends on" setting creates execution order. Silver and CDC both depend on bronze and run in parallel; gold waits for **both** to complete before running. This is how real pipelines maximize throughput.
-- MAGIC - **Graph** shows real-time task status during a run — queued (gray) → running (green spinner) → succeeded (green checkmark) or failed (red)
-- MAGIC - **Timeline** shows task durations and parallelism — overlapping bars confirm fan-out is working
-- MAGIC - **Runs tab** shows all historical runs as a matrix — spot patterns of success and failure at a glance
-- MAGIC - **Scheduling** lets you run the pipeline on a schedule — daily, hourly, or triggered by file arrival and table updates
-- MAGIC - **Repair Run** re-executes only failed tasks and their downstream dependents — saving time and compute in production
-- MAGIC - **Job parameters** like `{{catalog}}` are dynamic value references that resolve at runtime
-- MAGIC - **Task output passing** lets one task read another's SELECT output via task parameters — the audit task reads the gold task's status this way
-- MAGIC
-- MAGIC **The big picture:** Everything we built in this course — streaming tables, materialized views, AUTO CDC dimensions — comes together in a Lakeflow Job. The job is the operational wrapper that turns a collection of SQL objects into a reliable, scheduled, observable pipeline.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border: 2px solid #1976d2; background: linear-gradient(135deg, #e3f2fd, #f0f0ff); padding: 18px 22px; border-radius: 8px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; font-size:1.2em; margin-bottom:10px;">Task Output Passing — 1,000 Rows / 48 KB / 7 Days</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Notice the <code>SELECT</code> at the end of each SQL file (e.g., <code>SELECT 'Bronze refresh complete' AS status</code>). This produces <strong>task output</strong> — data that is captured by the job engine and can be read by downstream tasks.</p>
-- MAGIC     <p><strong>Important distinction:</strong> Task output passing is <strong>not</strong> how tasks read source tables. When <code>refresh_gold</code> queries <code>silver.orders_enriched</code>, it reads the table directly via SQL. Task output passing is a <strong>separate channel</strong> — a lightweight way to pass control data (row counts, status flags, table lists) between tasks via <code>{{ }}</code> references.</p>
-- MAGIC     <p><strong>Limits:</strong></p>
-- MAGIC     <ul>
-- MAGIC       <li>Up to <strong>1,000 rows</strong> or <strong>48 KB</strong> per task (whichever is hit first)</li>
-- MAGIC       <li>Retained for <strong>7 days</strong></li>
-- MAGIC       <li>Read via <code>{{tasks.&lt;name&gt;.output.rows}}</code> (all rows) or <code>{{tasks.&lt;name&gt;.output.first_row.&lt;col&gt;}}</code> (one value) — these references go in <strong>task configuration fields</strong> (parameter values, ForEach inputs, If/else operands). Inside SQL files, use <code>:param_name</code> to receive the resolved value.</li>
-- MAGIC     </ul>
-- MAGIC     <p><strong>In this demo</strong>, the SELECT outputs serve as completion confirmations. <strong>In Lab 4.3</strong>, the audit task reads the gold task's output to capture metrics without running duplicate queries.</p>
-- MAGIC   </div>
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
