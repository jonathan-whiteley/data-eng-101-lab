-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](../Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4.3 Lab - Orchestrating SQL Pipelines with Lakeflow Jobs
-- MAGIC
-- MAGIC #### Duration: ~15–20 Minutes
-- MAGIC
-- MAGIC ## Overview
-- MAGIC In this lab, you will bring together the pipeline objects built in Labs 2.3 and 3.3 into a single **Lakeflow Job**. You will review SQL files for each pipeline layer, build a multi-task DAG, configure **job parameters** that control both infrastructure (catalog) and business logic (report dimension), and practice **Repair Run** for failure recovery.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you will be able to:
-- MAGIC - Create SQL files that use **job parameters** with `IDENTIFIER()` for catalog portability
-- MAGIC - Build a Lakeflow Job with a **multi-task DAG** that mirrors the pipeline architecture
-- MAGIC - Configure a **parameterized report** where the job parameter controls the output
-- MAGIC - Add an **interval trigger** and practice **Repair Run**

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border: 2px solid #00897b; background: linear-gradient(135deg, #e0f2f1, #b2dfdb); padding: 18px 22px; border-radius: 8px; margin: 16px 0;">
-- MAGIC   <div style="display:flex; align-items:center; gap:12px; margin-bottom:10px;">
-- MAGIC     <img src="../Includes/images/icons/rocket-icon.png" height="28" style="vertical-align:middle;">
-- MAGIC     <strong style="color:#004d40; font-size:1.2em;">Finished Early? Take on the Bonus Challenges</strong>
-- MAGIC   </div>
-- MAGIC   <div style="color:#333; line-height:1.7;">
-- MAGIC     <p style="margin:0 0 8px 0;"><strong>Section G</strong> goes beyond the core lab with five hands-on challenges that apply additional Lakeflow Job capabilities from Lecture 4.1:</p>
-- MAGIC     <ul style="margin:4px 0 8px 0;">
-- MAGIC       <li><strong><u>Re-run with different parameters</u></strong> — see how one pipeline produces different reports</li>
-- MAGIC       <li><strong><u>Split tasks for parallelism</u></strong> — reduce pipeline duration with fan-out</li>
-- MAGIC       <li><strong><u>Dynamic value references</u></strong> — add job metadata to your report table</li>
-- MAGIC       <li><strong><u>ForEach iteration</u></strong> — automate multi-dimension reports in a single run</li>
-- MAGIC       <li><strong><u>Run-If conditions</u></strong> — configure always-on completion tasks</li>
-- MAGIC     </ul>
-- MAGIC     <p style="margin:0;">These exercises are <strong>intentionally less guided</strong> and encourage use of <strong>Genie Code</strong> as a learning tool.</p>
-- MAGIC   </div>
-- MAGIC </div>

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
-- MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Recommended Background</strong>
-- MAGIC   <div style="color:#333;">This lab orchestrates the same pipeline objects covered in <strong>Lab 2.3</strong> (streaming tables, materialized views) and <strong>Lab 3.3</strong> (CDC dimension tables). The classroom setup creates all required objects automatically, so you can run this lab independently — but completing those labs first will give you deeper context for what each task refreshes.</div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## A. REQUIRED - Classroom Setup
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Run the Setup Cell</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Run the following cell to verify prerequisites and create all pipeline objects.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.3-Lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Pipeline Architecture
-- MAGIC
-- MAGIC Here is the full pipeline architecture you will orchestrate as a Lakeflow Job. Each pipeline layer maps to one or more **tasks** in the job, plus a final **parameterized report** task that uses a job parameter to control its output.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <span style="color:#991b1b; font-weight:bold; font-style:italic;">If the diagram below does not render, click into the cell and then click out - this triggers the Mermaid library to load.</span>
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
-- MAGIC     subgraph REPORT["<b>REPORT — Parameterized Output (gold schema)</b>"]
-- MAGIC         PR["gold.pipeline_report<br/><small>Table · INSERT INTO · job parameter</small>"]
-- MAGIC     end
-- MAGIC     CF --> CFE
-- MAGIC     RO --> CFE
-- MAGIC     RP --> CFE
-- MAGIC     CDC1 --> CFE
-- MAGIC     CFE --> FBC
-- MAGIC     FBC --> PR
-- MAGIC     classDef bronze fill:#FFF3E0,stroke:#e65100,stroke-width:2px,color:#333
-- MAGIC     classDef silver fill:#E3F2FD,stroke:#1565c0,stroke-width:2px,color:#333
-- MAGIC     classDef gold fill:#E8F5E9,stroke:#2e7d32,stroke-width:2px,color:#333
-- MAGIC     classDef cdc fill:#F3E5F5,stroke:#1976d2,stroke-width:2px,color:#333
-- MAGIC     classDef report fill:#FFF9C4,stroke:#F9A825,stroke-width:2px,color:#333
-- MAGIC     class RO,RP,CF bronze
-- MAGIC     class CFE silver
-- MAGIC     class FBC gold
-- MAGIC     class CDC1 cdc
-- MAGIC     class PR report
-- MAGIC     style BRONZE fill:#FFF8E1,stroke:#e65100,stroke-width:2px
-- MAGIC     style SILVER fill:#E8EAF6,stroke:#1565c0,stroke-width:2px
-- MAGIC     style GOLD fill:#E8F5E9,stroke:#2e7d32,stroke-width:2px
-- MAGIC     style CDC fill:#F3E5F5,stroke:#1976d2,stroke-width:2px
-- MAGIC     style REPORT fill:#FFF9C4,stroke:#F9A825,stroke-width:2px
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

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Why This Shape?</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The job mirrors the pipeline's data flow:</p>
-- MAGIC     <ul style="margin:4px 0;">
-- MAGIC       <li><strong>Bronze</strong> and <strong>CDC</strong> are independent sources — they run in <strong>parallel</strong>. Bronze refreshes the streaming tables from Auto Loader; CDC refreshes the customer dimension from <code>customer_changes</code> (built by the classroom setup).</li>
-- MAGIC       <li><strong>Silver</strong> depends on <strong>both</strong> bronze and CDC — the enriched MV joins bronze tables (orders, products, feedback) with the CDC customer dimension for names and segments.</li>
-- MAGIC       <li><strong>Gold</strong> aggregates the enriched feedback by product category — the standard business-ready aggregation. It depends on silver.</li>
-- MAGIC       <li><strong>Report</strong> appends a <strong>parameterized summary</strong> to <code>pipeline_report</code>. The <code>report_dimension</code> job parameter controls which column the report groups by.</li>
-- MAGIC     </ul>
-- MAGIC     <p><strong>Why is the report a regular table, not a materialized view?</strong> MVs have fixed query definitions — you can't parameterize them with job parameters at runtime. When your output depends on a runtime parameter, a regular table with <code>INSERT INTO</code> is the right pattern.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border: 2px solid #1976d2; background: linear-gradient(135deg, #e3f2fd, #e8eaf6); padding: 18px 22px; border-radius: 8px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; font-size:1.2em; margin-bottom:10px;">How Tasks Connect — Two Mechanisms Used in This Job</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:8px 0;">
-- MAGIC       <tr style="background:#0d47a1; color:#fff;"><th style="padding:8px 10px; text-align:left;">Mechanism</th><th style="padding:8px 10px;">What It Does</th><th style="padding:8px 10px;">Example in This Job</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:8px 10px;"><strong>DAG Dependencies</strong><br/><small>("Depends on")</small></td><td style="padding:8px 10px;">Controls <strong>execution order</strong> — "don't start silver until both bronze and CDC finish." Tasks read shared tables directly via SQL, not through the job engine.</td><td style="padding:8px 10px;">refresh_silver depends on both refresh_bronze and refresh_cdc</td></tr>
-- MAGIC       <tr><td style="padding:8px 10px;"><strong>Job Parameters</strong><br/><small>catalog, report_dimension</small></td><td style="padding:8px 10px;">Values you configure once — <strong>every task</strong> in the job can use them. Resolved before SQL executes.</td><td style="padding:8px 10px;">catalog in every SQL file, report_dimension in the report task</td></tr>
-- MAGIC     </table>
-- MAGIC     <p style="margin-top:10px; color:#555; font-size:0.85em;">The bonus section (G) explores additional mechanisms from Lecture 4.1: dynamic value references, ForEach iteration, and Run-If conditions.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. The SQL Files
-- MAGIC
-- MAGIC The `lab_pipeline_sql` folder contains 5 SQL files — one per task. Three are complete; two have `FILL_IN` placeholders for you to complete.
-- MAGIC
-- MAGIC | File | Task | What It Does | Status |
-- MAGIC |------|------|-------------|--------|
-- MAGIC | `lab_refresh_bronze.sql` | refresh_bronze | Refreshes 3 bronze streaming tables | **FILL_IN** — 3 IDENTIFIER refs |
-- MAGIC | `lab_refresh_cdc.sql` | refresh_cdc | Refreshes the CDC dimension table | Complete |
-- MAGIC | `lab_refresh_silver.sql` | refresh_silver | Refreshes the silver MV | Complete |
-- MAGIC | `lab_refresh_gold.sql` | refresh_gold | Refreshes the gold MV | Complete |
-- MAGIC | `lab_build_report.sql` | build_report | Appends parameterized report (INSERT INTO) | **FILL_IN** — 2 IDENTIFIER refs |

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### C1. Update the Bronze File
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Edit lab_refresh_bronze.sql</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Open <code>lab_pipeline_sql/lab_refresh_bronze.sql</code> in the workspace sidebar. Replace each <code>FILL_IN</code> with the correct <code>IDENTIFIER()</code> reference for each bronze streaming table:
-- MAGIC     <ul>
-- MAGIC       <li><code>bronze.raw_orders</code></li>
-- MAGIC       <li><code>bronze.raw_products</code></li>
-- MAGIC       <li><code>bronze.customer_feedback</code></li>
-- MAGIC     </ul>
-- MAGIC     The <code>IDENTIFIER()</code> pattern wraps the catalog parameter so the job engine's single-quoting doesn't break the SQL: <code>IDENTIFIER({{catalog}} || '.schema.table')</code>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help — lab_refresh_bronze.sql</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyC1()" style="background:#1B3139; color:#fff; border:none; padding:6px 14px; border-radius:6px; font-size:0.85em; cursor:pointer; margin-bottom:6px;">Copy to clipboard</button>
-- MAGIC         <pre id="copy-c1" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.raw_orders');
-- MAGIC REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.raw_products');
-- MAGIC REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.customer_feedback');
-- MAGIC
-- MAGIC SELECT 'Bronze refresh complete' AS status, current_timestamp() AS completed_at;</code></pre>
-- MAGIC         <script>function copyC1(){const e=document.getElementById("copy-c1");if(!e)return;const t=e.innerText;if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbC1(t))}else{fbC1(t)}}function fbC1(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed. Please select and copy manually.")}finally{document.body.removeChild(a)}}</script>
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

-- MAGIC %md-sandbox
-- MAGIC ### C2. Review the CDC, Silver, and Gold Files
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Review the SQL Files</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Open these three files in the workspace sidebar — they are already complete. Read through each one before moving on.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>lab_refresh_cdc.sql</strong> — <code>REFRESH STREAMING TABLE</code> on the SCD Type 1 customer dimension. This is the table built with <code>FLOW AUTO CDC</code> in Lab 3.3. Each refresh processes new change events from <code>bronze.customer_changes</code> and updates the current customer state.</li>
-- MAGIC       <li><strong>lab_refresh_silver.sql</strong> — <code>REFRESH MATERIALIZED VIEW</code> on the enriched feedback MV. This MV joins four sources: <code>bronze.customer_feedback</code>, <code>bronze.raw_orders</code>, <code>bronze.raw_products</code>, and <code>demo_cdc.demo_cdc_customers_current</code>. Customer names and segments come from the CDC dimension — this is why silver depends on <strong>both</strong> bronze and CDC in the DAG.</li>
-- MAGIC       <li><strong>lab_refresh_gold.sql</strong> — <code>REFRESH MATERIALIZED VIEW</code> on <code>gold.feedback_by_category</code>, the business-ready aggregation by product category. This is the fixed gold layer — always grouped by category, always current after a refresh.</li>
-- MAGIC     </ul>
-- MAGIC     <p>All three files use the same <code>IDENTIFIER({{catalog}} || '...')</code> pattern you used in C1, and each ends with a <code>SELECT</code> confirmation that appears in the task's <strong>Output</strong> tab.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Update the Report File
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Edit lab_build_report.sql</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Open <code>lab_pipeline_sql/lab_build_report.sql</code>. Replace the two <code>FILL_IN</code> placeholders with <code>IDENTIFIER()</code> references &mdash; the same pattern you used in C1:
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC | FILL_IN Location | Schema.Table | Hint |
-- MAGIC |---|---|---|
-- MAGIC | `INSERT INTO FILL_IN` | `gold.pipeline_report` | Where the report rows are written |
-- MAGIC | `FROM FILL_IN` | `silver.customer_feedback_enriched` | Where the enriched data is read from |

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">About the CASE Expression (already filled in — just read it)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The rest of the file is complete. Notice the <code>CASE</code> expression — this is how the <code>report_dimension</code> job parameter controls the report's grouping column.</p>
-- MAGIC     <p>The job engine wraps <code>{{report_dimension}}</code> in single quotes at runtime (e.g., it becomes <code>'product_category'</code>), so you <strong>cannot use it directly as a column name</strong>. The CASE converts the quoted string into the correct column reference. The line <code>{{report_dimension}} AS dimension_name</code> resolves to a <strong>string literal</strong> (e.g., <code>'product_category' AS dimension_name</code>) &mdash; it records which dimension was used, not the column value:</p>
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:8px 0;">
-- MAGIC       <tr style="background:#1B3139; color:#fff;"><th style="padding:6px 10px; text-align:left;">Parameter resolves to</th><th style="padding:6px 10px;">CASE returns</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>'product_category'</code></td><td style="padding:6px 10px;">the <code>product_category</code> column</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;"><code>'product_brand'</code></td><td style="padding:6px 10px;">the <code>product_brand</code> column</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>'customer_segment'</code></td><td style="padding:6px 10px;">the <code>customer_segment</code> column</td></tr>
-- MAGIC     </table>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help — lab_build_report.sql</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyC3()" style="background:#1B3139; color:#fff; border:none; padding:6px 14px; border-radius:6px; font-size:0.85em; cursor:pointer; margin-bottom:6px;">Copy to clipboard</button>
-- MAGIC         <pre id="copy-c3" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>INSERT INTO IDENTIFIER({{catalog}} || '.gold.pipeline_report')
-- MAGIC SELECT
-- MAGIC   CASE {{report_dimension}}
-- MAGIC     WHEN 'product_category' THEN product_category
-- MAGIC     WHEN 'product_brand'    THEN product_brand
-- MAGIC     WHEN 'customer_segment' THEN customer_segment
-- MAGIC   END AS dimension_value,
-- MAGIC   {{report_dimension}} AS dimension_name,
-- MAGIC   COUNT(*) AS feedback_count,
-- MAGIC   ROUND(AVG(rating), 2) AS avg_rating,
-- MAGIC   ROUND(AVG(order_value), 2) AS avg_order_value,
-- MAGIC   current_timestamp() AS report_generated_at
-- MAGIC FROM IDENTIFIER({{catalog}} || '.silver.customer_feedback_enriched')
-- MAGIC GROUP BY 1, 2
-- MAGIC ORDER BY feedback_count DESC;
-- MAGIC
-- MAGIC SELECT * FROM IDENTIFIER({{catalog}} || '.gold.pipeline_report')
-- MAGIC WHERE report_generated_at = (
-- MAGIC   SELECT MAX(report_generated_at)
-- MAGIC   FROM IDENTIFIER({{catalog}} || '.gold.pipeline_report')
-- MAGIC );</code></pre>
-- MAGIC         <p style="margin-top:8px; color:#555;"><strong>The two FILL_INs:</strong> <code>IDENTIFIER({{catalog}} || '.gold.pipeline_report')</code> for INSERT INTO, and <code>IDENTIFIER({{catalog}} || '.silver.customer_feedback_enriched')</code> for FROM.</p>
-- MAGIC         <script>function copyC3(){const e=document.getElementById("copy-c3");if(!e)return;const t=e.innerText;if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbC3(t))}else{fbC3(t)}}function fbC3(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed. Please select and copy manually.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Build the Lakeflow Job
-- MAGIC
-- MAGIC Now create a Lakeflow Job that orchestrates these 5 SQL files. Bronze and CDC run in parallel, silver fans in from both, then gold and the report follow in sequence.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D1. Create the Job</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the left sidebar, right-click <strong>Jobs &amp; Pipelines</strong> and open in a new tab</li>
-- MAGIC       <li>Click <strong>Create</strong>, then select <strong>Job</strong></li>
-- MAGIC       <li>Click the job name at the top (defaults to "New Job") and rename it to: <strong>Feedback Pipeline Lab</strong></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D2. Add Task 1: refresh_bronze</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the <strong>Tasks</strong> tab, the default task type is Notebook. Click the task type dropdown and select <strong>SQL File</strong> (not SQL Query)</li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_bronze</code></li>
-- MAGIC       <li>Click the <strong>Path</strong> field and navigate to <code>lab_pipeline_sql/lab_refresh_bronze.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to your shared SQL warehouse</li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D3. Add Task 2: refresh_cdc (no dependencies)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Add task</strong> and select <strong>SQL File</strong></li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_cdc</code></li>
-- MAGIC       <li>Set <strong>Path</strong> to: <code>lab_pipeline_sql/lab_refresh_cdc.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same shared warehouse</li>
-- MAGIC       <li>Leave <strong>Depends on</strong> empty — CDC reads from <code>bronze.customer_changes</code>, which was built by the classroom setup, not by the bronze task</li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p>Bronze and CDC are <strong>independent</strong> — they will run in parallel. This matches the architecture diagram where bronze and CDC are separate source layers.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D4. Add Task 3: refresh_silver (depends on BOTH bronze and CDC)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Add task</strong> and select <strong>SQL File</strong></li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_silver</code></li>
-- MAGIC       <li>Set <strong>Path</strong> to: <code>lab_pipeline_sql/lab_refresh_silver.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same shared warehouse</li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <strong>both</strong> <code>refresh_bronze</code> and <code>refresh_cdc</code></li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p>Silver depends on <strong>both</strong> because the enriched MV joins bronze streaming tables (orders, products, feedback) with the CDC customer dimension (<code>demo_cdc_customers_current</code>). Silver won't start until both bronze and CDC have completed.</p>
-- MAGIC     <p><strong>Note:</strong> When you add a task with an existing task selected on the canvas, the UI may auto-wire a dependency connector. Always verify the <strong>Depends on</strong> field matches what you intend — you can remove or add dependencies manually.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D5. Add Task 4: refresh_gold (depends on refresh_silver)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Add task</strong> and select <strong>SQL File</strong></li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>refresh_gold</code></li>
-- MAGIC       <li>Set <strong>Path</strong> to: <code>lab_pipeline_sql/lab_refresh_gold.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same shared warehouse</li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <code>refresh_silver</code></li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p>Gold depends on silver because the <code>feedback_by_category</code> MV aggregates from <code>customer_feedback_enriched</code>.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D6. Add Task 5: build_report (depends on refresh_gold)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Add task</strong> and select <strong>SQL File</strong></li>
-- MAGIC       <li>Set <strong>Task name</strong> to: <code>build_report</code></li>
-- MAGIC       <li>Set <strong>Path</strong> to: <code>lab_pipeline_sql/lab_build_report.sql</code></li>
-- MAGIC       <li>Set <strong>SQL Warehouse</strong> to the same shared warehouse</li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <code>refresh_gold</code></li>
-- MAGIC       <li>Click <strong>Save task</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p>The report task runs last — it reads from the silver MV (<code>customer_feedback_enriched</code>) and appends a parameterized summary to <code>pipeline_report</code> in the gold schema.</p>
-- MAGIC     <p><strong>Why does build_report depend on refresh_gold but read from silver?</strong> The dependency on refresh_gold ensures the <em>entire pipeline</em> has completed before the report runs. Even though the report SQL reads from silver, the gold refresh validates that the full pipeline executed successfully. This is a common pattern: the dependency controls <strong>execution order</strong>, while the SQL controls <strong>data access</strong>.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- Run this cell and copy the result into the catalog parameter value
SELECT my_catalog AS your_catalog_name;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <strong>Copy this catalog name exactly as shown</strong> — paste it into the <code>catalog</code> job parameter. A mismatch will cause <code>IDENTIFIER()</code> resolution failures across all tasks.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">The report_dimension Parameter</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>This parameter controls what the report groups by. The report SQL file uses a CASE expression to map the parameter value to a column:</p>
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:8px 0;">
-- MAGIC       <tr style="background:#1B3139; color:#fff;"><th style="padding:6px 10px; text-align:left;">Parameter Value</th><th style="padding:6px 10px;">Column Used</th><th style="padding:6px 10px;">What You'll See</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>product_category</code></td><td style="padding:6px 10px;">product_category</td><td style="padding:6px 10px;">Avg rating per category (Electronics, Food, etc.)</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;"><code>product_brand</code></td><td style="padding:6px 10px;">product_brand</td><td style="padding:6px 10px;">Avg rating per brand</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>customer_segment</code></td><td style="padding:6px 10px;">customer_segment</td><td style="padding:6px 10px;">Avg rating per segment (from CDC dimension)</td></tr>
-- MAGIC     </table>
-- MAGIC     <p>The gold MV (<code>feedback_by_category</code>) always aggregates by product category — it's the fixed business-ready view. The report table gives you a <strong>flexible, on-demand view</strong> along any dimension.</p>
-- MAGIC     <p><strong style="color:#2e7d32; background:#f7fdf4; padding:2px 8px; border-radius:4px;">Pick <code>product_category</code> for now</strong> — you'll change it in section G to see a different report.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D7. Add Job Parameters</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The SQL files use two <code>{{ }}</code> parameters that need values. Without these, the job will fail because the placeholders won't resolve.</p>
-- MAGIC     <ol>
-- MAGIC       <li>On the <strong>right side</strong> of the job page, scroll down to <strong>Job parameters</strong></li>
-- MAGIC       <li>Click <strong>Edit parameters</strong></li>
-- MAGIC       <li>Add the first parameter:
-- MAGIC         <ul>
-- MAGIC           <li><strong>Key:</strong> <code>catalog</code></li>
-- MAGIC           <li><strong>Value:</strong> your catalog name — <em><u>copy from the cell output above</u></em></li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li>Click the <strong>+</strong> button to add a second parameter:
-- MAGIC         <ul>
-- MAGIC           <li><strong>Key:</strong> <code>report_dimension</code></li>
-- MAGIC           <li><strong>Value:</strong> <code>product_category</code></li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li>Click <strong>Save</strong></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">D8. Add an Interval Trigger (then Pause)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In Demo 4.2, we used a daily <strong>schedule</strong>. Here, use an <strong>interval trigger</strong> — a different trigger type.</p>
-- MAGIC     <ol>
-- MAGIC       <li>On the right side, find <strong>Schedules &amp; Triggers</strong></li>
-- MAGIC       <li>Click <strong>Add Trigger</strong></li>
-- MAGIC       <li>For <strong>Trigger type</strong>, select <strong>Scheduled</strong></li>
-- MAGIC       <li>Select <strong>Interval</strong> (not Schedule — this is a different trigger mode)</li>
-- MAGIC       <li>Set to <strong>Every 1 hour</strong></li>
-- MAGIC       <li>Click <strong>Save</strong></li>
-- MAGIC       <li>You will now see <strong>Edit Trigger</strong>, <strong>Pause</strong>, and <strong>Delete</strong> options &mdash; click <strong>Pause</strong> so it does not trigger during class</li>
-- MAGIC     </ol>
-- MAGIC     <p>Interval triggers run the job at regular intervals — useful for pipelines where data arrives continuously and you want near-real-time processing.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## E. Run and Monitor
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Run the Job</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click <strong>Run Now</strong> to start the job</li>
-- MAGIC       <li>Go to the <strong>Runs</strong> tab and click into the active run</li>
-- MAGIC       <li>Watch the pipeline execute:
-- MAGIC         <ul>
-- MAGIC           <li><strong>Bronze</strong> and <strong>CDC</strong> start immediately and run in <strong>parallel</strong></li>
-- MAGIC           <li><strong>Silver</strong> starts after both bronze and CDC complete (fan-in)</li>
-- MAGIC           <li><strong>Gold</strong> refreshes after silver</li>
-- MAGIC           <li><strong>Report</strong> appends the parameterized summary last</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li>After the run completes, click into the <strong>build_report</strong> task &mdash; the Output tab shows the report grouped by your chosen dimension</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After the job completes, verify the results:

-- COMMAND ----------

-- Gold MV — always aggregated by product category
SELECT * FROM IDENTIFIER(my_catalog || '.gold.feedback_by_category');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     This is the gold aggregation — always grouped by <strong>product category</strong>. Compare this with the <code>pipeline_report</code> output in the next cell, which groups by your chosen <code>report_dimension</code> parameter.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- Report table — grouped by your chosen report_dimension parameter
SELECT * FROM IDENTIFIER(my_catalog || '.gold.pipeline_report');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Repair Run Exercise
-- MAGIC
-- MAGIC Let's practice recovering from a **typo in a job parameter key** — a realistic mistake that only affects the task that uses that parameter.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">F1. Break the Job with a Parameter Typo</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Go to your <strong>Feedback Pipeline Lab</strong> job page</li>
-- MAGIC       <li>On the right side, scroll to <strong>Job parameters</strong> and click <strong>Edit parameters</strong></li>
-- MAGIC       <li>Change the <code>report_dimension</code> <strong>key</strong> to <code>report_dimenson</code> (misspell "dimension" as "dimenson")</li>
-- MAGIC       <li>Click <strong>Save</strong>, then click <strong>Run Now</strong></li>
-- MAGIC     </ol>
-- MAGIC     <p>The report SQL file references <code>{{report_dimension}}</code> — but that parameter no longer exists (you renamed it). The CASE expression will receive an unexpected value, causing a SQL error or incorrect results.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">F2. Observe, Fix, and Repair</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Watch the run: bronze, CDC, silver, and gold all <strong>succeed</strong> — only the report task <strong>fails</strong></li>
-- MAGIC       <li>Click into the failed report task to see the error</li>
-- MAGIC       <li>Go to <strong>Job parameters</strong> &rarr; <strong>Edit parameters</strong> and fix the key: change <code>report_dimenson</code> back to <code>report_dimension</code> &rarr; <strong>Save</strong></li>
-- MAGIC       <li>Go to the failed run in the Runs tab and click <strong>Repair Run</strong></li>
-- MAGIC       <li>Only the report task re-executes — bronze, CDC, silver, and gold are skipped (they already succeeded)</li>
-- MAGIC     </ol>
-- MAGIC     <p><strong>Repair Run re-executes failed tasks and their downstream dependents</strong> while skipping tasks that already succeeded (documentation (<a href="https://docs.databricks.com/aws/en/jobs/repair-job-failures" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/repair-job-failures" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/repair-job-failures" target="_blank">GCP</a>)). Since the failure was at the final task, only that one task re-runs — the four upstream tasks are not re-executed.</p>
-- MAGIC     <p style="margin-top:8px;"><strong style="color:#c62828;">Before continuing:</strong> Verify the parameter key is back to <code>report_dimension</code> (not the misspelled version). If it's still misspelled, fix it now — subsequent sections depend on it.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. (Bonus) Explore Your Job Further

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Quick Start — Create the Job with Code</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>If you skipped building the job in section D (or want a clean starting point), the cell below creates a job named <strong>Feedback Pipeline Lab — <em>your_email</em></strong> programmatically using the Databricks REST API. The job name includes your username so it's unique in shared workspaces. It automatically detects your catalog, workspace path, and available SQL warehouse. If a job with that name already exists, it is deleted and recreated.</p>
-- MAGIC     <p><strong style="color:#c62828;">IMPORTANT:</strong> You must <strong style="color:#c62828; background:#fef2f2; padding:2px 6px; border-radius:4px; border:1px solid #dc2626;">switch to Serverless Compute</strong> before running this cell — the SQL Warehouse cannot execute Python. Click the <strong>Connect</strong> dropdown and select <strong>Serverless</strong>. After the cell completes, you can switch back to the SQL Warehouse for the remaining exercises.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Create-Job-4.3-Lab

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #ff9800; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">These exercises are intentionally less guided</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The bonus exercises provide goals and key details but leave some steps for you to figure out. Use <strong>Genie Code</strong> (the AI assistant in the upper-right corner) to help — try prompts like <em>"How do I add a ForEach task in a Lakeflow Job?"</em> or <em>"What is the syntax for task parameters in a SQL file task?"</em></p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">G1. Re-Run with a Different Report Dimension</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The same pipeline can produce different business reports just by changing a parameter — no SQL editing needed.</p>
-- MAGIC     <ol>
-- MAGIC       <li>Go to <strong>Job parameters</strong> and change <code>report_dimension</code> to one of the other values:
-- MAGIC         <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:8px 0;">
-- MAGIC           <tr style="background:#1B3139; color:#fff;"><th style="padding:6px 10px; text-align:left;">Value</th><th style="padding:6px 10px;">What the Report Shows</th></tr>
-- MAGIC           <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>product_category</code></td><td style="padding:6px 10px;">Avg rating per category (Electronics, Food, etc.) — <em>current value</em></td></tr>
-- MAGIC           <tr><td style="padding:6px 10px;"><code>product_brand</code></td><td style="padding:6px 10px;">Avg rating per brand — which brands have the happiest customers?</td></tr>
-- MAGIC           <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>customer_segment</code></td><td style="padding:6px 10px;">Avg rating per segment (consumer, corporate, home_office)</td></tr>
-- MAGIC         </table>
-- MAGIC       </li>
-- MAGIC       <li>Click <strong>Save</strong>, then <strong>Run Now</strong></li>
-- MAGIC       <li>After the run completes, check the report task Output — the report now groups by the new dimension</li>
-- MAGIC     </ol>
-- MAGIC     <p>Then run the query below to see the accumulated history — both runs' results in one table:</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

SELECT * FROM IDENTIFIER(my_catalog || '.gold.pipeline_report')
ORDER BY report_generated_at, feedback_count DESC;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What Changed?</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The table now has <strong>both sets of rows</strong> — one batch per job run. The <code>dimension_name</code> column tells you which parameter value was used for each run, and <code>report_generated_at</code> timestamps each batch.</p>
-- MAGIC     <p>Because the report uses <code>INSERT INTO</code> (not CREATE OR REPLACE), the table accumulates report history. Same pipeline, same SQL files — different analytical output driven entirely by the job parameter.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">G2. Split Bronze into Parallel Tasks</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Right now, all three bronze REFRESHes run sequentially in a single task. Each REFRESH is independent — they read from different volume paths and write to different tables. Splitting them into separate tasks lets them run in <strong>parallel</strong>, reducing total job duration.</p>
-- MAGIC     <ol>
-- MAGIC       <li><strong>Create two new SQL files</strong> in the <code>lab_pipeline_sql/bonus</code> folder:
-- MAGIC         <ul>
-- MAGIC           <li><code>lab_refresh_bronze_products.sql</code> — containing: <code>REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.raw_products');</code></li>
-- MAGIC           <li><code>lab_refresh_bronze_feedback.sql</code> — containing: <code>REFRESH STREAMING TABLE IDENTIFIER({{catalog}} || '.bronze.customer_feedback');</code></li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li><strong>Edit the bronze file used by your job</strong> — remove the products and feedback lines, keeping only the <code>bronze.raw_orders</code> REFRESH. Check which file your <code>refresh_bronze</code> task points to:
-- MAGIC         <ul>
-- MAGIC           <li>If you built the job manually (section D): <code>lab_pipeline_sql/lab_refresh_bronze.sql</code></li>
-- MAGIC           <li>If you used the Quick Start: <code>lab_pipeline_sql/bonus/lab_refresh_bronze_solved.sql</code></li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li><strong>Update the job</strong>:
-- MAGIC         <ul>
-- MAGIC           <li>Add two new SQL File tasks: <code>refresh_bronze_products</code> and <code>refresh_bronze_feedback</code></li>
-- MAGIC           <li>Point each to its new file in <code>lab_pipeline_sql/bonus/</code></li>
-- MAGIC           <li>Set <strong>Depends on</strong> to empty for all three bronze tasks — they have no upstream dependencies</li>
-- MAGIC           <li>Update <code>refresh_silver</code> to depend on <strong>all three</strong> bronze tasks plus <code>refresh_cdc</code></li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li><strong>Run the job</strong> and watch the Timeline view — the three bronze tasks and CDC should all start at the same time</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">Why This Matters</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In production, bronze tables may ingest from large, slow sources. Running three 5-minute REFRESHes sequentially takes 15 minutes; running them in parallel takes 5. The DAG correctly expresses that silver needs <strong>all</strong> bronze data before it can enrich — but the bronze tasks themselves are independent.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">G3. Add Job Metadata to the Report Table</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Lakeflow Jobs expose <strong>dynamic value references</strong> that resolve at runtime — metadata about the job itself. Adding these to the report table creates provenance: you can trace every row back to <em>which job</em> produced it and <em>when the pipeline started</em>.</p>
-- MAGIC     <p>Dynamic value references work differently from job parameters. They <strong>cannot</strong> be used directly in a SQL file — they must be passed as <strong>task parameters</strong> on the specific task that needs them. In the SQL file, you receive them using the <code>:param_name</code> syntax.</p>
-- MAGIC     <p><strong>Steps:</strong></p>
-- MAGIC     <ol>
-- MAGIC       <li><strong>Add columns to the report table.</strong> Run the code cell below this callout.</li>
-- MAGIC       <li><strong>Edit <code>lab_build_report.sql</code></strong> — add <code>:job_name</code> and <code>:pipeline_started_at</code> to the SELECT:
-- MAGIC         <pre style="background:#1B3139; color:#E8E3DC; padding:10px 14px; border-radius:6px; font-size:0.85rem; margin:8px 0;">INSERT INTO IDENTIFIER({{catalog}} || '.gold.pipeline_report')
-- MAGIC SELECT
-- MAGIC   CASE {{report_dimension}} ... END AS dimension_value,
-- MAGIC   {{report_dimension}} AS dimension_name,
-- MAGIC   COUNT(*) AS feedback_count,
-- MAGIC   ROUND(AVG(rating), 2) AS avg_rating,
-- MAGIC   ROUND(AVG(order_value), 2) AS avg_order_value,
-- MAGIC   current_timestamp() AS report_generated_at,
-- MAGIC   :job_name AS job_name,
-- MAGIC   :pipeline_started_at AS pipeline_started_at
-- MAGIC FROM IDENTIFIER({{catalog}} || '.silver.customer_feedback_enriched')
-- MAGIC GROUP BY 1, 2
-- MAGIC ORDER BY feedback_count DESC;</pre>
-- MAGIC       </li>
-- MAGIC       <li><strong>Add task parameters to the build_report task</strong> in the Jobs UI:
-- MAGIC         <ul>
-- MAGIC           <li>Click into the <code>build_report</code> task &rarr; scroll to <strong>Parameters</strong> (task-level, not job-level)</li>
-- MAGIC           <li>Add: <strong>Key:</strong> <code>job_name</code> &rarr; <strong>Value:</strong> <code>{{job.name}}</code></li>
-- MAGIC           <li>Add: <strong>Key:</strong> <code>pipeline_started_at</code> &rarr; <strong>Value:</strong> <code>{{job.start_time.iso_datetime}}</code></li>
-- MAGIC           <li>Save the task</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li><strong>Run the job</strong> and query the report table — each row now includes the job name and when the pipeline started. Compare <code>pipeline_started_at</code> to <code>report_generated_at</code> to see total pipeline duration.</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

ALTER TABLE IDENTIFIER(my_catalog || '.gold.pipeline_report')
ADD COLUMNS IF NOT EXISTS (job_name STRING, pipeline_started_at STRING);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">The Two-Step Pattern for Dynamic Values</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Notice the difference between <strong>job parameters</strong> and <strong>task parameters</strong>:</p>
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:8px 0;">
-- MAGIC       <tr style="background:#0d47a1; color:#fff;"><th style="padding:6px 10px; text-align:left;">Type</th><th style="padding:6px 10px;">Where Configured</th><th style="padding:6px 10px;">How Used in SQL</th><th style="padding:6px 10px;">Example</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><strong>Job Parameter</strong></td><td style="padding:6px 10px;">Job-level settings</td><td style="padding:6px 10px;"><code>{{catalog}}</code> directly in SQL</td><td style="padding:6px 10px;">catalog, report_dimension</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;"><strong>Task Parameter</strong></td><td style="padding:6px 10px;">Individual task settings</td><td style="padding:6px 10px;"><code>:param_name</code> in SQL</td><td style="padding:6px 10px;">job_name, pipeline_started_at</td></tr>
-- MAGIC     </table>
-- MAGIC     <p>Dynamic value references like <code>{{job.name}}</code> go in the <strong>task parameter value field</strong> — the job engine resolves them and passes the result to the SQL file as a bound parameter via <code>:job_name</code>.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">G4. Use ForEach to Build All Three Reports in One Run</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In G1, you manually changed <code>report_dimension</code> and re-ran the job for each value. A <strong>ForEach task</strong> automates this — it iterates over an array of inputs and runs an inner task for each one, optionally in parallel.</p>
-- MAGIC     <p><strong>Goal:</strong> Replace the single <code>build_report</code> task with a ForEach that builds reports for all three dimensions — <code>product_category</code>, <code>product_brand</code>, and <code>customer_segment</code> — in a single job run, all written to the same <code>pipeline_report</code> table.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First, clear the report table so you can see clean results from the ForEach run. (If you completed G3 and added extra columns, those columns will appear as NULL in the ForEach results — this is expected since the ForEach SQL file doesn't include them.)

-- COMMAND ----------

TRUNCATE TABLE IDENTIFIER(my_catalog || '.gold.pipeline_report');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A SQL file has been provided at `lab_pipeline_sql/bonus/lab_build_report_single.sql`. It is similar to `lab_build_report.sql` but uses `{{input}}` instead of `{{report_dimension}}` — the ForEach task automatically sets `{{input}}` to the current element of the iteration array. Each iteration inserts rows for one dimension into the same `pipeline_report` table.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Steps</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li><strong>Remove the existing <code>build_report</code> task</strong> from the job (click the task &rarr; delete)</li>
-- MAGIC       <li><strong>Add a new task</strong> with type <strong>For each</strong></li>
-- MAGIC       <li>Set the <strong>Task name</strong> to: <code>build_all_reports</code></li>
-- MAGIC       <li>Under <strong>Depends on</strong>, select <code>refresh_gold</code></li>
-- MAGIC       <li>For <strong>Inputs</strong>, enter this JSON array:
-- MAGIC         <pre style="background:#1B3139; color:#E8E3DC; padding:10px 14px; border-radius:6px; font-size:0.85rem; margin:8px 0;">["product_category", "product_brand", "customer_segment"]</pre>
-- MAGIC       </li>
-- MAGIC       <li>Set <strong>Concurrency</strong> to <code>3</code> — all three iterations will run in parallel</li>
-- MAGIC       <li><strong>Configure the inner task:</strong>
-- MAGIC         <ul>
-- MAGIC           <li>Click <strong>Add a task</strong> inside the ForEach</li>
-- MAGIC           <li>Select <strong>SQL File</strong></li>
-- MAGIC           <li>Set the inner task name to: <code>build_report</code></li>
-- MAGIC           <li>Set <strong>Path</strong> to: <code>lab_pipeline_sql/bonus/lab_build_report_single.sql</code></li>
-- MAGIC           <li>Set <strong>SQL Warehouse</strong> to the shared warehouse</li>
-- MAGIC           <li>Save the inner task</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li>Save the ForEach task, then click <strong>Run Now</strong></li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After the run completes, query the report table — you should see all three dimensions from a single run:

-- COMMAND ----------

SELECT dimension_name, COUNT(*) AS rows, MIN(report_generated_at) AS generated_at
FROM IDENTIFIER(my_catalog || '.gold.pipeline_report')
GROUP BY dimension_name
ORDER BY dimension_name;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What Happened?</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The ForEach task ran the inner SQL file three times in parallel — once per input value. Each iteration inserted rows with a different <code>dimension_name</code> into the same <code>pipeline_report</code> table. One job run, three dimensions, zero manual parameter changes.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">G5. Add an ALL_DONE Completion Task</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>In production, you often need a task that runs <strong>regardless</strong> of whether the pipeline succeeded or failed — sending a notification, releasing resources, or logging completion. The <strong>Run if: All done</strong> condition makes this possible.</p>
-- MAGIC     <p><strong>Goal:</strong> Add a final task that always runs, even if upstream tasks failed.</p>
-- MAGIC     <ol>
-- MAGIC       <li><strong>Add a new SQL File task:</strong> <code>pipeline_complete</code>
-- MAGIC         <ul>
-- MAGIC           <li>Path: <code>lab_pipeline_sql/bonus/lab_pipeline_complete.sql</code></li>
-- MAGIC           <li>Depends on: the last task in your current DAG — this varies based on which exercises you completed:
-- MAGIC             <table style="width:100%; border-collapse:collapse; font-size:0.85em; margin:6px 0;">
-- MAGIC               <tr style="background:#f8f8f8;"><td style="padding:4px 8px;">Core lab only (A-F)</td><td style="padding:4px 8px;"><code>build_report</code></td></tr>
-- MAGIC               <tr><td style="padding:4px 8px;">After G4 (ForEach)</td><td style="padding:4px 8px;"><code>build_all_reports</code></td></tr>
-- MAGIC             </table>
-- MAGIC           </li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li><strong>Change the Run-If condition:</strong>
-- MAGIC         <ul>
-- MAGIC           <li>Click into the <code>pipeline_complete</code> task</li>
-- MAGIC           <li>Find the <strong>"Run if dependencies"</strong> dropdown (defaults to <code>All succeeded</code>)</li>
-- MAGIC           <li>Change it to <strong><code>All done</code></strong></li>
-- MAGIC           <li>Save the task</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li><strong>Test it:</strong> Intentionally break a parameter (like in section F) and run the job. Even though an upstream task fails, <code>pipeline_complete</code> should still execute.</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">The Six Run-If Conditions</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:8px 0;">
-- MAGIC       <tr style="background:#1B3139; color:#fff;"><th style="padding:6px 10px; text-align:left;">Condition</th><th style="padding:6px 10px;">When It Runs</th><th style="padding:6px 10px;">Use Case</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>All succeeded</code></td><td style="padding:6px 10px;">Every dependency succeeded</td><td style="padding:6px 10px;">Default — normal pipeline flow</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;"><code>All done</code></td><td style="padding:6px 10px;">Every dependency finished (success or failure)</td><td style="padding:6px 10px;">Cleanup, notifications, logging</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>None failed</code></td><td style="padding:6px 10px;">No dependency actively failed (skipped is OK)</td><td style="padding:6px 10px;">Conditional paths where not all branches run</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;"><code>At least one succeeded</code></td><td style="padding:6px 10px;">At least one dependency succeeded</td><td style="padding:6px 10px;">Partial-success pipelines</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;"><code>At least one failed</code></td><td style="padding:6px 10px;">At least one dependency failed</td><td style="padding:6px 10px;">Error branches, alerts, quarantine</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;"><code>All failed</code></td><td style="padding:6px 10px;">Every dependency failed</td><td style="padding:6px 10px;">Total failure handling</td></tr>
-- MAGIC     </table>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Key Summary and Takeaways
-- MAGIC
-- MAGIC | What You Did | Key Concept |
-- MAGIC |---|---|
-- MAGIC | Used `IDENTIFIER({{catalog}} || '...')` in SQL files | **Job parameters + IDENTIFIER()** make SQL files portable across catalogs |
-- MAGIC | Used a CASE expression with `{{report_dimension}}` | **Job parameters drive business logic** — the same pipeline produces different reports |
-- MAGIC | Report uses INSERT INTO, gold uses MV | **MVs can't be parameterized** — use regular tables with INSERT INTO when output depends on runtime parameters |
-- MAGIC | Built bronze + CDC &rarr; silver &rarr; gold &rarr; report | **DAG dependencies** mirror data flow — bronze and CDC run in parallel, silver fans in from both |
-- MAGIC | Set an interval trigger (then paused) | **Interval triggers** run jobs at regular frequencies for near-real-time pipelines |
-- MAGIC | Broke the report via parameter key typo, repaired | **Repair Run** re-executes only the failed subgraph — the later the failure, the less re-work |
-- MAGIC
-- MAGIC *If you completed the bonus exercises, you also explored: re-running with different parameters (G1), parallel task execution (G2), dynamic value references with task parameters (G3), ForEach iteration (G4), and Run-If conditions (G5).*

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
