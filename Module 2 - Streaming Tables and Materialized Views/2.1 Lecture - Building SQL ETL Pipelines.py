# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](../Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.1 Lecture - Building SQL ETL Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Legacy ingestion and transformation patterns -- full directory scans, static views, and scheduled CTAS rebuilds -- waste compute and deliver stale data. This lecture introduces the two declarative building blocks that replace them: **Streaming Tables** and **Materialized Views**.
# MAGIC
# MAGIC Legacy patterns include ingestion scripts that re-list entire directories on every run, static views that recompute terabytes on every dashboard refresh, and CTAS jobs that fully rebuild Gold tables even when only a handful of rows changed. Streaming Tables and Materialized Views solve these problems:
# MAGIC
# MAGIC - **Streaming Tables** ingest incrementally via Auto Loader -- only new files are processed on each refresh
# MAGIC - **Materialized Views** pre-compute and incrementally refresh transformations via the Enzyme engine -- only changed rows are reprocessed
# MAGIC
# MAGIC Both are serverless, declarative, and SQL-native.
# MAGIC
# MAGIC This lecture covers five sections that build on each other:
# MAGIC
# MAGIC - **A. Table Types** -- the four table types in Lakehouse SQL
# MAGIC - **B. Streaming Tables** -- incremental ingestion via Auto Loader, with quality expectations
# MAGIC - **C. Materialized Views** -- pre-computed, incrementally refreshed transformations
# MAGIC - **D. Choosing the Right Tool** -- ST vs MV decision framework
# MAGIC - **E. Operationalizing** -- EXPLAIN, REFRESH POLICY, and SCHEDULE
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Create a Streaming Table that incrementally ingests data from cloud storage using Auto Loader
# MAGIC - Explain why Auto Loader replaces COPY INTO for file ingestion -- exactly-once guarantees, schema evolution, and scalable file discovery
# MAGIC - Create a Materialized View that pre-computes and incrementally refreshes Silver-to-Gold transformations
# MAGIC - Explain why Materialized Views replace static Views -- pre-computed results, Enzyme incremental refresh, and cost reduction
# MAGIC - Use EXPLAIN CREATE MATERIALIZED VIEW to predict whether an MV will incrementalize before deploying
# MAGIC - Choose the right REFRESH POLICY (AUTO, INCREMENTAL, INCREMENTAL STRICT, FULL) based on EXPLAIN results and cost requirements
# MAGIC - Configure SCHEDULE EVERY, SCHEDULE CRON, or TRIGGER ON UPDATE to automate MV refresh timing
# MAGIC - Choose between Streaming Table and Materialized View for a given use case
# MAGIC - Describe how Materialized Views and Streaming Tables are powered

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Table Types in Lakehouse SQL
# MAGIC
# MAGIC Before diving into Streaming Tables and Materialized Views, let's set the stage with a quick look at all four table types available in Lakehouse SQL.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Four Table Types in Lakehouse SQL
# MAGIC
# MAGIC Lakehouse SQL gives you four distinct table types, each designed for a different purpose. This lecture focuses on the last two &mdash; **Streaming Tables** and **Materialized Views** &mdash; but it helps to see where they fit alongside the types you already know.
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto;">
# MAGIC <table style="width: 100%; border-collapse: collapse; line-height: 1.6;">
# MAGIC   <tr style="background: #1B3139; color: #fff;">
# MAGIC     <th style="padding: 12px 16px; text-align: left; border: 1px solid #2a4a54; font-size: 20px !important; width: 20%;">Type</th>
# MAGIC     <th style="padding: 12px 16px; text-align: left; border: 1px solid #2a4a54; font-size: 20px !important; width: 25%;">Created With</th>
# MAGIC     <th style="padding: 12px 16px; text-align: left; border: 1px solid #2a4a54; font-size: 20px !important; width: 15%;">Data Stored?</th>
# MAGIC     <th style="padding: 12px 16px; text-align: left; border: 1px solid #2a4a54; font-size: 20px !important; width: 40%;">Best For</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #F9F7F4;">
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><strong>Managed Table</strong><br/><small style="color:#666;">(Delta or Iceberg)</small></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><code>CREATE TABLE</code></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">Yes</td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">General-purpose storage with full CRUD &mdash; inserts, updates, deletes, merges</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #fff;">
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><strong>View</strong></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><code>CREATE VIEW</code></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">No</td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">Logical aliases &mdash; query re-executes every time, no storage cost but no performance gain</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f0f7ff;">
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><strong>Streaming Table</strong></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><code>CREATE STREAMING TABLE</code></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">Yes</td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">Incremental ingestion of <u>append-only</u> data from files via Auto Loader &mdash; processes only new files on each refresh. For non-append patterns (inserts, updates, deletes), use AUTO CDC with a Streaming Table</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f7fdf4;">
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><strong>Materialized View</strong></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;"><code>CREATE MATERIALIZED VIEW</code></td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">Yes</td>
# MAGIC     <td style="padding: 12px 16px; border: 1px solid #E8E3DC; font-size: 20px !important;">Pre-computed query results that <u>incrementally refresh</u> &mdash; only reprocesses changed rows, not the full dataset</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">The Two You Already Know</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Managed Tables</strong> (Delta or Iceberg) are the workhorses &mdash; you <code>CREATE TABLE</code>, insert data, update rows, delete records, and run merges. They store data physically and support full ACID transactions. You used these in Demo 1.2 when you explored the course dataset.</li>
# MAGIC           <li><strong>Views</strong> are just saved queries. <code>CREATE VIEW</code> stores the SQL definition, not the results. Every time you query a view, the underlying SQL re-executes against the source tables. Zero storage cost, but also zero performance benefit &mdash; a view over a terabyte table scans a terabyte every time.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">The Two This Lecture Deep-Dives Into</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Streaming Tables</strong> solve the ingestion problem. Instead of re-listing an entire directory of files on every run (like <code>COPY INTO</code>), a Streaming Table uses <strong>Auto Loader</strong> to track which files have already been processed and only pick up new ones. You write one <code>CREATE STREAMING TABLE</code> statement and the platform handles incremental file discovery, schema inference, and exactly-once processing.</li>
# MAGIC           <li><strong>Materialized Views</strong> solve the transformation problem. Instead of re-running an expensive join or aggregation every time a dashboard refreshes (like a View does), an MV stores the pre-computed result and uses the <strong>Enzyme engine</strong> to incrementally refresh only the rows that changed upstream. The result is faster queries and lower compute cost.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Why This Matters for the Course</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>The ETL pipeline you are building in this course uses <strong>Streaming Tables</strong> for the Bronze layer (ingestion from files) and <strong>Materialized Views</strong> for Silver and Gold layers (transformations and aggregations). Managed Tables hold the raw source data, and Views may appear for ad-hoc queries &mdash; but the pipeline backbone is ST + MV.</li>
# MAGIC           <li>Both Streaming Tables and Materialized Views are governed by <strong>Unity Catalog</strong> you saw the auto-created pipeline in Demo 1.2 and Lab 1.3.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Streaming Tables: Incremental Ingestion
# MAGIC
# MAGIC Streaming Tables are the modern, declarative way to ingest data from cloud storage into Databricks. This section introduces what a Streaming Table is and why it replaces the older COPY INTO command.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. What Is a Streaming Table?
# MAGIC
# MAGIC A **Streaming Table** is a Unity Catalog managed Delta table with built-in support for <u>incremental data processing</u> &mdash; it knows how to pick up new files from cloud storage, processing each one exactly once.
# MAGIC
# MAGIC <!-- ── Visual: button-expand-panels ── -->
# MAGIC
# MAGIC #### Select each feature to explore how Streaming Tables work
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif">
# MAGIC
# MAGIC <style>
# MAGIC .st1-row { display: block; white-space: nowrap; font-size: 0; width: 100%; box-sizing: border-box; }
# MAGIC .st1-box { display: inline-block; width: 23.5%; margin-right: 2%; min-height: 80px; background: #F9F7F4; border-top: 8px solid transparent; border-left: 2px solid transparent; border-right: 2px solid transparent; border-bottom: 2px solid transparent; border-radius: 8px; padding: 12px 8px; text-align: center; box-sizing: border-box; cursor: pointer; user-select: none; vertical-align: top; transition: transform 0.12s; }
# MAGIC .st1-box:last-child { margin-right: 0; }
# MAGIC .st1-box:hover { transform: translateY(-2px); }
# MAGIC .st1-box.st1-active { background: #fff; border-left-color: var(--st1-c); border-right-color: var(--st1-c); border-bottom-color: var(--st1-c); }
# MAGIC .st1-label { display: block; font-size: 14pt; font-weight: 700; color: #0b2026; line-height: 1.3; white-space: normal; pointer-events: none; }
# MAGIC .st1-detail-wrap { overflow: hidden; max-height: 0; opacity: 0; transition: max-height 0.35s ease, opacity 0.28s ease, margin-top 0.28s ease; margin-top: 0; }
# MAGIC .st1-detail-wrap.st1-open { max-height: 1200px; opacity: 1; margin-top: 12px; }
# MAGIC .st1-detail-card { background: #F9F7F4; border-radius: 10px; padding: 20px; box-sizing: border-box; border-top: 7px solid #ccc; }
# MAGIC .st1-can-block { border-left: 4px solid; border-radius: 0 6px 6px 0; padding: 14px 14px; margin-bottom: 14px; font-size: 14pt; line-height: 1.6; }
# MAGIC .st1-can-block ul { margin: 8px 0 0 0; padding-left: 20px; }
# MAGIC .st1-can-block li { margin-bottom: 8px; }
# MAGIC .st1-example-block { border-left: 4px solid; border-radius: 0 6px 6px 0; padding: 12px 14px; font-size: 14pt; line-height: 1.5; }
# MAGIC .st1-example-block pre { background: #1B3139; color: #e0e0e0; border-radius: 6px; padding: 12px 14px; font-family: monospace; font-size: 14pt; line-height: 1.6; margin: 8px 0 0 0; white-space: pre; overflow-x: auto; }
# MAGIC .st1-example-block .kw { color: #7ecbf5; font-weight: 600; }
# MAGIC .st1-example-block .fn { color: #dcdcaa; }
# MAGIC .st1-example-block .str { color: #ce9178; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="background: #F8F9FC; border: 3px solid #1B5162; border-radius: 10px; padding: 24px; box-sizing: border-box">
# MAGIC
# MAGIC   <div class="st1-row">
# MAGIC     <span class="st1-box st1-active" data-st1-id="0" onclick="st1Sel(0)" style="--st1-c:#2574B5; border-top-color:#2574B5">
# MAGIC       <span class="st1-label"><img src="../Includes/images/icons/databricks-sql-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> The SQL</span>
# MAGIC     </span><span class="st1-box" data-st1-id="1" onclick="st1Sel(1)" style="--st1-c:#02A36F; border-top-color:#02A36F">
# MAGIC       <span class="st1-label"><img src="../Includes/images/icons/input-ingest-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> Auto Loader</span>
# MAGIC     </span><span class="st1-box" data-st1-id="2" onclick="st1Sel(2)" style="--st1-c:#FF5F46; border-top-color:#FF5F46">
# MAGIC       <span class="st1-label"><img src="../Includes/images/icons/data-security-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> Data Reliability</span>
# MAGIC     </span><span class="st1-box" data-st1-id="3" onclick="st1Sel(3)" style="--st1-c:#FFAB00; border-top-color:#FFAB00">
# MAGIC       <span class="st1-label"><img src="../Includes/images/icons/cloud-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> Serverless</span>
# MAGIC     </span>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="st1-detail-wrap" id="st1-detail-wrap">
# MAGIC     <div class="st1-detail-card" id="st1-detail-card"></div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var ST1_ITEMS = [
# MAGIC   { title: "The SQL You Write", color: "#2574B5", bg: "rgba(37,116,181,0.10)",
# MAGIC     content: `<strong>One SQL statement</strong> creates a governed, incremental ingestion pipeline:<ul><li><code>STREAMING TABLE</code> &mdash; a <u>UC managed Delta table</u> with streaming semantics</li><li><code>STREAM read_files()</code> &mdash; <u>Auto Loader in SQL</u>, discovering new files automatically</li><li><strong>Schema inferred</strong> from source &mdash; <u>no column definitions needed</u> for JSON, Parquet, CSV, Avro</li><li><strong>Schema evolution</strong> &mdash; new columns added <u>automatically</u>; mismatches land in <code>_rescued_data</code></li></ul>`,
# MAGIC     example: `<strong>Create a Streaming Table:</strong><pre style="background:#1B3139;color:#e0e0e0;border-radius:6px;padding:12px 14px;font-family:monospace; line-height:1.6;margin:8px 0 0 0;white-space:pre;overflow-x:auto"><span style="color:#7ecbf5;font-weight:600">CREATE OR REPLACE STREAMING TABLE</span> bronze_orders\n<span style="color:#7ecbf5;font-weight:600">AS SELECT</span> *\n<span style="color:#7ecbf5;font-weight:600">FROM STREAM</span> <span style="color:#dcdcaa">read_files</span>(\n  '/Volumes/catalog/schema/raw/orders/',\n  format =&gt; 'json',\n  header =&gt; 'true'\n);</pre>` },
# MAGIC   { title: "Auto Loader \u2014 File Discovery", color: "#02A36F", bg: "rgba(2,163,111,0.10)",
# MAGIC     content: `<code>read_files()</code> is the SQL interface to <strong>Auto Loader</strong> &mdash; the same engine as Python&#39;s <code>cloudFiles</code>, with a friendlier SQL front door.<ul><li><strong>Directory listing</strong> (default) or <strong>cloud-native file notification</strong> for discovering <u>new files</u></li><li>Each <code>REFRESH</code> processes <u>only files that arrived since the last run</u></li><li>Scales to <strong>millions of files</strong> with directory listing; for <strong>billions of files</strong>, enable file notification mode</li><li>&#x25C6; At <a href="https://www.databricks.com/customers/sciplay" style="color:#02A36F">SciPlay</a>, Auto Loader delivered <strong><u>60x faster</u> time-to-insight</strong> (4 hours &#8594; 4 minutes)</li></ul>`,
# MAGIC     example: `<strong>Auto Loader with options:</strong><pre style="background:#1B3139;color:#e0e0e0;border-radius:6px;padding:12px 14px;font-family:monospace; line-height:1.6;margin:8px 0 0 0;white-space:pre;overflow-x:auto"><span style="color:#7ecbf5;font-weight:600">CREATE OR REPLACE STREAMING TABLE</span> bronze_events\n<span style="color:#7ecbf5;font-weight:600">AS SELECT</span> *\n<span style="color:#7ecbf5;font-weight:600">FROM STREAM</span> <span style="color:#dcdcaa">read_files</span>(\n  '/Volumes/catalog/schema/raw/events/',\n  format =&gt; 'json',\n  header =&gt; 'true',\n  <span style="color:#dcdcaa">schemaEvolutionMode</span> =&gt; 'addNewColumns',\n  <span style="color:#dcdcaa">inferColumnTypes</span> =&gt; true\n);</pre>` },
# MAGIC   { title: "Data Reliability", color: "#FF5F46", bg: "rgba(255,95,70,0.10)",
# MAGIC     content: `Streaming Tables provide four layers of data reliability out of the box:<ul><li><strong>Exactly-once processing</strong> &mdash; a persistent RocksDB checkpoint stamps every file as &#8220;received&#8221; so it is <u>never processed twice</u>. Survives restarts, failures, and reschedules</li><li><strong>Schema evolution</strong> &mdash; new source columns are added to the table <u>automatically</u> as they appear. No manual <code>ALTER TABLE</code> required</li><li><strong>Rescued data column</strong> &mdash; <code>_rescued_data</code> captures any fields that don&#39;t match the known schema as JSON, so <u>nothing is silently dropped</u>. It&#39;s your &#8220;lost and found&#8221; bin</li><li><strong>Unity Catalog governed</strong> &mdash; every Streaming Table is a UC managed Delta table with <u>permissions, tags, lineage, and audit logging</u> built in</li></ul>`,
# MAGIC     example: `<strong>Bottom line:</strong> Production-grade reliability with zero defensive code.` },
# MAGIC   { title: "Serverless SDP Pipeline", color: "#FFAB00", bg: "rgba(255,171,0,0.10)",
# MAGIC     content: `Every <code>CREATE</code> or <code>REFRESH</code> is powered by a <strong>serverless Lakeflow SDP (Spark Declarative Pipelines)</strong> pipeline. The SQL warehouse dispatches the command &mdash; the serverless pipeline does all the heavy lifting independently.<ul><li><strong>Separate compute</strong> &mdash; refresh processing runs on the <u>pipeline</u>, not on your <u>warehouse</u></li><li><strong>Not continuous</strong> &mdash; processes on <u>REFRESH only</u> (manual, scheduled, or triggered)</li><li><strong>Automatic scaling</strong> &mdash; the serverless pipeline <u>right-sizes compute</u> for each refresh, no cluster configuration needed</li><li><strong>Zero infrastructure</strong> &mdash; <u>no clusters</u> to configure, start, or monitor. The platform provisions and tears down compute <u>automatically</u></li></ul>`,
# MAGIC     example: `&#x25C6; <strong>At <a href="https://www.databricks.com/customers/block/delta-live-tables" style="color:#FFAB00">Block</a></strong>, switching to serverless streaming tables reduced pipeline development from <strong>days to hours</strong> and improved autoscaling efficiency.` }
# MAGIC ];
# MAGIC var st1Current = null;
# MAGIC function st1Sel(id) {
# MAGIC   var wrap = document.getElementById("st1-detail-wrap");
# MAGIC   var card = document.getElementById("st1-detail-card");
# MAGIC   var c = ST1_ITEMS[id];
# MAGIC   document.querySelectorAll(".st1-box").forEach(function(b) { b.classList.toggle("st1-active", parseInt(b.dataset.st1Id,10)===id); });
# MAGIC   if (st1Current === id) { wrap.classList.remove("st1-open"); document.querySelectorAll(".st1-box").forEach(function(b){b.classList.remove("st1-active");}); st1Current=null; return; }
# MAGIC   st1Current = id;
# MAGIC   card.style.borderTopColor = c.color;
# MAGIC   card.innerHTML = '<div style="font-size:18pt;font-weight:700;margin-bottom:12px;color:#0b2026;">'+c.title+'</div><div class="st1-can-block" style="background:'+c.bg+';border-color:'+c.color+'">'+c.content+'</div><div class="st1-example-block" style="background:'+c.bg+';border-color:'+c.color+'">'+c.example+'</div>';
# MAGIC   wrap.classList.add("st1-open");
# MAGIC }
# MAGIC st1Sel(0);
# MAGIC </script>
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">The SQL You Write</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>A <strong>Streaming Table</strong> is a Unity Catalog managed Delta table &mdash; it appears in Catalog Explorer, respects permissions, and supports tags, lineage, and all governance features like any other table</li>
# MAGIC           <li>The <code>STREAM</code> keyword tells the engine to use streaming semantics: track which files have been processed and only pick up new ones on each refresh</li>
# MAGIC           <li><code>read_files()</code> is the SQL interface to Auto Loader &mdash; the same engine as Python&#39;s <code>cloudFiles</code> format, just with a friendlier SQL front door</li>
# MAGIC           <li>You write one <code>CREATE STREAMING TABLE</code> statement &mdash; no file listing logic, no checkpoint management, no schema definitions</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Auto Loader &mdash; File Discovery</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Auto Loader discovers new files via <strong>directory listing</strong> (default) or <strong>cloud-native file notification</strong> for higher scale</li>
# MAGIC           <li>Each <code>REFRESH</code> processes only files that arrived since the last run &mdash; the RocksDB checkpoint tracks exactly where it left off</li>
# MAGIC           <li>Scales to <strong>millions of files</strong> with the default directory listing mode; for <strong>billions of files</strong>, enable file notification mode for constant-time discovery &mdash; unlike COPY INTO&#39;s file-tracking state which becomes expensive at scale</li>
# MAGIC           <li>&#x25C6; At <a href="https://www.databricks.com/customers/sciplay">SciPlay</a>, Auto Loader delivered a <strong>60x improvement in time-to-insight</strong> &mdash; from 4 hours to 4 minutes for gaming analytics pipelines</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Data Reliability</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Exactly-once processing</strong> &mdash; a persistent RocksDB checkpoint stamps every file as "received" so it is never processed twice. Survives restarts, failures, and reschedules</li>
# MAGIC           <li><strong>Schema inference</strong> &mdash; Auto Loader infers the schema from the first batch of files, no column definitions needed for JSON, Parquet, CSV, or Avro</li>
# MAGIC           <li><strong>Schema evolution</strong> &mdash; new source columns are added to the table automatically as they appear. No manual <code>ALTER TABLE</code> required</li>
# MAGIC           <li><strong>Rescued data</strong> &mdash; the <code>_rescued_data</code> column captures any fields that don&#39;t match the known schema as JSON, so nothing is silently dropped. It&#39;s your "lost and found" bin</li>
# MAGIC           <li><strong>Unity Catalog governed</strong> &mdash; permissions, tags, lineage, and audit logging from the moment the table is created</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Serverless SDP Pipeline</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>Every <code>CREATE</code> or <code>REFRESH</code> is powered by a <strong>serverless Lakeflow Spark Declarative Pipeline</strong> &mdash; the SQL warehouse dispatches the command, the pipeline does all the heavy lifting independently</li>
# MAGIC           <li><strong>Separate compute</strong> &mdash; refresh processing runs on the pipeline, not on your warehouse</li>
# MAGIC           <li><strong>Automatic scaling</strong> &mdash; the serverless pipeline right-sizes compute for each refresh, no cluster configuration needed</li>
# MAGIC           <li><strong>Zero infrastructure</strong> &mdash; no clusters to configure, start, or monitor. The platform provisions and tears down compute automatically</li>
# MAGIC           <li>&#x25C6; At <a href="https://www.databricks.com/customers/block/delta-live-tables">Block</a>, switching to serverless streaming tables reduced pipeline development from <strong>days to hours</strong> and improved autoscaling efficiency &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>
# MAGIC <script>  
# MAGIC <!-- Retry any images that failed to render on initial load (500ms and 1500ms delays -->
# MAGIC [500, 1500].forEach(function(delay) {
# MAGIC     setTimeout(function() {
# MAGIC         document.querySelectorAll('.two-pane img').forEach(function(img) {
# MAGIC             if (!img.complete || img.naturalWidth === 0) {                                                                                                                                
# MAGIC                 img.src = img.src;
# MAGIC             }                                                                                                                                                                             
# MAGIC         });     
# MAGIC     }, delay);
# MAGIC });
# MAGIC </script>
# MAGIC <script>  
# MAGIC <!-- Retry any images that failed to render on initial load (500ms and 1500ms delays -->
# MAGIC [500, 1500].forEach(function(delay) {
# MAGIC     setTimeout(function() {
# MAGIC         document.querySelectorAll('.two-pane img').forEach(function(img) {
# MAGIC             if (!img.complete || img.naturalWidth === 0) {                                                                                                                                
# MAGIC                 img.src = img.src;
# MAGIC             }                                                                                                                                                                             
# MAGIC         });     
# MAGIC     }, delay);
# MAGIC });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Why Auto Loader Replaces COPY INTO
# MAGIC
# MAGIC Section B1 introduced `read_files()` &mdash; the SQL interface to **Auto Loader**. Auto Loader is the ingestion engine that replaces COPY INTO, and Streaming Tables let you use it with a single SQL statement. COPY INTO is not deprecated, but Auto Loader addresses the key limitations that made COPY INTO difficult at scale.
# MAGIC
# MAGIC <!-- ── Visual: comparison-matrix ── -->
# MAGIC <div class="a2cm-wrap">
# MAGIC <style>
# MAGIC .a2cm-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 860px; margin: 0 auto; }
# MAGIC .a2cm-title { color: #1B3139; font-size: 1.4em; font-weight: 700; margin: 0 0 16px 0; text-align: center; }
# MAGIC .a2cm-table { width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border: 1px solid #E8E3DC; }
# MAGIC .a2cm-table thead th { background: #1B3139; color: white; font-size: 14pt; font-weight: 700; padding: 14px 18px; text-align: center; border-bottom: 2px solid #1B5162; }
# MAGIC .a2cm-table thead th:first-child { text-align: left; min-width: 200px; }
# MAGIC .a2cm-table tbody td { padding: 13px 18px; font-size: 14pt; text-align: center; border-bottom: 1px solid #E8E3DC; color: #1B3139; }
# MAGIC .a2cm-table tbody td:first-child { text-align: left; font-weight: 600; }
# MAGIC .a2cm-table tbody tr:nth-child(odd) { background: #F9F7F4; }
# MAGIC .a2cm-table tbody tr:nth-child(even) { background: white; }
# MAGIC .a2cm-table tbody tr:hover { background: #EDE9E3; }
# MAGIC .a2cm-check { color: #00A972; font-weight: 700; font-size: 1.15em; }
# MAGIC .a2cm-cross { color: #FF3621; font-weight: 700; font-size: 1.15em; }
# MAGIC .a2cm-partial { color: #C68B00; font-weight: 700; font-size: 1.05em; }
# MAGIC </style>
# MAGIC <h2 class="a2cm-title">COPY INTO vs. Auto Loader (Streaming Tables)</h2>
# MAGIC <table class="a2cm-table">
# MAGIC   <thead><tr>
# MAGIC     <th>Capability</th>
# MAGIC     <th>COPY INTO</th>
# MAGIC     <th>Streaming Table</th>
# MAGIC   </tr></thead>
# MAGIC   <tbody>
# MAGIC     <tr><td>File Discovery</td><td>Directory listing &mdash; slows as files grow</td><td><span class="a2cm-check"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"></span> Directory listing (default) or file notification &mdash; scales with either mode</td></tr>
# MAGIC     <tr><td>Scale at Millions of Files</td><td><span class="a2cm-cross"><img src="../Includes/images/icons/x-circle-icon.png" height="16" style="vertical-align:middle;"></span> Degrades</td><td><span class="a2cm-check"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"></span> Scales well; for billions of files, enable file notification mode</td></tr>
# MAGIC     <tr><td>Exactly-Once Guarantees</td><td><span class="a2cm-partial"><img src="../Includes/images/icons/warning-triangle.png" height="16" style="vertical-align:middle;"></span> State tables</td><td><span class="a2cm-check"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"></span> RocksDB checkpoint</td></tr>
# MAGIC     <tr><td>Schema Inference</td><td><span class="a2cm-partial"><img src="../Includes/images/icons/warning-triangle.png" height="16" style="vertical-align:middle;"></span> Limited (INFER_SCHEMA option)</td><td><span class="a2cm-check"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"></span> Automatic</td></tr>
# MAGIC     <tr><td>Schema Evolution</td><td><span class="a2cm-partial"><img src="../Includes/images/icons/warning-triangle.png" height="16" style="vertical-align:middle;"></span> Limited mergeSchema</td><td><span class="a2cm-check"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"></span> Full + _rescued_data</td></tr>
# MAGIC     <tr><td>Compute Isolation</td><td>Runs on warehouse</td><td><span class="a2cm-check"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"></span> Serverless pipeline</td></tr>
# MAGIC     <tr><td>Watermarks Required?</td><td>N/A &mdash; not a streaming construct</td><td><span class="a2cm-check"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"></span> Not for simple ingestion; required only for stateful operations (windowed aggregations, stream-stream joins)</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">File Discovery and Scale</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>COPY INTO</strong> scans the <u>entire directory</u> on every run and tracks loaded files via internal state tables &mdash; this becomes expensive as file counts grow into the tens of thousands</li>
# MAGIC           <li><strong>Auto Loader</strong> defaults to <u>directory listing mode</u>, which scales well to millions of files. For billions of files, explicitly enable <u>file notification mode</u> (<code>cloudFiles.useNotifications = true</code>) for constant-time file discovery</li>
# MAGIC           <li>The difference is architectural: COPY INTO is like manually checking a mailbox and crossing off letters. Auto Loader is a digital mailroom that scans every delivery automatically and keeps a tamper-proof log</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Exactly-Once and Compute Isolation</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Both provide <strong>exactly-once processing</strong> &mdash; the difference is robustness. Auto Loader&#39;s RocksDB checkpoint is <u>more resilient at scale</u> than COPY INTO&#39;s file-tracking state</li>
# MAGIC           <li>COPY INTO runs on the <u>calling warehouse</u>. Auto Loader runs on a <u>serverless SDP pipeline</u> &mdash; separate compute that doesn&#39;t compete with your queries</li>
# MAGIC           <li>&#x25C6; At <a href="https://www.databricks.com/customers/block/delta-live-tables">Block</a>, switching to Auto Loader with streaming tables reduced pipeline development from <strong>days to hours</strong> &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Schema Inference, Evolution, and Rescued Data</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>COPY INTO offers limited <code>mergeSchema</code> and an <code>INFER_SCHEMA</code> option, but <u>no automatic schema evolution</u> and <u>no rescue column</u> &mdash; schema management remains largely manual</li>
# MAGIC           <li>Auto Loader provides <strong>full automatic inference</strong>, <strong>automatic evolution</strong> (new columns added without <code>ALTER TABLE</code>), and the <code>_rescued_data</code> column for mismatched fields</li>
# MAGIC           <li>A major real estate technology company experienced columns being silently dropped during COPY INTO ingestion &mdash; a data quality issue that <code>_rescued_data</code> would have prevented</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Watermarks and When to Still Use COPY INTO</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>Common misconception: "Streaming Tables require watermarks." <strong>False</strong> for file ingestion &mdash; watermarks are only needed for stateful operations (windowed aggregations, stream-stream joins)</li>
# MAGIC           <li>COPY INTO is <strong>not deprecated</strong> and remains useful for: small-scale ad-hoc loads, re-processing specific file subsets, or environments without serverless compute</li>
# MAGIC           <li>For <u>production ingestion pipelines</u>, Auto Loader via Streaming Tables is the recommended path forward</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Quality Expectations on Streaming Tables

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC Raw data is messy &mdash; null IDs, negative prices, dates from the future. Without guardrails, bad records flow into Silver and Gold, causing broken dashboards and wrong business decisions. **Expectations** let you declare quality rules directly in the `CREATE STREAMING TABLE` statement, so validation happens inline as data is ingested &mdash; no separate audit job needed.
# MAGIC
# MAGIC <!-- ── Visual: flip-cards + interactive flow diagram ── -->
# MAGIC
# MAGIC #### Click each card to see the behavior and highlight the flow path
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <div class="dq-flip-wrap">
# MAGIC <style>
# MAGIC .dq-flip-wrap { max-width: 1100px; margin: 0 auto; font-family: system-ui, -apple-system, sans-serif; }
# MAGIC .dq-flip-row { display: flex; gap: 16px; justify-content: center; perspective: 1200px; }
# MAGIC .dq-flip-card { width: 320px; height: 220px; perspective: 1000px; cursor: pointer; }
# MAGIC .dq-flip-inner { position: relative; width: 100%; height: 100%; transition: transform 0.6s cubic-bezier(0.4,0,0.2,1); transform-style: preserve-3d; }
# MAGIC .dq-flip-card.dq-flipped .dq-flip-inner { transform: rotateY(180deg); }
# MAGIC .dq-flip-front, .dq-flip-back { position: absolute; top: 0; left: 0; width: 100%; height: 100%; backface-visibility: hidden; -webkit-backface-visibility: hidden; border-radius: 10px; box-sizing: border-box; }
# MAGIC .dq-flip-front { background: #F9F7F4; border: 2px solid #E8E3DC; box-shadow: 0 2px 8px rgba(27,49,57,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 18px 16px; }
# MAGIC .dq-flip-front-title { font-size: 17pt; font-weight: 700; color: #fff; padding: 6px 18px; border-radius: 6px; margin-bottom: 12px; }
# MAGIC .dq-flip-front-code { background: #1B3139; color: #f5f5f5; border-radius: 6px; padding: 10px 14px; font-family: monospace; font-size: 11pt; line-height: 1.6; text-align: left; width: 100%; box-sizing: border-box; }
# MAGIC .dq-flip-front-hint { font-size: 10pt; color: #999; margin-top: 10px; }
# MAGIC .dq-flip-back { transform: rotateY(180deg); color: #fff; padding: 18px 18px; display: flex; flex-direction: column; justify-content: flex-start; border-radius: 10px; }
# MAGIC .dq-flip-back-title { font-size: 14pt; font-weight: 700; margin-bottom: 8px; }
# MAGIC .dq-flip-back-text { font-size: 12pt; line-height: 1.55; }
# MAGIC .dq-flip-back-text ul { margin: 4px 0 0; padding-left: 16px; }
# MAGIC .dq-flip-back-text li { margin-bottom: 4px; }
# MAGIC .dq-kw { color: #7ecbf5; font-weight: 600; }
# MAGIC .dq-obj { color: #dcdcaa; }
# MAGIC /* SVG highlight states */
# MAGIC .dq-svg-dim { opacity: 0.15; transition: opacity 0.4s; }
# MAGIC .dq-svg-bright { opacity: 1; transition: opacity 0.4s; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="dq-flip-row">
# MAGIC   <!-- Warn -->
# MAGIC   <div class="dq-flip-card" data-dq="warn" onclick="dqFlip(this,'warn')">
# MAGIC     <div class="dq-flip-inner">
# MAGIC       <div class="dq-flip-front">
# MAGIC         <div class="dq-flip-front-title" style="background:#2574B5;">Warn (Default)</div>
# MAGIC         <div class="dq-flip-front-code"><span class="dq-kw">CONSTRAINT</span> <span class="dq-obj">valid_amount</span><br/><span class="dq-kw">EXPECT</span> (amount &gt; 0)</div>
# MAGIC         <div class="dq-flip-front-hint">Click to flip</div>
# MAGIC       </div>
# MAGIC       <div class="dq-flip-back" style="background:#2574B5;">
# MAGIC         <div class="dq-flip-back-title">Warn &mdash; Behavior</div>
# MAGIC         <div class="dq-flip-back-text">Invalid records are <strong>written to the target</strong>. Violations recorded in event_log.<ul><li><strong>Best for:</strong> Quality monitoring, non-critical fields</li><li><strong>Scenario:</strong> Track issues for the source team without blocking dashboards</li></ul></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Drop -->
# MAGIC   <div class="dq-flip-card" data-dq="drop" onclick="dqFlip(this,'drop')">
# MAGIC     <div class="dq-flip-inner">
# MAGIC       <div class="dq-flip-front">
# MAGIC         <div class="dq-flip-front-title" style="background:#E5A100;">Drop</div>
# MAGIC         <div class="dq-flip-front-code"><span class="dq-kw">CONSTRAINT</span> <span class="dq-obj">valid_amount</span><br/><span class="dq-kw">EXPECT</span> (amount &gt; 0)<br/><span class="dq-kw">ON VIOLATION DROP ROW</span></div>
# MAGIC         <div class="dq-flip-front-hint">Click to flip</div>
# MAGIC       </div>
# MAGIC       <div class="dq-flip-back" style="background:#C48600;">
# MAGIC         <div class="dq-flip-back-title">Drop &mdash; Behavior</div>
# MAGIC         <div class="dq-flip-back-text">Invalid records are <strong>dropped before written</strong> to target.<ul><li><strong>Best for:</strong> Test records, known bad patterns, duplicates</li><li><strong>Scenario:</strong> Filter test orders with amount = 0 before the dashboard</li></ul></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Fail -->
# MAGIC   <div class="dq-flip-card" data-dq="fail" onclick="dqFlip(this,'fail')">
# MAGIC     <div class="dq-flip-inner">
# MAGIC       <div class="dq-flip-front">
# MAGIC         <div class="dq-flip-front-title" style="background:#D93025;">Fail</div>
# MAGIC         <div class="dq-flip-front-code"><span class="dq-kw">CONSTRAINT</span> <span class="dq-obj">valid_amount</span><br/><span class="dq-kw">EXPECT</span> (amount &gt; 0)<br/><span class="dq-kw">ON VIOLATION FAIL UPDATE</span></div>
# MAGIC         <div class="dq-flip-front-hint">Click to flip</div>
# MAGIC       </div>
# MAGIC       <div class="dq-flip-back" style="background:#B71C1C;">
# MAGIC         <div class="dq-flip-back-title">Fail &mdash; Behavior</div>
# MAGIC         <div class="dq-flip-back-text">Invalid records <strong>prevent the update</strong>. Pipeline halts.<ul><li><strong>Best for:</strong> Financial data, regulatory reporting</li><li><strong>Scenario:</strong> Negative revenue stops everything &mdash; wrong &gt; late</li></ul></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <!-- Pipeline Expectations Flow Graph with highlight paths -->
# MAGIC <div style="max-width: 960px; margin: 0 auto; font-family: system-ui, -apple-system, sans-serif;">
# MAGIC <div style="font-size: 15pt; font-weight: 700; color: #1B3139; text-align: center; margin-bottom: 12px;">Pipeline Expectations Flow Graph</div>
# MAGIC <svg id="dq-flow-svg" viewBox="0 0 920 310" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:920px;display:block;margin:0 auto;">
# MAGIC   <defs>
# MAGIC     <marker id="dqah" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 10 3.5, 0 7" fill="#333"/></marker>
# MAGIC     <marker id="dqahd" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto"><polygon points="0 0, 10 3.5, 0 7" fill="#888"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- Shared path: Raw Data -> Expectation -> Fail -> Action -->
# MAGIC   <g class="dq-path-shared">
# MAGIC     <rect x="10" y="105" width="100" height="40" rx="5" fill="#e8e8e8" stroke="#999" stroke-width="2"/>
# MAGIC     <text x="60" y="130" text-anchor="middle" font-size="13" font-weight="600" fill="#333">Raw Data</text>
# MAGIC     <line x1="110" y1="125" x2="168" y2="125" stroke="#333" stroke-width="2" marker-end="url(#dqah)"/>
# MAGIC     <polygon points="240,50 310,125 240,200 170,125" fill="#FFF3C4" stroke="#C8A415" stroke-width="2"/>
# MAGIC     <text x="240" y="130" text-anchor="middle" font-size="14" font-weight="700" fill="#333">Expectation</text>
# MAGIC     <line x1="275" y1="163" x2="380" y2="195" stroke="#333" stroke-width="2" marker-end="url(#dqah)"/>
# MAGIC     <text x="315" y="192" font-size="12" font-weight="600" fill="#555">Fail</text>
# MAGIC     <polygon points="440,155 490,200 440,245 390,200" fill="#FFF3C4" stroke="#C8A415" stroke-width="2"/>
# MAGIC     <text x="440" y="205" text-anchor="middle" font-size="12" font-weight="700" fill="#333">Action</text>
# MAGIC   </g>
# MAGIC   <!-- Pass path (always visible) -->
# MAGIC   <g class="dq-path-pass">
# MAGIC     <line x1="275" y1="88" x2="420" y2="55" stroke="#333" stroke-width="2" marker-end="url(#dqah)"/>
# MAGIC     <text x="340" y="58" font-size="12" font-weight="600" fill="#555">Pass</text>
# MAGIC     <rect x="422" y="30" width="130" height="42" rx="5" fill="#C8F7C5" stroke="#5CB85C" stroke-width="2"/>
# MAGIC     <text x="487" y="56" text-anchor="middle" font-size="13" font-weight="600" fill="#2D6A2E">Keep Record</text>
# MAGIC     <line x1="552" y1="51" x2="740" y2="100" stroke="#333" stroke-width="2" marker-end="url(#dqah)"/>
# MAGIC   </g>
# MAGIC   <!-- Continue Processing (always visible) -->
# MAGIC   <g class="dq-path-continue">
# MAGIC     <rect x="742" y="85" width="160" height="50" rx="5" fill="#C8F7C5" stroke="#5CB85C" stroke-width="2"/>
# MAGIC     <text x="822" y="108" text-anchor="middle" font-size="13" font-weight="600" fill="#2D6A2E">Continue</text>
# MAGIC     <text x="822" y="125" text-anchor="middle" font-size="13" font-weight="600" fill="#2D6A2E">Processing</text>
# MAGIC   </g>
# MAGIC   <!-- WARN path -->
# MAGIC   <g class="dq-path-warn">
# MAGIC     <line x1="468" y1="173" x2="540" y2="140" stroke="#333" stroke-width="2" marker-end="url(#dqah)"/>
# MAGIC     <rect x="542" y="120" width="80" height="36" rx="5" fill="#FFE0B2" stroke="#E5A100" stroke-width="2"/>
# MAGIC     <text x="582" y="143" text-anchor="middle" font-size="12" font-weight="600" fill="#7A5800">Warn</text>
# MAGIC     <path d="M 582 120 Q 582 80, 530 60" fill="none" stroke="#888" stroke-width="2" stroke-dasharray="6,4" marker-end="url(#dqahd)"/>
# MAGIC   </g>
# MAGIC   <!-- DROP path -->
# MAGIC   <g class="dq-path-drop">
# MAGIC     <line x1="490" y1="200" x2="540" y2="200" stroke="#333" stroke-width="2" marker-end="url(#dqah)"/>
# MAGIC     <rect x="542" y="182" width="80" height="36" rx="5" fill="#FFCDD2" stroke="#E57373" stroke-width="2"/>
# MAGIC     <text x="582" y="205" text-anchor="middle" font-size="12" font-weight="600" fill="#C62828">Drop</text>
# MAGIC     <path d="M 622 200 Q 680 180, 740 140" fill="none" stroke="#888" stroke-width="2" stroke-dasharray="6,4" marker-end="url(#dqahd)"/>
# MAGIC   </g>
# MAGIC   <!-- FAIL path -->
# MAGIC   <g class="dq-path-fail">
# MAGIC     <line x1="440" y1="245" x2="440" y2="268" stroke="#333" stroke-width="2" marker-end="url(#dqah)"/>
# MAGIC     <rect x="380" y="270" width="120" height="36" rx="5" fill="#FFCDD2" stroke="#D32F2F" stroke-width="2"/>
# MAGIC     <text x="440" y="293" text-anchor="middle" font-size="12" font-weight="600" fill="#B71C1C">Fail Flow</text>
# MAGIC   </g>
# MAGIC </svg>
# MAGIC <div style="font-size: 12pt; color: #666; text-align: center; margin-top: 8px; font-style: italic;">Source: <a href="https://docs.databricks.com/aws/en/ldp/expectations" style="color: #2574B5;">Databricks Expectations Documentation</a></div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var dqActive = null;
# MAGIC function dqFlip(card, mode) {
# MAGIC   var wasFlipped = card.classList.contains('dq-flipped');
# MAGIC   // Reset all cards
# MAGIC   document.querySelectorAll('.dq-flip-card').forEach(function(c) { c.classList.remove('dq-flipped'); });
# MAGIC   // Toggle clicked card
# MAGIC   if (!wasFlipped) { card.classList.add('dq-flipped'); dqActive = mode; } else { dqActive = null; }
# MAGIC   // Highlight SVG paths
# MAGIC   var paths = ['warn','drop','fail'];
# MAGIC   if (dqActive) {
# MAGIC     paths.forEach(function(p) {
# MAGIC       var el = document.querySelector('.dq-path-' + p);
# MAGIC       if (el) el.style.opacity = (p === dqActive) ? '1' : '0.12';
# MAGIC     });
# MAGIC     // Keep shared, pass, continue visible
# MAGIC     document.querySelector('.dq-path-shared').style.opacity = '1';
# MAGIC     document.querySelector('.dq-path-pass').style.opacity = (dqActive === 'warn') ? '1' : '0.12';
# MAGIC     document.querySelector('.dq-path-continue').style.opacity = (dqActive !== 'fail') ? '1' : '0.12';
# MAGIC   } else {
# MAGIC     // Reset all to full opacity
# MAGIC     document.querySelectorAll('[class^="dq-path-"]').forEach(function(g) { g.style.opacity = '1'; });
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">How Expectations Work</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Inline validation</strong> &mdash; expectations are evaluated per record as data flows through the pipeline, not as a separate step after ingestion</li>
# MAGIC           <li>The <code>EXPECT</code> clause takes any <u>deterministic SQL Boolean expression</u> &mdash; column checks, range validations, <code>IS NOT NULL</code>, date comparisons, regex patterns, or combinations with <code>AND</code> / <code>OR</code></li>
# MAGIC           <li>You can stack <strong>multiple constraints</strong> on a single table. Each is evaluated independently &mdash; a row can fail one constraint and pass another</li>
# MAGIC           <li>Constraints are <strong>not</strong> allowed in <code>@append_flow</code> definitions &mdash; define them on the target streaming table instead</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Choosing the Right Violation Action</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Warn (default)</strong> &mdash; the gentlest option. Bad records are written to the table, but violation counts are logged. Use this for <u>observability without enforcement</u> &mdash; you want to see what is wrong before deciding how to handle it</li>
# MAGIC           <li><strong>DROP ROW</strong> &mdash; the practical production choice. Bad records are silently removed and violation metrics are tracked. Use this when you can tolerate losing a few bad rows (e.g., null customer IDs on an orders table)</li>
# MAGIC           <li><strong>FAIL UPDATE</strong> &mdash; the strictest option. The entire refresh fails on the first bad record. Use this for <u>regulatory or compliance gates</u> where any invalid data is unacceptable &mdash; for example, dates that must fall within a valid range for financial reporting</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Monitoring Expectations</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Violation metrics for <strong>Warn</strong> and <strong>DROP ROW</strong> actions are automatically logged to the pipeline event log and visible in the <strong>Data Quality</strong> tab of the pipeline UI</li>
# MAGIC           <li>You can query the event log directly to build dashboards or configure <strong>SQL Alerts</strong> on violation counts &mdash; for example, alert when more than 5% of records are dropped</li>
# MAGIC           <li><strong>FAIL UPDATE</strong> does not log violation metrics (the pipeline fails before metrics can be captured) &mdash; use the error message to diagnose the failing record</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Expression Rules and Limitations</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>Expressions must be <strong>deterministic</strong> &mdash; built-in SQL functions and operators only. No aggregate functions (<code>SUM</code>, <code>COUNT</code>), no window functions, no subqueries, no custom Python UDFs</li>
# MAGIC           <li>This means expectations validate <strong>individual rows</strong>, not cross-row patterns. For cross-row checks (primary key uniqueness, row count validation, missing record detection), use the <strong>advanced expectation patterns</strong> documented in the Databricks expectation patterns guide</li>
# MAGIC           <li>Expectations are specific to <strong>Streaming Tables</strong> (the <code>CONSTRAINT ... EXPECT</code> syntax). Materialized Views do not support expectations &mdash; quality checks on MVs are handled by validating the upstream tables that feed them</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Materialized Views: Incremental Transformations
# MAGIC
# MAGIC Now that data is landing in Bronze via Streaming Tables, how do you transform it for analytics? Materialized Views are the declarative answer &mdash; pre-computed, incrementally refreshed, and serverless.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. What Is a Materialized View?
# MAGIC
# MAGIC A **Materialized View** is a Unity Catalog managed Delta table that <u>physically stores pre-computed query results</u> &mdash; unlike a regular view, it doesn&#39;t recompute on every query.
# MAGIC
# MAGIC <!-- ── Visual: button-expand-panels ── -->
# MAGIC
# MAGIC #### Select each feature to explore how Materialized Views work
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif">
# MAGIC
# MAGIC <style>
# MAGIC .mv1-row { display: block; white-space: nowrap; font-size: 0; width: 100%; box-sizing: border-box; }
# MAGIC .mv1-box { display: inline-block; width: 23.5%; margin-right: 2%; min-height: 80px; background: #F9F7F4; border-top: 8px solid transparent; border-left: 2px solid transparent; border-right: 2px solid transparent; border-bottom: 2px solid transparent; border-radius: 8px; padding: 12px 8px; text-align: center; box-sizing: border-box; cursor: pointer; user-select: none; vertical-align: top; transition: transform 0.12s; }
# MAGIC .mv1-box:last-child { margin-right: 0; }
# MAGIC .mv1-box:hover { transform: translateY(-2px); }
# MAGIC .mv1-box.mv1-active { background: #fff; border-left-color: var(--mv1-c); border-right-color: var(--mv1-c); border-bottom-color: var(--mv1-c); }
# MAGIC .mv1-label { display: block; font-size: 14pt; font-weight: 700; color: #0b2026; line-height: 1.3; white-space: normal; pointer-events: none; }
# MAGIC .mv1-detail-wrap { overflow: hidden; max-height: 0; opacity: 0; transition: max-height 0.35s ease, opacity 0.28s ease, margin-top 0.28s ease; margin-top: 0; }
# MAGIC .mv1-detail-wrap.mv1-open { max-height: 1200px; opacity: 1; margin-top: 12px; }
# MAGIC .mv1-detail-card { background: #F9F7F4; border-radius: 10px; padding: 20px; box-sizing: border-box; border-top: 7px solid #ccc; }
# MAGIC .mv1-can-block { border-left: 4px solid; border-radius: 0 6px 6px 0; padding: 14px 14px; margin-bottom: 14px; font-size: 14pt; line-height: 1.6; }
# MAGIC .mv1-can-block ul { margin: 8px 0 0 0; padding-left: 20px; }
# MAGIC .mv1-can-block li { margin-bottom: 8px; }
# MAGIC .mv1-example-block { border-left: 4px solid; border-radius: 0 6px 6px 0; padding: 12px 14px; font-size: 14pt; line-height: 1.5; }
# MAGIC .mv1-example-block pre { background: #1B3139; color: #e0e0e0; border-radius: 6px; padding: 12px 14px; font-family: monospace; font-size: 14pt; line-height: 1.6; margin: 8px 0 0 0; white-space: pre; overflow-x: auto; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="background: #F8F9FC; border: 3px solid #1B5162; border-radius: 10px; padding: 24px; box-sizing: border-box">
# MAGIC
# MAGIC   <div class="mv1-row">
# MAGIC     <span class="mv1-box mv1-active" data-mv1-id="0" onclick="mv1Sel(0)" style="--mv1-c:#2574B5; border-top-color:#2574B5">
# MAGIC       <span class="mv1-label"><img src="../Includes/images/icons/databricks-sql-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> The SQL</span>
# MAGIC     </span><span class="mv1-box" data-mv1-id="1" onclick="mv1Sel(1)" style="--mv1-c:#02A36F; border-top-color:#02A36F">
# MAGIC       <span class="mv1-label"><img src="../Includes/images/icons/cache-sync-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> Incremental Refresh</span>
# MAGIC     </span><span class="mv1-box" data-mv1-id="2" onclick="mv1Sel(2)" style="--mv1-c:#FF5F46; border-top-color:#FF5F46">
# MAGIC       <span class="mv1-label"><img src="../Includes/images/icons/analytics-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> Pre-Computed Results</span>
# MAGIC     </span><span class="mv1-box" data-mv1-id="3" onclick="mv1Sel(3)" style="--mv1-c:#FFAB00; border-top-color:#FFAB00">
# MAGIC       <span class="mv1-label"><img src="../Includes/images/icons/cloud-icon.png" height="36" style="vertical-align:middle;margin-right:6px;"> Serverless</span>
# MAGIC     </span>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="mv1-detail-wrap" id="mv1-detail-wrap">
# MAGIC     <div class="mv1-detail-card" id="mv1-detail-card"></div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var MV1_ITEMS = [
# MAGIC   { title: "The SQL You Write", color: "#2574B5", bg: "rgba(37,116,181,0.10)",
# MAGIC     content: `<strong>One SQL statement</strong> creates a pre-computed, incrementally-refreshed table:<ul><li><code>MATERIALIZED VIEW</code> &mdash; a UC managed Delta table that stores query results physically</li><li>Write standard <strong>batch SQL</strong> (SELECT, JOIN, GROUP BY, window functions) &mdash; the platform incrementalizes it for you</li><li><code>CREATE OR REPLACE</code> executes <u>synchronously</u> &mdash; blocks until the initial data load completes</li><li>Ideal for <strong>Silver-to-Gold transformations</strong>: aggregations, joins, denormalization, business logic</li></ul>`,
# MAGIC     example: `<strong>Create a Materialized View:</strong><pre style="background:#1B3139;color:#e0e0e0;border-radius:6px;padding:12px 14px;font-family:monospace; line-height:1.6;margin:8px 0 0 0;white-space:pre;overflow-x:auto"><span style="color:#7ecbf5;font-weight:600">CREATE OR REPLACE MATERIALIZED VIEW</span>\n  gold_customer_summary\n<span style="color:#7ecbf5;font-weight:600">AS</span>\n<span style="color:#7ecbf5;font-weight:600">SELECT</span>\n  c.customer_id, c.loyalty_tier,\n  <span style="color:#dcdcaa">COUNT</span>(o.order_id) <span style="color:#7ecbf5">AS</span> order_count,\n  <span style="color:#dcdcaa">SUM</span>(o.quantity * o.unit_price) <span style="color:#7ecbf5">AS</span> total_revenue\n<span style="color:#7ecbf5;font-weight:600">FROM</span> silver_orders o\n<span style="color:#7ecbf5;font-weight:600">JOIN</span> silver_customers c\n  <span style="color:#7ecbf5;font-weight:600">ON</span> o.customer_id = c.customer_id\n<span style="color:#7ecbf5;font-weight:600">GROUP BY</span> c.customer_id, c.loyalty_tier;</pre>` },
# MAGIC   { title: "Incremental Refresh", color: "#02A36F", bg: "rgba(2,163,111,0.10)",
# MAGIC     content: `Instead of recomputing the entire result from scratch, an MV detects what changed in source data and <u>processes only the differences</u>.<ul><li><strong>You don&#39;t write incremental logic</strong> &mdash; you write standard batch SQL and the platform incrementalizes it for you</li><li><strong>Supports compositions</strong> of <u>joins, aggregations, and window functions</u> &mdash; see the full list of supported operations &#x2197; (<a href="https://docs.databricks.com/aws/en/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">Azure</a> | <a href="https://docs.databricks.com/gcp/en/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">GCP</a>)</li><li><strong>Automatically chooses the fastest strategy</strong> &mdash; whether that means updating individual rows, overwriting partitions, recomputing aggregates, or appending new results</li><li><strong>Enable these source table properties</strong> &mdash; incremental refresh relies on change tracking. Without them, the platform falls back to full recompute</li></ul>`,
# MAGIC     example: `<strong>For optimal incremental refresh</strong>, enable these on source tables:<pre style="background:#1B3139;color:#e0e0e0;border-radius:6px;padding:12px 14px;font-family:monospace; line-height:1.6;margin:8px 0 0 0;white-space:pre;overflow-x:auto"><span style="color:#7ecbf5;font-weight:600">ALTER TABLE</span> silver_orders <span style="color:#7ecbf5;font-weight:600">SET TBLPROPERTIES</span> (\n  <span style="color:#ce9178">&#39;delta.enableChangeDataFeed&#39;</span> = <span style="color:#ce9178">&#39;true&#39;</span>,\n  <span style="color:#ce9178">&#39;delta.enableRowTracking&#39;</span> = <span style="color:#ce9178">&#39;true&#39;</span>,\n  <span style="color:#ce9178">&#39;delta.enableDeletionVectors&#39;</span> = <span style="color:#ce9178">&#39;true&#39;</span>\n);</pre>` },
# MAGIC   { title: "Pre-Computed Results", color: "#FF5F46", bg: "rgba(255,95,70,0.10)",
# MAGIC     content: `A regular view recomputes on every query. An MV <u>stores results as a physical Delta table</u> &mdash; queries read from that table instead of scanning raw sources.<ul><li><strong>Regular VIEW</strong> &mdash; recomputes the full query every time. 10 dashboard refreshes = 10 full scans</li><li><strong>Materialized VIEW</strong> &mdash; reads stored results. Cost = periodic refresh + minimal read per query</li><li><strong>Best for dashboards and BI</strong> &mdash; any query that runs repeatedly against large tables benefits from pre-computation. Replace the view backing your dashboard and queries go from minutes to seconds</li></ul><div style="text-align:center;padding:10px 16px;background:#fff;border:1px solid #e0e0e0;border-radius:6px; margin-top:14px"><strong>Breakeven:</strong> In typical scenarios, after just a few queries between refreshes the MV pays for itself</div>`,
# MAGIC     example: `<strong>Real-world impact:</strong><ul style="margin:8px 0 0 0;padding-left:20px"><li>&#x25C6; <a href="https://www.databricks.com/customers/anyclip" style="color:#FF5F46;font-weight:600">AnyClip</a> &mdash; replacing static views with MVs delivered a <strong>98% improvement in query performance</strong>. Queries over terabytes dropped from hours to 0.5&ndash;3 minutes.</li><li>&#x25C6; <a href="https://www.databricks.com/customers/comcast" style="color:#FF5F46;font-weight:600">Comcast</a> &mdash; pre-computing results from billions of interactions achieved a <strong>10x reduction in compute costs</strong>.</li></ul>` },
# MAGIC   { title: "Serverless Execution", color: "#FFAB00", bg: "rgba(255,171,0,0.10)",
# MAGIC     content: `All MV refresh compute runs on <strong>serverless infrastructure</strong> &mdash; the platform provisions resources, executes the refresh, and scales automatically.<li><strong>Automatic scaling</strong> &mdash; the serverless pipeline <u>right-sizes compute</u> for each refresh</li><li><strong>Zero infrastructure</strong> &mdash; <u>no clusters</u> to configure, start, or monitor</li><li><strong>Unity Catalog governed</strong> &mdash; every MV is a UC managed Delta table with <u>permissions, tags, lineage, and audit logging</u></li></ul>`,
# MAGIC     example: `<strong>Bottom line:</strong> You write a SELECT statement. The platform stores results, refreshes incrementally, scales compute, and governs the output &mdash; all serverless.` }
# MAGIC ];
# MAGIC var mv1Current = null;
# MAGIC function mv1Sel(id) {
# MAGIC   var wrap = document.getElementById("mv1-detail-wrap");
# MAGIC   var card = document.getElementById("mv1-detail-card");
# MAGIC   var c = MV1_ITEMS[id];
# MAGIC   document.querySelectorAll(".mv1-box").forEach(function(b) { b.classList.toggle("mv1-active", parseInt(b.dataset.mv1Id,10)===id); });
# MAGIC   if (mv1Current === id) { wrap.classList.remove("mv1-open"); document.querySelectorAll(".mv1-box").forEach(function(b){b.classList.remove("mv1-active");}); mv1Current=null; return; }
# MAGIC   mv1Current = id;
# MAGIC   card.style.borderTopColor = c.color;
# MAGIC   card.innerHTML = '<div style="font-size:18pt;font-weight:700;margin-bottom:12px;color:#0b2026;">'+c.title+'</div><div class="mv1-can-block" style="background:'+c.bg+';border-color:'+c.color+'">'+c.content+'</div><div class="mv1-example-block" style="background:'+c.bg+';border-color:'+c.color+'">'+c.example+'</div>';
# MAGIC   wrap.classList.add("mv1-open");
# MAGIC }
# MAGIC mv1Sel(0);
# MAGIC </script>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">The SQL You Write</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>A <strong>Materialized View</strong> is a Unity Catalog managed Delta table that physically stores pre-computed query results &mdash; it appears in Catalog Explorer with full governance, lineage, and permissions</li>
# MAGIC           <li>You write standard batch SQL &mdash; <code>SELECT</code>, <code>JOIN</code>, <code>GROUP BY</code>, window functions &mdash; and the engine incrementalizes it for you. No incremental logic needed</li>
# MAGIC           <li>When you run <code>CREATE OR REPLACE MATERIALIZED VIEW</code>, the data is loaded immediately &mdash; the MV is populated and ready to query as soon as the command finishes</li>
# MAGIC           <li>Ideal for Silver-to-Gold transformations: aggregations, joins, denormalization, and business logic that powers dashboards, Genie Spaces, and BI tools</li>
# MAGIC           <li>The MV <strong>stores a physical copy of the results</strong> &mdash; this is the trade-off: more storage for dramatically faster queries. It is not a cached view; it is a real Delta table</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Incremental Refresh &mdash; How It Works</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Think of an MV as a <strong>pre-built report that sits on a shelf</strong> &mdash; you don&#39;t rebuild it from scratch each time someone asks for it; you just update the pages that changed</li>
# MAGIC           <li>The engine (called <strong>Enzyme</strong>) detects what changed in source tables and processes only the differences &mdash; automatically choosing the fastest strategy (updating rows, overwriting partitions, recomputing aggregates, or appending)</li>
# MAGIC           <li><code>AUTO</code> is the default refresh policy &mdash; the platform decides the cheapest way to refresh. Small changes get incremental; large changes may trigger a full recompute</li>
# MAGIC           <li><strong>What supports incremental refresh:</strong> all JOIN types, GROUP BY, UNION ALL, WHERE/HAVING/QUALIFY, window functions with PARTITION BY, CTEs, and subqueries</li>
# MAGIC           <li><strong>What does NOT:</strong> non-deterministic functions (<code>UUID()</code>, <code>RANDOM()</code>), data sources with row filters or column masks, foreign catalogs, and volumes</li>
# MAGIC           <li>For optimal incremental refresh, enable <code>delta.enableChangeDataFeed</code>, <code>delta.enableRowTracking</code>, and <code>delta.enableDeletionVectors</code> on source tables. Without these, the platform falls back to full recompute</li>
# MAGIC           <li><strong>Incremental refresh requires serverless compute</strong> &mdash; classic compute always does full recompute regardless of policy</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Pre-Computed Results vs Regular Views</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>A regular <code>VIEW</code> recomputes the full query on every access &mdash; 10 dashboard refreshes = 10 full scans of your source tables</li>
# MAGIC           <li>A <strong>Materialized View</strong> reads stored Delta results &mdash; cost = periodic refresh + minimal read per query. The breakeven is typically just a few queries</li>
# MAGIC           <li><strong>You can query an MV while it&#39;s refreshing</strong> &mdash; queries see the last-completed refresh state until the new refresh finishes. No downtime</li>
# MAGIC           <li>&#x25C6; At <a href="https://www.databricks.com/customers/anyclip">AnyClip</a>, replacing static views with MVs delivered a <strong>98% improvement in query performance</strong> &mdash; queries over terabytes dropped from hours to 0.5&ndash;3 minutes</li>
# MAGIC           <li>&#x25C6; At <a href="https://www.databricks.com/customers/comcast">Comcast</a>, pre-computing results from billions of interactions achieved a <strong>10x reduction in compute costs</strong></li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Serverless Execution</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>All MV compute runs on a <strong>serverless Lakeflow SDP pipeline</strong> &mdash; the SQL warehouse dispatches the command, the pipeline does the heavy lifting independently</li>
# MAGIC           <li><strong>Separate compute</strong> &mdash; refresh processing runs on the pipeline, not on your warehouse</li>
# MAGIC           <li><strong>Automatic scaling</strong> &mdash; the serverless pipeline right-sizes compute for each refresh, no cluster configuration needed</li>
# MAGIC           <li><strong>Unity Catalog governed</strong> &mdash; every MV is a UC managed Delta table with permissions, tags, lineage, and audit logging from the moment it is created</li>
# MAGIC           <li><strong>Scheduled refresh</strong> creates a managed Lakeflow Job automatically &mdash; visible in the Jobs &amp; Pipelines UI. You can also use <code>TRIGGER ON UPDATE</code> for event-driven refresh</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>
# MAGIC <script>  
# MAGIC <!-- Retry any images that failed to render on initial load (500ms and 1500ms delays -->
# MAGIC [500, 1500].forEach(function(delay) {
# MAGIC     setTimeout(function() {
# MAGIC         document.querySelectorAll('.two-pane img').forEach(function(img) {
# MAGIC             if (!img.complete || img.naturalWidth === 0) {                                                                                                                                
# MAGIC                 img.src = img.src;
# MAGIC             }                                                                                                                                                                             
# MAGIC         });     
# MAGIC     }, delay);
# MAGIC });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. View vs. MV vs. Table &mdash; The Analytics Engineer's Decision
# MAGIC
# MAGIC This is the decision you will make dozens of times when building a data model. Each choice is valid &mdash; the skill is matching the right type to the requirement.
# MAGIC
# MAGIC <!-- ── Visual: three-column decision cards ── -->
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
# MAGIC <div style="display: flex; gap: 16px; flex-wrap: wrap;">
# MAGIC
# MAGIC   <!-- VIEW card -->
# MAGIC   <div style="flex: 1; min-width: 280px; border: 2px solid #607d8b; border-radius: 12px; overflow: hidden;">
# MAGIC     <div style="background: #607d8b; color: white; padding: 14px 18px; text-align: center;">
# MAGIC       <div style="font-size: 16pt; font-weight: 700;">Use a VIEW when</div>
# MAGIC     </div>
# MAGIC     <div style="padding: 16px 18px; background: #FAFAFA; font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC       <ul style="margin: 0; padding-left: 20px;">
# MAGIC         <li>Transforms are <strong>cheap</strong> (filter, rename, cast)</li>
# MAGIC         <li>Data changes <strong>rarely</strong></li>
# MAGIC         <li><strong>Few consumers</strong> query the result</li>
# MAGIC         <li>You need <strong>always-current</strong> results (zero staleness)</li>
# MAGIC         <li>You want <strong>zero storage cost</strong></li>
# MAGIC       </ul>
# MAGIC       <div style="background: #1B3139; border-radius: 6px; padding: 10px 12px; font-family: monospace; font-size: 13pt; line-height: 1.6; color: #e0e0e0; margin-top: 12px;">
# MAGIC         <span style="color:#7ecbf5;font-weight:600">CREATE VIEW</span> silver_orders_clean<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">AS SELECT</span> order_id,<br/>
# MAGIC         &nbsp;&nbsp;<span style="color:#dcdcaa">CAST</span>(order_date <span style="color:#7ecbf5">AS</span> DATE),<br/>
# MAGIC         &nbsp;&nbsp;quantity * unit_price <span style="color:#7ecbf5">AS</span> amount<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">FROM</span> bronze_orders<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">WHERE</span> status != 'cancelled';
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- MV card -->
# MAGIC   <div style="flex: 1; min-width: 280px; border: 2px solid #02A36F; border-radius: 12px; overflow: hidden;">
# MAGIC     <div style="background: #02A36F; color: white; padding: 14px 18px; text-align: center;">
# MAGIC       <div style="font-size: 16pt; font-weight: 700;">Use a MATERIALIZED VIEW when</div>
# MAGIC     </div>
# MAGIC     <div style="padding: 16px 18px; background: #FAFAFA; font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC       <ul style="margin: 0; padding-left: 20px;">
# MAGIC         <li>Transforms are <strong>expensive</strong> (joins, aggregations)</li>
# MAGIC         <li>Data changes <strong>often</strong></li>
# MAGIC         <li><strong>Many consumers</strong> (dashboards, Genie Spaces)</li>
# MAGIC         <li>You can tolerate <strong>minutes-to-hours</strong> of staleness</li>
# MAGIC         <li>You want the platform to <strong>incrementalize</strong> for you</li>
# MAGIC       </ul>
# MAGIC       <div style="background: #1B3139; border-radius: 6px; padding: 10px 12px; font-family: monospace; font-size: 13pt; line-height: 1.6; color: #e0e0e0; margin-top: 12px;">
# MAGIC         <span style="color:#7ecbf5;font-weight:600">CREATE MATERIALIZED VIEW</span><br/>
# MAGIC         &nbsp;&nbsp;gold_customer_summary<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">AS SELECT</span> c.customer_id,<br/>
# MAGIC         &nbsp;&nbsp;<span style="color:#dcdcaa">COUNT</span>(o.order_id) <span style="color:#7ecbf5">AS</span> orders,<br/>
# MAGIC         &nbsp;&nbsp;<span style="color:#dcdcaa">SUM</span>(o.amount) <span style="color:#7ecbf5">AS</span> revenue<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">FROM</span> silver_orders o<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">JOIN</span> silver_customers c<br/>
# MAGIC         &nbsp;&nbsp;<span style="color:#7ecbf5;font-weight:600">ON</span> o.customer_id = c.customer_id<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">GROUP BY</span> c.customer_id;
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TABLE card -->
# MAGIC   <div style="flex: 1; min-width: 280px; border: 2px solid #E5A100; border-radius: 12px; overflow: hidden;">
# MAGIC     <div style="background: #E5A100; color: white; padding: 14px 18px; text-align: center;">
# MAGIC       <div style="font-size: 16pt; font-weight: 700;">Use a TABLE (INSERT INTO) when</div>
# MAGIC     </div>
# MAGIC     <div style="padding: 16px 18px; background: #FAFAFA; font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC       <ul style="margin: 0; padding-left: 20px;">
# MAGIC         <li>You need <strong>parameterized logic</strong></li>
# MAGIC         <li>ForEach or <strong>looping patterns</strong></li>
# MAGIC         <li><strong>Custom refresh control</strong> (MERGE, conditional inserts)</li>
# MAGIC         <li>Non-deterministic transforms that <strong>cannot incrementalize</strong></li>
# MAGIC         <li>Covered in <strong>Module 4</strong></li>
# MAGIC       </ul>
# MAGIC       <div style="background: #1B3139; border-radius: 6px; padding: 10px 12px; font-family: monospace; font-size: 13pt; line-height: 1.6; color: #e0e0e0; margin-top: 12px;">
# MAGIC         <span style="color:#7ecbf5;font-weight:600">INSERT INTO</span> gold_daily_report<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">SELECT</span> report_date,<br/>
# MAGIC         &nbsp;&nbsp;region, total_sales<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">FROM</span> silver_sales<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">WHERE</span> report_date =<br/>
# MAGIC         &nbsp;&nbsp;<span style="color:#dcdcaa">CURRENT_DATE</span>() - 1;
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">The MV as a Modeling Primitive</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>A <strong>Materialized View</strong> is not just a performance optimization &mdash; it is a <strong>modeling choice</strong> that declares: "this query defines a business metric, and I want the platform to keep it up to date automatically."</li>
# MAGIC           <li>You write standard batch SQL &mdash; <code>SELECT</code>, <code>JOIN</code>, <code>GROUP BY</code>, window functions &mdash; and the Enzyme engine incrementalizes it for you. No incremental logic needed.</li>
# MAGIC           <li>The MV stores a physical copy of the results as a Delta table. Queries read from that table instead of scanning raw sources. You can query an MV while it is refreshing &mdash; queries see the last-completed refresh state until the new refresh finishes.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Common Modeling Patterns</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Dashboard backing table:</strong> Replace the view behind your BI dashboard with an MV. Queries go from minutes to sub-seconds.</li>
# MAGIC           <li><strong>Genie Space source:</strong> Genie queries an MV for instant responses instead of re-running expensive joins.</li>
# MAGIC           <li><strong>Denormalized Gold table:</strong> Pre-join dimension and fact tables so downstream consumers do not need to write complex SQL.</li>
# MAGIC           <li><strong>Aggregation layer:</strong> Pre-compute SUM, COUNT, AVG at different grain levels so BI tools read aggregated results.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Source Table Properties for Optimal Refresh</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>For optimal incremental refresh, enable these properties on source tables:
# MAGIC             <ul>
# MAGIC               <li><code>delta.enableChangeDataFeed = 'true'</code></li>
# MAGIC               <li><code>delta.enableRowTracking = 'true'</code></li>
# MAGIC               <li><code>delta.enableDeletionVectors = 'true'</code></li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Without these, the platform falls back to full recompute &mdash; still correct, but more expensive.</li>
# MAGIC           <li>Incremental refresh also requires <strong>serverless compute</strong> &mdash; classic compute always does full recompute regardless of policy.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Choosing the Right Tool
# MAGIC
# MAGIC With both Streaming Tables and Materialized Views in your toolkit, the question becomes: which one should you use for a given use case? This section provides a decision framework and peels back the architectural layer they share.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. ST vs MV Decision Framework
# MAGIC
# MAGIC The fundamental distinction is **data semantics**. The right choice depends on three questions:
# MAGIC
# MAGIC <ul style="line-height: 1.7">
# MAGIC <li><strong>Append-only or updates?</strong> &mdash; STs are <u>append-by-default</u>. MVs maintain the full result and <u>reflect source changes</u></li>
# MAGIC <li><strong>Ingestion or transformation?</strong> &mdash; STs use <code>read_files()</code> for <u>file ingestion</u>. MVs use <code>SELECT</code> for <u>joins and aggregations</u></li>
# MAGIC <li><strong>Where in the medallion?</strong> &mdash; Bronze = <u>ST</u> &bull; Gold = <u>MV</u> &bull; Silver = <u>depends</u></li>
# MAGIC </ul>
# MAGIC
# MAGIC #### Click **Yes** or **No** at each step to follow your path
# MAGIC
# MAGIC <!-- ── Visual: decision-tree ── -->
# MAGIC <div class="d1dt-wrap">
# MAGIC <style>
# MAGIC .d1dt-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 920px; margin: 0 auto; padding: 24px; background: #F9F7F4; border: 1px solid #E8E3DC; border-radius: 12px; }
# MAGIC .d1dt-tree { display: flex; flex-direction: column; align-items: center; gap: 0; }
# MAGIC .d1dt-node { background: #FFF; border: 2px solid #E8E3DC; border-radius: 10px; padding: 18px 24px; max-width: 480px; width: 100%; text-align: center; }
# MAGIC .d1dt-node-q { font-size: 15pt; font-weight: 700; color: #0b2026; margin: 0 0 14px; line-height: 1.4; }
# MAGIC .d1dt-node-active { border-color: #2574B5; box-shadow: 0 2px 12px rgba(37,116,181,0.12); }
# MAGIC .d1dt-buttons { display: flex; gap: 12px; justify-content: center; }
# MAGIC .d1dt-btn { padding: 8px 28px; font-size: 14pt; font-weight: 700; border: 2px solid; border-radius: 6px; cursor: pointer; transition: background 0.15s, color 0.15s; background: #FFF; }
# MAGIC .d1dt-btn-yes { color: #02A36F; border-color: #02A36F; }
# MAGIC .d1dt-btn-yes:hover, .d1dt-btn-yes.d1dt-chosen { background: #02A36F; color: #FFF; }
# MAGIC .d1dt-btn-no { color: #E0382B; border-color: #E0382B; }
# MAGIC .d1dt-btn-no:hover, .d1dt-btn-no.d1dt-chosen { background: #E0382B; color: #FFF; }
# MAGIC .d1dt-conn { width: 2px; height: 24px; background: #E8E3DC; margin: 0 auto; }
# MAGIC .d1dt-conn-active { background: #2574B5; }
# MAGIC .d1dt-branch { display: none; flex-direction: column; align-items: center; width: 100%; }
# MAGIC .d1dt-branch.d1dt-vis { display: flex; }
# MAGIC .d1dt-split { display: flex; gap: 24px; width: 100%; justify-content: center; }
# MAGIC .d1dt-split-col { display: flex; flex-direction: column; align-items: center; flex: 1; max-width: 420px; }
# MAGIC .d1dt-leaf { background: #FFF; border-radius: 10px; padding: 18px 24px; min-width: 320px; max-width: 480px; width: 100%; text-align: center; }
# MAGIC .d1dt-leaf-text { font-size: 14pt; font-weight: 600; line-height: 1.5; margin: 0; }
# MAGIC .d1dt-rec-st { border: 2px solid #02A36F; background: rgba(2,163,111,0.04); }
# MAGIC .d1dt-rec-st .d1dt-leaf-text { color: #02A36F; }
# MAGIC .d1dt-rec-st::before { content: 'Streaming Table'; display: block; font-size: 14pt; font-weight: 700; color: #02A36F; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px; }
# MAGIC .d1dt-rec-mv { border: 2px solid #2574B5; background: rgba(37,116,181,0.04); }
# MAGIC .d1dt-rec-mv .d1dt-leaf-text { color: #2574B5; }
# MAGIC .d1dt-rec-mv::before { content: 'Materialized View'; display: block; font-size: 14pt; font-weight: 700; color: #2574B5; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px; }
# MAGIC .d1dt-alt { border: 2px solid #E5A100; background: rgba(229,161,0,0.04); }
# MAGIC .d1dt-alt .d1dt-leaf-text { color: #E5A100; }
# MAGIC .d1dt-alt::before { content: 'Alternative'; display: block; font-size: 14pt; font-weight: 700; color: #E5A100; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px; }
# MAGIC .d1dt-muted { opacity: 0.35; pointer-events: none; }
# MAGIC .d1dt-reset { display: inline-block; margin-top: 20px; padding: 8px 20px; font-size: 14pt; font-weight: 600; color: #1B3139; background: #FFF; border: 1px solid #E8E3DC; border-radius: 6px; cursor: pointer; }
# MAGIC .d1dt-reset:hover { background: #F9F7F4; }
# MAGIC </style>
# MAGIC <div class="d1dt-tree" id="d1dt-tree">
# MAGIC   <div class="d1dt-node d1dt-node-active" id="d1dt-root">
# MAGIC     <p class="d1dt-node-q">Is this ingestion from external cloud storage?</p>
# MAGIC     <div class="d1dt-buttons">
# MAGIC       <button class="d1dt-btn d1dt-btn-yes" onclick="d1dtC('root','yes')">Yes</button>
# MAGIC       <button class="d1dt-btn d1dt-btn-no" onclick="d1dtC('root','no')">No</button>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d1dt-split">
# MAGIC     <div class="d1dt-split-col">
# MAGIC       <div class="d1dt-conn" id="d1dt-conn-root-yes"></div>
# MAGIC       <div class="d1dt-branch" id="d1dt-branch-root-yes">
# MAGIC         <div class="d1dt-node" id="d1dt-node-y1">
# MAGIC           <p class="d1dt-node-q">Is the source append-only (logs, events, sensors)?</p>
# MAGIC           <div class="d1dt-buttons">
# MAGIC             <button class="d1dt-btn d1dt-btn-yes" onclick="d1dtC('y1','yes')">Yes</button>
# MAGIC             <button class="d1dt-btn d1dt-btn-no" onclick="d1dtC('y1','no')">No</button>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="d1dt-split">
# MAGIC           <div class="d1dt-split-col">
# MAGIC             <div class="d1dt-conn" id="d1dt-conn-y1-yes"></div>
# MAGIC             <div class="d1dt-branch" id="d1dt-branch-y1-yes">
# MAGIC               <div class="d1dt-leaf d1dt-rec-st"><p class="d1dt-leaf-text">Streaming Table &mdash; incremental ingestion with Auto Loader</p></div>
# MAGIC             </div>
# MAGIC           </div>
# MAGIC           <div class="d1dt-split-col">
# MAGIC             <div class="d1dt-conn" id="d1dt-conn-y1-no"></div>
# MAGIC             <div class="d1dt-branch" id="d1dt-branch-y1-no">
# MAGIC               <div class="d1dt-leaf d1dt-rec-st"><p class="d1dt-leaf-text">Streaming Table with AUTO CDC &mdash; handles inserts, updates, and deletes (Lecture 3)</p></div>
# MAGIC             </div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d1dt-split-col">
# MAGIC       <div class="d1dt-conn" id="d1dt-conn-root-no"></div>
# MAGIC       <div class="d1dt-branch" id="d1dt-branch-root-no">
# MAGIC         <div class="d1dt-node" id="d1dt-node-n1">
# MAGIC           <p class="d1dt-node-q">Do you need aggregations, joins, or business logic?</p>
# MAGIC           <div class="d1dt-buttons">
# MAGIC             <button class="d1dt-btn d1dt-btn-yes" onclick="d1dtC('n1','yes')">Yes</button>
# MAGIC             <button class="d1dt-btn d1dt-btn-no" onclick="d1dtC('n1','no')">No</button>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="d1dt-split">
# MAGIC           <div class="d1dt-split-col">
# MAGIC             <div class="d1dt-conn" id="d1dt-conn-n1-yes"></div>
# MAGIC             <div class="d1dt-branch" id="d1dt-branch-n1-yes">
# MAGIC               <div class="d1dt-leaf d1dt-rec-mv"><p class="d1dt-leaf-text">Materialized View &mdash; the platform incrementalizes your SQL automatically</p></div>
# MAGIC             </div>
# MAGIC           </div>
# MAGIC           <div class="d1dt-split-col">
# MAGIC             <div class="d1dt-conn" id="d1dt-conn-n1-no"></div>
# MAGIC             <div class="d1dt-branch" id="d1dt-branch-n1-no">
# MAGIC               <div class="d1dt-node" id="d1dt-node-n1n1">
# MAGIC                 <p class="d1dt-node-q">Does source data get updated or deleted?</p>
# MAGIC                 <div class="d1dt-buttons">
# MAGIC                   <button class="d1dt-btn d1dt-btn-yes" onclick="d1dtC('n1n1','yes')">Yes</button>
# MAGIC                   <button class="d1dt-btn d1dt-btn-no" onclick="d1dtC('n1n1','no')">No</button>
# MAGIC                 </div>
# MAGIC               </div>
# MAGIC               <div class="d1dt-split">
# MAGIC                 <div class="d1dt-split-col">
# MAGIC                   <div class="d1dt-conn" id="d1dt-conn-n1n1-yes"></div>
# MAGIC                   <div class="d1dt-branch" id="d1dt-branch-n1n1-yes">
# MAGIC                     <div class="d1dt-leaf d1dt-rec-mv"><p class="d1dt-leaf-text">Materialized View &mdash; reflects updates/deletes from source</p></div>
# MAGIC                   </div>
# MAGIC                 </div>
# MAGIC                 <div class="d1dt-split-col">
# MAGIC                   <div class="d1dt-conn" id="d1dt-conn-n1n1-no"></div>
# MAGIC                   <div class="d1dt-branch" id="d1dt-branch-n1n1-no">
# MAGIC                     <div class="d1dt-leaf d1dt-alt"><p class="d1dt-leaf-text">Either works &mdash; ST for simple append pass-through, MV if you may need to reprocess later</p></div>
# MAGIC                   </div>
# MAGIC                 </div>
# MAGIC               </div>
# MAGIC             </div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <button class="d1dt-reset" onclick="d1dtReset()">Reset</button>
# MAGIC </div>
# MAGIC <script>
# MAGIC function d1dtC(n,c){var y=document.getElementById('d1dt-branch-'+n+'-yes'),x=document.getElementById('d1dt-branch-'+n+'-no'),yc=document.getElementById('d1dt-conn-'+n+'-yes'),xc=document.getElementById('d1dt-conn-'+n+'-no'),nd=document.getElementById('d1dt-node-'+n)||document.getElementById('d1dt-root');if(!y||!x)return;nd.classList.add('d1dt-node-active');if(c==='yes'){y.classList.add('d1dt-vis');x.classList.remove('d1dt-vis');x.classList.add('d1dt-muted');y.classList.remove('d1dt-muted');if(yc)yc.classList.add('d1dt-conn-active');if(xc)xc.classList.remove('d1dt-conn-active');nd.querySelectorAll('.d1dt-btn').forEach(function(b){b.classList.remove('d1dt-chosen')});nd.querySelector('.d1dt-btn-yes').classList.add('d1dt-chosen');}else{x.classList.add('d1dt-vis');y.classList.remove('d1dt-vis');y.classList.add('d1dt-muted');x.classList.remove('d1dt-muted');if(xc)xc.classList.add('d1dt-conn-active');if(yc)yc.classList.remove('d1dt-conn-active');nd.querySelectorAll('.d1dt-btn').forEach(function(b){b.classList.remove('d1dt-chosen')});nd.querySelector('.d1dt-btn-no').classList.add('d1dt-chosen');}}
# MAGIC function d1dtShowAll(){document.querySelectorAll('.d1dt-branch').forEach(function(b){b.classList.add('d1dt-vis');b.classList.remove('d1dt-muted')});document.querySelectorAll('.d1dt-conn').forEach(function(c){c.classList.add('d1dt-conn-active')});document.querySelectorAll('.d1dt-node').forEach(function(n){n.classList.add('d1dt-node-active')});document.querySelectorAll('.d1dt-btn').forEach(function(b){b.classList.remove('d1dt-chosen')});}
# MAGIC function d1dtReset(){document.querySelectorAll('.d1dt-branch').forEach(function(b){b.classList.remove('d1dt-vis','d1dt-muted')});document.querySelectorAll('.d1dt-conn').forEach(function(c){c.classList.remove('d1dt-conn-active')});document.querySelectorAll('.d1dt-node').forEach(function(n){n.classList.remove('d1dt-node-active')});document.getElementById('d1dt-root').classList.add('d1dt-node-active');document.querySelectorAll('.d1dt-btn').forEach(function(b){b.classList.remove('d1dt-chosen')});}
# MAGIC </script>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">Understanding the Core Difference</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Streaming Table:</strong> Each input row is processed exactly once. Once a row is appended, it is never re-queried with later updates. Think of an ST as a conveyor belt &mdash; items go on once and move through in order, never going back.</li>
# MAGIC           <li><strong>Materialized View:</strong> The entire query result is maintained and can be refreshed to reflect changes in source data. Think of an MV as a whiteboard summary &mdash; erased and updated whenever the source material changes.</li>
# MAGIC           <li><strong>Medallion architecture mapping:</strong>
# MAGIC             <ul>
# MAGIC               <li><strong>Bronze:</strong> Streaming Table &mdash; raw ingestion from cloud storage via Auto Loader</li>
# MAGIC               <li><strong>Silver (simple cleaning):</strong> Streaming Table &mdash; append-only transformations, filtering, type casting</li>
# MAGIC               <li><strong>Silver (complex enrichment):</strong> Materialized View &mdash; joins, aggregations, reprocessing</li>
# MAGIC               <li><strong>Gold:</strong> Materialized View &mdash; pre-computed aggregations, BI-ready</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Both are stored as Delta tables in Unity Catalog and backed by serverless Lakeflow SDP pipelines.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Common Misconceptions</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><em>"STs are for streaming, MVs are for batch."</em> &mdash; Oversimplified. STs process each row once (streaming semantics) but can be scheduled like batch. MVs use batch semantics but can be refreshed frequently or triggered on upstream changes.</li>
# MAGIC           <li><em>"STs can do aggregations."</em> &mdash; Technically yes, with stateful streaming (watermarks and windows), but MVs are the simpler and recommended choice for aggregation use cases.</li>
# MAGIC           <li><em>"MVs can replace STs for ingestion."</em> &mdash; No. MVs do not use Auto Loader or <code>read_files()</code>. They cannot ingest from cloud storage the way STs can.</li>
# MAGIC           <li><em>"Use STs everywhere for best performance."</em> &mdash; STs are append-only and see each row once. If you need to reflect source data changes (updates, deletes), you need an MV.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Applying the Framework</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>Walk through the decision tree by asking:
# MAGIC             <ul>
# MAGIC               <li><strong>Is this ingestion from external storage?</strong> &rarr; Streaming Table with Auto Loader</li>
# MAGIC               <li><strong>Is the source append-only?</strong> (logs, events, sensors) &rarr; Streaming Table</li>
# MAGIC               <li><strong>Do you need aggregations, joins, or business logic?</strong> &rarr; Materialized View</li>
# MAGIC               <li><strong>Does source data get updated or deleted?</strong> &rarr; Materialized View (or ST with AUTO CDC for ingestion)</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>For CDC processing (SCD Type 1/2), use a Streaming Table with AUTO CDC flow.</li>
# MAGIC           <li>MVs and STs work together: a common pattern is an MV reading from an ST &mdash; ST for Bronze ingestion, MV for Gold aggregation. An ST can also stream from another ST.</li>
# MAGIC           <li>Rule of thumb: <strong>When in doubt, use MV for transformations and ST for ingestion.</strong></li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">All Decision Paths at a Glance</strong>
# MAGIC         <table style="width:100%;border-collapse:collapse;margin:8px 0 0 0; line-height:1.6">
# MAGIC           <thead><tr style="background:#1B5162;color:#fff;">
# MAGIC             <th style="padding:10px 12px;text-align:center;border:1px solid #1B5162">Ingestion from<br/>cloud storage?</th>
# MAGIC             <th style="padding:10px 12px;text-align:center;border:1px solid #1B5162">Append-only<br/>source?</th>
# MAGIC             <th style="padding:10px 12px;text-align:center;border:1px solid #1B5162">Aggregations,<br/>joins, logic?</th>
# MAGIC             <th style="padding:10px 12px;text-align:center;border:1px solid #1B5162">Source<br/>updates/deletes?</th>
# MAGIC             <th style="padding:10px 12px;text-align:center;border:1px solid #1B5162">Result</th>
# MAGIC           </tr></thead>
# MAGIC           <tbody>
# MAGIC             <tr style="background:#f7fdf4;"><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#02A36F">Yes</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#02A36F">Yes</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;border:1px solid #E8E3DC;color:#02A36F;font-weight:700">Streaming Table</td></tr>
# MAGIC             <tr style="background:#f7fdf4;"><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#02A36F">Yes</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#E0382B">No</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;border:1px solid #E8E3DC;color:#02A36F;font-weight:700">ST + AUTO CDC</td></tr>
# MAGIC             <tr style="background:#f0f7ff;"><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#E0382B">No</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#02A36F">Yes</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;border:1px solid #E8E3DC;color:#2574B5;font-weight:700">Materialized View</td></tr>
# MAGIC             <tr style="background:#f0f7ff;"><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#E0382B">No</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#E0382B">No</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#02A36F">Yes</td><td style="padding:8px 12px;border:1px solid #E8E3DC;color:#2574B5;font-weight:700">Materialized View</td></tr>
# MAGIC             <tr style="background:#fff8e1;"><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#E0382B">No</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;color:#999">&mdash;</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#E0382B">No</td><td style="padding:8px 12px;text-align:center;border:1px solid #E8E3DC;font-weight:600;color:#E0382B">No</td><td style="padding:8px 12px;border:1px solid #E8E3DC;color:#E5A100;font-weight:700">Either works</td></tr>
# MAGIC           </tbody>
# MAGIC         </table>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Serverless Refresh Architecture
# MAGIC
# MAGIC Every Streaming Table and Materialized View you create &mdash; whether from a SQL warehouse or a notebook &mdash; runs on **serverless compute** under the hood. Understanding this architecture explains how the platform delivers zero-infrastructure scaling, automatic incremental refresh via Enzyme, and production-grade reliability from simple SQL statements.
# MAGIC
# MAGIC <!-- ── Visual: stacked-layers ── -->
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
# MAGIC <div style="display: flex; align-items: stretch; gap: 12px">
# MAGIC   <div style="display: flex; flex-direction: column; justify-content: center; align-items: center; padding: 0 4px">
# MAGIC     <div style="width: 3px; flex: 1; background: linear-gradient(180deg, #2574B5, #02A36F, #1B3139); border-radius: 2px"></div>
# MAGIC     <div style="font-size: 22pt; color: #618794; margin-top: 4px;">&#x25BC;</div>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; display: flex; flex-direction: column; gap: 4px">
# MAGIC   <div style="color: white; border-radius: 8px 8px 4px 4px; padding: 14px 24px; text-align: center; background: #2574B5">
# MAGIC     <div style="font-size: 17pt; font-weight: 700;">SQL Interface</div>
# MAGIC     <div style="margin-top: 6px; opacity: 0.9">You run CREATE or REFRESH on a Streaming Table or Materialized View &mdash; everything below happens automatically</div>
# MAGIC   </div>
# MAGIC   <div style="color: white; border-radius: 4px; padding: 14px 24px; text-align: center; background: #1B5162">
# MAGIC     <div style="font-size: 17pt; font-weight: 700;">SQL Warehouse</div>
# MAGIC     <div style="margin-top: 6px; opacity: 0.9">Receives your statement and triggers serverless execution automatically</div>
# MAGIC   </div>
# MAGIC   <div style="border-radius: 4px; padding: 14px 24px; text-align: center; background: #02A36F; color: white">
# MAGIC     <div style="font-size: 17pt; font-weight: 700;">Serverless Compute</div>
# MAGIC     <div style="margin-top: 6px; opacity: 0.9">Scales automatically &mdash; zero infrastructure, zero cluster management</div>
# MAGIC   </div>
# MAGIC   <div style="display: flex; gap: 6px">
# MAGIC     <div style="flex: 1; border-radius: 4px; padding: 14px 20px; background: #00897B; color: white">
# MAGIC       <div style="font-size: 16pt; font-weight: 700; text-align: center;">Auto Loader</div>
# MAGIC       <div style="margin-top: 4px; opacity: 0.85; text-align: center"><u>Powers Streaming Tables</u></div>
# MAGIC       <ul style="margin: 10px 0 0 0; padding-left: 20px; opacity: 0.9; line-height: 1.6">
# MAGIC         <li>File discovery (directory listing or notification)</li>
# MAGIC         <li>RocksDB checkpoint for exactly-once</li>
# MAGIC         <li>Automatic schema evolution</li>
# MAGIC         <li><code style="background:rgba(255,255,255,0.2);padding:1px 4px;border-radius:3px;">_rescued_data</code> for mismatched fields</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="flex: 1; border-radius: 4px; padding: 14px 20px; background: #E5A100; color: white">
# MAGIC       <div style="font-size: 16pt; font-weight: 700; text-align: center;">Enzyme Engine</div>
# MAGIC       <div style="margin-top: 4px; opacity: 0.85; text-align: center"><u>Powers Materialized Views</u></div>
# MAGIC       <ul style="margin: 10px 0 0 0; padding-left: 20px; opacity: 0.9; line-height: 1.6">
# MAGIC         <li>Detects what changed in your source data</li>
# MAGIC         <li>Updates only the affected rows, partitions, or aggregates</li>
# MAGIC         <li>Picks the cheapest strategy automatically</li>
# MAGIC         <li>You will see the technique names in your event log</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="color: white; border-radius: 4px 4px 8px 8px; padding: 14px 24px; text-align: center; background: #1B3139">
# MAGIC     <div style="font-size: 17pt; font-weight: 700;">Unity Catalog Delta Tables</div>
# MAGIC     <div style="margin-top: 6px; opacity: 0.9">Governed storage with row tracking, change data feed, and deletion vectors enabled</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">SQL Interface &rarr; Serverless Execution</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>When you run <code>CREATE STREAMING TABLE</code>, <code>CREATE MATERIALIZED VIEW</code>, or <code>REFRESH</code>, the platform automatically provisions serverless compute to handle the work</li>
# MAGIC           <li>You interact through SQL; the platform manages everything underneath &mdash; compute provisioning, scaling, retries, and teardown</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Serverless Compute</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Every refresh runs on <strong>serverless compute</strong> &mdash; no clusters to configure, start, or monitor. The platform provisions and tears down resources automatically</li>
# MAGIC           <li><strong>Vertical autoscaling</strong> selects cost-efficient instance types; <strong>stream pipelining</strong> runs concurrent microbatches for better throughput (enabled by default)</li>
# MAGIC           <li><strong>Performance modes:</strong> Standard mode (lower cost, 4&ndash;6 minute startup) vs Performance-optimized mode (faster startup, higher throughput &mdash; Beta)</li>
# MAGIC           <li>The platform also handles automatic orchestration, dependency management, retry mechanisms, and event logging &mdash; all managed for you</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Auto Loader and Enzyme &mdash; Two Engines, One Pipeline</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Auto Loader</strong> powers Streaming Tables &mdash; file discovery, RocksDB checkpoint, schema evolution, and exactly-once ingestion all run inside the SDP pipeline</li>
# MAGIC           <li><strong>Enzyme</strong> powers Materialized Views &mdash; it detects what changed in source tables and selects the optimal refresh technique (you will see these in your event log)</li>
# MAGIC           <li>Both engines share the same serverless infrastructure, scaling, and governance &mdash; the difference is what they do with your data, not where they run</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Unity Catalog Delta Tables &mdash; Where Results Land</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>Every ST and MV is stored as a <strong>Unity Catalog managed Delta table</strong> &mdash; permissions, tags, lineage, and audit logging are built in from the moment of creation</li>
# MAGIC           <li>For optimal incremental refresh on MVs, source tables should have <code>delta.enableRowTracking</code>, <code>delta.enableChangeDataFeed</code>, and <code>delta.enableDeletionVectors</code> enabled</li>
# MAGIC           <li><strong>Billing:</strong> refresh compute is metered as serverless DBUs. You can attribute costs via <code>system.billing.usage</code> joined with table tags</li>
# MAGIC           <li>&#x25C6; At <a href="https://www.databricks.com/customers/eneco">Eneco</a>, this serverless engine processes <strong>petabytes of batch and streaming IoT data</strong> &mdash; the same infrastructure that powers your Gold-layer MVs handles enterprise-scale workloads</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Operationalizing Your Pipeline
# MAGIC
# MAGIC You know what Streaming Tables and Materialized Views are, and when to use each. Now it is time to make them production-ready &mdash; validating incrementalizability before deployment, scheduling automated refreshes, and monitoring pipeline health.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. EXPLAIN CREATE MATERIALIZED VIEW
# MAGIC
# MAGIC `EXPLAIN CREATE MATERIALIZED VIEW` analyzes your query <u>before</u> you create the MV &mdash; telling you whether it qualifies for incremental refresh so you can **fix problems before they reach production**.
# MAGIC
# MAGIC <!-- ── Visual: D1 integrated ── -->
# MAGIC <div class="d1-wrap">
# MAGIC <style>
# MAGIC /* ── D1 scoped styles ── */
# MAGIC .d1-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1100px; margin: 0 auto; color: #1B3139; }
# MAGIC /* ── Workflow top ── */
# MAGIC .d1-flow { display: flex; flex-direction: column; align-items: center; gap: 0; padding: 16px 0 0; }
# MAGIC .d1-flow-step { background: #E8F0FE; border: 2px solid #2574B5; border-radius: 10px; padding: 12px 28px; text-align: center; font-size: 14pt; font-weight: 600; }
# MAGIC .d1-flow-arrow { font-size: 20pt; line-height: 1; margin: 2px 0; }
# MAGIC .d1-flow-hint { font-size: 14pt; color: #618794; margin: 4px 0 6px; }
# MAGIC /* ── Flip cards as decision point ── */
# MAGIC .d1-fc-row { display: flex; gap: 20px; justify-content: center; perspective: 1200px; }
# MAGIC .d1-fc-col { display: flex; flex-direction: column; align-items: center; }
# MAGIC .d1-fc-col-yes { flex: 1; }
# MAGIC .d1-fc-col-no  { flex: 1; }
# MAGIC .d1-fc-card { width: 100%; height: 370px; perspective: 1000px; cursor: pointer; }
# MAGIC .d1-fc-inner { position: relative; width: 100%; height: 100%; transition: transform 0.6s cubic-bezier(0.4,0,0.2,1); transform-style: preserve-3d; }
# MAGIC .d1-fc-card.flipped .d1-fc-inner { transform: rotateY(180deg); }
# MAGIC .d1-fc-face { position: absolute; top: 0; left: 0; width: 100%; height: 100%; backface-visibility: hidden; -webkit-backface-visibility: hidden; border-radius: 12px; box-sizing: border-box; overflow: hidden; }
# MAGIC .d1-fc-front { display: flex; flex-direction: column; }
# MAGIC .d1-fc-front-hdr { padding: 10px 16px; text-align: center; color: white; font-size: 14pt; font-weight: 700; }
# MAGIC .d1-fc-front-code { flex: 1; background: #1B3139; color: #e0e0e0; padding: 14px 16px; font-family: monospace; font-size: 14pt; line-height: 1.7; }
# MAGIC .d1-fc-front-hint { text-align: center; padding: 6px; font-size: 14pt; color: #618794; background: #F9F7F4; }
# MAGIC .d1-fc-back { transform: rotateY(180deg); padding: 18px; display: flex; flex-direction: column; font-size: 14pt; font-family: monospace; line-height: 1.5; }
# MAGIC .d1-fc-back-sec { color: #618794; margin-top: 10px; }
# MAGIC .d1-fc-back-sec:first-child { margin-top: 0; }
# MAGIC /* ── Outcome boxes below each card ── */
# MAGIC .d1-outcome { margin-top: 6px; width: 100%; }
# MAGIC .d1-outcome-arrow { font-size: 18pt; line-height: 1; text-align: center; }
# MAGIC .d1-outcome-yes { background: #E6F9F0; border: 2px solid #00A972; border-radius: 10px; padding: 12px 16px; text-align: center; font-size: 14pt; font-weight: 600; }
# MAGIC .d1-outcome-no { display: flex; gap: 8px; }
# MAGIC .d1-outcome-no-box { flex: 1; background: #FFF0EE; border: 2px solid #FF5F46; border-radius: 10px; padding: 12px 8px; text-align: center; font-size: 14pt; font-weight: 600; }
# MAGIC .d1-outcome-or { display: flex; align-items: center; font-size: 14pt; font-weight: 700; color: #888; }
# MAGIC .d1-outcome-sub { font-size: 14pt; font-weight: 400; color: #555; margin-top: 2px; }
# MAGIC .d1-outcome-caveat { font-size: 14pt; color: #7a6a00; font-style: italic; margin-top: 6px; text-align: center; }
# MAGIC /* ── Lane backgrounds (applied via HTML classes alongside flex above) ── */
# MAGIC .d1-fc-col-yes.d1-fc-col-yes { background: rgba(0,169,114,0.06); border-radius: 16px; padding: 16px 14px 20px; }
# MAGIC .d1-fc-col-no.d1-fc-col-no   { background: rgba(255,95,70,0.06); border-radius: 16px; padding: 16px 14px 20px; }
# MAGIC /* ── Error grid inside NO lane ── */
# MAGIC .d1-err-label { font-size: 14pt; font-weight: 600; color: #FF5F46; margin: 16px 0 8px; }
# MAGIC .d1-err-grid { display: grid; grid-template-columns: 1fr; gap: 10px; }
# MAGIC .d1-err-card { background: #fff; border-radius: 8px; padding: 12px 14px; border-top: 3px solid #FF5F46; }
# MAGIC .d1-err-name { font-weight: 700; font-size: 14pt; color: #FF5F46; font-family: monospace; }
# MAGIC .d1-err-list { font-size: 14pt; color: #444; margin: 6px 0 0 0; padding-left: 18px; line-height: 1.5; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- ── STEPS 1 & 2 ── -->
# MAGIC <div class="d1-flow">
# MAGIC   <div style="font-size: 15pt; font-weight: 700; color: #1B3139; margin-bottom: 8px;">Recommended Workflow</div>
# MAGIC   <div class="d1-flow-step">1. Write your MV query</div>
# MAGIC   <div class="d1-flow-arrow" style="color:#2574B5">&#x25BC;</div>
# MAGIC   <div class="d1-flow-step">2. Run <code style="background:#d0e4f7;padding:2px 6px;border-radius:4px;">EXPLAIN CREATE MATERIALIZED VIEW</code></div>
# MAGIC   <div class="d1-flow-arrow" style="color:#2574B5">&#x25BC;</div>
# MAGIC   <div class="d1-flow-hint">Click each card to see what EXPLAIN returns</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ── FLIP CARDS = THE DECISION POINT ── -->
# MAGIC <div class="d1-fc-row">
# MAGIC   <!-- ── YES lane ── -->
# MAGIC   <div class="d1-fc-col d1-fc-col-yes">
# MAGIC     <div class="d1-fc-card" onclick="this.classList.toggle('flipped')">
# MAGIC       <div class="d1-fc-inner">
# MAGIC         <div class="d1-fc-face d1-fc-front" style="border: 2px solid #00A972">
# MAGIC           <div class="d1-fc-front-hdr" style="background:#00A972"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"> Incrementalizable</div>
# MAGIC           <div class="d1-fc-front-code">
# MAGIC             <span style="color:#7ecbf5;font-weight:600">EXPLAIN CREATE MATERIALIZED VIEW</span><br/>
# MAGIC             gold_revenue <span style="color:#7ecbf5">AS</span><br/>
# MAGIC             <span style="color:#7ecbf5;font-weight:600">SELECT</span> product,<br/>
# MAGIC             &nbsp;&nbsp;<span style="color:#dcdcaa">SUM</span>(quantity) <span style="color:#7ecbf5">AS</span> total_qty<br/>
# MAGIC             <span style="color:#7ecbf5;font-weight:600">FROM</span> sales_transactions<br/>
# MAGIC             <span style="color:#7ecbf5;font-weight:600">GROUP BY</span> product;
# MAGIC           </div>
# MAGIC           <div class="d1-fc-front-hint"><img src="../Includes/images/icons/refresh-icon.png" height="16" style="vertical-align: middle;"> Click to see output</div>
# MAGIC         </div>
# MAGIC         <div class="d1-fc-face d1-fc-back" style="background:#0b3d2b;color:#a8e6cf;border:2px solid #00A972;border-radius:12px">
# MAGIC           <div class="d1-fc-back-sec" style="color:#66bb6a">== Incremental Update Eligibility ==</div>
# MAGIC           <div style="color:#fff;font-weight:700;margin-top:4px">The Materialized View <u>can</u> be incrementally refreshed.</div>
# MAGIC           <div class="d1-fc-back-sec" style="color:#66bb6a">== Detailed Incrementalization Info ==</div>
# MAGIC           <div style="color:#a8e6cf;margin-top:4px">No issues detected.</div>
# MAGIC           <div class="d1-fc-back-sec" style="color:#66bb6a">== Physical Plan ==</div>
# MAGIC           <div style="color:#a8e6cf;margin-top:4px; opacity:0.7">Execute CreateMaterializedViewCommand<br/>&nbsp;&nbsp;+- Aggregate [product], [product, sum(quantity)]</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d1-outcome">
# MAGIC       <div class="d1-outcome-arrow" style="color:#00A972">&#x25BC;</div>
# MAGIC       <div class="d1-outcome-yes">3. Choose your <code style="background:#c8f0dc;padding:2px 6px;border-radius:4px;">REFRESH POLICY</code><br/><span class="d1-outcome-sub">(AUTO | INCREMENTAL | STRICT)</span></div>
# MAGIC       <div class="d1-outcome-caveat">Incrementalizable &#x2260; incrementalized &mdash; under AUTO the cost model may still pick full recompute. Use INCREMENTAL or STRICT to force it.</div>
# MAGIC       <!-- Full sequence example -->
# MAGIC       <div style="margin-top: 14px; background: #1B3139; border-radius: 10px; padding: 14px 16px; margin-top: 6px; font-family: monospace; line-height: 1.8; color: #e0e0e0">
# MAGIC         <span style="color:#618794">-- Create with policy</span><br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">CREATE MATERIALIZED VIEW</span><br/>
# MAGIC         gold_revenue<br/>
# MAGIC         <span style="color:#dcdcaa;font-weight:600">REFRESH POLICY AUTO</span><br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">AS</span><br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">SELECT</span> product,<br/>
# MAGIC         &nbsp;&nbsp;<span style="color:#dcdcaa">SUM</span>(quantity) <span style="color:#7ecbf5">AS</span> total_qty<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">FROM</span> sales_transactions<br/>
# MAGIC         <span style="color:#7ecbf5;font-weight:600">GROUP BY</span> product;
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- ── NO lane ── -->
# MAGIC   <div class="d1-fc-col d1-fc-col-no">
# MAGIC     <div class="d1-fc-card" onclick="this.classList.toggle('flipped')">
# MAGIC       <div class="d1-fc-inner">
# MAGIC         <div class="d1-fc-face d1-fc-front" style="border: 2px solid #FF5F46">
# MAGIC           <div class="d1-fc-front-hdr" style="background:#FF5F46"><img src="../Includes/images/icons/x-circle-icon.png" height="16" style="vertical-align:middle;"> Not Incrementalizable</div>
# MAGIC           <div class="d1-fc-front-code">
# MAGIC             <span style="color:#7ecbf5;font-weight:600">EXPLAIN CREATE MATERIALIZED VIEW</span><br/>
# MAGIC             gold_orders <span style="color:#7ecbf5">AS</span><br/>
# MAGIC             <span style="color:#7ecbf5;font-weight:600">SELECT</span> *,<br/>
# MAGIC             &nbsp;&nbsp;<span style="color:#dcdcaa">UUID</span>() <span style="color:#7ecbf5">AS</span> row_id<br/>
# MAGIC             <span style="color:#7ecbf5;font-weight:600">FROM</span> trips;
# MAGIC           </div>
# MAGIC           <div class="d1-fc-front-hint"><img src="../Includes/images/icons/refresh-icon.png" height="16" style="vertical-align: middle;"> Click to see output</div>
# MAGIC         </div>
# MAGIC         <div class="d1-fc-face d1-fc-back" style="background:#3d1111;color:#ffab91;border:2px solid #FF5F46;border-radius:12px">
# MAGIC           <div class="d1-fc-back-sec" style="color:#ef5350">== Incremental Update Eligibility ==</div>
# MAGIC           <div style="color:#fff;font-weight:700;margin-top:4px">The Materialized View <u>cannot</u> be incrementally refreshed.</div>
# MAGIC           <div class="d1-fc-back-sec" style="color:#ef5350">== Detailed Incrementalization Info ==</div>
# MAGIC           <div style="color:#ffab91;margin-top:4px">- EXPRESSION_NOT_DETERMINISTIC:<br/>&nbsp;&nbsp;UUID is not deterministic.<br/>&nbsp;&nbsp;Consider rewriting the query.</div>
# MAGIC           <div style="color:#ffab91;margin-top:6px">- ROW_TRACKING_NOT_ENABLED:<br/>&nbsp;&nbsp;Enable row tracking on<br/>&nbsp;&nbsp;source tables.</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d1-outcome">
# MAGIC       <div class="d1-outcome-arrow" style="color:#FF5F46">&#x25BC;</div>
# MAGIC       <div class="d1-outcome-no">
# MAGIC         <div class="d1-outcome-no-box">4a. Redesign<br/>the query<br/><span class="d1-outcome-sub">then re-run EXPLAIN</span></div>
# MAGIC         <div class="d1-outcome-or">OR</div>
# MAGIC         <div class="d1-outcome-no-box">4b. Accept full<br/>recompute<br/><code style="background:#fdd;padding:2px 6px;border-radius:4px">AUTO</code> / <code style="background:#fdd;padding:2px 6px;border-radius:4px">FULL</code></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Error conditions — inside the NO lane -->
# MAGIC     <div class="d1-err-label">Common error conditions:</div>
# MAGIC     <div class="d1-err-grid">
# MAGIC       <div class="d1-err-card">
# MAGIC         <div class="d1-err-name">EXPRESSION_NOT_DETERMINISTIC</div>
# MAGIC         <ul class="d1-err-list">
# MAGIC           <li><strong>Cause:</strong> <code>UUID()</code>, <code>RAND()</code> &mdash; <u>different result every run</u></li>
# MAGIC           <li><strong>Fix:</strong> Remove the function or compute at <u>query time</u></li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC       <div class="d1-err-card">
# MAGIC         <div class="d1-err-name">ROW_TRACKING_NOT_ENABLED</div>
# MAGIC         <ul class="d1-err-list">
# MAGIC           <li><strong>Cause:</strong> Source tables missing <u>change tracking</u></li>
# MAGIC           <li><strong>Fix:</strong> <code>ALTER TABLE SET TBLPROPERTIES('delta.enableRowTracking'='true')</code></li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="text-align: right; margin-top: 6px">All documented error conditions &#x2197; (<a href="https://docs.databricks.com/aws/en/error-messages/materialized-view-not-incrementalizable-error-class">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/error-messages/materialized-view-not-incrementalizable-error-class">Azure</a> | <a href="https://docs.databricks.com/gcp/en/error-messages/materialized-view-not-incrementalizable-error-class">GCP</a>)</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div><!-- end d1-wrap -->
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">What Makes a Query Non-Incrementalizable?</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li>When you run <code>EXPLAIN CREATE MATERIALIZED VIEW</code>, it checks your query for patterns that <strong>block incremental refresh</strong>:
# MAGIC             <ul>
# MAGIC               <li><strong>Non-deterministic functions:</strong> <code>UUID()</code>, <code>RANDOM()</code>, <code>RAND()</code> &mdash; these produce different results every run, so the engine cannot track what changed</li>
# MAGIC               <li><strong>Non-Delta sources:</strong> CSV, JSON, or foreign catalog tables &mdash; only Delta tables support change tracking</li>
# MAGIC               <li><strong>Row filters or column masks:</strong> security policies on source data prevent incremental tracking</li>
# MAGIC               <li><strong>External UDFs:</strong> user-defined functions that call external services are treated as non-deterministic</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>On the other hand, these patterns <strong>are</strong> incrementalizable &mdash; you can use them freely:
# MAGIC             <ul>
# MAGIC               <li><code>SELECT</code> with deterministic expressions, <code>GROUP BY</code> with standard aggregations</li>
# MAGIC               <li>All <code>JOIN</code> types, <code>UNION ALL</code>, <code>WHERE</code>/<code>HAVING</code>/<code>QUALIFY</code>, CTEs</li>
# MAGIC               <li>Window functions with <code>PARTITION BY</code></li>
# MAGIC               <li>The special exceptions <code>current_date()</code> and <code>current_timestamp()</code></li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Think of EXPLAIN as a <strong>building inspector</strong> who checks your blueprints before construction &mdash; the inspector confirms the design is structurally sound, but cannot predict the weather on construction day. Similarly, EXPLAIN confirms your query <em>can</em> be incrementalized, but the runtime cost model ultimately decides <em>whether</em> it will be.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">In Practice</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>EXPLAIN consumes <strong>minimal compute</strong> &mdash; it analyzes your query structure without processing any data. You can run it as many times as you want during development at negligible cost.</li>
# MAGIC           <li>This is currently a <strong>Beta feature</strong> (Databricks Runtime 17.3+). On serverless SQL Warehouses, it works without any separate preview enablement &mdash; you can start using it today.</li>
# MAGIC           <li>A good habit is to run EXPLAIN on every MV query before you deploy it. It takes seconds and can save you hours of debugging unexpected full-recompute behavior in production.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E2. SQL Patterns That Incrementalize (and Those That Don't)
# MAGIC
# MAGIC The way you structure your SQL determines whether Enzyme can optimize it. This is a **modeling skill** &mdash; learning which patterns to use and which to restructure.
# MAGIC
# MAGIC <!-- ── Visual: incrementalizable patterns comparison ── -->
# MAGIC <div style="max-width: 1050px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
# MAGIC
# MAGIC <div style="display: flex; gap: 16px; flex-wrap: wrap;">
# MAGIC
# MAGIC   <!-- Incrementalizes -->
# MAGIC   <div style="flex: 1; min-width: 400px;">
# MAGIC     <div style="background: #00A972; color: white; padding: 12px 18px; border-radius: 10px 10px 0 0; text-align: center; font-size: 15pt; font-weight: 700;">
# MAGIC       <img src="../Includes/images/icons/checkmark-success.png" height="18" style="vertical-align:middle;"> Incrementalizes
# MAGIC     </div>
# MAGIC     <div style="background: #E6F9F0; border: 2px solid #00A972; border-top: none; border-radius: 0 0 10px 10px; padding: 18px;">
# MAGIC       <table style="width: 100%; border-collapse: collapse; font-size: 14pt; line-height: 1.6;">
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><strong>Simple aggregations</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><code>SUM</code>, <code>COUNT</code>, <code>AVG</code>, <code>MIN</code>, <code>MAX</code></td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><strong>All JOIN types</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;">INNER, LEFT, RIGHT, FULL, CROSS</td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><strong>Filters</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><code>WHERE</code>, <code>HAVING</code>, <code>QUALIFY</code></td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><strong>CASE expressions</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;">Deterministic conditional logic</td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><strong>Window functions</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;">With <code>PARTITION BY</code></td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;"><strong>CTEs and subqueries</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #c8f0dc;">Standard <code>WITH</code> clauses</td></tr>
# MAGIC         <tr><td style="padding: 8px 0;"><strong>UNION ALL</strong></td><td style="padding: 8px 0;">Combining multiple sources</td></tr>
# MAGIC       </table>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Does NOT incrementalize -->
# MAGIC   <div style="flex: 1; min-width: 400px;">
# MAGIC     <div style="background: #FF5F46; color: white; padding: 12px 18px; border-radius: 10px 10px 0 0; text-align: center; font-size: 15pt; font-weight: 700;">
# MAGIC       <img src="../Includes/images/icons/x-circle-icon.png" height="18" style="vertical-align:middle;"> Does NOT Incrementalize
# MAGIC     </div>
# MAGIC     <div style="background: #FFF0EE; border: 2px solid #FF5F46; border-top: none; border-radius: 0 0 10px 10px; padding: 18px;">
# MAGIC       <table style="width: 100%; border-collapse: collapse; font-size: 14pt; line-height: 1.6;">
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #fdd;"><strong>Non-deterministic functions</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #fdd;"><code>UUID()</code>, <code>RAND()</code>, <code>RANDOM()</code></td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #fdd;"><strong>Non-Delta sources</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #fdd;">CSV, JSON, foreign catalogs</td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #fdd;"><strong>Row filters / column masks</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #fdd;">Security policies on source data</td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #fdd;"><strong>External UDFs</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #fdd;">Treated as non-deterministic</td></tr>
# MAGIC         <tr><td style="padding: 8px 0; border-bottom: 1px solid #fdd;"><strong>Missing source properties</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #fdd;">No row tracking or CDF enabled</td></tr>
# MAGIC         <tr><td style="padding: 8px 0;"><strong>Volume / path-based sources</strong></td><td style="padding: 8px 0;">Files, not Delta tables</td></tr>
# MAGIC       </table>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- How to fix callout -->
# MAGIC <div style="margin-top: 20px; padding: 16px 20px; background: #FFF8E1; border: 2px solid #E5A100; border-radius: 10px;">
# MAGIC   <div style="font-size: 15pt; font-weight: 700; color: #1B3139; margin-bottom: 8px;">How to Fix Non-Incrementalizable Queries</div>
# MAGIC   <div style="font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC     <ul style="margin: 0; padding-left: 20px;">
# MAGIC       <li><strong>Remove non-deterministic functions</strong> &mdash; if you need a UUID, generate it at query time or in a downstream view, not in the MV definition</li>
# MAGIC       <li><strong>Enable source table properties</strong> &mdash; <code>ALTER TABLE SET TBLPROPERTIES('delta.enableRowTracking'='true', 'delta.enableChangeDataFeed'='true', 'delta.enableDeletionVectors'='true')</code></li>
# MAGIC       <li><strong>Land non-Delta sources in Delta first</strong> &mdash; use a Streaming Table for file ingestion, then build the MV on the ST output</li>
# MAGIC       <li><strong>Pre-aggregate before joining</strong> &mdash; restructure complex queries so each CTE step is incrementalizable</li>
# MAGIC       <li><strong>Always re-run EXPLAIN after changes</strong> &mdash; verify the fix worked before deploying</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">The Design-Time Mindset</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li>In a traditional warehouse, you optimize queries after the fact with indexes and partitions. In Lakehouse SQL, you optimize at <strong>design time</strong> by writing SQL that Enzyme can incrementalize.</li>
# MAGIC           <li>The practical impact is significant: an incrementalizable MV over a terabyte table might refresh in seconds (processing only the 0.1% that changed), while a non-incrementalizable MV recomputes the entire terabyte on every refresh.</li>
# MAGIC           <li>Think of incrementalizability as a <strong>property of your SQL design</strong>, not a property of the data. Two queries that produce the same result can have very different refresh costs depending on how they are written.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Special Cases</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7">
# MAGIC           <li><code>current_date()</code> and <code>current_timestamp()</code> are special exceptions &mdash; they <strong>are</strong> incrementalizable despite being technically non-deterministic, because the platform handles them as "deterministic within a refresh."</li>
# MAGIC           <li><code>UNION</code> (with deduplication) may or may not incrementalize depending on the underlying complexity &mdash; prefer <code>UNION ALL</code> when you know sources are disjoint.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Enzyme Refresh Techniques</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>When your query incrementalizes, Enzyme automatically chooses the cheapest refresh technique:
# MAGIC             <ul>
# MAGIC               <li><strong>ROW_BASED:</strong> Updates individual rows &mdash; best for small changesets</li>
# MAGIC               <li><strong>PARTITION_OVERWRITE:</strong> Rewrites only affected partitions &mdash; best for partitioned tables with localized changes</li>
# MAGIC               <li><strong>AGGREGATE_RECOMPUTE:</strong> Recomputes only affected aggregation groups</li>
# MAGIC               <li><strong>FULL_RECOMPUTE:</strong> Rewrites everything &mdash; the fallback when the above are not cheaper</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>You can observe which technique was used by querying the event log for <code>planning_information</code> events.</li>
# MAGIC           <li>Full documentation on supported operations: (<a href="https://docs.databricks.com/aws/en/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">Azure</a> | <a href="https://docs.databricks.com/gcp/en/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">GCP</a>)</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Refresh Strategies as Business Decisions
# MAGIC
# MAGIC REFRESH POLICY and SCHEDULE are not just technical options &mdash; they are how you translate business SLA requirements into platform behavior. This section frames each policy and schedule as a **business decision**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. REFRESH POLICY
# MAGIC
# MAGIC `REFRESH POLICY` controls the *how* of refresh &mdash; whether the Enzyme engine rebuilds only what changed (incremental) or rewrites the full result from scratch. Choosing the right policy lets you balance cost, freshness guarantees, and operational predictability.
# MAGIC
# MAGIC <!-- ── Visual: tabs-color-cards (d2rp-) ── -->
# MAGIC <div class="d2rp-wrap">
# MAGIC <style>
# MAGIC .d2rp-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 920px; margin: 0 auto; padding: 24px; background: #FAFAFA; border: 1px solid #E0E0E0; border-radius: 12px; }
# MAGIC .d2rp-wrap input[type="radio"] { display: none; }
# MAGIC .d2rp-nav { display: flex; gap: 8px; flex-wrap: wrap; }
# MAGIC .d2rp-tab { flex: 1; min-width: 120px; text-align: center; padding: 10px 8px; font-size: 14pt; font-weight: 700; color: #1B3139; cursor: pointer; border: 2px solid #E0E0E0; border-radius: 6px; background: #FFF; transition: border-color 0.15s; user-select: none; position: relative; }
# MAGIC .d2rp-tab::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 5px; border-radius: 4px 4px 0 0; }
# MAGIC .d2rp-tab[for="d2rp-r1"]::before { background: #2574B5; }
# MAGIC .d2rp-tab[for="d2rp-r2"]::before { background: #02A36F; }
# MAGIC .d2rp-tab[for="d2rp-r3"]::before { background: #E0382B; }
# MAGIC .d2rp-tab[for="d2rp-r4"]::before { background: #E5A100; }
# MAGIC #d2rp-r1:checked ~ .d2rp-nav .d2rp-tab[for="d2rp-r1"] { border-color: #2574B5; box-shadow: 0 0 0 1px #2574B5; }
# MAGIC #d2rp-r2:checked ~ .d2rp-nav .d2rp-tab[for="d2rp-r2"] { border-color: #02A36F; box-shadow: 0 0 0 1px #02A36F; }
# MAGIC #d2rp-r3:checked ~ .d2rp-nav .d2rp-tab[for="d2rp-r3"] { border-color: #E0382B; box-shadow: 0 0 0 1px #E0382B; }
# MAGIC #d2rp-r4:checked ~ .d2rp-nav .d2rp-tab[for="d2rp-r4"] { border-color: #E5A100; box-shadow: 0 0 0 1px #E5A100; }
# MAGIC .d2rp-accent { display: none; height: 5px; border-radius: 3px; margin: 16px 0 0 0; }
# MAGIC #d2rp-r1:checked ~ .d2rp-panels .d2rp-accent-1 { display: block; background: #2574B5; }
# MAGIC #d2rp-r2:checked ~ .d2rp-panels .d2rp-accent-2 { display: block; background: #02A36F; }
# MAGIC #d2rp-r3:checked ~ .d2rp-panels .d2rp-accent-3 { display: block; background: #E0382B; }
# MAGIC #d2rp-r4:checked ~ .d2rp-panels .d2rp-accent-4 { display: block; background: #E5A100; }
# MAGIC .d2rp-panel { display: none; padding: 20px 24px 24px; background: #F9F7F4; border: 1px solid #E8E3DC; border-radius: 0 0 8px 8px; }
# MAGIC #d2rp-r1:checked ~ .d2rp-panels .d2rp-panel:nth-of-type(2) { display: block; }
# MAGIC #d2rp-r2:checked ~ .d2rp-panels .d2rp-panel:nth-of-type(4) { display: block; }
# MAGIC #d2rp-r3:checked ~ .d2rp-panels .d2rp-panel:nth-of-type(6) { display: block; }
# MAGIC #d2rp-r4:checked ~ .d2rp-panels .d2rp-panel:nth-of-type(8) { display: block; }
# MAGIC .d2rp-panel h3 { margin: 0 0 12px; font-size: 1.25em; color: #0b2026; font-weight: 800; }
# MAGIC .d2rp-panel ul { margin: 0 0 16px; padding-left: 20px; line-height: 1.7; color: #1B3139; font-size: 14pt; }
# MAGIC .d2rp-panel ul li { margin-bottom: 6px; }
# MAGIC .d2rp-panel code { background: #E8E3DC; padding: 2px 6px; border-radius: 4px; font-size: 1em; }
# MAGIC .d2rp-callout { background: #E8F0E4; border-left: 4px solid #02A36F; padding: 14px 18px; border-radius: 0 6px 6px 0; margin-top: 8px; font-size: 14pt; line-height: 1.6; }
# MAGIC .d2rp-callout strong { color: #0b2026; }
# MAGIC .d2rp-callout code { background: transparent; padding: 0; font-size: 1em; }
# MAGIC .d2rp-code { background: #1B3139; border-radius: 8px; padding: 14px 18px; margin-top: 10px; font-family: monospace; font-size: 14pt; line-height: 1.8; color: #e0e0e0; }
# MAGIC .d2rp-code .kw { color: #7ecbf5; font-weight: 600; }
# MAGIC .d2rp-code .pol { color: #dcdcaa; font-weight: 600; }
# MAGIC /* ── EXPLAIN × Policy matrix ── */
# MAGIC /* ── EXPLAIN × Policy two-pane ── */
# MAGIC .d2rp-ex { display: flex; gap: 0; margin-top: 16px; border-radius: 10px; overflow: hidden; font-size: 14pt; line-height: 1.5; }
# MAGIC .d2rp-ex-yes { flex: 1; background: #E6F9F0; border-left: 4px solid #00A972; padding: 12px 16px; }
# MAGIC .d2rp-ex-no  { flex: 1; background: #FFF0EE; border-left: 4px solid #FF5F46; padding: 12px 16px; }
# MAGIC .d2rp-ex-label { font-size: 14pt; font-weight: 700; margin-bottom: 4px; }
# MAGIC .d2rp-ex-yes .d2rp-ex-label { color: #00A972; }
# MAGIC .d2rp-ex-no  .d2rp-ex-label { color: #FF5F46; }
# MAGIC .d2rp-ex-text { color: #1B3139; }
# MAGIC </style>
# MAGIC <input type="radio" name="d2rp-grp" id="d2rp-r1" checked>
# MAGIC <input type="radio" name="d2rp-grp" id="d2rp-r2">
# MAGIC <input type="radio" name="d2rp-grp" id="d2rp-r3">
# MAGIC <input type="radio" name="d2rp-grp" id="d2rp-r4">
# MAGIC <div class="d2rp-nav">
# MAGIC   <label class="d2rp-tab" for="d2rp-r1">AUTO (default)</label>
# MAGIC   <label class="d2rp-tab" for="d2rp-r2">INCREMENTAL</label>
# MAGIC   <label class="d2rp-tab" for="d2rp-r3">INCREMENTAL STRICT</label>
# MAGIC   <label class="d2rp-tab" for="d2rp-r4">FULL</label>
# MAGIC </div>
# MAGIC <div class="d2rp-panels">
# MAGIC   <div class="d2rp-accent d2rp-accent-1"></div>
# MAGIC   <div class="d2rp-panel">
# MAGIC     <h3>REFRESH POLICY AUTO (Default)</h3>
# MAGIC     <ul>
# MAGIC       <li><strong>What it does:</strong> The Enzyme <u>cost model decides</u> incremental vs. full on each individual refresh</li>
# MAGIC       <li><strong>When to use it:</strong> Best <u>starting point</u> &mdash; no configuration needed; works correctly out of the box</li>
# MAGIC       <li><strong>How it decides:</strong> If a <u>small fraction</u> of rows changed, the model <u><em>picks incremental</em></u>; if <u>schema changed</u> or the changeset is large, it <u><em>picks full</em></u></li>
# MAGIC     </ul>
# MAGIC     <div class="d2rp-code"><span class="kw">CREATE MATERIALIZED VIEW</span> gold_revenue<br/><span class="pol">REFRESH POLICY AUTO</span><br/><span class="kw">AS SELECT</span> product, <span class="kw">SUM</span>(quantity) <span class="kw">AS</span> total_qty<br/><span class="kw">FROM</span> sales_transactions <span class="kw">GROUP BY</span> product;</div>
# MAGIC     <div class="d2rp-ex">
# MAGIC       <div class="d2rp-ex-yes"><div class="d2rp-ex-label"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"> Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#00795b;">Cost model decides</strong> each refresh &mdash; incremental when efficient, full when not</div></div>
# MAGIC       <div class="d2rp-ex-no"><div class="d2rp-ex-label"><img src="../Includes/images/icons/x-circle-icon.png" height="16" style="vertical-align:middle;"> Not Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#E5A100;">Full recompute</strong> every time</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d2rp-accent d2rp-accent-2"></div>
# MAGIC   <div class="d2rp-panel">
# MAGIC     <h3>REFRESH POLICY INCREMENTAL</h3>
# MAGIC     <ul>
# MAGIC       <li><strong>What it does:</strong> <u>Prefers incremental</u> refresh at runtime; <u>falls back to full</u> only when a specific refresh cannot be done incrementally (e.g., after a schema change)</li>
# MAGIC       <li><strong>When to use it:</strong> When you want incremental as the default but can <u>tolerate occasional full recomputes</u> at runtime</li>
# MAGIC       <li><strong>Requires incrementalizable query:</strong> <code>CREATE</code> <u>fails immediately</u> if the query itself is not incrementalizable &mdash; run <code>EXPLAIN</code> first</li>
# MAGIC     </ul>
# MAGIC     <div class="d2rp-code"><span class="kw">CREATE MATERIALIZED VIEW</span> gold_revenue<br/><span class="pol">REFRESH POLICY INCREMENTAL</span><br/><span class="kw">AS SELECT</span> product, <span class="kw">SUM</span>(quantity) <span class="kw">AS</span> total_qty<br/><span class="kw">FROM</span> sales_transactions <span class="kw">GROUP BY</span> product;</div>
# MAGIC     <div class="d2rp-ex">
# MAGIC       <div class="d2rp-ex-yes"><div class="d2rp-ex-label"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"> Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#00795b;">Incremental preferred</strong>, with fallback to full if needed</div></div>
# MAGIC       <div class="d2rp-ex-no"><div class="d2rp-ex-label"><img src="../Includes/images/icons/x-circle-icon.png" height="16" style="vertical-align:middle;"> Not Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#E0382B;">CREATE fails immediately</strong></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d2rp-accent d2rp-accent-3"></div>
# MAGIC   <div class="d2rp-panel">
# MAGIC     <h3>REFRESH POLICY INCREMENTAL STRICT</h3>
# MAGIC     <ul>
# MAGIC       <li><strong>What it does:</strong> Requires <u>incremental on every run</u> &mdash; <u>fails</u> rather than falling back to full</li>
# MAGIC       <li><strong>When to use it:</strong> Cost-sensitive pipelines where a <u>surprise full recompute</u> would be unacceptable</li>
# MAGIC       <li><strong>No cheat days:</strong> If incremental is not possible for any reason, the refresh <u>fails with an error</u> instead of silently doing a full recompute</li>
# MAGIC     </ul>
# MAGIC     <div class="d2rp-code"><span class="kw">CREATE MATERIALIZED VIEW</span> gold_revenue<br/><span class="pol">REFRESH POLICY INCREMENTAL STRICT</span><br/><span class="kw">AS SELECT</span> product, <span class="kw">SUM</span>(quantity) <span class="kw">AS</span> total_qty<br/><span class="kw">FROM</span> sales_transactions <span class="kw">GROUP BY</span> product;</div>
# MAGIC     <div class="d2rp-ex">
# MAGIC       <div class="d2rp-ex-yes"><div class="d2rp-ex-label"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"> Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#00795b;">Incremental only</strong> &mdash; fails rather than falling back to full</div></div>
# MAGIC       <div class="d2rp-ex-no"><div class="d2rp-ex-label"><img src="../Includes/images/icons/x-circle-icon.png" height="16" style="vertical-align:middle;"> Not Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#E0382B;">CREATE fails immediately</strong></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d2rp-accent d2rp-accent-4"></div>
# MAGIC   <div class="d2rp-panel">
# MAGIC     <h3>REFRESH POLICY FULL</h3>
# MAGIC     <ul>
# MAGIC       <li><strong>What it does:</strong> Always rewrites the <u>entire result from scratch</u>, regardless of how much source data changed</li>
# MAGIC       <li><strong>When to use it:</strong> Queries with <u>non-deterministic functions</u>, or when you need a guaranteed <u>clean slate</u> (e.g., after source corrections)</li>
# MAGIC       <li><strong>The escape hatch:</strong> Works on <u>any query</u> &mdash; even ones that are not incrementalizable. No EXPLAIN check required</li>
# MAGIC     </ul>
# MAGIC     <div class="d2rp-code"><span class="kw">CREATE MATERIALIZED VIEW</span> gold_revenue<br/><span class="pol">REFRESH POLICY FULL</span><br/><span class="kw">AS SELECT</span> product, <span class="kw">SUM</span>(quantity) <span class="kw">AS</span> total_qty<br/><span class="kw">FROM</span> sales_transactions <span class="kw">GROUP BY</span> product;</div>
# MAGIC     <div class="d2rp-ex">
# MAGIC       <div class="d2rp-ex-yes"><div class="d2rp-ex-label"><img src="../Includes/images/icons/checkmark-success.png" height="16" style="vertical-align:middle;"> Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#E5A100;">Full recompute</strong> &mdash; ignores incremental capability</div></div>
# MAGIC       <div class="d2rp-ex-no"><div class="d2rp-ex-label"><img src="../Includes/images/icons/x-circle-icon.png" height="16" style="vertical-align:middle;"> Not Incrementalizable</div><div class="d2rp-ex-text"><strong style="color:#E5A100;">Full recompute</strong> &mdash; the only option that works</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">The Four Refresh Policies</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>AUTO (default):</strong> The Enzyme cost model evaluates the changeset on every refresh and picks the cheapest strategy &mdash; incremental if the delta is small, full if it is large or if a schema change was detected. You get optimal cost without any manual tuning.</li>
# MAGIC           <li><strong>INCREMENTAL:</strong> Tells the platform to prefer incremental but permits a <u>full recompute as a fallback</u> when a specific refresh cannot be done incrementally (e.g., after a schema change). The <code>CREATE</code> statement itself <u>fails immediately</u> if the query is structurally non-incrementalizable &mdash; so you get an early error rather than a silent full refresh at runtime.</li>
# MAGIC           <li><strong>INCREMENTAL STRICT:</strong> Requires incremental on <u>every single refresh</u>. If anything prevents incremental &mdash; schema change, source table property missing &mdash; the refresh <u>fails with an explicit error</u> instead of silently falling back. Use this when a surprise full recompute would breach your SLA or budget.</li>
# MAGIC           <li><strong>FULL:</strong> Always rewrites the entire result from scratch. The simplest guarantee, and the <u>only option</u> for queries that use non-deterministic functions like <code>UUID()</code> or <code>RANDOM()</code>. Also useful after correcting source data when you want a guaranteed clean slate.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Connecting EXPLAIN to REFRESH POLICY</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Remember the critical distinction from D1: when EXPLAIN says <em>"The Materialized View can be incrementally refreshed"</em>, that means the query is <strong>incrementalizable</strong> (structurally capable) &mdash; not that it <strong>will be incrementalized</strong> at every refresh. REFRESH POLICY is how you control what happens next:
# MAGIC             <ul>
# MAGIC               <li><strong>Incrementalizable + AUTO:</strong> Cost model decides each refresh &mdash; incremental when efficient, full when not</li>
# MAGIC               <li><strong>Incrementalizable + INCREMENTAL:</strong> Incremental preferred, with fallback to full if needed</li>
# MAGIC               <li><strong>Incrementalizable + INCREMENTAL STRICT:</strong> Incremental only &mdash; fails rather than falling back to full</li>
# MAGIC               <li><strong>Not incrementalizable + INCREMENTAL/STRICT:</strong> <code>CREATE</code> fails immediately &mdash; you cannot force an incremental policy on a query that does not support it</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>If you attempt <code>INCREMENTAL</code> on a non-incrementalizable query, you will see the error class <code>MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE</code> with sub-reasons like <code>OPERATOR_NOT_SUPPORTED</code> or <code>EXPRESSION_NOT_DETERMINISTIC</code>. This is why D1 recommended running <code>EXPLAIN</code> before choosing your policy &mdash; it catches these issues at development time, not deployment time.</li>
# MAGIC           <li>Think of it as two gates: EXPLAIN is the <strong>first gate</strong> (can the query be incrementalized at all?), and REFRESH POLICY is the <strong>second gate</strong> (how strictly do you want to enforce it at runtime?).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">How the Cost Model Decides (AUTO)</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Under <code>AUTO</code>, the Enzyme engine computes an estimated cost for both strategies before each refresh and picks the lower one. Key inputs include:
# MAGIC             <ul>
# MAGIC               <li><strong>Changeset size:</strong> If only a small fraction of source rows changed, incremental reads only those rows; if nearly all rows changed, full is cheaper because it avoids tracking overhead</li>
# MAGIC               <li><strong>Schema changes:</strong> Any structural change on a source table (column added, type altered) typically forces a full recompute because the incremental plan becomes invalid</li>
# MAGIC               <li><strong>Source table properties:</strong> Without <code>delta.enableChangeDataFeed</code> and <code>delta.enableRowTracking</code>, the engine has no change feed to read &mdash; it falls back to full automatically</li>
# MAGIC               <li><strong>Operation type:</strong> Some aggregations (e.g., certain window functions) have higher incremental overhead than others, shifting the cost model toward full</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>You can observe what the cost model decided for each refresh by querying the event log for <code>planning_information</code> events &mdash; the message will show the Enzyme technique (e.g., <code>ROW_BASED</code>, <code>FULL_RECOMPUTE</code>).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">In Practice</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li><strong>Start with AUTO:</strong> For any new MV, <code>REFRESH POLICY AUTO</code> is the correct default. It requires no configuration, picks the cheapest strategy on each refresh, and handles edge cases gracefully.</li>
# MAGIC           <li><strong>Move to INCREMENTAL when you want a guarantee:</strong> Once you have verified via <code>EXPLAIN</code> and observed in the event log that your MV is consistently doing incremental refreshes, switch to <code>INCREMENTAL</code> to make that intent explicit and get an early error if something changes.</li>
# MAGIC           <li><strong>Use INCREMENTAL STRICT for cost-critical pipelines:</strong> If your budget depends on never doing a full recompute &mdash; for example, an MV over a petabyte-scale table where a full refresh would cost thousands of DBUs &mdash; <code>INCREMENTAL STRICT</code> makes the failure loud and explicit rather than silently expensive.</li>
# MAGIC           <li><strong>FULL as the escape hatch:</strong> Use <code>FULL</code> when the query cannot be incrementalized (non-deterministic functions, foreign catalogs) or when correctness after source corrections is more important than refresh cost.</li>
# MAGIC           <li>The official documentation confirms: both <code>INCREMENTAL</code> and <code>INCREMENTAL STRICT</code> fail at <code>CREATE</code> time if the query is not incrementalizable. The difference is only at <u>refresh time</u> &mdash; <code>INCREMENTAL</code> falls back to full, <code>STRICT</code> fails entirely. (docs: <a href="https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy">GCP</a>)</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F2. SCHEDULE and TRIGGER
# MAGIC
# MAGIC `SCHEDULE` controls the *when* of refresh &mdash; completely independently of which `REFRESH POLICY` you chose. You can pair any policy with any schedule, and you can change the schedule after creation without touching the MV definition.
# MAGIC
# MAGIC <!-- ── Visual: three-pane cards (d3sc-) ── -->
# MAGIC <div class="d3sc-wrap">
# MAGIC <style>
# MAGIC .d3sc-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1100px; margin: 0 auto; }
# MAGIC .d3sc-cards { display: flex; gap: 16px; flex-wrap: wrap; }
# MAGIC .d3sc-card { flex: 1; min-width: 240px; background: #F9F7F4; border: 1px solid #E8E3DC; border-radius: 10px; overflow: hidden; }
# MAGIC .d3sc-card-hdr { padding: 14px 18px 10px; color: #fff; font-size: 15pt; font-weight: 700; }
# MAGIC .d3sc-card-body { padding: 14px 18px 18px; font-size: 14pt; line-height: 1.7; color: #1B3139; }
# MAGIC .d3sc-card-body ul { margin: 8px 0 12px 0; padding-left: 20px; }
# MAGIC .d3sc-card-body li { margin-bottom: 6px; }
# MAGIC .d3sc-card-body code { background: #E8E3DC; padding: 2px 5px; border-radius: 3px; font-size: 1em; }
# MAGIC .d3sc-syntax { background: #1B3139; color: #e0e0e0; border-radius: 6px; padding: 10px 14px; font-family: monospace; font-size: 14pt; line-height: 1.6; margin-top: 10px; white-space: pre-wrap; }
# MAGIC .d3sc-syntax .kw { color: #7ecbf5; font-weight: 600; }
# MAGIC .d3sc-syntax .str { color: #ce9178; }
# MAGIC </style>
# MAGIC <div class="d3sc-cards">
# MAGIC   <div class="d3sc-card">
# MAGIC     <div class="d3sc-card-hdr" style="background: #2574B5">SCHEDULE EVERY</div>
# MAGIC     <div class="d3sc-card-body">
# MAGIC       <ul>
# MAGIC         <li>Refresh at a <u>fixed interval</u> &mdash; no cron syntax needed</li>
# MAGIC         <li><strong>Best for:</strong> Simple pipelines with regular cadence</li>
# MAGIC         <li><strong>Units:</strong> <code>SECONDS</code>, <code>MINUTES</code>, <code>HOURS</code>, <code>DAYS</code>, <code>WEEKS</code></li>
# MAGIC       </ul>
# MAGIC       <div class="d3sc-syntax"><span class="kw">CREATE OR REPLACE MATERIALIZED VIEW</span> mv_hourly<br/>  <span class="kw">SCHEDULE EVERY</span> 1 HOUR<br/><span class="kw">AS SELECT</span> ...</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d3sc-card">
# MAGIC     <div class="d3sc-card-hdr" style="background: #02A36F">SCHEDULE CRON</div>
# MAGIC     <div class="d3sc-card-body">
# MAGIC       <ul>
# MAGIC         <li>Precise cron expression with <u>required timezone</u></li>
# MAGIC         <li><strong>Best for:</strong> Weekdays at 6 AM, end-of-month, business hours</li>
# MAGIC         <li><strong>Gotcha:</strong> Omitting <code>AT TIME ZONE</code> throws an error</li>
# MAGIC       </ul>
# MAGIC       <div class="d3sc-syntax"><span class="kw">CREATE OR REPLACE MATERIALIZED VIEW</span> mv_weekday<br/>  <span class="kw">SCHEDULE CRON</span> <span class="str">'0 0 6 ? * MON-FRI'</span><br/>  <span class="kw">AT TIME ZONE</span> <span class="str">'America/New_York'</span><br/><span class="kw">AS SELECT</span> ...</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d3sc-card">
# MAGIC     <div class="d3sc-card-hdr" style="background: #E5A100">TRIGGER ON UPDATE</div>
# MAGIC     <div class="d3sc-card-body">
# MAGIC       <ul>
# MAGIC         <li><u>Event-driven</u> &mdash; refreshes when upstream sources change</li>
# MAGIC         <li><strong>Best for:</strong> Near-real-time sync without polling</li>
# MAGIC         <li><strong>Throttle:</strong> <code>AT MOST EVERY</code> prevents over-refreshing</li>
# MAGIC       </ul>
# MAGIC       <div class="d3sc-syntax"><span class="kw">CREATE OR REPLACE MATERIALIZED VIEW</span> mv_trigger<br/>  <span class="kw">TRIGGER ON UPDATE</span><br/>  <span class="kw">AT MOST EVERY</span> 5 MINUTES<br/><span class="kw">AS SELECT</span> ...<br/><span class="kw">FROM</span> base_table</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">SCHEDULE EVERY &mdash; Simple Intervals</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li>You specify a number and a time unit (<code>SECONDS</code>, <code>MINUTES</code>, <code>HOURS</code>, <code>DAYS</code>, <code>WEEKS</code>) and Databricks handles the rest. No cron syntax to learn.</li>
# MAGIC           <li>The interval is measured from the <u>end of the previous refresh</u>, not from a fixed wall-clock anchor &mdash; so <code>EVERY 1 HOUR</code> means "1 hour after the last refresh completed," not "on the hour."</li>
# MAGIC           <li>Best for pipelines where you want a consistent cadence and do not need to control the exact time of day.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">SCHEDULE CRON &mdash; Precise Timing</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Uses standard Quartz cron syntax for precise scheduling &mdash; weekday mornings, end-of-month, intra-day windows tied to a specific region.</li>
# MAGIC           <li>You <u>must</u> include <code>AT TIME ZONE</code>. If you omit it and no session timezone is set, the statement throws an error. Always use a canonical timezone like <code>'UTC'</code> or <code>'America/New_York'</code> &mdash; relying on an implicit session timezone leads to surprises when users in different regions share the same MV.</li>
# MAGIC           <li>Think of SCHEDULE EVERY as an <strong>egg timer</strong> (relative intervals) and SCHEDULE CRON as an <strong>alarm clock</strong> (fixed wall-clock times). Choose the one that matches your SLA.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">TRIGGER ON UPDATE &mdash; Event-Driven</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li>Instead of running on a clock, TRIGGER fires when an <u>upstream source table is modified</u>. Think of it as a <strong>doorbell</strong> &mdash; the refresh rings when data arrives, rather than checking on a timer.</li>
# MAGIC           <li>This is <u>not continuous streaming</u> &mdash; it launches a discrete refresh when a change is detected. For true sub-minute latency, use a Streaming Table instead.</li>
# MAGIC           <li>The optional <code>AT MOST EVERY</code> throttle prevents a high-frequency source from triggering hundreds of refreshes per hour. Minimum interval is <strong>1 minute</strong>.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Combining Policy and Schedule</strong>
# MAGIC         <ul style="margin: 8px 0 14px 0; color: #333; line-height: 1.7">
# MAGIC           <li><code>REFRESH POLICY</code> (the <em>how</em>) and <code>SCHEDULE</code> (the <em>when</em>) are completely orthogonal &mdash; you can pair any combination:
# MAGIC             <ul>
# MAGIC               <li><strong>AUTO + hourly:</strong> <code>REFRESH POLICY AUTO SCHEDULE EVERY 1 HOUR</code></li>
# MAGIC               <li><strong>INCREMENTAL + weekday mornings:</strong> <code>REFRESH POLICY INCREMENTAL SCHEDULE CRON '0 0 6 ? * MON-FRI' AT TIME ZONE 'America/New_York'</code></li>
# MAGIC               <li><strong>STRICT + event-driven:</strong> <code>REFRESH POLICY INCREMENTAL STRICT TRIGGER ON UPDATE AT MOST EVERY 5 MINUTES</code></li>
# MAGIC               <li><strong>FULL + nightly:</strong> <code>REFRESH POLICY FULL SCHEDULE CRON '0 0 2 * * ?' AT TIME ZONE 'UTC'</code></li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>When a schedule is active, Databricks automatically creates a managed <strong>Lakeflow Job</strong> &mdash; visible in Jobs &amp; Pipelines. Your warehouse does <u>not</u> need to be running for scheduled refreshes to execute.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Managing Schedules After Creation</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7">
# MAGIC           <li>You can add, modify, or drop a schedule after the MV is created &mdash; no need to recreate:
# MAGIC             <ul>
# MAGIC               <li><strong>Add:</strong> <code>ALTER MATERIALIZED VIEW mv ADD SCHEDULE CRON '...' AT TIME ZONE '...'</code></li>
# MAGIC               <li><strong>Change:</strong> <code>ALTER MATERIALIZED VIEW mv ALTER SCHEDULE CRON '...' AT TIME ZONE '...'</code></li>
# MAGIC               <li><strong>Remove:</strong> <code>ALTER MATERIALIZED VIEW mv DROP SCHEDULE</code></li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Manual refresh always works, even when a schedule is active: <code>REFRESH MATERIALIZED VIEW my_mv</code> (blocking) or <code>REFRESH MATERIALIZED VIEW my_mv ASYNC</code> (non-blocking).</li>
# MAGIC           <li>If a refresh is still running when the next schedule fires, the overlapping execution is <u>skipped</u> &mdash; no duplicate refreshes, no queuing.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC This lecture introduced the two declarative building blocks for SQL ETL on Databricks &mdash; Streaming Tables for incremental ingestion and Materialized Views for incremental transformations &mdash; along with the operational tools and decision framework you need to put them into production.
# MAGIC
# MAGIC Key takeaways from this lecture include:
# MAGIC <ul style="line-height: 1.7">
# MAGIC <li><strong>Streaming Tables ingest incrementally:</strong> A Streaming Table wraps Auto Loader via <code>read_files()</code>, providing exactly-once processing, automatic schema evolution, and a rescued data column &mdash; all on serverless compute separate from your warehouse.</li>
# MAGIC <li><strong>Auto Loader replaces COPY INTO:</strong> COPY INTO is listing-based and degrades at scale. Auto Loader uses checkpoint-based tracking with file notification mode, handling millions of files &mdash; and Streaming Tables make it accessible with a single SQL statement.</li>
# MAGIC <li><strong>Materialized Views pre-compute and refresh incrementally:</strong> An MV physically stores query results as a Delta table and uses the Enzyme engine to process only what changed &mdash; same answer as a full recompute, far less compute.</li>
# MAGIC <li><strong>MVs replace static Views for analytics:</strong> Views are pay-per-query (full scan every time); MVs are pay-per-refresh (compute once, read many). For BI dashboards with concurrent users, the cost savings are dramatic.</li>
# MAGIC <li><strong>EXPLAIN validates before deployment:</strong> <code>EXPLAIN CREATE MATERIALIZED VIEW</code> is a dry-run check that confirms structural eligibility for incrementalization without creating the MV. Use it in combination with <code>REFRESH POLICY</code> for full control.</li>
# MAGIC <li><strong>REFRESH POLICY controls the how:</strong> Four policies &mdash; AUTO lets the Enzyme cost model decide each refresh; INCREMENTAL prefers incremental with a full fallback; INCREMENTAL STRICT enforces incremental with no fallback (fails rather than surprises you); FULL always rewrites from scratch.</li>
# MAGIC <li><strong>SCHEDULE controls the when &mdash; independently:</strong> SCHEDULE EVERY for simple intervals, SCHEDULE CRON for precise time-aligned runs, and TRIGGER ON UPDATE for event-driven refresh when upstream sources change. Policy and schedule are orthogonal &mdash; combine any pair to match your SLA.</li>
# MAGIC <li><strong>Choose ST for ingestion, MV for transformations:</strong> Streaming Tables handle append-only ingestion and CDC. Materialized Views handle aggregations, joins, and business logic. Together they power the Bronze&ndash;Silver&ndash;Gold medallion pattern.</li>
# MAGIC <li><strong>Serverless compute powers everything under the hood:</strong> Every MV and ST runs on automatically provisioned serverless infrastructure with incremental refresh &mdash; production-grade reliability accessible to SQL analysts via simple DDL statements.</li>
# MAGIC </ul>
# MAGIC
# MAGIC In the next demo (Demo 2.2), you will get hands-on with these building blocks &mdash; creating your first Streaming Tables and Materialized Views against the course dataset.
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333">
# MAGIC         <li>Use Streaming Tables in Lakehouse SQL (<a href="https://docs.databricks.com/aws/en/ldp/dbsql/streaming">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/dbsql/streaming">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/dbsql/streaming">GCP</a>) &mdash; creating, scheduling, and refreshing Streaming Tables with Auto Loader on serverless compute</li>
# MAGIC         <li>Use Materialized Views in Lakehouse SQL (<a href="https://docs.databricks.com/aws/en/ldp/dbsql/materialized">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/dbsql/materialized">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/dbsql/materialized">GCP</a>) &mdash; creating, refreshing, and monitoring Materialized Views with Enzyme incremental refresh</li>
# MAGIC         <li>Incremental Refresh for Materialized Views (<a href="https://docs.databricks.com/aws/en/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">Azure</a> | <a href="https://docs.databricks.com/gcp/en/optimizations/incremental-refresh#support-for-materialized-view-incremental-refresh">GCP</a>) &mdash; supported operations, refresh techniques, and source table requirements for Enzyme</li>
# MAGIC         <li>EXPLAIN CREATE MATERIALIZED VIEW (<a href="https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-explain-materialized-view">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-explain-materialized-view">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-qry-explain-materialized-view">GCP</a>) &mdash; the pre-deployment dry-run command for predicting incrementalization eligibility</li>
# MAGIC         <li>REFRESH POLICY Reference (<a href="https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy">GCP</a>) &mdash; official documentation for AUTO, INCREMENTAL, INCREMENTAL STRICT, and FULL policies</li>
# MAGIC         <li>Lakeflow Spark Declarative Pipelines Concepts (<a href="https://docs.databricks.com/aws/en/ldp/concepts">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/concepts">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/concepts">GCP</a>) &mdash; the serverless framework architecture behind Materialized Views and Streaming Tables</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year">2026</span> Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/">Support</a>
# MAGIC
# MAGIC <script>document.getElementById("dbx-year").textContent = new Date().getFullYear();</script>
