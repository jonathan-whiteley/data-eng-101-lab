# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](../Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.1 Lecture - Orchestrating SQL Pipelines with Lakeflow Jobs
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC You've built the individual layers of a SQL ETL pipeline -- but production requires scheduling, dependency management, error handling, and observability. This lecture introduces Lakeflow Jobs, the Databricks-native orchestration platform that ties it all together.
# MAGIC
# MAGIC In Modules 2 and 3, you built MVs, Streaming Tables, and AUTO CDC dimensions -- but each table exists in isolation. Without orchestration, there is no scheduling, no dependency management, no error routing, and no visibility into what ran, when, or why it broke. Lakeflow Jobs fills this gap with SQL File tasks, DAG dependencies, ForEach iteration, dynamic value references, If/else branching, and full observability -- all without writing a single line of Python.
# MAGIC
# MAGIC This lecture covers five sections that build on each other:
# MAGIC
# MAGIC - **A. The Orchestration Gap** -- why individual tables are not enough for production
# MAGIC - **B. Lakeflow Jobs -- The Orchestration Platform** -- what it is, Jobs/Tasks/Runs, triggers, and the Lakeflow product family
# MAGIC - **C. SQL File Tasks** -- why SQL File is the best task type for analytics engineers, then anatomy: multi-statement execution, parameters, and output capture
# MAGIC - **D. Directed Acyclic Graphs (DAGs)** -- what a DAG is, five common patterns (linear, fan-out, fan-in, diamond, error branch), task dependencies, and Run-if conditions
# MAGIC - **E. The Complete Capability Set** -- dynamic value references, ForEach, If/else, and observability
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain what Lakeflow Jobs are and their main benefits for unified orchestration of data, analytics, and AI workloads
# MAGIC - Identify the core capabilities and use cases enabled by Lakeflow Jobs for Analytics Engineers
# MAGIC - Describe how SQL File tasks execute `.sql` files as pipeline steps within a Lakeflow Job
# MAGIC - Explain how Directed Acyclic Graphs (DAGs) enable task orchestration and workflow patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The Orchestration Gap

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. The Three-Stage Progression
# MAGIC
# MAGIC Analytics Engineers on Databricks don't need to learn everything on day one. Instead, you graduate through three stages as your requirements grow &mdash; each stage unlocks new capabilities while preserving everything you already know.
# MAGIC
# MAGIC <div class="a1-prog-wrap">
# MAGIC <style>
# MAGIC .a1-prog-wrap { max-width: 1100px; margin: 0 auto; font-family: system-ui, -apple-system, sans-serif; }
# MAGIC .a1-prog-flow { display: flex; align-items: stretch; gap: 0; justify-content: center; flex-wrap: nowrap; }
# MAGIC .a1-prog-stage { flex: 1; max-width: 320px; min-height: 340px; background: #F9F7F4; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); display: flex; flex-direction: column; padding: 22px; text-align: left; position: relative; box-sizing: border-box; }
# MAGIC .a1-prog-stage::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 8px; border-radius: 10px 10px 0 0; }
# MAGIC .a1-prog-stage.a1-s1::before { background: #4299E0; }
# MAGIC .a1-prog-stage.a1-s2::before { background: #00A972; }
# MAGIC .a1-prog-stage.a1-s3::before { background: #FF5F46; }
# MAGIC .a1-prog-num { font-size: 36pt; font-weight: 800; opacity: 0.15; margin-bottom: 4px; }
# MAGIC .a1-prog-title { font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px; }
# MAGIC .a1-prog-sub { font-size: 12pt; color: #666; font-style: italic; margin-bottom: 12px; }
# MAGIC .a1-prog-text { font-size: 13pt; color: #0b2026; line-height: 1.55; }
# MAGIC .a1-prog-text ul { margin: 0; padding-left: 18px; }
# MAGIC .a1-prog-text li { margin-bottom: 8px; }
# MAGIC .a1-prog-text li:last-child { margin-bottom: 0; }
# MAGIC .a1-prog-arrow { display: flex; align-items: center; justify-content: center; font-size: 28pt; color: #bbb; padding: 0 8px; }
# MAGIC .a1-prog-badge { display: inline-block; background: #1B3139; color: #fff; font-size: 10pt; font-weight: 700; padding: 3px 10px; border-radius: 20px; margin-bottom: 10px; letter-spacing: 0.05em; }
# MAGIC </style>
# MAGIC <div class="a1-prog-flow">
# MAGIC   <div class="a1-prog-stage a1-s1">
# MAGIC     <div class="a1-prog-badge">MODULES 2&ndash;3</div>
# MAGIC     <div class="a1-prog-num">1</div>
# MAGIC     <div class="a1-prog-title">Standalone MVs / STs</div>
# MAGIC     <div class="a1-prog-sub">SQL Editor &middot; one table at a time</div>
# MAGIC     <div class="a1-prog-text">
# MAGIC       <ul>
# MAGIC         <li>Create individual MVs and STs in the SQL Editor</li>
# MAGIC         <li>Each MV/ST gets its own <strong>auto-created SDP pipeline</strong> under the hood</li>
# MAGIC         <li>Good for <strong>getting started</strong>, quick reporting, and BI acceleration</li>
# MAGIC         <li>Familiar SQL &mdash; <code>CREATE MATERIALIZED VIEW</code>, <code>CREATE STREAMING TABLE</code></li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="a1-prog-arrow"><svg width="36" height="24" viewBox="0 0 36 24"><path d="M0 12 L26 12 M20 5 L28 12 L20 19" fill="none" stroke="#bbb" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/></svg></div>
# MAGIC   <div class="a1-prog-stage a1-s2">
# MAGIC     <div class="a1-prog-badge">THIS LECTURE</div>
# MAGIC     <div class="a1-prog-num">2</div>
# MAGIC     <div class="a1-prog-title">Lakeflow Jobs</div>
# MAGIC     <div class="a1-prog-sub">Cross-pipeline orchestration</div>
# MAGIC     <div class="a1-prog-text">
# MAGIC       <ul>
# MAGIC         <li><strong>SQL File tasks</strong> &mdash; parameterized <code>.sql</code> files as pipeline steps</li>
# MAGIC         <li><strong>ForEach iteration</strong> &mdash; loop over dimensions, tables, or regions</li>
# MAGIC         <li><strong>If/else branching</strong> &mdash; data-driven routing based on SQL output</li>
# MAGIC         <li><strong>Repair Run</strong> &mdash; re-execute only failed tasks, not the whole pipeline</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="margin-top: auto; padding-top: 12px; border-top: 1px solid #e0ddd8; font-size: 10pt; color: #888; line-height: 1.5;">Deeper dive: <a href="https://www.databricks.com/training/catalog/deploy-workloads-with-lakeflow-jobs-1684" style="color:#888; text-decoration:underline;">Deploy Workloads with Lakeflow Jobs</a> (4h ILT)</div>
# MAGIC   </div>
# MAGIC   <div class="a1-prog-arrow"><svg width="36" height="24" viewBox="0 0 36 24"><path d="M0 12 L26 12 M20 5 L28 12 L20 19" fill="none" stroke="#bbb" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/></svg></div>
# MAGIC   <div class="a1-prog-stage a1-s3" style="opacity: 0.55;">
# MAGIC     <div class="a1-prog-badge" style="background: #666;">OUT OF SCOPE</div>
# MAGIC     <div class="a1-prog-num">3</div>
# MAGIC     <div class="a1-prog-title">Full SDP Pipeline Mode</div>
# MAGIC     <div class="a1-prog-sub">Multi-file editor &middot; grouped tables</div>
# MAGIC     <div class="a1-prog-text">
# MAGIC       <ul>
# MAGIC         <li><strong>Data quality expectations</strong> &mdash; CONSTRAINT syntax with three violation actions</li>
# MAGIC         <li><strong>Multi-table DAGs</strong> &mdash; automatic dependency resolution across files</li>
# MAGIC         <li><strong>PRIVATE tables</strong> &mdash; intermediate results not registered in Unity Catalog</li>
# MAGIC         <li><strong>Unified event_log</strong> &mdash; one log for the entire pipeline</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="margin-top: auto; padding-top: 12px; border-top: 1px solid #e0ddd8; font-size: 10pt; color: #888; line-height: 1.5;">Deeper dive: <a href="https://www.databricks.com/training/catalog/build-data-pipelines-with-lakeflow-spark-declarative-pipelines-1686" style="color:#888; text-decoration:underline;">Build Data Pipelines with Spark Declarative Pipelines</a> (4h ILT)</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">What You Built So Far</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>In Module 2, you created <strong>Streaming Tables</strong> for incremental ingestion and <strong>Materialized Views</strong> for pre-computed Silver-to-Gold transformations</li>
# MAGIC           <li>In Module 3, you created <strong>AUTO CDC Streaming Tables</strong> for SCD Type 1 and Type 2 dimension maintenance</li>
# MAGIC           <li>Each of these is a standalone object &mdash; it knows how to refresh itself, and Databricks created an auto-managed SDP pipeline behind the scenes for each one</li>
# MAGIC           <li>This is Stage 1 of the progression &mdash; you are already using SDP without realizing it</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Why Graduate Beyond Standalone?</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>No scheduling or dependency awareness:</strong> Each standalone object refreshes independently. Silver has no idea whether Bronze finished. Gold has no idea whether Silver is current. Scheduling each on its own cron can lead to stale or incorrect data.</li>
# MAGIC           <li><strong>No error routing:</strong> If a refresh fails, there is no built-in mechanism to quarantine bad data, send an alert, or branch to an alternative path. You discover failures manually.</li>
# MAGIC           <li><strong>No parameterized iteration:</strong> You cannot loop over a list of tables or regions &mdash; each refresh must be defined individually.</li>
# MAGIC           <li><strong>Per-object monitoring:</strong> You check each MV or ST individually for refresh status. With 20+ tables, this becomes unmanageable.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">The Key Insight</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The progression is not about replacing what you know &mdash; it is about <strong>unlocking more</strong>. The same <code>CREATE MATERIALIZED VIEW</code> and <code>CREATE STREAMING TABLE</code> SQL you wrote in Modules 2&ndash;3 works identically inside Lakeflow Jobs. You just add orchestration on top.</li>
# MAGIC           <li>Think of it like driving: Stage 1 is local roads (standalone MVs/STs). Stage 2 is the logistics network (Lakeflow Jobs &mdash; coordinating multiple vehicles across routes). Stage 3 is the advanced highway system (full SDP pipeline mode &mdash; multi-table DAGs with quality gates and private tables).</li>
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
# MAGIC ### A2. Decision Framework &mdash; Which Stage Is Right for Your Use Case?
# MAGIC
# MAGIC Not every pipeline needs every capability. Use this decision framework to choose the right stage for your current requirements &mdash; and know when to graduate to the next one.
# MAGIC
# MAGIC <div class="a2-dec-wrap">
# MAGIC <style>
# MAGIC .a2-dec-wrap { max-width: 1000px; margin: 0 auto; font-family: system-ui, -apple-system, sans-serif; }
# MAGIC .a2-dec-table { width: 100%; border-collapse: separate; border-spacing: 0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 12px rgba(27,49,57,0.10); }
# MAGIC .a2-dec-table thead th { background: #1B3139; color: #fff; font-size: 14pt; font-weight: 700; letter-spacing: 0.03em; padding: 14px 18px; text-align: center; border: none; }
# MAGIC .a2-dec-table thead th:first-child { text-align: left; min-width: 260px; }
# MAGIC .a2-dec-table tbody td { padding: 12px 18px; font-size: 13pt; text-align: center; border-bottom: 1px solid #e4e7ec; color: #1B3139; }
# MAGIC .a2-dec-table tbody td:first-child { text-align: left; font-weight: 600; }
# MAGIC .a2-dec-table tbody tr:nth-child(odd) { background: #fff; }
# MAGIC .a2-dec-table tbody tr:nth-child(even) { background: #f8f9fb; }
# MAGIC .a2-dec-table tbody tr:last-child td { border-bottom: none; }
# MAGIC .a2-check { color: #00A972; font-weight: 700; font-size: 1.2em; }
# MAGIC .a2-cross { color: #D13438; font-weight: 700; font-size: 1.2em; }
# MAGIC .a2-dec-table thead th:nth-child(2) { background: #265775; }
# MAGIC .a2-dec-table thead th:nth-child(3) { background: #1a5c42; }
# MAGIC .a2-dec-table thead th:nth-child(4) { background: #8c2b1e; }
# MAGIC </style>
# MAGIC <table class="a2-dec-table">
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th>Requirement</th>
# MAGIC       <th>&#x2460; Standalone</th>
# MAGIC       <th>&#x2461; Lakeflow Jobs</th>
# MAGIC       <th>&#x2462; SDP Pipeline</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td>Simple reporting / BI acceleration</td>
# MAGIC       <td><span class="a2-check">&#x2713;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Scheduled refresh with dependencies</td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> DAG with "Depends on"</td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> Automatic dependency resolution</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Parameterized business logic</td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> {{catalog}}, :param</td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> <code>${var}</code> config params</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>ForEach iteration over dimensions</td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span></td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>If/else branching on SQL output</td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span></td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Cross-pipeline orchestration</td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span></td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Repair Run (re-execute failed subgraph)</td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> Re-run failed tasks + dependents</td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> Selective refresh of failed tables + downstream</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Data quality constraints (CONSTRAINT syntax)</td>
# MAGIC       <td>STs <span class="a2-check">&#x2713;</span> &nbsp; MVs <span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span> (in your SQL only)</td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> CONSTRAINT syntax (STs + MVs)</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>PRIVATE intermediate tables</td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-cross">&#x2717;</span></td>
# MAGIC       <td><span class="a2-check">&#x2713;</span></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Unified event_log monitoring</td>
# MAGIC       <td>Per-table event_logs (fragmented)</td>
# MAGIC       <td>Per-task run logs + system tables</td>
# MAGIC       <td><span class="a2-check">&#x2713;</span> One log for entire pipeline</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">How to Read This Table</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The table is ordered so that <strong>Lakeflow Jobs capabilities appear first</strong> (rows 2&ndash;7) since this is the focus of this lecture, followed by SDP-specific capabilities (rows 8&ndash;10) that are out of scope.</li>
# MAGIC           <li><strong>Standalone (Stage 1)</strong> covers what you built in Modules 2&ndash;3: individual MVs and STs created in the SQL Editor, each with an auto-managed SDP pipeline behind the scenes. Good for getting started and simple reporting.</li>
# MAGIC           <li><strong>Lakeflow Jobs (Stage 2, this lecture)</strong> adds orchestration: scheduled dependencies between tasks, parameterized SQL files, ForEach iteration, If/else branching, cross-pipeline coordination, and Repair Run. This is where standalone tables become a production pipeline.</li>
# MAGIC           <li><strong>SDP Pipeline Mode (Stage 3, out of scope)</strong> adds capabilities within a single pipeline: CONSTRAINT syntax for data quality on both STs and MVs, PRIVATE intermediate tables hidden from Unity Catalog, automatic dependency resolution across files, and a unified event_log. These are covered in the dedicated <a href="https://www.databricks.com/training/catalog/build-data-pipelines-with-lakeflow-spark-declarative-pipelines-1686">Build Data Pipelines with Spark Declarative Pipelines</a> course.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">When to Graduate from Stage 1 to Stage 2 (Lakeflow Jobs)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>You need <strong>scheduled, dependency-aware execution</strong> &mdash; Silver should not refresh until Bronze completes</li>
# MAGIC           <li>You need <strong>error routing</strong> &mdash; when a task fails, quarantine data or send an alert automatically</li>
# MAGIC           <li>You need <strong>parameterized iteration</strong> &mdash; loop over a list of tables, regions, or date ranges</li>
# MAGIC           <li>You need <strong>cross-pipeline orchestration</strong> &mdash; coordinate SQL refreshes with notebook tasks, ML training, or other Lakeflow Jobs</li>
# MAGIC           <li>You need <strong>Repair Run</strong> &mdash; re-execute only failed tasks instead of the entire pipeline</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">When to Graduate from Stage 2 to Stage 3 (SDP Pipeline Mode)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>You need <strong>declarative data quality constraints</strong> with violation actions (warn, drop, fail) applied directly to MVs and STs</li>
# MAGIC           <li>You need <strong>PRIVATE intermediate tables</strong> that are not registered in Unity Catalog &mdash; useful for staging data without cluttering the catalog</li>
# MAGIC           <li>You need <strong>automatic dependency resolution</strong> across multiple SQL files in a single pipeline &mdash; the engine infers which tables depend on which</li>
# MAGIC           <li>You need a <strong>unified event_log</strong> for the entire pipeline rather than per-task monitoring</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">They Are Complementary, Not Competing</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Lakeflow Jobs and SDP Pipeline Mode are not mutually exclusive. In production, many teams use <strong>both</strong>: SDP pipelines handle the ETL transformations with quality constraints, and a Lakeflow Job orchestrates the SDP pipeline alongside other tasks (notifications, downstream refreshes, ML training).</li>
# MAGIC           <li>The progression is about what you learn <em>next</em>, not what you replace. Each stage builds on the previous one.</li>
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
# MAGIC ## B. Lakeflow Jobs &mdash; The Orchestration Platform
# MAGIC
# MAGIC With the orchestration gap established, let's examine the platform that fills it. This section covers what Lakeflow Jobs is, how it works, and the core terminology you need before diving into tasks and DAGs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. What Are Lakeflow Jobs?
# MAGIC
# MAGIC **Lakeflow Jobs** is the Databricks-native orchestration service for scheduling, managing, and monitoring workflows across data engineering, analytics, and AI workloads. It runs over **100 million jobs per week** across 14,600+ customers and 187,000+ weekly users &mdash; making it one of the largest managed workflow orchestration systems in the industry.
# MAGIC
# MAGIC <div class="b1v-wrap">
# MAGIC <style>
# MAGIC .b1v-wrap { max-width: 1100px; margin: 0 auto; }
# MAGIC .b1v-sides { display: flex; justify-content: center; align-items: stretch; gap: 40px; flex-wrap: wrap; }
# MAGIC .b1v-box { width: 340px; min-height: 220px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); display: flex; flex-direction: column; justify-content: flex-start; gap: 12px; padding: 24px; text-align: center; position: relative; box-sizing: border-box; overflow: hidden; }
# MAGIC .b1v-box::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 8px; }
# MAGIC .b1v-box.b1v-one::before { background: #4299E0; }
# MAGIC .b1v-box.b1v-two::before { background: #00A972; }
# MAGIC .b1v-box.b1v-three::before { background: #FF5F46; }
# MAGIC .b1v-icon { font-size: 36pt; margin-top: 12px; }
# MAGIC .b1v-title { font-size: 18pt; font-weight: 700; color: #0b2026; }
# MAGIC .b1v-text { font-size: 14pt; color: #0b2026; line-height: 1.55; text-align: left; }
# MAGIC </style>
# MAGIC <div class="b1v-sides">
# MAGIC <div class="b1v-box b1v-one">
# MAGIC <div class="b1v-icon"><img src="../Includes/images/icons/input-ingest-icon.png" height="48" style="object-fit: contain;"></div>
# MAGIC <div class="b1v-title">Lakeflow Connect</div>
# MAGIC <div class="b1v-text">Managed ingestion connectors that pull data from external sources into your lakehouse</div>
# MAGIC </div>
# MAGIC <div class="b1v-box b1v-two">
# MAGIC <div class="b1v-icon"><img src="../Includes/images/icons/cache-sync-icon.png" height="48" style="object-fit: contain;"></div>
# MAGIC <div class="b1v-title">Lakeflow Spark Declarative Pipelines (SDP)</div>
# MAGIC <div class="b1v-text">The serverless ETL engine powering your MVs and Streaming Tables</div>
# MAGIC </div>
# MAGIC <div class="b1v-box b1v-three">
# MAGIC <div class="b1v-icon"><img src="../Includes/images/icons/massive-scale-data-eng.png" height="48" style="object-fit: contain;"></div>
# MAGIC <div class="b1v-title">Lakeflow Jobs</div>
# MAGIC <div class="b1v-text">The orchestration layer: scheduling, task dependencies, control flow, and monitoring for all workloads</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">The Lakeflow Product Family</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Lakeflow Jobs sits within the broader <strong>Lakeflow</strong> product family, which includes three components:
# MAGIC             <ul>
# MAGIC               <li><strong>Lakeflow Connect:</strong> Managed ingestion connectors that pull data from external sources into your lakehouse</li>
# MAGIC               <li><strong>Lakeflow Spark Declarative Pipelines (SDP):</strong> The engine that powers your Materialized Views and Streaming Tables under the hood &mdash; you have been using SDP since Module 2 without realizing it</li>
# MAGIC               <li><strong>Lakeflow Jobs:</strong> The orchestration layer that schedules and coordinates tasks &mdash; including SQL tasks, SDP pipeline refreshes, Python notebooks, Spark JARs, and more &mdash; into managed workflows</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>The name "Lakeflow Jobs" replaced the earlier names "Databricks Workflows" and "Databricks Jobs." In the Databricks UI, the sidebar label is <strong>Jobs and Pipelines</strong>. You may encounter the older names in blog posts, community forums, or older documentation &mdash; they refer to the same service.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Unified Orchestration &mdash; Not Just for SQL</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>While this course focuses on SQL workloads, Lakeflow Jobs orchestrates <strong>any Databricks workload</strong> in a single job &mdash; SQL tasks, Python notebooks, Spark JARs, SDP pipeline tasks, dbt tasks, and even other Lakeflow Jobs (via the "Run Job" task type)</li>
# MAGIC           <li>This means an analytics engineer can own the SQL transformation layer while a data scientist adds a model training step in the same pipeline &mdash; both coordinated by one orchestrator with one monitoring interface</li>
# MAGIC           <li>Think of it as a universal remote control: instead of needing separate orchestrators for SQL pipelines, notebook workflows, and ML training, one platform manages everything</li>
# MAGIC           <li>A common misconception: "Lakeflow Jobs is just a scheduler." It is not &mdash; it is a <strong>full orchestration engine</strong> with task dependencies, data flow between tasks, conditional branching, parameterized iteration, and rich observability. Scheduling is one component of a much larger system.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">In This Course</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>This lecture introduces the platform and its core concepts. Demo 4.2 covers the implementation details &mdash; SQL File task configuration, dynamic value references, and the diamond-pattern DAG in action. Lab 4.3 then puts it all together.</li>
# MAGIC           <li>The key capabilities, triggers, monitoring, and repair features are covered in detail across Sections B through E of this lecture and in the next demo.</li>
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
# MAGIC ### B2. Jobs, Tasks, and Runs &mdash; Core Terminology
# MAGIC
# MAGIC Before we build anything, you need three terms: a **Job** is the workflow definition, a **Task** is a single step within the job, and a **Run** is one execution of the job. Everything else in Lakeflow Jobs builds on these three concepts.
# MAGIC
# MAGIC <!-- ── Visual: lakeflow-ui-mockup v2 ── -->
# MAGIC <div class="b2v-wrap">
# MAGIC <style>
# MAGIC .b2v-wrap { max-width:1060px; margin:0 auto; font-family:system-ui,-apple-system,sans-serif; color:#0b2026; }
# MAGIC .b2v-btns { display:flex; gap:10px; margin-bottom:14px; justify-content:center; }
# MAGIC .b2v-btn { padding:10px 28px; border-radius:8px; font-size:15pt; font-weight:700; cursor:pointer; border:3px solid transparent; transition:all 0.2s; user-select:none; }
# MAGIC .b2v-btn:hover { transform:translateY(-2px); box-shadow:0 3px 10px rgba(0,0,0,0.12); }
# MAGIC .b2v-btn.b2v-on { border-color:currentColor; box-shadow:0 3px 12px rgba(0,0,0,0.15); transform:translateY(-2px); }
# MAGIC .b2v-ui { background:#fff; border:1px solid #ddd; border-radius:10px; overflow:visible; box-shadow:0 2px 12px rgba(0,0,0,0.06); position:relative; transition:outline 0.3s; }
# MAGIC .b2v-tb { background:#f8f9fa; border-bottom:1px solid #e0e0e0; padding:10px 16px; display:flex; align-items:center; gap:14px; font-size:13pt; border-radius:10px 10px 0 0; }
# MAGIC .b2v-tb-title { font-weight:700; font-size:15pt; color:#1B3139; }
# MAGIC .b2v-tb-tabs { display:flex; gap:0; margin-left:20px; }
# MAGIC .b2v-tb-tab { padding:6px 14px; font-size:13pt; color:#666; border-bottom:2px solid transparent; }
# MAGIC .b2v-tb-tab.b2v-tba { color:#1B3139; font-weight:600; border-bottom-color:#1B3139; }
# MAGIC .b2v-tb-meta { margin-left:auto; font-size:12pt; color:#888; transition:all 0.3s; padding:2px 4px; border-radius:4px; }
# MAGIC /* ── DAG grid layout ── */
# MAGIC .b2v-dag { position:relative; padding:20px 16px 14px; }
# MAGIC .b2v-grid { display:grid; grid-template-columns:200px 60px 200px 60px 200px; grid-template-rows:auto 20px auto; align-items:center; justify-content:center; gap:0; }
# MAGIC /* ── Task card ── */
# MAGIC .b2v-tk { background:#fff; border:1px solid #ddd; border-radius:8px; box-shadow:0 1px 4px rgba(0,0,0,0.06); overflow:hidden; transition:all 0.3s; }
# MAGIC .b2v-tk-bar { height:5px; }
# MAGIC .b2v-tk-bar.ok { background:#00A972; } .b2v-tk-bar.fl { background:#D93025; }
# MAGIC .b2v-tk-bd { padding:10px 12px; }
# MAGIC .b2v-tk-nm { font-size:13pt; font-weight:700; color:#1B3139; }
# MAGIC .b2v-tk-st { font-size:11pt; color:#666; margin-top:3px; transition:all 0.3s; padding:1px 2px; border-radius:3px; }
# MAGIC .b2v-tk-fl { font-size:10pt; color:#999; margin-top:4px; }
# MAGIC .b2v-ok { color:#00A972; } .b2v-fl { color:#D93025; }
# MAGIC /* ── SVG arrows ── */
# MAGIC .b2v-arrows { position:absolute; top:0; left:0; width:100%; height:100%; pointer-events:none; }
# MAGIC .b2v-arrows line { stroke:#ccc; stroke-width:1.5; }
# MAGIC .b2v-arrows polygon { fill:#ccc; }
# MAGIC /* ── Arrow cells in grid ── */
# MAGIC .b2v-acell { display:flex; align-items:center; justify-content:center; color:#bbb; font-size:22pt; }
# MAGIC /* ── Highlights ── */
# MAGIC .b2v-ui.hl-job { outline:3px dashed #2574B5; outline-offset:6px; border-radius:14px; }
# MAGIC .b2v-ui.hl-task .b2v-tk { box-shadow:0 0 0 3px #02A36F, 0 4px 12px rgba(2,163,111,0.2); transform:scale(1.02); }
# MAGIC .b2v-ui.hl-run .b2v-tk-st { background:#FFF3CD; }
# MAGIC .b2v-ui.hl-run .b2v-tb-meta { background:#FFF3CD; font-weight:600; color:#333; }
# MAGIC /* ── Detail panel ── */
# MAGIC .b2v-det { background:#fff; border:1px solid #e0e3ea; border-radius:10px; padding:16px 20px; margin-top:14px; min-height:70px; box-shadow:0 2px 6px rgba(0,0,0,0.04); transition:border-color 0.2s; }
# MAGIC .b2v-det-hd { display:flex; align-items:center; gap:8px; margin-bottom:8px; padding-bottom:8px; border-bottom:2px solid #f0f0f0; }
# MAGIC .b2v-det-dot { width:12px; height:12px; border-radius:50%; }
# MAGIC .b2v-det-t { font-size:18pt; font-weight:700; }
# MAGIC .b2v-det-b { font-size:14pt; line-height:1.7; color:#444; }
# MAGIC .b2v-det-b ul { padding-left:20px; margin:6px 0; }
# MAGIC .b2v-det-b li { margin-bottom:4px; font-size:14pt; }
# MAGIC .b2v-det-b code { background:#f0f2f7; padding:2px 5px; border-radius:4px; font-size:13pt; font-family:monospace; }
# MAGIC .b2v-det-b strong { font-weight:600; color:#0b2026; }
# MAGIC </style>
# MAGIC
# MAGIC <p style="font-size:14pt;color:#555;text-align:center;margin-bottom:10px;">Click each concept to highlight it in the Lakeflow Jobs UI below.</p>
# MAGIC
# MAGIC <div class="b2v-btns">
# MAGIC   <div class="b2v-btn b2v-on" onclick="b2v(0)" style="background:#EBF3FB;color:#2574B5;">Job</div>
# MAGIC   <div class="b2v-btn" onclick="b2v(1)" style="background:#E6F9F0;color:#02A36F;">Task</div>
# MAGIC   <div class="b2v-btn" onclick="b2v(2)" style="background:#FFF8E1;color:#E6A100;">Run</div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="b2v-ui" id="b2v-ui">
# MAGIC   <div class="b2v-tb">
# MAGIC     <div class="b2v-tb-title">Sales ETL Pipeline</div>
# MAGIC     <div class="b2v-tb-tabs"><div class="b2v-tb-tab b2v-tba">Graph</div><div class="b2v-tb-tab">Timeline</div><div class="b2v-tb-tab">List</div></div>
# MAGIC     <div class="b2v-tb-meta">Run #47 &middot; Apr 8, 3:15 AM &middot; 7m 11s</div>
# MAGIC   </div>
# MAGIC   <div class="b2v-dag">
# MAGIC     <div class="b2v-grid" style="grid-template-columns:210px 50px 210px 50px 210px;grid-template-rows:auto;">
# MAGIC       <!-- 3 sequential tasks: refresh_bronze → refresh_silver → refresh_gold -->
# MAGIC       <div class="b2v-tk"><div class="b2v-tk-bar ok"></div><div class="b2v-tk-bd"><div class="b2v-tk-nm">refresh_bronze <span class="b2v-ok"><img src="../Includes/images/icons/checkmark-success.png" height="14" style="vertical-align:middle;"></span></div><div class="b2v-tk-st">Succeeded &middot; 3m 42s</div><div class="b2v-tk-fl"><img src="../Includes/images/icons/file-code-icon.png" height="14" style="vertical-align:middle;"> demo_refresh_bronze.sql</div></div></div>
# MAGIC       <div class="b2v-acell">&#x2192;</div>
# MAGIC       <div class="b2v-tk"><div class="b2v-tk-bar ok"></div><div class="b2v-tk-bd"><div class="b2v-tk-nm">refresh_silver <span class="b2v-ok"><img src="../Includes/images/icons/checkmark-success.png" height="14" style="vertical-align:middle;"></span></div><div class="b2v-tk-st">Succeeded &middot; 2m 18s</div><div class="b2v-tk-fl"><img src="../Includes/images/icons/file-code-icon.png" height="14" style="vertical-align:middle;"> demo_refresh_silver.sql</div></div></div>
# MAGIC       <div class="b2v-acell">&#x2192;</div>
# MAGIC       <div class="b2v-tk"><div class="b2v-tk-bar fl"></div><div class="b2v-tk-bd"><div class="b2v-tk-nm">refresh_gold <span class="b2v-fl"><img src="../Includes/images/icons/x-circle-icon.png" height="14" style="vertical-align:middle;"></span></div><div class="b2v-tk-st">Failed &middot; 1m 11s</div><div class="b2v-tk-fl"><img src="../Includes/images/icons/file-code-icon.png" height="14" style="vertical-align:middle;"> demo_refresh_gold.sql</div></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="b2v-det" id="b2v-det"></div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var b2vD=[
# MAGIC   {t:'Job',bg:'#2574B5',h:'<strong>The entire workflow above is one Job &mdash; "Sales ETL Pipeline."</strong><ul><li>A Job is the <strong>workflow definition</strong>: what tasks to run, how they depend on each other, and when to trigger</li><li>The blue dashed outline shows the Job boundary &mdash; everything inside belongs to this single Job</li><li>This Job contains <strong>3 tasks</strong> in a linear chain: refresh orders, refresh customers, then aggregate into Gold</li><li>Jobs can be triggered by <strong>cron schedule</strong>, <strong>file arrival</strong>, <strong>table update</strong>, or <strong>on-demand</strong></li></ul>'},
# MAGIC   {t:'Task',bg:'#02A36F',h:'<strong>Each card in the DAG is one Task &mdash; a single unit of work.</strong><ul><li><strong>Task key:</strong> The unique name on each card &mdash; <code>refresh_bronze</code>, <code>refresh_silver</code>, <code>refresh_gold</code></li><li><strong>Type:</strong> These are all SQL File tasks (note the <code>.sql</code> file names). Tasks can also be Notebooks, Python scripts, SDP pipelines, dbt, ForEach, If/else, Run Job, and more</li><li><strong>Dependencies:</strong> The arrows show execution order &mdash; <code>refresh_silver</code> waits for <code>refresh_bronze</code>; <code>refresh_gold</code> waits for <code>refresh_silver</code></li><li><strong>In this course:</strong> Each task runs a <code>.sql</code> file that refreshes the Streaming Tables, Materialized Views, and AUTO CDC dimensions you built in Modules 2&ndash;3</li></ul>'},
# MAGIC   {t:'Run',bg:'#E6A100',h:'<strong>The highlighted metadata is the Run &mdash; one execution of the Job.</strong><ul><li>This is <strong>Run #47</strong>, started <strong>Apr 8, 3:15 AM</strong>, total duration <strong>7m 11s</strong> (3m42s + 2m18s + 1m11s)</li><li>Each task card shows its <strong>Task Run</strong>: status (Succeeded or Failed), duration, and the colored status bar (green = success, red = failure)</li><li>Notice <code>refresh_gold</code> failed &mdash; because it depends on the upstream tasks, you know the issue is in the Gold aggregation itself, not in the data it consumed. This is the power of orchestrated dependency tracking.</li><li>Run history is retained <strong>60 days</strong> in the UI and queryable indefinitely via <code>system.lakeflow</code> system tables</li></ul>'}
# MAGIC ];
# MAGIC window.b2v = function(i){
# MAGIC   document.querySelectorAll('.b2v-btn').forEach(function(b,j){b.classList.toggle('b2v-on',j===i);});
# MAGIC   var u=document.getElementById('b2v-ui');
# MAGIC   u.className='b2v-ui'+(i===0?' hl-job':'')+(i===1?' hl-task':'')+(i===2?' hl-run':'');
# MAGIC   var d=b2vD[i],p=document.getElementById('b2v-det');
# MAGIC   p.style.borderColor=d.bg;
# MAGIC   p.innerHTML='<div class="b2v-det-hd"><div class="b2v-det-dot" style="background:'+d.bg+'"></div><div class="b2v-det-t" style="color:'+d.bg+'">'+d.t+'</div></div><div class="b2v-det-b">'+d.h+'</div>';
# MAGIC }
# MAGIC b2v(0);
# MAGIC </script>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">Job &mdash; The Workflow Definition (Blue Outline in the Visual)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>In the visual above, the blue dashed outline represents the <strong>Job</strong> boundary. Everything inside &mdash; the toolbar, the DAG canvas, and every task card &mdash; belongs to this one Job named "Sales ETL Pipeline."</li>
# MAGIC           <li>A Job specifies four things: <strong>what</strong> tasks to run, <strong>how</strong> they depend on each other (the DAG), <strong>when</strong> to trigger, and <strong>who</strong> to notify on success or failure</li>
# MAGIC           <li>Jobs can be triggered by <strong>cron schedule</strong> (e.g., <code>0 3 * * *</code> for daily at 3 AM), <strong>file arrival</strong> (new files in cloud storage), <strong>table update</strong> (a Unity Catalog table is modified), or <strong>manual/API</strong> execution</li>
# MAGIC           <li>If a job is already running when a new trigger fires, the new run is <strong>queued</strong> by default, preventing concurrent runs from conflicting</li>
# MAGIC           <li>For the pipeline you built in Modules 2&ndash;3, a daily cron schedule or a table update trigger on the Bronze source tables would be the typical production configuration</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Task &mdash; A Single Unit of Work (Green-Highlighted Cards)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Click "Task" in the visual to see all three cards glow green &mdash; each card is one Task. The <strong>task key</strong> is the name on the card (<code>refresh_bronze</code>, <code>refresh_silver</code>, <code>refresh_gold</code>).</li>
# MAGIC           <li>Each task has a <strong>type</strong> that determines what it does. In this course, you will use SQL File tasks (note the <code>.sql</code> filenames on each card). The full set of task types includes:
# MAGIC             <ul>
# MAGIC               <li><strong>SQL tasks:</strong> SQL File, SQL Query, SQL Alert (Beta)</li>
# MAGIC               <li><strong>Compute tasks:</strong> Notebook, Python script, JAR, Spark Submit</li>
# MAGIC               <li><strong>Pipeline tasks:</strong> Lakeflow SDP Pipeline, dbt</li>
# MAGIC               <li><strong>Control flow:</strong> If/else (conditional branching), ForEach (iteration over arrays)</li>
# MAGIC               <li><strong>Composition:</strong> Run Job (trigger another Lakeflow Job as a nested workflow)</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Each task has <strong>dependencies</strong> on other tasks (the arrows between cards) and a <strong>compute target</strong> (SQL warehouse or cluster) that executes it</li>
# MAGIC           <li>The variety of task types is what makes Lakeflow Jobs <strong>unified</strong> orchestration &mdash; SQL, Python, Spark, ML, and control flow all in one pipeline</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Run &mdash; One Execution of the Job (Yellow-Highlighted Metadata)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Click "Run" in the visual to see the status metadata highlight in yellow &mdash; the toolbar shows <strong>Run #47</strong> with its start time and total duration, and each card shows its own <strong>Task Run</strong> status and timing.</li>
# MAGIC           <li>One Job can produce many Runs over time. Each Run is an <strong>immutable record</strong> &mdash; you can inspect Run #47 days later to see exactly what happened, how long each task took, and where the failure occurred.</li>
# MAGIC           <li>Each task in a Run produces a <strong>Task Run</strong> with its own status (Succeeded, Failed, Skipped), duration, output data, and logs. The green/red status bar on each card comes from the Task Run.</li>
# MAGIC           <li>Run history is retained <strong>60 days</strong> in the UI. For longer retention and custom analysis, query <code>system.lakeflow.job_run_timeline</code> and <code>system.lakeflow.job_task_run_timeline</code> with SQL.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">How They Fit Together</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>One <strong>Job</strong> contains one or more <strong>Tasks</strong>. When the Job triggers, it creates a <strong>Run</strong>. Each Task in the Run produces a <strong>Task Run</strong> with its own status, duration, and output.</li>
# MAGIC           <li>You can have multiple Runs of the same Job in flight simultaneously (concurrent runs), or configure the Job to queue new triggers until the current Run completes</li>
# MAGIC           <li>In our visual: the "Sales ETL Pipeline" Job defines 3 Tasks in a chain. Run #47 executed at 3:15 AM &mdash; <code>refresh_bronze</code> and <code>refresh_silver</code> succeeded, but <code>refresh_gold</code> failed. Demo 4.2 covers how to use <strong>Repair Run</strong> to re-execute only the failed task without rerunning the tasks that already succeeded.</li>
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
# MAGIC ## C. SQL File Tasks &mdash; Your Pipeline Building Block
# MAGIC
# MAGIC Section B established what Lakeflow Jobs is and how it works. Now we turn to the specific task type that matters most for SQL-first analytics engineers: the **SQL File task**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Why SQL File Tasks?
# MAGIC
# MAGIC Lakeflow Jobs supports many task types &mdash; SQL File, SQL Query, Notebook, Python, SDP Pipeline, dbt, and more. For SQL-first analytics engineers building ETL pipelines, **SQL File tasks** are the standard choice. Here is why.
# MAGIC
# MAGIC <div class="c2v-wrap">
# MAGIC <style>
# MAGIC .c2v-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 900px; margin: 0 auto; padding: 0; }
# MAGIC .c2v-table { width: 100%; border-collapse: separate; border-spacing: 0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 12px rgba(27,49,57,0.10); }
# MAGIC .c2v-table thead th { background: #1B3139; color: #fff; font-size: 14pt; font-weight: 700; letter-spacing: 0.03em; padding: 14px 20px; text-align: center; border: none; }
# MAGIC .c2v-table thead th:first-child { text-align: left; min-width: 180px; border-radius: 10px 0 0 0; }
# MAGIC .c2v-table thead th:last-child { border-radius: 0 10px 0 0; }
# MAGIC .c2v-table tbody td { padding: 13px 20px; font-size: 14pt; text-align: center; border-bottom: 1px solid #e4e7ec; color: #1B3139; }
# MAGIC .c2v-table tbody td:first-child { text-align: left; font-weight: 600; color: #1B3139; background: inherit; }
# MAGIC .c2v-table tbody tr:nth-child(odd) { background: #fff; }
# MAGIC .c2v-table tbody tr:nth-child(even) { background: #f8f9fb; }
# MAGIC .c2v-table tbody tr:last-child td:first-child { border-radius: 0 0 0 10px; }
# MAGIC .c2v-table tbody tr:last-child td:last-child { border-radius: 0 0 10px 0; }
# MAGIC .c2v-table tbody tr:last-child td { border-bottom: none; }
# MAGIC /* Highlight the SQL File column */
# MAGIC .c2v-table thead th:nth-child(2) { background: #224955; }
# MAGIC .c2v-table tbody td:nth-child(2) { background: #EBF5FB; }
# MAGIC .c2v-check { color: #00A972; font-weight: 700; font-size: 1.2em; }
# MAGIC .c2v-cross { color: #D13438; font-weight: 700; font-size: 1.2em; }
# MAGIC </style>
# MAGIC <table class="c2v-table">
# MAGIC     <thead>
# MAGIC         <tr>
# MAGIC             <th>Feature</th>
# MAGIC             <th>SQL File</th>
# MAGIC             <th>SQL Query</th>
# MAGIC             <th>SQL Alert</th>
# MAGIC         </tr>
# MAGIC     </thead>
# MAGIC     <tbody>
# MAGIC         <tr>
# MAGIC             <td>Multi-statement</td>
# MAGIC             <td><span class="c2v-check">&#x2713;</span></td>
# MAGIC             <td><span class="c2v-check">&#x2713;</span></td>
# MAGIC             <td><span class="c2v-cross">&#x2717;</span></td>
# MAGIC         </tr>
# MAGIC         <tr>
# MAGIC             <td>Parameters</td>
# MAGIC             <td><span class="c2v-check">&#x2713;</span> <span style="font-size:12pt;color:#555;">:param</span></td>
# MAGIC             <td><span class="c2v-check">&#x2713;</span></td>
# MAGIC             <td><span class="c2v-cross">&#x2717;</span></td>
# MAGIC         </tr>
# MAGIC         <tr>
# MAGIC             <td>Source</td>
# MAGIC             <td><code style="font-size:12pt;">.sql</code> file</td>
# MAGIC             <td>Saved query</td>
# MAGIC             <td>Alert definition</td>
# MAGIC         </tr>
# MAGIC         <tr>
# MAGIC             <td>Git sourcing</td>
# MAGIC             <td><span class="c2v-check">&#x2713;</span><br/><span style="font-size:11pt;color:#888;font-style:italic;">branch, review, merge</span></td>
# MAGIC             <td><span class="c2v-cross">&#x2717;</span></td>
# MAGIC             <td><span class="c2v-cross">&#x2717;</span></td>
# MAGIC         </tr>
# MAGIC         <tr>
# MAGIC             <td>Built-in threshold alerting</td>
# MAGIC             <td><span class="c2v-cross">&#x2717;</span></td>
# MAGIC             <td><span class="c2v-cross">&#x2717;</span></td>
# MAGIC             <td><span class="c2v-check">&#x2713;</span> <span style="font-size:12pt;color:#555;">alert_state output</span></td>
# MAGIC         </tr>
# MAGIC         <tr>
# MAGIC             <td>Status</td>
# MAGIC             <td><span style="font-weight:600;color:#00A972;">GA</span></td>
# MAGIC             <td>GA</td>
# MAGIC             <td><span style="color:#B07800;">Beta</span></td>
# MAGIC         </tr>
# MAGIC         <tr>
# MAGIC             <td style="font-weight:700;">Best for</td>
# MAGIC             <td><span style="font-weight:700;color:#00A972;">&#x2605; Analytics Engineers</span></td>
# MAGIC             <td><span style="font-size:12pt;">Ad-hoc monitoring</span></td>
# MAGIC             <td><span style="font-size:12pt;">Threshold alerting</span></td>
# MAGIC         </tr>
# MAGIC     </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">Multi-Statement Execution</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Both SQL File and SQL Query tasks support <strong>multiple statements</strong> separated by semicolons, executed sequentially top-to-bottom. If any statement fails, execution stops &mdash; subsequent statements do not run.</li>
# MAGIC           <li>A single <code>.sql</code> file can contain <code>REFRESH STREAMING TABLE</code>, <code>REFRESH MATERIALIZED VIEW</code>, <code>INSERT INTO</code>, validation queries, and a final <code>SELECT</code> for output &mdash; all executed sequentially as one logical pipeline step</li>
# MAGIC           <li>SQL Alert tasks evaluate a single alert condition and cannot be composed into multi-step workflows</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Parameters &mdash; Reusability Across Tasks</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>SQL File tasks support key-value parameters (<a href="https://docs.databricks.com/aws/en/jobs/parameter-use">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/parameter-use">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/parameter-use">GCP</a>) using <code>:param_name</code> syntax inside the <code>.sql</code> file. SQL Query tasks also support parameters, but SQL Alert tasks do not.</li>
# MAGIC           <li>Parameters enable a powerful pattern: write one <code>.sql</code> file and reuse it across multiple tasks with different parameter values. For example, the same refresh file can process different regions, date ranges, or table names by changing only the task configuration.</li>
# MAGIC           <li><strong>Important distinction:</strong> <code>:param_name</code> is used <em>inside</em> the SQL file. <code>{{dynamic.value.references}}</code> are used in <em>task configuration fields</em> (parameter values, ForEach inputs, If/else operands). These two systems connect: you can set a task parameter value to <code>{{tasks.upstream.output.first_row.region}}</code>, then reference <code>:region</code> in the SQL file.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Source &mdash; Where the SQL Lives</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SQL File</strong> references a <code>.sql</code> file stored in the Workspace filesystem or pulled from a remote Git repository at runtime. The file is identified by path.</li>
# MAGIC           <li><strong>SQL Query</strong> references a saved query in the Databricks SQL Editor, identified by a <code>query_id</code> (UUID). Anyone with edit access can modify the saved query at any time &mdash; changes take effect on the next run.</li>
# MAGIC           <li><strong>SQL Alert</strong> references an alert definition created in the SQL Editor. Each alert owns its own query and threshold condition.</li>
# MAGIC           <li><strong>Identity gotcha:</strong> SQL File tasks always respect the job's <strong>Run-as</strong> user. SQL Query tasks may execute as the <em>query owner</em> if the saved query is set to "Run as owner" &mdash; regardless of the job's Run-as setting. This has governance implications.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Git Sourcing &mdash; Version Control for Pipeline Code</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>SQL File is the <strong>only SQL task type that supports Git sourcing</strong> &mdash; pulling <code>.sql</code> files directly from GitHub, GitLab, Bitbucket, or Azure DevOps at runtime</li>
# MAGIC           <li>This enables proper CI/CD patterns: branch, review, merge your pipeline SQL in Git, and the Lakeflow Job automatically picks up the latest version</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/ahold-delhaize">Ahold Delhaize</a> adopted a Git-triggered pattern where configuration files checked into Git drive GitHub Actions + Terraform to create Lakeflow Jobs workflows &mdash; fully self-service pipeline management. &#x25C6;</li>
# MAGIC           <li>Files can also be stored as Workspace files (including Databricks Git folders) if direct Git integration is not needed</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Built-in Threshold Alerting &mdash; What Makes SQL Alert Unique</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>SQL Alert tasks evaluate a <strong>declarative condition</strong> (column + aggregation + operator + threshold) and produce an <code>alert_state</code> output: <code>TRIGGERED</code>, <code>OK</code>, or <code>UNKNOWN</code></li>
# MAGIC           <li>Downstream tasks can reference <code>{{tasks.&lt;name&gt;.output.alert_state}}</code> to branch the DAG with If/else &mdash; this dynamic value reference is exclusive to SQL Alert tasks</li>
# MAGIC           <li>SQL Alert tasks also support <strong>subscriber notifications</strong> with custom HTML templates &mdash; a built-in notification mechanism that SQL File and SQL Query tasks do not have</li>
# MAGIC           <li><strong>Important distinction:</strong> A SQL Alert task reports <code>Succeeded</code> even when the alert is <code>TRIGGERED</code>. Success/failure reflects whether the query ran without errors, not whether the threshold was exceeded. The <code>alert_state</code> value is separate from the task run status.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">When to Use the Other Task Types</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SQL Query:</strong> Use for monitoring or validation steps where you want to run a saved query from the SQL Editor &mdash; for example, a data quality check that already exists as a saved query. Also useful when analysts maintain the query in their familiar SQL Editor environment.</li>
# MAGIC           <li><strong>SQL Alert:</strong> Use for threshold-based monitoring within a pipeline &mdash; for example, alerting when a row count drops below a minimum. Provides the <code>alert_state</code> output for downstream conditional branching. Currently in <em>Beta</em> and does not support parameters.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Migration Context</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>If you are coming from other tools, SQL File tasks are the natural migration target:
# MAGIC             <ul>
# MAGIC               <li><strong>SQL Server Agent:</strong> Each job step &rarr; SQL File task; precedence constraints &rarr; task dependencies</li>
# MAGIC               <li><strong>SSIS:</strong> Each <code>.dtsx</code> package step &rarr; SQL File task</li>
# MAGIC               <li><strong>Apache Airflow:</strong> Each DAG task &rarr; Lakeflow Job task &mdash; with native SQL integration instead of Python operators</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>&#x25C6; A major financial services firm wanted SQL File tasks to call other SQL files in sequence &mdash; like stored procedure nesting. This is not supported today: each <code>.sql</code> file is self-contained. The recommended pattern is to define each file as a separate task in the DAG and wire the dependencies. &#x25C6;</li>
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
# MAGIC ### C2. Anatomy of a SQL File Task
# MAGIC
# MAGIC A **SQL File task** executes a <code>.sql</code> file containing one or more SQL statements &mdash; sequentially, top-to-bottom &mdash; through a **serverless or pro SQL warehouse**. It is the primary building block for SQL-native pipeline orchestration.
# MAGIC
# MAGIC <!-- ── Visual: pure-CSS side-by-side code explorer (no JS) ── -->
# MAGIC <div class="c2v-wrap">
# MAGIC <style>
# MAGIC .c2v-wrap { max-width:100%; margin:0 auto; font-family:system-ui,-apple-system,sans-serif; }
# MAGIC .c2v-wrap input[type="radio"] { display:none; }
# MAGIC /* ── Tab buttons ── */
# MAGIC .c2v-btns { display:flex; gap:10px; margin-bottom:14px; }
# MAGIC .c2v-btn { flex:1; padding:10px 18px; border-radius:8px; font-size:14pt; font-weight:600; cursor:pointer; border:2px solid transparent; transition:all 0.2s; user-select:none; text-align:center; }
# MAGIC .c2v-btn:hover { transform:translateY(-1px); box-shadow:0 2px 8px rgba(0,0,0,0.1); }
# MAGIC #c2v-t0:checked ~ .c2v-btns .c2v-btn-0 { border-color:#4299E0; box-shadow:0 2px 10px rgba(0,0,0,0.12); }
# MAGIC #c2v-t1:checked ~ .c2v-btns .c2v-btn-1 { border-color:#02A36F; box-shadow:0 2px 10px rgba(0,0,0,0.12); }
# MAGIC #c2v-t2:checked ~ .c2v-btns .c2v-btn-2 { border-color:#E6A100; box-shadow:0 2px 10px rgba(0,0,0,0.12); }
# MAGIC /* ── Side-by-side layout ── */
# MAGIC .c2v-split { display:flex; gap:0; border-radius:10px; overflow:hidden; box-shadow:0 2px 10px rgba(0,0,0,0.08); border:1px solid #e0e3ea; }
# MAGIC /* ── Code panel (left) ── */
# MAGIC .c2v-code { flex:1 1 50%; background:#1B3139; padding:18px 16px; font-family:'Cascadia Code','Fira Code','Consolas',monospace; font-size:12pt; line-height:1.9; color:#E8E3DC; overflow-x:auto; min-width:0; }
# MAGIC .c2v-code .c2v-line { display:block; padding:2px 8px; border-radius:4px; transition:background 0.2s; white-space:pre; opacity:0.35; }
# MAGIC .c2v-kw { color:#cba6f7; } .c2v-obj { color:#89b4fa; } .c2v-str { color:#a6e3a1; } .c2v-cmt { color:#6c7086; font-style:italic; }
# MAGIC .c2v-ln { color:#585b70; margin-right:14px; user-select:none; display:inline-block; width:20px; text-align:right; }
# MAGIC /* ── Highlight rules per tab ── */
# MAGIC #c2v-t0:checked ~ .c2v-split .c2v-code .c2v-g0 { opacity:1; background:rgba(66,153,224,0.2); }
# MAGIC #c2v-t1:checked ~ .c2v-split .c2v-code .c2v-g1 { opacity:1; background:rgba(2,163,111,0.2); }
# MAGIC #c2v-t2:checked ~ .c2v-split .c2v-code .c2v-g2 { opacity:1; background:rgba(248,168,5,0.25); }
# MAGIC /* ── Explanation panel (right) ── */
# MAGIC .c2v-explain { flex:1 1 50%; background:#f9f9fb; padding:18px 18px; font-size:14pt; line-height:1.6; color:#444; min-width:0; overflow-wrap:break-word; }
# MAGIC .c2v-explain strong { color:#0b2026; }
# MAGIC .c2v-explain code { background:#e8eaf0; padding:2px 6px; border-radius:4px; font-size:13pt; font-family:monospace; }
# MAGIC .c2v-explain-title { font-size:16pt; font-weight:700; margin-bottom:10px; padding-bottom:8px; border-bottom:2px solid #e0e3ea; }
# MAGIC .c2v-explain ul { padding-left:18px; margin:8px 0 0 0; }
# MAGIC .c2v-explain li { margin-bottom:6px; font-size:14pt; }
# MAGIC /* ── Show/hide explanation panels ── */
# MAGIC .c2v-panel { display:none; }
# MAGIC #c2v-t0:checked ~ .c2v-split .c2v-explain .c2v-p0 { display:block; }
# MAGIC #c2v-t1:checked ~ .c2v-split .c2v-explain .c2v-p1 { display:block; }
# MAGIC #c2v-t2:checked ~ .c2v-split .c2v-explain .c2v-p2 { display:block; }
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" name="c2v-tab" id="c2v-t0" checked />
# MAGIC <input type="radio" name="c2v-tab" id="c2v-t1" />
# MAGIC <input type="radio" name="c2v-tab" id="c2v-t2" />
# MAGIC
# MAGIC <div class="c2v-btns">
# MAGIC   <label class="c2v-btn c2v-btn-0" for="c2v-t0" style="background:#EBF3FB;color:#4299E0;">&#x2460; Multi-Statement</label>
# MAGIC   <label class="c2v-btn c2v-btn-1" for="c2v-t1" style="background:#E6F9F0;color:#02A36F;">&#x2461; Parameters</label>
# MAGIC   <label class="c2v-btn c2v-btn-2" for="c2v-t2" style="background:#FFF8E1;color:#E6A100;">&#x2462; Output Capture</label>
# MAGIC </div>
# MAGIC
# MAGIC <div class="c2v-split">
# MAGIC   <div class="c2v-code">
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 1</span><span class="c2v-cmt">-- refresh_orders.sql</span></span>
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 2</span></span>
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 3</span><span class="c2v-kw">REFRESH STREAMING TABLE</span></span>
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 4</span>  <span class="c2v-obj">bronze.raw_orders</span>;</span>
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 5</span></span>
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 6</span><span class="c2v-kw">REFRESH MATERIALIZED VIEW</span></span>
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 7</span>  <span class="c2v-obj">silver.orders_enriched</span>;</span>
# MAGIC <span class="c2v-line c2v-g0"><span class="c2v-ln"> 8</span></span>
# MAGIC <span class="c2v-line c2v-g1"><span class="c2v-ln"> 9</span><span class="c2v-cmt">-- Log this refresh with a parameter</span></span>
# MAGIC <span class="c2v-line c2v-g1"><span class="c2v-ln">10</span><span class="c2v-kw">INSERT INTO</span> <span class="c2v-obj">audit.refresh_log</span></span>
# MAGIC <span class="c2v-line c2v-g1"><span class="c2v-ln">11</span><span class="c2v-kw">VALUES</span> (<span class="c2v-str">:pipeline_name</span>,</span>
# MAGIC <span class="c2v-line c2v-g1"><span class="c2v-ln">12</span>        <span class="c2v-obj">current_timestamp</span>());</span>
# MAGIC <span class="c2v-line c2v-g1"><span class="c2v-ln">13</span></span>
# MAGIC <span class="c2v-line c2v-g2"><span class="c2v-ln">14</span><span class="c2v-cmt">-- Output for downstream tasks</span></span>
# MAGIC <span class="c2v-line c2v-g2"><span class="c2v-ln">15</span><span class="c2v-kw">SELECT</span> <span class="c2v-str">'complete'</span> <span class="c2v-kw">AS</span> status,</span>
# MAGIC <span class="c2v-line c2v-g2"><span class="c2v-ln">16</span>       <span class="c2v-obj">count</span>(*) <span class="c2v-kw">AS</span> row_count</span>
# MAGIC <span class="c2v-line c2v-g2"><span class="c2v-ln">17</span><span class="c2v-kw">FROM</span> <span class="c2v-obj">silver.orders_enriched</span>;</span>
# MAGIC   </div>
# MAGIC   <div class="c2v-explain">
# MAGIC     <div class="c2v-panel c2v-p0">
# MAGIC       <div class="c2v-explain-title" style="color:#4299E0;">&#x2460; Multi-Statement Execution</div>
# MAGIC       <ul><li>All statements run <strong>sequentially</strong>, top-to-bottom, separated by semicolons</li><li>If line 4 fails, lines 6&ndash;16 <strong>never execute</strong></li><li>No implicit transaction &mdash; earlier statements that succeeded are <strong>not rolled back</strong></li><li>This file refreshes Bronze, then Silver, then produces a status output &mdash; three steps in one file</li></ul>
# MAGIC     </div>
# MAGIC     <div class="c2v-panel c2v-p1">
# MAGIC       <div class="c2v-explain-title" style="color:#02A36F;">&#x2461; Parameters</div>
# MAGIC       <ul><li><code>:pipeline_name</code> on line 11 is a named parameter (<a href="https://docs.databricks.com/aws/en/jobs/parameter-use" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/parameter-use" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/parameter-use" target="_blank">GCP</a>) &mdash; replaced at runtime with the value set on the task configuration</li><li>Use <code>:param_name</code> (colon prefix) anywhere in the SQL file to reference task parameters<ul style="margin:4px 0 0 0;"><li><strong>Note:</strong> The <code>:param_name</code> syntax works in DML statements (<code>INSERT</code>, <code>MERGE</code>, <code>SELECT</code>) for value binding. For <code>REFRESH</code> statements, use <code>IDENTIFIER()</code> with job parameters (e.g., <code>REFRESH MATERIALIZED VIEW IDENTIFIER({{catalog}} || '.schema.table')</code>) to parameterize the table reference &mdash; this is the recommended pattern shown in Demo 4.2. See the IDENTIFIER clause documentation (<a href="https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names-identifier-clause" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-names-identifier-clause" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-names-identifier-clause" target="_blank">GCP</a>) for details</li></ul></li><li>The same <code>.sql</code> file can be reused across multiple tasks with different parameter values</li><li>Parameters are covered in detail in Demo 4.2</li></ul>
# MAGIC     </div>
# MAGIC     <div class="c2v-panel c2v-p2">
# MAGIC       <div class="c2v-explain-title" style="color:#E6A100;">&#x2462; Output Capture</div>
# MAGIC       <ul><li>The <strong>last SELECT</strong> (lines 15&ndash;17) produces output available to downstream tasks</li><li>Here it returns a status flag and row count &mdash; useful for validation or conditional branching downstream</li><li>Output is capped at <strong>1,000 rows</strong> or <strong>48 KB</strong> and kept for <strong>7 days</strong> &mdash; designed for control data, not large datasets (docs: <a href="https://docs.databricks.com/aws/en/jobs/dynamic-value-references#supported-value-references" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/dynamic-value-references#supported-value-references" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/dynamic-value-references#supported-value-references" target="_blank">GCP</a>)</li><li>Downstream tasks access this via <code>{{tasks.&lt;task_name&gt;.output.rows}}</code> and other dynamic value references (<a href="https://docs.databricks.com/aws/en/jobs/dynamic-value-references#supported-value-references" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/dynamic-value-references#supported-value-references" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/dynamic-value-references#supported-value-references" target="_blank">GCP</a>)</li></ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">How It Works</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A SQL File task takes a path to a <code>.sql</code> file and a SQL warehouse, then executes every statement in the file sequentially. Think of it like handing a script to a DBA (the SQL warehouse) who runs each statement in order and reports back the final result.</li>
# MAGIC           <li><strong>Multi-statement execution:</strong> A single file can contain DDL (<code>CREATE TABLE</code>), DML (<code>INSERT INTO</code>, <code>MERGE</code>), refresh commands (<code>REFRESH MATERIALIZED VIEW</code>), and queries (<code>SELECT</code>) &mdash; all separated by semicolons, all run in sequence</li>
# MAGIC           <li><strong>Failure behavior:</strong> If any statement fails, execution stops immediately. Subsequent statements do not execute. There is no implicit transaction wrapping &mdash; earlier statements that succeeded are <strong>not</strong> rolled back.</li>
# MAGIC           <li><strong>Output capture:</strong> Only the <strong>last SELECT statement</strong> in the file produces output that is captured and made available to downstream tasks &mdash; up to <strong>1,000 rows</strong> and <strong>48 KB</strong>. Design your files so the final statement produces the output you want downstream.</li>
# MAGIC           <li><strong>Output retention:</strong> Captured output is retained for <strong>7 days</strong>. If a job is paused and resumed after 7 days, output from earlier tasks is no longer available.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">File Sourcing and Version Control</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>SQL files can be sourced from two locations:
# MAGIC             <ul>
# MAGIC               <li><strong>Workspace files:</strong> Stored directly in the Databricks workspace, including Databricks Git folders for version control</li>
# MAGIC               <li><strong>Remote Git repositories:</strong> Pulled directly from GitHub, GitLab, Bitbucket, or Azure DevOps at runtime. Use relative paths without leading <code>/</code> or <code>./</code> (e.g., <code>etl/bronze/ingest.sql</code>)</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Git sourcing is the recommended production pattern &mdash; it gives you version control, code review, branching, and a clear audit trail of what SQL was running at any point in time</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/ahold-delhaize">Ahold Delhaize</a> adopted a Git-triggered pattern where configuration files checked into Git trigger GitHub Actions + Terraform to create Lakeflow Jobs workflows. Internal users define their pipelines via configuration files &mdash; the orchestration is fully self-service. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Parameterization</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>SQL File tasks support <strong>key-value parameters</strong> configured on the task. Inside the SQL file, reference parameters using the <strong>colon-prefixed <code>:param_name</code> syntax</strong>:
# MAGIC             <ul>
# MAGIC               <li><code>WHERE region = :region_param AND order_date &gt;= :start_date</code></li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Critical distinction:</strong> The <code>{{ }}</code> dynamic value reference syntax works in <strong>task configuration fields only</strong> (parameter values, ForEach inputs, If/else operands). Inside the <code>.sql</code> file itself, use <code>:param_name</code>. This is the <strong>#1 source of student confusion</strong> &mdash; mixing up where each syntax applies.</li>
# MAGIC           <li>You can combine both: set a task parameter value to <code>{{tasks.upstream_task.output.first_row.region}}</code> in the configuration, then reference <code>:region</code> in the SQL file. The dynamic reference resolves at runtime, populating the parameter, which the SQL engine reads.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Example: A Typical Silver-Layer SQL File</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Here is what a real pipeline step looks like as a SQL file:
# MAGIC             <pre style="background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 13pt; line-height: 1.7; overflow-x: auto; margin: 8px 0 0 0;"><code>-- refresh_orders.sql
# MAGIC -- Refreshes the orders pipeline: Bronze ingestion + Silver MV
# MAGIC
# MAGIC REFRESH STREAMING TABLE bronze.raw_orders;
# MAGIC REFRESH MATERIALIZED VIEW silver.orders_enriched;
# MAGIC
# MAGIC -- Final SELECT: output for downstream tasks
# MAGIC SELECT 'refresh_complete' AS status,
# MAGIC        current_timestamp() AS completed_at;</code></pre>
# MAGIC           </li>
# MAGIC           <li>The first three statements do the work. The final <code>SELECT</code> produces output that downstream tasks can consume &mdash; confirming success and recording the completion time.</li>
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
# MAGIC ## D. Directed Acyclic Graphs &mdash; How Tasks Connect
# MAGIC
# MAGIC With SQL File tasks as our building blocks and Lakeflow Jobs as our platform, we need a way to wire tasks together into a pipeline. This is where **Directed Acyclic Graphs (DAGs)** come in &mdash; the execution model that determines which tasks run, in what order, and what happens when something fails.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. DAGs and Common Patterns
# MAGIC
# MAGIC A **Directed Acyclic Graph (DAG)** is a graph where nodes represent tasks and edges represent dependencies &mdash; with two constraints: edges have a <u>direction</u> (Task A must finish before Task B starts) and the graph has <u>no cycles</u> (no circular dependencies allowed). In Lakeflow Jobs, the DAG is both the execution model and the visual interface. Below is an example showing the **diamond pattern** &mdash; the most common DAG shape for medallion-architecture SQL ETL pipelines.
# MAGIC
# MAGIC
# MAGIC <!-- ── DAG pattern tabs ── -->
# MAGIC <div class="d1t-wrap">
# MAGIC <style>
# MAGIC .d1t-wrap { max-width:100%; margin:16px auto 0; font-family:system-ui,-apple-system,sans-serif; }
# MAGIC .d1t-wrap input[type="radio"] { display:none; }
# MAGIC .d1t-nav { display:flex; gap:0; border-bottom:2px solid #e0e0e0; }
# MAGIC .d1t-nav label { flex:1; text-align:center; padding:10px 16px; font-size:14pt; font-weight:600; color:#666; cursor:pointer; border-bottom:3px solid transparent; margin-bottom:-2px; transition:all 0.15s; user-select:none; }
# MAGIC .d1t-nav label:hover { color:#1B3139; }
# MAGIC .d1t-pnl { display:none; padding:20px; background:#f8f9fa; border:1px solid #e0e0e0; border-top:none; border-radius:0 0 8px 8px; }
# MAGIC #d1t1:checked ~ .d1t-nav label[for="d1t1"] { color:#1B3139; border-bottom-color:#1B3139; }
# MAGIC #d1t1:checked ~ .d1t-pnls .d1t-pnl:nth-child(1) { display:block; }
# MAGIC #d1t2:checked ~ .d1t-nav label[for="d1t2"] { color:#1B3139; border-bottom-color:#1B3139; }
# MAGIC #d1t2:checked ~ .d1t-pnls .d1t-pnl:nth-child(2) { display:block; }
# MAGIC #d1t3:checked ~ .d1t-nav label[for="d1t3"] { color:#1B3139; border-bottom-color:#1B3139; }
# MAGIC #d1t3:checked ~ .d1t-pnls .d1t-pnl:nth-child(3) { display:block; }
# MAGIC #d1t4:checked ~ .d1t-nav label[for="d1t4"] { color:#1B3139; border-bottom-color:#1B3139; }
# MAGIC #d1t4:checked ~ .d1t-pnls .d1t-pnl:nth-child(4) { display:block; }
# MAGIC #d1t5:checked ~ .d1t-nav label[for="d1t5"] { color:#1B3139; border-bottom-color:#1B3139; }
# MAGIC #d1t5:checked ~ .d1t-pnls .d1t-pnl:nth-child(5) { display:block; }
# MAGIC .d1t-desc { font-size:14pt; color:#444; margin-bottom:14px; }
# MAGIC .d1t-dag { display:flex; align-items:center; justify-content:center; gap:8px; padding:10px 0; }
# MAGIC .d1t-tk { background:#fff; border:1px solid #ddd; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.06); overflow:hidden; display:inline-flex; flex-direction:column; white-space:nowrap; }
# MAGIC .d1t-tk-bar { height:4px; background:#00A972; }
# MAGIC .d1t-tk-bd { padding:8px 14px; }
# MAGIC .d1t-tk-nm { font-size:12pt; font-weight:700; color:#1B3139; }
# MAGIC .d1t-tk-st { font-size:10pt; color:#888; margin-top:2px; }
# MAGIC .d1t-arr { font-size:16pt; color:#bbb; }
# MAGIC .d1t-col { display:flex; flex-direction:column; gap:8px; }
# MAGIC </style>
# MAGIC <input type="radio" name="d1t-grp" id="d1t1" checked>
# MAGIC <input type="radio" name="d1t-grp" id="d1t2">
# MAGIC <input type="radio" name="d1t-grp" id="d1t3">
# MAGIC <input type="radio" name="d1t-grp" id="d1t4">
# MAGIC <input type="radio" name="d1t-grp" id="d1t5">
# MAGIC <div class="d1t-nav">
# MAGIC   <label for="d1t1">Linear</label>
# MAGIC   <label for="d1t2">Fan-Out</label>
# MAGIC   <label for="d1t3">Fan-In</label>
# MAGIC   <label for="d1t4">Diamond</label>
# MAGIC   <label for="d1t5">Error Branch</label>
# MAGIC </div>
# MAGIC <div class="d1t-pnls">
# MAGIC   <!-- Tab 1: Linear -->
# MAGIC   <div class="d1t-pnl">
# MAGIC     <div class="d1t-desc"><strong>Each task depends on the previous.</strong> Simple, no parallelism. Best for single-source pipelines with one transformation path.</div>
# MAGIC     <div class="d1t-dag">
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">refresh_bronze</div><div class="d1t-tk-st">Bronze &middot; Streaming Table</div></div></div>
# MAGIC       <div class="d1t-arr">&rarr;</div>
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">refresh_silver</div><div class="d1t-tk-st">Silver &middot; Materialized View</div></div></div>
# MAGIC       <div class="d1t-arr">&rarr;</div>
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">refresh_gold</div><div class="d1t-tk-st">Gold &middot; Materialized View</div></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Tab 2: Fan-Out -->
# MAGIC   <div class="d1t-pnl">
# MAGIC     <div class="d1t-desc"><strong>One task triggers multiple parallel downstream tasks.</strong> Independent branches run simultaneously. Best when a single source feeds multiple transformations.</div>
# MAGIC     <div class="d1t-dag">
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">ingest_orders</div><div class="d1t-tk-st">Bronze &middot; Streaming Table</div></div></div>
# MAGIC       <div style="font-size:30pt;color:#ccc;font-weight:200;line-height:1;">{</div>
# MAGIC       <div class="d1t-col">
# MAGIC         <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">orders_enriched</div><div class="d1t-tk-st">Silver &middot; Materialized View</div></div></div>
# MAGIC         <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">orders_by_region</div><div class="d1t-tk-st">Silver &middot; Materialized View</div></div></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Tab 3: Fan-In -->
# MAGIC   <div class="d1t-pnl">
# MAGIC     <div class="d1t-desc"><strong>Multiple parallel tasks converge into one downstream task.</strong> The downstream task waits for all upstream tasks to complete. Best when a Gold layer joins multiple Silver sources.</div>
# MAGIC     <div class="d1t-dag">
# MAGIC       <div class="d1t-col">
# MAGIC         <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">transform_orders</div><div class="d1t-tk-st">Silver &middot; Materialized View</div></div></div>
# MAGIC         <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">refresh_dimensions</div><div class="d1t-tk-st">Silver &middot; AUTO CDC Dimension</div></div></div>
# MAGIC       </div>
# MAGIC       <div style="font-size:36pt;color:#ccc;font-weight:200;line-height:1;">}</div>
# MAGIC       <div class="d1t-arr">&rarr;</div>
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">gold_sales</div><div class="d1t-tk-st">Gold &middot; Materialized View</div></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Tab 4: Diamond -->
# MAGIC   <div class="d1t-pnl">
# MAGIC     <div class="d1t-desc"><strong>One task fans out to parallel paths that reconverge.</strong> Combines fan-out and fan-in. Best when one source drives multiple transformations that must recombine.</div>
# MAGIC     <div class="d1t-dag">
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">ingest_orders</div><div class="d1t-tk-st">Bronze &middot; Streaming Table</div></div></div>
# MAGIC       <div style="font-size:30pt;color:#ccc;font-weight:200;line-height:1;">{</div>
# MAGIC       <div class="d1t-col">
# MAGIC         <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">orders_by_region</div><div class="d1t-tk-st">Silver &middot; Materialized View</div></div></div>
# MAGIC         <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">orders_by_product</div><div class="d1t-tk-st">Silver &middot; Materialized View</div></div></div>
# MAGIC       </div>
# MAGIC       <div style="font-size:30pt;color:#ccc;font-weight:200;line-height:1;">}</div>
# MAGIC       <div class="d1t-arr">&rarr;</div>
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">gold_dashboard</div><div class="d1t-tk-st">Gold &middot; Materialized View</div></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Tab 5: Error Branch -->
# MAGIC   <div class="d1t-pnl">
# MAGIC     <div class="d1t-desc"><strong>A validation task routes to success or failure paths.</strong> Uses Run-If conditions to branch the DAG based on task outcome. Best for pipelines that need data quality gates.</div>
# MAGIC     <div class="d1t-dag">
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">refresh_silver</div><div class="d1t-tk-st">Silver &middot; SQL File</div></div></div>
# MAGIC       <div class="d1t-arr">&rarr;</div>
# MAGIC       <div class="d1t-tk"><div class="d1t-tk-bar"></div><div class="d1t-tk-bd"><div class="d1t-tk-nm">validate_data</div><div class="d1t-tk-st">Validation &middot; SQL File</div></div></div>
# MAGIC       <div style="font-size:30pt;color:#ccc;font-weight:200;line-height:1;">{</div>
# MAGIC       <div class="d1t-col">
# MAGIC         <div class="d1t-tk" style="border-top:4px solid #00A972;"><div class="d1t-tk-bd"><div class="d1t-tk-nm" style="color:#00A972;">load_to_gold</div><div class="d1t-tk-st"><img src="../Includes/images/icons/checkmark-success.png" height="14" style="vertical-align:middle;"> All succeeded</div></div></div>
# MAGIC         <div class="d1t-tk" style="border-top:4px solid #D93025;"><div class="d1t-tk-bd"><div class="d1t-tk-nm" style="color:#D93025;">quarantine + alert</div><div class="d1t-tk-st"><img src="../Includes/images/icons/x-circle-icon.png" height="14" style="vertical-align:middle;"> At least one failed</div></div></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">What Makes It a DAG</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Directed:</strong> Each arrow flows one way &mdash; from upstream to downstream. <code>refresh_bronze &rarr; refresh_silver</code> means Bronze must finish before Silver starts.</li>
# MAGIC           <li><strong>Acyclic:</strong> No circular dependencies. If Task A depends on B which depends on C which depends back on A, the system would never know where to start. Databricks rejects circular configurations.</li>
# MAGIC           <li><strong>Graph:</strong> Tasks are nodes, dependencies are edges. The Lakeflow Jobs UI renders this as an interactive visual where you can click any node to see its details, logs, and status.</li>
# MAGIC           <li><strong>Automatic parallelism:</strong> You declare dependencies; the engine figures out what can run in parallel. Root tasks (no upstream dependencies) launch immediately. As each completes, downstream tasks with all dependencies satisfied launch next.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Linear (Tab 1)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The simplest pattern: <code>refresh_bronze &rarr; refresh_silver &rarr; refresh_gold</code> &mdash; each task depends on the previous</li>
# MAGIC           <li><strong>When to use:</strong> Single data source with one transformation path. Good starting point for small pipelines.</li>
# MAGIC           <li><strong>Limitation:</strong> No parallelism. If Bronze takes 3 minutes and Silver takes 4 minutes, total runtime is 7 minutes even if they could overlap. The other patterns solve this.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Fan-Out (Tab 2)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>One upstream task triggers multiple downstream tasks that run <strong>simultaneously</strong>: <code>ingest_orders</code> completes, then <code>orders_enriched</code> and <code>orders_by_region</code> both start at the same time</li>
# MAGIC           <li><strong>When to use:</strong> A single source needs multiple transformations &mdash; for example, the same Bronze orders feed both a Silver enrichment MV and a Silver regional aggregation MV</li>
# MAGIC           <li><strong>Benefit:</strong> Both Silver tasks run in parallel, cutting total runtime compared to running them sequentially</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Fan-In (Tab 3)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Multiple upstream tasks converge into one downstream task: <code>gold_sales</code> waits for <strong>both</strong> <code>transform_orders</code> and <code>refresh_dimensions</code> before it starts</li>
# MAGIC           <li><strong>When to use:</strong> A Gold-layer aggregation that joins data from multiple Silver sources &mdash; the most common pattern in medallion-architecture ETL</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/corning">Corning</a> structures their DAGs to follow the medallion architecture &mdash; Bronze, Silver, Gold &mdash; with fan-in at each layer ensuring correct execution order. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Diamond (Tab 4)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Combines fan-out and fan-in: one task fans out to parallel paths that reconverge downstream. The shape looks like a diamond: A &rarr; B + C &rarr; D</li>
# MAGIC           <li><strong>When to use:</strong> One source drives multiple independent transformations (fan-out) that must recombine in a single downstream aggregation (fan-in)</li>
# MAGIC           <li>In practice, most production DAGs are combinations of these patterns. The pipeline you built in Modules 2&ndash;3 naturally forms a diamond when orchestrated.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Error Branch (Tab 5)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A validation task creates two paths: the <strong>happy path</strong> (validation passes &rarr; load to Gold) and the <strong>error path</strong> (validation fails &rarr; quarantine data + send alert)</li>
# MAGIC           <li>This uses <strong>Run-If conditions</strong> (covered in D2): <code>load_to_gold</code> runs with "All succeeded" and <code>quarantine + alert</code> runs with "At least one failed"</li>
# MAGIC           <li>Error branches are what turn a DAG from a simple execution plan into a <strong>resilient pipeline</strong> that handles failures automatically instead of failing silently</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Designing Your DAG</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>For each task, ask: <strong>"What must finish before this can start?"</strong> Those are your upstream dependencies. If there is no dependency, do not add one &mdash; let the orchestrator parallelize.</li>
# MAGIC           <li>A common anti-pattern is creating a fully linear chain when tasks are actually independent &mdash; this wastes time by forcing sequential execution where parallelism is possible</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/the-rank-group-plc/lakeflow-jobs">The Rank Group</a> built a pattern-based DAG framework processing nearly <strong>20 million transactions per day</strong> from APIs, databases, and message queues &mdash; showing how DAG design scales from simple to complex. &#x25C6;</li>
# MAGIC           <li><strong>Job success</strong> is determined by <strong>leaf tasks</strong> &mdash; the tasks at the bottom of the DAG with no downstream dependents. If all leaf tasks succeed, the job succeeds, even if intermediate tasks failed and were handled by error branches.</li>
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
# MAGIC ### D2. Task Dependencies and Run-If Conditions
# MAGIC
# MAGIC Dependencies control more than execution order &mdash; they also determine **what happens when things go wrong**. Lakeflow Jobs provides six **"Run if"** conditions that govern whether a downstream task executes based on upstream outcomes, enabling sophisticated error handling without any code.
# MAGIC
# MAGIC <div class="d3v-wrap">
# MAGIC <style>
# MAGIC .d3v-wrap { max-width:100%; margin:0 auto; font-family:system-ui,-apple-system,sans-serif; display:flex; flex-direction:column; gap:6px; perspective:800px; }
# MAGIC .d3v-row { border-radius:10px; cursor:pointer; user-select:none; transition:all 0.3s ease; background:#f8f9fb; border:1px solid #e0e3ea; box-shadow:0 2px 4px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.02); transform:translateZ(0); }
# MAGIC .d3v-row:hover { box-shadow:0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04); transform:translateY(-1px) translateZ(4px); }
# MAGIC .d3v-row.d3v-open { box-shadow:0 8px 24px rgba(0,0,0,0.12), 0 4px 8px rgba(0,0,0,0.06); transform:translateY(-2px) translateZ(8px) scale(1.01); z-index:2; position:relative; }
# MAGIC .d3v-hdr { display:flex; align-items:center; gap:14px; padding:16px 22px; }
# MAGIC .d3v-bar { width:6px; height:40px; border-radius:3px; flex-shrink:0; box-shadow:inset 0 -2px 4px rgba(0,0,0,0.15); }
# MAGIC .d3v-name { font-size:15pt; font-weight:700; color:#1B3139; flex:1; }
# MAGIC .d3v-name .d3v-badge { font-weight:400; color:#888; font-size:12pt; margin-left:6px; }
# MAGIC .d3v-arrow { font-size:11pt; color:#999; transition:transform 0.3s cubic-bezier(0.4,0,0.2,1); }
# MAGIC .d3v-row.d3v-open .d3v-arrow { transform:rotate(90deg); color:#1B3139; }
# MAGIC .d3v-body { max-height:0; overflow:hidden; transition:max-height 0.35s cubic-bezier(0.4,0,0.2,1), padding 0.35s ease; padding:0 22px 0 42px; }
# MAGIC .d3v-row.d3v-open .d3v-body { max-height:300px; padding:0 22px 18px 42px; }
# MAGIC .d3v-beh { font-size:14pt; color:#444; line-height:1.6; margin-bottom:8px; }
# MAGIC .d3v-beh strong { color:#1B3139; }
# MAGIC .d3v-uses { font-size:14pt; color:#1B3139; font-weight:600; margin-bottom:4px; }
# MAGIC .d3v-body ul { margin:4px 0 0; padding-left:18px; font-size:14pt; color:#444; line-height:1.7; }
# MAGIC .d3v-body li { margin-bottom:4px; }
# MAGIC </style>
# MAGIC <div class="d3v-row d3v-open" onclick="d3vToggle(this)">
# MAGIC   <div class="d3v-hdr"><div class="d3v-bar" style="background:#00A972;"></div><div class="d3v-name">All succeeded<span class="d3v-badge">(default)</span></div><div class="d3v-arrow">&#x25B6;</div></div>
# MAGIC   <div class="d3v-body"><div class="d3v-beh">Runs only if <strong>every</strong> upstream task succeeded.</div><div class="d3v-uses">Use cases:</div><ul><li>Load to Gold after all Silver MVs refresh</li><li>Run aggregation after all sources are current</li><li>Standard happy-path processing</li></ul></div>
# MAGIC </div>
# MAGIC <div class="d3v-row" onclick="d3vToggle(this)">
# MAGIC   <div class="d3v-hdr"><div class="d3v-bar" style="background:#2574B5;"></div><div class="d3v-name">At least one succeeded</div><div class="d3v-arrow">&#x25B6;</div></div>
# MAGIC   <div class="d3v-body"><div class="d3v-beh">Runs if <strong>at least one</strong> dependency succeeded, even if others failed.</div><div class="d3v-uses">Use cases:</div><ul><li>Partial refresh when some sources are optional</li><li>Best-effort reporting dashboards</li><li>Multi-source pipelines with non-critical feeds</li></ul></div>
# MAGIC </div>
# MAGIC <div class="d3v-row" onclick="d3vToggle(this)">
# MAGIC   <div class="d3v-hdr"><div class="d3v-bar" style="background:#2574B5;"></div><div class="d3v-name">None failed</div><div class="d3v-arrow">&#x25B6;</div></div>
# MAGIC   <div class="d3v-body"><div class="d3v-beh">Runs if no dependency <strong>actively failed</strong> &mdash; skipped and excluded tasks pass through.</div><div class="d3v-uses">Use cases:</div><ul><li>Proceed when upstream tasks are skipped</li><li>Conditional paths where not all branches run</li><li>If/else patterns where one branch is always excluded</li></ul></div>
# MAGIC </div>
# MAGIC <div class="d3v-row" onclick="d3vToggle(this)">
# MAGIC   <div class="d3v-hdr"><div class="d3v-bar" style="background:#E5A100;"></div><div class="d3v-name">All done</div><div class="d3v-arrow">&#x25B6;</div></div>
# MAGIC   <div class="d3v-body"><div class="d3v-beh">Runs <strong>regardless</strong> of upstream status &mdash; succeeded, failed, or skipped.</div><div class="d3v-uses">Use cases:</div><ul><li>Drop temp tables</li><li>Send pipeline completion notifications</li><li>Log run metadata to an audit table</li><li>Release locks or clean up resources</li></ul></div>
# MAGIC </div>
# MAGIC <div class="d3v-row" onclick="d3vToggle(this)">
# MAGIC   <div class="d3v-hdr"><div class="d3v-bar" style="background:#E0382B;"></div><div class="d3v-name">At least one failed</div><div class="d3v-arrow">&#x25B6;</div></div>
# MAGIC   <div class="d3v-body"><div class="d3v-beh">Runs when <strong>at least one</strong> dependency failed.</div><div class="d3v-uses">Use cases:</div><ul><li>Quarantine bad data</li><li>Send Slack/email alerts</li><li>Write failure details to an error log table</li><li>Trigger a retry workflow</li></ul></div>
# MAGIC </div>
# MAGIC <div class="d3v-row" onclick="d3vToggle(this)">
# MAGIC   <div class="d3v-hdr"><div class="d3v-bar" style="background:#98182A;"></div><div class="d3v-name">All failed</div><div class="d3v-arrow">&#x25B6;</div></div>
# MAGIC   <div class="d3v-body"><div class="d3v-beh">Runs only if <strong>every</strong> dependency failed.</div><div class="d3v-uses">Use cases:</div><ul><li>Trigger incident response</li><li>Disable downstream consumers</li><li>Execute full pipeline rollback logic</li><li>Page the on-call team</li></ul></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC window.d3vToggle = function(row) {
# MAGIC   var wasOpen = row.classList.contains('d3v-open');
# MAGIC   document.querySelectorAll('.d3v-row').forEach(function(r) { r.classList.remove('d3v-open'); });
# MAGIC   if (!wasOpen) row.classList.add('d3v-open');
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">How to Configure Dependencies</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Click the accordion items above to explore each of the six Run-If conditions. Here we cover how to set them up and the behavioral details that matter in practice.</li>
# MAGIC           <li>Dependencies are configured using the <strong>"Depends on"</strong> field in the task settings. When you create a new task with an existing task selected in the UI, the dependency is added automatically.</li>
# MAGIC           <li>Each task also has a <strong>"Run if dependencies"</strong> dropdown. The default is <code>All succeeded</code>. You only need to change it when building error branches, cleanup tasks, or resilient pipelines.</li>
# MAGIC           <li>The orchestrator evaluates conditions after each task completes, then determines which downstream tasks to launch, skip, or exclude next.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Gotchas That Catch People Off Guard</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Skipped tasks are treated as successful</strong> &mdash; this is the #1 surprise. If a task is skipped (because its own upstream failed), it counts as "succeeded" for downstream Run-If evaluation. This matters when using "All succeeded" vs "None failed."</li>
# MAGIC           <li><strong>If all dependencies are excluded</strong>, the downstream task is also excluded &mdash; regardless of its Run-If condition. There is nothing to evaluate.</li>
# MAGIC           <li><strong>"None failed" vs "All succeeded":</strong> The difference only shows up when tasks are skipped. "All succeeded" requires every dependency to have actively run and succeeded. "None failed" passes skipped tasks through &mdash; it only blocks on active failures.</li>
# MAGIC           <li><strong>String comparison in If/else:</strong> The <code>==</code> operator does <strong>string comparison</strong> &mdash; <code>12.0 == 12</code> is <strong>false</strong>. Use <code>&gt;=</code> for numeric comparisons. This is covered in detail in Demo 4.2.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Error Routing &mdash; Connecting to D1's Error Branch Pattern</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The Error Branch tab in D1 above shows the DAG shape. Here is how the Run-If conditions make it work:
# MAGIC             <ul>
# MAGIC               <li><code>validate_data</code> &rarr; runs a SQL validation query</li>
# MAGIC               <li><code>load_to_gold</code> depends on <code>validate_data</code> with <strong>All succeeded</strong> &mdash; the happy path</li>
# MAGIC               <li><code>quarantine + alert</code> depends on <code>validate_data</code> with <strong>At least one failed</strong> &mdash; the error path</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>If validation passes, <code>load_to_gold</code> runs and <code>quarantine + alert</code> is excluded. If validation fails, the reverse happens. No procedural code needed &mdash; the DAG handles both paths automatically.</li>
# MAGIC           <li>You can add a cleanup task (like <code>send_notification</code>) with <strong>All done</strong> that runs after either path completes &mdash; ensuring the team always knows the pipeline ran.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Job Success and Leaf Tasks</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Job success is determined by leaf tasks</strong> &mdash; the tasks at the bottom of the DAG with no downstream dependents.</li>
# MAGIC           <li>If all leaf tasks succeed, the job succeeds &mdash; even if intermediate tasks failed and were handled by error branches. This is by design: a pipeline that detects a failure, quarantines the bad data, and sends an alert has <em>worked correctly</em>.</li>
# MAGIC           <li>This means your error-handling tasks should be leaf tasks (or lead to leaf tasks) so that a properly handled failure does not mark the entire job as failed.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">What Comes Next</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Run-If conditions route based on <strong>task success or failure</strong>. Section E of this lecture introduces <strong>If/else branching</strong> which routes based on <strong>SQL output values</strong> &mdash; a more granular form of conditional logic.</li>
# MAGIC           <li>Combined with <strong>dynamic value references</strong> and <strong>ForEach iteration</strong>, these give you a full data-driven orchestration toolkit &mdash; all covered in Section E of this lecture and Demo 4.2.</li>
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
# MAGIC ## E. The Complete Capability Set
# MAGIC
# MAGIC Sections C and D covered the two foundational concepts &mdash; SQL File tasks and DAGs. Lakeflow Jobs provides three additional SQL-native capabilities that complete the orchestration toolkit: **dynamic value references**, **ForEach iteration**, and **If/else branching** &mdash; plus rich **observability** for debugging and monitoring.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. SQL-Native Control Flow &mdash; Dynamic Values, ForEach, and If/Else
# MAGIC
# MAGIC These three capabilities transform Lakeflow Jobs from a task scheduler into a **data-driven orchestration engine**. A SQL task produces output rows &rarr; dynamic value references pass those rows between tasks &rarr; ForEach iterates over them &rarr; If/else branches on the values. The entire flow is SQL-native &mdash; no Python required.
# MAGIC
# MAGIC <div class="e1v-wrap">
# MAGIC <style>
# MAGIC .e1v-wrap { max-width:100%; margin:0 auto; font-family:system-ui,-apple-system,sans-serif; }
# MAGIC .e1v-cards { display:grid; grid-template-columns:1fr 1fr 1fr; gap:10px; margin-bottom:14px; }
# MAGIC .e1v-card { padding:14px 16px; border-radius:8px; cursor:pointer; user-select:none; border:2px solid #e0e3ea; background:#f8f9fb; transition:all 0.2s; text-align:center; position:relative; border-top:5px solid #ccc; }
# MAGIC .e1v-card:hover { transform:translateY(-2px); box-shadow:0 4px 12px rgba(0,0,0,0.08); }
# MAGIC .e1v-card.e1v-on { background:#fff; border-color:currentColor; box-shadow:0 4px 16px rgba(0,0,0,0.12); transform:translateY(-2px); }
# MAGIC .e1v-card.e1v-on::after { content:''; position:absolute; bottom:-8px; left:50%; transform:translateX(-50%); width:0; height:0; border-left:8px solid transparent; border-right:8px solid transparent; border-top:8px solid currentColor; opacity:0.5; }
# MAGIC .e1v-card-nm { font-size:15pt; font-weight:700; color:#1B3139; }
# MAGIC .e1v-card-sub { font-size:11pt; color:#888; margin-top:2px; }
# MAGIC .e1v-panel { background:#fff; border:1px solid #e0e3ea; border-radius:10px; padding:22px 26px; min-height:120px; box-shadow:0 2px 8px rgba(0,0,0,0.04); transition:border-color 0.2s; }
# MAGIC .e1v-panel-hdr { font-size:18pt; font-weight:700; margin-bottom:12px; padding-bottom:10px; border-bottom:2px solid #f0f0f0; }
# MAGIC .e1v-panel-body { font-size:14pt; line-height:1.7; color:#444; }
# MAGIC .e1v-panel-body strong { color:#1B3139; }
# MAGIC .e1v-panel-body ul { padding-left:20px; margin:6px 0; }
# MAGIC .e1v-panel-body li { margin-bottom:5px; }
# MAGIC .e1v-panel-body code { background:#f0f2f7; padding:2px 6px; border-radius:4px; font-size:13pt; font-family:monospace; }
# MAGIC .e1v-ex { background:#1B3139; color:#e0e0e0; border-radius:8px; padding:12px 16px; font-family:monospace; font-size:13pt; line-height:1.7; margin:10px 0; overflow-x:auto; }
# MAGIC .e1v-ex .e1v-kw { color:#cba6f7; } .e1v-ex .e1v-str { color:#a6e3a1; } .e1v-ex .e1v-obj { color:#89b4fa; }
# MAGIC .e1v-cta { border-left:3px solid; padding:10px 14px; border-radius:0 6px 6px 0; margin-top:10px; font-size:14pt; line-height:1.6; background:rgba(0,0,0,0.02); }
# MAGIC </style>
# MAGIC <div class="e1v-cards">
# MAGIC   <div class="e1v-card e1v-on" onclick="e1v(0)" style="border-top-color:#2574B5;color:#2574B5;"><div class="e1v-card-nm">Dynamic Value References</div><div class="e1v-card-sub">Pass data between tasks</div></div>
# MAGIC   <div class="e1v-card" onclick="e1v(1)" style="border-top-color:#02A36F;color:#02A36F;"><div class="e1v-card-nm">ForEach</div><div class="e1v-card-sub">Iterate over SQL results</div></div>
# MAGIC   <div class="e1v-card" onclick="e1v(2)" style="border-top-color:#F8A805;color:#F8A805;"><div class="e1v-card-nm">If/Else</div><div class="e1v-card-sub">Branch on SQL values</div></div>
# MAGIC </div>
# MAGIC <div class="e1v-panel" id="e1v-panel"></div>
# MAGIC </div>
# MAGIC <script>
# MAGIC var e1vD = [
# MAGIC   { t:'Dynamic Value References', c:'#2574B5',
# MAGIC     h:'<strong>What it does:</strong> Passes data between tasks using <code>{{ }}</code> syntax. Replaced with actual values at runtime &mdash; no Python needed.<br/><br/><strong>Key references for SQL pipelines:</strong><ul><li><code>{{tasks.&lt;name&gt;.output.rows}}</code> &mdash; all output rows (up to 1,000 rows, 48 KB)</li><li><code>{{tasks.&lt;name&gt;.output.first_row.&lt;column&gt;}}</code> &mdash; a specific column from the first row</li><li><code>{{tasks.&lt;name&gt;.result_state}}</code> &mdash; task outcome (success, failed, excluded, etc.)</li><li><code>{{job.parameters.&lt;key&gt;}}</code> &mdash; job-level parameter values</li></ul><strong>Example:</strong><div class="e1v-ex"><span class="e1v-kw">Task parameter value:</span><br/>count_param = <span class="e1v-obj">{{tasks.refresh_orders.output.first_row.row_count}}</span><br/><br/><span class="e1v-kw">Inside the SQL file:</span><br/><span class="e1v-str">WHERE</span> row_count &gt;= <span class="e1v-obj">:count_param</span></div><strong>Use cases:</strong><ul><li>Pass row counts from validation tasks to If/else conditions</li><li>Feed table name lists from config queries to ForEach loops</li><li>Chain task outputs: Bronze count &rarr; Silver validation &rarr; Gold conditional load</li></ul><div class="e1v-cta" style="border-color:#2574B5;"><strong>Gotcha:</strong> <code>{{ }}</code> works in <strong>task config fields only</strong> (parameter values, ForEach inputs, If/else operands). Inside SQL files, use <code>:param_name</code>. Syntax errors and non-existent references are treated as literal strings &mdash; typos produce no error.</div>' },
# MAGIC   { t:'ForEach', c:'#02A36F',
# MAGIC     h:'<strong>What it does:</strong> Runs a nested task in a loop, passing different parameters to each iteration. The input is typically the output rows of an upstream SQL task.<br/><br/><strong>How it works:</strong><ul><li>An upstream SQL task returns rows (e.g., a list of Gold table names from a config table)</li><li>ForEach receives those rows via <code>{{tasks.get_tables.output.rows}}</code></li><li>For each row, the nested task runs with <code>{{input.table_name}}</code> as the parameter</li><li><strong>Concurrency</strong> controls how many iterations run in parallel (default: 1)</li></ul><strong>Example:</strong><div class="e1v-ex"><span class="e1v-kw">Step 1 &mdash; SQL task returns:</span><br/>[{<span class="e1v-str">"table_name"</span>: <span class="e1v-str">"orders"</span>}, {<span class="e1v-str">"table_name"</span>: <span class="e1v-str">"customers"</span>}, {<span class="e1v-str">"table_name"</span>: <span class="e1v-str">"products"</span>}]<br/><br/><span class="e1v-kw">Step 2 &mdash; ForEach nested task runs 3x:</span><br/><span class="e1v-obj">REFRESH MATERIALIZED VIEW</span> gold.<span class="e1v-str">{{input.table_name}}</span>;</div><strong>Use cases:</strong><ul><li>Refresh multiple Gold MVs from a config-driven table list</li><li>Process data per region, per date, or per customer segment</li><li>Run the same validation across multiple schemas or catalogs</li></ul><div class="e1v-cta" style="border-color:#02A36F;"><strong>Limitations:</strong> ForEach cannot nest another ForEach (no multi-level loops). Input is capped at 1,000 rows / 48 KB. Downstream tasks depend on the parent ForEach, not individual iterations.</div>' },
# MAGIC   { t:'If/Else', c:'#F8A805',
# MAGIC     h:'<strong>What it does:</strong> Evaluates a boolean condition on SQL output and routes the pipeline to a true or false branch. This is data-driven branching &mdash; different from Run-If conditions (D2) which branch on task success/failure.<br/><br/><strong>How it works:</strong><ul><li>The condition compares two operands using operators like <code>==</code>, <code>!=</code>, <code>&gt;</code>, <code>&gt;=</code>, <code>&lt;=</code></li><li>Operands can be dynamic value references to SQL output, task values, or job parameters</li><li>Downstream tasks specify which branch: <code>task_name (true)</code> or <code>task_name (false)</code></li><li>Multiple tasks can run on either branch (serial or parallel)</li></ul><strong>Example:</strong><div class="e1v-ex"><span class="e1v-kw">Condition:</span><br/><span class="e1v-obj">{{tasks.validate.output.first_row.error_count}}</span> <span class="e1v-str">&gt;</span> <span class="e1v-str">0</span><br/><br/><span class="e1v-kw">True branch:</span> quarantine_bad_rows &rarr; send_alert<br/><span class="e1v-kw">False branch:</span> load_to_gold &rarr; update_dashboard</div><strong>Use cases:</strong><ul><li>Data quality gates: only load to Gold if validation passes</li><li>Conditional notifications: alert only when row counts drop below threshold</li><li>A/B processing: route to different transformation logic based on data characteristics</li></ul><div class="e1v-cta" style="border-color:#F8A805;"><strong>Gotcha:</strong> <code>==</code> and <code>!=</code> do <strong>string comparison</strong>. <code>12.0 == 12</code> is <strong>false</strong> (different strings). Use <code>&gt;=</code> for reliable numeric comparisons. Only numeric, string, and boolean values are supported.</div>' }
# MAGIC ];
# MAGIC window.e1v = function(i){
# MAGIC   document.querySelectorAll('.e1v-card').forEach(function(c,j){c.classList.toggle('e1v-on',j===i);});
# MAGIC   var d=e1vD[i],p=document.getElementById('e1v-panel');
# MAGIC   p.style.borderColor=d.c;
# MAGIC   p.innerHTML='<div class="e1v-panel-hdr" style="color:'+d.c+'">'+d.t+'</div><div class="e1v-panel-body">'+d.h+'</div>';
# MAGIC }
# MAGIC e1v(0);
# MAGIC </script>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">Dynamic Value References &mdash; The Full Reference Landscape</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The visual above shows the three most important references for SQL pipelines. The full set of available references spans five categories:
# MAGIC             <ul>
# MAGIC               <li><strong>Task output:</strong> <code>{{tasks.&lt;name&gt;.output.rows}}</code>, <code>{{tasks.&lt;name&gt;.output.first_row.&lt;col&gt;}}</code> &mdash; SQL query results from upstream tasks</li>
# MAGIC               <li><strong>Task state:</strong> <code>{{tasks.&lt;name&gt;.result_state}}</code> &mdash; returns <code>success</code>, <code>failed</code>, <code>excluded</code>, <code>canceled</code>, <code>timedout</code>, or other states</li>
# MAGIC               <li><strong>Job metadata:</strong> <code>{{job.id}}</code>, <code>{{job.run_id}}</code>, <code>{{job.name}}</code>, <code>{{job.trigger.type}}</code></li>
# MAGIC               <li><strong>Time references:</strong> <code>{{job.trigger.time.&lt;arg&gt;}}</code>, <code>{{job.start_time.&lt;arg&gt;}}</code></li>
# MAGIC               <li><strong>Workspace info:</strong> <code>{{workspace.id}}</code>, <code>{{workspace.url}}</code></li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>The two-syntax connection:</strong> <code>{{ }}</code> references resolve in <strong>task configuration fields</strong> (parameter values, ForEach inputs, If/else operands). Inside <code>.sql</code> files, use <code>:param_name</code>. They connect: set a task parameter value to <code>{{tasks.upstream.output.first_row.count}}</code>, then reference <code>:count</code> in your SQL. The dynamic reference resolves first at the orchestration layer, then the SQL engine reads the parameter value.</li>
# MAGIC           <li><strong>Error behavior:</strong> Syntax errors and non-existent references are treated as literal strings &mdash; no error is thrown. A typo like <code>{{tasks.misspeled.output.rows}}</code> passes as-is. Always verify resolved values in the task run details after your first test run.</li>
# MAGIC           <li><strong>Retention:</strong> SQL task output is retained for 7 days (<a href="https://docs.databricks.com/aws/en/jobs/dynamic-value-references#supported-value-references">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/dynamic-value-references#supported-value-references">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/dynamic-value-references#supported-value-references">GCP</a>). If a job is paused longer than that, downstream references to earlier task output will resolve to empty values.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">ForEach &mdash; Input Sources, Nesting, and Concurrency</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The visual above shows the most common pattern: SQL output rows feeding a ForEach loop. Here are the mechanics in detail.</li>
# MAGIC           <li><strong>Three input sources</strong> (with size limits):
# MAGIC             <ul>
# MAGIC               <li><strong>Task output references:</strong> <code>{{tasks.&lt;name&gt;.output.rows}}</code> &mdash; up to 48 KB. This is the standard SQL pipeline pattern.</li>
# MAGIC               <li><strong>Direct JSON array:</strong> Typed directly in the configuration &mdash; up to 5,000 characters. Good for small static lists.</li>
# MAGIC               <li><strong>Job parameters:</strong> <code>{{job.parameters.&lt;key&gt;}}</code> &mdash; up to 10,000 characters. Useful when the iteration list comes from the job trigger.</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Inside the nested task:</strong> Use <code>{{input}}</code> for the entire iteration value (the full JSON object for that row), or <code>{{input.&lt;key&gt;}}</code> for a specific field. Example: if the input row is <code>{"table_name": "orders", "priority": "high"}</code>, then <code>{{input.table_name}}</code> resolves to <code>"orders"</code>.</li>
# MAGIC           <li><strong>No nesting:</strong> You cannot put a ForEach inside another ForEach. For multi-level iteration, restructure with sequential ForEach tasks or use a notebook task for the inner loop.</li>
# MAGIC           <li><strong>Concurrency:</strong> Controls how many iterations run in parallel. Default is 1 (sequential). Increase for production but be mindful of SQL warehouse concurrency limits. Downstream tasks depend on the <strong>parent ForEach</strong>, not individual iterations &mdash; the parent is marked as failed if any iteration fails.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">If/Else &mdash; Operators, Gotchas, and the Run-If Distinction</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The visual above shows a data-driven branching example. Here are the nuances that matter in practice.</li>
# MAGIC           <li><strong>If/else vs Run-If &mdash; the critical distinction:</strong>
# MAGIC             <ul>
# MAGIC               <li><strong>Run-If</strong> (Section D2) routes based on <em>task success or failure</em> &mdash; "did the upstream task succeed?"</li>
# MAGIC               <li><strong>If/else</strong> routes based on <em>SQL output values</em> &mdash; "what did the upstream task return?"</li>
# MAGIC               <li>Use Run-If for error branches (task failed &rarr; quarantine). Use If/else for data-driven decisions (error_count &gt; 0 &rarr; quarantine).</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Operator behavior:</strong> <code>==</code> and <code>!=</code> perform <strong>string comparison</strong> (<code>12.0 == 12</code> is false). <code>&gt;</code>, <code>&gt;=</code>, and <code>&lt;=</code> perform <strong>numeric comparison</strong> (<code>12.0 &gt;= 12</code> is true). Only numeric, string, and boolean values are supported as operands.</li>
# MAGIC           <li><strong>Compound logic:</strong> Each If/else evaluates one condition. For AND/OR, chain multiple If/else tasks or compute the boolean in a SQL task and pass the result as output.</li>
# MAGIC           <li><strong>Boolean handling:</strong> Boolean task values are serialized to strings <code>"true"</code> or <code>"false"</code>. Use <code>==</code> for boolean comparisons, <code>&gt;=</code> for numeric.</li>
# MAGIC           <li><strong>Non-matching branch:</strong> Tasks on the branch that does not execute are skipped &mdash; they do not count as failures for downstream Run-If evaluation.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">How They Chain Together &mdash; The Lab 4.3 Pattern</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>In Lab 4.3, you will chain all three capabilities into a single pipeline:
# MAGIC             <ol>
# MAGIC               <li><strong>SQL task</strong> queries <code>gold_table_config</code> &rarr; returns a list of Gold table names to refresh</li>
# MAGIC               <li><strong>Dynamic value references</strong> pass those rows via <code>{{tasks.get_tables.output.rows}}</code> to a ForEach task</li>
# MAGIC               <li><strong>ForEach</strong> iterates over each row, running a nested SQL File task: <code>REFRESH MATERIALIZED VIEW gold.{{input.table_name}}</code></li>
# MAGIC               <li><strong>Validation SQL task</strong> checks for errors across all refreshed tables</li>
# MAGIC               <li><strong>If/else</strong> branches on <code>{{tasks.validate.output.first_row.error_count}} &gt; 0</code>: true &rarr; quarantine + alert; false &rarr; publish</li>
# MAGIC             </ol>
# MAGIC           </li>
# MAGIC           <li>This entire flow &mdash; config-driven table discovery, parameterized iteration, validation, conditional routing &mdash; executes without a single line of Python. The SQL you wrote in Modules 2&ndash;3 becomes the payload; Lakeflow Jobs handles the orchestration.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">What Comes Next</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>This section introduced <em>what</em> these capabilities are, <em>why</em> they matter, and <em>how they connect</em>. Demo 4.2 covers the implementation details: step-by-step UI configuration, exact parameter wiring, the <code>{{ }}</code> auto-complete menu in the Jobs UI, and common debugging patterns when references don't resolve as expected.</li>
# MAGIC           <li>The concepts are the hard part &mdash; once you understand how dynamic references, ForEach, and If/else connect, the syntax is straightforward.</li>
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
# MAGIC ### E2. Observability and Debugging
# MAGIC
# MAGIC Production pipelines need more than execution &mdash; they need **visibility**. Lakeflow Jobs provides multiple monitoring interfaces and queryable system tables so you can understand what ran, how long it took, what failed, and why.
# MAGIC
# MAGIC <div class="e2v-wrap">
# MAGIC <style>
# MAGIC .e2v-wrap { max-width:100%; margin:0 auto; font-family:system-ui,-apple-system,sans-serif; }
# MAGIC .e2v-layout { display:flex; gap:0; border:1px solid #e0e3ea; border-radius:10px; overflow:hidden; box-shadow:0 2px 10px rgba(0,0,0,0.06); min-height:300px; }
# MAGIC /* ── Sidebar ── */
# MAGIC .e2v-sidebar { flex:0 0 200px; background:#f8f9fb; border-right:1px solid #e0e3ea; display:flex; flex-direction:column; }
# MAGIC .e2v-nav { padding:12px 14px; cursor:pointer; user-select:none; font-size:14pt; font-weight:600; color:#666; border-left:4px solid transparent; transition:all 0.2s; display:flex; align-items:center; gap:10px; }
# MAGIC .e2v-nav:hover { background:#eef0f4; color:#1B3139; }
# MAGIC .e2v-nav.e2v-on { background:#fff; color:#1B3139; border-left-color:currentColor; font-weight:700; }
# MAGIC .e2v-nav-dot { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
# MAGIC /* ── Content panel ── */
# MAGIC .e2v-content { flex:1; padding:22px 26px; background:#fff; overflow-y:auto; }
# MAGIC .e2v-content-hdr { font-size:18pt; font-weight:700; margin-bottom:10px; padding-bottom:8px; border-bottom:2px solid #f0f0f0; }
# MAGIC .e2v-content-body { font-size:14pt; line-height:1.7; color:#444; }
# MAGIC .e2v-content-body strong { color:#1B3139; }
# MAGIC .e2v-content-body ul { padding-left:20px; margin:6px 0; }
# MAGIC .e2v-content-body li { margin-bottom:5px; }
# MAGIC .e2v-content-body code { background:#f0f2f7; padding:2px 6px; border-radius:4px; font-size:13pt; font-family:monospace; }
# MAGIC .e2v-cta { border-left:3px solid; padding:10px 14px; border-radius:0 6px 6px 0; margin-top:10px; font-size:14pt; line-height:1.6; background:rgba(0,0,0,0.02); }
# MAGIC .e2v-box { width: 340px; min-height: 220px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); display: flex; flex-direction: column; justify-content: flex-start; gap: 12px; padding: 24px; text-align: center; position: relative; box-sizing: border-box; overflow: hidden; }
# MAGIC .e2v-box::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 8px; }
# MAGIC .e2v-box.e2v-one::before { background: #4299E0; }
# MAGIC .e2v-box.e2v-two::before { background: #00A972; }
# MAGIC .e2v-box.e2v-three::before { background: #FF5F46; }
# MAGIC .e2v-icon { font-size: 36pt; margin-top: 12px; }
# MAGIC .e2v-title { font-size: 18pt; font-weight: 700; color: #0b2026; }
# MAGIC .e2v-text { font-size: 14pt; color: #0b2026; line-height: 1.55; text-align: left; }
# MAGIC </style>
# MAGIC <div class="e2v-layout">
# MAGIC   <div class="e2v-sidebar">
# MAGIC     <div class="e2v-nav e2v-on" onclick="e2v(0)" style="color:#2574B5;"><div class="e2v-nav-dot" style="background:#2574B5;"></div>Monitoring Views</div>
# MAGIC     <div class="e2v-nav" onclick="e2v(1)" style="color:#FF5F46;"><div class="e2v-nav-dot" style="background:#FF5F46;"></div>Repair Runs</div>
# MAGIC     <div class="e2v-nav" onclick="e2v(2)" style="color:#1B3139;"><div class="e2v-nav-dot" style="background:#1B3139;"></div>System Tables</div>
# MAGIC     <div class="e2v-nav" onclick="e2v(3)" style="color:#02A36F;"><div class="e2v-nav-dot" style="background:#02A36F;"></div>Notifications</div>
# MAGIC   </div>
# MAGIC   <div class="e2v-content" id="e2v-content"></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC var e2vD = [
# MAGIC   { t:'Monitoring Views', c:'#2574B5',
# MAGIC     h:'<strong>Three views for three different questions:</strong><br/><br/><strong>Matrix View &mdash; "Is my pipeline healthy over time?"</strong><ul><li>Grid of runs (columns) &times; tasks (rows) with color-coded cells: <span style="color:#00A972;font-weight:600;">green</span> (succeeded), <span style="color:#D93025;font-weight:600;">red</span> (failed), <span style="color:#E8A0B0;font-weight:600;">pink</span> (skipped), <span style="color:#DAA520;font-weight:600;">yellow</span> (retry), <span style="color:#999;font-weight:600;">gray</span> (pending/canceled)</li><li>Spot patterns across runs: "Task X fails every Tuesday" or "Silver refresh is getting slower"</li></ul><strong>Timeline View &mdash; "Where is the bottleneck?"</strong><ul><li>Horizontal bars showing task duration and overlap for a single run</li><li>Overlapping bars = parallelism working. Gaps = tasks waiting on dependencies. One long bar = your bottleneck.</li><li>Use this before adding compute &mdash; restructuring the DAG for more parallelism may be free.</li></ul><strong>Graph View &mdash; "What went wrong?"</strong><ul><li>The live DAG visualization &mdash; click any node to see status, source code, query history, logs, and errors</li><li>During a run, nodes update in real time: gray (waiting) &rarr; yellow (running) &rarr; green/red (complete)</li><li>This is the <strong>primary debugging interface</strong>: click the red node, read the error, fix the SQL.</li></ul><div class="e2v-cta" style="border-color:#2574B5;"><strong>Workflow:</strong> Start with Matrix (big picture) &rarr; drill into Timeline (single run performance) &rarr; use Graph (debug specific tasks).</div>' },
# MAGIC   { t:'Repair Runs', c:'#FF5F46',
# MAGIC     h:'<strong>What it does:</strong> When a run partially fails, Repair Run re-executes <strong>only the failed tasks and their downstream dependents</strong>. Successful tasks are skipped.<br/><br/><strong>How it works:</strong><ul><li>Navigate to the failed run &rarr; click <strong>"Repair Run"</strong></li><li>Select which failed tasks to re-run (or accept the default: all failed tasks)</li><li>Successful upstream tasks keep their original results &mdash; not re-executed</li><li>The repair is linked to the original run as a continuation, not a separate run</li></ul><strong>Why it matters:</strong><ul><li><strong>Time:</strong> If 8 of 10 tasks succeeded, re-run only 2 + dependents instead of all 10</li><li><strong>Cost:</strong> No wasted compute re-executing tasks that already produced correct results</li><li><strong>Audit trail:</strong> Full history of original run + all repair attempts in one place</li></ul><div class="e2v-cta" style="border-color:#FF5F46;"><strong>Example:</strong> <code>gold_sales</code> fails because a new column was added to the Silver MV but the Gold definition was not updated. Fix the Gold SQL, then Repair &mdash; Bronze and Silver are not re-executed.</div>' },
# MAGIC   { t:'System Tables', c:'#1B3139',
# MAGIC     h:'<strong>What it does:</strong> Every run and task run is logged to queryable system tables (<a href="https://docs.databricks.com/aws/en/admin/system-tables/jobs" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/jobs" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/admin/system-tables/jobs" target="_blank">GCP</a>) accessible with SQL.<br/><br/><strong>Key tables:</strong><ul><li><code>system.lakeflow.job_run_timeline</code> &mdash; job-level: run ID, status, start/end time, trigger type</li><li><code>system.lakeflow.job_task_run_timeline</code> &mdash; task-level: task key, status, duration, compute</li></ul><strong>What you can do:</strong><ul><li><strong>Cost analysis:</strong> Join with <code>system.billing.usage</code> to calculate per-pipeline and per-task costs</li><li><strong>Custom dashboards:</strong> Build AI/BI dashboards showing pipeline health, SLA compliance, and failure trends</li><li><strong>Automated alerts:</strong> Query on a schedule to detect anomalies (e.g., "alert if duration exceeds 2x average")</li><li><strong>Long-term retention:</strong> UI keeps 60 days. System tables retain data much longer.</li></ul><div class="e2v-cta" style="border-color:#1B3139;"><strong>Key insight:</strong> You monitor your SQL pipelines using the same SQL skills you use to build them &mdash; observability in SQL.</div>' },
# MAGIC   { t:'Notifications', c:'#02A36F',
# MAGIC     h:'<strong>What it does:</strong> Jobs can send <strong>email, Slack, or webhook notifications</strong> on start, success, failure, or duration threshold violations.<br/><br/><strong>The three-layer alerting strategy:</strong><ul><li><strong>In-pipeline (Sections D&ndash;E):</strong> Run-If conditions and If/else branching handle error routing automatically within the DAG</li><li><strong>Out-of-pipeline (Notifications):</strong> Email/Slack alerts notify teams immediately when runs fail or exceed time thresholds</li><li><strong>Historical (System Tables):</strong> Scheduled queries detect trends and anomalies across runs over time</li></ul><strong>Configuration:</strong><ul><li>Set up in the Job settings under "Notifications"</li><li>Choose trigger events: start, success, failure, duration exceeded</li><li>Multiple destinations supported per event (e.g., Slack for failures, email for duration alerts)</li></ul><div class="e2v-cta" style="border-color:#02A36F;">&#x25C6; <a href="https://www.databricks.com/customers/coxautomotive/workflows" style="color:#02A36F;">Cox Automotive</a> manages 300+ jobs with centralized monitoring and proactive failure notifications &mdash; ensuring issues are caught before downstream consumers are impacted. &#x25C6;</div>' }
# MAGIC ];
# MAGIC window.e2v = function(i){
# MAGIC   document.querySelectorAll('.e2v-nav').forEach(function(n,j){n.classList.toggle('e2v-on',j===i);});
# MAGIC   var d=e2vD[i],p=document.getElementById('e2v-content');
# MAGIC   p.innerHTML='<div class="e2v-content-hdr" style="color:'+d.c+'">'+d.t+'</div><div class="e2v-content-body">'+d.h+'</div>';
# MAGIC }
# MAGIC e2v(0);
# MAGIC </script>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #00695c;">Monitoring Views &mdash; Three Views, Three Questions</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Click "Monitoring Views" in the sidebar to see all three views. Each answers a different question about your pipeline:</li>
# MAGIC           <li><strong>Matrix View &mdash; "Is my pipeline healthy over time?"</strong> A grid of runs &times; tasks with color-coded cells. Look for patterns: a vertical red stripe = an entire run failed. A horizontal red stripe = one task keeps failing across runs. Gradual yellow creep = retries increasing (resource pressure or intermittent errors). The UI retains <strong>60 days</strong> of run history.</li>
# MAGIC           <li><strong>Timeline View &mdash; "Where is the bottleneck?"</strong> Horizontal bars showing task duration and overlap for a single run. If your pipeline takes 30 minutes but one task is 25 of those, you found your target. Idle gaps between bars = tasks waiting on dependencies &mdash; a DAG design opportunity. Check this <em>before</em> adding more compute.</li>
# MAGIC           <li><strong>Graph View &mdash; "What went wrong?"</strong> The live DAG visualization from Section B2 &mdash; but with real-time status updates. Click any node to see source code, query history, logs, and error messages. The debugging workflow: red node &rarr; click &rarr; read error &rarr; fix SQL &rarr; Repair Run.</li>
# MAGIC           <li><strong>Recommended workflow:</strong> Start with Matrix (big picture health) &rarr; drill into Timeline (single run performance) &rarr; use Graph (debug specific tasks). Each view narrows your focus from "is something wrong?" to "what exactly broke?"</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Repair Runs &mdash; Why Re-Run the Whole Pipeline?</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Click "Repair Runs" in the sidebar for the step-by-step workflow. This feature saves more time and money than almost any other Lakeflow Jobs capability.</li>
# MAGIC           <li><strong>The problem without repair:</strong> Your Gold aggregation fails at 3 AM. Without repair, you re-run the entire pipeline &mdash; Bronze ingestion (10 minutes), Silver transformation (15 minutes), Gold aggregation (5 minutes). That is 25 minutes of wasted compute re-executing tasks that already succeeded.</li>
# MAGIC           <li><strong>With repair:</strong> Click "Repair Run," select the failed Gold task, and only that task + its dependents re-execute. Bronze and Silver keep their original results. Total re-execution: 5 minutes instead of 30.</li>
# MAGIC           <li><strong>Repair vs new run:</strong> A repair is linked to the original run &mdash; the full history (original + all repair attempts) is visible as one logical execution. A new run starts fresh with no connection to the failure.</li>
# MAGIC           <li><strong>Course scenario:</strong> In Lab 4.3, if <code>gold_sales</code> fails because the MV definition does not match a schema change in <code>silver.orders_enriched</code>, you will fix the SQL and repair rather than re-running Bronze ingestion and Silver transformation.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">System Tables &mdash; Observability in SQL</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Click "System Tables" in the sidebar for the key table names and use cases. This is where observability becomes <strong>programmatic</strong> &mdash; you query your pipeline monitoring data with the same SQL skills you use to build the pipeline itself.</li>
# MAGIC           <li><strong>Key tables:</strong> <code>system.lakeflow.job_run_timeline</code> (job-level) and <code>system.lakeflow.job_task_run_timeline</code> (task-level). These contain run IDs, statuses, start/end times, durations, trigger types, and compute details.</li>
# MAGIC           <li><strong>Cost analysis:</strong> Join with <code>system.billing.usage</code> to calculate per-pipeline and per-task costs. Answer questions like "how much does the nightly sales pipeline cost per run?" or "which task consumes the most DBUs?"</li>
# MAGIC           <li><strong>Custom dashboards:</strong> Build AI/BI Dashboards showing pipeline health, SLA compliance, and failure trends &mdash; all powered by SQL queries against system tables.</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/coxautomotive/workflows">Cox Automotive</a> manages 300+ jobs with centralized monitoring and proactive failure notifications, ensuring issues are caught before downstream consumers are impacted. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Notifications &mdash; The Three-Layer Alerting Strategy</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Click "Notifications" in the sidebar for configuration details. Notifications are the <strong>out-of-pipeline</strong> alerting layer &mdash; they reach humans when the pipeline cannot self-heal.</li>
# MAGIC           <li><strong>The three layers work together:</strong>
# MAGIC             <ul>
# MAGIC               <li><strong>Layer 1 &mdash; In-pipeline (Sections D&ndash;E):</strong> Run-If conditions and If/else branching handle errors automatically within the DAG. No human needed for expected failure modes.</li>
# MAGIC               <li><strong>Layer 2 &mdash; Notifications:</strong> Email, Slack, or webhook alerts fire when runs fail or exceed duration thresholds. The team knows immediately.</li>
# MAGIC               <li><strong>Layer 3 &mdash; System tables:</strong> Scheduled queries detect subtle trends: "average duration increased 40% this week" or "failure rate doubled since the schema change." These catch issues that individual notifications miss.</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Together, these three layers create a production-grade alerting strategy: failures are handled automatically (Layer 1), teams are notified in real time (Layer 2), and long-term patterns are caught early (Layer 3).</li>
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
# MAGIC This lecture introduced Lakeflow Jobs as the orchestration platform for your SQL ETL pipelines. You now understand what Lakeflow Jobs is and how it fits into the Lakeflow product family, how Jobs, Tasks, and Runs work together, how SQL File tasks turn your <code>.sql</code> files into first-class pipeline steps, how DAGs model task dependencies to enable parallel execution and failure handling, and how the complete capability set &mdash; dynamic value references, ForEach, If/else, and observability &mdash; makes this a full orchestration engine for SQL-first practitioners.
# MAGIC
# MAGIC Key takeaways from this lecture include:
# MAGIC
# MAGIC - **Lakeflow Jobs is unified orchestration:** A fully managed platform running 100M+ jobs per week, supporting SQL, Python, Spark, ML, and AI workloads in a single job with up to 1,000 tasks, multiple trigger types, built-in retry and repair, and queryable system tables
# MAGIC - **SQL File tasks are the building block:** Execute multi-statement <code>.sql</code> files through a SQL warehouse, with Git-sourced version control, <code>:param_name</code> parameterization, and output capture (1,000 rows, 48 KB) for downstream tasks &mdash; no Python required
# MAGIC - **DAGs model task dependencies:** Directed Acyclic Graphs determine execution order, enable automatic parallelism (independent tasks run simultaneously), and control failure propagation through six "Run if" conditions from "All succeeded" to "All done"
# MAGIC - **The full capability set is SQL-native:** Dynamic value references pass SQL output between tasks, ForEach iterates over SQL result rows, If/else branches on SQL values, and system tables provide observability you can query with SQL &mdash; the same skills you use to build the pipeline also monitor and control it
# MAGIC
# MAGIC **Next:** Demo 4.2 dives into the implementation details &mdash; SQL File task configuration, dynamic value reference syntax, the diamond-pattern DAG, and Repair Run. Then Lab 4.3 puts it all together as you build a complete Lakeflow Job orchestrating the pipeline from Modules 2 and 3 with job parameters and conditional branching.
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Lakeflow Jobs Overview (<a href="https://docs.databricks.com/aws/en/jobs/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/">GCP</a>) &mdash; core concepts, all task types, scheduling, triggers, and platform limits</li>
# MAGIC         <li>SQL Task for Jobs (<a href="https://docs.databricks.com/aws/en/jobs/sql">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/sql">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/sql">GCP</a>) &mdash; SQL File task configuration, parameters, output capture, and compute requirements</li>
# MAGIC         <li>Configure Task Dependencies (<a href="https://docs.databricks.com/aws/en/jobs/run-if">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/run-if">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/run-if">GCP</a>) &mdash; DAG dependency configuration and all six Run-if condition behaviors</li>
# MAGIC         <li>Control Flow in Lakeflow Jobs (<a href="https://docs.databricks.com/aws/en/jobs/control-flow">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/control-flow">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/control-flow">GCP</a>) &mdash; overview of ForEach, If/else, dynamic value references, and conditional execution</li>
# MAGIC         <li>Monitoring and Observability (<a href="https://docs.databricks.com/aws/en/jobs/monitor">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/monitor">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/monitor">GCP</a>) &mdash; Matrix View, Timeline View, Graph View, run history, and system tables integration</li>
# MAGIC         <li>Repair Job Failures (<a href="https://docs.databricks.com/aws/en/jobs/repair-job-failures">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/repair-job-failures">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/repair-job-failures">GCP</a>) &mdash; how to re-run only failed tasks and their dependents without re-executing the entire pipeline</li>
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
