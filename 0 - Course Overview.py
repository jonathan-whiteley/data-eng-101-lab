# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Building ETL Pipelines with SQL
# MAGIC
# MAGIC ## Overview
# MAGIC This course teaches you how to build **production-ready ETL pipelines using pure SQL** on the Databricks Data Intelligence Platform. You will learn the declarative building blocks — **Streaming Tables**, **Materialized Views**, and **AUTO CDC** — that replace hundreds of lines of procedural SQL with concise, incremental, and fully managed pipeline definitions.
# MAGIC
# MAGIC The course follows a realistic retail dataset through the **medallion architecture** (Bronze → Silver → Gold), building a complete pipeline across four modules:
# MAGIC
# MAGIC - **Module 1 — SQL ETL on Databricks: The Big Picture** introduces the modern SQL ETL landscape, the SQL Editor and Genie Code AI assistant, and the course dataset's star schema.
# MAGIC
# MAGIC - **Module 2 — Streaming Tables and Materialized Views** covers incremental ingestion with Auto Loader, pre-computed transformations with Materialized Views, EXPLAIN-based incrementalizability checks, and refresh policies.
# MAGIC
# MAGIC - **Module 3 — Auto CDC** replaces complex MERGE INTO logic with declarative SCD Type 1 and Type 2 dimension management, including soft deletes and pipeline monitoring via `event_log()`.
# MAGIC
# MAGIC - **Module 4 — Orchestrating with Lakeflow Jobs** ties the pipeline together with SQL File tasks, DAG-based orchestration, branching workflows, and production monitoring.
# MAGIC
# MAGIC Through hands-on demos and labs, you will:
# MAGIC
# MAGIC - Navigate the **SQL Editor** and use **Genie Code** to generate, explain, and fix SQL
# MAGIC - Create **Streaming Tables** that incrementally ingest CSV files from Unity Catalog Volumes using Auto Loader
# MAGIC - Create **Materialized Views** with joins, type casting, null handling, and incremental refresh
# MAGIC - Use **EXPLAIN CREATE MATERIALIZED VIEW** to verify incrementalizability before deployment
# MAGIC - Configure **refresh policies** and **schedules** to automate pipeline refresh
# MAGIC - Build **SCD Type 1** (current state) and **SCD Type 2** (full history) dimensions using **AUTO CDC**
# MAGIC - Handle **soft deletes** with `APPLY AS DELETE WHEN` and monitor pipelines with `event_log()`
# MAGIC - Create a **Lakeflow Job** with SQL File tasks, parallel branches, and conditional error handling
# MAGIC
# MAGIC By the end of the course, you will have built a complete, incremental ETL pipeline — from raw file ingestion through enriched Silver views to business-ready Gold aggregations — orchestrated by a production Lakeflow Job, all in pure SQL.
# MAGIC
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC - Create **Streaming Tables** that incrementally ingest data from cloud storage using Auto Loader
# MAGIC - Create **Materialized Views** that pre-compute and incrementally refresh Silver-to-Gold transformations
# MAGIC - Use **EXPLAIN CREATE MATERIALIZED VIEW** to predict incrementalizability before deploying
# MAGIC - Build **SCD Type 1 and Type 2** dimensions using **AUTO CDC** with `FLOW AUTO CDC`, `KEYS`, `SEQUENCE BY`, and `APPLY AS DELETE`
# MAGIC - Monitor AUTO CDC pipelines with `event_log()` queries
# MAGIC - Orchestrate a multi-step ETL pipeline using **Lakeflow Jobs** with SQL File tasks and DAG-based workflows
# MAGIC
# MAGIC
# MAGIC ##### Course update and version can be found in the `Version Info` file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC 1. **Navigating the Databricks workspace** — Familiarity with the sidebar, Catalog Explorer, SQL Editor, and opening resources in new tabs.
# MAGIC
# MAGIC 2. **Unity Catalog basics** — Understanding of catalogs, schemas, tables, and volumes in Unity Catalog.
# MAGIC
# MAGIC 3. **Intermediate SQL** — Ability to write and read SQL statements including `SELECT`, `JOIN`, `GROUP BY`, `CAST`, `COALESCE`, `CREATE TABLE`, and `INSERT`. Familiarity with common table expressions (CTEs) is helpful but not required.
# MAGIC
# MAGIC 4. **Data warehousing concepts** — Familiarity with fact tables, dimension tables, star schemas, and the medallion architecture (Bronze / Silver / Gold).
# MAGIC
# MAGIC 5. **Basic understanding of ETL** — General awareness of Extract, Transform, Load workflows and why incremental processing matters for production pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Workspace Setup Information

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Databricks Provided Vocareum Workspace (Recommended)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #f0f7ff;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this course in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC - Your catalog name matches your Vocareum username and looks like: <strong>labuser_firstname_lastname</strong>
# MAGIC
# MAGIC - The course dataset (raw orders, customers, products, and feedback) is pre-loaded into your catalog's <strong>data</strong> schema.
# MAGIC
# MAGIC - A <strong>Shared SQL Warehouse</strong> is provided — all demos and labs should be attached to this warehouse.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Other Databricks Workspaces
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #f0f7ff;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC #### Catalog Information
# MAGIC
# MAGIC - If you are running this course in your own Databricks workspace, the setup notebooks will <strong>create a Unity Catalog catalog and schema for you</strong>.
# MAGIC
# MAGIC - The <strong>Create Catalog</strong> permission is required.
# MAGIC
# MAGIC - The catalog name is derived from your Databricks username and follows this pattern: <strong>labuser_username</strong>
# MAGIC
# MAGIC #### Compute Requirements
# MAGIC
# MAGIC - All demos and labs require a <strong>Shared SQL Warehouse</strong> or <strong>Serverless SQL Warehouse</strong>.
# MAGIC
# MAGIC - <strong>Do NOT use All Purpose Compute or personal clusters</strong> — Streaming Tables, Materialized Views, and AUTO CDC are not supported on general-purpose clusters in this course's SQL notebook context.
# MAGIC
# MAGIC #### Marketplace Data
# MAGIC
# MAGIC Marketplace data install is not required for this course. The course dataset is generated by the setup notebooks.
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #fff5f5;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Do Not Run in Production Environments</strong>
# MAGIC
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Only run this course in <strong>development or sandbox workspaces</strong>.</li>
# MAGIC <li>The setup scripts create catalogs, schemas, streaming tables, materialized views, and pipelines in your workspace.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #d97706;
# MAGIC   background: #fff8e1;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Troubleshooting</strong>
# MAGIC
# MAGIC <div style="color:#333;">
# MAGIC   <details>
# MAGIC     <summary><strong>Cannot create a catalog?</strong></summary>
# MAGIC     <p>If you do not have <code>CREATE CATALOG</code> permission, you can use an existing catalog by setting <code>catalog_forced</code> in the setup cell of any demo or lab.</p>
# MAGIC     <p><strong>SQL Warehouse variant:</strong> <code>DECLARE catalog_forced STRING DEFAULT 'your_catalog_name';</code></p>
# MAGIC   </details>
# MAGIC </div>
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
