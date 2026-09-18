# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Next Steps
# MAGIC
# MAGIC Congratulations on completing **Building ETL Pipelines with SQL** on Databricks!
# MAGIC
# MAGIC In this course, you built a complete, incremental ETL pipeline using pure SQL — from raw file ingestion through enriched Silver views to business-ready Gold aggregations, orchestrated by a production Lakeflow Job. Here is a recap of what you accomplished:
# MAGIC
# MAGIC | Module | What You Built |
# MAGIC |--------|---------------|
# MAGIC | **Module 1** — SQL ETL on Databricks | Explored the SQL Editor, used Genie Code to generate and fix SQL, created your first Streaming Table and Materialized View |
# MAGIC | **Module 2** — Streaming Tables and Materialized Views | Built the Bronze-to-Silver-to-Gold pipeline with Auto Loader ingestion, incremental MV refresh, and EXPLAIN-based incrementalizability checks |
# MAGIC | **Module 3** — AUTO CDC | Replaced complex MERGE INTO logic with declarative SCD Type 1 and Type 2 dimension management using FLOW AUTO CDC |
# MAGIC | **Module 4** — Orchestrating with Lakeflow Jobs | Created a multi-task Lakeflow Job with SQL File tasks, DAG-based parallelism, incremental ingestion, task output passing, and Repair Run |

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Additional Resources
# MAGIC
# MAGIC Explore the following resources to deepen your understanding of the topics covered in this course.
# MAGIC
# MAGIC ### A1. Streaming Tables and Auto Loader
# MAGIC
# MAGIC - Load data using streaming tables ([AWS](https://docs.databricks.com/aws/en/ldp/streaming-tables) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/streaming-tables) | [GCP](https://docs.databricks.com/gcp/en/ldp/streaming-tables)) — Overview of streaming table ingestion patterns
# MAGIC
# MAGIC - CREATE STREAMING TABLE ([AWS](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table) | [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table)) — SQL reference for creating streaming tables
# MAGIC
# MAGIC - Use streaming tables in Lakehouse SQL ([AWS](https://docs.databricks.com/aws/en/ldp/dbsql/streaming) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/dbsql/streaming) | [GCP](https://docs.databricks.com/gcp/en/ldp/dbsql/streaming)) — Recommended approach for SQL-based ingestion
# MAGIC
# MAGIC - What is Auto Loader? ([AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/) | [GCP](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/)) — How Auto Loader incrementally processes new files from cloud storage
# MAGIC
# MAGIC - read_files table-valued function ([AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/read_files) | [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/read_files)) — SQL syntax for the `read_files()` function used in streaming tables
# MAGIC
# MAGIC - Auto Loader options reference ([AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options) | [GCP](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/options)) — Format, schema inference, globbing, and more
# MAGIC
# MAGIC ### A2. Materialized Views and Incremental Refresh
# MAGIC
# MAGIC - Materialized views in Lakeflow Declarative Pipelines ([AWS](https://docs.databricks.com/aws/en/ldp/materialized-views) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/materialized-views) | [GCP](https://docs.databricks.com/gcp/en/ldp/materialized-views)) — Overview of materialized views
# MAGIC
# MAGIC - CREATE MATERIALIZED VIEW ([AWS](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view) | [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view)) — SQL reference for materialized views
# MAGIC
# MAGIC - Use materialized views in Lakehouse SQL ([AWS](https://docs.databricks.com/aws/en/ldp/dbsql/materialized) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/dbsql/materialized) | [GCP](https://docs.databricks.com/gcp/en/ldp/dbsql/materialized)) — Creating, configuring, and monitoring MVs
# MAGIC
# MAGIC - EXPLAIN CREATE MATERIALIZED VIEW ([AWS](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-explain-materialized-view) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-explain-materialized-view) | [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-qry-explain-materialized-view)) — Check incrementalizability before deploying
# MAGIC
# MAGIC - Manage incremental refresh ([AWS](https://docs.databricks.com/aws/en/optimizations/incremental-refresh) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/optimizations/incremental-refresh) | [GCP](https://docs.databricks.com/gcp/en/optimizations/incremental-refresh)) — How incremental refresh works and requirements
# MAGIC
# MAGIC - REFRESH POLICY ([AWS](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy) | [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy)) — AUTO, INCREMENTAL, or INCREMENTAL STRICT
# MAGIC
# MAGIC ### A3. AUTO CDC and Change Data Capture
# MAGIC
# MAGIC - The AUTO CDC APIs ([AWS](https://docs.databricks.com/aws/en/ldp/cdc) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc) | [GCP](https://docs.databricks.com/gcp/en/ldp/cdc)) — Simplify CDC with SCD Type 1 and Type 2 in pipelines
# MAGIC
# MAGIC - What is change data capture? ([AWS](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/what-is-change-data-capture) | [GCP](https://docs.databricks.com/gcp/en/ldp/what-is-change-data-capture)) — Concepts and overview
# MAGIC
# MAGIC - Advanced AUTO CDC topics ([AWS](https://docs.databricks.com/aws/en/ldp/cdc-advanced) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc-advanced) | [GCP](https://docs.databricks.com/gcp/en/ldp/cdc-advanced)) — Out-of-order records, tracked columns, and more
# MAGIC
# MAGIC - Tutorial: Build an ETL pipeline using change data capture ([AWS](https://docs.databricks.com/aws/en/ldp/tutorial-pipelines) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorial-pipelines) | [GCP](https://docs.databricks.com/gcp/en/ldp/tutorial-pipelines)) — Hands-on walkthrough
# MAGIC
# MAGIC ### A4. Lakeflow Jobs and Orchestration
# MAGIC
# MAGIC - What are Lakeflow Jobs? ([AWS](https://docs.databricks.com/aws/en/jobs/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/) | [GCP](https://docs.databricks.com/gcp/en/jobs/)) — Overview of Databricks' unified orchestration platform
# MAGIC
# MAGIC - SQL task for jobs ([AWS](https://docs.databricks.com/aws/en/jobs/sql) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/sql) | [GCP](https://docs.databricks.com/gcp/en/jobs/sql)) — Configure SQL query, file, alert, or dashboard tasks
# MAGIC
# MAGIC - Dynamic value references ([AWS](https://docs.databricks.com/aws/en/jobs/dynamic-value-references) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/dynamic-value-references) | [GCP](https://docs.databricks.com/gcp/en/jobs/dynamic-value-references)) — Job parameters, task output passing, and ForEach references
# MAGIC
# MAGIC - Repair a failed job run ([AWS](https://docs.databricks.com/aws/en/jobs/repair-job-failures) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/repair-job-failures) | [GCP](https://docs.databricks.com/gcp/en/jobs/repair-job-failures)) — Re-execute only failed tasks and their dependents
# MAGIC
# MAGIC - Create your first workflow ([AWS](https://docs.databricks.com/aws/en/jobs/jobs-quickstart) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/jobs-quickstart) | [GCP](https://docs.databricks.com/gcp/en/jobs/jobs-quickstart)) — Quickstart tutorial
# MAGIC
# MAGIC ### A5. Spark Declarative Pipelines (Under the Hood)
# MAGIC
# MAGIC - What are Spark Declarative Pipelines? ([AWS](https://docs.databricks.com/aws/en/ldp/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/) | [GCP](https://docs.databricks.com/gcp/en/ldp/)) — The engine that powers Streaming Tables and Materialized Views
# MAGIC
# MAGIC - Develop pipelines with SQL ([AWS](https://docs.databricks.com/aws/en/ldp/developer/sql-dev) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/sql-dev) | [GCP](https://docs.databricks.com/gcp/en/ldp/developer/sql-dev)) — SQL-specific pipeline development guide
# MAGIC
# MAGIC - event_log table-valued function ([AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/event_log) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/event_log) | [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/event_log)) — Query pipeline metrics via SQL
# MAGIC
# MAGIC - Monitor pipelines with event logs ([AWS](https://docs.databricks.com/aws/en/ldp/monitor-event-logs) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/monitor-event-logs) | [GCP](https://docs.databricks.com/gcp/en/ldp/monitor-event-logs)) — Audit logs, data quality checks, and lineage
# MAGIC
# MAGIC - Tutorial: Build an ETL pipeline ([AWS](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/getting-started/data-pipeline-get-started) | [GCP](https://docs.databricks.com/gcp/en/getting-started/data-pipeline-get-started)) — End-to-end walkthrough with Lakeflow Declarative Pipelines
# MAGIC
# MAGIC ### A6. SQL Editor and Genie Code
# MAGIC
# MAGIC - SQL Editor ([AWS](https://docs.databricks.com/aws/en/sql/user/sql-editor/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/user/sql-editor/) | [GCP](https://docs.databricks.com/gcp/en/sql/user/sql-editor/)) — The SQL authoring interface used throughout this course
# MAGIC
# MAGIC - Genie Code ([AWS](https://docs.databricks.com/aws/en/genie-code/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/genie-code/) | [GCP](https://docs.databricks.com/gcp/en/genie-code/)) — AI assistant for data work in Databricks
# MAGIC
# MAGIC ### A7. Unity Catalog and Architecture
# MAGIC
# MAGIC - What is Unity Catalog? ([AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog) | [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog)) — Unified governance for data and AI
# MAGIC
# MAGIC - Medallion lakehouse architecture ([AWS](https://docs.databricks.com/aws/en/lakehouse/medallion) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion) | [GCP](https://docs.databricks.com/gcp/en/lakehouse/medallion)) — The bronze/silver/gold pattern explained

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Blog Posts and Announcements
# MAGIC
# MAGIC - [Announcing Spark Declarative Pipelines](https://www.databricks.com/blog/announcing-general-availability-databricks-lakeflow) — The declarative SQL framework behind Streaming Tables and Materialized Views
# MAGIC
# MAGIC - [Introducing Databricks Lakeflow](https://www.databricks.com/blog/introducing-databricks-lakeflow) — Unified, intelligent data engineering
# MAGIC
# MAGIC - [Optimizing Materialized View Recomputes](https://www.databricks.com/blog/optimizing-materialized-views-recomputes) — Incremental refresh and the Enzyme engine
# MAGIC
# MAGIC - [Stop Hand-Coding CDC Pipelines](https://www.databricks.com/blog/stop-hand-coding-change-data-capture-pipelines) — How AUTO CDC simplifies SCD Type 1 and Type 2
# MAGIC
# MAGIC - [What is Change Data Capture?](https://www.databricks.com/blog/what-is-change-data-capture) — Conceptual overview
# MAGIC
# MAGIC - [Faster AI/BI Dashboards with Materialized Views](https://www.databricks.com/blog/faster-lakeview-dashboards-materialized-views) — How MVs accelerate downstream analytics
# MAGIC
# MAGIC - [What is Medallion Architecture?](https://www.databricks.com/blog/what-is-medallion-architecture) — The bronze/silver/gold pattern explained
# MAGIC
# MAGIC - [Introducing Genie Code](https://www.databricks.com/blog/introducing-genie-code) — AI assistant for data work
# MAGIC
# MAGIC - [Observability in Lakeflow](https://www.databricks.com/blog/how-observability-lakeflow-helps-you-build-reliable-data-pipelines) — Building reliable data pipelines with built-in monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Customer Stories
# MAGIC
# MAGIC See how organizations are building production data pipelines on Databricks:
# MAGIC
# MAGIC - [GetYourGuide](https://www.databricks.com/customers/getyourguide/dbsql) — Migrated from Snowflake to Lakehouse SQL with ~20% lower BI costs
# MAGIC
# MAGIC - [hipages](https://www.databricks.com/customers/hipages) — Consolidated three data warehouses into one lakehouse for self-service analytics
# MAGIC
# MAGIC - [Raiffeisen Bank International](https://www.databricks.com/customers/rbi) — Modernized Oracle/DB2/AWS warehouses to Lakehouse SQL
# MAGIC
# MAGIC - [Trek Bicycle](https://www.databricks.com/customers/trek-bicycle) — Unified point-of-sales data into a lakehouse with 3x daily refresh
# MAGIC
# MAGIC - [Explore all customer stories](https://www.databricks.com/customers) — Filter by industry and use case

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### D1. Continue Your Learning
# MAGIC
# MAGIC Expand your data and AI knowledge through Databricks self-paced and instructor-led training. These courses help you deepen your technical skills and gain hands-on experience with the Databricks platform.
# MAGIC
# MAGIC Visit the [Databricks Training and Certification](https://www.databricks.com/learn/training/home)
# MAGIC
# MAGIC ### D2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise by earning an official credential. Certifications demonstrate your ability to apply Databricks technologies in real-world data and AI workloads.
# MAGIC
# MAGIC Visit the [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification)

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
