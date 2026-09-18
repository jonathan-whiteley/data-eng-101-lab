# Building ETL Pipelines with SQL

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 240 minutes   | Estimated duration to complete the full course (4 modules with lectures, demos, and labs). |
| Level           | 200           | Intermediate — assumes familiarity with SQL, Unity Catalog basics, and data warehousing concepts. |
| Lab Status      | Active        | Course is actively maintained and delivered. |
| Course Location | N/A | Instructor-led and self-paced delivery through Databricks Academy. |
| Developer       | Jade Lauzon   | Primary developer of the course. |
| Reviewer        |               | Subject matter expert reviewer(s). |
| Product Version | N/A           | Initial release. |
| Created Date    | 04/14/2026    | Course creation date. |
---

## Description
This course teaches how to build **production-ready ETL pipelines using pure SQL** on the Databricks Data Intelligence Platform. Students learn the declarative building blocks — **Streaming Tables**, **Materialized Views**, and **AUTO CDC** — that replace hundreds of lines of procedural SQL with concise, incremental, and fully managed pipeline definitions. The course follows a realistic retail dataset through the **medallion architecture** (Bronze → Silver → Gold) and culminates in orchestrating the full pipeline with a **Lakeflow Job**.

## Learning Objectives
- Create **Streaming Tables** that incrementally ingest data from cloud storage using Auto Loader
- Create **Materialized Views** that pre-compute and incrementally refresh Silver-to-Gold transformations
- Use **EXPLAIN CREATE MATERIALIZED VIEW** to predict incrementalizability before deploying
- Build **SCD Type 1 and Type 2** dimensions using **AUTO CDC** with FLOW AUTO CDC, KEYS, SEQUENCE BY, and APPLY AS DELETE
- Monitor AUTO CDC pipelines with **event_log()** queries
- Orchestrate a multi-step ETL pipeline using **Lakeflow Jobs** with SQL File tasks and DAG-based workflows

## Requirements & Prerequisites
Before starting this course, ensure you have:
- A **Databricks** workspace with Unity Catalog enabled
- A Serverless SQL Warehouse
- **Either** `CREATE CATALOG` permission (auto-creates a per-user `labuser_<username>` catalog) **or** an existing catalog you own. To run on any workspace with no `CREATE CATALOG` rights, set `course_catalog` in `Includes/_config-course-catalog` to your catalog name (needs `USE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME`, `CREATE TABLE` on it). A pre-provisioned Vocareum workspace also works with the default.
- Familiarity with intermediate SQL (SELECT, JOIN, GROUP BY, CAST, COALESCE, CREATE TABLE)
- Understanding of data warehousing concepts (fact/dimension tables, star schemas, medallion architecture)

## Contents
This repository includes:
- **0 - Course Overview** notebook
- **1 - REQUIRED - Course Data Setup** notebook
- **Module 1 - SQL ETL on Databricks/**
  - 1.1 Lecture - SQL ETL on Databricks: The Big Picture
  - 1.2 Demo - Exploring the Course Dataset and SQL Editor
  - 1.3 Lab - Using the SQL Editor and Genie Code
- **Module 2 - Streaming Tables and Materialized Views/**
  - 2.1 Lecture - Building SQL ETL Pipelines
  - 2.2 Demo - Building a Silver-to-Gold Pipeline
  - 2.3 Lab - Building a Customer Feedback Pipeline
- **Module 3 - Auto CDC/**
  - 3.1 Lecture - AUTO CDC Streaming Dimension Updates
  - 3.2 Demo - Building Slowly Changing Dimensions with AUTO CDC
  - 3.3 Lab - Building Slowly Changing Dimensions
- **Module 4 - Orchestrating with Lakeflow Jobs/**
  - 4.1 Lecture - Orchestrating SQL Pipelines with Lakeflow Jobs
  - 4.2 Demo - Building a Lakeflow Job for the ETL Pipeline
  - 4.3 Lab - Orchestrating SQL Pipelines with Lakeflow Jobs
  - pipeline_sql/ — Demo 4.2 SQL files for Lakeflow Job tasks
  - lab_pipeline_sql/ — Lab 4.3 SQL files + bonus/
- **99 - Summary and Next Steps** notebook
- **Includes/**
  - Classroom-Setup-Common-SQL-WAREHOUSE
  - Classroom-Setup-{1.2, 1.3, 2.2, 2.3, 3.2, 3.3, 4.2, 4.3}
  - Course-Cleanup
  - Create-Job-4.3-Lab
  - data/ — Dataset generation scripts
  - images/ — DB Academy logo, icons

## Getting Started
1. Open **0 - Course Overview** to review the course objectives and workspace setup information.
2. Run **1 - REQUIRED - Course Data Setup** once to generate the course dataset.
3. Work through Modules 1–4 in order. Each module follows the pattern: Lecture → Demo → Lab.
4. Each demo and lab has its own classroom setup cell that runs automatically — just attach to a **Serverless SQL Warehouse** and run all cells.
