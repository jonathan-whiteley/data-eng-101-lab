-- Databricks notebook source
-- MAGIC %run ./Classroom-Setup-Common-SQL-WAREHOUSE

-- COMMAND ----------

USE SCHEMA default;

-- COMMAND ----------

SELECT
  'Setup Complete' AS Status,
  my_catalog AS Catalog,
  'default' AS Schema;
