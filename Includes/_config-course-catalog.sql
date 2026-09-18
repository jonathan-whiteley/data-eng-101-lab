-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Course Catalog Configuration
-- MAGIC
-- MAGIC ## This is the one place to configure where the course writes.
-- MAGIC
-- MAGIC Set `course_catalog` below to the catalog you want the course to use. Every demo, lab, and the data-setup notebook read this single value, so you only change it here.
-- MAGIC
-- MAGIC | Value | Behavior | Privileges needed |
-- MAGIC |---|---|---|
-- MAGIC | An existing catalog name (e.g. `jdub_demo`) | Uses that catalog as-is. **No `CREATE CATALOG` needed.** The course creates its schemas (`bronze`, `silver`, `gold`, `demo_cdc`, `data`) and a `data.course_data` volume inside it. | You must own the catalog or hold `ALL PRIVILEGES` on it (`USE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME`, `CREATE TABLE`). |
-- MAGIC | `NONE` | Original Academy behavior: auto-creates a per-user `labuser_<username>` catalog (or uses the Vocareum lab catalog). | Metastore-level `CREATE CATALOG` (or a pre-provisioned Vocareum workspace). |
-- MAGIC
-- MAGIC Leave the rest of the setup notebooks untouched.

-- COMMAND ----------

-- >>> EDIT THIS ONE VALUE <<<
-- Set to an existing catalog you own to run on any workspace with no CREATE CATALOG rights.
-- Set to 'NONE' to restore the original auto-create-labuser behavior.
DECLARE OR REPLACE course_catalog STRING DEFAULT 'jdub_demo';
