-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](../Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demo - Building Slowly Changing Dimensions with AUTO CDC
-- MAGIC ## Overview
-- MAGIC In this demo, we follow **two customers** through a CDC pipeline to see exactly how SCD Type 1 and Type 2 handle changes and deletes differently. We build both types side by side, add new events, refresh, and compare the results at every step.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC - Examine a CDC source table and identify INSERT, UPDATE, and DELETE operations
-- MAGIC - Build SCD Type 1 and Type 2 streaming tables with AUTO CDC
-- MAGIC - Observe how Type 1 overwrites history while Type 2 preserves it
-- MAGIC - Add new CDC events and see incremental processing in action
-- MAGIC - See how `APPLY AS DELETE WHEN` physically removes vs tombstones deleted customers

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
-- MAGIC     Run the setup cell below to configure your catalog, schemas, and load the CDC source data.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.2-Demo

-- COMMAND ----------

SELECT my_catalog AS Catalog, 'bronze' AS `Source Schema`, 'silver' AS `Target Schema`;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Confirm your catalog name appears in the output. The demo will create CDC streaming tables in the <strong>silver</strong> schema using data from the <strong>bronze</strong> schema.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## B. The CDC Source — How Changes Flow
-- MAGIC
-- MAGIC In production, customer data changes constantly: people move, upgrade their loyalty tier, or close their accounts. A static snapshot goes stale immediately.
-- MAGIC
-- MAGIC The `bronze.customer_changes` table is the **complete CDC feed** — it contains two layers:
-- MAGIC
-- MAGIC 1. **Initial load** — every customer captured as an INSERT event (the baseline, timestamped with their registration date)
-- MAGIC 2. **Subsequent changes** — UPDATEs, new INSERTs, and DELETEs that happened after the baseline
-- MAGIC
-- MAGIC This mirrors how real-world CDC works: the initial load is captured as INSERT events, then all subsequent changes flow in as a stream. AUTO CDC processes this combined feed to produce an always-current dimension table.
-- MAGIC
-- MAGIC The setup built `customer_changes` with **~4,000 events** (~2,500 initial INSERTs + ~1,500 change events). We will track two specific customers through every step of this demo:
-- MAGIC
-- MAGIC <div style="display:flex; gap:32px; margin:16px 0 20px 0;">
-- MAGIC   <div style="flex:1; border:2px solid #e65100; border-radius:10px; padding:16px 20px; background:#fff; text-align:center;">
-- MAGIC     <img src="../Includes/images/icons/mia-persona.png" height="56" style="margin-bottom:8px;"><br/>
-- MAGIC     <strong style="font-size:1.1em; color:#e65100;">Mia King (2665)</strong><br/>
-- MAGIC     <span style="color:#555;">Tier changes rapidly<br/>bronze → silver → gold → platinum</span>
-- MAGIC   </div>
-- MAGIC   <div style="flex:1; border:2px solid #00838f; border-radius:10px; padding:16px 20px; background:#fff; text-align:center;">
-- MAGIC     <img src="../Includes/images/icons/liam-persona.png" height="56" style="margin-bottom:8px;"><br/>
-- MAGIC     <strong style="font-size:1.1em; color:#00838f;">Liam Martin (2591)</strong><br/>
-- MAGIC     <span style="color:#555;">Moves across cities<br/>San Francisco → Houston → Dallas</span>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Overview of the CDC Feed

-- COMMAND ----------

-- See the total events and breakdown
SELECT COUNT(*) AS total_events FROM bronze.customer_changes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The combined CDC feed has ~4,000 events — ~2,500 initial INSERTs (the baseline load) plus ~1,500 subsequent changes. Every customer starts as an INSERT event, then subsequent UPDATEs and DELETEs flow in as the stream progresses.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Event Type Breakdown

-- COMMAND ----------

SELECT
  operation,
  COUNT(*) AS event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM bronze.customer_changes
GROUP BY operation
ORDER BY event_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The majority of events are INSERTs (the ~2,500 baseline customers plus ~300 new customers from the change stream). UPDATEs (address/tier changes) and DELETEs make up the rest. This is realistic — the initial load dominates, then changes trickle in.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">B3. <img src="../Includes/images/icons/mia-persona.png" height="28" style="vertical-align:middle;"> Mia King's Change History</h3>
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, operation, updated_at, city, loyalty_tier
FROM bronze.customer_changes
WHERE customer_id = 2665
ORDER BY updated_at;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC Mia was created in Denver as a bronze customer and her loyalty tier climbed through every level: <strong>bronze → silver → gold → platinum</strong>. Four events, each with a visible change. Remember this — we will compare how Type 1 and Type 2 represent this history.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">B4. <img src="../Includes/images/icons/liam-persona.png" height="28" style="vertical-align:middle;"> Liam Martin's Change History</h3>
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, operation, updated_at, city, loyalty_tier
FROM bronze.customer_changes
WHERE customer_id = 2591
ORDER BY updated_at;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC Liam was created in San Francisco, moved to Houston, upgraded to silver, then moved to Dallas. Four events, each with a visible change — city or tier.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Build SCD Type 1 and Type 2 — Side by Side
-- MAGIC
-- MAGIC Now we apply the CDC events to produce **always-current dimension tables**. Downstream pipelines (like the gold-layer aggregations in Demo 4.2) can join with these CDC-derived tables to get the latest customer state.
-- MAGIC
-- MAGIC We create two streaming tables — both with `APPLY AS DELETE WHEN` so they handle deletes from the start. The **only** difference is the last line: `SCD TYPE 1` vs `SCD TYPE 2`.
-- MAGIC
-- MAGIC | Clause | Purpose |
-- MAGIC |---|---|
-- MAGIC | `FLOW AUTO CDC` | Declares this as a CDC processing pipeline |
-- MAGIC | `FROM stream(...)` | Reads the source table as a stream |
-- MAGIC | `KEYS (customer_id)` | Business key — identifies each entity |
-- MAGIC | `APPLY AS DELETE WHEN` | Which rows represent deletions |
-- MAGIC | `SEQUENCE BY updated_at` | Handles out-of-order events by timestamp |
-- MAGIC | `COLUMNS * EXCEPT (operation)` | Excludes CDC metadata from the target |
-- MAGIC | `STORED AS SCD TYPE 1` or `2` | Current-state-only vs full version history |
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>Note:</strong> The lecture examples exclude both <code>operation</code> and <code>_metadata</code> because production CDC sources often include a <code>_metadata</code> column from Auto Loader ingestion. Our demo source table (<code>bronze.customer_changes</code>) does not have a <code>_metadata</code> column, so we only exclude <code>operation</code> here.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. SCD Type 1 — Current State Only

-- COMMAND ----------

DROP TABLE IF EXISTS silver.demo_customers_type1;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver.demo_customers_type1
FLOW AUTO CDC
FROM stream(bronze.customer_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What happened</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     The SCD Type 1 streaming table was created and populated from the full CDC feed. It processed all events and collapsed them into <strong>one row per customer</strong> — always the latest state. Deleted customers are removed entirely.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. SCD Type 2 — Full Version History

-- COMMAND ----------

DROP TABLE IF EXISTS silver.demo_customers_type2;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver.demo_customers_type2
FLOW AUTO CDC
FROM stream(bronze.customer_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What happened</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     The SCD Type 2 streaming table was created from the same CDC feed. Unlike Type 1, it preserves <strong>every version</strong> of each customer with <code>__START_AT</code> and <code>__END_AT</code> timestamps. The current version has <code>__END_AT = NULL</code>.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Initial Load — Compare Type 1 vs Type 2
-- MAGIC
-- MAGIC Both streaming tables have now processed the full ~4,000 events (baseline + CDC). Let's compare.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Row Counts

-- COMMAND ----------

SELECT 'Source (bronze.customer_changes)' AS table_name, COUNT(*) AS rows FROM bronze.customer_changes
UNION ALL SELECT 'Type 1 (current state)', COUNT(*) FROM silver.demo_customers_type1
UNION ALL SELECT 'Type 2 (full history)', COUNT(*) FROM silver.demo_customers_type2;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Source</strong> has ~4,000 events (2,500 baseline INSERTs + ~1,500 subsequent changes)</li>
-- MAGIC       <li><strong>Type 1</strong> has roughly 2,700 rows — one per customer (latest state only), including the ~2,500 baseline customers plus ~300 new customers from the change stream, minus those that were subsequently deleted.</li>
-- MAGIC       <li><strong>Type 2</strong> has more rows than Type 1 — it stores <strong>every version</strong> of each customer. Customers with multiple changes have multiple rows. The baseline INSERT is the first version, and each subsequent change creates a new version row.</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">D2. <img src="../Includes/images/icons/mia-persona.png" height="28" style="vertical-align:middle;"> Mia King — Type 1 vs Type 2</h3>
-- MAGIC
-- MAGIC Recall Mia's 4 raw CDC events from section B3:
-- MAGIC
-- MAGIC | # | operation | city | loyalty_tier |
-- MAGIC |---|---|---|---|
-- MAGIC | 1 | INSERT | Denver | bronze |
-- MAGIC | 2 | UPDATE | Denver | silver |
-- MAGIC | 3 | UPDATE | Denver | gold |
-- MAGIC | 4 | UPDATE | Denver | platinum |
-- MAGIC
-- MAGIC How does each SCD type represent these 4 events? Type 1 first:
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier
FROM silver.demo_customers_type1
WHERE customer_id = 2665;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>One row.</strong> Denver, platinum. Her entire journey through bronze → silver → gold is gone — Type 1 overwrites everything with the latest state.
-- MAGIC
-- MAGIC Now Type 2:
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier, __START_AT, __END_AT
FROM silver.demo_customers_type2
WHERE customer_id = 2665
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>Every version preserved.</strong> You can see exactly when Mia was bronze, when she upgraded to silver, gold, and platinum. Each row's <code>__END_AT</code> connects to the next row's <code>__START_AT</code>, forming a continuous timeline. The last row has <code>__END_AT = NULL</code> — that is her current state.
-- MAGIC
-- MAGIC <strong>Key insight:</strong> Type 1 answers <em>"What tier is Mia in now?"</em> — Type 2 answers <em>"What tier was Mia in on March 1st?"</em>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">D3. <img src="../Includes/images/icons/liam-persona.png" height="28" style="vertical-align:middle;"> Liam Martin — Type 1 vs Type 2</h3>
-- MAGIC
-- MAGIC Recall Liam's 4 raw CDC events from section B4:
-- MAGIC
-- MAGIC | # | operation | city | loyalty_tier |
-- MAGIC |---|---|---|---|
-- MAGIC | 1 | INSERT | San Francisco | bronze |
-- MAGIC | 2 | UPDATE | Houston | bronze |
-- MAGIC | 3 | UPDATE | Houston | silver |
-- MAGIC | 4 | UPDATE | Dallas | silver |
-- MAGIC
-- MAGIC Type 1:
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier
FROM silver.demo_customers_type1
WHERE customer_id = 2591;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC Type 2:
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier, __START_AT, __END_AT
FROM silver.demo_customers_type2
WHERE customer_id = 2591
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC Liam is present in both tables. Type 1 shows his latest state — Dallas, silver. Type 2 shows his full journey with <code>__END_AT = NULL</code> on the last row — he is an active customer.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. New Data Arrives — Incremental Processing
-- MAGIC
-- MAGIC In a real pipeline, CDC events arrive continuously. We simulate this by adding **~200 new events** to the source — including more updates, new customers, and **DELETE operations** for some existing customers.
-- MAGIC
-- MAGIC The new batch lives in `data.raw_customers_cdc_new_batch`. The **`data` schema** contains pre-loaded source tables provisioned by the course setup — it is read-only and shared across all students.
-- MAGIC
-- MAGIC Let's preview what's coming for our two focus customers.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">E1. <img src="../Includes/images/icons/mia-persona.png" height="28" style="vertical-align:middle;"> Preview — Mia's New Events</h3>
-- MAGIC </div>

-- COMMAND ----------

SELECT first_name, last_name, operation, updated_at, city, loyalty_tier
FROM data.raw_customers_cdc_new_batch
WHERE customer_id = 2665
ORDER BY updated_at;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC Mia gets 3 new events — drops from platinum to <strong>gold</strong>, moves from Denver to <strong>Atlanta</strong>, then climbs back to <strong>platinum</strong>. Every event shows a visible change.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">E2. <img src="../Includes/images/icons/liam-persona.png" height="28" style="vertical-align:middle;"> Preview — Liam's New Events</h3>
-- MAGIC </div>

-- COMMAND ----------

SELECT first_name, last_name, operation, updated_at, city, loyalty_tier
FROM data.raw_customers_cdc_new_batch
WHERE customer_id = 2591
ORDER BY updated_at;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC A single <strong>DELETE</strong> event. This is where Type 1 and Type 2 will diverge in how they handle Liam.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E3. Load the New Batch and Refresh

-- COMMAND ----------

SELECT COUNT(*) AS events_before FROM bronze.customer_changes;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Note this number (~4,000 events) — you will compare it after loading the new batch to confirm how many new CDC events were added.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

INSERT INTO bronze.customer_changes
SELECT * FROM data.raw_customers_cdc_new_batch;

-- COMMAND ----------

SELECT COUNT(*) AS events_after FROM bronze.customer_changes;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     The count increased by approximately <strong>200</strong> — confirming the new batch of CDC events was loaded into the source table. The streaming tables have not picked up these events yet; that happens on the next refresh.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

REFRESH STREAMING TABLE silver.demo_customers_type1;

-- COMMAND ----------

REFRESH STREAMING TABLE silver.demo_customers_type2;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">E4. <img src="../Includes/images/icons/mia-persona.png" height="28" style="vertical-align:middle;"> Mia King — After the Refresh</h3>
-- MAGIC
-- MAGIC Before the refresh, Type 1 showed Mia as <strong>Denver, platinum</strong> (1 row). Type 2 showed <strong>4 version rows</strong>. The new batch added: gold (downgrade) → Atlanta (city move) → platinum (upgrade back).
-- MAGIC
-- MAGIC <strong>Type 1 — Mia now:</strong>
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier
FROM silver.demo_customers_type1
WHERE customer_id = 2665;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>Atlanta, platinum.</strong> Denver overwritten — Type 1 only keeps the latest state.
-- MAGIC
-- MAGIC <strong>Type 2 — Mia's updated timeline:</strong>
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier, __START_AT, __END_AT
FROM silver.demo_customers_type2
WHERE customer_id = 2665
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #e65100; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>Compare this to section D2.</strong> The original 4 rows are still there, but the old platinum row was closed (its <code>__END_AT</code> was set) and 3 new version rows were appended — the gold downgrade, the Atlanta move, and the platinum return. The complete timeline from INSERT through every change is preserved.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <h3 style="margin:0 0 4px 0;">E5. <img src="../Includes/images/icons/liam-persona.png" height="28" style="vertical-align:middle;"> Liam Martin — The Delete</h3>
-- MAGIC
-- MAGIC Before the refresh, Liam was active in both tables — Type 1 showed <strong>Dallas, silver</strong> (1 row), Type 2 showed <strong>4 version rows</strong> with <code>__END_AT = NULL</code> on the last. A DELETE event just arrived.
-- MAGIC
-- MAGIC <strong>Type 1 — Is Liam still here?</strong>
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier
FROM silver.demo_customers_type1
WHERE customer_id = 2591;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>No rows.</strong> Liam was physically removed — <code>APPLY AS DELETE WHEN</code> with SCD Type 1 deletes the row entirely. No trace remains.
-- MAGIC
-- MAGIC <strong>Type 2 — Liam's tombstoned history:</strong>
-- MAGIC </div>

-- COMMAND ----------

SELECT customer_id, first_name, last_name, city, loyalty_tier, __START_AT, __END_AT
FROM silver.demo_customers_type2
WHERE customer_id = 2591
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #00838f; background: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC Liam's full history is still here — but every row now has a non-NULL <code>__END_AT</code>. His final version was closed with the deletion timestamp. The history is preserved for audit and compliance, but <code>WHERE __END_AT IS NULL</code> returns nothing for Liam — he has no current state.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>The key delete difference:</strong>
-- MAGIC <ul style="margin: 8px 0 0 0;">
-- MAGIC <li><strong>Type 1:</strong> Customer is <strong>gone</strong> — physically removed, no trace</li>
-- MAGIC <li><strong>Type 2:</strong> Customer is <strong>tombstoned</strong> — full history preserved, but no current version exists</li>
-- MAGIC </ul>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Genie Code: Explain AUTO CDC Syntax

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">AI-Assisted Code Explanation with /explain</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Select the SCD Type 2 <code>CREATE OR REFRESH STREAMING TABLE</code> statement from Section C2, open the Genie Code panel, and type <code>/explain</code>. Review the clause-by-clause explanation.</p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #ff9800; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Instructor Note — Non-Deterministic Output</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Genie Code output varies between runs. Reinforce the key clauses: <code>KEYS</code>, <code>SEQUENCE BY</code>, <code>STORED AS SCD TYPE</code>, and <code>APPLY AS DELETE WHEN</code>.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Key Summary and Takeaways
-- MAGIC
-- MAGIC | | SCD Type 1 | SCD Type 2 |
-- MAGIC |---|---|---|
-- MAGIC | **Rows per customer** | Always 1 (latest state only) | 1 per version (full history) |
-- MAGIC | **Mia after initial load** | Denver, platinum | 4 rows: bronze → silver → gold → platinum |
-- MAGIC | **Mia after new batch** | Atlanta, platinum | 7 rows: full timeline with downgrade and city move |
-- MAGIC | **Liam after delete** | Physically removed (0 rows) | Tombstoned — all rows have `__END_AT` set |
-- MAGIC | **Use case** | Current-state lookups, dashboards | Historical analysis, audit trails, compliance |
-- MAGIC
-- MAGIC **The pattern:** `CREATE OR REFRESH STREAMING TABLE ... FLOW AUTO CDC` with `KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`, and `STORED AS SCD TYPE` replaces what would otherwise require 100-150 lines of manual `MERGE INTO` logic.
-- MAGIC
-- MAGIC **The big picture:** `customer_changes` is a complete CDC feed — initial customer records captured as INSERT events, plus all subsequent changes. AUTO CDC processes this feed to produce an always-current dimension table that downstream pipelines (like the gold-layer aggregations in Demo 4.2) can join with for the latest customer state.

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
