-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](../Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3.3 Lab - Building Slowly Changing Dimensions
-- MAGIC
-- MAGIC #### Duration: ~15–20 Minutes
-- MAGIC
-- MAGIC ## Overview
-- MAGIC In this lab, you will build three CDC dimension tables from the same source — SCD Type 1 (current state), SCD Type 2 (full history), and SCD Type 2 with **TRACK HISTORY** filtering (meaningful history only). By comparing all three on the same customer, you will see exactly how each approach handles changes and why filtering out administrative noise produces cleaner dimension tables.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you will be able to:
-- MAGIC - Explore a CDC event stream and distinguish between administrative and business-meaningful changes
-- MAGIC - Study an **SCD Type 1** streaming table built with `FLOW AUTO CDC`
-- MAGIC - Build an **SCD Type 2** streaming table that preserves full version history
-- MAGIC - Build an **SCD Type 2 with TRACK HISTORY ON** to filter out noisy changes
-- MAGIC - Compare version counts across all three approaches to understand the tradeoffs
-- MAGIC - Observe **incremental CDC processing** with a new batch of events

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
-- MAGIC     Run the following cell to configure your working environment. The setup creates the <code>bronze.customer_changes</code> CDC source table (combining the baseline customer load with subsequent change events) and drops any leftover lab objects from previous runs.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.3-Lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Pipeline Architecture
-- MAGIC
-- MAGIC Here is the full pipeline architecture you will build across this course. This lab focuses on the **CDC dimension layer** (shown in purple) — the streaming tables that process change events into always-current and historical dimension tables.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
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
-- MAGIC     subgraph CDC["<b>CDC — Dimension Tables (This Lab)</b>"]
-- MAGIC         CDC1["lab_customers_type1<br/><small>ST · AUTO CDC · SCD Type 1</small>"]
-- MAGIC         CDC2["lab_customers_type2<br/><small>ST · AUTO CDC · SCD Type 2</small>"]
-- MAGIC         CDC3["lab_customers_type2_tracked<br/><small>ST · AUTO CDC · SCD Type 2 + TRACK HISTORY</small>"]
-- MAGIC     end
-- MAGIC     subgraph SILVER["<b>SILVER — Materialized Views</b>"]
-- MAGIC         CFE["customer_feedback_enriched<br/><small>MV · joins + context</small>"]
-- MAGIC     end
-- MAGIC     subgraph GOLD["<b>GOLD — Business-Ready Aggregations</b>"]
-- MAGIC         FBC["feedback_by_category<br/><small>MV · aggregation</small>"]
-- MAGIC     end
-- MAGIC     CF --> CFE
-- MAGIC     RO --> CFE
-- MAGIC     RP --> CFE
-- MAGIC     CDC1 --> CFE
-- MAGIC     CDC2 --> CFE
-- MAGIC     CDC3 --> CFE
-- MAGIC     CFE --> FBC
-- MAGIC     classDef bronze fill:#FFF3E0,stroke:#e65100,stroke-width:2px,color:#333
-- MAGIC     classDef silver fill:#E3F2FD,stroke:#1565c0,stroke-width:2px,color:#333
-- MAGIC     classDef gold fill:#E8F5E9,stroke:#2e7d32,stroke-width:2px,color:#333
-- MAGIC     classDef cdc fill:#F3E5F5,stroke:#1976d2,stroke-width:2px,color:#333
-- MAGIC     class RO,RP,CF bronze
-- MAGIC     class CFE silver
-- MAGIC     class FBC gold
-- MAGIC     class CDC1,CDC2,CDC3 cdc
-- MAGIC     style BRONZE fill:#FFF8E1,stroke:#e65100,stroke-width:2px
-- MAGIC     style SILVER fill:#E8EAF6,stroke:#1565c0,stroke-width:2px
-- MAGIC     style GOLD fill:#E8F5E9,stroke:#2e7d32,stroke-width:2px
-- MAGIC     style CDC fill:#F3E5F5,stroke:#1976d2,stroke-width:2px
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

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Explore the CDC Source
-- MAGIC
-- MAGIC Before building anything, let's understand the data. The `bronze.customer_changes` table is a **complete CDC feed** — it contains:
-- MAGIC
-- MAGIC 1. **Baseline INSERTs** — every customer from the initial load, captured as an INSERT event
-- MAGIC 2. **Subsequent changes** — UPDATEs (tier changes, address moves, email corrections), new INSERTs (new customers), and DELETEs (account closures)
-- MAGIC
-- MAGIC Each row has an `operation` column (`INSERT`, `UPDATE`, or `DELETE`) and an `updated_at` timestamp. The AUTO CDC engine uses these to determine what happened and in what order.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Event Type Breakdown

-- COMMAND ----------

SELECT
  operation,
  COUNT(*) AS event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM bronze.customer_changes
GROUP BY operation
ORDER BY event_count DESC;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #2e7d32; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#1b5e20; margin-bottom:6px; font-size: 1.1em;">What to observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     The majority of events are INSERTs (the baseline customer load). UPDATEs represent subsequent changes — tier upgrades, address moves, email corrections. DELETEs are account closures. This mix is typical of real-world CDC feeds.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### C2. Meet Christopher Mitchell (Customer 1231)
-- MAGIC
-- MAGIC We will follow this customer through all three SCD types.
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Run the Preview Query</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Run the query below to see Christopher's full change history in the CDC feed.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- No ORDER BY — notice the out-of-order event at the bottom
SELECT customer_id, first_name, last_name, operation, updated_at, city, loyalty_tier, customer_segment, email
FROM bronze.customer_changes
WHERE customer_id = 1231;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Not All Changes Are Equal</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Look carefully at Christopher's CDC events. Some changes are <strong>business-meaningful</strong> — his loyalty tier climbed from bronze → silver → gold → platinum, and he moved between Kansas City and Nashville. But other changes are just <strong>administrative noise</strong> — his email and phone changed without any business impact. Notice that one event arrived <strong>out of order</strong> (check the timestamps) — <code>SEQUENCE BY</code> handles this automatically.</p>
-- MAGIC     <p>This distinction matters for dimension tables:</p>
-- MAGIC     <ul>
-- MAGIC       <li><strong>Business-meaningful:</strong> city, state, customer_segment, loyalty_tier — these change who the customer IS for analytical purposes</li>
-- MAGIC       <li><strong>Administrative noise:</strong> email, phone, address, zip_code — these are contact details that change frequently but don't affect business analysis</li>
-- MAGIC     </ul>
-- MAGIC     <p>Here is Christopher's event timeline — notice the out-of-order event and which changes are noise vs signal:</p>
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:12px 0;">
-- MAGIC       <tr style="background:#1B3139; color:#fff;"><th style="padding:6px 10px; text-align:left;">#</th><th style="padding:6px 10px;">Timestamp</th><th style="padding:6px 10px;">City</th><th style="padding:6px 10px;">Tier</th><th style="padding:6px 10px;">Email</th><th style="padding:6px 10px;">Type</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;">1</td><td>2025-07-19</td><td>Kansas City</td><td>bronze</td><td>christophermitchell843@...</td><td style="color:#666;">Baseline INSERT</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;">2</td><td>2026-01-10</td><td>Kansas City</td><td style="color:#2e7d32; font-weight:bold;">silver</td><td style="color:#999;">chris.mitchell@...</td><td style="color:#2e7d32;">Tier change + email</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;">3</td><td>2026-01-15</td><td>Kansas City</td><td>silver</td><td style="color:#999;">christopher.m@...</td><td style="color:#e65100;">Email only (noise)</td></tr>
-- MAGIC       <tr style="background:#fff; border-left:3px solid #e65100;"><td style="padding:6px 10px;">*</td><td style="color:#e65100; font-weight:bold;">2026-01-20</td><td>Kansas City</td><td>silver</td><td>christopher.m@...</td><td style="color:#e65100;">Address only (OUT OF ORDER)</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;">4</td><td>2026-02-01</td><td style="color:#2e7d32; font-weight:bold;">Nashville</td><td style="color:#2e7d32; font-weight:bold;">gold</td><td>christopher.m@...</td><td style="color:#2e7d32;">City + tier change</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;">5</td><td>2026-02-10</td><td>Nashville</td><td>gold</td><td>christopher.m@...</td><td style="color:#e65100;">Phone only (noise)</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;">6</td><td>2026-02-20</td><td>Nashville</td><td>gold</td><td style="color:#999;">cmitchell.nash@...</td><td style="color:#e65100;">Email only (noise)</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;">7</td><td>2026-03-05</td><td style="color:#2e7d32; font-weight:bold;">Kansas City</td><td style="color:#2e7d32; font-weight:bold;">platinum</td><td>cmitchell.nash@...</td><td style="color:#2e7d32;">City + tier + segment</td></tr>
-- MAGIC     </table>
-- MAGIC     <p>Event <strong>*</strong> (highlighted) arrived last in the data but has an earlier timestamp. <code>SEQUENCE BY</code> places it in the correct chronological position. In sections D-G, you will see how each SCD approach handles these differently.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Build SCD Type 1 — Example (Study This Pattern)
-- MAGIC
-- MAGIC This first CDC table is **provided for you**. Run it and study the pattern — you will modify it for the next three exercises.
-- MAGIC
-- MAGIC SCD Type 1 keeps **one row per customer** — always the latest state. Every UPDATE overwrites the previous values. This version does **not** handle DELETEs yet (we add that in section F).
-- MAGIC
-- MAGIC Here is each clause and what it does:
-- MAGIC
-- MAGIC | Clause | Purpose |
-- MAGIC |---|---|
-- MAGIC | `FLOW AUTO CDC` | Declares this as a CDC processing pipeline |
-- MAGIC | `FROM stream(bronze.customer_changes)` | Reads the source table as a stream |
-- MAGIC | `KEYS (customer_id)` | Business key — identifies each entity uniquely |
-- MAGIC | `SEQUENCE BY updated_at` | Handles out-of-order events — uses the timestamp to determine correct ordering regardless of arrival order |
-- MAGIC | `COLUMNS * EXCEPT (operation)` | Includes all columns in the target table except `operation` (which is CDC metadata, not a business attribute) |
-- MAGIC | `STORED AS SCD TYPE 1` | Current-state-only behavior — one row per key, always overwritten |
-- MAGIC
-- MAGIC **Study the code below** and run it. Notice how each clause maps to the table above — this is the pattern you will adapt in sections E, F, and G.

-- COMMAND ----------

-- Run this cell — it is provided as a working example

DROP TABLE IF EXISTS silver.lab_customers_type1;

CREATE OR REFRESH STREAMING TABLE silver.lab_customers_type1
FLOW AUTO CDC
FROM stream(bronze.customer_changes)
KEYS (customer_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- Check Christopher Mitchell in Type 1: should be 1 row, latest state
SELECT customer_id, first_name, last_name, city, loyalty_tier, customer_segment, email
FROM silver.lab_customers_type1
WHERE customer_id = 1231;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p><strong>One row.</strong> Christopher's entire CDC history was collapsed into a single row showing his latest state — Kansas City, platinum tier, corporate segment. His journey through bronze → silver → gold → platinum, his move to Nashville and back, and all his email/phone changes are invisible. Type 1 answers <em>"Who is Christopher right now?"</em> but not <em>"What was his tier last month?"</em></p>
-- MAGIC     <p><strong>Note:</strong> This table does not have <code>APPLY AS DELETE WHEN</code> — customers who were deleted in the CDC feed may still appear. You will add delete handling in section F.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Build SCD Type 2 — Full Version History
-- MAGIC
-- MAGIC Now it is your turn. SCD Type 2 preserves **every version** of each customer. When any column changes, the engine:
-- MAGIC 1. **Closes** the current row — sets its `__END_AT` timestamp
-- MAGIC 2. **Opens** a new row — with `__START_AT` set to the event timestamp and `__END_AT = NULL`
-- MAGIC
-- MAGIC This lets you answer historical questions: *"What was Christopher's tier on January 15th?"* — just query `WHERE __START_AT <= '2026-01-15' AND (__END_AT > '2026-01-15' OR __END_AT IS NULL)`.
-- MAGIC
-- MAGIC **Your task:** The SQL below is a copy of section D with one placeholder. Change the SCD TYPE number from `1` to `2` — that is the only change needed. (We are not adding delete handling yet — that comes in section F.)

-- COMMAND ----------

-- Change the SCD TYPE number to build Type 2 instead of Type 1
-- Everything else is identical to section D

DROP TABLE IF EXISTS silver.lab_customers_type2;

CREATE OR REFRESH STREAMING TABLE silver.lab_customers_type2
FLOW AUTO CDC
FROM stream(bronze.customer_changes)
KEYS (customer_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE <FILL_IN>;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyE()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-e" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>DROP TABLE IF EXISTS silver.lab_customers_type2;
-- MAGIC
-- MAGIC CREATE OR REFRESH STREAMING TABLE silver.lab_customers_type2
-- MAGIC FLOW AUTO CDC
-- MAGIC FROM stream(bronze.customer_changes)
-- MAGIC KEYS (customer_id)
-- MAGIC SEQUENCE BY updated_at
-- MAGIC COLUMNS * EXCEPT (operation)
-- MAGIC STORED AS SCD TYPE 2;</code></pre>
-- MAGIC         <script>function copyE(){const e=document.getElementById("copy-block-e");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbE(t)):fbE(t)}function fbE(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
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

-- Christopher Mitchell in Type 2: should see multiple version rows with __START_AT and __END_AT
SELECT customer_id, first_name, last_name, city, loyalty_tier, email, __START_AT, __END_AT
FROM silver.lab_customers_type2
WHERE customer_id = 1231
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Christopher has <strong>multiple version rows</strong>. Each row has a <code>__START_AT</code> (when that version became active) and <code>__END_AT</code> (when the next version replaced it). The last row has <code>__END_AT = NULL</code> — that is his current state.</p>
-- MAGIC     <p>Look at the <code>email</code> column across versions. Some version transitions were triggered by email or phone changes alone — the city and tier stayed the same. These are the "noisy" versions we will filter out in section G.</p>
-- MAGIC     <p>Also note: the out-of-order event (address change at 2026-01-20) was placed in the correct chronological position by <code>SEQUENCE BY updated_at</code> — it did not create a version at the end of the timeline.</p>
-- MAGIC     <p><strong>Note:</strong> Like section D, this table does not have <code>APPLY AS DELETE WHEN</code> — deleted customers still appear. You will add delete handling in section F.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- Compare row counts: Type 1 vs Type 2
SELECT 'Type 1 (current state)' AS approach, COUNT(*) AS rows FROM silver.lab_customers_type1
UNION ALL
SELECT 'Type 2 (full history)', COUNT(*) FROM silver.lab_customers_type2;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Type 1</strong> has far fewer rows — one per customer (latest state only). Since neither table has <code>APPLY AS DELETE WHEN</code> yet, deleted customers still appear.</li>
-- MAGIC       <li><strong>Type 2</strong> has significantly more rows — multiple versions per customer. Every change to any column (including email, phone, address) creates a new version row.</li>
-- MAGIC     </ul>
-- MAGIC     <p>In section G, we will see how TRACK HISTORY reduces the Type 2 count by filtering out noisy changes.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Add Delete Handling — APPLY AS DELETE WHEN
-- MAGIC
-- MAGIC Sections D and E did not handle DELETE events — deleted customers still appear in those tables. In production, you need to handle deletions. The `APPLY AS DELETE WHEN` clause tells AUTO CDC which rows represent deletions.
-- MAGIC
-- MAGIC **How deletes work by SCD type:**
-- MAGIC - **Type 1:** DELETE events **physically remove** the customer row — gone, no trace
-- MAGIC - **Type 2:** DELETE events **tombstone** all version rows — every row gets a non-NULL `__END_AT`, but history is preserved for audit
-- MAGIC
-- MAGIC **Your task:** Build a Type 1 table WITH delete handling. Two things need filling in:
-- MAGIC 1. `APPLY AS DELETE WHEN` — what condition in the `operation` column means "delete this customer"?
-- MAGIC 2. `STORED AS SCD TYPE` — which type number for current-state-only?

-- COMMAND ----------

-- Fill in the two <FILL_IN> placeholders
-- 1. APPLY AS DELETE WHEN: what condition identifies DELETE events? (Hint: look at the operation column values from section C)
-- 2. STORED AS SCD TYPE: which type number keeps only the current state?

DROP TABLE IF EXISTS silver.lab_customers_type1_deletes;

CREATE OR REFRESH STREAMING TABLE silver.lab_customers_type1_deletes
FLOW AUTO CDC
FROM stream(bronze.customer_changes)
KEYS (customer_id)
<FILL_IN>
SEQUENCE BY updated_at
COLUMNS * EXCEPT (operation)
<FILL_IN>;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyF()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-f" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>DROP TABLE IF EXISTS silver.lab_customers_type1_deletes;
-- MAGIC
-- MAGIC CREATE OR REFRESH STREAMING TABLE silver.lab_customers_type1_deletes
-- MAGIC FLOW AUTO CDC
-- MAGIC FROM stream(bronze.customer_changes)
-- MAGIC KEYS (customer_id)
-- MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
-- MAGIC SEQUENCE BY updated_at
-- MAGIC COLUMNS * EXCEPT (operation)
-- MAGIC STORED AS SCD TYPE 1;</code></pre>
-- MAGIC         <script>function copyF(){const e=document.getElementById("copy-block-f");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbF(t)):fbF(t)}function fbF(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>

-- COMMAND ----------

-- Compare Type 1 WITHOUT vs WITH delete handling
SELECT 'Type 1 (no delete handling)' AS approach, COUNT(*) AS rows FROM silver.lab_customers_type1
UNION ALL
SELECT 'Type 1 (with APPLY AS DELETE)', COUNT(*) FROM silver.lab_customers_type1_deletes;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>The table WITH <code>APPLY AS DELETE WHEN</code> has <strong>fewer rows</strong> — the difference is the number of customers who had DELETE events in the CDC feed. Those customers were physically removed from the Type 1 table with delete handling, but still exist in the version without it.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Observe SCD Type 2 with TRACK HISTORY — Filtered Version History
-- MAGIC
-- MAGIC This is the **key new concept** in this lab — and the most challenging exercise. In section E, you saw that standard Type 2 creates a new version for **every** column change, including email updates, phone corrections, and address fixes. For Christopher Mitchell alone, that produced extra version rows just because he changed his email.
-- MAGIC
-- MAGIC In production dimension tables with millions of customers, this noise adds up fast — inflating storage, slowing queries, and making historical analysis harder to read.
-- MAGIC
-- MAGIC ### The Solution: TRACK HISTORY ON
-- MAGIC
-- MAGIC `TRACK HISTORY ON <columns>` tells the engine: **only create a new version when one of the listed columns changes.** Changes to unlisted columns are still applied — the latest email is always stored in each version row — but they do not *trigger* a new version.
-- MAGIC
-- MAGIC ### One Syntax Addition from Sections D and E
-- MAGIC
-- MAGIC **`TRACK HISTORY ON col1, col2, ...`** — the opt-in column list (goes after `STORED AS SCD TYPE 2`, as the last clause)
-- MAGIC
-- MAGIC ### Choosing Which Columns to Track
-- MAGIC
-- MAGIC Ask yourself: *"If only this column changed, would an analyst care?"*
-- MAGIC
-- MAGIC | Track these (signal) | Why | Don't track these (noise) | Why |
-- MAGIC |---|---|---|---|
-- MAGIC | `first_name`, `last_name` | Name changes are rare but meaningful | `email` | Contact detail — changes frequently, no analytical impact |
-- MAGIC | `city`, `state`, `country` | Geographic changes affect regional analysis | `phone` | Contact detail — corrections, carrier changes |
-- MAGIC | `customer_segment` | Consumer vs corporate affects sales strategy | `address` | Street address — formatting fixes, apartment changes |
-- MAGIC | `loyalty_tier` | Tier changes affect pricing and retention | `zip_code` | Often changes with address — no business meaning alone |
-- MAGIC
-- MAGIC ### Your Task
-- MAGIC
-- MAGIC The SQL below gives you the `TRACK HISTORY ON` line — but the AUTO CDC clauses you learned in sections D-F are `<FILL_IN>`. Write the full `FLOW AUTO CDC` block using what you practiced, then run it.

-- COMMAND ----------

-- Fill in the AUTO CDC clauses — the TRACK HISTORY line is provided for you
-- You need: FLOW AUTO CDC, FROM stream(...), KEYS, APPLY AS DELETE WHEN, SEQUENCE BY, COLUMNS * EXCEPT

DROP TABLE IF EXISTS silver.lab_customers_type2_tracked;

CREATE OR REFRESH STREAMING TABLE silver.lab_customers_type2_tracked
<FILL_IN>
STORED AS SCD TYPE 2
TRACK HISTORY ON first_name, last_name, city, state, country, customer_segment, loyalty_tier;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
-- MAGIC     <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
-- MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
-- MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
-- MAGIC         <strong style="color: #00695c; font-size: 1.1em;">Expand for Help</strong>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </summary>
-- MAGIC   <div style="border-left: 4px solid #009688; background: #f0faf9; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
-- MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
-- MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
-- MAGIC       <div>
-- MAGIC         <button onclick="copyG()">Copy to clipboard</button>
-- MAGIC         <pre id="copy-block-g" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; margin-top:8px;">
-- MAGIC <code>DROP TABLE IF EXISTS silver.lab_customers_type2_tracked;
-- MAGIC
-- MAGIC CREATE OR REFRESH STREAMING TABLE silver.lab_customers_type2_tracked
-- MAGIC FLOW AUTO CDC
-- MAGIC FROM stream(bronze.customer_changes)
-- MAGIC KEYS (customer_id)
-- MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
-- MAGIC SEQUENCE BY updated_at
-- MAGIC COLUMNS * EXCEPT (operation)
-- MAGIC STORED AS SCD TYPE 2
-- MAGIC TRACK HISTORY ON first_name, last_name, city, state, country, customer_segment, loyalty_tier;</code></pre>
-- MAGIC         <script>function copyG(){const e=document.getElementById("copy-block-g");if(!e)return;const t=e.innerText;navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(t).then(()=>alert("Copied to clipboard")).catch(()=>fbG(t)):fbG(t)}function fbG(t){const a=document.createElement("textarea");a.value=t;a.style.position="fixed";a.style.left="-9999px";document.body.appendChild(a);a.select();try{document.execCommand("copy");alert("Copied to clipboard")}catch(e){alert("Copy failed.")}finally{document.body.removeChild(a)}}</script>
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </details>

-- COMMAND ----------

-- Christopher Mitchell in filtered Type 2: fewer version rows
SELECT customer_id, first_name, last_name, city, loyalty_tier, email, __START_AT, __END_AT
FROM silver.lab_customers_type2_tracked
WHERE customer_id = 1231
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">The Key Comparison</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p>Compare Christopher's rows here with section E. You should see <strong>fewer version rows</strong> — the versions triggered by email-only, phone-only, and address-only changes are gone.</p>
-- MAGIC     <p>The key insight: the "missing" versions were administrative noise. They cluttered the history without adding analytical value. But the data is <strong>not lost</strong> — the latest email and phone are still stored in each version row. The change just did not trigger a new version.</p>
-- MAGIC     <p>Run the comparison query below to see the exact difference in version counts.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- Side-by-side version count comparison for Christopher Mitchell
SELECT 'Type 1 (current state)' AS approach, COUNT(*) AS christopher_rows
FROM silver.lab_customers_type1 WHERE customer_id = 1231
UNION ALL
SELECT 'Type 2 (full history)', COUNT(*)
FROM silver.lab_customers_type2 WHERE customer_id = 1231
UNION ALL
SELECT 'Type 2 + TRACK HISTORY', COUNT(*)
FROM silver.lab_customers_type2_tracked WHERE customer_id = 1231;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe — Christopher's Version Counts</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <table style="width:100%; border-collapse:collapse; font-size:0.9em; margin:12px 0;">
-- MAGIC       <tr style="background:#1B3139; color:#fff;"><th style="padding:6px 10px; text-align:left;">Approach</th><th style="padding:6px 10px;">Expected Rows</th><th style="padding:6px 10px;">Why</th></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;">Type 1</td><td style="text-align:center;">1</td><td>Latest state only — all history collapsed</td></tr>
-- MAGIC       <tr><td style="padding:6px 10px;">Type 2</td><td style="text-align:center;">~8</td><td>Every change (including email/phone) creates a version</td></tr>
-- MAGIC       <tr style="background:#f8f8f8;"><td style="padding:6px 10px;">Type 2 + TRACK HISTORY</td><td style="text-align:center;">~4</td><td>Only tier/city/segment changes create versions — noise absorbed</td></tr>
-- MAGIC     </table>
-- MAGIC     <p>If Type 2 and Type 2 + TRACK HISTORY show the <strong>same count</strong>, re-run the course data setup notebook (<code>1 - REQUIRED - Course Data Setup</code>) and then re-run this lab's setup. The curated Christopher events must be in the dataset for the difference to appear.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- Table-wide comparison
SELECT 'Type 1 (current state)' AS approach, COUNT(*) AS total_rows FROM silver.lab_customers_type1
UNION ALL
SELECT 'Type 2 (full history)', COUNT(*) FROM silver.lab_customers_type2
UNION ALL
SELECT 'Type 2 + TRACK HISTORY', COUNT(*) FROM silver.lab_customers_type2_tracked;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe — Table-Wide Counts</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Type 1:</strong> Close to ~2,500 rows (one per customer, minus deletions)</li>
-- MAGIC       <li><strong>Type 2:</strong> The largest — every change across all customers creates a version</li>
-- MAGIC       <li><strong>Type 2 + TRACK HISTORY:</strong> Smaller than Type 2 — the difference is the total noise filtered out across ALL customers</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Load New Data — Incremental Processing
-- MAGIC
-- MAGIC In production, CDC events arrive continuously — not as a single batch. We simulate this by adding **~200 new events** from a second batch. This batch includes more updates, new customers, and DELETE operations.
-- MAGIC
-- MAGIC The key question: **do the CDC tables reprocess everything from scratch, or only the new events?** After inserting the new batch into the source table, we refresh all three CDC tables and compare the counts with G1. If counts change by approximately the number of new events (not the full dataset), that confirms incremental processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### H1. Check Counts Before

-- COMMAND ----------

SELECT 'Source (bronze)' AS table_name, COUNT(*) AS rows FROM bronze.customer_changes
UNION ALL SELECT 'Type 1 (no deletes)', COUNT(*) FROM silver.lab_customers_type1
UNION ALL SELECT 'Type 1 (with deletes)', COUNT(*) FROM silver.lab_customers_type1_deletes
UNION ALL SELECT 'Type 2', COUNT(*) FROM silver.lab_customers_type2
UNION ALL SELECT 'Type 2 + TRACK HISTORY', COUNT(*) FROM silver.lab_customers_type2_tracked;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <p><strong>Note these numbers</strong> — you will compare them after the new batch arrives.</p>
-- MAGIC     <ul>
-- MAGIC       <li><strong>Source:</strong> ~4,000 events (baseline INSERTs + CDC changes)</li>
-- MAGIC       <li><strong>Type 1:</strong> ~2,600 rows — one per customer, minus deletions</li>
-- MAGIC       <li><strong>Type 2:</strong> ~3,800 rows — every change creates a version, including email/phone noise</li>
-- MAGIC       <li><strong>Type 2 + TRACK HISTORY:</strong> ~3,500 rows — <strong>fewer than Type 2</strong> because ~300 noise-only changes (email, phone, address) were absorbed without creating versions</li>
-- MAGIC     </ul>
-- MAGIC     <p>The gap between Type 2 and Type 2 + TRACK HISTORY is the total noise filtered out across all customers — that is the value of TRACK HISTORY.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### H2. Insert New Batch and Refresh
-- MAGIC
-- MAGIC The new batch contains ~200 events. The `data` schema is a read-only reference schema provided by the course setup — it contains pre-staged datasets used throughout the course. After inserting, we refresh all three CDC tables to process the new events incrementally.

-- COMMAND ----------

INSERT INTO bronze.customer_changes
SELECT * FROM data.raw_customers_cdc_new_batch;

-- COMMAND ----------

REFRESH STREAMING TABLE silver.lab_customers_type1;
REFRESH STREAMING TABLE silver.lab_customers_type1_deletes;
REFRESH STREAMING TABLE silver.lab_customers_type2;
REFRESH STREAMING TABLE silver.lab_customers_type2_tracked;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### H3. Check Counts After

-- COMMAND ----------

-- Compare with H1 — source grew by ~200, each CDC table processed the new events
SELECT 'Source (bronze)' AS table_name, COUNT(*) AS rows FROM bronze.customer_changes
UNION ALL SELECT 'Type 1 (no deletes)', COUNT(*) FROM silver.lab_customers_type1
UNION ALL SELECT 'Type 1 (with deletes)', COUNT(*) FROM silver.lab_customers_type1_deletes
UNION ALL SELECT 'Type 2', COUNT(*) FROM silver.lab_customers_type2
UNION ALL SELECT 'Type 2 + TRACK HISTORY', COUNT(*) FROM silver.lab_customers_type2_tracked;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976d2; background: #f0f7ff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Source:</strong> Grew by ~200 events (the new batch)</li>
-- MAGIC       <li><strong>Type 1:</strong> Row count may have decreased slightly — DELETE events in the new batch removed some customers</li>
-- MAGIC       <li><strong>Type 2:</strong> Grew the most — every new event creates version rows</li>
-- MAGIC       <li><strong>Type 2 + TRACK HISTORY:</strong> Grew less than unfiltered Type 2 — email/phone-only changes in the new batch were absorbed without creating new versions</li>
-- MAGIC     </ul>
-- MAGIC     <p>All three tables processed only the ~200 new events — not the full dataset. This is incremental CDC processing in action.</p>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## I. Key Summary and Takeaways
-- MAGIC
-- MAGIC In this lab, you explored four CDC dimension tables from the same source — studying one worked example and building three yourself — with progressive complexity:
-- MAGIC
-- MAGIC | What You Did | Key Concept |
-- MAGIC |---|---|
-- MAGIC | Explored Christopher Mitchell's CDC events | Not all changes are equal — **email changes are noise, tier/city changes are signal** |
-- MAGIC | **D.** Ran SCD Type 1 (given example) | **One row per customer** — latest state only, no delete handling |
-- MAGIC | **E.** Built SCD Type 2 (changed SCD TYPE) | **Full version history** — every change creates a new row with `__START_AT` / `__END_AT` timestamps |
-- MAGIC | **F.** Added APPLY AS DELETE WHEN | **Delete handling** — Type 1 physically removes deleted customers, Type 2 tombstones them |
-- MAGIC | **G.** Observed TRACK HISTORY ON (new syntax) | **Filtered history** — only business-meaningful changes create versions. Noise is absorbed, not lost. |
-- MAGIC | Compared version counts across all four | TRACK HISTORY had **fewer rows** than unfiltered Type 2 — cleaner history without losing data |
-- MAGIC | Loaded a new batch and refreshed | **Incremental CDC processing** — all four tables processed only the new events |
-- MAGIC
-- MAGIC **What comes next:** In the next lab, you will use the CDC dimension table to replace the static `data.raw_customers` snapshot in the silver and gold layers — connecting the CDC pipeline to the rest of the medallion architecture.

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
