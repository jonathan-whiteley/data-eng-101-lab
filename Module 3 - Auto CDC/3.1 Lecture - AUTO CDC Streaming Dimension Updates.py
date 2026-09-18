# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](../Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.1 Lecture - AUTO CDC Streaming Dimension Updates
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Maintaining SCD Type 1 and Type 2 dimensions with manual MERGE INTO is one of the most error-prone patterns in data engineering. This lecture introduces AUTO CDC -- a declarative SQL feature that replaces 100-150 lines of procedural MERGE logic with 7 declarative clauses.
# MAGIC
# MAGIC Manual `MERGE INTO` for SCD Type 1 and Type 2 requires 100-150 lines of procedural SQL per dimension table -- insert/update/delete logic, sequencing, deduplication, and history tracking. Out-of-order events and soft deletes multiply the complexity further. **AUTO CDC** replaces all of this with a declarative approach: define your keys, sequence column, and history tracking preferences, and the platform handles the rest with correctness guarantees out of the box.
# MAGIC
# MAGIC AUTO CDC is currently **Beta** in Lakehouse SQL and **GA** in Lakeflow Spark Declarative Pipelines (SDP).
# MAGIC
# MAGIC This lecture covers four sections that build on each other:
# MAGIC
# MAGIC - **A. The MERGE INTO Problem** -- why manual SCD maintenance snowballs into 150 lines of fragile SQL
# MAGIC - **B. AUTO CDC for SCD Type 1 and Type 2** -- Type 1 vs Type 2 introduction, KEYS and SEQUENCE BY, and TRACK HISTORY ON
# MAGIC - **C. Handling Deletes** -- `APPLY AS DELETE` for soft deletes in SCD Type 1 and Type 2
# MAGIC - **D. Monitoring** -- inspecting AUTO CDC pipeline metrics via `event_log()`
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain why manual MERGE INTO for SCD is complex, error-prone, and hard to maintain
# MAGIC - Create an AUTO CDC Streaming Table for SCD Type 1 (current state) using KEYS and SEQUENCE BY
# MAGIC - Create an AUTO CDC Streaming Table for SCD Type 2 (full history)
# MAGIC - Use APPLY AS DELETE to handle soft deletes in CDC flows
# MAGIC - Inspect AUTO CDC pipeline metrics via `event_log()`: upserted rows, deleted rows, and pipeline state
# MAGIC - Describe incremental REPLACE WHERE as a coming solution for scoped fact table refresh (awareness)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Why Dimension Management Is a Business Decision

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Three Requirements That Drive Your SCD Strategy
# MAGIC
# MAGIC Every dimension management decision starts with the same three business questions. The answers determine whether you need SCD Type 1, Type 2, or a hybrid &mdash; before you write a single line of SQL.
# MAGIC
# MAGIC <!-- ── Visual: A1 Business Requirements Decision Tree ── -->
# MAGIC <style>
# MAGIC .a1b-wrap { max-width:1100px; margin:0 auto; font-family:sans-serif; color:#0b2026; }
# MAGIC .a1b-scenario { background:#F9F7F4; border-radius:12px; padding:20px 24px; margin-bottom:20px; border:2px solid #EEEDE9; }
# MAGIC .a1b-scenario-title { font-size:16pt; font-weight:700; color:#1B3139; margin-bottom:10px; }
# MAGIC .a1b-scenario-text { font-size:14pt; color:#444; line-height:1.7; }
# MAGIC .a1b-reqs { display:flex; gap:16px; margin:20px 0; }
# MAGIC .a1b-req { flex:1; border-radius:10px; padding:18px 20px; border-top:6px solid transparent; }
# MAGIC .a1b-req-icon { font-size:28pt; margin-bottom:8px; text-align:center; }
# MAGIC .a1b-req-title { font-size:15pt; font-weight:700; text-align:center; margin-bottom:8px; }
# MAGIC .a1b-req-text { font-size:13pt; color:#444; line-height:1.6; }
# MAGIC .a1b-tree { background:#fff; border-radius:12px; border:2px solid #EEEDE9; padding:24px; margin-top:20px; }
# MAGIC .a1b-tree-title { font-size:16pt; font-weight:700; color:#1B3139; margin-bottom:16px; text-align:center; }
# MAGIC .a1b-tree-flow { display:flex; flex-direction:column; align-items:center; gap:0; }
# MAGIC .a1b-tree-q { background:#1B5162; color:white; border-radius:10px; padding:14px 24px; font-size:14pt; font-weight:600; text-align:center; max-width:500px; }
# MAGIC .a1b-tree-arrow { font-size:16pt; color:#618794; padding:4px 0; display:flex; gap:80px; justify-content:center; }
# MAGIC .a1b-tree-branch { display:flex; gap:24px; justify-content:center; width:100%; }
# MAGIC .a1b-tree-path { display:flex; flex-direction:column; align-items:center; gap:0; flex:1; max-width:340px; }
# MAGIC .a1b-tree-label { font-size:13pt; font-weight:700; padding:4px 12px; border-radius:6px; }
# MAGIC .a1b-tree-answer { border-radius:10px; padding:14px 18px; font-size:14pt; font-weight:600; text-align:center; width:100%; box-sizing:border-box; }
# MAGIC .a1b-tree-sub { font-size:12pt; color:#618794; text-align:center; margin-top:4px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="a1b-wrap">
# MAGIC   <!-- Business Scenario -->
# MAGIC   <div class="a1b-scenario">
# MAGIC     <div class="a1b-scenario-title">&#x1F3E2; The Scenario</div>
# MAGIC     <div class="a1b-scenario-text">
# MAGIC       Your customer dimension changed <strong>50,000 addresses</strong> last month. Three teams need different things from this data:
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Three Requirements -->
# MAGIC   <div class="a1b-reqs">
# MAGIC     <div class="a1b-req" style="background:rgba(37,116,181,0.06); border-top-color:#2574B5;">
# MAGIC       <div class="a1b-req-icon">&#x1F4CA;</div>
# MAGIC       <div class="a1b-req-title" style="color:#2574B5;">Point-in-Time Accuracy</div>
# MAGIC       <div class="a1b-req-text"><strong>Finance:</strong> "Which region was the customer in when they placed order #12345?" &mdash; Revenue attribution depends on the customer's state <em>at the time of the transaction</em>, not their current address.</div>
# MAGIC     </div>
# MAGIC     <div class="a1b-req" style="background:rgba(155,89,182,0.06); border-top-color:#9B59B6;">
# MAGIC       <div class="a1b-req-icon">&#x1F512;</div>
# MAGIC       <div class="a1b-req-title" style="color:#9B59B6;">Audit Compliance</div>
# MAGIC       <div class="a1b-req-text"><strong>Compliance:</strong> "Show me a 7-year trail of every change to this customer record." &mdash; SOX, HIPAA, and financial regulations require provable audit trails with timestamps.</div>
# MAGIC     </div>
# MAGIC     <div class="a1b-req" style="background:rgba(0,169,114,0.06); border-top-color:#00A972;">
# MAGIC       <div class="a1b-req-icon">&#x26A1;</div>
# MAGIC       <div class="a1b-req-title" style="color:#00A972;">Query Performance</div>
# MAGIC       <div class="a1b-req-text"><strong>BI Team:</strong> "Current-state dashboards must load in under 2 seconds." &mdash; SCD Type 2 tables are larger and slower to scan. Not every use case needs history.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Decision Tree -->
# MAGIC   <div class="a1b-tree">
# MAGIC     <div class="a1b-tree-title">Business Requirements Decision Tree</div>
# MAGIC     <div class="a1b-tree-flow">
# MAGIC       <div class="a1b-tree-q">Does the business need to answer questions about <u>historical state</u>?</div>
# MAGIC       <div class="a1b-tree-arrow"><span>&#x2B07; No</span><span>&#x2B07; Yes</span></div>
# MAGIC       <div class="a1b-tree-branch">
# MAGIC         <div class="a1b-tree-path">
# MAGIC           <div class="a1b-tree-label" style="background:#00A972; color:white;">NO &mdash; Current state only</div>
# MAGIC           <div style="font-size:14pt; color:#618794; padding:4px 0;">&#x2B07;</div>
# MAGIC           <div class="a1b-tree-answer" style="background:rgba(0,169,114,0.08); border:2px solid #00A972; color:#00A972;">SCD Type 1</div>
# MAGIC           <div class="a1b-tree-sub">Product catalogs, preferences, employee directory</div>
# MAGIC         </div>
# MAGIC         <div class="a1b-tree-path">
# MAGIC           <div class="a1b-tree-label" style="background:#2574B5; color:white;">YES &mdash; but only for specific columns</div>
# MAGIC           <div style="font-size:14pt; color:#618794; padding:4px 0;">&#x2B07;</div>
# MAGIC           <div class="a1b-tree-answer" style="background:rgba(37,116,181,0.08); border:2px solid #2574B5; color:#2574B5;">SCD Type 2 + TRACK HISTORY ON</div>
# MAGIC           <div class="a1b-tree-sub">Track region/tier changes, ignore address/phone</div>
# MAGIC         </div>
# MAGIC         <div class="a1b-tree-path">
# MAGIC           <div class="a1b-tree-label" style="background:#9B59B6; color:white;">YES &mdash; for all columns</div>
# MAGIC           <div style="font-size:14pt; color:#618794; padding:4px 0;">&#x2B07;</div>
# MAGIC           <div class="a1b-tree-answer" style="background:rgba(155,89,182,0.08); border:2px solid #9B59B6; color:#9B59B6;">SCD Type 2 (full history)</div>
# MAGIC           <div class="a1b-tree-sub">SOX audit trails, HIPAA data retention, full lineage</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
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
# MAGIC         <strong style="color: #00695c;">Point-in-Time Accuracy</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Revenue attribution</strong>: when you join fact_orders to dim_customers, which version of the customer record do you use? If the customer moved from Region A to Region B mid-quarter, the answer determines which regional sales team gets credit</li>
# MAGIC           <li><strong>SCD Type 1 cannot answer this</strong> &mdash; it only knows the customer's <em>current</em> region. By the time finance runs the Q3 report, the customer's address reflects Q4</li>
# MAGIC           <li><strong>SCD Type 2 solves this</strong> with <code>__START_AT</code> / <code>__END_AT</code> ranges that let you join: <code>WHERE f.order_date BETWEEN d.__START_AT AND COALESCE(d.__END_AT, current_timestamp())</code></li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Audit Compliance Requirements</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SOX (Sarbanes-Oxley)</strong>: requires knowing who approved what, when &mdash; if a customer's credit limit changed, auditors need the full chain of values with timestamps</li>
# MAGIC           <li><strong>HIPAA</strong>: healthcare data retention policies require maintaining historical records of patient demographic changes for specified retention periods</li>
# MAGIC           <li><strong>GDPR Article 17 (Right to Erasure)</strong>: interestingly, this regulation <em>conflicts</em> with audit trails &mdash; when a customer requests deletion, you must remove their data, which means SCD Type 2 history preservation can create compliance tension (more on this in Section D)</li>
# MAGIC           <li><strong>Financial services</strong>: 7-year record retention is standard; SCD Type 2 provides this out of the box with no additional instrumentation</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Query Performance Tradeoffs</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SCD Type 1</strong>: one row per key &mdash; fastest scans, smallest storage footprint, simplest joins</li>
# MAGIC           <li><strong>SCD Type 2</strong>: N rows per key (where N = number of changes) &mdash; table size grows with change velocity, point-in-time joins require range predicates</li>
# MAGIC           <li><strong>The middle ground</strong>: SCD Type 2 + <code>TRACK HISTORY ON</code> limits version creation to analytically meaningful columns, keeping table growth closer to Type 1 while preserving the history that matters</li>
# MAGIC           <li><strong>Best practice</strong>: create a view <code>dim_customers_current</code> wrapping <code>WHERE __END_AT IS NULL</code> for downstream consumers who only need current state &mdash; they get Type 1 performance from a Type 2 table</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">The Real-World Decision Process</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>In practice, most organizations use <strong>both SCD types</strong> across different dimension tables &mdash; product catalog (Type 1), customer demographics (Type 2 with selective tracking), employee directory (Type 1)</li>
# MAGIC           <li>The decision is per-table and per-column, not per-organization</li>
# MAGIC           <li><strong>When in doubt, start with Type 1</strong> &mdash; you can switch to Type 2 later by changing one word. The reverse (Type 2 to Type 1) <em>loses history permanently</em></li>
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
# MAGIC ### A2. Why You Need a Better Tool Than MERGE INTO
# MAGIC
# MAGIC SCD maintenance with `MERGE INTO` starts deceptively simple &mdash; a single statement to upsert a dimension table &mdash; and snowballs into 100&ndash;150 lines of procedural SQL that is brittle, untestable, and team-knowledge-dependent. Every additional requirement adds a new block to the tower.
# MAGIC
# MAGIC <!-- ── Visual: A1 Tabbed Explorer ── -->
# MAGIC <style>
# MAGIC .a1t-wrap { max-width:1100px; margin:0 auto; font-family:sans-serif; color:#0b2026; }
# MAGIC .a1t-layout { display:flex; flex-direction:column; gap:0; }
# MAGIC .a1t-tabs { display:flex; gap:8px; }
# MAGIC .a1t-tab {
# MAGIC   flex:1; padding:14px 18px; cursor:pointer; user-select:none;
# MAGIC   font-size:15pt; font-weight:700; color:#0b2026; text-align:center;
# MAGIC   background:#F9F7F4; border:2px solid #EEEDE9; border-bottom:none;
# MAGIC   border-top:6px solid transparent; border-radius:10px 10px 0 0;
# MAGIC   transition: background 0.2s, border-color 0.2s;
# MAGIC   display:flex; align-items:center; justify-content:center; gap:10px;
# MAGIC }
# MAGIC .a1t-tab:hover { background:#f0edea; }
# MAGIC .a1t-tab.active { background:#fff; border-left-color:#ddd; border-right-color:#ddd; }
# MAGIC .a1t-tab[data-a1t="0"] { border-top-color:#E24B4A; }
# MAGIC .a1t-tab[data-a1t="1"] { border-top-color:#F8A805; }
# MAGIC .a1t-tab-icon { height:36px; width:36px; object-fit:contain; }
# MAGIC .a1t-tab-label { line-height:1.3; }
# MAGIC .a1t-content { border:2px solid #ddd; border-top:none; border-radius:0 0 10px 10px; background:#fff; padding:20px 24px; overflow-y:auto; }
# MAGIC .a1t-panel { display:none; }
# MAGIC .a1t-panel.active { display:block; }
# MAGIC /* Complexity panel */
# MAGIC .a1t-snow-flex { display:flex; gap:14px; align-items:flex-start; }
# MAGIC .a1t-snow-col { flex:1; }
# MAGIC .a1t-snow-label { font-size:15pt; font-weight:700; margin-bottom:10px; }
# MAGIC .a1t-snow-code { border-radius:8px; padding:14px; font-family:monospace; font-size:14pt; line-height:1.4; }
# MAGIC .a1t-snow-arrow { display:flex; align-items:center; justify-content:center; font-size:28pt; color:#618794; flex-shrink:0; padding-top:40px; }
# MAGIC .a1t-note { margin-top:16px; padding:12px 18px; background:#F8F9FC; border:2px solid #4299E0; border-radius:8px; font-size:14pt; }
# MAGIC /* Silent failures panel */
# MAGIC .a1t-fail-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
# MAGIC .a1t-fail-card { background:#fff; border-radius:10px; padding:20px; border:2px solid #EEEDE9; transition:border-color 0.2s; }
# MAGIC .a1t-fail-card:hover { border-color:#bbb; }
# MAGIC .a1t-fail-hdr { display:flex; align-items:center; gap:12px; margin-bottom:12px; padding-bottom:12px; }
# MAGIC .a1t-fail-icon { height:40px; width:40px; object-fit:contain; }
# MAGIC .a1t-fail-title { font-size:15pt; font-weight:700; }
# MAGIC .a1t-fail-list { margin:0; padding-left:20px; font-size:14pt; line-height:1.7; color:#444; }
# MAGIC .a1t-fail-list li { margin-bottom:4px; }
# MAGIC .a1t-fail-list strong { color:#0b2026; }
# MAGIC .a1t-fail-list code { background:#f5f3f0; padding:1px 5px; border-radius:3px; font-size:13pt; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="a1t-wrap">
# MAGIC <div class="a1t-layout">
# MAGIC   <!-- Tabs -->
# MAGIC   <div class="a1t-tabs">
# MAGIC     <div class="a1t-tab active" data-a1t="0" onclick="a1tSelect(0)">
# MAGIC       <img class="a1t-tab-icon" src="../Includes/images/icons/massive-scale-data-eng.png" alt=""/>
# MAGIC       <span class="a1t-tab-label">Complexity Snowball</span>
# MAGIC     </div>
# MAGIC     <div class="a1t-tab" data-a1t="1" onclick="a1tSelect(1)">
# MAGIC       <img class="a1t-tab-icon" src="../Includes/images/icons/business-risk.png" alt=""/>
# MAGIC       <span class="a1t-tab-label">Silent Failures</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Content -->
# MAGIC   <div class="a1t-content">
# MAGIC     <!-- Panel 0: Complexity -->
# MAGIC     <div class="a1t-panel active" id="a1t-p0">
# MAGIC       <div style="font-size:18pt;font-weight:700;color:#0b2026;margin-bottom:4px;">The MERGE INTO Complexity Snowball</div>
# MAGIC       <div style="font-size:14pt;color:#5A6F77;margin-bottom:12px;">Each new CDC requirement adds 15&ndash;60 lines of procedural SQL to an already fragile MERGE block</div>
# MAGIC       <div class="a1t-snow-flex">
# MAGIC         <div class="a1t-snow-col">
# MAGIC           <div class="a1t-snow-label" style="color:#98102A;"><img src="../Includes/images/icons/x-circle-icon.png" height="18" style="vertical-align:middle;"> Procedural MERGE INTO</div>
# MAGIC           <div class="a1t-snow-code" style="background:#FFF6F4;border:2px solid rgba(152,16,42,0.2);">
# MAGIC             <strong>SCD Type 1 basic version</strong><br/>
# MAGIC             MERGE INTO dim_customers t<br/>
# MAGIC             USING source s ON t.customer_id = s.customer_id<br/>
# MAGIC             WHEN MATCHED THEN UPDATE SET ...<br/>
# MAGIC             WHEN NOT MATCHED THEN INSERT ...<br/>
# MAGIC             <em style="color:#98102A;">~20 lines</em><br/>
# MAGIC             <strong>+ Deduplication CTE (ROW_NUMBER)</strong> <em style="color:#98102A;">+15 lines</em><br/>
# MAGIC             <strong>+ Event ordering (watermark tracking)</strong> <em style="color:#98102A;">+25 lines</em><br/>
# MAGIC             <strong>+ Soft delete handling</strong> <em style="color:#98102A;">+30 lines</em><br/>
# MAGIC             <strong>+ SCD Type 2 history (MERGE + INSERT)</strong> <em style="color:#98102A;">+60 lines</em><br/>
# MAGIC             <div style="border-top:2px solid #98102A;margin-top:6px;padding-top:6px;font-weight:700;color:#98102A;font-size:15pt;">= 150 lines total</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="a1t-snow-arrow">&#x27A1;</div>
# MAGIC         <div class="a1t-snow-col">
# MAGIC           <div class="a1t-snow-label" style="color:#00A972;"><img src="../Includes/images/icons/checkmark-success.png" height="18" style="vertical-align:middle;"> Declarative AUTO CDC</div>
# MAGIC           <div class="a1t-snow-code" style="background:rgba(0,169,114,0.04);border:2px solid rgba(0,169,114,0.2);">
# MAGIC             CREATE OR REFRESH STREAMING TABLE dim_customers<br/>
# MAGIC             FLOW AUTO CDC<br/>
# MAGIC             FROM stream(bronze_customers_cdc)<br/>
# MAGIC             KEYS (customer_id)<br/>
# MAGIC             APPLY AS DELETE WHEN operation = 'DELETE'<br/>
# MAGIC             SEQUENCE BY updated_at<br/>
# MAGIC             COLUMNS * EXCEPT (operation, _metadata)<br/>
# MAGIC             STORED AS SCD TYPE 2;<br/><br/>
# MAGIC             <em style="color:#00A972;">Dedup, ordering, deletes &amp; history: handled automatically</em>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="a1t-note" style="margin-top:12px;"><img src="../Includes/images/icons/lightbulb-icon.png" height="18" style="vertical-align: middle;"> <strong>The Jenga tower:</strong> each MERGE requirement is a block added near the top. No one wants to pull a block out later &mdash; "it's working, don't touch it" becomes the team culture.</div>
# MAGIC     </div>
# MAGIC     <!-- Panel 1: Silent Failures -->
# MAGIC     <div class="a1t-panel" id="a1t-p1">
# MAGIC       <div style="font-size:18pt;font-weight:700;color:#0b2026;margin-bottom:4px;">Where MERGE Fails Silently</div>
# MAGIC       <div style="font-size:14pt;color:#5A6F77;margin-bottom:16px;">These bugs don't throw errors &mdash; they surface as wrong data weeks later</div>
# MAGIC       <div class="a1t-fail-grid">
# MAGIC         <!-- Card 1: Silent Duplicates -->
# MAGIC         <div class="a1t-fail-card">
# MAGIC           <div class="a1t-fail-hdr" style="border-bottom:2px solid #F8A805;">
# MAGIC             <img class="a1t-fail-icon" src="../Includes/images/icons/copy.png"/>
# MAGIC             <div class="a1t-fail-title" style="color:#C48600;">Silent Duplicate Processing</div>
# MAGIC           </div>
# MAGIC           <ul class="a1t-fail-list">
# MAGIC             <li>CDC sources routinely emit the <strong>same event twice</strong> &mdash; at-least-once delivery is the norm</li>
# MAGIC             <li>Without a <code>ROW_NUMBER</code> dedup CTE, MERGE applies the change multiple times</li>
# MAGIC             <li><strong>Result:</strong> inflated counts, double revenue &mdash; <em>and the job shows green</em></li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC         <!-- Card 2: Atomicity Gap -->
# MAGIC         <div class="a1t-fail-card">
# MAGIC           <div class="a1t-fail-hdr" style="border-bottom:2px solid #9B59B6;">
# MAGIC             <img class="a1t-fail-icon" src="../Includes/images/icons/broken-grid.png"/>
# MAGIC             <div class="a1t-fail-title" style="color:#7D3C98;">SCD Type 2 Atomicity Gap</div>
# MAGIC           </div>
# MAGIC           <ul class="a1t-fail-list">
# MAGIC             <li>MERGE can update the existing row but <strong>cannot simultaneously insert</strong> the new version</li>
# MAGIC             <li>The two-step pattern (<code>MERGE</code> then <code>INSERT</code>) creates a window where <strong>history is incomplete</strong></li>
# MAGIC             <li>Queries hitting the table mid-pipeline see <em>partial state</em></li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC         <!-- Card 3: Out-of-Order -->
# MAGIC         <div class="a1t-fail-card">
# MAGIC           <div class="a1t-fail-hdr" style="border-bottom:2px solid #E24B4A;">
# MAGIC             <img class="a1t-fail-icon" src="../Includes/images/icons/data-quality.png"/>
# MAGIC             <div class="a1t-fail-title" style="color:#E24B4A;">Out-of-Order Overwrites</div>
# MAGIC           </div>
# MAGIC           <ul class="a1t-fail-list">
# MAGIC             <li>Without explicit sequence tracking, a <strong>late-arriving event</strong> with an older timestamp overwrites the newer one</li>
# MAGIC             <li>Happens when events arrive in a subsequent batch out of order</li>
# MAGIC             <li><strong>Result:</strong> no warning, no error &mdash; <em>just wrong data</em></li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC         <!-- Card 4: Watermark Drift -->
# MAGIC         <div class="a1t-fail-card">
# MAGIC           <div class="a1t-fail-hdr" style="border-bottom:2px solid #1B5162;">
# MAGIC             <img class="a1t-fail-icon" src="../Includes/images/icons/event-log-rollover.png"/>
# MAGIC             <div class="a1t-fail-title" style="color:#1B5162;">Watermark Drift</div>
# MAGIC           </div>
# MAGIC           <ul class="a1t-fail-list">
# MAGIC             <li>Teams track <code>last_processed_sequence</code> in a <strong>separate metadata table</strong></li>
# MAGIC             <li>Must be updated <strong>transactionally</strong> with the MERGE &mdash; another failure point</li>
# MAGIC             <li><strong>If watermark fails:</strong> duplicate processing. <strong>If MERGE fails:</strong> <em>data gap</em></li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function a1tSelect(id) {
# MAGIC   document.querySelectorAll('.a1t-tab').forEach(function(t){ t.classList.toggle('active', parseInt(t.dataset.a1t)===id); });
# MAGIC   document.querySelectorAll('.a1t-panel').forEach(function(p,i){ p.classList.toggle('active', i===id); });
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
# MAGIC         <strong style="color: #00695c;">The Complexity Snowball</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>SCD Type 1 alone is roughly <strong>~20 lines</strong> of MERGE SQL &mdash; match on key, update on match, insert on no-match (with full column lists)</li>
# MAGIC           <li>Add <strong>deduplication</strong> for CDC sources that emit the same event twice: <strong>+15 lines</strong> of ROW_NUMBER window function logic in a CTE</li>
# MAGIC           <li>Add <strong>event ordering</strong> and watermark tracking to ensure the latest event wins: <strong>+25 lines</strong> for reading/updating a metadata table and filtering late arrivals</li>
# MAGIC           <li>Add <strong>soft delete handling</strong> (operation = 'DELETE'): <strong>+30 lines</strong> for conditional WHEN MATCHED THEN DELETE branching and tombstone logic</li>
# MAGIC           <li>Add <strong>SCD Type 2 history tracking</strong>: <strong>+60 lines</strong> &mdash; MERGE cannot INSERT and expire a row atomically, so you need a separate INSERT after each MERGE, plus versioning CTEs and start/end date management</li>
# MAGIC           <li>Think of it like a Jenga tower: each requirement is a block added near the top. The tower still stands, but no one wants to pull a block out later</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Where MERGE Fails Silently</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Silent duplicate processing</strong>: CDC sources use at-least-once delivery, so the same event can arrive twice. Without a <code>ROW_NUMBER</code> deduplication CTE, MERGE applies the change multiple times &mdash; inflating counts and revenue while the job shows green. This is the most common silent failure in production CDC pipelines</li>
# MAGIC           <li><strong>SCD Type 2 atomicity gap</strong>: MERGE can match and update the existing row (close it by setting <code>__END_AT</code>), but it cannot simultaneously insert the new version in the same statement. The two-step pattern (MERGE then INSERT) introduces a window where history is incomplete &mdash; queries hitting the table mid-pipeline see partial state</li>
# MAGIC           <li><strong>Out-of-order overwrites</strong>: without explicit sequence tracking, a late-arriving event with an older timestamp will overwrite a newer one if it arrives in a subsequent batch. No warning, no error &mdash; just wrong data. This is especially common with distributed CDC sources where partitions drain at different rates</li>
# MAGIC           <li><strong>Watermark drift</strong>: teams track <code>last_processed_sequence</code> in a separate metadata table, which must be updated transactionally with the MERGE. If the watermark update fails, you get duplicate processing on the next run. If the MERGE fails after the watermark advances, you get a data gap. Both failure modes are silent</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">The Maintenance Trap</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Teams become <strong>reluctant to touch</strong> fragile MERGE pipelines &mdash; "it's working, don't change it" becomes the team culture</li>
# MAGIC           <li>New requirements (add a column, change a key, handle a new delete signal) require re-validating the entire 150-line block</li>
# MAGIC           <li>Technical debt accumulates: workarounds are layered on top of workarounds until the original author is the only person who understands it</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/block" target="_blank">Block</a>: "time to define and develop a streaming pipeline went from <strong>days to hours</strong>" after moving to declarative pipelines &mdash; the maintenance burden was the primary driver &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Procedural vs Declarative: The Core Shift</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>With MERGE, you declare <strong>HOW</strong> to process data &mdash; the algorithm is your responsibility. With AUTO CDC, you declare <strong>WHAT</strong> you want &mdash; keys, sequence, SCD type &mdash; and the engine handles the HOW</li>
# MAGIC           <li>Think of it like automatic versus manual transmission: both get you where you're going, but automatic handles the gear changes so you can focus on the road</li>
# MAGIC           <li>Declarative SQL is also <strong>testable</strong>: you can verify correctness by checking the output table state, not by reading 150 lines of procedural logic</li>
# MAGIC           <li>Changes to requirements (new key column, different sequence field) require changing one clause, not refactoring the entire MERGE block</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">What AUTO CDC Replaces</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Deduplication</strong>: multiple events for the same key in the same batch are automatically resolved &mdash; the highest <code>SEQUENCE BY</code> value wins, no window function CTE needed</li>
# MAGIC           <li><strong>Sequencing and ordering</strong>: the engine guarantees that a late-arriving record with a lower sequence value will never overwrite a higher one, regardless of batch arrival order</li>
# MAGIC           <li><strong>Delete handling</strong>: <code>APPLY AS DELETE WHEN operation = 'DELETE'</code> &mdash; one clause replaces 30 lines of conditional MERGE branching</li>
# MAGIC           <li><strong>SCD Type 2 versioning</strong>: change one word (<code>SCD TYPE 1</code> &rarr; <code>SCD TYPE 2</code>) and the engine automatically manages <code>__START_AT</code>, <code>__END_AT</code>, tombstones, and current-record tracking</li>
# MAGIC           <li><strong>Watermark tracking</strong>: the streaming checkpoint is managed automatically &mdash; no metadata table, no last-processed-sequence bookkeeping</li>
# MAGIC           <li>In total, 5 clauses replace 5 separate concerns: <code>KEYS</code>, <code>SEQUENCE BY</code>, <code>APPLY AS DELETE</code>, <code>STORED AS SCD TYPE</code>, <code>COLUMNS</code></li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Syntax Reference</strong>
# MAGIC         <pre style="background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 14pt; line-height: 1.7; overflow-x: auto; margin: 8px 0 0 0;"><code>CREATE OR REFRESH STREAMING TABLE dim_customers
# MAGIC FLOW AUTO CDC
# MAGIC FROM stream(bronze_customers_cdc)
# MAGIC KEYS (customer_id)
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC SEQUENCE BY updated_at
# MAGIC COLUMNS * EXCEPT (operation, _metadata)
# MAGIC STORED AS SCD TYPE 2;</code></pre>
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
# MAGIC ## B. AUTO CDC for SCD Type 1 and Type 2

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. SCD Type 1 vs Type 2
# MAGIC
# MAGIC AUTO CDC supports two SCD patterns that serve different analytical needs:
# MAGIC
# MAGIC - **Type 1** &mdash; keeps only the <u>current state</u>; updates overwrite in place
# MAGIC - **Type 2** &mdash; preserves <u>complete version history</u>; each change creates a new row with `__START_AT` and `__END_AT` timestamps
# MAGIC
# MAGIC The syntax difference is exactly one word.
# MAGIC
# MAGIC <!-- ── Visual: two-pane-comparison ── -->
# MAGIC <style>
# MAGIC .c1tpc-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0 auto 24px auto; }
# MAGIC .c1tpc-sides { display: flex; justify-content: center; align-items: stretch; gap: 20px; }
# MAGIC .c1tpc-box { flex: 1; max-width: 520px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); display: flex; flex-direction: column; gap: 12px; padding: 22px; position: relative; box-sizing: border-box; }
# MAGIC .c1tpc-box::before { content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 8px; border-radius: 8px 8px 0 0; }
# MAGIC .c1tpc-box.left::before { background: #02A36F; }
# MAGIC .c1tpc-box.right::before { background: #2574B5; }
# MAGIC .c1tpc-icon { text-align: center; margin: 4px 0 6px 0; }
# MAGIC .c1tpc-icon img { height: 72px; width: 72px; object-fit: contain; }
# MAGIC .c1tpc-title { font-size: 18pt; font-weight: 700; color: #0b2026; text-align: center; margin-top: 4px; }
# MAGIC .c1tpc-subtitle { font-size: 14pt; color: #5A6F77; text-align: center; margin-bottom: 8px; }
# MAGIC .c1tpc-text { font-size: 14pt; color: #0b2026; line-height: 1.6; }
# MAGIC .c1tpc-text ul { margin: 0; padding-left: 18px; }
# MAGIC .c1tpc-text li { margin-bottom: 10px; }
# MAGIC .c1tpc-text li:last-child { margin-bottom: 0; }
# MAGIC </style>
# MAGIC <div class="c1tpc-wrap">
# MAGIC   <div class="c1tpc-sides">
# MAGIC     <div class="c1tpc-box left">
# MAGIC       <div class="c1tpc-icon"><img src="../Includes/images/icons/simple.png"/></div>
# MAGIC       <div class="c1tpc-title">SCD Type 1 &mdash; Current State</div>
# MAGIC       <div class="c1tpc-text">
# MAGIC         <ul>
# MAGIC           <li><strong>Keeps only current values</strong> &mdash; previous values are <u>overwritten</u> on update</li>
# MAGIC           <li><strong>One row</strong> <u>per business key</u> at all times</li>
# MAGIC           <li><strong>Cannot answer:</strong> <em>"What was this customer's country in June 2024?"</em></li>
# MAGIC           <li><strong>Syntax:</strong> <code>STORED AS SCD TYPE 1</code> (or omit &mdash; <u>default</u>)</li>
# MAGIC           <li><strong>Use when:</strong> current address, current price &mdash; no audit trail needed</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="c1tpc-box right">
# MAGIC       <div class="c1tpc-icon"><img src="../Includes/images/icons/data-lineage.png"/></div>
# MAGIC       <div class="c1tpc-title">SCD Type 2 &mdash; Full History</div>
# MAGIC       <div class="c1tpc-text">
# MAGIC         <ul>
# MAGIC           <li><strong>Preserves every version</strong> &mdash; each change creates a <u>new row</u></li>
# MAGIC           <li><strong>Multiple rows</strong> <u>per key</u>
# MAGIC             <ul><li><code>__END_AT IS NULL</code> = current version</li></ul>
# MAGIC           </li>
# MAGIC           <li><strong>Syntax:</strong> change one word &mdash; <code>STORED AS SCD TYPE 2</code>
# MAGIC             <ul><li><u>Engine auto-manages</u> <code>__START_AT</code>, <code>__END_AT</code>, and tombstones</li></ul>
# MAGIC           </li>
# MAGIC           <li><strong>Use when:</strong> audit trail, regulatory compliance, or point-in-time analytics</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
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
# MAGIC         <strong style="color: #00695c;">The Syntax Difference</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Changing <code>STORED AS SCD TYPE 1</code> to <code>STORED AS SCD TYPE 2</code> is the <strong>only syntax difference</strong>. Every other clause &mdash; KEYS, SEQUENCE BY, COLUMNS, the SELECT query &mdash; remains identical</li>
# MAGIC           <li>The engine handles versioning, tombstones, and current-record tracking automatically &mdash; no separate INSERT, no UNION pattern, no post-MERGE cleanup</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Choosing Between Type 1 and Type 2</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Choose Type 1</strong> when only the current value matters &mdash; current address, current price, current status. Simpler queries, smaller storage footprint</li>
# MAGIC           <li><strong>Choose Type 2</strong> when you need to answer &ldquo;what was the value at time X?&rdquo; &mdash; point-in-time joins to fact tables, regulatory audit trails, trend analysis</li>
# MAGIC           <li><strong>Start with Type 1 if unsure</strong> &mdash; you can always switch to Type 2 later by changing one word. The reverse (Type 2 &rarr; Type 1) loses history</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">The Middle Ground</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Type 2 + TRACK HISTORY ON</strong> lets you preserve history only for analytically meaningful columns while keeping storage costs closer to Type 1 &mdash; covered in detail in B3</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
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
# MAGIC ### B2. When Current State Is the Right Business Choice
# MAGIC
# MAGIC **SCD Type 1** keeps only the current state of each dimension record &mdash; when a source row changes, the target row is overwritten in place with no history preserved. This is not a limitation &mdash; it's the **correct choice** when the business only needs current values.
# MAGIC
# MAGIC <!-- ── Visual: Business Scenarios for Type 1 ── -->
# MAGIC <style>
# MAGIC .b1s-wrap { max-width:1100px; margin:0 auto 20px; font-family:sans-serif; color:#0b2026; }
# MAGIC .b1s-cards { display:flex; gap:16px; margin-bottom:20px; }
# MAGIC .b1s-card { flex:1; background:#F9F7F4; border-radius:10px; padding:18px; border-top:5px solid #00A972; }
# MAGIC .b1s-card-title { font-size:14pt; font-weight:700; color:#00A972; margin-bottom:8px; }
# MAGIC .b1s-card-text { font-size:13pt; color:#444; line-height:1.6; }
# MAGIC .b1s-warn { background:#FFF5F5; border-radius:10px; padding:18px; border-left:5px solid #E24B4A; margin-bottom:20px; }
# MAGIC .b1s-warn-title { font-size:14pt; font-weight:700; color:#E24B4A; margin-bottom:8px; }
# MAGIC .b1s-warn-text { font-size:13pt; color:#444; line-height:1.6; }
# MAGIC .b1s-compliance { background:rgba(155,89,182,0.06); border-radius:10px; padding:18px; border-left:5px solid #9B59B6; margin-bottom:20px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="b1s-wrap">
# MAGIC   <!-- Best-fit scenarios -->
# MAGIC   <div style="font-size:16pt; font-weight:700; color:#00A972; margin-bottom:10px;"> Best For: Dimensions Where Only Current State Matters</div>
# MAGIC   <div class="b1s-cards">
# MAGIC     <div class="b1s-card">
# MAGIC       <div class="b1s-card-title">Product Catalog</div>
# MAGIC       <div class="b1s-card-text">Current price, current description, current category. Historical prices live in the fact table (order line items), not the dimension.</div>
# MAGIC     </div>
# MAGIC     <div class="b1s-card">
# MAGIC       <div class="b1s-card-title">Customer Preferences</div>
# MAGIC       <div class="b1s-card-text">Current opt-in/out status, current communication preferences. Only the latest consent matters for compliance.</div>
# MAGIC     </div>
# MAGIC     <div class="b1s-card">
# MAGIC       <div class="b1s-card-title">Employee Directory</div>
# MAGIC       <div class="b1s-card-text">Current title, current department, current manager. HR systems track promotion history separately.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC <div style="max-width:1060px;margin:0 auto 20px;font-family:sans-serif;color:#0b2026;">
# MAGIC <!-- SCD Type 1 flow diagram -->
# MAGIC <div style="display:flex;align-items:center;gap:0;justify-content:center;">
# MAGIC   <!-- CDC Event -->
# MAGIC   <div style="background:#1B5162;border-radius:10px;padding:14px 16px;color:white;font-size:13pt;text-align:center;min-width:140px;">
# MAGIC     <div style="font-weight:700;margin-bottom:8px;font-size:14pt;">CDC Event</div>
# MAGIC     <div style="background:rgba(0,0,0,0.25);border-radius:6px;padding:5px 10px;margin-bottom:4px;">customer_id = 42</div>
# MAGIC     <div style="background:rgba(0,0,0,0.25);border-radius:6px;padding:5px 10px;margin-bottom:4px;">city = Seattle</div>
# MAGIC     <div style="background:rgba(0,0,0,0.25);border-radius:6px;padding:5px 10px;">op = UPDATE</div>
# MAGIC   </div>
# MAGIC   <!-- Arrow 1 -->
# MAGIC   <div style="display:flex;flex-direction:column;align-items:center;padding:0 6px;color:#618794;font-size:12pt;">
# MAGIC     <div style="font-size:11pt;font-weight:600;margin-bottom:2px;">OVERWRITE</div>
# MAGIC     <div style="font-size:18pt;">&#x25B6;</div>
# MAGIC   </div>
# MAGIC   <!-- BEFORE table -->
# MAGIC   <div style="border:2px solid #1B5162;border-radius:10px;padding:10px 14px;min-width:200px;">
# MAGIC     <div style="text-align:center;font-size:13pt;font-weight:600;color:#1B5162;margin-bottom:8px;">dim_customer &mdash; BEFORE</div>
# MAGIC     <div style="background:#f5f3f0;border-radius:6px;padding:5px 10px;font-size:13pt;margin-bottom:4px;text-align:center;">7 | Denver</div>
# MAGIC     <div style="background:#E24B4A;color:white;border-radius:6px;padding:5px 10px;font-size:13pt;font-weight:600;margin-bottom:4px;text-align:center;">42 | Portland</div>
# MAGIC     <div style="background:#f5f3f0;border-radius:6px;padding:5px 10px;font-size:13pt;text-align:center;">85 | Austin</div>
# MAGIC   </div>
# MAGIC   <!-- Arrow 2 -->
# MAGIC   <div style="display:flex;flex-direction:column;align-items:center;padding:0 6px;color:#618794;font-size:12pt;">
# MAGIC     <div style="font-size:11pt;font-weight:600;margin-bottom:2px;">RESULT</div>
# MAGIC     <div style="font-size:18pt;">&#x25B6;</div>
# MAGIC   </div>
# MAGIC   <!-- AFTER table -->
# MAGIC   <div style="border:2px solid #1B5162;border-radius:10px;padding:10px 14px;min-width:200px;">
# MAGIC     <div style="text-align:center;font-size:13pt;font-weight:600;color:#1B5162;margin-bottom:8px;">dim_customer &mdash; AFTER</div>
# MAGIC     <div style="background:#f5f3f0;border-radius:6px;padding:5px 10px;font-size:13pt;margin-bottom:4px;text-align:center;">7 | Denver</div>
# MAGIC     <div style="background:#00A972;color:white;border-radius:6px;padding:5px 10px;font-size:13pt;font-weight:600;margin-bottom:4px;text-align:center;">42 | Seattle</div>
# MAGIC     <div style="background:#f5f3f0;border-radius:6px;padding:5px 10px;font-size:13pt;text-align:center;">85 | Austin</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="font-size:14pt;color:#618794;font-style:italic;text-align:center;margin:10px 0 14px;">One row overwritten, no history &mdash; &ldquo;Portland&rdquo; is gone. This is the default behavior if <code style="background:#f5f3f0;padding:1px 5px;border-radius:3px;">STORED AS</code> is omitted.</div>
# MAGIC   <div style="background:#F9F7F4;border-radius:10px;padding:14px 20px;border-top:4px solid #1B5162;">
# MAGIC     <div style="font-size:14pt;font-weight:700;color:#1B5162;margin-bottom:8px;">When to Use SCD Type 1</div>
# MAGIC     <ul style="margin:0;padding-left:20px;font-size:14pt;line-height:1.7;color:#444;">
# MAGIC       <li><strong>Current value</strong> is all you need (current address, current price)</li>
# MAGIC       <li><strong>No audit trail</strong> required</li>
# MAGIC       <li><strong>Simplest</strong> CDC pattern &mdash; default if <code style="background:#e8e4df;padding:1px 5px;border-radius:3px;">STORED AS</code> is omitted</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ── Visual: code-concept-explorer ── -->
# MAGIC <div style="max-width: 1060px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <style>
# MAGIC .b1ce-cards { display: none; }
# MAGIC .b1ce-layout { display: flex; gap: 22px; align-items: stretch; }
# MAGIC .b1ce-code-wrap { flex: 1; position: relative; overflow: hidden; }
# MAGIC .b1ce-theme-btn {
# MAGIC   position: absolute; top: 10px; right: 12px; z-index: 2;
# MAGIC   background: rgba(255,255,255,0.12); border: 1px solid rgba(255,255,255,0.2);
# MAGIC   border-radius: 6px; padding: 5px 12px; font-size: 14pt; font-weight: 600; color: #cdd6f4;
# MAGIC   cursor: pointer; transition: background 0.15s, color 0.15s, border-color 0.15s;
# MAGIC }
# MAGIC .b1ce-theme-btn:hover { background: rgba(255,255,255,0.2); }
# MAGIC .b1ce-code-wrap.light .b1ce-theme-btn { background: rgba(0,0,0,0.06); border-color: rgba(0,0,0,0.15); color: #444; }
# MAGIC .b1ce-code-wrap.light .b1ce-theme-btn:hover { background: rgba(0,0,0,0.1); }
# MAGIC .b1ce-code {
# MAGIC   border-radius: 10px; padding: 20px 22px; font-family: 'Menlo','Consolas',monospace;
# MAGIC   font-size: 14pt; line-height: 1.75; overflow-x: auto; background: #1e1e2e; color: #cdd6f4;
# MAGIC   transition: background 0.3s, color 0.3s;
# MAGIC }
# MAGIC .b1ce-code-wrap.light .b1ce-code { background: #fafafa; color: #383a42; }
# MAGIC .b1ce-code .tk-kw { color: #cba6f7; } .b1ce-code .tk-fn { color: #89b4fa; }
# MAGIC .b1ce-code .tk-str { color: #a6e3a1; } .b1ce-code .tk-num { color: #fab387; }
# MAGIC .b1ce-code .tk-cmt { color: #6c7086; } .b1ce-code .tk-dim { color: #a6adc8; }
# MAGIC .b1ce-code-wrap.light .b1ce-code .tk-kw { color: #a626a4; } .b1ce-code-wrap.light .b1ce-code .tk-fn { color: #4078f2; }
# MAGIC .b1ce-code-wrap.light .b1ce-code .tk-str { color: #50a14f; } .b1ce-code-wrap.light .b1ce-code .tk-num { color: #986801; }
# MAGIC .b1ce-code-wrap.light .b1ce-code .tk-cmt { color: #a0a1a7; } .b1ce-code-wrap.light .b1ce-code .tk-dim { color: #696c77; }
# MAGIC .b1ce-code .line { display: block; padding: 1px 6px; border-radius: 3px; transition: background 0.25s, opacity 0.25s; }
# MAGIC .b1ce-code .line[data-g] { cursor: pointer; }
# MAGIC .b1ce-code.has-highlight .line { opacity: 0.35; }
# MAGIC .b1ce-code.has-highlight .line.hl { opacity: 1; background: rgba(255,255,255,0.08); }
# MAGIC .b1ce-code-wrap.light .b1ce-code.has-highlight .line.hl { background: rgba(0,0,0,0.06); }
# MAGIC .b1ce-explain { flex: 0 0 300px; display: flex; flex-direction: column; justify-content: center; }
# MAGIC .b1ce-explain-card { background: #F9F7F4; border-radius: 10px; border-top: 6px solid #ccc; padding: 20px; font-size: 14pt; line-height: 1.6; opacity: 0; transition: opacity 0.3s; }
# MAGIC .b1ce-explain-card.visible { opacity: 1; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- Concept selector cards -->
# MAGIC <div class="b1ce-cards">
# MAGIC   <div class="b1ce-card" data-id="0" onclick="b1ceSelect(0)" style="--dc:#E24B4A; border-top-color:#E24B4A;">
# MAGIC     <span class="b1ce-card-label">FLOW AUTO CDC + FROM stream()</span>
# MAGIC   </div>
# MAGIC   <div class="b1ce-card" data-id="1" onclick="b1ceSelect(1)" style="--dc:#02A36F; border-top-color:#02A36F;">
# MAGIC     <span class="b1ce-card-label">KEYS</span>
# MAGIC   </div>
# MAGIC   <div class="b1ce-card" data-id="2" onclick="b1ceSelect(2)" style="--dc:#9B59B6; border-top-color:#9B59B6;">
# MAGIC     <span class="b1ce-card-label">SEQUENCE BY</span>
# MAGIC   </div>
# MAGIC   <div class="b1ce-card" data-id="3" onclick="b1ceSelect(3)" style="--dc:#4299E0; border-top-color:#4299E0;">
# MAGIC     <span class="b1ce-card-label">COLUMNS * EXCEPT</span>
# MAGIC   </div>
# MAGIC   <div class="b1ce-card" data-id="4" onclick="b1ceSelect(4)" style="--dc:#F8A805; border-top-color:#F8A805;">
# MAGIC     <span class="b1ce-card-label">STORED AS SCD TYPE 1</span>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Code + Explanation layout -->
# MAGIC <div class="b1ce-layout">
# MAGIC   <div class="b1ce-code-wrap" id="b1ce-code-wrap">
# MAGIC     <div style="text-align:center;font-size:14pt;color:#618794;margin-bottom:6px;"><img src="../Includes/images/icons/cursor-icon.png" height="18" style="vertical-align: middle;"> Hover over each line to explore what it does</div>
# MAGIC     <div class="b1ce-code" id="b1ce-code" style="position:relative;">
# MAGIC       <button class="b1ce-theme-btn" id="b1ce-theme-btn" onclick="b1ceToggle()">Light Mode</button>
# MAGIC       <span class="line" data-g="5"><span class="tk-kw">CREATE OR REFRESH STREAMING TABLE</span> customers_current</span>
# MAGIC       <span class="line" data-g="0"><span class="tk-kw">FLOW AUTO CDC</span></span>
# MAGIC       <span class="line" data-g="0"><span class="tk-kw">FROM</span> <span class="tk-fn">stream</span>(<span class="tk-fn">customer_updates</span>)</span>
# MAGIC       <span class="line" data-g="1"><span class="tk-kw">KEYS</span> (<span class="tk-fn">customer_id</span>)</span>
# MAGIC       <span class="line" data-g="6"><span class="tk-kw">IGNORE NULL UPDATES</span></span>
# MAGIC       <span class="line" data-g="2"><span class="tk-kw">SEQUENCE BY</span> <span class="tk-fn">updated_at</span></span>
# MAGIC       <span class="line" data-g="3"><span class="tk-kw">COLUMNS</span> * <span class="tk-kw">EXCEPT</span> (<span class="tk-fn">operation</span>, <span class="tk-fn">_metadata</span>)</span>
# MAGIC       <span class="line" data-g="4"><span class="tk-kw">STORED AS SCD TYPE</span> <span class="tk-num">1</span>;</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b1ce-explain">
# MAGIC     <div class="b1ce-explain-card" id="b1ce-explain-card"></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var b1ce_DATA = [
# MAGIC   { color:'#E24B4A', title:'FLOW AUTO CDC + FROM stream()',
# MAGIC     text:'<code>FLOW AUTO CDC</code> declares this as a <strong>CDC pipeline</strong>.<br/><code>FROM stream()</code> reads <u>incrementally</u> from the source &mdash; the streaming checkpoint tracks the offset, so each <strong>REFRESH starts where the last one ended</strong>.' },
# MAGIC   { color:'#02A36F', title:'KEYS',
# MAGIC     text:'The <strong>business key</strong> used to <u>match</u> incoming CDC events to existing target rows.<br/>If a row with this key exists, it gets <strong>updated</strong>. If not, it gets <strong>inserted</strong>.<br/>Supports <strong>composite keys</strong>: <code>KEYS (product_id, region_code)</code>.' },
# MAGIC   { color:'#9B59B6', title:'SEQUENCE BY',
# MAGIC     text:'Determines the <strong>&ldquo;winning&rdquo; record</strong> when multiple changes exist for the same key.<br/>Like a <u>postmark</u> &mdash; the <strong>latest date wins</strong>, regardless of delivery order.<br/><code>NULL</code> values <strong>not supported</strong> in this column.' },
# MAGIC   { color:'#4299E0', title:'COLUMNS * EXCEPT',
# MAGIC     text:'<strong>Filters out</strong> metadata columns (<code>operation</code>, <code>_metadata</code>) from the target table.<br/>Only the <u>business columns</u> land in the dimension &mdash; keeps the target <strong>clean</strong>.' },
# MAGIC   { color:'#F8A805', title:'STORED AS SCD TYPE 1',
# MAGIC     text:'<strong>Current-state only</strong> &mdash; updates <u>overwrite</u>, no history retained.<br/>Like a <strong>whiteboard</strong>: erase and rewrite.<br/>This is the <u>default</u> if <code>STORED AS</code> is omitted.' },
# MAGIC   { color:'#1B5162', title:'CREATE OR REFRESH STREAMING TABLE',
# MAGIC     text:'Declares a <strong>streaming table</strong> &mdash; a <u>stateful</u> table backed by a streaming checkpoint.<br/><code>CREATE</code> builds it the <strong>first time</strong>; <code>REFRESH</code> processes only <u>new events</u> since the last checkpoint.<br/>The table <strong>persists between refreshes</strong>.' },
# MAGIC   { color:'#618794', title:'IGNORE NULL UPDATES',
# MAGIC     text:'If a CDC event has <code>NULL</code> for a non-key column, that column is <strong>left unchanged</strong> in the target &mdash; <u>not overwritten</u> with NULL.<br/>Handles <strong>partial updates</strong> from CDC sources that only send <u>changed fields</u>.' }
# MAGIC ];
# MAGIC var b1ceLight = false;
# MAGIC function b1ceToggle() {
# MAGIC   b1ceLight = !b1ceLight;
# MAGIC   document.getElementById('b1ce-code-wrap').classList.toggle('light', b1ceLight);
# MAGIC   document.getElementById('b1ce-theme-btn').textContent = b1ceLight ? 'Dark Mode' : 'Light Mode';
# MAGIC }
# MAGIC function b1ceShow(id) {
# MAGIC   var code = document.getElementById('b1ce-code'), card = document.getElementById('b1ce-explain-card');
# MAGIC   var c = b1ce_DATA[id];
# MAGIC   code.classList.add('has-highlight');
# MAGIC   code.querySelectorAll('.line').forEach(function(ln){ ln.classList.toggle('hl', ln.dataset.g===String(id)); });
# MAGIC   card.style.borderTopColor = c.color;
# MAGIC   card.innerHTML = '<div style="font-size:15pt;font-weight:700;margin-bottom:10px;color:'+c.color+';">'+c.title+'</div><div>'+c.text+'</div>';
# MAGIC   card.classList.add('visible');
# MAGIC }
# MAGIC function b1ceClear() {
# MAGIC   var code = document.getElementById('b1ce-code'), card = document.getElementById('b1ce-explain-card');
# MAGIC   code.classList.remove('has-highlight');
# MAGIC   code.querySelectorAll('.line').forEach(function(ln){ ln.classList.remove('hl'); });
# MAGIC   card.classList.remove('visible');
# MAGIC }
# MAGIC (function() {
# MAGIC   setTimeout(function() {
# MAGIC     document.querySelectorAll('#b1ce-code .line[data-g]').forEach(function(ln) {
# MAGIC       ln.addEventListener('mouseenter', function() { b1ceShow(parseInt(this.dataset.g)); });
# MAGIC       ln.addEventListener('mouseleave', function() { b1ceClear(); });
# MAGIC     });
# MAGIC   }, 100);
# MAGIC })()
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
# MAGIC         <strong style="color: #00695c;">What Each Clause Does</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong><code>CREATE OR REFRESH STREAMING TABLE</code></strong>: declares a <strong>stateful streaming table</strong> backed by a checkpoint. <code>CREATE</code> builds it the first time; <code>REFRESH</code> processes only new events since the last checkpoint &mdash; the table persists between refreshes</li>
# MAGIC           <li><strong><code>FLOW AUTO CDC</code> + <code>FROM stream()</code></strong>: <code>FLOW AUTO CDC</code> declares this as a <strong>CDC pipeline</strong>. <code>FROM stream()</code> reads incrementally from the source &mdash; the streaming checkpoint tracks the offset, so each refresh starts where the last one ended</li>
# MAGIC           <li><strong><code>KEYS (customer_id)</code></strong>: the <strong>business key</strong> used to match incoming CDC events to existing target rows. If a row with this key exists, it gets updated; if not, it gets inserted. Supports composite keys: <code>KEYS (product_id, region_code)</code></li>
# MAGIC           <li><strong><code>SEQUENCE BY updated_at</code></strong>: determines the <strong>winning record</strong> when multiple changes exist for the same key &mdash; the record with the highest value wins, regardless of delivery order. <code>NULL</code> values are not supported in this column</li>
# MAGIC           <li><strong><code>COLUMNS * EXCEPT (operation, _metadata)</code></strong>: filters out metadata columns from the target table &mdash; only the <strong>business columns</strong> land in the dimension. Keeps your target table clean</li>
# MAGIC           <li><strong><code>STORED AS SCD TYPE 1</code></strong>: <strong>current-state only</strong> &mdash; updates overwrite the target row in place, no history retained. This is the <strong>default</strong> if <code>STORED AS</code> is omitted</li>
# MAGIC           <li><strong><code>IGNORE NULL UPDATES</code></strong>: if a CDC event has <code>NULL</code> for a non-key column, that column is <strong>left unchanged</strong> in the target rather than overwritten with NULL &mdash; handles partial updates from CDC sources that only send changed fields</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">How SEQUENCE BY Handles Out-of-Order Events</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The engine compares the incoming event's <code>SEQUENCE BY</code> value to the current stored value for that key. If the incoming value is <strong>higher</strong>, the record is updated. If lower or equal, the event is discarded &mdash; it's a late arrival</li>
# MAGIC           <li>This guarantee holds <strong>across batches</strong>: a late-arriving event from yesterday will not overwrite today's record, even if it's processed in tomorrow's batch</li>
# MAGIC           <li>NULL values in the <code>SEQUENCE BY</code> column are <strong>not supported</strong> &mdash; all events must have a non-null sequence value</li>
# MAGIC           <li>For compound ordering (timestamp tie-breaking by event ID), use: <code>SEQUENCE BY STRUCT(updated_at, event_id)</code> &mdash; STRUCT comparison is lexicographic</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Composite Keys and Column Filtering</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Composite business keys: <code>KEYS (product_id, region_code)</code> &mdash; all key columns together identify a unique dimension row</li>
# MAGIC           <li><code>COLUMNS * EXCEPT (operation, _metadata, source_file_name)</code> &mdash; you can exclude any number of CDC control or metadata columns</li>
# MAGIC           <li>Alternatively, list columns explicitly: <code>COLUMNS (customer_id, first_name, last_name, email, updated_at)</code></li>
# MAGIC           <li>The source query (<code>FROM stream(...)</code>) can include transformations: casting, renaming, adding derived columns before the AUTO CDC engine applies its logic</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Syntax Reference</strong>
# MAGIC         <pre style="background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 14pt; line-height: 1.7; overflow-x: auto; margin: 8px 0 0 0;"><code>CREATE OR REFRESH STREAMING TABLE dim_customers (
# MAGIC   customer_id  BIGINT    NOT NULL COMMENT 'Business key',
# MAGIC   first_name   STRING,
# MAGIC   last_name    STRING,
# MAGIC   email        STRING,
# MAGIC   country_code STRING,
# MAGIC   updated_at   TIMESTAMP COMMENT 'CDC sequence column'
# MAGIC )
# MAGIC COMMENT 'Current-state customer dimension — SCD Type 1'
# MAGIC FLOW AUTO CDC
# MAGIC FROM stream(bronze_customers_cdc)
# MAGIC KEYS (customer_id)
# MAGIC IGNORE NULL UPDATES
# MAGIC SEQUENCE BY updated_at
# MAGIC COLUMNS * EXCEPT (operation, _metadata)
# MAGIC STORED AS SCD TYPE 1;</code></pre>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. SCD Type 2: Historical Dimensions for Audit and Analytics
# MAGIC
# MAGIC **SCD Type 2** preserves the complete history of every dimension record. Each change creates a new version row with `__START_AT` and `__END_AT` timestamps. The syntax difference from Type 1 is exactly one word &mdash; but the **business implications** are profound. Add **`TRACK HISTORY ON`** to control exactly which column changes trigger new version rows.
# MAGIC
# MAGIC <!-- ── Visual: B3 Tabbed Explorer ── -->
# MAGIC <style>
# MAGIC .b3t-wrap { max-width:1100px; margin:0 auto; font-family:sans-serif; color:#0b2026; }
# MAGIC .b3t-layout { display:flex; flex-direction:column; gap:0; }
# MAGIC .b3t-tabs { display:flex; gap:8px; }
# MAGIC .b3t-tab {
# MAGIC   flex:1; padding:14px 18px; cursor:pointer; user-select:none;
# MAGIC   font-size:15pt; font-weight:700; color:#0b2026; text-align:center;
# MAGIC   background:#F9F7F4; border:2px solid #EEEDE9; border-bottom:none;
# MAGIC   border-top:6px solid transparent; border-radius:10px 10px 0 0;
# MAGIC   transition: background 0.2s, border-color 0.2s;
# MAGIC   display:flex; align-items:center; justify-content:center; gap:10px;
# MAGIC }
# MAGIC .b3t-tab:hover { background:#f0edea; }
# MAGIC .b3t-tab.active { background:#fff; border-left-color:#ddd; border-right-color:#ddd; }
# MAGIC .b3t-tab[data-b3t="0"] { border-top-color:#00A972; }
# MAGIC .b3t-tab[data-b3t="1"] { border-top-color:#9B59B6; }
# MAGIC .b3t-content { border:2px solid #ddd; border-top:none; border-radius:0 0 10px 10px; background:#fff; padding:20px 24px; overflow-y:auto; }
# MAGIC .b3t-panel { display:none; }
# MAGIC .b3t-panel.active { display:block; }
# MAGIC .b3t-code { border-radius:10px; padding:20px 22px; font-family:'Menlo','Consolas',monospace; font-size:14pt; line-height:1.75; overflow-x:auto; background:#1e1e2e; color:#cdd6f4; margin-bottom:16px; }
# MAGIC .b3t-code .tk-kw { color:#cba6f7; }
# MAGIC .b3t-code .tk-fn { color:#89b4fa; }
# MAGIC .b3t-code .tk-num { color:#fab387; }
# MAGIC .b3t-code .tk-hl { background:rgba(155,89,182,0.25); display:block; padding:1px 6px; border-radius:3px; }
# MAGIC .b3t-code .tk-line { display:block; padding:1px 6px; }
# MAGIC .b3t-caption { margin-top:12px; font-size:14pt; color:#444; line-height:1.6; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="b3t-wrap">
# MAGIC <div class="b3t-layout">
# MAGIC   <!-- Tabs -->
# MAGIC   <div class="b3t-tabs">
# MAGIC     <div class="b3t-tab active" data-b3t="0" onclick="b3tSelect(0)">
# MAGIC       <span>SCD Type 2</span>
# MAGIC     </div>
# MAGIC     <div class="b3t-tab" data-b3t="1" onclick="b3tSelect(1)">
# MAGIC       <span>+ TRACK HISTORY ON</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Content -->
# MAGIC   <div class="b3t-content">
# MAGIC     <!-- Panel 0: SCD Type 2 -->
# MAGIC     <div class="b3t-panel active" id="b3t-p0">
# MAGIC       <div class="b3t-code">
# MAGIC         <span class="tk-line"><span class="tk-kw">CREATE OR REFRESH STREAMING TABLE</span> customers_history</span>
# MAGIC         <span class="tk-line"><span class="tk-kw">FLOW AUTO CDC</span></span>
# MAGIC         <span class="tk-line"><span class="tk-kw">FROM</span> <span class="tk-fn">stream</span>(<span class="tk-fn">customer_updates</span>)</span>
# MAGIC         <span class="tk-line"><span class="tk-kw">KEYS</span> (<span class="tk-fn">customer_id</span>)</span>
# MAGIC         <span class="tk-line"><span class="tk-kw">SEQUENCE BY</span> <span class="tk-fn">updated_at</span></span>
# MAGIC         <span class="tk-line"><span class="tk-kw">COLUMNS</span> * <span class="tk-kw">EXCEPT</span> (<span class="tk-fn">operation</span>, <span class="tk-fn">_metadata</span>)</span>
# MAGIC         <span class="tk-hl"><span class="tk-kw">STORED AS SCD TYPE</span> <span class="tk-num">2</span>;</span>
# MAGIC       </div>
# MAGIC       <div style="background:#F9F7F4;border-radius:10px;padding:18px 22px;border:2px solid #EEEDE9;">
# MAGIC         <!-- CDC Input Events -->
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#1B5162;margin-bottom:8px;">CDC Input Events</div>
# MAGIC         <table style="width:100%;border-collapse:collapse;font-size:13pt;font-family:monospace;margin-bottom:16px;">
# MAGIC           <tr style="background:#1B5162;color:white;">
# MAGIC             <th style="padding:6px 12px;text-align:left;"></th><th style="padding:6px 12px;text-align:left;">customer_id</th><th style="padding:6px 12px;text-align:left;">country</th><th style="padding:6px 12px;text-align:left;">city</th><th style="padding:6px 12px;text-align:left;">updated_at</th><th style="padding:6px 12px;text-align:left;">op</th>
# MAGIC           </tr>
# MAGIC           <tr style="background:#fff;border-left:4px solid #618794;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#618794;">&#x2460;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">US</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2024-01-01</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">INSERT</td></tr>
# MAGIC           <tr style="background:#fff;border-left:4px solid #2574B5;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#2574B5;">&#x2461;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;font-weight:700;color:#2574B5;">CA</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2024-06-15</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">UPDATE</td></tr>
# MAGIC           <tr style="background:#fff;border-left:4px solid #00A972;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#00A972;">&#x2462;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;font-weight:700;color:#00A972;">GB</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2025-03-01</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">UPDATE</td></tr>
# MAGIC           <tr style="background:#fff;border-left:4px solid #F8A805;"><td style="padding:6px 8px;font-family:sans-serif;font-weight:600;color:#F8A805;">&#x2463;</td><td style="padding:6px 12px;">42</td><td style="padding:6px 12px;">GB</td><td style="padding:6px 12px;font-weight:700;color:#F8A805;">Seattle</td><td style="padding:6px 12px;">2025-04-01</td><td style="padding:6px 12px;">UPDATE</td></tr>
# MAGIC         </table>
# MAGIC         <!-- Result Table -->
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#1B5162;margin-bottom:8px;">Result: dim_customers_history <span style="font-weight:400;color:#E24B4A;">(4 version rows &mdash; every change creates a new version)</span></div>
# MAGIC         <table style="width:100%;border-collapse:collapse;font-size:13pt;font-family:monospace;">
# MAGIC           <tr style="background:#1B5162;color:white;">
# MAGIC             <th style="padding:8px 12px;text-align:left;"></th><th style="padding:8px 12px;text-align:left;">customer_id</th><th style="padding:8px 12px;text-align:left;">country</th><th style="padding:8px 12px;text-align:left;">city</th><th style="padding:8px 12px;text-align:left;">__START_AT</th><th style="padding:8px 12px;text-align:left;">__END_AT</th><th style="padding:8px 12px;text-align:left;font-family:sans-serif;">Status</th>
# MAGIC           </tr>
# MAGIC           <tr style="background:#f5f3f0;border-left:4px solid #618794;">
# MAGIC             <td style="padding:7px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#618794;">&#x2460;</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">US</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2024-01-01</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2024-06-15</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;font-family:sans-serif;">closed</td>
# MAGIC           </tr>
# MAGIC           <tr style="background:rgba(37,116,181,0.06);border-left:4px solid #2574B5;">
# MAGIC             <td style="padding:7px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#2574B5;">&#x2461;</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#2574B5;font-weight:600;">CA</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2024-06-15</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2025-03-01</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;font-family:sans-serif;">closed</td>
# MAGIC           </tr>
# MAGIC           <tr style="background:rgba(0,169,114,0.06);border-left:4px solid #00A972;">
# MAGIC             <td style="padding:7px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#00A972;">&#x2462;</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#00A972;font-weight:600;">GB</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2025-03-01</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2025-04-01</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;font-family:sans-serif;">closed</td>
# MAGIC           </tr>
# MAGIC           <tr style="background:rgba(248,168,5,0.08);border-left:4px solid #F8A805;">
# MAGIC             <td style="padding:7px 8px;font-family:sans-serif;font-weight:600;color:#F8A805;">&#x2463;</td><td style="padding:7px 12px;">42</td><td style="padding:7px 12px;">GB</td><td style="padding:7px 12px;color:#F8A805;font-weight:600;">Seattle</td><td style="padding:7px 12px;">2025-04-01</td><td style="padding:7px 12px;"><strong>NULL</strong></td><td style="padding:7px 12px;color:#00A972;font-family:sans-serif;font-weight:600;">current</td>
# MAGIC           </tr>
# MAGIC         </table>
# MAGIC         <div class="b3t-caption">4 input events &rarr; <strong>4 version rows</strong>. <u>Every</u> change &mdash; including city &mdash; creates a new version.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Panel 1: + TRACK HISTORY ON -->
# MAGIC     <div class="b3t-panel" id="b3t-p1">
# MAGIC       <div class="b3t-code">
# MAGIC         <span class="tk-line"><span class="tk-kw">CREATE OR REFRESH STREAMING TABLE</span> customers_history</span>
# MAGIC         <span class="tk-line"><span class="tk-kw">FLOW AUTO CDC</span></span>
# MAGIC         <span class="tk-line"><span class="tk-kw">FROM</span> <span class="tk-fn">stream</span>(<span class="tk-fn">customer_updates</span>)</span>
# MAGIC         <span class="tk-line"><span class="tk-kw">KEYS</span> (<span class="tk-fn">customer_id</span>)</span>
# MAGIC         <span class="tk-line"><span class="tk-kw">SEQUENCE BY</span> <span class="tk-fn">updated_at</span></span>
# MAGIC         <span class="tk-line"><span class="tk-kw">COLUMNS</span> * <span class="tk-kw">EXCEPT</span> (<span class="tk-fn">operation</span>, <span class="tk-fn">_metadata</span>)</span>
# MAGIC         <span class="tk-line"><span class="tk-kw">STORED AS SCD TYPE</span> <span class="tk-num">2</span></span>
# MAGIC         <span class="tk-hl"><span class="tk-kw">TRACK HISTORY ON</span> * <span class="tk-kw">EXCEPT</span> (<span class="tk-fn">city</span>);</span>
# MAGIC       </div>
# MAGIC       <div style="background:#F9F7F4;border-radius:10px;padding:18px 22px;border:2px solid #EEEDE9;">
# MAGIC         <!-- CDC Input Events -->
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#1B5162;margin-bottom:8px;">CDC Input Events <span style="font-weight:400;color:#618794;">(same 4 events)</span></div>
# MAGIC         <table style="width:100%;border-collapse:collapse;font-size:13pt;font-family:monospace;margin-bottom:16px;">
# MAGIC           <tr style="background:#1B5162;color:white;">
# MAGIC             <th style="padding:6px 12px;text-align:left;"></th><th style="padding:6px 12px;text-align:left;">customer_id</th><th style="padding:6px 12px;text-align:left;">country</th><th style="padding:6px 12px;text-align:left;">city</th><th style="padding:6px 12px;text-align:left;">updated_at</th><th style="padding:6px 12px;text-align:left;">op</th><th style="padding:6px 12px;text-align:left;font-family:sans-serif;">Tracked?</th>
# MAGIC           </tr>
# MAGIC           <tr style="background:#fff;border-left:4px solid #618794;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#618794;">&#x2460;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">US</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2024-01-01</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">INSERT</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;color:#618794;">&mdash;</td></tr>
# MAGIC           <tr style="background:#fff;border-left:4px solid #2574B5;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#2574B5;">&#x2461;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;font-weight:700;color:#2574B5;">CA</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2024-06-15</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">UPDATE</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;color:#00A972;font-weight:600;">Yes &mdash; country</td></tr>
# MAGIC           <tr style="background:#fff;border-left:4px solid #00A972;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#00A972;">&#x2462;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;font-weight:700;color:#00A972;">GB</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2025-03-01</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">UPDATE</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;color:#00A972;font-weight:600;">Yes &mdash; country</td></tr>
# MAGIC           <tr style="background:#FFFBF0;border-left:4px solid #F8A805;"><td style="padding:6px 8px;font-family:sans-serif;font-weight:600;color:#F8A805;">&#x2463;</td><td style="padding:6px 12px;">42</td><td style="padding:6px 12px;">GB</td><td style="padding:6px 12px;font-weight:700;color:#F8A805;">Seattle</td><td style="padding:6px 12px;">2025-04-01</td><td style="padding:6px 12px;">UPDATE</td><td style="padding:6px 12px;font-family:sans-serif;color:#F8A805;font-weight:600;">No &mdash; city only</td></tr>
# MAGIC         </table>
# MAGIC         <!-- Result Table -->
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#1B5162;margin-bottom:8px;">Result: dim_customers_history <span style="font-weight:400;color:#618794;">(still only 3 version rows)</span></div>
# MAGIC         <table style="width:100%;border-collapse:collapse;font-size:13pt;font-family:monospace;">
# MAGIC           <tr style="background:#1B5162;color:white;">
# MAGIC             <th style="padding:8px 12px;text-align:left;"></th><th style="padding:8px 12px;text-align:left;">customer_id</th><th style="padding:8px 12px;text-align:left;">country</th><th style="padding:8px 12px;text-align:left;">city</th><th style="padding:8px 12px;text-align:left;">__START_AT</th><th style="padding:8px 12px;text-align:left;">__END_AT</th><th style="padding:8px 12px;text-align:left;font-family:sans-serif;">Status</th>
# MAGIC           </tr>
# MAGIC           <tr style="background:#f5f3f0;border-left:4px solid #618794;">
# MAGIC             <td style="padding:7px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#618794;">&#x2460;</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">US</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2024-01-01</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2024-06-15</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;font-family:sans-serif;">closed</td>
# MAGIC           </tr>
# MAGIC           <tr style="background:rgba(37,116,181,0.06);border-left:4px solid #2574B5;">
# MAGIC             <td style="padding:7px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#2574B5;">&#x2461;</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#2574B5;font-weight:600;">CA</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">Portland</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2024-06-15</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2025-03-01</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;font-family:sans-serif;">closed</td>
# MAGIC           </tr>
# MAGIC           <tr style="background:rgba(0,169,114,0.06);border-left:4px solid #00A972;">
# MAGIC             <td style="padding:7px 8px;font-family:sans-serif;font-weight:600;color:#00A972;"><span style="color:#00A972;">&#x2462;</span><span style="color:#F8A805;">&#x2463;</span></td><td style="padding:7px 12px;">42</td><td style="padding:7px 12px;color:#00A972;font-weight:600;">GB</td><td style="padding:7px 12px;"><strong style="color:#F8A805;">Seattle</strong></td><td style="padding:7px 12px;">2025-03-01</td><td style="padding:7px 12px;"><strong>NULL</strong></td><td style="padding:7px 12px;color:#00A972;font-family:sans-serif;font-weight:600;">current</td>
# MAGIC           </tr>
# MAGIC         </table>
# MAGIC         <div class="b3t-caption">4 input events &rarr; <strong>3 version rows</strong>. The city change to <strong style="color:#F8A805;">Seattle</strong> updated the current row <u>in place</u> &mdash; no new version because <code>city</code> is excluded from tracking.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function b3tSelect(id) {
# MAGIC   document.querySelectorAll('.b3t-tab').forEach(function(t){ t.classList.toggle('active', parseInt(t.dataset.b3t)===id); });
# MAGIC   document.querySelectorAll('.b3t-panel').forEach(function(p,i){ p.classList.toggle('active', i===id); });
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
# MAGIC         <strong style="color: #00695c;">__START_AT and __END_AT &mdash; How Version Tracking Works</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong><code>__START_AT</code></strong>: the value of the <code>SEQUENCE BY</code> column when this version became active. <strong>Not necessarily a wall-clock timestamp</strong> &mdash; if your <code>SEQUENCE BY</code> is a transaction ID, <code>__START_AT</code> is that transaction ID</li>
# MAGIC           <li><strong><code>__END_AT</code></strong>: the <code>SEQUENCE BY</code> value when this version was superseded. <code>NULL</code> means this is the <strong>currently active version</strong></li>
# MAGIC           <li><strong>No-gaps guarantee</strong>: for any key, the <code>__END_AT</code> of one version equals the <code>__START_AT</code> of the next. There are <u>no temporal gaps</u> in the history</li>
# MAGIC           <li><strong>Schema behavior</strong>: if you let the schema be inferred, the engine adds <code>__START_AT</code> and <code>__END_AT</code> automatically. If you define the schema explicitly, you <strong>must include them</strong> with the same data type as the <code>SEQUENCE BY</code> column</li>
# MAGIC           <li><strong>Out-of-order handling</strong>: the engine uses <code>SEQUENCE BY</code> to insert versions in the correct position in the timeline, even if they arrive late &mdash; the same guarantee as SCD Type 1, but applied to version chains</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Querying Current vs Historical Records</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Current state only</strong>: <code>WHERE __END_AT IS NULL</code> &mdash; equivalent to your SCD Type 1 table, but derived from full history</li>
# MAGIC           <li><strong>Full history for a key</strong>: <code>WHERE customer_id = 101 ORDER BY __START_AT</code></li>
# MAGIC           <li><strong>Point-in-time join to fact table</strong>: <code>JOIN dim d ON f.customer_id = d.customer_id AND f.order_date BETWEEN d.__START_AT AND COALESCE(d.__END_AT, current_timestamp())</code></li>
# MAGIC           <li><strong>Best practice</strong>: create a view <code>dim_customers_current</code> wrapping the <code>WHERE __END_AT IS NULL</code> filter &mdash; downstream consumers use the simple view without knowing the SCD2 structure underneath</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Selective Versioning with TRACK HISTORY ON</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Why not track everything?</strong> A customer who changes their city 50 times generates 50 version rows if city is tracked. At millions of customers, this compounds into <strong>storage cost</strong>, <strong>slower point-in-time joins</strong>, and <strong>analytical noise</strong> from version rows driven by operationally irrelevant changes</li>
# MAGIC           <li><strong>Exclude syntax</strong>: <code>TRACK HISTORY ON * EXCEPT (city)</code> &mdash; any column change <strong>except</strong> <code>city</code> triggers a new version. City changes update the current row <u>in place</u></li>
# MAGIC           <li><strong>Include syntax</strong>: <code>TRACK HISTORY ON (last_name, email, tier)</code> &mdash; <u>only</u> these columns trigger versioning. All other changes update in place</li>
# MAGIC           <li><strong>Key distinction</strong>: untracked column changes are <strong>still stored</strong> in the current version row &mdash; they just don't trigger a new version. No data is lost</li>
# MAGIC           <li><strong>Type 2 only</strong>: <code>TRACK HISTORY ON</code> is only valid for <code>STORED AS SCD TYPE 2</code>. For Type 1, all updates are already in-place overwrites by definition</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Tombstones, Deletes, and Garbage Collection</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>When a record is deleted (via <code>APPLY AS DELETE</code>), the SCD Type 2 table creates a <strong>tombstone</strong> row &mdash; a version with <code>__END_AT</code> populated and no successor. This prevents a <u>late-arriving update from resurrecting</u> the deleted record</li>
# MAGIC           <li><strong>Retention</strong>: tombstones are retained for a configurable period (default: 30 days) before garbage collection: <code>TBLPROPERTIES ('pipelines.cdc.tombstoneGCThresholdInSeconds' = '2592000')</code></li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Performance at Scale</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>SCD Type 2 is <strong>more expensive than Type 1</strong> due to full history tracking &mdash; expect 2&ndash;12x slower processing depending on change frequency and table size</li>
# MAGIC           <li><strong>DBR 17.3+</strong> includes important SCD2 performance optimizations including DPP/DFP improvements and predicate reordering fixes</li>
# MAGIC           <li><strong>Recommended configuration</strong>: liquid clustering on merge keys + <strong>Photon/serverless</strong> compute. Cluster by auto handles <code>__START_AT</code>/<code>__END_AT</code> columns automatically</li>
# MAGIC           <li>&#x25C6; A global payments company attempted SCD Type 2 at 500 TB scale using manual MERGE &mdash; the complexity forced them to fall back to SCD Type 1. AUTO CDC's declarative approach removes the MERGE+INSERT constraint entirely &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Common Misconceptions</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>"SCD Type 2 stores duplicate rows"</strong> &mdash; each row is a <u>distinct version</u> with different effective date ranges, not a copy</li>
# MAGIC           <li><strong>"I need to manage __START_AT and __END_AT myself"</strong> &mdash; AUTO CDC handles these automatically; you never write to them directly</li>
# MAGIC           <li><strong>"All column changes create new versions"</strong> &mdash; only if you don't use <code>TRACK HISTORY ON</code>. With it, you control exactly which columns trigger versioning</li>
# MAGIC           <li><strong>"SCD Type 2 doubles my storage"</strong> &mdash; depends entirely on change frequency. Many dimension rows change infrequently; <code>TRACK HISTORY ON</code> further reduces version count</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Syntax Reference</strong>
# MAGIC         <pre style="background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 14pt; line-height: 1.7; overflow-x: auto; margin: 8px 0 0 0;"><code>-- Basic SCD Type 2
# MAGIC CREATE OR REFRESH STREAMING TABLE dim_customers_history
# MAGIC FLOW AUTO CDC
# MAGIC FROM stream(bronze_customers_cdc)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY updated_at
# MAGIC COLUMNS * EXCEPT (operation, _metadata)
# MAGIC STORED AS SCD TYPE 2;
# MAGIC
# MAGIC -- With selective versioning
# MAGIC CREATE OR REFRESH STREAMING TABLE dim_customers_history
# MAGIC FLOW AUTO CDC
# MAGIC FROM stream(bronze_customers_cdc)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY updated_at
# MAGIC COLUMNS * EXCEPT (operation, _metadata)
# MAGIC STORED AS SCD TYPE 2
# MAGIC TRACK HISTORY ON * EXCEPT (city);
# MAGIC
# MAGIC -- Query current records:
# MAGIC SELECT * FROM dim_customers_history WHERE __END_AT IS NULL;
# MAGIC
# MAGIC -- Point-in-time query:
# MAGIC SELECT * FROM dim_customers_history
# MAGIC WHERE customer_id = 101
# MAGIC   AND __START_AT &lt;= '2024-09-01'
# MAGIC   AND (__END_AT &gt; '2024-09-01' OR __END_AT IS NULL);</code></pre>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Handling Deletes and Verification

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. APPLY AS DELETE &mdash; Handling Soft Deletes
# MAGIC
# MAGIC Most CDC sources do not physically remove rows from the stream &mdash; instead, they flag deletions with an operation column (e.g., `operation = 'DELETE'`). Without `APPLY AS DELETE`, the AUTO CDC engine would treat those flagged rows as upserts and insert them into your dimension table. **`APPLY AS DELETE WHEN`** tells the engine how to interpret the delete signal.
# MAGIC
# MAGIC <!-- ── Visual: D1 Code + Result Split View ── -->
# MAGIC <style>
# MAGIC .d1x-wrap { max-width:1100px; margin:0 auto; font-family:sans-serif; color:#0b2026; }
# MAGIC .d1x-input-row { cursor:pointer; transition: background 0.15s; }
# MAGIC .d1x-input-row:hover { background:rgba(27,49,57,0.04) !important; }
# MAGIC .d1x-result-row { transition: opacity 0.25s; }
# MAGIC .d1x-result-row.dim { opacity:0.3; }
# MAGIC .d1x-result-row.hl { opacity:1; }
# MAGIC .d1x-btn {
# MAGIC   padding:8px 18px; border-radius:8px; font-size:14pt; font-weight:700; cursor:pointer; user-select:none;
# MAGIC   border:2px solid #EEEDE9; background:#F9F7F4; color:#618794; transition: all 0.2s;
# MAGIC }
# MAGIC .d1x-btn:hover { background:#f0edea; }
# MAGIC .d1x-btn.active-t1 { background:#E24B4A; color:white; border-color:#E24B4A; }
# MAGIC .d1x-btn.active-t2 { background:#1B5162; color:white; border-color:#1B5162; }
# MAGIC .d1x-code { border-radius:10px; padding:16px 20px; font-family:'Menlo','Consolas',monospace; font-size:13pt; line-height:1.7; background:#1e1e2e; color:#cdd6f4; }
# MAGIC .d1x-code .tk-kw { color:#cba6f7; } .d1x-code .tk-fn { color:#89b4fa; } .d1x-code .tk-num { color:#fab387; }
# MAGIC .d1x-code .tk-hl { background:rgba(226,75,74,0.3); display:block; padding:1px 6px; border-radius:3px; }
# MAGIC .d1x-code .tk-swap { display:block; padding:1px 6px; }
# MAGIC .d1x-caption { margin-top:10px; font-size:14pt; color:#444; line-height:1.6; min-height:24px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="d1x-wrap">
# MAGIC <!-- Toggle buttons at top -->
# MAGIC <div style="display:flex;gap:8px;margin-bottom:14px;">
# MAGIC   <div class="d1x-btn active-t1" id="d1x-btn-t1" onclick="d1xMode(1)">
# MAGIC     <img src="../Includes/images/icons/gdpr.png" style="height:20px;width:20px;object-fit:contain;vertical-align:middle;margin-right:4px;"/>Type 1
# MAGIC   </div>
# MAGIC   <div class="d1x-btn" id="d1x-btn-t2" onclick="d1xMode(2)">
# MAGIC     <img src="../Includes/images/icons/compliance.png" style="height:20px;width:20px;object-fit:contain;vertical-align:middle;margin-right:4px;"/>Type 2
# MAGIC   </div>
# MAGIC </div>
# MAGIC <!-- Split: Code (left) + Input+Result (right) -->
# MAGIC <div style="display:flex;gap:16px;align-items:flex-start;">
# MAGIC   <!-- Left: Code block -->
# MAGIC   <div style="flex:1;">
# MAGIC     <div class="d1x-code" id="d1x-code">
# MAGIC       <span style="display:block;padding:1px 6px;"><span class="tk-kw">CREATE OR REFRESH STREAMING TABLE</span> dim_customers</span>
# MAGIC       <span style="display:block;padding:1px 6px;"><span class="tk-kw">FLOW AUTO CDC</span></span>
# MAGIC       <span style="display:block;padding:1px 6px;"><span class="tk-kw">FROM</span> <span class="tk-fn">stream</span>(<span class="tk-fn">bronze_cdc</span>)</span>
# MAGIC       <span style="display:block;padding:1px 6px;"><span class="tk-kw">KEYS</span> (<span class="tk-fn">customer_id</span>)</span>
# MAGIC       <span class="tk-hl"><span class="tk-kw">APPLY AS DELETE WHEN</span> operation = <span style="color:#a6e3a1;">'DELETE'</span></span>
# MAGIC       <span style="display:block;padding:1px 6px;"><span class="tk-kw">SEQUENCE BY</span> <span class="tk-fn">updated_at</span></span>
# MAGIC       <span style="display:block;padding:1px 6px;"><span class="tk-kw">COLUMNS</span> * <span class="tk-kw">EXCEPT</span> (<span class="tk-fn">operation</span>, <span class="tk-fn">_metadata</span>)</span>
# MAGIC       <span class="tk-swap" id="d1x-scd-line"><span class="tk-kw">STORED AS SCD TYPE</span> <span class="tk-num">1</span>;</span>
# MAGIC     </div>
# MAGIC     <div class="d1x-caption" id="d1x-caption"><strong>Type 1:</strong> Customer 42 is <u>physically removed</u>. Only customer 85 remains in the table.</div>
# MAGIC   </div>
# MAGIC   <!-- Right: CDC Input + Result stacked -->
# MAGIC   <div style="flex:1;">
# MAGIC     <!-- CDC Input -->
# MAGIC     <div style="font-size:14pt;font-weight:700;color:#1B5162;margin-bottom:6px;">CDC Input Events</div>
# MAGIC     <table style="width:100%;border-collapse:collapse;font-size:13pt;font-family:monospace;margin-bottom:14px;" id="d1x-input">
# MAGIC       <tr style="background:#1B5162;color:white;"><th style="padding:6px 12px;text-align:left;"></th><th style="padding:6px 12px;text-align:left;">customer_id</th><th style="padding:6px 12px;text-align:left;">country</th><th style="padding:6px 12px;text-align:left;">updated_at</th><th style="padding:6px 12px;text-align:left;">op</th></tr>
# MAGIC       <tr class="d1x-input-row" data-d1xr="0" style="background:#fff;border-left:4px solid #618794;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#618794;">&#x2460;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">US</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2024-01-01</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">INSERT</td></tr>
# MAGIC       <tr class="d1x-input-row" data-d1xr="1" style="background:#fff;border-left:4px solid #2574B5;"><td style="padding:6px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#2574B5;">&#x2461;</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">85</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">CA</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">2024-03-01</td><td style="padding:6px 12px;border-bottom:1px solid #e0dcd6;">INSERT</td></tr>
# MAGIC       <tr class="d1x-input-row" data-d1xr="2" style="background:#FFF0EE;border-left:4px solid #E24B4A;"><td style="padding:6px 8px;font-family:sans-serif;font-weight:600;color:#E24B4A;">&#x2462;</td><td style="padding:6px 12px;">42</td><td style="padding:6px 12px;">US</td><td style="padding:6px 12px;">2024-06-15</td><td style="padding:6px 12px;color:#E24B4A;font-weight:700;">DELETE</td></tr>
# MAGIC     </table>
# MAGIC     <!-- Result -->
# MAGIC     <div style="font-size:14pt;font-weight:700;color:#1B5162;margin-bottom:6px;" id="d1x-result-title">Result: dim_customers <span style="font-weight:400;color:#618794;">(Type 1)</span></div>
# MAGIC     <!-- Type 1 result -->
# MAGIC     <table style="width:100%;border-collapse:collapse;font-size:13pt;font-family:monospace;" id="d1x-result-t1">
# MAGIC       <tr style="background:#1B5162;color:white;"><th style="padding:7px 12px;text-align:left;"></th><th style="padding:7px 12px;text-align:left;">customer_id</th><th style="padding:7px 12px;text-align:left;">country</th><th style="padding:7px 12px;text-align:left;font-family:sans-serif;">Status</th></tr>
# MAGIC       <tr class="d1x-result-row" data-d1xo="1" style="background:#f5f3f0;border-left:4px solid #2574B5;"><td style="padding:7px 8px;font-family:sans-serif;font-weight:600;color:#2574B5;">&#x2461;</td><td style="padding:7px 12px;">85</td><td style="padding:7px 12px;">CA</td><td style="padding:7px 12px;color:#00A972;font-family:sans-serif;font-weight:600;">current</td></tr>
# MAGIC     </table>
# MAGIC     <div style="font-size:13pt;color:#444;font-style:italic;margin-top:6px;"><span style="color:#618794;">&#x2460;</span> Customer 42 inserted, then <span style="color:#E24B4A;">&#x2462;</span> <strong style="color:#E24B4A;">physically removed</strong> &mdash; no longer in the table</div>
# MAGIC     <!-- Type 2 result (hidden by default) -->
# MAGIC     <table style="width:100%;border-collapse:collapse;font-size:13pt;font-family:monospace;display:none;" id="d1x-result-t2">
# MAGIC       <tr style="background:#1B5162;color:white;"><th style="padding:7px 12px;text-align:left;"></th><th style="padding:7px 12px;text-align:left;">customer_id</th><th style="padding:7px 12px;text-align:left;">country</th><th style="padding:7px 12px;text-align:left;">__START_AT</th><th style="padding:7px 12px;text-align:left;">__END_AT</th><th style="padding:7px 12px;text-align:left;font-family:sans-serif;">Status</th></tr>
# MAGIC       <tr class="d1x-result-row" data-d1xo="0 2" style="background:rgba(226,75,74,0.06);border-left:4px solid #E24B4A;"><td style="padding:7px 8px;border-bottom:1px solid #e0dcd6;font-family:sans-serif;font-weight:600;color:#E24B4A;">&#x2460;&#x2462;</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">42</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;">US</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#618794;">2024-01-01</td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;"><strong style="color:#E24B4A;">2024-06-15</strong></td><td style="padding:7px 12px;border-bottom:1px solid #e0dcd6;color:#E24B4A;font-family:sans-serif;font-weight:600;">tombstone</td></tr>
# MAGIC       <tr class="d1x-result-row" data-d1xo="1" style="background:rgba(0,169,114,0.06);border-left:4px solid #2574B5;"><td style="padding:7px 8px;font-family:sans-serif;font-weight:600;color:#2574B5;">&#x2461;</td><td style="padding:7px 12px;">85</td><td style="padding:7px 12px;">CA</td><td style="padding:7px 12px;">2024-03-01</td><td style="padding:7px 12px;"><strong>NULL</strong></td><td style="padding:7px 12px;color:#00A972;font-family:sans-serif;font-weight:600;">current</td></tr>
# MAGIC     </table>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var d1xCurrentMode = 1;
# MAGIC var d1xCaptions = {
# MAGIC   1: {
# MAGIC     0: '&#x2460; <strong>INSERT customer 42</strong> &mdash; row added to dim_customers.',
# MAGIC     1: '&#x2461; <strong>INSERT customer 85</strong> &mdash; row added to dim_customers.',
# MAGIC     2: '&#x2462; <strong>DELETE customer 42</strong> &mdash; row is <u>physically removed</u>. Gone from the table entirely.',
# MAGIC     d: '<strong>Type 1:</strong> Customer 42 is <u>physically removed</u>. Only customer 85 remains in the table.'
# MAGIC   },
# MAGIC   2: {
# MAGIC     0: '&#x2460; <strong>INSERT customer 42</strong> &mdash; version row created with <code>__END_AT = NULL</code>.',
# MAGIC     1: '&#x2461; <strong>INSERT customer 85</strong> &mdash; version row created with <code>__END_AT = NULL</code>.',
# MAGIC     2: '&#x2462; <strong>DELETE customer 42</strong> &mdash; becomes a <strong style="color:#E24B4A;">tombstone</strong>: <code>__END_AT</code> set. Row <u>stays in history</u>.',
# MAGIC     d: '<strong>Type 2:</strong> Customer 42 becomes a <strong style="color:#E24B4A;">tombstone</strong> &mdash; <code>__END_AT</code> is set but the row stays in history. Retained 30 days.'
# MAGIC   }
# MAGIC };
# MAGIC function d1xMode(mode) {
# MAGIC   d1xCurrentMode = mode;
# MAGIC   var btn1 = document.getElementById('d1x-btn-t1'), btn2 = document.getElementById('d1x-btn-t2');
# MAGIC   var r1 = document.getElementById('d1x-result-t1'), r2 = document.getElementById('d1x-result-t2');
# MAGIC   var title = document.getElementById('d1x-result-title'), scdLine = document.getElementById('d1x-scd-line');
# MAGIC   var caption = document.getElementById('d1x-caption');
# MAGIC   if (mode === 1) {
# MAGIC     btn1.className = 'd1x-btn active-t1'; btn2.className = 'd1x-btn';
# MAGIC     r1.style.display = ''; r2.style.display = 'none';
# MAGIC     scdLine.innerHTML = '<span class="tk-kw">STORED AS SCD TYPE</span> <span class="tk-num">1</span>';
# MAGIC     title.innerHTML = 'Result: dim_customers <span style="font-weight:400;color:#618794;">(Type 1)</span>';
# MAGIC   } else {
# MAGIC     btn1.className = 'd1x-btn'; btn2.className = 'd1x-btn active-t2';
# MAGIC     r1.style.display = 'none'; r2.style.display = '';
# MAGIC     scdLine.innerHTML = '<span class="tk-kw">STORED AS SCD TYPE</span> <span class="tk-num">2</span>';
# MAGIC     title.innerHTML = 'Result: dim_customers_history <span style="font-weight:400;color:#618794;">(Type 2)</span>';
# MAGIC   }
# MAGIC   caption.innerHTML = d1xCaptions[mode].d;
# MAGIC   document.querySelectorAll('.d1x-result-row').forEach(function(r){ r.classList.remove('hl','dim'); });
# MAGIC }
# MAGIC setTimeout(function(){
# MAGIC   var resultTables = [document.getElementById('d1x-result-t1'), document.getElementById('d1x-result-t2')];
# MAGIC   document.querySelectorAll('#d1x-input .d1x-input-row').forEach(function(row){
# MAGIC     row.addEventListener('mouseenter', function(){
# MAGIC       var rid = this.dataset.d1xr;
# MAGIC       resultTables.forEach(function(tbl){
# MAGIC         if (!tbl) return;
# MAGIC         tbl.querySelectorAll('.d1x-result-row').forEach(function(r){
# MAGIC           var outputs = (r.dataset.d1xo||'').split(' ');
# MAGIC           if (outputs.indexOf(rid)!==-1){ r.classList.add('hl'); r.classList.remove('dim'); }
# MAGIC           else { r.classList.add('dim'); r.classList.remove('hl'); }
# MAGIC         });
# MAGIC       });
# MAGIC       document.getElementById('d1x-caption').innerHTML = d1xCaptions[d1xCurrentMode][parseInt(rid)] || '';
# MAGIC     });
# MAGIC     row.addEventListener('mouseleave', function(){
# MAGIC       resultTables.forEach(function(tbl){
# MAGIC         if (!tbl) return;
# MAGIC         tbl.querySelectorAll('.d1x-result-row').forEach(function(r){ r.classList.remove('hl','dim'); });
# MAGIC       });
# MAGIC       document.getElementById('d1x-caption').innerHTML = d1xCaptions[d1xCurrentMode].d;
# MAGIC     });
# MAGIC   });
# MAGIC }, 100);
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
# MAGIC         <strong style="color: #00695c;">Why APPLY AS DELETE Is Needed</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>In a CDC stream, a deleted source row arrives as a <strong>new event</strong> with <code>operation = 'DELETE'</code> &mdash; the row still has all its column values, just flagged for deletion</li>
# MAGIC           <li><strong>Without <code>APPLY AS DELETE</code></strong>, the engine treats this event as an upsert &mdash; it would <u>insert or update</u> the row rather than removing it. This is the most common CDC mistake</li>
# MAGIC           <li>The clause is <strong>optional</strong> &mdash; if your source never emits delete events, you don't need it. But if it does and you omit it, deleted customers silently reappear in your dimension</li>
# MAGIC           <li>The condition is evaluated <strong>before any upsert logic</strong> &mdash; matching events are routed to the delete path, everything else goes through the normal upsert</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Type 1 vs Type 2 Delete Behavior</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SCD Type 1</strong>: the matching row is <strong>physically removed</strong> from the target table &mdash; as shown in the visual when you toggle Type 1</li>
# MAGIC           <li><strong>SCD Type 2</strong>: a <strong>tombstone</strong> row is created &mdash; the current version's <code>__END_AT</code> is populated with the delete event's <code>SEQUENCE BY</code> value. The row remains in history but is excluded from <code>WHERE __END_AT IS NULL</code> queries &mdash; as shown when you toggle Type 2</li>
# MAGIC           <li><strong>Why tombstones matter</strong>: without the tombstone, a <u>late-arriving update</u> for the deleted key would see no existing record and create a new version as if the delete never happened. The tombstone blocks this resurrection</li>
# MAGIC           <li><strong>Tombstone retention</strong>: configurable via <code>TBLPROPERTIES ('pipelines.cdc.tombstoneGCThresholdInSeconds' = '2592000')</code> (default 30 days). After the retention period, tombstones are garbage collected</li>
# MAGIC           <li>If a DELETE event arrives for a key that <strong>does not exist</strong> in the target, it is a no-op &mdash; no error, no side effect</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Flexible Delete Conditions</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Simple</strong>: <code>APPLY AS DELETE WHEN operation = 'DELETE'</code></li>
# MAGIC           <li><strong>Compound</strong>: <code>APPLY AS DELETE WHEN operation = 'DELETE' OR status = 'CANCELLED'</code> &mdash; any boolean expression over source columns</li>
# MAGIC           <li><strong>Connector-specific flags</strong>: <code>APPLY AS DELETE WHEN _is_deleted = true</code> or <code>_change_type IN ('delete', 'update_preimage')</code></li>
# MAGIC           <li>If your source has <strong>no operation column</strong>, derive one via a temp view transformation before the AUTO CDC flow</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Common Misconceptions</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>"APPLY AS DELETE deletes the source record"</strong> &mdash; No. It interprets the <u>source event</u> as a delete instruction for the <u>target</u>. The source is never modified</li>
# MAGIC           <li><strong>"Soft deletes automatically become target deletes"</strong> &mdash; Only if the <code>APPLY AS DELETE WHEN</code> condition matches. Without the clause, delete-flagged events are treated as upserts</li>
# MAGIC           <li><strong>"SCD Type 2 deleted records are gone immediately"</strong> &mdash; They become tombstones with configurable retention. The row is still in the backing table; only the consumer view filters it out</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Syntax Reference</strong>
# MAGIC         <pre style="background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 14pt; line-height: 1.7; overflow-x: auto; margin: 8px 0 0 0;"><code>-- Handle soft deletes (Type 1 or Type 2):
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC
# MAGIC -- Compound condition:
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE' OR status = 'CANCELLED'
# MAGIC
# MAGIC -- Connector-specific flag:
# MAGIC APPLY AS DELETE WHEN _is_deleted = true</code></pre>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Monitoring and What's Next

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Dimension Quality Monitoring with event_log()
# MAGIC
# MAGIC AUTO CDC captures key metrics on every REFRESH: **`num_upserted_rows`** and **`num_deleted_rows`**. As an analytics engineer, these metrics tell you not just whether the pipeline ran, but whether your **dimension data quality** is healthy.
# MAGIC
# MAGIC <!-- ── Visual: topic-navigator ── -->
# MAGIC <style>
# MAGIC .e1tn-shell{display:flex;border:0.5px solid #dde0e8;border-radius:12px;overflow:hidden;background:#fff;font-family:system-ui,sans-serif}
# MAGIC .e1tn-nav{flex:0 0 260px;background:#f7f8fc;border-right:0.5px solid #dde0e8;padding:10px 0}
# MAGIC .e1tn-nav-label{font-size:14pt;font-weight:500;color:#999;letter-spacing:.7px;text-transform:uppercase;padding:0 14px 8px}
# MAGIC .e1tn-topic{padding:8px 14px;font-size:14pt;font-weight:500;color:#666;cursor:pointer;user-select:none;display:flex;align-items:center;justify-content:space-between;transition:background .12s,color .12s}
# MAGIC .e1tn-topic:hover{background:#eef0f7;color:#0b2026}
# MAGIC .e1tn-topic.open{color:#0b2026}
# MAGIC .e1tn-chev{font-size:14pt;transition:transform .2s;opacity:.4}
# MAGIC .e1tn-topic.open .e1tn-chev{transform:rotate(90deg);opacity:1}
# MAGIC .e1tn-subs{max-height:0;overflow:hidden;transition:max-height .28s ease}
# MAGIC .e1tn-subs.open{max-height:400px}
# MAGIC .e1tn-sub{padding:6px 14px 6px 26px;font-size:14pt;color:#666;cursor:pointer;user-select:none;border-left:2px solid transparent;transition:background .12s,color .12s,border-color .12s}
# MAGIC .e1tn-sub:hover{background:#eef0f7;color:#0b2026}
# MAGIC .e1tn-sub.active{color:#0b2026;border-left-color:#378ADD;background:#e8f2fc}
# MAGIC .e1tn-content{flex:1;padding:24px 28px;min-height:440px;overflow-y:auto}
# MAGIC .e1tn-ct{font-size:14pt;font-weight:500;color:#999;letter-spacing:.7px;text-transform:uppercase;margin-bottom:4px}
# MAGIC .e1tn-ttl{font-size:20pt;font-weight:500;margin-bottom:12px}
# MAGIC .e1tn-body{font-size:14pt;line-height:1.7;color:#555}
# MAGIC .e1tn-body ul,.e1tn-body ol{padding-left:16px;margin:6px 0}
# MAGIC .e1tn-body li{margin-bottom:4px;font-size:14pt}
# MAGIC .e1tn-body code{background:#f0f2f7;padding:1px 4px;border-radius:4px;font-size:14pt;font-family:monospace;color:#0b2026}
# MAGIC .e1tn-body strong{font-weight:500;color:#0b2026}
# MAGIC .e1tn-cb{margin-top:12px;border-radius:8px;overflow:hidden;border:0.5px solid #dde0e8}
# MAGIC .e1tn-cb pre{background:#f7f8fc;padding:12px 14px;font-family:monospace;font-size:14pt;line-height:1.7;overflow-x:auto;color:#0b2026}
# MAGIC .kw{color:#a855f7}.fn{color:#3b82f6}.st{color:#16a34a}.nm{color:#ea580c}.cm{color:#9ca3af}.dc{color:#d97706}
# MAGIC .e1tn-callout{border-left:2px solid;padding:10px 12px;margin-top:10px;font-size:14pt;line-height:1.6}
# MAGIC .e1tn-tbl{width:100%;border-collapse:collapse;margin-top:10px;font-size:14pt}
# MAGIC .e1tn-tbl th{background:#f0f2f7;color:#555;padding:7px 10px;text-align:left;font-weight:500;border-bottom:0.5px solid #dde0e8;font-size:14pt}
# MAGIC .e1tn-tbl td{padding:7px 10px;border-bottom:0.5px solid #dde0e8;color:#555;font-size:14pt}
# MAGIC </style>
# MAGIC
# MAGIC <div class="e1tn-shell">
# MAGIC   <div class="e1tn-nav">
# MAGIC     <div class="e1tn-nav-label">Topics</div>
# MAGIC     <div id="e1tn-sb"></div>
# MAGIC   </div>
# MAGIC   <div class="e1tn-content" id="e1tn-ct"></div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var e1tn_D = [
# MAGIC   {topic:"Data Quality Signals", subs:[
# MAGIC     {label:"Upsert/Delete Ratios", cat:"Data Quality Signals", color:"#378ADD", html:'<p>The <strong>ratio of upserts to deletes</strong> per refresh tells you about data quality, not just pipeline health. A sudden spike in deletes may indicate a source system purge; a spike in upserts may mean a schema change upstream caused a full re-emit.</p><div class="e1tn-cb"><pre><span class="kw">SELECT</span>\n  timestamp,\n  details:flow_progress.metrics.num_upserted_rows  <span class="kw">AS</span> rows_upserted,\n  details:flow_progress.metrics.num_deleted_rows   <span class="kw">AS</span> rows_deleted\n<span class="kw">FROM</span> <span class="fn">event_log</span>(<span class="st">\'dim_customers\'</span>)\n<span class="kw">WHERE</span> event_type = <span class="st">\'flow_progress\'</span>\n<span class="kw">ORDER BY</span> timestamp <span class="kw">DESC</span>\n<span class="kw">LIMIT</span> <span class="nm">10</span>;</pre></div><p style="font-size:13pt;font-weight:600;color:#378ADD;margin-top:12px;">What to Watch For:</p><table class="e1tn-tbl"><tr><th>Pattern</th><th>Possible Cause</th><th>Action</th></tr><tr><td>Upserts 10x normal</td><td>Source re-emitted full table</td><td>Check source CDC config</td></tr><tr><td>Deletes &gt; 5% of upserts</td><td>Mass customer purge or GDPR batch</td><td>Verify with compliance team</td></tr><tr><td>Zero rows in refresh</td><td>Source pipeline stalled</td><td>Check upstream bronze ingestion</td></tr></table>'},
# MAGIC     {label:"Dimension Growth Trends", cat:"Data Quality Signals", color:"#378ADD", html:'<p>For SCD Type 2 dimensions, track <code>num_output_rows</code> over time. If your dimension is growing faster than expected, you may be tracking too many columns or your source has high-velocity changes that create version noise.</p><div class="e1tn-cb"><pre><span class="kw">SELECT</span>\n  timestamp,\n  details:flow_progress.metrics.num_output_rows   <span class="kw">AS</span> total_rows,\n  details:flow_progress.metrics.num_upserted_rows <span class="kw">AS</span> new_versions\n<span class="kw">FROM</span> <span class="fn">event_log</span>(<span class="st">\'dim_customers_history\'</span>)\n<span class="kw">WHERE</span> event_type = <span class="st">\'flow_progress\'</span>\n  <span class="kw">AND</span> details:flow_progress.status = <span class="st">\'COMPLETED\'</span>\n<span class="kw">ORDER BY</span> timestamp <span class="kw">DESC</span>;</pre></div><div class="e1tn-callout" style="border-color:#F8A805;background:#FFFBF0"><strong>Storage alert:</strong> If total_rows is growing 5x faster than your source key count, consider adding <code>TRACK HISTORY ON</code> to reduce version noise from operationally irrelevant column changes.</div>'}
# MAGIC   ]},
# MAGIC   {topic:"Pipeline Health", subs:[
# MAGIC     {label:"SLA Monitoring", cat:"Pipeline Health", color:"#1D9E75", html:'<p>Track pipeline lifecycle to ensure your dimension refreshes meet <strong>SLA requirements</strong>. If downstream dashboards expect data by 8am, you need to know when the pipeline completed.</p><div class="e1tn-cb"><pre><span class="kw">SELECT</span>\n  timestamp,\n  details:state                <span class="kw">AS</span> pipeline_state,\n  details:update_id            <span class="kw">AS</span> update_id\n<span class="kw">FROM</span> <span class="fn">event_log</span>(<span class="st">\'dim_customers\'</span>)\n<span class="kw">WHERE</span> event_type = <span class="st">\'update_progress\'</span>\n<span class="kw">ORDER BY</span> timestamp <span class="kw">DESC</span>\n<span class="kw">LIMIT</span> <span class="nm">20</span>;</pre></div><p style="font-size:13pt;font-weight:600;color:#1D9E75;margin-top:12px;">Example Result:</p><table class="e1tn-tbl"><tr><th>timestamp</th><th>pipeline_state</th><th>update_id</th></tr><tr><td>2025-03-01 12:05:12</td><td style="color:#00A972;font-weight:600;">COMPLETED</td><td style="font-family:monospace;font-size:12pt;">a1b2c3d4-e5f6</td></tr><tr><td>2025-03-01 12:02:00</td><td style="color:#2574B5;font-weight:600;">RUNNING</td><td style="font-family:monospace;font-size:12pt;">a1b2c3d4-e5f6</td></tr><tr><td>2025-03-01 12:01:58</td><td style="color:#618794;font-weight:600;">WAITING_FOR_RESOURCES</td><td style="font-family:monospace;font-size:12pt;">a1b2c3d4-e5f6</td></tr></table>'},
# MAGIC     {label:"Failure Alerting", cat:"Pipeline Health", color:"#1D9E75", html:'<p>Build alerts on pipeline failures. As an analytics engineer, failed dimension refreshes mean <strong>stale data</strong> in downstream reports &mdash; which may violate SLAs or produce misleading analytics.</p><div class="e1tn-cb"><pre><span class="kw">SELECT</span>\n  timestamp,\n  details:state     <span class="kw">AS</span> pipeline_state,\n  details:update_id <span class="kw">AS</span> update_id,\n  message\n<span class="kw">FROM</span> <span class="fn">event_log</span>(<span class="st">\'dim_customers\'</span>)\n<span class="kw">WHERE</span> event_type = <span class="st">\'update_progress\'</span>\n  <span class="kw">AND</span> details:state = <span class="st">\'FAILED\'</span>\n<span class="kw">ORDER BY</span> timestamp <span class="kw">DESC</span>;</pre></div><div class="e1tn-callout" style="border-color:#E24B4A;background:#FFF5F5"><strong>Alerting best practice:</strong> Set up a Databricks SQL Alert on this query to notify the team within minutes of a dimension refresh failure &mdash; stale dimensions compound into incorrect downstream aggregations.</div>'}
# MAGIC   ]},
# MAGIC   {topic:"What to Watch For", subs:[
# MAGIC     {label:"Analytics Engineer Checklist", cat:"What to Watch For", color:"#9B59B6", html:'<p>Beyond pipeline health, here are the <strong>dimension-specific metrics</strong> that matter for data quality:</p><ul><li><strong>Version-to-key ratio</strong> (Type 2): <code>COUNT(*) / COUNT(DISTINCT customer_id)</code>. If this ratio is &gt; 5x, you may be tracking too many columns</li><li><strong>Refresh duration trend</strong>: if each refresh takes longer, your dimension may be growing faster than expected &mdash; time to add <code>TRACK HISTORY ON</code></li><li><strong>Delete anomalies</strong>: a sudden spike in <code>num_deleted_rows</code> may indicate a source system purge or a misconfigured CDC connector</li><li><strong>Zero-row refreshes</strong>: if a scheduled refresh processes zero rows for multiple consecutive runs, the source pipeline may have stalled</li><li><strong>Schema drift</strong>: new columns appearing in the source that are not yet handled by your <code>COLUMNS</code> clause</li></ul><div class="e1tn-callout" style="border-color:#9B59B6;background:rgba(155,89,182,0.06)"><strong>The analytics engineer\'s role:</strong> you are the bridge between the pipeline (data engineering) and the consumer (business intelligence). Monitoring dimension health is how you ensure that bridge is solid.</div>'}
# MAGIC   ]}
# MAGIC ];
# MAGIC
# MAGIC var e1tn_oT=0, e1tn_sT=0, e1tn_sS=0;
# MAGIC function e1tn_build(){
# MAGIC   var sb=document.getElementById('e1tn-sb'); sb.innerHTML='';
# MAGIC   e1tn_D.forEach(function(t,ti){
# MAGIC     var tp=document.createElement('div');
# MAGIC     tp.className='e1tn-topic'+(ti===e1tn_oT?' open':'');
# MAGIC     tp.innerHTML=t.topic+'<span class="e1tn-chev">&#x25B6;</span>';
# MAGIC     tp.onclick=function(){if(e1tn_oT!==ti){e1tn_oT=ti;e1tn_sT=ti;e1tn_sS=0;e1tn_build();e1tn_show();}};
# MAGIC     sb.appendChild(tp);
# MAGIC     var sl=document.createElement('div');
# MAGIC     sl.className='e1tn-subs'+(ti===e1tn_oT?' open':'');
# MAGIC     t.subs.forEach(function(s,si){
# MAGIC       var el=document.createElement('div');
# MAGIC       el.className='e1tn-sub'+(ti===e1tn_sT&&si===e1tn_sS?' active':'');
# MAGIC       el.textContent=s.label;
# MAGIC       el.onclick=function(e){e.stopPropagation();e1tn_sT=ti;e1tn_sS=si;document.querySelectorAll('.e1tn-sub').forEach(function(x){x.classList.remove('active')});el.classList.add('active');e1tn_show();};
# MAGIC       sl.appendChild(el);
# MAGIC     });
# MAGIC     sb.appendChild(sl);
# MAGIC   });
# MAGIC }
# MAGIC function e1tn_show(){
# MAGIC   var item=e1tn_D[e1tn_sT].subs[e1tn_sS];
# MAGIC   document.getElementById('e1tn-ct').innerHTML=
# MAGIC     '<div class="e1tn-ct">'+item.cat+'</div>'
# MAGIC     +'<div class="e1tn-ttl" style="color:'+item.color+'">'+item.label+'</div>'
# MAGIC     +'<div class="e1tn-body">'+item.html+'</div>';
# MAGIC }
# MAGIC e1tn_build(); e1tn_show();
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
# MAGIC         <strong style="color: #00695c;">Key Metrics: What They Tell You About Data Quality</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong><code>num_upserted_rows</code></strong>: rows inserted or updated in the target. For SCD Type 2, each new version counts as an upsert. A sudden 10x spike may indicate a source re-emit or schema change</li>
# MAGIC           <li><strong><code>num_deleted_rows</code></strong>: rows physically removed (Type 1) or tombstoned (Type 2). A spike may indicate a GDPR batch deletion or source system purge</li>
# MAGIC           <li><strong><code>num_output_rows</code></strong>: total rows in the target after the refresh. For Type 2, track this over time to monitor dimension growth</li>
# MAGIC           <li>These metrics are only available for <strong>AUTO CDC queries</strong> (pipelines using <code>KEYS</code> + <code>SEQUENCE BY</code>)</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Event Log Queries for Dimension Health</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><code>event_log('table_name')</code> returns the full event log for the pipeline managing that table &mdash; no pipeline ID needed</li>
# MAGIC           <li>Key event types: <code>flow_progress</code> (per-flow metrics), <code>update_progress</code> (pipeline state), <code>stream_progress</code> (checkpoint offsets)</li>
# MAGIC           <li>The <code>details</code> column is JSON &mdash; use the <code>:</code> operator: <code>details:flow_progress.metrics.num_upserted_rows</code></li>
# MAGIC           <li>The checkpoint is your "bookmark": <code>stream_progress</code> events confirm exactly-once processing boundaries</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Building Operational Dashboards</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Create a <strong>Databricks SQL Dashboard</strong> that tracks dimension refresh metrics over time &mdash; upsert trends, delete spikes, refresh durations</li>
# MAGIC           <li>Set up <strong>SQL Alerts</strong> for anomalies: zero-row refreshes, delete spikes &gt; 5% of total rows, refresh duration &gt; 2x historical average</li>
# MAGIC           <li>Share the dashboard with your BI team so they know when dimension data may be stale &mdash; proactive communication prevents downstream confusion</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Syntax Reference</strong>
# MAGIC         <pre style="background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 14pt; line-height: 1.7; overflow-x: auto; margin: 8px 0 0 0;"><code>-- Basic metrics query:
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details:flow_progress.metrics.num_upserted_rows  AS rows_upserted,
# MAGIC   details:flow_progress.metrics.num_deleted_rows   AS rows_deleted
# MAGIC FROM event_log('dim_customers')
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Dimension growth monitoring (Type 2):
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details:flow_progress.metrics.num_output_rows AS total_rows,
# MAGIC   details:flow_progress.metrics.num_upserted_rows AS new_versions
# MAGIC FROM event_log('dim_customers_history')
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND details:flow_progress.status = 'COMPLETED'
# MAGIC ORDER BY timestamp DESC;
# MAGIC
# MAGIC -- Failure alerting:
# MAGIC SELECT timestamp, message
# MAGIC FROM event_log('dim_customers')
# MAGIC WHERE event_type = 'update_progress'
# MAGIC   AND details:state = 'FAILED'
# MAGIC ORDER BY timestamp DESC;</code></pre>
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
# MAGIC ### Bonus: REPLACE WHERE &mdash; Scoped Fact Table Refresh (Awareness)
# MAGIC
# MAGIC **REPLACE WHERE** is a new flow type in Spark Declarative Pipelines that solves a specific problem: when a dimension changes, Materialized Views over large fact tables must fully recompute &mdash; even when only recent data is affected. REPLACE WHERE scopes the refresh to a defined window.
# MAGIC
# MAGIC <div style="background:#FFF0EE;border-left:4px solid #E24B4A;border-radius:4px;padding:12px 18px;margin:12px 0;font-size:14pt;">
# MAGIC   <strong style="color:#E24B4A;">Private Preview</strong> &mdash; this feature is not yet GA. Shown for awareness only; no hands-on in this lecture.
# MAGIC </div>
# MAGIC
# MAGIC <!-- ── Visual: Problem vs Solution ── -->
# MAGIC <div style="display:flex;gap:24px;max-width:1060px;margin:24px auto;flex-wrap:wrap;">
# MAGIC   <!-- LEFT panel: The Problem -->
# MAGIC   <div style="flex:1;min-width:280px;background:#FFF5F5;border:2px solid #E24B4A;border-radius:10px;padding:20px 24px;box-shadow:0 2px 8px rgba(226,75,74,0.08);">
# MAGIC     <div style="text-align:center;font-size:15pt;font-weight:700;color:#E24B4A;margin-bottom:14px;">The Problem</div>
# MAGIC     <div style="text-align:center;font-size:14pt;font-weight:600;color:#1B3139;margin-bottom:10px;">MV Full Recompute</div>
# MAGIC     <div style="background:#1B3139;color:#e0e0e0;border-radius:6px;padding:10px 14px;font-family:monospace;font-size:13pt;margin-bottom:12px;text-align:center;">fact_orders (10 years) JOIN dim_customers</div>
# MAGIC     <div style="color:#333;font-size:13pt;line-height:1.6;margin-bottom:12px;">1 dimension row changes &rarr; entire 10-year join recomputed</div>
# MAGIC   </div>
# MAGIC   <!-- RIGHT panel: The Solution -->
# MAGIC   <div style="flex:1;min-width:280px;background:#F0FAF6;border:2px solid #00A972;border-radius:10px;padding:20px 24px;box-shadow:0 2px 8px rgba(0,169,114,0.08);">
# MAGIC     <div style="text-align:center;font-size:15pt;font-weight:700;color:#00A972;margin-bottom:14px;">The Solution</div>
# MAGIC     <div style="text-align:center;font-size:14pt;font-weight:600;color:#1B3139;margin-bottom:10px;">REPLACE WHERE Scoped Refresh</div>
# MAGIC     <div style="background:#1B3139;color:#e0e0e0;border-radius:6px;padding:10px 14px;font-family:monospace;font-size:13pt;margin-bottom:12px;text-align:center;">fact_orders (last 7 days) JOIN dim_customers</div>
# MAGIC     <div style="color:#333;font-size:13pt;line-height:1.6;margin-bottom:12px;">Only the 7-day window is deleted, recomputed, and reinserted</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ── Code block: correct syntax ── -->
# MAGIC <pre style="background:#1B3139;color:#e0e0e0;border-radius:8px;padding:14px 18px;font-family:monospace;font-size:14pt;line-height:1.7;overflow-x:auto;margin:24px auto;max-width:1060px;"><code>CREATE OR REFRESH STREAMING TABLE orders_enriched
# MAGIC SCHEDULE EVERY 1 DAY
# MAGIC FLOW REPLACE WHERE
# MAGIC   order_date &gt;= date_add(current_timestamp(), -7)
# MAGIC BY NAME
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   o.order_date,
# MAGIC   d.country_code,
# MAGIC   o.qty,
# MAGIC   o.price
# MAGIC FROM orders_fct o
# MAGIC JOIN customers_dim d
# MAGIC ON o.customer_id = d.customer_id;</code></pre>
# MAGIC
# MAGIC <div style="max-width:1060px;margin:0 auto 24px auto;font-size:14pt;color:#333;line-height:1.7;">
# MAGIC The predicate <code>order_date &gt;= date_add(current_timestamp(), -7)</code> is automatically pushed down to the source scan. Only 7 days of data is read, deleted, recomputed, and reinserted. Everything outside the window is untouched &mdash; never scanned, never billed.
# MAGIC </div>
# MAGIC
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
# MAGIC         <strong style="color: #00695c;">How REPLACE WHERE Works</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Step 1</strong>: Delete all rows in the target matching the predicate (e.g., last 7 days)</li>
# MAGIC           <li><strong>Step 2</strong>: Recompute the source query &mdash; predicate is automatically applied to the source scan</li>
# MAGIC           <li><strong>Step 3</strong>: Insert the new results for the matching range</li>
# MAGIC           <li><code>BY NAME</code> is required &mdash; columns are matched by name, not position</li>
# MAGIC           <li>Powered by the Enzyme incrementalization engine &mdash; only replaces data that actually changed within the window</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Key Limitations and Gotchas</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Only one REPLACE WHERE flow per target table (no multiflow)</li>
# MAGIC           <li>Expectations are not supported on REPLACE WHERE targets</li>
# MAGIC           <li>In Lakehouse SQL: must use inline syntax; predicate overrides not supported</li>
# MAGIC           <li>Avoid predicates on aggregates or window function outputs &mdash; use base columns (e.g., date)</li>
# MAGIC           <li>On FULL REFRESH: all data is cleared and only the window is repopulated. Set <code>pipelines.reset.allowed = false</code> to prevent accidental full refreshes</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">The Complete Modern ETL Architecture</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The full declarative pipeline pattern:</li>
# MAGIC           <li><strong>Bronze</strong>: Streaming Tables (Auto Loader) &mdash; replaces COPY INTO</li>
# MAGIC           <li><strong>Dimensions</strong>: AUTO CDC (SCD1/SCD2) &mdash; replaces manual MERGE INTO (~150 lines)</li>
# MAGIC           <li><strong>Facts</strong>: REPLACE WHERE (date-scoped) &mdash; replaces INSERT OVERWRITE PARTITION</li>
# MAGIC           <li><strong>Gold/Reporting</strong>: Materialized Views (Enzyme) &mdash; replaces slow full-scan views</li>
# MAGIC           <li>This lecture covered the dimensions layer (AUTO CDC). REPLACE WHERE completes the picture for fact tables.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Backfill via Predicate Override API</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>For one-time historical corrections, use the predicate override API to temporarily widen the window</li>
# MAGIC           <li>Available via Python SDK and REST API</li>
# MAGIC           <li>SQL parameterized REPLACE WHERE is coming soon</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Syntax Reference</strong>
# MAGIC         <pre style="background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 14pt; line-height: 1.7; overflow-x: auto; margin: 8px 0 0 0;"><code>-- Inline syntax (works in Lakehouse SQL):
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_enriched
# MAGIC FLOW REPLACE WHERE order_date &gt;= date_add(current_timestamp(), -7)
# MAGIC BY NAME
# MAGIC SELECT o.*, d.country_code
# MAGIC FROM orders_fct o JOIN customers_dim d ON o.customer_id = d.customer_id;
# MAGIC
# MAGIC -- Long-form syntax:
# MAGIC CREATE STREAMING TABLE orders_enriched;
# MAGIC CREATE FLOW orders_enriched AS
# MAGIC INSERT INTO orders_enriched BY NAME
# MAGIC REPLACE WHERE order_date &gt;= date_add(current_timestamp(), -7)
# MAGIC SELECT o.*, d.country_code
# MAGIC FROM orders_fct o JOIN customers_dim d ON o.customer_id = d.customer_id;</code></pre>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Summary
# MAGIC
# MAGIC This lecture replaced the MERGE-based SCD pattern with AUTO CDC &mdash; a declarative SQL feature that handles deduplication, sequencing, delete handling, and history tracking in 7 clauses. You built SCD Type 1 and Type 2 streaming dimension tables, configured soft delete handling, and inspected pipeline metrics. Here are your key takeaways:
# MAGIC
# MAGIC - **Manual MERGE INTO breaks down**: SCD maintenance with MERGE snowballs from ~20 lines for the basic version to 150 lines once deduplication, ordering, deletes, and SCD Type 2 history are added &mdash; each requirement multiplies complexity and fragility
# MAGIC - **AUTO CDC is 7 declarative clauses**: `KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`, `STORED AS SCD TYPE`, and `COLUMNS` replace the entire MERGE pattern &mdash; you declare WHAT, the engine handles HOW
# MAGIC - **SCD Type 1 keeps current state**: `STORED AS SCD TYPE 1` (or default) overwrites in place; `SEQUENCE BY` ensures the highest-value record wins regardless of arrival order; `IGNORE NULL UPDATES` handles partial CDC events
# MAGIC - **SCD Type 2 preserves full history**: change one word to `STORED AS SCD TYPE 2`; the engine manages `__START_AT`, `__END_AT`, tombstones, and version continuity; `TRACK HISTORY ON * EXCEPT (...)` enables selective versioning to control storage cost
# MAGIC - **APPLY AS DELETE handles soft deletes in one clause**: `APPLY AS DELETE WHEN operation = 'DELETE'` replaces manual MERGE branching; Type 1 physically removes rows; Type 2 creates tombstones to prevent resurrection from late-arriving events
# MAGIC - **AUTO CDC metrics are queryable via event_log()**: `num_upserted_rows` and `num_deleted_rows` are captured per REFRESH; pipeline state tracking enables alerting on failures; `REPLACE WHERE` (Private Preview) is the coming complement for scoped fact table refresh
# MAGIC
# MAGIC **Next:** Lab 3.1 gives you hands-on practice building both SCD Type 1 and SCD Type 2 AUTO CDC pipelines, inserting test events including out-of-order and delete records, and verifying correct behavior with the SQL verification queries from this lecture.
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>AUTO CDC in Lakehouse SQL &mdash; Official Documentation (<a href="https://docs.databricks.com/aws/en/dlt/cdc" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/dlt/cdc" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/dlt/cdc" target="_blank">GCP</a>) &mdash; complete reference for AUTO CDC syntax, supported options, limitations, and Beta status in Lakehouse SQL</li>
# MAGIC         <li>CREATE STREAMING TABLE &mdash; SQL Language Reference (<a href="https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table" target="_blank">GCP</a>) &mdash; full syntax reference for all clauses including KEYS, SEQUENCE BY, STORED AS SCD TYPE, APPLY AS DELETE, TRACK HISTORY ON, and COLUMNS</li>
# MAGIC         <li>Monitoring AUTO CDC Pipelines &mdash; Event Log Reference (<a href="https://docs.databricks.com/aws/en/dlt/cdc#monitor-cdc-pipeline-metrics" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/dlt/cdc#monitor-cdc-pipeline-metrics" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/dlt/cdc#monitor-cdc-pipeline-metrics" target="_blank">GCP</a>) &mdash; how to query num_upserted_rows, num_deleted_rows, and flow_progress events; event type definitions and JSON path structure</li>
# MAGIC         <li>CREATE MATERIALIZED VIEW &mdash; SQL Language Reference (<a href="https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view" target="_blank">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view" target="_blank">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view" target="_blank">GCP</a>) &mdash; reference for REPLACE WHERE (Private Preview), FLOW syntax, and BY NAME qualifier for scoped fact table refresh</li>
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
