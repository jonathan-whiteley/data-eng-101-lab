-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![DB Academy](../Includes/images/db-academy.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.2 Demo - Exploring the Course Dataset and SQL Editor
-- MAGIC
-- MAGIC ## Overview
-- MAGIC This demo tours the SQL Editor — the workspace you'll use throughout this course — and explores the course dataset. You'll see the star schema data model, write your first queries, and create a Streaming Table and Materialized View to see how they appear in Unity Catalog.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC - Navigate the SQL Editor interface: tabs, toolbar, menus, Schema Browser, and results area
-- MAGIC - Explore the course dataset and trace star schema join paths
-- MAGIC - Explore the course dataset and observe data characteristics
-- MAGIC - Create a simple Streaming Table and Materialized View
-- MAGIC - Explore created objects in Unity Catalog: Details, Lineage, and the pipeline under the hood

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

-- MAGIC %md
-- MAGIC ## A. REQUIRED - Classroom Setup

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-1.2-Demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Tour the SQL Editor
-- MAGIC
-- MAGIC The **SQL Editor** is your primary workspace for this demo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Open the SQL Editor and Explore the Interface

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Open the SQL Editor</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the left sidebar, <strong>right-click SQL Editor</strong> and select <strong>Open in new tab</strong> &mdash; this keeps your notebook open alongside the editor.</li>
-- MAGIC       <li>Click the <strong>+</strong> button next to the tab name and select <strong>New Query</strong> to create a fresh query tab.</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>You now have a <strong>notebook tab</strong> and a <strong>SQL Editor tab</strong> open side by side</li>
-- MAGIC       <li>Copy queries from the notebook and paste into the SQL Editor throughout this demo</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. The SQL Editor Toolbar

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">The SQL Editor Toolbar</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Tab name</strong> &mdash; each query lives in its own tab; click to rename, click <strong>+</strong> to add tabs</li>
-- MAGIC       <li><strong>Run all</strong> &mdash; executes all statements in the tab; dropdown adjusts the row limit</li>
-- MAGIC       <li><strong>Catalog &middot; Schema selector</strong> &mdash; sets the default catalog and schema so you can write <code>SELECT * FROM raw_orders</code> instead of the fully qualified name</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Set Your Default Catalog and Schema</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>Click the <strong>Catalog &middot; Schema selector</strong> in the toolbar.</li>
-- MAGIC       <li>Set the <strong>catalog</strong> to your <code>labuser_XXXXX</code> catalog (the name printed by the setup cell in Section A).</li>
-- MAGIC       <li>Set the <strong>schema</strong> to <code>data</code>.</li>
-- MAGIC     </ol>
-- MAGIC     Queries in this demo will resolve without the full catalog prefix.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">New SQL Editor Toggle</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>New SQL editor: ON</strong> &mdash; toggles between the modern and legacy editor experience.
-- MAGIC         <ul>
-- MAGIC           <li><strong>ON</strong>: modern editor with enhanced autocomplete, inline Generate (&#x2318;+I on Mac / Ctrl+I on Windows) for Genie Code, and improved results display.</li>
-- MAGIC           <li><strong>OFF</strong>: legacy editor. Keep it <strong>ON</strong> for this course.</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Select a SQL Warehouse</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Click the <strong>Warehouse selector</strong> and choose a <strong>Serverless</strong> or <strong>Pro SQL Warehouse</strong>. If none is available, ask your instructor.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">Additional Toolbar Features</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Schedule</strong> &mdash; automates recurring runs (covered in the Lakeflow Jobs module)</li>
-- MAGIC       <li><strong>Share</strong> &mdash; share the query with teammates for viewing or editing</li>
-- MAGIC       <li><strong>Save</strong> &mdash; persists the query to your workspace; saved queries appear under <strong>Workspace</strong> in the left panel</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. The Editor Area and Results

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">Explore the Editor and Results Area</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <strong>Editor area (top half):</strong>
-- MAGIC     <ul>
-- MAGIC       <li><strong>Line numbers</strong> on the left &mdash; SQL code with syntax highlighting in the center.</li>
-- MAGIC       <li><strong>Generate (&#x2318;+I on Mac / Ctrl+I on Windows)</strong> button on the right &mdash; opens Genie Code inline to generate SQL from natural language.</li>
-- MAGIC       <li><strong>Add parameter</strong> button below the editor &mdash; creates named parameters for parameterized queries.</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">Results Area (bottom half)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Output</strong> window &mdash; shows query results in a grid. Click <strong>+</strong> next to it to add a visualization (chart, graph).</li>
-- MAGIC       <li>Column headers show <strong>data type icons</strong> (e.g., numeric, string) &mdash; hover to see the full type.</li>
-- MAGIC       <li>Toolbar icons on the right: <strong>search</strong>, <strong>filter</strong>, <strong>download</strong>, <strong>copy</strong>, <strong>expand</strong> &mdash; use these to explore results without writing more SQL.</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B4. The Menus

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">Explore the Menus (&#x22EE; kebab icon)</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Click the <strong>&#x22EE;</strong> (three dots) icon next to the star in the toolbar:
-- MAGIC     <ul>
-- MAGIC       <li><strong>File</strong> &rarr; Share, Schedule, Commit to Git, Clone, Rename, Move, Trash, Revert changes</li>
-- MAGIC       <li><strong>Edit</strong> &rarr; Undo, <strong>Format Query</strong> (&#x2318;+Shift+F on Mac / Ctrl+Shift+F on Windows) &mdash; auto-formats your SQL for readability. Try it! Also: Add parameter, Find (&#x2318;+F / Ctrl+F), Replace.</li>
-- MAGIC       <li><strong>View</strong> &rarr; Workspace theme, Editor theme (light/dark), Side panel, Appearance, Developer settings, <strong>Query Snippets</strong> &mdash; reusable SQL templates, Clear outputs.</li>
-- MAGIC       <li><strong>Help</strong> and <strong>Send feedback</strong></li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">Key Teaching Point</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <strong>Format Query</strong> (&#x2318;+Shift+F on Mac / Ctrl+Shift+F on Windows) is your best friend &mdash; it instantly cleans up messy SQL. Use it after every paste.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B5. The Left Panel Icons

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">The Left Panel Icons</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li><strong>Workspace</strong> (top) &mdash; browse files and folders; open SQL files directly</li>
-- MAGIC       <li><strong>Schema Browser</strong> (middle) &mdash; browse catalogs, schemas, tables, and columns; no active compute required</li>
-- MAGIC       <li><strong>Genie Code</strong> (bottom) &mdash; AI assistant chat panel with slash commands (<code>/doc</code>, <code>/explain</code>)</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     The <strong>Schema Browser</strong> is your visual map of data assets &mdash; browse tables, see column types, and preview data without writing SQL.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B6. Two Ways to Access Genie Code

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#333; margin-bottom:6px; font-size: 1.1em;">Two Genie Code Entry Points</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <strong>Genie Code</strong> is available in two places:
-- MAGIC     <ol>
-- MAGIC       <li><strong>Generate (&#x2318;+I on Mac / Ctrl+I on Windows)</strong> &mdash; inline prompt in the editor; type a natural-language request and Genie Code writes SQL at your cursor</li>
-- MAGIC       <li><strong>Genie Code panel</strong> (upper-right icon) &mdash; conversational side panel with slash commands (<code>/explain</code>, <code>/fix</code>, <code>/doc</code>, <code>/optimize</code>) and multi-turn conversations</li>
-- MAGIC     </ol>
-- MAGIC     *Generate* = inline SQL. *Genie Code panel* = conversational assistant.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC We'll use both throughout the following sections.
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Instructor Note &mdash; Non-Deterministic Output</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Genie Code output is non-deterministic &mdash; walk through whatever it produces. If unavailable, describe the feature verbally and continue.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## C. Explore the Course Dataset
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">How to Run Each Query</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     For each query: <strong>Copy to clipboard</strong> &rarr; switch to SQL Editor &rarr; &#x2318;+A / Ctrl+A, &#x2318;+V / Ctrl+V to replace &rarr; <strong>Run all</strong>.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Preview the Orders Fact Table

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC SELECT * FROM data.raw_orders;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><code>raw_orders</code> &mdash; central <strong>fact table</strong>, ~15,000 rows with <code>customer_id</code>, <code>product_id</code>, quantities, and prices</li>
-- MAGIC       <li><strong>Execution info</strong> in the toolbar shows timestamp and duration (e.g., "12:22 PM (5s)") &mdash; use this to gauge query performance</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Explore the Dimension Tables

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Try Running Multiple Queries</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Two queries below, separated by semicolons. Try both run modes:
-- MAGIC     <ul>
-- MAGIC       <li><strong>Highlight one query</strong> &rarr; <strong>Run Selected</strong> (&#x2318;+Enter on Mac / Ctrl+Enter on Windows) to run just that query</li>
-- MAGIC       <li><strong>Select nothing</strong> &rarr; <strong>Run all</strong> to run both; navigate results with the <strong>Results 1 of 2</strong> arrows</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC -- Customers dimension (~2,500 rows)
-- MAGIC SELECT * FROM data.raw_customers;
-- MAGIC
-- MAGIC -- Products dimension (~200 rows)
-- MAGIC SELECT * FROM data.raw_products;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>raw_customers</strong> (~2,500 rows) &mdash; notice the first row has a null email. About 3% of emails are null. Also notice mixed state formats: TX vs Texas.</li>
-- MAGIC       <li><strong>raw_products</strong> (~200 rows) &mdash; product names, categories, brands, and prices. Some products are inactive (<code>is_active = false</code>).</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Explore Feedback with Genie Code

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC SELECT * FROM data.customer_feedback;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Try These SQL Editor Features</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Try these features on the query you just ran:
-- MAGIC     <ol>
-- MAGIC       <li><strong>Format Query</strong> (&#x2318;+Shift+F on Mac / Ctrl+Shift+F on Windows) &mdash; auto-formats SQL with proper indentation</li>
-- MAGIC       <li><strong>/doc</strong> &mdash; open the <strong>Genie Code panel</strong> (upper-right icon), type <code>/doc</code>; review the inline comments in the diff view and <strong>Accept</strong> or <strong>Reject</strong></li>
-- MAGIC       <li><strong>/explain</strong> &mdash; in the Genie Code panel, type <code>/explain</code> for a step-by-step walkthrough of the query</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>customer_feedback</strong> &mdash; ~500 rows with ratings and free-text reviews, linked to orders via <code>order_id</code></li>
-- MAGIC       <li><code>/doc</code> and <code>/explain</code> work on any SQL &mdash; use them throughout the course</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C4. The Star Schema
-- MAGIC
-- MAGIC `raw_orders` is the **fact table** at the center; dimensions provide context: *who* (`raw_customers`), *what* (`raw_products`), *how they felt* (`customer_feedback`).

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="max-width: 1100px; margin: 0 auto;">
-- MAGIC <div>
-- MAGIC <div class="mermaid">
-- MAGIC flowchart LR
-- MAGIC     FB["<b style='font-size:1.1em'>customer_feedback</b><br/><small style='color:#999'><i>Supporting &middot; 500 rows</i></small><br/><small style='color:#E5A100'><b>PK:</b> feedback_id</small><br/><small style='color:#2574B5'><b>FK:</b> order_id</small><br/><small style='color:#666'>rating, feedback_text</small>"]
-- MAGIC     CUST["<b style='font-size:1.1em'>raw_customers</b><br/><small style='color:#999'><i>Dimension &middot; 2,500 rows</i></small><br/><small style='color:#E5A100'><b>PK:</b> customer_id</small><br/><small style='color:#666'>name, email, city</small><br/><small style='color:#666'>segment, loyalty_tier</small>"]
-- MAGIC     ORD["<b style='font-size:1.1em'>raw_orders (Fact)</b><br/><small style='color:#999'><i>~15,000 rows</i></small><br/><small style='color:#E5A100'><b>PK:</b> order_id</small><br/><small style='color:#2574B5'><b>FK:</b> customer_id</small><br/><small style='color:#2574B5'><b>FK:</b> product_id</small><br/><small style='color:#666'>order_date, quantity, unit_price</small>"]
-- MAGIC     PROD["<b style='font-size:1.1em'>raw_products</b><br/><small style='color:#999'><i>Dimension &middot; 200 rows</i></small><br/><small style='color:#E5A100'><b>PK:</b> product_id</small><br/><small style='color:#666'>product_name, category</small><br/><small style='color:#666'>brand, cost_price, list_price</small>"]
-- MAGIC     FB e1@== "order_id" ==> ORD
-- MAGIC     ORD e2@== "product_id" ==> PROD
-- MAGIC     ORD e3@== "customer_id" ==> CUST
-- MAGIC     e1@{ animate: true }
-- MAGIC     e2@{ animate: true }
-- MAGIC     e3@{ animate: true }
-- MAGIC     classDef fact fill:#1C3037,color:#fff,stroke:#1C3037,stroke-width:3px
-- MAGIC     classDef dim fill:#FFF4F0,color:#0b2026,stroke:#ff6f42,stroke-width:2px
-- MAGIC     class ORD fact
-- MAGIC     class CUST,PROD,FB dim
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="display: flex; justify-content: center; gap: 24px; margin-top: 8px; font-size: 0.875em; color: #555;">
-- MAGIC   <div style="display: flex; align-items: center; gap: 6px;"><span style="display: inline-block; width: 14px; height: 14px; background: #1C3037; border-radius: 3px;"></span> Fact table</div>
-- MAGIC   <div style="display: flex; align-items: center; gap: 6px;"><span style="display: inline-block; width: 14px; height: 14px; background: #FFF4F0; border: 2px solid #ff6f42; border-radius: 3px;"></span> Dimension / Supporting tables</div>
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
-- MAGIC     lineColor: "#1B5162",
-- MAGIC     primaryColor: "#FFF4F0",
-- MAGIC     primaryBorderColor: "#ff6f42"
-- MAGIC   }
-- MAGIC });
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">Key Teaching Point</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Every query in this course traces a path through this star &mdash; JOINs always pass through the fact table via foreign keys.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Joining Across the Star Schema
-- MAGIC
-- MAGIC Join across the star schema and explore how the tables relate.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Do High-Value Orders Correlate with Better Feedback?

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC SELECT
-- MAGIC   CASE WHEN o.quantity * o.unit_price > 500 THEN 'high' ELSE 'standard' END AS order_tier,
-- MAGIC   AVG(f.rating) AS avg_rating,
-- MAGIC   COUNT(*) AS feedback_count
-- MAGIC FROM data.customer_feedback f
-- MAGIC JOIN data.raw_orders o ON f.order_id = o.order_id
-- MAGIC GROUP BY order_tier;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>Joins 2 tables (feedback + orders) with a <strong>CASE</strong> expression to classify order tiers</li>
-- MAGIC       <li>Simplest star schema pattern &mdash; one hop from a supporting table through the fact table</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Average Feedback Rating by Product Category

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Try Genie Code &mdash; Generate a 3-Table JOIN</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the SQL Editor, select all (&#x2318;+A / Ctrl+A) and delete the previous query to start fresh.</li>
-- MAGIC       <li>Press <strong>&#x2318;+I</strong> (Mac) or <strong>Ctrl+I</strong> (Windows) to open the inline Genie Code prompt.</li>
-- MAGIC       <li>Type: <em>"Show average feedback rating by product category. Join @customer_feedback, @raw_orders, and @raw_products."</em>
-- MAGIC         <ul>
-- MAGIC           <li>Use the <strong>@</strong> symbol before each table name. A dropdown will appear &mdash; manually select the correct table from your catalog.</li>
-- MAGIC         </ul>
-- MAGIC       </li>
-- MAGIC       <li>Review what Genie Code produces, click <strong>Accept</strong>, then click <strong>Run All</strong>.</li>
-- MAGIC       <li>Compare the results with the reference query below. If Genie Code's output differs, that's expected &mdash; AI-generated SQL may use different column names or add extra filters. The key is whether it answers the same business question.</li>
-- MAGIC     </ol>
-- MAGIC     <details>
-- MAGIC       <summary style="cursor: pointer; color: #0d47a1; margin-top: 8px;"><strong>If Genie Code needs more detail, try this prompt instead:</strong></summary>
-- MAGIC       <div style="margin-top: 8px; padding: 8px 12px; background: #f3f4f6; border-radius: 4px;">
-- MAGIC         <em>"Show average feedback rating by product category. Join @customer_feedback, @raw_orders, and @raw_products. Join feedback to orders on order_id, then orders to products on product_id. Group by category."</em>
-- MAGIC       </div>
-- MAGIC     </details>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li>3 tables, 2 joins: feedback &rarr; orders &rarr; products</li>
-- MAGIC       <li>Join paths always pass through the <strong>fact table</strong> &mdash; dimensions never join directly to each other in a star schema</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary style="cursor: pointer; font-weight: bold; color: #333;">Reference query (copy this if Genie Code's version needs adjustment)</summary>
-- MAGIC <br/>
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC SELECT
-- MAGIC   p.category,
-- MAGIC   AVG(f.rating) AS avg_rating,
-- MAGIC   COUNT(*) AS feedback_count
-- MAGIC FROM data.customer_feedback f
-- MAGIC JOIN data.raw_orders o ON f.order_id = o.order_id
-- MAGIC JOIN data.raw_products p ON o.product_id = p.product_id
-- MAGIC GROUP BY p.category;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>
-- MAGIC </details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Create Your First ST and MV
-- MAGIC
-- MAGIC Create a **Streaming Table** and a **Materialized View**, then explore them in Unity Catalog.
-- MAGIC
-- MAGIC **For both the Streaming Table and Materialized View copy the code cell, paste it into the SQL editor and run it.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E1. Create a Streaming Table

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC CREATE OR REPLACE STREAMING TABLE default.demo_1_orders
-- MAGIC AS SELECT * FROM STREAM data.raw_orders;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Streaming Table created</strong> &mdash; <code>default.demo_1_orders</code> now ingests from <code>data.raw_orders</code> and when refreshed will incrementally ingest new data.</li>
-- MAGIC       <li>Query it like any other table</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Explore Your New Object in Catalog Explorer</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the left sidebar, click <strong>Catalog</strong> to open Catalog Explorer.</li>
-- MAGIC       <li>Navigate to your catalog &rarr; <code>default</code> schema.</li>
-- MAGIC       <li>Click <strong>demo_1_orders</strong> &rarr; <strong>Overview</strong> tab &mdash; <strong>Table type</strong> says <strong>Streaming Table</strong></li>
-- MAGIC       <li>Scroll to <strong>Current refresh status</strong> &mdash; click the pipeline link to see the Lakeflow pipeline</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E2. Create a Materialized View

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
-- MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#1B3139; color:#E8E3DC; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
-- MAGIC <code>
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW default.demo_1_revenue_summary
-- MAGIC AS SELECT
-- MAGIC   order_status,
-- MAGIC   COUNT(*) AS order_count,
-- MAGIC   ROUND(SUM(quantity * unit_price), 2) AS total_revenue
-- MAGIC FROM default.demo_1_orders
-- MAGIC GROUP BY order_status;
-- MAGIC </code></pre>
-- MAGIC <script>
-- MAGIC function copyBlock() {
-- MAGIC   const el = document.getElementById("copy-block");
-- MAGIC   if (!el) return;
-- MAGIC   const text = el.innerText;
-- MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
-- MAGIC     navigator.clipboard.writeText(text)
-- MAGIC       .then(() => alert("Copied to clipboard"))
-- MAGIC       .catch(err => {
-- MAGIC         console.error("Clipboard write failed:", err);
-- MAGIC         fallbackCopy(text);
-- MAGIC       });
-- MAGIC   } else {
-- MAGIC     fallbackCopy(text);
-- MAGIC   }
-- MAGIC }
-- MAGIC function fallbackCopy(text) {
-- MAGIC   const textarea = document.createElement("textarea");
-- MAGIC   textarea.value = text;
-- MAGIC   textarea.style.position = "fixed";
-- MAGIC   textarea.style.left = "-9999px";
-- MAGIC   document.body.appendChild(textarea);
-- MAGIC   textarea.select();
-- MAGIC   try {
-- MAGIC     document.execCommand("copy");
-- MAGIC     alert("Copied to clipboard");
-- MAGIC   } catch (err) {
-- MAGIC     alert("Copy failed. Please select and copy manually.");
-- MAGIC   } finally {
-- MAGIC     document.body.removeChild(textarea);
-- MAGIC   }
-- MAGIC }
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">What to Observe</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ul>
-- MAGIC       <li><strong>Materialized View</strong> <code>default.demo_1_revenue_summary</code> pre-computes revenue by order status - Enzyme automatically incrementalizes the refresh, so only changed data is reprocessed.  You can confirm this in the pipeline UI under the materialized view's refresh details.</li>
-- MAGIC       <li>Results are <strong>stored</strong> &mdash; queries read pre-computed data, not re-run the SQL</li>
-- MAGIC     </ul>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #d97706; background: #fff; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#92400e; margin-bottom:6px; font-size: 1.1em;">Explore Your New Objects in Catalog Explorer</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     <ol>
-- MAGIC       <li>In the left sidebar, click <strong>Catalog</strong> to open Catalog Explorer.</li>
-- MAGIC       <li>Navigate to your catalog &rarr; <code>default</code> schema.</li>
-- MAGIC       <li>Click <strong>demo_1_revenue_summary</strong> &rarr; <strong>Overview</strong> tab &mdash; <strong>Table type</strong> says <strong>Materialized View</strong></li>
-- MAGIC       <li>Scroll to <strong>Current refresh status</strong> &mdash; click the pipeline link to see the Lakeflow pipeline</li>
-- MAGIC       <li>Back in Catalog Explorer, click the<strong>Lineage</strong> tab to see how the MV depends on the ST</li>
-- MAGIC     </ol>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="border-left: 4px solid #16a34a; background: #f7fdf4; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
-- MAGIC   <strong style="display:block; color:#14532d; margin-bottom:6px; font-size: 1.1em;">Key Teaching Point</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC     Both STs and MVs land in <strong>Unity Catalog</strong> as governed tables with automatic tracking of object type, owner, lineage, and metadata.
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Key Summary and Takeaways
-- MAGIC
-- MAGIC Key takeaways:
-- MAGIC
-- MAGIC - **SQL Editor** &mdash; multi-tab queries, autocomplete, and **Genie Code** AI assistant
-- MAGIC - **Course dataset** in the `data` schema &mdash; `raw_orders` (fact), `raw_customers`, `raw_products` (dimensions), `customer_feedback` (supporting) with intentional quality issues
-- MAGIC - **Star schema joins** &mdash; every query traces a path through the fact table to its dimensions
-- MAGIC - **Streaming Tables** ingest data &mdash; `default.demo_1_orders` ingests from `data.raw_orders`
-- MAGIC - **Materialized Views** precompute transformations &mdash; `default.demo_1_revenue_summary` stores aggregated results
-- MAGIC - **Unity Catalog** governs everything with automatic lineage tracking
-- MAGIC
-- MAGIC **Next up:** In the next module, we'll learn the full SQL syntax for `CREATE STREAMING TABLE` and `CREATE MATERIALIZED VIEW` — the building blocks of every ETL pipeline in this course.

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
