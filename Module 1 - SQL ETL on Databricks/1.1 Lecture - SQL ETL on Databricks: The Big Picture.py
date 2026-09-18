# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](../Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.1 Lecture - SQL ETL on Databricks: The Big Picture
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC SQL-first teams inheriting legacy data warehouses face a common reality: hours-long ETL runs driven by hand-tuned stored procedures, ungoverned SQL scripts that no one wants to touch, and engineering teams so buried in operational overhead that the business can't get timely metrics.
# MAGIC
# MAGIC This lecture introduces the modern SQL ETL landscape on Databricks — the authoring modes, runtime engines, and governance model that replace those legacy pain points — and frames the specific tools you will use throughout the rest of this course.
# MAGIC
# MAGIC Databricks offers three authoring modes for SQL and non-code ETL personas:
# MAGIC - **Visual** (Lakeflow Designer) -- low-code, drag-and-drop pipeline building
# MAGIC - **Declarative** (Streaming Tables & Materialized Views) -- SQL-native, incremental, serverless
# MAGIC - **Procedural** (Stored Procedures) -- full SQL scripting with control flow
# MAGIC
# MAGIC All three are serverless, governed by Unity Catalog, production-ready with built-in observability, and land on the same lakehouse.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Position the Analytics Engineer role relative to Data Warehouse engineers and Data Analysts on the Databricks platform
# MAGIC - Identify the three SQL ETL authoring modes -- visual, declarative, and procedural -- and describe when each is the right fit
# MAGIC - Navigate the SQL ETL spectrum from Lakeflow Designer through Materialized Views and Streaming Tables to Stored Procedures
# MAGIC - Apply a decision framework to choose between declarative and procedural patterns based on transformation complexity
# MAGIC - Describe Materialized Views and Streaming Tables -- the two declarative building blocks used throughout this course -- and when to use each
# MAGIC - Recall how fact tables, dimension tables, and star schemas connect -- and trace join paths across the course dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The Analytics Engineer

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Who Is the Analytics Engineer?
# MAGIC
# MAGIC ETL on Databricks meets customers where they are. Every persona gets the right experience, without splitting your stack. The platform supports three distinct practitioner profiles — each with their own authoring patterns and runtime — all landing on the same governed lakehouse.
# MAGIC
# MAGIC <!-- ── Three-Persona Card Layout ── -->
# MAGIC <div style="display: flex; gap: 20px; margin: 24px 0 28px 0; flex-wrap: wrap;">
# MAGIC   <!-- Data Warehouse Engineers -->
# MAGIC   <div style="flex: 1; min-width: 240px; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; background: #fff;">
# MAGIC     <div style="height: 5px; background: #1b3a4b;"></div>
# MAGIC     <div style="padding: 20px 20px 16px 20px;">
# MAGIC       <div style="text-transform: uppercase; font-size: 0.875em; font-weight: 700; letter-spacing: 0.5px; color: #1b3a4b; margin-bottom: 6px;">Data Warehouse Engineers</div>
# MAGIC       <div style="font-size: 1.2em; font-weight: 700; color: #222; margin-bottom: 14px;">Modernize without rewriting</div>
# MAGIC       <div style="font-size: 0.875em; font-weight: 600; color: #555; margin-bottom: 8px;">Common patterns</div>
# MAGIC       <ul style="margin: 0 0 16px 0; padding-left: 24px; color: #444; font-size: 0.92em; line-height: 1.7;">
# MAGIC         <li>Stored procedures, SQL scripting, control flow</li>
# MAGIC         <li>Complex multi-statement transactions</li>
# MAGIC         <li>Hand-built orchestration and scheduling</li>
# MAGIC       </ul>
# MAGIC       <hr style="border: none; border-top: 1px solid #eee; margin: 12px 0;"/>
# MAGIC       <div style="font-size: 0.875em; text-transform: uppercase; letter-spacing: 0.5px; color: #888;">Runs on</div>
# MAGIC       <div style="font-size: 1.05em; font-weight: 700; color: #1b3a4b;">Lakehouse SQL + Lakeflow Jobs</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Analytics Engineers (highlighted) -->
# MAGIC   <div style="flex: 1; min-width: 240px; border: 2px solid #ff6f42; border-radius: 8px; overflow: hidden; background: #fff; box-shadow: 0 2px 8px rgba(255,111,66,0.15);">
# MAGIC     <div style="height: 5px; background: #ff6f42;"></div>
# MAGIC     <div style="padding: 20px 20px 16px 20px;">
# MAGIC       <div style="text-transform: uppercase; font-size: 0.875em; font-weight: 700; letter-spacing: 0.5px; color: #ff6f42; margin-bottom: 6px;">Analytics Engineers</div>
# MAGIC       <div style="font-size: 1.2em; font-weight: 700; color: #222; margin-bottom: 14px;">Reliable pipelines, less plumbing</div>
# MAGIC       <div style="font-size: 0.875em; font-weight: 600; color: #555; margin-bottom: 8px;">Common patterns</div>
# MAGIC       <ul style="margin: 0 0 16px 0; padding-left: 24px; color: #444; font-size: 0.92em; line-height: 1.7;">
# MAGIC         <li>Incremental transforms, Materialized Views, and quality checks</li>
# MAGIC         <li>Automated change-data capture and SCD Type 1 &amp; Type 2 patterns</li>
# MAGIC         <li>Scheduled and orchestrated multi-step SQL pipelines</li>
# MAGIC       </ul>
# MAGIC       <hr style="border: none; border-top: 1px solid #eee; margin: 12px 0;"/>
# MAGIC       <div style="font-size: 0.875em; text-transform: uppercase; letter-spacing: 0.5px; color: #888;">Runs on</div>
# MAGIC       <div style="font-size: 1.05em; font-weight: 700; color: #ff6f42;">Streaming Tables and Materialized Views</div>
# MAGIC       <div style="font-size: 0.875em; color: #999; margin-top: 2px;">Powered by serverless Spark Declarative Pipelines under the hood</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Analysts -->
# MAGIC   <div style="flex: 1; min-width: 240px; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; background: #fff;">
# MAGIC     <div style="height: 5px; background: #e8453c;"></div>
# MAGIC     <div style="padding: 20px 20px 16px 20px;">
# MAGIC       <div style="text-transform: uppercase; font-size: 0.875em; font-weight: 700; letter-spacing: 0.5px; color: #e8453c; margin-bottom: 6px;">Analysts</div>
# MAGIC       <div style="font-size: 1.2em; font-weight: 700; color: #222; margin-bottom: 14px;">Self-serve data prep, no ticket needed</div>
# MAGIC       <div style="font-size: 0.875em; font-weight: 600; color: #555; margin-bottom: 8px;">Common patterns</div>
# MAGIC       <ul style="margin: 0 0 16px 0; padding-left: 24px; color: #444; font-size: 0.92em; line-height: 1.7;">
# MAGIC         <li>Last-mile transforms and business-logic modeling</li>
# MAGIC         <li>Visual, low-code workflows with minimal learning curve</li>
# MAGIC         <li>Query acceleration for BI reporting</li>
# MAGIC       </ul>
# MAGIC       <hr style="border: none; border-top: 1px solid #eee; margin: 12px 0;"/>
# MAGIC       <div style="font-size: 0.875em; text-transform: uppercase; letter-spacing: 0.5px; color: #888;">Runs on</div>
# MAGIC       <div style="font-size: 1.05em; font-weight: 700; color: #e8453c;">Materialized Views + Lakeflow Designer</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC This course targets the **analytics engineer** in the center column — the SQL-first practitioner who transforms Silver-layer data into well-structured Gold-layer data marts, data models, and metric views. This is the "missing middle" of the Lakehouse SQL ecosystem: a growing role that formalizes what senior analysts are already doing when they move from ad-hoc queries and dashboards to owning reusable data models and pipelines in the lakehouse. Analytics engineers:
# MAGIC
# MAGIC - **Own** the transformation layer from Silver to Gold using SQL pipelines in Lakehouse SQL
# MAGIC - **Build** reusable data models, curated datasets, and ready-to-use tables for BI consumption
# MAGIC - **Publish** governed metric views and data models that power dashboards and Genie Spaces
# MAGIC - **Tune** query performance and maintain pipeline reliability without needing Spark or Python
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
# MAGIC         <strong style="color: #00695c;">Understanding the Three SQL Personas</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Data Warehouse Engineers</strong> &mdash; maintain end-to-end SQL ETL in legacy and lift-and-shift environments
# MAGIC             <ul>
# MAGIC               <li>Value <strong>control and predictability</strong> &mdash; stored procedures, SQL scripting, multi-statement transactions</li>
# MAGIC               <li>Often migrating from SQL Server, Oracle, Teradata, or Synapse &mdash; Lakebridge can automate up to 80% of the conversion</li>
# MAGIC               <li>Databricks stored procedures follow <strong>ANSI SQL/PSM</strong> (not T-SQL or PL/SQL), so syntax will differ from legacy platforms</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Analytics Engineers</strong> &mdash; build and maintain analytical transformations from Silver to Gold
# MAGIC             <ul>
# MAGIC               <li>The role was popularized by dbt, but on Databricks the tooling is different &mdash; instead of dbt models, you use <strong>materialized views</strong>, <strong>streaming tables</strong>, and <strong>Lakeflow Jobs</strong> for orchestration</li>
# MAGIC               <li>Core identity is the same: you shape raw materials into something the business can actually use &mdash; but your tools are native to the lakehouse</li>
# MAGIC               <li>If data engineers are the construction crew building roads and plumbing, analytics engineers are the interior designers who make the house livable</li>
# MAGIC               <li>SQL is the pipeline language &mdash; analytics engineers do not need Spark, Python, or notebooks to build production-grade pipelines on Databricks</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Data Analysts</strong> &mdash; consume curated data and perform last-mile transformations
# MAGIC             <ul>
# MAGIC               <li>Work primarily in Lakehouse SQL, AI/BI Dashboards, Genie Spaces, and BI tools like Tableau or Power BI</li>
# MAGIC               <li>Lakeflow Designer gives analysts a <strong>no-code visual canvas</strong> for self-serve data prep &mdash; no ticket required</li>
# MAGIC               <li>Consume <strong>UC-certified metric views</strong> for consistent KPIs &mdash; the analytics engineer builds the models, the analyst explores them</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Who This Course Is Built For</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Whether you are a <strong>senior data analyst</strong> ready to own pipelines (not just consume them), or a <strong>data engineer</strong> looking to understand the SQL-first path &mdash; the analytics engineer role sits at the center of the modern Lakehouse SQL ecosystem</li>
# MAGIC           <li>This course teaches the <strong>declarative middle of the spectrum</strong> &mdash; Materialized Views, Streaming Tables, AUTO CDC (Automated Change Data Capture), and Lakeflow Jobs &mdash; where SQL is the pipeline language and the platform handles the infrastructure</li>
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
# MAGIC ### A2. Where the Analytics Engineer Fits
# MAGIC
# MAGIC The three SQL ETL personas above map to the **authoring mode** each team uses on Databricks. But they also represent a spectrum of SQL skill and complexity. The table below zooms in on how these three personas differ — and where the analytics engineer sits between the DW engineer (most procedural control) and the data analyst (most self-serve).
# MAGIC
# MAGIC <!-- ── Spectrum Flow Diagram ── -->
# MAGIC <div style="display: flex; align-items: center; justify-content: center; gap: 0; margin: 24px 0 8px 0; flex-wrap: wrap;">
# MAGIC   <div style="background: #f5f5f5; border: 1px solid #ddd; border-radius: 8px 0 0 8px; padding: 16px 24px; text-align: center; min-width: 200px;">
# MAGIC     <div style="font-size: 0.875em; text-transform: uppercase; letter-spacing: 0.5px; color: #1b3a4b; margin-bottom: 4px;">Procedural</div>
# MAGIC     <div style="font-weight: 700; color: #1b3a4b; font-size: 1.05em;">DW Engineer</div>
# MAGIC     <div style="font-size: 0.875em; color: #666; margin-top: 4px;">Stored procedures &amp; SQL scripting</div>
# MAGIC   </div>
# MAGIC   &#x279C;
# MAGIC   <div style="background: #fff4f0; border: 2px solid #ff6f42; border-radius: 0; padding: 16px 24px; text-align: center; min-width: 200px; box-shadow: 0 2px 8px rgba(255,111,66,0.12);">
# MAGIC     <div style="font-size: 0.875em; text-transform: uppercase; letter-spacing: 0.5px; color: #ff6f42; margin-bottom: 4px;">Declarative</div>
# MAGIC     <div style="font-weight: 700; color: #ff6f42; font-size: 1.05em;">Analytics Engineer</div>
# MAGIC     <div style="font-size: 0.875em; color: #666; margin-top: 4px;">Materialized Views, Streaming Tables, Lakeflow Jobs</div>
# MAGIC   </div>
# MAGIC   &#x279C;
# MAGIC   <div style="background: #f5f5f5; border: 1px solid #ddd; border-radius: 0 8px 8px 0; padding: 16px 24px; text-align: center; min-width: 200px;">
# MAGIC     <div style="font-size: 0.875em; text-transform: uppercase; letter-spacing: 0.5px; color: #e8453c; margin-bottom: 4px;">Visual</div>
# MAGIC     <div style="font-weight: 700; color: #1b3a4b; font-size: 1.05em;">Data Analyst</div>
# MAGIC     <div style="font-size: 0.875em; color: #666; margin-top: 4px;">Lakeflow Designer &amp; AI/BI</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="text-align: center; font-size: 0.875em; color: #888; margin: 0 0 24px 0;">All three land on the same governed platform &mdash; <strong>Unity Catalog Delta tables</strong></div>
# MAGIC
# MAGIC <table style="width: 100% !important; max-width: 100% !important; border-collapse: collapse; table-layout: auto; margin: 0; padding: 0; border: 1px solid #ddd;">
# MAGIC   <thead>
# MAGIC     <tr style="background-color: #e0e0e0;">
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd;"><h4 style="margin: 0;">Aspect</h4></th>
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd;"><h4 style="margin: 0;">DW Engineer</h4></th>
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd; background: #fff4f0;"><h4 style="margin: 0; color: #ff6f42;">Analytics Engineer</h4></th>
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd;"><h4 style="margin: 0;">Data Analyst</h4></th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: right; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><strong>Primary Goal</strong></td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Maintain end-to-end SQL ETL with full procedural control</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd; background: #fffaf8;">Turn data into reusable analytics assets in Lakehouse SQL</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Query, visualize, and report on curated data</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: right; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><strong>Main Environment</strong></td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Lakehouse SQL Editor, Lakeflow Jobs, legacy DW tools</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd; background: #fffaf8;">Lakehouse SQL Editor, Lakeflow Jobs</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Lakehouse SQL, AI/BI Dashboards, Lakeflow Designer, Tableau/Power BI</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: right; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><strong>Typical Outputs</strong></td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Migrated stored procedures, multi-statement ETL scripts, complex business logic</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd; background: #fffaf8;">Semantic models, curated Gold tables, metric views</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Dashboards, reports, ad-hoc analyses, self-serve data prep</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: right; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><strong>Tech Emphasis</strong></td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Stored procedures (ANSI SQL/PSM), SQL scripting, control flow, multi-statement transactions</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd; background: #fffaf8;">SQL pipelines of Materialized Views and Streaming Tables, Lakeflow Jobs for SQL tasks, dimensional modeling</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">SQL queries, BI tools, no-code visual ETL (Lakeflow Designer), natural language exploration</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: right; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><strong>SQL Skill Level</strong></td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Advanced</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd; background: #fffaf8;">Intermediate</td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">Beginner to intermediate</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
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
# MAGIC         <strong style="color: #00695c;">Why the Differentiator Is Complexity, Not Language</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>All three personas in the table above are <strong>SQL-first</strong> &mdash; the spectrum runs from simple to complex, not from one language to another</li>
# MAGIC           <li>DW engineers value <strong>explicit control</strong> &mdash; stored procedures with conditional branching, loops, and multi-statement transactions</li>
# MAGIC           <li>Analytics engineers want <strong>reliable pipelines with less plumbing</strong> &mdash; describe what you want, the platform handles how</li>
# MAGIC           <li>Data analysts want <strong>self-serve access</strong> &mdash; governed outputs without writing procedural code</li>
# MAGIC           <li>When organizations don't clearly define these roles, DW engineers get pulled into last-mile BI work, analysts can't self-serve, and the analytics engineer gap creates bottlenecks</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Bridging Skills: Data Analyst &rarr; Analytics Engineer</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Advanced SQL</strong> &mdash; window functions, CTEs, performance tuning</li>
# MAGIC           <li><strong>Dimensional modeling</strong> &mdash; facts, dimensions, star schemas, surrogate keys</li>
# MAGIC           <li><strong>ELT-in-warehouse</strong> &mdash; pipelines of materialized views and streaming tables rather than external orchestration</li>
# MAGIC           <li><strong>Scheduling and orchestration</strong> &mdash; SQL tasks in Lakeflow Jobs with ForEach, If/else branching</li>
# MAGIC           <li>If you already have these skills, you are already functioning as an analytics engineer &mdash; this course gives you the formal toolkit to do it on Databricks</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">The Analytics Engineer in Practice</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; At <a href="https://www.databricks.com/customers/columbia">Columbia Sportswear</a>, the shift to declarative SQL pipelines achieved a <strong>48x ETL improvement</strong> &mdash; from 4 hours to 5 minutes. This is the kind of impact analytics engineers deliver when they own the transformation layer with the right tools</li>
# MAGIC           <li>A specialty chemicals manufacturer eliminated two separate cloud data platforms after consolidating 6 ERPs into Databricks &mdash; declarative SQL became their largest workload, maintained by analytics engineers rather than a centralized data engineering team</li>
# MAGIC           <li>The pattern is consistent: organizations that formalize the analytics engineer role and equip them with declarative SQL tools <strong>reduce pipeline development time</strong>, <strong>lower operational overhead</strong>, and <strong>free data engineers</strong> to focus on infrastructure rather than last-mile BI work</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. The SQL ETL Spectrum

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. From Simple to Complex
# MAGIC
# MAGIC Section A introduced three SQL personas. Those personas map to a **spectrum of SQL ETL tools** on Databricks, ordered by the control they offer. As you move from left to right, you gain more control but also take on more complexity and maintenance. The right tool depends on the nature of your transformation, not some abstract preference.
# MAGIC
# MAGIC <!-- ── Button Expand Panels — The SQL ETL Spectrum ── -->
# MAGIC <div style="max-width: 920px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <style>
# MAGIC .sp-row { display: flex; gap: 0; width: 100%; box-sizing: border-box; }
# MAGIC .sp-box { flex: 1; min-height: 80px; background: #F9F7F4; border-top: 6px solid transparent; border-left: 1px solid #E8E3DC; border-right: 1px solid #E8E3DC; border-bottom: 1px solid #E8E3DC; padding: 12px 8px; text-align: center; box-sizing: border-box; cursor: pointer; user-select: none; transition: transform 0.12s, background 0.2s; position: relative; }
# MAGIC .sp-box:first-child { border-radius: 8px 0 0 0; }
# MAGIC .sp-box:last-child { border-radius: 0 8px 0 0; }
# MAGIC .sp-box:hover { transform: translateY(-2px); }
# MAGIC .sp-box.sp-active { background: #fff; border-bottom-color: transparent; }
# MAGIC .sp-box .sp-label { display: block; font-size: 14pt; font-weight: 700; color: #0b2026; line-height: 1.3; pointer-events: none; }
# MAGIC .sp-box .sp-sub { display: block; font-size: 0.85em; color: #888; margin-top: 4px; pointer-events: none; }
# MAGIC .sp-arrow-bar { display: flex; align-items: center; justify-content: center; padding: 0; margin: -1px 0 0 0; background: #fff; border-left: 1px solid #E8E3DC; border-right: 1px solid #E8E3DC; }
# MAGIC .sp-arrow-inner { display: flex; align-items: center; width: 100%; padding: 6px 16px; }
# MAGIC .sp-arrow-line { flex: 1; height: 2px; background: linear-gradient(90deg, #00A972, #ff6f42, #1565c0, #7b1fa2); border-radius: 1px; }
# MAGIC .sp-arrow-left { font-size: 0.875em; color: #00A972; font-weight: 600; margin-right: 8px; white-space: nowrap; }
# MAGIC .sp-arrow-right { font-size: 0.875em; color: #7b1fa2; font-weight: 600; margin-left: 8px; white-space: nowrap; }
# MAGIC .sp-detail-wrap { overflow: hidden; max-height: 0; opacity: 0; transition: max-height 0.35s ease, opacity 0.28s ease; }
# MAGIC .sp-detail-wrap.sp-open { max-height: 600px; opacity: 1; }
# MAGIC .sp-detail-card { background: #F9F7F4; border-radius: 0 0 10px 10px; padding: 24px 28px; box-sizing: border-box; border: 1px solid #E8E3DC; border-top: none; }
# MAGIC .sp-detail-card .sp-d-title { font-size: 14pt; font-weight: 700; color: #0b2026; margin-bottom: 16px; }
# MAGIC .sp-detail-card .sp-d-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; font-size: 10.5pt; line-height: 1.6; color: #1B3139; }
# MAGIC .sp-d-item { background: #fff; border-radius: 8px; padding: 14px 16px; border-left: 4px solid #ccc; }
# MAGIC .sp-d-item .sp-d-label { font-weight: 700; color: #618794; font-size: 14pt; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; display: flex; align-items: center; gap: 6px; }
# MAGIC .sp-d-item .sp-d-label .sp-d-icon { font-size: 14pt; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="background: #F8F9FC; border: 2px solid #1B5162; border-radius: 10px; padding: 0; box-sizing: border-box; overflow: hidden;">
# MAGIC
# MAGIC   <div class="sp-row">
# MAGIC     <div class="sp-box sp-active" data-sp="0" onclick="spSel(0)" style="border-top-color:#00A972;">
# MAGIC       <span class="sp-label">Lakeflow Designer</span>
# MAGIC       <span class="sp-sub">Data Analysts</span>
# MAGIC     </div>
# MAGIC     <div class="sp-box" data-sp="1" onclick="spSel(1)" style="border-top-color:#ff6f42;">
# MAGIC       <span class="sp-label">Materialized Views / Streaming Tables</span>
# MAGIC       <span class="sp-sub">Analytics Engineers</span>
# MAGIC     </div>
# MAGIC     <div class="sp-box" data-sp="2" onclick="spSel(2)" style="border-top-color:#1565c0;">
# MAGIC       <span class="sp-label">Lakeflow SDP</span>
# MAGIC       <span class="sp-sub">Data Engineers</span>
# MAGIC     </div>
# MAGIC     <div class="sp-box" data-sp="3" onclick="spSel(3)" style="border-top-color:#7b1fa2;">
# MAGIC       <span class="sp-label">Stored Procedures</span>
# MAGIC       <span class="sp-sub">DW Engineers</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="sp-arrow-bar">
# MAGIC     <div class="sp-arrow-inner">
# MAGIC       <span class="sp-arrow-left">Simple<br/>Less control</span>
# MAGIC       <div class="sp-arrow-line"></div>
# MAGIC       <span class="sp-arrow-right">Complex<br/>More control <span style="font-size: 14px;">&#x25B6;</span></span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="sp-detail-wrap sp-open" id="sp-detail-wrap">
# MAGIC     <div class="sp-detail-card" id="sp-detail-card"></div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="text-align: center; font-size: 0.875em; color: #888; margin: 10px 0 0 0;">All four produce <strong>Unity Catalog-governed Delta tables</strong>. Click each tab to explore.</div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var SP_DATA = [
# MAGIC   { title:"Lakeflow Designer", color:"#00A972",
# MAGIC     authoring:"No-code drag-and-drop with natural language support",
# MAGIC     patterns:"Self-serve data prep, visual pipeline building, governed outputs without writing code",
# MAGIC     execution:"Generates a Materialized View — the visual canvas is a different authoring surface",
# MAGIC     orchestration:"Built-in scheduling via Lakeflow Jobs" },
# MAGIC   { title:"Materialized Views / Streaming Tables in Lakehouse SQL", color:"#ff6f42",
# MAGIC     authoring:"Declarative SQL — describe what you want, the platform handles how",
# MAGIC     patterns:"Incremental ETL, SCD Type 1 & Type 2 via AUTO CDC, Gold-layer aggregations, metric views",
# MAGIC     execution:"Every Materialized View and Streaming Table runs on a serverless compute with automatic incremental refresh",
# MAGIC     orchestration:"Lakeflow Jobs with SQL file tasks, ForEach, If/else branching" },
# MAGIC   { title:"Lakeflow SDP", color:"#1565c0",
# MAGIC     authoring:"SQL or Python files defining multi-table pipeline DAGs",
# MAGIC     patterns:"Quality expectations, streaming ingestion, complex CDC, multi-table transformation graphs",
# MAGIC     execution:"Managed Spark Declarative Pipelines with full lifecycle control, auto-scaling, and built-in monitoring",
# MAGIC     orchestration:"Lakeflow Jobs with pipeline tasks" },
# MAGIC   { title:"Stored Procedures", color:"#7b1fa2",
# MAGIC     authoring:"Procedural SQL (ANSI SQL/PSM) — specify how step-by-step",
# MAGIC     patterns:"Control flow, cursors, condition handlers, multi-statement transactions, legacy migration",
# MAGIC     execution:"Runs on SQL warehouse compute; persisted in Unity Catalog with GRANT-based governance",
# MAGIC     orchestration:"Lakeflow Jobs with SQL tasks" }
# MAGIC ];
# MAGIC var spCur = null;
# MAGIC function spSel(id) {
# MAGIC   var wrap = document.getElementById('sp-detail-wrap');
# MAGIC   var card = document.getElementById('sp-detail-card');
# MAGIC   var d = SP_DATA[id];
# MAGIC   document.querySelectorAll('.sp-box').forEach(function(b){ b.classList.toggle('sp-active', parseInt(b.dataset.sp)===id); });
# MAGIC   spCur = id;
# MAGIC   var iA='<img src="data:image/png;base64,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" height="28" style="object-fit:contain;vertical-align:middle;margin-right:6px;">', iP='<img src="data:image/png;base64,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" height="28" style="object-fit:contain;vertical-align:middle;margin-right:6px;">', iE='<img src="data:image/png;base64,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" height="28" style="object-fit:contain;vertical-align:middle;margin-right:6px;">', iO='<img src="data:image/png;base64,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" height="28" style="object-fit:contain;vertical-align:middle;margin-right:6px;">';
# MAGIC   card.innerHTML = '<div class="sp-d-title" style="color:'+d.color+';">'+d.title+'</div>'
# MAGIC     + '<div class="sp-d-grid">'
# MAGIC     + '<div class="sp-d-item" style="border-left-color:'+d.color+';"><div class="sp-d-label">'+iA+' Authoring</div>'+d.authoring+'</div>'
# MAGIC     + '<div class="sp-d-item" style="border-left-color:'+d.color+';"><div class="sp-d-label">'+iP+' Key Patterns</div>'+d.patterns+'</div>'
# MAGIC     + '<div class="sp-d-item" style="border-left-color:'+d.color+';"><div class="sp-d-label">'+iE+' Execution</div>'+d.execution+'</div>'
# MAGIC     + '<div class="sp-d-item" style="border-left-color:'+d.color+';"><div class="sp-d-label">'+iO+' Orchestration</div>'+d.orchestration+'</div>'
# MAGIC     + '</div>';
# MAGIC   wrap.classList.add('sp-open');
# MAGIC }
# MAGIC spSel(0);
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
# MAGIC         <strong style="color: #00695c;">Deeper Look at Each Spectrum Point</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Lakeflow Designer</strong> (visual, no-code)
# MAGIC             <ul>
# MAGIC               <li>AI-powered drag-and-drop canvas with natural language support</li>
# MAGIC               <li>Generates Materialized Views</li>
# MAGIC               <li>Same scalability, governance, and maintainability as hand-written pipelines</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Materialized Views / Streaming Tables / AUTO CDC in Lakehouse SQL</strong> (declarative SQL &mdash; this course)
# MAGIC             <ul>
# MAGIC               <li><strong>Materialized views</strong> physically store query results and refresh incrementally &mdash; only changed rows are processed</li>
# MAGIC               <li><strong>Streaming tables</strong> accept streaming flows (Append, AUTO CDC) &mdash; think of them as incrementally-loading tables, not real-time streaming systems</li>
# MAGIC               <li><strong>AUTO CDC</strong> replaces ~150 lines of procedural MERGE logic with 6 lines of declarative SQL for SCD Type 1 and Type 2</li>
# MAGIC               <li>All run on <strong>serverless compute</strong> under the hood</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Lakeflow SDP</strong> (declarative pipelines for data engineers)
# MAGIC             <ul>
# MAGIC               <li>Multi-table DAGs with quality expectations, complex CDC, and unified batch + streaming</li>
# MAGIC               <li>Authored in SQL or Python files &mdash; full lifecycle control with auto-scaling and built-in monitoring</li>
# MAGIC               <li>Built on open-source Apache Spark foundations</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Stored Procedures</strong> (procedural SQL for DW engineers)
# MAGIC             <ul>
# MAGIC               <li>Full ANSI SQL/PSM: compound statements, variables, control flow (IF/WHILE/FOR), condition handlers, cursors</li>
# MAGIC               <li>Persisted in Unity Catalog with GRANT-based access control</li>
# MAGIC               <li>Multi-statement transactions in Public Preview (BEGIN TRANSACTION / COMMIT / ROLLBACK)</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Start Simple, Unlock More</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Begin with Materialized Views for reporting &rarr; add AUTO CDC for dimensions &rarr; graduate to full Spark Declarative Pipelines when you need quality gates or multi-table DAGs</li>
# MAGIC           <li>The same SQL skills carry forward at every point on the spectrum</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/columbia">Columbia Sportswear</a> achieved a <strong>48x ETL improvement</strong> (4 hours to 5 minutes)</li>
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
# MAGIC ### B2. One Platform Under the Hood
# MAGIC
# MAGIC These are not separate products — they are different authoring surfaces into the same unified platform. Regardless of how a pipeline was authored, the outputs are Unity Catalog-governed Delta tables with the same lineage, governance, and access controls.
# MAGIC
# MAGIC <!-- ── Architecture Diagram ── -->
# MAGIC <div style="margin: 20px 0 24px 0;">
# MAGIC   <!-- Authoring Layer -->
# MAGIC   <div style="text-align: center; font-size: 0.875em; text-transform: uppercase; letter-spacing: 1px; color: #999; margin-bottom: 6px;">Authoring</div>
# MAGIC   <div style="display: flex; gap: 12px; justify-content: center; flex-wrap: wrap; margin-bottom: 8px;">
# MAGIC     <div style="background: #f5f5f5; border: 1px solid #ddd; border-radius: 6px; padding: 10px 18px; text-align: center; font-size: 0.9em;">
# MAGIC       <div style="font-weight: 600; color: #333;">Lakeflow Designer</div>
# MAGIC       <div style="font-size: 0.875em; color: #888;">visual canvas</div>
# MAGIC     </div>
# MAGIC     <div style="background: #f5f5f5; border: 1px solid #ddd; border-radius: 6px; padding: 10px 18px; text-align: center; font-size: 0.9em;">
# MAGIC       <div style="font-weight: 600; color: #333;">Lakehouse SQL Editor / Notebooks</div>
# MAGIC       <div style="font-size: 0.875em; color: #888;">Materialized Views, Streaming Tables, Stored Procedures</div>
# MAGIC     </div>
# MAGIC     <div style="background: #f5f5f5; border: 1px solid #ddd; border-radius: 6px; padding: 10px 18px; text-align: center; font-size: 0.9em;">
# MAGIC       <div style="font-weight: 600; color: #333;">SQL / Python Files</div>
# MAGIC       <div style="font-size: 0.875em; color: #888;">SDP pipelines</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="text-align: center; font-size: 1.2em; color: #999; margin: 4px 0;">&#x25BC; &#x25BC; &#x25BC;</div>
# MAGIC   <!-- Execution Layer — outer banner -->
# MAGIC   <div style="text-align: center; font-size: 0.875em; text-transform: uppercase; letter-spacing: 1px; color: #999; margin-bottom: 6px;">Execution</div>
# MAGIC   <div style="background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); border: 2px solid #42a5f5; border-radius: 8px; padding: 14px 20px; text-align: center; max-width: 500px; margin: 0 auto 8px auto;">
# MAGIC     <div style="font-weight: 700; color: #1565c0; font-size: 1em;">Serverless, Scalable, AI-Powered Compute</div>
# MAGIC   </div>
# MAGIC   <!-- Orchestration Layer -->
# MAGIC   <div style="display: flex; gap: 12px; justify-content: center; flex-wrap: wrap; margin-bottom: 8px;">
# MAGIC     <div style="background: linear-gradient(135deg, #fff4f0 0%, #ffe0d0 100%); border: 2px solid #ff6f42; border-radius: 8px; padding: 14px 20px; text-align: center; max-width: 500px;">
# MAGIC       <div style="font-weight: 700; color: #ff6f42; font-size: 1em;">Lakeflow Jobs &mdash; Orchestration</div>
# MAGIC       <div style="font-size: 0.875em; color: #555;">SQL file tasks, ForEach, If/else branching, dynamic value references</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="text-align: center; font-size: 1.2em; color: #999; margin: 4px 0;">&#x25BC;</div>
# MAGIC   <!-- Output Layer -->
# MAGIC   <div style="text-align: center; font-size: 0.875em; text-transform: uppercase; letter-spacing: 1px; color: #999; margin-bottom: 6px;">Output</div>
# MAGIC   <div style="display: flex; gap: 12px; justify-content: center; flex-wrap: wrap;">
# MAGIC     <div style="background: linear-gradient(135deg, #fff8e1 0%, #ffecb3 100%); border: 1px solid #ffca28; border-radius: 8px; padding: 14px 20px; text-align: center; max-width: 500px;">
# MAGIC       <div style="font-weight: 700; color: #f57f17; font-size: 1em;">Unity Catalog-Governed Delta Tables</div>
# MAGIC       <div style="font-size: 0.875em; color: #555;">Same lineage, governance, and access controls regardless of authoring mode</div>
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
# MAGIC         <strong style="color: #00695c;">The Unifying Principle</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Whether your colleague drags and drops in Lakeflow Designer, you write declarative SQL with Materialized Views and Streaming Tables, or your DW engineer builds stored procedures &mdash; the outputs all land in the <strong>same Unity Catalog tables</strong> with the same lineage, governance, and access controls</li>
# MAGIC           <li>These are not separate products &mdash; they are different <strong>authoring surfaces</strong> into the same governed platform. The diagram above shows how each layer connects</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Authoring Layer &mdash; Where You Write</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Lakeflow Designer</strong> &mdash; visual canvas for analysts</li>
# MAGIC           <li><strong>Lakehouse SQL Editor / Notebooks</strong> &mdash; where analytics engineers write Materialized Views, Streaming Tables, and stored procedures</li>
# MAGIC           <li><strong>SQL / Python Files</strong> &mdash; where data engineers define Spark Declarative Pipeline DAGs</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Execution Layer &mdash; Serverless, Scalable, AI-Powered Compute</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>All authoring modes run on <strong>serverless compute</strong> that scales automatically and requires no cluster management &mdash; but the underlying engine differs by mode</li>
# MAGIC           <li>Materialized Views, Streaming Tables, and Lakeflow Designer outputs all run on serverless compute that is automatically provisioned.</strong></li>
# MAGIC           <li><strong>SQL Warehouse</strong> &mdash; Stored procedures, ad-hoc queries, and BI workloads run on serverless SQL warehouse compute. Billed as <strong>serverless SQL warehouse DBUs</strong></li>
# MAGIC           <li>The key point: you never provision infrastructure for either path &mdash; the platform handles compute, scaling, and optimization behind the scenes</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Orchestration Layer &mdash; Lakeflow Jobs</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Orchestrates tasks across all authoring modes in a <strong>single DAG</strong></li>
# MAGIC           <li>SQL file tasks, pipeline tasks, ForEach patterns, If/else branching, dynamic value references</li>
# MAGIC           <li>Think of it as the SQL-native orchestration engine &mdash; not just for notebooks</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Output Layer &mdash; Unity Catalog-Governed Delta Tables</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Regardless of authoring mode, outputs land in the <strong>same Unity Catalog tables</strong> with the same lineage, governance, and access controls</li>
# MAGIC           <li>Governance does not depend on how the pipeline was authored &mdash; this is the "one platform" principle</li>
# MAGIC           <li>A specialty chemicals manufacturer consolidated 6 ERPs into Databricks, eliminating two separate cloud data platforms &mdash; different teams, different modes, one governed result</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Choosing the Correct Approach

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Match the Tool to the Person
# MAGIC
# MAGIC <!-- ── Tabs Color Cards: unified decision guide ── -->
# MAGIC <div class="tcc-wrap">
# MAGIC <style>
# MAGIC .tcc-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 920px; margin: 0 auto; padding: 24px; background: #FAFAFA; border: 1px solid #E0E0E0; border-radius: 12px; }
# MAGIC .tcc-wrap input[type="radio"] { display: none; }
# MAGIC .tcc-nav { display: flex; gap: 8px; margin: 0; padding: 0; flex-wrap: wrap; }
# MAGIC .tcc-tab { flex: 1; min-width: 110px; text-align: center; padding: 10px 8px; font-size: 0.88em; font-weight: 700; letter-spacing: 0.02em; color: #1B3139; cursor: pointer; border: 2px solid #E0E0E0; border-radius: 6px; background: #FFFFFF; transition: border-color 0.15s, box-shadow 0.15s; user-select: none; position: relative; }
# MAGIC .tcc-tab::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 5px; border-radius: 4px 4px 0 0; }
# MAGIC .tcc-tab[for="tcc-r1"]::before { background: #00A972; }
# MAGIC .tcc-tab[for="tcc-r2"]::before { background: #ff6f42; }
# MAGIC .tcc-tab[for="tcc-r3"]::before { background: #7b1fa2; }
# MAGIC #tcc-r1:checked ~ .tcc-nav .tcc-tab[for="tcc-r1"] { border-color: #00A972; box-shadow: 0 0 0 1px #00A972; }
# MAGIC #tcc-r2:checked ~ .tcc-nav .tcc-tab[for="tcc-r2"] { border-color: #ff6f42; box-shadow: 0 0 0 1px #ff6f42; }
# MAGIC #tcc-r3:checked ~ .tcc-nav .tcc-tab[for="tcc-r3"] { border-color: #7b1fa2; box-shadow: 0 0 0 1px #7b1fa2; }
# MAGIC .tcc-accent { display: none; height: 5px; border-radius: 3px; margin: 16px 0 0 0; }
# MAGIC #tcc-r1:checked ~ .tcc-panels .tcc-accent-1 { display: block; background: #00A972; }
# MAGIC #tcc-r2:checked ~ .tcc-panels .tcc-accent-2 { display: block; background: #ff6f42; }
# MAGIC #tcc-r3:checked ~ .tcc-panels .tcc-accent-3 { display: block; background: #7b1fa2; }
# MAGIC .tcc-panel { display: none; padding: 20px 24px 24px 24px; background: #F9F7F4; border: 1px solid #E8E3DC; border-radius: 0 0 8px 8px; margin-top: 0; flex-direction: column; }
# MAGIC #tcc-r1:checked ~ .tcc-panels .tcc-panel:nth-of-type(2) { display: flex; }
# MAGIC #tcc-r2:checked ~ .tcc-panels .tcc-panel:nth-of-type(4) { display: flex; }
# MAGIC #tcc-r3:checked ~ .tcc-panels .tcc-panel:nth-of-type(6) { display: flex; }
# MAGIC .tcc-panel h3 { margin: 0 0 12px 0; font-size: 1.2em; color: #0b2026; font-weight: 800; }
# MAGIC .tcc-panel ul { margin: 0 0 16px 0; padding-left: 20px; line-height: 1.7; color: #1B3139; font-size: 0.95em; }
# MAGIC .tcc-panel ul li { margin-bottom: 6px; }
# MAGIC .tcc-panel code { background: #E8E3DC; padding: 2px 6px; border-radius: 4px; font-size: 0.9em; }
# MAGIC .tcc-callout { padding: 12px 16px; border-radius: 0 6px 6px 0; margin-top: 8px; font-size: 0.92em; line-height: 1.6; border-left: 4px solid; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="text-align: center; padding: 0 0 14px 0; border-bottom: 1px solid #E0E0E0; margin: 0 0 16px 0;">
# MAGIC   <div style="font-size: 1.08em; font-weight: 700; color: #1B3139; margin-bottom: 4px;">The spectrum reflects fit, not a ranking. Moving right does <u>not</u> mean "better."</div>
# MAGIC   <div style="font-size: 0.88em; color: #6B7785; line-height: 1.5;">It means taking on more control, more explicit logic, and more maintenance responsibility.</div>
# MAGIC </div>
# MAGIC
# MAGIC <input type="radio" name="tcc-grp" id="tcc-r1" checked>
# MAGIC <input type="radio" name="tcc-grp" id="tcc-r2">
# MAGIC <input type="radio" name="tcc-grp" id="tcc-r3">
# MAGIC
# MAGIC <div class="tcc-nav">
# MAGIC   <label class="tcc-tab" for="tcc-r1">Lakeflow Designer</label>
# MAGIC   <label class="tcc-tab" for="tcc-r2">Materialized Views / Streaming Tables</label>
# MAGIC   <label class="tcc-tab" for="tcc-r3">Stored Procedures</label>
# MAGIC </div>
# MAGIC
# MAGIC <div class="tcc-panels">
# MAGIC   <!-- Designer -->
# MAGIC   <div class="tcc-accent tcc-accent-1"></div>
# MAGIC   <div class="tcc-panel">
# MAGIC     <h3>Use Lakeflow Designer when&hellip;</h3>
# MAGIC     <ul>
# MAGIC       <li>Your team includes <strong>non-technical contributors</strong> who need self-serve ETL</li>
# MAGIC       <li>The pipeline logic is <strong>straightforward data prep</strong> or filtering</li>
# MAGIC       <li>You want <strong>governed outputs without writing any code</strong></li>
# MAGIC     </ul>
# MAGIC     <div style="background: rgba(0,169,114,0.06); border-radius: 6px; padding: 8px 12px; margin-top: 10px; font-size: 0.875em; color: #333;">
# MAGIC       <strong style="color: #00A972;">Misconception:</strong> <em>"Designer is just a toy."</em> &mdash; It generates real ELT code with production level pipelines.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- MVs / STs -->
# MAGIC   <div class="tcc-accent tcc-accent-2"></div>
# MAGIC   <div class="tcc-panel">
# MAGIC     <h3>Use Materialized Views / Streaming Tables in Lakehouse SQL when&hellip;</h3>
# MAGIC     <ul>
# MAGIC       <li>Your transformations are <strong>expressible as SQL queries</strong></li>
# MAGIC       <li>You want <strong>incremental processing</strong> without manual state management</li>
# MAGIC       <li>You are building <strong>Gold-layer aggregations</strong> or curated datasets</li>
# MAGIC     </ul>
# MAGIC     <div style="background: rgba(255,111,66,0.06); border-radius: 6px; padding: 8px 12px; margin-top: 10px; font-size: 0.875em; color: #333;">
# MAGIC       <strong style="color: #ff6f42;">Misconception:</strong> <em>"These are just views and tables."</em> &mdash; Every one runs on a serverless, scalable, AI-powered compute with automatic incremental refresh.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Stored Procedures -->
# MAGIC   <div class="tcc-accent tcc-accent-3"></div>
# MAGIC   <div class="tcc-panel">
# MAGIC     <h3>Use Stored Procedures when&hellip;</h3>
# MAGIC     <ul>
# MAGIC       <li>Complex business rules require <strong>conditional branching, loops, or error handling</strong></li>
# MAGIC       <li><strong>Multi-statement transactional atomicity</strong> is required</li>
# MAGIC       <li>You are <strong>migrating existing procedural ETL</strong> from SQL Server, Oracle, or Teradata</li>
# MAGIC     </ul>
# MAGIC     <div style="background: rgba(123,31,162,0.06); border-radius: 6px; padding: 8px 12px; margin-top: 10px; font-size: 0.875em; color: #333;">
# MAGIC       <strong style="color: #7b1fa2;">Misconception:</strong> <em>"Stored procedures are legacy."</em> &mdash; Procedural SQL is first-class on Databricks for complex control flow and migration scenarios.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- ── Hybrid callout ── -->
# MAGIC <div style="background: #F8F9FC; border: 1px solid #D7DEE8; border-radius: 8px; padding: 14px 20px; margin: 16px 0 8px 0; display: flex; align-items: center; gap: 14px;">
# MAGIC   <img src="../Includes/images/icons/cache-sync-icon-replace.png" height="28" style="vertical-align: middle;">
# MAGIC   <div style="color: #334155; font-size: 0.92em; line-height: 1.5;">
# MAGIC     <strong>Hybrid is the norm.</strong> Use AUTO CDC for dimensions, Materialized Views for aggregations, and stored procedures for complex business logic &mdash; all on one governed platform. Organizations rarely pick just one point on the spectrum.
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
# MAGIC         <strong style="color: #00695c;">Match the Tool to the Person, Not the Problem</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The same transformation can often be built in any of the three modes &mdash; the deciding factor is <strong>who is building and maintaining it</strong></li>
# MAGIC           <li>A team of SQL-proficient analysts will be more productive with <strong>Materialized Views and Streaming Tables</strong> than with stored procedures they cannot debug. A DW engineer migrating complex control flow will be more productive with <strong>stored procedures</strong> than trying to force declarative patterns where they don't fit</li>
# MAGIC           <li>Forcing the wrong mode on a team creates friction, slows delivery, and leads to pipelines that nobody wants to maintain</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Hybrid Is the Norm</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Organizations rarely pick just one point on the spectrum &mdash; they use <strong>multiple modes for different layers</strong></li>
# MAGIC           <li>For example, a common pattern is using Lakeflow Spark Declarative Pipelines for bronze-to-silver (where automated incremental processing adds the most value) and Lakeflow Jobs with SQL tasks for silver-to-gold (where flexibility for custom scripting is needed)</li>
# MAGIC           <li><strong>AUTO CDC in Lakehouse SQL</strong> is the bridge product &mdash; it lets teams adopt declarative patterns for SCD handling (6 lines of SQL) while keeping stored procedures for other logic, all in the same SQL warehouse environment</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Applying This to Your Team</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>When evaluating a new transformation, ask: <strong>who will build it, who will maintain it, and how complex is the logic?</strong> The answers point to the right mode</li>
# MAGIC           <li>You do not need to standardize on one mode &mdash; the platform supports all three in the same pipeline, orchestrated by Lakeflow Jobs</li>
# MAGIC           <li>As your team's skills evolve, you can progressively move transformations from procedural to declarative without replatforming</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. Which Authoring Mode Fits?
# MAGIC
# MAGIC <!-- ── Decision Flowchart: FLAT structure, all siblings, no nesting ── -->
# MAGIC <div class="df-wrap">
# MAGIC <style>
# MAGIC .df-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 680px; margin: 0 auto; padding: 24px; background: #F8F9FC; border: 1px solid #D7DEE8; border-radius: 12px; }
# MAGIC .df-wrap input[type="radio"] { display: none; }
# MAGIC .df-wrap > * { margin: 0; padding: 0; }
# MAGIC .df-q { background: #fff; border: 2px solid #1B5162; border-radius: 10px; padding: 18px 24px; text-align: center; max-width: 520px; margin: 0 auto !important; }
# MAGIC .df-q-text { font-size: 1em; font-weight: 700; color: #0b2026; line-height: 1.4; margin: 0 0 4px 0 !important; }
# MAGIC .df-q-sub { font-size: 0.875em; color: #6B7785; margin: 0 0 14px 0 !important; }
# MAGIC .df-tabs { display: flex; gap: 12px; justify-content: center; }
# MAGIC .df-tab { padding: 8px 28px; font-size: 0.88em; font-weight: 700; border: 2px solid #E0E0E0; border-radius: 6px; cursor: pointer; transition: all 0.15s; background: #fff; }
# MAGIC .df-tab-yes { color: #02A36F; border-color: #02A36F; }
# MAGIC .df-tab-no { color: #E0382B; border-color: #E0382B; }
# MAGIC .df-ar { width: 2px; height: 24px; background: #1B5162; margin: 0 auto !important; }
# MAGIC .df-ah { width: 0; height: 0; border-left: 6px solid transparent; border-right: 6px solid transparent; border-top: 8px solid #1B5162; margin: 0 auto !important; }
# MAGIC .df-ar.ar-y { margin-left: 38% !important; }
# MAGIC .df-ah.ar-y { margin-left: calc(38% - 4px) !important; }
# MAGIC .df-ar.ar-n { margin-left: 58% !important; }
# MAGIC .df-ah.ar-n { margin-left: calc(58% - 4px) !important; }
# MAGIC .df-hide { display: none !important; }
# MAGIC .df-outcome { border-radius: 10px; padding: 16px 20px; text-align: center; max-width: 480px; margin: 0 auto !important; }
# MAGIC .df-outcome-text { font-size: 0.92em; font-weight: 600; line-height: 1.5; margin: 0 !important; }
# MAGIC .df-outcome-rec { border: 2px solid #02A36F; background: rgba(2,163,111,0.04); }
# MAGIC .df-outcome-rec .df-outcome-text { color: #02A36F; }
# MAGIC .df-outcome-rec::before { content: 'RECOMMENDED'; display: block; font-size: 0.875em; font-weight: 700; color: #02A36F; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px; }
# MAGIC /* State: Root Yes (no-code → Designer) */
# MAGIC #df-a-yes:checked ~ .df-show-a-yes { display: block !important; }
# MAGIC #df-a-yes:checked ~ .q-root .df-tab-yes { background: #02A36F; color: #fff; }
# MAGIC /* State: Root No → show L2 question */
# MAGIC #df-a-no:checked ~ .df-show-a-no { display: block !important; }
# MAGIC #df-a-no:checked ~ .q-root .df-tab-no { background: #E0382B; color: #fff; }
# MAGIC /* State: L2 Yes (procedural) → show L3 migration question */
# MAGIC #df-b-yes:checked ~ .df-show-b-yes { display: block !important; }
# MAGIC #df-b-yes:checked ~ .q-l2 .df-tab-yes { background: #02A36F; color: #fff; }
# MAGIC /* State: L2 No (declarative) → show L3 CDC question */
# MAGIC #df-b-no:checked ~ .df-show-b-no { display: block !important; }
# MAGIC #df-b-no:checked ~ .q-l2 .df-tab-no { background: #E0382B; color: #fff; }
# MAGIC /* State: L3 migration Yes */
# MAGIC #df-c-yes:checked ~ .df-show-c-yes { display: block !important; }
# MAGIC #df-c-yes:checked ~ .q-l3p .df-tab-yes { background: #02A36F; color: #fff; }
# MAGIC /* State: L3 migration No */
# MAGIC #df-c-no:checked ~ .df-show-c-no { display: block !important; }
# MAGIC #df-c-no:checked ~ .q-l3p .df-tab-no { background: #E0382B; color: #fff; }
# MAGIC /* State: L3 CDC Yes */
# MAGIC #df-d-yes:checked ~ .df-show-d-yes { display: block !important; }
# MAGIC #df-d-yes:checked ~ .q-l3d .df-tab-yes { background: #02A36F; color: #fff; }
# MAGIC /* State: L3 CDC No */
# MAGIC #df-d-no:checked ~ .df-show-d-no { display: block !important; }
# MAGIC #df-d-no:checked ~ .q-l3d .df-tab-no { background: #E0382B; color: #fff; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- ALL radios at the top level — flat siblings -->
# MAGIC <input type="radio" name="df-a" id="df-a-yes">
# MAGIC <input type="radio" name="df-a" id="df-a-no">
# MAGIC <input type="radio" name="df-b" id="df-b-yes">
# MAGIC <input type="radio" name="df-b" id="df-b-no">
# MAGIC <input type="radio" name="df-c" id="df-c-yes">
# MAGIC <input type="radio" name="df-c" id="df-c-no">
# MAGIC <input type="radio" name="df-d" id="df-d-yes">
# MAGIC <input type="radio" name="df-d" id="df-d-no">
# MAGIC
# MAGIC <!-- Header -->
# MAGIC <div style="text-align: center; margin: 0 0 8px 0 !important;">
# MAGIC   <div style="font-size: 1.08em; font-weight: 700; color: #1B3139; margin-bottom: 4px;">All three modes share the same governed platform. The question is which fits your team.</div>
# MAGIC   <div style="font-size: 0.875em; color: #6B7785; margin-bottom: 12px;">Both declarative and procedural SQL run on Photon-accelerated compute &mdash; performance is <u>not</u> the differentiator between these modes.</div>
# MAGIC   <div style="font-size: 0.875em; color: #44515C;">Click <strong style="color: #02A36F;">Yes</strong> or <strong style="color: #E0382B;">No</strong> at each step to follow your path.</div>
# MAGIC </div>
# MAGIC <div class="df-ar"></div><div class="df-ah"></div>
# MAGIC
# MAGIC <!-- Q1: No-code? -->
# MAGIC <div class="df-q q-root">
# MAGIC   <p class="df-q-text">Does your team need to build this without writing code?</p>
# MAGIC   <p class="df-q-sub">(non-technical contributors, self-serve data prep, visual pipeline building)</p>
# MAGIC   <div class="df-tabs">
# MAGIC     <label class="df-tab df-tab-yes" for="df-a-yes">Yes</label>
# MAGIC     <label class="df-tab df-tab-no" for="df-a-no">No</label>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q1 Yes → Designer outcome -->
# MAGIC <div class="df-hide df-show-a-yes"><div class="df-ar ar-y"></div><div class="df-ah ar-y"></div></div>
# MAGIC <div class="df-hide df-show-a-yes df-outcome df-outcome-rec">
# MAGIC   <p class="df-outcome-text">Lakeflow Designer &mdash; no-code visual canvas that generates production-grade serverless pipelines</p>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q1 No → arrow + Q2 -->
# MAGIC <div class="df-hide df-show-a-no"><div class="df-ar ar-n"></div><div class="df-ah ar-n"></div></div>
# MAGIC <div class="df-hide df-show-a-no df-q q-l2">
# MAGIC   <p class="df-q-text">Does the transformation need explicit step-by-step control?</p>
# MAGIC   <p class="df-q-sub">(conditional branching, loops, error recovery, multi-statement atomicity)</p>
# MAGIC   <div class="df-tabs">
# MAGIC     <label class="df-tab df-tab-yes" for="df-b-yes">Yes</label>
# MAGIC     <label class="df-tab df-tab-no" for="df-b-no">No</label>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q2 Yes → arrow + Q3 (migration) -->
# MAGIC <div class="df-hide df-show-b-yes"><div class="df-ar ar-y"></div><div class="df-ah ar-y"></div></div>
# MAGIC <div class="df-hide df-show-b-yes df-q q-l3p">
# MAGIC   <p class="df-q-text">Are you migrating existing procedural ETL from another platform?</p>
# MAGIC   <p class="df-q-sub">(SQL Server, Oracle, Teradata, Synapse)</p>
# MAGIC   <div class="df-tabs">
# MAGIC     <label class="df-tab df-tab-yes" for="df-c-yes">Yes</label>
# MAGIC     <label class="df-tab df-tab-no" for="df-c-no">No</label>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q3 migration Yes → SP lift-and-shift -->
# MAGIC <div class="df-hide df-show-c-yes"><div class="df-ar ar-y"></div><div class="df-ah ar-y"></div></div>
# MAGIC <div class="df-hide df-show-c-yes df-outcome df-outcome-rec">
# MAGIC   <p class="df-outcome-text">Stored Procedures &mdash; lift-and-shift to ANSI SQL/PSM, then identify patterns that can move to declarative</p>
# MAGIC   <p style="font-size: 0.875em; color: #6B7785; font-style: italic; margin: 8px 0 0 0 !important;">Deep-dive: SQL Programming and Procedural Logic course</p>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q3 migration No → SP complex logic -->
# MAGIC <div class="df-hide df-show-c-no"><div class="df-ar ar-n"></div><div class="df-ah ar-n"></div></div>
# MAGIC <div class="df-hide df-show-c-no df-outcome df-outcome-rec">
# MAGIC   <p class="df-outcome-text">Stored Procedures &mdash; the right tool for complex conditional logic and multi-table atomicity</p>
# MAGIC   <p style="font-size: 0.875em; color: #6B7785; font-style: italic; margin: 8px 0 0 0 !important;">Deep-dive: SQL Programming and Procedural Logic course</p>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q2 No → arrow + Q3 (CDC) -->
# MAGIC <div class="df-hide df-show-b-no"><div class="df-ar ar-n"></div><div class="df-ah ar-n"></div></div>
# MAGIC <div class="df-hide df-show-b-no df-q q-l3d">
# MAGIC   <p class="df-q-text">Is this primarily change application or history tracking?</p>
# MAGIC   <p class="df-q-sub">(upserts/deletes to dimension tables, SCD Type 1 or Type 2 patterns)</p>
# MAGIC   <div class="df-tabs">
# MAGIC     <label class="df-tab df-tab-yes" for="df-d-yes">Yes</label>
# MAGIC     <label class="df-tab df-tab-no" for="df-d-no">No</label>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q3 CDC Yes → AUTO CDC -->
# MAGIC <div class="df-hide df-show-d-yes"><div class="df-ar ar-y"></div><div class="df-ah ar-y"></div></div>
# MAGIC <div class="df-hide df-show-d-yes df-outcome df-outcome-rec">
# MAGIC   <p class="df-outcome-text"><strong>Declarative:</strong> AUTO CDC &mdash; replace ~150 lines of MERGE logic with 6 lines of declarative SQL</p>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Q3 CDC No → MVs/STs -->
# MAGIC <div class="df-hide df-show-d-no"><div class="df-ar ar-n"></div><div class="df-ah ar-n"></div></div>
# MAGIC <div class="df-hide df-show-d-no df-outcome df-outcome-rec">
# MAGIC   <p class="df-outcome-text"><strong>Declarative:</strong> Materialized Views &amp; Streaming Tables &mdash; incremental derived tables with automatic refresh</p>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC (function(){
# MAGIC   var w = document.currentScript.parentElement;
# MAGIC   var groups = {
# MAGIC     'df-a': ['df-b','df-c','df-d'],
# MAGIC     'df-b': ['df-c','df-d']
# MAGIC   };
# MAGIC   w.addEventListener('change', function(e) {
# MAGIC     var n = e.target.name;
# MAGIC     if (groups[n]) {
# MAGIC       groups[n].forEach(function(g) {
# MAGIC         w.querySelectorAll('input[name="'+g+'"]').forEach(function(r) { r.checked = false; });
# MAGIC       });
# MAGIC     }
# MAGIC   });
# MAGIC })();
# MAGIC </script>
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
# MAGIC         <strong style="color: #00695c;">Walking Through the Decision Flowchart</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>First question: does your team need to build this without writing code?</strong>
# MAGIC             <ul>
# MAGIC               <li>If yes &rarr; <strong>Lakeflow Designer</strong> provides a no-code visual canvas that generates real serverless pipeline code under the hood</li>
# MAGIC               <li>If no &rarr; you are writing SQL, and the next question determines which style</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Second question: does the transformation need explicit step-by-step control?</strong>
# MAGIC             <ul>
# MAGIC               <li>If yes &rarr; <strong>Stored Procedures</strong> with ANSI SQL/PSM. The follow-up question (migrating or net-new?) determines whether you lift-and-shift existing logic or build new procedural patterns</li>
# MAGIC               <li>If no &rarr; declarative SQL, and the third question narrows the tool</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li><strong>Third question: is this primarily change application or history tracking?</strong>
# MAGIC             <ul>
# MAGIC               <li>If yes &rarr; <strong>AUTO CDC</strong> handles SCD Type 1 and Type 2 declaratively, replacing ~150 lines of manual MERGE with ~6 lines</li>
# MAGIC               <li>If no &rarr; <strong>Materialized Views and Streaming Tables</strong> for incremental derived tables with automatic refresh</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Key Technical Details</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Performance is not the differentiator</strong> &mdash; both declarative and procedural SQL run on Photon-accelerated compute. The decision should be driven by authoring model, team expertise, and maintenance burden</li>
# MAGIC           <li><strong>Stored procedures and Materialized Views can coexist</strong> &mdash; Lakeflow Jobs orchestrates both as tasks in a single DAG, so you can mix procedural and declarative steps in the same pipeline</li>
# MAGIC           <li><strong>ANSI SQL/PSM standard</strong> &mdash; stored procedures on Databricks follow ANSI SQL/PSM (not T-SQL or PL/SQL), so syntax differs from SQL Server or Oracle</li>
# MAGIC           <li><strong>Lakebridge</strong> can automate up to 80% of the migration from legacy SQL dialects to Databricks-compatible ANSI SQL</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">In Practice</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Hybrid is the norm</strong> &mdash; use AUTO CDC for change-tracking dimensions, Materialized Views for aggregations and derived tables, and stored procedures for complex business logic that resists declarative expression</li>
# MAGIC           <li>Different layers and different teams within the same organization often use different modes &mdash; all landing in the same governed Unity Catalog tables</li>
# MAGIC           <li>The flowchart above helps you identify which path fits <strong>each transformation</strong> in your pipeline &mdash; you do not need to pick one mode for everything</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. See the Difference
# MAGIC
# MAGIC The best way to understand the spectrum is to see the **same transformation** expressed in all three modes. Below is a Gold-layer aggregation &mdash; total revenue by product category &mdash; built three different ways. Click each tab to compare.
# MAGIC
# MAGIC <!-- ── CSS Tabs: 3-mode code comparison ── -->
# MAGIC <div class="c3t-wrap">
# MAGIC <style>
# MAGIC .c3t-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 820px; margin: 0 auto; padding: 0; }
# MAGIC .c3t-wrap input[type="radio"] { display: none; }
# MAGIC .c3t-nav { display: flex; gap: 0; border-bottom: 2px solid #E8E3DC; margin: 0; padding: 0; }
# MAGIC .c3t-nav label { display: inline-block; padding: 12px 28px; font-size: 0.92em; font-weight: 600; color: #1B5162; cursor: pointer; border-bottom: 3px solid transparent; margin-bottom: -2px; transition: color 0.15s, border-color 0.15s; user-select: none; background: none; }
# MAGIC .c3t-nav label:hover { color: #FF3621; }
# MAGIC .c3t-panel { display: none; padding: 24px 20px; background: #F9F7F4; border: 1px solid #E8E3DC; border-top: none; border-radius: 0 0 8px 8px; line-height: 1.6; color: #1B3139; font-size: 0.92em; min-height: 100px; }
# MAGIC .c3t-code { background: #1B3139; color: #e0e0e0; border-radius: 6px; padding: 16px; font-family: 'SFMono-Regular', Consolas, monospace; font-size: 0.88em; line-height: 1.6; white-space: pre; overflow-x: auto; margin: 12px 0 0 0; }
# MAGIC .c3t-code .kw { color: #7ecbf5; } .c3t-code .fn { color: #dcdcaa; } .c3t-code .str { color: #ce9178; } .c3t-code .cm { color: #6a9955; }
# MAGIC .c3t-stats { display: flex; gap: 16px; margin-top: 14px; }
# MAGIC .c3t-stat { background: #fff; border: 1px solid #E8E3DC; border-radius: 6px; padding: 10px 14px; flex: 1; text-align: center; }
# MAGIC .c3t-stat-val { font-size: 1.3em; font-weight: 700; color: #1B3139; }
# MAGIC .c3t-stat-label { font-size: 0.875em; color: #6B7785; margin-top: 2px; }
# MAGIC #c3t-1:checked ~ .c3t-nav label[for="c3t-1"] { color: #00A972; border-bottom-color: #00A972; }
# MAGIC #c3t-1:checked ~ .c3t-panels .c3t-panel:nth-of-type(1) { display: block; }
# MAGIC #c3t-2:checked ~ .c3t-nav label[for="c3t-2"] { color: #ff6f42; border-bottom-color: #ff6f42; }
# MAGIC #c3t-2:checked ~ .c3t-panels .c3t-panel:nth-of-type(2) { display: block; }
# MAGIC #c3t-3:checked ~ .c3t-nav label[for="c3t-3"] { color: #7b1fa2; border-bottom-color: #7b1fa2; }
# MAGIC #c3t-3:checked ~ .c3t-panels .c3t-panel:nth-of-type(3) { display: block; }
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" name="c3t-grp" id="c3t-1">
# MAGIC <input type="radio" name="c3t-grp" id="c3t-2" checked>
# MAGIC <input type="radio" name="c3t-grp" id="c3t-3">
# MAGIC
# MAGIC <div class="c3t-nav">
# MAGIC   <label for="c3t-1">Visual (Lakeflow Designer)</label>
# MAGIC   <label for="c3t-2">Declarative (Materialized View)</label>
# MAGIC   <label for="c3t-3">Procedural (Stored Procedure)</label>
# MAGIC </div>
# MAGIC
# MAGIC <div class="c3t-panels">
# MAGIC   <div class="c3t-panel">
# MAGIC     <p><strong>In Lakeflow Designer, you would:</strong></p>
# MAGIC     <ol style="margin: 8px 0 0 0; padding-left: 20px;">
# MAGIC       <li>Drag <strong>source nodes</strong> for <code>raw_orders</code>, <code>raw_products</code>, and <code>raw_customers</code></li>
# MAGIC       <li>Add <strong>join nodes</strong> connecting orders to products (on product_id) and orders to customers (on customer_id)</li>
# MAGIC       <li>Add a <strong>filter</strong> for completed orders and a <strong>calculated column</strong> for revenue (quantity &times; unit_price &times; (1 - discount_pct))</li>
# MAGIC       <li>Add an <strong>aggregate transformation</strong>: total revenue, order count, and average order value grouped by customer</li>
# MAGIC       <li>Connect the output to a <strong>target table</strong> named <code>gold_customer_order_summary</code></li>
# MAGIC       <li>Click <strong>Publish</strong> &mdash; Designer generates serverless pipeline code and handles scheduling automatically</li>
# MAGIC     </ol>
# MAGIC     <div style="margin-top: 12px; padding: 10px 14px; background: rgba(0,169,114,0.06); border-left: 3px solid #00A972; border-radius: 0 6px 6px 0; font-size: 0.875em;">
# MAGIC       <strong>No code written.</strong> The visual canvas handles joins, filters, calculations, and aggregations. Scheduling and incremental refresh are managed by the platform.
# MAGIC     </div>
# MAGIC     <div class="c3t-stats">
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">0</div><div class="c3t-stat-label">Lines of code</div></div>
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">Low</div><div class="c3t-stat-label">SQL skill required</div></div>
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">Auto</div><div class="c3t-stat-label">Refresh &amp; scheduling</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="c3t-panel">
# MAGIC     <p><strong>With a Materialized View in Lakehouse SQL:</strong></p>
# MAGIC     <div class="c3t-code">CREATE OR REPLACE MATERIALIZED VIEW
# MAGIC   gold_customer_order_summary
# MAGIC AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.first_name,
# MAGIC   c.last_name,
# MAGIC   c.loyalty_tier,
# MAGIC   COUNT(o.order_id)                          AS order_count,
# MAGIC   SUM(o.quantity * o.unit_price
# MAGIC       * (1 - o.discount_pct))                AS total_revenue,
# MAGIC   AVG(o.quantity * o.unit_price
# MAGIC       * (1 - o.discount_pct))                AS avg_order_value
# MAGIC FROM raw_orders o
# MAGIC JOIN raw_customers c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_status = 'completed'
# MAGIC GROUP BY c.customer_id, c.first_name,
# MAGIC          c.last_name, c.loyalty_tier;</div>
# MAGIC     <div style="margin-top: 12px; padding: 10px 14px; background: rgba(255,111,66,0.06); border-left: 3px solid #ff6f42; border-radius: 0 6px 6px 0; font-size: 0.875em;">
# MAGIC       <strong>You describe what, the platform handles how.</strong> Incremental refresh, scheduling, and error recovery are all managed automatically. No staging, no cleanup, no logging code.
# MAGIC     </div>
# MAGIC     <div class="c3t-stats">
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">~14</div><div class="c3t-stat-label">Lines of SQL</div></div>
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">Intermediate</div><div class="c3t-stat-label">SQL skill required</div></div>
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">Auto</div><div class="c3t-stat-label">Refresh &amp; scheduling</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="c3t-panel">
# MAGIC     <p><strong>With a stored procedure:</strong></p>
# MAGIC     <div class="c3t-code">CREATE OR REPLACE PROCEDURE
# MAGIC   refresh_customer_order_summary(
# MAGIC     IN min_order_date DATE DEFAULT '2020-01-01'
# MAGIC   )
# MAGIC   LANGUAGE SQL
# MAGIC   SQL SECURITY INVOKER
# MAGIC AS BEGIN
# MAGIC   -- Variables for validation and logging
# MAGIC   DECLARE source_count INT;
# MAGIC   DECLARE rows_affected INT;
# MAGIC
# MAGIC   -- Validate: ensure source has data
# MAGIC   SET source_count = (
# MAGIC     SELECT COUNT(*) FROM raw_orders
# MAGIC     WHERE order_date >= min_order_date
# MAGIC   );
# MAGIC   IF source_count = 0 THEN
# MAGIC     SIGNAL SQLSTATE '45000'
# MAGIC     SET MESSAGE_TEXT =
# MAGIC       'No orders found for the given date range';
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Step 1: Build clean staging table
# MAGIC   DROP TABLE IF EXISTS _tmp_clean_orders;
# MAGIC   CREATE TABLE _tmp_clean_orders AS
# MAGIC   SELECT
# MAGIC     o.order_id,
# MAGIC     o.customer_id,
# MAGIC     o.quantity * o.unit_price
# MAGIC       * (1 - COALESCE(o.discount_pct, 0))
# MAGIC       AS revenue
# MAGIC   FROM raw_orders o
# MAGIC   WHERE o.order_status = 'completed'
# MAGIC     AND o.order_date >= min_order_date
# MAGIC     AND o.unit_price IS NOT NULL
# MAGIC     AND o.quantity > 0;
# MAGIC
# MAGIC   -- Step 2: Build the summary
# MAGIC   DROP TABLE IF EXISTS gold_customer_order_summary;
# MAGIC   CREATE TABLE gold_customer_order_summary AS
# MAGIC   SELECT
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.loyalty_tier,
# MAGIC     COUNT(t.order_id) AS order_count,
# MAGIC     SUM(t.revenue)    AS total_revenue,
# MAGIC     AVG(t.revenue)    AS avg_order_value
# MAGIC   FROM _tmp_clean_orders t
# MAGIC   JOIN raw_customers c
# MAGIC     ON t.customer_id = c.customer_id
# MAGIC   GROUP BY c.customer_id, c.first_name,
# MAGIC            c.last_name, c.loyalty_tier;
# MAGIC
# MAGIC   -- Step 3: Capture row count
# MAGIC   SET rows_affected = (
# MAGIC     SELECT COUNT(*)
# MAGIC     FROM gold_customer_order_summary
# MAGIC   );
# MAGIC
# MAGIC   -- Step 4: Log the refresh
# MAGIC   UPDATE gold_table_config SET
# MAGIC     last_refresh = current_timestamp()
# MAGIC   WHERE table_name =
# MAGIC     'gold_customer_order_summary';
# MAGIC
# MAGIC   -- Step 5: Cleanup
# MAGIC   DROP TABLE IF EXISTS _tmp_clean_orders;
# MAGIC END;</div>
# MAGIC     <div style="margin-top: 12px; padding: 10px 14px; background: rgba(123,31,162,0.06); border-left: 3px solid #7b1fa2; border-radius: 0 6px 6px 0; font-size: 0.875em;">
# MAGIC       <strong>Full control, full responsibility.</strong> Parameters, input validation, null handling, staging, cleanup, and refresh logging are all explicit. This is practical when you need conditional logic, parameterized execution, or multi-step coordination that a Materialized View cannot express.
# MAGIC     </div>
# MAGIC     <div class="c3t-stats">
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">~50</div><div class="c3t-stat-label">Lines of SQL</div></div>
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">Advanced</div><div class="c3t-stat-label">SQL skill required</div></div>
# MAGIC       <div class="c3t-stat"><div class="c3t-stat-val">Manual</div><div class="c3t-stat-label">State management</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
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
# MAGIC         <strong style="color: #00695c;">Three-Mode Comparison</strong>
# MAGIC         <table style="width: 100%; border-collapse: collapse; margin: 8px 0 16px 0; border: 1px solid #b2dfdb;">
# MAGIC           <thead>
# MAGIC             <tr style="background-color: #e0f2f1;">
# MAGIC               <th style="padding: 10px 12px; text-align: left; border: 1px solid #b2dfdb; font-size: 0.9em;">Criterion</th>
# MAGIC               <th style="padding: 10px 12px; text-align: center; border: 1px solid #b2dfdb; font-size: 0.9em;">Visual (Designer)</th>
# MAGIC               <th style="padding: 10px 12px; text-align: center; border: 1px solid #b2dfdb; font-size: 0.9em;">Declarative (Materialized Views / Streaming Tables)</th>
# MAGIC               <th style="padding: 10px 12px; text-align: center; border: 1px solid #b2dfdb; font-size: 0.9em;">Procedural (Stored Procedures)</th>
# MAGIC             </tr>
# MAGIC           </thead>
# MAGIC           <tbody>
# MAGIC             <tr>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-weight: 600; font-size: 0.875em;">Authoring</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Drag-and-drop, natural language</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Describe <em>what</em> you want &mdash; system handles <em>how</em></td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Specify <em>how</em> step-by-step with control flow</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-weight: 600; font-size: 0.875em;">Persona</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Data Analysts</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Analytics Engineers</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">DW Engineers</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-weight: 600; font-size: 0.875em;">Error handling</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Managed by framework</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Framework handles retries and failures</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Condition handlers (SIGNAL/RESIGNAL)</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-weight: 600; font-size: 0.875em;">Transactions</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Single-statement per refresh</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Single-statement per refresh</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Multi-statement (Public Preview)</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-weight: 600; font-size: 0.875em;">Execution</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Generates serverless pipeline code under the hood</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Serverless Spark Declarative Pipelines</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">SQL warehouse compute</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-weight: 600; font-size: 0.875em;">Best for</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Self-serve data prep, non-technical teams</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Incremental ETL, CDC/SCD, Gold-layer aggregations</td>
# MAGIC               <td style="padding: 8px 12px; border: 1px solid #b2dfdb; font-size: 0.875em;">Complex control flow, legacy migration, multi-table atomicity</td>
# MAGIC             </tr>
# MAGIC           </tbody>
# MAGIC         </table>
# MAGIC         <strong style="color: #00695c;">Understanding the Three Approaches</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Think of it as <strong>transportation</strong>: visual is calling a ride-share (tap a destination), declarative is an autonomous vehicle (you say where to go, it figures out how), and procedural is driving a manual car (full control, you shift gears)</li>
# MAGIC           <li>The <strong>Visual</strong> tab shows Lakeflow Designer &mdash; no SQL written, but the output is real pipeline code. Joins, filters, aggregations, and scheduling are all handled visually</li>
# MAGIC           <li>The <strong>Declarative</strong> tab shows a Materialized View &mdash; ~14 lines that define the transformation as a single query. The platform handles incremental refresh, scheduling, and error recovery automatically</li>
# MAGIC           <li>The <strong>Procedural</strong> tab shows a practical stored procedure &mdash; ~50 lines with parameters, input validation, staging, cleanup, and refresh logging. Every step is explicit, which gives you full control when you need it</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">What the Stored Procedure Adds (and Why)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>IN parameters</strong> &mdash; the procedure accepts a <code>min_order_date</code> so the same logic can be reused for different date ranges without rewriting the query</li>
# MAGIC           <li><strong>Input validation</strong> &mdash; the IF/THEN/SIGNAL pattern checks that source data exists before doing expensive work, failing early with a clear error message</li>
# MAGIC           <li><strong>Null handling and data quality</strong> &mdash; COALESCE on discount_pct and explicit filters on null prices and zero quantities ensure clean data in the staging table</li>
# MAGIC           <li><strong>Multi-step coordination</strong> &mdash; staging, aggregation, row count capture, refresh logging, and temp table cleanup run in a specific sequence that the procedure controls</li>
# MAGIC           <li>None of these capabilities are available in a Materialized View or Lakeflow Designer &mdash; this is where stored procedures earn their complexity</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Migration Is an Opportunity to Move Along the Spectrum</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/pepsico">PepsiCo</a> had stored procedures to migrate but explicitly chose to modernize, not just re-platform &mdash; they moved from procedural to declarative during migration</li>
# MAGIC           <li>A global insurance company took the opposite approach: migrated all 39 stored procedures from SQL Server as-is (same paradigm, better platform), with zero escalations</li>
# MAGIC           <li>Both strategies are valid &mdash; the right choice depends on your team's timeline and strategic direction</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Course Focus</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>This course focuses on the <strong>declarative side</strong> of the spectrum &mdash; the code comparison above shows why that is the recommended starting point for most SQL ETL patterns</li>
# MAGIC           <li>The <strong>SQL Programming and Procedural Logic</strong> course is the deep-dive path for stored procedures and SQL scripting</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. What You Will Be Building

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. The Building Blocks: Materialized Views and Streaming Tables
# MAGIC
# MAGIC The declarative tools at the center of the spectrum &mdash; the ones this course teaches &mdash; are built on two core objects. Before we look at the data model you will build with them, let's understand what each one does and when to use it.
# MAGIC
# MAGIC <!-- ── Two-Pane Comparison: MV vs ST ── -->
# MAGIC <div style="max-width: 920px; margin: 16px auto; font-family: sans-serif; display: flex; gap: 20px;">
# MAGIC   <!-- Materialized Views card -->
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); position: relative; padding: 22px 22px 16px 22px; box-sizing: border-box;">
# MAGIC     <div style="position: absolute; top: 0; left: 0; width: 100%; height: 8px; background: #ff6f42; border-radius: 8px 8px 0 0;"></div>
# MAGIC     <div style="font-size: 1.1em; font-weight: 700; color: #ff6f42; text-align: center; margin: 8px 0 10px 0;">Materialized Views</div>
# MAGIC     <div style="font-size: 0.875em; color: #555; text-align: center; margin-bottom: 10px; font-style: italic;">A table that stores precomputed query results and refreshes incrementally</div>
# MAGIC     <ul style="margin: 0 0 12px 0; padding-left: 18px; font-size: 0.875em; line-height: 1.7; color: #333;">
# MAGIC       <li>Stores results as a <strong>Delta table</strong> &mdash; not just a query plan</li>
# MAGIC       <li>Only <strong>changed data</strong> is reprocessed on refresh</li>
# MAGIC       <li>Best for <strong>aggregations, joins, derived columns</strong></li>
# MAGIC     </ul>
# MAGIC     <div style="background: #1B3139; color: #e0e0e0; border-radius: 6px; padding: 10px 12px; font-family: monospace; font-size: 0.875em; line-height: 1.5; white-space: pre; overflow-x: auto;">CREATE OR REPLACE MATERIALIZED VIEW
# MAGIC   gold_customer_summary
# MAGIC AS
# MAGIC SELECT customer_id,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(quantity * unit_price)
# MAGIC     AS total_revenue
# MAGIC FROM raw_orders
# MAGIC GROUP BY customer_id;</div>
# MAGIC   </div>
# MAGIC   <!-- Streaming Tables card -->
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); position: relative; padding: 22px 22px 16px 22px; box-sizing: border-box;">
# MAGIC     <div style="position: absolute; top: 0; left: 0; width: 100%; height: 8px; background: #1B5162; border-radius: 8px 8px 0 0;"></div>
# MAGIC     <div style="font-size: 1.1em; font-weight: 700; color: #1B5162; text-align: center; margin: 8px 0 10px 0;">Streaming Tables</div>
# MAGIC     <div style="font-size: 0.875em; color: #555; text-align: center; margin-bottom: 10px; font-style: italic;">A table that ingests data incrementally with exactly-once guarantees</div>
# MAGIC     <ul style="margin: 0 0 12px 0; padding-left: 18px; font-size: 0.875em; line-height: 1.7; color: #333;">
# MAGIC       <li>Appends new rows &mdash; <strong>never reprocesses</strong> old data</li>
# MAGIC       <li><strong>Auto Loader</strong> handles file discovery and checkpointing</li>
# MAGIC       <li>Best for <strong>ingestion from files, CDC feeds, event streams</strong></li>
# MAGIC     </ul>
# MAGIC     <div style="background: #1B3139; color: #e0e0e0; border-radius: 6px; padding: 10px 12px; font-family: monospace; font-size: 0.875em; line-height: 1.5; white-space: pre; overflow-x: auto;">CREATE OR REPLACE STREAMING TABLE
# MAGIC   bronze_orders
# MAGIC AS SELECT *
# MAGIC FROM STREAM read_files(
# MAGIC   '/volumes/raw/orders/',
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC );</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- When to use which — prominent callout -->
# MAGIC <div style="background: #F8F9FC; border: 1px solid #D7DEE8; border-radius: 10px; padding: 16px 20px; margin: 16px auto; max-width: 920px;">
# MAGIC   <div style="text-align: center; font-weight: 700; color: #1B3139; font-size: 1em; margin-bottom: 10px;">When to use which?</div>
# MAGIC   <div style="display: flex; gap: 16px;">
# MAGIC     <div style="flex: 1; text-align: center;">
# MAGIC       <div style="font-weight: 700; color: #ff6f42; margin-bottom: 4px;">Materialized View</div>
# MAGIC       <div style="font-size: 0.875em; color: #333;">Your transformation is expressible as a <strong>SELECT</strong> &mdash; aggregations, joins, filters, derived columns</div>
# MAGIC     </div>
# MAGIC     <div style="width: 2px; background: #D7DEE8;"></div>
# MAGIC     <div style="flex: 1; text-align: center;">
# MAGIC       <div style="font-weight: 700; color: #1B5162; margin-bottom: 4px;">Streaming Table</div>
# MAGIC       <div style="font-size: 0.875em; color: #333;">Data arrives incrementally and is <strong>append-only</strong> &mdash; file ingestion, CDC change feeds, event streams</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="text-align: center; margin-top: 12px; padding-top: 10px; border-top: 1px solid #D7DEE8; font-size: 0.875em; color: #333;">
# MAGIC     Both are <strong>Unity Catalog managed tables</strong> powered by serverless compute with automatic scaling. The next module covers these in depth.
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
# MAGIC         <strong style="color: #00695c;">Materialized Views in Detail</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A Materialized View is a <strong>Unity Catalog managed Delta table</strong> that physically stores precomputed query results &mdash; not just a query plan like a regular view</li>
# MAGIC           <li>Think of it as a <strong>pre-built report that sits on a shelf</strong> &mdash; you don't rebuild it from scratch each time; you update only the pages that changed</li>
# MAGIC           <li>The <strong>Enzyme optimizer</strong> automatically determines whether an incremental or full refresh is cheaper, then executes the optimal strategy</li>
# MAGIC           <li>You write standard batch SQL (<code>SELECT</code> with JOINs, GROUP BY, window functions) and the platform incrementalizes it for you &mdash; no manual state tracking</li>
# MAGIC           <li>All refresh compute runs on a <strong>serverless pipeline</strong>, not the SQL warehouse &mdash; the warehouse only coordinates the operation</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Streaming Tables in Detail</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A Streaming Table is a <strong>Unity Catalog managed Delta table</strong> with built-in support for streaming and incremental processing</li>
# MAGIC           <li>Think of it as a <strong>conveyor belt</strong> &mdash; items go on once and move through in order, never going back. Each row is processed exactly once</li>
# MAGIC           <li><strong>Auto Loader</strong> (accessed via <code>read_files()</code> in SQL) handles file discovery, schema inference, and exactly-once guarantees using a persistent checkpoint</li>
# MAGIC           <li>Streaming Tables do <strong>not continuously run</strong> &mdash; they process only on REFRESH (manual, scheduled, or triggered). Think of them as incrementally-loading tables, not real-time streaming systems</li>
# MAGIC           <li>The <code>_rescued_data</code> column automatically captures any fields that don't match the known schema &mdash; nothing is silently dropped</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Choosing Between Them</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Is it ingestion from files or external sources?</strong> &rarr; Streaming Table (Materialized Views cannot use Auto Loader)</li>
# MAGIC           <li><strong>Is it an aggregation, join, or derived transformation?</strong> &rarr; Materialized View</li>
# MAGIC           <li><strong>Is it append-only with no updates or deletes?</strong> &rarr; Streaming Table is simpler</li>
# MAGIC           <li><strong>Does the source data get updated or deleted?</strong> &rarr; Materialized View (it can recompute to reflect changes)</li>
# MAGIC           <li><strong>Rule of thumb:</strong> Use Streaming Tables for ingestion, Materialized Views for transformation. When in doubt at the Silver layer, Materialized Views are the safer default</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">What Runs Under the Hood</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Both Materialized Views and Streaming Tables are powered by <strong>serverless compute pipelines</strong> &mdash; the same engine, regardless of which object you create</li>
# MAGIC           <li>The SQL warehouse is like a <strong>receptionist</strong> who takes your order; the serverless pipeline is the <strong>kitchen</strong> that cooks the food</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Facts, Dimensions, and Star Schemas
# MAGIC
# MAGIC Now you know the building blocks &mdash; Materialized Views for transformations and Streaming Tables for ingestion. The question is: **what data will you build with them?** This course uses a star schema &mdash; a central fact table surrounded by dimension tables, connected by foreign keys. You will use Materialized Views to aggregate fact data into Gold-layer data marts, Streaming Tables to ingest raw data incrementally, and AUTO CDC to keep dimension tables current as source records change (covered in depth in Module 3). This is the data model you will transform, aggregate, and maintain throughout the modules ahead.
# MAGIC
# MAGIC <!-- ── Two-Pane: Facts vs Dimensions ── -->
# MAGIC <div style="max-width: 900px; margin: 16px auto; font-family: sans-serif; display: flex; gap: 20px;">
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); position: relative; padding: 22px; box-sizing: border-box;">
# MAGIC     <div style="position: absolute; top: 0; left: 0; width: 100%; height: 8px; background: #1B3139; border-radius: 8px 8px 0 0;"></div>
# MAGIC     <div style="font-size: 1.1em; font-weight: 700; color: #1B3139; text-align: center; margin: 8px 0 12px 0;">Fact Tables</div>
# MAGIC     <ul style="margin: 0; padding-left: 18px; font-size: 0.9em; line-height: 1.7; color: #333;">
# MAGIC       <li>Record <strong>what happened</strong> &mdash; transactions, events, measurements</li>
# MAGIC       <li><strong>Large and append-heavy</strong> &mdash; grow continuously over time</li>
# MAGIC       <li>Contain <strong>foreign keys</strong> to dimension tables + <strong>numeric measures</strong> (amount, quantity)</li>
# MAGIC       <li>Examples: orders, page views, sensor readings, financial transactions</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); position: relative; padding: 22px; box-sizing: border-box;">
# MAGIC     <div style="position: absolute; top: 0; left: 0; width: 100%; height: 8px; background: #ff6f42; border-radius: 8px 8px 0 0;"></div>
# MAGIC     <div style="font-size: 1.1em; font-weight: 700; color: #ff6f42; text-align: center; margin: 8px 0 12px 0;">Dimension Tables</div>
# MAGIC     <ul style="margin: 0; padding-left: 18px; font-size: 0.9em; line-height: 1.7; color: #333;">
# MAGIC       <li>Describe <strong>who, what, where, when, why</strong> &mdash; the context around each fact</li>
# MAGIC       <li><strong>Smaller but change over time</strong> &mdash; customers move, products get re-categorized</li>
# MAGIC       <li>Provide the <strong>descriptive attributes</strong> that make facts meaningful</li>
# MAGIC       <li>Examples: customers, products, stores, dates, employees</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ── Star Schema + Questions: stacked ── -->
# MAGIC <div style="max-width: 1100px; margin: 0 auto;">
# MAGIC
# MAGIC <!-- Top: Mermaid diagram (full width) -->
# MAGIC <div>
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     FB["<b style='font-size:1.1em'>customer_feedback (Fact)</b><br/><small style='color:#999'><i>(Streaming Table)</i></small><br/><small style='color:#E5A100'><b>PK:</b> feedback_id</small><br/><small style='color:#2574B5'><b>FK:</b> order_id</small><br/><small style='color:#666'>rating, feedback_text</small>"]
# MAGIC     CUST["<b style='font-size:1.1em'>raw_customers</b><br/><small style='color:#999'><i>(Streaming Table &mdash; with CDC)</i></small><br/><small style='color:#E5A100'><b>PK:</b> customer_id</small><br/><small style='color:#666'>name, email, city</small><br/><small style='color:#666'>segment, loyalty_tier</small>"]
# MAGIC     ORD["<b style='font-size:1.1em'>raw_orders (Fact)</b><br/><small style='color:#999'><i>(Streaming Table)</i></small><br/><small style='color:#E5A100'><b>PK:</b> order_id</small><br/><small style='color:#2574B5'><b>FK:</b> customer_id</small><br/><small style='color:#2574B5'><b>FK:</b> product_id</small><br/><small style='color:#666'>order_date, qty, unit_price</small>"]
# MAGIC     PROD["<b style='font-size:1.1em'>raw_products</b><br/><small style='color:#999'><i>(Streaming Table &mdash; with CDC)</i></small><br/><small style='color:#E5A100'><b>PK:</b> product_id</small><br/><small style='color:#666'>product_name, category</small><br/><small style='color:#666'>brand, cost_price, list_price</small>"]
# MAGIC     FB e1@== "order_id" ==> ORD
# MAGIC     ORD e2@== "product_id" ==> PROD
# MAGIC     ORD e3@== "customer_id" ==> CUST
# MAGIC     e1@{ animate: true }
# MAGIC     e2@{ animate: true }
# MAGIC     e3@{ animate: true }
# MAGIC     classDef fact fill:#1C3037,color:#fff,stroke:#1C3037,stroke-width:3px
# MAGIC     classDef dim fill:#FFF4F0,color:#0b2026,stroke:#ff6f42,stroke-width:2px
# MAGIC     class ORD fact
# MAGIC     class CUST,PROD dim
# MAGIC     class FB fact
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Legend -->
# MAGIC <div style="display: flex; justify-content: center; gap: 24px; margin-top: 8px; font-size: 0.875em; color: #555;">
# MAGIC   <div style="display: flex; align-items: center; gap: 6px;"><span style="display: inline-block; width: 14px; height: 14px; background: #1C3037; border-radius: 3px;"></span> Fact tables (raw_orders, customer_feedback) &mdash; Streaming Tables</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 6px;"><span style="display: inline-block; width: 14px; height: 14px; background: #FFF4F0; border: 2px solid #ff6f42; border-radius: 3px;"></span> Dimension tables (raw_customers, raw_products) &mdash; Streaming Tables with CDC</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 6px;"><span style="display: inline-block; width: 14px; height: 14px; background: #02A36F; border-radius: 3px;"></span> Answers &mdash; Materialized Views</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Below: Questions panel (full width, horizontal) -->
# MAGIC <div style="margin-top: 16px;">
# MAGIC   <div style="background: #F8F9FC; border: 1px solid #D7DEE8; border-radius: 10px; padding: 16px 18px;">
# MAGIC     <div style="font-size: 0.875em; font-weight: 700; color: #1B3139; margin-bottom: 12px; text-align: center;">One schema, many questions &mdash; each adds complexity</div>
# MAGIC     <div style="display: flex; gap: 10px; flex-wrap: wrap;" id="erd-questions">
# MAGIC       <div class="erd-q" data-nodes="FB,ORD" data-edges="e1" data-sql="CREATE OR REPLACE MATERIALIZED VIEW gold_order_feedback_correlation AS&#10;SELECT&#10;  CASE WHEN o.quantity * o.unit_price > 500 THEN 'high' ELSE 'standard' END AS order_tier,&#10;  AVG(f.rating) AS avg_rating, COUNT(*) AS feedback_count&#10;FROM customer_feedback f&#10;JOIN raw_orders o ON f.order_id = o.order_id&#10;GROUP BY order_tier;" style="flex: 1; min-width: 200px; background: #fff; border-left: 4px solid #02A36F; border-radius: 0 8px 8px 0; padding: 10px 14px; cursor: pointer; transition: box-shadow 0.2s;">
# MAGIC         <div style="font-size: 0.875em; font-weight: 700; color: #02A36F; margin-bottom: 4px;">Q1 &bull; 2 tables, 1 join</div>
# MAGIC         <div style="font-size: 0.875em; color: #333; line-height: 1.4;"><em>"Do high-value orders correlate with better feedback?"</em></div>
# MAGIC         <div style="font-size: 0.875em; color: #888; margin-top: 4px;">feedback &rarr; orders (unit_price, qty)</div>
# MAGIC       </div>
# MAGIC       <div class="erd-q" data-nodes="FB,ORD,PROD" data-edges="e1,e2" data-sql="CREATE OR REPLACE MATERIALIZED VIEW gold_feedback_by_category AS&#10;SELECT&#10;  p.category, AVG(f.rating) AS avg_rating, COUNT(*) AS feedback_count&#10;FROM customer_feedback f&#10;JOIN raw_orders o ON f.order_id = o.order_id&#10;JOIN raw_products p ON o.product_id = p.product_id&#10;GROUP BY p.category;" style="flex: 1; min-width: 200px; background: #fff; border-left: 4px solid #2574B5; border-radius: 0 8px 8px 0; padding: 10px 14px; cursor: pointer; transition: box-shadow 0.2s;">
# MAGIC         <div style="font-size: 0.875em; font-weight: 700; color: #2574B5; margin-bottom: 4px;">Q2 &bull; 3 tables, 2 joins</div>
# MAGIC         <div style="font-size: 0.875em; color: #333; line-height: 1.4;"><em>"What is the average feedback rating by product category?"</em></div>
# MAGIC         <div style="font-size: 0.875em; color: #888; margin-top: 4px;">feedback &rarr; orders &rarr; products (category)</div>
# MAGIC       </div>
# MAGIC       <div class="erd-q" data-nodes="ORD,CUST,PROD" data-edges="e2,e3" data-sql="CREATE OR REPLACE MATERIALIZED VIEW gold_revenue_by_tier_category AS&#10;SELECT&#10;  c.loyalty_tier, p.category,&#10;  SUM(o.quantity * o.unit_price) AS total_revenue&#10;FROM raw_orders o&#10;JOIN raw_customers c ON o.customer_id = c.customer_id&#10;JOIN raw_products p ON o.product_id = p.product_id&#10;GROUP BY c.loyalty_tier, p.category;" style="flex: 1; min-width: 200px; background: #fff; border-left: 4px solid #E5A100; border-radius: 0 8px 8px 0; padding: 10px 14px; cursor: pointer; transition: box-shadow 0.2s;">
# MAGIC         <div style="font-size: 0.875em; font-weight: 700; color: #E5A100; margin-bottom: 4px;">Q3 &bull; 3 tables, 2 joins (two dims)</div>
# MAGIC         <div style="font-size: 0.875em; color: #333; line-height: 1.4;"><em>"Which product categories are driving the most revenue for each loyalty tier?"</em></div>
# MAGIC         <div style="font-size: 0.875em; color: #888; margin-top: 4px;">orders (qty &times; unit_price) &rarr; customers (loyalty_tier) + products (category)</div>
# MAGIC       </div>
# MAGIC       <div class="erd-q" data-nodes="FB,ORD,CUST,PROD" data-edges="e1,e2,e3" data-sql="CREATE OR REPLACE MATERIALIZED VIEW gold_platinum_feedback_by_category AS&#10;SELECT&#10;  p.category, AVG(f.rating) AS avg_rating, COUNT(*) AS feedback_count&#10;FROM customer_feedback f&#10;JOIN raw_orders o ON f.order_id = o.order_id&#10;JOIN raw_customers c ON o.customer_id = c.customer_id&#10;JOIN raw_products p ON o.product_id = p.product_id&#10;WHERE c.loyalty_tier = 'platinum'&#10;GROUP BY p.category;" style="flex: 1; min-width: 200px; background: #fff; border-left: 4px solid #7b1fa2; border-radius: 0 8px 8px 0; padding: 10px 14px; cursor: pointer; transition: box-shadow 0.2s;">
# MAGIC         <div style="font-size: 0.875em; font-weight: 700; color: #7b1fa2; margin-bottom: 4px;">Q4 &bull; 4 tables, 3 joins</div>
# MAGIC         <div style="font-size: 0.875em; color: #333; line-height: 1.4;"><em>"Which product categories are platinum-tier customers giving the best feedback on?"</em></div>
# MAGIC         <div style="font-size: 0.875em; color: #888; margin-top: 4px;">feedback &rarr; orders &rarr; customers (loyalty_tier) + products (category)</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- SQL display area -->
# MAGIC     <div id="erd-sql-display" style="display: none; margin-top: 12px; background: #1B3139; color: #e0e0e0; border-radius: 8px; padding: 14px 16px; font-family: monospace; font-size: 0.875em; line-height: 1.5; white-space: pre; overflow-x: auto;">
# MAGIC     </div>
# MAGIC     <div style="text-align: center; font-size: 0.875em; color: #6B7785; margin-top: 12px; font-style: italic;">Click a question to highlight its join path and see the Materialized View that answers it.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({
# MAGIC   startOnLoad: true,
# MAGIC   theme: "base",
# MAGIC   themeVariables: {
# MAGIC     background: "#FFFFFF",
# MAGIC     lineColor: "#1B5162",
# MAGIC     primaryColor: "#FFF4F0",
# MAGIC     primaryBorderColor: "#ff6f42"
# MAGIC   }
# MAGIC });
# MAGIC // Wait for mermaid to render, then set up click highlighting
# MAGIC setTimeout(function() {
# MAGIC   var svg = document.querySelector('.mermaid svg');
# MAGIC   if (!svg) return;
# MAGIC   var allNodes = svg.querySelectorAll('.node');
# MAGIC   var allEdges = svg.querySelectorAll('.edge-pattern-dotted, .edge-thickness-normal, .edge-thickness-thick, [class*="edge"]');
# MAGIC   var allPaths = svg.querySelectorAll('path, line');
# MAGIC   var activeQ = null;
# MAGIC   document.querySelectorAll('.erd-q').forEach(function(card) {
# MAGIC     card.addEventListener('click', function() {
# MAGIC       var nodes = card.dataset.nodes.split(',');
# MAGIC       var edges = card.dataset.edges.split(',');
# MAGIC       if (activeQ === card) { activeQ = null; resetAll(); return; }
# MAGIC       activeQ = card;
# MAGIC       // Dim everything
# MAGIC       allNodes.forEach(function(n) { n.style.opacity = '0.15'; });
# MAGIC       svg.querySelectorAll('[id*="edge"], [id*="e1"], [id*="e2"], [id*="e3"]').forEach(function(e) { e.style.opacity = '0.1'; });
# MAGIC       // Highlight matching nodes
# MAGIC       nodes.forEach(function(nid) {
# MAGIC         svg.querySelectorAll('[id*="' + nid + '"]').forEach(function(el) {
# MAGIC           if (el.classList.contains('node')) el.style.opacity = '1';
# MAGIC         });
# MAGIC       });
# MAGIC       // Highlight matching edges
# MAGIC       edges.forEach(function(eid) {
# MAGIC         svg.querySelectorAll('[id*="' + eid + '"]').forEach(function(el) { el.style.opacity = '1'; });
# MAGIC       });
# MAGIC       // Highlight active card
# MAGIC       document.querySelectorAll('.erd-q').forEach(function(c) { c.style.boxShadow = 'none'; c.style.opacity = '0.5'; });
# MAGIC       card.style.boxShadow = '0 2px 12px rgba(0,0,0,0.15)';
# MAGIC       card.style.opacity = '1';
# MAGIC       // Show SQL
# MAGIC       var sqlBox = document.getElementById('erd-sql-display');
# MAGIC       if (card.dataset.sql) {
# MAGIC         sqlBox.textContent = card.dataset.sql.replace(/&#10;/g, '\n');
# MAGIC         sqlBox.style.display = 'block';
# MAGIC       }
# MAGIC     });
# MAGIC   });
# MAGIC   function resetAll() {
# MAGIC     allNodes.forEach(function(n) { n.style.opacity = '1'; });
# MAGIC     svg.querySelectorAll('[id*="edge"], [id*="e1"], [id*="e2"], [id*="e3"]').forEach(function(e) { e.style.opacity = '1'; });
# MAGIC     document.querySelectorAll('.erd-q').forEach(function(c) { c.style.boxShadow = 'none'; c.style.opacity = '1'; });
# MAGIC     document.getElementById('erd-sql-display').style.display = 'none';
# MAGIC   }
# MAGIC }, 2000);
# MAGIC </script>
# MAGIC
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
# MAGIC         <strong style="color: #00695c;">What Is a Fact Table?</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A fact table records <strong>what happened</strong> &mdash; transactions, events, measurements. Think of each row as a receipt: it captures an order, a page view, a sensor reading, or a financial transaction</li>
# MAGIC           <li>Each row contains two types of columns:
# MAGIC             <ul>
# MAGIC               <li><strong>Foreign keys</strong> that point to dimension tables (customer_id, product_id) &mdash; these are the "who" and "what" of the event</li>
# MAGIC               <li><strong>Numeric measures</strong> that you aggregate in queries (quantity, unit_price, shipping_cost) &mdash; these are the "how much"</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li>Fact tables are typically the <strong>largest tables</strong> in the warehouse because they grow continuously as new events accumulate (append-heavy)</li>
# MAGIC           <li>In the diagram above, <strong>raw_orders</strong> is the fact table &mdash; every order is a row with FKs to customers and products, plus measures like quantity and unit_price</li>
# MAGIC           <li><strong>customer_feedback</strong> is also a fact table &mdash; it records individual feedback events linked to orders via order_id. Each row captures a rating and comment about a specific transaction, making it an event/transaction table rather than a dimension</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">What Is a Dimension Table?</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A dimension table describes <strong>who, what, where, when, and why</strong> &mdash; the context that makes facts meaningful. Without dimensions, a fact row is just numbers and IDs</li>
# MAGIC           <li>Dimension tables are typically <strong>smaller</strong> than fact tables, but they are the tables that <strong>change over time</strong>: customers move to new cities, products get re-categorized, employees change roles</li>
# MAGIC           <li>How you handle those changes determines your <strong>SCD pattern</strong> (Type 1 overwrites, Type 2 keeps history) &mdash; covered in a later module with AUTO CDC</li>
# MAGIC           <li>In the diagram above, <strong>raw_customers</strong> (name, email, loyalty_tier) and <strong>raw_products</strong> (product_name, category, brand) are dimension tables</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">What Is a Star Schema?</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A star schema is the most common dimensional model: a <strong>central fact table</strong> surrounded by <strong>dimension tables</strong>, connected by foreign keys</li>
# MAGIC           <li>Named after its visual shape &mdash; the fact table is the center, dimensions radiate outward like points on a star</li>
# MAGIC           <li>Optimized for analytical queries because joins are <strong>simple and predictable</strong>: every query follows the pattern <em>"Customer X bought Product Y on Date Z"</em></li>
# MAGIC           <li>The key principle: <strong>dimensions do not join directly to each other</strong> &mdash; the fact table is always the hub. In the diagram, customer_feedback reaches products by going through raw_orders, not directly</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Reading the Join Path Questions (Q1&ndash;Q4)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Each question demonstrates that the <strong>same set of tables answers many different business questions</strong> &mdash; the star schema is designed for this flexibility</li>
# MAGIC           <li>Notice how complexity increases: Q1 touches 2 tables, Q4 touches all 4 &mdash; but the join pattern is always <strong>FK &rarr; PK through the fact table</strong></li>
# MAGIC           <li>This is why dimensional modeling matters for analytics engineers: you design the schema once, and it serves dashboards, reports, Genie Spaces, and ad-hoc queries without restructuring</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #00695c;">Why Not a Single Wide Table?</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A common alternative is a <strong>denormalized wide table</strong> with all columns in one place &mdash; no joins required</li>
# MAGIC           <li>The tradeoff: wide tables create <strong>tight coupling</strong> with BI tools, make updates difficult (changing a customer's address means updating every row they appear in), and reduce query performance at scale</li>
# MAGIC           <li>Fact/dimension models with primary and foreign key constraints provide <strong>flexibility, scalability, and clearer semantics</strong> &mdash; the recommended production pattern on Databricks</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <div style="background: #FFF9C4; border-left: 4px solid #F9A825; padding: 12px 16px; border-radius: 4px; margin: 16px 0; font-size: 0.95em;">
# MAGIC <strong>Note:</strong> This course covers the data modeling fundamentals needed to build ETL pipelines with SQL. For a comprehensive deep dive into data modeling &mdash; including star schemas, slowly changing dimensions, and advanced modeling patterns &mdash; see the dedicated <a href="https://www.databricks.com/training/catalog/data-modeling-strategies-3730">Data Modeling Strategies</a> course.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC This lecture introduced the modern SQL ETL landscape on Databricks &mdash; from the analytics engineer role that this course serves, through the three authoring modes and the spectrum of tools, to the declarative building blocks and star schema data model you will build with them.
# MAGIC
# MAGIC Key takeaways from this lecture include:
# MAGIC
# MAGIC - **The analytics engineer owns the middle:** This SQL-first role bridges DW engineers and data analysts, owning the Silver-to-Gold transformation layer using Materialized Views, Streaming Tables, and Lakeflow Jobs.
# MAGIC - **Three modes, one platform:** Visual (Lakeflow Designer), declarative (Materialized Views, Streaming Table), and procedural (stored procedures) are different entry points into the same governed platform. The spectrum reflects fit, not a ranking &mdash; match the mode to the team, not just the problem.
# MAGIC - **Choose based on your logic, not performance:** Both declarative and procedural SQL run on Photon-accelerated compute — performance is not the differentiator between these modes. The decision framework in Section C helps you pick the right approach for each transformation.
# MAGIC - **Materialized Views and Streaming Tables are the building blocks:** Materialized Views precompute and incrementally refresh transformations. Streaming Tables ingest data incrementally with exactly-once guarantees. Together they power the declarative pipelines you will build throughout this course.
# MAGIC - **Facts, dimensions, and the star schema are what you build:** The course dataset &mdash; orders, customers, products, and feedback &mdash; forms a star schema where one set of tables answers many different analytical questions.
# MAGIC
# MAGIC In the next module, you will get hands-on with the SQL Editor and the course dataset, and begin building your first materialized views and streaming tables.
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Procedural vs. Declarative Data Processing (<a href="https://docs.databricks.com/aws/en/data-engineering/procedural-vs-declarative">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/data-engineering/procedural-vs-declarative">Azure</a> | <a href="https://docs.databricks.com/gcp/en/data-engineering/procedural-vs-declarative">GCP</a>) &mdash; the official selection guidance for choosing between stored procedures and declarative patterns</li>
# MAGIC         <li>Materialized Views in Lakehouse SQL (<a href="https://docs.databricks.com/aws/en/ldp/dbsql/materialized">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/dbsql/materialized">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/dbsql/materialized">GCP</a>) &mdash; creating, refreshing, and scheduling Materialized Views on serverless compute</li>
# MAGIC         <li>Change Data Capture and SCD Patterns (<a href="https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/what-is-change-data-capture">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/what-is-change-data-capture">GCP</a>) &mdash; AUTO CDC for SCD Type 1 and Type 2 (covered in a later module)</li>
# MAGIC         <li>The <strong>SQL Programming and Procedural Logic</strong> course &mdash; the deep-dive path for stored procedures and SQL scripting</li>
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
