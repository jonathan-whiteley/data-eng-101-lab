# Databricks notebook source
# Create-Job-4.3-Lab.py
# Programmatically creates the Feedback Pipeline Lab job
# matching sections D1-D8 of Lab 4.3.
# Must run on Serverless Compute (not SQL Warehouse).

# COMMAND ----------

import requests, json

# Get workspace connection details
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Derive catalog from username (same logic as Classroom-Setup-Common)
username = spark.sql("SELECT current_user()").first()[0]
safe_name = username.split("@")[0].replace(".", "_").replace("-", "_").lower()
catalog = f"labuser_{safe_name}"
job_name = f"Feedback Pipeline Lab — {username}"

# Verify catalog exists
try:
    spark.sql(f"DESCRIBE CATALOG `{catalog}`")
    print(f"Catalog: {catalog}")
except Exception as e:
    raise ValueError(f"Catalog '{catalog}' not found. Run the Classroom Setup (Section A) first.")

# Determine SQL file paths from this notebook's location
notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
# Find the course root
# When called via %run, notebookPath() returns the CALLING notebook's path
# (e.g., .../Building-ETL-SQL/Module 4 - .../4.3 Lab - ...), not this file's path.
# The caller is in a module subfolder, so we go up 2 levels to reach the course root.
parts = notebook_path.split("/")
if "Includes" in parts:
    # Direct execution: this notebook's path includes Includes/
    course_root = "/".join(parts[:parts.index("Includes")])
else:
    # %run context: caller is in a module subfolder, go up 2 levels
    course_root = "/".join(parts[:-2])

# Jobs API expects paths without /Workspace prefix
if course_root.startswith("/Workspace"):
    course_root = course_root[len("/Workspace"):]

sql_path = f"{course_root}/Module 4 - Orchestrating with Lakeflow Jobs/lab_pipeline_sql"
print(f"Notebook path: {notebook_path}")
print(f"Course root:   {course_root}")
print(f"SQL path:      {sql_path}")

# Find a SQL warehouse
wh_response = requests.get(f"https://{host}/api/2.0/sql/warehouses", headers=headers)
warehouses = wh_response.json().get("warehouses", [])
warehouse_id = None
for wh in warehouses:
    if wh.get("state") == "RUNNING":
        warehouse_id = wh["id"]
        break
if not warehouse_id and warehouses:
    warehouse_id = warehouses[0]["id"]

assert warehouse_id, "No SQL warehouse found. Ensure a SQL warehouse exists in this workspace."
print(f"Warehouse: {warehouse_id}")

# COMMAND ----------

# Delete existing job with the same name to avoid duplicates
list_response = requests.get(
    f"https://{host}/api/2.1/jobs/list",
    headers=headers,
    params={"name": job_name}
)
for job in list_response.json().get("jobs", []):
    print(f"Deleting existing job: {job['job_id']}")
    requests.post(f"https://{host}/api/2.1/jobs/delete", headers=headers, json={"job_id": job["job_id"]})

# COMMAND ----------

# Helper to build a SQL File task
def sql_task(task_key, filename, depends_on=None):
    task = {
        "task_key": task_key,
        "sql_task": {
            "file": {
                "path": f"{sql_path}/{filename}",
                "source": "WORKSPACE"
            },
            "warehouse_id": warehouse_id
        }
    }
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    return task

# Create the 5-task job matching sections D1-D8
job_payload = {
    "name": job_name,
    "tasks": [
        sql_task("refresh_bronze", "bonus/lab_refresh_bronze_solved.sql"),
        sql_task("refresh_cdc",    "lab_refresh_cdc.sql"),
        sql_task("refresh_silver", "lab_refresh_silver.sql",
                 depends_on=["refresh_bronze", "refresh_cdc"]),
        sql_task("refresh_gold",   "lab_refresh_gold.sql",
                 depends_on=["refresh_silver"]),
        sql_task("build_report",   "bonus/lab_build_report_solved.sql",
                 depends_on=["refresh_gold"]),
    ],
    "parameters": [
        {"name": "catalog",          "default": catalog},
        {"name": "report_dimension", "default": "product_category"}
    ],
    "trigger": {
        "periodic": {
            "interval": 1,
            "unit": "HOURS"
        },
        "pause_status": "PAUSED"
    }
}

create_response = requests.post(
    f"https://{host}/api/2.1/jobs/create",
    headers=headers,
    json=job_payload
)

if create_response.status_code != 200:
    raise Exception(f"Job creation failed: {create_response.text}")

job_id = create_response.json()["job_id"]

print(f"\nJob created successfully!")
print(f"  Job ID:     {job_id}")
print(f"  Job name:   {job_name}")
print(f"  Tasks:      refresh_bronze, refresh_cdc, refresh_silver, refresh_gold, build_report")
print(f"  Parameters: catalog={catalog}, report_dimension=product_category")
print(f"  Trigger:    Every 1 hour (PAUSED)")
print(f"\nOpen the job in Jobs & Pipelines to view or run it.")
