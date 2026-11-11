# Databricks notebook source

# MAGIC %md
# MAGIC # Platform Tour: Architecture, Delta Lake, Unity Catalog and Compute
# MAGIC **Databricks Data Engineer Associate series · Lesson A01 · Exam guide (Aug 2026) Section 1, Databricks Intelligence Platform (6%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless). Self-contained fictional study-plan rows. No downloads, packages, external paths, or paid-only features. Run from the top.
# MAGIC
# MAGIC Objectives covered: core components (architecture, Delta Lake, Unity Catalog); compute services, limitations and cost models (serverless notebook vs SQL warehouse hands-on; classic all-purpose, jobs compute, pools and DBUs as concepts, because Free Edition is serverless only).
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_platform_<12 hex>` and one managed table `study_plan`. No overwrite, no DROP of existing objects. A full rerun creates another small schema. Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: where am I?
# MAGIC Unity Catalog uses a three-level namespace: `catalog.schema.table`. Read the current catalog instead of hard coding it.

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").first()[0]
print("current catalog:", catalog)
print("current schema:", spark.sql("SELECT current_schema()").first()[0])

spark.sql(f"SHOW SCHEMAS IN {catalog}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: create a schema and a managed table
# MAGIC No `LOCATION` means a **managed** table: Unity Catalog manages the files and their lifecycle. Delta is the default table format on Databricks.

# COMMAND ----------

import uuid
schema = f"bt_platform_{uuid.uuid4().hex[:12]}"     # new every run
spark.sql(f"CREATE SCHEMA {catalog}.{schema} COMMENT 'Build and Teach A01'")
table_fqn = f"{catalog}.{schema}.study_plan"

spark.sql(f"""CREATE TABLE {table_fqn} (
  lesson_id STRING, section STRING, topic STRING, weight_pct INT
) COMMENT 'Fictional study-plan rows'""")   # no LOCATION = managed
print("created", table_fqn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: insert three rows and read them back
# MAGIC The weights are the Associate exam-guide section weights; the rows themselves are fictional.

# COMMAND ----------

spark.sql(f"""INSERT INTO {table_fqn} VALUES
  ('A01', 'S1', 'Platform', 6),
  ('A02', 'S2', 'Ingestion', 21),
  ('A07', 'S3', 'Transformation', 22)""")

plan = spark.table(table_fqn)
plan.orderBy("lesson_id").show()
assert plan.count() == 3
print("CHECK: 3 rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: DESCRIBE DETAIL and the table type
# MAGIC `format` should be `delta`; `DESCRIBE TABLE EXTENDED` shows `Type = MANAGED`.

# COMMAND ----------

detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}")
detail.select("format", "numFiles", "sizeInBytes", "createdAt").show(truncate=False)
assert detail.first()["format"] == "delta"

ext = spark.sql(f"DESCRIBE TABLE EXTENDED {table_fqn}")
ext.filter("col_name = 'Type'").show(truncate=False)        # MANAGED

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: UPDATE, then DESCRIBE HISTORY
# MAGIC Every commit to the Delta transaction log becomes a new version. Expect CREATE TABLE, WRITE (the insert) and UPDATE. Background maintenance such as predictive optimization can add extra versions later.

# COMMAND ----------

spark.sql(f"""UPDATE {table_fqn}
  SET topic = 'Transformation and modeling'
  WHERE lesson_id = 'A07'""")

hist = spark.sql(f"DESCRIBE HISTORY {table_fqn}")
hist.select("version", "operation").orderBy("version").show(truncate=False)
assert "UPDATE" in [r.operation for r in hist.collect()]
print("Query this table from the SQL editor with:", table_fqn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same table, different compute: the SQL warehouse
# MAGIC Free Edition includes **one** SQL warehouse, limited to **2X-Small** (in a new workspace it is the *Serverless Starter Warehouse*). Open **SQL Editor**, select that warehouse, and run (use the name printed above):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT lesson_id, section, topic, weight_pct
# MAGIC FROM workspace.bt_platform_xxxxxxxxxxxx.study_plan
# MAGIC ORDER BY lesson_id;
# MAGIC ```
# MAGIC
# MAGIC Same three rows and the same Unity Catalog governance; only the compute changes.
# MAGIC
# MAGIC **Paid workspaces only (concepts):** classic all-purpose compute (interactive, custom size/runtime, auto termination), jobs compute (created per job run), instance pools (idle instances for faster start). Cost model: DBUs billed per second; classic compute also incurs cloud VM cost; jobs compute has a lower DBU rate than all-purpose; pools charge no DBUs while idle (the cloud provider still bills the instances).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge
# MAGIC Predict before you run: `DELETE FROM study_plan WHERE weight_pct < 10`, then `DESCRIBE HISTORY`.
# MAGIC 1. How many rows remain?  2. What is the newest operation?  3. Can you still read the A01 row?
# MAGIC
# MAGIC **Explanation:** 2 rows remain (A02, A07). The newest history entry is DELETE (a new version). Yes: time travel with `VERSION AS OF` the version before the delete, because Delta keeps older data files until `VACUUM` removes them after the retention period. Uncomment the cell below to check.

# COMMAND ----------

# from pyspark.sql import functions as F
# spark.sql(f"DELETE FROM {table_fqn} WHERE weight_pct < 10")
# latest = spark.sql(f"DESCRIBE HISTORY {table_fqn}").orderBy(F.desc("version")).first()
# print(latest.version, latest.operation)                      # DELETE
# assert spark.table(table_fqn).count() == 2
# spark.sql(f"SELECT * FROM {table_fqn} VERSION AS OF {latest.version - 1} WHERE lesson_id = 'A01'").show()
