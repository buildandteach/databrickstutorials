# Databricks notebook source

# MAGIC %md
# MAGIC # Auto Loader Schema Enforcement and Evolution
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A04 · Exam guide (Aug 2026) Section 2, Data Ingestion and Loading (21%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless). Self-contained fictional orders. No downloads, packages, external paths, or paid-only features. Run from the top.
# MAGIC
# MAGIC Objective covered: use Auto Loader with schema enforcement and evolution: `cloudFiles.schemaLocation`, default inference (strings for CSV and JSON), `cloudFiles.inferColumnTypes`, `cloudFiles.schemaHints`, `cloudFiles.schemaEvolutionMode`, the `_rescued_data` column, and `mergeSchema` on the Delta write. Serverless supports `trigger(availableNow=True)`, so each run processes the files that are there and stops.
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_autoloader_` + 12 hex characters, two managed volumes (`landing`, `checkpoints`) and one managed table (`orders_bronze`) inside that schema only. Nothing outside it is changed or dropped. A full rerun creates another small schema (optional cleanup cell at the end). Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a schema and two volumes
# MAGIC `landing` holds the source files (it plays the part of cloud object storage). `checkpoints` holds the stream checkpoint and the schema location.

# COMMAND ----------

import uuid
catalog = spark.sql("SELECT current_catalog()").first()[0]
schema = f"bt_autoloader_{uuid.uuid4().hex[:12]}"

spark.sql(f"CREATE SCHEMA {catalog}.{schema}")
spark.sql(f"CREATE VOLUME {catalog}.{schema}.landing")
spark.sql(f"CREATE VOLUME {catalog}.{schema}.checkpoints")
spark.sql(f"USE {catalog}.{schema}")

src  = f"/Volumes/{catalog}/{schema}/landing/orders"
ckpt = f"/Volumes/{catalog}/{schema}/checkpoints"
print("schema:", f"{catalog}.{schema}")
print("source folder:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: day 1, a file lands
# MAGIC This cell plays the source system: a CSV file with a header row and three fictional orders.

# COMMAND ----------

day1 = """order_id,customer,amount,order_date
1001,ana,25.50,2026-11-10
1002,ben,12.00,2026-11-10
1003,cara,40.25,2026-11-10
"""
dbutils.fs.put(f"{src}/orders_day1.csv", day1)
display(dbutils.fs.ls(src))     # 1 file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: default inference
# MAGIC `cloudFiles.schemaLocation` turns on schema inference and evolution. Auto Loader samples the files (first 50 GB or 1,000 files) and stores the schema under `_schemas` at that location. For CSV, JSON and XML every column is inferred as a **string** by default, and Auto Loader adds a `_rescued_data` column. No stream is started here; `printSchema()` is enough.
# MAGIC
# MAGIC Expected: `order_id`, `customer`, `amount`, `order_date` all `string`, plus `_rescued_data: string`.

# COMMAND ----------

peek = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation",
          f"{ckpt}/peek")
  .load(src))

peek.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: the bronze stream (read side + write side)
# MAGIC - `cloudFiles.inferColumnTypes = true`: typed inference, like the regular Spark reader.
# MAGIC - `cloudFiles.schemaHints`: pin the types you know (`amount DECIMAL(10,2), order_date DATE`). Hints apply on top of inference and are used only when you don't provide a schema with `.schema(...)`.
# MAGIC - The schema location and the checkpoint share one folder.
# MAGIC - `mergeSchema = true` on the write lets the Delta table accept new columns.
# MAGIC - `trigger(availableNow=True)`: process the available files, then stop (serverless).

# COMMAND ----------

chk = f"{ckpt}/orders_bronze"

reader = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", chk)
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaHints",
    "amount DECIMAL(10,2), order_date DATE"))

def run_bronze():
  (reader.load(src)
    .writeStream
    .option("checkpointLocation", chk)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("orders_bronze")
  ).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: run 1
# MAGIC Expected: 3 rows; `order_id: integer`, `customer: string`, `amount: decimal(10,2)`, `order_date: date`, `_rescued_data: string`.

# COMMAND ----------

run_bronze()
spark.table("orders_bronze").printSchema()
print(spark.table("orders_bronze").count())     # 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: day 2, a new column and a bad value
# MAGIC `channel` is a new column. Order 1005 has `amount = N/A`, which isn't a decimal.

# COMMAND ----------

day2 = """order_id,customer,amount,order_date,channel
1004,dev,18.75,2026-11-11,web
1005,eli,N/A,2026-11-11,store
"""
dbutils.fs.put(f"{src}/orders_day2.csv", day2)

# new column: channel
# bad value: amount = N/A (not a decimal)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: run 2, the stream stops
# MAGIC Default `cloudFiles.schemaEvolutionMode` is `addNewColumns` (when no schema is provided). Auto Loader merges `channel` into the schema at the schema location, then the stream fails with an unknown field exception (`UnknownFieldException`). Existing column types don't change. No day 2 rows are written yet, so the table still has 3 rows. In production, run Auto Loader in Lakeflow Jobs with retries so the restart is automatic.

# COMMAND ----------

try:
    run_bronze()
except Exception as e:
    print("stream stopped:", str(e)[:160])

print(spark.table("orders_bronze").count())
# still 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: run 3, restart
# MAGIC Same code. Auto Loader reads with the updated schema; `mergeSchema` on the write adds `channel` to the Delta table (without it the write fails with a schema mismatch). Expected: 5 rows, `channel` null for the day 1 rows, order 1005 `amount` null with `_rescued_data` = `{"amount":"N/A","_file_path":".../orders_day2.csv"}`.

# COMMAND ----------

run_bronze()     # same code, schema now has channel

display(spark.table("orders_bronze")
  .select("order_id", "amount", "channel",
          "_rescued_data")
  .orderBy("order_id"))
# 5 rows

display(spark.table("orders_bronze")
  .where("_rescued_data IS NOT NULL")
  .select("order_id", "_rescued_data"))       # 1005 only

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema evolution modes (`cloudFiles.schemaEvolutionMode`)
# MAGIC | Mode | On a new column |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (default without a provided schema) | stream fails once, schema updated, restart resumes; existing types unchanged |
# MAGIC | `addNewColumnsWithTypeWidening` | same, plus widens supported types such as int to long (Public Preview) |
# MAGIC | `rescue` | never evolves, never fails; new columns go to `_rescued_data` |
# MAGIC | `failOnNewColumns` | fails and stays failed until you update the provided schema or remove the file |
# MAGIC | `none` (default with a provided schema) | new columns ignored, never fails; not rescued unless `rescuedDataColumn` is set |
# MAGIC
# MAGIC `addNewColumns` is not allowed with a provided schema, but works with schema hints.
# MAGIC
# MAGIC ## What lands in `_rescued_data`
# MAGIC Type mismatches, columns missing from the schema, and case mismatches, as a JSON blob with the source `_file_path`. With a rescued data column, type mismatches don't fail the read; only malformed records do.
# MAGIC
# MAGIC Recap of the basics: Auto Loader Foundations video https://youtu.be/4HEnm4xL1_0 and lesson A02 https://youtu.be/cLrBSH5DXIQ. COPY INTO: lesson A03 https://youtu.be/LS0gDlRD_1g.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge
# MAGIC After cell 8, a day 3 file lands with another new column, `coupon`.
# MAGIC 1. You call the same `run_bronze()` (default mode). What happens on the next run, and after that?
# MAGIC 2. A different stream, set to `schemaEvolutionMode = rescue`, reads day 3. New column? Failure?
# MAGIC 3. A stream with a provided `.schema(...)` and no mode set reads day 3. Which mode applies, and where does `coupon` go?
# MAGIC
# MAGIC Solution (uncomment to check Q1): 1) it fails once with an unknown field exception for `coupon` and the schema location is updated; run it again, day 3 loads, and `mergeSchema` adds `coupon` to the table. 2) `rescue`: no failure, no new column, `coupon` values go into `_rescued_data`. 3) `none`: `coupon` is ignored, no failure, and it isn't rescued unless `rescuedDataColumn` is set.

# COMMAND ----------

# day3 = """order_id,customer,amount,order_date,channel,coupon
# 1006,fay,21.00,2026-11-12,web,SAVE10
# """
# dbutils.fs.put(f"{src}/orders_day3.csv", day3)
# try:
#     run_bronze()                       # Q1: stops once (unknown field: coupon)
# except Exception as e:
#     print("stream stopped:", str(e)[:160])
# run_bronze()                           # day 3 loads
# print(spark.table("orders_bronze").columns)   # ... channel, coupon
# print(spark.table("orders_bronze").count())   # 6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional cleanup
# MAGIC Drops only the schema this notebook created in this run (its volumes, checkpoints and table).

# COMMAND ----------

# spark.sql(f"DROP SCHEMA {catalog}.{schema} CASCADE")
