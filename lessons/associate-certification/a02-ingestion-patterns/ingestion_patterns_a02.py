# Databricks notebook source

# MAGIC %md
# MAGIC # Ingestion Patterns: Batch, Streaming and Incremental
# MAGIC **Databricks Data Engineer Associate series · Lesson A02 · Exam guide (Aug 2026) Section 2, Data Ingestion and Loading (21%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless). Self-contained fictional orders. No downloads, packages, external paths, or paid-only features. Run from the top.
# MAGIC
# MAGIC Objective covered: enable and detail data ingestion patterns, including batch, streaming and incremental loading, and import data from sources such as local files, Lakeflow Connect standard connectors and Lakeflow Connect managed connectors (managed connectors are explained as concepts).
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_ingest_<12 hex>`, one managed volume `landing`, and two managed tables (`orders_batch`, `orders_bronze`) inside that schema only. The only overwrite is of `orders_batch`, which this notebook creates. No DROP of existing objects. A full rerun creates another small schema. Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a schema and a volume as the landing zone
# MAGIC A **volume** is a Unity Catalog object that stores files. Its path starts with `/Volumes/catalog/schema/volume`.

# COMMAND ----------

import uuid
catalog = spark.sql("SELECT current_catalog()").first()[0]
schema = f"bt_ingest_{uuid.uuid4().hex[:12]}"      # new every run
spark.sql(f"CREATE SCHEMA {catalog}.{schema} COMMENT 'Build and Teach A02'")
spark.sql(f"CREATE VOLUME {catalog}.{schema}.landing")

base = f"/Volumes/{catalog}/{schema}/landing"
src  = f"{base}/orders"                     # raw files land here
ckpt = f"{base}/_checkpoints/orders_bronze"  # Auto Loader progress
print("schema:", f"{catalog}.{schema}")
print("landing folder:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: day 1, a small file lands
# MAGIC This cell plays the source system: it writes a comma-separated values (CSV) file with three fictional orders.

# COMMAND ----------

day1 = """order_id,customer,amount,order_date
1001,ana,25.50,2026-11-10
1002,ben,12.00,2026-11-10
1003,cara,40.25,2026-11-10
"""
dbutils.fs.put(f"{src}/orders_day1.csv", day1)
display(dbutils.fs.ls(src))      # 1 file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: pattern 1, batch load
# MAGIC `spark.read` is a batch read: it lists and reads **every** file in the folder each run, so the write uses `overwrite` (append would duplicate rows).

# COMMAND ----------

order_schema = "order_id INT, customer STRING, amount DOUBLE, order_date DATE"

def run_batch():
    df = (spark.read.format("csv").option("header", "true")
          .schema(order_schema).load(src))          # reads EVERY file
    df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.orders_batch")
    return spark.table(f"{catalog}.{schema}.orders_batch").count()

print("orders_batch rows:", run_batch())      # 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: pattern 2, incremental load with Auto Loader
# MAGIC `cloudFiles` is Auto Loader. The checkpoint records which files are already processed. `availableNow` processes everything new, then stops (serverless supports `availableNow`; `processingTime` and continuous triggers raise `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`).

# COMMAND ----------

from pyspark.sql.functions import col
bronze = f"{catalog}.{schema}.orders_bronze"

def run_auto_loader():
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv").option("header", "true")
              .schema(order_schema).load(src)
              .withColumn("source_file", col("_metadata.file_name")))
    (stream.writeStream.option("checkpointLocation", ckpt)
           .trigger(availableNow=True).toTable(bronze).awaitTermination())

run_auto_loader()
print("orders_bronze rows:", spark.table(bronze).count())   # 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: day 2 arrives, run both again

# COMMAND ----------

day2 = """order_id,customer,amount,order_date
1004,dev,18.75,2026-11-11
1005,eli,33.10,2026-11-11
"""
dbutils.fs.put(f"{src}/orders_day2.csv", day2)

before = spark.table(bronze).count()
print("orders_batch rows:", run_batch())                 # 5, re-read 5 rows
run_auto_loader()
print("Auto Loader read:", spark.table(bronze).count() - before)   # 2
print("orders_bronze rows:", spark.table(bronze).count())          # 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: which file did each row come from?

# COMMAND ----------

(spark.table(bronze)
   .groupBy("source_file").count()
   .orderBy("source_file").show())
# orders_day1.csv 3, orders_day2.csv 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming (concept for serverless)
# MAGIC Always-on streaming uses the same code with a different trigger. On serverless notebooks and jobs (all of Free Edition), only `availableNow` (and the deprecated `once`) are supported. For continuous workloads Databricks recommends a pipeline in continuous mode, or an `availableNow` query in a continuous job.
# MAGIC
# MAGIC ```python
# MAGIC # NOT supported on serverless notebooks (raises INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED):
# MAGIC # stream.writeStream.option("checkpointLocation", ckpt).trigger(processingTime="1 minute").toTable(bronze)
# MAGIC ```
# MAGIC
# MAGIC ## Local files and Lakeflow Connect
# MAGIC - Local files: **New > Add or upload data > Create or modify table** (one-time load into a managed Delta table), or upload files into a volume and read them with code as above.
# MAGIC - Lakeflow Connect **standard connectors**: Spark and SQL APIs (Auto Loader, `COPY INTO`, Structured Streaming for message buses).
# MAGIC - Lakeflow Connect **managed connectors**: configured connections + ingestion pipelines for sources like Salesforce, Workday, SharePoint or SQL Server. They need a real source and credentials; database change data capture (CDC) connectors are paid-workspace features. Concepts only here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge
# MAGIC A day 3 file lands with 4 new orders. You run `run_batch()` and `run_auto_loader()` again.
# MAGIC 1. How many rows are in `orders_batch`?
# MAGIC 2. How many rows does this Auto Loader run read? Total in bronze?
# MAGIC 3. If all three batch runs had used `mode("append")`, how many rows would `orders_batch` hold?
# MAGIC
# MAGIC Solution (uncomment to check): 1) 9, 2) reads 4, bronze total 9, 3) 3 + 5 + 9 = 17 rows with duplicates.

# COMMAND ----------

# day3 = """order_id,customer,amount,order_date
# 1006,fay,21.00,2026-11-12
# 1007,gus,9.99,2026-11-12
# 1008,hana,55.00,2026-11-12
# 1009,ivo,14.50,2026-11-12
# """
# dbutils.fs.put(f"{src}/orders_day3.csv", day3)
# before = spark.table(bronze).count()
# print("orders_batch rows:", run_batch())                          # 9
# run_auto_loader()
# print("Auto Loader read:", spark.table(bronze).count() - before)  # 4
# print("orders_bronze rows:", spark.table(bronze).count())         # 9
