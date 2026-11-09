# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Files Incrementally with Auto Loader into a Bronze Delta Table — Build and Teach
# MAGIC Python notebook, Databricks Free Edition, serverless compute. Fictional data only; no downloads, dependency installs, external locations, or custom Spark configs. Run from the top, one cell at a time.
# MAGIC 
# MAGIC Project: small CSV order files land in a Unity Catalog volume. Auto Loader (`cloudFiles`) ingests only files it has not seen before into a managed **bronze** Delta table, using a checkpoint so each file is processed exactly once. Bronze keeps raw records, including a malformed value captured in the rescued data column, plus where and when each row arrived.
# MAGIC 
# MAGIC Safety: creates one new `bt_ingest_...` schema in your current writable catalog, two managed volumes inside it, three tiny CSV files, and one managed bronze table. No overwrite, DROP, DELETE, DBFS root, continuous or processing-time triggers. Files are written with `overwrite=False`, and the stream only appends. Re-running the setup cell creates another tiny demo schema; do not rerun it unnecessarily. Demo objects remain until you deliberately clean them up yourself.

# COMMAND ----------

# 1. Create an isolated schema and two managed Unity Catalog volumes
from decimal import Decimal
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, DecimalType)

def qi(name):
    return "`" + name.replace("`", "``") + "`"

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
demo_schema = "bt_ingest_" + uuid4().hex[:12]
assert demo_schema.startswith("bt_ingest_")
schema_fqn = qi(catalog_name) + "." + qi(demo_schema)
spark.sql(f"CREATE SCHEMA {schema_fqn} COMMENT 'Build and Teach fictional Auto Loader demo'")
spark.sql(f"CREATE VOLUME {schema_fqn}.{qi('raw_files')} COMMENT 'Landing zone for fictional CSV files'")
spark.sql(f"CREATE VOLUME {schema_fqn}.{qi('pipeline_state')} COMMENT 'Auto Loader checkpoint storage'")
bronze_fqn = schema_fqn + "." + qi("bronze_orders")
spark.sql(f"SHOW VOLUMES IN {schema_fqn}").show(truncate=False)
print("Demo schema:", schema_fqn)
print("Bronze table (created by the first stream run):", bronze_fqn)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Volume paths: source files vs. stream state
# MAGIC A Unity Catalog volume is governed file storage with a `/Volumes/<catalog>/<schema>/<volume>/` path. Keep the checkpoint out of the folder Auto Loader scans: source files live in `raw_files`, stream state lives in `pipeline_state`. The checkpoint is what remembers which files were already ingested, so never delete or share it between different streams.

# COMMAND ----------

# 2. Define absolute volume paths (serverless: no DBFS root, no relative paths)
volume_root = f"/Volumes/{catalog_name}/{demo_schema}"
source_dir = f"{volume_root}/raw_files/orders/"
checkpoint_dir = f"{volume_root}/pipeline_state/bronze_orders/_checkpoint"
dbutils.fs.mkdirs(source_dir)
assert not checkpoint_dir.startswith(source_dir)
print("Source directory :", source_dir)
print("Checkpoint       :", checkpoint_dir)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Batch 1: two CSV files arrive
# MAGIC `dbutils.fs.put(..., overwrite=False)` refuses to replace an existing file. If this cell already succeeded, skip it rather than rerunning.

# COMMAND ----------

# 3. Land batch 1 (2 files, 5 rows)
header = "order_id,store,quantity,unit_price\n"
batch1 = {
    "orders_2026_11_09_a.csv": header + "101,NORTH,2,10.00\n102,SOUTH,1,5.50\n103,WEST,3,2.00\n",
    "orders_2026_11_09_b.csv": header + "104,NORTH,1,8.00\n105,EAST,4,1.25\n",
}
for name, text in batch1.items():
    dbutils.fs.put(source_dir + name, text, overwrite=False)
landed = sorted(f.name for f in dbutils.fs.ls(source_dir))
print("Files in landing folder:", landed)
assert landed == ["orders_2026_11_09_a.csv", "orders_2026_11_09_b.csv"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define the Auto Loader stream once
# MAGIC - `format("cloudFiles")` + `cloudFiles.format = csv` turns on Auto Loader.
# MAGIC - An explicit schema enforces types. With a provided schema, values that do not fit (like the text `two` in an integer column) are not silently lost: `rescuedDataColumn` stores them as JSON in `_rescued_data`, and the typed column becomes null. (Without an explicit schema you would add `cloudFiles.schemaLocation` so Auto Loader can infer and track the schema.)
# MAGIC - `_metadata.file_path` / `_metadata.file_name` record the source file; `current_timestamp()` records ingestion time (UTC on serverless).
# MAGIC - `trigger(availableNow=True)` processes every file available when the run starts, then stops. It is the recommended serverless trigger; continuous and processing-time triggers are not supported on serverless notebooks.
# MAGIC - `checkpointLocation` stores progress, giving exactly-once file ingestion across runs.

# COMMAND ----------

# 4. Reusable Auto Loader -> bronze function (same source, schema, checkpoint, table every run)
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("store", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
])

def bronze_stream(run_label):
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(order_schema)
        .load(source_dir)
        .select("*",
                F.col("_metadata.file_path").alias("source_file_path"),
                F.col("_metadata.file_name").alias("source_file"),
                F.current_timestamp().alias("ingested_at_utc"),
                F.lit(run_label).alias("load_run")))

def progress_rows(query):
    total = 0
    for p in query.recentProgress:
        try:
            total += int(p["numInputRows"])
        except (TypeError, KeyError):
            total += int(getattr(p, "numInputRows", 0) or 0)
    return total

def run_bronze_ingest(run_label):
    query = (bronze_stream(run_label).writeStream
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(bronze_fqn))
    query.awaitTermination()
    print(f"{run_label}: stream finished; rows reported by progress = {progress_rows(query)}")
    return query

bronze_stream("preview").printSchema()
print("Stream defined; nothing ingested yet.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run 1: ingest batch 1
# MAGIC The first run creates the managed bronze Delta table and records both files in the checkpoint.

# COMMAND ----------

# 5. First availableNow run
run_bronze_ingest("run1")
after_run1 = spark.table(bronze_fqn)
after_run1.select("order_id", "store", "quantity", "unit_price", "_rescued_data",
                  "source_file", "load_run").orderBy("order_id").show(truncate=False)
assert after_run1.count() == 5
assert after_run1.select("source_file").distinct().count() == 2
assert after_run1.filter(F.col("_rescued_data").isNotNull()).count() == 0
print("RUN 1 CHECK: 5 rows from 2 files, 0 rescued")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Batch 2: one new file, including one malformed value
# MAGIC Order 107 has `two` instead of a number in `quantity`. In bronze we keep the record and capture the bad value instead of dropping it; cleaning and quarantine belong in the next (silver) step.

# COMMAND ----------

# 6. Land batch 2 (1 new file, 3 rows; order 107 is malformed)
new_name = "orders_2026_11_09_c.csv"
dbutils.fs.put(source_dir + new_name,
               header + "106,SOUTH,2,3.50\n107,EAST,two,4.00\n108,WEST,1,12.00\n",
               overwrite=False)
print(dbutils.fs.head(source_dir + new_name))
landed = sorted(f.name for f in dbutils.fs.ls(source_dir))
print("Files in landing folder:", landed)
assert len(landed) == 3


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run 2: rerun the SAME stream — only the new file is processed
# MAGIC Same source, same checkpoint, same table. Auto Loader skips the two files already recorded in the checkpoint and ingests only `orders_2026_11_09_c.csv`.

# COMMAND ----------

# 7. Second availableNow run with the same checkpoint
run_bronze_ingest("run2")
bronze = spark.table(bronze_fqn)
by_run = bronze.groupBy("load_run", "source_file").count().orderBy("load_run", "source_file")
by_run.show(truncate=False)
assert bronze.count() == 8
run2 = bronze.filter(F.col("load_run") == "run2")
assert run2.count() == 3
assert [r.source_file for r in run2.select("source_file").distinct().collect()] == [new_name]
assert bronze.groupBy("order_id").count().filter("count > 1").count() == 0
print("RUN 2 CHECK: only 1 new file / 3 new rows; total 8 rows; no duplicate order_id")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inspect the rescued record
# MAGIC The typed `quantity` is null, and `_rescued_data` holds the original text plus the source file path.

# COMMAND ----------

# 8. Rescued data column
rescued = bronze.filter(F.col("_rescued_data").isNotNull())
rescued.select("order_id", "store", "quantity", "unit_price", "_rescued_data").show(truncate=False)
r = rescued.first()
assert rescued.count() == 1
assert r.order_id == 107 and r.quantity is None and '"two"' in r._rescued_data
print("RESCUE CHECK: order 107 kept in bronze; quantity null; original value 'two' rescued")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Run again with NO new files — exactly-once check
# MAGIC Rerunning an ingestion job should be safe. With no new files, Auto Loader appends nothing
# MAGIC to the table. Prove that with the table count and the absence of a `run3` load_run label.
# MAGIC Note: `recentProgress` can still print a non-zero progress total (observed: 8). That figure
# MAGIC is not the count of newly appended rows — the table stayed at 8 and no `run3` rows appeared.

# COMMAND ----------

# 9. Third availableNow run, no new files
run_bronze_ingest("run3_no_new_files")
bronze = spark.table(bronze_fqn)
assert bronze.count() == 8
assert bronze.filter(F.col("load_run") == "run3_no_new_files").count() == 0
print("IDEMPOTENCY CHECK: still 8 rows; rerun added 0 rows")
# Progress print above may show a non-zero total from recentProgress; table count is the idempotency proof.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. SQL verification of the persisted bronze table
# MAGIC Checks stored results with SQL, not a DataFrame variable. Revenue ignores the rescued row because its quantity is null: that row needs fixing in silver, not deleting in bronze.

# COMMAND ----------

# 10. SQL verification and PASS
summary = spark.sql(f"""
SELECT COUNT(*)                                         AS total_rows,
       COUNT(DISTINCT source_file)                      AS files_ingested,
       COUNT(_rescued_data)                             AS rescued_rows,
       COUNT(DISTINCT order_id)                         AS distinct_orders,
       CAST(SUM(quantity * unit_price) AS DECIMAL(18,2)) AS valid_revenue
FROM {bronze_fqn}
""")
summary.show(truncate=False)
s = summary.first()
assert (s.total_rows, s.files_ingested, s.rescued_rows, s.distinct_orders) == (8, 3, 1, 8)
assert s.valid_revenue == Decimal("63.50")
spark.sql(f"""
SELECT load_run, source_file, COUNT(*) AS rows_loaded
FROM {bronze_fqn} GROUP BY load_run, source_file ORDER BY load_run, source_file
""").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {bronze_fqn}").select("version", "operation").orderBy("version").show(truncate=False)
print("PASS: bronze = 8 rows / 3 files / 1 rescued / 8 distinct orders / valid revenue 63.50")
print("Copy for a SQL cell:")
print(f"SELECT * FROM {bronze_fqn} ORDER BY order_id;")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Concept note: Auto Loader vs. COPY INTO
# MAGIC Both load files incrementally into Unity Catalog tables and skip files already loaded.
# MAGIC 
# MAGIC | | Auto Loader (`cloudFiles`) | `COPY INTO` (SQL) |
# MAGIC |---|---|---|
# MAGIC | Interface | Structured Streaming (Python/SQL, `read_files` in pipelines) | One SQL command |
# MAGIC | File tracking | Checkpoint location | Target table's load metadata |
# MAGIC | Scale | Designed for very large/continuously growing file counts | Good for simpler loads of thousands of files |
# MAGIC | Schema | Inference, evolution modes, rescued data column | Schema options per command (`mergeSchema`) |
# MAGIC | Typical run | `availableNow` in a scheduled job, or Lakeflow pipelines | Rerun the same command on a schedule |
# MAGIC 
# MAGIC Illustration only, NOT run in this notebook (it writes to a table):
# MAGIC ```sql
# MAGIC -- COPY INTO <catalog>.<schema>.bronze_orders_copy
# MAGIC -- FROM '/Volumes/<catalog>/<schema>/raw_files/orders/'
# MAGIC -- FILEFORMAT = CSV
# MAGIC -- FORMAT_OPTIONS ('header' = 'true')
# MAGIC -- COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC ```
# MAGIC Rerunning the same `COPY INTO` skips files it already loaded, similar to Auto Loader's checkpoint behavior.
# MAGIC 
# MAGIC ## 12. Practice challenge (prediction; no extra writes needed)
# MAGIC A fourth file `orders_2026_11_09_d.csv` lands with two rows: `109,NORTH,3,2.50` and `110,SOUTH,1,free`. You run `run_bronze_ingest("run4")`.
# MAGIC 1. How many rows are in bronze afterward, and how many come from `run4`?
# MAGIC 2. How many rows have `_rescued_data`, and which column is null for order 110?
# MAGIC 3. What is the new valid revenue?
# MAGIC 4. What happens if you run `run_bronze_ingest("run4")` again?
# MAGIC 
# MAGIC Answers are at the bottom of this notebook.
# MAGIC 
# MAGIC ## Recap
# MAGIC - Land files in a Unity Catalog volume; keep the checkpoint in a separate location.
# MAGIC - Auto Loader = `format("cloudFiles")` + `cloudFiles.format`; explicit schema enforces types.
# MAGIC - `rescuedDataColumn` keeps values that do not match the schema instead of dropping them.
# MAGIC - Add `_metadata` file info and ingestion time for lineage in bronze.
# MAGIC - `trigger(availableNow=True)` + checkpoint = incremental, exactly-once, serverless-friendly batch-style runs.
# MAGIC - Verify the persisted table with SQL.
# MAGIC 
# MAGIC Exam alignment: official Databricks Certified Data Engineer Associate guide (August 2026), Section 2 "Data Ingestion and Loading" includes ingestion patterns (batch, streaming, incremental), COPY INTO, and Auto Loader landing data into Unity Catalog-governed tables. The Professional guide (July 2026) Section 2 includes append-only batch/streaming pipelines with Delta and Section 3 includes quarantining bad data with Auto Loader in classic jobs; this lesson is a foundation for those, not full coverage. No exam guarantee.
# MAGIC 
# MAGIC ### Practice answers
# MAGIC 1. 10 rows total; 2 from `run4` (only the new file).
# MAGIC 2. 2 rescued rows (orders 107 and 110); for order 110 `unit_price` is null and `free` is in `_rescued_data`.
# MAGIC 3. 63.50 + 3 × 2.50 = 71.00 (order 110 contributes nothing because its price is null).
# MAGIC 4. Nothing new is appended: the checkpoint already records file d. Total stays 10.
# MAGIC 
# MAGIC Build and Teach — https://buildandteach.com/
