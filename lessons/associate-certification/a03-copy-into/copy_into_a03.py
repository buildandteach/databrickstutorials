# Databricks notebook source

# MAGIC %md
# MAGIC # COPY INTO Incremental Loading
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A03 · Exam guide (Aug 2026) Section 2, Data Ingestion and Loading (21%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless). Self-contained fictional orders. No downloads, packages, external paths, or paid-only features. Run from the top.
# MAGIC
# MAGIC Objective covered: use the `COPY INTO` command to incrementally load files from cloud object storage into Unity Catalog governed tables. In Free Edition a Unity Catalog **volume** is the source (cloud object storage paths need an external location set up by an admin).
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_copyinto_` + 12 hex characters, one managed volume `landing`, and two managed tables (`orders_copy`, `orders_auto`) inside that schema only. Nothing outside it is changed or dropped. A full rerun creates another small schema (optional cleanup cell at the end). Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a schema and a volume as the source
# MAGIC A **volume** is a Unity Catalog object that stores files. `USE` makes the new schema current so table names can be short.

# COMMAND ----------

import uuid
catalog = spark.sql("SELECT current_catalog()").first()[0]
schema = f"bt_copyinto_{uuid.uuid4().hex[:12]}"  # new every run

spark.sql(f"CREATE SCHEMA {catalog}.{schema}")
spark.sql(f"CREATE VOLUME {catalog}.{schema}.landing")
spark.sql(f"USE {catalog}.{schema}")

# source folder in the volume
src = f"/Volumes/{catalog}/{schema}/landing/orders"
print("schema:", f"{catalog}.{schema}")
print("source folder:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: day 1, a file lands
# MAGIC This cell plays the source system: a comma-separated values file with a header row and three fictional orders.

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
# MAGIC ## Cell 3: the target table
# MAGIC `COPY INTO` needs an existing Delta table. A managed table in a Unity Catalog schema is governed by Unity Catalog.

# COMMAND ----------

spark.sql("""
CREATE TABLE orders_copy (
  order_id    INT,
  customer    STRING,
  amount      DOUBLE,
  order_date  DATE,
  source_file STRING
)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: COPY INTO, run 1
# MAGIC - `FROM (SELECT ... FROM 'path')` shapes each row: CSV values are read as strings, so we cast them, and `_metadata.file_name` records the source file.
# MAGIC - `FILEFORMAT = CSV`; `FORMAT_OPTIONS` are passed to the file reader.
# MAGIC - The result has `num_affected_rows`, `num_inserted_rows` and `num_skipped_corrupt_files`. Expected: 3 inserted.

# COMMAND ----------

copy_sql = f"""
COPY INTO orders_copy
FROM (SELECT CAST(order_id AS INT) AS order_id,
             customer,
             CAST(amount AS DOUBLE) AS amount,
             CAST(order_date AS DATE) AS order_date,
             _metadata.file_name AS source_file
      FROM '{src}')
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
"""
display(spark.sql(copy_sql))     # 3 inserted

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: run the exact same command again
# MAGIC `COPY INTO` is idempotent: files already loaded into **this** table are skipped (even if edited since). Expected: 0 inserted, still 3 rows.

# COMMAND ----------

# same command, no new files
display(spark.sql(copy_sql))
# num_inserted_rows = 0

print(spark.table("orders_copy").count())
# still 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: day 2 arrives
# MAGIC Only the new file loads. Expected: 2 inserted, 5 in total, one row group per source file.

# COMMAND ----------

day2 = """order_id,customer,amount,order_date
1004,dev,18.75,2026-11-11
1005,eli,33.10,2026-11-11
"""
dbutils.fs.put(f"{src}/orders_day2.csv", day2)
display(spark.sql(copy_sql))     # 2 inserted

display(spark.sql("""
  SELECT source_file, count(*) AS n FROM orders_copy
  GROUP BY source_file ORDER BY 1"""))
# orders_day1.csv 3  ·  orders_day2.csv 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Options
# MAGIC | Clause | What it controls |
# MAGIC |---|---|
# MAGIC | `FORMAT_OPTIONS` | the file reader: `header`, `inferSchema`, `mergeSchema` (merge the source file schemas), `ignoreCorruptFiles`, ... |
# MAGIC | `COPY_OPTIONS` | the copy: `mergeSchema` (evolve the target table), `force` (`true` turns idempotency off and reloads every file) |
# MAGIC | `PATTERN = 'glob'` / `FILES = (...)` | which files to load (not both; `FILES` up to 1,000 names) |
# MAGIC | `VALIDATE [ALL \| n ROWS]` | check parsing, schema and constraints without writing |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: a schemaless table, columns inferred
# MAGIC An empty placeholder table (no columns) gets its schema from the files when `mergeSchema` is `true` in `COPY_OPTIONS`. A new table has its own load history, so both existing files load. Expected: 5 inserted. Until the first `COPY INTO`, the placeholder table can't take `INSERT INTO` or `MERGE INTO`.

# COMMAND ----------

# a placeholder table with no columns
spark.sql("CREATE TABLE IF NOT EXISTS orders_auto")

auto_sql = f"""
COPY INTO orders_auto
FROM '{src}'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true',
                'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
"""
display(spark.sql(auto_sql))     # 5 inserted

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: day 3 adds a column, the schema evolves
# MAGIC Expected: 2 inserted, a new `channel` column (null for the 5 older rows), 7 rows in total.

# COMMAND ----------

day3 = """order_id,customer,amount,order_date,channel
1006,fay,21.00,2026-11-12,web
1007,gus,9.99,2026-11-12,store
"""
dbutils.fs.put(f"{src}/orders_day3.csv", day3)
display(spark.sql(auto_sql))               # 2 inserted
spark.table("orders_auto").printSchema()   # + channel
print(spark.table("orders_auto").count())  # 7
display(spark.table("orders_auto").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPY INTO vs Auto Loader (recap: lesson A02 and the Auto Loader Foundations video)
# MAGIC | | COPY INTO | Auto Loader |
# MAGIC |---|---|---|
# MAGIC | Interface | SQL command, rerun on a schedule | `cloudFiles` Structured Streaming source |
# MAGIC | Tracks loaded files | load history kept with the target table | checkpoint |
# MAGIC | Scale (Databricks guidance) | thousands of files over time | millions of files or more; fewer discovery operations, work split into batches |
# MAGIC | Schema | `mergeSchema` | richer schema inference and evolution (lesson A04) |
# MAGIC | Reloading a chosen subset of files | easier (`FILES`, `PATTERN`) | harder |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge
# MAGIC After cell 8, `orders_copy` has 5 rows (days 1 and 2) and `orders_day3.csv` (2 rows plus a `channel` column) is in the source folder.
# MAGIC 1. You run `copy_sql` (cell 4) again. Rows inserted? Total rows? Does `channel` appear in `orders_copy`?
# MAGIC 2. Then you add `COPY_OPTIONS ('force' = 'true')` and run it. Rows inserted? Total rows?
# MAGIC 3. Two million new files land every day. `COPY INTO` or Auto Loader?
# MAGIC
# MAGIC Solution (uncomment to check): 1) 2 inserted (only `orders_day3.csv` is new for `orders_copy`), total 7, no `channel` column because the SELECT lists the columns. 2) `force` reloads all 3 files: 3 + 2 + 2 = 7 inserted, total 14 with duplicates. 3) Auto Loader.

# COMMAND ----------

# display(spark.sql(copy_sql))                              # Q1: 2 inserted
# print(spark.table("orders_copy").count())                 # 7
# print("channel" in spark.table("orders_copy").columns)    # False
# display(spark.sql(copy_sql.rstrip() + "\nCOPY_OPTIONS ('force' = 'true')"))   # Q2: 7 inserted
# print(spark.table("orders_copy").count())                 # 14

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional cleanup
# MAGIC Drops only the schema this notebook created in this run.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA {catalog}.{schema} CASCADE")
