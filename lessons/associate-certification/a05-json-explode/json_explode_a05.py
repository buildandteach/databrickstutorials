# Databricks notebook source

# MAGIC %md
# MAGIC # JSON and Nested Data with explode
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A05 · Exam guide (Aug 2026) Section 2, Data Ingestion and Loading (21%) and Section 3, Data Transformation and Modeling (22%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless). Self-contained fictional orders. No downloads, packages, external paths, or paid-only features. Run from the top.
# MAGIC
# MAGIC Objectives covered: ingest semi-structured data (JSON and nested data) into Unity Catalog governed Delta tables (Section 2), and manipulate columns and rows, including exploding arrays (Section 3). You will read nested JSON with an inferred and an explicit schema, use dot notation on structs, flatten arrays with `explode`, `explode_outer` and `posexplode`, write a flat silver table, parse a JSON string with `from_json`, and query JSON strings with the Databricks SQL colon syntax.
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_json_` + 12 hex characters, one managed volume (`landing`) and two managed tables (`orders_bronze`, `order_items`) inside that schema only. Nothing outside it is changed or dropped. A full rerun creates another small schema (optional cleanup cell at the end). Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a schema and a volume
# MAGIC `landing` holds the source file (it plays the part of cloud object storage or a connector's landing zone).

# COMMAND ----------

import uuid
catalog = spark.sql("SELECT current_catalog()").first()[0]
schema = f"bt_json_{uuid.uuid4().hex[:12]}"

spark.sql(f"CREATE SCHEMA {catalog}.{schema}")
spark.sql(f"CREATE VOLUME {catalog}.{schema}.landing")
spark.sql(f"USE {catalog}.{schema}")

src = f"/Volumes/{catalog}/{schema}/landing/orders"
print("schema:", f"{catalog}.{schema}")
print("source folder:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: write nested JSON (one order per line)
# MAGIC Each order has a `customer` struct (with a nested `address` struct), an `items` array of structs (`sku`, `qty`, `price`) and a `tags` array of strings. Order 1003 has an empty `items` array and order 1004 has no `items` field at all (null).

# COMMAND ----------

rows = [
 '{"order_id":1001,"customer":{"name":"ana",'
  '"address":{"city":"Austin","country":"US"}},'
  '"items":[{"sku":"A1","qty":2,"price":5.0},'
  '{"sku":"B2","qty":1,"price":12.5}],'
  '"tags":["web","promo"]}',
 '{"order_id":1002,"customer":{"name":"ben",'
  '"address":{"city":"Dallas","country":"US"}},'
  '"items":[{"sku":"C3","qty":3,"price":4.0}],'
  '"tags":["store"]}',
 '{"order_id":1003,"customer":{"name":"cara",'
  '"address":{"city":"Toronto","country":"CA"}},'
  '"items":[],"tags":[]}',
 '{"order_id":1004,"customer":{"name":"dev",'
  '"address":{"city":"Austin","country":"US"}},'
  '"tags":["web"]}',
]
dbutils.fs.put(f"{src}/orders.json", "\n".join(rows))
display(dbutils.fs.ls(src))     # 1 file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: read with schema inference
# MAGIC The JSON reader expects JSON Lines by default (one complete record per line). For a pretty-printed file whose records span several lines, set `multiLine` to `true`.
# MAGIC
# MAGIC Expected: `customer` struct (`address` struct with `city`, `country`; `name`), `items` array of struct (`price: double`, `qty: long`, `sku: string`), `order_id: long`, `tags` array of string. Whole numbers are inferred as `long`, decimals as `double`, and field names are sorted. (Auto Loader, lesson A04, infers JSON columns as strings unless `cloudFiles.inferColumnTypes` is `true`.)

# COMMAND ----------

raw_df = spark.read.json(src)
raw_df.printSchema()

# default: JSON Lines, one record per line
# pretty-printed file spanning lines?
#   spark.read.option("multiLine", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: read with an explicit schema, save the bronze table
# MAGIC A DDL string describes nested types: `STRUCT<...>` and `ARRAY<...>`. With an explicit schema Spark skips the inference pass and uses your types (`INT`, `DECIMAL(10,2)`). The nested columns are stored as they are in the Delta table `orders_bronze`.

# COMMAND ----------

schema_ddl = """order_id INT,
  customer STRUCT<name: STRING,
    address: STRUCT<city: STRING,
                    country: STRING>>,
  items ARRAY<STRUCT<sku: STRING, qty: INT,
                     price: DECIMAL(10,2)>>,
  tags ARRAY<STRING>"""

orders = spark.read.schema(schema_ddl).json(src)
orders.write.saveAsTable("orders_bronze")
print(spark.table("orders_bronze").count())     # 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: dot notation into structs
# MAGIC `customer.address.city` reaches two levels down; the new column takes the last field name. `customer.*` expands every struct field into its own column. Expected: 4 rows (dot notation never adds rows): 1001 ana Austin, 1002 ben Dallas, 1003 cara Toronto, 1004 dev Austin.

# COMMAND ----------

from pyspark.sql.functions import col

display(orders.select(
  "order_id",
  "customer.name",
  col("customer.address.city").alias("city")))

# all fields of a struct as columns:
display(orders.select("order_id", "customer.*"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: explode, one row per array element
# MAGIC `explode` returns one row per element and repeats the other selected columns. Null and empty arrays produce **no** rows. Expected: **3** rows (1001 twice, 1002 once); orders 1003 (empty array) and 1004 (null) disappear. This is the answer to the opening question.

# COMMAND ----------

from pyspark.sql.functions import explode

items = orders.select(
  "order_id",
  explode("items").alias("item"))

display(items)
print(items.count())     # 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: explode_outer and posexplode
# MAGIC - `explode_outer`: like `explode`, but a null or empty array still gives one row with a null element. Expected: **5** rows.
# MAGIC - `posexplode`: adds the element position. Columns `pos` and `col`; `pos` starts at **0**. Skips null and empty arrays (`posexplode_outer` keeps them). Expected: 1001 pos 0 (A1) and 1 (B2), 1002 pos 0 (C3).
# MAGIC - On a map column, `explode` returns `key` and `value` columns.

# COMMAND ----------

from pyspark.sql.functions import (
    explode_outer, posexplode)

outer = orders.select("order_id",
  explode_outer("items").alias("item"))
print(outer.count())                # 5

pos = orders.select("order_id",
  posexplode("items"))     # pos, col
display(pos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: flatten to a silver table
# MAGIC Explode the items, then use dot notation on the exploded struct. Expected `order_items`: 3 rows; line totals 10.00 and 12.50 for 1001, 12.00 for 1002. Use `explode_outer` instead if orders without items must be kept.

# COMMAND ----------

silver = (orders
  .select("order_id", "customer.name",
          explode("items").alias("item"))
  .select("order_id", "name", "item.sku",
          "item.qty", "item.price",
          (col("item.qty") * col("item.price"))
            .alias("line_total")))

silver.write.saveAsTable("order_items")
display(spark.table("order_items").orderBy("order_id", "sku"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: from_json, parse a JSON string column
# MAGIC When JSON arrives as a string (a message payload or an API response saved as text), `from_json(column, schema)` returns a struct. Here the file is read as text, so each line is one string in the column `value`. `schema_of_json` can derive a schema from a sample string. Expected: 4 rows, 1001 Austin, 1002 Dallas, 1003 Toronto, 1004 Austin.

# COMMAND ----------

from pyspark.sql.functions import from_json

text_df = spark.read.text(src)   # value: string

parsed = text_df.select(
  from_json("value", schema_ddl).alias("o"))

display(parsed.select("o.order_id",
  "o.customer.address.city"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Databricks SQL colon syntax on JSON strings
# MAGIC `value:field` extracts a field from a JSON string column; dots go deeper; `[0]` indexes an array (0-based); `[*]` pulls a field from every element. Results are strings; cast with `::`. `[*]` works only on JSON strings, not on native ARRAY columns. Expected for 1001: `1001`, `Austin`, `A1`, `["A1","B2"]`.
# MAGIC
# MAGIC Databricks also has a VARIANT type for semi-structured data (Professional exam topic, covered in the Professional series).

# COMMAND ----------

text_df.createOrReplaceTempView("raw_orders")

display(spark.sql("""
  SELECT value:order_id::int     AS order_id,
         value:customer.address.city AS city,
         value:items[0].sku       AS first_sku,
         value:items[*].sku       AS all_skus
  FROM raw_orders"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11: explode in SQL
# MAGIC In Databricks SQL and Databricks Runtime 12.2 LTS and above, calling `explode` in the SELECT list or with LATERAL VIEW is deprecated (it still works); call it as a table reference with `LATERAL`. Expected: 3 rows. With `explode_outer` all 4 orders are kept.

# COMMAND ----------

display(spark.sql("""
  SELECT o.order_id, i.item.sku, i.item.qty
  FROM orders_bronze AS o,
    LATERAL explode(o.items) AS i(item)
"""))

# 3 rows · explode_outer here keeps all 4 orders

display(spark.sql("""
  SELECT o.order_id, i.item.sku
  FROM orders_bronze AS o,
    LATERAL explode_outer(o.items) AS i(item)
"""))   # 5 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam traps
# MAGIC 1. `explode` drops rows whose array is null or empty.
# MAGIC 2. `explode_outer` keeps them, with a null element.
# MAGIC 3. `posexplode` returns `pos` and `col`, and `pos` starts at 0.
# MAGIC 4. A pretty-printed JSON file needs `multiLine = true`.
# MAGIC 5. The JSON reader infers real types; Auto Loader infers JSON columns as strings unless `cloudFiles.inferColumnTypes`.
# MAGIC
# MAGIC Earlier lessons: A01 https://youtu.be/PglvHIK-QmU · A02 https://youtu.be/cLrBSH5DXIQ · A03 https://youtu.be/LS0gDlRD_1g · Lesson A04 (Auto Loader schema evolution) in this repo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge
# MAGIC Tags per order: 1001 `["web","promo"]`, 1002 `["store"]`, 1003 `[]`, 1004 `["web"]`.
# MAGIC 1. How many rows does `explode("tags")` return?
# MAGIC 2. How many with `explode_outer("tags")`?
# MAGIC 3. With `posexplode("tags")`, what position does `promo` get?
# MAGIC
# MAGIC Solution (uncomment to check): 1) 4 rows (2 + 1 + 0 + 1). 2) 5 rows, 1003 kept with a null tag. 3) `promo` is pos 1 (`web` is 0).

# COMMAND ----------

# from pyspark.sql.functions import explode, explode_outer, posexplode
# print(orders.select("order_id", explode("tags")).count())          # Q1: 4
# print(orders.select("order_id", explode_outer("tags")).count())    # Q2: 5
# display(orders.select("order_id", posexplode("tags")))              # Q3: promo -> pos 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional cleanup
# MAGIC Drops only the schema this notebook created in this run (its volume and tables).

# COMMAND ----------

# spark.sql(f"DROP SCHEMA {catalog}.{schema} CASCADE")
