# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Cleaning with PySpark and SQL
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A07 · Exam guide (Aug 2026) Section 3, Data Transformation and Modeling (22%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless compute). Fictional orders only. No downloads or packages.
# MAGIC
# MAGIC Objectives covered:
# MAGIC - Implement data cleaning: read bronze tables with PySpark or SQL, clean nulls, standardize data types, and write new silver tables.
# MAGIC - Manipulate columns, rows and table structures: add, drop, split and rename columns, and apply filters. (Exploding arrays was Lesson A05.)
# MAGIC
# MAGIC On the job: a nightly bronze orders feed arrives with bad and missing values. Silver must still load, and you must report how many rows were rejected and why (cell 10).
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_silver_` + 12 hex characters in the `workspace` catalog, and the tables `bronze_orders`, `silver_orders` and `orders_rejected` inside that schema only. Nothing outside it is changed or dropped. Expected results in the comments were checked with a local Apache Spark 4.0 run of the same code, not on Databricks. Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a fresh schema

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

schema = "bt_silver_" + uuid.uuid4().hex[:12]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")
spark.sql(f"USE workspace.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: the bronze table
# MAGIC Six fictional orders, every column a string, as Databricks recommends for bronze. Each row has one problem: extra spaces around Ana's name, `N/A` in Ben's amount, a missing customer on 1003, a missing `order_id`, a missing amount on 1005, and a combined `city_state` column. `_rescued_data` is the kind of metadata column Auto Loader adds (Lesson A04); here it is null on every row. Expected: `6`.

# COMMAND ----------

rows = [
 ("1001", " Ana Lopez ", "25.50", "2026-11-02", "Austin, TX"),
 ("1002", "Ben Wu", "N/A", "2026-11-03", "Dallas, TX"),
 ("1003", None, "14.00", "2026-11-03", "Denver, CO"),
 (None, "Cara Diaz", "9.99", "2026-11-04", "Austin, TX"),
 ("1005", "Dev Shah", None, "2026-11-05", "Tulsa, OK"),
 ("1006", "Eli Park", "42.00", "2026-11-05", "Austin, TX")]
names = ["order_id", "customer_name", "amount",
         "order_date", "city_state"]
ddl = ", ".join(f"{n} string" for n in names)

(spark.createDataFrame(rows, ddl)
  .withColumn("_rescued_data", F.lit(None).cast("string"))
  .write.mode("overwrite").saveAsTable("bronze_orders"))

bronze = spark.read.table("bronze_orders")
print(bronze.count())   # 6

display(bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Trap 1, cast on serverless
# MAGIC Serverless compute runs with American National Standards Institute (ANSI) SQL mode on by default (as do Apache Spark 4.0 and Databricks Runtime 17.0+). An invalid cast raises `CAST_INVALID_INPUT` instead of returning NULL, so one bad value fails the whole query. The `try` block keeps Run all going.

# COMMAND ----------

try:
    bronze.select(
        F.col("amount").cast("decimal(10,2)")).show()
except Exception as e:
    print(str(e).splitlines()[0][:90])

# [CAST_INVALID_INPUT] The value 'N/A' of
# the type "STRING" cannot be cast to
# "DECIMAL(10,2)" because it is malformed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: try_cast turns bad values into NULL, then count them
# MAGIC `try_cast` returns NULL for a malformed value. It only forgives bad values: a cast between types that can't be cast at all still errors. A NULL from `try_cast` hides the original value, so count the bad rows first. Expected: `1` (order 1002, `N/A`).

# COMMAND ----------

typed = bronze.withColumn(
    "amount", F.expr("try_cast(amount AS DECIMAL(10,2))"))

bad = bronze.filter(
    F.col("amount").isNotNull()
    & F.expr("try_cast(amount AS DECIMAL(10,2))").isNull())
print(bad.count())   # 1, order 1002 with N/A

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Trap 2, dropna() with no subset
# MAGIC `dropna()` defaults to `how="any"`. Every row has a null `_rescued_data`, so it returns 0 rows. Name the required columns with `subset`. Expected: `0`, then `5`.

# COMMAND ----------

print(bronze.dropna().count())                      # 0
print(bronze.dropna(subset=["order_id"]).count())   # 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Trap 3, fillna only fills matching types
# MAGIC `fillna(0)` ignores string columns (no error). Use a dict. Expected: `1`, then `0`.

# COMMAND ----------

f0 = bronze.fillna(0)
print(f0.filter(F.col("customer_name").isNull()).count())  # 1

fd = bronze.fillna({"customer_name": "unknown"})
print(fd.filter(F.col("customer_name").isNull()).count())  # 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: bronze to silver in one chain
# MAGIC Drop rows with no `order_id`, cast types (`try_cast` for amount), parse the date, trim and fill the customer, split `city_state` into `city` and `state` with `split(...).getItem(n)`, drop the old columns, rename `customer_name` to `customer`, filter out rows with no amount, and write a new managed Delta table. With ANSI on, `getItem` past the end of the array raises an error; `get()` returns NULL instead.
# MAGIC
# MAGIC Expected `silver_orders` (3 rows): 1001 Ana Lopez 25.50 2026-11-02 Austin TX · 1003 unknown 14.00 2026-11-03 Denver CO · 1006 Eli Park 42.00 2026-11-05 Austin TX. Schema: `order_id` int, `amount` decimal(10,2), `order_date` date, the rest string.

# COMMAND ----------

silver = (bronze
  .dropna(subset=["order_id"])               # nulls: drop
  .withColumn("order_id", F.col("order_id").cast("int"))
  .withColumn("amount",
      F.expr("try_cast(amount AS DECIMAL(10,2))"))
  .withColumn("order_date",
      F.to_date("order_date", "yyyy-MM-dd"))
  .withColumn("customer_name", F.trim("customer_name"))
  .fillna({"customer_name": "unknown"})      # nulls: fill
  .withColumn("city",
      F.split("city_state", ", ").getItem(0))  # split, add
  .withColumn("state",
      F.split("city_state", ", ").getItem(1))
  .drop("city_state", "_rescued_data")         # drop
  .withColumnRenamed("customer_name", "customer")
  .filter(F.col("amount").isNotNull()))        # filter

silver.write.mode("overwrite").saveAsTable("silver_orders")

silver.printSchema()
display(spark.table("silver_orders").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: catch the silent no-op
# MAGIC `drop` and `withColumnRenamed` do nothing if the column does not exist, so a typo does not fail. Check the columns after the chain.

# COMMAND ----------

oops = silver.withColumnRenamed("cust_name", "client")
print(oops.columns == silver.columns)   # True, no error

print(silver.columns)
# ['order_id', 'customer', 'amount',
#  'order_date', 'city', 'state']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: the same silver table in SQL, CREATE TABLE AS SELECT (CTAS)
# MAGIC `coalesce` plays the role of `fillna`; leaving a column out of the `SELECT` plays the role of `drop`. Expected: the same 3 rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_orders AS
# MAGIC SELECT CAST(order_id AS INT) AS order_id,
# MAGIC   coalesce(trim(customer_name), 'unknown') AS customer,
# MAGIC   try_cast(amount AS DECIMAL(10,2)) AS amount,
# MAGIC   to_date(order_date) AS order_date,
# MAGIC   split(city_state, ', ')[0] AS city,
# MAGIC   split(city_state, ', ')[1] AS state
# MAGIC FROM bronze_orders
# MAGIC WHERE order_id IS NOT NULL
# MAGIC   AND try_cast(amount AS DECIMAL(10,2)) IS NOT NULL;

# COMMAND ----------

display(spark.table("silver_orders").orderBy("order_id"))   # same 3 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: on the job, count what you rejected
# MAGIC Give every rejected bronze row a reason, count by reason, and keep the raw rows in their own table. Expected: one row each for `no order_id`, `bad amount` and `no amount` (order may vary), and `6 3 3`: bronze = silver + rejected. Data quality checks and validation rules get a full lesson in A11.

# COMMAND ----------

reason = (F.when(F.col("order_id").isNull(), "no order_id")
  .when(F.col("amount").isNull(), "no amount")
  .when(F.expr("try_cast(amount AS DECIMAL(10,2))").isNull(),
        "bad amount"))

rejected = (bronze.withColumn("reason", reason)
  .filter(F.col("reason").isNotNull()))
rejected.groupBy("reason").count().show()
rejected.write.mode("overwrite") \
  .saveAsTable("orders_rejected")

silver = spark.table("silver_orders")
print(bronze.count(), silver.count(), rejected.count())   # 6 3 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practical takeaway: bronze to silver checklist
# MAGIC 1. Read bronze, write a NEW silver table (never clean bronze in place).
# MAGIC 2. Required columns only: `dropna(subset=[...])`.
# MAGIC 3. Types: `try_cast`, then count the NULLs it made.
# MAGIC 4. Fill with a dict, never a bare `fillna(0)`.
# MAGIC 5. Check `columns` after `drop` and `withColumnRenamed`.
# MAGIC 6. Reconcile: bronze rows = silver rows + rejected rows.
# MAGIC
# MAGIC Memory trick: cast fails loud. try_cast fails quiet. A quiet NULL still needs a count.
# MAGIC
# MAGIC ## Cell 11: reusable snippet

# COMMAND ----------

def try_cast_count(df, col, to="DECIMAL(10,2)"):
    typed = F.expr(f"try_cast({col} AS {to})")
    bad = df.filter(F.col(col).isNotNull()
                    & typed.isNull())
    print(f"{col}: {bad.count()} bad value(s)")
    return df.withColumn(col, typed), bad

clean, bad = try_cast_count(bronze, "amount")
# amount: 1 bad value(s)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge
# MAGIC 1. Remove the `.filter(...)` line from cell 7. How many rows land in `silver_orders`?
# MAGIC 2. What does `bronze.dropna(how="all").count()` return on the six rows?
# MAGIC 3. Bronze gains a `status` column with `"shipped"`, `"SHIPPED"`, `" Shipped"` and null. In silver, make every value lowercase with no spaces, and fill nulls with `"unknown"`. What do you add?
# MAGIC
# MAGIC Solution (uncomment to check).

# COMMAND ----------

# print(bronze.dropna(subset=["order_id"]).count())   # Q1: 5 (1002 and 1005 keep a NULL amount)
# print(bronze.dropna(how="all").count())              # Q2: 6 (no row is null in every column)
# Q3: before the fillna step add
#   .withColumn("status", F.lower(F.trim("status")))
# and add "status": "unknown" to the fillna dict.
# trim and lower return NULL for a NULL input, so fillna still fills it.

# COMMAND ----------

# MAGIC %md
# MAGIC Earlier lessons: A01 https://youtu.be/PglvHIK-QmU · A02 https://youtu.be/cLrBSH5DXIQ · A03 https://youtu.be/LS0gDlRD_1g · A04 https://youtu.be/AzpWro9XjJY · A05 https://youtu.be/r70MNc5Kla8 · A06 https://youtu.be/-O0mWjGb1gI
# MAGIC
# MAGIC ## Optional cleanup
# MAGIC Drops only the schema this run created.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA workspace.{schema} CASCADE")
