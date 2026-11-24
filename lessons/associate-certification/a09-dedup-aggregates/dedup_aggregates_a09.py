# Databricks notebook source
# MAGIC %md
# MAGIC # Deduplication and Aggregates with PySpark and SQL
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A09 · Exam guide (Aug 2026) Section 3, Data Transformation and Modeling (22%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless compute). Fictional data only. No downloads or packages.
# MAGIC
# MAGIC Objective covered: perform data deduplication operations and aggregate operations on DataFrames, such as count, approximate count distinct, and mean, summary.
# MAGIC
# MAGIC On the job: an orders change data capture (CDC) feed, `orders_cdc`, sends the whole row again on every change. Rows arrive late and files repeat. Keep the latest change per `order_id`, then report the daily order count and average order value (cells 6 to 9, and the reusable snippet in cell 14).
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_dedup_` + 12 hex characters in the `workspace` catalog, and the table `orders_cdc` inside that schema only. Expected results in the comments were checked with a local Apache Spark 4.0 run of the same code, not on Databricks. Which row `dropDuplicates(["order_id"])` keeps is not guaranteed, so cell 4 may show different rows on your run. Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a fresh schema

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.window import Window

schema = "bt_dedup_" + uuid.uuid4().hex[:12]
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")
spark.sql(f"USE workspace.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: the orders_cdc feed
# MAGIC Eight rows in arrival order. Order 5003 is an exact copy, order 5001 has a same-day update (24.00 to 28.50), order 5002 has a late update (30.00 to 36.00, arriving Nov 4). Expected: `8`.

# COMMAND ----------

rows = [
 (5001, 101, "2026-11-02", "24.00", "insert", "2026-11-02 09:00:00"),
 (5002, 102, "2026-11-02", "30.00", "insert", "2026-11-02 10:00:00"),
 (5001, 101, "2026-11-02", "28.50", "update", "2026-11-02 15:30:00"),
 (5003, 103, "2026-11-03", "13.50", "insert", "2026-11-03 08:00:00"),
 (5003, 103, "2026-11-03", "13.50", "insert", "2026-11-03 08:00:00"),
 (5004, 101, "2026-11-03", "40.00", "insert", "2026-11-03 09:15:00"),
 (5005, 104, "2026-11-03", "18.00", "insert", "2026-11-03 11:45:00"),
 (5002, 102, "2026-11-02", "36.00", "update", "2026-11-04 07:05:00")]

cols = ("order_id int, customer_id int, order_date string, "
        "amount string, op string, change_ts string")
(spark.createDataFrame(rows, cols)
  .withColumn("order_date", F.to_date("order_date"))
  .withColumn("amount",
              F.col("amount").cast("decimal(10,2)"))
  .withColumn("change_ts", F.to_timestamp("change_ts"))
  .write.mode("overwrite").saveAsTable("orders_cdc"))

orders_cdc = spark.read.table("orders_cdc")
print(orders_cdc.count())   # 8

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: distinct and dropDuplicates()
# MAGIC Both remove only rows that match in every column. Expected: `7`, `7`.

# COMMAND ----------

print(orders_cdc.distinct().count())         # 7
print(orders_cdc.dropDuplicates().count())   # 7

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: dropDuplicates on a key
# MAGIC One row per `order_id`, but you cannot choose which. Expected: `5` rows. One local run kept 5001 at 24.00 (old) and 5002 at 36.00 (new); a second local run of this notebook kept 30.00 (old) for 5002. Your run may differ.

# COMMAND ----------

dd = orders_cdc.dropDuplicates(["order_id"])
print(dd.count())   # 5

(dd.select("order_id", "amount", "change_ts")
   .orderBy("order_id").show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: the key matters
# MAGIC Too narrow (`customer_id`) deletes a real order; too wide (`order_id` + `change_ts`) keeps the updates. Expected: `4`, then `7`.

# COMMAND ----------

# too narrow: one order per customer
print(orders_cdc.dropDuplicates(
    ["customer_id"]).count())                # 4

# too wide: every update looks unique
print(orders_cdc.dropDuplicates(
    ["order_id", "change_ts"]).count())      # 7

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: keep the latest change with row_number
# MAGIC Partition by the business key, order by the change timestamp descending, keep row 1. Expected: `5`; 5001 = 28.50, 5002 = 36.00 (still on its order date, Nov 2).

# COMMAND ----------

w = (Window.partitionBy("order_id")
           .orderBy(F.col("change_ts").desc()))

orders_latest = (orders_cdc
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn"))

print(orders_latest.count())   # 5
orders_latest.orderBy("order_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: the same thing in SQL
# MAGIC Expected: 2026-11-02, 2, 32.25 and 2026-11-03, 3, 23.83.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_date,
# MAGIC        COUNT(*)              AS order_count,
# MAGIC        ROUND(AVG(amount), 2) AS avg_order_value
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY order_id ORDER BY change_ts DESC) AS rn
# MAGIC   FROM orders_cdc)
# MAGIC WHERE rn = 1
# MAGIC GROUP BY order_date ORDER BY order_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: daily order count and average order value
# MAGIC Aggregate after you dedupe. Expected: 2026-11-02, 2, 32.25 and 2026-11-03, 3, 23.83.

# COMMAND ----------

daily = (orders_latest
    .groupBy("order_date")
    .agg(F.count("*").alias("order_count"),
         F.round(F.avg("amount"), 2)
          .alias("avg_order_value"))
    .orderBy("order_date"))
daily.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: the same report on the raw feed (wrong)
# MAGIC Expected: 2026-11-02, 4, 29.63 and 2026-11-03, 4, 21.25. No error; the numbers are just wrong. `F.avg` and `F.mean` are the same function and skip NULLs.

# COMMAND ----------

(orders_cdc.groupBy("order_date")
    .agg(F.count("*").alias("order_count"),
         F.round(F.avg("amount"), 2)
          .alias("avg_order_value"))
    .orderBy("order_date").show())

# 2026-11-02   4   29.63   <- wrong
# 2026-11-03   4   21.25   <- wrong

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: count, count_distinct, approx_count_distinct
# MAGIC Expected: `8`, `8`, `5`, `5`.

# COMMAND ----------

orders_cdc.select(
  F.count("*").alias("rows"),                      # 8
  F.count("order_id").alias("count_order_id"),     # 8
  F.count_distinct("order_id").alias("distinct"),  # 5
  F.approx_count_distinct("order_id")
   .alias("approx"),                               # 5
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11: NULLs
# MAGIC `count("*")` counts rows; `count(col)` and `count_distinct` skip NULLs. Expected: `9`, `8`, `5`.

# COMMAND ----------

null_row = spark.createDataFrame(
    [(None, 105, None, None, "insert", None)],
    orders_cdc.schema)

orders_cdc.unionByName(null_row).select(
    F.count("*"),                 # 9
    F.count("order_id"),          # 8
    F.count_distinct("order_id")  # 5
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 12: approx_count_distinct on more data
# MAGIC HyperLogLog++, default maximum relative standard deviation (`rsd`) 0.05. Local run: 95546, 102065, 100000 (Databricks may differ slightly). For `rsd` below 0.01, `count_distinct` is more efficient.

# COMMAND ----------

spark.range(100000).agg(
    F.approx_count_distinct("id"),        # 95546
    F.approx_count_distinct("id", 0.1),   # 102065
    F.count_distinct("id"),               # 100000
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 13: describe and summary
# MAGIC `describe()`: count, mean, stddev, min, max. `summary()` adds approximate 25%, 50%, 75% percentiles. Expected for amount: count 5, mean 27.200000, min 13.50, 25% 18.0, 50% 28.5, 75% 36.0, max 40.00.

# COMMAND ----------

orders_latest.select("amount").describe().show()
orders_latest.select("amount").summary().show()
orders_latest.select("amount").summary(
    "count", "mean", "min", "max").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practical takeaway: dedup checklist
# MAGIC 1. **Business key.** What makes a row unique for the business? Here `order_id`, not `customer_id`, and not `order_id` plus `change_ts`.
# MAGIC 2. **Sort.** Which column says which row is newest? Here `change_ts`. Add a tiebreaker if two rows can share it.
# MAGIC 3. **Keep latest.** `row_number()` over `partitionBy(key).orderBy(sort desc)`, keep `rn = 1`. Use `dropDuplicates(key)` only when any surviving row is acceptable, such as exact copies.
# MAGIC 4. **Check the counts.** `count()` after dedupe should equal `count_distinct(key)` before it. Here both are 5.
# MAGIC 5. **Then aggregate.** Count, average and sum on the deduplicated DataFrame.
# MAGIC
# MAGIC Memory trick: dropDuplicates keeps a row. row_number keeps the right row.
# MAGIC
# MAGIC ## Cell 14: reusable snippet
# MAGIC Expected: no assertion error, and the same daily report as cell 8.

# COMMAND ----------

def keep_latest(df, key_cols, ts_col):
    w = (Window.partitionBy(*key_cols)
               .orderBy(F.col(ts_col).desc()))
    return (df.withColumn("rn", F.row_number().over(w))
              .filter("rn = 1")
              .drop("rn"))

orders_latest = keep_latest(
    orders_cdc, ["order_id"], "change_ts")

expected = orders_cdc.select(
    F.count_distinct("order_id")).first()[0]
assert orders_latest.count() == expected   # 5 == 5

daily = (orders_latest.groupBy("order_date")
    .agg(F.count("*").alias("order_count"),
         F.round(F.avg("amount"), 2)
          .alias("avg_order_value")))
daily.orderBy("order_date").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam-style question (original)
# MAGIC A data engineer must report the exact number of distinct customers who placed an order last month, for an invoice to a partner. The `orders` table has 2 billion rows. Which expression should they use?
# MAGIC - A) `F.approx_count_distinct("customer_id")`
# MAGIC - B) `F.count("customer_id")`
# MAGIC - C) `F.count_distinct("customer_id")`
# MAGIC - D) `orders.dropDuplicates(["order_id"]).count()`
# MAGIC
# MAGIC Answer: **C.** Exact and distinct means `count_distinct`. A is an estimate (about 5% by default), fine for a dashboard but not an invoice. B counts every non-NULL `customer_id`. D counts unique orders, not customers.
# MAGIC
# MAGIC ## Practice challenge
# MAGIC 1. `orders_cdc.filter("op = 'insert'").dropDuplicates().count()`
# MAGIC 2. Using `orders_latest`, what is customer 101's total amount?
# MAGIC 3. `orders_latest.select(F.count_distinct("customer_id"))`
# MAGIC
# MAGIC Solution (uncomment to check).

# COMMAND ----------

# print(orders_cdc.filter("op = 'insert'").dropDuplicates().count())   # Q1: 5 (6 insert rows, one exact copy of 5003)
# print(orders_latest.filter("customer_id = 101").agg(F.sum("amount")).first()[0])   # Q2: 68.50 (5001 at 28.50 + 5004 at 40.00)
# print(orders_latest.select(F.count_distinct("customer_id")).first()[0])   # Q3: 4 (customers 101, 102, 103, 104)

# COMMAND ----------

# MAGIC %md
# MAGIC Earlier lessons: A01 https://youtu.be/PglvHIK-QmU · A02 https://youtu.be/cLrBSH5DXIQ · A03 https://youtu.be/LS0gDlRD_1g · A04 https://youtu.be/AzpWro9XjJY · A05 https://youtu.be/r70MNc5Kla8 · A06 https://youtu.be/-O0mWjGb1gI · A07 https://youtu.be/daDUswtJYTM · A08 [a08-joins-unions](../a08-joins-unions/)
# MAGIC
# MAGIC ## Optional cleanup
# MAGIC Drops only the schema this run created.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA workspace.{schema} CASCADE")
