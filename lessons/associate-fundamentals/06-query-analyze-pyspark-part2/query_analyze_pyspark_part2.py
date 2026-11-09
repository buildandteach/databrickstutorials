# Databricks notebook source
# MAGIC %md
# MAGIC # Query and Analyze Data with PySpark — Part 2
# MAGIC **Aggregate with groupBy and analyze with window functions**
# MAGIC 
# MAGIC Python notebook for Databricks Free Edition (serverless). Series: Part 2 of 3. Self-contained fictional sample (same orders as Part 1). No downloads, packages, external paths, UDFs, or paid-only features. Run from the top.
# MAGIC 
# MAGIC This part is **read-only**. Part 1 covered select/filter/nulls. Part 3 covers Spark SQL temp views and a safe managed Delta summary.

# COMMAND ----------

# Explicit schemas + tiny fictional orders (standalone sample)
from decimal import Decimal
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                              StringType, DecimalType, DateType)

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer", StringType(), True),
    StructField("region", StringType(), True),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("order_date", DateType(), False),
])
orders = spark.createDataFrame([
    (101, "Ana", "NORTH", "Books", 2, Decimal("10.00"), date(2026, 10, 1)),
    (102, "Bo", "SOUTH", "Toys", 1, Decimal("25.00"), date(2026, 10, 2)),
    (103, "Cy", "WEST", "Books", 3, Decimal("8.50"), date(2026, 10, 2)),
    (104, "Dee", "NORTH", "Toys", 2, Decimal("15.00"), date(2026, 10, 3)),
    (105, None, "EAST", "Books", 1, Decimal("12.00"), date(2026, 10, 3)),
    (106, "Fay", "SOUTH", "Toys", None, Decimal("20.00"), date(2026, 10, 4)),
    (107, "Gus", "WEST", "Books", 4, None, date(2026, 10, 4)),
    (108, "Hana", "NORTH", "Toys", 1, Decimal("30.00"), date(2026, 10, 5)),
], schema).withColumn(
    "order_total",
    (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)")
)
orders.orderBy("order_id").show(truncate=False)
assert orders.count() == 8
valid = orders.filter(F.col("quantity").isNotNull() & F.col("unit_price").isNotNull())
assert valid.count() == 6
assert valid.agg(F.sum("order_total")).first()[0] == Decimal("142.50")
print("INPUT CHECK: 8 orders; 6 valid revenue rows totaling 142.50")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate: revenue and counts by region and category
# MAGIC `groupBy` changes the grain. `count("*")` counts every row. Summing decimal revenue keeps currency totals exact for this tiny example. Sorting controls presentation only.

# COMMAND ----------

region_category = (valid.groupBy("region", "category")
    .agg(F.count("*").alias("order_count"),
         F.sum("order_total").cast("decimal(18,2)").alias("revenue"))
    .orderBy("region", "category"))
region_category.show(truncate=False)
assert region_category.count() == 5
assert region_category.agg(F.sum("order_count")).first()[0] == 6
assert region_category.agg(F.sum("revenue")).first()[0] == Decimal("142.50")
print("AGG CHECK: 5 region/category buckets, 6 orders, revenue 142.50")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Region-only rollup
# MAGIC A second aggregation answers a different grain: one row per region. Reconcile that region totals still sum to 142.50.

# COMMAND ----------

by_region = (valid.groupBy("region")
    .agg(F.count("*").alias("order_count"),
         F.sum("order_total").cast("decimal(18,2)").alias("revenue"))
    .orderBy("region"))
by_region.show(truncate=False)
assert by_region.count() == 4
assert by_region.agg(F.sum("revenue")).first()[0] == Decimal("142.50")
assert [tuple(r) for r in by_region.collect()] == [
    ("EAST", 1, Decimal("12.00")),
    ("NORTH", 3, Decimal("80.00")),
    ("SOUTH", 1, Decimal("25.00")),
    ("WEST", 1, Decimal("25.50")),
]
print("REGION CHECK: EAST 12 / NORTH 80 / SOUTH 25 / WEST 25.50")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Window functions: row number and running total
# MAGIC Windows compute values relative to a partition without collapsing rows. Number rows by order_total within category using `row_number` (ties broken by higher order_id first). Compute a running revenue total by order_date across all valid orders.

# COMMAND ----------

from pyspark.sql import Window
w_rank = Window.partitionBy("category").orderBy(
    F.col("order_total").desc(), F.col("order_id").desc()
)
w_run = Window.orderBy("order_date", "order_id").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
ranked = (valid
    .withColumn("category_rank", F.row_number().over(w_rank))
    .withColumn("running_revenue",
                F.sum("order_total").over(w_run).cast("decimal(18,2)"))
    .select("order_id", "category", "order_date", "order_total",
            "category_rank", "running_revenue")
    .orderBy("order_date", "order_id"))
ranked.show(truncate=False)
toys = {r.order_id: r.category_rank
        for r in ranked.filter(F.col("category") == "Toys").collect()}
assert toys[108] == 1 and toys[104] == 2 and toys[102] == 3
assert ranked.orderBy(F.desc("order_date"), F.desc("order_id")).first().running_revenue == Decimal("142.50")
print("WINDOW CHECK: Toys ranks 108=1,104=2,102=3; final running revenue 142.50")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice (no writes)
# MAGIC From `valid`, keep Books only, number rows by order_total descending (break ties with order_id descending), and show order_id, order_total, and category_rank. Predict: 103 (25.50, 1), 101 (20.00, 2), 105 (12.00, 3).
# MAGIC 
# MAGIC ## Recap — Part 2
# MAGIC - groupBy changes the grain; always reconcile counts and revenue to the source.
# MAGIC - Windows add ranks and running totals without collapsing rows until you choose to aggregate.
# MAGIC - **Next:** Part 3 — Spark SQL temp views and a safe persisted Delta summary.
# MAGIC 
# MAGIC Official Associate August 2026 Section 3 includes aggregations such as count. Window functions support the same analysis skills on Free Edition. Independent education — no exam-pass guarantee.
# MAGIC https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf
# MAGIC 
# MAGIC Build and Teach — https://buildandteach.com/
