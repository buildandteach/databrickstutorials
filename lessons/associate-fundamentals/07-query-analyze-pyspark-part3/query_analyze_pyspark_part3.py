# Databricks notebook source
# MAGIC %md
# MAGIC # Query and Analyze Data with PySpark — Part 3
# MAGIC **Spark SQL temp views and a safe managed Delta summary**
# MAGIC 
# MAGIC Python notebook for Databricks Free Edition (serverless). Series: Part 3 of 3. Self-contained fictional sample (same orders as Parts 1–2). No downloads, packages, external paths, UDFs, or paid-only features. Run from the top.
# MAGIC 
# MAGIC Parts 1–2 covered the DataFrame API for querying, nulls, aggregations, and windows. This part registers a temp view, answers the same questions in SQL, then optionally writes **one NEW** managed Delta summary in a unique `bt_query_<suffix>` schema with `mode("error")`.
# MAGIC 
# MAGIC Safety: no overwrite, append, DROP, or reuse of existing user tables. If a write already succeeded, skip it and run verification. Full reruns create another tiny demo schema.

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
region_category = (valid.groupBy("region", "category")
    .agg(F.count("*").alias("order_count"),
         F.sum("order_total").cast("decimal(18,2)").alias("revenue"))
    .orderBy("region", "category"))
assert valid.count() == 6 and region_category.count() == 5
assert region_category.agg(F.sum("revenue")).first()[0] == Decimal("142.50")
print("INPUT CHECK: 6 valid rows / 5 region-category buckets / revenue 142.50")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Register a temporary view
# MAGIC `createOrReplaceTempView` makes the DataFrame queryable with Spark SQL in this session only. Prefer explicit column lists when teaching which fields matter.

# COMMAND ----------

valid.createOrReplaceTempView("valid_orders")
sql_high = spark.sql("""
SELECT order_id, customer, region, category, order_total
FROM valid_orders
WHERE order_total >= 30.00
ORDER BY order_total DESC, order_id DESC
""")
sql_high.show(truncate=False)
assert [r.order_id for r in sql_high.collect()] == [108, 104]
assert sql_high.agg(F.sum("order_total")).first()[0] == Decimal("60.00")
print("SQL CHECK: high-value orders 108,104 totaling 60.00")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Same aggregation in SQL
# MAGIC Confirm the regional/category summary with SQL. Totals must match the DataFrame aggregation already built in memory.

# COMMAND ----------

sql_summary = spark.sql("""
SELECT region, category,
       COUNT(*) AS order_count,
       CAST(SUM(order_total) AS DECIMAL(18,2)) AS revenue
FROM valid_orders
GROUP BY region, category
ORDER BY region, category
""")
sql_summary.show(truncate=False)
assert [tuple(r) for r in sql_summary.collect()] == [
    ("EAST", "Books", 1, Decimal("12.00")),
    ("NORTH", "Books", 1, Decimal("20.00")),
    ("NORTH", "Toys", 2, Decimal("60.00")),
    ("SOUTH", "Toys", 1, Decimal("25.00")),
    ("WEST", "Books", 1, Decimal("25.50")),
]
assert sql_summary.agg(F.sum("revenue")).first()[0] == Decimal("142.50")
print("SQL AGG CHECK: 5 buckets totaling revenue 142.50 (matches DataFrame)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an isolated managed-table destination
# MAGIC Choose a catalog where you may create a schema. Permission failure means stop and choose a permitted catalog. This cell creates a new schema each time it runs — do not rerun it just to refresh verification.

# COMMAND ----------

from uuid import uuid4

def qi(name):
    return "`" + name.replace("`", "``") + "`"

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
demo_schema = "bt_query_" + uuid4().hex[:12]
assert demo_schema.startswith("bt_query_")
schema_fqn = qi(catalog_name) + "." + qi(demo_schema)
table_fqn = schema_fqn + "." + qi("region_category_revenue")
spark.sql(f"CREATE SCHEMA {schema_fqn} COMMENT 'Build and Teach fictional query/analyze Part 3 demo'")
print("Demo schema:", schema_fqn)
print("New managed Delta summary:", table_fqn)


# COMMAND ----------

# Exact spelling "error" refuses replacement if the table already exists.
assert demo_schema.startswith("bt_query_")
region_category.write.format("delta").mode("error").saveAsTable(table_fqn)
print("CREATED managed Delta summary:", table_fqn)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the persisted summary with SQL
# MAGIC Read the stored table with its fully qualified name. Expected buckets are assertions, not pre-populated outputs.

# COMMAND ----------

persisted = spark.sql(f"""
SELECT region, category, order_count, revenue
FROM {table_fqn}
ORDER BY region, category
""")
persisted.show(truncate=False)
expected = [
    ("EAST", "Books", 1, Decimal("12.00")),
    ("NORTH", "Books", 1, Decimal("20.00")),
    ("NORTH", "Toys", 2, Decimal("60.00")),
    ("SOUTH", "Toys", 1, Decimal("25.00")),
    ("WEST", "Books", 1, Decimal("25.50")),
]
assert [tuple(r) for r in persisted.collect()] == expected
checks = spark.sql(f"""
SELECT COUNT(*) AS bucket_count,
       SUM(order_count) AS total_orders,
       SUM(revenue) AS total_revenue
FROM {table_fqn}
""")
checks.show()
a = checks.first()
assert (a.bucket_count, a.total_orders, a.total_revenue) == (5, 6, Decimal("142.50"))
print("PASS: stored summary = 5 buckets / 6 orders / revenue 142.50")
print("Copy for a SQL cell:")
print(f"SELECT * FROM {table_fqn} ORDER BY region, category;")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice (no writes)
# MAGIC Using SQL on `valid_orders`, list WEST or EAST orders ordered by order_id. Predict: 103 (WEST, 25.50), 105 (EAST, 12.00). Do not overwrite the saved summary.
# MAGIC 
# MAGIC ## Recap — Part 3 (series complete)
# MAGIC - createOrReplaceTempView + spark.sql answer the same questions as the DataFrame API.
# MAGIC - Save only a NEW managed Delta table with mode("error"), then verify every persisted bucket.
# MAGIC - Series path: Part 1 shaped rows and nulls → Part 2 aggregated and ranked → Part 3 queried in SQL and persisted safely.
# MAGIC 
# MAGIC Official Associate August 2026 Section 3 includes aggregations and data quality validation. This series practices a Free Edition subset. Independent education — no exam-pass guarantee.
# MAGIC https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf
# MAGIC 
# MAGIC Build and Teach — https://buildandteach.com/
