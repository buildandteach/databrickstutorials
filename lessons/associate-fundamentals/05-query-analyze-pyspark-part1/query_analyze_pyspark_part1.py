# Databricks notebook source
# MAGIC %md
# MAGIC # Query and Analyze Data with PySpark — Part 1
# MAGIC **Select, filter, derive columns, and inspect nulls**
# MAGIC 
# MAGIC Python notebook for Databricks Free Edition (serverless). Series: Part 1 of 3. Fictional inline sample only — no downloads, packages, external paths, UDFs, or paid-only features. Run from the top.
# MAGIC 
# MAGIC This part is **read-only**: no tables are created or replaced. Part 2 covers groupBy, aggregations, and window functions. Part 3 covers Spark SQL temp views and a safe managed Delta summary.

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
print("INPUT CHECK: 8 orders (2 incomplete quantity/price rows for null practice)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Select, filter, and derive
# MAGIC `select` chooses columns. `filter` / `where` keep rows. `withColumn` returns a NEW DataFrame. Incomplete numeric rows stay available for null practice and are excluded from revenue examples with an explicit validity rule.

# COMMAND ----------

# Valid revenue rows require both quantity and unit_price.
valid = orders.filter(
    F.col("quantity").isNotNull() & F.col("unit_price").isNotNull()
).withColumn(
    "is_high_value", F.col("order_total") >= F.lit(Decimal("25.00"))
)
books_north_west = (valid
    .select("order_id", "customer", "region", "category", "order_total", "is_high_value")
    .where((F.col("category") == "Books") & (F.col("region").isin("NORTH", "WEST")))
    .orderBy("order_id"))
books_north_west.show(truncate=False)
assert valid.count() == 6
assert books_north_west.count() == 2
assert [r.order_id for r in books_north_west.collect()] == [101, 103]
assert books_north_west.agg(F.sum("order_total")).first()[0] == Decimal("45.50")
print("SELECT/FILTER CHECK: 6 valid rows; Books in NORTH/WEST = 101,103 totaling 45.50")


# COMMAND ----------

# MAGIC %md
# MAGIC ## High-value filter and ordering
# MAGIC Keep shaping the same `valid` DataFrame: filter on the derived flag, then `orderBy` for a predictable display. Sorting controls presentation only.

# COMMAND ----------

high_value = (valid
    .filter(F.col("is_high_value"))
    .select("order_id", "customer", "region", "category", "order_total")
    .orderBy(F.col("order_total").desc(), F.col("order_id").desc()))
high_value.show(truncate=False)
assert [r.order_id for r in high_value.collect()] == [108, 104, 103, 102]
assert high_value.agg(F.sum("order_total")).first()[0] == Decimal("110.50")
print("HIGH-VALUE CHECK: 108,104,103,102 totaling 110.50")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect nulls before cleaning
# MAGIC `printSchema`, `summary`, and explicit null counts make gaps visible. Incomplete rows were kept on purpose. Cleaning is a business decision; this lesson only inspects.

# COMMAND ----------

orders.printSchema()
orders.select("quantity", "unit_price", "order_total").summary(
    "count", "min", "max", "mean"
).show()
null_counts = orders.select(
    F.count(F.when(F.col("customer").isNull(), 1)).alias("null_customer"),
    F.count(F.when(F.col("quantity").isNull(), 1)).alias("null_quantity"),
    F.count(F.when(F.col("unit_price").isNull(), 1)).alias("null_unit_price"),
)
null_counts.show()
assert null_counts.first().asDict() == {
    "null_customer": 1, "null_quantity": 1, "null_unit_price": 1
}
print("NULL CHECK: one missing customer, quantity, and unit_price")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice (no writes)
# MAGIC From `valid`, select Toys in SOUTH or NORTH, keep order_id / customer / order_total, and order by order_total descending. Predict before running: 108 (30.00), 104 (30.00), 102 (25.00).
# MAGIC 
# MAGIC ## Recap — Part 1
# MAGIC - Select and filter shape the rows you care about; withColumn derives new columns on a NEW DataFrame.
# MAGIC - Exclude incomplete revenue rows with an explicit null rule rather than inventing totals.
# MAGIC - Inspect nulls with schema/summary/counts before any cleaning policy.
# MAGIC - **Next:** Part 2 — groupBy, aggregations, and window functions on the same sample.
# MAGIC 
# MAGIC Official Associate August 2026 Section 3 covers column/row manipulation and data quality checks. This part practices a Free Edition subset. Independent education — no exam-pass guarantee.
# MAGIC https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf
# MAGIC 
# MAGIC Build and Teach — https://buildandteach.com/
