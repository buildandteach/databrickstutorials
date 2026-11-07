# Databricks notebook source
# MAGIC %md
# MAGIC # Clean and Transform Data with PySpark — Build and Teach
# MAGIC **Hands-on Free Edition lesson.** Fictional orders; no files to download. Python notebook on serverless compute.
# MAGIC 
# MAGIC Learn select, filter, null handling, withColumn, orderBy, and a safe managed Delta write. Run cells top to bottom. Transformations return new DataFrames; they do not change the original.
# MAGIC 
# MAGIC Safety: this notebook creates a uniquely named demo schema in your currently selected catalog, and one new table. No overwrite, DROP, external path, custom Spark config, or Python UDF. A second write in the same run stops instead of replacing data. A full rerun makes a new demo schema. Keep the printed identifier for later review. Small demo tables remain until you deliberately remove them yourself.

# COMMAND ----------

# 1. Imports and a tiny, fictional dataset with explicit nullable fields
from decimal import Decimal
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                              StringType, DecimalType)

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("region", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("internal_note", StringType(), True),
])
data = [
    (1001, " Ana ", "north", 2, Decimal("10.00"), "fictional"),
    (1002, "Bo", "south", None, Decimal("5.00"), "missing quantity"),
    (1003, None, " east ", 3, Decimal("7.50"), "missing customer"),
    (1004, "Dee", "south", -1, Decimal("9.00"), "invalid quantity"),
    (1005, "Eli", "east", 1, None, "missing price"),
    (1006, "Fay", "north", 1, Decimal("12.00"), None),
    (1007, "Gus", "west", 2, Decimal("4.25"), None),
    (1008, "Hana", "  ", 1, Decimal("3.00"), "blank region"),
]
raw_orders = spark.createDataFrame(data, schema)
raw_orders.printSchema()
raw_orders.orderBy("order_id").show(truncate=False)
print("Raw rows:", raw_orders.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select only the columns we need
# MAGIC `select` creates a new DataFrame. The raw data still has its internal note column. `show` and `count` are actions that ask Spark to compute a result. All counts here are teaching checks for eight rows, not a recommended large-production monitoring design.

# COMMAND ----------

selected = raw_orders.select(
    "order_id", "customer", "region", "quantity", "unit_price"
)
assert "internal_note" not in selected.columns
assert "internal_note" in raw_orders.columns
selected.orderBy("order_id").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Make a deliberate null-handling policy
# MAGIC Do not turn missing quantities or prices into zero: that would invent sales information. Reject rows with missing/invalid required numeric values. For optional descriptive labels, use `Unknown`. This lesson treats quantity as strictly positive and permits a zero price; returns need a separate business rule. SQL null comparisons are not true; use `isNull` / `isNotNull` when testing for missing values.

# COMMAND ----------

valid = (
    F.col("order_id").isNotNull()
    & F.col("quantity").isNotNull()
    & F.col("unit_price").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("unit_price") >= 0)
)
accepted = selected.filter(valid)
# Because every part of valid has explicit null guards, rejected rows are clear.
rejected = selected.filter(~valid)
print("Accepted:", accepted.count(), "Rejected:", rejected.count())
rejected.orderBy("order_id").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clean labels using built-in functions
# MAGIC `trim` removes surrounding spaces. Empty and null labels become `Unknown`. Region names are uppercased. `withColumn` returns a new DataFrame with a new or replaced column; it does not mutate `accepted`. These built-ins avoid a Python UDF.

# COMMAND ----------

def label_or_unknown(column_name):
    value = F.trim(F.col(column_name))
    return F.when(value.isNull() | (value == ""), F.lit("Unknown")).otherwise(value)

labeled = (
    accepted
    .withColumn("customer", label_or_unknown("customer"))
    .withColumn("region", F.upper(label_or_unknown("region")))
)
labeled.orderBy("order_id").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Derive a total and order the displayed result
# MAGIC Decimal prices avoid binary floating-point examples for currency. We explicitly cast each derived total to decimal(12,2). The chosen size fits this tiny dataset; production precision and overflow policies need deliberate design. `orderBy` controls this displayed result, not the physical order of a saved table.

# COMMAND ----------

clean_orders = labeled.withColumn(
    "order_total", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)")
)
clean_orders.orderBy("order_id").show(truncate=False)
# The original DataFrame is still unchanged.
assert raw_orders.count() == 8
assert "order_total" not in raw_orders.columns
assert clean_orders.count() == 5
assert rejected.count() == 3
assert clean_orders.agg(F.sum("order_total")).first()[0] == Decimal("66.00")
print("In-memory checks passed: 8 raw, 5 clean, 3 rejected, total 66.00")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create an isolated managed Delta table — fail rather than overwrite
# MAGIC Use the writable catalog already selected in the notebook. If you get a permissions error, stop and select a catalog where you have permission to create a schema; do not bypass permissions. The random suffix prevents clashes. Keep the printed name for the SQL result. Running this setup again creates a fresh demo schema, not a replacement for an old one.

# COMMAND ----------

# Quote identifiers; never insert a raw user-supplied identifier into SQL.
def qi(name):
    return "`" + name.replace("`", "``") + "`"

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
demo_schema = "bt_clean_orders_" + uuid4().hex[:12]
assert demo_schema.startswith("bt_clean_orders_")
schema_fqn = qi(catalog_name) + "." + qi(demo_schema)
table_fqn = schema_fqn + "." + qi("clean_orders")
spark.sql(f"CREATE SCHEMA {schema_fqn} COMMENT 'Build and Teach fictional orders demo'")
print("Demo schema:", schema_fqn)
print("New managed table:", table_fqn)


# COMMAND ----------

# mode("error") deliberately fails if the table exists; no overwrites.
# If this cell says the table exists, skip it and run verification below.
assert demo_schema.startswith("bt_clean_orders_")
clean_orders.write.format("delta").mode("error").saveAsTable(table_fqn)
print("Created new managed Delta table:", table_fqn)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify the stored result, not just the in-memory DataFrame
# MAGIC Read the fully qualified table. SQL uses ORDER BY for predictable presentation. No table should be assumed sorted simply because an earlier DataFrame was sorted.

# COMMAND ----------

verified = spark.sql(f"""
SELECT order_id, customer, region, quantity, unit_price, order_total
FROM {table_fqn}
ORDER BY order_id
""")
verified.show(truncate=False)
summary = spark.sql(f"""
SELECT COUNT(*) AS row_count, SUM(order_total) AS total_sales
FROM {table_fqn}
""")
summary.show()
check = summary.first()
assert check["row_count"] == 5
assert check["total_sales"] == Decimal("66.00")
assert [r.order_id for r in verified.collect()] == [1001, 1003, 1006, 1007, 1008]
print("PASS: persisted Delta table has 5 rows, total_sales = 66.00")
print("Copy this SQL for a SQL cell:")
print(f"SELECT * FROM {table_fqn} ORDER BY order_id;")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge (does not change the saved table)
# MAGIC From `clean_orders`, select NORTH orders with `order_total >= 15`, then select only order_id, customer and order_total, and show the result ordered by order_id. Predict the result before running. Expected: order 1001, Ana, 20.00. Leave the saved table alone.
# MAGIC 
# MAGIC ## Cheat sheet
# MAGIC ```python
# MAGIC # select columns; filter rows
# MAGIC result = df.select("a", "b").filter(F.col("a").isNotNull())
# MAGIC # derive or replace a column — returns a NEW DataFrame
# MAGIC result = result.withColumn("c", F.col("b") * 2)
# MAGIC result.orderBy("a").show()
# MAGIC # new managed table; stop if it already exists
# MAGIC result.write.format("delta").mode("error").saveAsTable(unique_table_name)
# MAGIC ```
# MAGIC 
# MAGIC No automatic cleanup is included. Your small demo schema and table remain for inspection; deliberately remove only the exact demo objects you created if you later choose to clean up.
# MAGIC 
# MAGIC Build and Teach — https://buildandteach.com/
# MAGIC Independent education, no exam-pass guarantee. Foundation skills support further study, not a complete Associate or Professional exam course.
