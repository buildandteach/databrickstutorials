# Databricks notebook source
# MAGIC %md
# MAGIC # Choosing an Ingestion Method: Lakeflow Connect, JDBC and REST
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A06 · Exam guide (Aug 2026) Section 2, Data Ingestion and Loading (21%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless). Fictional orders only. No downloads or packages.
# MAGIC
# MAGIC Objectives covered: configure Lakeflow Connect to ingest from enterprise sources into Unity Catalog governed tables; use JDBC/ODBC or REST clients in notebooks to land data in cloud storage or directly in Unity Catalog tables, scheduled with Lakeflow Jobs; prioritize between Auto Loader, Lakeflow Connect (standard and managed connectors), partner connectors and other methods.
# MAGIC
# MAGIC **What runs where**
# MAGIC - Cells 1-5 (REST pattern) run in Free Edition. Free Edition restricts outbound internet access to a limited set of trusted domains, so cell 2 simulates a paginated REST API with a local function; the real `requests.get(...)` call is shown in a comment.
# MAGIC - Cells 6-8 (JDBC from an external database) and the Lakeflow Connect database connector (markdown section) need a **paid workspace** and a reachable database. They are **not runnable in Free Edition**; the JDBC cells are guarded by `RUN_EXTERNAL = False`.
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_ingest_` + 12 hex characters, two managed volumes (`landing`, `checkpoints`) and two managed tables (`api_orders_bronze`, `api_orders_direct`) inside that schema only. Nothing outside it is changed or dropped. Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

RUN_EXTERNAL = False   # True only in a paid workspace with a reachable database, secrets and connections set up

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a schema and two volumes
# MAGIC `landing` holds the raw API responses (it plays the part of cloud object storage). `checkpoints` holds the Auto Loader schema location and stream checkpoint.

# COMMAND ----------

import uuid
catalog = spark.sql("SELECT current_catalog()").first()[0]
schema = f"bt_ingest_{uuid.uuid4().hex[:12]}"

spark.sql(f"CREATE SCHEMA {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")
for v in ["landing", "checkpoints"]:
    spark.sql(f"CREATE VOLUME {v}")

base = f"/Volumes/{catalog}/{schema}"
raw = f"{base}/landing/api/orders"
print("schema:", f"{catalog}.{schema}")
print("raw folder:", raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: a simulated paginated REST API
# MAGIC Page 1 returns three orders and `next_page = 2`; page 2 returns two orders and `next_page = None`. In a paid workspace (or for an allowed domain) replace the body of `get_page` with a real call, for example
# MAGIC `requests.get(url, params={"page": page}, headers={"Authorization": f"Bearer {dbutils.secrets.get('api', 'token')}"}, timeout=30).json()`.
# MAGIC Keep tokens in Databricks secrets, never in the notebook.

# COMMAND ----------

CUSTOMERS = ["ana", "ben", "cara", "dev", "eli"]
AMOUNTS = [25.0, 40.5, 12.0, 60.0, 8.25]
STATUS = ["paid", "paid", "refunded", "paid", "paid"]

def order(i):
    return {"order_id": i, "customer": CUSTOMERS[i - 1],
            "amount": AMOUNTS[i - 1], "status": STATUS[i - 1]}

PAGES = {
  1: {"data": [order(1), order(2), order(3)],
      "next_page": 2},
  2: {"data": [order(4), order(5)],
      "next_page": None}}

def get_page(page):
    # real API (paid workspace or allowed domain):
    # requests.get(url, params={"page": page},
    #   headers=auth, timeout=30).json()
    return PAGES[page]

print(get_page(1)["next_page"], len(get_page(1)["data"]))   # 2 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: follow the pages, land raw JSON in the volume
# MAGIC One file per page, one JSON record per line (JSON Lines), one folder per run (`run_YYYYMMDDHHMMSS`), so nothing is overwritten and every response can be replayed. Expected: prints `5`, two files landed.

# COMMAND ----------

import json, datetime as dt
run_id = dt.datetime.now().strftime("%Y%m%d%H%M%S")
page, n = 1, 0
while page:
    body = get_page(page)
    rows = [json.dumps(r) for r in body["data"]]
    path = f"{raw}/run_{run_id}/p{page}.json"
    dbutils.fs.put(path, "\n".join(rows))
    n += len(rows)
    page = body["next_page"]
print(n)    # 5
display(dbutils.fs.ls(f"{raw}/run_{run_id}"))   # p1.json, p2.json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: load the raw files into a Unity Catalog table with Auto Loader
# MAGIC Same incremental pattern as lesson A04. `cloudFiles.inferColumnTypes` makes `amount` a double and `order_id` a long (otherwise JSON columns are inferred as strings). `availableNow` processes the new files and stops, so a scheduled Lakeflow Jobs run only reads newly landed pages. Expected: 5 rows; columns `amount`, `customer`, `order_id`, `status` (sorted) plus `_rescued_data` (null).

# COMMAND ----------

q = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaLocation",
          f"{base}/checkpoints/schema")
  .load(raw)
  .writeStream
  .option("checkpointLocation",
          f"{base}/checkpoints/bronze")
  .trigger(availableNow=True)
  .toTable("api_orders_bronze"))
q.awaitTermination()

bronze = spark.table("api_orders_bronze")
print(bronze.count())     # 5
display(bronze.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: or write the records straight to a table
# MAGIC Simple for small responses, but there is no raw copy to replay, and running the cell again appends the same five orders (10 rows). Plan for duplicates, for example with a `MERGE` on `order_id`. Expected: `5` on the first run.

# COMMAND ----------

rows, page = [], 1
while page:
    body = get_page(page)
    rows += body["data"]
    page = body["next_page"]

(spark.createDataFrame(rows)
  .write.mode("append")
  .saveAsTable("api_orders_direct"))
print(spark.table("api_orders_direct").count())     # 5 on the first run

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cells 6-8: JDBC from an external database (paid workspace, not runnable in Free Edition)
# MAGIC Spark's built-in JDBC reader reads the schema from the database and maps the types. Credentials come from Databricks secrets (or a Unity Catalog connection), never from the URL. By default the read is **one query on one thread**; set `partitionColumn`, `lowerBound`, `upperBound` and `numPartitions` (an indexed, evenly distributed column; don't overload the source) to read in parallel. `fetchSize` sets rows per round trip. The `query` option pushes a query to the database (it can't be combined with `partitionColumn`; use `dbtable` with a subquery instead).
# MAGIC
# MAGIC `jdbc_url` example form: `jdbc:postgresql://HOST:5432/crm` (placeholder, set your own).

# COMMAND ----------

if RUN_EXTERNAL:
    jdbc_url = "jdbc:postgresql://HOST:5432/crm"   # placeholder
    user = dbutils.secrets.get("crm", "user")
    pwd  = dbutils.secrets.get("crm", "password")

    df = (spark.read.format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "public.customers")
      .option("user", user)
      .option("password", pwd)
      .load())
    df.write.saveAsTable("customers_bronze")
else:
    print("skipped: needs a paid workspace and a reachable database (RUN_EXTERNAL = False)")

# COMMAND ----------

if RUN_EXTERNAL:
    df = (spark.read.format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "public.customers")
      .option("user", user)
      .option("password", pwd)
      .option("partitionColumn", "customer_id")
      .option("lowerBound", "1")
      .option("upperBound", "1000000")
      .option("numPartitions", "8")
      .option("fetchSize", "1000")
      .load())

    # pushdown instead of a table:
    pushed = (spark.read.format("jdbc")
      .option("url", jdbc_url)
      .option("user", user)
      .option("password", pwd)
      .option("query", "SELECT * FROM orders WHERE amount > 0")
      .load())
else:
    print("skipped: needs a paid workspace and a reachable database (RUN_EXTERNAL = False)")

# COMMAND ----------

# MAGIC %md
# MAGIC Governed alternative: a **JDBC Unity Catalog connection** stores the driver, URL and credentials once (created in Catalog Explorer or with `CREATE CONNECTION`). Querying users need `USE CONNECTION`, can't pass `url` or `host`, and the `query` runs in the source database's own SQL dialect. ODBC from Python (for example `pyodbc`) also works, but you install and manage the driver; JDBC is built into Spark.

# COMMAND ----------

if RUN_EXTERNAL:
    df = (spark.read.format("jdbc")
      .option("databricks.connection",
              "crm_jdbc")
      .option("query",
              "SELECT * FROM customers")
      .load())

    # no url, no password in the notebook
    # needs USE CONNECTION on crm_jdbc
else:
    print("skipped: needs a paid workspace and a reachable database (RUN_EXTERNAL = False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Connect managed database connector (paid workspace, not runnable in Free Edition)
# MAGIC Five parts: a **connection** (Unity Catalog securable with the host and credentials), an **ingestion gateway** (classic compute, runs continuously, reads change logs before they're truncated), a **staging volume** (Unity Catalog volume, staged data purged after 30 days), an **ingestion pipeline** (serverless, triggered on a schedule by a job, not continuous) and **destination streaming tables**. First run = full snapshot, later runs apply changes, including deletes (change data capture). SaaS connectors (Salesforce, Workday, ...) use a connection, a pipeline and tables only.
# MAGIC
# MAGIC UI: **Data Ingestion** > SQL Server (under Databricks connectors) > connection > names for the ingestion pipeline, gateway and staging location > source tables > destination catalog and schema > Validate > schedule. The source must have change tracking or CDC enabled.
# MAGIC
# MAGIC Declarative Automation Bundle (shape only; see the Databricks docs "Ingest data from SQL Server" for every option):
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   pipelines:
# MAGIC     gateway:
# MAGIC       continuous: true
# MAGIC       gateway_definition:
# MAGIC         connection_name: sales_sqlserver
# MAGIC         gateway_storage_catalog: main
# MAGIC         gateway_storage_schema: lfc_staging
# MAGIC     ingestion:
# MAGIC       ingestion_definition:
# MAGIC         ingestion_gateway_id: ${resources.pipelines.gateway.id}
# MAGIC         objects:
# MAGIC           - schema:
# MAGIC               source_catalog: sales
# MAGIC               source_schema: dbo
# MAGIC               destination_catalog: main
# MAGIC               destination_schema: bronze_sales
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prioritize: scenario words to method
# MAGIC | Scenario | Method |
# MAGIC |---|---|
# MAGIC | Files keep arriving in cloud storage, any volume | Auto Loader |
# MAGIC | A few files, SQL, idempotent reload | COPY INTO |
# MAGIC | Supported SaaS app or database, least maintenance | Lakeflow Connect managed connector |
# MAGIC | Database changes including deletes | Managed database connector (CDC) |
# MAGIC | Custom query, or a database without a connector | JDBC (or a query-based connector) |
# MAGIC | HTTP API without a connector | REST client in a notebook + Lakeflow Jobs schedule |
# MAGIC | Team already runs a partner tool | Partner connector (Partner Connect) |
# MAGIC | Query in place, no copy | Lakehouse Federation |
# MAGIC
# MAGIC Decide on data volume, ingestion frequency, data types and governance with Unity Catalog. Databricks recommends starting with the most managed layer that supports the source.
# MAGIC
# MAGIC ## Exam traps
# MAGIC 1. Start with the most managed layer that supports your source.
# MAGIC 2. A database CDC connector's gateway runs continuously on classic compute; the ingestion pipeline runs on a schedule on serverless.
# MAGIC 3. A JDBC read is one single-threaded query unless you set `partitionColumn`, `lowerBound`, `upperBound` and `numPartitions`.
# MAGIC 4. Credentials go in secrets or a Unity Catalog connection, never in the JDBC URL.
# MAGIC 5. Auto Loader reads files, not databases or APIs: those need a connector or code that lands files.
# MAGIC
# MAGIC Earlier lessons: A01 https://youtu.be/PglvHIK-QmU · A02 https://youtu.be/cLrBSH5DXIQ · A03 https://youtu.be/LS0gDlRD_1g · A04 https://youtu.be/AzpWro9XjJY · A05 https://youtu.be/r70MNc5Kla8

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice challenge
# MAGIC Pick the method:
# MAGIC 1. Clickstream JSON files land in an S3 bucket every few minutes, millions per day.
# MAGIC 2. Salesforce opportunities must land in Unity Catalog daily with minimal maintenance.
# MAGIC 3. An internal HTTP API with no connector must be pulled every hour into a table.
# MAGIC
# MAGIC Solution (uncomment to check).

# COMMAND ----------

# answers = {
#   "Q1": "Auto Loader (files in cloud storage, incremental, scales to millions of files)",
#   "Q2": "Lakeflow Connect managed SaaS connector (configured, not coded)",
#   "Q3": "REST client in a notebook: land raw in a volume, Auto Loader, schedule hourly with Lakeflow Jobs",
# }
# for k, v in answers.items(): print(k, v)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional cleanup
# MAGIC Drops only the schema this run created.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA {catalog}.{schema} CASCADE")
