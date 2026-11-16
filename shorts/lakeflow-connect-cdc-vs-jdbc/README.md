# Lakeflow Connect vs JDBC: SQL Server Deletes

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A06, Choosing an Ingestion Method: Lakeflow Connect, JDBC and REST](../../lessons/associate-certification/a06-ingestion-methods/) in the Databricks Data Engineer Associate Series. Exam guide Section 2, Data Ingestion and Loading.

A Java Database Connectivity (JDBC) read runs a query, and a query only returns the rows that exist now. A row deleted in SQL Server just disappears, so there is no delete to apply, and you would write and maintain the merge logic yourself. The Lakeflow Connect managed database connector uses change data capture (CDC): an ingestion gateway (classic compute, runs continuously) reads the change log, and a scheduled serverless ingestion pipeline applies inserts, updates and deletes to Unity Catalog streaming tables. The first run takes a full snapshot, then each run applies only the changes.

A06 exam question: a SQL Server orders table must land in Unity Catalog every 30 minutes, deletes included, with the least code to maintain. Answer: the Lakeflow Connect managed database connector.

```python
df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders").load()
# JDBC: rows that exist now. A deleted row is just missing.
# Lakeflow Connect database connector (CDC): inserts, updates, deletes.
```

Free Edition: Lakeflow Connect database connectors are not runnable in Databricks Free Edition. They need a paid workspace and a reachable database, and the gateway runs on classic compute. SQL Server needs change tracking or CDC turned on.

## Memory trick

A query sees what's there. CDC sees what changed.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
