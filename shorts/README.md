# Shorts

One-page cheat sheets for published Build and Teach Shorts. Each PDF matches that Short's script: a definition, key points, a tiny example, and a misconception check. They are not summaries of the longer lessons.

Generate them from the repository root:

```bash
python3 cheatsheets/generate_short_cheatsheets.py
```

Future Shorts need a sheet too. Add the topic in that script, run it, commit `shorts/<topic>/cheatsheet.pdf`, and add a row here and in the root README.

| Short | Video | Cheat sheet |
|---|---|---|
| What is a lakehouse? | [youtube.com/shorts/LB9BbsPiJX0](https://youtube.com/shorts/LB9BbsPiJX0) | [PDF](lakehouse/cheatsheet.pdf) |
| What is Databricks? | [youtube.com/shorts/P2MW-ws3pUQ](https://youtube.com/shorts/P2MW-ws3pUQ) | [PDF](databricks/cheatsheet.pdf) |
| What is Delta Lake? | [youtube.com/shorts/Wh7SJFTdJWs](https://youtube.com/shorts/Wh7SJFTdJWs) | [PDF](delta-lake/cheatsheet.pdf) |
| What is a Spark DataFrame? | [youtube.com/shorts/qQPjfrxBsRI](https://youtube.com/shorts/qQPjfrxBsRI) | [PDF](dataframe/cheatsheet.pdf) |
| What is a Databricks notebook? | [youtube.com/shorts/JlCLoWxcSg8](https://youtube.com/shorts/JlCLoWxcSg8) | [PDF](notebook/cheatsheet.pdf) |
| What are ACID transactions? | [youtube.com/shorts/gtamYg4ylX4](https://youtube.com/shorts/gtamYg4ylX4) | [PDF](acid-transactions/cheatsheet.pdf) |
| What is Apache Spark? | [youtube.com/shorts/Gv6Xpn_zwpY](https://youtube.com/shorts/Gv6Xpn_zwpY) | [PDF](apache-spark/cheatsheet.pdf) |
| Delta Lake vs. Parquet | [youtube.com/shorts/OHsaXYJfSVk](https://youtube.com/shorts/OHsaXYJfSVk) | [PDF](delta-vs-parquet/cheatsheet.pdf) |
| What is a data lake? | [youtube.com/shorts/yM5bQyyQkx4](https://youtube.com/shorts/yM5bQyyQkx4) | [PDF](data-lake/cheatsheet.pdf) |
| Select and filter rows in PySpark | [youtube.com/shorts/PEDSIxOwqo4](https://youtube.com/shorts/PEDSIxOwqo4) | [PDF](select-filter-nulls/cheatsheet.pdf) |
| groupBy aggregations in PySpark | [youtube.com/shorts/nzDoDiGYpdo](https://youtube.com/shorts/nzDoDiGYpdo) | [PDF](groupby-aggregations/cheatsheet.pdf) |
| Spark SQL and a safe Delta write | [youtube.com/shorts/db0g-hUIFJc](https://youtube.com/shorts/db0g-hUIFJc) | [PDF](spark-sql-delta/cheatsheet.pdf) |
| Choose Databricks Compute by Workload | [youtu.be/jB1ayvonDas](https://youtu.be/jB1ayvonDas) | [PDF](compute-choice/cheatsheet.pdf) |
| Auto Loader: Load Only New Files · [Lesson 2, Ingestion Patterns](../lessons/associate-certification/a02-ingestion-patterns/) | [youtube.com/shorts/Bv2EzzZoSnI](https://youtube.com/shorts/Bv2EzzZoSnI) | [PDF](auto-loader-checkpoint/cheatsheet.pdf) |
| COPY INTO: Rerun Loads 0 Rows · [Lesson A03, COPY INTO Incremental Loading](../lessons/associate-certification/a03-copy-into/) | [youtube.com/shorts/j2Hz6lvYsz0](https://youtube.com/shorts/j2Hz6lvYsz0) | [PDF](copy-into-idempotent/cheatsheet.pdf) |
| Auto Loader: A New Column Fails Once · [Lesson A04, Auto Loader Schema Evolution](../lessons/associate-certification/a04-autoloader-schema-evolution/) | [youtube.com/shorts/OEVf6PPOcVY](https://youtube.com/shorts/OEVf6PPOcVY) | [PDF](auto-loader-new-column-fails/cheatsheet.pdf) |
| explode vs explode_outer: 3 Rows or 5 · [Lesson A05, JSON and Nested Data with explode](../lessons/associate-certification/a05-json-explode/) | [youtube.com/shorts/uK2g4muf03c](https://youtube.com/shorts/uK2g4muf03c) | [PDF](explode-vs-explode-outer/cheatsheet.pdf) |
| Lakeflow Connect vs JDBC: SQL Server Deletes · [Lesson A06, Choosing an Ingestion Method](../lessons/associate-certification/a06-ingestion-methods/) | [youtube.com/shorts/CkzOkBrxHFo](https://youtube.com/shorts/CkzOkBrxHFo) | [PDF](lakeflow-connect-cdc-vs-jdbc/cheatsheet.pdf) |
| PySpark cast vs try_cast: One N/A Fails the Job · [Lesson A07, Bronze to Silver Cleaning](../lessons/associate-certification/a07-bronze-to-silver/) | Coming soon | [PDF](pyspark-cast-vs-try-cast/cheatsheet.pdf) |
| PySpark union vs UNION ALL: Duplicates Are Kept · [Lesson A08, Joins and Unions](../lessons/associate-certification/a08-joins-unions/) | [youtube.com/shorts/H_MMKRj7CYk](https://youtube.com/shorts/H_MMKRj7CYk) | [PDF](pyspark-union-vs-union-all/cheatsheet.pdf) |
| PySpark dropDuplicates Trap: The Latest Row Is Not Guaranteed · [Lesson A09, Deduplication and Aggregates](../lessons/associate-certification/a09-dedup-aggregates/) | [youtube.com/shorts/PdoTF8ZuEEY](https://youtube.com/shorts/PdoTF8ZuEEY) | [PDF](pyspark-dropduplicates-latest-row/cheatsheet.pdf) |
