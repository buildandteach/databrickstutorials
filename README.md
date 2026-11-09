# databrickstutorials

Build and Teach lessons for Databricks Free Edition.

The first track is **Associate fundamentals**. It starts with managed Delta tables and is the base later lessons can build toward Professional skills.

Each lesson folder includes a 1–2 page cheat sheet, linked in the table below. Future lessons need a matching PDF: add the sheet in [`cheatsheets/generate_cheatsheets.py`](cheatsheets/generate_cheatsheets.py), run that script from the repository root, commit `cheatsheet.pdf`, and link it from the lesson README and this index.

| Lesson | Cheat sheet | What you practice |
|---|---|---|
| [01 First Delta Lake table](lessons/associate-fundamentals/01-first-delta-lake-table/) | [PDF](lessons/associate-fundamentals/01-first-delta-lake-table/cheatsheet.pdf) | Managed Delta tables, SQL and PySpark reads, `UPDATE`, `DESCRIBE HISTORY`, and time travel |
| [02 Clean and transform with PySpark](lessons/associate-fundamentals/02-clean-transform-pyspark/) | [PDF](lessons/associate-fundamentals/02-clean-transform-pyspark/cheatsheet.pdf) | `select`, `filter`, null handling, `withColumn`, and a `mode("error")` Delta write |
| [03 Join and aggregate with PySpark](lessons/associate-fundamentals/03-join-aggregate-pyspark/) | [PDF](lessons/associate-fundamentals/03-join-aggregate-pyspark/cheatsheet.pdf) | Inner and left joins, join grain, and a `mode("error")` region summary |
| [04 Ingest files with Auto Loader](lessons/associate-fundamentals/04-autoloader-ingestion/) · [video](https://youtu.be/4HEnm4xL1_0) | [PDF](lessons/associate-fundamentals/04-autoloader-ingestion/cheatsheet.pdf) | Incremental CSV ingestion into one bronze Delta table |
| [05 Query and analyze with PySpark, part 1 of 3](lessons/associate-fundamentals/05-query-analyze-pyspark-part1/) · [video](https://youtu.be/Wh3oLjesxTc) | [PDF](lessons/associate-fundamentals/05-query-analyze-pyspark-part1/cheatsheet.pdf) | Select, filter, derived columns, and nulls. Read-only. [Short](shorts/select-filter-nulls/) |
| [06 Query and analyze with PySpark, part 2 of 3](lessons/associate-fundamentals/06-query-analyze-pyspark-part2/) · [video](https://youtu.be/H3Et0gWCnek) | [PDF](lessons/associate-fundamentals/06-query-analyze-pyspark-part2/cheatsheet.pdf) | `groupBy`, aggregations, and window functions. Read-only. [Short](shorts/groupby-aggregations/) |
| [07 Query and analyze with PySpark, part 3 of 3](lessons/associate-fundamentals/07-query-analyze-pyspark-part3/) · [video](https://youtu.be/sqNHVjQdz9M) | [PDF](lessons/associate-fundamentals/07-query-analyze-pyspark-part3/cheatsheet.pdf) | Spark SQL, then one `mode("error")` summary table. [Short](shorts/spark-sql-delta/) |

Lessons 05–07 are one series, Query and Analyze Data with PySpark. Parts 1 and 2 only read the in-memory sample. Part 3 writes a new `bt_query_<suffix>` schema.

Open each lesson README for Free Edition setup. Lesson 01 uses fictional customers and orders in `byte_delta_demo_20261106`. Lesson 02 uses eight fictional orders and writes a new `bt_clean_orders_<suffix>` schema. Lesson 03 uses four customers and six orders and writes a new `bt_joins_<suffix>` schema. Lesson 04 writes a new `bt_ingest_<suffix>` schema. Lessons 03–07 ship the supplied cheat-sheet PDFs; those companion packages did not include generator source for the lesson PDFs.

## Shorts

Each published Short has its own one-page cheat sheet. A Short sheet is the video's definition and example, not a summary of a long lesson. Future Shorts need a sheet too: add it in [`cheatsheets/generate_short_cheatsheets.py`](cheatsheets/generate_short_cheatsheets.py), run that script from the repository root, commit `shorts/<topic>/cheatsheet.pdf`, and add a row to this table.

| Short | Video | Cheat sheet |
|---|---|---|
| What is a lakehouse? | [youtube.com/shorts/LB9BbsPiJX0](https://youtube.com/shorts/LB9BbsPiJX0) | [PDF](shorts/lakehouse/cheatsheet.pdf) |
| What is Databricks? | [youtube.com/shorts/P2MW-ws3pUQ](https://youtube.com/shorts/P2MW-ws3pUQ) | [PDF](shorts/databricks/cheatsheet.pdf) |
| What is Delta Lake? | [youtube.com/shorts/Wh7SJFTdJWs](https://youtube.com/shorts/Wh7SJFTdJWs) | [PDF](shorts/delta-lake/cheatsheet.pdf) |
| What is a Spark DataFrame? | [youtube.com/shorts/qQPjfrxBsRI](https://youtube.com/shorts/qQPjfrxBsRI) | [PDF](shorts/dataframe/cheatsheet.pdf) |
| What is a Databricks notebook? | [youtube.com/shorts/JlCLoWxcSg8](https://youtube.com/shorts/JlCLoWxcSg8) | [PDF](shorts/notebook/cheatsheet.pdf) |
| What are ACID transactions? | Coming soon (pending review) | [PDF](shorts/acid-transactions/cheatsheet.pdf) |
| What is Apache Spark? | Coming soon (pending review) | [PDF](shorts/apache-spark/cheatsheet.pdf) |
| Delta Lake vs. Parquet | [youtube.com/shorts/OHsaXYJfSVk](https://youtube.com/shorts/OHsaXYJfSVk) | [PDF](shorts/delta-vs-parquet/cheatsheet.pdf) |
| What is a data lake? | [youtube.com/shorts/yM5bQyyQkx4](https://youtube.com/shorts/yM5bQyyQkx4) | [PDF](shorts/data-lake/cheatsheet.pdf) |
| Select and filter rows in PySpark | [youtu.be/Wh3oLjesxTc](https://youtu.be/Wh3oLjesxTc) | [PDF](shorts/select-filter-nulls/cheatsheet.pdf) |
| groupBy aggregations in PySpark | [youtu.be/H3Et0gWCnek](https://youtu.be/H3Et0gWCnek) | [PDF](shorts/groupby-aggregations/cheatsheet.pdf) |
| Spark SQL and a safe Delta write | [youtu.be/sqNHVjQdz9M](https://youtu.be/sqNHVjQdz9M) | [PDF](shorts/spark-sql-delta/cheatsheet.pdf) |
