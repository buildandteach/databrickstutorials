# databrickstutorials

Build and Teach lessons for Databricks Free Edition.

The first track is **Associate fundamentals**. It starts with managed Delta tables and is the base later lessons can build toward Professional skills.

Each lesson folder includes a 1–2 page cheat sheet, linked in the table below. Future lessons need a matching PDF: add the sheet in [`cheatsheets/generate_cheatsheets.py`](cheatsheets/generate_cheatsheets.py), run that script from the repository root, commit `cheatsheet.pdf`, and link it from the lesson README and this index.

| Lesson | Cheat sheet | What you practice |
|---|---|---|
| [01 First Delta Lake table](lessons/associate-fundamentals/01-first-delta-lake-table/) | [PDF](lessons/associate-fundamentals/01-first-delta-lake-table/cheatsheet.pdf) | Managed Delta tables, SQL and PySpark reads, `UPDATE`, `DESCRIBE HISTORY`, and time travel |
| [02 Clean and transform with PySpark](lessons/associate-fundamentals/02-clean-transform-pyspark/) | [PDF](lessons/associate-fundamentals/02-clean-transform-pyspark/cheatsheet.pdf) | `select`, `filter`, null handling, `withColumn`, and a `mode("error")` Delta write |
| [03 Join and aggregate with PySpark](lessons/associate-fundamentals/03-join-aggregate-pyspark/) | [PDF](lessons/associate-fundamentals/03-join-aggregate-pyspark/cheatsheet.pdf) | Inner and left joins, join grain, and a `mode("error")` region summary |

Open each lesson README for Free Edition setup. Lesson 01 uses fictional customers and orders in `byte_delta_demo_20261106`. Lesson 02 uses eight fictional orders and writes a new `bt_clean_orders_<suffix>` schema. Lesson 03 uses four customers and six orders and writes a new `bt_joins_<suffix>` schema. Lesson 03's PDF is the supplied finished sheet; its generator source was not in the companion package.

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
