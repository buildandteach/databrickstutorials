# databrickstutorials

Build and Teach lessons for Databricks Free Edition.

The first track is **Associate fundamentals**. It starts with managed Delta tables and is the base later lessons can build toward Professional skills.

Each lesson folder includes a 1–2 page cheat sheet, linked in the table below. Future lessons need a matching PDF: add the sheet in [`cheatsheets/generate_cheatsheets.py`](cheatsheets/generate_cheatsheets.py), run that script from the repository root, commit `cheatsheet.pdf`, and link it from the lesson README and this index.

| Lesson | Cheat sheet | What you practice |
|---|---|---|
| [01 First Delta Lake table](lessons/associate-fundamentals/01-first-delta-lake-table/) | [PDF](lessons/associate-fundamentals/01-first-delta-lake-table/cheatsheet.pdf) | Managed Delta tables, SQL and PySpark reads, `UPDATE`, `DESCRIBE HISTORY`, and time travel |
| [02 Clean and transform with PySpark](lessons/associate-fundamentals/02-clean-transform-pyspark/) | [PDF](lessons/associate-fundamentals/02-clean-transform-pyspark/cheatsheet.pdf) | `select`, `filter`, null handling, `withColumn`, and a `mode("error")` Delta write |

Open each lesson README for Free Edition setup. Lesson 01 uses fictional customers and orders in `byte_delta_demo_20261106`. Lesson 02 uses eight fictional orders and writes a new `bt_clean_orders_<suffix>` schema.
