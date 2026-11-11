# Query and analyze data with the SQL Editor — part 1 of 3

Associate fundamentals lesson for **Databricks Free Edition**. This part practices `SELECT`, `WHERE`, `ORDER BY`, `CASE`, and nulls on eight fictional orders. Parts 2 and 3 continue the same sample.

Video: [youtu.be/wDC8HrQTWms](https://youtu.be/wDC8HrQTWms)

## Sample data only

The query builds the orders with a `VALUES` clause. Two rows are incomplete: 106 has no quantity and 107 has no unit price. 105 has no customer. There is no real personal data. This part does not create or replace any table.

## Setup on Databricks Free Edition

1. Open the SQL Editor.
2. Create a new query and paste in `sql_editor_part1.sql`.
3. Attach the **Serverless Starter Warehouse**.
4. Turn off **Run all statements**.
5. Run one statement at a time.

A valid row needs both quantity and unit price. The baseline is 6 valid orders and revenue 142.50. Books in NORTH or WEST are orders 101 (20.00) and 103 (25.50).

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page reference for this part. It is the supplied PDF.
