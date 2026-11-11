# Query and analyze data with the SQL Editor — part 2 of 3

Associate fundamentals lesson for **Databricks Free Edition**. This part practices `GROUP BY`, `HAVING`, joins, CTEs, and window functions. It uses the same eight fictional orders as part 1, plus a small customers list.

Video: coming soon.

## Sample data only

Valid orders are the six rows with both quantity and unit price. Their revenue is 142.50. The customers list is seven loyalty tiers used only for the join. There is no real personal data. This part does not create or replace any table.

## Setup on Databricks Free Edition

1. Open the SQL Editor.
2. Create a new query and paste in `sql_editor_part2.sql`.
3. Attach the **Serverless Starter Warehouse**.
4. Turn off **Run all statements**.
5. Run one statement at a time.

`GROUP BY` region and category returns five groups. An inner join to customers keeps 5 rows and 130.50. A left join keeps all 6 valid orders and 142.50, because order 105 has a null customer and null does not match.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page reference for this part. It is the supplied PDF.
