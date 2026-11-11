# Query and analyze data with the SQL Editor — part 3 of 3

Associate fundamentals lesson for **Databricks Free Edition**. This part saves the sample as managed tables, builds a region and category summary, and checks the stored totals.

Video: coming soon.

## Sample data only

The orders are the same eight fictional rows as parts 1 and 2. The customers table is the seven loyalty tiers. There is no real personal data.

## What this part creates

The script uses the `workspace` catalog and creates schema `bt_sql_6aab57cf80df` if it does not already exist. It then creates managed tables `orders`, `customers`, and `region_category_revenue` in that schema. There is no `LOCATION`, so Unity Catalog owns the files. A second run of a plain `CREATE TABLE` fails if that table is already there. The script does not drop the schema.

The valid-order baseline on the saved table is 6 orders and revenue 142.50.

## Setup on Databricks Free Edition

1. Open the SQL Editor.
2. Create a new query and paste in `sql_editor_part3.sql`.
3. Attach the **Serverless Starter Warehouse**.
4. Turn off **Run all statements**.
5. Run one statement at a time.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page reference for this part. It is the supplied PDF.
