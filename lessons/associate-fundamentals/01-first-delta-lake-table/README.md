# Build your first Delta Lake table

Associate fundamentals lesson for **Databricks Free Edition**. You create a managed Delta table, query it with SQL and PySpark, update one fictional row, read `DESCRIBE HISTORY`, and time-travel to the version from before that update.

This is the first hands-on lesson in the Associate track. Later lessons can build from here toward Professional topics. This notebook stays on the create, read, update, history, and time-travel path.

## Sample data only

Every row is fictional practice data. The customers are labeled sample rows (Ada Lovelace, Alan Turing, Grace Hopper), and each row's `data_note` is `SAMPLE — fictional`. The orders are the same kind of sample: three made-up amounts tied to those customer ids. There is no real personal data and no customer extract.

Signup dates are lesson values so the `DATE` column is easy to read. They are not an operational dataset.

## Use the demo schema

The notebook creates objects only under the current catalog, in the schema `byte_delta_demo_20261106`:

| Object | Name |
|---|---|
| Schema | `{current_catalog}.byte_delta_demo_20261106` |
| Table | `{current_catalog}.byte_delta_demo_20261106.customers` |
| Practice table | `{current_catalog}.byte_delta_demo_20261106.orders` |

`current_catalog()` is read at runtime, so the names match the Free Edition workspace (often `workspace`). The schema name is unique to this tutorial. Leave `DEMO_SCHEMA`, `CUSTOMERS_TABLE`, and `ORDERS_TABLE` as they are. The setup cell stops if those names are changed. It quotes each identifier, and it refuses `DELETE` or `UPDATE` unless the target is one of those two tables.

What the notebook writes:

- `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` for those names
- `DELETE` of rows in those two tables only, so a re-run starts from the sample rows again
- `INSERT` of the labeled sample rows
- `UPDATE` of the sample Alan Turing row (`customer_id = 2` and a `SAMPLE` note), setting `city` to `Cambridge`

It does not `DROP`, `CREATE OR REPLACE`, or overwrite some other table. It does not use a DBFS `/mnt` path, an external `LOCATION`, or cloud storage you configure. Tables are managed Delta tables on Free Edition's default storage. Cleanup of the demo schema, if you want it later, is something you do yourself in Catalog Explorer. This notebook does not drop anything.

## History versions change, so capture one

Delta adds a version for each commit. Creating the table, deleting rows, inserting rows, and updating a row are separate commits. Run the notebook again and the version numbers move forward. Do not type a version number such as `VERSION AS OF 1`. That number belongs to whatever commit happened to land there, and the next load moves it.

Before the update, the notebook runs `DESCRIBE HISTORY`, takes `max(version)`, and stores that integer in the Python variable `pre_update_version`. The time-travel cell uses that variable in `VERSION AS OF` and in PySpark `versionAsOf`. This lesson does not call `spark.conf.set` to pass state between cells. If the serverless session restarts, run the notebook again from the top so the variable is set in that session.

## Setup on Databricks Free Edition

Use a **Python** notebook attached to **Serverless**. These cells are PySpark. They run in the notebook, top to bottom, in one session.

### Create a notebook (current UI)

1. Open the workspace **home** page.
2. Choose **New > Notebook**.
3. Set the **title**, for example `Build Your First Delta Lake Table`.
4. Select **Python**.
5. Select **Serverless**.

### Import this lesson

Importing is the alternative to retyping the cells into a blank notebook.

1. In the left sidebar, open **Workspace** and go to your home folder.
2. Open the folder menu and choose **Import**.
3. Select `first_delta_lake_table.ipynb` from this folder.
4. Open the imported notebook and confirm the title, **Python**, and **Serverless**.

`first_delta_lake_table.py` imports the same way. It is this notebook in Databricks source format (`# Databricks notebook source`). The `.ipynb` is the authoritative copy.

### Code cells and Text cells

The lesson alternates **Text** cells and **Code** cells. Text cells are the narrative (headings, what to expect, safety notes). Code cells are Python. Add a cell from the cell control and choose **Code** or **Text**. Running the notebook executes the Code cells.

### Run

- **Run the selected cell:** **Ctrl+Enter**
- **Run all cells:** **Run > Run and debug > Run all**, or **Alt+Shift+Enter**

Shortcuts can differ by operating system and browser. If a shortcut does nothing, use the Run menu. The menu shows the shortcut your workspace is using.

Run all from the top. The time-travel cell needs `pre_update_version` from the capture cell earlier in the same session.

## What the output should demonstrate

Version numbers in `DESCRIBE HISTORY` will not match a fixed example. Judge the rows and the before/after cities. `DESCRIBE HISTORY` is produced in your workspace and can include your user and notebook path. This repository does not contain those values.

| Section | What you should see |
|---|---|
| Resolve catalog | Prints your catalog, `byte_delta_demo_20261106`, the customers and orders names, and the fictional-sample line |
| Create table | Prints `Ready: managed Delta table ...customers`. Catalog Explorer shows the schema and managed table |
| Load customers | Prints `Inserted fictional sample rows: 3` |
| SQL read | Three rows in `customer_id` order: Ada / London, Alan / Manchester, Grace / New York. Each `data_note` is `SAMPLE — fictional` |
| PySpark read | The same three rows |
| Filter | Ada Lovelace / London only, shown from PySpark and again from SQL |
| Capture version | Prints `Pre-update Delta version ...: N` where `N` is whatever `max(version)` is on this run |
| Update | Alan's city is `Cambridge`. Ada and Grace are unchanged |
| History and time travel | History includes an `UPDATE`. SQL `VERSION AS OF N` and the PySpark read of version `N` still show Alan in `Manchester` |
| Orders practice | Three joined rows: Ada 1001 amount 42.5, Alan 1002 amount 19.99, Ada 1003 amount 7.25. Grace has no order, so she is not in the join |

A `DOUBLE` amount can display as `42.5`. The sample note on each order is still `SAMPLE — fictional`.

The orders cell is the starter. The stretch, which you write yourself, is to update one amount, capture that table's pre-update version in a Python variable, and compare it with `VERSION AS OF`.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the one-to-two page quick reference for this lesson: safe-write rules, the sample rows, the pre-update version variable, and the time-travel check. It is not a substitute for the notebook.

Regenerate it from the repository root with `python3 cheatsheets/generate_cheatsheets.py`. Every lesson in this track needs a matching PDF, linked here and from the root lesson index.

## Files

| File | Role |
|---|---|
| `first_delta_lake_table.ipynb` | Authoritative notebook. Import this into Free Edition |
| `first_delta_lake_table.py` | Same notebook as Databricks source, for import or diff |
| [cheatsheet.pdf](cheatsheet.pdf) | Printable quick reference |
| `README.md` | This setup guide |

## Validation

Notebook JSON parses as nbformat 4, with 12 Text cells and 10 Code cells, and empty outputs. Each Code cell parses as Python. The `.py` companion parses as Python and starts with the Databricks notebook header.

Those checks are syntax checks. **No cloud execution was performed.** The notebook was not run in a Databricks workspace, on serverless compute, or on any other cluster. Run it in your own Free Edition workspace to see the live tables.
