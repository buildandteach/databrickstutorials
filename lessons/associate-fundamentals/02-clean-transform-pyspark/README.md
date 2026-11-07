# Clean and transform data with PySpark

Associate fundamentals lesson for **Databricks Free Edition**. Eight fictional orders are cleaned in memory, then saved as one new managed Delta table. You practice `select`, `filter`, null handling, `withColumn`, `orderBy`, and a write that stops if the table already exists.

## Sample data only

There is nothing to download. The notebook builds eight invented orders in memory. Prices are `DECIMAL(10, 2)`. Each row can contain a missing value. `internal_note` explains the sample problem and is not part of the saved table. There is no real personal data.

## What the notebook writes

The setup cell reads `current_catalog()` and creates a new schema named `bt_clean_orders_` plus 12 hex characters. The only table is `clean_orders` in that schema. A second full run creates another schema. It does not replace the previous one.

The write is:

```python
clean_orders.write.format("delta").mode("error").saveAsTable(table_fqn)
```

`mode("error")` fails if that table already exists. Keep that spelling. On Free Edition Spark Connect, the alias `errorifexists` was rejected during client serialization, before a table was created. Do not switch the mode to `overwrite`.

The notebook does not `DROP`, does not set a custom Spark config, does not use a Python UDF, and does not write to an external `LOCATION` or a `/mnt` path. If the write cell has already succeeded, skip it and run the verification cell. The demo schema stays until you remove that exact schema yourself.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. Run the cells from the top in one session.

### Create a notebook (current UI)

1. Open the workspace **home** page.
2. Choose **New > Notebook**.
3. Set the **title**, for example `Clean and Transform Data with PySpark`.
4. Select **Python**.
5. Select **Serverless**.

### Import this lesson

1. In the left sidebar, open **Workspace** and go to your home folder.
2. Open the folder menu and choose **Import**.
3. Select `clean_transform_pyspark.ipynb` from this folder.
4. Open the imported notebook and confirm the title, **Python**, and **Serverless**.

`clean_transform_pyspark.py` imports the same way. It is this notebook in Databricks source format. The `.ipynb` is the authoritative copy.

### Code cells and Text cells

Text cells are the narrative. Code cells are Python. There are eight code cells. Add a cell from the cell control and choose **Code** or **Text**.

### Run

- **Run the selected cell:** **Ctrl+Enter**
- **Run all cells:** **Run > Run and debug > Run all**, or **Alt+Shift+Enter**

Shortcuts can differ by operating system and browser. If a shortcut does nothing, use the Run menu.

## What a successful run shows

Transformations return new DataFrames. `raw_orders` stays at 8 rows and keeps `internal_note`.

| Check | Result |
|---|---|
| Raw rows | 8 |
| Accepted | 5 |
| Rejected | 3 |
| Persisted `order_id` | 1001, 1003, 1006, 1007, 1008 |
| Totals | 20.00, 22.50, 12.00, 8.50, 3.00 |
| SQL `row_count` | 5 |
| SQL `total_sales` | 66.00 |

Rejected, and kept out of the table: 1002 (quantity null), 1004 (quantity -1), 1005 (price null). Missing numbers are not filled with zero.

Labels on the saved rows: Ana is trimmed, 1003's customer is `Unknown`, regions are uppercased, and 1008's blank region is `UNKNOWN`.

The in-memory cell prints `In-memory checks passed: 8 raw, 5 clean, 3 rejected, total 66.00`. The verification cell prints `PASS: persisted Delta table has 5 rows, total_sales = 66.00`.

Your schema suffix will differ from any earlier run. Keep the printed name if you want to inspect that demo later.

## Practice

From `clean_orders`, keep NORTH rows with `order_total >= 15`, select `order_id`, `customer`, and `order_total`, and show them ordered by `order_id`. Expected row: 1001, Ana, 20.00. Do not write another table.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the one-to-two page quick reference: the filter, the label helper, `mode("error")`, and the checked totals. It is not the full notebook.

Regenerate it from the repository root with `python3 cheatsheets/generate_cheatsheets.py`. Every lesson in this track needs a matching PDF, linked here and from the root lesson index.

## Files

| File | Role |
|---|---|
| `clean_transform_pyspark.ipynb` | Authoritative notebook. Import this into Free Edition |
| `clean_transform_pyspark.py` | Same notebook as Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Printable quick reference |
| [script.md](script.md) | Narration script for the companion video |
| [VALIDATED_RUN.md](VALIDATED_RUN.md) | Note from the Free Edition run that checked all 8 code cells |
| `README.md` | This setup guide |

## Validation

A Free Edition run on November 7, 2026 passed all 8 code cells after the write mode was set to `"error"`. Observed counts were 8 raw, 5 clean, 3 rejected, and `total_sales` 66.00. One demo created in that run was `workspace.bt_clean_orders_debb4dacfe9b.clean_orders`. No overwrite and no drop were performed. Details are in [VALIDATED_RUN.md](VALIDATED_RUN.md). Screenshot files from that run are not in this repository.

This repository check parses the notebook JSON and compiles every code cell and the `.py` source. **No cloud execution was performed for that check.** The notebook was not run again in a Databricks workspace while these files were added.

The narration script is in the repository so the lesson and the video stay aligned. This change does not upload a video or edit YouTube.
