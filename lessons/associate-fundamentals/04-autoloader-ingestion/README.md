# Ingest files incrementally with Auto Loader

Associate fundamentals lesson for **Databricks Free Edition**. Small fictional CSV order files land in a Unity Catalog volume. Auto Loader ingests only new files into one managed bronze Delta table.

## Sample data only

There is nothing to download. The notebook writes three tiny CSV files into a new demo volume. One quantity is the word `two`, so that row is rescued instead of failing the load. There is no real personal data.

## What the notebook writes

The setup cell reads `current_catalog()` and creates a new schema named `bt_ingest_` plus 12 hex characters. Inside it:

- Volumes `raw_files` and `pipeline_state`
- Managed table `bronze_orders`
- Three CSV files under the orders folder, each written with `overwrite=False`

The stream only appends. A later run with no new files does not add rows. The notebook does not `DROP`, `DELETE`, or replace an existing file. Re-running setup creates another demo schema.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. There are 10 code cells. Run them from the top, once each. Do not rerun the schema cell or a file-write cell that already succeeded.

### Create a notebook (current UI)

1. Open the workspace **home** page.
2. Choose **New > Notebook**.
3. Set the **title**, for example `Ingest Files Incrementally with Auto Loader`.
4. Select **Python**.
5. Select **Serverless**.

### Import this lesson

Import `autoloader_ingestion.ipynb`, then confirm **Python** and **Serverless**. `autoloader_ingestion.py` is the same notebook in Databricks source form. The `.ipynb` is authoritative.

### Run

- **Run the selected cell:** **Ctrl+Enter**
- **Run all cells:** **Run > Run and debug > Run all**, or **Alt+Shift+Enter**

Shortcuts can differ by platform.

## What a successful run shows

| Check | Result |
|---|---|
| First batch | 5 orders from 2 files, 0 rescued |
| Second batch | 3 new rows from 1 new file; 8 rows total; no duplicate `order_id` |
| Rescued row | Order 107, quantity null, price 4.00 |
| Rerun with no new files | Still 8 rows; no rows tagged with that rerun |
| SQL summary | 8 rows, 3 files, 1 rescued, 8 distinct orders, valid revenue 63.50 |

History shows `CREATE TABLE` and two `STREAMING UPDATE` versions. One observed run used `workspace.bt_ingest_3b5d5d736c66.bronze_orders`. The no-new-files rerun reported a progress count of 8 while the assertion confirmed 0 newly appended rows.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the supplied two-page reference. The companion package did not include generator source for this PDF, so it is committed as the finished file.

## Files

| File | Role |
|---|---|
| `autoloader_ingestion.ipynb` | Authoritative notebook |
| `autoloader_ingestion.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Supplied two-page reference |
| [script.md](script.md) | Narration script |
| [VALIDATED_RUN.md](VALIDATED_RUN.md) | Free Edition run note for all 10 code cells |
| `README.md` | This setup guide |

## Validation

A Free Edition run on November 9, 2026 passed all 10 code cells. This repository check compiles the notebook and the `.py` source and confirms the PDF has 2 pages. **No cloud execution was performed for that check.** Private workspace URLs from the run note were removed before commit. This change does not upload a video or edit YouTube.
