# Join and aggregate data with PySpark

Associate fundamentals lesson for **Databricks Free Edition**. Four fictional customers and six fictional orders are joined, then summarized by region. One new managed Delta table stores the safe summary.

## Sample data only

There is nothing to download. Customers are Ana, Bo, Cy, and Dee. Orders 101–104 match customers. Order 105 uses customer 99, who is not in the lookup. Order 106 has a null customer key. Prices are decimal. There is no real personal data.

## What the notebook writes

The setup cell reads `current_catalog()` and creates a new schema named `bt_joins_` plus 12 hex characters. The only table is `region_revenue` in that schema. A second full run creates another schema. It does not replace the previous one.

The write is:

```python
region_summary.write.format("delta").mode("error").saveAsTable(table_fqn)
```

`mode("error")` fails if that table already exists. Keep that spelling. If the write cell has already succeeded, skip it and run verification. The notebook does not `DROP`, append, overwrite, or use an external `LOCATION`. The duplicate-key example is read-only and is not saved.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. Run the nine code cells from the top in one session.

### Create a notebook (current UI)

1. Open the workspace **home** page.
2. Choose **New > Notebook**.
3. Set the **title**, for example `Join and Aggregate Data with PySpark`.
4. Select **Python**.
5. Select **Serverless**.

### Import this lesson

1. In the left sidebar, open **Workspace** and go to your home folder.
2. Open the folder menu and choose **Import**.
3. Select `join_aggregate_pyspark.ipynb`.
4. Confirm **Python** and **Serverless**.

`join_aggregate_pyspark.py` imports the same way. The `.ipynb` is the authoritative copy.

### Run

- **Run the selected cell:** **Ctrl+Enter**
- **Run all cells:** **Run > Run and debug > Run all**, or **Alt+Shift+Enter**

Shortcuts can differ by platform. If a shortcut does nothing, use the Run menu.

## What a successful run shows

| Check | Result |
|---|---|
| Customers / orders / revenue | 4 / 6 / 68.00 |
| Inner join | 4 matched orders / 56.00 |
| Left join | 6 orders / 68.00, with 2 unmatched (105 and 106) |
| Bad duplicate-key demo | 8 rows / 93.00, read-only, not saved |
| Safe summary | 4 buckets / 6 orders / 4 matched / 68.00 |

Persisted buckets: NORTH 2/2/25.00, SOUTH 1/1/22.50, UNMATCHED 2/0/12.00, WEST 1/1/8.50. The verification cell prints `PASS: stored summary = 4 buckets / 6 orders / 4 matched / revenue 68.00`. Your schema suffix will differ. One observed run used `workspace.bt_joins_cf76173c8c4a.region_revenue`.

## Practice

From `left_orders`, keep matched orders only, group by region, and compute order count and revenue. The notebook's prediction is NORTH 2/25.00, SOUTH 1/22.50, WEST 1/8.50, with matched revenue 56.00. That prediction is not a separately captured result. Do not write another table.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the supplied two-page reference for this lesson. The companion package did not include generator source for this PDF, so it is committed as the finished file and is not produced by `cheatsheets/generate_cheatsheets.py`.

## Files

| File | Role |
|---|---|
| `join_aggregate_pyspark.ipynb` | Authoritative notebook |
| `join_aggregate_pyspark.py` | Same notebook as Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Supplied two-page reference |
| [script.md](script.md) | Narration script for the companion video |
| [VALIDATED_RUN.md](VALIDATED_RUN.md) | Free Edition run note for all 9 code cells |
| `README.md` | This setup guide |

## Validation

A Free Edition run on November 8, 2026 passed all 9 code cells. Counts match the table above. No overwrite and no drop were performed. This repository check compiles the notebook and the `.py` source and confirms the PDF has 2 pages. **No cloud execution was performed for that check.**

Publication of the lesson and video waits for Henry's approval. This change does not upload a video or edit YouTube.
