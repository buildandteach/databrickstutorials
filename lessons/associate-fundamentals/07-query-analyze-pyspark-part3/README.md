# Query and analyze data with PySpark — part 3 of 3

Associate fundamentals lesson for **Databricks Free Edition**. This part answers the part 1 and 2 questions in Spark SQL, then saves one new managed Delta summary.

## Sample data only

The notebook rebuilds the same eight fictional orders. Six rows are valid. There is no download and no real personal data.

## What the notebook writes

The setup cell reads `current_catalog()` and creates a new schema named `bt_query_` plus 12 hex characters. The only table is `region_category_revenue`.

```python
region_category.write.format("delta").mode("error").saveAsTable(table_fqn)
```

`mode("error")` fails if that table already exists. If the write already succeeded, skip it and run verification. The notebook does not overwrite, append, or `DROP`. A full rerun creates another schema.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. There are 6 code cells. Run them from the top. Do not rerun the write cell after it succeeds.

1. Home **New > Notebook**. Title, for example `Query and Analyze Data with PySpark — Part 3`. Select **Python** and **Serverless**.
2. Or import `query_analyze_pyspark_part3.ipynb` and confirm those settings.

`query_analyze_pyspark_part3.py` is the Databricks source form. The `.ipynb` is authoritative.

- **Run the selected cell:** **Ctrl+Enter**
- **Run all cells:** **Run > Run and debug > Run all**, or **Alt+Shift+Enter**

## What a successful run shows

| Check | Result |
|---|---|
| Input | 8 orders, 6 valid, 5 region/category buckets, revenue 142.50 |
| SQL high value | Orders 108 and 104, 30.00 each, total 60.00 |
| SQL aggregation | Same five buckets as the DataFrame result, revenue 142.50 |
| Persisted summary | 5 buckets, 6 orders, revenue 142.50 |

Persisted rows: EAST/Books 1/12.00, NORTH/Books 1/20.00, NORTH/Toys 2/60.00, SOUTH/Toys 1/25.00, WEST/Books 1/25.50. The verification cell prints `PASS: stored summary = 5 buckets / 6 orders / revenue 142.50`. One observed run used `workspace.bt_query_cf911148fc3d.region_category_revenue`.

## Practice

Using SQL on `valid_orders`, list WEST or EAST orders by `order_id`. The notebook's prediction is 103 (WEST, 25.50) and 105 (EAST, 12.00). That prediction is not a separately captured result. Do not write another table.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the supplied two-page reference. Generator source for this PDF was not in the companion package.

## Files

| File | Role |
|---|---|
| `query_analyze_pyspark_part3.ipynb` | Authoritative notebook |
| `query_analyze_pyspark_part3.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Supplied two-page reference |
| [script.md](script.md) | Narration script |
| [VALIDATED_RUN.md](VALIDATED_RUN.md) | Free Edition run note for all 6 code cells |
| `README.md` | This setup guide |

## Validation

A Free Edition run on November 9, 2026 passed all 6 code cells with `mode("error")`. This repository check compiles the notebook and the `.py` source and confirms the PDF has 2 pages. **No cloud execution was performed for that check.** Private workspace URLs were removed from the run note. This change does not edit YouTube.
