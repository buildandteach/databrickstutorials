# Query and analyze data with PySpark — part 1 of 3

Associate fundamentals lesson for **Databricks Free Edition**. This part selects, filters, derives columns, and inspects nulls. Parts 2 and 3 continue the same fictional orders.

## Sample data only

Eight invented orders are built in the notebook. Two rows are missing a quantity or a price so null handling is visible. There is no download and no real personal data.

## What this part writes

Nothing. This part is read-only. No schema or table is created.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. There are 4 code cells. Run them from the top.

1. Home **New > Notebook**. Title, for example `Query and Analyze Data with PySpark — Part 1`. Select **Python** and **Serverless**.
2. Or import `query_analyze_pyspark_part1.ipynb` and confirm those settings.

`query_analyze_pyspark_part1.py` is the Databricks source form. The `.ipynb` is authoritative.

- **Run the selected cell:** **Ctrl+Enter**
- **Run all cells:** **Run > Run and debug > Run all**, or **Alt+Shift+Enter**

## What a successful run shows

| Check | Result |
|---|---|
| Input | 8 orders. Totals for 101–108: 20.00, 25.00, 25.50, 30.00, 12.00, null, null, 30.00 |
| Both quantity and price present | 6 rows |
| Books in NORTH or WEST | Orders 101 (20.00) and 103 (25.50), total 45.50 |
| High value, ordered | 108, 104, 103, 102; total 110.50 |
| Null counts | Customer 1, quantity 1, unit price 1. Means: quantity 2.0, unit price 17.214286, order total 23.750000 |

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the supplied two-page reference. Generator source for this PDF was not in the companion package.

## Files

| File | Role |
|---|---|
| `query_analyze_pyspark_part1.ipynb` | Authoritative notebook |
| `query_analyze_pyspark_part1.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Supplied two-page reference |
| [script.md](script.md) | Narration script |
| [VALIDATED_RUN.md](VALIDATED_RUN.md) | Free Edition run note for all 4 code cells |
| `README.md` | This setup guide |

## Validation

A Free Edition run on November 9, 2026 passed all 4 code cells. This repository check compiles the notebook and the `.py` source and confirms the PDF has 2 pages. **No cloud execution was performed for that check.** Private workspace URLs were removed from the run note. This change does not edit YouTube.
