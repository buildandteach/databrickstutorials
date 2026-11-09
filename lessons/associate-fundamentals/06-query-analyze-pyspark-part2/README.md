# Query and analyze data with PySpark — part 2 of 3

Associate fundamentals lesson for **Databricks Free Edition**. This part aggregates with `groupBy` and ranks rows with window functions. It uses the same fictional orders as part 1. Part 3 continues in SQL.

## Sample data only

The notebook rebuilds the eight invented orders. Six rows have both a quantity and a price. There is no download and no real personal data.

## What this part writes

Nothing. This part is read-only. No schema or table is created.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. There are 4 code cells. Run them from the top.

1. Home **New > Notebook**. Title, for example `Query and Analyze Data with PySpark — Part 2`. Select **Python** and **Serverless**.
2. Or import `query_analyze_pyspark_part2.ipynb` and confirm those settings.

`query_analyze_pyspark_part2.py` is the Databricks source form. The `.ipynb` is authoritative.

- **Run the selected cell:** **Ctrl+Enter**
- **Run all cells:** **Run > Run and debug > Run all**, or **Alt+Shift+Enter**

## What a successful run shows

| Check | Result |
|---|---|
| Valid revenue rows | 6 rows, sum 142.50 |
| Region and category | 5 buckets: EAST/Books 12.00, NORTH/Books 20.00, NORTH/Toys 60.00, SOUTH/Toys 25.00, WEST/Books 25.50 |
| By region | EAST 12.00, NORTH 80.00, SOUTH 25.00, WEST 25.50 |
| Window | Toys ranks 108, 104, 102. Final running revenue 142.50 |

Spark may warn that the unpartitioned running-revenue window uses one partition. That warning is expected and the checks still pass.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the supplied two-page reference. Generator source for this PDF was not in the companion package.

## Files

| File | Role |
|---|---|
| `query_analyze_pyspark_part2.ipynb` | Authoritative notebook |
| `query_analyze_pyspark_part2.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Supplied two-page reference |
| [script.md](script.md) | Narration script |
| [VALIDATED_RUN.md](VALIDATED_RUN.md) | Free Edition run note for all 4 code cells |
| `README.md` | This setup guide |

## Validation

A Free Edition run on November 9, 2026 passed all 4 code cells. This repository check compiles the notebook and the `.py` source and confirms the PDF has 2 pages. **No cloud execution was performed for that check.** Private workspace URLs were removed from the run note. This change does not edit YouTube.
