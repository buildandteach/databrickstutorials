# Validated run — Query and Analyze Data with PySpark, Part 2

**Status: PASS, with a non-fatal Spark window warning.** Imported the handoff notebook as a new Python notebook, named it **Build and Teach - Query and Analyze Data with PySpark - Part 2**, selected **Serverless**, captured the early controls screenshot before execution, then ran all four code cells once in order. This part is read-only; no schema/table writes or destructive actions occurred.

- Total notebook cells: 9; code cells: 2, 4, 6, 8.
- Expected-result mismatches: none.

## Actual cell results

| Code # / cell | Observed result |
|---|---|
| 1 / 2 | **PASS.** Displayed 8 input orders; 6 valid revenue rows summed to 142.50. `INPUT CHECK` passed. |
| 2 / 4 | **PASS.** Five region/category buckets: EAST/Books 1 row / 12.00; NORTH/Books 1 / 20.00; NORTH/Toys 2 / 60.00; SOUTH/Toys 1 / 25.00; WEST/Books 1 / 25.50. Total: 6 orders / 142.50. `AGG CHECK` passed. |
| 3 / 6 | **PASS.** Region rollup: EAST 1 / 12.00; NORTH 3 / 80.00; SOUTH 1 / 25.00; WEST 1 / 25.50. Revenue total 142.50. `REGION CHECK` passed. |
| 4 / 8 | **PASS.** Window output (order ID, category, rank, running revenue): 101 Books/2/20.00; 102 Toys/3/45.00; 103 Books/1/70.50; 104 Toys/2/100.50; 105 Books/3/112.50; 108 Toys/1/142.50. Toys ranks were 108=1, 104=2, 102=3; final running revenue 142.50. Assertion/check passed. |

## Errors and notes

No notebook cell errors; all assertions passed. Spark emitted a non-fatal warning for the global running-revenue window: `WARN WindowExpression: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.` This was expected from the unpartitioned cumulative window and did not affect results. The browser console also showed 404s for optional workspace/editor configuration and browse metadata resources; these did not affect execution.

## Actual-UI screenshots

Saved under `validation_actual_screenshots/` with account/avatar areas cropped:

- `A_early_controls.png` — Part 2 title, Python, Serverless, Run all and code-cell run control, before execution.
- `B_input_valid_check.png` — input rows and 6-valid / 142.50 check.
- `C_region_category_aggregation.png` — five region/category buckets and aggregate check.
- `D_by_region_rollup.png` — four-region totals and check.
- `E_window_rank_and_running_revenue.png` — ranked window output, running totals, Spark warning and check.
