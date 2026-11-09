# Validated run — Query and Analyze Data with PySpark, Part 3

**Status: PASS.** Imported the handoff notebook as a new Python notebook, named it **Build and Teach - Query and Analyze Data with PySpark - Part 3**, selected **Serverless**, captured the early controls screenshot before execution, then ran all six code cells once in order. The notebook created one uniquely named schema and one managed Delta table. The table write used the specified `mode("error")`; no overwrite, drop, or append was run.

- Total notebook cells: 12; code cells: 2, 4, 6, 8, 9, 11.
- Created schema: `workspace.bt_query_cf911148fc3d`
- Created managed Delta table: `workspace.bt_query_cf911148fc3d.region_category_revenue`
- Expected-result mismatches: none.

## Actual cell results

| Code # / cell | Observed result |
|---|---|
| 1 / 2 | **PASS.** Displayed the 8 source orders; 6 valid rows, 5 region/category buckets, and revenue 142.50. `INPUT CHECK` passed. |
| 2 / 4 | **PASS.** SQL high-value result returned orders 108 and 104, each 30.00; total 60.00. SQL assertion/check passed. |
| 3 / 6 | **PASS.** SQL aggregation returned the same five buckets as the DataFrame result: EAST/Books 1 / 12.00; NORTH/Books 1 / 20.00; NORTH/Toys 2 / 60.00; SOUTH/Toys 1 / 25.00; WEST/Books 1 / 25.50. Revenue total 142.50; match assertion passed. |
| 4 / 8 | **PASS.** Created schema `workspace.bt_query_cf911148fc3d`; printed destination `workspace.bt_query_cf911148fc3d.region_category_revenue`. |
| 5 / 9 | **PASS.** Created the managed Delta table using `.format("delta").mode("error").saveAsTable(table_fqn)`. Printed the created FQN. This destination cell was run once only. |
| 6 / 11 | **PASS.** Read the persisted table and matched all five expected rows. Aggregate verification: 5 buckets, 6 orders, total revenue 142.50. Final output: `PASS: stored summary = 5 buckets / 6 orders / revenue 142.50`. A copyable SELECT was printed. |

Persisted rows observed: EAST/Books 1 / 12.00; NORTH/Books 1 / 20.00; NORTH/Toys 2 / 60.00; SOUTH/Toys 1 / 25.00; WEST/Books 1 / 25.50.

## Errors and notes

No notebook cell errors; all assertions and the final PASS check succeeded. The browser console showed 404s for optional workspace/editor configuration and browse metadata resources (including table-browse metadata); these did not affect the notebook run or persisted verification. No output mismatches against the handoff expectations.

## Actual-UI screenshots

Saved under `validation_actual_screenshots/` with account/avatar areas cropped:

- `A_early_controls.png` — Part 3 title, Python, Serverless, Run all and code-cell run control, before execution.
- `B0_input_validation.png` — Part 3 input data and the initial valid-row / bucket / revenue check.
- `B_sql_high_value.png` — SQL high-value rows and total.
- `C_sql_aggregation.png` — five SQL aggregate buckets and matching revenue check.
- `D_destination_fqn.png` — actual schema and table FQN.
- `E_managed_delta_created.png` — successful managed Delta write.
- `F_persisted_rows_pass.png` — persisted rows, aggregate verification, final PASS and copyable SELECT.
