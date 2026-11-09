# Validated run — Query and Analyze Data with PySpark, Part 1

**Status: PASS.** Imported the handoff notebook as a new Python notebook, named it **Build and Teach - Query and Analyze Data with PySpark - Part 1**, selected **Serverless**, captured the early controls screenshot before execution, then ran all four code cells once in order. This part is read-only; no schema/table writes or destructive actions occurred.

- Total notebook cells: 9; code cells: 2, 4, 6, 8.
- Expected-result mismatches: none.

## Actual cell results

| Code # / cell | Observed result |
|---|---|
| 1 / 2 | **PASS.** Displayed all 8 input orders. `INPUT CHECK: 8 orders (2 incomplete quantity/price rows for null practice)`. `order_total` was 20.00, 25.00, 25.50, 30.00, 12.00, NULL, NULL, 30.00 for IDs 101–108 respectively. |
| 2 / 4 | **PASS.** Six rows had both quantity and unit price. Books in NORTH/WEST: order 101 (20.00) and 103 (25.50), total 45.50. `SELECT/FILTER CHECK` passed. |
| 3 / 6 | **PASS.** High-value rows ordered by descending total then order ID: 108 (30.00), 104 (30.00), 103 (25.50), 102 (25.00); total 110.50. Assertion/check passed. |
| 4 / 8 | **PASS.** Printed the input schema and numeric summary. Counts: quantity 7, unit_price 7, order_total 6; means: 2.0, 17.214286, 23.750000 respectively. Null counts: customer 1, quantity 1, unit_price 1. `NULL CHECK` passed. |

## Errors and notes

No notebook cell errors; all assertions passed. The browser console showed 404s for optional workspace/editor configuration and browse metadata resources; these did not affect notebook execution. No output mismatches against the handoff expectations.

## Actual-UI screenshots

Saved under `validation_actual_screenshots/` with account/avatar areas cropped:

- `A_early_controls.png` — Part 1 title, Python, Serverless, Run all and code-cell run control, before execution.
- `B_input_and_check.png` — eight input rows and INPUT CHECK.
- `C_books_north_west.png` — Books NORTH/WEST selection and SELECT/FILTER CHECK.
- `D_high_value_ordered.png` — ordered high-value results and check.
- `E_null_counts_summary.png` — schema, numeric summary, null counts and NULL CHECK.
