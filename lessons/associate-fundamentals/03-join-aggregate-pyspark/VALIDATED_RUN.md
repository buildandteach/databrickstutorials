# Observed actual Free Edition run — Sunday Nov 8, 2026 CT
Parent/browser worker executed all NINE code cells successfully, with no errors, before narration/render.
- 4 customers, 6 orders; revenue 68.00; lookup/order key uniqueness PASS.
- Inner: 4 matched orders, 56.00.
- Safe left: 6 orders, 2 unmatched (105/106; null right fields), original revenue 68.00 preserved.
- Deliberately bad read-only demonstration: customer key 1 count 2; join 8 rows / inflated 93.00. This was never persisted and is not used downstream.
- Safe summary: NORTH 2 orders / 2 matched / 25.00; SOUTH 1/1/22.50; UNMATCHED 2/0/12.00; WEST 1/1/8.50.
- New managed table: workspace.bt_joins_cf76173c8c4a.region_revenue. Write mode exact canonical error. No overwrite/drop/append.
- Persisted SQL exact rows and summary assertions PASS: 4 buckets / 6 orders / 4 matched / 68.00.
Actual screenshot provenance: validation_actual_screenshots/01 through 11 supplied by browser worker. Final render crops out top account area, other notebook tabs, sidebar and browser identifiers. No browser address bar shown. Demo schema/table names deliberately identify only fictional-data objects.
Rerun write refusal is documented intended behavior, not a claim that a second write was actually attempted.
Practice matched-only report is labeled a prediction, not fabricated observed output.
