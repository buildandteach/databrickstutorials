# Actual Free Edition validation — Nov 7, 2026 CT
Parent performed actual UI run in the existing lesson notebook. All 8 code cells passed after changing writer mode to "error". Original alias "errorifexists" failed during client serialization before a table was created.

Observed schema: workspace.bt_clean_orders_debb4dacfe9b
Observed table: workspace.bt_clean_orders_debb4dacfe9b.clean_orders
8 raw orders / 5 accepted / 3 rejected. Persisted IDs 1001,1003,1006,1007,1008; totals 20.00,22.50,12.00,8.50,3.00. SQL summary row_count=5 / total_sales=66.00; PASS printed after assertions.
No overwrite/drop performed. Rerun refusal is explained as intended documented behavior; no duplicate-write error screenshot is claimed.
Raw schema root/order_id were offscreen in screenshot, but actual schema was parent-confirmed. Video uses local matching explicit-schema code for this portion, not a fabricated screenshot.
Source screenshots copied into assets/live; rendered crops exclude browser URLs, workspace notebook IDs, sidebars, other notebook tabs and avatar. Demo schema/table identifiers are intentionally shown; these identify only this lesson’s fictional-data demo.
