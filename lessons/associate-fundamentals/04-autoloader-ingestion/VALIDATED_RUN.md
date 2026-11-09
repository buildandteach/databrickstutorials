# Validation run — Auto Loader ingestion

**Status: PASS, with one informational progress-count mismatch.** The notebook was imported as a new Python notebook, renamed exactly **Build and Teach - Auto Loader Ingestion**, attached to **Serverless**, and its 10 code cells were each run once in order. Cell 2 (schema/volume creation) was not rerun. No other notebooks were modified and no destructive operations were run.

- Catalog: `workspace`
- Created schema: `workspace.bt_ingest_3b5d5d736c66`
- Created volumes: `workspace.bt_ingest_3b5d5d736c66.raw_files`, `workspace.bt_ingest_3b5d5d736c66.pipeline_state`
- Created managed bronze table: `workspace.bt_ingest_3b5d5d736c66.bronze_orders`
- Source folder: `/Volumes/workspace/bt_ingest_3b5d5d736c66/raw_files/orders/`
- Checkpoint: `/Volumes/workspace/bt_ingest_3b5d5d736c66/pipeline_state/bronze_orders/_checkpoint`

## Actual results, in execution order

| Code # / notebook cell | Observed result |
|---|---|
| 1 / cell 2 | **PASS.** `SHOW VOLUMES` listed `pipeline_state` and `raw_files`. Printed schema FQN `` `workspace`.`bt_ingest_3b5d5d736c66` `` and bronze FQN `` `workspace`.`bt_ingest_3b5d5d736c66`.`bronze_orders` ``. |
| 2 / cell 4 | **PASS.** Printed the source folder and checkpoint paths above. |
| 3 / cell 6 | **PASS.** Wrote 86 bytes and 68 bytes. Listing: `orders_2026_11_09_a.csv`, `orders_2026_11_09_b.csv`. |
| 4 / cell 8 | **PASS.** Schema: `order_id` integer, `store` string, `quantity` integer, `unit_price` decimal(10,2), `_rescued_data` string, `source_file_path` string (nullable=false), `source_file` string (nullable=false), `ingested_at_utc` timestamp (nullable=false), `load_run` string (nullable=false). Printed `Stream defined; nothing ingested yet.` |
| 5 / cell 10 | **PASS.** `run1` reported 5 progress rows. Table contained orders 101–105 from files a and b, all `_rescued_data` null. `RUN 1 CHECK: 5 rows from 2 files, 0 rescued`. |
| 6 / cell 12 | **PASS.** Wrote 87 bytes. Printed c.csv with `106,SOUTH,2,3.50`, `107,EAST,two,4.00`, `108,WEST,1,12.00`. Listing contained files a, b, and c. |
| 7 / cell 14 | **PASS.** `run2` reported 3 progress rows. Grouping: run1/a=3, run1/b=2, run2/c=3. `RUN 2 CHECK: only 1 new file / 3 new rows; total 8 rows; no duplicate order_id`. |
| 8 / cell 16 | **PASS.** One rescued row: order 107, EAST, quantity NULL, unit_price 4.00. Actual `_rescued_data`: `{"quantity":"two","_file_path":"/Volumes/workspace/bt_ingest_3b5d5d736c66/raw_files/orders/orders_2026_11_09_c.csv"}`. Rescue check passed. |
| 9 / cell 18 | **PASS with informational mismatch.** Actual output: `run3_no_new_files: stream finished; rows reported by progress = 8`; `IDEMPOTENCY CHECK: still 8 rows; rerun added 0 rows`. The notebook assertion confirmed no rows with `load_run = run3_no_new_files` and total remained 8, but the progress figure was 8 rather than the handoff's expected 0. |
| 10 / cell 20 | **PASS.** SQL summary: 8 rows, 3 files, 1 rescued, 8 distinct orders, valid revenue 63.50. Per-file rows: run1/a=3, run1/b=2, run2/c=3. History: v0 `CREATE TABLE`, v1 `STREAMING UPDATE`, v2 `STREAMING UPDATE`. Final PASS line: `PASS: bronze = 8 rows / 3 files / 1 rescued / 8 distinct orders / valid revenue 63.50`. Copyable SELECT was printed. |

## Errors and notes

- No notebook cell raised an error; all assertions and the final PASS check succeeded.
- The browser console recorded 12 failed resource requests (404s) for workspace editor/config metadata and unrelated table-browse metadata. They did not prevent import, execution, or validation.
- The only result mismatch was cell 9's progress count of 8 despite its passing idempotency assertion and zero newly appended rows; record that distinction in downstream narration.

## Actual-UI screenshots

Saved in `validation_actual_screenshots/` with account/avatar areas cropped:

- `A_early_controls.png` — title, Python, Serverless, Run all, cell run control; captured before execution.
- `B_cell1_volumes_fqns.png` — volumes and printed FQNs.
- `C_catalog_volumes_and_three_files.png` — Catalog Explorer showing `raw_files/orders/`, three CSVs, and the `pipeline_state` volume in the tree.
- `D1_cell3_batch1_listing.png` — two-file batch 1 listing.
- `D2_cell4_printschema.png` — actual schema output and no-ingestion message.
- `E_run1_complete.png` — first run, rows, and check.
- `F_cell6_batch2_csv.png` — malformed c.csv input and three-file listing.
- `G_run2_grouping_check.png` — run/file grouping and check.
- `H_rescued_row_wide.png` — rescued row with actual JSON and check.
- `I_run3_idempotency.png` — run 3 progress output and idempotency check.
- `J1_sql_summary_and_run_groups.png` — SQL summary and per-run/file results.
- `J2_history_versions_pass.png` — history versions, PASS, and copyable SELECT.

The pre-existing `validation_actual_screenshots/IMPORT_BLOCKED.png` was not altered; it documents an earlier attempt and is superseded by this successful run.
