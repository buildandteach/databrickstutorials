# A01 Platform tour: architecture, Delta Lake, Unity Catalog and compute

Lesson 1 of the **Databricks Data Engineer Associate series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 1, Databricks Intelligence Platform (6%)**:

- Understand the core components of the platform: architecture, Delta Lake and Unity Catalog.
- Understand the compute services, their characteristics, limitations and cost models, and choose the right one for each workload.

Video: [youtu.be/PglvHIK-QmU](https://youtu.be/PglvHIK-QmU)

Recap: [Lesson 01, first Delta Lake table](../../associate-fundamentals/01-first-delta-lake-table/) covers time travel in more depth.

## Sample data only

Three fictional study-plan rows (`A01`, `A02`, `A07`) with the exam-guide section weights. No download and no personal data.

## What the notebook writes

The setup cell reads `current_catalog()` and creates a new schema named `bt_platform_` plus 12 hex characters. The only table is the managed Delta table `study_plan`. `CREATE SCHEMA` and `CREATE TABLE` have no `IF NOT EXISTS`, so they fail loudly instead of reusing an object. The notebook runs one `INSERT` and one `UPDATE` on that new table only. It does not overwrite or `DROP` anything. A full rerun creates another schema. The practice-challenge `DELETE` cell is commented out.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. Run the 6 code cells from the top (the last one is the commented-out challenge solution).

1. Home **New > Notebook**, select **Python** and **Serverless**, or import `platform_tour_a01.ipynb`.
2. After cell 5, open the **Structured Query Language (SQL) Editor**, choose the workspace's one SQL warehouse (2X-Small in Free Edition), and run the query from the notebook's SQL warehouse section with the table name printed by cell 5.

`platform_tour_a01.py` is the Databricks source form. The `.ipynb` is authoritative.

## What a successful run shows

| Check | Expected |
|---|---|
| Cell 3 | 3 rows: A01/S1/Platform/6, A02/S2/Ingestion/21, A07/S3/Transformation/22 |
| Cell 4 | `format` = `delta`; `Type` = `MANAGED` |
| Cell 5 | History includes CREATE TABLE, WRITE (the insert) and UPDATE (background maintenance may add more versions) |
| SQL warehouse | Same 3 rows (A07 topic now "Transformation and modeling") |

## Compute concepts (paid workspaces)

Free Edition is serverless only. Classic all-purpose compute, jobs compute and instance pools are explained in the video and cheat sheet as concepts. Cost model: Databricks Units (DBUs) billed per second; classic compute also has cloud virtual machine (VM) cost; jobs compute has a lower DBU rate than all-purpose; pools charge no DBUs while instances are idle (the cloud provider still bills them).

## Practice

Predict the result of `DELETE ... WHERE weight_pct < 10` followed by `DESCRIBE HISTORY`. Answer: 2 rows remain (A02, A07), the newest operation is DELETE, and A01 is still readable with `VERSION AS OF` the version before the delete until `VACUUM` removes old files.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `platform_tour_a01.ipynb` | Authoritative notebook |
| `platform_tour_a01.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. **No Free Edition cloud run has been recorded for this notebook yet.** Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
