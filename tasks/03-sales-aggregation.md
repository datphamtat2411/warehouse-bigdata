Read `AGENTS.md`.

Plan Sales Aggregation for M04 Data Processing (J03) from the current repository state.

Start discovery around:
- `SALES_DAILY` and `STORE_DAILY_SALES` Contract v1;
- existing dataset validation and lineage conventions;
- Spark processing patterns, fixtures, and focused test infrastructure.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies.

Read only:
- `docs/04_DATASET.md` — Sections 5 and 10;
- `docs/06_DATA_PIPELINE.md` — Sections 7.4 and 15.1;
- `docs/CONTRACTS.md` — Catalog Rules and Producer/Consumer Matrix.

Plan these capabilities:
- implement reusable PySpark DataFrame aggregations over canonical `sales_daily`;
- produce `store_daily_sales` and `category_monthly_sales`;
- validate aggregate grain, completeness, uniqueness, and reconciled totals;
- provide distributed correctness checks and focused Spark tests.

Task-specific constraints:
- consume `SALES_DAILY` Contract v1 without altering source observations;
- compute store-daily totals by `(store_id, date)` with `total_units` and `observed_series_count`, including zero-sale observations;
- conform exactly to `STORE_DAILY_SALES` Contract v1;
- compute category-monthly totals across stores by `(cat_id, month_start)` using calendar months derived from actual dates;
- scope both aggregates by an explicit `source_snapshot_id`; never combine different snapshots;
- define the missing category-monthly output contract explicitly for Human approval without silently changing frozen Contract v1 semantics;
- use distributed Spark aggregations without driver-side collection of full datasets;
- preserve integer quantities without rounding, clipping, or replacing missing values;
- reject invalid or duplicate source facts rather than masking them through aggregation;
- reconcile each aggregate's summed `total_units` against the same complete `sales_daily` input scope;
- keep aggregation logic independent of persistence and orchestration.

Out of scope:
- sales normalization and price/calendar enrichment;
- HDFS staging, Parquet persistence, manifests, and snapshot promotion;
- forecasting features, revenue estimation, and ClickHouse publication.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- Data Contracts & Interfaces
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability, not file. Keep File Delta to Modify / Add / Remove inventory only. Do not modify repository files.
