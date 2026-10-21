Read `AGENTS.md`.

Plan Sales Enrichment for M04 Data Processing (J03) from the current repository state.

Start discovery around:
- `SALES_DAILY` and `SALES_ENRICHED` Contract v1;
- existing M5 calendar and weekly-price source schemas;
- dataset validation utilities and Spark testing conventions.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies.

Read only:
- `docs/04_DATASET.md` — Sections 6–7;
- `docs/06_DATA_PIPELINE.md` — Section 7;
- `docs/CONTRACTS.md` — relevant dataset and lineage contracts.

Plan these capabilities:
- implement reusable PySpark DataFrame enrichment of canonical `sales_daily`;
- validate calendar and weekly-price lookup data before joining;
- attach calendar context, state-specific SNAP indicators, and weekly selling prices;
- derive missing-price and zero-sales indicators;
- provide distributed integrity checks, price-coverage metrics, and focused Spark tests.

Task-specific constraints:
- consume `sales_daily` according to Contract v1; preserve its grain, original fields, values, and provenance;
- validate unique calendar `d` and `date`, valid dates, and complete calendar coverage;
- join calendar on `day_key = d`; verify existing dates and `wm_yr_wk` remain consistent;
- join weekly prices on exactly `(store_id, item_id, wm_yr_wk)` using LEFT JOIN;
- reject duplicate price keys and malformed, nonfinite, or nonpositive populated prices;
- retain missing prices as NULL with `price_missing = true`; never impute zero or discard unmatched sales;
- preserve nullable calendar events and derive `snap_applicable` from the corresponding state indicator;
- derive `is_zero_sale` without removing zero-sales observations;
- output exactly `SALES_ENRICHED` Contract v1; enforce unchanged row cardinality and sales totals;
- avoid unconditional broadcast of the multi-million-row price table and driver-side collection of expanded data.

Out of scope:
- sales normalization and raw ingestion;
- analytical aggregations;
- HDFS orchestration, Parquet persistence, snapshot promotion, and manifests;
- forecasting features, price imputation, and revenue analytics.

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
