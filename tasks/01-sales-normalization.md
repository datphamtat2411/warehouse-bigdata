Read `AGENTS.md`.

Plan Sales Normalization for M04 Data Processing (J02) from the current repository state.

Start discovery around:
- existing M5 source schemas, profiling, and ingestion boundaries;
- `SALES_DAILY` Contract v1 and dataset validation utilities;
- Spark transformation conventions and focused test infrastructure.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies.

Read only:
- `docs/04_DATASET.md` — Sections 5–6;
- `docs/06_DATA_PIPELINE.md` — Section 6;
- `docs/CONTRACTS.md` — relevant dataset and lineage contracts.

Plan these capabilities:
- implement reusable PySpark DataFrame normalization for configurable, bounded day-column groups;
- validate source identifiers, series uniqueness, selected day columns, sales values, and calendar mappings;
- transform wide sales into canonical `sales_daily` records with correct types and preserved zero-sales observations;
- provide distributed correctness checks and focused Spark transformation tests.

Task-specific constraints:
- use `sales_train_evaluation.csv` semantics: observed `d_1`–`d_1941`; never append overlapping validation history;
- preserve six source metadata fields, mapping `id` to `series_id`;
- reject null, malformed, negative, nonintegral, or overflowing sales values without silent coercion;
- enforce unique nonblank source `(item_id, store_id)` keys and unique, valid calendar day/date mappings;
- join calendar through `day_key = d`; require complete coverage and preserve Walmart retail-week semantics;
- output exactly `SALES_DAILY` Contract v1, including its grain, fields, types, and provenance;
- preserve source-cell values and enforce `series_count × selected_day_count` output cardinality;
- support bounded chunks without hardcoding chunk width or collecting expanded records onto the driver;
- keep transformation independent of HDFS lifecycle and snapshot publication.

Out of scope:
- raw HDFS ingestion, source-manifest orchestration;
- chunk scheduling, checkpointing, retries, and full-scale execution;
- Parquet persistence, compaction, snapshot identities, and promotion;
- price enrichment, analytical aggregation, and forecasting features.

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
