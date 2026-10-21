Read `AGENTS.md`.

Plan Normalization Snapshot Pipeline for M04 Data Processing (J02) from the current repository state.

Start discovery around:
- M03 raw HDFS ingestion, source manifests, and lineage;
- Task 01 normalization interfaces and `SALES_DAILY` Contract v1;
- Spark submission, HDFS utilities, and focused integration tests.

Follow only task-related source dependencies.

Read only:
- `docs/06_DATA_PIPELINE.md` — Sections 6, 9, 11–12;
- `docs/CONTRACTS.md` — Run Identities, Manifests and Lifecycle, HDFS Artifact Conventions.

Plan these capabilities:
- orchestrate full-history J02 normalization from validated HDFS raw sources using the existing Spark Standalone infrastructure;
- execute configurable bounded day chunks with validation, exclusive HDFS staging, and safe retries;
- compact validated chunks into partitioned, compressed `sales_daily` Parquet;
- verify persisted output and publish validated snapshot metadata, quality evidence, and run manifests.

Task-specific constraints:
- reuse Task 01 transformation logic; do not reimplement normalization;
- require validated `source_version` provenance and actual HDFS inputs; prohibit local-data fallback;
- preserve Contract v1 `source_version`, `pipeline_run_id`, and `source_snapshot_id` semantics and artifact paths;
- process `d_1`–`d_1941` without dropping zero-sales observations or incorporating overlapping validation history;
- for canonical M5, reconcile 30,490 series, 1,941 days, and 59,181,090 rows against measured results;
- validate chunk coverage, row counts, source totals, unique keys, calendar mappings, and final Parquet schema;
- use exclusive staging paths; resume only verified compatible chunks, never blindly append;
- publish `VALIDATED` only after persisted-output verification; failed or incomplete runs must not promote data;
- preserve immutable validated snapshots and report unresolved identity or contract conflicts for Human review;
- use bounded Spark operations without collecting expanded records onto the driver;
- retain resource-aware execution and existing distributed runtime boundaries.

Out of scope:
- rewriting normalization algorithms;
- price enrichment and analytical aggregation;
- forecasting, ClickHouse publication, and application features;
- unrelated infrastructure redesign or comprehensive performance benchmarking.

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
