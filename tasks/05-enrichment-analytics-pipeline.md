Read `AGENTS.md`.

Plan Enrichment & Analytics Pipeline for M04 Data Processing (J03) from the current repository state.

Start discovery around:
- Task 02 enrichment and Task 03 aggregation interfaces;
- Task 04 validated `sales_daily` snapshots and processing lifecycle;
- M03 HDFS raw sources, Spark submission, manifests, and integration tests.

Follow only task-related source dependencies.

Read only:
- `docs/06_DATA_PIPELINE.md` — Sections 7, 9, 11–12;
- `docs/04_DATASET.md` — Sections 6–7;
- `docs/CONTRACTS.md` — relevant dataset, lineage, and lifecycle contracts.

Plan these capabilities:
- orchestrate J03 using validated `sales_daily` and corresponding M5 calendar/price sources;
- reuse approved enrichment and aggregation components;
- persist validated `sales_enriched`, `store_daily_sales`, and category-monthly aggregates as HDFS Parquet;
- produce quality reports, run manifests, and reproducible output metadata;
- support safe retries and validated output promotion.

Task-specific constraints:
- consume only validated, version-compatible inputs; preserve `source_version`, `source_snapshot_id`, and `pipeline_run_id` semantics;
- reuse Tasks 02–03 without duplicating transformation or aggregation logic;
- enforce price-key uniqueness, valid price domains, calendar consistency, and unchanged enrichment cardinality;
- preserve missing-price NULL semantics, zero-sales observations, and original recorded units;
- validate aggregate keys, expected coverage, and totals against the same `sales_daily` snapshot;
- preserve `SALES_ENRICHED` and `STORE_DAILY_SALES` Contract v1;
- require Human-approved category-monthly contract and storage conventions; report unresolved boundaries rather than inventing them;
- use immutable HDFS candidate outputs and Contract v1 paths where defined;
- promote only independently verified persisted outputs; failed or incomplete runs must not expose validated artifacts;
- prevent retry duplication and cross-snapshot mixing;
- use distributed, resource-aware Spark operations without collecting full datasets onto the driver.

Out of scope:
- rewriting normalization, enrichment, or aggregation algorithms;
- raw ingestion and J02 snapshot orchestration;
- forecasting features, ClickHouse publication, and Streamlit;
- unrelated infrastructure changes and comprehensive performance benchmarking.

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
