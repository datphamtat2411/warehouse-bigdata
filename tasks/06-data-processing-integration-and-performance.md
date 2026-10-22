Read `AGENTS.md`.

Plan Data Processing Integration & Performance for M04 (J02–J03) from the current repository state.

Start discovery around:
- completed J02/J03 pipelines, contracts, manifests, and quality reports;
- existing Spark Standalone submission, HDFS verification, and runtime configuration;
- focused integration tests and performance measurement infrastructure.

Follow only task-related source dependencies.

Read only:
- `docs/06_DATA_PIPELINE.md` — Sections 9, 11–13, 15–16;
- `docs/11_TESTING.md` — Sections 5–6 and 12;
- `docs/CONTRACTS.md` — relevant dataset, lineage, and lifecycle contracts.

Plan these capabilities:
- verify end-to-end J02→J03 execution against validated M5 sources in HDFS;
- independently reconcile persisted Parquet schemas, counts, keys, coverage, and sales totals;
- verify actual distributed execution across both Spark Workers;
- measure normalization, enrichment, aggregation, and storage performance;
- evaluate resource-aware configuration alternatives using reproducible workloads;
- produce integration evidence and performance reports.

Task-specific constraints:
- reuse approved Tasks 01–05; do not redesign processing algorithms or orchestration;
- preserve Contract v1 schemas, snapshot identities, provenance, and immutable output semantics;
- for canonical full M5, verify 30,490 series, 1,941 days, and 59,181,090 normalized observations;
- verify calendar coverage, zero-sales preservation, price NULL semantics, enrichment cardinality, and aggregate reconciliation;
- independently reread persisted HDFS Parquet outputs; successful job exits alone are insufficient;
- confirm actual executor/task activity on both Workers, not merely cluster registration;
- benchmark comparable workloads with fixed input scope and validated output correctness;
- evaluate chunk sizing, partitioning, shuffle behavior, file fragmentation, and memory/spill metrics where relevant;
- record measured timings, configurations, resource usage, and limitations without assuming speedups;
- keep expensive integration runs explicit, opt-in, and compatible with the 16 GB host;
- avoid full-data driver collection, unrelated service changes, and speculative optimization frameworks.

Out of scope:
- new ETL features, algorithms, or dataset contracts;
- forecasting, inventory optimization, ClickHouse, and Streamlit;
- unrelated infrastructure refactoring;
- production orchestration, monitoring platforms, or performance claims without measurements.

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
