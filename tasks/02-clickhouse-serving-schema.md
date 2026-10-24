Read `AGENTS.md`.

Plan ClickHouse Serving Schema for the current M08 Distributed Analytical Database module.

Start discovery around:
- existing Contract v1 dataset definitions, serving mappings, field types, grains, and lineage requirements;
- the ClickHouse cluster topology and configuration established by Task 01;
- existing SQL conventions, schema initialization mechanisms, and focused database tests.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies. Avoid broad repository discovery.

Read only:
- `docs/09_DATABASE_DESIGN.md` — physical schema, sharding strategy, and distributed verification;
- `docs/CONTRACTS.md` — canonical dataset identities and serving contracts.

Plan these capabilities:
- create the `warehouse` database and matching local `MergeTree` tables on both shards;
- expose eight `Distributed` serving tables through `clickhouse-1`: `sales_daily`, `forecast_daily`, `forecast_metrics`, `inventory_scenario`, `central_supply_scenario`, `replenishment`, `allocation`, and `policy_evaluation`;
- provide reproducible, rerunnable schema initialization with explicit per-node DDL execution;
- verify physical schema consistency, cluster routing, and distributed SQL behavior.

Task-specific constraints:
- preserve existing Contract v1 dataset definitions and serving mappings as the authoritative logical interface; resolve outdated illustrative DDL against those contracts without silently changing them;
- preserve exact serving names, columns, compatible types, nullability, lineage fields, and logical grains;
- map logical `date` to `sale_date`, `replenishment_recommendations` to `replenishment`, `allocation_recommendations` to `allocation`, and `inventory_policy_evaluation` to `policy_evaluation`;
- use `cityHash64(item_id, store_id)` for product–store facts, `cityHash64(item_id)` for central supply, and `cityHash64(evaluation_run_id, holdout_id, method_id)` for forecast metrics;
- retain forecast quantities as finite, nonnegative `Float64` values without rounding; preserve nullable evaluation metrics and distinguish observed, predicted, and simulated data;
- use the existing `warehouse_cluster` with two independent shards, one replica each; do not require Keeper, `ReplicatedMergeTree`, or `ON CLUSTER`;
- treat `MergeTree ORDER BY` as physical ordering, not business-key uniqueness enforcement;
- make schema initialization safe to repeat and fail explicitly on incompatible existing schemas; never implicitly drop populated tables;
- keep schema verification independent of real M5 publication. Any synthetic test records must be isolated and removed.

Out of scope:
- HDFS/Parquet ingestion, batch publication, reconciliation, approval, and retry workflows;
- business-rule validation, forecast computation, and inventory optimization;
- Streamlit queries, dashboard implementation, and performance optimization beyond basic schema suitability;
- materialized views, unnecessary aggregate tables, replication, and high availability.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability or concern, not by file. Keep File Delta to Modify / Add / Remove inventory only. Specify focused schema and distributed-query verification against the Task 01 cluster. Do not modify repository files during PLAN.
