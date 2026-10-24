# 01-clickhouse-cluster-infrastructure.md

Read `AGENTS.md`.

Plan ClickHouse Cluster Infrastructure for the current M08 Distributed Analytical Database module.

Start discovery around:
- the existing Docker Compose topology, service profiles, shared network, resource limits, and persistent volumes;
- current infrastructure configuration, lifecycle scripts, health checks, and focused infrastructure tests;
- `.env.example`, local environment conventions, and the existing `ClickHouseConfig` loader.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies. Avoid broad repository discovery.

Read only:
- `docs/09_DATABASE_DESIGN.md` — topology, resource strategy, and operational limits;
- `docs/05_TECH_STACK.md` — ClickHouse version and compatibility baseline.

Plan these capabilities:
- deploy two functional ClickHouse servers through the existing Docker Compose project, with independent persistent storage;
- configure `warehouse_cluster` with exactly two shards and one replica per shard, using static cluster discovery on both nodes;
- establish `clickhouse-1` as the query entry point, with working inter-node connectivity;
- provide reproducible environment setup, service lifecycle commands, health checks, and focused topology verification.

Task-specific constraints:
- the host starts without an installed ClickHouse server or configured ClickHouse environment values; Docker Compose must provision the servers without requiring host-level installation;
- use the same explicitly pinned, compatible ClickHouse image version on both shards; never use `latest`;
- preserve existing HDFS/Spark services, profiles, networking, configuration, and volumes without destructive initialization or recreation;
- support the existing `WAREHOUSE_CLICKHOUSE_*` configuration contract; document host-versus-container endpoints and local `.env` initialization without overwriting unrelated settings;
- keep credentials private and Git-ignored; restrict any published database port to localhost and establish separate reader/publisher access boundaries;
- provide bounded CPU/memory settings suitable for the 16 GB WSL2 host and independent ClickHouse startup without requiring Spark;
- verify both nodes and their cluster metadata using actual SQL connectivity, not container status alone;
- use no Keeper, replication, distributed DDL coordination, or high-availability components.

Out of scope:
- application-specific `MergeTree` and `Distributed` table schemas or migrations;
- M5 ingestion, batch publication, reconciliation, and approval;
- real-data shard distribution acceptance, dashboard queries, and Streamlit implementation;
- changes to existing HDFS/Spark workflows or historical artifacts.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability or concern, not by file. Keep File Delta to Modify / Add / Remove inventory only. Include a bounded live cluster-topology verification and its required prerequisites. Do not modify repository files during PLAN.
