Read `AGENTS.md`.

Plan HDFS Client & Namespace Contract — HDFS Infrastructure.

Start discovery around:
- the shared `HDFSConfig` model, `load_hdfs_config()`, and current configuration validation behavior;
- `.env.example` and the README's documented HDFS settings;
- existing focused configuration tests and consumers of the HDFS settings.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies; avoid broad repository discovery.

Read only:
- `docs/CONTRACTS.md` (HDFS Artifact Conventions);
- `docs/03_SYSTEM_ARCHITECTURE.md` (deployment topology and HDFS interfaces).

Plan these capabilities:
- align the shared HDFS base-path default and its documentation with approved Contract v1;
- expose one consistent configuration contract for future ingestion and Spark-facing Python components;
- cover the canonical path, URI validation, and required/optional setting behavior with focused tests.

Task-specific constraints:
- `WAREHOUSE_HDFS_BASE_PATH` defaults to `/data/warehouse`, matching the frozen logical paths in `docs/CONTRACTS.md`; do not invent an alternative namespace or redefine artifact identities;
- the canonical container-internal NameNode URI is `hdfs://hdfs-namenode:8020`; it is valid only where the Docker service hostname resolves;
- keep `WAREHOUSE_HDFS_URI` explicitly required and supplied by the runtime; do not silently default to an unreachable container hostname for host-side consumers;
- keep `.env.example` safe and explicit about the internal URI; retain existing side-effect-free, typed configuration loading and error semantics;
- update only directly affected documentation and tests; do not add HDFS connections, directory creation, or filesystem side effects to the configuration loader.

Out of scope:
- Hadoop Compose, service networking, volumes, and NameNode initialization (T01);
- runtime HDFS integration, replication, recovery, or external-client connectivity tests (T03);
- M5 ingestion, Spark jobs, manifest publication, and changes to frozen data contracts.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- Infrastructure Interfaces
- Configuration
- Affected Files
- Focused Verification
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by concern. List Affected Files as Modify / Add / Remove only. Do not modify repository files.
