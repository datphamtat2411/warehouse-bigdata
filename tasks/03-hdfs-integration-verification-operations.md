Read `AGENTS.md`.

Plan HDFS Integration Verification & Operations — HDFS Infrastructure.

Start discovery around:

- the deployed Hadoop Compose topology, lifecycle commands, and HDFS configuration from T01;
- the shared HDFS URI/base-path contract established by T02;
- existing infrastructure smoke checks, focused test conventions, and operational documentation.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies; avoid broad repository discovery.

Read only:

- `docs/11_TESTING.md` (HDFS and distributed-storage verification);
- `docs/02_SYSTEM_REQUIREMENTS.md` (FR-02 and NFR-01);
- `docs/03_SYSTEM_ARCHITECTURE.md` (HDFS topology and networking).

Plan these capabilities:

- provide repeatable HDFS integration checks using small, isolated test fixtures;
- verify two live DataNodes, actual block placement, and replication factor 2;
- verify HDFS write/read integrity, persistence across controlled restart/recreate, and DataNode interruption/recovery;
- verify that an independent container on the shared network can access NameNode metadata and DataNode blocks;
- document focused verification, diagnostics, and M02 connectivity handoff.

Task-specific constraints:

- run PLAN just-in-time after T01 and T02 are integrated; inspect their current implementation rather than assuming file paths;
- use `hdfs dfsadmin -report` and `hdfs fsck` evidence; healthy containers alone do not prove block replication;
- keep test data separate from M5 and approved artifacts; clean up only paths created by the checks;
- preserve all persistent volumes and existing cluster state; never format NameNode, delete volumes, or reset data during verification;
- stop and restore a DataNode only in a controlled test after confirming the fixture has two replicas;
- keep Docker-dependent checks explicit and outside default lightweight CI; report unavailable or unexecuted runtime checks honestly.

Out of scope:

- redesigning T01 topology or T02 configuration contracts; report blocking defects instead;
- Spark deployment/jobs, M5 ingestion, manifests, production data processing, HA, and broad benchmarks.

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
