Read `AGENTS.md`.

Plan Distributed Spark–HDFS Verification for M02 — Spark Infrastructure.

Start discovery around:
- T01 cluster startup, Master/Worker registration, and runtime diagnostics;
- T02 short-lived PySpark submission interface and driver/executor configuration;
- existing HDFS independent-client checks, fixture ownership, and integration-test conventions.

Use these as entry points. Follow only task-related source dependencies; avoid broad discovery.

Read only:
- `docs/11_TESTING.md` (§6, Spark distributed acceptance);
- `docs/05_TECH_STACK.md` (§6, cluster execution evidence);
- `infrastructure/hadoop/README.md` (M02 connectivity handoff and test safety).

Plan these capabilities:
- provide a bounded, deterministic Spark DataFrame workload submitted through T02 to T01's Standalone cluster;
- read a test-owned HDFS input, perform verifiable transformations, write Parquet to HDFS, and independently validate reread results;
- establish that both registered Workers actually execute tasks for the same application;
- produce reproducible, inspectable execution evidence and focused offline/opt-in integration checks.

Task-specific constraints:
- use `spark://spark-master:7077`, never `local[*]` or a local-filesystem substitute; require T01 and T02 to be operational rather than recreating their implementations;
- verify driver/executor HDFS connectivity via `hdfs://hdfs-namenode:8020`, including real DataNode block access; preserve existing Hadoop client XML and `/data/warehouse` contracts;
- collect actual application identity, worker/executor task activity, relevant stages, configuration, and correctness results; two ALIVE Workers alone are insufficient;
- make assertions deterministic without relying on incidental scheduler placement; use a bounded workload that can demonstrate both Workers, and report missing evidence as failure, not success;
- isolate fixture paths, verify ownership before cleanup, and never format, reset, disrupt, or delete M01 volumes or unrelated HDFS data;
- mark live cluster tests opt-in and excluded from default pytest/CI; enforce finite timeouts and report unmet prerequisites or incomplete evidence honestly.

Out of scope:
- Spark deployment or submission redesign (T01/T02);
- M5 ingestion, business ETL, MLlib training, large benchmarks, and disruptive HDFS recovery tests.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by concern; keep File Delta to Modify / Add / Remove inventory only. Do not modify repository files.
