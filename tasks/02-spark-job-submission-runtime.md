Read `AGENTS.md`.

Plan Spark Job Submission Runtime for M02 — Spark Infrastructure.

Start discovery around:
- existing Spark/HDFS configuration loaders, Python packaging, CLI and batch-entrypoint conventions;
- Compose profiles, Spark networking/runtime contracts, and Hadoop client XML configuration;
- focused configuration, command-invocation, and error-handling tests.

Use these as entry points. Follow only task-related source dependencies; avoid broad repository discovery.

Read only:
- `docs/03_SYSTEM_ARCHITECTURE.md` (short-lived drivers and batch execution);
- `docs/05_TECH_STACK.md` (Spark/PySpark compatibility and resource constraints);
- `docs/06_DATA_PIPELINE.md` (§12.1, future job invocation boundary only).

Plan these capabilities:
- provide a reusable, short-lived container-based `spark-submit` runtime for future Python batch jobs;
- submit applications to the configured Spark Standalone Master with explicit application arguments and configuration;
- prepare driver networking, Hadoop client access, dependency packaging, and bounded execution resources;
- provide a documented invocation path with deterministic exit status, failure reporting, and focused offline checks.

Task-specific constraints:
- use Spark Standalone **client** deploy mode for PySpark; target `spark://spark-master:7077`, never silently fall back to `local[*]`;
- align Spark, Java, Python 3.11, Scala, and Hadoop client compatibility with T01's verified runtime; retain project `uv`/lockfile conventions;
- ensure executors can reach the containerized driver and both driver/executors can resolve HDFS NameNode and DataNode endpoints on `warehouse-bigdata`;
- preserve `WAREHOUSE_SPARK_MASTER_URL`, `WAREHOUSE_HDFS_URI=hdfs://hdfs-namenode:8020`, and `/data/warehouse`; mount existing Hadoop XML files read-only without modifying M01;
- make submission ephemeral and repeatable; propagate application arguments and nonzero failures, avoid hardcoded host paths or unbounded resources;
- respect T01 service ownership and existing HDFS initialization, volumes, and default startup; prefer a justified, minimal solution over unnecessary abstractions.

Out of scope:
- Spark Master/Worker deployment or redesign (T01);
- distributed worker-task/HDFS read-write acceptance workloads (T03);
- M5 ingestion, ETL/MLlib implementations, workflow schedulers, and unrelated services.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by concern; keep File Delta to Modify / Add / Remove inventory. Do not modify repository files.
