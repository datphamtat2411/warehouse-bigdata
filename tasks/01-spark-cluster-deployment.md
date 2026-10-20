Read `AGENTS.md`.

Plan Spark Cluster Deployment for M02 — Spark Infrastructure.

Start discovery around:
- existing Compose topology, HDFS lifecycle, networking, resource limits, and service checks;
- Spark runtime/image compatibility and the existing Hadoop client configuration;
- focused infrastructure validation and smoke-test conventions.

Use these as entry points. Follow only task-related source dependencies; avoid broad discovery.

Read only:
- `docs/03_SYSTEM_ARCHITECTURE.md` (Spark topology and execution modes);
- `docs/05_TECH_STACK.md` (runtime compatibility and resource policy);
- `docs/02_SYSTEM_REQUIREMENTS.md` (NFR-02 and NFR-06).

Plan these capabilities:
- deploy one Spark Standalone Master and two Workers in the existing Compose project, behind an opt-in `spark` profile;
- provide repeatable, Spark-scoped startup, shutdown, diagnostics, and registration checks;
- prepare Worker networking, Hadoop client configuration, and compatible runtime dependencies for later distributed PySpark applications.

Task-specific constraints:
- preserve the `warehouse-bigdata` Compose project/network, default HDFS-only startup, existing HDFS services, persistent volumes, and explicit initialization; never format or reset HDFS storage;
- use Spark 3.5.7, Scala 2.12, and Java 11 as the selected baseline; verify Python 3.11 and Hadoop 3.4.3 compatibility before pinning a trusted image digest; derive a minimal image only when necessary;
- expose Master RPC internally as `spark-master:7077`; publish the Master UI to loopback only;
- bound Docker CPU/RAM, Spark-advertised Worker resources, daemon heaps, logs, and scratch storage for the 16 GB host / approximately 10 GB WSL2 budget, preserving headroom for HDFS and a later driver;
- use two genuinely registered, correctly resourced Workers as the readiness criterion; running containers alone are insufficient;
- prefer the best justified project solution over shortcuts, without speculative infrastructure.

Out of scope:
- driver packaging, job submission, or application entrypoints (T02);
- proof of actual executor tasks and Spark–HDFS read/write correctness (T03);
- M5 ETL, MLlib, History Server, YARN, Kubernetes, and unrelated services.

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
