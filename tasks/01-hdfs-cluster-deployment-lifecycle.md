Read `AGENTS.md`.

Plan HDFS Cluster Deployment & Lifecycle— HDFS Infrastructure.

Start discovery around:
- existing infrastructure/deployment conventions and any tracked Hadoop or Compose configuration;
- HDFS service topology, networking, storage lifecycle, and startup assumptions;
- existing focused configuration or infrastructure verification patterns.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies; avoid broad repository discovery.

Read only:
- `docs/03_SYSTEM_ARCHITECTURE.md`;
- `docs/05_TECH_STACK.md`;
- `docs/02_SYSTEM_REQUIREMENTS.md`.

Plan these capabilities:
- deploy one NameNode and two independently registered DataNodes through Docker Compose;
- configure HDFS block storage with replication factor 2 and distinct persistent volumes for NameNode metadata and each DataNode;
- provide repeatable startup, shutdown, restart, health checks, and safe first-time initialization;
- expose a stable internal HDFS endpoint and DataNode connectivity for future Spark containers;
- provide minimal operational instructions and focused cluster smoke checks.

Task-specific constraints:
- use service identities `hdfs-namenode`, `hdfs-datanode-1`, and `hdfs-datanode-2`; NameNode RPC is `8020`, Web UI is `9870` and bound to localhost;
- use the official Apache Hadoop 3.4.3 image, pinned to a verified immutable digest; keep compatible Java/runtime settings;
- target Docker Compose on Ubuntu/WSL2, a 16 GB Windows host, and resource-conscious JVM/container settings;
- use a shared Docker network resolvable by future Spark clients, including direct DataNode block access;
- initialize a fresh NameNode explicitly and safely; never automatically reformat existing metadata or discard volumes on restart/recreate; fail on inconsistent state;
- retain simple authentication only inside the isolated development environment; do not claim physical-host fault tolerance.

Out of scope:
- Python HDFS client configuration, `.env.example`, and namespace-default alignment (T02);
- comprehensive replication, failure/recovery, and cross-container integration test tooling (T03);
- Spark deployment, M5 ingestion, pipeline data/manifests, HA, YARN, Kerberos, and unrelated services.

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
