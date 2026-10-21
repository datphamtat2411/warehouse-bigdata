Read `AGENTS.md`.

Plan **HDFS Raw Ingestion & Integrity** for M03 — Data Ingestion.

Start discovery around:
- existing HDFS configuration, client access patterns, and storage conventions;
- HDFS lifecycle, file-integrity verification, and focused integration tests;
- Contract v1 manifests and the Task 01 artifact interface.

Read only:
- `docs/02_SYSTEM_REQUIREMENTS.md`;
- `docs/04_DATASET.md`;
- `docs/06_DATA_PIPELINE.md`;
- `docs/CONTRACTS.md`;
- `docs/11_TESTING.md`.

Follow only task-related source dependencies. Avoid broad repository discovery.

Plan these capabilities:
- Consume Task 01's `source_manifests.json`, `source_profile.json`, and original local CSVs; verify validated status, source identity, and local file integrity before ingestion.
- Transfer unchanged source files into `/data/warehouse/raw/m5/source_version=<source_version>/`, preserving original filenames.
- Stage uploads in an isolated run-owned HDFS location; verify remote file sizes, streamed SHA-256 checksums, and HDFS block integrity before promotion.
- Verify replication factor two and actual block placement across both configured DataNodes.
- Publish verified HDFS source references and file-level manifests compatible with Contract v1.
- Support safe reruns: recognize matching validated source versions, reject conflicting existing data, and prevent incomplete uploads from becoming valid raw snapshots.

Task-specific constraints:
- Preserve Task 01's content-derived `source_version`; do not introduce competing source identities or redefine shared manifests.
- Treat Task 01 manifests as local-source evidence, not proof of successful HDFS ingestion.
- Keep final source-version paths immutable. Never overwrite validated raw data or delete unrelated HDFS paths.
- Do not mark remote artifacts `VALIDATED` before integrity and placement checks pass.
- Reuse existing Hadoop infrastructure and configuration; avoid unnecessary connectors, services, or memory-heavy file buffering.
- Keep HDFS integration tests explicit and non-disruptive; preserve existing cluster state and volumes.

Out of scope:
- Kaggle downloading, source profiling, or modification of original CSVs;
- end-to-end ingestion orchestration, Spark transformations, curated Parquet, and infrastructure redesign.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` under roughly 150 lines. Organize Proposed Changes by capability; File Delta lists only Modify / Add / Remove. Do not modify repository files.
