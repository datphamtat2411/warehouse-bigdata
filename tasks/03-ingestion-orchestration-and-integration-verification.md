Read `AGENTS.md`.

Plan **Ingestion Orchestration & Integration Verification** for M03 — Data Ingestion.

Start discovery around:
- Task 01 source profiling, manifests, and local artifact interfaces;
- Task 02 HDFS ingestion, integrity verification, and rerun behavior;
- existing CLI, configuration, run-manifest contracts, and focused HDFS/Spark integration tests.

Read only:
- `docs/02_SYSTEM_REQUIREMENTS.md`;
- `docs/04_DATASET.md`;
- `docs/06_DATA_PIPELINE.md`;
- `docs/CONTRACTS.md`;
- `docs/11_TESTING.md`.

Follow only task-related source dependencies. Avoid broad repository discovery.

Plan these capabilities:
- Expose one explicit CLI workflow accepting an extracted local M5 directory and truthful source provenance, coordinating Task 01 profiling and Task 02 HDFS ingestion without duplicating their logic.
- Validate stage handoffs, preserve the content-derived `source_version`, and record a distinct `pipeline_run_id` for each execution attempt.
- Produce run-level metadata using `PIPELINE_RUN_MANIFEST` v1, including source lineage, output locations, quality evidence, execution outcome, and failure details where applicable.
- Enforce fail-fast execution: reject invalid source profiles before HDFS mutation and never report incomplete or failed ingestion as `VALIDATED`.
- Support reproducible reruns against an existing validated source version without overwriting immutable artifacts or treating partial uploads as successful.
- Provide opt-in integration verification using real M5 files: confirm manifest consistency, HDFS byte integrity, replication across both DataNodes, and a minimal Spark Standalone read from the actual HDFS raw path.
- Document the bounded operational workflow and evidence needed to reproduce ingestion acceptance.

Task-specific constraints:
- Reuse Task 01/02 interfaces, existing infrastructure, configuration, and Contract v1; do not introduce competing ingestion implementations or manifest schemas.
- Keep `source_version`, `pipeline_run_id`, and downstream `source_snapshot_id` semantically distinct.
- Never silently substitute local files for HDFS during distributed verification.
- Keep live tests explicit, non-disruptive, and isolated; do not automatically initialize clusters, delete volumes, or alter unrelated datasets.

Out of scope:
- Kaggle downloading or new source-quality rules;
- redesigning Task 01/02, Spark transformations, curated Parquet, downstream pipelines, or infrastructure expansion.

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
