Read `AGENTS.md`.

Plan Validated Batch Publication and Approval for the current M08 Distributed Analytical Database module.

Start discovery around:
- Contract v1 dataset definitions, serving mappings, manifest validation, and publication lifecycle;
- existing M5 Small artifact selection, HDFS/Parquet readers, and J03/J06/J07 artifact verification;
- ClickHouse connectivity, Task 02 serving schemas, existing batch-job conventions, and focused tests.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies. Avoid broad repository discovery.

Read only:
- `docs/CONTRACTS.md` — dataset mappings, manifests, and application read contract;
- `docs/SMALL_DEMO_MODE.md` — artifact selection, lineage, and scope validation;
- `docs/06_DATA_PIPELINE.md` — J08 publication boundary;
- `docs/09_DATABASE_DESIGN.md` — publication, reconciliation, and failure handling.

Plan these capabilities:
- preflight explicitly selected, immutable `VALIDATED` upstream artifacts and their compatible lineage;
- load approved M5 Small sales, forecasts, metrics, and inventory outputs from HDFS/Parquet into the eight Task 02 ClickHouse serving tables;
- provide a bounded Python batch publisher with deterministic field mapping, validation, and controlled offline reload;
- reconcile source records against physical shard data and Distributed query results;
- create an auditable publication manifest and promote one coherent application-readable release only after successful verification.

Task-specific constraints:
- use only `demo_sample`, its frozen selection digest, and explicitly selected upstream identities; never search for latest artifacts, resample, regenerate upstream outputs, or process full M5;
- preserve Contract v1 logical grains, unique keys, serving mappings, types, nullability, provenance, and run identifiers; reject incompatible inputs before publication;
- use bounded Parquet reads and batch inserts without collecting entire datasets into memory;
- publish through the existing Distributed tables; preserve their sharding expressions and verify physical delivery to both shards;
- implement a controlled offline replacement, not append-based retries; repeat execution must never duplicate approved business keys or mix publication versions;
- validate dataset row counts, uniqueness, representative aggregates, forecast coverage, and inventory supply constraints before approval;
- `policy_evaluation` requires compatible validated historical evaluation artifacts; missing labels or metrics must never be fabricated or silently replaced;
- preserve distinct forecast and policy evaluation identities and one coherent source/model/forecast/scenario/decision context;
- J08 alone assigns `publication_id` and `APPROVED`; update authoritative HDFS `/data/warehouse/manifests/published/current.json` and its exact application-side copy `artifacts/approved_publication.json` only after reconciliation and application-readiness checks;
- failures must leave publication unapproved and serving explicitly unavailable during an incomplete offline reload; do not claim atomic cross-shard replacement or uninterrupted availability;
- preserve existing HDFS/Spark data, volumes, and artifacts.

Out of scope:
- ClickHouse infrastructure or serving-schema redesign;
- upstream ETL, ML training, forecasting, and inventory computation;
- Streamlit implementation, streaming ingestion, HA, and zero-downtime deployment.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability or concern, not by file. Keep File Delta to Modify / Add / Remove inventory only. Include focused publication, retry, failure, and reconciliation verification. Do not modify repository files during PLAN.
