Read `AGENTS.md`.

Plan Inventory Artifact Publication for the current M07-Lite module.

Start discovery around:

* existing J06 immutable artifact staging, independent readback, manifest validation, owner-side promotion, and recovery flows;
* Contract v1 scenario, decision, and policy-evaluation datasets/manifests, HDFS paths, and focused publication tests;
* Task 03 forward decision outputs and Task 04 historical comparison outputs and their lineage/coverage evidence.

Use these as entry points, not an exhaustive file list. Follow task-related source dependencies only; avoid broad repository discovery.

Read only:

* `docs/CONTRACTS.md` (manifests, lifecycle, paths, and identities);
* `docs/SMALL_DEMO_MODE.md` (Section 6);
* `docs/06_DATA_PIPELINE.md` (relevant publication and validation sections).

Plan these capabilities:

* persist bounded Task 03 scenario, central-supply, replenishment, and allocation datasets as an immutable J07 decision artifact bundle;
* persist Task 04 historical policy outcomes as a separate immutable evaluation bundle;
* validate persisted content independently, publish Contract v1 manifests and auditable evidence, and support safe retry/recovery without recomputation.

Task-specific constraints:

* Use the approved M5 Small `demo_sample` selection, verified upstream `VALIDATED` artifacts, frozen population/digest, and exact source/forecast/model/scenario identities. Reject conflicting or unavailable inputs; never select “latest”, resample, or rebuild upstream data.
* Preserve Contract v1 dataset names, schemas, logical keys, lineage, provenance, and manifest definitions. Use `/data/warehouse/outputs/decisions/{decision_run_id}/` for decision bundles and `/data/warehouse/outputs/evaluation/{evaluation_run_id}/` for historical evaluation; keep scenario datasets and metadata associated with their decision bundle rather than inventing an independent canonical scenario root.
* Keep forward-demo decisions and historical test evaluation distinctly identified and scoped. Do not manufacture policy outcomes for forward predictions or mix incompatible policy/forecast/scenario runs.
* Validate before promotion: physical Parquet schema, unique keys, required fields, row counts, coverage/exclusions, lineage, per-SKU supply conservation, and fresh persisted readback. Record integrity checksums and reproducibility metadata; `COMPLETED` or `_SUCCESS` alone never means `VALIDATED`.
* Reuse existing HDFS owner-side staging/promotion conventions where appropriate. Never overwrite a sealed artifact or silently accept conflicting retries; failed candidates must not appear published. Avoid generalizing or rewriting unrelated J01–J06 publication behavior.
* J07 produces `VALIDATED` analytical artifacts only. J08 owns ClickHouse materialization, `APPROVED` serving publication, and `published/current.json`.

Out of scope:

* changing scenario, replenishment, allocation, or evaluation algorithms;
* additional scenarios/backtests, full-M5 execution, ClickHouse/Streamlit, and serving approval.

Return task-workspace `plan.md` with exactly:

* Repository Findings
* Affected Files
* Proposed Changes
* Focused Verification

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability, specify focused tests and recovery cases, and do not modify repository files.
