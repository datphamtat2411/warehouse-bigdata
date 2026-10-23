Read `AGENTS.md`.

Plan **M06-T04 — Distributed Spark/HDFS Acceptance & Operational Handoff** for M06.

Start discovery around:
- Spark Standalone submission and opt-in integration-test conventions;
- J03/J04 snapshot selection and HDFS lifecycle;
- T01 model reload, T02 forecast publication and T03 evaluation.

Follow task-related source dependencies only.

Read only the relevant sections of:
- `docs/06_DATA_PIPELINE.md` — Spark/HDFS execution and artifact lifecycle;
- `docs/07_MACHINE_LEARNING.md` — ML acceptance and reproducibility;
- `docs/CONTRACTS.md` — identities, manifests and downstream datasets;
- `docs/11_TESTING.md` — distributed and ML acceptance criteria.

Plan these capabilities:
- provide one opt-in real-M5 acceptance entry point using existing Spark/HDFS infrastructure;
- select canonical VALIDATED M5 Small J02/J03/J04 inputs (100 series, 1,941 days, 194,100 observations), frozen selection digest and T01 model trained on at most 10,000 observations; execute or verify T02 and T03 without unnecessary upstream recomputation;
- independently read persisted model, forecast, metric and manifest artifacts; reconcile lineage, schemas, keys, seven-day completeness, coverage, model reload and paired metric aggregates;
- produce evidence and reproducible J07/J08 handoff instructions.

Task-specific constraints:
- require an explicitly selected validated, full-period upstream snapshot covering the locked historical windows; a 30-day M04 smoke artifact or synthetic fixture is **not** acceptable real-M5 ML evidence;
- use the frozen T01 model and frozen ten-SKU × ten-store population, with explicit eligibility exclusions, T02 `d_1941` forward issue and T03 approved rolling origins; no refit, tuning or test leakage;
- prove actual Spark Standalone execution on the configured Master and both Workers, not `local[*]`; record application IDs, completed distributed tasks, stage/job logs, run IDs, counts, timings and available resource observations;
- inspect persisted Contract v1 outputs and HDFS integrity independently of successful process exit; preserve immutable artifacts and distinguish excluded, eligible and scored series;
- never report unexecuted acceptance as passed; missing prerequisites, infrastructure or failed validation remain explicit blockers;
- keep BUILD verification offline; real-cluster execution requires explicit opt-in authorization.
- use `docs/SMALL_DEMO_MODE.md` and `WAREHOUSE_SMALL_ARTIFACTS`; never require 59-million-row processing for project acceptance. Full-mode compatibility is optional/reference behavior requiring separate authorization.

Out of scope:
- reimplementing J01–J04, retraining/redesigning T01–T03, modifying Contract v1 or introducing orchestration infrastructure;
- repeated M04 smoke runs, exhaustive benchmarks, resilience testing, ClickHouse, Streamlit and J07/J08 implementation.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` around 150 lines. Organize Proposed Changes by capability; File Delta lists Modify/Add/Remove only. Specify one focused offline verification command and a separate opt-in live acceptance command with evidence gates. Do not modify repository files.
