Read `AGENTS.md`.

Plan Inventory Decision Batch Integration for the current M07-Lite module.

Start discovery around:

* canonical M5 Small selection and validated J03 sales / J06 forecast read flows;
* Contract v1 scenario, decision, and lineage validation patterns;
* Task 01 scenario generator, Task 02 replenishment/allocation interfaces, and focused integration tests.

Use these as entry points, not an exhaustive file list. Follow task-related source dependencies only; avoid broad repository discovery.

Read only:

* `docs/SMALL_DEMO_MODE.md` (Sections 5–6);
* `docs/CONTRACTS.md` (datasets, identities, and HDFS conventions);
* `docs/08_BUSINESS_RULES_AND_OPTIMIZATION.md` (Sections 6–8).

Plan these capabilities:

* bind an explicitly selected, validated forward J06 forecast to matching validated J03 observed history;
* generate one deterministic scenario and a complete replenishment/shortage-first decision batch by reusing Tasks 01–02;
* produce Contract v1-compatible logical scenario, central-supply, replenishment, and allocation rows with auditable coverage and publication-ready metadata.

Task-specific constraints:

* Use `WAREHOUSE_SMALL_ARTIFACTS`, frozen `demo_sample` membership/selection digest, and exact upstream manifests. Reject missing, non-`VALIDATED`, or conflicting artifacts; never search for “latest”, resample, or regenerate upstream outputs.
* The forward-demo issue date is `2016-05-22` (`d_1941`). Use observed history only through that date; no future labels, forecast retraining, or fabricated inventory observations.
* Respect the J06 eligible scored cohort and explicit exclusions, not an assumed 100 completed series. Validate seven unique consecutive horizons per included item-store; reject conflicting dates, keys, values, or lineage instead of silently dropping or zero-filling records.
* Keep reads and driver-side decisions bounded to M5 Small: at most 700 forecast rows and their necessary seven-day historical lookbacks. Avoid full-history collection or heavyweight Spark decision computation.
* Reuse the Task 01 generator and Task 02 policies without reimplementing their formulas. Apply only `shortage_first_proportional_v1` in this forward batch; baseline comparison belongs to Task 04.
* Bind `decision_run_id` to one source snapshot, model/forecast run, scenario, issue date, policy ID/version. Enforce Contract v1 keys, required fields, nonnegative quantities, per-SKU supply limits, and deterministic ordering; report missing/ineligible series with reasons.
* Prepare bounded outputs and manifest inputs for Task 05; do not claim published `VALIDATED` artifacts.

Out of scope:

* changes to scenario or policy algorithms; historical outcome simulation and policy evaluation;
* durable HDFS/Parquet publication, ClickHouse loading, Streamlit, or full-M5 execution.

Return task-workspace `plan.md` with exactly:

* Repository Findings
* Affected Files
* Proposed Changes
* Focused Verification

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability, and specify focused tests and exclusions. Do not modify repository files.
