Read `AGENTS.md`.

Plan Replenishment and Allocation Policies for the current M07-Lite inventory module.

Start discovery around:

* existing forecast, scenario, replenishment, and allocation Contract v1 definitions and tests;
* J06 seven-day forecast validation and Python domain/test conventions;
* the boundary between scenario outputs, policies, and batch artifacts.

Use these as entry points, not an exhaustive file list. Follow only task-related dependencies; avoid broad repository discovery.

Read only:

* `docs/08_BUSINESS_RULES_AND_OPTIMIZATION.md` (Sections 7, 8, and 10.1/10.3);
* `docs/CONTRACTS.md` (decision dataset keys, identifiers, and provenance).

Keep discovery inside replenishment and allocation business logic.

Plan these capabilities:

* calculate seven-day planning demand, store replenishment requests, and separate critical/safety needs;
* allocate each SKU's limited simulated central supply using `shortage_first_proportional_v1`;
* implement deterministic capped `equal_share_v1` allocation for subsequent paired historical evaluation;
* expose bounded pure-Python results for later Contract v1 batch mapping.

Task-specific constraints:

* Require exactly seven finite, nonnegative, unrounded predictions for horizons 1–7, unique and dated `issue_date + horizon`; reject incomplete, duplicate, cross-run or mismatched scenario inputs rather than substituting zeros.
* Compute `F=sum(predicted_units)`, `D=ceil(F)` once, `target=D+S`, `R=max(0,target-I)`, `critical=max(0,D-I)`, `safety=R-critical`, and `forecast_shortage=(I<D)`; retain unrounded `F`.
* Store stock `I`, safety `S`, and per-SKU central supply `K` must be bounded nonnegative integers. Enforce compatible item/store membership, issue dates, scenario/source identifiers, and forecast lineage.
* Allocate critical needs first, then safety needs. Within each phase apply integer largest-remainder proportional allocation; rank exact remainder numerators descending, then `store_id` ascending. Never rely on floats or Python `assert` alone for runtime validation.
* For `equal_share_v1`, distribute capacity evenly across stores with outstanding requests, cap by request, redistribute excess, and break one-unit ties by ascending `store_id`; avoid unit-by-unit iteration over large quantities.
* For each SKU enforce `0 <= allocated <= requested`, `unmet_request=requested-allocated`, and `sum(allocated) <= K`; never pool supply between SKUs. Results must not depend on input ordering.
* Preserve Contract v1 semantics: recommendations are not shipments, actual stock, or lost sales. Do not require baseline outperformance.

Out of scope:

* inventory-scenario generation; Spark/HDFS reads, jobs, Parquet persistence, publication, ClickHouse or Streamlit;
* policy backtesting, historical outcome simulation, forecast-model evaluation, scenario tuning, routing, or general-purpose solvers.

Return task-workspace `plan.md` with exactly the `AGENTS.md` sections: Repository Findings, Affected Files, Proposed Changes, and Focused Verification. Keep it within roughly 150 lines. Organize Proposed Changes by capability, including internal contracts and exclusions; specify focused tests. Do not modify repository files.
