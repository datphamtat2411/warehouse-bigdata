Read `AGENTS.md`.

Plan Historical Policy Evaluation for the current M07-Lite inventory module.

Start discovery around:

* existing J06 chronological test-origin forecast artifacts, eligibility evidence, and historical-label read flows;
* validated J03 sales, M5 Small artifact selection, and policy-evaluation Contract v1 tests;
* Task 01 scenario generation and Task 02 replenishment, shortage-first, and equal-share policies.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies; avoid broad repository discovery.

Read only:

* `docs/08_BUSINESS_RULES_AND_OPTIMIZATION.md` (Sections 10–12);
* `docs/CONTRACTS.md` (policy-evaluation contract, identities, and lineage);
* `docs/SMALL_DEMO_MODE.md` (Sections 5–6).

Plan these capabilities:

* evaluate one precommitted historical test origin, `d_1913`, against observed `d_1914`–`d_1920` sales;
* reuse Tasks 01–02 to construct identical scenario inputs and requests, then compare `shortage_first_proportional_v1` against `equal_share_v1`;
* produce Contract v1 `inventory_policy_evaluation` logical rows, paired coverage/exclusion evidence, and interpretable aggregate policy metrics.

Task-specific constraints:

* Select the immutable `demo_sample` artifacts through `WAREHOUSE_SMALL_ARTIFACTS`. Verify `VALIDATED` J03/J06 test-evaluation lineage, frozen population/digest, and the explicitly bound origin forecast. Never use “latest”, regenerate upstream, substitute unavailable forecasts, or switch origin after viewing results.
* Fix the same eligible complete seven-horizon item-store population, `issue_date`, forecast, opening/safety stock, central capacity, and requests for both policies. Require matching seven-day observed labels; report incomplete/invalid cases as exclusions, not zero outcomes.
* Generate stock using only observed history through `d_1913`. Keep outcome labels inaccessible to scenario, replenishment, and allocation calculations; freeze deterministic decision inputs/results before scoring.
* Simulate each policy independently from `stock_0 = opening_stock + allocated_units`; for each future day, `fulfilled=min(stock, observed_sales)`, `unmet=max(0, observed_sales-stock)`, then reduce stock. No mid-horizon replenishment or backorders.
* Enforce `fulfilled + unmet = observed-sales proxy`, inventory conservation, per-SKU capacity, and consistent lineage. Keep distinct policy/decision identities, the same scenario identity, and Contract v1 evaluation keys, fields, and `run_type=test`.
* Report paired fulfilled/unmet units, ending stock, excess over safety, stockout days, and pooled fulfillment rates; zero demand makes the rate undefined, not zero or 100%. Preserve negative/tied results; never claim real Walmart savings.

Out of scope:

* Task 03 forward-demo batch integration; changes to scenario/policy algorithms;
* scenario grids, forecast-method comparison, Spark-heavy recomputation, durable HDFS publication, ClickHouse, or Streamlit.

Return task-workspace `plan.md` with exactly:

* Repository Findings
* Affected Files
* Proposed Changes
* Focused Verification

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability; define focused tests and exclusions. Do not modify repository files.
