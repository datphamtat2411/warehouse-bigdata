Read `AGENTS.md`.

Plan **Deterministic Inventory Scenario Generation** for M07-Lite. Implement a reusable, pure-Python domain generator, not an HDFS batch job.

**Start discovery around:**
- existing Contract v1 inventory-scenario, central-supply and scenario-manifest definitions and their validation tests;
- established Python domain-module patterns, deterministic identity/digest helpers and focused fixture conventions;
- the boundary where J03 observed sales and frozen M5 Small series eligibility will later enter J07.

Use these as entry points, not an exhaustive source-file list. Follow task-related source dependencies only.

**Read only these project docs, and only relevant sections:**
- `docs/08_BUSINESS_RULES_AND_OPTIMIZATION.md` (Section 6);
- `docs/CONTRACTS.md` (scenario datasets, identities and provenance).

**Plan these capabilities:**
- accept an explicit issue date, immutable source/scope identity, fixed eligible item-store population and observed daily sales for the seven calendar days `T-6..T`;
- produce Contract v1-compatible `inventory_scenario` store rows and `central_supply_scenario` per-SKU rows, with reproducible scenario metadata;
- validate coverage, inputs and deterministic output invariants independently of Spark, HDFS and forecast inference.

**Task-specific constraints:**
- Use one declared `historical_scaled` configuration: opening-stock factor `alpha=0.35`, safety factor `gamma=0.20`, central-supply factor `beta=0.20`. These are assumptions, not empirically calibrated settings; no alternative presets or randomization.
- For each eligible `(item, store)`, let `H` be the sum of seven observed nonnegative integer sales values. Compute opening stock `floor(alpha*H)`, safety stock `ceil(gamma*H)` and **one** central supply per item `floor(beta*sum(H across its eligible stores))`. Use exact, reproducible decimal arithmetic.
- Require seven distinct dates per eligible series, no duplicates, no future observations, no invalid quantities, and bounded nonnegative integer outputs. Missing history is ineligible/error, never silently zero-filled.
- Never resample the frozen `demo_sample` cohort, invent absent stores or use forecast values/held-out future labels to generate stock. Input membership must be explicitly supplied and validated.
- Preserve existing Contract v1 field names and keys. Use `origin_label=SIMULATED_FROM_M5_HISTORY` and null `scenario_seed`; identify central supply as simulated, not measured warehouse stock.
- Derive stable `scenario_id` and `parameter_digest` from canonical versioned inputs, including source/scope, issue date, eligible population, observed lookback and factors. Input ordering must not affect identities or outputs; changed inputs must not silently overwrite a scenario.

**Out of scope:**
- replenishment requests, shortage-first or baseline allocation, policy evaluation;
- J03/J06 artifact reads, Spark jobs, HDFS/Parquet persistence, publication and ClickHouse/Streamlit;
- manual scenarios, scenario grids or scenario tuning against outcome labels.

Return task-workspace `plan.md` using the four sections required by `AGENTS.md`: **Repository Findings**, **Affected Files**, **Proposed Changes**, **Focused Verification**. Keep it near or below 150 lines; specify contract boundaries, focused tests and explicit exclusions. Do not modify repository files.
