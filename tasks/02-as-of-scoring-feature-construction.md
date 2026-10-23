# M05-T02 — As-of Scoring Feature Construction

Read `AGENTS.md`.

Plan J04's leakage-safe, reusable as-of scoring feature construction for M05, supporting M06's seven-day recursive forecasting. Do not implement.

**Start discovery around:**
- T01 historical feature definitions, Spark transformation interfaces, and temporal eligibility rules;
- Forecast-origin history preparation, temporary prediction state, and seven-day feature dependencies;
- `FORECAST_FEATURES` v1.0 compatibility, numerical precision, and focused temporal tests.

Use T01's implemented feature builder, `src/warehouse_bigdata/contracts/datasets.py`, relevant forecasting boundaries, and focused tests as entry points. Follow task-related source dependencies only.

**Read only:**
- `docs/02_SYSTEM_REQUIREMENTS.md` — FR-05, FR-08, DR-06;
- `docs/07_MACHINE_LEARNING.md` — §§5–7, 8.2–8.3, 9.2–9.4, 16.1;
- `docs/06_DATA_PIPELINE.md` — §§8.3–8.4;
- `docs/CONTRACTS.md` — feature contract and versioning.

**Plan these capabilities:**

- **As-of history preparation:** Provide a reusable Spark DataFrame interface accepting validated historical data and explicit issue date `T`. Build a bounded, ordered per-series history state using actual sales only through `T`, with at least 28 contiguous calendar days. Preserve item/store/department identity and source/feature lineage. Do not mutate canonical history.
- **Scoring feature construction:** Accept temporary history state, issue date, target date, and known calendar context. Produce one typed feature row per eligible series for `T+h`, supporting horizons 1–7 without executing model inference.
- **Recursive compatibility:** Calculate `lag_1/7/28` and `rolling_mean_7/28` using actual observations through `T` and caller-supplied predictions for preceding future horizons. Require continuous, correctly ordered history. Never substitute actual sales after `T`, including observed validation/test outcomes.
- **Numerical correctness:** Preserve finite, nonnegative fractional predictions without rounding. Use an internal scoring representation supporting floating-point lags; do not force recursive values into the frozen integer-lag `FORECAST_FEATURES` v1.0 schema or silently revise that contract.
- **Point-in-time context:** Keep `as_of_date = T` across the issue window. Derive target-date calendar features using T01 conventions and approved known-ahead event policy. Freeze last-known-price and price-missing state at `T`; never read realized future prices or labels.
- **Eligibility and diagnostics:** Validate series identity, contiguous history, unique dates, finite values, prediction provenance, and sequential horizon readiness. Expose explicit exclusions/reasons without silently dropping series or manufacturing zero sales.
- **Execution efficiency:** Support batched Spark processing of eligible series with bounded recent-history state, avoiding repeated full-history scans for each horizon.

**Task-specific constraints:**
- Reuse T01 feature semantics; maintain first-horizon consistency with historical one-step features.
- Keep scoring representation internal, named, typed, and consumable by M06's fitted preprocessing pipeline.
- Use native distributed Spark operations; avoid row-wise Python UDFs, full-data collection, unnecessary state retention, and speculative infrastructure.
- M06 owns prediction generation, nonnegative prediction post-processing, temporary state updates, and the seven-step orchestration loop.
- Identify unresolved interface or contract conflicts explicitly; do not invent policies.

**Focused verification must cover:**
- First-horizon parity with T01 for identical as-of inputs.
- Fractional prediction reuse: predicted `d_35 = 40.5` becomes `lag_1` at `d_36`, with correctly updated rolling means.
- Future-sales/price poisoning, issue-date cutoff, calendar/event behavior, and absence of future labels.
- Missing, duplicated, invalid, or discontinuous histories; incorrect prediction order; multi-series isolation.
- Stable lineage, scoring schema, eligibility diagnostics, and bounded Spark execution.

**Out of scope:**
Historical feature redesign, T03 HDFS publication, model fitting/loading, fitted preprocessing, `model.transform()`, prediction state updates, recursive loop orchestration, forecast persistence, and evaluation metrics.

Return `plan.md` with **exactly these sections, in order:**

1. **Task Plan**
2. **Repository Findings**
3. **Proposed Changes**
4. **API Surface**
5. **Configuration**
6. **File Delta**
7. **Focused Tests**
8. **Out of Scope**

Keep `plan.md` within roughly 150 lines.

Organize **Proposed Changes** by capability/concern, not by file. Use **API Surface** to define as-of state preparation, scoring feature interfaces, their inputs/outputs, and eligibility diagnostics—not HTTP endpoints. Record task-specific parameters under **Configuration**. Keep **File Delta** strictly to Modify / Add / Remove inventory. Specify focused tests and verification commands under **Focused Tests**.

This task-specific eight-section PLAN format takes precedence over the generic four-section PLAN outline in `AGENTS.md`; retain all other repository rules and report the discrepancy.

Do not modify repository files.
