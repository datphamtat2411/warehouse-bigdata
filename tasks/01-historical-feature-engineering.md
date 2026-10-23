# M05-T01 — Historical Feature Engineering

Read `AGENTS.md`.

Plan J04 historical, point-in-time-correct feature engineering for M05. Do not implement.

**Start discovery around:**
- M04 `SALES_ENRICHED` schema, calendar/weekly-price semantics, and daily series continuity;
- Spark historical feature construction, temporal eligibility, and focused tests;
- Frozen `FORECAST_FEATURES` v1.0 schema, lineage, and M06 integration boundaries.

**Read only:**
- `docs/02_SYSTEM_REQUIREMENTS.md` — FR-05 and DR-06;
- `docs/07_MACHINE_LEARNING.md` — §§5–8 and §16.1;
- `docs/06_DATA_PIPELINE.md` — §8;
- `docs/CONTRACTS.md` — feature contract and versioning.

Start source discovery at `src/warehouse_bigdata/processing/sales_enrichment.py`, `src/warehouse_bigdata/contracts/datasets.py`, and focused tests. Follow task-related dependencies only.

**Plan these capabilities:**

- **Transformation interface:** Reusable Spark DataFrame feature builder consuming validated M04 `sales_enriched`, with explicit `source_snapshot_id` and `feature_version`. Preserve item/store/department identities, observed labels, and lineage. No HDFS I/O.
- **Input integrity and eligibility:** Enforce unique item-store-date grain, consistent series metadata, valid dates, and nonnegative sales. Reject corrupt or duplicate source observations; exclude insufficient or discontinuous lookbacks with explicit reason codes and coverage counts. Preserve genuine zero sales.
- **Historical demand features:** Compute `lag_1/7/28` and `rolling_mean_7/28` from exactly the preceding calendar days. Require 28 complete prior days. Verify date continuity before using row-offset windows; never include target-day sales or substitute missing observations with zero.
- **Calendar features:** Derive target-date `day_of_week` (ISO Monday=1), `month`, and `is_weekend`. Derive `event_flag` only under a documented known-at-issue event policy; do not assume all recorded M5 events were preannounced.
- **Price features:** Derive `last_known_price` from the latest usable per-series price available no later than `t-1`. Keep it nullable; `price_missing` indicates no usable as-of price. Never use target/future prices, future backfill, or fabricated zero prices.
- **Historical roles:** Set `as_of_date = target_date - 1 day`, with observed `label`. Assign `feature_role` as `train` (`d_29–d_1885`), `validation` (`d_1886–d_1913`), or `test` (`d_1914–d_1941`), subject to eligibility. Earlier days supply lookback history.
- **Contract compliance:** Produce exactly the frozen `FORECAST_FEATURES` v1.0 fields, logical types, nullability, scoped uniqueness, and invariants. Provide eligibility diagnostics separately without adding public dataset fields.

**Task-specific constraints:**
- Historical holdout rows are one-step supervised examples, not recursive seven-day backtest inputs.
- Use native distributed Spark operations; no full-data collection, row-wise Python UDFs, fitted preprocessing, or sampling before feature construction.
- Preserve T01 feature semantics for T02 reuse without implementing scoring state.
- Identify unresolved source/event availability or contract conflicts explicitly. Do not invent policies or silently change frozen contracts.

**Focused verification must cover:**
- Hand-calculated `d_35` features: lags `34/28/7`, rolling means `31/20.5`;
- target/future-sales and future-price leakage;
- gaps, duplicates, negative sales, valid zeros, and series isolation;
- price missingness, calendar/event behavior, chronological boundaries, and exact schema conformity.

**Out of scope:**
T02 recursive scoring features, T03 HDFS orchestration/publication, model training, inference, feature sampling, and forecast evaluation.

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

Organize **Proposed Changes** by capability/concern, not by file. Use **API Surface** for Spark transformation interfaces, input/output contracts, and eligibility diagnostics—not HTTP endpoints. Record task-specific parameters and configuration decisions under **Configuration**. Keep **File Delta** strictly to Modify / Add / Remove inventory. Specify focused tests and verification commands under **Focused Tests**.

This task-specific eight-section PLAN format takes precedence over the generic four-section PLAN outline in `AGENTS.md`; retain all other repository rules and report the discrepancy.

Do not modify repository files.
