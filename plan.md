# Task Plan

- Implement M05-T01: reusable J04 historical, point-in-time-correct Spark feature construction.
- Affected domains: M04 input integrity, forecasting features, frozen contracts, and focused Spark tests.
- Acceptance: exact FORECAST_FEATURES v1.0 output; correct calendar-day history; explicit eligibility; no sales/price lookahead.
- Follow the task's eight-section format instead of AGENTS.md's generic four-section outline.
- The event/price availability policies below are explicitly human-approved; their approval prerequisites for BUILD are satisfied.
- BUILD order: focused tests first, transformation implementation second, targeted verification last.

# Repository Findings

- `processing/sales_enrichment.py::enrich_sales_daily` preserves observed sales and joins prices by item/store/retail week.
- `validate_sales_enriched` checks schema, required values, duplicate date/day grains, nonnegative sales, and price/zero flags.
- It does not establish contiguous history or consistent series metadata across dates.
- Missing prices remain null; supplied prices must be finite and positive. Event fields are nullable recorded calendar attributes.
- SALES_ENRICHED has no event announcement timestamp or price publication timestamp; weekly membership alone cannot establish advance availability.
- `contracts/datasets.py::FORECAST_FEATURES` freezes 20 fields, row-level source/feature lineage, and derived-feature provenance.
- Its item/store/target key is scoped by source snapshot, feature version, and role. Only price and label are logically nullable.
- `contracts/validation.py` validates bounded Python records; use it only in tiny tests, not distributed production processing.
- No forecasting package exists. Its first real implementation belongs under `src/warehouse_bigdata/forecasting/`.
- Existing Spark tests use module-scoped `local[2]` sessions and two shuffle partitions.
- Tooling: Python 3.11, uv lockfile, pytest, and PySpark 3.5.7 in the `spark-test` dependency group.
- Documentation inspected across domains because temporal rules depend on input semantics, role boundaries, and contract compatibility:
  - `02_SYSTEM_REQUIREMENTS.md`: FR-05 and DR-06.
  - `07_MACHINE_LEARNING.md`: §§5–8 and §16.1.
  - `06_DATA_PIPELINE.md`: §8, “J04 — Feature Engineering and Strict Time Boundaries.”
  - `CONTRACTS.md`: “Catalog Rules,” “Producer and Consumer Matrix,” and “Run Identities.”
- Historical validation/test rows are one-step examples (§7.5.3); M06 must not use them directly as simultaneous seven-day backtest inputs.

# Proposed Changes

## Input integrity and lineage
- Consume one validated M04 snapshot; require explicit, nonblank source_snapshot_id and feature_version.
- Check exact input fields/types and required values; reject duplicates rather than deduplicating.
- Reject malformed day keys, inconsistent day indices/dates, negative/null sales, and invalid prices or source flags.
- Verify a consistent one-to-one day-index/date mapping and calendar-day spacing across mappings.
- Require stable dept/category/state/series metadata within item/store and an unambiguous series_id-to-item/store mapping.
- Trust M04's authoritative calendar mapping; the builder cannot revalidate against raw calendar data it does not receive.
- Keep validation distributed, combining related checks where practical; retrieve only bounded failure samples or scalar aggregates.
- Attach lineage without changing observed labels or item/store/department identities.

## Calendar-day demand features and eligibility
- Partition by item_id/store_id and order by an integer calendar-day ordinal derived from date.
- Use native numeric range windows: [-1,-1], [-7,-7], [-28,-28] for exact-date lags; [-7,-1] and [-28,-1] for means.
- Require exactly 28 unique observations in the preceding 28 calendar days before emitting a historical feature row.
- This avoids row-offset assumptions and dense date expansion; no missing observation is replaced by zero.
- Compute over complete supplied history before filtering historical roles.
- Preserve observed zeros, including all-zero eligible series.
- Classify observed target rows separately from the public feature dataset:
  - LOOKBACK_ONLY: d_1–d_28, retained as context.
  - INSUFFICIENT_HISTORY: candidate has fewer than 28 calendar days since its first available observation.
  - DISCONTINUOUS_HISTORY: sufficient elapsed history but fewer than 28 observations in its required window.
  - ELIGIBLE: complete required history and valid historical role.
- Record prior-day count and missing-day count; define the above precedence for mutually exclusive statuses.
- Missing target observations produce no fabricated labels or features; diagnostics cover supplied observations and their lookbacks.
- Aggregate coverage by role/status, including observed, candidate, eligible, excluded, and price-missing counts.

## Calendar and event availability
- Derive ISO weekday, month, and weekend from target_date; do not reuse M5 wday as ISO weekday.
- HUMAN-APPROVED: under §7.5.4, set event_flag=False when no independently verified advance-availability evidence exists, preserving the frozen FORECAST_FEATURES v1.0 contract, including its documented semantics and lineage.
- False means “no verified known scheduled event,” not “no event occurred.”
- Event information is intentionally disabled for this feature version; document this limitation.
- Do not infer preannouncement from event names/types. A later approved event policy requires an explicit feature-version decision.

## Historical as-of prices
- Use the latest non-null per-series sell_price on a dated observation no later than target_date - 1.
- Implement native last(..., ignorenulls=True) over an unbounded-preceding range ending at -1.
- Retain earlier known prices across null-price periods, including prices older than the 28-day demand window.
- Set price_missing from last_known_price nullability, independently of the target row's source price_missing.
- Never read target/future prices, backward-fill, or manufacture zero prices.
- HUMAN-APPROVED: for historical simulation, assume a sell_price attached to date d becomes available at the close of d and may only be used for target dates strictly after d.
- Weekly source prices have no actual publication timestamps; this policy is an explicitly approved simulation assumption.
- Document this as a historical simulation assumption, not a claim about verified real-world price publication timing; M04 alone cannot prove price timing.

## Historical roles and contract boundary
- Set target_date=date, as_of_date=date-1, and label=observed unit_sales.
- Assign train d_29–d_1885, validation d_1886–d_1913, and test d_1914–d_1941, subject to eligibility.
- Reject observations outside the canonical labeled day range; never synthesize d_1942+ labels.
- Project exactly FORECAST_FEATURES.field_names in declared order; diagnostics remain separate.
- Enforce required-value, finite/nonnegative, role/date, price-flag, lineage, and scoped-uniqueness invariants with Spark checks.
- Preserve catalog label nullability while requiring non-null values for emitted historical rows.
- Check logical nullability through value constraints; Spark's inferred nullable metadata is not evidence that invalid nulls are permitted.
- Keep reusable date/history/price expressions in this module for T02; introduce no scoring-state abstraction.

# API Surface

- `build_historical_features(sales_enriched: DataFrame, *, source_snapshot_id: str, feature_version: str) -> HistoricalFeatureResult`
- `HistoricalFeatureResult`: named outputs `features`, `eligibility`, and `coverage`, all Spark DataFrames.
- `HistoricalFeatureError(ValueError)`: invalid configuration, corrupt input, or violated output invariants.
- Input: exact SALES_ENRICHED v1.0, one snapshot supplied by the caller; no filesystem or manifest access.
- Features: exact FORECAST_FEATURES v1.0; strings, dates, Long lags/label, Double means/price, integer calendar values, Boolean flags.
- Eligibility: source/version, item/store, target date/day index, nullable historical role, status, prior-day count, missing-day count.
- Coverage: distributed status/role aggregates; counts reconcile input context plus candidates, and eligible plus excluded candidates.
- Input validation is eager enough to reject corrupt data; output DataFrames retain normal Spark execution semantics.
- The signature uses the explicitly human-approved fixed availability policies above; no event/price approval decisions remain unresolved.

# Configuration

- Required caller parameters: source_snapshot_id and feature_version; do not generate either implicitly.
- Contract version remains 1.0; feature_version identifies transformation semantics and is not automatically the contract version.
- Fix lag offsets, window lengths, role boundaries, and ISO weekday convention as documented module constants.
- Use the minimum required 28-day lookback; do not introduce the optional 56-day diagnostic window.
- Document approved event/price assumptions in module documentation and bind their semantics to feature_version.
- No new dependencies, environment variables, YAML configuration, sampling controls, Spark tuning framework, or persistent caches.

# File Delta

- Modify: none.
- Add: `src/warehouse_bigdata/forecasting/__init__.py`.
- Add: `src/warehouse_bigdata/forecasting/features.py`.
- Add: `tests/unit/test_historical_features.py`.
- Remove: none.

# Focused Tests

- Write tiny deterministic M04-shaped fixtures and a module-scoped local Spark session before production changes.
- Hand calculation: unit_sales(d_n)=n; d_35 has lags 34/28/7 and rolling means 31/20.5.
- Leakage: change target/future sales while maintaining valid source flags; same-target predictors stay unchanged, target label changes only as expected.
- Price timing: target/future changes do not affect same-target features; latest earlier price wins, including across weekly boundaries.
- Price states: all-null history, first price only on target/future dates, older carried price, and series isolation.
- Integrity: duplicate date/day grains, negative/null sales, invalid dates/mappings, inconsistent metadata, and invalid prices fail.
- Eligibility: insufficient startup, internal gaps despite 28 older rows, recovery after 28 complete days, and exact coverage reconciliation.
- Valid zeros remain eligible and are not confused with gaps; shuffled input and multiple items/stores preserve isolation.
- Calendar/events: all ISO weekdays, weekends, month transitions, and recorded events under the approved availability policy.
- Boundaries: d_28/29, d_1885/1886, d_1913/1914, and d_1941; truncated histories and zero-eligible output.
- Contract: exact ordered fields/types, permitted nulls, historical labels, lineage, scoped uniqueness, and no diagnostic-field leakage.
- Cross-check tiny output records against the existing catalog validator; production validation stays distributed.
- Run: `uv run --frozen --group dev --group spark-test pytest -q tests/unit/test_historical_features.py`
- After success, stop testing and run `git status --short`. On failure, make one corrective pass and rerun the same command once.

# Out of Scope

- T02 recursive scoring state/features; T03 HDFS jobs, orchestration, manifests, persistence, and publication.
- M06 training, fitted encoders/imputation, sampling, inference, seven-day evaluation, and performance benchmarks.
- Frozen contract changes, fabricated observations, retrospective event assumptions, and claims of verified price publication timing.
```
