Read `AGENTS.md`.

Plan **M06-T02 — Seven-Day Recursive Forecasting & HDFS Artifacts** for J06.

Start discovery around:
- J03 validated sales snapshots and J04 feature/lineage contracts;
- existing as-of history preparation, scoring features, eligibility and focused tests;
- T01 fitted-model loading, Spark orchestration, HDFS publication and manifest conventions.

Follow only task-related source dependencies; avoid broad discovery.

Read only the relevant sections of:
- `docs/07_MACHINE_LEARNING.md` — recursive inference, information timing and forecast artifacts;
- `docs/CONTRACTS.md` — `forecast_daily`, run identities, manifests and storage paths.

Plan these capabilities:
- load an explicitly selected, validated T01 `PipelineModel` and J03 snapshot; reject incompatible schema or lineage;
- consume the frozen M5 Small ten-SKU × ten-store selection from upstream scope evidence; validate its digest without resampling. Intended population is 100 series and at most 700 daily forecast rows, with explicit eligibility exclusions;
- reuse `prepare_as_of_history()` and `build_scoring_features()` for seven Spark-batched recursive horizons;
- publish validated `FORECAST_DAILY` v1 Parquet, coverage/exclusions and Contract v1 `FORECAST_MANIFEST` on immutable HDFS paths.

Task-specific constraints:
- issue date `T` is the information boundary: exclude future actuals, realized prices and identity changes; use known calendar only, issue-time price and `event_flag=False`;
- predict `T+1..T+7` sequentially; set `predicted_units=max(0.0, raw_prediction)` and feed back **unrounded doubles**; retain finite `raw_prediction`;
- preserve original J03 observations; keep predicted-versus-observed provenance only in issue-scoped temporary state;
- reuse T01 fitted preprocessing and feature order, preserving fractional lags; never refit or redesign J04 features;
- reject invalid/nonfinite scoring; publish only seven complete, unique, consecutive horizons per series; exclude incomplete/unsupported series explicitly;
- preserve `(forecast_run_id, item_id, store_id, forecast_date)` uniqueness; `model_run_id` remains lineage; retain run type and source/feature identities;
- batch inference on Spark; prepare bounded history once; avoid driver collection and release obsolete caches;
- support historical origins and forward demo `d_1941`; never fabricate labels or accuracy.

Out of scope:
- T01 training/tuning, seasonal-naïve baselines, metrics or full rolling-origin evaluation;
- J04 redesign, Contract v1 changes, J07/J08, ClickHouse, UI and new infrastructure;
- M04 smoke reruns and live cluster acceptance during routine BUILD.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` near 150 lines. Organize Proposed Changes by capability; File Delta lists only Modify/Add/Remove. Specify one small focused command covering recursion and future-leakage; separate live acceptance. Do not modify repository files.
