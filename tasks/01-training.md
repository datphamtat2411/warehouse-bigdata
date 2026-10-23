Read `AGENTS.md`.

Plan **M06-T01 — Global Random Forest Training & Model Persistence** for J05.

Start discovery around:
- J04 feature-snapshot selection, validation, schema and lineage;
- existing forecasting features and J06 as-of scoring feature shape;
- Spark job orchestration, HDFS artifact/manifest lifecycle and focused ML tests.

Follow related source dependencies only; avoid broad discovery.

Read only the relevant sections of:
- `docs/07_MACHINE_LEARNING.md` — temporal protocol, features, training and model artifacts;
- `docs/CONTRACTS.md` — dataset identities, model manifest and HDFS conventions.

Plan these capabilities:
- consume the canonical validated M5 Small J04 `FORECAST_FEATURES` snapshot (`demo_sample`, ten SKUs × ten stores, full 1,941-day history); validate its frozen selection digest and reject incompatible schema, lineage, missing train data or invalid feature values;
- select a deterministic training sample covering stores, departments, time periods and zero/nonzero sales; record selected keys, policy and counts;
- fit one Spark MLlib `RandomForestRegressor` through a persistable preprocessing `PipelineModel`, compatible with historical training and future recursive scoring;
- persist the model, fitted preprocessing, selection evidence and Contract v1 `MODEL_MANIFEST` to immutable HDFS paths; validate integrity and reload for representative inference.

Task-specific constraints:
- fit model, encoders and imputer **only** on labels `d_29`–`d_1885`; never fit on validation `d_1886`–`d_1913` or test `d_1914`–`d_1941`;
- use fitted `store_id`/`dept_id` categorical encoders with unknown-category handling; no item-ID one-hot; include lag, rolling, calendar, price and missing-price predictors in a fixed feature order;
- use training-fitted median price imputation; if every training price is missing, fail explicitly;
- normalize integer training lags and fractional recursive-scoring lags to compatible double predictors without truncation; preserve J04 feature semantics and `event_flag=False`;
- start with `numTrees=20`, `maxDepth=6`, `maxBins=32`, `minInstancesPerNode=5`, `subsamplingRate=0.8`, `seed=42`, `maxMemoryInMB=128`; no tuning grid;
- standard real-data training uses at most **10,000** eligible observations after feature generation, with sampling seed 42; retain stratified quotas and exact selected keys. If the cap cannot cover occupied strata, fail with cap/stratum/eligible-row diagnostics; never silently drop strata;
- preserve existing model identities, provenance, immutability and manifest contracts; never overwrite a completed model artifact.

Out of scope:
- recursive seven-day forecast production, forecasting metrics or seasonal-naïve evaluation;
- J04 redesign, Contract v1 changes, inventory optimization, ClickHouse, UI and new ML infrastructure;
- full-dataset runs, M04 smoke reruns or cluster acceptance during routine BUILD verification.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` around 150 lines. Organize Proposed Changes by capability; File Delta is Modify/Add/Remove inventory only. Specify one small focused verification command and separate opt-in live execution. Do not modify repository files.
