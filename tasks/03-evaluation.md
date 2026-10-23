Read `AGENTS.md`.

Plan **M06-T03 — Chronological Backtesting & Forecast Evaluation** for J05/J06.

Start discovery around:
- T01 frozen model selection, manifest and fitted PipelineModel;
- T02 seven-day inference, forecast identities, coverage and historical as-of state;
- J03 observed labels, forecast-metric contracts, HDFS publication and focused evaluation tests.

Follow only task-related source dependencies.

Read only the relevant sections of:
- `docs/07_MACHINE_LEARNING.md` — temporal evaluation, baselines and metrics;
- `docs/CONTRACTS.md` — `forecast_metrics`, evaluation identity and artifacts;
- `docs/11_TESTING.md` — ML evaluation acceptance.

Plan these capabilities:
- run four seven-day validation origins (`d_1885`, `d_1892`, `d_1899`, `d_1906`) and four locked-test origins (`d_1913`, `d_1920`, `d_1927`, `d_1934`) using T02 recursive scoring;
- generate seasonal-naïve predictions from the observed same-weekday sales available at each issue date;
- compare both methods on identical eligible series, origins, horizons and observed-label pairs; record exclusions and coverage differences;
- publish immutable `FORECAST_METRICS` v1, paired prediction/actual evidence, evaluation lineage and a documented pre-test model freeze.

Task-specific constraints:
- use the T01 fitted model without refitting or tuning; validation is diagnostic, and test outcomes never influence configuration, sampling or model selection;
- each issue T predicts `T+1..T+7` without consuming actuals after T; join future labels only for scoring; later origins may use observations already known at that later issue;
- seasonal naïve for horizon h is observed sales at `T+h-7`, never predicted or future actual sales;
- use the same frozen M5 Small ten-SKU × ten-store cohort (`demo_sample`, validated selection digest) and existing eligibility rules; paired metrics use the intersection of valid outcomes and retain explicit dropped-series counts;
- compute MAE, RMSE and WAPE from paired aggregates, pooled (`horizon=0`) and for horizons 1–7; pooled WAPE uses sums, and a zero actual denominator yields null;
- honor `(evaluation_run_id, holdout_id, method_id, metric_name, horizon)` uniqueness and existing model/forecast lineage; label validation and test distinctly;
- label all metrics `demo_sample`; preserve negative or inconclusive comparisons and never describe sample accuracy as full-M5 or catalog-wide generalization evidence.

Out of scope:
- training, tuning, alternate models, T02 inference redesign or J04 changes;
- J07/J08, ClickHouse, UI, infrastructure and repeated live benchmarks;
- M04 smoke reruns or real-cluster acceptance during routine BUILD.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` around 150 lines. Organize Proposed Changes by capability; File Delta is Modify/Add/Remove only. Specify one small synthetic focused test command; reserve real-data evaluation for opt-in T04 acceptance. Do not modify repository files.
