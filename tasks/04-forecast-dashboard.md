Read `AGENTS.md`.

Plan **M09 Forecast Dashboard** for the Vietnamese Streamlit application.

Start discovery around:
- Task 01 application navigation, approved publication context, and shared selections;
- Task 02 eligible product–store catalog and existing forecast/metric read contracts;
- presentation conventions and focused application tests.

Read only:
- `docs/10_APPLICATION_DESIGN.md` (sections 4, 8, 11–14);
- `docs/02_SYSTEM_REQUIREMENTS.md` (FR-07, FR-08, FR-15);
- `docs/M09_APPLICATION_HANDOFF.md` (read results and historical evaluation bindings).

Follow task-related source dependencies only. Keep discovery within Forecast presentation and tests.

Plan these capabilities:
- render **Dự báo nhu cầu** with eligible store/product selection, approved forecast issue date, and run identity;
- display exactly seven dated daily predictions (horizons 1–7), a seven-day total, daily average, coverage indicator, Plotly chart, and matching detail table;
- present historical Random Forest versus seasonal-naïve evaluation using comparable approved holdout metrics (MAE, RMSE, WAPE), with method, metric horizon, and evaluation context clearly identified;
- provide explicit Vietnamese unsupported, incomplete, unavailable, and query-error states;
- add focused tests for selection, seven-day completeness, total/average consistency, metric comparability, and failure handling.

Task-specific constraints:
- reuse Task 01 context/state and Task 02 repository/catalog; do not duplicate approval logic, SQL, or data-access contracts;
- accept only approved eligible item–store pairs; never infer missing dates, fill forecasts with zero, or silently switch runs;
- show forecasts as predicted **units sold**, not observed demand, revenue, or guaranteed outcomes; keep displayed rounding separate from calculations;
- compare model/baseline only on matching historical cohorts and horizons; use approved historical evaluation bindings, never forward-demo labels;
- treat missing/null/non-comparable metrics as unavailable, not zero error or proof of model improvement; label WAPE as a percentage when defined;
- keep user-facing text Vietnamese and engineering code/logs English.

Out of scope:
- Overview, inventory, replenishment, allocation, policy evaluation, model training/scoring, new repository queries, downloads, and what-if controls.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability, not file. File Delta is Modify / Add / Remove inventory only. Do not modify repository files.
