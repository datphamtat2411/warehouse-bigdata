Read `AGENTS.md`.

Plan **M09 Inventory & Replenishment Dashboard** for the Vietnamese Streamlit application.

Start discovery around:
- Task 01 approved publication context, navigation, and shared item/store selections;
- Task 02 `get_replenishment`, eligible catalog, and `ReadResult` contracts;
- existing J07 replenishment outputs, presentation conventions, and focused application tests.

Read only:
- `docs/10_APPLICATION_DESIGN.md` (sections 4, 9, 11–14);
- `docs/02_SYSTEM_REQUIREMENTS.md` (FR-09, FR-10, FR-16);
- `docs/08_BUSINESS_RULES_AND_OPTIMIZATION.md` (section 7);
- `docs/M09_APPLICATION_HANDOFF.md` (read states and publication bindings).

Follow task-related source dependencies only. Keep discovery within Inventory presentation and focused tests.

Plan these capabilities:
- render **Tồn kho** for an approved eligible item–store pair, showing scenario identity, forecast issue date, and planning horizon;
- show stored seven-day forecast total, simulated opening stock, safety stock, target stock, replenishment request, and forecast-shortage status with appropriate units;
- present clearly differentiated KPI cards, a Plotly comparison, a detail table, and an explanation grounded in approved output values;
- distinguish no replenishment needed from missing recommendations, incomplete data, and query failures;
- test selection, displayed quantities, scenario provenance, no-request cases, and error/empty-state behavior.

Task-specific constraints:
- reuse Task 01 context/state and Task 02 reads; no page-level SQL, publication discovery, or new data-access contracts;
- display published J07 decisions, never recalculate replenishment formulas, rounding, or flags in the UI;
- keep predicted sales, simulated inventory, safety targets, and calculated requests semantically distinct; safety stock is not an additive chart component of forecast demand;
- `request_units` is a store request before central allocation, not a placed purchase order or guaranteed shipment; shortage flags are forecasts, not observed Walmart stockouts;
- never substitute zeros, stale runs, or partial rows for missing approved results; Vietnamese UI, English code/logs.

Out of scope:
- distribution/allocation, policy evaluation, scenario editing/what-if, ML/ETL, schema changes, downloads, and other dashboard pages.

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
