Read `AGENTS.md`.

Plan **M09 Historical Overview Dashboard** for the Vietnamese Streamlit application.

Start discovery around:
- the Task 01 application shell, approved publication context, and shared selections;
- Task 02 store/product catalog, store-wide and item-level historical reads, and `ReadResult`;
- existing presentation patterns and focused application tests.

Read only:
- `docs/10_APPLICATION_DESIGN.md` (sections 4, 7, 11–14);
- `docs/02_SYSTEM_REQUIREMENTS.md` (FR-14).

Follow task-related source dependencies only. Keep discovery within Overview presentation and tests.

Plan these capabilities:
- render the Vietnamese **Tổng quan** page with observed M5 sales, approved source identity, and measured loaded scope;
- support approved store selection, bounded historical date filters, and an optional eligible-product filter; use store-level sales for all products and item-level observed sales when a product is selected;
- display coverage KPIs for stores, supported products, and loaded historical days, separately from total units sold under the current filters;
- show a chronological daily sales Plotly chart and a detail table; chart, filtered-sales KPI, and table must reconcile to the same dates, store, product, and publication;
- add focused page/presentation tests for filter behavior, KPI consistency, provenance, and missing/error states.

Task-specific constraints:
- consume Task 01 context/state and Task 02 read contracts; do not duplicate approval validation, catalog discovery, or SQL inside page code;
- restrict date selection to confirmed loaded history and repository query limits; never scan the full M5 dataset in the UI;
- label observed **units sold**, not revenue, orders, demand guarantees, or real stock; distinguish loaded M5 Small coverage from the original M5 population;
- absent historical rows are not zero sales; reject invalid/unsupported filters and incomplete or unavailable reads without partial charts, fabricated values, or fallback runs;
- all user-facing strings are Vietnamese; engineering identifiers and logs remain English.

Out of scope:
- forecasts, inventory/replenishment, allocation, policy evaluation, new read contracts/schema, downloads, and what-if controls.

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
