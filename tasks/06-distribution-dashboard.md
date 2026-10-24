Read `AGENTS.md`.

Plan **M09 Distribution & Allocation Dashboard** for the Vietnamese Streamlit application.

Start discovery around:
- Task 01 approved context, navigation, and shared product/policy selections;
- Task 02 eligible catalog, `get_central_supply`, `get_allocation`, `get_policy_evaluation`, and `ReadResult`;
- existing J07 allocation/evaluation contracts and focused application tests.

Read only:
- `docs/10_APPLICATION_DESIGN.md` (sections 4, 10–14);
- `docs/02_SYSTEM_REQUIREMENTS.md` (FR-11, FR-12, FR-17);
- `docs/08_BUSINESS_RULES_AND_OPTIMIZATION.md` (sections 8, 10–11);
- `docs/M09_APPLICATION_HANDOFF.md` (read states and evaluation bindings).

Follow task-related source dependencies only. Keep discovery within Distribution presentation and focused tests.

Plan these capabilities:
- render **Phân bổ hàng** for one approved product, scenario, and policy across every eligible store;
- show unique product-level simulated central supply, total requested, total allocated, and total unmet quantities;
- provide a per-store requested-versus-allocated Plotly chart, detail table, and understandable policy explanation;
- present proposed-versus-baseline historical policy metrics only when matching, complete evaluated cohorts are available;
- test selection, cross-store aggregation, allocation feasibility, fair comparisons, and unavailable/incomplete states.

Task-specific constraints:
- reuse Task 01 context/state and Task 02 reads; no page-level SQL, approval duplication, or new repository contracts;
- distribution is product-wide: the selected store may be highlighted but must never filter allocation totals;
- derive central supply from the unique supply record, never sum repeated allocation-row capacity;
- require complete eligible-store coverage, nonnegative integer quantities, `allocated <= requested`, `unmet = requested - allocated`, and total allocated no greater than central supply; reject inconsistent results instead of rendering partial totals;
- distinguish unmet **store requests** from simulated unmet **customer-demand proxy**; handle zero-request denominators explicitly;
- compare historical outcomes only for the same evaluated stores, periods, scenario, forecast, and policy inputs; unavailable comparisons are not zero or evidence of improvement;
- display published J07 decisions without recalculating allocation policies; Vietnamese UI, English code/logs.

Out of scope:
- other dashboards, scenario editing/what-if, policy recomputation, new SQL/schema, publication writes, ETL/ML, and downloads.

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
