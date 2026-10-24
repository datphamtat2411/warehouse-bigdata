Read `AGENTS.md`.

Plan **M09 Application Read Contracts & Eligible Catalog** for the current Streamlit module.

Start discovery around:
- `ClickHouseReadRepository`, `ReadResult`, `ReadLimits`, existing application read tests;
- approved publication bindings and serving mappings for `forecast_daily`, `sales_daily`, and `store_daily_sales`;
- `PublicationContext` coverage, store/policy eligibility, and current distributed-query conventions.

Read only:
- `docs/10_APPLICATION_DESIGN.md` (sections 6 and 11);
- `docs/09_DATABASE_DESIGN.md` (section 8);
- `docs/CONTRACTS.md` (Application Read Contract);
- `docs/M09_APPLICATION_HANDOFF.md`.

Follow task-related source dependencies only. Keep discovery within application data access and its focused tests.

Plan these capabilities:
- provide a bounded `get_eligible_items(store_id, search_text, limit=50)` operation for approved product–store selections, including explicit unsupported/empty states;
- support bounded item–store historical sales for product filtering without altering the existing store-wide trend;
- preserve and expose the existing seven publication-bound read operations to later pages with consistent `ReadResult` semantics;
- add focused, infrastructure-independent tests for eligibility, history grain, boundaries, lineage, and failure states.

Task-specific constraints:
- extend the existing repository; consume caller-supplied validated `PublicationContext` rather than duplicating Task 01 approval loading or session management;
- derive selectable pairs from approved forecast coverage, checking seven-day completeness; restrict stores and policies to approved context, never invent product names or IDs;
- use `sales_daily` for item-filtered history; never misrepresent `store_daily_sales` as product-level data or observed units as revenue;
- preserve parameterized, read-only, bounded queries on ClickHouse `Distributed` tables, identity filtering, historical evaluation bindings, and fail-closed shard behavior;
- distinguish `empty`, `incomplete`, `unavailable`, and `error`; never fabricate zeros, partial totals, fallback runs, or latest IDs.

Out of scope:
- Streamlit shell/state/navigation, page-specific visuals/KPIs, business-policy recomputation;
- database migrations, publication writes, ETL/ML execution, or live infrastructure testing.

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
