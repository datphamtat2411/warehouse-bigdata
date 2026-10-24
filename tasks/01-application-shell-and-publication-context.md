Read `AGENTS.md`.

Plan **M09 Application Shell & Approved Publication Context** for the Vietnamese Streamlit application.

Start discovery around:
- existing approved-publication loading, HDFS availability fencing, `PublicationContext`, and application read-repository lifecycle;
- current Python dependencies, configuration conventions, and focused application tests;
- Streamlit entry-point and session-state integration boundaries.

Read only:
- `docs/10_APPLICATION_DESIGN.md` (sections 3–4, 11, 13–14);
- `docs/M09_APPLICATION_HANDOFF.md`;
- `docs/CONTRACTS.md` (Application Read Contract).

Follow only task-related source dependencies. Keep discovery within application bootstrapping, approval context, and shared UI state.

Plan these capabilities:
- provide one runnable Streamlit shell with exactly four Vietnamese navigation destinations: **Tổng quan**, **Dự báo nhu cầu**, **Tồn kho**, **Phân bổ hàng**; establish a clean handoff for later page implementations;
- expose one validated, coherent publication context throughout each page render, with a sidebar summary of approved identity, issue date, scope, and provenance;
- establish shared store/item/policy selection state and clear dependent selections/query caches when approval changes; leave catalog eligibility checks to later tasks;
- show understandable Vietnamese unavailable/incomplete/loading states, without fabricated analytics;
- add the necessary UI dependencies and focused, infrastructure-independent shell/state tests.

Task-specific constraints:
- reuse the existing publication validator; HDFS `published/current.json` is authoritative and `artifacts/approved_publication.json` is only its verified copy;
- revalidate approval before each application request/rerun; respect the HDFS unavailable fence; fail closed on absent, revoked, inconsistent, or unreachable approval;
- never independently choose latest run IDs, switch publication during a render, or serve stale cached results as current;
- preserve publication/run identity across navigation; invalidate dependent state when identity changes;
- user-visible text is Vietnamese; code and logs are English; remain read-only and use approved `demo_sample` scope truthfully.

Out of scope:
- ClickHouse query/catalog extensions, page-specific KPIs/charts/tables, and page business logic;
- what-if controls, downloads, authentication, background jobs, ETL/ML execution, and publication writes.

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
