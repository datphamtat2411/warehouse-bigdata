Read `AGENTS.md`.

Plan **M09 Cross-Page Integration & Acceptance** for the Vietnamese Streamlit application.

Start discovery around:
- the application shell, approval lifecycle, navigation, session selections, and caching;
- all four dashboard entry points, presentation guards, and existing repository contracts;
- focused UI tests and opt-in, read-only ClickHouse integration checks.

Read only:
- `docs/10_APPLICATION_DESIGN.md` (sections 3, 11–14, 18);
- `docs/02_SYSTEM_REQUIREMENTS.md` (FR-13–FR-18);
- `docs/11_TESTING.md` (sections 10–11, application responsiveness);
- `docs/M09_APPLICATION_HANDOFF.md` (read contract and verification boundaries).

Follow task-related source dependencies only. Keep discovery within cross-page integration, acceptance, and focused tests.

Plan these capabilities:
- verify all four pages work when opened directly and navigate coherently within one approved publication;
- retain compatible store, item, and policy selections; invalidate dependent selections and cached outputs when filters or publication identity change;
- standardize Vietnamese loading, unsupported, empty, incomplete, and unavailable states without stale or partial displays;
- exercise the complete observed-sales → forecast → simulated replenishment → allocation workflow using deterministic synthetic UI tests;
- define an opt-in, non-disruptive read-only acceptance check against an existing approved M5 Small publication and running two-shard ClickHouse, with independently reconciled representative outputs and recorded results.

Task-specific constraints:
- reuse completed shell, pages, and repositories; fix only defects required for integration, without redesigning individual dashboard features;
- preserve HDFS approval/fencing, publication-pinned identities, historical evaluation bindings, and fail-closed distributed reads;
- distinguish observed, predicted, simulated, calculated, and evaluated values; never manufacture metrics, zeros, or business improvements;
- preserve product-wide allocation totals despite store-specific selections;
- report tests as PASS, FAIL, BLOCKED, or NOT RUN using actual evidence; unavailable infrastructure must not trigger service startup or upstream regeneration.

Out of scope:
- new dashboards/features, what-if, exports, authentication, new read APIs/schema, ML/ETL, infrastructure changes, destructive shard experiments, full-pipeline reruns, and broad M10 benchmarking.

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
