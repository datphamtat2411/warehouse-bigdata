# 04-distributed-query-verification-and-handoff.md

Read `AGENTS.md`.

Plan Distributed Query Verification and Application Handoff for the current M08 Distributed Analytical Database module.

Start discovery around:
- the two-shard ClickHouse topology, local/Distributed tables, and existing database verification utilities;
- Task 03 publication manifests, approved context selection, and reconciliation evidence;
- established application read contracts, analytical query requirements, and focused integration tests.

Use these as entry points, not an exhaustive file list. Follow only task-related source dependencies. Avoid broad repository discovery.

Read only:
- `docs/09_DATABASE_DESIGN.md` — distributed verification and dashboard query contracts;
- `docs/CONTRACTS.md` — publication identity and application read contracts;
- `docs/11_TESTING.md` — ClickHouse integration and performance verification.

Plan these capabilities:
- independently verify the deployed two-shard cluster using real, approved M5 Small data;
- prove physical data distribution, Distributed query execution, and reconciliation against the approved publication;
- provide reproducible, bounded analytical queries supporting the seven established application read operations;
- produce concise, reproducible verification evidence and a clear M09 application handoff.

Task-specific constraints:
- verify exactly two active shards with one replica each, separate physical storage, and nonempty project-derived records on both nodes;
- reconcile physical shard-local counts and representative aggregates with Distributed results and the corresponding approved source scope;
- verify actual cross-shard reads, not merely cluster metadata or two running containers;
- enforce `skip_unavailable_shards = 0`; an unavailable shard must cause an explicit failure rather than successful partial results;
- bind every application query to one approved `PublicationContext`; never independently select latest model, forecast, scenario, decision, or evaluation runs;
- use read-only, parameterized SQL with bounded filters and result sizes;
- preserve observed versus predicted versus simulated semantics, including product-level central supply and compatible evaluation cohorts;
- return exactly seven dated forecasts for eligible completed series; missing or incomplete coverage must remain explicit;
- expose historical forecast/policy comparisons only when compatible validated evaluation artifacts exist; never fabricate missing metrics;
- record actual query timings, dataset coverage, shard counts, environment, and limitations without claiming unmeasured performance;
- reuse Task 02/03 infrastructure and publication mechanisms; do not redesign them or mutate approved data.

Out of scope:
- ClickHouse deployment, schema migrations, publication loading, and approval implementation;
- upstream Spark, ML, or inventory algorithm changes;
- Streamlit pages, visualizations, and interactive application implementation;
- large-scale benchmarking, caching frameworks, HA, and replication.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` within roughly 150 lines. Organize Proposed Changes by capability or concern, not by file. Keep File Delta to Modify / Add / Remove inventory only. Specify focused live verification and application-read contract checks using approved M5 Small data. Do not modify repository files during PLAN.
