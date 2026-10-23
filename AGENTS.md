# Agent Rules — Warehouse Big Data

Keep changes within the active task. Prefer the best justified solution, not merely the quickest one, while avoiding over-engineering, speculative abstractions, unnecessary dependencies, and unrequested features.

## Current Project Dataset Policy — M5 Small

**M5 Small (`demo_sample`) is the standard project execution dataset:** ten deterministic
SKUs × all ten original stores = 100 series, with all 1,941 days (`d_1`–`d_1941`),
producing exactly 194,100 real observed-sales records. It is a subset of original M5
(3,049 SKUs / 30,490 series / 59,181,090 derived observations), not synthetic sales.

- All real-M5 integration, distributed acceptance, evaluation, performance measurement,
  publication and dashboard demonstrations use this frozen population and scope digest.
- Unit/contract tests use tiny deterministic synthetic fixtures; infrastructure-only
  connectivity tests remain independent of M5 data.
- The 30-day full-catalog M04 smoke workflow and 128-day benchmark matrix are retired.
  Historical HDFS snapshots and evidence must be preserved, not reused as ML inputs.
- Full M5 is not a routine development, evaluation, acceptance or demonstration workload.
  Heavy full-M5 execution requires explicit user authorization, explicit `full_m5`
  selection and `--confirm-large-workload`. Omitted arguments must select safe small scope.
- Reuse existing VALIDATED small artifacts through `WAREHOUSE_SMALL_ARTIFACTS` and
  verify upstream publication/selection identity. Never automatically regenerate full-M5
  snapshots for testing or regenerate upstream datasets when artifacts are missing.
- Agent-generated commands must select the safe small-data profile; downstream scope
  must agree with immutable upstream evidence. Never independently resample the cohort.
- Demo metrics describe `demo_sample` only and must not be presented as full-dataset
  accuracy, catalog-wide generalization, or full-dataset acceptance evidence.
- Practical commands, lineage, artifact selection and recovery are defined in
  `docs/SMALL_DEMO_MODE.md`. Consult its relevant section for execution tasks.

## Repository Boundaries

This is a single Python project, not a monorepo. Follow the agreed target directory structure; create folders only when their first real implementation needs them. Do not introduce new top-level directories, services, or alternative implementations without a task-specific reason.

- `src/warehouse_bigdata/`: ingestion, processing, forecasting, optimization, publication, shared utilities, and batch-job entry points.
- `infrastructure/`: Hadoop, Spark, and ClickHouse deployment configuration.
- `app/`: Vietnamese Streamlit application and read-only data access.
- `sql/`: ClickHouse migrations and queries.
- `tests/`: focused automated checks; `scripts/`: developer commands.
- `data/` and `artifacts/`: local/generated material, not source control.

Keep business logic in its owning module. `jobs/` orchestrates; it does not reimplement algorithms. The UI reads approved analytical outputs; it must not run large ETL jobs or retrain models.

## Documentation Routing — Strict Context Budget

The numbered files in `docs/` are **long design specifications**, not required background reading. **Never read all eleven, scan the entire `docs/` directory, or open complete documents by default.** Route by the active task, not by curiosity.

| Task concern | Primary document | Supporting document, only if needed |
|---|---|---|
| Project scope / business goal | `01_PROJECT_OVERVIEW.md` | `02_SYSTEM_REQUIREMENTS.md` |
| Acceptance criteria / requirement changes | `02_SYSTEM_REQUIREMENTS.md` | Owning domain document |
| Deployment topology / services | `03_SYSTEM_ARCHITECTURE.md` | `05_TECH_STACK.md` |
| Source M5 schema / data meaning | `04_DATASET.md` | `06_DATA_PIPELINE.md` |
| Spark ETL / batch jobs / HDFS outputs | `06_DATA_PIPELINE.md` | `04_DATASET.md` |
| Forecast features / training / scoring | `07_MACHINE_LEARNING.md` | `04_DATASET.md` or `06_DATA_PIPELINE.md` |
| Inventory simulation / replenishment / allocation | `08_BUSINESS_RULES_AND_OPTIMIZATION.md` | `07_MACHINE_LEARNING.md` |
| ClickHouse schema / sharding / publication | `09_DATABASE_DESIGN.md` | `06_DATA_PIPELINE.md` |
| Streamlit UI / data access | `10_APPLICATION_DESIGN.md` | `09_DATABASE_DESIGN.md` |
| Tests / performance evaluation | `11_TESTING.md` | Document owning the tested behavior |
| Foundation / repository tooling | Relevant code and configuration first | `05_TECH_STACK.md` only for a disputed stack decision |

For any selected document:

1. Search for relevant headings or terms **within that file only**; read the smallest section that answers the task. Do not read it cover to cover unless the task genuinely requires it.
2. Start with **one primary document**. Open a supporting document only to resolve a specific contract, dependency, or conflict.
3. For a cross-domain task, list the affected domains first, then inspect the necessary sections of each owning document. Explain broader reading in `plan.md`.
4. Record only actionable constraints, exact section references, and unresolved questions in `plan.md`. Do not copy long passages into the plan.
5. Do not invent undocumented requirements, reconcile contradictions silently, or treat a planned design as a working implementation. If authoritative documents and existing code disagree, record the mismatch before choosing a change.

Document authority is topic-specific: `02` owns requirements, `04` source semantics, `06` pipelines, `07` forecasting, `08` inventory policy, `09` database, `10` UI, and `11` testing. Other documents provide context, not automatic permission to expand scope.

## Non-Negotiable Project Contracts

- Python is managed with `uv`; follow the pinned project version and committed lockfile. Technical docs, code, identifiers, and logs are in **English**; user-facing UI text is in **Vietnamese**.
- M5 records **observed unit sales**, not individual orders or verified inventory. Derived rows, forecasts, and simulated inventory must retain distinct provenance labels.
- Keep the batch architecture: HDFS storage, Spark distributed processing and MLlib forecasting, ClickHouse distributed analytical serving, and a lightweight Streamlit UI. Do not add streaming platforms or microservices without a requirement.
- Preserve the seven-day forecasting horizon, chronological evaluation, and prevention of future-data leakage. Report measured model/policy results rather than assumed improvements.
- Do not claim real Walmart stock, operational savings, physical multi-host fault tolerance, or ClickHouse high availability from the proposed local simulation.

## PLAN

PLAN owns discovery and task-specific decisions; it does **not** implement.

1. Identify the task's locked scope, owning modules, and observable acceptance criteria.
2. Inspect only related source, configuration, tests, and dependencies. Use Documentation Routing for the few necessary design sections.
3. Identify affected contracts, boundaries, risks, and the **narrowest useful verification**.
4. Produce a concise task handoff named `plan.md` in the task workspace (do not commit a shared root plan file unless requested), using:
   - **Repository Findings** — verified facts and exact relevant doc headings.
   - **Affected Files** — existing files and required new files.
   - **Proposed Changes** — ordered steps, invariants, and out-of-scope items.
   - **Focused Verification** — one targeted command or explicit acceptance check.

If a material requirement is ambiguous or contradictory, mark it unresolved; do not fabricate a decision.

## BUILD

BUILD implements the approved task plan; it does **not** repeat discovery.

1. Read `plan.md` and the directly affected files. Do not reopen routed docs unless the plan names an exact section or a concrete conflict emerges.
2. Write or update focused tests **before production changes** when the task is testable. For docs/config-only work, use the narrow check specified in the plan.
3. Implement only planned changes. Expand scope only for an unavoidable direct dependency; report the deviation.
4. Run only the planned focused verification. Do not run full suites, full builds, service startup, large Spark jobs, or manual end-to-end checks without task-specific justification.
5. If verification passes, **stop testing**; run one final `git status --short`, report, and stop.
6. If verification fails, investigate that task-related failure, make **one corrective pass**, and rerun the **same** verification once. If it still fails, report the blocker and stop.

## Git and Reporting

Do not perform Git write operations (commit, push, branch creation, merge, reset, or PR creation) unless explicitly requested. PLAN may inspect relevant history; BUILD does not inspect unrelated history or diffs. Never commit secrets, raw datasets, virtual environments, or generated artifacts.

Report only **changes made, focused verification and its actual result, and blockers/deviations**. Do not claim tests, distributed execution, benchmarks, or integrations passed without executing them.
