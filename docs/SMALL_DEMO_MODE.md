# M5 Small — Standard Project Execution Profile

## 1. Project Decision and Scope

M5 Small is the standard **real-M5** population for development, integration,
distributed acceptance, evaluation, performance measurement, future publication and
dashboards. Its identifier is `demo_sample`. It reduces computation while retaining
real observations, complete history, distributed Spark/HDFS, quality gates and provenance.
The code implements J02–J06 and chronological evaluation. M06 acceptance is a separate
explicitly authorized operation; this guide records no runtime/accuracy result until that
operation is executed.

## 2. Original M5 vs M5 Small

| Property | Original M5 reference | Standard M5 Small |
|---|---:|---:|
| Unique items | 3,049 | 10 |
| Original stores | 10 | All 10 |
| Item-store series | 30,490 | 100 |
| Departments | 7 | All 7 when supported |
| Historical days | 1,941 | All 1,941 |
| Observed daily rows | 59,181,090 | **194,100** |
| Scope | `full_m5` (optional, guarded) | `demo_sample` (standard) |

M5 records observed unit sales, including zeros, not individual orders or verified
inventory. The small population is a real subset, not synthetic replacement sales.
Future forecasts, derived features and simulated inventory retain distinct provenance.

## 3. Deterministic SKU Selection

`contracts/dataset_scope.py` owns `metadata-department-round-robin-sha256-v1`, seed 42.
J02 reads only six identity fields into a bounded metadata collection (30,490 records).
All source item/store keys and IDs must be unique; item/department/category metadata
must agree across all ten original stores, with consistent store/state identities.
Incomplete identities fail rather than being silently dropped.

Within each department, sort items by SHA-256 of canonical JSON `[42,item_id]`, then
`item_id` as tie-breaker. Traverse sorted departments round-robin by item rank and take
ten unique items. This covers all seven available departments first, then distributes
the remaining three deterministically. Daily sales, outcomes and fitted metrics never
participate. J03–J06 and evaluation must not resample this population.

## 4. Scope Metadata and Lineage

Every scoped artifact contains immutable `_scope.json` (`m5-scope-v1`). Required fields:

```text
dataset_scope, selection_policy, selection_seed, source_version, source_snapshot_id,
selected_item_ids, selected_store_ids, selected_department_ids, selected_sku_count,
selected_series_count, original_series_count, excluded_series_count,
historical_day_count, expected_observation_count, selection_digest
```

Version 1 also requires `schema_version`, `available_department_ids` and the exact sorted
`selected_series` identity records. JSON serialization uses sorted object keys, ASCII
escaping, compact separators, no NaN/Infinity, and one trailing LF. `selection_digest`
is `sha256:` plus SHA-256 of that document with the digest field omitted. Sorted arrays
and item/store ordering are validated, not left to partition ordering.

Contract v1 manifest fields and fingerprints are unchanged. The existing JSON coverage
field binds scope and digest; J04–evaluation bindings embed the identical evidence.
Consumers reject missing/mismatched scope, source identities, or non-VALIDATED upstream
publication. `_SUCCESS` alone never proves publication or approval.
Scoped job/publisher metadata reads have an explicit 64 MiB ceiling for nested bindings
and bounded occupied-stratum evidence; this is a read bound, not a reserved allocation.

## 5. J02–J06 Processing

- **J02:** validate original immutable raw source; filter to selected 100 wide rows
  before any expansion. Persist a run-owned `filtered_wide` checkpoint with all 1,941
  original string day fields, independently compare every cell with the selected source
  in bounded identity/day-column groups, and record schema/selection/file checksums.
  Retry uses that validated checkpoint.
  Bounded 128-day chunks retain quality, unit/zero totals, keys/calendar alignment,
  persisted schema validation and recovery. Final demo compaction uses four partitions.
- **J03:** read the VALIDATED J02 snapshot, preserve all 194,100 records and population,
  apply existing calendar/weekly-price semantics without row multiplication, reconcile
  analytical aggregates and propagate the identical scope.
- **J04:** preserve lookback 28; train d_29–d_1885, validation d_1886–d_1913, test
  d_1914–d_1941. Lags, rolling means, prior-known prices, missingness/eligibility and
  diagnostics remain intact. Fully eligible complete history gives 191,300 feature rows
  (185,700 train + 2,800 validation + 2,800 test); measured eligibility is authoritative.
- **J05:** existing global MLlib Random Forest/fitted preprocessing, cap 10,000,
  sampling seed 42. Preserve deterministic stratification and selected keys. Infeasible
  occupied strata fail with cap/stratum/eligible counts. Persist/reload the complete model.
- **J06:** consume the frozen ten-SKU/ten-store cohort. Forward issue is 2016-05-22
  (d_1941), seven recursive predictions, at most 700 output rows. Unsupported series
  have explicit exclusions. Feedback retains unrounded doubles; publication is finite,
  nonnegative and unique. No future actuals enter scoring.
- **Evaluation:** validation origins 1885/1892/1899/1906; test origins
  1913/1920/1927/1934. Each predicts seven days. Preserve seasonal-naïve paired
  populations, MAE/RMSE/WAPE and exclusions. Freeze model/cohort/protocol before test.
  Each phase has 2,800 intended audited keys and 48 metric rows, not 28,000 keys.

## 6. Canonical Artifact Selection

`WAREHOUSE_SMALL_ARTIFACTS` is a **local filesystem JSON path**, for example
`artifacts/small-artifacts.json`. It is not an HDFS URI and is never executor input.
It selects immutable source/snapshot/run/model/forecast/evaluation IDs plus the expected
selection digest. There is no latest-artifact search or full/smoke fallback.

Use an existing validated selector whenever possible. `init` creates fresh identities
only for an explicitly approved first small execution. Its initial digest is null;
J02 execution reads the published scope and fills that field in the local selector only
when it is null. A previously frozen digest must match and is never replaced.
Read-only integration refuses unresolved digests. Select only stages actually published;
`verify --through` requires every artifact through the named completed stage. Its report
records `verified_through` and `verified_stages`; the default `test` requires J02/J03,
features, model, forecast, validation evaluation and test evaluation, including all
producing pipeline run IDs. A processing-only selector can validate through J03 only.

HDFS artifacts live below `/data/warehouse/{raw,curated,features,models,outputs,...}`.
Within the Compose network the URI prefix is `hdfs://hdfs-namenode:8020`. Host paths
under `artifacts/` store selectors/logs/reports only, not substitutes for HDFS inputs.

## 7. Testing Strategy

1. **Offline unit/contract:** tiny deterministic synthetic fixtures; no live Hadoop or
   Standalone. Local Spark fixtures test algorithms without real-M5 observations.
2. **M5 Small integration:** read-only independent persisted readback of explicitly
   selected VALIDATED artifacts; schema, counts, keys, scope/digest, source lineage,
   unit/zero/price/aggregate reconciliation. Missing inputs fail clearly, never regenerate.
3. **Live small acceptance:** separate explicit authorization, serial execution only,
   actual Master/both-Worker tasks, HDFS publication, persisted model reload, seven-day
   coverage/exclusions, chronological evaluation and execution timings.

Default pytest excludes integration. `-m integration` still skips small-data tests
without their explicit options. The existing J04 `--run-m05-published` and
`--run-m05-lifecycle` options now use the same canonical selector; lifecycle is expensive.
Synthetic infrastructure-only tests remain independent of M5.

## 8. Retired 30-Day Smoke Workflow

The old full-catalog d_1–d_30 workflow (914,700 rows), its option/marker/runner and
seven-case 128-day benchmark matrix are retired. Their historical HDFS artifacts and
evidence remain untouched. They are not valid ML inputs. Useful independent
reconciliation, immutable recovery, permission and task-evidence assertions survive
in synthetic and M5 Small checks. Legacy empty `.env` selectors are migration reminders.

## 9. HDFS Staging and Permissions

Spark runs as `spark`; publication is owned by `hadoop`. The owner prepares only the
exact requested staging path and optional empty `events` child, verifies ownership,
and grants scoped Spark access/default ACLs. It refuses nonempty execution evidence,
foreign ownership and unsafe state. It never recursively changes shared namespace ACLs.
The driver verifies effective Spark write access before creating its SparkContext.

Existing shared namespace parents must already pass `hdfs_namespace check`. Do not
automatically recreate infrastructure, HDFS volumes or run namespace setup to hide a
failed precondition. Owner publication claims, seals and exclusively renames candidates.

## 10. Exact Runtime Commands

Run from the repository root. Commands below are procedures for a **separately
authorized session**, not permission to launch live jobs during ordinary BUILD.

### Local setup and readiness

```bash
uv sync --locked --group spark-test
bash scripts/spark-cluster.sh status
bash scripts/spark-cluster.sh check
```

After code changes, the prepared runtime must contain the current code. Its explicit
build is `docker compose -f infrastructure/compose.yaml --profile spark-submit build spark-submit`.
Do not build/start services merely because an integration artifact is missing.

### Reuse existing selection

Set `.env`: `WAREHOUSE_SMALL_ARTIFACTS=artifacts/small-artifacts.json`. Verify local syntax:

```bash
bash scripts/load-env.sh uv run --locked python -c \
  'from warehouse_bigdata.common.small_artifacts import load_small_artifacts; s=load_small_artifacts(); print(s["dataset_scope"], s["selection_digest"])'
```

### First approved small run only

Use the exact source version from existing VALIDATED immutable raw evidence:

```bash
uv run --locked python -m warehouse_bigdata.common.small_demo init \
  --artifacts artifacts/small-artifacts.json --source-version "${SOURCE_VERSION:?set the validated raw source SHA-256 identity}"
```

The generated selector has unique small snapshot, J02–J06 and evaluation identities.
The runner prepares exact run permissions and executes only the explicitly listed stages:

```bash
bash scripts/load-env.sh uv run --locked python -m warehouse_bigdata.common.small_demo run \
  --artifacts artifacts/small-artifacts.json --stages j02 j03 j04 --authorize-live
bash scripts/load-env.sh uv run --locked python -m warehouse_bigdata.common.small_demo run \
  --artifacts artifacts/small-artifacts.json --stages j05 --authorize-live
bash scripts/load-env.sh uv run --locked python -m warehouse_bigdata.common.small_demo run \
  --artifacts artifacts/small-artifacts.json --stages j06 validation test --authorize-live
```

J05 and J06/evaluation are measured separately; no tuning/retraining is implicit. To use
existing upstream artifacts, select their exact IDs/digest and list only downstream stages.
Before preparing or submitting each stage, the runner performs a read-only owner-side
preflight of its required upstream publications: VALIDATED status, sealed paths, exact
source/snapshot/run/output identities, frozen ML bindings and the selector's expected
selection digest. Missing or conflicting inputs stop the stage before a Spark driver
starts. J02 checks validated raw input and any existing frozen selection; a new null
digest remains unresolved until completed-execution and published-scope readback succeed.

### Narrow manual owner preparation

For one explicitly chosen run (the serial runner performs the same operation):

```bash
docker compose -f infrastructure/compose.yaml --profile spark-submit run --rm --no-deps -T \
  --pull never --user 1000:100 --entrypoint /opt/warehouse-app/.venv/bin/python spark-submit \
  -m warehouse_bigdata.common.hdfs_namespace prepare-run --pipeline-run-id "${RUN_ID:?set the exact new run ID}" --events
```

Low-level J02 uses `warehouse_bigdata/jobs/normalize_sales_daily.py` through
`scripts/spark-submit.sh`, with explicit `--dataset-scope demo_sample`, source version,
snapshot/run IDs, HDFS URI and Standalone master. `stage_arguments()` in
`common/small_demo.py` defines the exact executable argument sets for all J02–J06 jobs.
The runner is the recommended reproducible command surface, avoiding scattered IDs.

## 11. Verification Commands and Expected Counts

Distributed immutable-artifact readback (no ETL/model fitting):

```bash
bash scripts/load-env.sh uv run --locked python -m warehouse_bigdata.common.small_demo verify \
  --artifacts artifacts/small-artifacts.json --through j04 --authorize-live
bash scripts/load-env.sh uv run --locked python -m warehouse_bigdata.common.small_demo verify \
  --artifacts artifacts/small-artifacts.json --through test --authorize-live
bash scripts/load-env.sh uv run --locked pytest -q -m integration --run-small-integration \
  tests/integration/test_m04_processing.py
```

Expected J02/J03 facts: 10 items, 10 stores, 100 unique pairs, 1,941 days and **194,100
rows**, zero duplicate daily keys and matching selected-wide unit/zero totals. J03
price matches plus missing-price records must sum to 194,100; aggregates reconcile.
J04 may have exclusions; full eligible targets number 191,300. J05 cap ≤10,000;
J06 publishes seven rows per successful series, at most 700. Validation/test each
audit 2,800 intended keys and persist 48 metrics with paired-population evidence.

Explicit serial live acceptance/performance, after selecting the exact authorized stages
in `.env` (for example `WAREHOUSE_SMALL_STAGES="j05 j06 validation test"`):

```bash
bash scripts/load-env.sh uv run --locked pytest -q -m integration --run-small-acceptance \
  tests/integration/test_m04_performance.py
```

Do not use pytest-xdist or overlap Spark acceptance sessions. The final full-chain
readback requires all selected downstream artifacts. A partial run is not a passing
complete-chain acceptance. Missing inputs remain blockers.

## 12. M06 Distributed Acceptance and Handoff

M06 has one real-cluster entry point. It is serial, requires `--run-small-acceptance`,
and never starts infrastructure or regenerates J01–J05 inputs. The selector must be
the frozen `demo_sample` selector through `test`; J03/J04 must retain all 1,941 days
and the selected 194,100 observed rows. The accepted execution stages are only
`j06`, `validation`, and `test`.

The empty `WAREHOUSE_SMALL_STAGES` value verifies existing selected outputs only.
When stages are listed, the coordinator verifies the complete J02/J03/J04/J05/model
chain first, validates already published downstream outputs, and submits only absent
outputs from the explicitly listed stages. Missing unlisted output, a conflicting
immutable identity, or an incompatible validation freeze is a blocker. A 30-day
smoke artifact is never a valid M06 input.

Run only in a separately authorized, prepared session:

```bash
bash scripts/load-env.sh env WAREHOUSE_SMALL_STAGES="j06 validation test" uv run --locked pytest -q -m integration --run-small-acceptance tests/integration/test_m06_acceptance.py
```

The coordinator writes a unique `artifacts/m06-acceptance/<acceptance_run_id>/`
bundle. It retains commands, raw logs, resource snapshots, producer event-log
evidence, a fresh readback application, model reload/probe results, persisted
forecast and paired-metric reconciliation, and targeted HDFS DataNode/block
placement checks. Successful task participation must be attributable to both
configured Workers; failed, retried, incomplete, truncated, idle, or single-Worker
evidence blocks acceptance. Point-in-time resource samples are labeled as such and
are not peak-memory measurements.

Report states are `NOT_RUN`, `BLOCKED`, `FAILED`, and `PASSED`. `PASSED` requires
all gates, including fresh checksums, schema/lineage identity, seven-day forecast
coverage and exclusions, four-origin chronological pairing, 48 metric rows per
phase, successful readback execution on both Workers, and HDFS integrity. A
partial readback or successful process exit without the `SMALL_READBACK` report is
not success. Preserve partial evidence for recovery; recovery may retry the same
frozen downstream request but does not refit the model or alter upstream IDs.

The handoff identifies the validated forecast and metric paths, exact selector
lineage and digest, seven-day totals, coverage/exclusions, Contract v1 schemas and
fresh checksums. M06 does not assign a publication ID, update an approved pointer,
or approve serving. J07/J08 may consume the handoff only after their own approval
and publication gates.

## 13. Performance Measurement

The serial runner writes unique `artifacts/small-demo/<uuid>/` command, stdout/stderr,
actual per-stage wall times, completed-event evidence and Docker resource samples.
`small_evidence` validates one completed nonrolling event log, actual Standalone master,
application ID and successful tasks on **both** named Workers. It reports stage/job/task
counts, input/output bytes, executor runtime, spill and GC; unavailable metrics are not
invented. Docker snapshots are point-in-time usage, not measured peak memory.

The J02–J04 target **under 30 minutes** is unverified. Record J05/J06 separately. Compare
only identical scope/digest/code and disclose resource/cache conditions. No benchmark
matrix is required. Raw integrity validation still reads original source bytes and can
be material overhead; the filtered-wide checkpoint removes repeated CSV parsing by chunks.

Inspect a completed run independently:

```bash
docker compose -f infrastructure/compose.yaml --profile spark-submit run --rm --no-deps -T \
  --pull never --user 1000:100 --entrypoint /opt/warehouse-app/.venv/bin/python spark-submit \
  -m warehouse_bigdata.common.small_evidence --pipeline-run-id "${RUN_ID:?set a completed small run ID}"
```

## 14. Failure Recovery

- Preserve failed staging, event logs and immutable IDs. Never append or overwrite to
  make counts agree. Missing validated upstream data is a blocker, not a rebuild request.
- For a frozen publication request, retry the same runner stages/selector with
  `--recover --authorize-live`. The submission wrapper attempts owner recovery first.
   Every J02–evaluation frozen request is checked against the requested job/source/input
   binding before publication recovery. Once publication is completed and independently
   verified, the launcher skips the driver, preserving the original single event log.
   This includes recovery interrupted between candidate promotion and completion-marker
   publication. Compare immutable hashes/readback; recovery is not a new Spark timing.
- If no complete frozen request exists or a filtered-wide checkpoint is incomplete,
  preserve it and use a new run ID. New source population/configuration requires a new
  snapshot identity; changing only downstream attempts must keep the upstream selection.
- Do not reuse old full-M5 or `m04-smoke-*` identities. A previously running full-history
  J02 is not cancelled or cleaned automatically. Inspect its exact app/run ID and owner
  evidence, obtain an explicit manual cancellation decision if necessary, wait for the
  intended transition, and select fresh small IDs. Do not delete its staging or history.
- No operation here recreates HDFS volumes, resets Docker infrastructure, or repairs
  shared permissions recursively.

## 15. Full-M5 Safety Guard

J02 defaults to `demo_sample`. `full_m5` requires **both** explicit
`--dataset-scope full_m5` and `--confirm-large-workload`, plus separate user authorization.
Downstream derives scope from immutable evidence and rejects mismatched requested scope;
full upstream requires the same explicit two flags. Reduced-history operational J02
arguments are rejected. Optional full algorithms/source constants stay testable offline.
No supported routine test, startup or demonstration invokes a full-M5 reshape.

## 16. Known Limitations

The ten-item cohort is not a catalog-wide accuracy/generalization sample. All metrics
must identify `demo_sample`. No real Walmart inventory, physical multi-host resilience,
operational savings or ClickHouse high availability is established by a local simulation.
ClickHouse/UI/inventory implementation is outside this profile change. Live small-chain
acceptance, actual runtime/accuracy, and the under-30-minute objective remain pending.

## 17. Instructions for Future Agents

Read `AGENTS.md`; route documents by the task. Reuse canonical VALIDATED small artifacts,
verify selection/publication/source identity, and generate safe `demo_sample` commands.
Keep unit tests synthetic, preserve locked temporal boundaries and seven-day recursion,
and never regenerate full data for tests. Explicitly authorize and serialize live work;
report only executed evidence. Preserve historical full/smoke artifacts. Use this guide's
implemented CLI rather than inventing commands or inferring a latest snapshot.
