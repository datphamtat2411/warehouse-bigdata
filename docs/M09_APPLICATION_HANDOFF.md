# M09 read-only application handoff

Construct `ClickHouseReadRepository(load_approved_publication_context(path), client,
ReadLimits())` using the existing `WAREHOUSE_CLICKHOUSE_*` reader connection.
The default publication path is `artifacts/approved_publication.json`. Loading
checks the HDFS availability fence, exact authoritative pointer, approved manifest,
release checksum, nine dataset bindings and frozen small coverage. Reload the context
before each application request; discard a repository when approval changes.

Seven methods implement `APPLICATION_READ_CONTRACTS`. SQL uses bound parameters,
explicit publication dataset identities, `readonly=1`, a 30-second timeout,
`skip_unavailable_shards=0`, throwing overflow behavior and a 1,000-row bound.
Historical ranges default to at most 366 days. No independent latest-ID selection
exists. Evaluation methods use historical `evaluation_binding()` identities.

Results carry `state`, `rows`, `provenance`, and `reason`. States are `ok`, `empty`,
`incomplete`, `unavailable`, and `error`. Query failures discard partial results.
Forecast reads require exactly seven finite nonnegative values with horizons 1–7,
matching dates and `forecast_status=ok`. Missing values are never filled with zero.
Supply reads require one product-level simulated row and never sum store display
values. Allocation requires the approved policy and complete store population.
Observed, predicted, simulated, calculated and evaluated values remain distinct.

## Verification

Use existing running services and validated artifacts. Export the existing reader,
HDFS and `WAREHOUSE_SMALL_ARTIFACTS` settings in the shell; do not copy `.env` to
evidence. Run from the repository root:

```sh
bash scripts/verify-clickhouse-distributed.sh --publication artifacts/approved_publication.json --evidence-dir artifacts/verification
WAREHOUSE_VERIFY_APPROVED_DATA=1 uv run --locked pytest -m integration tests/integration/test_clickhouse_distributed_verification.py -q
```

Run serially. Missing upstream artifacts are blockers, never regeneration triggers.
The verifier checks physical node presence, equal-weight topology, matching local
schemas/engines, separately inspected Compose storage volumes, shard routing,
nonempty observed sales on both nodes and all nine local/Distributed/source counts,
measures, null counts and fingerprints against the immutable publication statistics.
The isolated outage probe targets an unreachable remote endpoint and requires a
network failure; it does not stop deployed services or establish deployed-shard
failover behavior. A real deployed-shard outage experiment remains a separate
acceptance limitation requiring infrastructure authorization.

Evidence goes under ignored `artifacts/verification/<unique-run-id>/evidence.json`:
timestamp, Git revision, environment, version, topology, volume identities,
publication identity/checksum, scope digest, coverage, source/distributed/shard
statistics, routing checks, actual query timings, outage probe and limitations.
Evidence is written only after successful checks and revalidation of approval.
Metrics describe `demo_sample` only. No performance target, full-catalog accuracy,
HA or physical multi-host resilience is established. This handoff implements no UI.
