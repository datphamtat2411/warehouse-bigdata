# ClickHouse cluster infrastructure

The opt-in `clickhouse` profile deploys the digest-pinned 26.8 image, exactly two
shards and one replica per shard. Both nodes mount static `warehouse_cluster`
metadata read-only. This single-host deployment provides no high availability.

## Initialization and volume-preserving lifecycle

Requires Docker Compose V2, a running daemon, Python 3 and free localhost port 8123.
No host ClickHouse, HDFS or Spark installation is needed.

```bash
bash scripts/init-clickhouse-env.sh
bash scripts/clickhouse-cluster.sh up
bash scripts/clickhouse-cluster.sh status
bash scripts/clickhouse-cluster.sh check
bash scripts/clickhouse-cluster.sh logs
bash scripts/clickhouse-cluster.sh stop
```

The initializer fills missing/empty ClickHouse settings in ignored `.env`, preserves
unrelated settings and nonempty values, silently generates passwords and sets mode
0600. Keep `.env`, expanded Compose configuration and container environments private.
Use three distinct account names. Reader settings retain the application contract;
publisher and admin settings are infrastructure-only.

Host HTTP endpoint: `http://localhost:8123` (IPv4 loopback only).
Container HTTP endpoint: `http://clickhouse-1:8123`.
Native `clickhouse-1:9000` and `clickhouse-2:9000` remain internal to the bridge.
Distributed requests use the publisher credentials.

Each node has 1 CPU, 1 GiB container memory, a 768 MiB server cap, bounded
background pools and rotating Docker logs. These are starting limits for the
project's 10 GiB WSL2 working budget, not measured workload capacity.

`up` uses bounded waits and `--no-deps` on only the two ClickHouse services.
`stop` and recreation preserve separate `clickhouse-1-data` and `clickhouse-2-data`
volumes. Never delete volumes to change credentials. HDFS/Spark are not operated.

On first empty-volume initialization the official image creates the private admin;
the bootstrap independently creates an empty `warehouse` database and idempotent
reader/publisher users on both nodes. Reader has SELECT on `warehouse.*` and
`system.clusters`. Publisher has SELECT/INSERT on `warehouse.*` and SELECT on
`system.one` for distributed probes. Serving tables are initialized separately below.
Bootstrap does not rerun on populated volumes: editing `.env` does not rotate
persisted reader/publisher passwords. Account changes require explicit authenticated
administration on each node; retain credentials consistent with persisted users.

## Focused verification

```bash
uv run --locked pytest -q tests/unit/test_config.py tests/unit/test_clickhouse_infrastructure.py
docker compose -f infrastructure/compose.yaml --profile clickhouse config --quiet
bash scripts/clickhouse-cluster.sh up
bash scripts/clickhouse-cluster.sh check
uv run --locked pytest -q -m integration tests/integration/test_clickhouse_topology.py
```

`check` runs SELECT 1, version() and hostName() independently on each node,
requires exact system.clusters rows `1/1/clickhouse-1` and `2/1/clickhouse-2`,
checks clusterAllReplicas against system.one reaches both hosts and requires
matching 26.8 versions. Integration tests are explicitly opt-in and use existing
services. All checks are independent of M5.

## Contract v1 serving schema

After environment initialization and with both services running:

```bash
bash scripts/clickhouse-schema.sh apply
bash scripts/clickhouse-schema.sh apply
bash scripts/clickhouse-schema.sh check
uv run --locked pytest -q tests/unit/test_clickhouse_schema.py tests/unit/test_clickhouse_infrastructure.py
uv run --locked pytest -q -m integration tests/integration/test_clickhouse_schema.py
```

The runner requires Python 3 and the existing Compose containers, using the admin
account configured by `WAREHOUSE_CLICKHOUSE_ADMIN_USER` and
`WAREHOUSE_CLICKHOUSE_ADMIN_PASSWORD` in private `.env`. Container credentials
must match persisted users. No host ClickHouse client is needed. Readiness and
the exact two-shard topology are checked before schema operations.

`001_warehouse_local.sql` explicitly creates nine independent MergeTree tables on
each node. `002_warehouse_distributed.sql` creates their Distributed counterparts
only on `clickhouse-1`: `sales_daily`, `store_daily_sales`, `forecast_daily`,
`forecast_metrics`, `inventory_scenario`, `central_supply_scenario`,
`replenishment`, `allocation`, and `policy_evaluation`.

Both commands validate columns, types including nullability, engines, sort keys,
Distributed targets and sharding. Initialization uses conditional creation and
preserves existing rows. Incompatible tables cause a nonzero exit and require an
explicitly reviewed migration. Application is not transactional across nodes;
after a partial failure, fix its cause and rerun. `check` performs no DDL.

Contract v1 governs numeric widths and lineage. Observed sales retain
`source_snapshot_id`; `date` maps to `sale_date`; raw model predictions remain in
Parquet. Predicted values, simulated inputs, recommendations and historical policy
outcomes remain separate. The documented sort keys are indexes, not uniqueness
constraints. Product-store facts hash both identifiers; central supply hashes
item, store aggregates hash store/date, and forecast metrics hash
evaluation/holdout/method. Keep `skip_unavailable_shards = 0` so unavailable shards
fail explicitly.

The integration test uses synthetic rows in a unique disposable database and
captures `SHOW CREATE TABLE` output in pytest's temporary directory. It never reads
or publishes M5 artifacts. Warehouse reader/publisher grants remain unchanged.
This unreplicated single-host simulation has no Keeper or high availability.
Publication, approval and Streamlit implementation remain separate tasks.
