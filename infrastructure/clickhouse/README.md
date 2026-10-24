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
`system.one` for distributed probes. There are no serving tables.
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
services. All checks are independent of M5. Serving schemas, publication and
Streamlit remain future work.
