# HDFS deployment and lifecycle (T01)

This is a persistent, single-host development cluster: one NameNode and two
DataNodes, with replication factor **2**. It demonstrates distributed software
roles, not physical-host fault tolerance, HA, production security, or proven host
capacity. Spark, ingestion, Python client configuration (T02), and comprehensive
failure/recovery or separate-client integration checks (T03) are not included.

## Runtime and interfaces

All four Compose services, including the opt-in initialization helper, use the
official Apache image:

```text
apache/hadoop:3.4.3@sha256:127774dadab40ce84df7ac668a7a8c99945688b3fe336f1388f4477ca33e1529
```

T01 explicitly selects **Hadoop 3.4.3**, rather than the 3.4.2 candidate in the stack
design (§12). The image is `linux/amd64`, includes Java 11, and supplies Hadoop in
`/opt/hadoop`. ARM hosts require working amd64 emulation. Retain the image's
`JAVA_HOME` and remaining Hadoop configuration files. Only `core-site.xml` and
`hdfs-site.xml` are overlaid, read-only.

| Service / hostname | Development-network ports | Published host ports |
|---|---|---|
| `hdfs-namenode` | RPC `8020`, HTTP/JMX `9870` | **`127.0.0.1:9870` only** |
| `hdfs-datanode-1` | Transfer `9866`, IPC `9867`, HTTP/JMX `9864` | None |
| `hdfs-datanode-2` | Transfer `9866`, IPC `9867`, HTTP/JMX `9864` | None |

The Compose project and user-defined bridge network are both named
`warehouse-bigdata`. There are no static IPs or fixed container names. The stable
internal endpoint is **`hdfs://hdfs-namenode:8020`**. Each DataNode advertises its
own service hostname. Hadoop clients and DataNode replication use these hostnames
for direct block access, not just NameNode RPC.

Future Spark drivers **and** executors must join this network and receive both
Hadoop client XML files. A separate Compose project can declare the existing
network as external with `name: warehouse-bigdata`. Host-side RPC access is not
configured; exposing just RPC would not make DataNode block endpoints reachable.
The existing Python environment contract is unchanged by T01.

HDFS uses simple authentication and normal filesystem permission checks. This is
not an authenticated security boundary: only trusted development containers should
join the bridge. Do not expose the cluster to an untrusted network.

## Initialize once and start

Prerequisites: Docker Engine with Docker Compose v2 (`up --wait` support), Bash,
and enough available memory/disk for the image and persistent volumes. Run these
commands from the repository root:

```bash
bash scripts/init-hdfs.sh
docker compose -f infrastructure/compose.yaml up -d --wait --wait-timeout 180
bash scripts/smoke-hdfs.sh
```

Initialization is **explicit**, never part of daemon startup. The wrapper resolves
repository paths even when invoked from another directory. It refuses running,
paused, restarting, or otherwise active HDFS containers. Do not run initialization
and lifecycle commands concurrently. The `init` profile keeps the short-lived
`hdfs-init` helper out of normal `up`; use the wrapper rather than invoking that
helper directly and bypassing its stopped-container check.

The helper mounts all three volumes and classifies their contents **before any
mutation**, including hidden entries:

- All three empty: prepare ownership of only the three volume roots, then format
  the NameNode once with `-format -nonInteractive`, **without force**.
- Valid NameNode metadata and compatible or empty DataNode storage: report
  `already initialized`, without formatting, ownership changes, or cleanup.
- Partial/missing NameNode metadata with retained contents, unrecognized or
  malformed metadata, or mismatched cluster IDs: fail, preserving everything.

Root is used only by the helper to prepare fresh volume ownership and switch to
`hadoop` (UID **1000**, GID **100**) for formatting. Daemons always run as that
unprivileged user. The image's automatic-formatting starter is bypassed by
`dumb-init` and the project lifecycle script. Existing ownership is never rewritten
recursively. Normal NameNode startup requires a complete metadata structure;
DataNodes can initialize genuinely empty storage through registration. Hadoop
itself validates deeper image/edit-log/block-pool integrity; these guards are not
repair tools or substitutes for backups.

## Health versus write-readiness

Compose health probes use bounded HTTP/JMX and TCP requests, **not recurring
health-check JVMs**. NameNode health checks responsiveness and RPC listening only;
it does not depend on DataNodes or safe mode. DataNodes depend on that health and
then check their own service and live registration under their advertised hostname.
This avoids a startup dependency cycle.

Before writes, require both distinct live nodes and NameNode safe mode off:

```bash
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode \
  bash /opt/warehouse-hdfs/healthcheck.sh wait-ready 180
```

The smoke script includes this finite readiness wait. It checks the two persistent
DataNode UUIDs, the live registration report, a tiny uniquely named file's content
SHA-256 and HDFS checksum, and block placement on both nodes. It removes **only its
own** temporary HDFS directory, even on failure. It does not stop DataNodes or test
external Spark clients. CLI operations have finite timeouts.

## Volume-preserving operations

Three independent Compose-managed named volumes hold real HDFS metadata/blocks:

| Volume | Daemon path |
|---|---|
| `warehouse-bigdata_hdfs-namenode-data` | `/var/lib/hdfs/name` |
| `warehouse-bigdata_hdfs-datanode-1-data` | `/var/lib/hdfs/data` on DataNode 1 |
| `warehouse-bigdata_hdfs-datanode-2-data` | `/var/lib/hdfs/data` on DataNode 2 |

Keep the Compose project/volume names stable. Do not delete/prune volumes, reset
cluster identity, force-format metadata, or attempt to fix a mismatch by removing
individual storage directories. There is intentionally no reset/cleanup command.

```bash
# Stop gracefully, retaining containers and volumes.
docker compose -f infrastructure/compose.yaml stop
docker compose -f infrastructure/compose.yaml up -d --wait --wait-timeout 180

# Restart existing containers; readiness may lag container startup.
docker compose -f infrastructure/compose.yaml restart
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode \
  bash /opt/warehouse-hdfs/healthcheck.sh wait-ready 180

# Remove containers/network, retaining all named volumes. Never add a volume-removal flag.
docker compose -f infrastructure/compose.yaml down
docker compose -f infrastructure/compose.yaml up -d --wait --wait-timeout 180

# Recreate containers against the same storage; do not initialize again.
docker compose -f infrastructure/compose.yaml up -d --force-recreate --wait --wait-timeout 180

# Inspect state and bounded recent logs.
docker compose -f infrastructure/compose.yaml ps -a
docker compose -f infrastructure/compose.yaml logs --tail 100 hdfs-namenode hdfs-datanode-1 hdfs-datanode-2
```

Daemons run in the foreground under `dumb-init`, receive graceful termination, and
have a **60-second** stop grace period. Restart policy is `on-failure:3`; failed
initialization is not retried automatically. Docker JSON logs rotate at 10 MiB,
three files per container.

Open the NameNode UI at **http://127.0.0.1:9870**. DataNode UI links use internal
container hostnames and are not published to the host. Use the NameNode UI, JMX
from the bridge, and CLI reports for inspection.

## Inconsistent-state diagnostics

Stop all daemons before inspecting initialization problems. Preserve all volumes
and logs. Read identity metadata without editing it:

```bash
docker compose -f infrastructure/compose.yaml --profile init run --rm --no-deps \
  --entrypoint /bin/bash hdfs-init -c \
  'for file in /var/lib/hdfs/name/current/VERSION /var/lib/hdfs/datanode-{1,2}/current/VERSION; do printf "%s\n" "$file"; cat "$file"; done'
```

Compare `clusterID` across existing storage and retain each DataNode's
`datanodeUuid`. If NameNode metadata is missing while block storage remains, do
**not** format: recover the matching NameNode backup or investigate the original
volume mapping. A failed first format may leave partial files; a subsequent init
will refuse them. Diagnose the failure rather than clearing storage. Unhealthy
registration may indicate DNS/network/configuration issues or incompatible
metadata; inspect daemon logs instead of disabling permissions or forcing safe
mode off. Compose health does not automatically restart an unhealthy process.

## Resource limits and measurement

| Role | Heap minimum / maximum | Container memory | CPU limit |
|---|---|---|---|
| NameNode | 128 / 512 MiB | 1 GiB | 1 |
| Each DataNode | 128 / 256 MiB | 768 MiB | 1 |
| Initialization helper | 128 / 256 MiB | 512 MiB | 1 |

These use Hadoop's `HADOOP_HEAPSIZE_MIN/MAX`, leaving native-memory headroom.
Daemon container limits total **2.5 GiB**, not measured working-set demand. The
helper runs separately while daemons are stopped. Treat these as starting limits
within the approximately 10 GiB WSL2 budget; reserve memory for Docker/the OS and
future Spark, ClickHouse, and UI workloads. Tune only using measured demand,
leaving headroom and updating the declared limits; no capacity claim is implied.

```bash
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode hdfs version
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode java -version
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'
```

### Implementation acceptance snapshot — 2026-10-18

- Focused unit tests: **27 passed**; all four shell syntax checks and Compose
  validation passed. Fresh-volume initialization and the smoke check passed with
  two live DataNodes, safe mode off, matching content SHA-256, and both block replicas.
- The following identities and a tiny persistence sentinel survived `stop`/`up`,
  `restart`, `down`/`up`, and forced container recreation:

  ```text
  clusterID:       CID-0b2e78f8-ce2d-43ee-9dd6-1ff628f26659
  DataNode 1 UUID: f4b7933b-f040-451f-8e0e-0ddca29024f3
  DataNode 2 UUID: 99750ada-c578-43af-ad2b-145b8568bf86
  ```

- Initialization refused active daemons. Two repeated initializations while stopped
  preserved storage file hashes, ownership, modes, and sizes. The sentinel remained
  readable afterward and was then removed; all named volumes were retained.
- Running Hadoop **3.4.3** / OpenJDK **11.0.28** were confirmed. The loopback UI
  returned HTTP **200**; RPC and DataNode ports had no host bindings. Docker settings,
  live JVM heap flags, and cgroup memory/CPU limits matched the declared values.
- One-shot Docker memory readings: NameNode **206.5 MiB**, DataNode 1 **183.3 MiB**,
  DataNode 2 **169.1 MiB**. These are a tiny-workload snapshot, not a capacity test.
- The final inspection passed on its allowed retry after correcting `docker top`
  to include a PID column (`-eo pid,args`); no deployment correction was needed.
  The three daemon containers were left running and healthy.

## Focused offline checks

```bash
uv run --locked pytest -q tests/unit/test_hdfs_lifecycle.py
bash -n infrastructure/hadoop/lifecycle.sh
bash -n infrastructure/hadoop/healthcheck.sh
bash -n scripts/init-hdfs.sh
bash -n scripts/smoke-hdfs.sh
docker compose -f infrastructure/compose.yaml config --quiet
```

The unit checks use temporary directories and stub external commands. They do not
start services, install Hadoop, or change volumes; default CI remains service-free.
