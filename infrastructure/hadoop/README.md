# HDFS deployment, integration verification, and operations (T01–T03)

This is a persistent, single-host development cluster: one NameNode and two
DataNodes, with replication factor **2**. It demonstrates distributed software
roles, not physical-host fault tolerance, HA, production security, or proven host
capacity. T02 supplies side-effect-free Python configuration loading; T03 supplies
opt-in independent-client, replication, persistence, and interruption/recovery
verification. Spark and source-data ingestion acceptance remain later gates.

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

## T03 integration verification

The tests inspect the **existing, initialized, running** T01 cluster. A requested
runtime test fails with a prerequisite reason if Docker, the explicit URI, matching
configuration, or write-readiness is unavailable. The tests never initialize or
format storage, delete volumes, change replication, or start an absent cluster.

Default pytest selection excludes the registered `integration` marker. These
focused offline checks need no running services:

```bash
uv run --locked pytest -q tests/unit/test_hdfs_verification.py
uv run --locked ruff check tests/integration tests/unit/test_hdfs_verification.py
uv run --locked ruff format --check tests/integration tests/unit/test_hdfs_verification.py
```

Run the non-disruptive scenario explicitly from the repository root:

```bash
WAREHOUSE_HDFS_URI=hdfs://hdfs-namenode:8020 uv run --locked pytest -q -m integration tests/integration/test_hdfs.py
```

This executes the independent-client scenario and skips the three disruptive cases
**before fixture setup**. It creates a uniquely named, 512 MiB/one-CPU temporary
client on `warehouse-bigdata` with a 256 MiB Hadoop heap. The client inherits the
resolved Compose image, platform, UID/GID, and two read-only XML bind sources. Its
daemon entrypoint is bypassed and no HDFS storage volumes are attached.

Each scenario exclusively creates its own `/t03-integration-<32-hex-uuid>` directory
and writes a deterministic, non-empty **65,536-byte** binary payload. This test-only
namespace is outside M5 and approved artifact paths. `load_hdfs_config()` supplies
the required URI and records the base-path contract (default `/data/warehouse`);
the tests do not publish outputs there. A URI targeting a different endpoint is
rejected before fixture creation.

Success requires all of the following, beyond container health:

- Service DNS, NameNode RPC/metadata operations, and TCP access to **both** advertised
  DataNode transfer endpoints (`9866`) from the separate client.
- Both expected live hostnames, normal registration, distinct persistent DataNode
  UUIDs, distinct current transfer addresses, and NameNode safe mode off.
- Configured and file replication **2** without a `setrep` workaround.
- Non-zero fixture blocks and fixture-scoped `fsck -files -blocks -locations` with
  no missing/corrupt blocks and exactly two distinct expected locations for **every
  block**. Addresses come from the current live report, including after recreation.
- A fresh external-client read with the expected byte count, SHA-256, and exact bytes.

### Maintenance-window scenarios

Reserve an **exclusive maintenance window**, suspend other HDFS writers and
lifecycle operations, and run serially (no xdist workers). Supplying the additional
flag acknowledges this window:

```bash
WAREHOUSE_HDFS_URI=hdfs://hdfs-namenode:8020 uv run --locked pytest -q -m integration tests/integration/test_hdfs.py --run-hdfs-disruptions
```

T03 also takes a non-blocking local lock to prevent overlapping T03 invocations;
this does not coordinate unrelated Docker/HDFS clients. The three extra scenarios
have independent fixtures and baseline evidence:

| Scenario | Controlled operation | Required evidence |
|---|---|---|
| Restart persistence | Restart only the three HDFS daemons | Same containers, cluster/namespace identity, UUIDs, named-volume attachments, block IDs, bytes/hash, and two-node placement |
| Recreation persistence | Force-recreate only those daemons; retain network and named volumes | All three container IDs replaced; persistent identities, attachments, fixture block IDs/content, and replication unchanged |
| Interruption/recovery | Stop only `hdfs-datanode-2` after reconfirming two replicas | Confirmed stopped container, byte-exact fresh read via the remaining replica, restoration, new heartbeat/full block-report timestamps, and restored two-node placement |

Restoration is registered **before** issuing a lifecycle command, including a stop
that subsequently fails or times out. The harness restores affected daemons before
removing its fixture. After interruption it requests a full block report on DataNode
2's IPC port `9867` and waits for renewed contact **and** block-report timestamps.

The current heartbeat recheck is `300000` ms and heartbeat interval is `3` seconds;
dead-node detection may take approximately **630 seconds**. A short stop can leave
the node in the live report. That cached count is not degraded-read or recovery
evidence; do not change heartbeat policy or wait for a dead count as a substitute.

### Bounds, evidence, and outcomes

Commands are bounded: approximately 30 seconds for diagnostics, 120 seconds for
I/O, 180 seconds for readiness/placement, and 300 seconds for lifecycle operations.
Hadoop processes also run under an in-container timeout, with a short kill/transport
grace. Every read starts a fresh Hadoop CLI process rather than retaining a stream.

Each entered scenario writes unique, Git-ignored evidence to
`artifacts/test-runs/<run-id>/`:

- `commands.jsonl`: exact arguments, start/end timestamps, timeout, return code, and
  timeout outcome; numbered `.stdout`/`.stderr` files retain raw reports and fsck output.
- `events.jsonl`: attempted commands, registration, storage identity, confirmed down
  state, restoration, and renewed-contact evidence.
- Configuration/client/image details, baseline and post-operation snapshots,
  content hashes/counts, volume mappings, block IDs, and per-block locations.
- `outcome.json`: actual scenario status, original failure, separate restoration or
  cleanup errors, and fixture/client identifiers for recovery.

Only an exact directory whose exclusive `mkdir` succeeded may be deleted, and only
a temporary client with both the exact name and matching run-ownership label may
be removed. A timed-out/failed mkdir does not prove ownership. Uncertain fixture
ownership or failed restoration retains/reports the possible path for investigation.
Cleanup failures fail the run without replacing the original failure.

Tooling implementation is not runtime acceptance. Retain the actual pytest summary
and run outcomes. Skipped scenarios, unavailable Docker/configuration, and unexecuted
maintenance checks must be reported as such. This is single-host software-role
distribution evidence, not HA, physical-host fault tolerance, Spark compatibility,
or source-data ingestion acceptance.

### T03 execution snapshot — 2026-10-19

- Focused offline verification: **49 tests passed**.
- Explicit non-disruptive runtime selection: **1 passed, 3 skipped**. Both normal
  DataNode registrations and distinct UUIDs were verified. The single 65,536-byte
  fixture block had two distinct locations on the expected nodes; a fresh independent
  client read matched the payload exactly. Content SHA-256:
  `7daca2095d0438260fa849183dfc67faa459fdf4936e1bc91eec6b281b27e4c2`.
- The owned fixture and temporary client were removed with no restoration/cleanup
  errors. Local evidence run:
  `t03-20261019T111055-test_independent_client_registration_replication_and_integrity-3339cc473d0b4e97b7dd701bd86383b9`.
- Restart persistence, forced-recreation persistence, and interruption/recovery were
  **not executed**; their acceptance remains pending an exclusive maintenance window.

## Bounded connectivity and registration diagnostics

Run these read-only diagnostics from the repository root; each command targets only
HDFS and has a finite timeout:

```bash
timeout 30s docker compose -f infrastructure/compose.yaml ps -a hdfs-namenode hdfs-datanode-1 hdfs-datanode-2
timeout 30s docker compose -f infrastructure/compose.yaml logs --since 10m --tail 100 hdfs-namenode hdfs-datanode-1 hdfs-datanode-2
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode timeout 30s hdfs dfsadmin -report
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode timeout 30s hdfs dfsadmin -safemode get
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode timeout 30s hdfs getconf -confKey fs.defaultFS
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode timeout 30s hdfs dfs -ls /
```

Check DNS and individual TCP paths (including block transfer, not just RPC):

```bash
docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode timeout 30s bash -ec '
  for host in hdfs-namenode hdfs-datanode-1 hdfs-datanode-2; do getent hosts "$host"; done
  for endpoint in hdfs-namenode:8020 hdfs-datanode-1:9866 hdfs-datanode-2:9866; do
    timeout 3s bash -c '\''exec 3<>/dev/tcp/$1/$2'\'' _ "${endpoint%:*}" "${endpoint##*:}"
  done
'
```

These daemon-side probes help locate faults; use the independent-client test for
external-client acceptance. For a retained, positively identified fixture, inspect
only that exact file with a bounded `hdfs fsck <exact-fixture>/payload -files -blocks
-locations`. Compare **each block's** locations with the current report. For a newly
joined client, RPC success plus transfer failure usually warrants checking network
membership, advertised DataNode hostnames, XML mounts, DNS, and port `9866` access.

## Interrupted-run recovery

Normal failures and timeouts execute restoration and scoped cleanup. A host/process
crash or `SIGKILL` can prevent finalizers. Use the recorded run identifiers, command
results, and ownership evidence; do not initialize storage or remove volumes.

1. Re-establish the maintenance window and check for other active test/lifecycle
   processes. The local lock is released when its process exits; the lock file's
   existence does not mean a run is active.
2. Restore the three existing daemons against their current volumes and wait for
   actual write-readiness, then inspect registration and bounded logs:

   ```bash
   timeout 300s docker compose -f infrastructure/compose.yaml up -d --no-deps --no-recreate --wait --wait-timeout 180 hdfs-namenode hdfs-datanode-1 hdfs-datanode-2
   docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode timeout 190s bash /opt/warehouse-hdfs/healthcheck.sh wait-ready 180
   docker compose -f infrastructure/compose.yaml exec -T hdfs-namenode timeout 30s hdfs dfsadmin -report
   ```

3. Verify the run's successful **exclusive mkdir** command before removing only its
   exact `/t03-integration-<uuid>` directory via a bounded `hdfs dfs -rm -r -skipTrash
   <exact-owned-directory>`. Never infer ownership from the prefix alone or use a
   wildcard. If creation timed out or ownership is uncertain, investigate and retain
   the path rather than deleting it.
4. Inspect the exact `t03-client-<uuid>` container. Match its name and
   `warehouse-bigdata.t03.run` label to the evidence before a bounded
   `docker rm --force <exact-owned-client>` (without a volume-removal option).
5. Record any remaining paths/clients or restoration blockers. Rerun the appropriate
   opt-in scenario only after prerequisites are restored; do not mark an interrupted
   or partially cleaned run as passed.

## M02 connectivity handoff

- [ ] Join the existing external bridge **`warehouse-bigdata`**; verify service DNS.
- [ ] Supply **`WAREHOUSE_HDFS_URI=hdfs://hdfs-namenode:8020`** explicitly to the
  shared loader. Record the base-path contract **`/data/warehouse`** and any explicit
  override; configuration loading alone does not establish connectivity.
- [ ] Mount the current `core-site.xml` and `hdfs-site.xml` read-only under
  `/opt/hadoop/etc/hadoop`, retain image-provided Hadoop/Java configuration, and
  record client identity **UID 1000 / GID 100** for the tested setup.
- [ ] Demonstrate metadata/RPC access **and both DataNodes' direct TCP `9866` block
  access**, followed by independent-client byte/hash and per-block replica evidence.
- [ ] Carry the actual evidence directory and executed/skipped/blocked scenario
  results into the handoff. Do not imply a maintenance scenario ran from baseline
  success alone.

Future clients, including Spark **drivers and executors**, need both metadata and
block connectivity. T03 does not deploy Spark, prove Spark compatibility, ingest
M5, or authorize writing approved analytical outputs.
