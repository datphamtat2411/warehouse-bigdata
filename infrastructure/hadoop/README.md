# HDFS deployment, integration verification, and operations (T01–T03)

This is a persistent, single-host development cluster: one NameNode and two
DataNodes, with replication factor **2**. It demonstrates distributed software
roles, not physical-host fault tolerance, HA, production security, or proven host
capacity. T02 supplies side-effect-free Python configuration loading; T03 supplies
opt-in independent-client, replication, persistence, and interruption/recovery
verification. M02's bounded synthetic Spark–HDFS acceptance passed on 2026-10-21,
as recorded in the M02 handoff below; curated source-data processing remains a later
gate.

## M5 raw ingestion

The M5 workflow consumes an already-acquired local CSV directory. It profiles the source
before creating an HDFS client, uploads through the existing bounded Hadoop client, and
publishes only a Contract v1 run manifest after independent size, SHA-256, replication,
and block-placement checks pass. The source directory is read by host-side Python;
Kaggle downloading is not part of this workflow.

The supported launcher is a host workflow, not a Compose application container. It requires
Ubuntu-local `uv`, Bash, `realpath`, `od`, `mktemp`, Docker Engine, and a reachable Docker
daemon. The existing initialized HDFS daemons must already be write-ready. It reuses the
pinned Apache Hadoop 3.4.3 image, the existing `warehouse-bigdata` bridge, and the two
read-only `core-site.xml`/`hdfs-site.xml` binds. It does not start, recreate, or reinitialize
HDFS or Spark services.

The host launcher sources the repository `.env` automatically; it supplies the
run-owned client-container variables itself. With no `.env`, inherited variables remain
supported:

| Variable | Requirement |
|---|---|
| `WAREHOUSE_HDFS_URI` | Required; normally `hdfs://hdfs-namenode:8020`, matching `core-site.xml` |
| `WAREHOUSE_HDFS_BASE_PATH` | Optional; defaults to `/data/warehouse` |
| `WAREHOUSE_ARTIFACTS_DIR` | Optional writable Ubuntu-local evidence directory |
| `WAREHOUSE_SCRATCH_DIR` | Optional writable disk-backed Ubuntu-local scratch; defaults to `artifacts/scratch` |
| `WAREHOUSE_M5_SOURCE_DIR` / `WAREHOUSE_M5_SOURCE_URL` | Optional source inputs; explicit launcher flags take precedence |
| `WAREHOUSE_M5_DOWNLOADED_AT_UTC` | Optional actual acquisition timestamp; blank preserves unknown provenance |
| `WAREHOUSE_HDFS_CLI_CONTAINER` / `WAREHOUSE_HDFS_CLI_OWNER` | Launcher-owned; do not set manually |

With the existing initialized HDFS and Spark services ready, and the prepared
Spark runtime available for the later verification, run from the repository root:

```bash
bash scripts/ingest-m5.sh \
  --source-dir /path/to/m5-csvs \
  --source-url https://www.kaggle.com/competitions/m5-forecasting-accuracy/data
```

The launcher creates one uniquely named, run-labeled Hadoop client with no HDFS storage
volume. Python runs on the Ubuntu host and sends Hadoop CLI operations through the explicit
`docker exec -i <client> hdfs ...` prefix, so CSV uploads and remote hashes remain streamed.
The client is checked for the pinned image, running/healthy state, bridge membership,
ownership label, effective Hadoop settings, and read-only XML mounts before ingestion. On
success, failure, or interruption, cleanup removes only the exact client whose name and
ownership label match this invocation. An absent, unhealthy, or misconfigured client fails
before HDFS mutation.

The attempt writes local evidence below `artifacts/m5-ingestion/<pipeline_run_id>/` and
uses `/data/warehouse/raw/m5/source_version=<content-derived-id>/` plus
`/data/warehouse/manifests/runs/<pipeline_run_id>.json`. A matching source rerun gets a
new pipeline run ID and reuses the validated raw snapshot. Existing raw files and run
manifests are never overwritten. Profile, upload, integrity, and publication failures
are recorded as `FAILED`; no incomplete upload is reported as `VALIDATED`.

`--downloaded-at-utc` is optional. Supply the actual UTC acquisition time when it is
known; otherwise the profile and every source manifest retain `downloaded_at_utc: null`.
The pipeline records profiling time separately and does not infer or verify Kaggle's
download time. The live source workflow still requires the original CSVs, the official
source URL, and the already-running HDFS/Spark prerequisites. Source profiling creates its
SQLite price-key index only below `WAREHOUSE_SCRATCH_DIR`, verifies that directory with an
owned write probe, and removes the run-owned temporary directory after profiling.

`scripts/ingest-m5-raw.sh` is retired so there is no second container lifecycle or ingestion
implementation. The `warehouse-bigdata ingest-m5-raw` command remains available for focused
tests and explicitly configured low-level callers; normal operation should use the combined
launcher.

The opt-in integration test requires explicit `WAREHOUSE_M5_SOURCE_DIR`,
`WAREHOUSE_M5_SOURCE_URL` values plus the already-running HDFS/Spark prerequisites. It checks the
stored source-manifest lineage, remote byte sizes and SHA-256 values, replication factor
2, and every block's locations on both current DataNodes before requesting the separate
Spark Standalone HDFS read. It leaves validated M5 raw snapshots in place and changes no
volumes or unrelated paths. This cluster is distributed across containers on one host;
it does not demonstrate physical multi-host fault tolerance.

For the live M03 acceptance, first run the launcher above and retain its
`artifacts/m5-ingestion/<pipeline_run_id>/` evidence. Then submit the existing Spark
verification job, using the returned actual raw HDFS URI and `source_version`:

```bash
bash scripts/spark-submit.sh -- \
  warehouse_bigdata/jobs/verify_m5_raw_hdfs.py \
  --raw-uri hdfs://hdfs-namenode:8020/data/warehouse/raw/m5/source_version=sha256-... \
  --source-version sha256:...
```

This uses the existing Spark submission image and Standalone topology and must read the
actual promoted HDFS path. Do not substitute a local path or start/reinitialize services
as part of the acceptance. A live run remains blocked until real M5 CSVs, healthy HDFS and
Spark services, the prepared Spark submission image, and explicit approval for the live
acceptance are available.

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
bash scripts/load-env.sh uv run --locked pytest -q tests/unit/test_hdfs_lifecycle.py
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

Default pytest selection and lightweight CI exclude live HDFS integration tests
through the registered `integration` marker. The complete suite is run manually
against Docker. These focused offline checks need no running services:

```bash
uv run --locked pytest -q tests/unit/test_hdfs_verification.py
uv run --locked ruff check tests/integration tests/unit/test_hdfs_verification.py
uv run --locked ruff format --check tests/integration tests/unit/test_hdfs_verification.py
```

Run the non-disruptive scenario explicitly from the repository root:

```bash
bash scripts/load-env.sh uv run --locked pytest -q -m integration tests/integration/test_hdfs.py
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
bash scripts/load-env.sh uv run --locked pytest -q -m integration tests/integration/test_hdfs.py --run-hdfs-disruptions
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
`artifacts/test-runs/<run-id>/`. Keep this generated evidence local and out of Git:

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

The user-reported complete suite was executed manually on **October 19, 2026**
against the local Docker-based Hadoop cluster with this exact command:

```bash
WAREHOUSE_HDFS_URI=hdfs://hdfs-namenode:8020 \
uv run --locked pytest -vv -m integration \
tests/integration/test_hdfs.py \
--run-hdfs-disruptions
```

| Pytest scenario | Reported result | Validated behavior |
|---|---|---|
| `test_independent_client_registration_replication_and_integrity` | **PASSED** | Independent-client connectivity, actual block replication on both distinct DataNodes, and byte-level integrity |
| `test_fixture_persists_after_daemon_restart` | **PASSED** | Fixture persistence, byte/hash integrity, persistent identities, and two-node block placement after daemon restart |
| `test_fixture_persists_after_forced_container_recreation` | **PASSED** | Fixture persistence and unchanged storage identities, block content, and replication after container recreation with named volumes preserved |
| `test_degraded_read_and_datanode_recovery` | **PASSED** | Byte-exact read during a controlled DataNode interruption, followed by restoration, renewed heartbeat/block-report evidence, and restored two-node placement |

**Reported pytest summary: 4 passed in 384.85 seconds.** The earlier non-disruptive
baseline (**1 passed, 3 skipped**) is historical and superseded by this complete run.
All three maintenance-window scenarios passed.

Reported companion verification:

- Default offline pytest suite: **225 passed, 4 deselected**. Live HDFS integration
  tests remain excluded from default pytest and CI selection.
- Ruff lint passed. Ruff formatting passed after formatting
  `tests/unit/test_hdfs_lifecycle.py`.
- GitHub Actions `python-quality` passed for PR #1 at commit
  `c78431f5f40a6c4406371d11adae2f43ab5a1a8c`.

These historical results establish the M01 local HDFS infrastructure scope. The
separately executed M02 Spark integration acceptance is recorded in the handoff
below; M5 ingestion and end-to-end data-pipeline acceptance remain unverified gates.
Carry a specific local evidence directory into a handoff only after verifying its
existence and identity.

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

## M02 connectivity handoff — distributed Spark–HDFS verification

M02 T03 adds one opt-in, non-disruptive scenario over the existing M01 HDFS harness,
M02 T01 Standalone cluster, and T02 attached submission interface. One live execution
**passed on 2026-10-21**: application `app-20261021012008-0000`, **1 passed in 309.02s**,
30 successful tasks across both Workers, complete event-log/oracle/Parquet evidence,
and HDFS preservation. The canonical
[M02 execution record](../spark/README.md#historical-phase-2-live-acceptance--2026-10-21)
references the retained evidence directory and its `phase2-review/report.md`, including
the non-blocking observer exit 137 and audited VERSION timestamp-comment differences.
A selected future scenario still fails if prerequisites or proof are unavailable;
default pytest and CI remain service-free.

### Prerequisites and commands for future runs

The commands below are reproducible procedures. Another live acceptance execution
requires separate explicit approval and must not be retried automatically.

Use serial pytest execution, reserve the existing HDFS maintenance lock, and avoid
concurrent external writers or lifecycle operations. No disruption flag is needed.
Require the initialized, write-ready NameNode and both current DataNodes on the
existing `warehouse-bigdata` bridge, plus the current Master and both one-core /
1024 MiB Workers. Their current IDs, running container/image identities, resources,
read-only XML mounts, and registration are inspected. Registration alone cannot
pass application acceptance.

Prepare T01/T02 through their existing interfaces before requesting the live test.
In particular, rebuild the prepared driver image after application source changes:

```bash
docker compose -f infrastructure/compose.yaml --profile spark-submit build spark-submit
bash scripts/spark-cluster.sh check
```

The test never builds images, starts services, repairs configuration, installs host
PySpark, changes resource caps, or substitutes a local Spark master. Both endpoints
must be supplied explicitly and must match the exact internal endpoints below.
The default `/data/warehouse` base path and any explicit override are recorded;
fixture paths are never derived from that setting. Both Hadoop XML files stay
unchanged and read-only.

Offline implementation gate:

```bash
uv run --locked pytest -q tests/unit/test_spark_verification.py tests/unit/test_hdfs_verification.py
```

Explicit live acceptance gate, from the repository root:

```bash
bash scripts/load-env.sh uv run --locked pytest -q -m integration tests/integration/test_spark_hdfs.py
```

### Fixture, workload, and required proof

Only an exclusively owned `/t03-integration-<uuid>/spark/` subtree is provisioned.
It is labelled **synthetic verification data** and contains four JSONL files of 256
rows each, with integer fields `record_id`, `bucket`, `quantity`, and
`unit_price_cents`. For record IDs 0–1023, the fixed arithmetic rules are:

- `bucket = record_id % 8`, `quantity = record_id % 5 + 1`;
- `unit_price_cents = 100 + 7 * (record_id % 17)`;
- keep `record_id % 3 != 0`; sum quantity and integer `quantity * unit_price_cents`
  and count records per bucket.

Inputs are readable but not writable by Spark. Only the fixture-scoped `work/`
and `events/` destinations are mode 777, accommodating Spark **185:185** and the
independent Hadoop client **1000:100**. Approved data paths and M01 storage volumes
are not modified. Storage identities and the harness's original payload are
checked again after the application.

`warehouse_bigdata/jobs/verify_spark_hdfs.py` is submitted through T02 with fixed
parallelism/shuffle partitions 2 and adaptive SQL disabled. Its common module
configures uncompressed, non-rolling HDFS event logging **before SparkContext
creation**, reads an explicit integer schema, performs DataFrame transformations,
writes Parquet with `errorifexists`, and uses a fresh uncached DataFrame to reread
it. The host independently checks actual rows, types, input/output counts, unique
keys, nulls, and exact integer aggregates: **682 contributing records, eight keys**.
The host Python oracle is independent of Spark; it is deliberately not the literal
local-Spark comparison described by DIST-08, because this task forbids local execution.

One diagnostic barrier stage consumes `input_df.repartition(2).rdd`. Its two
concurrent one-CPU tasks must use both one-core Workers; each consumes non-empty
fixture rows and exchanges partition/task-attempt/hostname evidence using
`allGather`. The driver and each task check NameNode RPC and both DataNode transfer
endpoints, then launch a fresh, bounded, 64 MiB-heap bundled Java Hadoop client to
read actual HDFS bytes. Their byte counts and SHA-256 must match the host-created
input. No scheduling sleeps or repeated attempts to obtain incidental placement
are used.

Success additionally requires:

- An active Master/Worker JSON snapshot matching the application, both current
  Worker IDs, and actual executor placements.
- A complete pinned Spark 3.5.7 event log with matching run/application identities,
  effective settings, application start/end, all three run-scoped phase groups,
  completed stages/jobs, and successful workload tasks on both Workers. Returned
  barrier task-attempt IDs must match successful tasks in the same two-task barrier
  stage on the expected executors. Failed/retried attempts remain separate.
- Fresh independent-client reads of every input and Parquet data file, matching
  sizes, and per-file `fsck` proof of non-zero blocks replicated on both current
  DataNode transfer addresses. Parquet semantics are compared with the oracle;
  physical output hashes are retained as evidence, not compared across runs.
- Confirmed T02 driver removal and absence of the application and its executors
  from current active state before fixture deletion, plus successful scoped cleanup.

### Bounds and evidence inspection

Submission is limited to **300 seconds**, then TERM gives T02 **45 seconds** for
its own cancellation/driver cleanup, within a **360-second host deadline**. The
coordinator does not remove drivers itself. Barrier admission uses five checks at
two-second intervals; synchronization is limited to 45 seconds. TCP requests are
limited to two seconds, JSON snapshots to ten seconds, each probe JVM to 20 seconds,
and termination confirmation to 30 seconds. Existing HDFS command/placement bounds
also apply. Event retrieval is bounded to 60 seconds and rejects empty, truncated,
in-progress, or **over-16-MiB** evidence; each data file is capped at 1 MiB.

Inspect the actual `artifacts/test-runs/<run-id>/` from pytest's evidence note:

- `spark-prerequisites.json`, `spark-submission.json`, `spark-submit-command.json`, and
  `spark-application-records.json`: container/prepared-image identities,
  configuration/command-source hashes, actual imported and baked source hashes,
  executor-archive member/hash evidence, XML hashes, and actual runtime versions.
  A stale driver image fails source comparison against the checkout.
- `spark-fixture.json` and `spark-correctness.json`: fixture provenance, fixed
  rules, independent expected values, and actual Parquet reread values.
- `spark-events.jsonl` and `spark-event-log.json`: independently retrieved raw
  events and their byte/hash identity. `spark-execution.json` correlates jobs,
  stages, successful probe tasks, per-Worker/executor task counts, input/shuffle
  bytes, execution/elapsed/GC time, and spill. Equal task counts are not required.
- `spark-data-files.json`, `spark-hdfs-preservation.json`, and
  `spark-termination.json`: independent content/replica evidence, unchanged HDFS
  identities/payload, and actual final application/driver state.
- Existing numbered raw `.stdout`/`.stderr`, `commands.jsonl`, `events.jsonl`, and
  `outcome.json`: exact commands and bounds, original failure, separate evidence or
  cleanup errors, retention reasons, and the exact remaining fixture path.

Missing evidence, incorrect data, timeout, and cleanup failure cannot produce a
passing outcome. Tooling/offline success does not establish live distributed
execution. This fixed single-host infrastructure fixture makes no M5 ingestion,
speedup, operational savings, HA, or physical multi-host fault-tolerance claim.

### Retained-fixture recovery

If application termination or evidence collection is unproven, `retain_fixture`
prevents deletion and records a **failed** outcome with the exact path and reason.
The original error is preserved separately from finalization/cleanup errors. The
temporary Hadoop client is still removed through its existing ownership checks.

1. Re-establish exclusive access and inspect the run's raw commands, structured
   startup identity, `spark-termination.json`, and `outcome.json`.
2. Confirm the exact `warehouse-spark-submit-<token>` driver is absent or stopped;
   if present, match its exact name and `io.warehouse-bigdata.spark-submit.owner`
   label to that token. Resolve cancellation/removal through T02's existing
   ownership-scoped interface. Inspect current Master/Worker JSON and establish
   that this run's application/executors are no longer active before deleting any
   possible write destination. Do not infer termination from an exited host command.
3. Retrieve complete event/data evidence with bounded fresh-client operations where
   possible. Preserve and report incomplete proof as a failed run.
4. Check the original successful exclusive `mkdir` evidence before removing only
   its exact `/t03-integration-<uuid>` directory using the bounded M01 recovery
   procedure above. A familiar prefix alone is not ownership. Never delete an
   ancestor, an approved analytical path, a wildcard, or a storage volume.
5. Record remaining blockers and use a new opt-in run after prerequisites are
   restored. Do not relabel the interrupted/retained outcome as passed.
