# Spark Standalone operations — M02 T01/T02

This deployment adds one Master and two explicitly configured Workers to the
existing `warehouse-bigdata` Compose project and bridge. It is opt-in through the
`spark` profile; default startup selects only the three HDFS daemons. T01 readiness
means two current, reachable Worker processes registered with the expected
resources. T02 adds a short-lived client-mode submission runtime, documented below.
Distributed application execution and Spark–HDFS data correctness require T03
acceptance; registration and submission packaging are not full NFR-02 evidence.

## Prerequisites and lifecycle

- Docker Compose v2 with CPU/memory enforcement and Linux tmpfs support.
- The approved startup budget is **10 GiB Docker-visible memory**: HDFS and Spark
  container limits total 7 GiB, with approximately 1.5 GiB reserved for one submission
  driver and 1.5 GiB for system overhead. This is a conservative planning budget,
  **not a measured minimum needed by the idle T01 daemons**. The wrappers enforce
  the planned budget and report actual capacity; they do not configure the host.
  Live acceptance observed **10.70 GiB** after the Docker/WSL allocation was
  increased. A separate two-minute idle measurement is recorded below.
- Existing initialized, healthy HDFS on `warehouse-bigdata`, including both
  DataNodes. Follow the [HDFS guide](../hadoop/README.md) for its own lifecycle.
- Registry and Python artifact access for the first image build.

Run from any working directory by addressing the wrapper through its repository
path. The following examples start from the repository root:

```bash
bash scripts/spark-cluster.sh up
bash scripts/spark-cluster.sh status
bash scripts/spark-cluster.sh check
bash scripts/smoke-spark.sh
bash scripts/spark-cluster.sh logs
bash scripts/spark-cluster.sh stop
```

`up` checks Docker memory and the existing HDFS cluster, builds the shared image,
targets the three Spark services with `--no-deps`, and waits up to 180 seconds for
genuine registration. Build/start has a separate 600-second ceiling. `stop` sends
graceful termination only to those three services with a 60-second grace period.
The containers, network, and HDFS storage are retained. Failures propagate their
exit code and print Spark-only state and the latest 100 log lines per service.
`status`, `logs`, and `check` also work when diagnosing a failed startup.

Master and Workers run foreground `spark-class` processes under `tini`, as
UID/GID **185:185**, with `on-failure:3`. Worker dependencies name only Master
health; Master health has no Worker dependency. The standalone submission REST
service is disabled.

## Endpoints and Hadoop configuration

| Endpoint | Address | Exposure |
|---|---|---|
| Master RPC | `spark://spark-master:7077` | Internal bridge only |
| Master UI | `http://127.0.0.1:8080` | Host loopback only |
| Worker 1 RPC / UI | `spark-worker-1:7078` / `http://spark-worker-1:8081` | Internal only |
| Worker 2 RPC / UI | `spark-worker-2:7078` / `http://spark-worker-2:8081` | Internal only |
| NameNode RPC | `hdfs-namenode:8020` | Existing internal endpoint |
| DataNode transfer | `hdfs-datanode-1:9866`, `hdfs-datanode-2:9866` | Existing internal endpoints |

Both existing Hadoop XML files are individually mounted read-only under
`/opt/hadoop/etc/hadoop`, with `HADOOP_CONF_DIR` pointing there. This preserves
hostname-based DataNode addressing. Clients require direct DataNode connectivity
in addition to NameNode RPC. The smoke check tests these paths from every Spark
service, plus Worker-to-Master and mutual Worker RPC reachability.

`WAREHOUSE_SPARK_MASTER_URL` remains an explicit Python configuration input.
`spark://spark-master:7077` requires Compose DNS and is not a host-side address.
The shared image sets `PYSPARK_PYTHON=/opt/python/bin/python3.11`. T02 retains that
executor interpreter and uses a non-editable project virtual environment for the driver.

## Runtime pins and qualification

| Component | Qualified value |
|---|---|
| Platform / base | `linux/amd64`, official `spark:3.5.7-scala2.12-java11-ubuntu` |
| Spark base source digest | `sha256:e119521e81658967567bfcb6f9c66d5a7349b6975669ad3af7ccd7b1b5759c47` |
| uv / source digest | `0.13.1`, `sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336` |
| Python | CPython **3.11.17**, python-build-standalone **20261013**, x86_64 GNU stripped install-only artifact |
| Python artifact SHA-256 | `3b8e5a248bcc351265ffe68f5e457668a2e09c99a47f81f658988c7d3d107522` |
| Java | Temurin **11.0.30+7** |
| Spark / bundled PySpark | **3.5.7** / **3.5.7** |
| Bundled Scala | **2.12.18** |
| Bundled Hadoop client / HDFS server | **3.3.4** / **3.4.3** |
| Shared local tag | `warehouse-bigdata-spark:3.5.7-python3.11.17` |
| Initial qualified local image ID | `sha256:742e85d71ffe3cf5f8b1386889ce9a0b8ea1f2076340fe3315bcc240f4af145c` |
| Live acceptance image ID, all three services | `sha256:e1868cd926b6ec1963e359455774d90e44a16ef49afcc33ee9b051911ec4a998` |

Artifact URL:
`https://releases.astral.sh/github/python-build-standalone/releases/download/20261013/cpython-3.11.17%2B20261013-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz`.
The Dockerfile verifies the explicit SHA-256 and then installs through pinned uv
using a local mirror and uv's own checksum verification. It keeps the bundled
Spark/PySpark and Hadoop jars; no application dependencies are installed.

Qualification executed on **2026-10-20**:

- Official Spark and uv candidate digests pulled successfully for amd64.
- Java/Spark/Scala/Hadoop version commands and Python/PySpark imports returned the
  values above. `Class.forName("org.apache.hadoop.hdfs.DistributedFileSystem")`
  loaded successfully with the bundled jars.
- A bounded, read-only `org.apache.hadoop.fs.FsShell -ls /`, using those jars and
  the existing mounted XML on the shared network, exited **0** against HDFS 3.4.3.
  The root listing was empty. This demonstrates metadata compatibility, not
  Spark application read/write correctness. Hadoop's same-major wire compatibility
  policy justifies retaining the bundled client rather than replacing jars.
- The derived image built successfully with explicit Python checksum verification.
  The qualified and subsequently deployed local image IDs are recorded above;
  rebuild IDs are local artifacts, not registry digests.

Specification boundaries remain explicit: `05_TECH_STACK.md` §12 lists a Hadoop
3.4.2 candidate, while this task uses the already deployed 3.4.3. The complete
integration chain in §12.2 extends beyond this infrastructure task.

## Resource, logging, and scratch policy

| Setting | Master | Each Worker |
|---|---|---|
| Docker CPU limit | 0.5 | 1 |
| Docker RAM limit | 512 MiB | 2 GiB |
| Advertised resources | Not an execution Worker | 1 core, 1,024 MiB |
| Daemon initial / maximum heap | 128 / 256 MiB | 128 / 256 MiB |
| `/tmp` tmpfs hard ceiling | 64 MiB | 256 MiB |
| Docker daemon logs | `json-file`, 10 MiB × 3 files | Same |

Container roots are read-only. Worker work files and application logs use
`/tmp/warehouse-spark/work`; local spill uses `/tmp/warehouse-spark/local`.
Other daemon logs/PIDs remain under `/tmp/warehouse-spark`, and JVM temporary
files use `/tmp`. Docker captures foreground daemon console output.

Tmpfs consumption is charged against the container RAM limit. Each Worker has
512 MiB beyond advertised application memory, maximum daemon heap, and maximum
scratch for Python/native/process overhead. Stopped-application cleanup runs
every **300 seconds**, retaining files for **3,600 seconds**; it supplements the
hard scratch ceiling. This is a small-workload infrastructure baseline. Workload
overhead and full-dataset spill capacity still require later measurement.

The smoke check inspects actual Docker limits, cgroup limits, JVM flags, mounted
scratch capacity/usage, log rotation, image consistency, loopback binding, and
read-only Hadoop mounts. It submits no applications.

### Measured T01 daemon memory (idle, 2026-10-20)

With all three HDFS daemons and all three Spark daemons running and healthy, Docker
reported **11,485,257,728 bytes (10.70 GiB)** available to the engine. Sixteen
`docker stats` observations over 120 seconds measured combined HDFS+Spark daemon
usage between **1,212.5 and 1,227.3 MiB** (mean **1,215.1 MiB**, about **1.19 GiB**).
Docker's displayed Linux usage excludes some file-cache charge; the cgroup readings
below include memory charged to each container.

| Daemon | cgroup `memory.current` at sample end | cgroup `memory.peak` since container start |
|---|---:|---:|
| HDFS NameNode | 253.2 MiB | 255.5 MiB |
| HDFS DataNode 1 | 232.1 MiB | 235.1 MiB |
| HDFS DataNode 2 | 228.2 MiB | 231.4 MiB |
| Spark Master | 193.5 MiB | 303.6 MiB |
| Spark Worker 1 | 194.3 MiB | 220.5 MiB |
| Spark Worker 2 | 186.4 MiB | 201.5 MiB |
| **Combined current** | **1,287.7 MiB (1.26 GiB)** | — |

The sum of the six independently recorded per-container high-water marks is
**1.41 GiB**; their individual peaks are not necessarily simultaneous, so this is
an upper bound rather than an observed aggregate peak. Across the two-minute
sample, WSL guest `MemAvailable` was at least **7.69 GiB** (mean 7.71 GiB),
`MemFree` never fell below **6.49 GiB**, and `SwapFree` remained **3.00 GiB**.
All six container cgroups reported zero OOM events/kills; Docker reported zero
restarts and no OOM-killed containers.

**Interpretation:** the configured 10 GiB budget is ample for the measured
HDFS+Spark daemon startup and idle-ready operation; it is not evidence that these
daemons consume or intrinsically require 10 GiB. In this sample, current cgroup
usage was about **12%** of Docker-visible memory, and the sum of per-container
historical peaks was about **13%**. The 7 GiB total container limit is a ceiling,
not observed consumption. This measurement included service startup/recovery
high-water marks for the current containers plus two minutes of idle-ready
sampling, but **no Spark application, driver, executor task, ETL, or M5 workload**.
Therefore it does not establish memory sufficiency for future Spark jobs or the
minimum Docker/WSL allocation at which the topology can start reliably. Those
require a separately scoped, bounded workload benchmark; no such result is claimed
for T01.

## Health and readiness semantics

`healthcheck.py` uses only Python's standard library and direct bridge HTTP/TCP
requests (without HTTP proxies). Individual I/O has a one-second timeout and
JSON responses have a 1 MiB size ceiling. One-shot probes share a six-second
monotonic deadline, with Docker's eight-second healthcheck timeout as an outer
bound. `wait-ready [seconds]` uses one monotonic deadline, defaults to 180 seconds,
and accepts only 1–600. The wrapper adds an outer timeout.

- `master`: RPC listener, canonical Master URL, and `ALIVE` status only.
- `worker`: its current local UI ID must match one fresh `ALIVE` registration at
  the Master with the configured host, RPC/UI address, cores, and memory.
- `ready`: exactly the two expected live Workers, distinct current IDs, matching
  Master totals, and independently reachable Worker UIs/RPC listeners.
- Heartbeats may be at most 30 seconds old (five seconds of future-clock tolerance).
  Retained `DEAD` entries are ignored. Stale, duplicate, missing, or malformed
  registrations and resource mismatches fail readiness.

Stopping a Worker must fail readiness immediately through its independent
endpoint, even before the Master expires its cached registration.

## Short-lived submission runtime — M02 T02

The `spark-submit` profile selects a separate, ephemeral driver service. It has no
dependencies and publishes no ports. T01 still owns Master/Worker startup and
shutdown; M01 still owns HDFS initialization, storage, and lifecycle. Prepare those
services through their existing guides before submitting an application.

### Explicit image preparation and invocation

The qualified T01 image `warehouse-bigdata-spark:3.5.7-python3.11.17` must already be
available locally. Build **only the driver application layer**, from the repository root:

```bash
docker compose -f infrastructure/compose.yaml --profile spark-submit build spark-submit
```

This derives `warehouse-bigdata-spark-submit:3.5.7-python3.11.17` from T01 without
changing its Dockerfile or bundled Spark/PySpark, Java, Scala, Hadoop client, or
Python. The repository-context allowlist includes packaging metadata, the locked
dependencies, `src/`, and the ZIP helper. During the build, `uv sync --locked
--no-dev --no-editable --python /opt/python/bin/python3.11` installs the project in
`/opt/warehouse-app/.venv`. A deterministic ZIP of the same copied source tree is
stored at `/opt/warehouse-app/warehouse_bigdata.zip` for `--py-files`. Its entries
start with `warehouse_bigdata/`; bytecode and caches are excluded.

Builds may need dependency-cache/network access. `--locked` preserves the project
resolution, but does not independently pin the ranged `uv_build` build backend.
There is no installation during submission and no host-source mount. **Rebuild the
driver image explicitly after application/package changes or a T01 base rebuild**
so installed driver code and shipped executor imports stay consistent.

Host API, callable through the script's repository path from any working directory:

```text
bash scripts/spark-submit.sh [--conf KEY=VALUE ...] -- APPLICATION.py [APPLICATION_ARGS...]
```

Container API: `warehouse-bigdata spark-submit` with the same argument contract.
The image entrypoint already supplies that subcommand under `tini -g`. Application
paths are relative to `/opt/warehouse-app/src`; they must be existing, contained
`.py` files. Absolute paths, escaping symlinks/paths, and missing files fail preflight.
Preflight also checks the project ZIP and both Hadoop XML files, including matching
`fs.defaultFS` and hostname-based DataNode addressing. Configuration parsing itself
does not perform I/O.

Illustrative invocation **only after this future job has been implemented and the
driver image rebuilt**; this task adds no production job placeholders:

```bash
bash scripts/spark-submit.sh --conf spark.sql.shuffle.partitions=4 \
  -- warehouse_bigdata/jobs/my_job.py --run-id demo-001
```

The `--` before the application is mandatory. Spark options go before the
application; everything after the application, including spaces, Unicode, leading
dashes, additional `--conf`, and shell metacharacters, is forwarded as individual
arguments without shell evaluation. Quote arguments normally in the invoking shell.

### Configuration, networking, and limits

Compose explicitly supplies `WAREHOUSE_SPARK_MASTER_URL=spark://spark-master:7077`,
`WAREHOUSE_HDFS_URI=hdfs://hdfs-namenode:8020`, and
`WAREHOUSE_HDFS_BASE_PATH=/data/warehouse`. Submission requires Standalone and
always selects `--deploy-mode client`; there is no local fallback. The generic
shared Spark configuration loader still supports explicit local modes for other
callers.

Both Hadoop XML mounts and `HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop` are reused
unchanged. Submission sets `spark.hadoop.fs.defaultFS`, retains hostname-based
DataNode access, and sends the HDFS URI/base path to executor environments. Driver,
Workers, NameNode, and DataNodes share the existing `warehouse-bigdata` bridge.

The launcher creates a unique DNS-safe driver container name and injects it through
the internal `_WAREHOUSE_SPARK_DRIVER_HOST` setting. It uses no shared service
aliases. The driver advertises that name, binds `0.0.0.0`, and uses internal RPC
**7079** and driver block-manager **7080**, with port retries disabled. Driver UI
is disabled. These addresses require bridge DNS and are not host-side endpoints.

| Setting | Driver | Executors |
|---|---|---|
| Interpreter | `/opt/warehouse-app/.venv/bin/python` | `/opt/python/bin/python3.11` |
| Container CPU/RAM | 1 CPU / 1.5 GiB | Existing T01 Worker limits |
| JVM heap | 512 MiB | 512 MiB each |
| Application core cap | Docker CPU limit | 1 core each, 2 total |
| Scratch | `/tmp` tmpfs, 128 MiB | Existing 256 MiB Worker tmpfs |
| Dynamic allocation | Disabled | Disabled |

The driver uses UID/GID **185:185**, a read-only root, `restart: "no"`, and rotating
`json-file` logs (10 MiB × 3). Tmpfs is charged to the RAM limit. These are
conservative initial bounds, not measured workload capacity; the budget covers
**one active driver**, even though invocation names do not collide.

Repeated `--conf KEY=VALUE` supports task-specific `spark.sql.*` settings and a
positive `spark.default.parallelism`. Keys must be well formed and unique, and
values must be nonempty and single-line. Runtime deployment, networking, Python,
dependencies, HDFS, and resource overrides are rejected. Reserved SQL settings
include `spark.sql.warehouse.dir`, `spark.sql.extensions`,
`spark.sql.catalogImplementation`, `spark.sql.defaultCatalog`, `spark.sql.catalog.*`,
and `spark.sql.hive.*`. No public resource environment-variable inventory is added.

### Lifecycle, failures, and cancellation

The launcher first checks for the prepared driver image, then reuses T01's
read-only `spark-cluster.sh check`. It runs attached with
`docker compose run --rm --no-deps -T --pull never`; it does not prepare images or
start services implicitly. Output is inherited. The installed CLI replaces itself
with `/opt/spark/bin/spark-submit` via `os.execvpe`, preserving submission/application
failure status.

- Usage/configuration/preflight errors: **2**.
- Missing executable: **127**; other executable replacement errors: **126**.
- Docker/image/readiness failures and application exit codes: propagated unchanged.
- Cancellation: **130** for interrupt, **143** for termination.

Cancellation terminates the attached client within a finite bound, then checks up
to five times for the invocation-owned container, including a late create request.
Diagnostics, graceful stop (10-second grace), and forced removal require both the
unique name and matching ownership label/token. Each Docker cleanup command has a
timeout; cleanup/diagnostic failures do not replace the primary status. Other
drivers, Spark daemons, HDFS containers, named volumes, and the bridge are never
cleanup targets. There is no job-duration timeout imposed by this wrapper.

### Focused T02 verification

Offline tests and static checks:

```bash
uv run --locked pytest -q tests/test_cli.py tests/unit/test_spark_submit.py tests/unit/test_spark_submit_runtime.py
bash -n scripts/spark-submit.sh
uv run --locked ruff check src/warehouse_bigdata/cli.py src/warehouse_bigdata/common/spark_submit.py scripts/package-spark-app.py tests/test_cli.py tests/unit/test_spark_submit.py tests/unit/test_spark_submit_runtime.py
uv run --locked ruff format --check src/warehouse_bigdata/cli.py src/warehouse_bigdata/common/spark_submit.py scripts/package-spark-app.py tests/test_cli.py tests/unit/test_spark_submit.py tests/unit/test_spark_submit_runtime.py
docker compose -f infrastructure/compose.yaml config --quiet
docker compose -f infrastructure/compose.yaml --profile spark config --quiet
docker compose -f infrastructure/compose.yaml --profile spark-submit config --quiet
```

Daemon-free rendering must preserve the complete pre-task default/`spark`/`init`
models, project identity, volumes, and network. Default selection remains three
HDFS daemons; `spark` adds only Master/two Workers; `spark-submit` adds only the
ephemeral submission service. Its dependencies and published ports must be empty.

A driver-image-only build and a bounded, network-disabled packaging probe are
justified to verify the non-editable installation, installed metadata, driver and
executor interpreter selection, ZIP imports outside the checkout, and CLI help.
They do not start cluster services or a Spark application. Distributed executor
placement, HDFS read/write workloads, M5 ETL, and MLlib implementation remain
outside T02 acceptance.

## T01 focused verification and live acceptance

Offline checks (no Docker daemon or network needed for pytest):

```bash
uv run --locked pytest -q tests/unit/test_spark_healthcheck.py tests/unit/test_spark_lifecycle.py tests/unit/test_hdfs_lifecycle.py
bash -n scripts/spark-cluster.sh scripts/smoke-spark.sh
uv run --locked ruff check infrastructure/spark/healthcheck.py tests/unit/test_spark_healthcheck.py tests/unit/test_spark_lifecycle.py
uv run --locked ruff format --check infrastructure/spark/healthcheck.py tests/unit/test_spark_healthcheck.py tests/unit/test_spark_lifecycle.py
docker compose -f infrastructure/compose.yaml config --quiet
docker compose -f infrastructure/compose.yaml --profile spark config --quiet
```

Default `config --services` must select exactly the existing three HDFS daemons;
`--profile spark config --services` must add exactly the three Spark services.
The HDFS definitions, initializer, network, and named volumes must match the
pre-task Compose model.

After the memory prerequisite is resolved, in one Bash session at the repository
root, compare HDFS identities and attachments around the approved Spark cycle:

```bash
compose=(docker compose -f "$PWD/infrastructure/compose.yaml")
hdfs=(hdfs-namenode hdfs-datanode-1 hdfs-datanode-2)
mapfile -t ids < <("${compose[@]}" ps --quiet "${hdfs[@]}")
[[ ${#ids[@]} == 3 ]]
before=$(docker inspect --format '{{.Id}} {{json .Mounts}}' "${ids[@]}")
bash scripts/spark-cluster.sh up
bash scripts/smoke-spark.sh
bash scripts/spark-cluster.sh stop
bash scripts/spark-cluster.sh up
bash scripts/smoke-spark.sh
mapfile -t ids < <("${compose[@]}" ps --quiet "${hdfs[@]}")
after=$(docker inspect --format '{{.Id}} {{json .Mounts}}' "${ids[@]}")
[[ "$before" == "$after" ]]
```

Then stop only Worker 2, require readiness failure, restore it, and wait for
readiness recovery. Capture the Master JSON immediately after the stop to show
any still-cached registration; the independent Worker probe must still fail:

```bash
"${compose[@]}" --profile spark stop --timeout 60 spark-worker-2
"${compose[@]}" --profile spark exec -T spark-master /opt/python/bin/python3.11 -c \
  'import urllib.request; print(urllib.request.urlopen("http://spark-master:8080/json/", timeout=2).read().decode())'
if bash scripts/spark-cluster.sh check; then
    printf 'Unexpected readiness with Worker 2 stopped.\n' >&2
    exit 1
fi
"${compose[@]}" --profile spark up --detach --no-deps spark-worker-2
"${compose[@]}" --profile spark exec -T spark-master /opt/python/bin/python3.11 \
  /opt/warehouse-spark/healthcheck.py wait-ready 180
```

### Execution record

Focused verification on **2026-10-20**:

- The exact three-file pytest command above passed: **97 passed in 3.31 seconds**.
- Bash syntax checks passed for both wrappers.
- Focused Ruff lint and format checks passed for all three new Python files after
  one corrective pass for a Python 3.11 timeout alias and test formatting.
- Default and `spark` Compose model validation passed: **3** default HDFS services
  and **6** services with Spark. Comparison with the pre-task model confirmed
  identical complete default/init models, project identity, network, and volumes.
- The final derived image built and runtime qualification passed as recorded above,
  including UID/GID 185:185, Python/PySpark imports, version commands, and read-only
  HDFS metadata access using the final image.

The initial Docker memory prerequisite blocker was resolved on **2026-10-20**.
Docker then reported **11,485,257,728 bytes (10.70 GiB; 10,953 whole MiB)**; actual
idle daemon resource measurements are recorded in the section above.
The following planned live checks were executed successfully:

| Acceptance check | Actual result |
|---|---|
| `up` → smoke → `stop` → `up` → smoke | **Passed**, 59.89 seconds for the complete cycle; both smoke checks passed |
| Master registration | `ALIVE`, canonical URL, exactly two `ALIVE` Workers; 2 cores and 2,048 MiB total advertised memory |
| Worker resources/current IDs | Each independently reachable Worker matched its current local UI ID and advertised hostname, RPC port 7078, 1 core, and 1,024 MiB |
| Docker/cgroup limits | Master: 0.5 CPU / 536,870,912 bytes; each Worker: 1 CPU / 2,147,483,648 bytes |
| Actual daemon JVM flags | All three used `-Xms128m`, `-Xmx256m`, and `-Djava.io.tmpdir=/tmp`; REST disablement and Worker cleanup flags matched configuration |
| Tmpfs capacity/usage | Master: 67,108,864-byte cap; each Worker: 268,435,456-byte cap; each used 32,768 bytes at both smoke snapshots |
| Runtime/logging/storage inspection | All three shared one image per smoke snapshot, UID/GID 185:185, read-only roots and Hadoop binds, and rotating `json-file` logs (10 MiB × 3) |
| Published endpoints | Only the Master UI published, on `127.0.0.1:8080`; Master/Worker RPC and Worker UIs remained internal |
| Bridge connectivity | All three Spark services reached Master RPC, both Worker RPC listeners, NameNode RPC, and both DataNode transfer ports |
| Hadoop metadata access | Bundled client `FsShell -ls /` exited 0 during both smoke checks; no data writes |
| Worker 2 interruption/recovery | **Passed**, 8.33 seconds including inspection and restoration; readiness exited 1 while stopped, then recovered within the 180-second deadline |
| Final deployment state | Master and both Workers running and Docker-healthcheck `healthy`; all three use the live acceptance image ID above |

The Master retained Worker 2's previous registration as **`DEAD`** immediately
after its graceful stop. Readiness correctly rejected the single-live-Worker
state with `Expected exactly two ALIVE Workers`, despite that retained entry.
Restoration produced a new current Worker 2 ID and returned to two live Workers:

| Worker | Current registered ID after recovery |
|---|---|
| `spark-worker-1` | `worker-20261020035126-spark-worker-1-7078` |
| `spark-worker-2` | `worker-20261020035201-spark-worker-2-7078` |

HDFS container IDs, start times, restart counts, and all mount attachments were
compared before/after the Spark lifecycle cycle and again around Worker 2
interruption/recovery; all matched. The preserved persistent attachments were:

| HDFS service | Container ID prefix | Named volume |
|---|---|---|
| `hdfs-namenode` | `8592a43e2772` | `warehouse-bigdata_hdfs-namenode-data` |
| `hdfs-datanode-1` | `73b10875bed2` | `warehouse-bigdata_hdfs-datanode-1-data` |
| `hdfs-datanode-2` | `af32787e83ab` | `warehouse-bigdata_hdfs-datanode-2-data` |

T01 infrastructure acceptance **passed** with no remaining deployment blocker.
These idle-daemon/scratch observations do not measure application workload
overhead or full-dataset capacity. No applications were submitted; distributed
executor-task evidence, Spark–HDFS data read/write correctness, and M5 processing
remain outside this task.
