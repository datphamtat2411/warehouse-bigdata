# Spark Standalone operations — M02 T01/T02/T03

This deployment adds one Master and two explicitly configured Workers to the
existing `warehouse-bigdata` Compose project and bridge. It is opt-in through the
`spark` profile; default startup selects only the three HDFS daemons. T01 readiness
means two current, reachable Worker processes registered with the expected
resources. T02 adds a short-lived client-mode submission runtime, documented below.
T03's bounded synthetic distributed Spark–HDFS acceptance **passed on 2026-10-21**;
the [historical execution record](#historical-phase-2-live-acceptance--2026-10-21)
contains the application, task, correctness, and preservation evidence. Registration
and submission packaging alone are not distributed application acceptance, and this
fixture does not establish full-project NFR-02 or M5 processing acceptance.

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
| Bulk `/tmp` tmpfs hard ceiling (`noexec`) | 64 MiB | 224 MiB |
| Private executable JVM-temp tmpfs | None | 32 MiB |
| Aggregate tmpfs ceiling | 64 MiB | 256 MiB |
| Docker daemon logs | `json-file`, 10 MiB × 3 files | Same |

Container roots are read-only. Worker work files and application logs use
`/tmp/warehouse-spark/work`; local spill uses `/tmp/warehouse-spark/local`.
Other daemon logs/PIDs remain under `/tmp/warehouse-spark`. The Master uses `/tmp`
for JVM temporary files; Workers and the ephemeral driver use the private
`/opt/warehouse-spark/native` mount described below. Docker captures foreground
daemon console output.

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

### Readiness command bounds and failure interpretation

All three wrapper readiness calls use Compose `exec -T --interactive=false` with
STDIN redirected from `/dev/null`: the HDFS prerequisite and Spark registration
probe during `up`, and the Spark `ready` probe during `check`. They receive immediate
EOF rather than waiting for or consuming an invoking terminal or open pipe.
STDIN redirection is scoped to those readiness calls.

The host wrapper uses GNU `timeout` with its normal process-group behavior and
`--kill-after=2s`. A command that outlives its TERM deadline receives KILL after
another two seconds, including invocation-owned descendants in that process group:

| Readiness call | In-container deadline | Host TERM deadline | KILL grace | Host bound |
|---|---|---|---|---|
| `up`: HDFS `wait-ready 30` | 30s | 45s | 2s | 47s |
| `up`: Spark `wait-ready 180` | 180s | 195s | 2s | 197s |
| `check`: Spark `ready` | 6s | 15s | 2s | 17s |

On failure, Spark-only `ps --all` and `logs --tail 100` diagnostics each receive
**12s TERM + 2s KILL grace**. The original failing status is saved before diagnostics
and the `ERR` trap is disabled to prevent re-entry. Diagnostic failure or timeout
does not replace that status. A failed `check` therefore has a configured bound of
**17 + 14 + 14 = 45 seconds**, plus normal shell overhead, leaving approximately
15 seconds under T03's existing 60-second caller deadline. Startup readiness bounds
exclude the independent build/start stage and any subsequent failure diagnostics.

A printed success marker is not proof of completion: readiness requires the command
to exit **0**. Native failures propagate unchanged; GNU timeout normally returns
**124** when its deadline expires and may produce **137** when KILL escalation is
required. These host-command bounds retain the substantive readiness checks above:
exactly two current, independently reachable Workers with matching registrations
and resources.

The historical intermittent Compose `exec` hang was **not reproduced** in the
bounded diagnostic probes on **2026-10-20**; its root cause remains **unproven**.
A separate synthetic command that printed success-like output and ignored TERM
confirmed that TERM-only timeout could continue waiting; adding KILL grace bounded
that command. STDIN isolation and forced-kill grace are defensive mitigations,
not a confirmed cure for the historical Compose incident.

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
changing its Dockerfile or bundled Spark/PySpark, Java, Scala, Hadoop client JARs, or
Python. The driver additionally includes the official **HDFS CLI 3.4.3**, copied
from the same digest-pinned Apache Hadoop image used by the HDFS services. Only its
`hdfs` launcher, shell support/configuration, and common/HDFS libraries are copied;
YARN, MapReduce, and native libraries are excluded. `/opt/hadoop/bin` is on the
driver PATH, with `HADOOP_HOME=/opt/hadoop` and a 256 MiB maximum CLI JVM heap.
The shell-required `HADOOP_YARN_HOME` and `HADOOP_MAPRED_HOME` also point to
`/opt/hadoop`; their component libraries are not installed. Driver-only Log4j
configuration sends Hadoop diagnostics to stderr so metadata/byte stdout remains
machine-readable.
The CLI uses its own `/opt/hadoop/share/hadoop` libraries and the existing XML mounts;
Spark retains its separate bundled Hadoop 3.3.4 classpath. No CLI installation is
needed on the Master or Workers.

M04's driver-side `HdfsCliClient` requires `hdfs dfs`, `getconf`, `dfsadmin`, and
`fsck`, including ACL/write preflight before SparkContext creation. Bundled Spark
Hadoop JARs alone do not provide this CLI. Rebuild the driver image with the command
above before rerunning M04 after this runtime change.

J02/J03 use `SparkHdfsCliClient`: file operations, safe-mode reads, checksums, and
file-specific `fsck` remain CLI-backed. Live DataNode registrations/transfer addresses
come from the same NameNode's read-only `NameNodeInfo` JMX endpoint, with bounded
responses and a five-second I/O timeout. The adapter requires both expected live
DataNodes in normal service and distinct transfer addresses. Standard
`dfsadmin -report` requires superuser privileges for its DataNode report; the driver
continues to run as `spark`, while owner-side verification retains DFSAdmin checks.

The repository-context allowlist includes packaging metadata, the locked
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

Host-side wrappers load the repository `.env` through `scripts/load-env.sh`, but that
file does not replace the explicit container environment above or alter the existing
`warehouse-bigdata` bridge. Keep host/integration values in `.env`; keep the
Docker-internal HDFS and Spark endpoints in the Compose service definition.

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
| Bulk scratch (`noexec`) | `/tmp` tmpfs, 96 MiB | `/tmp` tmpfs, 224 MiB per Worker |
| Executable JVM temp | Private 32 MiB tmpfs | Private 32 MiB tmpfs per Worker |
| Aggregate tmpfs ceiling | 128 MiB | 256 MiB per Worker |
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

## M5 raw HDFS read verification

`warehouse_bigdata/jobs/verify_m5_raw_hdfs.py` is a minimal, opt-in Spark Standalone
application for a validated M5 raw snapshot. It accepts an explicit HDFS raw URI and
the exact content-derived `source_version`, checks the effective Hadoop endpoint,
replication, and hostname-based DataNode setting, then reads the stored source manifest
and sales header from HDFS through Spark. It rejects `file://`, relative, and
wrong-endpoint paths before importing PySpark; there is no local fallback or Spark
transformation.

After `ingest-m5` has published a raw snapshot, rebuild the prepared driver image when
application sources changed and submit only after the existing HDFS and Spark services
are ready:

```bash
bash scripts/spark-submit.sh -- \
  warehouse_bigdata/jobs/verify_m5_raw_hdfs.py \
  --raw-uri hdfs://hdfs-namenode:8020/data/warehouse/raw/m5/source_version=sha256-... \
  --source-version sha256:...
```

The job emits one `WAREHOUSE_M5_RAW` evidence record and does not create or delete
approved/raw data. The real-source integration command also verifies independent HDFS
byte/hash evidence and both-DataNode placement before this Spark submission:

```bash
bash scripts/load-env.sh uv run --locked pytest -q -m integration tests/integration/test_m5_ingestion.py
```

This is a non-disruptive check against already running services. The local Compose
deployment demonstrates distributed software roles on one physical host, not physical
multi-host fault tolerance or a full M5 ETL workload.

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

This is the historical **T01** record from **2026-10-20**, before the ZSTD JVM-temp
partition below. Its Worker IDs, JVM flags, and scratch observations describe that
execution, rather than the later T03 deployment policy.

Focused verification on that date:

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

| Worker | Historical registered ID after T01 recovery |
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

T01 infrastructure acceptance **passed** with no remaining deployment blocker at
that checkpoint. These idle-daemon/scratch observations do not measure application
workload overhead or full-dataset capacity. No applications were submitted during
the T01 checks; the later T03 record below supplies distributed executor-task and
Spark–HDFS read/write evidence. M5 processing remains outside M02 acceptance.

## M02 T03 — completed rollout and distributed acceptance

The Spark-only rollout was completed on **2026-10-20** and its historical evidence
was accepted as **`PHASE_1_VERIFIED`**. One separately authorized live T03 execution
passed on **2026-10-21**. The dated records below establish the completed M02 scope;
the following verification and maintenance procedures are for future approved runs.

### Evidence and selected runtime policy

T03 application `app-20261020140926-0001` completed shuffle-map tasks on both
Workers, then failed in driver map-output serialization with
`UnsatisfiedLinkError: /tmp/libzstd-jni-*.so: failed to map segment from shared object`.
The subsequent RPC timeouts exhausted the unchanged 300-second submission deadline.
The incident is retained under
`artifacts/test-runs/t03-20261020T140712-test_distributed_spark_hdfs_round_trip-e96e8aada0cb4277a9dea044705cddb6/`;
its incomplete event log and missing Parquet output are **failed T03 evidence**.
Keep that directory and HDFS fixture
`/t03-integration-c68b2620714846c8b03113796281df90` intact.

Bounded PLAN diagnostics on the existing Worker and driver images reproduced the
failure with `/tmp` mounted `noexec`. The same bundled Spark ZSTD codec successfully
compressed and decompressed bytes when JNI extraction used executable storage.
The selected private-directory probes also loaded Snappy, LZ4 JNI, Netty epoll,
and RocksDB. Executable native mappings were observed under the private directory;
all tested native libraries used **14,684,160 bytes**, with ZSTD alone about 1 MiB.
These measurements justify this focused partition, not arbitrary concurrent JVM
capacity or distributed workload acceptance.

The Compose-only fix partitions the existing tmpfs budgets:

| Runtime | `/tmp` (`rw,nosuid,nodev,noexec`, mode `1777`) | Private JVM temp | Total |
|---|---:|---:|---:|
| Master | 64 MiB | None | 64 MiB |
| Each Worker | 224 MiB | 32 MiB | 256 MiB |
| Ephemeral driver | 96 MiB | 32 MiB | 128 MiB |

`/opt/warehouse-spark/native` uses
`rw,exec,nosuid,nodev,size=33554432,uid=185,gid=185,mode=0700`.
Workers and the driver inherit
`JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=/opt/warehouse-spark/native`.
Workers also explicitly set
`SPARK_DAEMON_JAVA_OPTS=-Xms128m -Djava.io.tmpdir=/opt/warehouse-spark/native`:
the image's explicit `-Djava.io.tmpdir=/tmp` overrides `JAVA_TOOL_OPTIONS` unless
corrected. The regression exercises that precedence failure with a real JVM.
Master options, Spark work/spill/log/PID locations, heaps, cgroups, versions,
compression, driver lifetime, and submission deadlines retain their existing policy.
**No image rebuild is required**; the diagnostic helper is streamed over STDIN.

Making all `/tmp` executable also worked, but would expose the whole writable Spark
scratch area to executable mappings. The selected mount keeps bulk scratch
`noexec`, confines writable executable storage to a bounded, private directory,
and uses the JVM's existing extraction behavior across tested JNI libraries.
It costs 32 MiB of bulk scratch per Worker/driver and adds mount/environment
configuration. `java.io.tmpdir` routes **general JVM temporary files too**; this is
not a native-only directory. Mode `0700` excludes other identities, not untrusted
code running as the same UID 185. Tmpfs remains charged to unchanged container RAM
limits. The historical Phase 2 record below validates actual executor inheritance
and observes the reduced scratch partition under the bounded synthetic workload;
full-dataset capacity remains unmeasured.

### Historical Phase 1 rollout — 2026-10-20

Session **`zstd-phase1-20261020T160900Z-402c3853`** ran from approximately
16:09:26 to 16:14:55 UTC. Its **41 recorded commands exited 0**, with no supervisor
timeout, across preflight, baseline, rollout, smoke, driver JNI, and finish phases.
The subsequent historical review accepted this evidence as `PHASE_1_VERIFIED`.

Evidence root:
`artifacts/test-runs/zstd-rollout-20261020T160926Z-3ab814dc/`.
Use its `commands.jsonl`, `session-manifest.json`, raw command outputs,
`phase1-status.json`, and `phase1-report.json` together; phase labels alone are
insufficient evidence.

| Gate | Recorded result |
|---|---|
| Preflight/idle | Five-profile Compose preservation, approved local image/platform/resource checks, 10 GiB memory prerequisite, HDFS/Spark readiness, and no active application/driver; idle checked again immediately before recreation |
| Scoped recreation | Command 0020 recreated exactly `spark-master`, `spark-worker-1`, and `spark-worker-2`, using `--no-deps --no-build --pull never --force-recreate`; all three received new container IDs on the unchanged runtime image |
| Readiness/smoke | Both current Workers registered; infrastructure/resource/mount/JVM/connectivity checks passed |
| JNI | Both Workers and the ephemeral driver completed real Spark ZSTD codec round trips, Java exit 0, `Native.isLoaded()=true`, and executable mappings under `/opt/warehouse-spark/native` |
| Preservation | `hdfs-before.json`, `hdfs-after.json`, and `hdfs-after-jni.json` are byte-identical: three HDFS identities/volumes/attachments, storage VERSION hashes, the retained fixture listing/seven checksums, and 137 incident-artifact fingerprints |

Worker JNI output is in `0029-worker-smoke.stdout`; driver output is in
`0030-driver-native.stdout` and `driver-native.json`. This rollout submitted no
T03 application. The earlier failed preflight remains preserved in
`failed-session-archive/`; its status was not converted into a successful run.

### Historical Phase 2 live acceptance — 2026-10-21

One live execution used implementation commit
**`e210f603e835b025c7e65962a5fe8cf32665b0a1`**, branch
`feat/spark-infrastructure`, and the existing locked environment. The actual command
was:

```bash
uv run --locked pytest -q -m integration tests/integration/test_spark_hdfs.py::test_distributed_spark_hdfs_round_trip
```

It exited **0**, reporting **`1 passed in 309.02s`**. The process monotonic duration
was **309.455 seconds**. Recorded UTC endpoints were 01:17:46.080491–01:23:02.759518,
which span **316.679 seconds**; that wall-clock/monotonic difference is retained
explicitly, without assigning an unverified cause. The Spark application event
timestamps span **50.100 seconds**, distinct from total fixture/setup/verification
time. No retry or additional live test was executed.

Evidence root:
`artifacts/test-runs/t03-20261021T011746-test_distributed_spark_hdfs_round_trip-9862ce26e9ae4476b336f906c58f0c47/`.
The final report is `phase2-review/report.md` beneath that root. Evidence is local,
Git-ignored generated material and must remain available for review.

| Acceptance check | Actual result |
|---|---|
| Application/driver | `app-20261021012008-0000` on `spark://spark-master:7077`; driver `warehouse-spark-submit-be30544474c47cc8d10f194914a862bf` |
| Distributed work | **30 successful tasks**, **10 successful jobs**, **19 completed stages**, zero failed attempts; 18 shuffle-map tasks and 12 result tasks |
| Both current Workers | Worker 1: executor 0, 15 successful tasks; Worker 2: executor 1, 15 successful tasks. Registrations were `worker-20261021010618-spark-worker-1-7078` and `worker-20261021010618-spark-worker-2-7078` |
| Barrier correlation | Stage 3: task 5/partition 1 on Worker 1 and task 6/partition 0 on Worker 2, 512 rows each, agreeing allGather identities and matching successful event-log tasks |
| Shuffle | **16,254 bytes written**, **8,124 remote bytes read**, **8,130 local bytes read** |
| HDFS access | Driver and both executor probes freshly read 16,530 bytes with the host input SHA-256; NameNode RPC and both DataNode transfer endpoints were checked |
| Independent oracle | **1,024 input rows**, **682 contributing records**, eight aggregate rows/eight unique keys, zero nulls; exact host-Python integer oracle matched the actual Parquet reread |
| Schema/output | `bucket: int`; `record_count`, `quantity_total`, `amount_cents`: `bigint`; `_SUCCESS` and exactly two committed Parquet files, **1,366** and **1,430 bytes**, independently read and replicated on both current DataNodes |
| Event log/source identity | **639,740 bytes**, **158 records**, complete `SparkListenerLogStart` through `SparkListenerApplicationEnd`; application/executor/job/stage/task identities and all three phase groups correlated; installed/baked/archive sources and Hadoop XML matched |
| Termination/cleanup | Application `FINISHED`, driver removed, no matching active application/executors; only the successful run's owned fixture/client cleaned by the harness; no cleanup/restoration errors |
| Preservation | All six service IDs, starts, images, configuration, mounts and restart counts matched across this run; three persistent volume records, five pre-run VERSION hashes, historical fixture/seven checksums and 137 incident files matched; 1,472 protected source/evidence files had zero differences |

Primary proof is retained in `0053.stdout`/`0053.stderr`, `commands.jsonl`,
`spark-application-records.json`, `spark-correctness.json`, `spark-events.jsonl`,
`spark-event-log.json`, `spark-execution.json`, `spark-data-files.json`,
`spark-termination.json`, `spark-hdfs-preservation.json`, and `outcome.json`.
There are 88 harness command records and exactly one submission record. The event
log SHA-256 is
`c3d956bc3b6abaa8c74500104c724e10ed4d473819489c8e907c8b39b9b66673`.

Actual executor process captures and launch logs show
`JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=/opt/warehouse-spark/native`, `-Xmx512M`, and no
conflicting explicit `/tmp` JVM-temp option. Both executor process maps contain
executable `r-xp` ZSTD JNI mappings in the private mount. The driver also mapped
ZSTD there and reported the same `java.io.tmpdir` in its event-log system properties.

Read-only observation captured 27 samples per Worker and four successful driver
samples. Largest observed cgroup peak counters and sampled tmpfs usage were:

| Runtime | Peak counter / RAM limit, MiB | Private temp sampled maximum / cap, MiB | Bulk scratch sampled maximum / cap, MiB |
|---|---:|---:|---:|
| Worker 1 | 640.277 / 2048 | 1.375 / 32 | 0.2148 / 224 |
| Worker 2 | 667.887 / 2048 | 1.1055 / 32 | 0.2031 / 224 |
| Driver | 500.715 / 1536 | 1.1055 / 32 | 0.0977 / 96 |

Observed memory-limit/OOM/kill counters were zero; final Worker counters remained
zero. Both Workers had zero memory/disk task spills, and retained executor logs
contained no native mapping, out-of-memory, or no-space errors. Counters include
resident processes/observation overhead; sampled scratch maxima are not continuous
high-water measurements. The removed driver's final counters were unavailable.
See `phase2-review/resource-sample-*.json`, `executor-launch-logs.json`,
`executor-resource-review.json`, and `postflight*.json` in the supplemental review
directory for the underlying observations and comparisons.

#### Audited metadata differences and non-blocking diagnostics

- Before Phase 2 preflight, all six services had already restarted at approximately
  **2026-10-21 01:06:10 UTC** with the same container IDs/images/configuration. The
  current registrations above, rather than Phase 1's old Worker registrations, were
  used for task correlation. No service restart/recreation occurred during Phase 2.
- Unset Spark HostConfig `Dns`, `DnsOptions`, and `DnsSearch` fields appeared as
  empty lists instead of historical nulls. Their semantic values were unchanged;
  every other HostConfig field matched. This was a task-local comparison diagnosis,
  not a runtime configuration change.
- Startup refreshed timestamp comments in all five HDFS VERSION files. The audit
  reproduced each historical SHA-256 by substituting the actual retained old
  **timestamp comment** into the current noncomment bytes; cluster, namespace,
  block-pool, storage and DataNode identities matched. Current verified byte hashes
  became the Phase 2 baseline and matched exactly afterward. Proof is in
  `phase2-review/version-comment-comparison.json` and
  `storage-properties-diagnostic.json`. These audited differences do not waive
  future unexplained identity/data/preservation mismatches.
- A supplemental read-only driver sampling command exited **137** at
  **01:20:57.784 UTC**, after ApplicationEnd at **01:20:56.684** and overlapping
  ephemeral-driver teardown. The application/submission exited 0, removal was
  confirmed and captured OOM counters were zero. This is a non-blocking interrupted
  observation, not an application exit 137 or proof of an OOM kill; the exact kill
  cause is not independently established. Raw record:
  `phase2-review/command-0096-sample-017-5451cc967c2624dfc35f.json`.
- Review erratum: `phase2-review/report.md` calls command 0054's message
  `No such object`; actual **`0054.stderr` says `No such container`**. Its expected
  exit **1** confirms the owned driver was absent. The wording difference does not
  change the removal result; the original report is preserved unchanged.

The successful owned fixture was
`/t03-integration-d9674d7dc8b44efeaf9b7ae10d9128cb`; it was removed only after
complete evidence and termination checks. Host-side evidence remains retained,
along with the earlier failed incident/fixture. No assertion, codec, timeout,
resource cap, image or runtime configuration was changed to obtain the pass.

This completes the bounded **M02 T01/T02/T03** acceptance scope on a **single-host**
multi-container deployment. It demonstrates real distributed software execution
and synthetic-data correctness, not full M5 ingestion/ETL/MLlib, production capacity,
speedup, operational savings, HA, physical multi-host fault tolerance, or every
project-wide distributed-data gate. The oracle is independent host Python, not a
claim that the literal DIST-08 local-Spark comparison was executed.

### Future change verification: offline and disposable checks

For later runtime changes, the following offline gate uses no Docker services:

```bash
uv run --locked pytest -q tests/unit/test_spark_native_runtime.py tests/unit/test_spark_lifecycle.py tests/unit/test_spark_submit.py tests/unit/test_spark_submit_runtime.py tests/unit/test_spark_verification.py
bash -n scripts/smoke-spark.sh
uv run --locked ruff check scripts/check-spark-native.py tests/unit/test_spark_native_runtime.py tests/unit/test_spark_lifecycle.py tests/integration/test_spark_native_runtime.py
uv run --locked ruff format --check scripts/check-spark-native.py tests/unit/test_spark_native_runtime.py tests/unit/test_spark_lifecycle.py tests/integration/test_spark_native_runtime.py
```

Render default, `init`, `spark`, `spark-submit`, and combined Spark profiles with
`config --format json`. Compare complete models with their pre-edit snapshots:
only the listed Spark tmpfs and JVM-temp environment deltas are permitted. HDFS,
the initializer, volumes, network, images, resources, endpoints, and service
selection must match.

The explicitly selected Docker regression uses **existing local images only**:

```bash
bash scripts/load-env.sh uv run --locked pytest -q -m integration tests/integration/test_spark_native_runtime.py
```

It creates no Spark context/application and attaches no cluster volumes/network.
Disposable containers use read-only roots, UID/GID 185:185, 384 MiB RAM, 0.5 CPU,
128 PIDs, dropped capabilities, and `no-new-privileges`. Positive mounts/environment
come from the rendered Compose model. Both images must complete a real Spark ZSTD
codec round trip, report `Native.isLoaded()`, Java exit 0, and executable mappings
inside the private mount. Both the `noexec` and JVM-option-precedence controls
must genuinely attempt loading and exit Java 1 with the mapping error.

`scripts/check-spark-native.py` records effective identity/mounts/ownership/mode/
capacity, cgroup limits, JVM properties, Java status/errors, codec outcome, and
executable native mappings. Compilation has a 10-second deadline and Java a
20-second deadline; timed-out invocation-owned process groups are killed and
reaped. The helper removes only its private probe workspace. The bundled ZSTD
loader deletes its own extraction on normal JVM exit; no shared scratch cleanup
is performed. Smoke streams the same helper independently to each Worker with
a 45-second TERM / 2-second KILL outer bound; existing smoke deadlines stay intact.

### Future Spark-only maintenance window

These are reproducible procedures for a future change, separate from the completed
historical rollout above. **Do not execute without separate explicit approval.**
Use an idle cluster and one exclusive Bash session at the repository root. Stop on
any failed command, partial recreation, preservation mismatch, or incomplete JNI evidence;
do not retry live T03 automatically. Existing historical T01 execution records
above predate this new partition and do not qualify its deployment.

Prepare an evidence directory and verify the existing prerequisites:

```bash
set -euo pipefail
compose=(docker compose -f "$PWD/infrastructure/compose.yaml")
hdfs=(hdfs-namenode hdfs-datanode-1 hdfs-datanode-2)
fixture=/t03-integration-c68b2620714846c8b03113796281df90
incident=artifacts/test-runs/t03-20261020T140712-test_distributed_spark_hdfs_round_trip-e96e8aada0cb4277a9dea044705cddb6
evidence="artifacts/test-runs/zstd-rollout-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$evidence"
memory=$(timeout --kill-after=2s 15 docker info --format '{{.MemTotal}}')
[[ $memory =~ ^[0-9]+$ ]] && (( memory >= 10 * 1024 * 1024 * 1024 ))
timeout --kill-after=2s 45 "${compose[@]}" exec -T --interactive=false hdfs-namenode \
  bash /opt/warehouse-hdfs/healthcheck.sh wait-ready 30 < /dev/null
bash scripts/spark-cluster.sh check
timeout --kill-after=2s 15 "${compose[@]}" --profile spark exec -T --interactive=false \
  spark-master /opt/python/bin/python3.11 -c '
import json, urllib.request
with urllib.request.urlopen("http://spark-master:8080/json/", timeout=2) as response:
    master = json.load(response)
assert master["status"] == "ALIVE"
assert master["activeapps"] == [] and master["activedrivers"] == [], "Cluster is not idle"
print(json.dumps(master, sort_keys=True))' < /dev/null | tee "$evidence/idle-master.json"
```

Capture stable container/attachment/storage identities and read-only fingerprints
of all retained fixture files and incident artifacts. The generated T03 fixture
paths contain no whitespace. Each HDFS checksum invocation is individually bounded:

```bash
snapshot_hdfs() {
  local ids service storage
  ids=$(timeout --kill-after=2s 15 "${compose[@]}" ps --all --quiet "${hdfs[@]}")
  local -a containers
  mapfile -t containers <<< "$ids"
  [[ ${#containers[@]} == 3 && -n ${containers[0]} ]]
  timeout --kill-after=2s 15 docker inspect --format \
    '{{.Id}} {{.State.StartedAt}} {{.RestartCount}} {{.State.Running}} {{.State.Health.Status}} {{json .Mounts}}' \
    "${containers[@]}"
  for service in "${hdfs[@]}"; do
    storage=/var/lib/hdfs/data
    [[ $service != hdfs-namenode ]] || storage=/var/lib/hdfs/name
    timeout --kill-after=2s 15 "${compose[@]}" exec -T --interactive=false "$service" \
      bash -euo pipefail -c 'sha256sum "$1/current/VERSION"; for f in "$1"/current/BP-*/current/VERSION; do
        [[ ! -f $f ]] || sha256sum "$f"
      done' bash "$storage" < /dev/null
  done
  timeout --kill-after=2s 180 "${compose[@]}" exec -T --interactive=false hdfs-namenode \
    bash -euo pipefail -c '
listing=$(timeout --kill-after=2s 20 hdfs dfs -ls -R "$1")
printf "%s\n" "$listing"
while read -r permissions replicas owner group bytes day clock path; do
  if [[ $permissions == -* ]]; then
    timeout --kill-after=2s 15 hdfs dfs -checksum "$path"
  fi
done <<< "$listing"' bash "$fixture" < /dev/null
  uv run --locked python - "$incident" <<'PY'
import hashlib
import sys
from pathlib import Path
root = Path(sys.argv[1])
assert root.is_dir()
for path in sorted(root.rglob("*")):
    if path.is_file():
        print(hashlib.sha256(path.read_bytes()).hexdigest(), path)
PY
}
snapshot_hdfs > "$evidence/hdfs-before.txt"
```

Recreate **only the three Spark daemons**, without build, pull, or dependency startup;
then check HDFS preservation immediately and wait for both current Workers:

```bash
timeout --kill-after=2s 120 "${compose[@]}" --profile spark up \
  --detach --no-deps --no-build --pull never --force-recreate \
  spark-master spark-worker-1 spark-worker-2
snapshot_hdfs > "$evidence/hdfs-after.txt"
diff -u "$evidence/hdfs-before.txt" "$evidence/hdfs-after.txt"
timeout --kill-after=2s 195 "${compose[@]}" --profile spark exec -T --interactive=false \
  spark-master /opt/python/bin/python3.11 /opt/warehouse-spark/healthcheck.py wait-ready 180 < /dev/null
bash scripts/smoke-spark.sh | tee "$evidence/worker-smoke.txt"
```

Smoke must show successful **independent JNI loads on both Workers** as well as
the existing readiness/resource/mount checks. Next verify the ephemeral driver's
actual configured mounts/environment with the same helper, without a Spark application:

```bash
probe_name="warehouse-zstd-review-$(date -u +%Y%m%dT%H%M%SZ)-$$"
trap 'timeout --kill-after=2s 10 docker rm --force "$probe_name" > /dev/null 2>&1 || true' EXIT
timeout --kill-after=2s 45 "${compose[@]}" --profile spark-submit run \
  --rm --no-deps --pull never -T --name "$probe_name" --entrypoint /usr/bin/tini \
  spark-submit -g -- /opt/python/bin/python3.11 - \
  --bulk-mib 96 --memory-mib 1536 --cpus 1 \
  < scripts/check-spark-native.py | tee "$evidence/driver-native.json"
trap - EXIT
snapshot_hdfs > "$evidence/hdfs-after-jni.txt"
diff -u "$evidence/hdfs-before.txt" "$evidence/hdfs-after-jni.txt"
```

All three JNI probes must exit 0, prove an actual Spark codec round trip and
executable private-directory mappings, and match identity, effective mount flags,
permissions, capacities, JVM temp, and cgroup limits before requesting T03 approval.
Do not use cluster-wide resets, volume removal, or fixture cleanup for this incident.

### Future opt-in T03 acceptance

For a future live run, first verify current repository/source/image identities,
healthy idle Spark and HDFS services, and protected historical evidence/fixtures.
Set `WAREHOUSE_SPARK_MASTER_URL`, `WAREHOUSE_HDFS_URI`, and the recorded
`WAREHOUSE_HDFS_BASE_PATH=/data/warehouse` in the ignored project `.env` (or use
the host loader with inherited values). Retain the existing 300-second submission,
45-second cleanup-grace and 360-second host bounds.

After reviewing the current rollout/JNI/preservation evidence and receiving
**separate explicit approval for that live run**, execute exactly once:

```bash
bash scripts/load-env.sh uv run --locked pytest -q -m integration tests/integration/test_spark_hdfs.py::test_distributed_spark_hdfs_round_trip
```

Require correlated workload/barrier-task evidence from both **current** Workers,
a complete event log and successful jobs/stages/task metrics, source/archive/XML
correlation, and the independent oracle (1,024 source rows, 682 contributing records,
eight correctly typed aggregates). Require `_SUCCESS`, exactly two committed
Parquet files with independently validated bytes/schema, replication two and
both-DataNode placement, proven application/driver/executor termination, unchanged
HDFS identities/baseline payload, and preservation of historical evidence/fixture.
Review driver properties and executor launch logs for inherited JVM temp and any
conflicting flags. On failure retain the new evidence/fixture and stop for review.
For each future run, a nonzero status or incomplete evidence is a failed acceptance;
retain its fixture according to the harness policy and stop for review. Historical
acceptance is not authorization for an automatic rerun or repair.
