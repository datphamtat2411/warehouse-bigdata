#!/usr/bin/env bash
# T01 infrastructure evidence only: this script never submits a Spark application.
set -euo pipefail
if [[ $# != 0 ]]; then
    printf 'Usage: bash scripts/smoke-spark.sh\n' >&2
    exit 2
fi

script_dir=${BASH_SOURCE[0]%/*}
[[ $script_dir == "${BASH_SOURCE[0]}" ]] && script_dir=.
repository=$(cd -- "$script_dir/.." && pwd)
# shellcheck disable=SC1091
source "$repository/scripts/load-env.sh"
compose=(docker compose -f "$repository/infrastructure/compose.yaml" --profile spark)
services=(spark-master spark-worker-1 spark-worker-2)
diagnostics() {
    bash "$repository/scripts/spark-cluster.sh" status >&2 || true
    bash "$repository/scripts/spark-cluster.sh" logs >&2 || true
}
trap 'result=$?; diagnostics; exit "$result"' ERR

memory=$(timeout 15 docker info --format '{{.MemTotal}}')
if [[ ! $memory =~ ^[0-9]+$ ]] || (( memory < 10 * 1024 * 1024 * 1024 )); then
    printf 'Smoke acceptance guard is the planned 10 GiB budget (not a measured minimum); reported Docker bytes: %s\n' \
        "${memory:-unknown}" >&2
    exit 1
fi
printf 'Docker memory: %s MiB; HDFS + Spark limits: 7168 MiB.\n' "$((memory / 1024 / 1024))"
bash "$repository/scripts/spark-cluster.sh" check

containers=()
for service in "${services[@]}"; do
    container=$(timeout 15 "${compose[@]}" ps --quiet "$service")
    [[ -n $container && $container != *$'\n'* ]]
    containers+=("$container")
done

# Inspect engine-enforced limits and mounts, not just the intended Compose model.
timeout 15 docker inspect "${containers[@]}" | \
    timeout 15 "${compose[@]}" exec -T spark-master /opt/python/bin/python3.11 -c '
import json
import sys

containers = json.load(sys.stdin)
expected = {"spark-master", "spark-worker-1", "spark-worker-2"}
assert len(containers) == 3
assert {c["Config"]["Labels"]["com.docker.compose.service"] for c in containers} == expected
assert len({c["Image"] for c in containers}) == 1, "Runtime image IDs differ"
for container in containers:
    config, host = container["Config"], container["HostConfig"]
    service = config["Labels"]["com.docker.compose.service"]
    master = service == "spark-master"
    assert config["Labels"]["com.docker.compose.project"] == "warehouse-bigdata"
    assert config["Image"] == "warehouse-bigdata-spark:3.5.7-python3.11.17"
    assert config["Hostname"] == service and config["User"] == "185:185"
    assert config["Entrypoint"] == ["/usr/bin/tini", "--", "/opt/spark/bin/spark-class"]
    assert container["State"]["Running"] and not container["State"]["OOMKilled"]
    assert host["ReadonlyRootfs"] and host["Memory"] == (512 if master else 2048) * 1024**2
    assert host["NanoCpus"] == (500_000_000 if master else 1_000_000_000)
    assert host["RestartPolicy"] == {"Name": "on-failure", "MaximumRetryCount": 3}
    assert config["StopTimeout"] == 60
    assert host["LogConfig"] == {
        "Type": "json-file", "Config": {"max-size": "10m", "max-file": "3"}
    }
    native = "/opt/warehouse-spark/native"
    size = 67108864 if master else 234881024
    assert set(host["Tmpfs"]) == ({"/tmp"} if master else {"/tmp", native})
    assert {"rw", "nosuid", "nodev", "noexec", f"size={size}", "mode=1777"} <= set(
        host["Tmpfs"]["/tmp"].split(",")
    )
    if not master:
        options = set(host["Tmpfs"][native].split(","))
        assert {"rw", "exec", "nosuid", "nodev", "size=33554432",
                "uid=185", "gid=185", "mode=0700"} <= options
        assert "noexec" not in options
        env = dict(value.split("=", 1) for value in config["Env"])
        assert env["JAVA_TOOL_OPTIONS"] == f"-Djava.io.tmpdir={native}"
        assert env["SPARK_DAEMON_JAVA_OPTS"] == f"-Xms128m -Djava.io.tmpdir={native}"
    mounts = container["Mounts"]
    assert len(mounts) == 2 and all(m["Type"] == "bind" and not m["RW"] for m in mounts)
    for name in ("core-site.xml", "hdfs-site.xml"):
        mount = next(m for m in mounts if m["Destination"] == f"/opt/hadoop/etc/hadoop/{name}")
        assert mount["Source"].endswith(f"/infrastructure/hadoop/{name}")
    assert set(container["NetworkSettings"]["Networks"]) == {"warehouse-bigdata"}
    bindings = {p: value for p, value in container["NetworkSettings"]["Ports"].items() if value}
    assert bindings == ({"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]} if master else {})
    image, memory = container["Image"], host["Memory"]
    print(f"{service}: image={image}; Docker RAM={memory}; "
          f"scratch cap={size}; rotating logs and read-only mounts verified")
'

for service in "${services[@]}"; do
    timeout 75 "${compose[@]}" exec -T "$service" /opt/python/bin/python3.11 - "$service" <<'SPARK_SMOKE'
import os
import platform
import socket
import subprocess
import sys
from pathlib import Path

import pyspark

service = sys.argv[1]
master = service == "spark-master"
assert os.getuid() == os.getgid() == 185
assert platform.python_version() == "3.11.17" and pyspark.__version__ == "3.5.7"
assert os.environ["PYSPARK_PYTHON"] == "/opt/python/bin/python3.11"
assert os.environ["HADOOP_CONF_DIR"] == "/opt/hadoop/etc/hadoop"
assert os.environ["SPARK_DAEMON_MEMORY"] == "256m"
for name, value in {
    "SPARK_WORKER_DIR": "/tmp/warehouse-spark/work",
    "SPARK_LOCAL_DIRS": "/tmp/warehouse-spark/local",
    "SPARK_LOG_DIR": "/tmp/warehouse-spark/logs",
    "SPARK_PID_DIR": "/tmp/warehouse-spark",
}.items():
    assert os.environ[name] == value
jars = Path("/opt/spark/jars")
for jar in (
    "spark-core_2.12-3.5.7.jar", "scala-library-2.12.18.jar",
    "hadoop-client-api-3.3.4.jar", "hadoop-client-runtime-3.3.4.jar",
):
    assert (jars / jar).is_file(), jar

java = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=10, check=True)
assert 'version "11.0.30"' in java.stderr
print(f"{service}: Python {platform.python_version()}, PySpark {pyspark.__version__}, "
      "Scala 2.12.18, Hadoop client 3.3.4; " + java.stderr.splitlines()[0])

assert Path("/proc/1/comm").read_text().strip() == "tini"
main_class = "org.apache.spark.deploy." + ("master.Master" if master else "worker.Worker")
daemons = []
for process in Path("/proc").iterdir():
    if process.name.isdecimal():
        try:
            command = (process / "cmdline").read_bytes().decode().split("\0")
        except (OSError, UnicodeError):
            continue
        if main_class in command and Path(command[0]).name == "java":
            daemons.append(command)
assert len(daemons) == 1
command = daemons[0]
native = "/opt/warehouse-spark/native"
temporary = "/tmp" if master else native
for option in ("-Xms128m", "-Xmx256m", f"-Djava.io.tmpdir={temporary}"):
    assert option in command, option
assert [option for option in command if option.startswith("-Djava.io.tmpdir=")] == [
    f"-Djava.io.tmpdir={temporary}"
]
if master:
    assert "-Dspark.master.rest.enabled=false" in command
else:
    assert os.environ["JAVA_TOOL_OPTIONS"] == f"-Djava.io.tmpdir={native}"
    assert os.environ["SPARK_DAEMON_JAVA_OPTS"] == f"-Xms128m -Djava.io.tmpdir={native}"
    for option in (
        "-Dspark.worker.cleanup.enabled=true", "-Dspark.worker.cleanup.interval=300",
        "-Dspark.worker.cleanup.appDataTtl=3600",
    ):
        assert option in command, option
    assert Path("/tmp/warehouse-spark/work").is_dir()
print(f"{service}: JVM flags: {command}")

expected_memory = (512 if master else 2048) * 1024**2
cgroup = Path("/sys/fs/cgroup")
if (cgroup / "memory.max").exists():
    actual_memory = int((cgroup / "memory.max").read_text())
    quota, period = map(int, (cgroup / "cpu.max").read_text().split())
else:
    actual_memory = int((cgroup / "memory/memory.limit_in_bytes").read_text())
    quota = int((cgroup / "cpu/cpu.cfs_quota_us").read_text())
    period = int((cgroup / "cpu/cpu.cfs_period_us").read_text())
assert actual_memory == expected_memory
assert quota / period == (0.5 if master else 1)
scratch = os.statvfs("/tmp")
capacity = scratch.f_blocks * scratch.f_frsize
used = (scratch.f_blocks - scratch.f_bfree) * scratch.f_frsize
assert capacity == (64 if master else 224) * 1024**2
mount = next(line for line in Path("/proc/self/mountinfo").read_text().splitlines()
             if line.split()[4] == "/tmp")
assert " - tmpfs " in mount
assert {"rw", "nosuid", "nodev", "noexec"} <= set(mount.split()[5].split(","))
print(f"{service}: cgroup RAM={actual_memory}, CPUs={quota / period}; "
      f"tmpfs used={used} / {capacity} bytes (charged to container RAM)")

targets = [("spark-master", 7077), ("spark-worker-1", 7078), ("spark-worker-2", 7078),
           ("hdfs-namenode", 8020), ("hdfs-datanode-1", 9866), ("hdfs-datanode-2", 9866)]
for host, port in targets:
    with socket.create_connection((host, port), timeout=2):
        pass
    print(f"{service} -> {host}:{port}: reachable")
if master:
    result = subprocess.run(
        ["java", "-Xms32m", "-Xmx128m", "-cp", "/opt/hadoop/etc/hadoop:/opt/spark/jars/*",
         "org.apache.hadoop.fs.FsShell", "-ls", "/"],
        capture_output=True, text=True, timeout=30, check=True,
    )
    print("Bundled HDFS client metadata listing passed (no data writes): " + result.stdout)
SPARK_SMOKE
    if [[ $service != spark-master ]]; then
        timeout --kill-after=2s 45 "${compose[@]}" exec -T "$service" /opt/python/bin/python3.11 - \
            --bulk-mib 224 --memory-mib 2048 --cpus 1 < "$repository/scripts/check-spark-native.py"
    fi
done
printf 'Spark infrastructure smoke passed: two current Workers; both ZSTD JNI loads verified; no application submitted.\n'
