"""Fixed synthetic M02 T03 workload, not an ETL or benchmarking interface.

PySpark imports are deliberately lazy. All writes are below the caller's exact
test-owned HDFS subtree; the caller owns creation, independent assertions, and cleanup.
"""

import hashlib
import importlib.resources
import json
import os
import platform
import re
import socket
import subprocess
import time
from functools import partial
from pathlib import Path
from urllib.request import ProxyHandler, build_opener
from zipfile import ZipFile

from warehouse_bigdata.common.config import (
    ConfigurationError,
    load_hdfs_config,
    load_spark_config,
)

MASTER = "spark://spark-master:7077"
HDFS_URI = "hdfs://hdfs-namenode:8020"
WORKERS = ("spark-worker-1", "spark-worker-2")
HDFS_ENDPOINTS = ("hdfs-namenode:8020", "hdfs-datanode-1:9866", "hdfs-datanode-2:9866")
RECORD_PREFIX = "WAREHOUSE_SPARK_HDFS "
FIXTURE_PATTERN = re.compile(
    r"hdfs://hdfs-namenode:8020/t03-integration-[0-9a-f]{32}/spark\Z"
)


def validate_request(run_id: str, fixture_root: str, probe_sha256: str, env=None) -> None:
    """Reject substitutions before importing Spark, making a connection, or doing I/O."""
    if load_spark_config(env).master_url != MASTER:
        raise ConfigurationError(f"Verification requires the exact Standalone endpoint {MASTER}.")
    if load_hdfs_config(env).uri != HDFS_URI:
        raise ConfigurationError(f"Verification requires the exact HDFS endpoint {HDFS_URI}.")
    if not re.fullmatch(r"t03-[A-Za-z0-9_-]{1,150}", run_id):
        raise ConfigurationError("Verification requires a bounded, safe t03 run identity.")
    if not FIXTURE_PATTERN.fullmatch(fixture_root):
        raise ConfigurationError("Verification requires the exact owned HDFS spark/ subtree.")
    if not re.fullmatch(r"[0-9a-f]{64}", probe_sha256):
        raise ConfigurationError("Verification requires the host input's SHA-256 digest.")


def spark_settings(fixture_root: str) -> dict[str, str]:
    if not FIXTURE_PATTERN.fullmatch(fixture_root):
        raise ConfigurationError("Unsafe HDFS event-log destination.")
    return {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": fixture_root + "/events",
        "spark.eventLog.compress": "false",
        "spark.eventLog.rolling.enabled": "false",
        "spark.barrier.sync.timeout": "45s",
        "spark.scheduler.barrier.maxConcurrentTasksCheck.interval": "2s",
        "spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures": "5",
        "spark.scheduler.maxRegisteredResourcesWaitingTime": "30s",
        "spark.scheduler.minRegisteredResourcesRatio": "1.0",
        "spark.task.cpus": "1",
        "spark.ui.enabled": "false",
    }


def emit_record(kind: str, run_id: str, **values) -> None:
    print(RECORD_PREFIX + json.dumps({"kind": kind, "run_id": run_id, **values}), flush=True)


def source_fingerprint() -> dict:
    """Hash actual imported code, baked submission files, executor archive, and XML."""
    def hashes(root, prefix="") -> dict[str, str]:
        result = {}
        for path in root.iterdir():
            name = prefix + path.name
            if path.is_dir():
                result.update(hashes(path, name + "/"))
            elif name.endswith(".py"):
                result[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        return result

    archive_path = Path("/opt/warehouse-app/warehouse_bigdata.zip")
    with ZipFile(archive_path) as archive:
        members = {
            name.removeprefix("warehouse_bigdata/"): hashlib.sha256(archive.read(name)).hexdigest()
            for name in archive.namelist()
            if name.startswith("warehouse_bigdata/") and name.endswith(".py")
        }
    return {
        "sources": hashes(importlib.resources.files("warehouse_bigdata")),
        "baked_sources": hashes(Path("/opt/warehouse-app/src/warehouse_bigdata")),
        "archive_sources": members,
        "archive_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        "xml_sha256": {
            name: hashlib.sha256(
                (Path(os.environ.get("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")) / name).read_bytes()
            ).hexdigest()
            for name in ("core-site.xml", "hdfs-site.xml")
        },
        "python_version": platform.python_version(),
        "hdfs_base_path": load_hdfs_config().base_path,
    }


def check_hdfs_endpoints() -> list[str]:
    for endpoint in HDFS_ENDPOINTS:
        host, port = endpoint.split(":")
        with socket.create_connection((host, int(port)), timeout=2):
            pass
    return list(HDFS_ENDPOINTS)


def probe_hdfs(probe_uri: str, expected_sha256: str) -> dict:
    """Fresh bounded JVM byte read, also usable inside a Python executor worker.

    The bundled Hadoop FsShell performs the actual RPC and block transfer. Socket
    connectivity alone is never used as evidence that file content was read.
    """
    suffix = "/input/part-00.jsonl"
    if not probe_uri.endswith(suffix) or not FIXTURE_PATTERN.fullmatch(
        probe_uri.removesuffix(suffix)
    ):
        raise ConfigurationError("Unsafe HDFS probe file.")
    endpoints = check_hdfs_endpoints()
    try:
        completed = subprocess.run(
            [
                "java", "-Xms16m", "-Xmx64m", "-cp",
                "/opt/hadoop/etc/hadoop:/opt/spark/jars/*",
                "org.apache.hadoop.fs.FsShell", "-cat", probe_uri,
            ],
            capture_output=True,
            timeout=20,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("Fresh Hadoop byte read timed out after 20s.") from error
    if completed.returncode != 0:
        raise RuntimeError(
            "Fresh Hadoop byte read failed: " + completed.stderr.decode(errors="replace")[-2000:]
        )
    payload = completed.stdout
    digest = hashlib.sha256(payload).hexdigest()
    if not 0 < len(payload) <= 65536 or digest != expected_sha256:
        raise RuntimeError("Fresh Hadoop byte count/SHA-256 digest mismatch.")
    return {"bytes": len(payload), "sha256": digest, "endpoints": endpoints}


def read_cluster_snapshot() -> dict:
    """Capture live Master/Worker JSON from inside the bridge, without the Spark UI."""
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + 10

    def read(host, port):
        remaining = min(2, deadline - time.monotonic())
        if remaining <= 0:
            raise RuntimeError("Spark JSON snapshot deadline expired.")
        with opener.open(f"http://{host}:{port}/json/", timeout=remaining) as response:
            payload = response.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
            raise RuntimeError("Oversized Spark JSON evidence.")
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise RuntimeError("Malformed Spark JSON evidence.")
        return value

    return {
        "captured_at_ms": int(time.time() * 1000),
        "master": read("spark-master", 8080),
        "workers": {host: read(host, 8081) for host in WORKERS},
    }


def _probe_partition(rows, *, probe_uri: str, probe_sha256: str):
    from pyspark import BarrierTaskContext

    context = BarrierTaskContext.get()
    consumed = sum(1 for _ in rows)
    if consumed <= 0:
        raise RuntimeError("Barrier probe must consume non-empty fixture-backed rows.")
    host = socket.gethostname()
    if host not in WORKERS:
        raise RuntimeError(f"Unexpected executor host: {host}")
    identity = {
        "host": host,
        "partition_id": context.partitionId(),
        "task_attempt_id": context.taskAttemptId(),
        "task_attempt_number": context.attemptNumber(),
        "stage_id": context.stageId(),
        # The event log supplies the stage-attempt ID; PySpark exposes only stageId.
        "rows_consumed": consumed,
        **probe_hdfs(probe_uri, probe_sha256),
        "python_version": platform.python_version(),
    }
    peers = [json.loads(value) for value in context.allGather(json.dumps(identity))]
    if len(peers) != 2 or {peer["host"] for peer in peers} != set(WORKERS):
        raise RuntimeError("Barrier did not execute on both expected Workers.")
    yield {**identity, "peers": peers}


def run_verification(*, run_id: str, fixture_root: str, probe_sha256: str) -> dict:
    """Run the fixed DataFrame round trip and return only actual, bounded evidence."""
    validate_request(run_id, fixture_root, probe_sha256)
    from pyspark import SparkContext
    from pyspark.sql import SparkSession, functions as F, types as T

    if SparkContext._active_spark_context is not None:
        raise RuntimeError("Event logging must be configured before SparkContext creation.")
    host = os.environ.get("_WAREHOUSE_SPARK_DRIVER_HOST", "")
    if not re.fullmatch(r"warehouse-spark-submit-[0-9a-f]{32}", host):
        raise ConfigurationError("Verification must run in the uniquely named T02 driver.")
    emit_record("startup", run_id, driver_host=host, **source_fingerprint())
    builder = SparkSession.builder.master(MASTER).appName(f"warehouse-spark-hdfs-{run_id}")
    for key, value in spark_settings(fixture_root).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    context = spark.sparkContext
    emit_record("application", run_id, application_id=context.applicationId, driver_host=host)
    try:
        if context.master != MASTER:
            raise RuntimeError("Actual SparkContext is not the required Standalone master.")
        hadoop = context._jsc.hadoopConfiguration()
        for key, value in (
            ("fs.defaultFS", HDFS_URI),
            ("dfs.replication", "2"),
            ("dfs.client.use.datanode.hostname", "true"),
        ):
            if hadoop.get(key) != value:
                raise RuntimeError(f"Actual Hadoop configuration mismatch: {key}")
        context.setJobGroup(f"{run_id}:input-probe", "Synthetic fixture input and Worker probes", True)
        schema = T.StructType([
            T.StructField(name, T.IntegerType(), False)
            for name in ("record_id", "bucket", "quantity", "unit_price_cents")
        ])
        input_df = spark.read.schema(schema).option("mode", "FAILFAST").json(fixture_root + "/input")
        input_count = input_df.count()
        probe_uri = fixture_root + "/input/part-00.jsonl"
        driver_probe = probe_hdfs(probe_uri, probe_sha256)
        probes = input_df.repartition(2).rdd.barrier().mapPartitions(
            partial(_probe_partition, probe_uri=probe_uri, probe_sha256=probe_sha256)
        ).collect()
        active_cluster = read_cluster_snapshot()

        context.setJobGroup(f"{run_id}:transform-write", "Synthetic integer aggregation to Parquet", True)
        transformed = (
            input_df.filter(F.col("record_id") % 3 != 0)
            .withColumn("line_amount_cents", F.col("quantity").cast("long") * F.col("unit_price_cents"))
            .groupBy("bucket")
            .agg(
                F.count("record_id").alias("record_count"),
                F.sum("quantity").alias("quantity_total"),
                F.sum("line_amount_cents").alias("amount_cents"),
            )
            .repartition(2)
        )
        destination = fixture_root + "/work/parquet"
        transformed.write.mode("errorifexists").parquet(destination)

        context.setJobGroup(f"{run_id}:parquet-reread", "Fresh uncached Parquet verification", True)
        reread = spark.read.parquet(destination)
        actual_rows = [row.asDict() for row in reread.limit(9).collect()]
        null_expression = F.col(reread.columns[0]).isNull()
        for column in reread.columns[1:]:
            null_expression = null_expression | F.col(column).isNull()
        result = {
            "run_id": run_id,
            "application_id": context.applicationId,
            "driver_host": host,
            "fixture_root": fixture_root,
            "provenance": "synthetic verification data",
            "input_count": input_count,
            "rows": actual_rows,
            "schema": {field.name: field.dataType.simpleString() for field in reread.schema.fields},
            "row_count": reread.count(),
            "unique_keys": reread.select("bucket").distinct().count(),
            "null_count": reread.filter(null_expression).count(),
            "contributing_records": reread.agg(F.sum("record_count")).first()[0],
            "driver_probe": driver_probe,
            "probes": probes,
            "active_cluster": active_cluster,
            "versions": {
                "spark": spark.version,
                "python": platform.python_version(),
                "java": context._jvm.java.lang.System.getProperty("java.version"),
                "hadoop": context._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
            },
        }
    finally:
        spark.stop()  # Flush ApplicationEnd and rename the non-rolling event log before returning.
    return result
