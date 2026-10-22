"""Bounded synthetic Standalone disk/spill probe; no HDFS data or business inputs."""

import json
import os
import socket
import tempfile
from pathlib import Path


def scratch_partition(rows):
    """Require simultaneous real disk writes on the two one-core Workers."""
    from pyspark import BarrierTaskContext

    context = BarrierTaskContext.get()
    root = Path(os.environ["SPARK_LOCAL_DIRS"].split(",")[0])
    if not root.is_relative_to("/var/lib/warehouse-spark/local"):
        raise RuntimeError(f"Executor scratch is misplaced: {root}")
    if os.getuid() != 185 or os.getgid() != 185:
        raise RuntimeError("Executor is not UID/GID 185:185")
    payload = b"x" * 1024**2  # 1 MiB, not a sparse allocation.
    with tempfile.TemporaryDirectory(prefix="scratch-verification-", dir=root) as directory:
        probe = Path(directory) / "allocated"
        with probe.open("xb") as stream:
            for _ in range(256):
                stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        size = probe.stat().st_size
        if size != 256 * 1024**2:
            raise RuntimeError("Synthetic scratch allocation was incomplete")
        with probe.open("rb") as stream:
            for _ in range(256):
                if stream.read(len(payload)) != payload:
                    raise RuntimeError("Synthetic scratch read-back failed")
        context.barrier()
        result = {
            "host": socket.gethostname(),
            "uid": os.getuid(),
            "root": str(root),
            "allocated_bytes": size,
            "rows": sum(1 for _ in rows),
        }
    yield result


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    with tempfile.TemporaryDirectory(prefix="scratch-events-") as directory:
        spark = (
            SparkSession.builder.appName("warehouse-synthetic-scratch-spill")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.compress", "false")
            .config("spark.eventLog.dir", Path(directory).as_uri())
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.shuffle.spill.numElementsForceSpillThreshold", "10000")
            .config("spark.shuffle.compress", "false")
            .config("spark.shuffle.spill.compress", "false")
            .getOrCreate()
        )
        try:
            if spark.sparkContext.master != "spark://spark-master:7077":
                raise RuntimeError(
                    "Synthetic verification requires the existing Standalone cluster"
                )
            app_id = spark.sparkContext.applicationId
            probes = (
                spark.sparkContext.parallelize(range(2), 2)
                .barrier()
                .mapPartitions(scratch_partition)
                .collect()
            )
            if {probe["host"] for probe in probes} != {"spark-worker-1", "spark-worker-2"}:
                raise RuntimeError("Both Workers must independently pass the disk write probe")
            # 400,000 deterministic rows, 256-byte payload, two shuffle/sort partitions.
            frame = spark.range(400_000, numPartitions=2).select(
                "id", F.repeat(F.sha2(F.col("id").cast("string"), 256), 4).alias("payload")
            )
            counts = (
                frame.repartition(2, "id")
                .sortWithinPartitions("payload")
                .rdd.mapPartitions(lambda rows: [sum(1 for _ in rows)])
                .collect()
            )
            if sum(counts) != 400_000:
                raise RuntimeError("Synthetic sorted row count mismatch")
        finally:
            spark.stop()
        metrics = {}
        for file in Path(directory).iterdir():
            if not file.name.startswith("app-"):
                continue
            for line in file.read_text().splitlines():
                event = json.loads(line)
                if event.get("Event") != "SparkListenerTaskEnd":
                    continue
                if event["Task End Reason"]["Reason"] != "Success":
                    raise RuntimeError("Synthetic workload had a failed task attempt")
                host = event["Task Info"]["Host"]
                totals = metrics.setdefault(
                    host, {"successful_tasks": 0, "disk_spill_bytes": 0, "shuffle_write_bytes": 0}
                )
                task = event["Task Metrics"]
                totals["successful_tasks"] += 1
                totals["disk_spill_bytes"] += task["Disk Bytes Spilled"]
                totals["shuffle_write_bytes"] += task.get("Shuffle Write Metrics", {}).get(
                    "Shuffle Bytes Written", 0
                )
        if set(metrics) != {"spark-worker-1", "spark-worker-2"} or any(
            worker["disk_spill_bytes"] <= 0 or worker["shuffle_write_bytes"] <= 0
            for worker in metrics.values()
        ):
            raise RuntimeError(f"Missing real shuffle/spill evidence on both Workers: {metrics}")
        print(
            "WAREHOUSE_SPARK_SCRATCH "
            + json.dumps(
                {
                    "application_id": app_id,
                    "synthetic_rows": 400_000,
                    "probes": probes,
                    "workers": metrics,
                    "status": "VALIDATED",
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
