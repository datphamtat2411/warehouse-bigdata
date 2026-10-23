"""Read completed run event logs and immutable selection metadata, without Spark."""

import argparse
import hashlib
import json

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import render_curated_sales_daily_path, render_staging_path
from warehouse_bigdata.common.scope_evidence import read_scope
from warehouse_bigdata.contracts.dataset_scope import ScopeError

MAX_EVENT_BYTES = 256 * 1024 * 1024


def completed_execution(client, config, run):
    root = render_staging_path(config.base_path, run) + "/events"
    paths = client.list_children(root)
    if len(paths) != 1 or paths[0].endswith(".inprogress"):
        raise ScopeError("requires exactly one completed nonrolling Spark event log: " + root)
    path = paths[0]
    size = client.stat(path).size_bytes
    if not 0 < size <= MAX_EVENT_BYTES:
        raise ScopeError("event log exceeds the bounded evidence limit")
    payload = client.read_remote(path)
    if len(payload) != size:
        raise ScopeError("event log changed during readback")
    workers, stages, jobs, application, ended, master = {}, set(), set(), None, False, None
    for line in payload.splitlines():
        event = json.loads(line)
        kind = event.get("Event")
        if kind == "SparkListenerApplicationStart":
            application = event["App ID"]
        elif kind == "SparkListenerApplicationEnd":
            ended = True
        elif kind == "SparkListenerEnvironmentUpdate":
            master = event.get("Spark Properties", {}).get("spark.master")
        elif kind == "SparkListenerJobStart":
            jobs.add(event["Job ID"])
        elif (
            kind == "SparkListenerTaskEnd"
            and event.get("Task End Reason", {}).get("Reason") == "Success"
        ):
            info, metrics = event["Task Info"], event.get("Task Metrics", {})
            stages.add(event["Stage ID"])
            values = workers.setdefault(
                info["Host"],
                dict(
                    successful_tasks=0,
                    executor_run_time_ms=0,
                    gc_time_ms=0,
                    memory_spill_bytes=0,
                    disk_spill_bytes=0,
                    input_bytes=0,
                    output_bytes=0,
                ),
            )
            values["successful_tasks"] += 1
            for output, key in (
                ("executor_run_time_ms", "Executor Run Time"),
                ("gc_time_ms", "JVM GC Time"),
                ("memory_spill_bytes", "Memory Bytes Spilled"),
                ("disk_spill_bytes", "Disk Bytes Spilled"),
            ):
                values[output] += metrics.get(key, 0)
            values["input_bytes"] += metrics.get("Input Metrics", {}).get("Bytes Read", 0)
            values["output_bytes"] += metrics.get("Output Metrics", {}).get("Bytes Written", 0)
    if (
        not application
        or not ended
        or master != "spark://spark-master:7077"
        or set(workers) != {"spark-worker-1", "spark-worker-2"}
    ):
        raise ScopeError("completed Standalone execution on both named Workers is not proven")
    return dict(
        application_id=application,
        spark_master=master,
        workers=workers,
        completed_stage_count=len(stages),
        job_count=len(jobs),
        event_log_path=path,
        event_log_bytes=size,
        event_log_sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id")
    parser.add_argument("--source-snapshot-id")
    args = parser.parse_args()
    client, config = HdfsCliClient(metadata_limit=MAX_EVENT_BYTES), load_hdfs_config()
    report = {}
    if args.pipeline_run_id:
        report["execution"] = completed_execution(client, config, args.pipeline_run_id)
    if args.source_snapshot_id:
        report["scope"] = read_scope(
            client, render_curated_sales_daily_path(config.base_path, args.source_snapshot_id)
        )
    if not report:
        parser.error("select a pipeline run and/or source snapshot")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
