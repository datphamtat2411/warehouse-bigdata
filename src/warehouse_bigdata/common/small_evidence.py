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
EXPECTED_MASTER = "spark://spark-master:7077"
EXPECTED_WORKERS = ("spark-worker-1", "spark-worker-2")


def completed_execution(
    client,
    config,
    run,
    *,
    expected_application_id=None,
    expected_application_name=None,
    expected_master=EXPECTED_MASTER,
    expected_workers=EXPECTED_WORKERS,
    expected_event_log_dir=None,
):
    """Reconcile one bounded completed Spark event log.

    Worker participation is derived only from successful tasks in successful
    stage attempts. Failed/retried attempts remain explicit in the returned
    evidence instead of being folded into the successful counts. ``Task Info.Index``
    is the canonical task-set identity used for coverage. ``Partition ID`` is
    retained as the separate RDD partition identity and is optional for legacy
    event records produced before Spark 3.3; a missing index is rejected because
    task-set coverage would otherwise be ambiguous. The legacy ``partition``
    report field keeps its prior fallback behavior, while ``task_set_index`` and
    ``partition_id`` expose the two identities explicitly.
    """
    root = render_staging_path(config.base_path, run) + "/events"
    paths = client.list_children(root)
    if len(paths) != 1 or paths[0].endswith(".inprogress"):
        raise ScopeError("requires exactly one completed nonrolling Spark event log: " + root)
    path = paths[0]
    if expected_event_log_dir is not None and root != expected_event_log_dir.rstrip("/"):
        raise ScopeError("event log path differs from the expected producing run")
    stat = client.stat(path)
    size = stat.size_bytes if hasattr(stat, "size_bytes") else stat["size_bytes"]
    if not 0 < size <= MAX_EVENT_BYTES:
        raise ScopeError("event log exceeds the bounded evidence limit")
    payload = client.read_remote(path)
    if len(payload) != size:
        raise ScopeError("event log changed during readback")
    expected_workers = tuple(expected_workers)
    expected_worker_set = set(expected_workers)
    if not expected_worker_set or len(expected_worker_set) != len(expected_workers):
        raise ScopeError("expected Workers must be distinct and nonempty")
    if not isinstance(expected_master, str) or not expected_master.startswith("spark://"):
        raise ScopeError("expected Spark master must be a Standalone URL")

    lines = payload.splitlines()
    if not lines or not payload.endswith(b"\n"):
        raise ScopeError("event log is truncated or missing its terminal newline")

    executors = {}
    workers = {}
    jobs = {}
    stage_attempts = {}
    task_attempts = {}
    task_ids = set()
    task_attempt_ids = set()
    partition_ids_by_task = {}
    partition_to_task_index = {}
    event_kinds = []
    application = None
    application_name = None
    application_end_count = 0
    log_start_count = 0
    environment_properties = None

    def _nonnegative_int(value, name):
        if type(value) is not int or value < 0:
            raise ScopeError(f"event log contains invalid {name}")
        return value

    def stage_key(event, *, info=None):
        info = info if info is not None else event.get("Stage Info")
        if info is not None and not isinstance(info, dict):
            raise ScopeError("Spark event has malformed Stage Info")
        stage_id = (info or {}).get("Stage ID", event.get("Stage ID"))
        attempt_id = (info or {}).get("Stage Attempt ID", event.get("Stage Attempt ID"))
        return (
            _nonnegative_int(stage_id, "stage ID"),
            _nonnegative_int(attempt_id, "stage attempt ID"),
        )

    def stage_info(info):
        if not isinstance(info, dict):
            raise ScopeError("Spark stage evidence is missing Stage Info")
        key = stage_key({}, info=info)
        task_count = _nonnegative_int(info.get("Number of Tasks"), "stage task count")
        if task_count == 0:
            raise ScopeError("Spark stage evidence contains an empty stage")
        return key, task_count

    def ensure_stage(key, *, task_count=None):
        record = stage_attempts.setdefault(
            key,
            {
                "stage_id": key[0],
                "stage_attempt_id": key[1],
                "submitted": False,
                "completed": False,
                "successful": False,
                "expected_tasks": None,
                "tasks": [],
                "failure_reason": None,
                "properties": {},
            },
        )
        if task_count is not None:
            if record["expected_tasks"] not in (None, task_count):
                raise ScopeError("Spark stage task count changed across event records")
            record["expected_tasks"] = task_count
        return record

    for raw_line in lines:
        if not raw_line.strip():
            raise ScopeError("event log contains a blank record")
        try:
            event = json.loads(raw_line)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ScopeError("event log contains malformed JSON") from error
        if not isinstance(event, dict):
            raise ScopeError("event log records must be JSON objects")
        kind = event.get("Event")
        event_kinds.append(kind)
        if kind == "SparkListenerApplicationStart":
            if application is not None:
                raise ScopeError("event log contains duplicate application starts")
            application = event.get("App ID")
            application_name = event.get("App Name")
            if not isinstance(application, str) or not application:
                raise ScopeError("application start is missing App ID")
            if not isinstance(application_name, str) or not application_name:
                raise ScopeError("application start is missing App Name")
        elif kind == "SparkListenerLogStart":
            log_start_count += 1
        elif kind == "SparkListenerExecutorAdded":
            executor_id = event.get("Executor ID")
            info = event.get("Executor Info")
            if (
                not isinstance(executor_id, str)
                or not executor_id
                or executor_id in executors
                or not isinstance(info, dict)
                or not isinstance(info.get("Host"), str)
                or not info["Host"]
            ):
                raise ScopeError("executor-host mapping is missing or duplicated")
            executors[executor_id] = {
                "host": info["Host"],
                "total_cores": info.get("Total Cores"),
            }
        elif kind == "SparkListenerApplicationEnd":
            application_end_count += 1
        elif kind == "SparkListenerEnvironmentUpdate":
            if environment_properties is not None:
                raise ScopeError("event log contains duplicate environment updates")
            properties = event.get("Spark Properties")
            if not isinstance(properties, dict):
                raise ScopeError("environment update is missing Spark Properties")
            environment_properties = properties
        elif kind == "SparkListenerJobStart":
            job_id = event.get("Job ID")
            _nonnegative_int(job_id, "job ID")
            if job_id in jobs:
                raise ScopeError("event log contains duplicate job starts")
            stage_ids = event.get("Stage IDs")
            infos = event.get("Stage Infos")
            if not isinstance(stage_ids, list) or not stage_ids:
                raise ScopeError("job start is missing Stage IDs")
            stage_ids = [_nonnegative_int(stage_id, "stage ID") for stage_id in stage_ids]
            if not isinstance(infos, list) or len(infos) != len(stage_ids):
                raise ScopeError("job start stage IDs and infos differ")
            parsed = [stage_info(info) for info in infos]
            if {key[0] for key, _ in parsed} != set(stage_ids) or len(set(stage_ids)) != len(
                stage_ids
            ):
                raise ScopeError("job start stage IDs and infos differ")
            jobs[job_id] = {
                "ended": False,
                "successful": False,
                "result": None,
                "stage_ids": tuple(stage_ids),
            }
            for key, task_count in parsed:
                ensure_stage(key, task_count=task_count)
        elif kind == "SparkListenerStageSubmitted":
            info = event.get("Stage Info")
            key, task_count = stage_info(info)
            record = ensure_stage(key, task_count=task_count)
            if record["submitted"]:
                raise ScopeError("event log contains duplicate stage submissions")
            record["submitted"] = True
            properties = event.get("Properties", {})
            if not isinstance(properties, dict):
                raise ScopeError("stage submission has malformed Properties")
            record["properties"] = properties
        elif kind == "SparkListenerStageCompleted":
            info = event.get("Stage Info")
            key, task_count = stage_info(info)
            record = stage_attempts.get(key)
            if record is None or not record["submitted"]:
                raise ScopeError("stage completion has no submitted stage attempt")
            if record["completed"]:
                raise ScopeError("event log contains duplicate stage completions")
            ensure_stage(key, task_count=task_count)
            record["completed"] = True
            failure = info.get("Failure Reason")
            record["successful"] = failure is None
            record["failure_reason"] = failure
        elif kind == "SparkListenerJobEnd":
            job_id = event.get("Job ID")
            if job_id not in jobs:
                raise ScopeError("job completion has no submitted job")
            if jobs[job_id]["ended"]:
                raise ScopeError("event log contains duplicate job completions")
            result_data = event.get("Job Result")
            if not isinstance(result_data, dict) or not isinstance(result_data.get("Result"), str):
                raise ScopeError("job completion is missing a result")
            result = result_data["Result"]
            jobs[job_id].update(ended=True, successful=result == "JobSucceeded", result=result)
        elif kind == "SparkListenerTaskEnd":
            key = stage_key(event)
            info = event.get("Task Info")
            record = stage_attempts.get(key)
            if record is None or not record["submitted"]:
                raise ScopeError("task completion has no submitted stage attempt")
            if not isinstance(info, dict):
                raise ScopeError("task completion is missing Task Info")
            task_id = _nonnegative_int(info.get("Task ID"), "task ID")
            if "Index" not in info:
                raise ScopeError("task completion is missing task-set index")
            task_set_index = _nonnegative_int(info["Index"], "task-set index")
            if "Partition ID" in info:
                partition_id = _nonnegative_int(info["Partition ID"], "RDD partition ID")
            else:
                partition_id = None
            attempt = _nonnegative_int(info.get("Attempt"), "task attempt")
            if task_set_index >= record["expected_tasks"]:
                raise ScopeError("task-set index is outside the stage task set")
            task_attempt_key = (key, task_set_index, attempt)
            if task_attempt_key in task_attempt_ids:
                raise ScopeError("event log contains duplicate logical task attempt")
            task_attempt_ids.add(task_attempt_key)
            logical_task_key = (key[0], task_set_index)
            if partition_id is not None:
                previous_partition_id = partition_ids_by_task.get(logical_task_key)
                if previous_partition_id is not None and previous_partition_id != partition_id:
                    raise ScopeError("RDD partition ID differs across task attempts")
                partition_ids_by_task[logical_task_key] = partition_id
                partition_key = (key[0], partition_id)
                previous_index = partition_to_task_index.get(partition_key)
                if previous_index is not None and previous_index != task_set_index:
                    raise ScopeError("RDD partition ID maps to multiple task-set indices")
                partition_to_task_index[partition_key] = task_set_index
            launch_time = _nonnegative_int(info.get("Launch Time"), "task launch time")
            finish_time = _nonnegative_int(info.get("Finish Time"), "task finish time")
            if finish_time < launch_time:
                raise ScopeError("task completion times are not chronological")
            host = info.get("Host")
            executor_id = info.get("Executor ID")
            if (
                not isinstance(host, str)
                or not host
                or not isinstance(executor_id, str)
                or not executor_id
            ):
                raise ScopeError("task completion is missing executor identity")
            reason_data = event.get("Task End Reason")
            if not isinstance(reason_data, dict) or not isinstance(reason_data.get("Reason"), str):
                raise ScopeError("task completion is missing a result reason")
            if type(info.get("Failed")) is not bool or type(info.get("Killed")) is not bool:
                raise ScopeError("task completion is missing failure flags")
            metrics = event.get("Task Metrics")
            if not isinstance(metrics, dict):
                raise ScopeError("task completion is missing Task Metrics")
            if task_id in task_ids:
                raise ScopeError("event log contains duplicate task completions")
            task_ids.add(task_id)
            reason = reason_data["Reason"]
            task = {
                "stage": key,
                "task_id": task_id,
                # Keep the legacy field's fallback behavior while exposing both
                # Spark identities explicitly to new consumers.
                "partition": partition_id if partition_id is not None else task_set_index,
                "task_set_index": task_set_index,
                "partition_id": partition_id,
                "attempt": attempt,
                "launch_time": launch_time,
                "finish_time": finish_time,
                "host": host,
                "executor_id": executor_id,
                "success": reason == "Success" and not info["Failed"] and not info["Killed"],
                "reason": reason,
                "metrics": metrics,
            }
            record["tasks"].append(task)
            task_attempts.setdefault(logical_task_key, []).append(task)

    if event_kinds[0] != "SparkListenerLogStart" or log_start_count != 1:
        raise ScopeError("event log is missing exactly one SparkListenerLogStart")
    if event_kinds[-1] != "SparkListenerApplicationEnd" or application_end_count != 1:
        raise ScopeError("event log is missing terminal ApplicationEnd")
    if application is None:
        raise ScopeError("event log is missing application start")
    if environment_properties is None:
        raise ScopeError("event log is missing effective environment configuration")
    master = environment_properties.get("spark.master")
    if expected_application_id is not None and application != expected_application_id:
        raise ScopeError("completed event log application identity differs")
    if expected_application_name is not None and application_name != expected_application_name:
        raise ScopeError("completed event log application name differs")
    if master != expected_master:
        raise ScopeError("completed event log Spark master differs")
    if not jobs or any(not job["ended"] or not job["successful"] for job in jobs.values()):
        raise ScopeError("completed event log does not prove successful Spark jobs")
    if not stage_attempts or any(
        not stage["submitted"] or not stage["completed"] for stage in stage_attempts.values()
    ):
        raise ScopeError("completed event log does not prove submitted and completed stages")

    job_stage_ids = {stage_id for job in jobs.values() for stage_id in job["stage_ids"]}
    if any(stage["stage_id"] not in job_stage_ids for stage in stage_attempts.values()):
        raise ScopeError("event log contains an orphan stage attempt")
    final_stage_keys = {}
    for stage_id in job_stage_ids:
        attempts = [key for key in stage_attempts if key[0] == stage_id]
        if not attempts:
            raise ScopeError("job references a stage with no submitted attempt")
        final_key = max(attempts, key=lambda key: key[1])
        final = stage_attempts[final_key]
        if not final["successful"]:
            raise ScopeError("job has no successful final stage attempt")
        final_stage_keys[stage_id] = final_key
    if any(
        final_stage_keys.get(stage_id) is None
        for job in jobs.values()
        for stage_id in job["stage_ids"]
    ):
        raise ScopeError("job stage attribution is incomplete")

    successful_tasks = []
    failed_task_attempts = sum(
        not task["success"] for record in stage_attempts.values() for task in record["tasks"]
    )
    retried_task_attempts = sum(max(0, len(attempts) - 1) for attempts in task_attempts.values())
    failed_task_records = []
    retried_task_records = []
    for attempts in task_attempts.values():
        if len(attempts) > 1:
            retried_task_records.extend(attempts)
        failed_task_records.extend(task for task in attempts if not task["success"])
    for stage in stage_attempts.values():
        attempts_by_index = {}
        for task in stage["tasks"]:
            attempts_by_index.setdefault(task["task_set_index"], []).append(task["attempt"])
        for attempts in attempts_by_index.values():
            numbers = sorted(attempts)
            if numbers != list(range(len(numbers))):
                raise ScopeError("task retry attempt sequence is incomplete")
        if stage["successful"]:
            successful = [task for task in stage["tasks"] if task["success"]]
            task_indices = {task["task_set_index"] for task in successful}
            expected_tasks = stage["expected_tasks"]
            if (
                expected_tasks is None
                or len(successful) != expected_tasks
                or task_indices != set(range(expected_tasks))
            ):
                raise ScopeError(
                    "successful stage task-set index coverage is incomplete (partition coverage)"
                )
            stage["successful_task_indices"] = sorted(task_indices)
            stage["successful_partitions"] = len(task_indices)
            successful_tasks.extend(successful)
    for stage in stage_attempts.values():
        if "successful_partitions" not in stage:
            stage["successful_partitions"] = 0
        if "successful_task_indices" not in stage:
            stage["successful_task_indices"] = []
        stage["failed_task_attempts"] = sum(not task["success"] for task in stage["tasks"])

    def task_record(task):
        return {
            "stage_id": task["stage"][0],
            "stage_attempt_id": task["stage"][1],
            "task_id": task["task_id"],
            "partition": task["partition"],
            "task_set_index": task["task_set_index"],
            "partition_id": task["partition_id"],
            "attempt": task["attempt"],
            "launch_time": task["launch_time"],
            "finish_time": task["finish_time"],
            "host": task["host"],
            "executor_id": task["executor_id"],
            "success": task["success"],
            "reason": task["reason"],
            "metrics": task["metrics"],
        }

    for task in successful_tasks:
        executor = executors.get(task["executor_id"])
        if executor is None or executor["host"] != task["host"]:
            raise ScopeError("successful task executor-host mapping differs")
        if task["host"] not in expected_worker_set:
            raise ScopeError("successful task ran outside the configured Workers")
        metrics = task["metrics"]
        required_metrics = {
            "executor_run_time_ms": metrics.get("Executor Run Time"),
            "gc_time_ms": metrics.get("JVM GC Time"),
            "memory_spill_bytes": metrics.get("Memory Bytes Spilled"),
            "disk_spill_bytes": metrics.get("Disk Bytes Spilled"),
            "input_bytes": metrics.get("Input Metrics", {}).get("Bytes Read"),
            "output_bytes": metrics.get("Output Metrics", {}).get("Bytes Written"),
            "shuffle_read_remote_bytes": metrics.get("Shuffle Read Metrics", {}).get(
                "Remote Bytes Read"
            ),
            "shuffle_read_local_bytes": metrics.get("Shuffle Read Metrics", {}).get(
                "Local Bytes Read"
            ),
            "shuffle_write_bytes": metrics.get("Shuffle Write Metrics", {}).get(
                "Shuffle Bytes Written"
            ),
        }
        if any(type(value) is not int or value < 0 for value in required_metrics.values()):
            raise ScopeError("successful task metrics are incomplete or invalid")
        values = workers.setdefault(
            task["host"],
            dict(
                successful_tasks=0,
                executor_run_time_ms=0,
                gc_time_ms=0,
                memory_spill_bytes=0,
                disk_spill_bytes=0,
                input_bytes=0,
                output_bytes=0,
                shuffle_read_bytes=0,
                shuffle_write_bytes=0,
                elapsed_ms=0,
                executor_ids=set(),
            ),
        )
        values["successful_tasks"] += 1
        if task["executor_id"] is not None:
            values["executor_ids"].add(task["executor_id"])
        for output, key in required_metrics.items():
            if output in {"shuffle_read_remote_bytes", "shuffle_read_local_bytes"}:
                continue
            if output == "shuffle_write_bytes":
                values[output] += key
            else:
                values[output] += key
        values["shuffle_read_bytes"] += (
            required_metrics["shuffle_read_remote_bytes"]
            + required_metrics["shuffle_read_local_bytes"]
        )
        values["elapsed_ms"] += task["finish_time"] - task["launch_time"]
    if set(workers) != expected_worker_set or any(
        values["successful_tasks"] == 0 for values in workers.values()
    ):
        raise ScopeError("completed Standalone execution on both named Workers is not proven")
    for values in workers.values():
        values["executor_ids"] = sorted(values["executor_ids"])
    stage_records = []
    for stage in sorted(
        stage_attempts.values(), key=lambda value: (value["stage_id"], value["stage_attempt_id"])
    ):
        stage_records.append(
            {
                "stage_id": stage["stage_id"],
                "stage_attempt_id": stage["stage_attempt_id"],
                "expected_tasks": stage["expected_tasks"],
                "successful_task_indices": stage["successful_task_indices"],
                "successful_partitions": stage["successful_partitions"],
                "failed_task_attempts": stage["failed_task_attempts"],
                "successful": stage["successful"],
                "failure_reason": stage["failure_reason"],
                "task_attempts": [task_record(task) for task in stage["tasks"]],
            }
        )
    return dict(
        application_id=application,
        application_name=application_name,
        spark_master=master,
        effective_configuration=environment_properties,
        workers=workers,
        executor_host_mappings={key: value["host"] for key, value in executors.items()},
        completed_stage_count=len({stage["stage_id"] for stage in stage_attempts.values()}),
        successful_stage_attempt_count=sum(
            stage["successful"] for stage in stage_attempts.values()
        ),
        submitted_stage_attempt_count=len(stage_attempts),
        failed_stage_attempt_count=sum(
            not stage["successful"] for stage in stage_attempts.values()
        ),
        successful_partition_count=sum(
            stage_attempts[key]["successful_partitions"] for key in final_stage_keys.values()
        ),
        successful_task_count=len(successful_tasks),
        successful_job_count=sum(job["successful"] for job in jobs.values()),
        failed_task_attempt_count=failed_task_attempts,
        retried_task_attempt_count=retried_task_attempts,
        failed_task_records=[task_record(task) for task in failed_task_records],
        retried_task_records=[task_record(task) for task in retried_task_records],
        stage_attempts=stage_records,
        successful_jobs=[job_id for job_id, job in jobs.items() if job["successful"]],
        job_count=len(jobs),
        event_log_path=path,
        event_log_bytes=size,
        event_log_sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id")
    parser.add_argument("--source-snapshot-id")
    parser.add_argument("--expected-application-id")
    parser.add_argument("--expected-application-name")
    parser.add_argument("--expected-master", default=EXPECTED_MASTER)
    parser.add_argument("--expected-event-log-dir")
    args = parser.parse_args()
    client, config = HdfsCliClient(metadata_limit=MAX_EVENT_BYTES), load_hdfs_config()
    report = {}
    if args.pipeline_run_id:
        expected_event_log_dir = (
            args.expected_event_log_dir.removeprefix(config.uri)
            if args.expected_event_log_dir is not None
            else None
        )
        report["execution"] = completed_execution(
            client,
            config,
            args.pipeline_run_id,
            expected_application_id=args.expected_application_id,
            expected_application_name=args.expected_application_name,
            expected_master=args.expected_master,
            expected_event_log_dir=expected_event_log_dir,
        )
    if args.source_snapshot_id:
        report["scope"] = read_scope(
            client, render_curated_sales_daily_path(config.base_path, args.source_snapshot_id)
        )
    if not report:
        parser.error("select a pipeline run and/or source snapshot")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
