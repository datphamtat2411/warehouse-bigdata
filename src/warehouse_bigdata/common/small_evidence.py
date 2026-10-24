"""Read completed run event logs and immutable selection metadata, without Spark."""

import argparse
import hashlib
import json
import re
from collections import defaultdict

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import render_curated_sales_daily_path, render_staging_path
from warehouse_bigdata.common.scope_evidence import read_scope
from warehouse_bigdata.contracts.dataset_scope import ScopeError

# Measured M5 Small validation event log: 1,274,181,933 bytes.
# Retain a finite bound with approximately 26% headroom.
MAX_EVENT_BYTES = 1536 * 1024 * 1024
# The largest measured J06 JSON record was 5,444,499 bytes.
MAX_EVENT_RECORD_BYTES = 8 * 1024 * 1024
STREAM_FALLBACK_CHUNK_BYTES = 1024 * 1024
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
    submitted stage attempts. JobStart snapshots describe a DAG, not necessarily
    a task set: Spark can assign a new stage ID to already available shuffle
    outputs. Unsubmitted references require earlier successful shuffle execution
    of the same root RDD and narrow lineage. They never count as executed work.
    Failed/retried attempts remain explicit in the returned evidence.
    ``Task Info.Index`` is the canonical task-set identity used for coverage.
    ``Partition ID`` is retained as the separate RDD partition identity and is
    optional for legacy event records produced before Spark 3.3; a missing index
    is rejected because task-set coverage would otherwise be ambiguous. The
    legacy ``partition`` report field keeps its prior fallback behavior, while
    ``task_set_index`` and ``partition_id`` expose the two identities explicitly.
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
    if type(size) is not int or not 0 < size <= MAX_EVENT_BYTES:
        raise ScopeError("event log exceeds the bounded evidence limit")
    expected_workers = tuple(expected_workers)
    expected_worker_set = set(expected_workers)
    if not expected_worker_set or len(expected_worker_set) != len(expected_workers):
        raise ScopeError("expected Workers must be distinct and nonempty")
    if not isinstance(expected_master, str) or not expected_master.startswith("spark://"):
        raise ScopeError("expected Spark master must be a Standalone URL")

    executors = {}
    workers = {}
    jobs = {}
    stage_attempts = {}
    stage_definitions = {}
    stage_references = defaultdict(list)
    task_attempts = {}
    task_ids = set()
    task_attempt_ids = set()
    task_start_attempt_ids = set()
    task_start_task_ids = set()
    task_start_records = {}
    partition_ids_by_task = {}
    partition_to_task_index = {}
    event_count = 0
    first_event_kind = None
    last_event_kind = None
    application = None
    application_name = None
    application_end_count = 0
    log_start_count = 0
    environment_properties = None
    application_start_time = None
    application_end_time = None

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
        parent_ids = info.get("Parent IDs", [])
        if (
            not isinstance(parent_ids, list)
            or any(type(parent_id) is not int or parent_id < 0 for parent_id in parent_ids)
            or len(parent_ids) != len(set(parent_ids))
            or key[0] in parent_ids
        ):
            raise ScopeError("Spark stage evidence contains invalid parent IDs")
        lineage = None
        if "RDD Info" in info:
            rdds = info["RDD Info"]
            if not isinstance(rdds, list) or not rdds:
                raise ScopeError("Spark stage evidence has malformed RDD lineage")
            identities = []
            for rdd in rdds:
                if not isinstance(rdd, dict):
                    raise ScopeError("Spark stage evidence has malformed RDD lineage")
                rdd_id = _nonnegative_int(rdd.get("RDD ID"), "RDD ID")
                partitions = _nonnegative_int(rdd.get("Number of Partitions"), "RDD partitions")
                parents = rdd.get("Parent IDs")
                if (
                    not isinstance(parents, list)
                    or any(type(parent) is not int or parent < 0 for parent in parents)
                    or len(parents) != len(set(parents))
                    or rdd_id in parents
                ):
                    raise ScopeError("Spark stage evidence has invalid RDD parent IDs")
                identities.append((rdd_id, partitions, tuple(sorted(parents))))
            if len({identity[0] for identity in identities}) != len(identities):
                raise ScopeError("Spark stage evidence has duplicate RDD identities")
            parents_by_rdd = {identity[0]: set(identity[2]) for identity in identities}
            remaining = set(parents_by_rdd)
            while remaining:
                ready = {rdd_id for rdd_id in remaining if not parents_by_rdd[rdd_id] & remaining}
                if not ready:
                    raise ScopeError("Spark stage evidence has cyclic RDD lineage")
                remaining -= ready
            # StageInfo.fromStage puts stage.rdd first; narrow ancestors are unordered.
            lineage = (identities[0][0], tuple(sorted(identities)))
        definition = (tuple(sorted(parent_ids)), lineage)
        previous = stage_definitions.setdefault(key[0], definition)
        if previous != definition:
            raise ScopeError("Spark stage parents or RDD lineage changed across event records")
        return key, task_count, definition[0], lineage

    def lifecycle_snapshot(info):
        return {
            field: info.get(field)
            for field in ("Number of Tasks", "Submission Time", "Completion Time", "Failure Reason")
        }

    def ensure_stage(key, *, task_count=None, parent_ids=None, lineage=None, job_start_info=None):
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
                "parent_ids": None,
                "job_start_infos": [],
                "submission_position": None,
                "completion_position": None,
                "lineage": lineage,
            },
        )
        if task_count is not None:
            if record["expected_tasks"] not in (None, task_count):
                raise ScopeError("Spark stage task count changed across event records")
            record["expected_tasks"] = task_count
        if parent_ids is not None:
            parent_ids = tuple(parent_ids)
            if record["parent_ids"] not in (None, parent_ids):
                raise ScopeError("Spark stage parent IDs changed across event records")
            record["parent_ids"] = parent_ids
        if job_start_info is not None:
            record["job_start_infos"].append(job_start_info)
        return record

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ScopeError("event log contains duplicate JSON fields")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ScopeError("event log contains non-JSON numeric constants")

    def process_line(raw_line):
        nonlocal application, application_name, application_end_count
        nonlocal application_start_time, application_end_time
        nonlocal event_count, first_event_kind, last_event_kind
        nonlocal environment_properties, log_start_count
        if not raw_line.strip():
            raise ScopeError("event log contains a blank record")
        try:
            event = json.loads(
                raw_line, object_pairs_hook=unique_object, parse_constant=invalid_constant
            )
        except (TypeError, ValueError, RecursionError) as error:
            raise ScopeError("event log contains malformed JSON") from error
        if not isinstance(event, dict):
            raise ScopeError("event log records must be JSON objects")
        kind = event.get("Event")
        if not isinstance(kind, str) or not kind:
            raise ScopeError("event log record is missing its Event identity")
        position = event_count
        event_count += 1
        if first_event_kind is None:
            first_event_kind = kind
        last_event_kind = kind
        if kind == "SparkListenerApplicationStart":
            if application is not None:
                raise ScopeError("event log contains duplicate application starts")
            application = event.get("App ID")
            application_name = event.get("App Name")
            if not isinstance(application, str) or not application:
                raise ScopeError("application start is missing App ID")
            if not isinstance(application_name, str) or not application_name:
                raise ScopeError("application start is missing App Name")
            if "Timestamp" in event:
                application_start_time = _nonnegative_int(
                    event["Timestamp"], "application start time"
                )
        elif kind == "SparkListenerLogStart":
            log_start_count += 1
            if not isinstance(event.get("Spark Version"), str) or not event["Spark Version"]:
                raise ScopeError("event log is missing its Spark version")
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
                "removed": False,
            }
        elif kind == "SparkListenerExecutorRemoved":
            executor = executors.get(event.get("Executor ID"))
            if executor is None or executor["removed"]:
                raise ScopeError("executor removal has no active executor-host mapping")
            executor["removed"] = True
        elif kind == "SparkListenerApplicationEnd":
            application_end_count += 1
            if "Timestamp" in event:
                application_end_time = _nonnegative_int(event["Timestamp"], "application end time")
        elif kind == "SparkListenerEnvironmentUpdate":
            if environment_properties is not None:
                raise ScopeError("event log contains duplicate environment updates")
            properties = event.get("Spark Properties")
            if not isinstance(properties, dict):
                raise ScopeError("environment update is missing Spark Properties")
            environment_properties = properties
        elif kind == "SparkListenerJobStart":
            if application is None:
                raise ScopeError("job start precedes application start")
            job_id = event.get("Job ID")
            _nonnegative_int(job_id, "job ID")
            if job_id in jobs:
                raise ScopeError("event log contains duplicate job starts")
            stage_ids = event.get("Stage IDs")
            infos = event.get("Stage Infos")
            if not isinstance(stage_ids, list):
                raise ScopeError("job start is missing Stage IDs")
            stage_ids = [_nonnegative_int(stage_id, "stage ID") for stage_id in stage_ids]
            if not isinstance(infos, list) or len(infos) != len(stage_ids):
                raise ScopeError("job start stage IDs and infos differ")
            parsed = [stage_info(info) for info in infos]
            if {key[0] for key, _, _, _ in parsed} != set(stage_ids) or len(set(stage_ids)) != len(
                stage_ids
            ):
                raise ScopeError("job start stage IDs and infos differ")
            stage_info_by_id = {
                key[0]: {
                    "stage_attempt_id": key[1],
                    "task_count": task_count,
                    "parent_ids": parent_ids,
                    "lineage": lineage,
                    "snapshot": lifecycle_snapshot(snapshot),
                }
                for snapshot, (key, task_count, parent_ids, lineage) in zip(
                    infos, parsed, strict=True
                )
            }
            jobs[job_id] = {
                "ended": False,
                "successful": False,
                "result": None,
                "stage_ids": tuple(stage_ids),
                "stage_infos": stage_info_by_id,
                "end_position": None,
                "start_position": position,
            }
            for info, (key, _, parent_ids, lineage) in zip(infos, parsed, strict=True):
                stage_references[key[0]].append(job_id)
                ensure_stage(
                    key,
                    parent_ids=parent_ids,
                    lineage=lineage,
                    job_start_info=lifecycle_snapshot(info),
                )
        elif kind == "SparkListenerStageSubmitted":
            info = event.get("Stage Info")
            key, task_count, parent_ids, lineage = stage_info(info)
            if not any(not jobs[job_id]["ended"] for job_id in stage_references[key[0]]):
                raise ScopeError("event log contains an orphan stage submission")
            record = ensure_stage(
                key, task_count=task_count, parent_ids=parent_ids, lineage=lineage
            )
            if record["submitted"]:
                raise ScopeError("event log contains duplicate stage submissions")
            record["submitted"] = True
            record["submission_position"] = position
            record["submission_info"] = lifecycle_snapshot(info)
            for field in ("Submission Time", "Completion Time"):
                if field in info and info[field] is not None:
                    _nonnegative_int(info[field], field)
            failure = info.get("Failure Reason")
            if failure is not None and not isinstance(failure, str):
                raise ScopeError("stage submission has malformed failure reason")
            properties = event.get("Properties", {})
            if not isinstance(properties, dict):
                raise ScopeError("stage submission has malformed Properties")
        elif kind == "SparkListenerStageCompleted":
            info = event.get("Stage Info")
            key, task_count, parent_ids, lineage = stage_info(info)
            record = stage_attempts.get(key)
            if record is None or not record["submitted"]:
                raise ScopeError("stage completion has no submitted stage attempt")
            if record["completed"]:
                raise ScopeError("event log contains duplicate stage completions")
            ensure_stage(key, task_count=task_count, parent_ids=parent_ids, lineage=lineage)
            record["completed"] = True
            record["completion_position"] = position
            record["completion_info"] = lifecycle_snapshot(info)
            for field in ("Submission Time", "Completion Time"):
                if field in info:
                    _nonnegative_int(info[field], field)
            submission_time = record["submission_info"].get("Submission Time")
            if submission_time is not None and (
                info.get("Submission Time") != submission_time
                or info.get("Completion Time", submission_time) < submission_time
            ):
                raise ScopeError("stage completion has contradictory lifecycle times")
            if (
                record["submission_info"].get("Completion Time") is not None
                and info.get("Completion Time") != record["submission_info"]["Completion Time"]
            ):
                raise ScopeError("stage completion has contradictory lifecycle times")
            if (
                record["submission_info"].get("Failure Reason") is not None
                and info.get("Failure Reason") != record["submission_info"]["Failure Reason"]
            ):
                raise ScopeError("stage completion has contradictory lifecycle evidence")
            failure = info.get("Failure Reason")
            if failure is not None and not isinstance(failure, str):
                raise ScopeError("stage completion has malformed failure reason")
            record["successful"] = failure is None
            record["failure_reason"] = failure
        elif kind == "SparkListenerJobEnd":
            job_id = event.get("Job ID")
            _nonnegative_int(job_id, "job ID")
            if job_id not in jobs:
                raise ScopeError("job completion has no submitted job")
            if jobs[job_id]["ended"]:
                raise ScopeError("event log contains duplicate job completions")
            result_data = event.get("Job Result")
            if not isinstance(result_data, dict) or not isinstance(result_data.get("Result"), str):
                raise ScopeError("job completion is missing a result")
            result = result_data["Result"]
            jobs[job_id].update(
                ended=True,
                successful=result == "JobSucceeded",
                result=result,
                end_position=position,
            )
        elif kind == "SparkListenerTaskStart":
            key = stage_key(event)
            record = stage_attempts.get(key)
            if record is None or not record["submitted"]:
                raise ScopeError("task start has no submitted stage attempt")
            if record["completed"]:
                raise ScopeError("task start follows stage completion")
            info = event.get("Task Info")
            if not isinstance(info, dict):
                raise ScopeError("task start is missing Task Info")
            task_id = _nonnegative_int(info.get("Task ID"), "task ID")
            if "Index" not in info:
                raise ScopeError("task start is missing task-set index")
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
                raise ScopeError("task start follows task completion")
            if task_attempt_key in task_start_attempt_ids:
                raise ScopeError("event log contains duplicate logical task starts")
            if task_id in task_start_task_ids:
                raise ScopeError("event log contains duplicate task starts")
            task_start_attempt_ids.add(task_attempt_key)
            task_start_task_ids.add(task_id)
            logical_task_key = (key, task_set_index)
            if partition_id is not None:
                previous_partition_id = partition_ids_by_task.get(logical_task_key)
                if previous_partition_id is not None and previous_partition_id != partition_id:
                    raise ScopeError("RDD partition ID differs across task attempts")
                partition_ids_by_task[logical_task_key] = partition_id
                partition_key = (key, partition_id)
                previous_index = partition_to_task_index.get(partition_key)
                if previous_index is not None and previous_index != task_set_index:
                    raise ScopeError("RDD partition ID maps to multiple task-set indices")
                partition_to_task_index[partition_key] = task_set_index
                if record["lineage"] is not None:
                    root_id, rdds = record["lineage"]
                    root_partitions = next(rdd[1] for rdd in rdds if rdd[0] == root_id)
                    if partition_id >= root_partitions:
                        raise ScopeError("RDD partition ID is outside the stage RDD")
            launch_time = _nonnegative_int(info.get("Launch Time"), "task launch time")
            host = info.get("Host")
            executor_id = info.get("Executor ID")
            if (
                not isinstance(host, str)
                or not host
                or not isinstance(executor_id, str)
                or not executor_id
            ):
                raise ScopeError("task start is missing executor identity")
            executor = executors.get(executor_id)
            if executor is None or executor["removed"] or executor["host"] != host:
                raise ScopeError("task start executor-host mapping differs")
            task_start_records[task_attempt_key] = {
                "task_id": task_id,
                "partition_id": partition_id,
                "launch_time": launch_time,
                "host": host,
                "executor_id": executor_id,
            }
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
            task_start = task_start_records.get(task_attempt_key)
            if task_start is None:
                raise ScopeError("task completion has no matching task start")
            logical_task_key = (key, task_set_index)
            if partition_id is not None:
                previous_partition_id = partition_ids_by_task.get(logical_task_key)
                if previous_partition_id is not None and previous_partition_id != partition_id:
                    raise ScopeError("RDD partition ID differs across task attempts")
                partition_ids_by_task[logical_task_key] = partition_id
                partition_key = (key, partition_id)
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
            if task_start != {
                "task_id": task_id,
                "partition_id": partition_id,
                "launch_time": launch_time,
                "host": host,
                "executor_id": executor_id,
            }:
                raise ScopeError("task start and completion identities differ")
            reason_data = event.get("Task End Reason")
            if not isinstance(reason_data, dict) or not isinstance(reason_data.get("Reason"), str):
                raise ScopeError("task completion is missing a result reason")
            if type(info.get("Failed")) is not bool or type(info.get("Killed")) is not bool:
                raise ScopeError("task completion is missing failure flags")
            metrics = event.get("Task Metrics")
            # JsonProtocol omits metrics when a failed task never supplied them.
            # Retain absence, but never waive metrics for successful work.
            if (metrics is not None and not isinstance(metrics, dict)) or (
                reason_data["Reason"] == "Success" and metrics is None
            ):
                raise ScopeError("task completion is missing Task Metrics")
            if task_id in task_ids:
                raise ScopeError("event log contains duplicate task completions")
            task_ids.add(task_id)
            reason = reason_data["Reason"]
            if reason == "Success" and (info["Failed"] or info["Killed"]):
                raise ScopeError("successful task completion has contradictory failure flags")
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
                "end_position": position,
                "task_type": event.get("Task Type"),
            }
            record["tasks"].append(task)
            # A task-set index is local to an attempt, not to the stage ID. Retry
            # attribution across stage attempts uses RDD partition identity instead.
            retry_key = (
                (key[0], "partition", partition_id)
                if partition_id is not None
                else (key, "index", task_set_index)
            )
            task_attempts.setdefault(retry_key, []).append(task)

    line_buffer = bytearray()
    line_count = 0
    last_byte = None

    def consume_chunk(chunk):
        nonlocal last_byte, line_count
        if not isinstance(chunk, bytes) or len(chunk) > MAX_EVENT_RECORD_BYTES + 1:
            raise ScopeError("event log transfer chunk exceeds the bounded record limit")
        if chunk:
            last_byte = chunk[-1]
            line_buffer.extend(chunk)
        while True:
            newline = line_buffer.find(b"\n")
            if newline < 0:
                if len(line_buffer) > MAX_EVENT_RECORD_BYTES:
                    raise ScopeError("event log record exceeds the bounded record limit")
                return
            raw_line = bytes(line_buffer[:newline])
            del line_buffer[: newline + 1]
            if len(raw_line) > MAX_EVENT_RECORD_BYTES:
                raise ScopeError("event log record exceeds the bounded record limit")
            line_count += 1
            process_line(raw_line)

    stream_remote = getattr(client, "stream_remote", None)
    if callable(stream_remote):
        streamed = stream_remote(path, consume_chunk, max_bytes=MAX_EVENT_BYTES)
        streamed_size = (
            streamed.size_bytes if hasattr(streamed, "size_bytes") else streamed["size_bytes"]
        )
        streamed_sha256 = streamed.sha256 if hasattr(streamed, "sha256") else streamed["sha256"]
    else:
        # Existing in-memory test doubles predate the production streaming API.
        payload = client.read_remote(path)
        if not isinstance(payload, bytes) or len(payload) > MAX_EVENT_BYTES:
            raise ScopeError("event log read exceeds the bounded evidence limit")
        for offset in range(0, len(payload), STREAM_FALLBACK_CHUNK_BYTES):
            consume_chunk(payload[offset : offset + STREAM_FALLBACK_CHUNK_BYTES])
        streamed_size = len(payload)
        streamed_sha256 = "sha256:" + hashlib.sha256(payload).hexdigest()

    after = client.stat(path)
    after_size = after.size_bytes if hasattr(after, "size_bytes") else after["size_bytes"]
    if streamed_size != size or after_size != size:
        raise ScopeError("event log changed during readback")
    if not (
        isinstance(streamed_sha256, str)
        and re.fullmatch(r"sha256:[0-9a-f]{64}", streamed_sha256) is not None
    ):
        raise ScopeError("event log readback did not provide a complete SHA-256")
    if line_count == 0 or line_buffer or last_byte != ord("\n"):
        raise ScopeError("event log is truncated or missing its terminal newline")

    if task_start_attempt_ids - task_attempt_ids:
        raise ScopeError("task start has no matching task completion")
    if event_count == 0 or first_event_kind != "SparkListenerLogStart" or log_start_count != 1:
        raise ScopeError("event log is missing exactly one SparkListenerLogStart")
    if last_event_kind != "SparkListenerApplicationEnd" or application_end_count != 1:
        raise ScopeError("event log is missing terminal ApplicationEnd")
    if application is None:
        raise ScopeError("event log is missing application start")
    if (
        application_start_time is not None
        and application_end_time is not None
        and application_end_time < application_start_time
    ):
        raise ScopeError("application completion times are not chronological")
    if environment_properties is None:
        raise ScopeError("event log is missing effective environment configuration")
    master = environment_properties.get("spark.master")
    if expected_application_id is not None and application != expected_application_id:
        raise ScopeError("completed event log application identity differs")
    if expected_application_name is not None and application_name != expected_application_name:
        raise ScopeError("completed event log application name differs")
    if master != expected_master:
        raise ScopeError("completed event log Spark master differs")
    for field, identity in (("spark.app.id", application), ("spark.app.name", application_name)):
        if field in environment_properties and environment_properties[field] != identity:
            raise ScopeError("effective configuration application identity differs")
    if not jobs or any(not job["ended"] or not job["successful"] for job in jobs.values()):
        raise ScopeError("completed event log does not prove successful Spark jobs")

    def validate_job_stage_graph(job):
        stage_ids = set(job["stage_ids"])
        for info in job["stage_infos"].values():
            if not set(info["parent_ids"]) <= stage_ids:
                raise ScopeError("job stage snapshot contains an orphan parent stage")
        remaining = set(stage_ids)
        while remaining:
            ready = {
                stage_id
                for stage_id in remaining
                if not (set(job["stage_infos"][stage_id]["parent_ids"]) & remaining)
            }
            if not ready:
                raise ScopeError("job stage snapshot contains a cyclic DAG")
            remaining -= ready

        parents = {parent for info in job["stage_infos"].values() for parent in info["parent_ids"]}
        if stage_ids and len(stage_ids - parents) != 1:
            raise ScopeError("job stage snapshot is not a connected single-result DAG")

    for job in jobs.values():
        validate_job_stage_graph(job)

    planned_only_stage_keys = set()
    for key, stage in stage_attempts.items():
        if stage["submitted"]:
            continue
        if stage["completed"] or stage["tasks"] or not stage["job_start_infos"]:
            raise ScopeError("unsubmitted stage attempt has execution evidence")
        if key[1] != 0:
            raise ScopeError("planned-only stage snapshot has a noninitial attempt")
        for info in stage["job_start_infos"]:
            if any(
                info.get(field) is not None
                for field in ("Submission Time", "Completion Time", "Failure Reason")
            ):
                raise ScopeError("planned-only stage snapshot contains execution evidence")
        planned_only_stage_keys.add(key)

    submitted_stage_attempts = {
        key: stage for key, stage in stage_attempts.items() if stage["submitted"]
    }
    if not submitted_stage_attempts or any(
        not stage["completed"] for stage in submitted_stage_attempts.values()
    ):
        raise ScopeError("completed event log does not prove submitted and completed stages")

    submitted_by_stage = defaultdict(list)
    for key, stage in submitted_stage_attempts.items():
        submitted_by_stage[key[0]].append(stage)
        if not any(
            jobs[job_id]["start_position"]
            < stage["submission_position"]
            < stage["completion_position"]
            < jobs[job_id]["end_position"]
            for job_id in stage_references[key[0]]
        ):
            raise ScopeError("submitted stage has no completed owning job lifecycle")
    final_stage_keys = {}
    for stage_id, stages in submitted_by_stage.items():
        stages.sort(key=lambda stage: stage["submission_position"])
        first_attempt = 1 if (stage_id, 0) in planned_only_stage_keys else 0
        if [stage["stage_attempt_id"] for stage in stages] != list(
            range(first_attempt, first_attempt + len(stages))
        ):
            raise ScopeError("stage retry attempt sequence is incomplete or contradictory")
        final = stages[-1]
        if not final["successful"]:
            raise ScopeError("job has no successful final stage attempt")
        final_stage_keys[stage_id] = (stage_id, final["stage_attempt_id"])

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
    for stage in submitted_stage_attempts.values():
        attempts_by_index = {}
        for task in stage["tasks"]:
            attempts_by_index.setdefault(task["task_set_index"], []).append(task["attempt"])
        for attempts in attempts_by_index.values():
            numbers = sorted(attempts)
            if numbers != list(range(len(numbers))):
                raise ScopeError("task retry attempt sequence is incomplete")
        if stage["successful"]:
            successful = [task for task in stage["tasks"] if task["success"]]
            task_indices = {
                task["task_set_index"]
                for task in successful
                if task["end_position"] < stage["completion_position"]
            }
            expected_tasks = stage["expected_tasks"]
            if expected_tasks is None or task_indices != set(range(expected_tasks)):
                raise ScopeError(
                    "successful stage task-set index coverage is incomplete (partition coverage)"
                )
            stage["successful_task_indices"] = sorted(task_indices)
            stage["successful_partitions"] = len(task_indices)
            successful_tasks.extend(successful)
    for stage in submitted_stage_attempts.values():
        if "successful_partitions" not in stage:
            stage["successful_partitions"] = 0
        if "successful_task_indices" not in stage:
            stage["successful_task_indices"] = []
        stage["failed_task_attempts"] = sum(not task["success"] for task in stage["tasks"])

    shuffle_sources = defaultdict(list)
    for stage in submitted_stage_attempts.values():
        if stage["successful"] and stage["lineage"] is not None:
            successful = [
                task
                for task in stage["tasks"]
                if task["success"] and task["end_position"] < stage["completion_position"]
            ]
            if all(task["task_type"] == "ShuffleMapTask" for task in successful):
                shuffle_sources[stage["lineage"]].append((stage, successful))

    def prior_shuffle_execution(info, before):
        lineage = info["lineage"]
        if lineage is None:
            raise ScopeError("planned shuffle reuse is missing its RDD lineage")
        root_id, rdds = lineage
        partitions = next(rdd[1] for rdd in rdds if rdd[0] == root_id)
        sources = [
            (stage, tasks)
            for stage, tasks in shuffle_sources[lineage]
            if stage["completion_position"] < before
        ]
        covered = {task["partition_id"] for _, tasks in sources for task in tasks}
        if partitions == 0:
            if info.get("task_count", partitions) != 0 or sources or covered:
                raise ScopeError("zero-partition planned stage has execution evidence")
            return set()
        if not sources or covered != set(range(partitions)):
            raise ScopeError("planned stage reuse lacks prior complete successful shuffle evidence")
        return {(stage["stage_id"], stage["stage_attempt_id"]) for stage, _ in sources}

    planned_reuse_sources = defaultdict(set)
    planned_jobs = defaultdict(set)
    for job_id, job in jobs.items():
        for stage_id, info in job["stage_infos"].items():
            key = (stage_id, info["stage_attempt_id"])
            stage = submitted_stage_attempts.get(key)
            if key in planned_only_stage_keys:
                if (
                    info["task_count"]
                    != stage_attempts[key]["job_start_infos"][0]["Number of Tasks"]
                ):
                    raise ScopeError("planned shuffle task count changed across snapshots")
                lineage = info["lineage"]
                if lineage is not None:
                    root_id, rdds = lineage
                    if info["task_count"] != next(rdd[1] for rdd in rdds if rdd[0] == root_id):
                        raise ScopeError("planned shuffle task count differs from its root RDD")
                planned_reuse_sources[key].update(
                    prior_shuffle_execution(info, job["start_position"])
                )
                planned_jobs[key].add(job_id)
            if stage is not None:
                snapshot = info["snapshot"]
                if stage["submission_position"] < job["start_position"]:
                    if info["task_count"] != stage["expected_tasks"]:
                        raise ScopeError("job snapshot contradicts submitted stage task count")
                elif any(
                    snapshot.get(field) is not None
                    for field in ("Submission Time", "Completion Time", "Failure Reason")
                ):
                    raise ScopeError("job snapshot contains future stage execution evidence")
                # A snapshot may precede submission or describe an already running /
                # completed attempt. Any lifecycle fields it carries must agree.
                for field in ("Submission Time", "Completion Time", "Failure Reason"):
                    if snapshot.get(field) is not None and snapshot[field] != stage[
                        "completion_info"
                    ].get(field):
                        raise ScopeError("job snapshot contradicts stage execution lifecycle")
            candidates = [
                attempt
                for attempt in submitted_by_stage[stage_id]
                if attempt["successful"]
                and attempt["stage_attempt_id"] >= info["stage_attempt_id"]
                and attempt["completion_position"] < job["end_position"]
            ]
            if not candidates and key not in planned_only_stage_keys:
                raise ScopeError("job completion precedes successful stage completion")

    for stage in submitted_stage_attempts.values():
        for parent_id in stage["parent_ids"]:
            if any(
                parent["successful"]
                and parent["completion_position"] < stage["submission_position"]
                for parent in submitted_by_stage[parent_id]
            ):
                continue
            parent_key = (parent_id, 0)
            if parent_key not in planned_only_stage_keys:
                raise ScopeError("stage submission precedes successful parent dependency")
            parent = stage_attempts[parent_key]
            prior_shuffle_execution({"lineage": parent["lineage"]}, stage["submission_position"])

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
            "task_type": task["task_type"],
        }

    for task in successful_tasks:
        executor = executors.get(task["executor_id"])
        if executor is None or executor["host"] != task["host"]:
            raise ScopeError("successful task executor-host mapping differs")
        if task["host"] not in expected_worker_set:
            raise ScopeError("successful task ran outside the configured Workers")
        metrics = task["metrics"]
        if any(
            not isinstance(metrics.get(name), dict)
            for name in (
                "Input Metrics",
                "Output Metrics",
                "Shuffle Read Metrics",
                "Shuffle Write Metrics",
            )
        ):
            raise ScopeError("successful task metrics are incomplete or invalid")
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
        submitted_stage_attempts.values(),
        key=lambda value: (value["stage_id"], value["stage_attempt_id"]),
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
                "parent_ids": list(stage["parent_ids"]),
                "task_attempts": [task_record(task) for task in stage["tasks"]],
            }
        )
    planned_only_stage_records = [
        {
            "stage_id": key[0],
            "stage_attempt_id": key[1],
            "expected_tasks": stage_attempts[key]["job_start_infos"][0]["Number of Tasks"],
            "parent_ids": list(stage_attempts[key]["parent_ids"]),
            "root_rdd_id": stage_attempts[key]["lineage"][0],
            "referenced_by_jobs": sorted(planned_jobs[key]),
            "reuse_sources": [
                {"stage_id": stage_id, "stage_attempt_id": attempt_id}
                for stage_id, attempt_id in sorted(planned_reuse_sources[key])
            ],
        }
        for key in sorted(planned_only_stage_keys)
    ]
    return dict(
        application_id=application,
        application_name=application_name,
        spark_master=master,
        effective_configuration=environment_properties,
        workers=workers,
        executor_host_mappings={key: value["host"] for key, value in executors.items()},
        completed_stage_count=len(
            {stage["stage_id"] for stage in submitted_stage_attempts.values()}
        ),
        successful_stage_attempt_count=sum(
            stage["successful"] for stage in submitted_stage_attempts.values()
        ),
        submitted_stage_attempt_count=len(submitted_stage_attempts),
        failed_stage_attempt_count=sum(
            not stage["successful"] for stage in submitted_stage_attempts.values()
        ),
        planned_only_stage_attempt_count=len(planned_only_stage_records),
        planned_only_stage_attempts=planned_only_stage_records,
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
        event_log_sha256=streamed_sha256,
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
    client = HdfsCliClient(
        metadata_limit=MAX_EVENT_BYTES,
        command_timeout=1800,
    )
    config = load_hdfs_config()
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
