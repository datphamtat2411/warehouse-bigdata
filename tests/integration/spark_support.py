"""Independent, opt-in M02 T03 coordinator over the existing HDFS/T02 interfaces."""

import hashlib
import json
import re
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path

from tests.integration.hdfs_support import (
    IO_TIMEOUT,
    ROOT,
    HDFSVerification,
    VerificationError,
    parse_fsck,
    require,
)
from warehouse_bigdata.common.spark_verification import (
    HDFS_ENDPOINTS,
    HDFS_URI,
    MASTER,
    RECORD_PREFIX,
    WORKERS,
    spark_settings,
    validate_request,
)

MAX_EVENT_BYTES = 16 * 1024 * 1024
# M04 J03's completed 30-day log measured 26,143,896 bytes (2026-10-22).
# Keep larger multi-query M04 evidence bounded independently of the T03 fixture.
MAX_M04_EVENT_BYTES = 64 * 1024 * 1024
MAX_DATA_BYTES = 1024 * 1024
DRIVER_LABEL = "io.warehouse-bigdata.spark-submit.owner"
SPARK_SERVICES = ("spark-master", *WORKERS)
PHASES = ("input-probe", "transform-write", "parquet-reread")
SCHEMA = {
    "bucket": "int",
    "record_count": "bigint",
    "quantity_total": "bigint",
    "amount_cents": "bigint",
}


def synthetic_files() -> dict[str, bytes]:
    """Four fixed JSONL files, with no dependency on Spark or business source data."""
    files = {}
    for part in range(4):
        records = [
            {
                "record_id": index,
                "bucket": index % 8,
                "quantity": index % 5 + 1,
                "unit_price_cents": 100 + index % 17 * 7,
            }
            for index in range(part * 256, (part + 1) * 256)
        ]
        files[f"part-{part:02d}.jsonl"] = (
            "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n"
        ).encode()
    return files


def expected_rows() -> list[dict]:
    """Host-only integer oracle; never executes or calls the Spark transformation."""
    result = []
    for bucket in range(8):
        indices = [index for index in range(bucket, 1024, 8) if index % 3 != 0]
        result.append(
            {
                "bucket": bucket,
                "record_count": len(indices),
                "quantity_total": sum(index % 5 + 1 for index in indices),
                "amount_cents": sum(
                    (index % 5 + 1) * (100 + 7 * (index % 17)) for index in indices
                ),
            }
        )
    return result


def assert_correctness(result: dict) -> None:
    require(
        result.get("schema") == SCHEMA, "Actual Parquet field types differ from the integer schema."
    )
    for field, expected in (
        ("input_count", 1024),
        ("row_count", 8),
        ("unique_keys", 8),
        ("null_count", 0),
        ("contributing_records", 682),
    ):
        require(
            type(result.get(field)) is int and result[field] == expected,
            f"Actual Parquet/input count mismatch: {field}",
        )
    rows = result.get("rows")
    require(isinstance(rows, list) and len(rows) == 8, "Expected eight actual reread rows.")
    require(
        all(
            isinstance(row, dict)
            and row.keys() == SCHEMA.keys()
            and all(type(value) is int for value in row.values())
            for row in rows
        ),
        "Actual Parquet rows contain missing fields, nulls, or non-integer values.",
    )
    require(
        sorted(rows, key=lambda row: row["bucket"]) == expected_rows(),
        "Actual Parquet reread differs from the independent Python oracle.",
    )


def assert_probes(result: dict, probe_bytes: bytes) -> None:
    expected = {
        "bytes": len(probe_bytes),
        "sha256": hashlib.sha256(probe_bytes).hexdigest(),
        "endpoints": list(HDFS_ENDPOINTS),
    }
    driver = result.get("driver_probe", {})
    require(
        all(driver.get(key) == value for key, value in expected.items()),
        "Driver did not prove fresh HDFS bytes and both transfer endpoints.",
    )
    probes = result.get("probes")
    require(
        isinstance(probes, list) and len(probes) == 2, "Expected exactly two barrier task probes."
    )
    for probe in probes:
        require(
            all(probe.get(key) == value for key, value in expected.items()),
            "Executor HDFS byte/endpoint evidence differs from the host input.",
        )
        for key in (
            "partition_id",
            "task_attempt_id",
            "task_attempt_number",
            "stage_id",
            "rows_consumed",
        ):
            require(
                type(probe.get(key)) is int and probe[key] >= 0,
                f"Invalid barrier task identity/count: {key}",
            )
        require(probe["rows_consumed"] > 0, "Barrier task consumed no fixture rows.")
    require(
        {probe.get("host") for probe in probes} == set(WORKERS),
        "Both distinct expected Worker hosts must execute barrier tasks.",
    )
    require(
        {probe["partition_id"] for probe in probes} == {0, 1}
        and len({probe["task_attempt_id"] for probe in probes}) == 2
        and len({probe["stage_id"] for probe in probes}) == 1,
        "Duplicate or mismatched barrier partition/task identities.",
    )
    require(
        sum(probe["rows_consumed"] for probe in probes) == 1024,
        "Barrier tasks did not consume the complete synthetic fixture.",
    )
    identities = [
        {key: value for key, value in probe.items() if key != "peers"} for probe in probes
    ]
    for probe in probes:
        peers = probe.get("peers")
        require(
            isinstance(peers, list)
            and len(peers) == 2
            and all(isinstance(peer, dict) for peer in peers),
            "Missing barrier allGather evidence.",
        )
        require(
            sorted(peers, key=lambda peer: peer.get("partition_id", -1))
            == sorted(identities, key=lambda peer: peer["partition_id"]),
            "Barrier tasks disagree about their allGather participation.",
        )


def read_records(stdout: bytes, run_id: str, *, complete: bool = True) -> dict[str, dict]:
    require(len(stdout) <= 2 * 1024 * 1024, "Oversized structured application stdout.")
    records = {}
    for line in stdout.decode(errors="strict").splitlines():
        if not line.startswith(RECORD_PREFIX):
            continue
        try:
            record = json.loads(line[len(RECORD_PREFIX) :])
        except ValueError as error:
            raise VerificationError("Malformed structured application record.") from error
        require(
            isinstance(record, dict) and record.get("run_id") == run_id,
            "Structured application run identity mismatch.",
        )
        kind = record.get("kind")
        require(
            kind in ("startup", "application", "result") and kind not in records,
            "Unknown or duplicate structured application record.",
        )
        records[kind] = record
    if complete:
        require(
            set(records) == {"startup", "application", "result"},
            "Missing startup/application/result evidence.",
        )
    return records


def assert_sources(startup: dict, expected: dict, expected_xml: dict) -> None:
    for field in ("sources", "baked_sources", "archive_sources"):
        require(
            startup.get(field) == expected,
            f"Driver {field} are stale or differ from the expected checkout; rebuild through T02.",
        )
    require(startup.get("xml_sha256") == expected_xml, "Driver XML hashes differ from current T01.")
    require(
        re.fullmatch(r"[0-9a-f]{64}", startup.get("archive_sha256", "")),
        "Missing actual project archive hash.",
    )


def validate_active_cluster(snapshot: dict, application_id: str, app_name: str) -> dict[str, str]:
    """Parse Spark 3.5.7 deploy.JsonProtocol; Worker executors have appdesc, not state."""
    master = snapshot.get("master", {})
    require(
        master.get("url") == MASTER and master.get("status") == "ALIVE",
        "Active evidence is not from the current Standalone Master.",
    )
    alive = [worker for worker in master.get("workers", []) if worker.get("state") == "ALIVE"]
    require(
        len(alive) == 2 and {worker.get("host") for worker in alive} == set(WORKERS),
        "Active Master evidence must contain the two expected Workers.",
    )
    require(len({worker.get("id") for worker in alive}) == 2, "Duplicate active Worker IDs.")
    apps = [app for app in master.get("activeapps", []) if app.get("id") == application_id]
    require(
        len(apps) == 1 and apps[0].get("name") == app_name and apps[0].get("state") == "RUNNING",
        "Matching application was not active during the Worker snapshot.",
    )
    placements = {}
    for registered in alive:
        host = registered["host"]
        local = snapshot.get("workers", {}).get(host, {})
        require(
            local.get("id") == registered.get("id") and local.get("masterurl") == MASTER,
            f"Current Worker identity mismatch: {host}",
        )
        require(registered.get("port") == 7078, f"Unexpected Worker RPC port: {host}")
        for field, value in (("cores", 1), ("memory", 1024)):
            require(
                registered.get(field) == local.get(field) == value,
                f"Active Worker resources differ from T01: {host}/{field}",
            )
        heartbeat, captured = registered.get("lastheartbeat"), snapshot.get("captured_at_ms")
        require(
            type(heartbeat) is int
            and type(captured) is int
            and -5000 <= captured - heartbeat <= 30000,
            f"Stale active Worker registration: {host}",
        )
        executors = [
            executor
            for executor in local.get("executors", [])
            if executor.get("appid") == application_id
        ]
        require(len(executors) == 1, f"No unique active application executor on {host}.")
        executor = executors[0]
        require(
            executor.get("memory") == 512 and executor.get("appdesc", {}).get("name") == app_name,
            f"Active executor application/resource mismatch on {host}.",
        )
        identifier = str(executor.get("id", ""))
        require(
            identifier.isdecimal() and identifier not in placements,
            "Duplicate/invalid active executor ID.",
        )
        placements[identifier] = host
    return placements


def parse_events(payload: bytes, *, max_bytes: int = MAX_EVENT_BYTES) -> list[dict]:
    require(0 < len(payload) <= max_bytes, "Empty or oversized Spark event-log evidence.")
    require(payload.endswith(b"\n"), "Truncated Spark event-log evidence.")
    try:
        events = [json.loads(line) for line in payload.splitlines()]
    except (ValueError, UnicodeError) as error:
        raise VerificationError("Malformed or incomplete Spark event log.") from error
    require(
        all(isinstance(event, dict) and isinstance(event.get("Event"), str) for event in events),
        "Malformed Spark listener records.",
    )
    require(
        events[0]["Event"] == "SparkListenerLogStart", "Missing Spark event format/version header."
    )
    require(
        events[-1]["Event"] == "SparkListenerApplicationEnd",
        "Missing complete application termination.",
    )
    for kind in (
        "SparkListenerLogStart",
        "SparkListenerApplicationStart",
        "SparkListenerApplicationEnd",
    ):
        require(
            sum(event["Event"] == kind for event in events) == 1,
            f"Missing or duplicate application evidence: {kind}",
        )
    require(events[0].get("Spark Version") == "3.5.7", "Expected pinned Spark 3.5.7 event format.")
    return events


def summarize_event_log(
    payload: bytes,
    *,
    run_id: str,
    application_id: str,
    fixture_root: str,
    probes: list[dict],
    active_cluster: dict,
) -> dict:
    events = parse_events(payload)
    app_name = f"warehouse-spark-hdfs-{run_id}"
    placements = validate_active_cluster(active_cluster, application_id, app_name)
    application = next(
        event for event in events if event["Event"] == "SparkListenerApplicationStart"
    )
    require(
        application.get("App ID") == application_id and application.get("App Name") == app_name,
        "Event log application/run identity mismatch.",
    )
    environments = [event for event in events if event["Event"] == "SparkListenerEnvironmentUpdate"]
    require(len(environments) == 1, "Missing/duplicate effective application configuration.")
    properties = environments[0].get("Spark Properties", {})
    expected = {
        **spark_settings(fixture_root),
        "spark.master": MASTER,
        "spark.app.name": app_name,
        "spark.app.id": application_id,
        "spark.default.parallelism": "2",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.adaptive.enabled": "false",
        "spark.cores.max": "2",
        "spark.executor.cores": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.dynamicAllocation.enabled": "false",
    }
    for key, value in expected.items():
        require(properties.get(key) == value, f"Effective Spark configuration mismatch: {key}")

    executors, submitted, completed, jobs, job_ends, tasks = {}, {}, {}, {}, {}, {}
    for event in events:
        kind = event["Event"]
        if kind == "SparkListenerExecutorAdded":
            identifier, info = event["Executor ID"], event["Executor Info"]
            require(
                identifier not in executors
                and info.get("Host") in WORKERS
                and info.get("Total Cores") == 1,
                "Invalid/duplicate executor addition.",
            )
            executors[identifier] = info
        elif kind == "SparkListenerJobStart":
            identifier = event["Job ID"]
            require(identifier not in jobs, "Duplicate job start.")
            jobs[identifier] = event
        elif kind == "SparkListenerJobEnd":
            identifier = event["Job ID"]
            require(identifier not in job_ends, "Duplicate job end.")
            job_ends[identifier] = event
        elif kind in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
            info = event["Stage Info"]
            key = info["Stage ID"], info["Stage Attempt ID"]
            target = submitted if kind == "SparkListenerStageSubmitted" else completed
            require(key not in target, "Duplicate stage-attempt record.")
            target[key] = event
        elif kind == "SparkListenerTaskEnd":
            info = event["Task Info"]
            identifier = info["Task ID"]
            require(identifier not in tasks, "Duplicate task-attempt completion.")
            tasks[identifier] = event

    require(set(placements) <= executors.keys(), "Active executors have no event-log additions.")
    require(jobs and jobs.keys() == job_ends.keys(), "Missing job completion evidence.")
    require(
        submitted and submitted.keys() == completed.keys(),
        "Missing stage-attempt completion evidence.",
    )
    groups = {f"{run_id}:{phase}" for phase in PHASES}
    job_groups = {job.get("Properties", {}).get("spark.jobGroup.id") for job in jobs.values()}
    require(job_groups == groups, "Missing or unattributable workload job groups.")
    successful_stages = {
        key
        for key, event in completed.items()
        if not event["Stage Info"].get("Failure Reason")
        and type(event["Stage Info"].get("Completion Time")) is int
    }
    for identifier, job in jobs.items():
        require(
            job_ends[identifier].get("Job Result", {}).get("Result") == "JobSucceeded",
            f"Workload job did not succeed: {identifier}",
        )
        require(
            job.get("Stage IDs")
            and all(
                any(key[0] == stage for key in successful_stages) for stage in job["Stage IDs"]
            ),
            "A relevant workload stage did not complete successfully.",
        )

    successes, failures = {}, []
    for identifier, event in tasks.items():
        info = event["Task Info"]
        stage = event["Stage ID"], event["Stage Attempt ID"]
        executor = info["Executor ID"]
        record = {
            "task_id": identifier,
            "stage_id": stage[0],
            "stage_attempt": stage[1],
            "executor_id": executor,
            "host": info["Host"],
            "partition_id": info["Partition ID"],
            "attempt": info["Attempt"],
            "reason": event.get("Task End Reason"),
            "launch_time": info["Launch Time"],
            "finish_time": info["Finish Time"],
            "metrics": event.get("Task Metrics", {}),
        }
        success = (
            event.get("Task End Reason", {}).get("Reason") == "Success"
            and not info.get("Failed")
            and not info.get("Killed")
        )
        if not success:
            failures.append(record)
            continue
        require(
            executor in executors and executors[executor]["Host"] == info["Host"],
            "Successful task has no matching executor placement.",
        )
        require(stage in submitted, "Successful task has no submitted stage attempt.")
        # Success in an abandoned stage attempt is not successful workload evidence.
        if stage not in successful_stages:
            failures.append({**record, "discarded_stage_attempt": True})
            continue
        values = record["metrics"]
        required_metrics = [
            values.get(key)
            for key in (
                "Executor Run Time",
                "JVM GC Time",
                "Memory Bytes Spilled",
                "Disk Bytes Spilled",
            )
        ] + [
            values.get(section, {}).get(key)
            for section, key in (
                ("Input Metrics", "Bytes Read"),
                ("Shuffle Read Metrics", "Remote Bytes Read"),
                ("Shuffle Read Metrics", "Local Bytes Read"),
                ("Shuffle Write Metrics", "Shuffle Bytes Written"),
            )
        ]
        require(
            all(type(value) is int and value >= 0 for value in required_metrics),
            "Missing or invalid measured successful-task metrics.",
        )
        successes[identifier] = record

    for key in successful_stages:
        count = completed[key]["Stage Info"].get("Number of Tasks")
        partitions = {
            task["partition_id"]
            for task in successes.values()
            if (task["stage_id"], task["stage_attempt"]) == key
        }
        require(
            type(count) is int and count > 0 and len(partitions) == count,
            "Completed stage is missing successful workload task evidence.",
        )
        require(
            submitted[key].get("Properties", {}).get("spark.jobGroup.id") in groups,
            "Completed stage has no run-scoped phase attribution.",
        )
    require(
        {record["host"] for record in successes.values()} == set(WORKERS),
        "Registered/idle executors do not prove successful work on both Workers.",
    )
    require(len(probes) == 2, "Missing barrier task identities.")
    matched = []
    for probe in probes:
        task = successes.get(probe.get("task_attempt_id"))
        require(task is not None, "Barrier probe task attempt was not successful in the event log.")
        require(
            task["host"] == probe.get("host") == placements.get(task["executor_id"])
            and task["stage_id"] == probe.get("stage_id")
            and task["partition_id"] == probe.get("partition_id")
            and task["attempt"] == probe.get("task_attempt_number")
            and task["stage_attempt"] == probe.get("stage_attempt_number", task["stage_attempt"]),
            "Barrier task-attempt/stage/host identity mismatch.",
        )
        stage = submitted[task["stage_id"], task["stage_attempt"]]
        require(
            stage.get("Properties", {}).get("spark.jobGroup.id") == f"{run_id}:input-probe"
            and stage["Stage Info"].get("Number of Tasks") == 2
            and any(rdd.get("Barrier") is True for rdd in stage["Stage Info"].get("RDD Info", [])),
            "Probe tasks were not part of the completed two-task barrier stage.",
        )
        matched.append(task)
    require(
        {task["host"] for task in matched} == set(WORKERS)
        and len({(task["stage_id"], task["stage_attempt"]) for task in matched}) == 1,
        "Barrier stage did not complete on both current Workers.",
    )

    metrics = {}
    for identifier, executor in executors.items():
        totals = Counter()
        for task in successes.values():
            if task["executor_id"] != identifier:
                continue
            values = task["metrics"]
            totals.update(
                {
                    "successful_tasks": 1,
                    "input_bytes": values.get("Input Metrics", {}).get("Bytes Read", 0),
                    "shuffle_read_bytes": sum(
                        values.get("Shuffle Read Metrics", {}).get(key, 0)
                        for key in ("Remote Bytes Read", "Local Bytes Read")
                    ),
                    "shuffle_write_bytes": values.get("Shuffle Write Metrics", {}).get(
                        "Shuffle Bytes Written", 0
                    ),
                    "executor_run_ms": values.get("Executor Run Time", 0),
                    "gc_ms": values.get("JVM GC Time", 0),
                    "memory_spill_bytes": values.get("Memory Bytes Spilled", 0),
                    "disk_spill_bytes": values.get("Disk Bytes Spilled", 0),
                    "elapsed_ms": task["finish_time"] - task["launch_time"],
                }
            )
        metrics[identifier] = {"host": executor["Host"], **dict(totals)}
    worker_metrics = {}
    for host in WORKERS:
        totals = Counter()
        for values in metrics.values():
            if values["host"] == host:
                totals.update({key: value for key, value in values.items() if key != "host"})
        worker_metrics[host] = dict(totals)
    execution_totals = Counter()
    for task in successes.values():
        values = task["metrics"]
        execution_totals.update(
            {
                "successful_tasks": 1,
                "input_bytes": values.get("Input Metrics", {}).get("Bytes Read", 0),
                "shuffle_read_bytes": sum(
                    values.get("Shuffle Read Metrics", {}).get(key, 0)
                    for key in ("Remote Bytes Read", "Local Bytes Read")
                ),
                "shuffle_write_bytes": values.get("Shuffle Write Metrics", {}).get(
                    "Shuffle Bytes Written", 0
                ),
                "executor_run_ms": values.get("Executor Run Time", 0),
                "gc_ms": values.get("JVM GC Time", 0),
                "memory_spill_bytes": values.get("Memory Bytes Spilled", 0),
                "disk_spill_bytes": values.get("Disk Bytes Spilled", 0),
            }
        )
    execution_metrics = {
        **dict(execution_totals),
        "task_attempts": len(tasks),
        "failed_attempts": len(failures),
        "successful_task_hosts": sorted({record["host"] for record in successes.values()}),
        "successful_executor_ids": sorted({record["executor_id"] for record in successes.values()}),
    }
    return {
        "application_id": application_id,
        "run_id": run_id,
        "successful_tasks": len(successes),
        "failed_attempts": failures,
        "executors": metrics,
        "workers": worker_metrics,
        "probe_tasks": matched,
        "effective_configuration": properties,
        "stages": [event for event in completed.values()],
        "jobs": [{"start": job, "end": job_ends[identifier]} for identifier, job in jobs.items()],
        "execution_metrics": execution_metrics,
    }


def _m04_stage_info(info: object) -> dict:
    require(isinstance(info, dict), "Malformed M04 stage info.")
    require(
        all(
            type(info.get(field)) is int and info[field] >= 0
            for field in ("Stage ID", "Stage Attempt ID", "Number of Tasks")
        ),
        "Invalid M04 stage identity or task count.",
    )
    parents = info.get("Parent IDs", [])
    require(
        isinstance(parents, list)
        and all(type(parent) is int and parent >= 0 for parent in parents)
        and len(parents) == len(set(parents))
        and info["Stage ID"] not in parents,
        "Invalid M04 stage parents.",
    )
    return info


def _require_m04_job_stages(infos: dict[int, dict], submitted: dict, completed: dict) -> None:
    """Check the DAG against listener evidence available when this job succeeds."""
    for info in infos.values():
        require(set(info.get("Parent IDs", [])) <= infos.keys(), "Incomplete M04 job stage DAG.")
    remaining = set(infos)
    while remaining:
        ready = {
            stage
            for stage in remaining
            if not (set(infos[stage].get("Parent IDs", [])) & remaining)
        }
        require(ready, "Cyclic M04 job stage DAG.")
        remaining -= ready

    executed = set()
    for stage, info in infos.items():
        attempts = [key for key in submitted if key[0] == stage]
        if attempts:
            require(
                all(key in completed for key in attempts),
                "M04 submitted stage attempt is missing completion evidence.",
            )
            require(
                not completed[max(attempts)]["Stage Info"].get("Failure Reason"),
                "M04 submitted stage has no successful final completed attempt.",
            )
            require(
                info.get("Parent IDs", [])
                == submitted[max(attempts)]["Stage Info"].get("Parent IDs", []),
                "M04 job snapshot and submitted stage parents disagree.",
            )
            executed.add(stage)
        else:
            # JobStart snapshots include unexecuted ancestors when Spark reuses
            # shuffle/cache output. They must not claim an attempt actually ran.
            require(
                info["Stage Attempt ID"] == 0
                and info.get("Submission Time") is None
                and info.get("Completion Time") is None
                and not info.get("Failure Reason"),
                "M04 skipped stage snapshot contradicts listener execution history.",
            )
    require(executed, "M04 job does not have successful completed stages.")
    ancestors = set(executed)
    pending = list(executed)
    while pending:
        for parent in infos[pending.pop()].get("Parent IDs", []):
            if parent not in ancestors:
                ancestors.add(parent)
                pending.append(parent)
    require(
        ancestors == infos.keys(),
        "M04 skipped stages are not ancestors of successful executed stages.",
    )


def summarize_m04_event_log(
    payload: bytes,
    *,
    run_id: str,
    application_name: str,
    require_both_workers: bool = True,
) -> dict:
    """Summarize real M04 task placement and resource metrics independently."""
    events = parse_events(payload, max_bytes=MAX_M04_EVENT_BYTES)
    application = next(
        event for event in events if event["Event"] == "SparkListenerApplicationStart"
    )
    require(application.get("App Name") == application_name, "M04 application name mismatch.")
    application_id = application.get("App ID")
    require(isinstance(application_id, str) and application_id, "M04 application ID is missing.")
    environments = [event for event in events if event["Event"] == "SparkListenerEnvironmentUpdate"]
    require(len(environments) == 1, "M04 event log lacks one effective environment record.")
    properties = environments[0].get("Spark Properties", {})
    require(
        isinstance(properties, dict)
        and isinstance(properties.get("spark.master"), str)
        and properties["spark.master"].startswith("spark://")
        and properties.get("spark.ui.enabled") == "false",
        "M04 event log did not prove Standalone/no-UI execution.",
    )

    executors = {}
    jobs, job_ends, job_stages = {}, {}, {}
    submitted, completed, tasks = {}, {}, {}
    for event in events:
        kind = event["Event"]
        if kind == "SparkListenerExecutorAdded":
            identifier = event.get("Executor ID")
            info = event.get("Executor Info", {})
            require(
                isinstance(identifier, str)
                and identifier not in executors
                and isinstance(info, dict)
                and info.get("Host") in WORKERS,
                "Invalid or duplicate M04 executor placement.",
            )
            executors[identifier] = info
        elif kind == "SparkListenerJobStart":
            identifier = event.get("Job ID")
            require(
                type(identifier) is int and identifier >= 0 and identifier not in jobs,
                "Invalid or duplicate M04 job start.",
            )
            stage_ids, infos = event.get("Stage IDs"), event.get("Stage Infos")
            require(
                isinstance(stage_ids, list)
                and stage_ids
                and all(type(stage) is int and stage >= 0 for stage in stage_ids)
                and len(stage_ids) == len(set(stage_ids))
                and isinstance(infos, list)
                and len(infos) == len(stage_ids),
                "Malformed M04 job stage evidence.",
            )
            infos = [_m04_stage_info(info) for info in infos]
            require(
                {info["Stage ID"] for info in infos} == set(stage_ids),
                "M04 job stage IDs and infos disagree.",
            )
            require(isinstance(event.get("Properties"), dict), "Malformed M04 job properties.")
            group = event["Properties"].get("spark.jobGroup.id")
            require(
                isinstance(group, str) and group.startswith(f"{run_id}:"),
                "M04 jobs are not attributable to the run-scoped job group.",
            )
            job_stages[identifier] = {info["Stage ID"]: info for info in infos}
            jobs[identifier] = event
        elif kind == "SparkListenerJobEnd":
            identifier = event.get("Job ID")
            require(
                type(identifier) is int and identifier in jobs and identifier not in job_ends,
                "Invalid, orphaned or duplicate M04 job end.",
            )
            result = event.get("Job Result")
            require(
                isinstance(result, dict) and result.get("Result") == "JobSucceeded",
                f"M04 Spark job did not succeed: {identifier}",
            )
            _require_m04_job_stages(job_stages[identifier], submitted, completed)
            job_ends[identifier] = event
        elif kind == "SparkListenerStageSubmitted":
            info = _m04_stage_info(event.get("Stage Info"))
            key = info["Stage ID"], info["Stage Attempt ID"]
            require(key not in submitted, "Duplicate M04 stage submission.")
            previous = [attempt for attempt in submitted if attempt[0] == key[0]]
            require(
                all(attempt in completed and attempt[1] < key[1] for attempt in previous),
                "M04 stage retry precedes prior attempt completion or has invalid attempt order.",
            )
            require(
                type(info.get("Submission Time")) is int
                and info["Submission Time"] >= 0
                and isinstance(event.get("Properties"), dict),
                "Invalid M04 stage submission evidence.",
            )
            group = event["Properties"].get("spark.jobGroup.id")
            require(
                isinstance(group, str)
                and any(
                    identifier not in job_ends
                    and key[0] in job_stages[identifier]
                    and job["Properties"]["spark.jobGroup.id"] == group
                    for identifier, job in jobs.items()
                ),
                "M04 submitted stage has no active run-scoped job attribution.",
            )
            submitted[key] = event
        elif kind == "SparkListenerStageCompleted":
            info = _m04_stage_info(event.get("Stage Info"))
            key = info["Stage ID"], info["Stage Attempt ID"]
            require(key not in completed, "Duplicate M04 stage completion.")
            require(key in submitted, "M04 stage completion has no submitted stage attempt.")
            start = submitted[key]["Stage Info"]
            require(
                info.get("Submission Time") == start["Submission Time"]
                and type(info.get("Completion Time")) is int
                and info["Completion Time"] >= start["Submission Time"]
                and info["Number of Tasks"] == start["Number of Tasks"]
                and info.get("Parent IDs", []) == start.get("Parent IDs", [])
                and (info.get("Failure Reason") is None or isinstance(info["Failure Reason"], str)),
                "Inconsistent M04 stage completion history.",
            )
            completed[key] = event
        elif kind in ("SparkListenerTaskStart", "SparkListenerTaskEnd"):
            key = event.get("Stage ID"), event.get("Stage Attempt ID")
            require(
                all(type(value) is int and value >= 0 for value in key) and key in submitted,
                "M04 task has no submitted stage attempt.",
            )
            info = event.get("Task Info")
            require(isinstance(info, dict), "Malformed M04 task info.")
            identifier = info.get("Task ID")
            require(type(identifier) is int and identifier >= 0, "Invalid M04 task identity.")
            if kind != "SparkListenerTaskEnd":
                continue
            require(identifier not in tasks, "Duplicate M04 task completion.")
            require(
                all(
                    type(info.get(field)) is int and info[field] >= 0
                    for field in ("Partition ID", "Attempt", "Launch Time", "Finish Time")
                )
                and info["Finish Time"] >= info["Launch Time"]
                and type(info.get("Failed")) is bool
                and type(info.get("Killed")) is bool
                and isinstance(info.get("Executor ID"), str)
                and isinstance(info.get("Host"), str)
                and isinstance(event.get("Task End Reason"), dict)
                and isinstance(event["Task End Reason"].get("Reason"), str),
                "Invalid M04 task completion evidence.",
            )
            tasks[identifier] = event

    require(jobs and jobs.keys() == job_ends.keys(), "M04 jobs are not all completed.")
    require(
        submitted and submitted.keys() == completed.keys(),
        "M04 submitted stage attempts are missing completion evidence.",
    )
    require(
        {key[0] for key in submitted}
        <= set().union(*(set(infos) for infos in job_stages.values())),
        "M04 submitted stages have no job attribution.",
    )
    successful_stages = {
        key
        for key, event in completed.items()
        if not event.get("Stage Info", {}).get("Failure Reason")
        and type(event.get("Stage Info", {}).get("Completion Time")) is int
    }
    for stage in {key[0] for key in submitted}:
        # A later failed attempt must not be hidden by an earlier success.
        final = max(key for key in submitted if key[0] == stage)
        require(
            final in successful_stages,
            "M04 submitted stage has no successful final completed attempt.",
        )

    successes = []
    failures = []
    for event in tasks.values():
        info = event.get("Task Info", {})
        stage = event.get("Stage ID"), event.get("Stage Attempt ID")
        success = (
            event.get("Task End Reason", {}).get("Reason") == "Success"
            and not info.get("Failed")
            and not info.get("Killed")
        )
        record = {
            "stage_id": stage[0],
            "stage_attempt": stage[1],
            "executor_id": info.get("Executor ID"),
            "host": info.get("Host"),
            "partition_id": info.get("Partition ID"),
            "launch_time": info.get("Launch Time"),
            "finish_time": info.get("Finish Time"),
            "metrics": event.get("Task Metrics", {}),
        }
        if not success or stage not in successful_stages:
            failures.append(record)
            continue
        executor_id = info.get("Executor ID")
        require(
            executor_id in executors and executors[executor_id].get("Host") == info.get("Host"),
            "M04 task has no matching executor placement.",
        )
        metrics = record["metrics"]
        require(
            isinstance(metrics, dict)
            and all(
                isinstance(metrics.get(section), dict)
                for section in (
                    "Input Metrics",
                    "Shuffle Read Metrics",
                    "Shuffle Write Metrics",
                )
            ),
            "M04 successful task metrics are incomplete or invalid.",
        )
        required_metrics = [
            metrics.get(key)
            for key in (
                "Executor Run Time",
                "JVM GC Time",
                "Memory Bytes Spilled",
                "Disk Bytes Spilled",
            )
        ] + [
            metrics.get(section, {}).get(key)
            for section, key in (
                ("Input Metrics", "Bytes Read"),
                ("Shuffle Read Metrics", "Remote Bytes Read"),
                ("Shuffle Read Metrics", "Local Bytes Read"),
                ("Shuffle Write Metrics", "Shuffle Bytes Written"),
            )
        ]
        require(
            all(type(value) is int and value >= 0 for value in required_metrics),
            "M04 successful task metrics are incomplete or invalid.",
        )
        successes.append(record)

    hosts = {record["host"] for record in successes}
    if require_both_workers:
        require(hosts == set(WORKERS), "M04 successful tasks did not execute on both Workers.")
    require(successes, "M04 event log has no successful task evidence.")
    for key in successful_stages:
        partitions = {
            task["partition_id"]
            for task in successes
            if (task["stage_id"], task["stage_attempt"]) == key
        }
        require(
            len(partitions) == completed[key]["Stage Info"]["Number of Tasks"],
            "M04 completed stage is missing successful task evidence.",
        )

    executor_metrics = {}
    for identifier, executor in executors.items():
        totals = Counter()
        for task in successes:
            if task["executor_id"] != identifier:
                continue
            values = task["metrics"]
            totals.update(
                {
                    "successful_tasks": 1,
                    "input_bytes": values.get("Input Metrics", {}).get("Bytes Read", 0),
                    "shuffle_read_bytes": sum(
                        values.get("Shuffle Read Metrics", {}).get(key, 0)
                        for key in ("Remote Bytes Read", "Local Bytes Read")
                    ),
                    "shuffle_write_bytes": values.get("Shuffle Write Metrics", {}).get(
                        "Shuffle Bytes Written", 0
                    ),
                    "executor_run_ms": values.get("Executor Run Time", 0),
                    "gc_ms": values.get("JVM GC Time", 0),
                    "memory_spill_bytes": values.get("Memory Bytes Spilled", 0),
                    "disk_spill_bytes": values.get("Disk Bytes Spilled", 0),
                    "elapsed_ms": task["finish_time"] - task["launch_time"],
                }
            )
        executor_metrics[identifier] = {"host": executor["Host"], **dict(totals)}
    worker_metrics = {}
    for host in WORKERS:
        totals = Counter()
        for values in executor_metrics.values():
            if values["host"] == host:
                totals.update({key: value for key, value in values.items() if key != "host"})
        worker_metrics[host] = dict(totals)
    execution_totals = Counter()
    for task in successes:
        values = task["metrics"]
        execution_totals.update(
            {
                "successful_tasks": 1,
                "input_bytes": values.get("Input Metrics", {}).get("Bytes Read", 0),
                "shuffle_read_bytes": sum(
                    values.get("Shuffle Read Metrics", {}).get(key, 0)
                    for key in ("Remote Bytes Read", "Local Bytes Read")
                ),
                "shuffle_write_bytes": values.get("Shuffle Write Metrics", {}).get(
                    "Shuffle Bytes Written", 0
                ),
                "executor_run_ms": values.get("Executor Run Time", 0),
                "gc_ms": values.get("JVM GC Time", 0),
                "memory_spill_bytes": values.get("Memory Bytes Spilled", 0),
                "disk_spill_bytes": values.get("Disk Bytes Spilled", 0),
            }
        )
    execution_metrics = {
        **dict(execution_totals),
        "task_attempts": len(tasks),
        "failed_attempts": len(failures),
        "successful_task_hosts": sorted({record["host"] for record in successes}),
        "successful_executor_ids": sorted({record["executor_id"] for record in successes}),
    }
    return {
        "application_id": application_id,
        "application_name": application_name,
        "run_id": run_id,
        "successful_tasks": len(successes),
        "failed_attempts": failures,
        "executors": executor_metrics,
        "workers": worker_metrics,
        "effective_configuration": properties,
        "stage_count": len(successful_stages),
        "job_count": len(jobs),
        "execution_metrics": execution_metrics,
    }


def submit_verification(hdfs: HDFSVerification, application_args: list[str]):
    argv = [
        "timeout",
        "--foreground",
        "--signal=TERM",
        "--kill-after=45s",
        "300s",
        "bash",
        str(ROOT / "scripts/spark-submit.sh"),
        "--conf",
        "spark.default.parallelism=2",
        "--conf",
        "spark.sql.shuffle.partitions=2",
        "--conf",
        "spark.sql.adaptive.enabled=false",
        "--",
        "warehouse_bigdata/jobs/verify_spark_hdfs.py",
        *application_args,
    ]
    hdfs.evidence.write(
        "spark-submit-command",
        {
            "argv": argv,
            "sha256": hashlib.sha256(json.dumps(argv).encode()).hexdigest(),
        },
    )
    return hdfs.runner.run(argv, timeout=360, check=False)


def assert_submission_status(returncode: int) -> None:
    require(
        returncode == 0,
        "Spark submission timed out (TERM at 300s; 45s T02 cleanup grace)."
        if returncode in (124, 137)
        else f"Spark submission failed with exit {returncode}.",
    )


def assert_driver_identity(info: dict, name: str) -> None:
    require(
        re.fullmatch(r"warehouse-spark-submit-[0-9a-f]{32}", name),
        "Missing/unsafe reported T02 driver identity.",
    )
    token = name.removeprefix("warehouse-spark-submit-")
    require(
        info.get("Name") == f"/{name}"
        and info.get("Config", {}).get("Labels", {}).get(DRIVER_LABEL) == token,
        "T02 driver name/ownership label mismatch; coordinator never removes drivers.",
    )


def snapshot_cluster(hdfs: HDFSVerification, master_container: str, *, timeout: float = 20) -> dict:
    # Use the already-installed T01 probe in a fresh process, independent of the driver.
    code = (
        "import json,runpy,time\n"
        "p=runpy.run_path('/opt/warehouse-spark/healthcheck.py')\n"
        "d=time.monotonic()+10\n"
        "value={'captured_at_ms':int(time.time()*1000),'master':p['check_master'](d),"
        "'workers':{h:p['read_json'](h,8081,d) for h in p['WORKERS']}}\n"
        "print(json.dumps(value))\n"
    )
    result = hdfs.runner.run(
        [
            "docker",
            "exec",
            master_container,
            "timeout",
            "--kill-after=2s",
            "12s",
            "/opt/python/bin/python3.11",
            "-c",
            code,
        ],
        timeout=timeout,
    )
    return json.loads(result.stdout)


def prerequisites(hdfs: HDFSVerification) -> dict:
    require(not hdfs.parallel, "Spark verification requires serial pytest execution (no xdist).")
    require(
        hdfs.fixture.owned and hdfs.fixture.created_path == hdfs.fixture.path,
        "Spark verification requires positively owned HDFS fixture creation.",
    )
    validate_request(hdfs.evidence.run_id, HDFS_URI + hdfs.fixture.path + "/spark", "0" * 64)
    try:
        hdfs.runner.run(["bash", str(ROOT / "scripts/spark-cluster.sh"), "check"], timeout=60)
        resolved = json.loads(
            hdfs.compose(
                "--profile", "spark", "--profile", "spark-submit", "config", "--format", "json"
            ).stdout
        )
        ids = hdfs.compose(
            "--profile", "spark", "ps", "--all", "--quiet", *SPARK_SERVICES
        ).stdout.split()
        require(len(ids) == 3, "Start the existing T01 Spark Master and both Workers through T01.")
        inspected = json.loads(
            hdfs.runner.run(["docker", "inspect", *[value.decode() for value in ids]]).stdout
        )
        services = {}
        for info in inspected:
            labels = info.get("Config", {}).get("Labels", {})
            service = labels.get("com.docker.compose.service")
            require(
                labels.get("com.docker.compose.project") == "warehouse-bigdata"
                and service in SPARK_SERVICES
                and service not in services,
                "Unexpected/duplicate Spark container ownership.",
            )
            spec, state = resolved["services"][service], info["State"]
            require(
                state["Running"] and not state["Paused"] and not state["Restarting"],
                f"Spark service is not currently running normally: {service}",
            )
            require(
                info["Config"]["Image"] == spec["image"]
                and info["Config"]["User"] == "185:185"
                and info["Config"]["Hostname"] == service
                and hdfs.spec.network in info["NetworkSettings"]["Networks"],
                f"Running Spark container differs from T01: {service}",
            )
            mounts = {mount["Destination"]: mount for mount in info["Mounts"]}
            for source, target in hdfs.spec.xml_mounts:
                mount = mounts.get(target, {})
                require(
                    mount.get("Source") == source
                    and mount.get("Type") == "bind"
                    and not mount.get("RW"),
                    f"Spark Hadoop XML mount mismatch: {service}",
                )
            services[service] = info
        require(set(services) == set(SPARK_SERVICES), "Missing current Spark service container.")
        require(
            len({info["Image"] for info in services.values()}) == 1,
            "Spark runtime image IDs differ.",
        )
        driver_image_name = resolved["services"]["spark-submit"]["image"]
        image = json.loads(
            hdfs.runner.run(["docker", "image", "inspect", driver_image_name]).stdout
        )
        require(
            len(image) == 1, "Prepare the T02 driver image through its existing build interface."
        )
        registration = snapshot_cluster(hdfs, services["spark-master"]["Id"])
        expected_sources = {
            str(path.relative_to(ROOT / "src/warehouse_bigdata")): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in sorted((ROOT / "src/warehouse_bigdata").rglob("*.py"))
        }
        xml_hashes = {
            Path(source).name: hashlib.sha256(Path(source).read_bytes()).hexdigest()
            for source, _ in hdfs.spec.xml_mounts
        }
        command_sources = {
            name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in (
                "infrastructure/compose.yaml",
                "infrastructure/spark/submit.Dockerfile",
                "scripts/spark-submit.sh",
                "scripts/spark-cluster.sh",
                "scripts/package-spark-app.py",
            )
        }
        result = {
            "containers": services,
            "driver_image": image[0],
            "registration": registration,
            "expected_sources": expected_sources,
            "xml_sha256": xml_hashes,
            "command_config_sha256": command_sources,
            "hdfs_configuration": asdict(hdfs.config),
        }
        hdfs.evidence.write("spark-prerequisites", result)
        return result
    except Exception as error:
        raise VerificationError(
            "Spark prerequisite blocked: existing ready T01 services and a prepared T02 image are required; "
            f"no automatic build/start/repair. {error}"
        ) from error


def create_spark_fixture(hdfs: HDFSVerification, root: str, files: dict[str, bytes]) -> None:
    require(
        hdfs.hdfs("dfs", "-stat", "%a", hdfs.fixture.path).strip() == "755",
        "Owned HDFS parent must already permit Spark traversal (mode 755).",
    )
    # Exclusive mkdir throughout. Writable permissions never extend beyond spark/.
    for directory in (root, root + "/input", root + "/work", root + "/events"):
        hdfs.hdfs("dfs", "-mkdir", directory)
    hdfs.hdfs("dfs", "-chmod", "755", root, root + "/input")
    hdfs.hdfs("dfs", "-chmod", "777", root + "/work", root + "/events")
    for name, payload in files.items():
        path = root + "/input/" + name
        hdfs.hdfs("dfs", "-put", "-", path, timeout=IO_TIMEOUT, input_data=payload)
        hdfs.hdfs("dfs", "-chmod", "444", path)
    manifest = {
        "provenance": "synthetic verification data",
        "run_id": hdfs.evidence.run_id,
        "fixture_root": root,
        "input_rows": 1024,
        "contributing_records": 682,
        "rules": {
            "bucket": "record_id % 8",
            "quantity": "record_id % 5 + 1",
            "unit_price_cents": "100 + 7 * (record_id % 17)",
            "filter": "record_id % 3 != 0",
        },
        "input_files": {
            name: {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
            for name, payload in files.items()
        },
        "expected_rows": expected_rows(),
        "oracle": "independent host Python, not local Spark",
    }
    hdfs.hdfs("dfs", "-put", "-", root + "/fixture.json", input_data=json.dumps(manifest).encode())
    hdfs.evidence.write("spark-fixture", manifest)


def confirm_termination(
    hdfs: HDFSVerification, startup: dict, application_id: str | None, preflight: dict
) -> None:
    name = startup.get("driver_host", "")
    require(
        re.fullmatch(r"warehouse-spark-submit-[0-9a-f]{32}", name),
        "Driver termination is unproven: no valid T02 startup identity.",
    )
    deadline = time.monotonic() + 30
    app_name = f"warehouse-spark-hdfs-{hdfs.evidence.run_id}"
    expected_workers = {
        host: value["id"] for host, value in preflight["registration"]["workers"].items()
    }
    while time.monotonic() < deadline:
        inspected = hdfs.runner.run(
            ["docker", "inspect", "--type", "container", name],
            timeout=min(5, max(0.1, deadline - time.monotonic())),
            check=False,
        )
        removed = inspected.returncode != 0
        if removed:
            require(
                b"No such object" in inspected.stderr or b"No such container" in inspected.stderr,
                "Docker could not establish driver removal.",
            )
        else:
            values = json.loads(inspected.stdout)
            require(len(values) == 1, "Ambiguous driver inspection.")
            assert_driver_identity(values[0], name)
        remaining = deadline - time.monotonic()
        require(remaining > 0, "Application termination deadline expired.")
        cluster = snapshot_cluster(
            hdfs, preflight["containers"]["spark-master"]["Id"], timeout=min(20, remaining)
        )
        require(
            cluster["master"].get("url") == MASTER and cluster["master"].get("status") == "ALIVE",
            "Application termination cannot be established from this Master.",
        )
        alive = [
            worker
            for worker in cluster["master"].get("workers", [])
            if worker.get("state") == "ALIVE"
        ]
        require(
            len(alive) == 2
            and {worker.get("host"): worker.get("id") for worker in alive} == expected_workers
            and {host: value.get("id") for host, value in cluster["workers"].items()}
            == expected_workers,
            "Worker identities changed before termination confirmation.",
        )
        require(
            isinstance(cluster["master"].get("activeapps"), list)
            and all(
                isinstance(worker.get("executors"), list) for worker in cluster["workers"].values()
            ),
            "Incomplete active application/executor termination state.",
        )

        def matches(app):
            return (
                application_id is not None
                and (app.get("id") == application_id or app.get("appid") == application_id)
            ) or app.get("name", app.get("appdesc", {}).get("name")) == app_name

        active_apps = [app for app in cluster["master"].get("activeapps", []) if matches(app)]
        active_executors = [
            executor
            for worker in cluster["workers"].values()
            for executor in worker.get("executors", [])
            if matches(executor)
        ]
        hdfs.evidence.write(
            "spark-termination",
            {
                "driver_host": name,
                "driver_removed": removed,
                "application_id": application_id,
                "matching_active_apps": active_apps,
                "matching_active_executors": active_executors,
                "cluster": cluster,
            },
        )
        if removed and not active_apps and not active_executors:
            return
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    raise VerificationError("T02 driver removal/application termination was not proven within 30s.")


def collect_event_log(hdfs: HDFSVerification, root: str, application_id: str | None) -> bytes:
    directory = root + "/events"
    paths = hdfs.hdfs("dfs", "-ls", "-C", directory).splitlines()
    require(len(paths) == 1, "Expected one non-rolling application event log.")
    path = paths[0]
    name = path.rsplit("/", 1)[-1]
    require(
        path == directory + "/" + name and re.fullmatch(r"app-\d{14}-\d+(?:\.inprogress)?", name),
        "Unexpected/uncontained application event-log file.",
    )
    size = int(hdfs.hdfs("dfs", "-stat", "%b", path).strip())
    require(
        0 < size <= MAX_EVENT_BYTES, "Empty/oversized event log; truncated evidence is forbidden."
    )
    payload = hdfs.client_command("hdfs", "dfs", "-cat", path, timeout=60)
    (hdfs.evidence.directory / "spark-events.jsonl").write_bytes(payload)
    require(
        len(payload) == size, "Event log changed or was truncated during independent retrieval."
    )
    parse_events(payload)
    require(
        application_id is not None and name == application_id,
        "Incomplete/in-progress or mismatched application event log.",
    )
    hdfs.evidence.write(
        "spark-event-log",
        {
            "path": path,
            "bytes": size,
            "sha256": hashlib.sha256(payload).hexdigest(),
        },
    )
    return payload


def inspect_data_files(hdfs: HDFSVerification, root: str, files: dict[str, bytes]) -> None:
    output = root + "/work/parquet"
    listed = hdfs.hdfs("dfs", "-ls", "-C", output).splitlines()
    require(output + "/_SUCCESS" in listed, "Parquet output has no successful commit marker.")
    parquet = [
        path
        for path in listed
        if path.rsplit("/", 1)[-1].startswith("part-") and path.endswith(".parquet")
    ]
    require(
        len(parquet) == 2
        and len(set(parquet)) == 2
        and set(listed) == {*parquet, output + "/_SUCCESS"},
        "Expected exactly two bounded committed Parquet data files.",
    )
    require(
        all(path == output + "/" + path.rsplit("/", 1)[-1] for path in parquet),
        "Uncontained Parquet data-file listing.",
    )
    expected_inputs = {root + "/input/" + name: payload for name, payload in files.items()}
    actual_inputs = hdfs.hdfs("dfs", "-ls", "-C", root + "/input").splitlines()
    require(set(actual_inputs) == set(expected_inputs), "HDFS input file set changed.")
    nodes = hdfs.live_nodes()
    addresses = {node.address for node in nodes.values()}
    deadline = time.monotonic() + 180
    results = {}
    for path in [*expected_inputs, *sorted(parquet)]:
        replication, length = hdfs.hdfs("dfs", "-stat", "%r %b", path).split()
        size = int(length)
        require(
            replication == "2" and 0 < size <= MAX_DATA_BYTES,
            f"Data-file replication/size differs from the bounded fixture: {path}",
        )
        while True:
            remaining = deadline - time.monotonic()
            require(remaining > 0, "Per-file two-DataNode placement deadline expired.")
            try:
                blocks = parse_fsck(
                    hdfs.hdfs(
                        "fsck", path, "-files", "-blocks", "-locations", timeout=min(30, remaining)
                    ),
                    addresses,
                )
                require(
                    sum(block.length for block in blocks) == size,
                    "Data-file block sizes differ from stat.",
                )
                break
            except VerificationError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(min(2, max(0, deadline - time.monotonic())))
        payload = hdfs.client_command("hdfs", "dfs", "-cat", path, timeout=IO_TIMEOUT)
        require(len(payload) == size, f"Fresh-client data-file byte count mismatch: {path}")
        if path in expected_inputs:
            require(
                payload == expected_inputs[path], f"Independent HDFS input bytes changed: {path}"
            )
        else:
            require(
                payload.startswith(b"PAR1") and payload.endswith(b"PAR1"),
                f"Independent read is not a complete Parquet data file: {path}",
            )
        results[path] = {
            "bytes": size,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "blocks": [asdict(block) for block in blocks],
        }
        hdfs.evidence.write(
            "spark-data-files",
            {
                "nodes": {host: asdict(node) for host, node in nodes.items()},
                "files": results,
            },
        )


def finish_safely(
    hdfs: HDFSVerification,
    *,
    terminated: bool,
    evidence_complete: bool,
    original: BaseException | None,
) -> None:
    reasons = []
    if not terminated:
        reasons.append("Spark driver/application termination is unproven")
    if not evidence_complete:
        reasons.append("Spark evidence collection is incomplete")
    if reasons:
        reason = "; ".join(reasons)
        hdfs.retain_fixture(reason)
        if original is not None:
            original.add_note(reason)
        else:
            raise VerificationError(reason)


def verify_spark_hdfs(hdfs: HDFSVerification) -> None:
    """Coordinate one fixed live scenario; never build, start, repair, or cancel drivers."""
    preflight = prerequisites(hdfs)
    run_id, root = hdfs.evidence.run_id, HDFS_URI + hdfs.fixture.path + "/spark"
    files = synthetic_files()
    before = hdfs.storage_snapshot()
    baseline = hdfs.read_fixture("spark-hdfs-payload-before")
    create_spark_fixture(hdfs, root, files)
    args = [
        "--run-id",
        run_id,
        "--fixture-root",
        root,
        "--probe-sha256",
        hashlib.sha256(files["part-00.jsonl"]).hexdigest(),
    ]
    hdfs.evidence.write(
        "spark-submission",
        {
            "application_args": args,
            "master": MASTER,
            "hdfs": HDFS_URI,
            "settings": {
                "spark.default.parallelism": "2",
                "spark.sql.shuffle.partitions": "2",
                "spark.sql.adaptive.enabled": "false",
            },
            "application_args_sha256": hashlib.sha256(json.dumps(args).encode()).hexdigest(),
            "submission_seconds": 300,
            "cleanup_grace_seconds": 45,
            "host_deadline_seconds": 360,
        },
    )
    # Runner saves even timed-out commands' partial stdout before raising. Retain
    # that exact slot so failed submissions still get termination/evidence handling.
    stdout_path = hdfs.evidence.directory / f"{hdfs.evidence.sequence + 1:04d}.stdout"
    failure = None
    records, stdout = {}, b""
    terminated = sources_checked = event_complete = analyzed = data_complete = preserved = False

    def failed(label: str, error: BaseException) -> None:
        nonlocal failure
        if failure is None:
            failure = error
        else:
            failure.add_note(f"{label}: {error}")
            hdfs.errors.append(f"{label}: {error}")

    try:
        submitted = submit_verification(hdfs, args)
        stdout = submitted.stdout
        assert_submission_status(submitted.returncode)
        records = read_records(stdout, run_id)
        hdfs.evidence.write("spark-application-records", records)
        assert_sources(records["startup"], preflight["expected_sources"], preflight["xml_sha256"])
        sources_checked = True
        result = records["result"]
        require(
            records["application"].get("application_id") == result.get("application_id"),
            "Application/result identity mismatch.",
        )
        require(
            records["startup"].get("driver_host")
            == records["application"].get("driver_host")
            == result.get("driver_host")
            and result.get("fixture_root") == root
            and result.get("provenance") == "synthetic verification data",
            "Application driver/fixture/provenance mismatch.",
        )
        require(
            result.get("versions", {}).get("spark") == "3.5.7",
            "Actual Spark runtime version mismatch.",
        )
        hdfs.evidence.write(
            "spark-correctness",
            {
                "actual": result,
                "expected": expected_rows(),
                "oracle": "independent Python, not local Spark",
            },
        )
        assert_correctness(result)
        assert_probes(result, files["part-00.jsonl"])
    except BaseException as error:
        failure = error
    finally:
        try:
            if not stdout and stdout_path.exists():
                stdout = stdout_path.read_bytes()
            records = read_records(stdout, run_id, complete=False)
            hdfs.evidence.write("spark-application-records", records)
        except BaseException as error:
            failed("Application records unavailable", error)
        application_id = records.get("application", {}).get("application_id")
        try:
            confirm_termination(hdfs, records.get("startup", {}), application_id, preflight)
            terminated = True
        except BaseException as error:
            failed("Termination confirmation failed", error)
        try:
            payload = collect_event_log(hdfs, root, application_id)
            event_complete = True
            require(
                set(records) == {"startup", "application", "result"},
                "Incomplete application records.",
            )
            summary = summarize_event_log(
                payload,
                run_id=run_id,
                application_id=application_id,
                fixture_root=root,
                probes=records["result"].get("probes", []),
                active_cluster=records["result"].get("active_cluster", {}),
            )
            hdfs.evidence.write("spark-execution", summary)
            analyzed = True
        except BaseException as error:
            failed("Event-log collection/verification failed", error)
        try:
            if terminated:
                inspect_data_files(hdfs, root, files)
                data_complete = True
                after = hdfs.storage_snapshot()
                content = hdfs.read_fixture("spark-hdfs-payload-after")
                hdfs.evidence.write(
                    "spark-hdfs-preservation",
                    {
                        "before": before,
                        "after": after,
                        "payload_before": baseline,
                        "payload_after": content,
                    },
                )
                require(
                    content == baseline,
                    "Existing harness payload changed during Spark verification.",
                )
                for field in ("containers", "volumes", "identities"):
                    require(
                        before[field] == after[field], f"HDFS storage identity changed: {field}"
                    )
                preserved = True
        except BaseException as error:
            failed("Independent HDFS data evidence/preservation failed", error)
        finish_safely(
            hdfs,
            terminated=terminated,
            evidence_complete=sources_checked
            and event_complete
            and analyzed
            and data_complete
            and preserved,
            original=failure,
        )
    if failure is not None:
        raise failure
