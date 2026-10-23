"""Offline M06 coordination and distributed-execution evidence gates."""

import copy
import json
from types import SimpleNamespace

import pytest

from tests.unit.test_hdfs_namespace import CONFIG
from warehouse_bigdata.contracts.dataset_scope import ScopeError


def _event_log(*, master="spark://spark-master:7077", workers=("spark-worker-1", "spark-worker-2")):
    task_count = len(workers)
    events = [
        dict(Event="SparkListenerLogStart", **{"Spark Version": "3.5.7"}),
        dict(
            Event="SparkListenerApplicationStart", **{"App Name": "m06-readback", "App ID": "app-1"}
        ),
        dict(
            Event="SparkListenerEnvironmentUpdate",
            **{"Spark Properties": {"spark.master": master}},
        ),
        *[
            dict(
                Event="SparkListenerExecutorAdded",
                **{
                    "Executor ID": str(index + 1),
                    "Executor Info": {"Host": host, "Total Cores": 1},
                },
            )
            for index, host in enumerate(workers)
        ],
        dict(
            Event="SparkListenerJobStart",
            **{
                "Job ID": 1,
                "Stage IDs": [2],
                "Stage Infos": [
                    {"Stage ID": 2, "Stage Attempt ID": 0, "Number of Tasks": task_count}
                ],
            },
        ),
        dict(
            Event="SparkListenerStageSubmitted",
            **{
                "Stage Info": {
                    "Stage ID": 2,
                    "Stage Attempt ID": 0,
                    "Number of Tasks": task_count,
                },
                "Properties": {"spark.jobGroup.id": "m06"},
            },
        ),
    ]
    for index, host in enumerate(workers):
        events.append(
            dict(
                Event="SparkListenerTaskEnd",
                **{
                    "Stage ID": 2,
                    "Stage Attempt ID": 0,
                    "Task End Reason": {"Reason": "Success"},
                    "Task Info": {
                        "Task ID": index,
                        "Index": index,
                        "Partition ID": index,
                        "Attempt": 0,
                        "Launch Time": 1,
                        "Finish Time": 11,
                        "Host": host,
                        "Executor ID": str(index + 1),
                        "Failed": False,
                        "Killed": False,
                    },
                    "Task Metrics": {
                        "Executor Run Time": 10,
                        "JVM GC Time": 1,
                        "Memory Bytes Spilled": 2,
                        "Disk Bytes Spilled": 3,
                        "Input Metrics": {"Bytes Read": 4},
                        "Output Metrics": {"Bytes Written": 5},
                        "Shuffle Read Metrics": {
                            "Remote Bytes Read": 6,
                            "Local Bytes Read": 7,
                        },
                        "Shuffle Write Metrics": {"Shuffle Bytes Written": 8},
                    },
                },
            )
        )
    events.extend(
        [
            dict(
                Event="SparkListenerStageCompleted",
                **{
                    "Stage Info": {
                        "Stage ID": 2,
                        "Stage Attempt ID": 0,
                        "Number of Tasks": task_count,
                    }
                },
            ),
            dict(
                Event="SparkListenerJobEnd",
                **{"Job ID": 1, "Job Result": {"Result": "JobSucceeded"}},
            ),
            dict(Event="SparkListenerApplicationEnd"),
        ]
    )
    with_starts = []
    for event in events:
        if event["Event"] == "SparkListenerTaskEnd":
            with_starts.append(
                {
                    "Event": "SparkListenerTaskStart",
                    "Stage ID": event["Stage ID"],
                    "Stage Attempt ID": event["Stage Attempt ID"],
                    "Task Info": copy.deepcopy(event["Task Info"]),
                }
            )
        with_starts.append(event)
    events = with_starts
    return ("\n".join(json.dumps(event) for event in events) + "\n").encode()


def _event_client(payload):
    return SimpleNamespace(
        list_children=lambda path: [path + "/app-1"],
        stat=lambda path: SimpleNamespace(size_bytes=len(payload)),
        read_remote=lambda path: payload,
    )


def test_completed_execution_reconciles_jobs_stages_partitions_and_workers():
    from warehouse_bigdata.common.small_evidence import completed_execution

    evidence = completed_execution(
        _event_client(_event_log()),
        CONFIG,
        "m06-run",
        expected_application_id="app-1",
        expected_application_name="m06-readback",
        expected_master="spark://spark-master:7077",
    )

    assert evidence["application_id"] == "app-1"
    assert evidence["successful_job_count"] == 1
    assert evidence["successful_stage_attempt_count"] == 1
    assert evidence["planned_only_stage_attempt_count"] == 0
    assert evidence["successful_partition_count"] == 2
    assert evidence["workers"]["spark-worker-1"]["successful_tasks"] == 1
    assert evidence["workers"]["spark-worker-2"]["successful_tasks"] == 1


def test_completed_execution_accepts_job_dag_snapshot_for_skipped_stage():
    from tests.unit.test_small_evidence import reuse_events
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = reuse_events()
    payload = ("\n".join(json.dumps(event) for event in events) + "\n").encode()
    evidence = completed_execution(_event_client(payload), CONFIG, "m06-run")

    assert evidence["planned_only_stage_attempt_count"] == 1
    assert evidence["planned_only_stage_attempts"][0]["stage_id"] == 1
    assert evidence["submitted_stage_attempt_count"] == 2
    assert evidence["completed_stage_count"] == 2
    assert evidence["successful_partition_count"] == 4
    assert [stage["stage_id"] for stage in evidence["stage_attempts"]] == [0, 2]


def test_completed_execution_validates_task_start_events():
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [json.loads(line) for line in _event_log().splitlines()]
    payload = ("\n".join(json.dumps(event) for event in events) + "\n").encode()

    evidence = completed_execution(_event_client(payload), CONFIG, "m06-run")
    assert evidence["successful_task_count"] == 2


@pytest.mark.parametrize(
    "kwargs,pattern",
    [
        ({"expected_application_id": "other"}, "application"),
        ({"expected_application_name": "other"}, "name"),
        ({"expected_master": "spark://other:7077"}, "master"),
    ],
)
def test_completed_execution_rejects_wrong_identity_before_acceptance(kwargs, pattern):
    from warehouse_bigdata.common.small_evidence import completed_execution

    with pytest.raises(ScopeError, match=pattern):
        completed_execution(_event_client(_event_log()), CONFIG, "m06-run", **kwargs)


@pytest.mark.parametrize(
    "mutate,pattern",
    [
        (lambda events: events[:-2], "ApplicationEnd"),
        (
            lambda events: [event for event in events if event["Event"] != "SparkListenerJobEnd"],
            "job",
        ),
        (
            lambda events: [
                {**event, "Task End Reason": {"Reason": "FetchFailed"}}
                if event["Event"] == "SparkListenerTaskEnd"
                else event
                for event in events
            ],
            "partition",
        ),
    ],
)
def test_completed_execution_rejects_truncated_failed_or_incomplete_work(mutate, pattern):
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [json.loads(line) for line in _event_log().splitlines()]
    payload = ("\n".join(json.dumps(event) for event in mutate(events)) + "\n").encode()
    with pytest.raises(ScopeError, match=pattern):
        completed_execution(_event_client(payload), CONFIG, "m06-run")


def test_completed_execution_requires_real_stage_submission_and_executor_binding():
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [json.loads(line) for line in _event_log().splitlines()]
    without_submission = [
        event for event in events if event["Event"] != "SparkListenerStageSubmitted"
    ]
    with pytest.raises(ScopeError, match="submitted stage"):
        completed_execution(
            _event_client(("\n".join(json.dumps(e) for e in without_submission) + "\n").encode()),
            CONFIG,
            "m06-run",
        )

    mismatched = [
        {
            **event,
            "Executor Info": {"Host": "wrong-worker", "Total Cores": 1},
        }
        if event["Event"] == "SparkListenerExecutorAdded" and event["Executor ID"] == "1"
        else event
        for event in events
    ]
    with pytest.raises(ScopeError, match="executor-host"):
        completed_execution(
            _event_client(("\n".join(json.dumps(e) for e in mismatched) + "\n").encode()),
            CONFIG,
            "m06-run",
        )


@pytest.mark.parametrize("kind", ["SparkListenerTaskStart", "SparkListenerTaskEnd"])
def test_completed_execution_rejects_orphan_task_event_for_planned_stage(kind):
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [json.loads(line) for line in _event_log().splitlines()]
    job = next(event for event in events if event["Event"] == "SparkListenerJobStart")
    job["Stage IDs"].insert(0, 1)
    job["Stage Infos"].insert(
        0, {"Stage ID": 1, "Stage Attempt ID": 0, "Number of Tasks": 2, "Parent IDs": []}
    )
    orphan = copy.deepcopy(
        next(event for event in events if event["Event"] == "SparkListenerTaskEnd")
    )
    orphan["Event"] = kind
    orphan["Stage ID"] = 1
    orphan["Task Info"]["Task ID"] = 99
    events.insert(-1, orphan)
    payload = ("\n".join(json.dumps(event) for event in events) + "\n").encode()

    with pytest.raises(ScopeError, match="submitted stage"):
        completed_execution(_event_client(payload), CONFIG, "m06-run")


def test_completed_execution_rejects_submitted_stage_without_completion():
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [
        event
        for event in (json.loads(line) for line in _event_log().splitlines())
        if event["Event"] != "SparkListenerStageCompleted"
    ]
    payload = ("\n".join(json.dumps(event) for event in events) + "\n").encode()

    with pytest.raises(ScopeError, match="stage"):
        completed_execution(_event_client(payload), CONFIG, "m06-run")


def test_completed_execution_rejects_a_failed_final_stage_retry():
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [json.loads(line) for line in _event_log().splitlines()]
    job_end = next(
        index for index, event in enumerate(events) if event["Event"] == "SparkListenerJobEnd"
    )
    retry = [
        {
            "Event": "SparkListenerStageSubmitted",
            "Stage Info": {"Stage ID": 2, "Stage Attempt ID": 1, "Number of Tasks": 2},
            "Properties": {},
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {
                "Stage ID": 2,
                "Stage Attempt ID": 1,
                "Number of Tasks": 2,
                "Failure Reason": "FetchFailed",
            },
        },
    ]
    events[job_end:job_end] = retry
    payload = ("\n".join(json.dumps(event) for event in events) + "\n").encode()
    with pytest.raises(ScopeError, match="successful final"):
        completed_execution(_event_client(payload), CONFIG, "m06-run")


def test_m06_stage_selection_is_downstream_only_and_serial():
    from tests.integration.m06_support import parse_requested_stages

    assert parse_requested_stages([]) == ()
    assert parse_requested_stages(["j06", "validation", "test"]) == (
        "j06",
        "validation",
        "test",
    )
    for stages in (["j05"], ["test", "validation"], ["j06", "j06"]):
        with pytest.raises(ScopeError):
            parse_requested_stages(stages)


def test_acceptance_report_states_do_not_promote_partial_gates():
    from tests.integration.m06_support import report_status

    assert report_status({"scope": True, "readback": True}) == "PASSED"
    assert report_status({"scope": True, "readback": False}) == "FAILED"
    assert report_status({"scope": False}, blocked=True) == "BLOCKED"
    assert report_status({}) == "NOT_RUN"
