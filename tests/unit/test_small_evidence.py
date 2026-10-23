"""Spark 3.5.7 lifecycle and shuffle-reuse evidence, without Spark or HDFS."""

import copy
import hashlib
import json

import pytest

from tests.unit.test_hdfs_namespace import CONFIG
from tests.unit.test_m06_acceptance import _event_client, _event_log
from warehouse_bigdata.common.small_evidence import MAX_EVENT_BYTES, completed_execution
from warehouse_bigdata.contracts.dataset_scope import ScopeError


def stage(stage_id, *, attempt=0, tasks=2, parents=(), rdd=10, partitions=2):
    return {
        "Stage ID": stage_id,
        "Stage Attempt ID": attempt,
        "Number of Tasks": tasks,
        "Parent IDs": list(parents),
        "RDD Info": [{"RDD ID": rdd, "Parent IDs": [], "Number of Partitions": partitions}],
    }


def job_start(job_id, *infos):
    return {
        "Event": "SparkListenerJobStart",
        "Job ID": job_id,
        "Stage IDs": [info["Stage ID"] for info in infos],
        "Stage Infos": copy.deepcopy(list(infos)),
    }


def job_end(job_id):
    return {
        "Event": "SparkListenerJobEnd",
        "Job ID": job_id,
        "Job Result": {"Result": "JobSucceeded"},
    }


def execution(info, *, task_id=0, partition_ids=(0, 1), task_type="ShuffleMapTask"):
    events = [{"Event": "SparkListenerStageSubmitted", "Stage Info": copy.deepcopy(info)}]
    templates = [
        event
        for event in map(json.loads, _event_log().splitlines())
        if event["Event"] == "SparkListenerTaskEnd"
    ]
    for index, partition in enumerate(partition_ids):
        end = copy.deepcopy(templates[index % 2])
        end.update(
            {
                "Stage ID": info["Stage ID"],
                "Stage Attempt ID": info["Stage Attempt ID"],
                "Task Type": task_type,
            }
        )
        end["Task Info"].update(
            {"Task ID": task_id + index, "Index": index, "Partition ID": partition}
        )
        events.extend(
            [
                {
                    "Event": "SparkListenerTaskStart",
                    "Stage ID": info["Stage ID"],
                    "Stage Attempt ID": info["Stage Attempt ID"],
                    "Task Info": copy.deepcopy(end["Task Info"]),
                },
                end,
            ]
        )
    events.append({"Event": "SparkListenerStageCompleted", "Stage Info": copy.deepcopy(info)})
    return events


def header():
    return [
        event
        for event in map(json.loads, _event_log().splitlines())
        if event["Event"]
        in {
            "SparkListenerLogStart",
            "SparkListenerApplicationStart",
            "SparkListenerEnvironmentUpdate",
            "SparkListenerExecutorAdded",
        }
    ]


def reuse_events():
    producer, planned, consumer = stage(0), stage(1), stage(2, parents=(1,), rdd=20)
    return [
        *header(),
        job_start(0, producer),
        *execution(producer),
        job_end(0),
        job_start(1, planned, consumer),
        *execution(consumer, task_id=2, task_type="ResultTask"),
        job_end(1),
        {"Event": "SparkListenerApplicationEnd"},
    ]


def payload(events):
    return ("\n".join(json.dumps(event) for event in events) + "\n").encode()


def validate(events):
    return completed_execution(
        _event_client(payload(events)),
        CONFIG,
        "run",
        expected_application_id="app-1",
        expected_application_name="m06-readback",
    )


def test_shuffle_reuse_is_proven_by_prior_successful_rdd_lineage():
    events = reuse_events()
    result = validate(events)
    assert result["successful_job_count"] == 2
    assert result["submitted_stage_attempt_count"] == 2
    assert result["successful_task_count"] == 4
    planned = result["planned_only_stage_attempts"][0]
    assert planned["stage_id"] == 1
    assert planned["root_rdd_id"] == 10
    assert planned["referenced_by_jobs"] == [1]
    assert planned["reuse_sources"] == [{"stage_id": 0, "stage_attempt_id": 0}]
    assert result["event_log_sha256"] == "sha256:" + hashlib.sha256(payload(events)).hexdigest()


def test_normal_multistage_job_executes_all_dependencies():
    first, second = stage(0), stage(2, parents=(0,), rdd=20)
    result = validate(
        [
            *header(),
            job_start(0, first, second),
            *execution(first),
            *execution(second, task_id=2, task_type="ResultTask"),
            job_end(0),
            {"Event": "SparkListenerApplicationEnd"},
        ]
    )
    assert result["planned_only_stage_attempt_count"] == 0
    assert result["successful_partition_count"] == 4


def test_child_submission_cannot_precede_its_unavailable_parent():
    first, second = stage(0), stage(2, parents=(0,), rdd=20)
    with pytest.raises(ScopeError, match="parent|dependenc"):
        validate(
            [
                *header(),
                job_start(0, first, second),
                *execution(second, task_id=2, task_type="ResultTask"),
                *execution(first),
                job_end(0),
                {"Event": "SparkListenerApplicationEnd"},
            ]
        )


def test_stage_completion_without_submission_is_rejected_directly():
    info = stage(0)
    with pytest.raises(ScopeError, match="completion has no submitted"):
        validate(
            [
                *header(),
                job_start(0, info),
                {"Event": "SparkListenerStageCompleted", "Stage Info": info},
                job_end(0),
                {"Event": "SparkListenerApplicationEnd"},
            ]
        )


@pytest.mark.parametrize(
    "change",
    [
        "no-prior",
        "future-prior",
        "wrong-rdd",
        "wrong-lineage",
        "result-not-shuffle",
        "partial-shuffle",
    ],
)
def test_planned_reference_requires_prior_complete_shuffle_execution(change):
    events = reuse_events()
    if change == "no-prior":
        events = (
            events[:5]
            + events[next(i for i, event in enumerate(events) if event.get("Job ID") == 1) :]
        )
    elif change == "future-prior":
        first = next(i for i, event in enumerate(events) if event.get("Job ID") == 0)
        second = next(i for i, event in enumerate(events) if event.get("Job ID") == 1)
        events = events[:first] + events[second:-1] + events[first:second] + events[-1:]
    elif change in {"wrong-rdd", "wrong-lineage"}:
        planned = next(event for event in events if event.get("Job ID") == 1)["Stage Infos"][0]
        planned["RDD Info"][0]["RDD ID" if change == "wrong-rdd" else "Parent IDs"] = (
            99 if change == "wrong-rdd" else [99]
        )
    elif change == "result-not-shuffle":
        for event in events:
            if event.get("Stage ID") == 0 and event["Event"] == "SparkListenerTaskEnd":
                event["Task Type"] = "ResultTask"
    else:
        for event in events:
            for info in event.get("Stage Infos", []) + (
                [event["Stage Info"]] if "Stage Info" in event else []
            ):
                if info["Stage ID"] == 0:
                    info["Number of Tasks"] = 1
        events = [
            event
            for event in events
            if not (event.get("Stage ID") == 0 and event.get("Task Info", {}).get("Index") == 1)
        ]
    with pytest.raises(ScopeError, match="reuse|prior|shuffle"):
        validate(events)


@pytest.mark.parametrize(
    "change", ["disconnected", "orphan-parent", "cycle", "execution-time", "noninitial-attempt"]
)
def test_planned_snapshots_must_be_consistent_job_dependencies(change):
    events = reuse_events()
    job = next(event for event in events if event.get("Job ID") == 1)
    planned = job["Stage Infos"][0]
    if change == "disconnected":
        job["Stage Infos"].append(stage(3, rdd=30))
        job["Stage IDs"].append(3)
    elif change in {"orphan-parent", "cycle"}:
        planned["Parent IDs"] = [99 if change == "orphan-parent" else 2]
    elif change == "execution-time":
        planned["Completion Time"] = 1
    else:
        planned["Stage Attempt ID"] = 1
    with pytest.raises(ScopeError):
        validate(events)


def test_previously_executed_stage_can_be_referenced_without_resubmission():
    events = reuse_events()
    job = next(event for event in events if event.get("Job ID") == 1)
    job["Stage IDs"][0] = 0
    job["Stage Infos"][0]["Stage ID"] = 0
    for event in events:
        if event.get("Stage Info", {}).get("Stage ID") == 2:
            event["Stage Info"]["Parent IDs"] = [0]
    job["Stage Infos"][1]["Parent IDs"] = [0]
    result = validate(events)
    assert result["submitted_stage_attempt_count"] == 2
    assert result["planned_only_stage_attempt_count"] == 0


def test_completed_map_job_can_reuse_all_its_shuffle_work():
    events = reuse_events()[:-1]
    events.extend([job_start(2, stage(3)), job_end(2), {"Event": "SparkListenerApplicationEnd"}])
    result = validate(events)
    assert result["successful_job_count"] == 3
    assert result["submitted_stage_attempt_count"] == 2
    assert result["planned_only_stage_attempt_count"] == 2


def test_partial_stage_retry_has_its_own_task_set_and_partition_mapping():
    planned = stage(0, tasks=3, partitions=3)
    first = stage(0, tasks=3, partitions=3)
    retry = stage(0, attempt=1, tasks=1, partitions=3)
    result = validate(
        [
            *header(),
            job_start(0, planned),
            *execution(first, partition_ids=(0, 1, 2)),
            *execution(retry, task_id=3, partition_ids=(2,)),
            job_end(0),
            {"Event": "SparkListenerApplicationEnd"},
        ]
    )
    assert [s["expected_tasks"] for s in result["stage_attempts"]] == [3, 1]
    assert [s["successful_task_indices"] for s in result["stage_attempts"]] == [[0, 1, 2], [0]]
    assert result["stage_attempts"][1]["task_attempts"][0]["partition_id"] == 2


def test_job_snapshot_population_is_not_the_submitted_missing_task_count():
    producer = stage(0, tasks=3, partitions=3)
    planned = stage(1, tasks=3, partitions=3)
    submitted = stage(1, tasks=1, partitions=3)
    result = validate(
        [
            *header(),
            job_start(0, producer),
            *execution(producer, partition_ids=(0, 1, 2)),
            job_end(0),
            job_start(1, planned),
            *execution(submitted, task_id=3, partition_ids=(2,)),
            job_end(1),
            {"Event": "SparkListenerApplicationEnd"},
        ]
    )
    assert result["stage_attempts"][1]["expected_tasks"] == 1


def test_failed_task_can_omit_unavailable_metrics_before_successful_retry():
    events = reuse_events()
    original = next(e for e in events if e["Event"] == "SparkListenerTaskEnd")
    original_start = next(e for e in events if e["Event"] == "SparkListenerTaskStart")
    failed = copy.deepcopy(original)
    failed["Task Info"].update({"Task ID": 10, "Failed": True})
    failed["Task End Reason"] = {"Reason": "ExceptionFailure"}
    failed.pop("Task Metrics")
    failed_start = {
        "Event": "SparkListenerTaskStart",
        "Stage ID": 0,
        "Stage Attempt ID": 0,
        "Task Info": copy.deepcopy(failed["Task Info"]),
    }
    original["Task Info"]["Attempt"] = 1
    original_start["Task Info"]["Attempt"] = 1
    events[events.index(original_start) : events.index(original_start)] = [failed_start, failed]
    result = validate(events)
    assert result["failed_task_attempt_count"] == 1
    assert result["failed_task_records"][0]["metrics"] is None
    assert result["successful_task_count"] == 4


def test_failed_stage_attempt_is_retained_after_successful_retry():
    first, retry = stage(0), stage(0, attempt=1)
    failed = execution(first)
    failed[-1]["Stage Info"]["Failure Reason"] = "FetchFailed"
    result = validate(
        [
            *header(),
            job_start(0, first),
            *failed,
            *execution(retry, task_id=2),
            job_end(0),
            {"Event": "SparkListenerApplicationEnd"},
        ]
    )
    assert result["failed_stage_attempt_count"] == 1
    assert result["successful_stage_attempt_count"] == 1
    assert result["successful_task_count"] == 2


def test_initial_skipped_stage_can_later_submit_attempt_one():
    events = reuse_events()[:-1]
    retry = stage(1, attempt=1, tasks=1)
    events.extend(
        [
            job_start(2, stage(1)),
            *execution(retry, task_id=4, partition_ids=(1,)),
            job_end(2),
            {"Event": "SparkListenerApplicationEnd"},
        ]
    )
    result = validate(events)
    assert result["planned_only_stage_attempt_count"] == 1
    assert result["submitted_stage_attempt_count"] == 3


def test_zero_task_submission_and_empty_job_add_no_executed_work():
    events = reuse_events()[:-1]
    empty = stage(3, tasks=0, rdd=30, partitions=0)
    events.extend(
        [
            job_start(2, empty),
            *execution(empty, task_id=4, partition_ids=()),
            job_end(2),
            job_start(3),
            job_end(3),
            {"Event": "SparkListenerApplicationEnd"},
        ]
    )
    result = validate(events)
    assert result["successful_job_count"] == 4
    assert result["successful_task_count"] == 4
    assert result["stage_attempts"][-1]["successful_partitions"] == 0


def test_late_speculative_task_end_preserves_attempts_without_filling_coverage():
    events = reuse_events()
    original = next(e for e in events if e["Event"] == "SparkListenerTaskEnd")
    late = copy.deepcopy(original)
    late["Task Info"].update({"Task ID": 10, "Attempt": 1, "Killed": True})
    late["Task End Reason"] = {"Reason": "TaskKilled"}
    start = {
        "Event": "SparkListenerTaskStart",
        "Stage ID": 0,
        "Stage Attempt ID": 0,
        "Task Info": copy.deepcopy(late["Task Info"]),
    }
    events.insert(events.index(original), start)
    events.insert(-1, late)
    result = validate(events)
    assert result["successful_task_count"] == 4
    assert result["failed_task_attempt_count"] == 1
    assert result["retried_task_attempt_count"] == 1


def test_distinct_successful_attempts_do_not_inflate_unique_task_coverage():
    events = reuse_events()
    original = next(e for e in events if e["Event"] == "SparkListenerTaskEnd")
    duplicate = copy.deepcopy(original)
    duplicate["Task Info"].update({"Task ID": 10, "Attempt": 1})
    start = {
        "Event": "SparkListenerTaskStart",
        "Stage ID": 0,
        "Stage Attempt ID": 0,
        "Task Info": copy.deepcopy(duplicate["Task Info"]),
    }
    events[events.index(original) + 1 : events.index(original) + 1] = [start, duplicate]
    result = validate(events)
    assert result["successful_task_count"] == 5
    assert result["successful_partition_count"] == 4
    assert result["retried_task_attempt_count"] == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("Task ID", 99),
        ("Partition ID", 1),
        ("Launch Time", 2),
        ("Host", "other"),
        ("Executor ID", "other"),
    ],
)
def test_task_end_must_match_its_start(field, value):
    events = reuse_events()
    end = next(e for e in events if e["Event"] == "SparkListenerTaskEnd")
    end["Task Info"][field] = value
    with pytest.raises(ScopeError):
        validate(events)


@pytest.mark.parametrize(
    "change",
    [
        "missing-stage-end",
        "no-stage-start",
        "no-task-start",
        "orphan-task",
        "failed-job",
        "failed-final-stage",
        "duplicate-end",
        "duplicate-start",
        "missing-coverage",
        "wrong-executor",
        "missing-metric",
        "malformed-metric",
        "no-app-end",
        "late-only-success",
    ],
)
def test_rejects_incomplete_or_contradictory_execution(change):
    events = reuse_events()
    task = next(e for e in events if e["Event"] == "SparkListenerTaskEnd")
    start = next(e for e in events if e["Event"] == "SparkListenerTaskStart")
    completed = next(e for e in events if e["Event"] == "SparkListenerStageCompleted")
    if change == "missing-stage-end":
        events.remove(completed)
    elif change == "no-stage-start":
        events.remove(next(e for e in events if e["Event"] == "SparkListenerStageSubmitted"))
    elif change == "no-task-start":
        events.remove(start)
    elif change == "orphan-task":
        task["Stage ID"] = 999
    elif change == "failed-job":
        next(e for e in events if e["Event"] == "SparkListenerJobEnd")["Job Result"] = {
            "Result": "JobFailed"
        }
    elif change == "failed-final-stage":
        completed["Stage Info"]["Failure Reason"] = "failure"
    elif change in {"duplicate-start", "duplicate-end"}:
        original = start if change.endswith("start") else task
        events.insert(events.index(original), copy.deepcopy(original))
    elif change == "missing-coverage":
        events.remove(start)
        events.remove(task)
    elif change == "wrong-executor":
        for event in (start, task):
            event["Task Info"]["Host"] = "spark-worker-2"
    elif change == "missing-metric":
        task["Task Metrics"].pop("JVM GC Time")
    elif change == "malformed-metric":
        task["Task Metrics"]["Input Metrics"] = []
    elif change == "no-app-end":
        events.pop()
    else:
        events.remove(task)
        events.insert(events.index(completed) + 1, task)
    with pytest.raises(ScopeError):
        validate(events)


@pytest.mark.parametrize(
    "bad_payload",
    [
        b'{"Event":\n',
        b"{}\n",
        b"[]\n",
        b"\n",
        b'{"Event":"x","Event":"y"}\n',
        b'{"Event":"x","bad":NaN}\n',
        _event_log().replace(
            b'"Spark Version": "3.5.7"',
            b'"Spark Version": "3.5.7", "Spark Version": "3.5.7"',
            1,
        ),
        _event_log().replace(
            b'"Spark Version": "3.5.7"', b'"Spark Version": "3.5.7", "invalid": NaN', 1
        ),
        _event_log().rstrip(b"\n"),
    ],
)
def test_malformed_or_truncated_logs_fail_closed(bad_payload):
    with pytest.raises(ScopeError):
        completed_execution(_event_client(bad_payload), CONFIG, "run")


@pytest.mark.parametrize(
    "change", ["inprogress", "multiple", "changed-size", "oversized", "wrong-directory"]
)
def test_event_log_read_boundary_is_preserved(change):
    client = _event_client(_event_log())
    kwargs = {}
    if change in {"inprogress", "multiple"}:
        client.list_children = lambda path: (
            [path + "/app.inprogress"]
            if change == "inprogress"
            else [path + "/app-1", path + "/app-2"]
        )
    elif change == "changed-size":
        client.stat = lambda path: {"size_bytes": 1}
    elif change == "oversized":
        client.stat = lambda path: {"size_bytes": MAX_EVENT_BYTES + 1}
    else:
        kwargs["expected_event_log_dir"] = "/other"
    with pytest.raises(ScopeError):
        completed_execution(client, CONFIG, "run", **kwargs)


@pytest.mark.parametrize(
    "field,value",
    [("Number of Tasks", 1), ("Parent IDs", [99]), ("Stage Attempt ID", 1), ("Failure Reason", {})],
)
def test_stage_completion_must_reconcile_with_submission(field, value):
    events = reuse_events()
    next(e for e in events if e["Event"] == "SparkListenerStageCompleted")["Stage Info"][field] = (
        value
    )
    with pytest.raises(ScopeError):
        validate(events)
