"""No live services: artifact reuse, authorization, and exact-run staging boundaries."""

import copy
import json
import sys
from types import SimpleNamespace

import pytest

from tests.unit.test_m5_small import evidence
from warehouse_bigdata.contracts.dataset_scope import ScopeError


def complete_selector():
    from warehouse_bigdata.common.small_demo import RUN_KEYS

    scope = evidence()
    return dict(
        schema_version="small-artifacts-v1",
        dataset_scope="demo_sample",
        source_version=scope["source_version"],
        source_snapshot_id=scope["source_snapshot_id"],
        selection_digest=scope["selection_digest"],
        feature_version="historical-v1",
        model_run_id="small-model",
        forecast_run_id="small-forward",
        validation_evaluation_run_id="small-validation",
        test_evaluation_run_id="small-test",
        **{key: "small-" + stage for stage, key in RUN_KEYS.items()},
    )


def _execution_events():
    from tests.unit.test_m06_acceptance import _event_log

    return [json.loads(line) for line in _event_log().splitlines()]


def _execution_client(events):
    payload = ("\n".join(json.dumps(event) for event in events) + "\n").encode()
    return SimpleNamespace(
        list_children=lambda path: [path + "/app-1"],
        stat=lambda path: SimpleNamespace(size_bytes=len(payload)),
        read_remote=lambda path: payload,
    )


def _task_events(events):
    return [event for event in events if event["Event"] == "SparkListenerTaskEnd"]


def _task_event(events, index):
    return next(event for event in _task_events(events) if event["Task Info"]["Index"] == index)


def test_immutable_scope_write_rejects_different_selection():
    from warehouse_bigdata.common.scope_evidence import read_scope, write_scope

    class Memory:
        files = {}

        def exists(self, path):
            return path in self.files

        def upload_bytes(self, payload, path):
            assert path not in self.files
            self.files[path] = payload

        def read_remote(self, path):
            return self.files[path]

    client = Memory()
    scope = evidence()
    write_scope(client, "/candidate", scope)
    before = dict(client.files)
    write_scope(client, "/candidate", scope)
    assert before == client.files
    assert read_scope(client, "/candidate") == scope
    other = {**scope, "source_snapshot_id": "other"}
    with pytest.raises(ScopeError):
        write_scope(client, "/candidate", other)
    with pytest.raises(ScopeError, match="missing"):
        read_scope(client, "/legacy")


def test_scope_read_uses_exact_namenode_status_and_preserves_permission_denial():
    from warehouse_bigdata.common.hdfs_client import HdfsCommandError
    from warehouse_bigdata.common.scope_evidence import read_scope

    def no_cli(path):
        pytest.fail("scope reads must use available exact-path NameNode status")

    client = SimpleNamespace(
        path_status=lambda path: {"type": "FILE"},
        exists=no_cli,
        read_remote=lambda path: json.dumps(evidence()).encode(),
    )
    assert read_scope(client, "/published") == evidence()

    def denied(path):
        raise HdfsCommandError("access denied")

    client.path_status = denied
    with pytest.raises(HdfsCommandError, match="denied"):
        read_scope(client, "/published")


def test_canonical_selector_never_falls_back(tmp_path):
    from warehouse_bigdata.common.small_artifacts import load_small_artifacts

    path = tmp_path / "small.json"
    with pytest.raises((ScopeError, FileNotFoundError)):
        load_small_artifacts(path)
    document = dict(
        schema_version="small-artifacts-v1",
        dataset_scope="demo_sample",
        source_version="sha256:" + "a" * 64,
        source_snapshot_id="small-snapshot",
        selection_digest=evidence()["selection_digest"],
        j02_pipeline_run_id="small-j02",
        j03_pipeline_run_id="small-j03",
        feature_version="historical-v1",
        j04_pipeline_run_id="small-j04",
    )
    path.write_text(json.dumps(document))
    assert load_small_artifacts(path)["dataset_scope"] == "demo_sample"
    for bad in (
        {**document, "dataset_scope": "full_m5"},
        {**document, "source_snapshot_id": "m04-smoke-legacy"},
        {k: v for k, v in document.items() if k != "selection_digest"},
    ):
        path.write_text(json.dumps(bad))
        with pytest.raises(ScopeError):
            load_small_artifacts(path)


def test_live_execution_requires_authorization_before_any_command(tmp_path):
    from warehouse_bigdata.common.small_demo import run_live

    with pytest.raises(ScopeError, match="authorization"):
        run_live({}, stages=["j02"], authorize=False, evidence_dir=tmp_path)


def test_real_data_collection_is_serial_and_opt_in():
    from tests.integration.conftest import pytest_collection_modifyitems

    class Config:
        def getoption(self, option, default=None):
            return False

    class Item:
        marks = []

        def get_closest_marker(self, marker):
            return marker == "m5_small"

        def add_marker(self, marker):
            self.marks.append(marker)

    item = Item()
    pytest_collection_modifyitems(Config(), [item])
    assert any("--run-small-integration" in mark.kwargs.get("reason", "") for mark in item.marks)


def test_training_default_and_strata_diagnostics():
    from warehouse_bigdata.forecasting.training import (
        TrainingConfig,
        TrainingError,
        allocate_quotas,
    )

    assert TrainingConfig().cap == 10000
    quotas = allocate_quotas([(index, 3) for index in range(4200)], 10000)
    assert len(quotas) == 4200 and sum(quotas.values()) == 10000 and min(quotas.values()) >= 1
    with pytest.raises(TrainingError, match=r"cap=1.*occupied_strata=2.*eligible_rows=3"):
        allocate_quotas([("a", 1), ("b", 2)], 1)


def test_owner_staging_refuses_nonempty_or_foreign_state():
    from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
    from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError, prepare_run_staging

    client = PermissionHdfs()
    client.identity = "spark"
    with pytest.raises(HdfsNamespaceError):
        prepare_run_staging(client, CONFIG, pipeline_run_id="small-j02")


def test_owner_preparation_is_exact_and_refuses_execution_evidence():
    from pathlib import PurePosixPath

    from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
    from warehouse_bigdata.common.hdfs_namespace import (
        HdfsNamespaceError,
        prepare_run_staging,
        provision_publication_namespace,
    )
    from warehouse_bigdata.common.hdfs_permissions import HDFS_SPARK_ACL_SPEC

    class Client(PermissionHdfs):
        acls = {}

        def get_acl(self, path):
            return self.acls.get(path, super().get_acl(path))

        def set_acl(self, path, spec):
            self.calls.append(("acl", path, spec))
            self.acls[path] = spec.replace(",", "\n")

        def list_children(self, path):
            return tuple(p for p in self.entries if str(PurePosixPath(p).parent) == path)

    client = Client()
    provision_publication_namespace(client, CONFIG)
    before = set(client.entries)
    result = prepare_run_staging(client, CONFIG, pipeline_run_id="small-j02", events=True)
    root = result["staging_path"]
    assert set(client.entries) - before == {root, root + "/events"}
    assert client.acls[root] == HDFS_SPARK_ACL_SPEC.replace(",", "\n")
    client.upload_bytes(b"frozen", root + "/input_binding.json")
    with pytest.raises(HdfsNamespaceError, match="evidence"):
        prepare_run_staging(client, CONFIG, pipeline_run_id="small-j02", events=True)
    client.remove(root + "/input_binding.json")
    client.entries[root]["permission"] = "777"
    with pytest.raises(HdfsNamespaceError, match="world-writable"):
        prepare_run_staging(client, CONFIG, pipeline_run_id="small-j02", events=True)


@pytest.mark.parametrize("partition_ids", [(0, 1), (11, 29)])
def test_execution_evidence_separates_task_set_index_and_rdd_partition_id(partition_ids):
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = _execution_events()
    for event, partition_id in zip(_task_events(events), partition_ids, strict=True):
        event["Task Info"]["Partition ID"] = partition_id

    execution = completed_execution(_execution_client(events), CONFIG, "small-j02")
    tasks = execution["stage_attempts"][0]["task_attempts"]
    assert [task["task_set_index"] for task in tasks] == [0, 1]
    assert [task["partition_id"] for task in tasks] == list(partition_ids)
    assert [task["partition"] for task in tasks] == list(partition_ids)
    assert execution["stage_attempts"][0]["successful_task_indices"] == [0, 1]
    assert execution["successful_partition_count"] == 2


def test_execution_evidence_accepts_missing_rdd_partition_id_as_legacy_compatibility():
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = _execution_events()
    for event in _task_events(events):
        event["Task Info"].pop("Partition ID")

    execution = completed_execution(_execution_client(events), CONFIG, "small-j02")
    tasks = execution["stage_attempts"][0]["task_attempts"]
    assert [task["partition_id"] for task in tasks] == [None, None]
    assert [task["partition"] for task in tasks] == [0, 1]


def test_execution_evidence_rejects_missing_task_set_index_and_incomplete_coverage():
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    missing_index = _execution_events()
    _task_event(missing_index, 0)["Task Info"].pop("Index")
    with pytest.raises(ScopeError, match="task-set index"):
        completed_execution(_execution_client(missing_index), CONFIG, "small-j02")

    incomplete = _execution_events()
    task = _task_event(incomplete, 1)
    incomplete.remove(task)
    with pytest.raises(ScopeError, match="task-set index coverage"):
        completed_execution(_execution_client(incomplete), CONFIG, "small-j02")


@pytest.mark.parametrize(
    "field,value,pattern",
    [
        ("Index", "not-an-int", "task-set index"),
        ("Index", -1, "task-set index"),
        ("Partition ID", "not-an-int", "RDD partition ID"),
        ("Partition ID", -1, "RDD partition ID"),
    ],
)
def test_execution_evidence_rejects_malformed_task_identity(field, value, pattern):
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = _execution_events()
    _task_event(events, 0)["Task Info"][field] = value
    with pytest.raises(ScopeError, match=pattern):
        completed_execution(_execution_client(events), CONFIG, "small-j02")


def test_execution_evidence_rejects_duplicate_logical_task_attempt():
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = _execution_events()
    duplicate = copy.deepcopy(_task_event(events, 0))
    duplicate["Task Info"]["Task ID"] = 100
    stage_completed = next(
        index
        for index, event in enumerate(events)
        if event["Event"] == "SparkListenerStageCompleted"
    )
    events.insert(stage_completed, duplicate)
    with pytest.raises(ScopeError, match="duplicate logical task attempt"):
        completed_execution(_execution_client(events), CONFIG, "small-j02")


def _retried_execution_events(*, conflicting_partition=False, missing_first_attempt=False):
    events = _execution_events()
    original = _task_event(events, 0)
    if missing_first_attempt:
        original["Task Info"]["Attempt"] = 1
        return events
    failed = copy.deepcopy(original)
    failed["Task Info"].update({"Task ID": 100, "Attempt": 0, "Failed": True})
    failed["Task End Reason"] = {"Reason": "FetchFailed"}
    original["Task Info"].update({"Task ID": 101, "Attempt": 1})
    if conflicting_partition:
        failed["Task Info"]["Partition ID"] = 11
        original["Task Info"]["Partition ID"] = 12
    events.insert(events.index(original), failed)
    return events


def test_execution_evidence_keeps_failed_retry_attributed_to_its_logical_task():
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    execution = completed_execution(
        _execution_client(_retried_execution_events()), CONFIG, "small-j02"
    )
    assert execution["successful_task_count"] == 2
    assert execution["failed_task_attempt_count"] == 1
    assert execution["retried_task_attempt_count"] == 1
    assert len(execution["retried_task_records"]) == 2


@pytest.mark.parametrize(
    "events,pattern",
    [
        (_retried_execution_events(missing_first_attempt=True), "retry attempt sequence"),
        (_retried_execution_events(conflicting_partition=True), "RDD partition ID differs"),
    ],
)
def test_execution_evidence_rejects_inconsistent_retry_identity(events, pattern):
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    with pytest.raises(ScopeError, match=pattern):
        completed_execution(_execution_client(events), CONFIG, "small-j02")


@pytest.mark.parametrize(
    "removed_event,pattern",
    [("SparkListenerStageCompleted", "stage"), ("SparkListenerApplicationEnd", "ApplicationEnd")],
)
def test_execution_evidence_requires_stage_and_application_termination(removed_event, pattern):
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [event for event in _execution_events() if event["Event"] != removed_event]
    with pytest.raises(ScopeError, match=pattern):
        completed_execution(_execution_client(events), CONFIG, "small-j02")


def test_execution_evidence_rejects_successful_work_on_only_one_worker():
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = _execution_events()
    for event in _task_events(events):
        event["Task Info"].update(Host="spark-worker-1", **{"Executor ID": "1"})
    with pytest.raises(ScopeError, match="both named Workers"):
        completed_execution(_execution_client(events), CONFIG, "small-j02")


def test_execution_evidence_requires_both_workers_and_completed_standalone():
    from types import SimpleNamespace

    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [
        dict(Event="SparkListenerLogStart", **{"Spark Version": "3.5.7"}),
        dict(
            Event="SparkListenerApplicationStart",
            **{"App Name": "test", "App ID": "app-test"},
        ),
        dict(
            Event="SparkListenerEnvironmentUpdate",
            **{"Spark Properties": {"spark.master": "spark://spark-master:7077"}},
        ),
        *[
            dict(
                Event="SparkListenerExecutorAdded",
                **{
                    "Executor ID": str(index + 1),
                    "Executor Info": {"Host": host, "Total Cores": 1},
                },
            )
            for index, host in enumerate(("spark-worker-1", "spark-worker-2"))
        ],
        dict(
            Event="SparkListenerJobStart",
            **{
                "Job ID": 1,
                "Stage IDs": [1],
                "Stage Infos": [{"Stage ID": 1, "Stage Attempt ID": 0, "Number of Tasks": 2}],
            },
        ),
        dict(
            Event="SparkListenerStageSubmitted",
            **{
                "Stage Info": {"Stage ID": 1, "Stage Attempt ID": 0, "Number of Tasks": 2},
                "Properties": {},
            },
        ),
    ]
    for index, host in enumerate(("spark-worker-1", "spark-worker-2")):
        events.append(
            dict(
                Event="SparkListenerTaskEnd",
                **{
                    "Stage ID": 1,
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
                        "Disk Bytes Spilled": 12,
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
                        "Stage ID": 1,
                        "Stage Attempt ID": 0,
                        "Number of Tasks": 2,
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
    payload = ("\n".join(json.dumps(e) for e in events) + "\n").encode()
    client = SimpleNamespace(
        list_children=lambda p: [p + "/app-1"],
        stat=lambda p: SimpleNamespace(size_bytes=len(payload)),
        read_remote=lambda p: payload,
    )
    assert len(completed_execution(client, CONFIG, "small-j02")["workers"]) == 2
    payload = ("\n".join(json.dumps(e) for e in events[:-1]) + "\n").encode()
    with pytest.raises(ScopeError, match="ApplicationEnd"):
        completed_execution(client, CONFIG, "small-j02")


@pytest.mark.parametrize(
    "through,key",
    [
        ("j03", "j02_pipeline_run_id"),
        ("j03", "j03_pipeline_run_id"),
        ("j04", "j04_pipeline_run_id"),
        ("j04", "feature_version"),
        ("j05", "j05_pipeline_run_id"),
        ("j05", "model_run_id"),
        ("j06", "j06_pipeline_run_id"),
        ("j06", "forecast_run_id"),
        ("validation", "validation_pipeline_run_id"),
        ("validation", "validation_evaluation_run_id"),
        ("test", "test_pipeline_run_id"),
        ("test", "test_evaluation_run_id"),
        ("test", "model_run_id"),
        ("test", "forecast_run_id"),
        ("test", "validation_evaluation_run_id"),
    ],
)
def test_readback_requires_every_identity_through_requested_stage(through, key):
    from warehouse_bigdata.jobs.verify_small_artifacts import verify

    selector = complete_selector()
    selector.pop(key)
    with pytest.raises(ScopeError, match=key):
        verify(None, selector, through=through, client=None, config=None)


def test_default_full_readback_rejects_processing_only_selector():
    from warehouse_bigdata.jobs.verify_small_artifacts import verify

    selector = {
        k: v
        for k, v in complete_selector().items()
        if k
        in {
            "schema_version",
            "dataset_scope",
            "source_version",
            "source_snapshot_id",
            "selection_digest",
            "j02_pipeline_run_id",
            "j03_pipeline_run_id",
        }
    }
    with pytest.raises(ScopeError, match="j04_pipeline_run_id"):
        verify(None, selector, client=None, config=None)


@pytest.mark.parametrize("through", ["j03", "j04", "j05", "j06", "validation", "test"])
def test_readback_reports_exact_verified_extent(monkeypatch, through):
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common import small_preflight
    from warehouse_bigdata.forecasting import (
        acceptance,
        evaluation,
        evaluation_artifacts,
        feature_input,
        forecast_artifacts,
        model_artifacts,
        recursive_forecasting,
    )
    from warehouse_bigdata.ingestion import hdfs_raw
    from warehouse_bigdata.jobs import verify_small_artifacts as module
    from warehouse_bigdata.processing import m04_verification

    selector = complete_selector()
    scope = {**evidence(), "original_series_count": 30490}
    monkeypatch.setattr(
        small_preflight, "validate_small_publications", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(module, "read_scope", lambda *args, **kwargs: scope)
    monkeypatch.setattr(hdfs_raw, "validate_hdfs_raw_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        m04_verification,
        "run_m04_verification",
        lambda *args, **kwargs: SimpleNamespace(as_dict=lambda: {"status": "VALIDATED"}),
    )

    class Frame:
        def count(self):
            return 2800

        def where(self, *args):
            return self

        def limit(self, *args):
            return self

        def select(self, *args):
            return self

        def collect(self):
            return [SimpleNamespace(raw_prediction=1.0)]

    frame = Frame()
    monkeypatch.setattr(
        feature_input,
        "read_validated_forecast_features_snapshot",
        lambda *args, **kwargs: (frame, {"scope": scope}),
    )
    monkeypatch.setattr(
        model_artifacts,
        "read_validated_model_metadata",
        lambda **kwargs: {
            "input_binding": {"scope": scope},
            "manifest": {"training_row_count": 10000},
        },
    )
    monkeypatch.setattr(
        model_artifacts,
        "load_validated_model",
        lambda *args, **kwargs: SimpleNamespace(transform=lambda *args: frame),
    )
    monkeypatch.setattr(
        forecast_artifacts, "validate_forecast_evidence", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        recursive_forecasting, "validate_forecast_daily", lambda *args, **kwargs: {"row_count": 700}
    )
    monkeypatch.setattr(
        evaluation_artifacts, "validate_evaluation_evidence", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(evaluation, "validate_metrics", lambda *args: None)
    monkeypatch.setattr(
        acceptance,
        "verify_ml_artifacts",
        lambda *args, **kwargs: {
            key: ({"row_count": 2800} if key == "features" else {})
            for key, stage in (
                ("features", "j04"),
                ("model", "j05"),
                ("forecast", "j06"),
                ("validation", "validation"),
                ("test", "test"),
            )
            if kwargs["through"] in ("j04", "j05", "j06", "validation", "test")
            and ["j04", "j05", "j06", "validation", "test"].index(stage)
            <= ["j04", "j05", "j06", "validation", "test"].index(kwargs["through"])
        },
    )
    client = SimpleNamespace(read_remote=lambda path: b'{"created_at":"2026-01-01T00:00:00Z"}')
    spark = SimpleNamespace(
        read=SimpleNamespace(parquet=lambda path: frame), createDataFrame=lambda *args: frame
    )

    report = module.verify(spark, selector, through=through, client=client, config=CONFIG)
    stages = ("j02", "j03", "j04", "j05", "j06", "validation", "test")
    expected = list(stages[: stages.index(through) + 1])
    assert report["status"] == "VALIDATED"
    assert report["verified_through"] == through
    assert report["verified_stages"] == expected
    for stage, key in (
        ("j04", "features"),
        ("j05", "model"),
        ("j06", "forecast"),
        ("validation", "validation"),
        ("test", "test"),
    ):
        assert (key in report) == (stage in expected)


@pytest.mark.parametrize("through", ["j03", "test"])
def test_verify_cli_forwards_explicit_required_stage(monkeypatch, tmp_path, through):
    from warehouse_bigdata.common import small_demo

    path = tmp_path / "selector.json"
    path.write_text(json.dumps(complete_selector()))
    calls = []
    monkeypatch.setattr(
        small_demo.subprocess,
        "run",
        lambda command, **kwargs: calls.append(command) or SimpleNamespace(returncode=0),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "small-demo",
            "verify",
            "--artifacts",
            str(path),
            "--through",
            through,
            "--authorize-live",
        ],
    )
    with pytest.raises(SystemExit) as result:
        small_demo.main()
    assert result.value.code == 0
    assert calls[0][calls[0].index("--through") + 1] == through


def test_verify_cli_rejects_missing_ml_before_submission(monkeypatch, tmp_path):
    from warehouse_bigdata.common import small_demo

    selector = complete_selector()
    selector.pop("model_run_id")
    path = tmp_path / "selector.json"
    path.write_text(json.dumps(selector))
    monkeypatch.setattr(
        sys, "argv", ["small-demo", "verify", "--artifacts", str(path), "--authorize-live"]
    )
    calls = []
    monkeypatch.setattr(small_demo.subprocess, "run", lambda *args, **kwargs: calls.append(args))
    with pytest.raises(ScopeError, match="model_run_id"):
        small_demo.main()
    assert calls == []


def fake_live_commands(monkeypatch, *, preflight_status=0, measured_digest=None):
    from warehouse_bigdata.common import small_demo

    calls = []

    def popen(args, stdout, stderr):
        calls.append(args)
        if "warehouse_bigdata.common.small_evidence" in args:
            scope = evidence()
            if measured_digest:
                scope = {**scope, "selection_digest": measured_digest}
            stdout.write(
                json.dumps({"execution": {"application_id": "app-1"}, "scope": scope}).encode()
            )
        status = preflight_status if "warehouse_bigdata.common.small_preflight" in args else 0
        return SimpleNamespace(wait=lambda **kwargs: status)

    monkeypatch.setattr(small_demo.subprocess, "Popen", popen)
    return calls


def test_failed_preflight_prevents_prepare_and_spark_submission(monkeypatch, tmp_path):
    from warehouse_bigdata.common.small_demo import run_live

    calls = fake_live_commands(monkeypatch, preflight_status=23)
    with pytest.raises(ScopeError, match="preflight"):
        run_live(complete_selector(), stages=["j03"], authorize=True, evidence_dir=tmp_path / "run")
    assert not any(
        "prepare-run" in command or "scripts/spark-submit.sh" in command for command in calls
    )
    assert any("warehouse_bigdata.common.small_preflight" in command for command in calls)


@pytest.mark.parametrize("digest", [None, evidence()["selection_digest"]])
def test_j02_initializes_only_unresolved_digest_after_preflight(monkeypatch, tmp_path, digest):
    from warehouse_bigdata.common.small_demo import run_live

    selector = {**complete_selector(), "selection_digest": digest}
    calls = fake_live_commands(monkeypatch)
    report = run_live(
        selector, stages=["j02", "j03"], authorize=True, evidence_dir=tmp_path / "run"
    )
    assert report["status"] == "EXECUTED_PENDING_INDEPENDENT_READBACK"
    assert selector["selection_digest"] == evidence()["selection_digest"]
    for stage in ("j02", "j03"):
        preflight = next(
            command
            for command in calls
            if "--stage" in command and command[command.index("--stage") + 1] == stage
        )
        submit = next(
            command
            for command in calls
            if f"warehouse_bigdata/jobs/{'normalize_sales_daily' if stage == 'j02' else 'enrich_analytics'}.py"
            in command
        )
        prepare = next(
            command for command in calls if "prepare-run" in command and "small-" + stage in command
        )
        assert calls.index(preflight) < calls.index(prepare) < calls.index(submit)
        frozen = json.loads(preflight[preflight.index("--selector-json") + 1])
        assert frozen["selection_digest"] == (
            digest if stage == "j02" else selector["selection_digest"]
        )


def test_j02_readback_cannot_replace_a_frozen_digest(monkeypatch, tmp_path):
    from warehouse_bigdata.common.small_demo import run_live

    selector = complete_selector()
    original = dict(selector)
    fake_live_commands(monkeypatch, measured_digest="sha256:" + "b" * 64)
    with pytest.raises(ScopeError, match="digest"):
        run_live(selector, stages=["j02", "j03"], authorize=True, evidence_dir=tmp_path / "run")
    assert selector == original
    report = json.loads((tmp_path / "run/report.json").read_text())
    assert report["status"] == "FAILED"
    assert "j03" not in report["stages"]


def test_full_chain_integration_readback_rejects_partial_report(monkeypatch, tmp_path):
    from tests.integration import small_support

    def command(*args, on_stdout_line, **kwargs):
        on_stdout_line(b'SMALL_READBACK={"status":"VALIDATED","verified_through":"j03"}')

    monkeypatch.setattr(small_support, "container_command", command)
    with pytest.raises(AssertionError, match="test"):
        small_support.readback(tmp_path, selector=complete_selector())
