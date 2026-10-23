"""No live services: artifact reuse, authorization, and exact-run staging boundaries."""

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


def test_execution_evidence_requires_both_workers_and_completed_standalone():
    from types import SimpleNamespace

    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.small_evidence import completed_execution

    events = [
        dict(Event="SparkListenerApplicationStart", **{"App ID": "app-test"}),
        dict(Event="SparkListenerApplicationEnd"),
        dict(
            Event="SparkListenerEnvironmentUpdate",
            **{"Spark Properties": {"spark.master": "spark://spark-master:7077"}},
        ),
    ]
    for host in ("spark-worker-1", "spark-worker-2"):
        events.append(
            dict(
                Event="SparkListenerTaskEnd",
                **{
                    "Stage ID": 1,
                    "Task End Reason": {"Reason": "Success"},
                    "Task Info": {"Host": host},
                    "Task Metrics": {"Disk Bytes Spilled": 12},
                },
            )
        )
    payload = "\n".join(json.dumps(e) for e in events).encode()
    client = SimpleNamespace(
        list_children=lambda p: [p + "/app-1"],
        stat=lambda p: SimpleNamespace(size_bytes=len(payload)),
        read_remote=lambda p: payload,
    )
    assert len(completed_execution(client, CONFIG, "small-j02")["workers"]) == 2
    payload = "\n".join(json.dumps(e) for e in events[:-1]).encode()
    with pytest.raises(ScopeError, match="both"):
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
