"""Offline regression checks for the reduced-scope live M04 coordinator."""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.integration import m04_support as support
from tests.integration.hdfs_support import VerificationError
from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import render_raw_m5_path
from warehouse_bigdata.ingestion.hdfs_raw import (
    HdfsIntegrityEvidence,
    HdfsRawIngestionError,
    ValidatedHdfsRawSnapshot,
)
from warehouse_bigdata.ingestion.m5_source import (
    OFFICIAL_SOURCE_URL,
    compute_source_version,
)
from warehouse_bigdata.processing.normalization_snapshot import (
    NormalizationSnapshotConfig,
    build_chunk_plan,
)

CONFIG = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")
TOKEN = "a" * 32


def _snapshot() -> ValidatedHdfsRawSnapshot:
    names = ("sales_train_evaluation.csv", "calendar.csv", "sell_prices.csv")
    hashes = {name: "sha256:" + str(index) * 64 for index, name in enumerate(names)}
    version = compute_source_version(hashes)
    raw_path = render_raw_m5_path(CONFIG.base_path, version)
    return ValidatedHdfsRawSnapshot(
        status="VALIDATED",
        source_version=version,
        raw_path=raw_path,
        manifests=tuple(
            {
                "source_file": name,
                "artifact_checksum": digest,
                "source_name": "m5-forecasting-accuracy",
                "source_url": OFFICIAL_SOURCE_URL,
                "source_schema_version": "m5-source-v1",
                "downloaded_at_utc": None,
            }
            for name, digest in hashes.items()
        ),
        integrity=HdfsIntegrityEvidence(
            "original-ingestion", version, raw_path, "/original-staging", 2, (), (), "original-time"
        ),
    )


def test_smoke_scope_is_exactly_first_30_days_for_every_series() -> None:
    config = NormalizationSnapshotConfig(
        days_per_chunk=support.SMOKE_DAY_COUNT,
        expected_day_count=support.SMOKE_DAY_COUNT,
    )
    chunks = build_chunk_plan(config, "/data/warehouse/staging/pipeline_run_id=smoke")

    assert config.expected_series_count == 30_490
    assert config.row_count == support.SMOKE_ROW_COUNT == 914_700
    assert len(chunks) == 1
    assert chunks[0].day_keys == tuple(f"d_{day}" for day in range(1, 31))
    assert NormalizationSnapshotConfig().row_count == support.FULL_ROW_COUNT == 59_181_090


def test_smoke_preflight_finishes_before_source_preparation_and_uses_unique_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = []
    client = object()
    monkeypatch.setattr(support, "load_hdfs_config", lambda: CONFIG)
    monkeypatch.setattr(support, "hdfs_client", lambda hdfs: client)

    def preflight(hdfs, config, actual_client, *, token):
        assert config == CONFIG and actual_client is client
        calls.append(("preflight", token))
        return {"status": "READY"}

    def prepare(*args, **kwargs):
        calls.append(("source", None))
        return {}

    def case(*args, **kwargs):
        calls.append(("case", kwargs))
        assert args[1]["preflight"] == {"status": "READY"}
        return {"status": "VALIDATED"}

    monkeypatch.setattr(support, "_smoke_preflight", preflight)
    monkeypatch.setattr(support, "_prepare_smoke_source", prepare)
    monkeypatch.setattr(support, "run_case", case)

    for _ in range(2):
        support.run_smoke(SimpleNamespace(), tmp_path)

    assert [name for name, _ in calls] == ["preflight", "source", "case"] * 2
    configurations = [value for name, value in calls if name == "case"]
    assert configurations[0]["token"] != configurations[1]["token"]
    for configuration in configurations:
        assert configuration == {
            "label": "smoke",
            "expected_day_count": 30,
            "days_per_chunk": 30,
            "shuffle_partitions": 8,
            "adaptive": True,
            "cores_max": 2,
            "verify_recovery": False,
            "token": configuration["token"],
        }


def test_smoke_preflight_failure_prevents_source_work(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(support, "load_hdfs_config", lambda: CONFIG)
    monkeypatch.setattr(support, "hdfs_client", lambda hdfs: object())

    def blocked(*args, **kwargs):
        raise VerificationError("Spark or ACL prerequisite blocked")

    monkeypatch.setattr(support, "_smoke_preflight", blocked)
    monkeypatch.setattr(
        support, "_prepare_smoke_source", lambda *args, **kwargs: pytest.fail("source work started")
    )

    with pytest.raises(VerificationError, match="prerequisite blocked"):
        support.run_smoke(SimpleNamespace(), tmp_path)


def test_validated_raw_reuse_keeps_production_integrity_gate_without_local_profiling(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    snapshot = _snapshot()
    client = object()
    calls = []
    monkeypatch.setenv("WAREHOUSE_M04_SOURCE_VERSION", snapshot.source_version)
    monkeypatch.delenv("WAREHOUSE_M5_SOURCE_URL", raising=False)
    monkeypatch.delenv("WAREHOUSE_M5_DOWNLOADED_AT_UTC", raising=False)
    monkeypatch.setattr(
        support, "source_environment", lambda: pytest.fail("local CSVs were consulted")
    )
    monkeypatch.setattr(
        support, "run_m5_ingestion", lambda *args, **kwargs: pytest.fail("local profiling ran")
    )

    def validate(version, **kwargs):
        calls.append((version, kwargs))
        return snapshot

    monkeypatch.setattr(support, "validate_hdfs_raw_snapshot", validate)

    source = support._prepare_smoke_source(tmp_path, SimpleNamespace(), CONFIG, client)

    assert calls == [(snapshot.source_version, {"client": client, "config": CONFIG})]
    assert source["source_version"] == snapshot.source_version
    assert source["raw_path"] == snapshot.raw_path
    assert source["raw_manifests"] == snapshot.manifests
    assert source["preparation"]["mode"] == "validated-raw-reuse"
    assert source["preparation"]["integrity"]["pipeline_run_id"] == "original-ingestion"


def test_invalid_reused_raw_never_falls_back_to_profiling(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("WAREHOUSE_M04_SOURCE_VERSION", "sha256:" + "b" * 64)

    def invalid(*args, **kwargs):
        raise HdfsRawIngestionError("raw SHA-256 mismatch")

    monkeypatch.setattr(support, "validate_hdfs_raw_snapshot", invalid)
    monkeypatch.setattr(
        support, "prepare_source", lambda *args: pytest.fail("invalid raw was hidden by fallback")
    )

    with pytest.raises(HdfsRawIngestionError, match="SHA-256 mismatch"):
        support._prepare_smoke_source(tmp_path, SimpleNamespace(), CONFIG, object())


@pytest.mark.parametrize("conflict", ["identity", "url", "acquisition-time", "unofficial"])
def test_raw_reuse_rejects_identity_and_provenance_conflicts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, conflict: str
) -> None:
    snapshot = _snapshot()
    monkeypatch.setenv("WAREHOUSE_M04_SOURCE_VERSION", snapshot.source_version)
    monkeypatch.delenv("WAREHOUSE_M5_SOURCE_URL", raising=False)
    monkeypatch.delenv("WAREHOUSE_M5_DOWNLOADED_AT_UTC", raising=False)
    if conflict == "identity":
        snapshot = replace(snapshot, source_version="sha256:" + "f" * 64)
    elif conflict == "url":
        monkeypatch.setenv("WAREHOUSE_M5_SOURCE_URL", "https://example.com/not-m5")
    elif conflict == "acquisition-time":
        monkeypatch.setenv("WAREHOUSE_M5_DOWNLOADED_AT_UTC", "2026-10-22T00:00:00Z")
    else:
        snapshot = replace(
            snapshot,
            manifests=tuple(
                {**manifest, "source_url": "https://example.com/not-m5"}
                for manifest in snapshot.manifests
            ),
        )
    monkeypatch.setattr(support, "validate_hdfs_raw_snapshot", lambda *args, **kwargs: snapshot)

    with pytest.raises((VerificationError, ValueError), match="identity|provenance|Kaggle"):
        support._prepare_smoke_source(tmp_path, SimpleNamespace(), CONFIG, object())


def test_smoke_without_raw_version_uses_existing_ingestion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("WAREHOUSE_M04_SOURCE_VERSION", raising=False)
    prepared = {"source_version": "existing-ingestion-result"}
    calls = []
    hdfs = object()
    monkeypatch.setattr(
        support,
        "prepare_source",
        lambda *args: calls.append(args) or prepared,
    )

    result = support._prepare_smoke_source(tmp_path, hdfs, CONFIG, object())

    assert calls == [(tmp_path, hdfs)]
    assert result["source_version"] == "existing-ingestion-result"
    assert result["preparation"]["mode"] == "profile-and-ingest"


@pytest.mark.parametrize("collision", [False, True])
def test_preflight_checks_paths_readiness_acl_and_empty_events_before_source(
    monkeypatch: pytest.MonkeyPatch, collision: bool
) -> None:
    calls = []
    monkeypatch.setattr(
        support,
        "load_spark_config",
        lambda: SimpleNamespace(master_url="spark://spark-master:7077"),
    )
    monkeypatch.setattr(support, "spark_prerequisites", lambda h: calls.append("readiness"))
    monkeypatch.setattr(
        support, "_create_event_directory", lambda *args, **kwargs: calls.append("acl")
    )
    monkeypatch.setattr(
        support, "_probe_smoke_access", lambda *args, **kwargs: calls.append("effective-access")
    )
    client = SimpleNamespace(
        exists=lambda path: calls.append(("exists", path)) or collision,
        list_children=lambda path: calls.append(("events", path)) or (),
    )
    hdfs = SimpleNamespace(evidence=SimpleNamespace(write=lambda *args: None))

    if collision:
        with pytest.raises(VerificationError, match="already exists"):
            support._smoke_preflight(hdfs, CONFIG, client, token=TOKEN)
        assert "acl" not in calls and "effective-access" not in calls
    else:
        support._smoke_preflight(hdfs, CONFIG, client, token=TOKEN)
        assert calls.count("acl") == 3
        assert calls.count("effective-access") == 1
        assert len([call for call in calls if isinstance(call, tuple) and call[0] == "events"]) == 3
        assert not any("retry" in str(call) for call in calls)
        assert calls.index("readiness") < calls.index("acl") < calls.index("effective-access")


@pytest.mark.parametrize("writable", [False, True])
def test_driver_probe_checks_effective_spark_acl_and_output_ancestors_without_spark(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], writable: bool
) -> None:
    from warehouse_bigdata.common import config, hdfs_permissions, spark_verification

    permissions = []
    commands = []
    events = support._case_event_directories(
        CONFIG, label="smoke", token=TOKEN, include_retries=False
    )
    client = SimpleNamespace(
        getconf=lambda key: {
            "fs.defaultFS": CONFIG.uri,
            "dfs.replication": "2",
            "dfs.namenode.acls.enabled": "true",
            "dfs.client.use.datanode.hostname": "true",
            "dfs.datanode.use.datanode.hostname": "true",
        }[key],
        safe_mode=lambda: "Safe mode is OFF",
        report=lambda: {"hdfs-datanode-1": {}, "hdfs-datanode-2": {}},
        list_children=lambda path: (),
        exists=lambda path: path == CONFIG.base_path,
        is_directory=lambda path: True,
        whoami=lambda: "spark",
    )
    monkeypatch.setattr(config, "load_hdfs_config", lambda: CONFIG)
    monkeypatch.setattr(
        hdfs_permissions,
        "prepare_spark_run_permissions",
        lambda *args, **kw: permissions.append(kw),
    )
    monkeypatch.setattr(support, "assert_sources", lambda *args: None)
    monkeypatch.setattr(spark_verification, "source_fingerprint", lambda: {})
    from warehouse_bigdata.common import spark_hdfs_client

    monkeypatch.setattr(spark_hdfs_client, "SparkHdfsCliClient", lambda: client)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda args, **kw: (
            commands.append((args, kw)) or SimpleNamespace(returncode=0 if writable else 1)
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe",
            CONFIG.uri,
            CONFIG.base_path,
            json.dumps(events),
            json.dumps([CONFIG.base_path + "/curated/new/snapshot"]),
        ],
    )

    if writable:
        exec(compile(support._SMOKE_ACCESS_CODE, "m04-smoke-probe", "exec"), {})
        assert json.loads(capsys.readouterr().out)["hdfs_user"] == "spark"
    else:
        with pytest.raises(RuntimeError, match="cannot create output paths"):
            exec(compile(support._SMOKE_ACCESS_CODE, "m04-smoke-probe", "exec"), {})
    assert permissions == [
        {"pipeline_run_id": run_id, "event_log_dir": CONFIG.uri + path} for path, run_id in events
    ]
    assert commands == [
        (["hdfs", "dfs", "-test", "-w", CONFIG.base_path], {"capture_output": True, "timeout": 20})
    ]


@pytest.mark.parametrize("verify_recovery", [False, True])
def test_run_case_reuses_production_jobs_and_keeps_full_recovery_as_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, verify_recovery: bool
) -> None:
    submissions = []
    directories = []
    monkeypatch.setattr(support, "REPORT_ROOT", tmp_path)
    monkeypatch.setattr(
        support,
        "load_spark_config",
        lambda: SimpleNamespace(master_url="spark://spark-master:7077"),
    )
    monkeypatch.setattr(
        support,
        "_create_event_directory",
        lambda client, config, path, **kw: directories.append(path),
    )
    monkeypatch.setattr(support, "hdfs_client", lambda hdfs: object())
    monkeypatch.setattr(support, "_collect_event_summary", lambda *args, **kwargs: {"workers": {}})
    monkeypatch.setattr(
        support, "_validate_run_manifest", lambda *args, **kwargs: {"status": "VALIDATED"}
    )
    snapshot = _snapshot()
    monkeypatch.setattr(
        support,
        "_read_json",
        lambda *args: {
            "status": "VALIDATED",
            "source_snapshot_id": f"m04-smoke-snapshot-{TOKEN}",
            "row_count": 914_700,
        },
    )

    def immutable(*args):
        if not verify_recovery:
            pytest.fail("recovery hash pass ran in smoke")
        return {"snapshot_sha256": "sha256:unchanged"}

    monkeypatch.setattr(support, "_immutable_snapshot", immutable)

    def submit(hdfs, application, arguments, conf, output_dir):
        submissions.append((application, arguments, conf, output_dir))
        if application.endswith("verify_m04_outputs.py"):
            return b'WAREHOUSE_M04 {"status":"VALIDATED","storage":{"hashed":true}}\n'
        if output_dir.name.endswith("retry"):
            return b"status=REUSED\n"
        return b'status=VALIDATED\ntimings={"total_seconds":1.0}\n'

    monkeypatch.setattr(support, "_submit", submit)
    source = {
        "hdfs_config": CONFIG,
        "client": object(),
        "source_version": snapshot.source_version,
        "raw_path": snapshot.raw_path,
        "raw_manifests": snapshot.manifests,
    }
    options = {} if verify_recovery else {"verify_recovery": False}
    report = support.run_case(
        SimpleNamespace(),
        source,
        label="smoke",
        expected_day_count=30,
        days_per_chunk=30,
        shuffle_partitions=8,
        adaptive=True,
        cores_max=2,
        token=TOKEN,
        **options,
    )

    initial_jobs = [
        "warehouse_bigdata/jobs/normalize_sales_daily.py",
        "warehouse_bigdata/jobs/enrich_analytics.py",
    ]
    expected_jobs = (
        initial_jobs
        + (initial_jobs if verify_recovery else [])
        + ["warehouse_bigdata/jobs/verify_m04_outputs.py"]
    )
    assert [item[0] for item in submissions] == expected_jobs
    for index in (0, -1):
        args = submissions[index][1]
        assert args[args.index("--expected-day-count") + 1] == "30"
    verify_args = submissions[-1][1]
    assert verify_args[verify_args.index("--expected-series-count") + 1] == "30490"
    assert len(directories) == (5 if verify_recovery else 3)
    assert report["recovery"]["enabled"] is verify_recovery
    if verify_recovery:
        assert report["recovery"]["j02"]["retry_status"] == "REUSED"
        assert report["recovery"]["j03"]["retry_status"] == "REUSED"
        assert report["recovery"]["j02"]["immutable_unchanged"] is True
        assert report["recovery"]["j03"]["immutable_unchanged"] is True
    assert report["acceptance_scope"] == "reduced-history"
    assert report["full_history_accepted"] is False
    assert report["configuration"]["expected_row_count"] == 914_700
    assert report["storage"] == {"hashed": True}
    report_file = tmp_path / "smoke" / TOKEN / "report.json"
    assert json.loads(report_file.read_text())["full_history_accepted"] is False
    assert "not full-history acceptance" in report_file.with_suffix(".md").read_text()
    with pytest.raises(FileExistsError):
        support.run_case(
            SimpleNamespace(),
            source,
            label="smoke",
            expected_day_count=30,
            days_per_chunk=30,
            shuffle_partitions=8,
            adaptive=True,
            cores_max=2,
            token=TOKEN,
            **options,
        )
    assert len(submissions) == len(expected_jobs)


def test_live_smoke_needs_an_explicit_flag_before_hdfs_fixture_setup() -> None:
    from tests.integration import conftest

    smoke = importlib.import_module("tests.integration.test_m04_smoke")
    assert {mark.name for mark in smoke.pytestmark} == {"integration", "m04_smoke"}
    marks = []
    item = SimpleNamespace(
        get_closest_marker=lambda name: object() if name == "m04_smoke" else None,
        add_marker=marks.append,
    )
    config = SimpleNamespace(getoption=lambda name, **kw: False)
    conftest.pytest_collection_modifyitems(config, [item])
    assert len(marks) == 1 and marks[0].name == "skip"
    assert "--run-m04-smoke" in marks[0].kwargs["reason"]


def test_full_correctness_keeps_full_count_and_recovery_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = []
    monkeypatch.setattr(support, "load_hdfs_config", lambda: CONFIG)
    monkeypatch.setattr(support, "prepare_source", lambda *args: {})
    monkeypatch.setattr(support, "run_case", lambda *args, **kw: calls.append(kw))

    support.run_correctness(SimpleNamespace(), tmp_path)

    assert calls[0]["expected_day_count"] == 1_941
    assert calls[0]["days_per_chunk"] == 128
    assert "verify_recovery" not in calls[0]
