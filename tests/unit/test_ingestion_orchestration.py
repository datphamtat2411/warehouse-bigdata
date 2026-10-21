"""Focused tests for the combined M5 ingestion workflow."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest
from warehouse_bigdata.ingestion import orchestration
from warehouse_bigdata.ingestion.m5_source import SourceProfileResult
from warehouse_bigdata.jobs.verify_m5_raw_hdfs import validate_request

SOURCE_VERSION = "sha256:" + "a" * 64
TIMESTAMP = "2026-10-21T12:34:56Z"
HDFS_CONFIG = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")


def _profile_result(status: str = VALIDATED) -> SourceProfileResult:
    return SourceProfileResult(
        status=status,
        source_version=SOURCE_VERSION,
        file_profiles={},
        findings=("invalid_prices=1",) if status != VALIDATED else (),
        manifests=(),
        profile={
            "status": status,
            "source_version": SOURCE_VERSION,
            "validation_scope": {"hdfs_validated": False},
            "files": {},
            "blocking_counts": {"invalid_prices": 1} if status != VALIDATED else {},
        },
    )


def _raw_result(status: str = "PROMOTED") -> SimpleNamespace:
    integrity = SimpleNamespace(
        pipeline_run_id="raw-owner",
        source_version=SOURCE_VERSION,
        raw_path="/data/warehouse/raw/m5/source_version=sha256-" + "a" * 64,
        staging_path="/data/warehouse/staging/pipeline_run_id=raw-owner",
        replication=2,
        datanodes=(),
        files=(),
        created_at=TIMESTAMP,
    )
    return SimpleNamespace(
        status=status,
        pipeline_run_id="raw-owner",
        source_version=SOURCE_VERSION,
        raw_path=integrity.raw_path,
        staging_path=None,
        manifests=(),
        integrity=integrity,
    )


def _artifact_writer(calls: list[tuple[str, object]]):
    def write(result: SourceProfileResult, root: Path) -> dict[str, Path]:
        calls.append(("write", root))
        root.mkdir(parents=True, exist_ok=True)
        profile_path = root / "source_profile.json"
        manifests_path = root / "source_manifests.json"
        profile_path.write_text(json.dumps(result.profile), encoding="utf-8")
        manifests_path.write_text(json.dumps(list(result.manifests)), encoding="utf-8")
        return {"source_profile": profile_path, "source_manifests": manifests_path}

    return write


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    profile_status: str = VALIDATED,
    raw_status: str = "PROMOTED",
    pipeline_run_id: str = "run-one",
    calls: list[tuple[str, object]] | None = None,
    publication_error: str | None = None,
    downloaded_at_utc: str | None = TIMESTAMP,
    client: object | None = object(),
):
    events = [] if calls is None else calls
    monkeypatch.setattr(
        orchestration,
        "profile_m5_source",
        lambda source_dir, provenance: events.append(
            ("profile", source_dir, provenance.downloaded_at_utc)
        )
        or _profile_result(profile_status),
    )
    monkeypatch.setattr(
        orchestration,
        "write_source_artifacts",
        _artifact_writer(events),
    )
    monkeypatch.setattr(
        orchestration,
        "ingest_m5_raw",
        lambda *args, **kwargs: events.append(("ingest", args)) or _raw_result(raw_status),
    )
    if publication_error is None:
        monkeypatch.setattr(
            orchestration,
            "publish_run_manifest",
            lambda *args, **kwargs: events.append(("publish", args[0]))
            or "/data/warehouse/manifests/runs/" + pipeline_run_id + ".json",
        )
    else:
        monkeypatch.setattr(
            orchestration,
            "publish_run_manifest",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError(publication_error)),
        )
    return orchestration.run_m5_ingestion(
        tmp_path / "source",
        "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
        downloaded_at_utc,
        artifact_root=tmp_path / "artifacts",
        hdfs_config=HDFS_CONFIG,
        client=client,
        clock=lambda: TIMESTAMP,
        pipeline_run_id=pipeline_run_id,
    ), events


def test_orchestration_uses_the_production_client_factory_when_not_injected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[HDFSConfig] = []
    monkeypatch.setattr(
        orchestration,
        "create_hdfs_cli_client",
        lambda config: calls.append(config) or object(),
    )

    result, events = _run(tmp_path, monkeypatch, client=None)

    assert result.status == VALIDATED
    assert calls == [HDFS_CONFIG]
    assert [event[0] for event in events] == ["profile", "write", "ingest", "publish"]


def test_task01_artifacts_are_written_before_hdfs_ingestion(tmp_path, monkeypatch) -> None:
    result, events = _run(tmp_path, monkeypatch)

    assert result.status == VALIDATED
    assert [event[0] for event in events] == ["profile", "write", "ingest", "publish"]
    assert result.artifact_paths["source_profile"].is_file()
    assert result.artifact_paths["source_manifests"].is_file()


def test_combined_ingestion_succeeds_without_download_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, events = _run(tmp_path, monkeypatch, downloaded_at_utc=None)

    assert result.status == VALIDATED
    assert events[0][2] is None


def test_failed_profile_fails_fast_without_hdfs_client_or_ingestion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(
        orchestration,
        "profile_m5_source",
        lambda source_dir, provenance: events.append(("profile", source_dir))
        or _profile_result("FAILED"),
    )
    monkeypatch.setattr(orchestration, "write_source_artifacts", _artifact_writer(events))
    monkeypatch.setattr(
        orchestration,
        "ingest_m5_raw",
        lambda *args, **kwargs: pytest.fail("failed profile reached HDFS ingestion"),
    )
    monkeypatch.setattr(
        orchestration,
        "publish_run_manifest",
        lambda *args, **kwargs: pytest.fail("failed profile reached run-manifest publication"),
    )
    monkeypatch.setattr(
        orchestration,
        "HdfsCliClient",
        lambda: pytest.fail("failed profile constructed an HDFS client"),
    )

    result = orchestration.run_m5_ingestion(
        tmp_path / "source",
        "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
        TIMESTAMP,
        artifact_root=tmp_path / "artifacts",
        clock=lambda: TIMESTAMP,
        pipeline_run_id="run-failed-profile",
    )

    assert result.status == "FAILED"
    assert result.source_version == SOURCE_VERSION
    assert [event[0] for event in events] == ["profile", "write"]
    manifest = json.loads(result.artifact_paths["pipeline_run_manifest"].read_text())
    assert manifest["status"] == "FAILED"
    assert "source_snapshot_id" not in manifest


def test_successful_manifest_is_contract_valid_and_excludes_snapshot_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, _ = _run(tmp_path, monkeypatch)

    manifest = json.loads(result.artifact_paths["pipeline_run_manifest"].read_text())
    validate_manifest(
        PIPELINE_RUN_MANIFEST,
        manifest,
        lineage_context={
            "required_status": VALIDATED,
            "pipeline_run_id": result.pipeline_run_id,
            "source_version": SOURCE_VERSION,
        },
    )
    assert manifest["status"] == VALIDATED
    assert manifest["source_version"] == SOURCE_VERSION
    assert manifest["pipeline_run_id"] == result.pipeline_run_id
    assert "source_snapshot_id" not in manifest


def test_matching_rerun_uses_distinct_run_manifest_ids_and_keeps_raw_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, _ = _run(tmp_path, monkeypatch, pipeline_run_id="run-first", raw_status="PROMOTED")
    second, _ = _run(tmp_path, monkeypatch, pipeline_run_id="run-second", raw_status="REUSED")

    assert first.status == second.status == VALIDATED
    assert first.pipeline_run_id != second.pipeline_run_id
    assert first.raw_path == second.raw_path
    assert first.artifact_paths["pipeline_run_manifest"] != second.artifact_paths[
        "pipeline_run_manifest"
    ]


def test_nonvalidated_raw_outcome_is_failed_and_not_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[tuple[str, object]] = []
    result, events = _run(
        tmp_path,
        monkeypatch,
        raw_status="FAILED",
        calls=events,
    )

    assert result.status == "FAILED"
    assert result.raw_path is None
    assert [event[0] for event in events] == ["profile", "write", "ingest"]
    manifest = json.loads(result.artifact_paths["pipeline_run_manifest"].read_text())
    assert manifest["status"] == "FAILED"


def test_publication_failure_keeps_raw_path_but_marks_attempt_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, _ = _run(tmp_path, monkeypatch, publication_error="publication unavailable")

    assert result.status == "FAILED"
    assert result.raw_path is not None
    assert any("publication unavailable" in detail for detail in result.failure_details)
    manifest = json.loads(result.artifact_paths["pipeline_run_manifest"].read_text())
    assert manifest["status"] == "FAILED"
    with pytest.raises(ContractValidationError):
        validate_manifest(PIPELINE_RUN_MANIFEST, manifest, lineage_context={"required_status": VALIDATED})


def test_existing_pipeline_run_id_is_rejected_without_overwriting_evidence(tmp_path: Path) -> None:
    attempt = tmp_path / "artifacts" / "m5-ingestion" / "run-existing"
    attempt.mkdir(parents=True)
    marker = attempt / "marker.txt"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(orchestration.IngestionOrchestrationError, match="already exists"):
        orchestration.run_m5_ingestion(
            tmp_path / "source",
            "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
            TIMESTAMP,
            artifact_root=tmp_path / "artifacts",
            pipeline_run_id="run-existing",
        )

    assert marker.read_text(encoding="utf-8") == "keep"


def test_profile_exception_writes_bounded_failure_evidence_without_hdfs_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        orchestration,
        "profile_m5_source",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("source directory unavailable")),
    )
    monkeypatch.setattr(
        orchestration,
        "HdfsCliClient",
        lambda: pytest.fail("profile failure constructed an HDFS client"),
    )

    result = orchestration.run_m5_ingestion(
        tmp_path / "source",
        "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
        TIMESTAMP,
        artifact_root=tmp_path / "artifacts",
        clock=lambda: TIMESTAMP,
        pipeline_run_id="run-profile-error",
    )

    assert result.status == "FAILED"
    failure_path = result.artifact_paths["failure_manifest"]
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    assert failure["status"] == "FAILED"
    assert failure["pipeline_run_id"] == "run-profile-error"
    assert failure["source_version"] is None


def test_spark_raw_verifier_rejects_local_input_without_fallback() -> None:
    with pytest.raises(ValueError, match="absolute URI"):
        validate_request(
            "/data/warehouse/raw/m5/source_version=sha256-" + "a" * 64,
            SOURCE_VERSION,
            "hdfs://hdfs-namenode:8020",
        )
