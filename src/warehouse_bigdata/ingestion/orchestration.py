"""Thin orchestration for the profiled M5-to-HDFS ingestion workflow."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from warehouse_bigdata.common.config import HDFSConfig, load_hdfs_config, load_paths_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.contracts.manifests import FAILED, PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest
from warehouse_bigdata.ingestion.hdfs_raw import (
    HdfsIntegrityEvidence,
    ingest_m5_raw,
    publish_run_manifest,
    render_run_manifest_path,
)
from warehouse_bigdata.ingestion.m5_source import (
    SourceProfileResult,
    SourceProvenance,
    profile_m5_source,
    write_source_artifacts,
)

_PIPELINE_RUN_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SUCCESSFUL_RAW_STATUSES = frozenset({"PROMOTED", "REUSED"})
_MAX_FAILURE_DETAILS = 8
_MAX_FAILURE_LENGTH = 1_000


class IngestionOrchestrationError(RuntimeError):
    """Raised when an ingestion attempt cannot be safely reserved or identified."""


@dataclass(frozen=True, slots=True)
class IngestionRunResult:
    """Bounded outcome and local evidence paths for one ingestion attempt."""

    status: str
    pipeline_run_id: str
    source_version: str | None
    source_snapshot_id: str | None
    artifact_paths: Mapping[str, Path]
    raw_path: str | None
    failure_details: tuple[str, ...]


def run_m5_ingestion(
    source_dir: Path,
    source_url: str,
    downloaded_at_utc: str | None = None,
    *,
    artifact_root: Path | None = None,
    hdfs_config: HDFSConfig | None = None,
    client: HdfsCliClient | Any | None = None,
    clock: Callable[[], str] | None = None,
    pipeline_run_id: str | None = None,
) -> IngestionRunResult:
    """Profile, hand off, ingest, and publish one immutable M5 raw attempt."""
    run_id = _validated_or_generated_run_id(pipeline_run_id)
    root = Path(artifact_root) if artifact_root is not None else load_paths_config().artifacts_dir
    attempt_root = _reserve_attempt(root, run_id)
    timestamp = _utc_now if clock is None else clock
    created_at = timestamp()
    artifact_paths: dict[str, Path] = {}

    try:
        provenance = SourceProvenance(
            source_url=source_url,
            downloaded_at_utc=downloaded_at_utc,
        )
        profile_result = profile_m5_source(Path(source_dir), provenance)
    except Exception as error:
        detail = _failure_detail(error)
        failure_path = _write_failure_manifest(
            attempt_root,
            run_id,
            created_at,
            (detail,),
        )
        return _result(
            FAILED,
            run_id,
            None,
            artifact_paths={"failure_manifest": failure_path},
            raw_path=None,
            failure_details=(detail,),
        )

    try:
        source_artifacts = write_source_artifacts(profile_result, attempt_root)
    except Exception as error:
        detail = _failure_detail(error)
        failure_path = _write_failure_manifest(
            attempt_root,
            run_id,
            created_at,
            (detail,),
        )
        return _result(
            FAILED,
            run_id,
            profile_result.source_version,
            artifact_paths={"failure_manifest": failure_path},
            raw_path=None,
            failure_details=(detail,),
        )
    artifact_paths.update(source_artifacts)

    if profile_result.status != VALIDATED:
        details = _profile_failure_details(profile_result)
        manifest, quality_path, manifest_path = _failure_artifacts(
            attempt_root,
            run_id,
            profile_result,
            created_at,
            timestamp(),
            details,
            config=None,
            raw_path=None,
            raw_result=None,
        )
        artifact_paths.update(
            quality_report=quality_path,
            pipeline_run_manifest=manifest_path,
        )
        return _result(
            FAILED,
            run_id,
            profile_result.source_version,
            artifact_paths=artifact_paths,
            raw_path=None,
            failure_details=details,
        )

    settings: HDFSConfig | None = hdfs_config
    hdfs: HdfsCliClient | Any | None = client
    try:
        settings = load_hdfs_config() if settings is None else settings
        hdfs = HdfsCliClient() if hdfs is None else hdfs
        raw_result = ingest_m5_raw(
            Path(source_dir),
            source_artifacts["source_profile"],
            source_artifacts["source_manifests"],
            run_id,
            client=hdfs,
            config=settings,
            now=timestamp,
        )
    except Exception as error:
        details = (_failure_detail(error),)
        manifest, quality_path, manifest_path = _failure_artifacts(
            attempt_root,
            run_id,
            profile_result,
            created_at,
            timestamp(),
            details,
            config=settings,
            raw_path=None,
            raw_result=None,
        )
        artifact_paths.update(
            quality_report=quality_path,
            pipeline_run_manifest=manifest_path,
        )
        return _result(
            FAILED,
            run_id,
            profile_result.source_version,
            artifact_paths=artifact_paths,
            raw_path=None,
            failure_details=details,
        )

    raw_status = getattr(raw_result, "status", None)
    raw_path = getattr(raw_result, "raw_path", None)
    if raw_status not in _SUCCESSFUL_RAW_STATUSES or not isinstance(raw_path, str):
        details = (
            f"raw ingestion returned nonvalidated status {raw_status!r}; "
            "only PROMOTED and REUSED are eligible for validation",
        )
        manifest, quality_path, manifest_path = _failure_artifacts(
            attempt_root,
            run_id,
            profile_result,
            created_at,
            timestamp(),
            details,
            config=settings,
            raw_path=raw_path if isinstance(raw_path, str) else None,
            raw_result=raw_result,
        )
        artifact_paths.update(
            quality_report=quality_path,
            pipeline_run_manifest=manifest_path,
        )
        return _result(
            FAILED,
            run_id,
            profile_result.source_version,
            artifact_paths=artifact_paths,
            raw_path=None,
            failure_details=details,
        )

    try:
        manifest = _build_pipeline_manifest(
            run_id,
            profile_result,
            created_at,
            timestamp(),
            status=VALIDATED,
            config=settings,
            raw_path=raw_path,
            raw_result=raw_result,
            quality_report_uri=(attempt_root / "quality_report.json").resolve().as_uri(),
            failure_details=(),
        )
        quality_path = _write_quality_report(
            attempt_root,
            _quality_document(profile_result, raw_result, status=VALIDATED, failure_details=()),
        )
        manifest_path = attempt_root / "pipeline_run_manifest.json"
        publish_run_manifest(
            manifest,
            integrity=raw_result.integrity,
            client=hdfs,
            config=settings,
        )
        _atomic_write_json(manifest_path, manifest)
    except Exception as error:
        details = (_failure_detail(error),)
        manifest, quality_path, manifest_path = _failure_artifacts(
            attempt_root,
            run_id,
            profile_result,
            created_at,
            timestamp(),
            details,
            config=settings,
            raw_path=raw_path,
            raw_result=raw_result,
        )
        artifact_paths.update(
            quality_report=quality_path,
            pipeline_run_manifest=manifest_path,
        )
        return _result(
            FAILED,
            run_id,
            profile_result.source_version,
            artifact_paths=artifact_paths,
            raw_path=raw_path,
            failure_details=details,
        )

    artifact_paths.update(
        quality_report=quality_path,
        pipeline_run_manifest=manifest_path,
    )
    return _result(
        VALIDATED,
        run_id,
        profile_result.source_version,
        artifact_paths=artifact_paths,
        raw_path=raw_path,
        failure_details=(),
    )


def _validated_or_generated_run_id(value: str | None) -> str:
    if value is None:
        return f"ingest-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex}"
    if not isinstance(value, str) or not _PIPELINE_RUN_ID.fullmatch(value):
        raise IngestionOrchestrationError("pipeline_run_id must be a bounded safe identifier")
    return value


def _reserve_attempt(root: Path, run_id: str) -> Path:
    attempt_root = root / "m5-ingestion" / run_id
    try:
        attempt_root.parent.mkdir(parents=True, exist_ok=True)
        attempt_root.mkdir()
    except FileExistsError as error:
        raise IngestionOrchestrationError(
            f"pipeline_run_id evidence directory already exists: {attempt_root}"
        ) from error
    except OSError as error:
        raise IngestionOrchestrationError(
            f"cannot reserve pipeline_run_id evidence directory {attempt_root}: {error}"
        ) from error
    return attempt_root


def _failure_artifacts(
    attempt_root: Path,
    run_id: str,
    profile_result: SourceProfileResult,
    created_at: str,
    completed_at: str,
    failure_details: tuple[str, ...],
    *,
    config: HDFSConfig | None,
    raw_path: str | None,
    raw_result: Any | None,
) -> tuple[dict[str, object], Path, Path]:
    quality_path = _write_quality_report(
        attempt_root,
        _quality_document(profile_result, raw_result, status=FAILED, failure_details=failure_details),
    )
    manifest = _build_pipeline_manifest(
        run_id,
        profile_result,
        created_at,
        completed_at,
        status=FAILED,
        config=config,
        raw_path=raw_path,
        raw_result=raw_result,
        quality_report_uri=quality_path.resolve().as_uri(),
        failure_details=failure_details,
    )
    manifest_path = attempt_root / "pipeline_run_manifest.json"
    _atomic_write_json(manifest_path, manifest)
    return manifest, quality_path, manifest_path


def _build_pipeline_manifest(
    run_id: str,
    profile_result: SourceProfileResult,
    created_at: str,
    completed_at: str,
    *,
    status: str,
    config: HDFSConfig | None,
    raw_path: str | None,
    raw_result: Any | None,
    quality_report_uri: str,
    failure_details: tuple[str, ...],
) -> dict[str, object]:
    if config is not None:
        artifact_path = render_run_manifest_path(config.base_path, run_id)
        artifact_uri = config.uri.rstrip("/") + artifact_path
    else:
        artifact_uri = quality_report_uri.removesuffix("quality_report.json") + "pipeline_run_manifest.json"

    raw_uri = None
    if config is not None and isinstance(raw_path, str):
        raw_uri = config.uri.rstrip("/") + raw_path
    raw_manifests = getattr(raw_result, "manifests", None)
    row_count = len(raw_manifests) if isinstance(raw_manifests, tuple) else len(profile_result.manifests)
    profile = profile_result.profile
    output_names = ["raw_m5"] if raw_uri is not None else []
    output_uris = [raw_uri] if raw_uri is not None else []
    manifest: dict[str, object] = {
        "manifest_schema_version": PIPELINE_RUN_MANIFEST.version,
        "contract_name": PIPELINE_RUN_MANIFEST.name,
        "contract_version": PIPELINE_RUN_MANIFEST.version,
        "manifest_kind": PIPELINE_RUN_MANIFEST.manifest_kind,
        "status": status,
        "producer": "warehouse-bigdata:ingest-m5-orchestration",
        "artifact_uri": artifact_uri,
        "artifact_checksum": None,
        "schema_fingerprint": _manifest_schema_fingerprint(),
        "grain": "One M5 ingestion pipeline execution",
        "logical_unique_key": "pipeline_run_id",
        "row_count": row_count,
        "required_field_null_counts": json.dumps(
            {
                "source_profile": profile.get("blocking_counts", {}),
                "hdfs": _hdfs_quality(raw_result),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "code_revision": "warehouse-bigdata:ingest-m5-orchestration-v1",
        "configuration_digest": "warehouse-bigdata:ingest-m5-configuration-v1",
        "created_at": created_at,
        "completed_at": completed_at,
        "coverage_and_exclusion_counts": json.dumps(
            {
                "source_profile": {
                    "findings": list(profile_result.findings),
                    "validation_scope": profile.get("validation_scope", {}),
                },
                "hdfs_integrity": _hdfs_quality(raw_result),
                "failure_details": list(failure_details),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "pipeline_run_id": run_id,
        "source_version": profile_result.source_version,
        "output_dataset_names": json.dumps(output_names, separators=(",", ":")),
        "output_dataset_uris": json.dumps(output_uris, separators=(",", ":")),
        "quality_report_uri": quality_report_uri,
    }
    try:
        validate_manifest(
            PIPELINE_RUN_MANIFEST,
            manifest,
            lineage_context={
                "required_status": status,
                "pipeline_run_id": run_id,
                "source_version": profile_result.source_version,
            },
        )
    except ContractValidationError as error:
        raise IngestionOrchestrationError(f"generated invalid PIPELINE_RUN_MANIFEST: {error}") from error
    return manifest


def _quality_document(
    profile_result: SourceProfileResult,
    raw_result: Any | None,
    *,
    status: str,
    failure_details: tuple[str, ...],
) -> dict[str, object]:
    return {
        "quality_schema_version": "m5-ingestion-quality-v1",
        "status": status,
        "source_version": profile_result.source_version,
        "source_profile_status": profile_result.status,
        "findings": list(profile_result.findings),
        "blocking_counts": profile_result.profile.get("blocking_counts", {}),
        "hdfs_integrity": _hdfs_quality(raw_result),
        "failure_details": list(failure_details),
    }


def _hdfs_quality(raw_result: Any | None) -> dict[str, object]:
    integrity = getattr(raw_result, "integrity", None)
    if not isinstance(integrity, HdfsIntegrityEvidence):
        return {}
    return {
        "status": getattr(raw_result, "status", None),
        "raw_path": integrity.raw_path,
        "staging_path": integrity.staging_path,
        "replication": integrity.replication,
        "datanodes": [asdict(node) for node in integrity.datanodes],
        "files": [asdict(file_evidence) for file_evidence in integrity.files],
        "created_at": integrity.created_at,
    }


def _profile_failure_details(result: SourceProfileResult) -> tuple[str, ...]:
    if result.findings:
        return tuple(result.findings[:_MAX_FAILURE_DETAILS])
    return ("source profile did not reach VALIDATED status",)


def _write_quality_report(attempt_root: Path, value: Mapping[str, object]) -> Path:
    path = attempt_root / "quality_report.json"
    _atomic_write_json(path, value)
    return path


def _write_failure_manifest(
    attempt_root: Path,
    run_id: str,
    created_at: str,
    failure_details: tuple[str, ...],
) -> Path:
    path = attempt_root / "failure_manifest.json"
    _atomic_write_json(
        path,
        {
            "manifest_type": "ingestion_attempt_failure",
            "status": FAILED,
            "pipeline_run_id": run_id,
            "source_version": None,
            "created_at": created_at,
            "failure_details": list(failure_details[:_MAX_FAILURE_DETAILS]),
        },
    )
    return path


def _result(
    status: str,
    pipeline_run_id: str,
    source_version: str | None,
    *,
    artifact_paths: Mapping[str, Path],
    raw_path: str | None,
    failure_details: tuple[str, ...],
) -> IngestionRunResult:
    return IngestionRunResult(
        status=status,
        pipeline_run_id=pipeline_run_id,
        source_version=source_version,
        source_snapshot_id=None,
        artifact_paths=dict(artifact_paths),
        raw_path=raw_path,
        failure_details=failure_details[:_MAX_FAILURE_DETAILS],
    )


def _failure_detail(error: BaseException) -> str:
    message = str(error).strip() or error.__class__.__name__
    return f"{error.__class__.__name__}: {message[:_MAX_FAILURE_LENGTH]}"


def _manifest_schema_fingerprint() -> str:
    payload = json.dumps(PIPELINE_RUN_MANIFEST.field_names, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _atomic_write_json(path: Path, value: object) -> None:
    temporary_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=True, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
