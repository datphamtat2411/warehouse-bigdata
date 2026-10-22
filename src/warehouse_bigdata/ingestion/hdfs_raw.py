"""Immutable HDFS raw ingestion and independent integrity verification."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Final

from warehouse_bigdata.common.config import HDFSConfig, load_hdfs_config
from warehouse_bigdata.common.hdfs_client import (
    EXPECTED_DATANODES,
    EXPECTED_REPLICATION,
    BlockPlacement,
    DataNodeReport,
    HdfsCliClient,
    HdfsCommandError,
    RemoteFileHash,
    RemoteFileStat,
)
from warehouse_bigdata.common.hdfs_paths import HdfsPathError, render_hdfs_artifact_path
from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client
from warehouse_bigdata.contracts.manifests import (
    PIPELINE_RUN_MANIFEST,
    SOURCE_MANIFEST,
    VALIDATED,
)
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest
from warehouse_bigdata.ingestion.m5_source import (
    RECOGNIZED_M5_FILES,
    REQUIRED_M5_FILES,
    SourceProvenance,
    compute_source_version,
)

CSV_CHUNK_SIZE: Final = 1024 * 1024
_SOURCE_VERSION = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_PIPELINE_RUN_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_MUTABLE_MANIFEST_FIELDS = frozenset(
    {
        "artifact_uri",
        "artifact_checksum",
        "size_bytes",
        "status",
        "producer",
        "code_revision",
        "created_at",
        "completed_at",
    }
)


class HdfsRawIngestionError(RuntimeError):
    """Raised when raw ingestion cannot prove a complete immutable snapshot."""


@dataclass(frozen=True, slots=True)
class LocalSourceFile:
    """One locally revalidated source file and its Task 01 manifest."""

    source_file: str
    path: Path
    size_bytes: int
    sha256: str
    manifest: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class LocalSourceBundle:
    """Task 01 artifacts after strict local preflight."""

    source_dir: Path
    source_version: str
    source_name: str
    source_url: str
    downloaded_at_utc: str | None
    source_schema_version: str
    files: tuple[LocalSourceFile, ...]


@dataclass(frozen=True, slots=True)
class RemoteFileEvidence:
    """Independent HDFS evidence for one uploaded raw file."""

    source_file: str
    remote_path: str
    artifact_uri: str
    size_bytes: int
    sha256: str
    hdfs_checksum: str
    replication: int
    blocks: tuple[BlockPlacement, ...]


@dataclass(frozen=True, slots=True)
class HdfsIntegrityEvidence:
    """Non-contract sidecar evidence for one validated raw snapshot."""

    pipeline_run_id: str
    source_version: str
    raw_path: str
    staging_path: str
    replication: int
    datanodes: tuple[DataNodeReport, ...]
    files: tuple[RemoteFileEvidence, ...]
    created_at: str


@dataclass(frozen=True, slots=True)
class RawIngestionResult:
    """Immutable outcome for a promoted or matching immutable raw snapshot."""

    status: str
    pipeline_run_id: str
    source_version: str
    raw_path: str
    staging_path: str | None
    manifests: tuple[Mapping[str, object], ...]
    integrity: HdfsIntegrityEvidence


@dataclass(frozen=True, slots=True)
class ValidatedHdfsRawSnapshot:
    """A raw HDFS snapshot independently revalidated before Spark reads it."""

    status: str
    source_version: str
    raw_path: str
    manifests: tuple[Mapping[str, object], ...]
    integrity: HdfsIntegrityEvidence


def render_run_manifest_path(base_path: str, pipeline_run_id: str) -> str:
    """Render one Contract v1 run-manifest path without creating it."""
    if not isinstance(pipeline_run_id, str) or not _PIPELINE_RUN_ID.fullmatch(pipeline_run_id):
        raise HdfsRawIngestionError("pipeline_run_id must be a bounded safe identifier")
    return _render_path(base_path, "run_manifest", pipeline_run_id=pipeline_run_id)


def publish_run_manifest(
    manifest: Mapping[str, object],
    *,
    integrity: HdfsIntegrityEvidence,
    client: HdfsCliClient | Any | None = None,
    config: HDFSConfig | None = None,
) -> str:
    """Exclusively create and independently verify one validated run manifest."""
    source_version = _string_or_error(manifest.get("source_version"), "run manifest source_version")
    pipeline_run_id = _string_or_error(
        manifest.get("pipeline_run_id"), "run manifest pipeline_run_id"
    )
    try:
        validate_manifest(
            PIPELINE_RUN_MANIFEST,
            manifest,
            lineage_context={
                "required_status": VALIDATED,
                "source_version": source_version,
                "pipeline_run_id": pipeline_run_id,
            },
        )
    except ContractValidationError as error:
        raise HdfsRawIngestionError(f"invalid PIPELINE_RUN_MANIFEST: {error}") from error
    if integrity.source_version != source_version:
        raise HdfsRawIngestionError(
            "run manifest source_version does not match HDFS integrity evidence"
        )

    settings = load_hdfs_config() if config is None else config
    path = render_run_manifest_path(settings.base_path, pipeline_run_id)
    expected_uri = _hdfs_uri(settings.uri, path)
    if manifest.get("artifact_uri") != expected_uri:
        raise HdfsRawIngestionError("run manifest artifact_uri does not match its Contract v1 path")
    payload = _json_bytes(manifest)
    addresses = {node.address for node in integrity.datanodes}
    try:
        hdfs = create_hdfs_cli_client(settings) if client is None else client
        if _exists(hdfs, path):
            raise HdfsRawIngestionError(
                f"run manifest path already exists; refusing to overwrite: {path}"
            )
        hdfs.mkdir_parents(str(PurePosixPath(path).parent))
        hdfs.upload_bytes(payload, path)
        _verify_metadata_file(hdfs, path, payload, addresses)
    except HdfsRawIngestionError:
        raise
    except (HdfsCommandError, OSError, ValueError, KeyError, TypeError) as error:
        raise HdfsRawIngestionError(f"run manifest publication failed: {error}") from error
    return path


def preflight_source_artifacts(
    source_dir: Path,
    profile_path: Path,
    manifest_path: Path,
) -> LocalSourceBundle:
    """Validate Task 01 evidence and recompute every local file identity."""
    source = Path(source_dir)
    if not source.is_dir():
        raise HdfsRawIngestionError(f"source directory does not exist: {source}")

    profile = _load_json_object(Path(profile_path), "source profile")
    if profile.get("status") != VALIDATED:
        raise HdfsRawIngestionError("source profile must have status VALIDATED")
    scope = profile.get("validation_scope")
    if not isinstance(scope, Mapping) or scope.get("hdfs_validated") is not False:
        raise HdfsRawIngestionError(
            "source profile must remain local-only with hdfs_validated=false"
        )

    source_version = _required_string(profile, "source_version")
    if not _SOURCE_VERSION.fullmatch(source_version):
        raise HdfsRawIngestionError("source profile source_version is not a valid SHA-256 identity")
    source_name = _required_string(profile, "source_name")
    source_url = _required_string(profile, "source_url")
    source_schema_version = _required_string(profile, "source_schema_version")
    if "downloaded_at_utc" not in profile:
        raise HdfsRawIngestionError(
            "source artifact field downloaded_at_utc must be present and may be null"
        )
    downloaded_at_utc = profile["downloaded_at_utc"]
    if downloaded_at_utc is not None and not isinstance(downloaded_at_utc, str):
        raise HdfsRawIngestionError(
            "source artifact field downloaded_at_utc must be an ISO-8601 UTC timestamp or null"
        )
    try:
        provenance = SourceProvenance(
            downloaded_at_utc=downloaded_at_utc,
            source_url=source_url,
            source_name=source_name,
            source_schema_version=source_schema_version,
        )
    except (TypeError, ValueError) as error:
        raise HdfsRawIngestionError(f"invalid source profile provenance: {error}") from error
    shared = {
        "source_name": provenance.source_name,
        "source_url": provenance.source_url,
        "downloaded_at_utc": provenance.downloaded_at_utc,
        "source_schema_version": provenance.source_schema_version,
    }

    profile_files = profile.get("files")
    if not isinstance(profile_files, Mapping):
        raise HdfsRawIngestionError("source profile files must be a mapping")
    if set(profile_files) != set(RECOGNIZED_M5_FILES):
        raise HdfsRawIngestionError(
            "source profile file set does not match the recognized M5 package"
        )

    try:
        entries = tuple(source.iterdir())
    except OSError as error:
        raise HdfsRawIngestionError(f"cannot list source directory {source}: {error}") from error
    local_paths = {
        entry.name: entry for entry in entries if entry.is_file() and entry.suffix.lower() == ".csv"
    }
    unexpected = set(local_paths) - set(RECOGNIZED_M5_FILES)
    if unexpected:
        raise HdfsRawIngestionError(f"unexpected local M5 CSV files: {sorted(unexpected)}")

    present_names: set[str] = set()
    for filename, raw_profile in profile_files.items():
        if not isinstance(filename, str) or not isinstance(raw_profile, Mapping):
            raise HdfsRawIngestionError("source profile contains a malformed file profile")
        if raw_profile.get("source_file") != filename:
            raise HdfsRawIngestionError(f"source profile filename identity mismatch for {filename}")
        present = raw_profile.get("present")
        if present is True:
            present_names.add(filename)
        elif present is not False:
            raise HdfsRawIngestionError(f"source profile present flag is invalid for {filename}")
    if present_names != set(local_paths):
        raise HdfsRawIngestionError("local CSV files do not match the source profile file set")
    if not REQUIRED_M5_FILES <= present_names:
        raise HdfsRawIngestionError("required M5 CSV files are missing from the local source")

    raw_manifests = _load_json_array(Path(manifest_path), "source manifests")
    manifests: dict[str, Mapping[str, object]] = {}
    for raw_manifest in raw_manifests:
        if not isinstance(raw_manifest, Mapping):
            raise HdfsRawIngestionError("source manifests must contain JSON objects")
        filename = raw_manifest.get("source_file")
        if not isinstance(filename, str) or filename in manifests:
            raise HdfsRawIngestionError(
                "source manifests contain a missing or duplicate source_file"
            )
        if filename not in present_names:
            raise HdfsRawIngestionError(f"source manifest contains unexpected file {filename!r}")
        try:
            validate_manifest(
                SOURCE_MANIFEST,
                raw_manifest,
                lineage_context={"required_status": VALIDATED, "source_version": source_version},
            )
        except ContractValidationError as error:
            raise HdfsRawIngestionError(
                f"invalid SOURCE_MANIFEST for {filename}: {error}"
            ) from error
        for field_name, expected in shared.items():
            if raw_manifest.get(field_name) != expected:
                raise HdfsRawIngestionError(
                    f"source metadata mismatch for {filename}: {field_name}"
                )
        if raw_manifest.get("source_version") != source_version:
            raise HdfsRawIngestionError(f"source_version mismatch for {filename}")
        manifests[filename] = MappingProxyType(dict(raw_manifest))
    if set(manifests) != present_names:
        raise HdfsRawIngestionError("source manifest file set does not match local CSV files")

    files: list[LocalSourceFile] = []
    file_hashes: dict[str, str] = {}
    for filename in sorted(present_names):
        path = local_paths[filename]
        raw_profile = profile_files[filename]
        if not isinstance(raw_profile, Mapping):
            raise HdfsRawIngestionError(f"malformed local profile for {filename}")
        if raw_profile.get("validation_status") != VALIDATED:
            raise HdfsRawIngestionError(f"local profile for {filename} is not VALIDATED")
        expected_size = raw_profile.get("size_bytes")
        expected_hash = raw_profile.get("sha256")
        if (
            not isinstance(expected_size, int)
            or expected_size < 0
            or not isinstance(expected_hash, str)
        ):
            raise HdfsRawIngestionError(
                f"local profile integrity fields are invalid for {filename}"
            )
        _validate_sha256(expected_hash, f"local profile {filename}")
        before_size, digest = _hash_local_file(path)
        after_size = _safe_stat_size(path, filename)
        if before_size != after_size or before_size != expected_size:
            raise HdfsRawIngestionError(f"local file size mismatch for {filename}")
        if digest != expected_hash:
            raise HdfsRawIngestionError(f"local SHA-256 mismatch for {filename}")
        manifest = manifests[filename]
        if manifest.get("artifact_uri") != path.resolve().as_uri():
            raise HdfsRawIngestionError(f"local artifact URI mismatch for {filename}")
        if manifest.get("artifact_checksum") != digest:
            raise HdfsRawIngestionError(f"manifest checksum mismatch for {filename}")
        if manifest.get("size_bytes") != expected_size:
            raise HdfsRawIngestionError(f"manifest size mismatch for {filename}")
        if manifest.get("source_file_row_count") != raw_profile.get("row_count"):
            raise HdfsRawIngestionError(f"manifest row count mismatch for {filename}")
        file_hashes[filename] = digest
        files.append(
            LocalSourceFile(
                source_file=filename,
                path=path,
                size_bytes=before_size,
                sha256=digest,
                manifest=manifest,
            )
        )

    recomputed_version = compute_source_version(file_hashes)
    if recomputed_version != source_version:
        raise HdfsRawIngestionError("recomputed source_version does not match Task 01 artifacts")
    return LocalSourceBundle(
        source_dir=source,
        source_version=source_version,
        source_name=shared["source_name"],
        source_url=shared["source_url"],
        downloaded_at_utc=shared["downloaded_at_utc"],
        source_schema_version=shared["source_schema_version"],
        files=tuple(files),
    )


def ingest_m5_raw(
    source_dir: Path,
    profile_path: Path,
    manifest_path: Path,
    pipeline_run_id: str,
    *,
    client: HdfsCliClient | Any | None = None,
    config: HDFSConfig | None = None,
    now: Callable[[], str] | None = None,
) -> RawIngestionResult:
    """Stage, independently verify, and atomically promote immutable M5 raw data."""
    if not isinstance(pipeline_run_id, str) or not _PIPELINE_RUN_ID.fullmatch(pipeline_run_id):
        raise HdfsRawIngestionError("pipeline_run_id must be a bounded safe identifier")
    bundle = preflight_source_artifacts(source_dir, profile_path, manifest_path)
    settings = load_hdfs_config() if config is None else config
    timestamp = _utc_now if now is None else now
    raw_path = _render_path(settings.base_path, "raw_m5", source_version=bundle.source_version)
    staging_path = _render_path(settings.base_path, "staging", pipeline_run_id=pipeline_run_id)

    try:
        hdfs = create_hdfs_cli_client(settings) if client is None else client
        nodes = _verify_cluster_prerequisites(hdfs, settings)
        if _exists(hdfs, raw_path):
            if not _is_directory(hdfs, raw_path):
                raise HdfsRawIngestionError(f"existing raw path is not a directory: {raw_path}")
            manifests, integrity = _validate_snapshot(
                hdfs,
                settings,
                bundle,
                raw_path,
                nodes,
            )
            return RawIngestionResult(
                status="REUSED",
                pipeline_run_id=pipeline_run_id,
                source_version=bundle.source_version,
                raw_path=raw_path,
                staging_path=None,
                manifests=manifests,
                integrity=integrity,
            )

        parent = str(PurePosixPath(staging_path).parent)
        hdfs.mkdir_parents(parent)
        hdfs.mkdir_parents(str(PurePosixPath(raw_path).parent))
        stage_owned = False
        failure: BaseException | None = None
        try:
            hdfs.mkdir_exclusive(staging_path)
            stage_owned = True
        except Exception as error:
            raise HdfsRawIngestionError(
                f"staging path {staging_path} was not exclusively created; ownership is uncertain"
            ) from error

        try:
            addresses = {node.address for node in nodes.values()}
            remote_files = []
            for local_file in bundle.files:
                remote_path = f"{staging_path}/{local_file.source_file}"
                hdfs.upload_file(local_file.path, remote_path)
                remote_files.append(
                    _verify_remote_file(
                        hdfs,
                        settings,
                        local_file,
                        remote_path,
                        raw_path,
                        addresses,
                    )
                )

            remote_manifests = tuple(
                _build_remote_manifest(local_file, evidence, settings, raw_path, timestamp())
                for local_file, evidence in zip(bundle.files, remote_files, strict=True)
            )
            integrity = HdfsIntegrityEvidence(
                pipeline_run_id=pipeline_run_id,
                source_version=bundle.source_version,
                raw_path=raw_path,
                staging_path=staging_path,
                replication=EXPECTED_REPLICATION,
                datanodes=tuple(nodes[name] for name in sorted(nodes)),
                files=tuple(remote_files),
                created_at=timestamp(),
            )
            metadata = {
                "source_manifests.json": _json_bytes(
                    [dict(manifest) for manifest in remote_manifests]
                ),
                "hdfs_integrity.json": _json_bytes(_integrity_document(integrity)),
            }
            for filename, payload in metadata.items():
                remote_path = f"{staging_path}/{filename}"
                hdfs.upload_bytes(payload, remote_path)
                _verify_metadata_file(hdfs, remote_path, payload, addresses)
            expected_children = {f"{staging_path}/{local.source_file}" for local in bundle.files}
            expected_children.update(f"{staging_path}/{name}" for name in metadata)
            if set(hdfs.list_children(staging_path)) != expected_children:
                raise HdfsRawIngestionError(
                    "staging candidate is incomplete or contains unexpected files"
                )
            hdfs.rename(staging_path, raw_path)
            stage_owned = False
            manifests, final_integrity = _validate_snapshot(
                hdfs,
                settings,
                bundle,
                raw_path,
                nodes,
            )
            return RawIngestionResult(
                status="PROMOTED",
                pipeline_run_id=pipeline_run_id,
                source_version=bundle.source_version,
                raw_path=raw_path,
                staging_path=staging_path,
                manifests=manifests,
                integrity=final_integrity,
            )
        except BaseException as error:
            failure = error
            raise
        finally:
            if stage_owned:
                try:
                    hdfs.remove(staging_path)
                except Exception as cleanup_error:
                    message = (
                        f"staging cleanup failed; retained path {staging_path}: {cleanup_error}"
                    )
                    if failure is not None:
                        failure.add_note(message)
                    else:
                        raise HdfsRawIngestionError(message) from cleanup_error
    except HdfsRawIngestionError:
        raise
    except (HdfsCommandError, OSError, ValueError, KeyError, TypeError) as error:
        raise HdfsRawIngestionError(str(error)) from error


def validate_hdfs_raw_snapshot(
    source_version: str,
    *,
    client: HdfsCliClient | Any | None = None,
    config: HDFSConfig | None = None,
) -> ValidatedHdfsRawSnapshot:
    """Revalidate one immutable raw snapshot before any Spark source read.

    The source manifests, raw file bytes, metadata sidecars, and HDFS block
    placement are checked independently of Spark.  Callers receive only a
    validated HDFS path and never a local-data fallback.
    """
    if not isinstance(source_version, str) or not _SOURCE_VERSION.fullmatch(source_version):
        raise HdfsRawIngestionError("source_version must be a content-derived SHA-256 identity")
    settings = load_hdfs_config() if config is None else config
    raw_path = _render_path(settings.base_path, "raw_m5", source_version=source_version)

    try:
        hdfs = create_hdfs_cli_client(settings) if client is None else client
        nodes = _verify_cluster_prerequisites(hdfs, settings)
        if not _exists(hdfs, raw_path) or not _is_directory(hdfs, raw_path):
            raise HdfsRawIngestionError(f"validated raw snapshot is not a directory: {raw_path}")

        manifest_path = f"{raw_path}/source_manifests.json"
        manifest_payload = hdfs.read_remote(manifest_path)
        _verify_metadata_file(
            hdfs,
            manifest_path,
            manifest_payload,
            {node.address for node in nodes.values()},
        )
        raw_manifests = _decode_array(manifest_payload, "raw source manifests")
        bundle = _bundle_from_remote_manifests(raw_manifests, source_version)
        manifests, integrity = _validate_snapshot(
            hdfs,
            settings,
            bundle,
            raw_path,
            nodes,
        )
        return ValidatedHdfsRawSnapshot(
            status=VALIDATED,
            source_version=source_version,
            raw_path=raw_path,
            manifests=manifests,
            integrity=integrity,
        )
    except HdfsRawIngestionError:
        raise
    except (HdfsCommandError, OSError, ValueError, KeyError, TypeError) as error:
        raise HdfsRawIngestionError(f"raw HDFS snapshot validation failed: {error}") from error


def _verify_cluster_prerequisites(client: Any, config: HDFSConfig) -> dict[str, DataNodeReport]:
    expected_configuration = {
        "fs.defaultFS": config.uri,
        "dfs.replication": str(EXPECTED_REPLICATION),
        "dfs.client.use.datanode.hostname": "true",
        "dfs.datanode.use.datanode.hostname": "true",
    }
    for key, expected in expected_configuration.items():
        actual = client.getconf(key).strip()
        if actual != expected:
            raise HdfsRawIngestionError(
                f"Hadoop configuration mismatch for {key}: expected {expected!r}, got {actual!r}"
            )
    if client.safe_mode().strip() != "Safe mode is OFF":
        raise HdfsRawIngestionError("NameNode safe mode is not OFF")
    nodes = client.report()
    if set(nodes) != EXPECTED_DATANODES or len({node.address for node in nodes.values()}) != 2:
        raise HdfsRawIngestionError("exactly two distinct expected DataNodes must be live")
    return nodes


def _verify_remote_file(
    client: Any,
    config: HDFSConfig,
    local_file: LocalSourceFile,
    remote_path: str,
    raw_path: str,
    addresses: set[str],
) -> RemoteFileEvidence:
    stat = client.stat(remote_path)
    if not isinstance(stat, RemoteFileStat):
        raise HdfsRawIngestionError(f"invalid HDFS stat result for {local_file.source_file}")
    if stat.size_bytes != local_file.size_bytes:
        raise HdfsRawIngestionError(f"remote size mismatch for {local_file.source_file}")
    if stat.replication != EXPECTED_REPLICATION:
        raise HdfsRawIngestionError(f"remote replication mismatch for {local_file.source_file}")
    remote_hash = client.hash_remote(remote_path)
    if not isinstance(remote_hash, RemoteFileHash):
        raise HdfsRawIngestionError(f"invalid remote hash result for {local_file.source_file}")
    if remote_hash.size_bytes != local_file.size_bytes or remote_hash.sha256 != local_file.sha256:
        raise HdfsRawIngestionError(f"remote SHA-256 mismatch for {local_file.source_file}")
    checksum = client.checksum(remote_path)
    if not isinstance(checksum, str) or not checksum.strip():
        raise HdfsRawIngestionError(f"missing HDFS checksum for {local_file.source_file}")
    blocks = client.fsck(remote_path, addresses, expected_replication=EXPECTED_REPLICATION)
    _validate_block_evidence(blocks, stat.size_bytes, addresses, local_file.source_file)
    return RemoteFileEvidence(
        source_file=local_file.source_file,
        remote_path=remote_path,
        artifact_uri=_hdfs_uri(config.uri, f"{raw_path}/{local_file.source_file}"),
        size_bytes=stat.size_bytes,
        sha256=remote_hash.sha256,
        hdfs_checksum=checksum,
        replication=stat.replication,
        blocks=tuple(blocks),
    )


def _validate_block_evidence(
    blocks: tuple[BlockPlacement, ...],
    size_bytes: int,
    addresses: set[str],
    source_file: str,
) -> None:
    if size_bytes == 0:
        if blocks:
            raise HdfsRawIngestionError(f"empty remote file has unexpected blocks: {source_file}")
        return
    if not blocks or sum(block.length for block in blocks) != size_bytes:
        raise HdfsRawIngestionError(f"HDFS block lengths do not match {source_file}")
    for block in blocks:
        if block.length <= 0 or len(block.locations) != 2 or set(block.locations) != addresses:
            raise HdfsRawIngestionError(f"HDFS block placement is incomplete for {source_file}")


def _verify_metadata_file(
    client: Any,
    remote_path: str,
    expected_payload: bytes,
    addresses: set[str],
) -> None:
    stat = client.stat(remote_path)
    if not isinstance(stat, RemoteFileStat) or stat.size_bytes != len(expected_payload):
        raise HdfsRawIngestionError(f"metadata size mismatch for {remote_path}")
    if stat.replication != EXPECTED_REPLICATION:
        raise HdfsRawIngestionError(f"metadata replication mismatch for {remote_path}")
    remote_hash = client.hash_remote(remote_path)
    if not isinstance(remote_hash, RemoteFileHash) or remote_hash.sha256 != _sha256(
        expected_payload
    ):
        raise HdfsRawIngestionError(f"metadata SHA-256 mismatch for {remote_path}")
    if client.read_remote(remote_path) != expected_payload:
        raise HdfsRawIngestionError(f"metadata content mismatch for {remote_path}")
    blocks = client.fsck(remote_path, addresses, expected_replication=EXPECTED_REPLICATION)
    _validate_block_evidence(blocks, len(expected_payload), addresses, remote_path)


def _build_remote_manifest(
    local_file: LocalSourceFile,
    evidence: RemoteFileEvidence,
    config: HDFSConfig,
    raw_path: str,
    created_at: str,
) -> Mapping[str, object]:
    record = dict(local_file.manifest)
    record.update(
        {
            "status": VALIDATED,
            "producer": "warehouse-bigdata:hdfs-raw-ingestion",
            "artifact_uri": _hdfs_uri(config.uri, f"{raw_path}/{local_file.source_file}"),
            "artifact_checksum": evidence.sha256,
            "size_bytes": evidence.size_bytes,
            "code_revision": "warehouse-bigdata:hdfs-raw-ingestion-v1",
            "created_at": created_at,
            "completed_at": created_at,
        }
    )
    try:
        validate_manifest(
            SOURCE_MANIFEST,
            record,
            lineage_context={
                "required_status": VALIDATED,
                "source_version": record["source_version"],
            },
        )
    except ContractValidationError as error:
        raise HdfsRawIngestionError(
            f"generated invalid HDFS SOURCE_MANIFEST for {local_file.source_file}: {error}"
        ) from error
    return MappingProxyType(record)


def _bundle_from_remote_manifests(
    raw_manifests: list[object],
    source_version: str,
) -> LocalSourceBundle:
    expected_files = set(RECOGNIZED_M5_FILES)
    by_file: dict[str, Mapping[str, object]] = {}
    for raw_manifest in raw_manifests:
        if not isinstance(raw_manifest, Mapping):
            raise HdfsRawIngestionError("raw source manifests must contain JSON objects")
        filename = raw_manifest.get("source_file")
        if not isinstance(filename, str) or filename not in expected_files or filename in by_file:
            raise HdfsRawIngestionError("raw source manifests contain an invalid file set")
        try:
            validate_manifest(
                SOURCE_MANIFEST,
                raw_manifest,
                lineage_context={"required_status": VALIDATED, "source_version": source_version},
            )
        except ContractValidationError as error:
            raise HdfsRawIngestionError(
                f"invalid validated raw SOURCE_MANIFEST for {filename}: {error}"
            ) from error
        size_bytes = raw_manifest.get("size_bytes")
        checksum = raw_manifest.get("artifact_checksum")
        if not isinstance(size_bytes, int) or size_bytes < 0:
            raise HdfsRawIngestionError(f"invalid raw source size for {filename}")
        if not isinstance(checksum, str):
            raise HdfsRawIngestionError(f"invalid raw source checksum for {filename}")
        _validate_sha256(checksum, f"raw source {filename}")
        by_file[filename] = MappingProxyType(dict(raw_manifest))

    if not REQUIRED_M5_FILES <= set(by_file) or not by_file:
        raise HdfsRawIngestionError("validated raw source manifests do not cover required M5 files")
    first = next(iter(by_file.values()))
    shared_fields = ("source_name", "source_url", "downloaded_at_utc", "source_schema_version")
    for filename, manifest in by_file.items():
        if manifest.get("source_version") != source_version:
            raise HdfsRawIngestionError(f"raw source_version mismatch for {filename}")
        for field in shared_fields:
            if manifest.get(field) != first.get(field):
                raise HdfsRawIngestionError(f"raw source provenance mismatch for {filename}: {field}")

    files = tuple(
        LocalSourceFile(
            source_file=filename,
            path=Path("/"),
            size_bytes=int(manifest["size_bytes"]),
            sha256=str(manifest["artifact_checksum"]),
            manifest=manifest,
        )
        for filename, manifest in sorted(by_file.items())
    )
    return LocalSourceBundle(
        source_dir=Path("/"),
        source_version=source_version,
        source_name=_required_string(first, "source_name"),
        source_url=_required_string(first, "source_url"),
        downloaded_at_utc=first.get("downloaded_at_utc"),
        source_schema_version=_required_string(first, "source_schema_version"),
        files=files,
    )


def _validate_snapshot(
    client: Any,
    config: HDFSConfig,
    bundle: LocalSourceBundle,
    raw_path: str,
    nodes: Mapping[str, DataNodeReport],
) -> tuple[tuple[Mapping[str, object], ...], HdfsIntegrityEvidence]:
    expected_names = {local.source_file for local in bundle.files}
    expected_names.update({"source_manifests.json", "hdfs_integrity.json"})
    children = set(client.list_children(raw_path))
    expected_children = {f"{raw_path}/{name}" for name in expected_names}
    if children != expected_children:
        raise HdfsRawIngestionError(
            "existing raw snapshot is incomplete or contains conflicting contents"
        )
    addresses = {node.address for node in nodes.values()}
    manifest_path = f"{raw_path}/source_manifests.json"
    sidecar_path = f"{raw_path}/hdfs_integrity.json"
    manifest_payload = client.read_remote(manifest_path)
    sidecar_payload = client.read_remote(sidecar_path)
    _verify_metadata_file(client, manifest_path, manifest_payload, addresses)
    _verify_metadata_file(client, sidecar_path, sidecar_payload, addresses)
    raw_manifests = _decode_array(manifest_payload, "existing source manifests")
    by_file: dict[str, Mapping[str, object]] = {}
    for raw_manifest in raw_manifests:
        if not isinstance(raw_manifest, Mapping):
            raise HdfsRawIngestionError("existing source manifests contain a malformed record")
        filename = raw_manifest.get("source_file")
        if not isinstance(filename, str) or filename in by_file:
            raise HdfsRawIngestionError(
                "existing source manifests contain duplicate source_file values"
            )
        by_file[filename] = raw_manifest
    if set(by_file) != expected_names - {"source_manifests.json", "hdfs_integrity.json"}:
        raise HdfsRawIngestionError(
            "existing source manifest file set conflicts with local artifacts"
        )

    remote_files = []
    for local_file in bundle.files:
        stored = by_file[local_file.source_file]
        remote_path = f"{raw_path}/{local_file.source_file}"
        evidence = _verify_remote_file(
            client,
            config,
            local_file,
            remote_path,
            raw_path,
            addresses,
        )
        _assert_existing_manifest(stored, local_file, evidence)
        remote_files.append(evidence)

    sidecar = _decode_object(sidecar_payload, "existing HDFS integrity sidecar")
    _assert_existing_sidecar(sidecar, bundle, raw_path, remote_files)
    integrity = HdfsIntegrityEvidence(
        pipeline_run_id=_string_or_error(sidecar.get("pipeline_run_id"), "sidecar pipeline_run_id"),
        source_version=bundle.source_version,
        raw_path=raw_path,
        staging_path=_string_or_error(sidecar.get("staging_path"), "sidecar staging_path"),
        replication=EXPECTED_REPLICATION,
        datanodes=tuple(nodes[name] for name in sorted(nodes)),
        files=tuple(remote_files),
        created_at=_string_or_error(sidecar.get("created_at"), "sidecar created_at"),
    )
    return tuple(
        MappingProxyType(dict(by_file[local.source_file])) for local in bundle.files
    ), integrity


def _assert_existing_manifest(
    stored: Mapping[str, object],
    local_file: LocalSourceFile,
    evidence: RemoteFileEvidence,
) -> None:
    try:
        validate_manifest(
            SOURCE_MANIFEST,
            stored,
            lineage_context={
                "required_status": VALIDATED,
                "source_version": local_file.manifest["source_version"],
            },
        )
    except ContractValidationError as error:
        raise HdfsRawIngestionError(f"existing source manifest is invalid: {error}") from error
    if stored.get("source_file") != local_file.source_file:
        raise HdfsRawIngestionError(
            "existing source manifest source_file conflicts with local artifacts"
        )
    if stored.get("artifact_uri") != evidence.artifact_uri:
        raise HdfsRawIngestionError(
            f"existing source manifest URI conflicts for {local_file.source_file}"
        )
    if (
        stored.get("artifact_checksum") != evidence.sha256
        or stored.get("size_bytes") != evidence.size_bytes
    ):
        raise HdfsRawIngestionError(
            f"existing source manifest integrity conflicts for {local_file.source_file}"
        )
    for field_name, expected in local_file.manifest.items():
        if field_name in {
            "source_name",
            "source_url",
            "downloaded_at_utc",
            "source_schema_version",
        }:
            if stored.get(field_name) != expected:
                raise HdfsRawIngestionError(
                    f"existing source manifest provenance conflicts for "
                    f"{local_file.source_file}: {field_name}"
                )
            continue
        if field_name not in _MUTABLE_MANIFEST_FIELDS and stored.get(field_name) != expected:
            raise HdfsRawIngestionError(
                f"existing source manifest metadata conflicts for {local_file.source_file}"
            )


def _assert_existing_sidecar(
    sidecar: Mapping[str, object],
    bundle: LocalSourceBundle,
    raw_path: str,
    remote_files: list[RemoteFileEvidence],
) -> None:
    if sidecar.get("status") != VALIDATED or sidecar.get("source_version") != bundle.source_version:
        raise HdfsRawIngestionError(
            "existing HDFS integrity sidecar is not a matching validated snapshot"
        )
    if sidecar.get("raw_path") != raw_path or sidecar.get("replication") != EXPECTED_REPLICATION:
        raise HdfsRawIngestionError("existing HDFS integrity sidecar path or replication conflicts")
    raw_files = sidecar.get("files")
    if not isinstance(raw_files, list):
        raise HdfsRawIngestionError("existing HDFS integrity sidecar files are malformed")
    by_file = {item.get("source_file"): item for item in raw_files if isinstance(item, Mapping)}
    if set(by_file) != {evidence.source_file for evidence in remote_files}:
        raise HdfsRawIngestionError("existing HDFS integrity sidecar file set conflicts")
    for evidence in remote_files:
        item = by_file[evidence.source_file]
        if (
            item.get("size_bytes") != evidence.size_bytes
            or item.get("sha256") != evidence.sha256
            or item.get("hdfs_checksum") != evidence.hdfs_checksum
            or item.get("replication") != EXPECTED_REPLICATION
            or item.get("block_ids") != [block.block_id for block in evidence.blocks]
        ):
            raise HdfsRawIngestionError(
                f"existing HDFS integrity evidence conflicts for {evidence.source_file}"
            )


def _integrity_document(integrity: HdfsIntegrityEvidence) -> dict[str, object]:
    return {
        "schema_version": "hdfs-integrity-v1",
        "status": VALIDATED,
        "pipeline_run_id": integrity.pipeline_run_id,
        "source_version": integrity.source_version,
        "raw_path": integrity.raw_path,
        "staging_path": integrity.staging_path,
        "replication": integrity.replication,
        "created_at": integrity.created_at,
        "datanodes": [asdict(node) for node in integrity.datanodes],
        "datanode_addresses": [node.address for node in integrity.datanodes],
        "files": [
            {
                "source_file": evidence.source_file,
                "remote_path": evidence.remote_path,
                "artifact_uri": evidence.artifact_uri,
                "size_bytes": evidence.size_bytes,
                "sha256": evidence.sha256,
                "hdfs_checksum": evidence.hdfs_checksum,
                "replication": evidence.replication,
                "block_ids": [block.block_id for block in evidence.blocks],
                "blocks": [asdict(block) for block in evidence.blocks],
            }
            for evidence in integrity.files
        ],
    }


def _render_path(base_path: str, artifact_name: str, **values: str) -> str:
    try:
        return render_hdfs_artifact_path(base_path, artifact_name, **values)
    except HdfsPathError as error:
        raise HdfsRawIngestionError(str(error)) from error


def _hdfs_uri(uri: str, path: str) -> str:
    return uri.rstrip("/") + path


def _load_json_object(path: Path, label: str) -> dict[str, object]:
    value = _load_json(path, label)
    if not isinstance(value, dict):
        raise HdfsRawIngestionError(f"{label} must contain a JSON object")
    return value


def _load_json_array(path: Path, label: str) -> list[object]:
    value = _load_json(path, label)
    if not isinstance(value, list):
        raise HdfsRawIngestionError(f"{label} must contain a JSON array")
    return value


def _load_json(path: Path, label: str) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise HdfsRawIngestionError(f"cannot load {label} {path}: {error}") from error


def _decode_array(payload: bytes, label: str) -> list[object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise HdfsRawIngestionError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if not isinstance(value, list):
        raise HdfsRawIngestionError(f"{label} must be a JSON array")
    return value


def _decode_object(payload: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise HdfsRawIngestionError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise HdfsRawIngestionError(f"{label} must be a JSON object")
    return value


def _required_string(value: Mapping[str, object], name: str) -> str:
    result = value.get(name)
    if not isinstance(result, str) or not result:
        raise HdfsRawIngestionError(f"source artifact field {name} must be a non-empty string")
    return result


def _string_or_error(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise HdfsRawIngestionError(f"{label} must be a non-empty string")
    return value


def _validate_sha256(value: str, label: str) -> None:
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise HdfsRawIngestionError(f"{label} must be a sha256:<64 lowercase hex> value")


def _hash_local_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(CSV_CHUNK_SIZE):
                size += len(chunk)
                digest.update(chunk)
    except OSError as error:
        raise HdfsRawIngestionError(f"cannot hash local file {path}: {error}") from error
    return size, f"sha256:{digest.hexdigest()}"


def _safe_stat_size(path: Path, filename: str) -> int:
    try:
        return path.stat().st_size
    except OSError as error:
        raise HdfsRawIngestionError(f"cannot stat local file {filename}: {error}") from error


def _sha256(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _json_bytes(value: object) -> bytes:
    serializable = _jsonable(value)
    return (
        json.dumps(serializable, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _jsonable(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _exists(client: Any, path: str) -> bool:
    try:
        return bool(client.exists(path))
    except Exception as error:
        raise HdfsRawIngestionError(f"cannot inspect HDFS path {path}: {error}") from error


def _is_directory(client: Any, path: str) -> bool:
    try:
        return bool(client.is_directory(path))
    except Exception as error:
        raise HdfsRawIngestionError(f"cannot inspect HDFS directory {path}: {error}") from error
