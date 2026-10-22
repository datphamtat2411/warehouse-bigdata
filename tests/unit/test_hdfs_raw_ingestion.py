"""Offline tests for staged M5 raw ingestion and immutable promotion."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_client import (
    BlockPlacement,
    DataNodeReport,
    HdfsCommandError,
    RemoteFileHash,
    RemoteFileStat,
)
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, SOURCE_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.ingestion import hdfs_raw
from warehouse_bigdata.ingestion.hdfs_raw import (
    HdfsRawIngestionError,
    ingest_m5_raw,
    publish_run_manifest,
    render_run_manifest_path,
    validate_hdfs_raw_snapshot,
)
from warehouse_bigdata.ingestion.m5_source import RECOGNIZED_M5_FILES, compute_source_version

SOURCE_URL = "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data"
DOWNLOADED_AT = "2026-10-21T12:34:56Z"
TEST_HDFS_CONFIG = HDFSConfig(
    uri="hdfs://hdfs-namenode:8020",
    base_path="/data/warehouse",
)
DATA = {
    "calendar.csv": b"date,wm_yr_wk\n2011-01-29,11101\n",
    "sales_train_evaluation.csv": b"id,d_1\nseries,0\n",
    "sell_prices.csv": b"store_id,item_id,wm_yr_wk,sell_price\nCA_1,FOODS_1,11101,1.0\n",
}


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _coverage_document(
    *,
    last_day: str,
    observed_day_count: int = 1969,
    missing_day_count: int = 0,
    unexpected_day_count: int = 0,
) -> str:
    return json.dumps(
        {
            "validation_scope": "local_source_file",
            "coverage": {
                "expected_day_count": 1969,
                "observed_day_count": observed_day_count,
                "missing_day_count": missing_day_count,
                "unexpected_day_count": unexpected_day_count,
                "first_day": "d_1",
                "last_day": last_day,
            },
            "duplicate_counts": {},
            "invalid_counts": {},
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _artifacts(
    root: Path,
    *,
    downloaded_at_utc: str | None = DOWNLOADED_AT,
    coverage_by_file: dict[str, str] | None = None,
) -> tuple[Path, Path, Path]:
    source = root / "source"
    source.mkdir(exist_ok=True)
    for filename, payload in DATA.items():
        (source / filename).write_bytes(payload)

    hashes = {filename: _digest(payload) for filename, payload in DATA.items()}
    source_version = compute_source_version(hashes)
    profiles: dict[str, dict[str, object]] = {}
    manifests: list[dict[str, object]] = []
    grains = {
        "calendar.csv": "calendar row",
        "sales_train_evaluation.csv": "observed sales row",
        "sell_prices.csv": "sell price row",
    }
    keys = {
        "calendar.csv": "d,date",
        "sales_train_evaluation.csv": "id",
        "sell_prices.csv": "store_id,item_id,wm_yr_wk",
    }
    for filename in sorted(RECOGNIZED_M5_FILES):
        path = source / filename
        present = filename in DATA
        profiles[filename] = {
            "source_file": filename,
            "required": filename not in {"sales_train_validation.csv", "sample_submission.csv"},
            "present": present,
            "row_count": 1 if present else 0,
            "size_bytes": path.stat().st_size if present else None,
            "sha256": hashes.get(filename),
            "schema_fingerprint": "sha256:" + "0" * 64 if present else None,
            "null_counts": {},
            "duplicate_counts": {},
            "invalid_counts": {},
            "coverage": {},
            "diagnostics": [],
            "validation_status": VALIDATED,
        }
        if not present:
            continue
        manifests.append(
            {
                "manifest_schema_version": SOURCE_MANIFEST.version,
                "contract_name": SOURCE_MANIFEST.name,
                "contract_version": SOURCE_MANIFEST.version,
                "manifest_kind": SOURCE_MANIFEST.manifest_kind,
                "status": VALIDATED,
                "producer": "warehouse-bigdata:m5-source-profile",
                "artifact_uri": path.resolve().as_uri(),
                "artifact_checksum": hashes[filename],
                "schema_fingerprint": "sha256:" + "0" * 64,
                "grain": grains[filename],
                "logical_unique_key": keys[filename],
                "row_count": 1,
                "required_field_null_counts": "{}",
                "code_revision": "warehouse-bigdata:m5-source-profile-v1",
                "configuration_digest": "sha256:" + "1" * 64,
                "created_at": DOWNLOADED_AT,
                "completed_at": DOWNLOADED_AT,
                "coverage_and_exclusion_counts": (coverage_by_file or {}).get(filename, "{}"),
                "source_version": source_version,
                "source_name": "m5-forecasting-accuracy",
                "source_url": SOURCE_URL,
                "downloaded_at_utc": downloaded_at_utc,
                "source_file": filename,
                "size_bytes": path.stat().st_size,
                "source_schema_version": "m5-source-v1",
                "source_file_row_count": 1,
            }
        )

    profile = {
        "profile_schema_version": "1.0",
        "status": VALIDATED,
        "source_version": source_version,
        "source_name": "m5-forecasting-accuracy",
        "source_url": SOURCE_URL,
        "downloaded_at_utc": downloaded_at_utc,
        "source_schema_version": "m5-source-v1",
        "validation_scope": {
            "scope": "local_source_files",
            "hdfs_validated": False,
            "spark_validated": False,
            "curated_data_validated": False,
        },
        "files": profiles,
    }
    profile_path = root / "source_profile.json"
    manifest_path = root / "source_manifests.json"
    profile_path.write_text(json.dumps(profile), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifests), encoding="utf-8")
    return source, profile_path, manifest_path


@dataclass
class FakeHdfs:
    files: dict[str, bytes]
    directories: set[str]
    calls: list[tuple[Any, ...]]
    corrupt_filename: str | None = None
    mkdir_failure: Exception | None = None

    def __init__(self) -> None:
        self.files = {}
        self.directories = {"/data/warehouse"}
        self.calls = []
        self.corrupt_filename = None
        self.mkdir_failure = None

    def getconf(self, key: str) -> str:
        self.calls.append(("getconf", key))
        return {
            "fs.defaultFS": "hdfs://hdfs-namenode:8020",
            "dfs.replication": "2",
            "dfs.namenode.acls.enabled": "true",
            "dfs.client.use.datanode.hostname": "true",
            "dfs.datanode.use.datanode.hostname": "true",
        }[key]

    def safe_mode(self) -> str:
        self.calls.append(("safe_mode",))
        return "Safe mode is OFF"

    def report(self) -> dict[str, DataNodeReport]:
        self.calls.append(("report",))
        return {
            "hdfs-datanode-1": DataNodeReport("hdfs-datanode-1", "10.0.0.1:9866", "now", "now"),
            "hdfs-datanode-2": DataNodeReport("hdfs-datanode-2", "10.0.0.2:9866", "now", "now"),
        }

    def exists(self, path: str) -> bool:
        self.calls.append(("exists", path))
        return path in self.files or path in self.directories

    def is_directory(self, path: str) -> bool:
        self.calls.append(("is_directory", path))
        return path in self.directories

    def mkdir_parents(self, path: str) -> None:
        self.calls.append(("mkdir_parents", path))
        pieces = path.strip("/").split("/")
        current = ""
        for piece in pieces:
            current += "/" + piece
            self.directories.add(current)

    def mkdir_exclusive(self, path: str) -> None:
        self.calls.append(("mkdir_exclusive", path))
        if self.mkdir_failure is not None:
            raise self.mkdir_failure
        if path in self.files or path in self.directories:
            raise HdfsCommandError("path already exists")
        self.directories.add(path)

    def upload_file(self, local_path: Path, remote_path: str) -> None:
        self.calls.append(("upload_file", remote_path))
        if remote_path in self.files:
            raise HdfsCommandError("file already exists")
        payload = local_path.read_bytes()
        if local_path.name == self.corrupt_filename:
            payload = bytes([payload[0] ^ 1]) + payload[1:]
        self.files[remote_path] = payload

    def upload_bytes(self, payload: bytes, remote_path: str) -> None:
        self.calls.append(("upload_bytes", remote_path))
        if remote_path in self.files:
            raise HdfsCommandError("file already exists")
        self.files[remote_path] = payload

    def stat(self, path: str) -> RemoteFileStat:
        self.calls.append(("stat", path))
        return RemoteFileStat(len(self.files[path]), 2)

    def hash_remote(self, path: str) -> RemoteFileHash:
        self.calls.append(("hash_remote", path))
        payload = self.files[path]
        return RemoteFileHash(len(payload), _digest(payload))

    def checksum(self, path: str) -> str:
        self.calls.append(("checksum", path))
        return "MD5-of-0MD5-of-512CRC32C:stub"

    def fsck(
        self,
        path: str,
        addresses: set[str],
        *,
        expected_replication: int = 2,
    ) -> tuple[BlockPlacement, ...]:
        self.calls.append(("fsck", path))
        if not self.files[path]:
            return ()
        return (BlockPlacement("BP-test:blk_1_1", len(self.files[path]), tuple(sorted(addresses))),)

    def list_children(self, path: str) -> tuple[str, ...]:
        self.calls.append(("list_children", path))
        prefix = path.rstrip("/") + "/"
        result = set()
        for child in (*self.directories, *self.files):
            if child.startswith(prefix) and "/" not in child[len(prefix) :]:
                result.add(child)
        return tuple(sorted(result))

    def read_remote(self, path: str) -> bytes:
        self.calls.append(("read_remote", path))
        return self.files[path]

    def rename(self, source: str, destination: str) -> None:
        self.calls.append(("rename", source, destination))
        if destination in self.directories or destination in self.files:
            raise HdfsCommandError("destination exists")
        moved_directories = {
            path: destination + path.removeprefix(source)
            for path in self.directories
            if path == source or path.startswith(source + "/")
        }
        moved_files = {
            path: destination + path.removeprefix(source)
            for path in self.files
            if path.startswith(source + "/")
        }
        self.directories.difference_update(moved_directories)
        self.directories.update(moved_directories.values())
        for path, moved in moved_files.items():
            self.files[moved] = self.files.pop(path)

    def remove(self, path: str) -> None:
        self.calls.append(("remove", path))
        for child in list(self.files):
            if child == path or child.startswith(path + "/"):
                del self.files[child]
        self.directories.difference_update(
            {child for child in self.directories if child == path or child.startswith(path + "/")}
        )


def _ingest(
    root: Path,
    client: FakeHdfs,
    run_id: str = "run-one",
    *,
    downloaded_at_utc: str | None = DOWNLOADED_AT,
    coverage_by_file: dict[str, str] | None = None,
):
    source, profile, manifests = _artifacts(
        root,
        downloaded_at_utc=downloaded_at_utc,
        coverage_by_file=coverage_by_file,
    )
    return ingest_m5_raw(
        source,
        profile,
        manifests,
        run_id,
        client=client,
        config=TEST_HDFS_CONFIG,
        now=lambda: DOWNLOADED_AT,
    )


def _replace_remote_manifest_field(
    client: FakeHdfs,
    raw_path: str,
    filename: str,
    field_name: str,
    value: object,
) -> None:
    manifest_path = f"{raw_path}/source_manifests.json"
    records = json.loads(client.files[manifest_path])
    for record in records:
        if record["source_file"] == filename:
            record[field_name] = value
            client.files[manifest_path] = json.dumps(records).encode()
            return
    raise AssertionError(f"missing remote manifest for {filename}")


def test_fake_client_ingestion_does_not_require_hdfs_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WAREHOUSE_HDFS_URI", raising=False)
    monkeypatch.delenv("WAREHOUSE_HDFS_BASE_PATH", raising=False)
    client = FakeHdfs()

    result = _ingest(tmp_path, client)

    assert result.status == "PROMOTED"
    assert client.files[f"{result.raw_path}/calendar.csv"] == DATA["calendar.csv"]


def test_preflight_rejects_changed_local_bytes_before_hdfs_mutation(tmp_path: Path) -> None:
    source, profile, manifests = _artifacts(tmp_path)
    (source / "calendar.csv").write_bytes(b"changed")
    client = FakeHdfs()

    with pytest.raises(HdfsRawIngestionError, match="calendar.csv"):
        ingest_m5_raw(source, profile, manifests, "run-preflight", client=client)

    assert client.calls == []


def test_source_version_mismatch_is_rejected_before_hdfs_mutation(tmp_path: Path) -> None:
    source, profile, manifests = _artifacts(tmp_path)
    document = json.loads(profile.read_text(encoding="utf-8"))
    document["source_version"] = "sha256:" + "f" * 64
    profile.write_text(json.dumps(document), encoding="utf-8")
    client = FakeHdfs()

    with pytest.raises(HdfsRawIngestionError, match="source_version"):
        ingest_m5_raw(source, profile, manifests, "run-version", client=client)

    assert client.calls == []


def test_preflight_accepts_consistent_null_acquisition_provenance(tmp_path: Path) -> None:
    client = FakeHdfs()

    result = _ingest(tmp_path, client, "run-null-provenance", downloaded_at_utc=None)

    assert result.status == "PROMOTED"
    assert result.manifests
    assert all(manifest["downloaded_at_utc"] is None for manifest in result.manifests)


def test_raw_ingestion_uses_the_production_client_factory_when_not_injected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, profile, manifests = _artifacts(tmp_path)
    client = FakeHdfs()
    config = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")
    calls: list[HDFSConfig] = []
    monkeypatch.setattr(
        hdfs_raw,
        "create_hdfs_cli_client",
        lambda received: calls.append(received) or client,
    )

    result = ingest_m5_raw(
        source,
        profile,
        manifests,
        "run-factory",
        config=config,
        now=lambda: DOWNLOADED_AT,
    )

    assert result.status == "PROMOTED"
    assert calls == [config]


def test_preflight_rejects_profile_manifest_provenance_mismatch(tmp_path: Path) -> None:
    source, profile, manifests = _artifacts(tmp_path, downloaded_at_utc=None)
    records = json.loads(manifests.read_text(encoding="utf-8"))
    records[0]["downloaded_at_utc"] = DOWNLOADED_AT
    manifests.write_text(json.dumps(records), encoding="utf-8")
    client = FakeHdfs()

    with pytest.raises(HdfsRawIngestionError, match="downloaded_at_utc"):
        ingest_m5_raw(source, profile, manifests, "run-mismatched-provenance", client=client)

    assert client.calls == []


def test_remote_mismatch_cleans_only_owned_staging_path(tmp_path: Path) -> None:
    source, profile, manifests = _artifacts(tmp_path)
    client = FakeHdfs()
    client.corrupt_filename = "calendar.csv"

    with pytest.raises(HdfsRawIngestionError, match="SHA-256"):
        ingest_m5_raw(
            source,
            profile,
            manifests,
            "run-corrupt",
            client=client,
            config=TEST_HDFS_CONFIG,
        )

    assert ("remove", "/data/warehouse/staging/pipeline_run_id=run-corrupt") in client.calls
    assert not any(call[0] == "rename" for call in client.calls)
    assert not client.exists("/data/warehouse/raw/m5/source_version=" + "x")


def test_promotion_happens_after_metadata_is_verified(tmp_path: Path) -> None:
    source, profile, manifests = _artifacts(tmp_path)
    client = FakeHdfs()

    result = _ingest(tmp_path, client, "run-order")

    stage_manifest = "/data/warehouse/staging/pipeline_run_id=run-order/source_manifests.json"
    read_index = client.calls.index(("read_remote", stage_manifest))
    rename_index = next(index for index, call in enumerate(client.calls) if call[0] == "rename")
    assert read_index < rename_index
    assert result.status == "PROMOTED"


def test_matching_rerun_is_idempotent_without_overwrite_or_new_stage(tmp_path: Path) -> None:
    client = FakeHdfs()
    first = _ingest(tmp_path, client, "run-first")
    before = len(client.calls)

    second = _ingest(tmp_path, client, "run-second")

    assert second.status == "REUSED"
    assert second.raw_path == first.raw_path
    mutating = {
        "mkdir_parents",
        "mkdir_exclusive",
        "upload_file",
        "upload_bytes",
        "rename",
        "remove",
    }
    assert not any(call[0] in mutating for call in client.calls[before:])


def test_legacy_calendar_coverage_is_accepted_for_matching_reuse(tmp_path: Path) -> None:
    client = FakeHdfs()
    legacy = _coverage_document(last_day="d_999")
    corrected = _coverage_document(last_day="d_1969")

    first = _ingest(
        tmp_path,
        client,
        "run-legacy-calendar",
        coverage_by_file={"calendar.csv": legacy},
    )
    second = _ingest(
        tmp_path,
        client,
        "run-corrected-calendar",
        coverage_by_file={"calendar.csv": corrected},
    )

    assert first.status == "PROMOTED"
    assert second.status == "REUSED"
    assert second.raw_path == first.raw_path


@pytest.mark.parametrize(
    ("legacy_coverage", "corrected_coverage"),
    [
        (
            _coverage_document(last_day="d_999", observed_day_count=1968, missing_day_count=1),
            _coverage_document(last_day="d_1969"),
        ),
        (
            _coverage_document(last_day="d_999"),
            _coverage_document(last_day="d_1969", observed_day_count=1968, missing_day_count=1),
        ),
    ],
)
def test_legacy_calendar_coverage_requires_complete_coverage(
    tmp_path: Path,
    legacy_coverage: str,
    corrected_coverage: str,
) -> None:
    client = FakeHdfs()
    _ingest(
        tmp_path,
        client,
        "run-incomplete-legacy-calendar",
        coverage_by_file={"calendar.csv": legacy_coverage},
    )
    before = dict(client.files)

    with pytest.raises(HdfsRawIngestionError, match="coverage_and_exclusion_counts"):
        _ingest(
            tmp_path,
            client,
            "run-incomplete-corrected-calendar",
            coverage_by_file={"calendar.csv": corrected_coverage},
        )

    assert client.files == before


@pytest.mark.parametrize(
    ("filename", "field_name", "value", "message"),
    [
        (
            "sell_prices.csv",
            "coverage_and_exclusion_counts",
            _coverage_document(last_day="d_1"),
            "coverage_and_exclusion_counts",
        ),
        ("calendar.csv", "source_file_row_count", 2, "source_file_row_count"),
    ],
)
def test_legacy_calendar_exception_rejects_unrelated_manifest_mismatches(
    tmp_path: Path,
    filename: str,
    field_name: str,
    value: object,
    message: str,
) -> None:
    client = FakeHdfs()
    first = _ingest(
        tmp_path,
        client,
        "run-unrelated-mismatch-first",
        coverage_by_file={"calendar.csv": _coverage_document(last_day="d_999")},
    )
    _replace_remote_manifest_field(client, first.raw_path, filename, field_name, value)
    before = dict(client.files)

    with pytest.raises(HdfsRawIngestionError, match=message):
        _ingest(
            tmp_path,
            client,
            "run-unrelated-mismatch-second",
            coverage_by_file={"calendar.csv": _coverage_document(last_day="d_1969")},
        )

    assert client.files == before


def test_legacy_calendar_reuse_does_not_rewrite_existing_snapshot(tmp_path: Path) -> None:
    client = FakeHdfs()
    first = _ingest(
        tmp_path,
        client,
        "run-immutable-legacy-calendar",
        coverage_by_file={"calendar.csv": _coverage_document(last_day="d_999")},
    )
    before_files = dict(client.files)
    before_calls = len(client.calls)

    reused = _ingest(
        tmp_path,
        client,
        "run-immutable-corrected-calendar",
        coverage_by_file={"calendar.csv": _coverage_document(last_day="d_1969")},
    )

    assert reused.status == "REUSED"
    assert client.files == before_files
    assert json.loads(client.files[f"{first.raw_path}/source_manifests.json"]) == json.loads(
        before_files[f"{first.raw_path}/source_manifests.json"]
    )
    mutating = {
        "mkdir_parents",
        "mkdir_exclusive",
        "upload_file",
        "upload_bytes",
        "rename",
        "remove",
    }
    assert not any(call[0] in mutating for call in client.calls[before_calls:])


def test_validated_raw_snapshot_can_be_rechecked_before_spark_reads(
    tmp_path: Path,
) -> None:
    client = FakeHdfs()
    ingested = _ingest(tmp_path, client, "run-raw-recheck")

    validated = validate_hdfs_raw_snapshot(
        ingested.source_version,
        client=client,
        config=TEST_HDFS_CONFIG,
    )

    assert validated.status == VALIDATED
    assert validated.raw_path == ingested.raw_path
    assert {manifest["source_file"] for manifest in validated.manifests} == set(DATA)


def test_conflicting_existing_snapshot_is_rejected_without_modification(tmp_path: Path) -> None:
    client = FakeHdfs()
    first = _ingest(tmp_path, client, "run-existing")
    client.files[first.raw_path + "/calendar.csv"] = b"conflict"
    before = len(client.calls)

    with pytest.raises(HdfsRawIngestionError, match="calendar.csv"):
        _ingest(tmp_path, client, "run-conflict")

    mutating = {
        "mkdir_parents",
        "mkdir_exclusive",
        "upload_file",
        "upload_bytes",
        "rename",
        "remove",
    }
    assert not any(call[0] in mutating for call in client.calls[before:])


def test_existing_snapshot_rejects_conflicting_acquisition_provenance(tmp_path: Path) -> None:
    client = FakeHdfs()
    first = _ingest(tmp_path, client, "run-provenance-first")
    before = len(client.calls)

    with pytest.raises(HdfsRawIngestionError, match="provenance"):
        _ingest(
            tmp_path,
            client,
            "run-provenance-second",
            downloaded_at_utc=None,
        )

    assert first.status == "PROMOTED"
    mutating = {
        "mkdir_parents",
        "mkdir_exclusive",
        "upload_file",
        "upload_bytes",
        "rename",
        "remove",
    }
    assert not any(call[0] in mutating for call in client.calls[before:])


def test_uncertain_exclusive_creation_does_not_grant_cleanup_ownership(tmp_path: Path) -> None:
    client = FakeHdfs()
    client.mkdir_failure = HdfsCommandError("mkdir timed out")

    with pytest.raises(HdfsRawIngestionError, match="staging"):
        _ingest(tmp_path, client, "run-uncertain")

    assert not any(call[0] == "remove" for call in client.calls)


def _run_manifest(run_id: str, source_version: str, artifact_uri: str) -> dict[str, object]:
    return {
        "manifest_schema_version": "1.0",
        "contract_name": PIPELINE_RUN_MANIFEST.name,
        "contract_version": PIPELINE_RUN_MANIFEST.version,
        "manifest_kind": PIPELINE_RUN_MANIFEST.manifest_kind,
        "status": VALIDATED,
        "producer": "warehouse-bigdata:ingest-m5-orchestration",
        "artifact_uri": artifact_uri,
        "artifact_checksum": None,
        "schema_fingerprint": "sha256:" + "1" * 64,
        "grain": "One M5 ingestion pipeline execution",
        "logical_unique_key": "pipeline_run_id",
        "row_count": 1,
        "required_field_null_counts": "{}",
        "code_revision": "warehouse-bigdata:ingest-m5-v1",
        "configuration_digest": "warehouse-bigdata:ingest-m5-config-v1",
        "created_at": DOWNLOADED_AT,
        "completed_at": DOWNLOADED_AT,
        "coverage_and_exclusion_counts": "{}",
        "pipeline_run_id": run_id,
        "source_version": source_version,
        "output_dataset_names": '["raw_m5"]',
        "output_dataset_uris": '["hdfs://hdfs-namenode:8020/data/warehouse/raw"]',
        "quality_report_uri": "file:///tmp/quality.json",
    }


def test_run_manifest_publication_is_exclusive_and_independently_verified(tmp_path: Path) -> None:
    client = FakeHdfs()
    config = TEST_HDFS_CONFIG
    source, profile, manifests = _artifacts(tmp_path)
    raw = ingest_m5_raw(
        source,
        profile,
        manifests,
        "run-manifest-source",
        client=client,
        config=config,
        now=lambda: DOWNLOADED_AT,
    )
    path = render_run_manifest_path(config.base_path, "run-manifest-publish")
    manifest = _run_manifest("run-manifest-publish", raw.source_version, config.uri + path)
    validate_manifest(PIPELINE_RUN_MANIFEST, manifest)

    published = publish_run_manifest(
        manifest,
        integrity=raw.integrity,
        client=client,
        config=config,
    )

    assert published == path
    assert client.files[path]
    before = client.files[path]
    with pytest.raises(HdfsRawIngestionError, match="refusing to overwrite"):
        publish_run_manifest(
            manifest,
            integrity=raw.integrity,
            client=client,
            config=config,
        )
    assert client.files[path] == before


def test_run_manifest_path_preserves_safe_pipeline_run_identity() -> None:
    assert render_run_manifest_path("/data/warehouse", "run-20261021") == (
        "/data/warehouse/manifests/runs/run-20261021.json"
    )
