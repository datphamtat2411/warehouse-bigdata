"""Permission-aware regressions for M04 namespace and owner publication."""

from __future__ import annotations

import copy
import json
from pathlib import PurePosixPath

import pytest

from tests.unit.test_hdfs_raw_ingestion import _run_manifest
from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_namespace import (
    HdfsNamespaceError,
    provision_publication_namespace,
    publication_parents,
    validate_publication_namespace,
)
from warehouse_bigdata.common.hdfs_publication import (
    finalize_publication,
    stage_publication,
)

CONFIG = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")
RUN_ID = "permission-test-001"
STAGING = CONFIG.base_path + "/staging/pipeline_run_id=" + RUN_ID
FINAL = CONFIG.base_path + "/curated/sales_daily/source_snapshot_id=unique-001"
MANIFEST = CONFIG.base_path + "/manifests/runs/" + RUN_ID + ".json"


class PermissionHdfs:
    """Model parent W+X for rename/delete separately from child write access."""

    def __init__(self) -> None:
        self.identity = "hadoop"
        self.entries = {
            path: {"owner": "hadoop", "permission": "755", "type": "DIRECTORY"}
            for path in ("/", "/data", CONFIG.base_path)
        }
        self.files: dict[str, bytes] = {}
        self.calls: list[tuple] = []
        self.denied: set[str] = set()

    def effective_user(self):
        return self.identity

    def path_status(self, path):
        return self.entries.get(path)

    def can_access(self, path, permission):
        if path in self.denied:
            return False
        metadata = self.entries[path]
        digit = int(metadata["permission"][-3 if self.identity == metadata["owner"] else -1])
        required = sum({"r": 4, "w": 2, "x": 1, "-": 0}[letter] for letter in permission)
        return digit & required == required

    def exists(self, path):
        return path in self.entries

    def is_directory(self, path):
        return self.entries.get(path, {}).get("type") == "DIRECTORY"

    def mkdir_exclusive(self, path):
        assert path not in self.entries
        parent = str(PurePosixPath(path).parent)
        if not self.can_access(parent, "wx"):
            raise PermissionError(parent)
        self.entries[path] = {"owner": self.identity, "permission": "755", "type": "DIRECTORY"}
        self.calls.append(("mkdir", path))

    def set_permission(self, path, mode, *, recursive=False):
        self.calls.append(("chmod", path, mode, recursive))
        for child in self.entries:
            if child == path or (recursive and child.startswith(path + "/")):
                self.entries[child]["permission"] = (
                    ("555" if self.entries[child]["type"] == "DIRECTORY" else "444")
                    if mode == "a=rX"
                    else mode
                )

    def clear_acl(self, path, *, recursive=False):
        self.calls.append(("clear_acl", path, recursive))

    def set_owner(self, path, owner, *, recursive=False):
        assert self.identity == "hadoop"
        self.calls.append(("chown", path, owner, recursive))
        for child in self.entries:
            if child == path or (recursive and child.startswith(path + "/")):
                self.entries[child]["owner"] = owner

    def get_acl(self, path):
        metadata = self.entries[path]
        return f"# owner: {metadata['owner']}\nuser::rwx\ngroup::r-x\nother::r-x\n"

    def upload_bytes(self, payload, path):
        if path in self.entries:
            raise FileExistsError(path)
        if not self.can_access(str(PurePosixPath(path).parent), "wx"):
            raise PermissionError(path)
        self.entries[path] = {"owner": self.identity, "permission": "644", "type": "FILE"}
        self.files[path] = payload
        self.calls.append(("upload", path))

    def read_remote(self, path):
        return self.files[path]

    def rename_exclusive(self, source, destination):
        for parent in (str(PurePosixPath(source).parent), str(PurePosixPath(destination).parent)):
            if not self.can_access(parent, "wx"):
                raise PermissionError(parent)
        if destination in self.entries:
            raise FileExistsError(destination)
        self.calls.append(("rename", source, destination))
        for path in tuple(self.entries):
            if path == source or path.startswith(source + "/"):
                target = destination + path[len(source) :]
                self.entries[target] = self.entries.pop(path)
                if path in self.files:
                    self.files[target] = self.files.pop(path)

    def remove(self, path):
        if not self.can_access(str(PurePosixPath(path).parent), "wx"):
            raise PermissionError(path)
        self.calls.append(("remove", path))
        for child in tuple(self.entries):
            if child == path or child.startswith(path + "/"):
                del self.entries[child]
                self.files.pop(child, None)


def test_missing_parent_reports_exact_path_identity_and_required_access():
    client = PermissionHdfs()
    client.identity = "spark"
    with pytest.raises(HdfsNamespaceError, match=r"/data/warehouse/curated.*spark.*r-x"):
        validate_publication_namespace(client, CONFIG)
    assert client.calls == []


def test_owner_provisioning_is_minimal_and_idempotent_and_preserves_existing_data():
    client = PermissionHdfs()
    raw = CONFIG.base_path + "/raw"
    client.entries[raw] = {"owner": "hadoop", "permission": "555", "type": "DIRECTORY"}
    before = copy.deepcopy(client.entries)
    provision_publication_namespace(client, CONFIG)
    first = copy.deepcopy(client.entries)
    client.calls.clear()
    provision_publication_namespace(client, CONFIG)
    assert client.entries == first
    assert client.calls == []
    assert all(client.entries[path] == entry for path, entry in before.items())
    assert len(publication_parents(CONFIG)) == 8
    assert CONFIG.base_path + "/models" in publication_parents(CONFIG)
    assert CONFIG.base_path + "/features/demand_forecasting" in publication_parents(CONFIG)
    client.identity = "spark"
    validate_publication_namespace(client, CONFIG)
    for path in (*publication_parents(CONFIG), CONFIG.base_path, CONFIG.base_path + "/staging"):
        assert not client.can_access(path, "w")


def test_j06_forecast_namespace_is_owner_provisioned_and_spark_readonly():
    client = PermissionHdfs()
    provision_publication_namespace(client, CONFIG)
    assert CONFIG.base_path + "/outputs/forecasts" in publication_parents(CONFIG)
    client.identity = "spark"
    validate_publication_namespace(client, CONFIG)
    for path in (CONFIG.base_path + "/outputs", CONFIG.base_path + "/outputs/forecasts"):
        assert not client.can_access(path, "w")


def test_spark_cannot_provision_or_silently_bypass_denied_owner_write():
    client = PermissionHdfs()
    client.identity = "spark"
    with pytest.raises(HdfsNamespaceError, match="hadoop.*spark"):
        provision_publication_namespace(client, CONFIG)
    assert client.calls == []
    client.identity = "hadoop"
    provision_publication_namespace(client, CONFIG)
    client.denied.add(CONFIG.base_path + "/manifests/runs")
    with pytest.raises(HdfsNamespaceError, match=r"manifests/runs.*hadoop.*rwx"):
        validate_publication_namespace(client, CONFIG, probe_writes=True)


def _staged(client):
    provision_publication_namespace(client, CONFIG)
    client.mkdir_exclusive(STAGING)
    candidate = STAGING + "/candidate"
    client.mkdir_exclusive(candidate)
    manifest = _run_manifest(RUN_ID, "sha256:" + "a" * 64, CONFIG.uri + MANIFEST)
    manifest.update(
        source_snapshot_id="unique-001",
        producer="warehouse-bigdata:normalize-sales-daily",
        output_dataset_names='["sales_daily"]',
        output_dataset_uris=json.dumps([CONFIG.uri + FINAL]),
        quality_report_uri=CONFIG.uri + STAGING + "/quality_report.json",
    )
    client.upload_bytes(
        json.dumps(
            {
                "status": "VALIDATED",
                "pipeline_run_id": RUN_ID,
                "source_snapshot_id": "unique-001",
                "source_version": manifest["source_version"],
                "artifact_uri": CONFIG.uri + FINAL,
            }
        ).encode(),
        candidate + "/_snapshot_manifest.json",
    )
    client.upload_bytes(b"bounded parquet stand-in", candidate + "/part-00000.parquet")
    client.upload_bytes(b'{"status":"VALIDATED"}', STAGING + "/quality_report.json")
    stage_publication(client, CONFIG, manifest, result_status="VALIDATED")
    client.set_owner(candidate, "spark", recursive=True)
    return candidate


def test_owner_seals_candidates_before_atomic_promotion_and_publishes_manifest_last():
    client = PermissionHdfs()
    candidate = _staged(client)
    client.calls.clear()
    result = finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID)
    assert result == "VALIDATED"
    assert not client.exists(candidate)
    assert client.exists(FINAL) and client.exists(MANIFEST)
    rename = next(
        index for index, call in enumerate(client.calls) if call[0] == "rename" and call[2] == FINAL
    )
    seal = next(
        index for index, call in enumerate(client.calls) if call[0] == "chown" and call[3] is True
    )
    assert seal < rename
    assert client.entries[FINAL]["owner"] == "hadoop"
    assert [call for call in client.calls if call[0] == "rename"][-1][2] == MANIFEST

    client.identity = "spark"
    assert not client.can_access(FINAL, "w")
    assert not client.can_access(FINAL + "/part-00000.parquet", "w")
    with pytest.raises(PermissionError):
        client.remove(FINAL)
    with pytest.raises(PermissionError):
        client.rename_exclusive(FINAL, FINAL + "-stolen")


def test_published_snapshots_and_manifest_are_untouched_by_idempotent_retry():
    client = PermissionHdfs()
    _staged(client)
    finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID)
    before = copy.deepcopy((client.entries, client.files))
    client.calls.clear()
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID) == "REUSED"
    assert (client.entries, client.files) == before
    assert client.calls == []


def test_legacy_owner_writable_snapshot_remains_read_only_to_spark_and_is_reused():
    client = PermissionHdfs()
    _staged(client)
    finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID)
    # Older owner-published snapshots used 755/644. Owner W is not Spark W.
    client.entries[FINAL]["permission"] = "755"
    client.entries[FINAL + "/part-00000.parquet"]["permission"] = "644"
    before = copy.deepcopy((client.entries, client.files))
    client.calls.clear()
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID) == "REUSED"
    assert (client.entries, client.files) == before
    assert client.calls == []
    client.identity = "spark"
    assert not client.can_access(FINAL, "w")
    assert not client.can_access(FINAL + "/part-00000.parquet", "w")


def test_existing_snapshot_conflict_and_missing_parent_fail_before_any_promotion():
    client = PermissionHdfs()
    candidate = _staged(client)
    client.entries[FINAL] = {"owner": "hadoop", "permission": "555", "type": "DIRECTORY"}
    client.calls.clear()
    with pytest.raises(HdfsNamespaceError, match="both exist|conflict"):
        finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID)
    assert not client.calls
    assert client.exists(candidate)
    del client.entries[FINAL]
    del client.entries[str(PurePosixPath(FINAL).parent)]
    with pytest.raises(HdfsNamespaceError, match="sales_daily.*hadoop.*rwx"):
        finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID)
    assert not client.calls


def test_owner_retry_resumes_a_sealed_candidate_after_interrupted_promotion():
    client = PermissionHdfs()
    _staged(client)
    rename = client.rename_exclusive

    def interrupted(source, destination):
        if destination == FINAL:
            raise OSError("publication interrupted")
        rename(source, destination)

    client.rename_exclusive = interrupted
    with pytest.raises(OSError, match="interrupted"):
        finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID)
    assert not client.exists(FINAL) and not client.exists(MANIFEST)
    client.rename_exclusive = rename
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN_ID) == "VALIDATED"
    assert client.exists(FINAL) and client.exists(MANIFEST)


def test_webhdfs_publication_uses_exact_no_overwrite_rename(monkeypatch):
    from warehouse_bigdata.common.hdfs_client import HdfsCliClient

    client = HdfsCliClient()
    calls = []
    monkeypatch.setattr(
        client, "_webhdfs", lambda path, operation, **kw: calls.append((path, operation, kw))
    )
    client.rename_exclusive("/candidate", "/snapshot")
    assert calls == [
        (
            "/candidate",
            "RENAME",
            {"method": "PUT", "destination": "/snapshot", "renameoptions": "NONE"},
        )
    ]
