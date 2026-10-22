"""Focused checks for scoped Spark HDFS ACL preparation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_client import HdfsCommandError
from warehouse_bigdata.common.hdfs_permissions import (
    HDFS_SPARK_ACL_SPEC,
    HdfsPermissionError,
    prepare_spark_run_permissions,
)

HDFS_CONFIG = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")
RUN = "/data/warehouse/staging/pipeline_run_id=run-001"
EVENTS = f"{RUN}/events"
CHUNKS = f"{RUN}/chunks"
CANDIDATE = f"{RUN}/candidates/sales_enriched"


@dataclass
class FakeAclHdfs:
    identity: str = "hadoop"
    directories: set[str] = field(
        default_factory=lambda: {"/data/warehouse", "/data/warehouse/staging", RUN, EVENTS}
    )
    owners: dict[str, str] = field(default_factory=lambda: {RUN: "hadoop", EVENTS: "hadoop"})
    acl_paths: set[str] = field(default_factory=set)
    calls: list[tuple[Any, ...]] = field(default_factory=list)
    deny_setfacl: bool = False
    deny_probe: bool = False

    def exists(self, path: str) -> bool:
        self.calls.append(("exists", path))
        return path in self.directories

    def is_directory(self, path: str) -> bool:
        self.calls.append(("is_directory", path))
        return path in self.directories

    def set_acl(self, path: str, spec: str) -> None:
        self.calls.append(("set_acl", path, spec))
        if self.deny_setfacl or self.identity != self.owners.get(path, self.identity):
            raise HdfsCommandError(f"permission denied for {path}")
        assert spec == HDFS_SPARK_ACL_SPEC
        self.acl_paths.add(path)

    def get_acl(self, path: str) -> str:
        self.calls.append(("get_acl", path))
        access = "user:spark:rwx\nmask::rwx\n" if path in self.acl_paths else ""
        default = "default:user:spark:rwx\ndefault:mask::rwx\n" if path in self.acl_paths else ""
        return (
            f"# file: {path}\n# owner: {self.owners.get(path, self.identity)}\n"
            "# group: supergroup\nuser::rwx\ngroup::r-x\n"
            f"{access}mask::rwx\nother::r-x\n{default}"
            "default:user::rwx\ndefault:group::r-x\ndefault:other::r-x\n"
        )

    def whoami(self) -> str:
        self.calls.append(("whoami",))
        return self.identity

    def upload_bytes(self, payload: bytes, path: str) -> None:
        self.calls.append(("upload_bytes", path, payload))
        if self.deny_probe or self.identity != "spark":
            raise HdfsCommandError(f"write denied for {path}")

    def remove(self, path: str) -> None:
        self.calls.append(("remove", path))

    def add_child(self, path: str) -> None:
        parent = path.rsplit("/", 1)[0]
        assert parent in self.acl_paths
        self.directories.add(path)
        self.owners[path] = self.identity
        self.acl_paths.add(path)


def test_owner_applies_scoped_acl_without_changing_directory_ownership() -> None:
    client = FakeAclHdfs()

    paths = prepare_spark_run_permissions(
        client,
        HDFS_CONFIG,
        pipeline_run_id="run-001",
        event_log_dir=HDFS_CONFIG.uri + EVENTS,
        verify_effective_access=False,
    )

    assert paths == (RUN, EVENTS)
    assert client.owners == {RUN: "hadoop", EVENTS: "hadoop"}
    assert [call for call in client.calls if call[0] == "set_acl"] == [
        ("set_acl", RUN, HDFS_SPARK_ACL_SPEC),
        ("set_acl", EVENTS, HDFS_SPARK_ACL_SPEC),
    ]
    assert not any(call[0] == "chmod" or call[0] == "chown" for call in client.calls)


def test_default_acl_is_inherited_by_chunks_and_parquet_candidates() -> None:
    client = FakeAclHdfs()
    prepare_spark_run_permissions(
        client,
        HDFS_CONFIG,
        pipeline_run_id="run-001",
        verify_effective_access=False,
    )

    client.add_child(CHUNKS)
    client.add_child(f"{RUN}/candidates")
    client.add_child(CANDIDATE)

    assert "user:spark:rwx" in client.get_acl(CHUNKS)
    assert "default:user:spark:rwx" in client.get_acl(CHUNKS)
    assert "user:spark:rwx" in client.get_acl(CANDIDATE)
    assert "default:user:spark:rwx" in client.get_acl(CANDIDATE)


def test_spark_retry_reuses_exact_acl_and_proves_write_access() -> None:
    client = FakeAclHdfs()
    prepare_spark_run_permissions(
        client,
        HDFS_CONFIG,
        pipeline_run_id="run-001",
        event_log_dir=HDFS_CONFIG.uri + EVENTS,
        verify_effective_access=False,
    )
    client.identity = "spark"
    before = len([call for call in client.calls if call[0] == "set_acl"])

    prepare_spark_run_permissions(
        client,
        HDFS_CONFIG,
        pipeline_run_id="run-001",
        event_log_dir=HDFS_CONFIG.uri + EVENTS,
    )

    assert len([call for call in client.calls if call[0] == "set_acl"]) == before
    assert [call[0] for call in client.calls[-8:]].count("upload_bytes") == 2
    assert [call[0] for call in client.calls[-8:]].count("remove") == 2


def test_acl_denial_fails_without_a_permissive_fallback() -> None:
    client = FakeAclHdfs(deny_setfacl=True)

    with pytest.raises(HdfsPermissionError, match="ACL setup failed.*pipeline_run_id=run-001"):
        prepare_spark_run_permissions(
            client,
            HDFS_CONFIG,
            pipeline_run_id="run-001",
            verify_effective_access=False,
        )

    assert not any(call[0] == "chmod" for call in client.calls)


def test_effective_write_denial_is_reported_even_when_acl_is_present() -> None:
    client = FakeAclHdfs()
    prepare_spark_run_permissions(
        client,
        HDFS_CONFIG,
        pipeline_run_id="run-001",
        verify_effective_access=False,
    )
    client.identity = "spark"
    client.deny_probe = True

    with pytest.raises(HdfsPermissionError, match="effective Spark write access"):
        prepare_spark_run_permissions(
            client,
            HDFS_CONFIG,
            pipeline_run_id="run-001",
        )


def test_unrelated_hdfs_paths_are_rejected_before_acl_mutation() -> None:
    client = FakeAclHdfs()

    with pytest.raises(HdfsPermissionError, match="run-owned staging"):
        prepare_spark_run_permissions(
            client,
            HDFS_CONFIG,
            pipeline_run_id="run-001",
            event_log_dir="hdfs://hdfs-namenode:8020/data/warehouse/curated/unrelated",
            verify_effective_access=False,
        )

    assert not any(call[0] == "set_acl" for call in client.calls)


def test_traversal_is_rejected_before_it_can_be_normalized_into_the_run() -> None:
    client = FakeAclHdfs()

    with pytest.raises(HdfsPermissionError, match="safe HDFS path"):
        prepare_spark_run_permissions(
            client,
            HDFS_CONFIG,
            pipeline_run_id="run-001",
            event_log_dir=HDFS_CONFIG.uri + EVENTS + "/../events",
            verify_effective_access=False,
        )

    assert not any(call[0] == "set_acl" for call in client.calls)
