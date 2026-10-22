"""Offline tests for bounded M04 evidence helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

from tests.integration import m04_support


class _Client:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _local(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def list_children(self, path: str) -> tuple[str, ...]:
        return tuple(sorted(f"{path.rstrip('/')}/{child.name}" for child in self._local(path).iterdir()))

    def is_directory(self, path: str) -> bool:
        return self._local(path).is_dir()

    def stat(self, path: str) -> SimpleNamespace:
        return SimpleNamespace(size_bytes=self._local(path).stat().st_size, replication=2)

    def hash_remote(self, path: str) -> SimpleNamespace:
        payload = self._local(path).read_bytes()
        return SimpleNamespace(
            size_bytes=len(payload),
            sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
        )


def test_immutable_snapshot_records_parquet_and_metadata_hashes(tmp_path: Path) -> None:
    output = "/data/warehouse/curated/sales_daily/source_snapshot_id=snapshot"
    (tmp_path / output.lstrip("/")).mkdir(parents=True)
    parquet = tmp_path / output.lstrip("/") / "part-00000.parquet"
    parquet.write_bytes(b"parquet-bytes")
    metadata = "/data/warehouse/manifests/runs/run.json"
    (tmp_path / metadata.lstrip("/")).parent.mkdir(parents=True)
    (tmp_path / metadata.lstrip("/")).write_bytes(b"manifest-bytes")

    snapshot = m04_support._immutable_snapshot(
        _Client(tmp_path),
        {"sales_daily": output},
        (metadata,),
    )

    assert snapshot["outputs"]["sales_daily"]["file_count"] == 1
    assert snapshot["outputs"]["sales_daily"]["files"][0]["sha256"].startswith("sha256:")
    assert snapshot["metadata"][metadata]["sha256"] == (
        "sha256:" + hashlib.sha256(b"manifest-bytes").hexdigest()
    )


def test_job_status_requires_one_structured_status_line() -> None:
    assert m04_support._job_status(b"status=REUSED\n") == "REUSED"
