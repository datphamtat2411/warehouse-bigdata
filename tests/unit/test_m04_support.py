"""Offline tests for M04 runtime dependencies and bounded evidence helpers."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.integration import m04_support
from tests.integration.hdfs_support import VerificationError
from warehouse_bigdata.common import spark_hdfs_client
from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_client import HdfsCommandError
from warehouse_bigdata.common.hdfs_paths import render_staging_path
from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient

HDFS_CONFIG = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")


def test_m04_event_collection_accepts_measured_scale_with_its_own_finite_bound(
    tmp_path, monkeypatch
):
    from tests.integration import spark_support

    payload = b" " * (spark_support.MAX_EVENT_BYTES + 1) + b'{"Event":"fixture"}\n'
    path = "/events/app-20261022134732-0002"
    client = SimpleNamespace(
        list_children=lambda directory: (path,),
        stat=lambda remote: SimpleNamespace(size_bytes=len(payload)),
        read_remote=lambda remote: payload,
    )
    monkeypatch.setattr(m04_support, "summarize_m04_event_log", lambda data, **kwargs: {})
    result = m04_support._collect_event_summary(
        client, "/events", run_id="run", application_name="job", output_dir=tmp_path
    )
    assert result["event_log"]["bytes"] == len(payload)
    assert (tmp_path / "event-log.jsonl").read_bytes() == payload
    client.stat = lambda remote: SimpleNamespace(size_bytes=spark_support.MAX_M04_EVENT_BYTES + 1)
    client.read_remote = lambda remote: pytest.fail("oversized evidence must fail before retrieval")
    with pytest.raises(VerificationError, match="evidence bound"):
        m04_support._collect_event_summary(
            client, "/events", run_id="run", application_name="job", output_dir=tmp_path
        )


class _DirectoryClient:
    def __init__(self) -> None:
        self.directories = {"/data/warehouse", "/data/warehouse/staging"}
        self.calls: list[tuple[str, str]] = []

    def exists(self, path: str) -> bool:
        self.calls.append(("exists", path))
        return path in self.directories

    def is_directory(self, path: str) -> bool:
        self.calls.append(("is_directory", path))
        return path in self.directories

    def mkdir_parents(self, path: str) -> None:
        self.calls.append(("mkdir_parents", path))
        self.directories.add(path)

    def mkdir_exclusive(self, path: str) -> None:
        self.calls.append(("mkdir_exclusive", path))
        self.directories.add(path)


def _event_path(run_id: str, event_name: str = "events") -> str:
    return f"{render_staging_path(HDFS_CONFIG.base_path, run_id)}/{event_name}"


class _Client:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _local(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def list_children(self, path: str) -> tuple[str, ...]:
        return tuple(
            sorted(f"{path.rstrip('/')}/{child.name}" for child in self._local(path).iterdir())
        )

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


def test_failed_submission_shows_both_bounded_streams_and_keeps_full_logs(
    tmp_path: Path,
) -> None:
    stdout = b"stdout-preamble\n" + b"x" * 3_000 + b"\nFileNotFoundError: 'hdfs'\n"
    stderr = b"stderr-preamble\n" + b"y" * 3_000 + b"\nWARN NativeCodeLoader\n"
    completed = SimpleNamespace(returncode=1, stdout=stdout, stderr=stderr)
    hdfs = SimpleNamespace(runner=SimpleNamespace(run=lambda *args, **kwargs: completed))

    with pytest.raises(VerificationError) as failure:
        m04_support._submit(hdfs, "job.py", ["--run-id", "run-001"], {}, tmp_path)

    message = str(failure.value)
    assert "Spark application failed (job.py, exit 1)" in message
    assert "stdout (last 2000 characters):\n" + stdout.decode()[-2000:] in message
    assert "stderr (last 2000 characters):\n" + stderr.decode()[-2000:] in message
    assert "stdout-preamble" not in message and "stderr-preamble" not in message
    assert f"Full logs: {tmp_path}" in message
    assert (tmp_path / "stdout").read_bytes() == stdout
    assert (tmp_path / "stderr").read_bytes() == stderr
    assert json.loads((tmp_path / "command.json").read_text())[-3:] == [
        "job.py",
        "--run-id",
        "run-001",
    ]


def test_successful_submission_returns_stdout_unchanged(tmp_path: Path) -> None:
    stdout = b"status=VALIDATED\n"
    completed = SimpleNamespace(returncode=0, stdout=stdout, stderr=b"diagnostic\n")
    hdfs = SimpleNamespace(runner=SimpleNamespace(run=lambda *args, **kwargs: completed))

    assert m04_support._submit(hdfs, "job.py", [], {}, tmp_path) == stdout


def _namenode_info() -> dict[str, object]:
    return {
        "beans": [
            {
                "name": "Hadoop:service=NameNode,name=NameNodeInfo",
                "LiveNodes": json.dumps(
                    {
                        f"hdfs-datanode-{index}:9866": {
                            "xferaddr": f"172.18.0.{index + 1}:9866",
                            "adminState": "In Service",
                            "lastContact": 2,
                            "lastBlockReport": 300,
                        }
                        for index in (1, 2)
                    }
                ),
            }
        ],
    }


def _monitoring_client(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> SparkHdfsCliClient:
    client = SparkHdfsCliClient()
    monkeypatch.setattr(
        client,
        "getconf",
        lambda key: {
            "fs.defaultFS": HDFS_CONFIG.uri,
            "dfs.namenode.http-address": "0.0.0.0:9870",
        }[key],
    )

    def open_report(url: str, *, timeout: float) -> io.BytesIO:
        assert url == (
            "http://hdfs-namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
        )
        assert 0 < timeout <= 5
        return io.BytesIO(payload)

    monkeypatch.setattr(
        spark_hdfs_client,
        "build_opener",
        lambda *args: SimpleNamespace(open=open_report),
    )
    return client


def test_m04_driver_reads_live_nodes_without_privileged_dfsadmin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _monitoring_client(monkeypatch, json.dumps(_namenode_info()).encode())

    nodes = client.report()

    assert set(nodes) == {"hdfs-datanode-1", "hdfs-datanode-2"}
    assert nodes["hdfs-datanode-1"].address == "172.18.0.2:9866"
    assert nodes["hdfs-datanode-1"].last_contact == "2 seconds ago"


@pytest.mark.parametrize("invalid", ["missing", "duplicate-address", "decommissioned", "bad-age"])
def test_m04_driver_rejects_incomplete_or_invalid_live_node_reports(
    monkeypatch: pytest.MonkeyPatch, invalid: str
) -> None:
    document = _namenode_info()
    bean = document["beans"][0]
    live = json.loads(bean["LiveNodes"])
    node = live["hdfs-datanode-2:9866"]
    if invalid == "missing":
        del live["hdfs-datanode-2:9866"]
    elif invalid == "duplicate-address":
        node["xferaddr"] = live["hdfs-datanode-1:9866"]["xferaddr"]
    elif invalid == "decommissioned":
        node["adminState"] = "Decommissioned"
    else:
        node["lastContact"] = -1
    bean["LiveNodes"] = json.dumps(live)
    client = _monitoring_client(monkeypatch, json.dumps(document).encode())

    with pytest.raises(HdfsCommandError, match="NameNode live-node report"):
        client.report()


@pytest.mark.parametrize("payload", [b"not-json", b"{}", b"x" * 65_537])
def test_m04_driver_rejects_malformed_or_oversized_monitoring_responses(
    monkeypatch: pytest.MonkeyPatch, payload: bytes
) -> None:
    client = _monitoring_client(monkeypatch, payload)

    with pytest.raises(HdfsCommandError, match="NameNode live-node report"):
        client.report()


@pytest.mark.parametrize(
    ("pipeline_run_id", "event_name"),
    [
        ("m04-correctness-full-j02-" + "a" * 32, "events"),
        ("m04-correctness-full-j03-" + "b" * 32, "events"),
    ],
)
def test_create_event_directory_accepts_canonical_j02_and_j03_paths(
    monkeypatch: pytest.MonkeyPatch,
    pipeline_run_id: str,
    event_name: str,
) -> None:
    client = _DirectoryClient()
    acl_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        m04_support,
        "prepare_spark_run_permissions",
        lambda *args, **kwargs: acl_calls.append(kwargs),
    )
    path = _event_path(pipeline_run_id, event_name)

    m04_support._create_event_directory(
        client,
        HDFS_CONFIG,
        path,
        expected_pipeline_run_id=pipeline_run_id,
    )

    staging_path = render_staging_path(HDFS_CONFIG.base_path, pipeline_run_id)
    assert staging_path in client.directories
    assert path in client.directories
    assert acl_calls == [
        {
            "pipeline_run_id": pipeline_run_id,
            "event_log_dir": HDFS_CONFIG.uri + path,
            "verify_effective_access": False,
        }
    ]


@pytest.mark.parametrize(
    "path",
    [
        _event_path("m04-correctness-full-j03-" + "a" * 32),
        _event_path("m04-correctness-full-j02-" + "a" * 32) + "/../events-other",
        "/data/warehouse/staging/pipeline_run_id=m04-correctness-full-j02-"
        + "a" * 32
        + "-unrelated/events",
        "/wrong/warehouse/staging/pipeline_run_id=m04-correctness-full-j02-" + "a" * 32 + "/events",
        "/data/warehouse/raw/m5/source_version=sha256-" + "a" * 64 + "/events",
        "/data/warehouse/curated/sales_daily/source_snapshot_id=snapshot-001/events",
        _event_path("m04-correctness-full-j02-" + "a" * 32) + "/nested",
    ],
)
def test_create_event_directory_rejects_non_owned_or_unsafe_paths(path: str) -> None:
    client = _DirectoryClient()
    expected_pipeline_run_id = "m04-correctness-full-j02-" + "a" * 32

    with pytest.raises(VerificationError, match="event directory|staging path"):
        m04_support._create_event_directory(
            client,
            HDFS_CONFIG,
            path,
            expected_pipeline_run_id=expected_pipeline_run_id,
        )

    assert client.calls == []


def test_retry_event_directory_reuses_one_run_namespace_without_touching_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _DirectoryClient()
    acl_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        m04_support,
        "prepare_spark_run_permissions",
        lambda *args, **kwargs: acl_calls.append(kwargs),
    )
    pipeline_run_id = "m04-correctness-full-j02-" + "c" * 32
    initial = _event_path(pipeline_run_id)
    retry = _event_path(pipeline_run_id, "events-retry-j02")

    m04_support._create_event_directory(
        client,
        HDFS_CONFIG,
        initial,
        expected_pipeline_run_id=pipeline_run_id,
    )
    m04_support._create_event_directory(
        client,
        HDFS_CONFIG,
        retry,
        expected_pipeline_run_id=pipeline_run_id,
    )

    assert initial in client.directories
    assert retry in client.directories
    assert "/data/warehouse/raw" not in " ".join(path for _, path in client.calls)
    assert "/data/warehouse/curated" not in " ".join(path for _, path in client.calls)
    assert [call["event_log_dir"] for call in acl_calls] == [
        HDFS_CONFIG.uri + initial,
        HDFS_CONFIG.uri + retry,
    ]


def test_run_correctness_validates_event_paths_before_source_profiling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []
    monkeypatch.setattr(m04_support, "load_hdfs_config", lambda: HDFS_CONFIG)
    monkeypatch.setattr(
        m04_support,
        "_validate_case_event_directories",
        lambda *args, **kwargs: events.append("paths"),
    )
    monkeypatch.setattr(
        m04_support,
        "prepare_source",
        lambda *args, **kwargs: events.append("source") or {},
    )
    monkeypatch.setattr(
        m04_support,
        "run_case",
        lambda *args, **kwargs: events.append("case") or {"status": "VALIDATED"},
    )

    m04_support.run_correctness(SimpleNamespace(), tmp_path)

    assert events == ["paths", "source", "case"]
